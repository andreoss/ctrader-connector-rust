//! Connection task and the handle callers talk to it through.
//!
//! One task owns the socket. Callers hold a cloneable [`Connection`]; a request travels over a
//! bounded command channel, is framed with a fresh `clientMsgId` and resolves when the response
//! carrying the same id arrives. Messages the server pushes on its own are fanned out as
//! [`Event`]s. The task sends a heartbeat whenever nothing was written for the heartbeat
//! interval, which keeps the server from closing an idle connection.
//!
//! Backpressure: the command channel is bounded, so callers wait while the task is busy, and the
//! event stream is a bounded broadcast, so a subscriber that falls behind skips the oldest
//! events instead of stalling the task.
//!
//! Reconnection: when the socket is lost the task fails the requests in flight, reports
//! [`Event::Disconnected`] and connects again, waiting twice as long before every further
//! attempt. Once connected it sends the registered requests again in the order of their
//! [`ReplayStage`] (application, accounts, subscriptions) and then reports
//! [`Event::Reconnected`]. Requests made while the connection is down fail at once with the
//! reason of the loss.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::common::config::{ConfigurationOpenApi, DEFAULT_RECONNECT_DELAY, DEFAULT_TIMEOUT};
use crate::common::errors::{ConnectorError, ServerError, TransportError};
use crate::open_api::messages::{
    ProtoErrorRes, ProtoHeartbeatEvent, ProtoMessage, ProtoOaErrorRes,
};
use crate::open_api::payload::{Payload, Request};
use crate::open_api::transport::{Connect, Framed};

/// Pause after the last write before a heartbeat is sent.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Number of requests that may wait in the command channel.
pub const DEFAULT_COMMAND_CAPACITY: usize = 64;

/// Number of events kept for a subscriber that falls behind.
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

/// Longest pause between two reconnection attempts.
pub const RECONNECT_DELAY_LIMIT: Duration = Duration::from_secs(60);

const SHORTEST_INTERVAL: Duration = Duration::from_millis(1);

macro_rules! log_debug {
    ($($argument:tt)*) => {{
        #[cfg(feature = "logging")]
        tracing::debug!($($argument)*);
    }};
}

/// Tunable behaviour of a [`Connection`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct ConnectionOptions {
    timeout: Duration,
    heartbeat_interval: Duration,
    command_capacity: usize,
    event_capacity: usize,
    reconnect_delay: Duration,
    reconnect_attempts: Option<u32>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            reconnect_delay: DEFAULT_RECONNECT_DELAY,
            reconnect_attempts: None,
        }
    }
}

impl From<&ConfigurationOpenApi> for ConnectionOptions {
    fn from(configuration: &ConfigurationOpenApi) -> Self {
        Self::default()
            .timeout(configuration.timeout())
            .reconnect_delay(configuration.reconnect_delay())
    }
}

impl ConnectionOptions {
    /// Starts from the default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the time allowed between sending a request and receiving its response.
    ///
    /// A duration below one millisecond is raised to one millisecond.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout.max(SHORTEST_INTERVAL);
        self
    }

    /// Sets the pause after the last write before a heartbeat is sent.
    ///
    /// A duration below one millisecond is raised to one millisecond.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval.max(SHORTEST_INTERVAL);
        self
    }

    /// Sets the number of requests that may wait in the command channel; at least one.
    pub fn command_capacity(mut self, capacity: usize) -> Self {
        self.command_capacity = capacity.max(1);
        self
    }

    /// Sets the number of events kept for a subscriber that falls behind; at least one.
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity.max(1);
        self
    }

    /// Sets the pause before the first reconnection attempt; it doubles before every further
    /// attempt up to [`RECONNECT_DELAY_LIMIT`].
    ///
    /// A duration below one millisecond is raised to one millisecond.
    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay.max(SHORTEST_INTERVAL);
        self
    }

    /// Sets how many times in a row the task tries to connect again before it gives up.
    ///
    /// `None` keeps trying, which is the default; `Some(0)` turns reconnection off.
    pub const fn reconnect_attempts(mut self, attempts: Option<u32>) -> Self {
        self.reconnect_attempts = attempts;
        self
    }
}

/// Something that happened on the connection without a request asking for it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// The server sent a message that answers no pending request.
    Message(ProtoMessage),
    /// The connection was lost for the given reason; reconnection follows.
    Disconnected(TransportError),
    /// An attempt to connect again failed; another one follows unless the limit is reached.
    ReconnectFailed {
        /// Number of the attempt since the loss, starting at one.
        attempt: u32,
        /// Reason the attempt failed.
        reason: TransportError,
    },
    /// The connection is established again and the registered requests were replayed.
    Reconnected {
        /// Number of the attempt that succeeded, starting at one.
        attempt: u32,
    },
    /// A registered request was refused or left unanswered while it was replayed.
    ReplayFailed {
        /// Key the request was registered under.
        key: ReplayKey,
        /// Failure of the replayed request.
        error: ConnectorError,
    },
}

impl Event {
    /// Returns the pushed message when the event carries one of type `M`.
    #[must_use]
    pub fn decode<M: Payload>(&self) -> Option<M> {
        match self {
            Self::Message(message) => M::from_message(message).ok(),
            _ => None,
        }
    }
}

/// Counters of a connection since it was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Stats {
    /// Requests written to the socket.
    pub requests_sent: u64,
    /// Heartbeats written to the socket.
    pub heartbeats_sent: u64,
    /// Heartbeats received from the server.
    pub heartbeats_received: u64,
    /// Times the connection was established again after a loss.
    pub reconnects: u64,
}

#[derive(Debug, Default)]
struct Counters {
    requests_sent: AtomicU64,
    heartbeats_sent: AtomicU64,
    heartbeats_received: AtomicU64,
    reconnects: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Phase of the restoration a replayed request belongs to; earlier phases are replayed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ReplayStage {
    /// Authorisation of the application.
    Application,
    /// Authorisation of a trading account.
    Account,
    /// Subscription to a stream of events.
    Subscription,
}

/// Name a request is registered under for replay after a reconnection.
///
/// Registering another request under the same key replaces the earlier one.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplayKey {
    stage: ReplayStage,
    id: String,
}

impl ReplayKey {
    /// Builds a key from the phase and a name unique within it.
    #[must_use]
    pub fn new(stage: ReplayStage, id: impl Into<String>) -> Self {
        Self {
            stage,
            id: id.into(),
        }
    }

    /// Returns the phase the request is replayed in.
    #[must_use]
    pub const fn stage(&self) -> ReplayStage {
        self.stage
    }

    /// Returns the name of the request within its phase.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone)]
struct Stored {
    payload_type: u32,
    payload: Vec<u8>,
}

type Replays = BTreeMap<ReplayKey, Stored>;

type Reply = oneshot::Sender<Result<ProtoMessage, ConnectorError>>;

struct Outbound {
    payload_type: u32,
    payload: Vec<u8>,
    reply: Reply,
}

enum Command {
    Request(Outbound),
    Replay(ReplayKey, Option<Stored>),
    Reconnect,
    Shutdown,
}

/// Cloneable handle of a connection task.
///
/// The task ends when [`Connection::shutdown`] is called or the last handle is dropped.
#[derive(Debug, Clone)]
pub struct Connection {
    commands: mpsc::Sender<Command>,
    events: broadcast::Sender<Event>,
    counters: Arc<Counters>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Connection {
    /// Connects to `host` on `port` and starts the connection task.
    ///
    /// Must be called from within a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the connection cannot be established.
    pub async fn open<C: Connect>(
        connector: C,
        host: &str,
        port: u16,
        options: ConnectionOptions,
    ) -> Result<Self, ConnectorError> {
        let stream = connector.connect(host, port).await?;
        log_debug!(host, port, "connected");
        let (commands, receiver) = mpsc::channel(options.command_capacity);
        let (events, _) = broadcast::channel(options.event_capacity);
        let counters = Arc::new(Counters::default());
        let actor = Actor {
            connector,
            host: host.to_owned(),
            port,
            heartbeat_at: Instant::now() + options.heartbeat_interval,
            options,
            commands: receiver,
            events: events.clone(),
            counters: Arc::clone(&counters),
            queue: VecDeque::new(),
            pending: HashMap::new(),
            deadlines: VecDeque::new(),
            replays: Replays::new(),
            next_id: 0,
        };
        let task = tokio::spawn(actor.run(Framed::new(stream)));
        Ok(Self {
            commands,
            events,
            counters,
            task: Arc::new(Mutex::new(Some(task))),
        })
    }

    /// Sends a request and waits for its response.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server answers with an error,
    /// [`ConnectorError::Timeout`] when it does not answer in time,
    /// [`ConnectorError::Transport`] when the connection fails while the request is pending,
    /// [`ConnectorError::UnexpectedResponse`] when the answer has another type, and
    /// [`ConnectorError::Closed`] when the connection is shut down.
    pub async fn request<R: Request>(&self, request: &R) -> Result<R::Response, ConnectorError> {
        let message = self
            .request_message(R::PAYLOAD_TYPE, request.encode_to_vec())
            .await?;
        R::Response::from_message(&message)
    }

    /// Sends an already encoded request and waits for the envelope of its response.
    ///
    /// # Errors
    ///
    /// Fails like [`Connection::request`], except that the type of the answer is not checked.
    pub async fn request_message(
        &self,
        payload_type: u32,
        payload: Vec<u8>,
    ) -> Result<ProtoMessage, ConnectorError> {
        let (reply, response) = oneshot::channel();
        let outbound = Outbound {
            payload_type,
            payload,
            reply,
        };
        self.commands
            .send(Command::Request(outbound))
            .await
            .map_err(|_| ConnectorError::Closed)?;
        response.await.map_err(|_| ConnectorError::Closed)?
    }

    /// Registers a request to be sent again, under `key`, after every reconnection.
    ///
    /// The request is not sent now. Requests are replayed in the order of their keys.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down.
    pub async fn register_replay<R: Request>(
        &self,
        key: ReplayKey,
        request: &R,
    ) -> Result<(), ConnectorError> {
        let stored = Stored {
            payload_type: R::PAYLOAD_TYPE,
            payload: request.encode_to_vec(),
        };
        self.command(Command::Replay(key, Some(stored))).await
    }

    /// Forgets the request registered under `key`.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down.
    pub async fn remove_replay(&self, key: ReplayKey) -> Result<(), ConnectorError> {
        self.command(Command::Replay(key, None)).await
    }

    /// Drops the socket on this side, which starts a reconnection as a loss would.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down.
    pub async fn force_reconnect(&self) -> Result<(), ConnectorError> {
        self.command(Command::Reconnect).await
    }

    async fn command(&self, command: Command) -> Result<(), ConnectorError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| ConnectorError::Closed)
    }

    /// Subscribes to the events of the connection, starting with the next one.
    ///
    /// A subscriber that falls more than the event capacity behind skips the oldest events and
    /// is told how many were skipped.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Returns the counters of the connection.
    #[must_use]
    pub fn stats(&self) -> Stats {
        Stats {
            requests_sent: self.counters.requests_sent.load(Ordering::Relaxed),
            heartbeats_sent: self.counters.heartbeats_sent.load(Ordering::Relaxed),
            heartbeats_received: self.counters.heartbeats_received.load(Ordering::Relaxed),
            reconnects: self.counters.reconnects.load(Ordering::Relaxed),
        }
    }

    /// Stops the connection task and waits until the socket is closed.
    ///
    /// Requests still pending fail with [`ConnectorError::Closed`]. Calling it again is
    /// harmless.
    pub async fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown).await;
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

#[derive(Debug)]
enum Exit {
    Shutdown,
    Lost(TransportError),
}

struct Actor<C: Connect> {
    connector: C,
    host: String,
    port: u16,
    options: ConnectionOptions,
    commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<Event>,
    counters: Arc<Counters>,
    queue: VecDeque<Outbound>,
    pending: HashMap<String, Reply>,
    deadlines: VecDeque<(Instant, String)>,
    replays: Replays,
    next_id: u64,
    heartbeat_at: Instant,
}

impl<C: Connect> Actor<C> {
    async fn run(mut self, mut framed: Framed<C::Stream>) {
        let error = loop {
            match self.serve(&mut framed).await {
                Exit::Shutdown => break ConnectorError::Closed,
                Exit::Lost(reason) => {
                    log_debug!(%reason, "connection lost");
                    framed.close().await;
                    self.fail_in_flight(&ConnectorError::Transport(reason.clone()));
                    let _ = self.events.send(Event::Disconnected(reason.clone()));
                    match self.reconnect(reason).await {
                        Ok(fresh) => framed = fresh,
                        Err(error) => break error,
                    }
                }
            }
        };
        self.fail_all(&error);
        framed.close().await;
    }

    async fn reconnect(
        &mut self,
        reason: TransportError,
    ) -> Result<Framed<C::Stream>, ConnectorError> {
        let mut last = reason;
        let mut delay = self.options.reconnect_delay;
        let longest = RECONNECT_DELAY_LIMIT.max(delay);
        let mut attempt = 0_u32;
        loop {
            if self
                .options
                .reconnect_attempts
                .is_some_and(|limit| attempt >= limit)
            {
                return Err(ConnectorError::Transport(last));
            }
            attempt = attempt.saturating_add(1);
            hold(&mut self.commands, &mut self.replays, &last, sleep(delay)).await?;
            delay = delay.saturating_mul(2).min(longest);
            let replays: Vec<(ReplayKey, Stored)> = self
                .replays
                .iter()
                .map(|(key, stored)| (key.clone(), stored.clone()))
                .collect();
            let work = restore(
                &self.connector,
                &self.host,
                self.port,
                self.options.timeout,
                replays,
                &self.events,
            );
            match hold(&mut self.commands, &mut self.replays, &last, work).await? {
                Ok(framed) => {
                    log_debug!(attempt, "reconnected");
                    bump(&self.counters.reconnects);
                    self.heartbeat_at = Instant::now() + self.options.heartbeat_interval;
                    let _ = self.events.send(Event::Reconnected { attempt });
                    return Ok(framed);
                }
                Err(reason) => {
                    log_debug!(attempt, %reason, "reconnection failed");
                    let _ = self.events.send(Event::ReconnectFailed {
                        attempt,
                        reason: reason.clone(),
                    });
                    last = reason;
                }
            }
        }
    }

    async fn serve(&mut self, framed: &mut Framed<C::Stream>) -> Exit {
        loop {
            if let Err(reason) = self.flush(framed).await {
                return Exit::Lost(reason);
            }
            let wake = self.next_wake();
            tokio::select! {
                incoming = framed.receive() => match incoming {
                    Ok(message) => self.dispatch(message),
                    Err(reason) => return Exit::Lost(reason),
                },
                command = self.commands.recv() => match command {
                    Some(Command::Request(outbound)) => self.queue.push_back(outbound),
                    Some(Command::Replay(key, stored)) => store(&mut self.replays, key, stored),
                    Some(Command::Reconnect) => return Exit::Lost(TransportError::Closed),
                    Some(Command::Shutdown) | None => return Exit::Shutdown,
                },
                () = sleep_until(wake) => {
                    if let Err(reason) = self.tick(framed).await {
                        return Exit::Lost(reason);
                    }
                }
            }
        }
    }

    fn next_wake(&self) -> Instant {
        self.deadlines
            .front()
            .map_or(self.heartbeat_at, |(deadline, _)| {
                self.heartbeat_at.min(*deadline)
            })
    }

    async fn flush(&mut self, framed: &mut Framed<C::Stream>) -> Result<(), TransportError> {
        while let Some(outbound) = self.queue.pop_front() {
            if outbound.reply.is_closed() {
                continue;
            }
            self.next_id += 1;
            let id = self.next_id.to_string();
            let message = ProtoMessage {
                payload_type: outbound.payload_type,
                payload: Some(outbound.payload),
                client_msg_id: Some(id.clone()),
            };
            match framed.send(&message).await {
                Ok(()) => {
                    let now = Instant::now();
                    bump(&self.counters.requests_sent);
                    self.heartbeat_at = now + self.options.heartbeat_interval;
                    self.deadlines
                        .push_back((now + self.options.timeout, id.clone()));
                    self.pending.insert(id, outbound.reply);
                }
                Err(error @ TransportError::FrameTooLong { .. }) => {
                    let _ = outbound.reply.send(Err(error.into()));
                }
                Err(reason) => {
                    let _ = outbound.reply.send(Err(reason.clone().into()));
                    return Err(reason);
                }
            }
        }
        Ok(())
    }

    async fn tick(&mut self, framed: &mut Framed<C::Stream>) -> Result<(), TransportError> {
        let now = Instant::now();
        while let Some((deadline, id)) = self.deadlines.pop_front() {
            if deadline > now {
                self.deadlines.push_front((deadline, id));
                break;
            }
            if let Some(reply) = self.pending.remove(&id) {
                let _ = reply.send(Err(ConnectorError::Timeout(self.options.timeout)));
            }
        }
        if now >= self.heartbeat_at {
            framed
                .send(&ProtoHeartbeatEvent::default().to_message(None))
                .await?;
            bump(&self.counters.heartbeats_sent);
            self.heartbeat_at = Instant::now() + self.options.heartbeat_interval;
        }
        Ok(())
    }

    fn dispatch(&mut self, message: ProtoMessage) {
        if message.payload_type == ProtoHeartbeatEvent::PAYLOAD_TYPE {
            bump(&self.counters.heartbeats_received);
            return;
        }
        let reply = message
            .client_msg_id
            .as_ref()
            .and_then(|id| self.pending.remove(id));
        match reply {
            Some(reply) => {
                let _ = reply.send(outcome(message));
            }
            None => {
                let _ = self.events.send(Event::Message(message));
            }
        }
    }

    fn fail_in_flight(&mut self, error: &ConnectorError) {
        self.deadlines.clear();
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(error.clone()));
        }
        for outbound in self.queue.drain(..) {
            let _ = outbound.reply.send(Err(error.clone()));
        }
    }

    fn fail_all(&mut self, error: &ConnectorError) {
        self.fail_in_flight(error);
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            if let Command::Request(outbound) = command {
                let _ = outbound.reply.send(Err(error.clone()));
            }
        }
    }
}

fn store(replays: &mut Replays, key: ReplayKey, stored: Option<Stored>) {
    match stored {
        Some(stored) => {
            replays.insert(key, stored);
        }
        None => {
            replays.remove(&key);
        }
    }
}

async fn hold<F: Future>(
    commands: &mut mpsc::Receiver<Command>,
    replays: &mut Replays,
    reason: &TransportError,
    work: F,
) -> Result<F::Output, ConnectorError> {
    tokio::pin!(work);
    loop {
        tokio::select! {
            output = &mut work => return Ok(output),
            command = commands.recv() => match command {
                Some(Command::Request(outbound)) => {
                    let _ = outbound.reply.send(Err(reason.clone().into()));
                }
                Some(Command::Replay(key, stored)) => store(replays, key, stored),
                Some(Command::Reconnect) => {}
                Some(Command::Shutdown) | None => return Err(ConnectorError::Closed),
            },
        }
    }
}

async fn restore<C: Connect>(
    connector: &C,
    host: &str,
    port: u16,
    limit: Duration,
    replays: Vec<(ReplayKey, Stored)>,
    events: &broadcast::Sender<Event>,
) -> Result<Framed<C::Stream>, TransportError> {
    let mut framed = Framed::new(connector.connect(host, port).await?);
    for (index, (key, stored)) in replays.into_iter().enumerate() {
        let id = format!("replay-{index}");
        let message = ProtoMessage {
            payload_type: stored.payload_type,
            payload: Some(stored.payload),
            client_msg_id: Some(id.clone()),
        };
        framed.send(&message).await?;
        let failure = match timeout(limit, answer_to(&mut framed, &id, events)).await {
            Ok(answer) => outcome(answer?).err(),
            Err(_) => Some(ConnectorError::Timeout(limit)),
        };
        if let Some(error) = failure {
            log_debug!(%error, "replayed request failed");
            let _ = events.send(Event::ReplayFailed { key, error });
        }
    }
    Ok(framed)
}

async fn answer_to<S: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut Framed<S>,
    id: &str,
    events: &broadcast::Sender<Event>,
) -> Result<ProtoMessage, TransportError> {
    loop {
        let message = framed.receive().await?;
        if message.client_msg_id.as_deref() == Some(id) {
            return Ok(message);
        }
        if message.payload_type != ProtoHeartbeatEvent::PAYLOAD_TYPE {
            let _ = events.send(Event::Message(message));
        }
    }
}

fn outcome(message: ProtoMessage) -> Result<ProtoMessage, ConnectorError> {
    if message.payload_type == ProtoOaErrorRes::PAYLOAD_TYPE {
        Err(ProtoOaErrorRes::from_message(&message)
            .map_or_else(|error| error, |response| ServerError::from(response).into()))
    } else if message.payload_type == ProtoErrorRes::PAYLOAD_TYPE {
        Err(ProtoErrorRes::from_message(&message)
            .map_or_else(|error| error, |response| ServerError::from(response).into()))
    } else {
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ConnectionOptions, Event, ReplayKey, ReplayStage, outcome};
    use crate::common::config::ConfigurationOpenApi;
    use crate::common::errors::{ConnectorError, ErrorCode, TransportError};
    use crate::open_api::messages::{
        ProtoErrorRes, ProtoMessage, ProtoOaErrorRes, ProtoOaSpotEvent, ProtoOaVersionRes,
    };
    use crate::open_api::payload::Payload;

    #[test]
    fn options_take_timeout_and_reconnect_delay_of_the_configuration() {
        let configuration = ConfigurationOpenApi::builder()
            .client_id("id")
            .client_secret("secret")
            .access_token("token")
            .timeout(Duration::from_secs(3))
            .reconnect_delay(Duration::from_secs(2))
            .build()
            .expect("configuration");
        let options = ConnectionOptions::from(&configuration);
        assert_eq!(
            options,
            ConnectionOptions::new()
                .timeout(Duration::from_secs(3))
                .reconnect_delay(Duration::from_secs(2))
        );
    }

    #[test]
    fn options_raise_degenerate_values() {
        let options = ConnectionOptions::new()
            .timeout(Duration::ZERO)
            .heartbeat_interval(Duration::ZERO)
            .command_capacity(0)
            .event_capacity(0)
            .reconnect_delay(Duration::ZERO)
            .reconnect_attempts(Some(4));
        assert_eq!(options.timeout, Duration::from_millis(1));
        assert_eq!(options.heartbeat_interval, Duration::from_millis(1));
        assert_eq!(options.command_capacity, 1);
        assert_eq!(options.event_capacity, 1);
        assert_eq!(options.reconnect_delay, Duration::from_millis(1));
        assert_eq!(options.reconnect_attempts, Some(4));
        assert_eq!(ConnectionOptions::new().reconnect_attempts, None);
    }

    #[test]
    fn error_responses_become_server_errors() {
        let open_api = ProtoOaErrorRes {
            error_code: "CH_CLIENT_AUTH_FAILURE".to_owned(),
            ..ProtoOaErrorRes::default()
        };
        assert!(matches!(
            outcome(open_api.to_message(None)),
            Err(ConnectorError::Server(error)) if error.code == ErrorCode::ChClientAuthFailure
        ));
        let common = ProtoErrorRes {
            error_code: "UNSUPPORTED_MESSAGE".to_owned(),
            ..ProtoErrorRes::default()
        };
        assert!(matches!(
            outcome(common.to_message(None)),
            Err(ConnectorError::Server(error)) if error.code == ErrorCode::UnsupportedMessage
        ));
        for payload_type in [50, 2142] {
            let malformed = ProtoMessage {
                payload_type,
                payload: Some(vec![0xff]),
                client_msg_id: None,
            };
            assert!(matches!(
                outcome(malformed),
                Err(ConnectorError::Transport(TransportError::Decode(_)))
            ));
        }
        let fine = ProtoOaVersionRes::default().to_message(None);
        assert_eq!(outcome(fine.clone()), Ok(fine));
    }

    #[test]
    fn event_decodes_only_the_message_it_carries() {
        let spot = ProtoOaSpotEvent {
            ctid_trader_account_id: 1,
            symbol_id: 2,
            ..ProtoOaSpotEvent::default()
        };
        let event = Event::Message(spot.to_message(None));
        assert_eq!(event.decode::<ProtoOaSpotEvent>(), Some(spot));
        assert_eq!(event.decode::<ProtoOaVersionRes>(), None);
        let lost = Event::Disconnected(TransportError::Closed);
        assert_eq!(lost.decode::<ProtoOaSpotEvent>(), None);
        let again = Event::Reconnected { attempt: 1 };
        assert_eq!(again.decode::<ProtoOaSpotEvent>(), None);
    }

    #[test]
    fn replay_keys_order_by_stage_then_name() {
        let mut keys = [
            ReplayKey::new(ReplayStage::Subscription, "a"),
            ReplayKey::new(ReplayStage::Account, "2"),
            ReplayKey::new(ReplayStage::Application, "z"),
            ReplayKey::new(ReplayStage::Account, "1"),
        ];
        keys.sort();
        let order: Vec<(ReplayStage, &str)> =
            keys.iter().map(|key| (key.stage(), key.id())).collect();
        assert_eq!(
            order,
            vec![
                (ReplayStage::Application, "z"),
                (ReplayStage::Account, "1"),
                (ReplayStage::Account, "2"),
                (ReplayStage::Subscription, "a"),
            ]
        );
    }
}
