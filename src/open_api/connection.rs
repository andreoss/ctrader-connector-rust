//! Connection task and the handle callers talk to it through.
//!
//! One task owns the socket. Callers hold a cloneable [`Connection`]; a request travels over a
//! bounded command channel, is framed with a fresh `clientMsgId` and resolves when the response
//! carrying the same id arrives. Messages the server pushes on its own are fanned out as
//! [`Event`]s. The task sends a heartbeat whenever nothing was written for the heartbeat
//! interval, which keeps the server from closing an idle connection.
//!
//! Deadlines: nothing the task waits for on the socket is open-ended. A write that does not
//! complete within the time allowed for a response fails with
//! [`TransportError::WriteTimeout`], and a server that sends nothing at all, not even a
//! heartbeat, for the period set with [`ConnectionOptions::inbound_timeout`] fails the
//! connection with [`TransportError::Idle`]. Both count as a loss of the connection.
//! Every duration an option carries is added to the clock without overflow: a wait that would
//! end more than thirty years ahead ends thirty years ahead, so [`Duration::MAX`] stands for
//! never and turns the wait it is given to off.
//!
//! Backpressure: the command channel is bounded, so callers wait while the task is busy, and the
//! event stream is a bounded broadcast, so a subscriber that falls behind skips the oldest
//! events instead of stalling the task.
//!
//! Limits: the server accepts a number of requests per second on one connection, fewer for
//! historical data. The task holds requests back so that neither limit is exceeded, and when
//! the server still answers that a limit was exceeded it pauses and sends the request again, up
//! to the configured number of retries.
//!
//! Reconnection: when the socket is lost the task fails the requests in flight, reports
//! [`Event::Disconnected`] and connects again, waiting twice as long before every further
//! attempt. Once connected it sends the registered requests again in the order of their
//! [`ReplayStage`] (application, accounts, subscriptions) and then reports
//! [`Event::Reconnected`]. The registered requests are read one at a time, each just before it
//! is sent, so a registration removed meanwhile is not replayed and one added meanwhile is.
//! Requests made while the connection is down fail at once with the reason of the loss.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::common::config::{
    ConfigurationOpenApi, DEFAULT_BACKOFF, DEFAULT_RECONNECT_DELAY, DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
};
use crate::common::errors::{ConnectorError, ErrorCode, ServerError, TransportError};
use crate::open_api::messages::{
    ProtoErrorRes, ProtoHeartbeatEvent, ProtoMessage, ProtoOaErrorRes,
};
use crate::open_api::payload::{Payload, Request, is_historical};
use crate::open_api::transport::{Connect, Framed};

/// Pause after the last write before a heartbeat is sent.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Number of requests that may wait in the command channel.
pub const DEFAULT_COMMAND_CAPACITY: usize = 64;

/// Number of events kept for a subscriber that falls behind.
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

/// Requests the server accepts per second on one connection, historical data excluded.
pub const REQUEST_LIMIT: u32 = 50;

/// Requests for historical data the server accepts per second on one connection.
pub const HISTORICAL_LIMIT: u32 = 5;

/// Longest pause after the server answered that a limit was exceeded.
pub const BACKOFF_LIMIT: Duration = Duration::from_secs(30);

const LIMIT_WINDOW: Duration = Duration::from_millis(1100);

/// Longest pause between two reconnection attempts.
pub const RECONNECT_DELAY_LIMIT: Duration = Duration::from_secs(60);

const SHORTEST_INTERVAL: Duration = Duration::from_millis(1);

/// Pause between two heartbeats the server sends on a connection that is otherwise quiet.
pub const SERVER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Silence of the server after which the connection counts as lost unless another period is
/// set: two and a half times [`SERVER_HEARTBEAT_INTERVAL`], so one heartbeat of the server may
/// go missing, and the next one be late by half an interval, before a connection is given up.
pub const DEFAULT_INBOUND_TIMEOUT: Duration = Duration::from_secs(75);

const LONGEST_WAIT: Duration = Duration::from_secs(30 * 365 * 86_400);

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
    request_limit: u32,
    historical_limit: u32,
    retries: u32,
    backoff: Duration,
    inbound_timeout: Option<Duration>,
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
            request_limit: REQUEST_LIMIT,
            historical_limit: HISTORICAL_LIMIT,
            retries: DEFAULT_RETRIES,
            backoff: DEFAULT_BACKOFF,
            inbound_timeout: None,
        }
    }
}

impl From<&ConfigurationOpenApi> for ConnectionOptions {
    fn from(configuration: &ConfigurationOpenApi) -> Self {
        Self::default()
            .timeout(configuration.timeout())
            .reconnect_delay(configuration.reconnect_delay())
            .retries(configuration.retries())
            .backoff(configuration.backoff())
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

    /// Sets the number of requests sent per second, historical data excluded; at least one.
    pub fn request_limit(mut self, limit: u32) -> Self {
        self.request_limit = limit.max(1);
        self
    }

    /// Sets the number of requests for historical data sent per second; at least one.
    pub fn historical_limit(mut self, limit: u32) -> Self {
        self.historical_limit = limit.max(1);
        self
    }

    /// Sets how often a request is sent again after the server answered that a limit was
    /// exceeded; zero hands the error to the caller at once.
    pub const fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Sets the pause after the server answered that a limit was exceeded; it doubles with
    /// every retry of the same request up to [`BACKOFF_LIMIT`].
    ///
    /// A duration below one millisecond is raised to one millisecond.
    pub fn backoff(mut self, backoff: Duration) -> Self {
        self.backoff = backoff.max(SHORTEST_INTERVAL);
        self
    }

    /// Sets how long the server may stay silent before the connection counts as lost.
    ///
    /// Every frame the server sends, heartbeats included, starts the period anew. When it runs
    /// out the task reports [`Event::Disconnected`] with [`TransportError::Idle`] and connects
    /// again. Left unset, the period is [`DEFAULT_INBOUND_TIMEOUT`]. It follows from the pace of
    /// the server alone, which sends a heartbeat of its own every
    /// [`SERVER_HEARTBEAT_INTERVAL`] on a connection that is otherwise quiet, and from nothing
    /// the client is set to do: neither the heartbeat interval nor the time allowed for a
    /// response changes it. A period shorter than the pace of the server gives up connections
    /// that are in order.
    ///
    /// A duration below one millisecond is raised to one millisecond. [`Duration::MAX`] turns
    /// the watch off: the server may then stay silent for as long as it likes.
    pub fn inbound_timeout(mut self, period: Duration) -> Self {
        self.inbound_timeout = Some(period.max(SHORTEST_INTERVAL));
        self
    }

    fn silence(&self) -> Duration {
        self.inbound_timeout.unwrap_or(DEFAULT_INBOUND_TIMEOUT)
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
    /// Requests sent again after the server answered that a limit was exceeded.
    pub throttle_retries: u64,
}

#[derive(Debug, Default)]
struct Counters {
    requests_sent: AtomicU64,
    heartbeats_sent: AtomicU64,
    heartbeats_received: AtomicU64,
    reconnects: AtomicU64,
    throttle_retries: AtomicU64,
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

#[derive(Clone, PartialEq, Eq)]
struct Stored {
    payload_type: u32,
    payload: Vec<u8>,
}

type Replays = BTreeMap<ReplayKey, Stored>;

type Reply = oneshot::Sender<Result<ProtoMessage, ConnectorError>>;

struct Outbound {
    payload_type: u32,
    payload: Vec<u8>,
    historical: bool,
    attempts: u32,
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
            heartbeat_at: after(Instant::now(), options.heartbeat_interval),
            silent_at: after(Instant::now(), options.silence()),
            options,
            commands: receiver,
            events: events.clone(),
            counters: Arc::clone(&counters),
            queue: VecDeque::new(),
            pending: HashMap::new(),
            deadlines: VecDeque::new(),
            replays: Replays::new(),
            throttles: [Throttle::default(), Throttle::default()],
            paused_until: None,
            send_at: None,
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
            .send(R::PAYLOAD_TYPE, request.encode_to_vec(), R::HISTORICAL)
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
        self.send(payload_type, payload, is_historical(payload_type))
            .await
    }

    async fn send(
        &self,
        payload_type: u32,
        payload: Vec<u8>,
        historical: bool,
    ) -> Result<ProtoMessage, ConnectorError> {
        let (reply, response) = oneshot::channel();
        let outbound = Outbound {
            payload_type,
            payload,
            reply,
            historical,
            attempts: 0,
        };
        self.commands
            .send(Command::Request(outbound))
            .await
            .map_err(|_| ConnectorError::Closed)?;
        response.await.map_err(|_| ConnectorError::Closed)?
    }

    /// Registers a request to be sent again, under `key`, after every reconnection.
    ///
    /// The request is not sent now. Requests are replayed in the order of their keys. A request
    /// registered or replaced while the connection is being restored is replayed before
    /// [`Event::Reconnected`] is reported.
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
    /// While the connection is being restored the registered requests are looked up one at a
    /// time, each just before it is sent, so a request removed before its turn is not replayed.
    /// A request that was replayed already stays in force on the server: the task knows no
    /// request that would withdraw it, which is left to the caller once
    /// [`Event::Reconnected`] is reported.
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

    /// Waits until the connection task has ended, after which every request fails with
    /// [`ConnectorError::Closed`].
    pub async fn closed(&self) {
        self.commands.closed().await;
    }

    /// Returns the counters of the connection.
    #[must_use]
    pub fn stats(&self) -> Stats {
        Stats {
            requests_sent: self.counters.requests_sent.load(Ordering::Relaxed),
            heartbeats_sent: self.counters.heartbeats_sent.load(Ordering::Relaxed),
            heartbeats_received: self.counters.heartbeats_received.load(Ordering::Relaxed),
            reconnects: self.counters.reconnects.load(Ordering::Relaxed),
            throttle_retries: self.counters.throttle_retries.load(Ordering::Relaxed),
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
    pending: HashMap<String, Outbound>,
    deadlines: VecDeque<(Instant, String)>,
    replays: Replays,
    throttles: [Throttle; 2],
    paused_until: Option<Instant>,
    send_at: Option<Instant>,
    next_id: u64,
    heartbeat_at: Instant,
    silent_at: Instant,
}

impl<C: Connect> Actor<C> {
    async fn run(mut self, mut framed: Framed<C::Stream>) {
        let error = loop {
            match self.serve(&mut framed).await {
                Exit::Shutdown => break ConnectorError::Closed,
                Exit::Lost(reason) => {
                    log_debug!(%reason, "connection lost");
                    close(&mut framed, self.options.timeout).await;
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
        close(&mut framed, self.options.timeout).await;
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
            match self.restore(&last).await? {
                Ok((framed, replays_sent)) => {
                    log_debug!(attempt, "reconnected");
                    bump(&self.counters.reconnects);
                    let seeded = replays_sent
                        .min(usize::try_from(self.options.request_limit).unwrap_or(usize::MAX));
                    if let Some(throttle) = self.throttles.first_mut() {
                        throttle
                            .sent
                            .extend(std::iter::repeat_n(Instant::now(), seeded));
                    }
                    self.heartbeat_at = after(Instant::now(), self.options.heartbeat_interval);
                    self.silent_at = after(Instant::now(), self.options.silence());
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

    async fn restore(
        &mut self,
        reason: &TransportError,
    ) -> Result<Result<(Framed<C::Stream>, usize), TransportError>, ConnectorError> {
        let limit = self.options.timeout;
        let spacing = LIMIT_WINDOW / self.options.request_limit;
        let connect = self.connector.connect(&self.host, self.port);
        let mut framed = match hold(&mut self.commands, &mut self.replays, reason, connect).await? {
            Ok(stream) => Framed::new(stream),
            Err(reason) => return Ok(Err(reason)),
        };
        let mut sent = Replays::new();
        loop {
            let next = self
                .replays
                .iter()
                .find(|(key, stored)| sent.get(*key) != Some(*stored))
                .map(|(key, stored)| (key.clone(), stored.clone()));
            let Some((key, stored)) = next else {
                return Ok(Ok((framed, sent.len())));
            };
            sent.insert(key.clone(), stored.clone());
            let id = format!("replay-{}", sent.len());
            let step = Box::pin(replay(&mut framed, id, key, stored, limit, &self.events));
            if let Err(reason) = hold(&mut self.commands, &mut self.replays, reason, step).await? {
                return Ok(Err(reason));
            }
            hold(
                &mut self.commands,
                &mut self.replays,
                reason,
                sleep(spacing),
            )
            .await?;
        }
    }

    async fn serve(&mut self, framed: &mut Framed<C::Stream>) -> Exit {
        loop {
            if let Err(reason) = self.flush(framed).await {
                return Exit::Lost(reason);
            }
            let wake = self.next_wake();
            let capacity = self.options.command_capacity;
            tokio::select! {
                incoming = framed.receive() => match incoming {
                    Ok(message) => self.heard(message),
                    Err(reason) => return Exit::Lost(reason),
                },
                command = self.commands.recv(), if self.queue.len() < capacity => match command {
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

    fn heard(&mut self, message: ProtoMessage) {
        self.silent_at = after(Instant::now(), self.options.silence());
        self.dispatch(message);
    }

    fn next_wake(&self) -> Instant {
        let mut wake = self.heartbeat_at.min(self.silent_at);
        if let Some((deadline, _)) = self.deadlines.front() {
            wake = wake.min(*deadline);
        }
        if let Some(send_at) = self.send_at {
            wake = wake.min(send_at);
        }
        wake
    }

    fn next_ready(&mut self, now: Instant) -> Option<usize> {
        self.send_at = None;
        if let Some(until) = self.paused_until {
            if until > now {
                if !self.queue.is_empty() {
                    self.send_at = Some(until);
                }
                return None;
            }
            self.paused_until = None;
        }
        let limits = [self.options.request_limit, self.options.historical_limit];
        let mut ready = [None, None];
        for ((throttle, limit), ready) in self.throttles.iter_mut().zip(limits).zip(&mut ready) {
            *ready = throttle.ready_at(now, limit);
        }
        for (index, outbound) in self.queue.iter().enumerate() {
            match ready
                .get(usize::from(outbound.historical))
                .copied()
                .flatten()
            {
                None => return Some(index),
                Some(at) => self.send_at = Some(self.send_at.map_or(at, |known| known.min(at))),
            }
        }
        None
    }

    async fn flush(&mut self, framed: &mut Framed<C::Stream>) -> Result<(), TransportError> {
        loop {
            let now = Instant::now();
            let Some(outbound) = self
                .next_ready(now)
                .and_then(|index| self.queue.remove(index))
            else {
                return Ok(());
            };
            if outbound.reply.is_closed() {
                continue;
            }
            self.next_id += 1;
            let id = self.next_id.to_string();
            let message = ProtoMessage {
                payload_type: outbound.payload_type,
                payload: Some(outbound.payload.clone()),
                client_msg_id: Some(id.clone()),
            };
            match write(framed, &message, self.options.timeout).await {
                Ok(()) => {
                    bump(&self.counters.requests_sent);
                    if let Some(throttle) = self.throttles.get_mut(usize::from(outbound.historical))
                    {
                        throttle.sent.push_back(now);
                    }
                    self.heartbeat_at = after(now, self.options.heartbeat_interval);
                    self.deadlines
                        .push_back((after(now, self.options.timeout), id.clone()));
                    self.pending.insert(id, outbound);
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
    }

    async fn tick(&mut self, framed: &mut Framed<C::Stream>) -> Result<(), TransportError> {
        let now = Instant::now();
        while let Some((deadline, id)) = self.deadlines.pop_front() {
            if deadline > now {
                self.deadlines.push_front((deadline, id));
                break;
            }
            if let Some(outbound) = self.pending.remove(&id) {
                let _ = outbound
                    .reply
                    .send(Err(ConnectorError::Timeout(self.options.timeout)));
            }
        }
        if now >= self.silent_at {
            let silence = self.options.silence();
            let late = timeout(Duration::ZERO, framed.receive())
                .await
                .unwrap_or(Err(TransportError::Idle(silence)));
            self.heard(late?);
        }
        if now >= self.heartbeat_at {
            let beat = ProtoHeartbeatEvent::default().to_message(None);
            write(framed, &beat, self.options.timeout).await?;
            bump(&self.counters.heartbeats_sent);
            self.heartbeat_at = after(Instant::now(), self.options.heartbeat_interval);
        }
        Ok(())
    }

    fn dispatch(&mut self, message: ProtoMessage) {
        if message.payload_type == ProtoHeartbeatEvent::PAYLOAD_TYPE {
            bump(&self.counters.heartbeats_received);
            return;
        }
        let waiting = message
            .client_msg_id
            .as_ref()
            .and_then(|id| self.pending.remove(id));
        let Some(mut outbound) = waiting else {
            let _ = self.events.send(Event::Message(message));
            return;
        };
        let result = outcome(message);
        if over_limit(&result) && outbound.attempts < self.options.retries {
            let pause = self
                .options
                .backoff
                .saturating_mul(1 << outbound.attempts.min(16))
                .min(BACKOFF_LIMIT.max(self.options.backoff));
            let until = after(Instant::now(), pause);
            log_debug!(
                payload_type = outbound.payload_type,
                ?pause,
                "limit exceeded"
            );
            self.paused_until = Some(self.paused_until.map_or(until, |known| known.max(until)));
            outbound.attempts += 1;
            bump(&self.counters.throttle_retries);
            self.queue.push_front(outbound);
        } else {
            let _ = outbound.reply.send(result);
        }
    }

    fn fail_in_flight(&mut self, error: &ConnectorError) {
        self.deadlines.clear();
        for (_, outbound) in self.pending.drain() {
            let _ = outbound.reply.send(Err(error.clone()));
        }
        for outbound in self.queue.drain(..) {
            let _ = outbound.reply.send(Err(error.clone()));
        }
        self.paused_until = None;
        self.send_at = None;
        for throttle in &mut self.throttles {
            throttle.sent.clear();
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

#[derive(Default)]
struct Throttle {
    sent: VecDeque<Instant>,
}

impl Throttle {
    fn ready_at(&mut self, now: Instant, limit: u32) -> Option<Instant> {
        while self
            .sent
            .front()
            .is_some_and(|sent| now.duration_since(*sent) >= LIMIT_WINDOW)
        {
            self.sent.pop_front();
        }
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let excess = (self.sent.len() + 1).checked_sub(limit)?;
        let blocking = excess.checked_sub(1)?;
        self.sent
            .get(blocking)
            .map(|sent| after(*sent, LIMIT_WINDOW))
    }
}

fn after(start: Instant, period: Duration) -> Instant {
    start.checked_add(period.min(LONGEST_WAIT)).unwrap_or(start)
}

fn over_limit(result: &Result<ProtoMessage, ConnectorError>) -> bool {
    matches!(
        result,
        Err(ConnectorError::Server(error)) if matches!(
            error.code,
            ErrorCode::RequestFrequencyExceeded | ErrorCode::BlockedPayloadType
        )
    )
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

async fn write<S: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut Framed<S>,
    message: &ProtoMessage,
    limit: Duration,
) -> Result<(), TransportError> {
    timeout(limit, framed.send(message))
        .await
        .unwrap_or(Err(TransportError::WriteTimeout(limit)))
}

async fn close<S: AsyncRead + AsyncWrite + Unpin>(framed: &mut Framed<S>, limit: Duration) {
    let _ = timeout(limit, framed.close()).await;
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

async fn replay<S: AsyncRead + AsyncWrite + Unpin>(
    framed: &mut Framed<S>,
    id: String,
    key: ReplayKey,
    stored: Stored,
    limit: Duration,
    events: &broadcast::Sender<Event>,
) -> Result<(), TransportError> {
    let message = ProtoMessage {
        payload_type: stored.payload_type,
        payload: Some(stored.payload),
        client_msg_id: Some(id.clone()),
    };
    write(framed, &message, limit).await?;
    let failure = match timeout(limit, answer_to(framed, &id, events)).await {
        Ok(answer) => outcome(answer?).err(),
        Err(_) => Some(ConnectorError::Timeout(limit)),
    };
    if let Some(error) = failure {
        log_debug!(%error, "replayed request failed");
        let _ = events.send(Event::ReplayFailed { key, error });
    }
    Ok(())
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

    use super::{
        ConnectionOptions, DEFAULT_INBOUND_TIMEOUT, Event, LIMIT_WINDOW, LONGEST_WAIT, ReplayKey,
        ReplayStage, SERVER_HEARTBEAT_INTERVAL, Throttle, after, outcome, over_limit,
    };
    use crate::common::config::ConfigurationOpenApi;
    use crate::common::errors::{ConnectorError, ErrorCode, TransportError};
    use crate::open_api::messages::{
        ProtoErrorRes, ProtoMessage, ProtoOaErrorRes, ProtoOaSpotEvent, ProtoOaVersionRes,
    };
    use crate::open_api::payload::Payload;
    use tokio::time::Instant;

    #[test]
    fn options_take_timing_and_retries_of_the_configuration() {
        let configuration = ConfigurationOpenApi::builder()
            .client_id("id")
            .client_secret("secret")
            .access_token("token")
            .timeout(Duration::from_secs(3))
            .reconnect_delay(Duration::from_secs(2))
            .retries(7)
            .backoff(Duration::from_millis(250))
            .build()
            .expect("configuration");
        let options = ConnectionOptions::from(&configuration);
        assert_eq!(
            options,
            ConnectionOptions::new()
                .timeout(Duration::from_secs(3))
                .reconnect_delay(Duration::from_secs(2))
                .retries(7)
                .backoff(Duration::from_millis(250))
        );
    }

    #[test]
    fn silence_allowed_follows_the_pace_of_the_server_unless_it_is_set() {
        assert_eq!(DEFAULT_INBOUND_TIMEOUT, Duration::from_secs(75));
        assert!(DEFAULT_INBOUND_TIMEOUT >= SERVER_HEARTBEAT_INTERVAL * 2);
        assert_eq!(ConnectionOptions::new().silence(), DEFAULT_INBOUND_TIMEOUT);
        let quick = ConnectionOptions::new()
            .heartbeat_interval(Duration::from_secs(2))
            .timeout(Duration::from_secs(1));
        assert_eq!(quick.silence(), DEFAULT_INBOUND_TIMEOUT);
        assert_eq!(
            quick.clone().inbound_timeout(Duration::ZERO).silence(),
            Duration::from_millis(1)
        );
        assert_eq!(
            quick.inbound_timeout(Duration::from_secs(90)).silence(),
            Duration::from_secs(90)
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
            .reconnect_attempts(Some(4))
            .request_limit(0)
            .historical_limit(0)
            .retries(0)
            .backoff(Duration::ZERO);
        assert_eq!(options.timeout, Duration::from_millis(1));
        assert_eq!(options.heartbeat_interval, Duration::from_millis(1));
        assert_eq!(options.command_capacity, 1);
        assert_eq!(options.event_capacity, 1);
        assert_eq!(options.reconnect_delay, Duration::from_millis(1));
        assert_eq!(options.reconnect_attempts, Some(4));
        assert_eq!(options.request_limit, 1);
        assert_eq!(options.historical_limit, 1);
        assert_eq!(options.retries, 0);
        assert_eq!(options.backoff, Duration::from_millis(1));
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

    #[test]
    fn throttle_names_the_moment_the_next_request_may_go() {
        let start = Instant::now();
        let mut throttle = Throttle::default();
        assert_eq!(throttle.ready_at(start, 2), None);
        throttle.sent.push_back(start);
        assert_eq!(throttle.ready_at(start, 2), None);
        throttle.sent.push_back(start + Duration::from_millis(10));
        assert_eq!(throttle.ready_at(start, 2), Some(start + LIMIT_WINDOW));
        throttle.sent.push_back(start + Duration::from_millis(20));
        assert_eq!(
            throttle.ready_at(start, 2),
            Some(start + Duration::from_millis(10) + LIMIT_WINDOW)
        );
        let later = start + LIMIT_WINDOW + Duration::from_millis(10);
        assert_eq!(throttle.ready_at(later, 2), None);
        assert_eq!(throttle.sent.len(), 1);
    }

    #[test]
    fn waits_too_long_for_the_clock_end_at_the_longest_wait() {
        let start = Instant::now();
        let brief = Duration::from_secs(90);
        assert_eq!(after(start, brief), start + brief);
        assert_eq!(after(start, LONGEST_WAIT), start + LONGEST_WAIT);
        assert_eq!(after(start, Duration::MAX), start + LONGEST_WAIT);
        let far = after(start, Duration::MAX);
        assert_eq!(after(far, Duration::MAX), far + LONGEST_WAIT);
        let options = ConnectionOptions::new()
            .timeout(Duration::MAX)
            .heartbeat_interval(Duration::MAX);
        assert_eq!(options.silence(), DEFAULT_INBOUND_TIMEOUT);
        assert_eq!(
            options.inbound_timeout(Duration::MAX).silence(),
            Duration::MAX
        );
        let mut throttle = Throttle::default();
        throttle.sent.push_back(far);
        assert_eq!(throttle.ready_at(far, 1), Some(far + LIMIT_WINDOW));
    }

    #[test]
    fn only_limit_errors_are_retried() {
        let refusal = |code: &str| {
            outcome(
                ProtoOaErrorRes {
                    error_code: code.to_owned(),
                    ..ProtoOaErrorRes::default()
                }
                .to_message(None),
            )
        };
        assert!(over_limit(&refusal("REQUEST_FREQUENCY_EXCEEDED")));
        assert!(over_limit(&refusal("BLOCKED_PAYLOAD_TYPE")));
        assert!(!over_limit(&refusal("INVALID_REQUEST")));
        assert!(!over_limit(&Ok(
            ProtoOaVersionRes::default().to_message(None)
        )));
        assert!(!over_limit(&Err(ConnectorError::Closed)));
    }
}
