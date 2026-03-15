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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::common::config::{ConfigurationOpenApi, DEFAULT_TIMEOUT};
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
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
        }
    }
}

impl From<&ConfigurationOpenApi> for ConnectionOptions {
    fn from(configuration: &ConfigurationOpenApi) -> Self {
        Self::default().timeout(configuration.timeout())
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
}

/// Something that happened on the connection without a request asking for it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// The server sent a message that answers no pending request.
    Message(ProtoMessage),
    /// The connection was lost for the given reason.
    Disconnected(TransportError),
}

impl Event {
    /// Returns the pushed message when the event carries one of type `M`.
    #[must_use]
    pub fn decode<M: Payload>(&self) -> Option<M> {
        match self {
            Self::Message(message) => M::from_message(message).ok(),
            Self::Disconnected(_) => None,
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
}

#[derive(Debug, Default)]
struct Counters {
    requests_sent: AtomicU64,
    heartbeats_sent: AtomicU64,
    heartbeats_received: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

type Reply = oneshot::Sender<Result<ProtoMessage, ConnectorError>>;

#[derive(Debug)]
struct Outbound {
    payload_type: u32,
    payload: Vec<u8>,
    reply: Reply,
}

#[derive(Debug)]
enum Command {
    Request(Outbound),
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
            framed: Framed::new(stream),
            heartbeat_at: Instant::now() + options.heartbeat_interval,
            options,
            commands: receiver,
            events: events.clone(),
            counters: Arc::clone(&counters),
            queue: VecDeque::new(),
            pending: HashMap::new(),
            deadlines: VecDeque::new(),
            next_id: 0,
        };
        let task = tokio::spawn(actor.run());
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

struct Actor<S> {
    framed: Framed<S>,
    options: ConnectionOptions,
    commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<Event>,
    counters: Arc<Counters>,
    queue: VecDeque<Outbound>,
    pending: HashMap<String, Reply>,
    deadlines: VecDeque<(Instant, String)>,
    next_id: u64,
    heartbeat_at: Instant,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Actor<S> {
    async fn run(mut self) {
        match self.serve().await {
            Exit::Shutdown => self.fail_all(&ConnectorError::Closed),
            Exit::Lost(reason) => {
                log_debug!(%reason, "connection lost");
                self.fail_all(&ConnectorError::Transport(reason.clone()));
                let _ = self.events.send(Event::Disconnected(reason));
            }
        }
        self.framed.close().await;
    }

    async fn serve(&mut self) -> Exit {
        loop {
            if let Err(reason) = self.flush().await {
                return Exit::Lost(reason);
            }
            let wake = self.next_wake();
            tokio::select! {
                incoming = self.framed.receive() => match incoming {
                    Ok(message) => self.dispatch(message),
                    Err(reason) => return Exit::Lost(reason),
                },
                command = self.commands.recv() => match command {
                    Some(Command::Request(outbound)) => self.queue.push_back(outbound),
                    Some(Command::Shutdown) | None => return Exit::Shutdown,
                },
                () = sleep_until(wake) => {
                    if let Err(reason) = self.tick().await {
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

    async fn flush(&mut self) -> Result<(), TransportError> {
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
            match self.framed.send(&message).await {
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

    async fn tick(&mut self) -> Result<(), TransportError> {
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
            self.framed
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

    fn fail_all(&mut self, error: &ConnectorError) {
        self.deadlines.clear();
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(error.clone()));
        }
        for outbound in self.queue.drain(..) {
            let _ = outbound.reply.send(Err(error.clone()));
        }
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            if let Command::Request(outbound) = command {
                let _ = outbound.reply.send(Err(error.clone()));
            }
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

    use super::{ConnectionOptions, Event, outcome};
    use crate::common::config::ConfigurationOpenApi;
    use crate::common::errors::{ConnectorError, ErrorCode, TransportError};
    use crate::open_api::messages::{
        ProtoErrorRes, ProtoMessage, ProtoOaErrorRes, ProtoOaSpotEvent, ProtoOaVersionRes,
    };
    use crate::open_api::payload::Payload;

    #[test]
    fn options_take_the_timeout_of_the_configuration() {
        let configuration = ConfigurationOpenApi::builder()
            .client_id("id")
            .client_secret("secret")
            .access_token("token")
            .timeout(Duration::from_secs(3))
            .build()
            .expect("configuration");
        let options = ConnectionOptions::from(&configuration);
        assert_eq!(
            options,
            ConnectionOptions::new().timeout(Duration::from_secs(3))
        );
    }

    #[test]
    fn options_raise_degenerate_values() {
        let options = ConnectionOptions::new()
            .timeout(Duration::ZERO)
            .heartbeat_interval(Duration::ZERO)
            .command_capacity(0)
            .event_capacity(0);
        assert_eq!(options.timeout, Duration::from_millis(1));
        assert_eq!(options.heartbeat_interval, Duration::from_millis(1));
        assert_eq!(options.command_capacity, 1);
        assert_eq!(options.event_capacity, 1);
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
    }
}
