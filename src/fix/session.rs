//! Session of the FIX 4.4 API: one task that owns the socket of a logged-on session.
//!
//! The task logs on, numbers the messages it sends, checks the numbers of the messages it
//! receives, keeps the session alive with heartbeats and test requests, and hands every
//! message received to the readers of its events. Commands reach it over a channel, so the
//! socket has a single owner and a single writer.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};

use crate::common::config::ConfigurationFix;
use crate::common::errors::{ConnectorError, FixError, TransportError};
use crate::common::time::Timestamp;
use crate::common::transport::Connect;
use crate::fix::codec::{Frame, Header, decode, encode};
use crate::fix::messages::{Heartbeat, Logon, Logout, Message, TestRequest};

const COMMAND_CAPACITY: usize = 64;
const EVENT_CAPACITY: usize = 4096;
const READ_CHUNK: usize = 8 * 1024;
const SHORTEST_TICK: Duration = Duration::from_millis(20);
const LONGEST_TICK: Duration = Duration::from_secs(1);
const PROBE: &str = "alive";

/// Which of the two sessions of an account a connection serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionKind {
    /// Prices and symbols.
    Quote,
    /// Orders and positions.
    Trade,
}

impl SessionKind {
    /// Returns the session qualifier the wire carries (50 and 57).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quote => "QUOTE",
            Self::Trade => "TRADE",
        }
    }
}

/// A message received from the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    /// Sequence number of the message (34).
    pub seq: u64,
    /// Tells whether the server marked the message as a repetition (43).
    pub poss_dup: bool,
    /// The message.
    pub message: Message,
}

/// What happens on a session, in the order it happens.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// A message arrived.
    Message(Inbound),
    /// A message arrived that the library cannot read; it is skipped.
    Unparsed(FixError),
    /// The server ended the session with a logout.
    LoggedOut {
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// The connection was lost.
    Disconnected(TransportError),
}

pub(crate) enum Command {
    Send {
        message: Message,
        reply: oneshot::Sender<Result<u64, ConnectorError>>,
    },
    Logout {
        reply: oneshot::Sender<Result<(), ConnectorError>>,
    },
}

pub(crate) struct Started {
    pub(crate) commands: mpsc::Sender<Command>,
    pub(crate) events: broadcast::Sender<Event>,
    pub(crate) closed: watch::Receiver<bool>,
}

enum End {
    Shutdown,
    LoggedOut(Option<String>),
    Lost(TransportError),
}

struct Live<S> {
    stream: S,
    buffer: Vec<u8>,
    next_out: u64,
    next_in: u64,
    last_sent: Instant,
    last_received: Instant,
    probed: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Live<S> {
    fn new(stream: S) -> Self {
        let now = Instant::now();
        Self {
            stream,
            buffer: Vec::new(),
            next_out: 1,
            next_in: 1,
            last_sent: now,
            last_received: now,
            probed: false,
        }
    }

    async fn receive(&mut self) -> Result<Result<Frame, FixError>, TransportError> {
        loop {
            let before = self.buffer.len();
            match decode(&mut self.buffer) {
                Ok(Some(frame)) => return Ok(Ok(frame)),
                Ok(None) => {}
                Err(error) if self.buffer.len() < before => return Ok(Err(error)),
                Err(error) => return Err(TransportError::Decode(error.to_string())),
            }
            let mut chunk = [0_u8; READ_CHUNK];
            let read = self.stream.read(&mut chunk).await?;
            match chunk.get(..read) {
                Some(bytes) if read > 0 => self.buffer.extend_from_slice(bytes),
                _ => return Err(TransportError::Closed),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.stream.shutdown().await;
    }
}

struct Actor<C: Connect> {
    connector: C,
    configuration: ConfigurationFix,
    kind: SessionKind,
    commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<Event>,
    closed: watch::Sender<bool>,
}

impl<C: Connect> Actor<C> {
    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }

    async fn transmit(
        &self,
        live: &mut Live<C::Stream>,
        message: &Message,
    ) -> Result<u64, ConnectorError> {
        let seq = live.next_out;
        let qualifier = self.kind.as_str();
        let header = Header::new(
            "",
            self.configuration.sender_comp_id(),
            self.configuration.target_comp_id(),
            seq,
            Timestamp::now(),
        )
        .qualified(qualifier, qualifier);
        let bytes = encode(&message.to_frame(header))?;
        let limit = self.configuration.timeout();
        let written = timeout(limit, async {
            live.stream.write_all(&bytes).await?;
            live.stream.flush().await
        })
        .await;
        match written {
            Err(_) => Err(TransportError::WriteTimeout(limit).into()),
            Ok(Err(error)) => Err(TransportError::from(error).into()),
            Ok(Ok(())) => {
                live.next_out = seq.saturating_add(1);
                live.last_sent = Instant::now();
                Ok(seq)
            }
        }
    }

    async fn establish(&self) -> Result<Live<C::Stream>, ConnectorError> {
        let port = match self.kind {
            SessionKind::Quote => self.configuration.quote_port(),
            SessionKind::Trade => self.configuration.trade_port(),
        };
        let stream = self
            .connector
            .connect(self.configuration.host(), port)
            .await?;
        let mut live = Live::new(stream);
        let seconds = u32::try_from(self.configuration.heartbeat().as_secs()).unwrap_or(u32::MAX);
        let logon = Message::from(Logon {
            encrypt_method: 0,
            heart_bt_int: seconds,
            reset_seq_num: Some(true),
            username: Some(self.configuration.username().to_owned()),
            password: Some(self.configuration.password().clone()),
        });
        self.transmit(&mut live, &logon).await?;
        let limit = self.configuration.timeout();
        let frame = timeout(limit, live.receive())
            .await
            .map_err(|_| ConnectorError::Timeout(limit))???;
        match Message::from_frame(&frame)? {
            Message::Logon(_) => {
                live.next_in = frame.header.seq.saturating_add(1);
                live.last_received = Instant::now();
                Ok(live)
            }
            Message::Logout(logout) => {
                live.close().await;
                Err(FixError::LogonRefused { text: logout.text }.into())
            }
            other => Err(FixError::UnexpectedMessage(other.msg_type().to_owned()).into()),
        }
    }

    async fn leave(&self, live: &mut Live<C::Stream>) -> Result<(), ConnectorError> {
        self.transmit(live, &Logout { text: None }.into()).await?;
        let limit = self.configuration.timeout();
        let answered = timeout(limit, async {
            loop {
                match live.receive().await? {
                    Err(error) => self.emit(Event::Unparsed(error)),
                    Ok(frame) => match Message::from_frame(&frame) {
                        Ok(Message::Logout(_)) => return Ok::<(), TransportError>(()),
                        Ok(message) => self.emit(Event::Message(Inbound {
                            seq: frame.header.seq,
                            poss_dup: frame.header.poss_dup,
                            message,
                        })),
                        Err(error) => self.emit(Event::Unparsed(error)),
                    },
                }
            }
        })
        .await;
        live.close().await;
        match answered {
            Err(_) => Err(ConnectorError::Timeout(limit)),
            Ok(Err(error)) => Err(error.into()),
            Ok(Ok(())) => Ok(()),
        }
    }

    async fn accept(&self, live: &mut Live<C::Stream>, frame: Frame) -> Option<End> {
        live.last_received = Instant::now();
        live.probed = false;
        let seq = frame.header.seq;
        if seq < live.next_in && !frame.header.poss_dup {
            return Some(End::Lost(TransportError::Decode(format!(
                "sequence number {seq} is below the expected {}",
                live.next_in
            ))));
        }
        live.next_in = live.next_in.max(seq.saturating_add(1));
        let message = match Message::from_frame(&frame) {
            Ok(message) => message,
            Err(error) => {
                self.emit(Event::Unparsed(error));
                return None;
            }
        };
        let answer = match &message {
            Message::TestRequest(request) => Some(Message::from(Heartbeat {
                test_req_id: Some(request.test_req_id.clone()),
            })),
            Message::Logout(_) => Some(Logout { text: None }.into()),
            _ => None,
        };
        let logged_out = match &message {
            Message::Logout(logout) => Some(logout.text.clone()),
            _ => None,
        };
        self.emit(Event::Message(Inbound {
            seq,
            poss_dup: frame.header.poss_dup,
            message,
        }));
        if let Some(answer) = answer {
            match self.transmit(live, &answer).await {
                Err(ConnectorError::Transport(error)) if logged_out.is_none() => {
                    return Some(End::Lost(error));
                }
                _ => {}
            }
        }
        logged_out.map(End::LoggedOut)
    }

    async fn beat(&self, live: &mut Live<C::Stream>) -> Option<End> {
        let heartbeat = self.configuration.heartbeat();
        let now = Instant::now();
        let silent = now.saturating_duration_since(live.last_received);
        let limit = heartbeat.saturating_mul(5) / 2;
        if silent >= limit {
            return Some(End::Lost(TransportError::Idle(limit)));
        }
        let message = if silent >= heartbeat.saturating_mul(3) / 2 && !live.probed {
            live.probed = true;
            Message::from(TestRequest {
                test_req_id: PROBE.to_owned(),
            })
        } else if now.saturating_duration_since(live.last_sent) >= heartbeat {
            Heartbeat { test_req_id: None }.into()
        } else {
            return None;
        };
        match self.transmit(live, &message).await {
            Err(ConnectorError::Transport(error)) => Some(End::Lost(error)),
            _ => None,
        }
    }

    async fn command(&self, live: &mut Live<C::Stream>, command: Option<Command>) -> Option<End> {
        match command {
            None => {
                let _ = self.transmit(live, &Logout { text: None }.into()).await;
                live.close().await;
                Some(End::Shutdown)
            }
            Some(Command::Logout { reply }) => {
                let _ = reply.send(self.leave(live).await);
                Some(End::Shutdown)
            }
            Some(Command::Send { message, reply }) => {
                let result = self.transmit(live, &message).await;
                let lost = match &result {
                    Err(ConnectorError::Transport(error)) => Some(End::Lost(error.clone())),
                    _ => None,
                };
                let _ = reply.send(result);
                lost
            }
        }
    }

    async fn serve(&mut self, live: &mut Live<C::Stream>) -> End {
        let period = (self.configuration.heartbeat() / 4).clamp(SHORTEST_TICK, LONGEST_TICK);
        let mut ticker = interval(period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            let end = tokio::select! {
                command = self.commands.recv() => self.command(live, command).await,
                received = live.receive() => match received {
                    Err(error) => Some(End::Lost(error)),
                    Ok(Err(error)) => {
                        self.emit(Event::Unparsed(error));
                        None
                    }
                    Ok(Ok(frame)) => self.accept(live, frame).await,
                },
                _ = ticker.tick() => self.beat(live).await,
            };
            if let Some(end) = end {
                return end;
            }
        }
    }

    async fn run(mut self, mut live: Live<C::Stream>) {
        match self.serve(&mut live).await {
            End::Shutdown => {}
            End::LoggedOut(text) => {
                live.close().await;
                self.emit(Event::LoggedOut { text });
            }
            End::Lost(error) => {
                live.close().await;
                self.emit(Event::Disconnected(error));
            }
        }
        let _ = self.closed.send(true);
    }
}

/// Connects, logs on and starts the task of the session.
pub(crate) async fn start<C: Connect>(
    connector: C,
    configuration: ConfigurationFix,
    kind: SessionKind,
) -> Result<Started, ConnectorError> {
    let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
    let (events, _) = broadcast::channel(EVENT_CAPACITY);
    let (closed, closed_receiver) = watch::channel(false);
    let actor = Actor {
        connector,
        configuration,
        kind,
        commands: receiver,
        events: events.clone(),
        closed,
    };
    let live = actor.establish().await?;
    tokio::spawn(actor.run(live));
    Ok(Started {
        commands,
        events,
        closed: closed_receiver,
    })
}
