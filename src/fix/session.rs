//! Session of the FIX 4.4 API: one task that owns the socket of a logged-on session.
//!
//! The task logs on, numbers the messages it sends, checks the numbers of the messages it
//! receives, keeps the session alive with heartbeats and test requests, and hands every
//! message received to the readers of its events. Commands reach it over a channel, so the
//! socket has a single owner and a single writer.
//!
//! Sequence numbers: a message of the server that arrives ahead of its turn is dropped and
//! every message from the missing one on is asked for again with a resend request; the
//! session goes on once the server has repeated them or has skipped them with a sequence
//! reset. A logout or a resend request ahead of its turn is acted on at once all the same.
//! When the server asks
//! for messages again, the task answers with a sequence reset that skips them all: no
//! message, and above all no order, is ever sent twice. [`Event::Skipped`] names the range.
//!
//! Reconnection: when the connection is lost the task reports [`Event::Disconnected`], then
//! connects and logs on again, a handful of times at most, and reports
//! [`Event::Reconnected`]. Both sides count from one again after every logon, so no gap
//! outlives a connection. Nothing sent over the lost connection is sent again, and the
//! server has forgotten its subscriptions. When no attempt succeeds the task reports
//! [`Event::Ended`] and the session is over. A message handed to the task between the two
//! connections is refused with [`ConnectorError::Closed`] without being sent.
//!
//! Keep-alive: the task sleeps until the next moment something is due, so a heartbeat
//! leaves when the interval since the last message sent ends, not later. A wait the clock
//! cannot hold, such as an answer awaited for [`Duration::MAX`], is never due; the heartbeat
//! interval is bounded by the configuration, so the watch on the server always is.

use std::future::pending;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::common::config::ConfigurationFix;
use crate::common::errors::{ConnectorError, FixError, TransportError};
use crate::common::time::Timestamp;
use crate::common::transport::Connect;
use crate::fix::codec::{Frame, Header, Unreadable, encode, take};
use crate::fix::messages::{
    Heartbeat, Logon, Logout, Message, Reject, ResendRequest, SequenceReset, TestRequest,
};

const COMMAND_CAPACITY: usize = 64;
const EVENT_CAPACITY: usize = 4096;
const READ_CHUNK: usize = 8 * 1024;
const PROBE: &str = "alive";
const RECONNECT_ATTEMPTS: u32 = 5;
const EVERY_LATER_MESSAGE: u64 = 0;
const REQUIRED_TAG_MISSING: u32 = 1;
const INVALID_MSG_TYPE: u32 = 11;
const REJECTS: [&str; 2] = ["3", "j"];

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
    /// A message arrived that the library cannot read; it is skipped. A message whose bytes
    /// are intact and whose turn it is is counted and answered with a reject (3) that names
    /// the message, its type where it is known, the tag and the reason: 11 for a type this
    /// library does not know, 1 for a missing tag. No business message reject (j) is ever
    /// sent: the server refuses one with a reject and closes the connection. A reject or a
    /// business message reject that cannot be read is counted and never answered.
    /// A message whose bytes are damaged is not counted, so that the server is asked to send
    /// it again. While the session logs out an unreadable message is reported and not
    /// answered.
    Unparsed(FixError),
    /// The server ended the session with a logout.
    LoggedOut {
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// The connection was lost; the session tries to connect again.
    Disconnected(TransportError),
    /// An attempt to connect again failed.
    ReconnectFailed {
        /// Number of the attempt, from one.
        attempt: u32,
        /// What failed.
        error: ConnectorError,
    },
    /// The session is logged on again over a new connection. Both sides count from one
    /// again, and the server has forgotten the subscriptions of the lost connection.
    Reconnected {
        /// Number of the attempt that succeeded, from one.
        attempt: u32,
    },
    /// The session gave up connecting again and is over.
    Ended,
    /// Messages of the server are missing and were asked for again.
    Gap {
        /// Sequence number that was expected.
        expected: u64,
        /// Sequence number that arrived.
        received: u64,
    },
    /// The server asked for messages again and was told to skip them; none is sent twice.
    Skipped {
        /// First sequence number skipped.
        from: u64,
        /// Last sequence number skipped.
        to: u64,
    },
}

pub(crate) enum Command {
    Send {
        message: Message,
        reply: oneshot::Sender<Result<u64, ConnectorError>>,
    },
    Logout {
        reply: oneshot::Sender<Result<(), ConnectorError>>,
    },
    Reconnect {
        reply: oneshot::Sender<Result<(), ConnectorError>>,
    },
}

pub(crate) struct Started {
    pub(crate) commands: mpsc::Sender<Command>,
    pub(crate) events: broadcast::Sender<Event>,
    pub(crate) closed: watch::Receiver<bool>,
}

enum Order {
    InTurn,
    Repeated,
    Behind,
    Ahead,
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
    awaited: Option<(u64, Instant)>,
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
            awaited: None,
        }
    }

    fn advance(&mut self, next_in: u64) {
        self.next_in = next_in;
        if self.awaited.is_some_and(|(last, _)| last < next_in) {
            self.awaited = None;
        }
    }

    async fn receive(&mut self) -> Result<Result<Frame, Unreadable>, TransportError> {
        loop {
            match take(&mut self.buffer) {
                Ok(Some(taken)) => return Ok(taken),
                Ok(None) => {}
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

fn after(from: Instant, wait: Duration) -> Option<Instant> {
    from.checked_add(wait)
}

async fn reached(due: Option<Instant>) {
    match due {
        Some(due) => sleep_until(due).await,
        None => pending().await,
    }
}

fn rejection(seq: u64, msg_type: Option<&str>, error: &FixError) -> Option<Reject> {
    let (named, ref_tag_id, reason) = match error {
        FixError::MissingField { msg_type, tag } => (
            Some(msg_type.as_str()),
            Some(*tag),
            Some(REQUIRED_TAG_MISSING),
        ),
        FixError::InvalidField { msg_type, tag } => (Some(msg_type.as_str()), Some(*tag), None),
        FixError::UnsupportedMessage(_) => (None, None, Some(INVALID_MSG_TYPE)),
        _ => (None, None, None),
    };
    let ref_msg_type = msg_type.or(named);
    if ref_msg_type.is_some_and(|msg_type| REJECTS.contains(&msg_type)) {
        return None;
    }
    Some(Reject {
        ref_seq_num: seq,
        text: Some(error.to_string()),
        ref_tag_id,
        ref_msg_type: ref_msg_type.map(str::to_owned),
        reason,
    })
}

struct Actor<C: Connect> {
    connector: C,
    configuration: ConfigurationFix,
    kind: SessionKind,
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
        self.write(live, seq, false, message).await?;
        live.next_out = seq.saturating_add(1);
        Ok(seq)
    }

    async fn write(
        &self,
        live: &mut Live<C::Stream>,
        seq: u64,
        repeated: bool,
        message: &Message,
    ) -> Result<(), ConnectorError> {
        let qualifier = self.kind.as_str();
        let mut header = Header::new(
            "",
            self.configuration.sender_comp_id(),
            self.configuration.target_comp_id(),
            seq,
            Timestamp::now(),
        )
        .qualified(qualifier, qualifier);
        if repeated {
            header.poss_dup = true;
            header.orig_sending_time = Some(header.sending_time);
        }
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
                live.last_sent = Instant::now();
                Ok(())
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
            .map_err(|_| ConnectorError::Timeout(limit))??
            .map_err(|unreadable| unreadable.error)?;
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
                    Err(unreadable) => self.emit(Event::Unparsed(unreadable.error)),
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

    async fn ask_again(&self, live: &mut Live<C::Stream>, received: u64) -> Option<End> {
        if let Some((last, since)) = live.awaited {
            live.awaited = Some((last.max(received), since));
            return None;
        }
        let expected = live.next_in;
        self.emit(Event::Gap { expected, received });
        live.awaited = Some((received, Instant::now()));
        let request = ResendRequest {
            begin_seq_no: expected,
            end_seq_no: EVERY_LATER_MESSAGE,
        };
        match self.transmit(live, &request.into()).await {
            Err(ConnectorError::Transport(error)) => Some(End::Lost(error)),
            _ => None,
        }
    }

    async fn skip(&self, live: &mut Live<C::Stream>, from: u64) -> Option<End> {
        let to = live.next_out.checked_sub(1).filter(|to| *to >= from)?;
        let reset = SequenceReset {
            gap_fill: Some(true),
            new_seq_no: live.next_out,
        };
        let written = self.write(live, from, true, &reset.into()).await;
        self.emit(Event::Skipped { from, to });
        match written {
            Err(ConnectorError::Transport(error)) => Some(End::Lost(error)),
            _ => None,
        }
    }

    fn sequence(live: &mut Live<C::Stream>, frame: &Frame, message: Option<&Message>) -> Order {
        let seq = frame.header.seq;
        if let Some(Message::SequenceReset(reset)) = message {
            if reset.gap_fill != Some(true) {
                live.advance(reset.new_seq_no.max(live.next_in));
                return Order::InTurn;
            }
        }
        if seq < live.next_in {
            return if frame.header.poss_dup {
                Order::Repeated
            } else {
                Order::Behind
            };
        }
        if seq > live.next_in {
            return Order::Ahead;
        }
        let next = seq.saturating_add(1);
        match message {
            Some(Message::SequenceReset(reset)) => live.advance(reset.new_seq_no.max(next)),
            _ => live.advance(next),
        }
        Order::InTurn
    }

    async fn accept(&self, live: &mut Live<C::Stream>, frame: Frame) -> Option<End> {
        live.last_received = Instant::now();
        live.probed = false;
        let seq = frame.header.seq;
        let parsed = Message::from_frame(&frame);
        match Self::sequence(live, &frame, parsed.as_ref().ok()) {
            Order::InTurn => {}
            Order::Repeated => return None,
            Order::Behind => {
                return Some(End::Lost(TransportError::Decode(format!(
                    "sequence number {seq} is below the expected {}",
                    live.next_in
                ))));
            }
            Order::Ahead => {
                let asked = self.ask_again(live, seq).await;
                if asked.is_some()
                    || !matches!(parsed, Ok(Message::Logout(_) | Message::ResendRequest(_)))
                {
                    return asked;
                }
            }
        }
        let message = match parsed {
            Ok(message) => message,
            Err(error) => {
                let reject = rejection(seq, Some(&frame.header.msg_type), &error);
                self.emit(Event::Unparsed(error));
                return match self.transmit(live, &reject?.into()).await {
                    Err(ConnectorError::Transport(error)) => Some(End::Lost(error)),
                    _ => None,
                };
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
        let asked_from = match &message {
            Message::ResendRequest(request) => Some(request.begin_seq_no),
            _ => None,
        };
        self.emit(Event::Message(Inbound {
            seq,
            poss_dup: frame.header.poss_dup,
            message,
        }));
        if let Some(from) = asked_from {
            return self.skip(live, from).await;
        }
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

    async fn unreadable(&self, live: &mut Live<C::Stream>, unreadable: Unreadable) -> Option<End> {
        let Unreadable {
            seq,
            msg_type,
            error,
        } = unreadable;
        let Some(seq) = seq else {
            self.emit(Event::Unparsed(error));
            return None;
        };
        live.last_received = Instant::now();
        live.probed = false;
        if seq > live.next_in {
            return self.ask_again(live, seq).await;
        }
        let in_turn = seq == live.next_in;
        let reject = rejection(seq, msg_type.as_deref(), &error);
        self.emit(Event::Unparsed(error));
        if !in_turn {
            return None;
        }
        live.advance(seq.saturating_add(1));
        match self.transmit(live, &reject?.into()).await {
            Err(ConnectorError::Transport(error)) => Some(End::Lost(error)),
            _ => None,
        }
    }

    async fn beat(&self, live: &mut Live<C::Stream>) -> Option<End> {
        let heartbeat = self.configuration.heartbeat();
        let now = Instant::now();
        let silent = now.saturating_duration_since(live.last_received);
        let limit = heartbeat.saturating_mul(5) / 2;
        if silent >= limit {
            return Some(End::Lost(TransportError::Idle(limit)));
        }
        let patience = self.configuration.timeout();
        if let Some((_, since)) = live.awaited {
            if now.saturating_duration_since(since) >= patience {
                return Some(End::Lost(TransportError::Decode(format!(
                    "the messages from {} on were not sent again",
                    live.next_in
                ))));
            }
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
            Ok(_) => None,
            Err(ConnectorError::Transport(error)) => Some(End::Lost(error)),
            Err(error) => Some(End::Lost(TransportError::Decode(error.to_string()))),
        }
    }

    fn due(&self, live: &Live<C::Stream>) -> Option<Instant> {
        let heartbeat = self.configuration.heartbeat();
        let halves = if live.probed { 5 } else { 3 };
        let silence = after(live.last_received, heartbeat.saturating_mul(halves) / 2);
        let patience = live
            .awaited
            .and_then(|(_, since)| after(since, self.configuration.timeout()));
        [after(live.last_sent, heartbeat), silence, patience]
            .into_iter()
            .flatten()
            .min()
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
            Some(Command::Reconnect { reply }) => {
                let _ = reply.send(Ok(()));
                Some(End::Lost(TransportError::Closed))
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

    async fn serve(
        &self,
        live: &mut Live<C::Stream>,
        commands: &mut mpsc::Receiver<Command>,
    ) -> End {
        loop {
            let due = self.due(live);
            let end = tokio::select! {
                command = commands.recv() => self.command(live, command).await,
                received = live.receive() => match received {
                    Err(error) => Some(End::Lost(error)),
                    Ok(Err(unreadable)) => self.unreadable(live, unreadable).await,
                    Ok(Ok(frame)) => self.accept(live, frame).await,
                },
                () = reached(due) => self.beat(live).await,
            };
            if let Some(end) = end {
                return end;
            }
        }
    }

    async fn recover(&self, commands: &mut mpsc::Receiver<Command>) -> Option<Live<C::Stream>> {
        for attempt in 1..=RECONNECT_ATTEMPTS {
            let pause = self.configuration.reconnect_delay().saturating_mul(attempt);
            let attempted = async {
                sleep(pause).await;
                self.establish().await
            };
            tokio::pin!(attempted);
            let outcome = loop {
                tokio::select! {
                    outcome = &mut attempted => break outcome,
                    command = commands.recv() => match command {
                        None => return None,
                        Some(Command::Logout { reply }) => {
                            let _ = reply.send(Ok(()));
                            return None;
                        }
                        Some(Command::Reconnect { reply }) => {
                            let _ = reply.send(Ok(()));
                        }
                        Some(Command::Send { reply, .. }) => {
                            let _ = reply.send(Err(ConnectorError::Closed));
                        }
                    },
                }
            };
            match outcome {
                Ok(live) => {
                    self.emit(Event::Reconnected { attempt });
                    return Some(live);
                }
                Err(error) => {
                    let refused =
                        matches!(error, ConnectorError::Fix(FixError::LogonRefused { .. }));
                    self.emit(Event::ReconnectFailed { attempt, error });
                    if refused {
                        break;
                    }
                }
            }
        }
        self.emit(Event::Ended);
        None
    }

    async fn run(self, mut live: Live<C::Stream>, mut commands: mpsc::Receiver<Command>) {
        loop {
            match self.serve(&mut live, &mut commands).await {
                End::Shutdown => break,
                End::LoggedOut(text) => {
                    live.close().await;
                    self.emit(Event::LoggedOut { text });
                    break;
                }
                End::Lost(error) => {
                    live.close().await;
                    self.emit(Event::Disconnected(error));
                    match self.recover(&mut commands).await {
                        Some(fresh) => live = fresh,
                        None => break,
                    }
                }
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
        events: events.clone(),
        closed,
    };
    let live = actor.establish().await?;
    tokio::spawn(actor.run(live, receiver));
    Ok(Started {
        commands,
        events,
        closed: closed_receiver,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{Instant, timeout};

    use super::{after, reached};

    #[tokio::test(start_paused = true)]
    async fn wait_the_clock_cannot_hold_is_never_due() {
        let now = Instant::now();
        let brief = Duration::from_secs(90);
        assert_eq!(after(now, brief), Some(now + brief));
        assert_eq!(after(now, Duration::MAX), None);
        assert_eq!(timeout(brief, reached(after(now, brief))).await, Ok(()));
        assert_eq!(now.elapsed(), brief);
        let year = Duration::from_secs(365 * 86_400);
        assert!(timeout(year, reached(None)).await.is_err());
        assert_eq!(now.elapsed(), brief + year);
    }
}
