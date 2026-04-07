//! Client of one FIX session of a trading account.
//!
//! An account has two sessions, one for prices and one for trading; each is a connection of
//! its own and a [`FixClient`] of its own.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationFix;
//! use ctrader_connector::fix::client::FixClient;
//! use ctrader_connector::fix::session::SessionKind;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = FixClient::connect(ConfigurationFix::from_env()?, SessionKind::Quote).await?;
//! client.logout().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::timeout;

use crate::common::config::ConfigurationFix;
use crate::common::errors::{ConnectorError, FixError, Interruption, TransportError};
use crate::common::time::Timestamp;
use crate::common::transport::{Connect, Plain, Tls};
use crate::fix::messages::Message;
use crate::fix::session::{Command, Event, SessionKind, start};

/// Cloneable client of one FIX session.
#[derive(Debug, Clone)]
pub struct FixClient {
    commands: mpsc::Sender<Command>,
    events: broadcast::Sender<Event>,
    closed: watch::Receiver<bool>,
    kind: SessionKind,
    timeout: Duration,
    counter: Arc<AtomicU64>,
}

impl FixClient {
    /// Connects to the host named by the configuration and logs on to the session.
    ///
    /// The connection runs over TLS unless the configuration asks for none.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the connection cannot be established,
    /// [`ConnectorError::Fix`] with [`FixError::LogonRefused`] when the server answers the
    /// logon with a logout, and [`ConnectorError::Timeout`] when it does not answer in time.
    pub async fn connect(
        configuration: ConfigurationFix,
        kind: SessionKind,
    ) -> Result<Self, ConnectorError> {
        if configuration.encrypted() {
            Self::connect_with(configuration, kind, Tls::new()?).await
        } else {
            Self::connect_with(configuration, kind, Plain).await
        }
    }

    /// Connects through the given connector and logs on to the session.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`FixClient::connect`].
    pub async fn connect_with<C: Connect>(
        configuration: ConfigurationFix,
        kind: SessionKind,
        connector: C,
    ) -> Result<Self, ConnectorError> {
        let limit = configuration.timeout();
        let started = start(connector, configuration, kind).await?;
        Ok(Self {
            commands: started.commands,
            events: started.events,
            closed: started.closed,
            kind,
            timeout: limit,
            counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Returns which session the client serves.
    #[must_use]
    pub const fn kind(&self) -> SessionKind {
        self.kind
    }

    /// Returns the longest wait for an answer of the server.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Subscribes to the events of the session from this moment on.
    ///
    /// A reader that falls behind loses the oldest events and is told how many.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Sends a message and returns the sequence number it was given.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] when the message cannot be encoded,
    /// [`ConnectorError::Transport`] when the write fails, and [`ConnectorError::Closed`]
    /// when the session has ended.
    pub async fn send(&self, message: Message) -> Result<u64, ConnectorError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Send { message, reply })
            .await
            .map_err(|_| ConnectorError::Closed)?;
        answer.await.map_err(|_| ConnectorError::Closed)?
    }

    /// Logs out and closes the connection; the session ends for every clone of the client.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Timeout`] when the server does not confirm the logout in
    /// time, [`ConnectorError::Transport`] when the connection fails first, and
    /// [`ConnectorError::Closed`] when the session has ended already. The connection is
    /// closed in every case.
    pub async fn logout(&self) -> Result<(), ConnectorError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Logout { reply })
            .await
            .map_err(|_| ConnectorError::Closed)?;
        answer.await.map_err(|_| ConnectorError::Closed)?
    }

    /// Waits until the session has ended.
    pub async fn closed(&self) {
        let mut closed = self.closed.clone();
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// Sends a request and waits for the message `pick` accepts as its answer.
    ///
    /// `pick` sees every message that arrives after the request was sent and returns the
    /// outcome once it recognises the answer. `reference` is the identifier the request
    /// carries, by which a refusal of the server names it; it may be empty.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] with [`FixError::Rejected`] or
    /// [`FixError::BusinessRejected`] when the server refuses the request,
    /// [`ConnectorError::Timeout`] when no answer arrives in time,
    /// [`ConnectorError::Transport`] when the session ends first,
    /// [`ConnectorError::Lagged`] when the reader fell behind and the answer may have been
    /// skipped, and [`ConnectorError::Closed`] when the session has ended already.
    pub async fn request<T>(
        &self,
        message: Message,
        reference: &str,
        pick: impl FnMut(&Message) -> Option<Result<T, ConnectorError>>,
    ) -> Result<T, ConnectorError> {
        self.exchange(message, reference, false, pick).await
    }

    /// Sends a request that must not be sent twice, such as an order, and waits for its answer.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`FixClient::request`], except that a wait cut short by the
    /// deadline or by the loss of the session is reported as
    /// [`ConnectorError::OutcomeUnknown`]: the server may have acted on the request.
    pub async fn request_once<T>(
        &self,
        message: Message,
        reference: &str,
        pick: impl FnMut(&Message) -> Option<Result<T, ConnectorError>>,
    ) -> Result<T, ConnectorError> {
        self.exchange(message, reference, true, pick).await
    }

    /// Returns an identifier no other request of this client carries.
    #[must_use]
    pub fn next_id(&self, prefix: &str) -> String {
        let number = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}{}n{number}", Timestamp::now().as_millis())
    }

    /// Sends a request and waits for what `pick` accepts as its answer.
    ///
    /// A reject of the session level or of the application level that refers to the request,
    /// by its sequence number or by `reference`, ends the wait with a typed error. For a
    /// `critical` request, one that must not be sent twice, a wait cut short by the deadline
    /// or by the loss of the connection is reported as [`ConnectorError::OutcomeUnknown`].
    async fn exchange<T>(
        &self,
        message: Message,
        reference: &str,
        critical: bool,
        mut pick: impl FnMut(&Message) -> Option<Result<T, ConnectorError>>,
    ) -> Result<T, ConnectorError> {
        let interrupted = |interruption: Interruption| match (critical, interruption) {
            (true, interruption) => ConnectorError::OutcomeUnknown(interruption),
            (false, Interruption::Timeout(limit)) => ConnectorError::Timeout(limit),
            (false, Interruption::Transport(error)) => ConnectorError::Transport(error),
        };
        let mut events = self.events.subscribe();
        let seq = match self.send(message).await {
            Err(ConnectorError::Transport(error)) => {
                return Err(interrupted(Interruption::Transport(error)));
            }
            sent => sent?,
        };
        let limit = self.timeout;
        let waited = timeout(limit, async {
            loop {
                let inbound = match events.recv().await {
                    Ok(Event::Message(inbound)) => inbound,
                    Ok(Event::Disconnected(error)) => {
                        return Err(interrupted(Interruption::Transport(error)));
                    }
                    Ok(Event::LoggedOut { .. }) | Err(RecvError::Closed) => {
                        return Err(interrupted(Interruption::Transport(TransportError::Closed)));
                    }
                    Ok(_) => continue,
                    Err(RecvError::Lagged(skipped)) => {
                        return Err(ConnectorError::Lagged { skipped });
                    }
                };
                match &inbound.message {
                    Message::Reject(reject) if reject.ref_seq_num == seq => {
                        return Err(FixError::Rejected {
                            ref_seq_num: seq,
                            tag: reject.ref_tag_id,
                            reason: reject.reason,
                            text: reject.text.clone(),
                        }
                        .into());
                    }
                    Message::BusinessReject(reject)
                        if reject.ref_seq_num == Some(seq)
                            || (!reference.is_empty()
                                && reject.ref_id.as_deref() == Some(reference)) =>
                    {
                        return Err(FixError::BusinessRejected {
                            ref_id: reject.ref_id.clone(),
                            reason: reject.reason,
                            text: reject.text.clone(),
                        }
                        .into());
                    }
                    other => {
                        if let Some(result) = pick(other) {
                            return result;
                        }
                    }
                }
            }
        })
        .await;
        waited.unwrap_or_else(|_| Err(interrupted(Interruption::Timeout(limit))))
    }
}
