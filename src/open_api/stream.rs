//! Reading of the events of a connection on behalf of a typed stream.

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::common::errors::ConnectorError;
use crate::open_api::connection::{Connection, Event};

/// Events of a connection, from the moment the feed is created until the connection is shut
/// down.
#[derive(Debug)]
pub(crate) struct Feed {
    connection: Connection,
    events: broadcast::Receiver<Event>,
}

/// Tells whether the event precedes the data the server sends anew while the subscriptions are
/// replayed: the loss of the connection, or the failure of an attempt to restore it.
///
/// State rebuilt from the events is reset on this marker, which travels in the order of the
/// events themselves, so that nothing replayed is applied on top of what was known before or
/// wiped after it arrived.
pub(crate) const fn starts_replay(event: &Event) -> bool {
    matches!(
        event,
        Event::Disconnected(_) | Event::ReconnectFailed { .. }
    )
}

impl Feed {
    /// Starts to listen to the events of the connection.
    pub(crate) fn new(connection: &Connection) -> Self {
        Self {
            connection: connection.clone(),
            events: connection.events(),
        }
    }

    /// Waits for the next event.
    ///
    /// A reader that fell so far behind that events were dropped unread is told so once, with
    /// [`ConnectorError::Lagged`], and then goes on with the oldest event still held.
    pub(crate) async fn next(&mut self) -> Result<Event, ConnectorError> {
        tokio::select! {
            biased;
            event = self.events.recv() => match event {
                Ok(event) => Ok(event),
                Err(RecvError::Lagged(skipped)) => Err(ConnectorError::Lagged { skipped }),
                Err(RecvError::Closed) => Err(ConnectorError::Closed),
            },
            () = self.connection.closed() => Err(ConnectorError::Closed),
        }
    }
}
