//! Stream of the execution events of an account.
//!
//! The server sends the events of every authorised account by itself; nothing is subscribed
//! to. The stream also delivers the events that answer the requests of this client, such as
//! the acceptance [`OpenApiClient::place_order`] returns, so the life of an order is read from
//! one place and in the order the server reported it.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::executions::TradeEvent;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let mut executions = client.executions(account);
//! loop {
//!     match executions.next().await? {
//!         TradeEvent::Execution(execution) => println!("{:?}", execution.kind),
//!         TradeEvent::OrderError(error) => println!("refused: {}", error.code),
//!         _ => {}
//!     }
//! }
//! # }
//! ```

use crate::common::errors::{ConnectorError, ErrorCode, ValueError};
use crate::common::ids::{AccountId, OrderId, PositionId};
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{ProtoOaExecutionEvent, ProtoOaOrderErrorEvent};
use crate::open_api::stream::Feed;
use crate::open_api::trading::Execution;

/// Refusal of a request that trades, as the server reports it in an order error event.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct OrderError {
    /// Account the refused request was made for.
    pub account: AccountId,
    /// Reason of the refusal.
    pub code: ErrorCode,
    /// Order the refused request was about.
    pub order: Option<OrderId>,
    /// Position the refused request was about.
    pub position: Option<PositionId>,
    /// Explanation the server gives.
    pub description: Option<String>,
}

impl TryFrom<ProtoOaOrderErrorEvent> for OrderError {
    type Error = ValueError;

    fn try_from(event: ProtoOaOrderErrorEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            account: AccountId::new(event.ctid_trader_account_id)?,
            code: ErrorCode::from_wire(&event.error_code),
            order: event.order_id.map(OrderId::new).transpose()?,
            position: event.position_id.map(PositionId::new).transpose()?,
            description: event.description,
        })
    }
}

/// One event of the trading of an account.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TradeEvent {
    /// Something happened to an order, a position or the money of the account; the
    /// [`Execution::kind`] tells what.
    Execution(Box<Execution>),
    /// The server refused a request that trades.
    OrderError(OrderError),
}

impl TradeEvent {
    /// Returns the execution when the event is one.
    #[must_use]
    pub fn execution(&self) -> Option<&Execution> {
        match self {
            Self::Execution(execution) => Some(execution),
            Self::OrderError(_) => None,
        }
    }

    /// Returns the order the event is about, when it names one.
    #[must_use]
    pub fn order(&self) -> Option<OrderId> {
        match self {
            Self::Execution(execution) => execution.order.as_ref().map(|order| order.id),
            Self::OrderError(error) => error.order,
        }
    }
}

/// Execution events and order error events of one account, in the order the server sent them.
///
/// The stream starts with the first event that arrives after it was created and goes on
/// across reconnections; events the server sent while the connection was down are not sent
/// again, so after a reconnection the state of the account is read anew with
/// [`OpenApiClient::reconcile`].
///
/// A stream that is read too slowly loses events: the connection holds a limited number of
/// them, set with
/// [`ConnectionOptions::event_capacity`](crate::open_api::connection::ConnectionOptions::event_capacity).
/// The loss is reported once by [`ExecutionStream::next`] with [`ConnectorError::Lagged`] and
/// the stream then goes on with the oldest event still held.
#[derive(Debug)]
pub struct ExecutionStream {
    feed: Feed,
    account: AccountId,
}

impl ExecutionStream {
    /// Returns the account the events belong to.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Waits for the next event of the account.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down,
    /// [`ConnectorError::Lagged`] when events were dropped because the stream was read too
    /// slowly, and [`ConnectorError::Value`] when the server sends a value out of range; the
    /// stream can be read on after the last two.
    pub async fn next(&mut self) -> Result<TradeEvent, ConnectorError> {
        loop {
            let event = self.feed.next().await?;
            if let Some(execution) = event.decode::<ProtoOaExecutionEvent>() {
                if execution.ctid_trader_account_id == self.account.get() {
                    let execution = Execution::try_from(execution)?;
                    return Ok(TradeEvent::Execution(Box::new(execution)));
                }
            } else if let Some(error) = event.decode::<ProtoOaOrderErrorEvent>() {
                if error.ctid_trader_account_id == self.account.get() {
                    return Ok(TradeEvent::OrderError(OrderError::try_from(error)?));
                }
            }
        }
    }
}

impl OpenApiClient {
    /// Starts to read the execution events and order error events of an authorised account.
    ///
    /// Nothing is sent: the server delivers these events for every account authorised on the
    /// connection. Create the stream before the request whose events are to be read.
    #[must_use]
    pub fn executions(&self, account: AccountId) -> ExecutionStream {
        ExecutionStream {
            feed: Feed::new(self.connection()),
            account,
        }
    }
}
