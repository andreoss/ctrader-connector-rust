//! Orders of the trade session.
//!
//! A [`NewOrder`] is built for one of the three kinds the session takes, at the market, at
//! a limit or at a stop, and placed with [`FixClient::place`]. The answer is an
//! [`OrderReport`], the execution report of the server with the fields every such report of
//! an order carries made mandatory.
//!
//! The quantity of an order counts units of the base asset of the symbol, not lots: the
//! server accepts `0.01` for a symbol traded in hundredths and `1000` for one traded in
//! thousands, and reports the same number back.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationFix;
//! use ctrader_connector::common::ids::SymbolId;
//! use ctrader_connector::fix::client::FixClient;
//! use ctrader_connector::fix::messages::Side;
//! use ctrader_connector::fix::session::SessionKind;
//! use ctrader_connector::fix::trading::NewOrder;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = FixClient::connect(ConfigurationFix::from_env()?, SessionKind::Trade).await?;
//! let order = NewOrder::limit(SymbolId::new(1)?, Side::Buy, "1000".parse()?, "0.9".parse()?)
//!     .label("example");
//! let report = client.place(order).await?;
//! println!("{} is {:?}", report.order_id, report.status);
//! client.logout().await?;
//! # Ok(())
//! # }
//! ```

use crate::common::decimals::{Price, Volume};
use crate::common::errors::{ConnectorError, FixError};
use crate::common::ids::{OrderId, PositionId, SymbolId};
use crate::common::time::Timestamp;
use crate::fix::client::FixClient;
use crate::fix::messages::{
    ExecType, ExecutionReport, Message, NewOrderSingle, OrderKind, OrderStatus, Side, TimeInForce,
};

const ORDER_PREFIX: &str = "o";
const TAG_ORDER_ID: u32 = 37;
const TAG_CL_ORD_ID: u32 = 11;
const TAG_SYMBOL: u32 = 55;
const TAG_SIDE: u32 = 54;
const TAG_ORDER_QTY: u32 = 38;

/// An order to be placed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrder {
    symbol: SymbolId,
    side: Side,
    quantity: Volume,
    kind: OrderKind,
    price: Option<Price>,
    stop_price: Option<Price>,
    expiry: Option<Timestamp>,
    position: Option<PositionId>,
    label: Option<String>,
}

impl NewOrder {
    const fn new(symbol: SymbolId, side: Side, quantity: Volume, kind: OrderKind) -> Self {
        Self {
            symbol,
            side,
            quantity,
            kind,
            price: None,
            stop_price: None,
            expiry: None,
            position: None,
            label: None,
        }
    }

    /// Starts an order executed at once at the price of the market.
    #[must_use]
    pub const fn market(symbol: SymbolId, side: Side, quantity: Volume) -> Self {
        Self::new(symbol, side, quantity, OrderKind::Market)
    }

    /// Starts an order that waits for the given price or a better one.
    #[must_use]
    pub const fn limit(symbol: SymbolId, side: Side, quantity: Volume, price: Price) -> Self {
        let mut order = Self::new(symbol, side, quantity, OrderKind::Limit);
        order.price = Some(price);
        order
    }

    /// Starts an order that becomes a market order once the given price is reached.
    #[must_use]
    pub const fn stop(symbol: SymbolId, side: Side, quantity: Volume, price: Price) -> Self {
        let mut order = Self::new(symbol, side, quantity, OrderKind::Stop);
        order.stop_price = Some(price);
        order
    }

    /// Sets the label the order and its position carry.
    #[must_use]
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Names the position the order adds to or reduces; without it the order opens a
    /// position of its own.
    #[must_use]
    pub const fn position(mut self, position: PositionId) -> Self {
        self.position = Some(position);
        self
    }

    /// Sets the moment a waiting order expires; without it the order waits until cancelled.
    #[must_use]
    pub const fn expires(mut self, at: Timestamp) -> Self {
        self.expiry = Some(at);
        self
    }

    /// Returns the kind of the order.
    #[must_use]
    pub const fn kind(&self) -> OrderKind {
        self.kind
    }

    fn message(&self, cl_ord_id: &str) -> NewOrderSingle {
        NewOrderSingle {
            cl_ord_id: cl_ord_id.to_owned(),
            symbol: self.symbol,
            side: self.side,
            transact_time: Timestamp::now(),
            order_qty: self.quantity,
            ord_type: self.kind,
            price: self.price,
            stop_px: self.stop_price,
            time_in_force: self.expiry.map(|_| TimeInForce::GoodTillDate),
            expire_time: self.expiry,
            position_id: self.position,
            designation: self.label.clone(),
        }
    }
}

/// State of an order as the server reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderReport {
    /// Identifier the server gave the order, the same the other API of the server uses.
    pub order_id: OrderId,
    /// Identifier the client gave the order.
    pub cl_ord_id: String,
    /// What the report tells: an acceptance, a fill, a change, a cancellation or a state.
    pub exec_type: ExecType,
    /// State of the order.
    pub status: OrderStatus,
    /// Symbol of the order.
    pub symbol: SymbolId,
    /// Side of the order.
    pub side: Side,
    /// Quantity ordered, in units of the base asset.
    pub quantity: Volume,
    /// Kind of the order.
    pub kind: Option<OrderKind>,
    /// Limit price.
    pub price: Option<Price>,
    /// Stop price.
    pub stop_price: Option<Price>,
    /// How long the order stays in force.
    pub time_in_force: Option<TimeInForce>,
    /// Moment the order expires.
    pub expiry: Option<Timestamp>,
    /// Quantity filled so far.
    pub filled: Option<Volume>,
    /// Quantity still open.
    pub remaining: Option<Volume>,
    /// Quantity of the fill the report tells of.
    pub last_quantity: Option<Volume>,
    /// Average price of the fills.
    pub average_price: Option<Price>,
    /// Position the order belongs to.
    pub position: Option<PositionId>,
    /// Label of the order.
    pub label: Option<String>,
    /// Moment of the report.
    pub time: Option<Timestamp>,
    /// Explanation of the server.
    pub text: Option<String>,
}

impl TryFrom<&ExecutionReport> for OrderReport {
    type Error = FixError;

    fn try_from(report: &ExecutionReport) -> Result<Self, FixError> {
        let missing = |tag| FixError::MissingField {
            msg_type: ExecutionReport::MSG_TYPE.to_owned(),
            tag,
        };
        Ok(Self {
            order_id: report.order_id.ok_or_else(|| missing(TAG_ORDER_ID))?,
            cl_ord_id: report
                .cl_ord_id
                .clone()
                .ok_or_else(|| missing(TAG_CL_ORD_ID))?,
            exec_type: report.exec_type,
            status: report.ord_status,
            symbol: report.symbol.ok_or_else(|| missing(TAG_SYMBOL))?,
            side: report.side.ok_or_else(|| missing(TAG_SIDE))?,
            quantity: report.order_qty.ok_or_else(|| missing(TAG_ORDER_QTY))?,
            kind: report.ord_type,
            price: report.price,
            stop_price: report.stop_px,
            time_in_force: report.time_in_force,
            expiry: report.expire_time,
            filled: report.cum_qty,
            remaining: report.leaves_qty,
            last_quantity: report.last_qty,
            average_price: report.avg_px,
            position: report.position_id,
            label: report.designation.clone(),
            time: report.transact_time,
            text: report.text.clone(),
        })
    }
}

pub(crate) fn refused(report: &ExecutionReport) -> ConnectorError {
    FixError::OrderRejected {
        cl_ord_id: report.cl_ord_id.clone(),
        reason: report.ord_rej_reason,
        text: report.text.clone(),
    }
    .into()
}

fn placed(
    report: &ExecutionReport,
    kind: OrderKind,
) -> Option<Result<OrderReport, ConnectorError>> {
    match (report.exec_type, kind) {
        (ExecType::New, OrderKind::Market) => None,
        (ExecType::Rejected | ExecType::Canceled | ExecType::Expired, _) => {
            Some(Err(refused(report)))
        }
        _ => Some(OrderReport::try_from(report).map_err(ConnectorError::from)),
    }
}

impl FixClient {
    /// Places an order and returns the report that settles the request: the fill of a market
    /// order, the acceptance of a limit or stop order.
    ///
    /// The order is sent once and never again by the library.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] with [`FixError::OrderRejected`] when the server
    /// accepts the message and then refuses, cancels or expires the order, with
    /// [`FixError::BusinessRejected`] or [`FixError::Rejected`] when it refuses the message,
    /// and with [`FixError::MissingField`] when its report lacks a field every report of an
    /// order carries. Returns [`ConnectorError::OutcomeUnknown`] when the answer does not
    /// arrive in time or the session is lost first: the order may or may not exist. Returns
    /// otherwise the errors of [`FixClient::request_once`].
    pub async fn place(&self, order: NewOrder) -> Result<OrderReport, ConnectorError> {
        let id = self.next_id(ORDER_PREFIX);
        let kind = order.kind();
        self.request_once(order.message(&id).into(), &id, |message| match message {
            Message::ExecutionReport(report) if report.cl_ord_id.as_deref() == Some(&id) => {
                placed(report, kind)
            }
            _ => None,
        })
        .await
    }
}
