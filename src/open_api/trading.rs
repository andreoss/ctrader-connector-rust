//! Orders, positions and deals of a trading account, and the requests that change them.
//!
//! Volumes are in units of the symbol and prices are exact decimals; the wire integers and
//! doubles are produced and read here and nowhere else. An order is checked against the
//! trading details of its symbol before anything is sent.
//!
//! A request that trades is answered by the first execution event the server sends for it,
//! which for an order at market is its acceptance; the events that follow, the fill among them,
//! are read from the events of the connection. Such a request is written to the socket at most
//! once and is never sent again by this library: when the connection fails or the time allowed
//! runs out after it was written, the caller gets [`ConnectorError::OutcomeUnknown`] and has to
//! look the order up before deciding to place it again.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::common::decimals::Volume;
//! use ctrader_connector::common::ids::SymbolId;
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::trading::{Label, NewOrder, TradeSide};
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let symbol = &client.symbol_details(account, &[SymbolId::new(1)?]).await?[0];
//! let volume: Volume = "1000".parse()?;
//! let order = NewOrder::market(account, symbol.id, TradeSide::Buy, volume)
//!     .label(Label::new("example")?);
//! let accepted = client.place_order(&order, symbol).await?;
//! println!("{:?}: {:?}", accepted.kind, accepted.order.map(|order| order.id));
//! # Ok(())
//! # }
//! ```

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::common::decimals::{PRICE_SCALE, Price, Volume};
use crate::common::errors::{ConnectorError, ErrorCode, ServerError, ValueError};
use crate::common::ids::{AccountId, DealId, OrderId, PositionId, SymbolId};
use crate::common::time::Timestamp;
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{
    ProtoMessage, ProtoOaClosePositionDetail, ProtoOaDeal, ProtoOaDealStatus,
    ProtoOaExecutionEvent, ProtoOaExecutionType, ProtoOaNewOrderReq, ProtoOaOrder,
    ProtoOaOrderErrorEvent, ProtoOaOrderStatus, ProtoOaOrderType, ProtoOaPosition,
    ProtoOaPositionStatus, ProtoOaTimeInForce, ProtoOaTradeSide,
};
use crate::open_api::payload::Payload;
use crate::open_api::symbols::Symbol;

/// Largest number of characters of a [`Label`].
pub const MAX_LABEL_LENGTH: usize = 100;
/// Largest number of characters of a [`Comment`].
pub const MAX_COMMENT_LENGTH: usize = 512;

const DEFAULT_MONEY_DIGITS: u32 = 2;

const LABEL: &str = "label";
const COMMENT: &str = "comment";
const VOLUME: &str = "volume";
const PRICE: &str = "price";
const MONEY: &str = "amount of money";
const DISTANCE: &str = "distance of the protection";
const SYMBOL: &str = "symbol of the order";
const TRADE_SIDE: &str = "trade side";
const ORDER_TYPE: &str = "order type";
const ORDER_STATUS: &str = "order status";
const TIME_IN_FORCE: &str = "time in force";
const EXPIRY: &str = "expiry of the order";
const POSITION_STATUS: &str = "position status";
const DEAL_STATUS: &str = "deal status";
const EXECUTION_TYPE: &str = "execution type";
const SLIPPAGE: &str = "slippage in points";
const PROTECTION: &str = "protection given as a price";

macro_rules! text {
    ($(#[$doc:meta])* $name:ident, $kind:expr, $limit:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// Wraps the text.
            ///
            /// # Errors
            ///
            /// Returns [`ValueError::TooLong`] when the text has more characters than the
            /// server keeps.
            pub fn new(text: impl Into<String>) -> Result<Self, ValueError> {
                let text = text.into();
                let length = text.chars().count();
                if length > $limit {
                    return Err(ValueError::TooLong {
                        kind: $kind,
                        length,
                        limit: $limit,
                    });
                }
                Ok(Self(text))
            }

            /// Returns the text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

text!(
    /// Label a client gives its orders, kept by the position an order opens; at most
    /// [`MAX_LABEL_LENGTH`] characters.
    Label,
    LABEL,
    MAX_LABEL_LENGTH
);
text!(
    /// Comment of an order, kept by the position it opens; at most [`MAX_COMMENT_LENGTH`]
    /// characters.
    Comment,
    COMMENT,
    MAX_COMMENT_LENGTH
);

/// Direction of a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeSide {
    /// The symbol is bought.
    Buy,
    /// The symbol is sold.
    Sell,
}

impl TradeSide {
    /// Returns the other direction, the one that closes a trade of this direction.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    const fn to_wire(self) -> ProtoOaTradeSide {
        match self {
            Self::Buy => ProtoOaTradeSide::Buy,
            Self::Sell => ProtoOaTradeSide::Sell,
        }
    }

    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaTradeSide::try_from(raw) {
            Ok(ProtoOaTradeSide::Buy) => Ok(Self::Buy),
            Ok(ProtoOaTradeSide::Sell) => Ok(Self::Sell),
            Err(_) => Err(ValueError::OutOfRange { kind: TRADE_SIDE }),
        }
    }
}

/// Kind of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OrderType {
    /// Executed at once at the price of the market.
    Market,
    /// Executed at the limit price or a better one.
    Limit,
    /// Turned into an order at market once the stop price is reached.
    Stop,
    /// Stop loss or take profit of a position.
    StopLossTakeProfit,
    /// Executed at once unless the price moved further than the slippage allows.
    MarketRange,
    /// Turned into a limit order once the stop price is reached.
    StopLimit,
}

impl OrderType {
    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaOrderType::try_from(raw) {
            Ok(ProtoOaOrderType::Market) => Ok(Self::Market),
            Ok(ProtoOaOrderType::Limit) => Ok(Self::Limit),
            Ok(ProtoOaOrderType::Stop) => Ok(Self::Stop),
            Ok(ProtoOaOrderType::StopLossTakeProfit) => Ok(Self::StopLossTakeProfit),
            Ok(ProtoOaOrderType::MarketRange) => Ok(Self::MarketRange),
            Ok(ProtoOaOrderType::StopLimit) => Ok(Self::StopLimit),
            Err(_) => Err(ValueError::OutOfRange { kind: ORDER_TYPE }),
        }
    }
}

/// State of an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OrderStatus {
    /// Accepted by the server and waiting to be executed.
    Accepted,
    /// Executed in full.
    Filled,
    /// Refused by the server.
    Rejected,
    /// Past the moment it was good till.
    Expired,
    /// Cancelled on request.
    Cancelled,
}

impl OrderStatus {
    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaOrderStatus::try_from(raw) {
            Ok(ProtoOaOrderStatus::OrderStatusAccepted) => Ok(Self::Accepted),
            Ok(ProtoOaOrderStatus::OrderStatusFilled) => Ok(Self::Filled),
            Ok(ProtoOaOrderStatus::OrderStatusRejected) => Ok(Self::Rejected),
            Ok(ProtoOaOrderStatus::OrderStatusExpired) => Ok(Self::Expired),
            Ok(ProtoOaOrderStatus::OrderStatusCancelled) => Ok(Self::Cancelled),
            Err(_) => Err(ValueError::OutOfRange { kind: ORDER_STATUS }),
        }
    }
}

/// How long an order stays in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimeInForce {
    /// Until the given moment.
    GoodTillDate(Timestamp),
    /// Until it is executed or cancelled.
    GoodTillCancel,
    /// Whatever can be executed at once is, the rest is cancelled.
    ImmediateOrCancel,
    /// Executed at once in full or not at all.
    FillOrKill,
    /// Executed when the market opens.
    MarketOnOpen,
}

impl TimeInForce {
    fn from_wire(raw: Option<i32>, expiry: Option<i64>) -> Result<Self, ValueError> {
        let wire = raw.map_or(Ok(ProtoOaTimeInForce::ImmediateOrCancel), |raw| {
            ProtoOaTimeInForce::try_from(raw)
        });
        match wire {
            Ok(ProtoOaTimeInForce::GoodTillDate) => expiry
                .ok_or(ValueError::Missing { kind: EXPIRY })
                .and_then(Timestamp::from_millis)
                .map(Self::GoodTillDate),
            Ok(ProtoOaTimeInForce::GoodTillCancel) => Ok(Self::GoodTillCancel),
            Ok(ProtoOaTimeInForce::ImmediateOrCancel) => Ok(Self::ImmediateOrCancel),
            Ok(ProtoOaTimeInForce::FillOrKill) => Ok(Self::FillOrKill),
            Ok(ProtoOaTimeInForce::MarketOnOpen) => Ok(Self::MarketOnOpen),
            Err(_) => Err(ValueError::OutOfRange {
                kind: TIME_IN_FORCE,
            }),
        }
    }
}

/// State of a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PositionStatus {
    /// Open.
    Open,
    /// Closed.
    Closed,
    /// Created for a pending order that has not been executed yet.
    Created,
    /// In a state of error.
    Error,
}

impl PositionStatus {
    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaPositionStatus::try_from(raw) {
            Ok(ProtoOaPositionStatus::PositionStatusOpen) => Ok(Self::Open),
            Ok(ProtoOaPositionStatus::PositionStatusClosed) => Ok(Self::Closed),
            Ok(ProtoOaPositionStatus::PositionStatusCreated) => Ok(Self::Created),
            Ok(ProtoOaPositionStatus::PositionStatusError) => Ok(Self::Error),
            Err(_) => Err(ValueError::OutOfRange {
                kind: POSITION_STATUS,
            }),
        }
    }
}

/// State of a deal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DealStatus {
    /// Executed in full.
    Filled,
    /// Executed in part.
    PartiallyFilled,
    /// Refused by the liquidity provider.
    Rejected,
    /// Refused by the server.
    InternallyRejected,
    /// Failed with an error.
    Error,
    /// Missed by the liquidity provider.
    Missed,
}

impl DealStatus {
    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaDealStatus::try_from(raw) {
            Ok(ProtoOaDealStatus::Filled) => Ok(Self::Filled),
            Ok(ProtoOaDealStatus::PartiallyFilled) => Ok(Self::PartiallyFilled),
            Ok(ProtoOaDealStatus::Rejected) => Ok(Self::Rejected),
            Ok(ProtoOaDealStatus::InternallyRejected) => Ok(Self::InternallyRejected),
            Ok(ProtoOaDealStatus::Error) => Ok(Self::Error),
            Ok(ProtoOaDealStatus::Missed) => Ok(Self::Missed),
            Err(_) => Err(ValueError::OutOfRange { kind: DEAL_STATUS }),
        }
    }
}

/// What an execution event reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExecutionType {
    /// The order was accepted.
    Accepted,
    /// The order was executed in full.
    Filled,
    /// The order, or the protection of a position, was amended.
    Replaced,
    /// The order was cancelled.
    Cancelled,
    /// The order expired.
    Expired,
    /// The order was refused.
    Rejected,
    /// The request to cancel the order was refused.
    CancelRejected,
    /// Swap was charged to or paid for a position.
    Swap,
    /// Money was deposited to or withdrawn from the account.
    DepositWithdraw,
    /// The order was executed in part.
    PartiallyFilled,
    /// A bonus was deposited to or withdrawn from the account.
    BonusDepositWithdraw,
}

impl ExecutionType {
    fn from_wire(raw: i32) -> Result<Self, ValueError> {
        match ProtoOaExecutionType::try_from(raw) {
            Ok(ProtoOaExecutionType::OrderAccepted) => Ok(Self::Accepted),
            Ok(ProtoOaExecutionType::OrderFilled) => Ok(Self::Filled),
            Ok(ProtoOaExecutionType::OrderReplaced) => Ok(Self::Replaced),
            Ok(ProtoOaExecutionType::OrderCancelled) => Ok(Self::Cancelled),
            Ok(ProtoOaExecutionType::OrderExpired) => Ok(Self::Expired),
            Ok(ProtoOaExecutionType::OrderRejected) => Ok(Self::Rejected),
            Ok(ProtoOaExecutionType::OrderCancelRejected) => Ok(Self::CancelRejected),
            Ok(ProtoOaExecutionType::Swap) => Ok(Self::Swap),
            Ok(ProtoOaExecutionType::DepositWithdraw) => Ok(Self::DepositWithdraw),
            Ok(ProtoOaExecutionType::OrderPartialFill) => Ok(Self::PartiallyFilled),
            Ok(ProtoOaExecutionType::BonusDepositWithdraw) => Ok(Self::BonusDepositWithdraw),
            Err(_) => Err(ValueError::OutOfRange {
                kind: EXECUTION_TYPE,
            }),
        }
    }
}

/// Converts a price the wire carries as a double, rounding it to the places a price can have.
pub(crate) fn double_price(raw: f64) -> Result<Price, ValueError> {
    let value = Decimal::try_from(raw).map_err(|_| ValueError::OutOfRange { kind: PRICE })?;
    Price::new(value.round_dp(PRICE_SCALE))
}

fn optional_price(raw: Option<f64>) -> Result<Option<Price>, ValueError> {
    raw.map(double_price).transpose()
}

fn relative_price(raw: Option<i64>) -> Result<Option<Price>, ValueError> {
    raw.map(|raw| {
        u64::try_from(raw)
            .map_err(|_| ValueError::OutOfRange { kind: DISTANCE })
            .and_then(|raw| Price::from_wire(raw, PRICE_SCALE))
    })
    .transpose()
}

/// Converts an amount of money the wire carries as an integer scaled by `moneyDigits`.
pub(crate) fn money(raw: i64, digits: Option<u32>) -> Result<Decimal, ValueError> {
    Decimal::try_new(raw, digits.unwrap_or(DEFAULT_MONEY_DIGITS))
        .map_err(|_| ValueError::OutOfRange { kind: MONEY })
}

fn moment(raw: Option<i64>) -> Result<Option<Timestamp>, ValueError> {
    raw.map(Timestamp::from_millis).transpose()
}

/// Order as the server knows it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Order {
    /// Identifier of the order.
    pub id: OrderId,
    /// Symbol the order trades.
    pub symbol: SymbolId,
    /// Direction of the order.
    pub side: TradeSide,
    /// Volume of the order, in units of the symbol.
    pub volume: Volume,
    /// Kind of the order.
    pub kind: OrderType,
    /// State of the order.
    pub status: OrderStatus,
    /// How long the order stays in force.
    pub time_in_force: TimeInForce,
    /// Limit price of a limit order.
    pub limit_price: Option<Price>,
    /// Stop price of a stop or stop-limit order.
    pub stop_price: Option<Price>,
    /// Stop loss as a price.
    pub stop_loss: Option<Price>,
    /// Take profit as a price.
    pub take_profit: Option<Price>,
    /// Stop loss as a distance from the price of execution.
    pub relative_stop_loss: Option<Price>,
    /// Take profit as a distance from the price of execution.
    pub relative_take_profit: Option<Price>,
    /// Slippage allowed to a market-range or stop-limit order, in points.
    pub slippage_points: Option<u32>,
    /// Volume executed so far.
    pub executed_volume: Option<Volume>,
    /// Price the order was executed at.
    pub execution_price: Option<Price>,
    /// Position the order opened, changes or closes.
    pub position: Option<PositionId>,
    /// Label the client gave the order.
    pub label: Option<String>,
    /// Comment of the order.
    pub comment: Option<String>,
    /// Identifier the client gave the order.
    pub client_order_id: Option<String>,
    /// Tells whether the order closes a position or a part of one.
    pub closing: bool,
    /// Tells whether the stop loss follows the price.
    pub trailing_stop_loss: bool,
    /// Moment the order was created.
    pub opened: Option<Timestamp>,
    /// Moment the order last changed.
    pub updated: Option<Timestamp>,
}

impl TryFrom<ProtoOaOrder> for Order {
    type Error = ValueError;

    fn try_from(order: ProtoOaOrder) -> Result<Self, Self::Error> {
        let data = order.trade_data;
        Ok(Self {
            id: OrderId::new(order.order_id)?,
            symbol: SymbolId::new(data.symbol_id)?,
            side: TradeSide::from_wire(data.trade_side)?,
            volume: Volume::from_wire(data.volume)?,
            kind: OrderType::from_wire(order.order_type)?,
            status: OrderStatus::from_wire(order.order_status)?,
            time_in_force: TimeInForce::from_wire(order.time_in_force, order.expiration_timestamp)?,
            limit_price: optional_price(order.limit_price)?,
            stop_price: optional_price(order.stop_price)?,
            stop_loss: optional_price(order.stop_loss)?,
            take_profit: optional_price(order.take_profit)?,
            relative_stop_loss: relative_price(order.relative_stop_loss)?,
            relative_take_profit: relative_price(order.relative_take_profit)?,
            slippage_points: order
                .slippage_in_points
                .map(|points| {
                    u32::try_from(points).map_err(|_| ValueError::OutOfRange { kind: SLIPPAGE })
                })
                .transpose()?,
            executed_volume: order.executed_volume.map(Volume::from_wire).transpose()?,
            execution_price: optional_price(order.execution_price)?,
            position: order
                .position_id
                .filter(|id| *id != 0)
                .map(PositionId::new)
                .transpose()?,
            label: data.label,
            comment: data.comment,
            client_order_id: order.client_order_id,
            closing: order.closing_order.unwrap_or(false),
            trailing_stop_loss: order.trailing_stop_loss.unwrap_or(false),
            opened: moment(data.open_timestamp)?,
            updated: moment(order.utc_last_update_timestamp)?,
        })
    }
}

/// Position as the server knows it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Position {
    /// Identifier of the position.
    pub id: PositionId,
    /// Symbol the position is in.
    pub symbol: SymbolId,
    /// Direction of the position.
    pub side: TradeSide,
    /// Volume of the position, in units of the symbol.
    pub volume: Volume,
    /// State of the position.
    pub status: PositionStatus,
    /// Price the position was opened at, averaged over its deals.
    pub price: Option<Price>,
    /// Stop loss as a price.
    pub stop_loss: Option<Price>,
    /// Take profit as a price.
    pub take_profit: Option<Price>,
    /// Swap charged to the position so far, in the deposit currency.
    pub swap: Decimal,
    /// Commission charged to the position so far, in the deposit currency.
    pub commission: Option<Decimal>,
    /// Margin the position holds, in the deposit currency.
    pub used_margin: Option<Decimal>,
    /// Label of the order that opened the position.
    pub label: Option<String>,
    /// Comment of the order that opened the position.
    pub comment: Option<String>,
    /// Tells whether the stop loss is guaranteed.
    pub guaranteed_stop_loss: bool,
    /// Tells whether the stop loss follows the price.
    pub trailing_stop_loss: bool,
    /// Moment the position was opened.
    pub opened: Option<Timestamp>,
    /// Moment the position last changed.
    pub updated: Option<Timestamp>,
}

impl TryFrom<ProtoOaPosition> for Position {
    type Error = ValueError;

    fn try_from(position: ProtoOaPosition) -> Result<Self, Self::Error> {
        let data = position.trade_data;
        let digits = position.money_digits;
        Ok(Self {
            id: PositionId::new(position.position_id)?,
            symbol: SymbolId::new(data.symbol_id)?,
            side: TradeSide::from_wire(data.trade_side)?,
            volume: Volume::from_wire(data.volume)?,
            status: PositionStatus::from_wire(position.position_status)?,
            price: optional_price(position.price)?,
            stop_loss: optional_price(position.stop_loss)?,
            take_profit: optional_price(position.take_profit)?,
            swap: money(position.swap, digits)?,
            commission: position
                .commission
                .map(|raw| money(raw, digits))
                .transpose()?,
            used_margin: position
                .used_margin
                .map(|raw| {
                    i64::try_from(raw)
                        .map_err(|_| ValueError::OutOfRange { kind: MONEY })
                        .and_then(|raw| money(raw, digits))
                })
                .transpose()?,
            label: data.label,
            comment: data.comment,
            guaranteed_stop_loss: position.guaranteed_stop_loss.unwrap_or(false),
            trailing_stop_loss: position.trailing_stop_loss.unwrap_or(false),
            opened: moment(data.open_timestamp)?,
            updated: moment(position.utc_last_update_timestamp)?,
        })
    }
}

/// What a deal that closes a position, or a part of one, did to the account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ClosingDetail {
    /// Price the closed volume was opened at.
    pub entry_price: Price,
    /// Profit before swap and commission, in the deposit currency; negative for a loss.
    pub gross_profit: Decimal,
    /// Swap of the closed volume, in the deposit currency.
    pub swap: Decimal,
    /// Commission of the closed volume, in the deposit currency.
    pub commission: Decimal,
    /// Balance of the account after the deal, in the deposit currency.
    pub balance: Decimal,
    /// Volume the deal closed.
    pub closed_volume: Option<Volume>,
}

impl TryFrom<ProtoOaClosePositionDetail> for ClosingDetail {
    type Error = ValueError;

    fn try_from(detail: ProtoOaClosePositionDetail) -> Result<Self, Self::Error> {
        let digits = detail.money_digits;
        Ok(Self {
            entry_price: double_price(detail.entry_price)?,
            gross_profit: money(detail.gross_profit, digits)?,
            swap: money(detail.swap, digits)?,
            commission: money(detail.commission, digits)?,
            balance: money(detail.balance, digits)?,
            closed_volume: detail.closed_volume.map(Volume::from_wire).transpose()?,
        })
    }
}

/// Execution of an order, in full or in part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Deal {
    /// Identifier of the deal.
    pub id: DealId,
    /// Order the deal executed.
    pub order: OrderId,
    /// Position the deal opened, added to or closed.
    pub position: PositionId,
    /// Symbol the deal traded.
    pub symbol: SymbolId,
    /// Direction of the deal.
    pub side: TradeSide,
    /// Volume the deal was sent with.
    pub volume: Volume,
    /// Volume the deal executed.
    pub filled_volume: Volume,
    /// State of the deal.
    pub status: DealStatus,
    /// Moment the deal was created.
    pub created: Timestamp,
    /// Moment the deal was executed.
    pub executed: Timestamp,
    /// Price the deal was executed at.
    pub execution_price: Option<Price>,
    /// Commission charged for the deal, in the deposit currency.
    pub commission: Option<Decimal>,
    /// What the deal did to the account, when it closed a position or a part of one.
    pub closing: Option<ClosingDetail>,
}

impl TryFrom<ProtoOaDeal> for Deal {
    type Error = ValueError;

    fn try_from(deal: ProtoOaDeal) -> Result<Self, Self::Error> {
        let digits = deal.money_digits;
        Ok(Self {
            id: DealId::new(deal.deal_id)?,
            order: OrderId::new(deal.order_id)?,
            position: PositionId::new(deal.position_id)?,
            symbol: SymbolId::new(deal.symbol_id)?,
            side: TradeSide::from_wire(deal.trade_side)?,
            volume: Volume::from_wire(deal.volume)?,
            filled_volume: Volume::from_wire(deal.filled_volume)?,
            status: DealStatus::from_wire(deal.deal_status)?,
            created: Timestamp::from_millis(deal.create_timestamp)?,
            executed: Timestamp::from_millis(deal.execution_timestamp)?,
            execution_price: optional_price(deal.execution_price)?,
            commission: deal.commission.map(|raw| money(raw, digits)).transpose()?,
            closing: deal
                .close_position_detail
                .map(ClosingDetail::try_from)
                .transpose()?,
        })
    }
}

/// One execution event: what happened, and the order, position and deal it happened to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Execution {
    /// Account the event belongs to.
    pub account: AccountId,
    /// What happened.
    pub kind: ExecutionType,
    /// Order the event is about, as it is after the event.
    pub order: Option<Order>,
    /// Position the event is about, as it is after the event.
    pub position: Option<Position>,
    /// Deal the event reports.
    pub deal: Option<Deal>,
    /// Reason the server gives when it refuses an order.
    pub error_code: Option<ErrorCode>,
    /// Tells whether the server acted by itself, as when a stop loss is hit, and not on a
    /// request of a client.
    pub server_event: bool,
}

impl TryFrom<ProtoOaExecutionEvent> for Execution {
    type Error = ValueError;

    fn try_from(event: ProtoOaExecutionEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            account: AccountId::new(event.ctid_trader_account_id)?,
            kind: ExecutionType::from_wire(event.execution_type)?,
            order: event.order.map(Order::try_from).transpose()?,
            position: event.position.map(Position::try_from).transpose()?,
            deal: event.deal.map(Deal::try_from).transpose()?,
            error_code: event.error_code.as_deref().map(ErrorCode::from_wire),
            server_event: event.is_server_event.unwrap_or(false),
        })
    }
}

impl From<ProtoOaOrderErrorEvent> for ServerError {
    fn from(event: ProtoOaOrderErrorEvent) -> Self {
        let error = Self::new(ErrorCode::from_wire(&event.error_code));
        let error = match event.description {
            Some(description) => error.with_description(description),
            None => error,
        };
        match AccountId::new(event.ctid_trader_account_id) {
            Ok(account) => error.with_account_id(account),
            Err(_) => error,
        }
    }
}

fn refusal(execution: &Execution) -> ServerError {
    let code = execution
        .error_code
        .clone()
        .unwrap_or(ErrorCode::UnknownError);
    let what = match execution.kind {
        ExecutionType::CancelRejected => "the request to cancel the order was refused",
        _ => "the order was refused",
    };
    ServerError::new(code)
        .with_description(what)
        .with_account_id(execution.account)
}

/// Reads the first message that answers a request that trades.
fn traded(message: &ProtoMessage) -> Result<Execution, ConnectorError> {
    if message.payload_type == ProtoOaOrderErrorEvent::PAYLOAD_TYPE {
        return Err(ProtoOaOrderErrorEvent::from_message(message)
            .map_or_else(|error| error, |event| ServerError::from(event).into()));
    }
    let execution = Execution::try_from(ProtoOaExecutionEvent::from_message(message)?)?;
    match execution.kind {
        ExecutionType::Rejected | ExecutionType::CancelRejected => Err(refusal(&execution).into()),
        _ => Ok(execution),
    }
}

fn checked_volume(volume: Volume, symbol: &Symbol) -> Result<i64, ValueError> {
    let value = volume.value();
    let smallest = Decimal::new(1, crate::common::decimals::VOLUME_SCALE);
    let minimum = symbol.min_volume.map_or(smallest, Volume::value);
    if value < minimum || value.is_zero() {
        return Err(ValueError::BelowMinimum {
            kind: VOLUME,
            value,
            minimum,
        });
    }
    if let Some(maximum) = symbol.max_volume.map(Volume::value) {
        if value > maximum {
            return Err(ValueError::AboveMaximum {
                kind: VOLUME,
                value,
                maximum,
            });
        }
    }
    if let Some(step) = symbol.step_volume.map(Volume::value) {
        let aligned = value.checked_rem(step).is_some_and(|rest| rest.is_zero());
        if !step.is_zero() && !aligned {
            return Err(ValueError::OffStep {
                kind: VOLUME,
                value,
                step,
            });
        }
    }
    volume.to_wire()
}

fn checked_digits(price: Price, kind: &'static str, symbol: &Symbol) -> Result<Price, ValueError> {
    let value = price.value();
    if value.normalize().scale() > symbol.digits {
        return Err(ValueError::TooFine {
            kind,
            value,
            digits: symbol.digits,
        });
    }
    Ok(price)
}

fn checked_distance(distance: Price, symbol: &Symbol) -> Result<i64, ValueError> {
    let distance = checked_digits(distance, DISTANCE, symbol)?;
    if distance.value().is_zero() {
        return Err(ValueError::BelowMinimum {
            kind: DISTANCE,
            value: distance.value(),
            minimum: Decimal::new(1, symbol.digits.min(PRICE_SCALE)),
        });
    }
    i64::try_from(distance.to_wire()?).map_err(|_| ValueError::Unrepresentable {
        kind: DISTANCE,
        value: distance.value(),
    })
}

fn checked_price(price: Price, kind: &'static str, symbol: &Symbol) -> Result<f64, ValueError> {
    let value = checked_digits(price, kind, symbol)?.value();
    if value.is_zero() {
        return Err(ValueError::BelowMinimum {
            kind,
            value,
            minimum: Decimal::new(1, symbol.digits.min(PRICE_SCALE)),
        });
    }
    value
        .to_f64()
        .ok_or(ValueError::Unrepresentable { kind, value })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Terms {
    Market,
    Limit(Price),
    Stop(Price),
    StopLimit { stop: Price, slippage: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Protection {
    At(Price),
    Away(Price),
}

impl Protection {
    fn price(self, market: bool, symbol: &Symbol) -> Result<Option<f64>, ValueError> {
        match self {
            Self::At(_) if market => Err(ValueError::Unsupported { kind: PROTECTION }),
            Self::At(price) => checked_price(price, PROTECTION, symbol).map(Some),
            Self::Away(_) => Ok(None),
        }
    }

    fn distance(self, symbol: &Symbol) -> Result<Option<i64>, ValueError> {
        match self {
            Self::At(_) => Ok(None),
            Self::Away(distance) => checked_distance(distance, symbol).map(Some),
        }
    }
}

/// Order to be placed.
///
/// An order at market is executed at once. A limit, a stop and a stop-limit order wait at
/// their price until the market reaches it, till they are cancelled or, when
/// [`good_till`](NewOrder::good_till) names a moment, till then.
///
/// Stop loss and take profit are given as prices or as distances from the price the order is
/// executed at: the stop loss lies that far below the price of a buy and above the price of a
/// sell, the take profit on the other side. The server takes only distances for an order at
/// market.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[must_use]
pub struct NewOrder {
    account: AccountId,
    symbol: SymbolId,
    side: TradeSide,
    volume: Volume,
    terms: Terms,
    expiry: Option<Timestamp>,
    label: Option<Label>,
    comment: Option<Comment>,
    stop_loss: Option<Protection>,
    take_profit: Option<Protection>,
}

impl NewOrder {
    const fn new(
        account: AccountId,
        symbol: SymbolId,
        side: TradeSide,
        volume: Volume,
        terms: Terms,
    ) -> Self {
        Self {
            account,
            symbol,
            side,
            volume,
            terms,
            expiry: None,
            label: None,
            comment: None,
            stop_loss: None,
            take_profit: None,
        }
    }

    /// Orders the volume, in units of the symbol, at the price of the market.
    pub const fn market(
        account: AccountId,
        symbol: SymbolId,
        side: TradeSide,
        volume: Volume,
    ) -> Self {
        Self::new(account, symbol, side, volume, Terms::Market)
    }

    /// Orders the volume, in units of the symbol, at the given price or a better one.
    pub const fn limit(
        account: AccountId,
        symbol: SymbolId,
        side: TradeSide,
        volume: Volume,
        price: Price,
    ) -> Self {
        Self::new(account, symbol, side, volume, Terms::Limit(price))
    }

    /// Orders the volume, in units of the symbol, at market once the given price is reached.
    pub const fn stop(
        account: AccountId,
        symbol: SymbolId,
        side: TradeSide,
        volume: Volume,
        price: Price,
    ) -> Self {
        Self::new(account, symbol, side, volume, Terms::Stop(price))
    }

    /// Orders the volume, in units of the symbol, once the given stop price is reached, at a
    /// price no further from it than the slippage, in points of the symbol.
    ///
    /// The server models the limit of a stop-limit order as that distance and takes no limit
    /// price for it.
    pub const fn stop_limit(
        account: AccountId,
        symbol: SymbolId,
        side: TradeSide,
        volume: Volume,
        stop: Price,
        slippage: u32,
    ) -> Self {
        Self::new(
            account,
            symbol,
            side,
            volume,
            Terms::StopLimit { stop, slippage },
        )
    }

    /// Keeps a pending order in force till the given moment and no longer.
    ///
    /// An order at market does not wait, so one with an expiry is refused before it is sent.
    pub const fn good_till(mut self, expiry: Timestamp) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// Gives the order a label, which the position it opens keeps.
    pub fn label(mut self, label: Label) -> Self {
        self.label = Some(label);
        self
    }

    /// Gives the order a comment, which the position it opens keeps.
    pub fn comment(mut self, comment: Comment) -> Self {
        self.comment = Some(comment);
        self
    }

    /// Sets a stop loss at the given price, in place of one set before.
    ///
    /// An order at market with a stop loss given as a price is refused before it is sent.
    pub const fn stop_loss(mut self, price: Price) -> Self {
        self.stop_loss = Some(Protection::At(price));
        self
    }

    /// Sets a take profit at the given price, in place of one set before.
    ///
    /// An order at market with a take profit given as a price is refused before it is sent.
    pub const fn take_profit(mut self, price: Price) -> Self {
        self.take_profit = Some(Protection::At(price));
        self
    }

    /// Sets a stop loss at the given distance from the price of execution, in place of one
    /// set before.
    pub const fn stop_loss_distance(mut self, distance: Price) -> Self {
        self.stop_loss = Some(Protection::Away(distance));
        self
    }

    /// Sets a take profit at the given distance from the price of execution, in place of one
    /// set before.
    pub const fn take_profit_distance(mut self, distance: Price) -> Self {
        self.take_profit = Some(Protection::Away(distance));
        self
    }

    /// Returns the account the order is placed on.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Returns the symbol the order trades.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Returns the direction of the order.
    #[must_use]
    pub const fn side(&self) -> TradeSide {
        self.side
    }

    /// Returns the volume of the order, in units of the symbol.
    #[must_use]
    pub const fn volume(&self) -> Volume {
        self.volume
    }

    /// Returns the kind of the order.
    #[must_use]
    pub const fn kind(&self) -> OrderType {
        match self.terms {
            Terms::Market => OrderType::Market,
            Terms::Limit(_) => OrderType::Limit,
            Terms::Stop(_) => OrderType::Stop,
            Terms::StopLimit { .. } => OrderType::StopLimit,
        }
    }

    fn to_wire(&self, symbol: &Symbol) -> Result<ProtoOaNewOrderReq, ValueError> {
        if symbol.id != self.symbol {
            return Err(ValueError::Mismatch { kind: SYMBOL });
        }
        let market = matches!(self.terms, Terms::Market);
        if market && self.expiry.is_some() {
            return Err(ValueError::Unsupported { kind: EXPIRY });
        }
        let (kind, limit_price, stop_price, slippage) = match self.terms {
            Terms::Market => (ProtoOaOrderType::Market, None, None, None),
            Terms::Limit(price) => (
                ProtoOaOrderType::Limit,
                Some(checked_price(price, PRICE, symbol)?),
                None,
                None,
            ),
            Terms::Stop(price) => (
                ProtoOaOrderType::Stop,
                None,
                Some(checked_price(price, PRICE, symbol)?),
                None,
            ),
            Terms::StopLimit { stop, slippage } => (
                ProtoOaOrderType::StopLimit,
                None,
                Some(checked_price(stop, PRICE, symbol)?),
                Some(
                    i32::try_from(slippage)
                        .map_err(|_| ValueError::OutOfRange { kind: SLIPPAGE })?,
                ),
            ),
        };
        let time_in_force = match (market, self.expiry) {
            (true, _) => None,
            (false, Some(_)) => Some(ProtoOaTimeInForce::GoodTillDate),
            (false, None) => Some(ProtoOaTimeInForce::GoodTillCancel),
        };
        let price = |protection: Option<Protection>| {
            protection.map_or(Ok(None), |protection| protection.price(market, symbol))
        };
        let distance = |protection: Option<Protection>| {
            protection.map_or(Ok(None), |protection| protection.distance(symbol))
        };
        Ok(ProtoOaNewOrderReq {
            ctid_trader_account_id: self.account.get(),
            symbol_id: self.symbol.get(),
            order_type: i32::from(kind),
            trade_side: i32::from(self.side.to_wire()),
            volume: checked_volume(self.volume, symbol)?,
            limit_price,
            stop_price,
            slippage_in_points: slippage,
            time_in_force: time_in_force.map(i32::from),
            expiration_timestamp: self.expiry.map(Timestamp::as_millis),
            label: self.label.as_ref().map(|label| label.as_str().to_owned()),
            comment: self
                .comment
                .as_ref()
                .map(|comment| comment.as_str().to_owned()),
            stop_loss: price(self.stop_loss)?,
            take_profit: price(self.take_profit)?,
            relative_stop_loss: distance(self.stop_loss)?,
            relative_take_profit: distance(self.take_profit)?,
            ..ProtoOaNewOrderReq::default()
        })
    }
}

impl OpenApiClient {
    pub(crate) async fn trade<R: Payload>(&self, request: &R) -> Result<Execution, ConnectorError> {
        let message = self
            .connection()
            .request_once(R::PAYLOAD_TYPE, request.encode_to_vec())
            .await?;
        traded(&message)
    }

    /// Places an order on an authorised account and returns the first execution event the
    /// server answers with.
    ///
    /// `symbol` holds the trading details of the symbol of the order, as
    /// [`OpenApiClient::symbol_details`] returns them. The order is checked against them and
    /// nothing is sent when its volume lies outside the smallest and largest volume of the
    /// symbol or off its step, when a price or a distance is finer than the digits of the
    /// symbol, or when the order has terms its kind does not take.
    ///
    /// The first event of an order the server takes is its acceptance, with
    /// [`ExecutionType::Accepted`]. An order at market is executed right after: the fill
    /// arrives as a further event, which is read from the events of the connection. A pending
    /// order stays in force and is listed among the orders of the account.
    ///
    /// The request is written at most once and never sent again by this library.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Value`] when the order does not fit the symbol or the answer
    /// carries a value out of range, [`ConnectorError::Server`] with the code the server gives
    /// when it refuses the order, be it with an error, an order error event or an execution
    /// event that reports the refusal, [`ConnectorError::OutcomeUnknown`] when the request was
    /// written and no answer arrived, in which case the order may or may not be in force, and
    /// the errors of
    /// [`Connection::request_once`](crate::open_api::connection::Connection::request_once)
    /// otherwise.
    pub async fn place_order(
        &self,
        order: &NewOrder,
        symbol: &Symbol,
    ) -> Result<Execution, ConnectorError> {
        let request = order.to_wire(symbol)?;
        self.trade(&request).await
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::{
        ClosingDetail, Comment, Deal, DealStatus, Execution, ExecutionType, Label,
        MAX_COMMENT_LENGTH, MAX_LABEL_LENGTH, Order, OrderStatus, OrderType, Position,
        PositionStatus, TimeInForce, TradeSide, double_price, money, relative_price, traded,
    };
    use crate::common::errors::{ConnectorError, ErrorCode, ValueError};
    use crate::common::time::Timestamp;
    use crate::open_api::messages::{
        ProtoOaClosePositionDetail, ProtoOaDeal, ProtoOaExecutionEvent, ProtoOaOrder,
        ProtoOaOrderErrorEvent, ProtoOaPosition, ProtoOaTradeData,
    };
    use crate::open_api::payload::Payload;

    fn out_of_range(kind: &'static str) -> ValueError {
        ValueError::OutOfRange { kind }
    }

    fn data() -> ProtoOaTradeData {
        ProtoOaTradeData {
            symbol_id: 7,
            volume: 250,
            trade_side: 2,
            open_timestamp: Some(1_700_000_000_000),
            label: Some("run".to_owned()),
            comment: Some("note".to_owned()),
            ..ProtoOaTradeData::default()
        }
    }

    fn order() -> ProtoOaOrder {
        ProtoOaOrder {
            order_id: 9,
            trade_data: data(),
            order_type: 6,
            order_status: 1,
            expiration_timestamp: Some(1_800_000_000_000),
            execution_price: Some(101.5),
            executed_volume: Some(100),
            utc_last_update_timestamp: Some(1_700_000_000_500),
            slippage_in_points: Some(30),
            closing_order: Some(true),
            limit_price: Some(100.123_45),
            stop_price: Some(99.5),
            stop_loss: Some(90.0),
            take_profit: Some(120.25),
            client_order_id: Some("mine".to_owned()),
            time_in_force: Some(1),
            position_id: Some(0),
            relative_stop_loss: Some(150_000),
            relative_take_profit: Some(300_000),
            trailing_stop_loss: Some(true),
            ..ProtoOaOrder::default()
        }
    }

    fn position() -> ProtoOaPosition {
        ProtoOaPosition {
            position_id: 5,
            trade_data: data(),
            position_status: 1,
            swap: -1_234,
            price: Some(100.5),
            stop_loss: Some(90.0),
            take_profit: Some(120.0),
            utc_last_update_timestamp: Some(1_700_000_000_900),
            commission: Some(-7),
            guaranteed_stop_loss: Some(true),
            used_margin: Some(2_500),
            money_digits: Some(3),
            trailing_stop_loss: Some(true),
            ..ProtoOaPosition::default()
        }
    }

    fn deal() -> ProtoOaDeal {
        ProtoOaDeal {
            deal_id: 3,
            order_id: 9,
            position_id: 5,
            volume: 250,
            filled_volume: 100,
            symbol_id: 7,
            create_timestamp: 1_700_000_000_000,
            execution_timestamp: 1_700_000_000_010,
            execution_price: Some(101.5),
            trade_side: 1,
            deal_status: 3,
            commission: Some(-12),
            close_position_detail: Some(ProtoOaClosePositionDetail {
                entry_price: 100.5,
                gross_profit: 1_000,
                swap: -10,
                commission: -24,
                balance: 1_000_966,
                closed_volume: Some(100),
                money_digits: None,
                ..ProtoOaClosePositionDetail::default()
            }),
            money_digits: Some(2),
            ..ProtoOaDeal::default()
        }
    }

    fn decimal(text: &str) -> Decimal {
        text.parse().expect("decimal")
    }

    #[test]
    fn texts_are_limited_in_characters() {
        let label = Label::new("é".repeat(MAX_LABEL_LENGTH)).expect("label");
        assert_eq!(label.as_str().chars().count(), MAX_LABEL_LENGTH);
        assert_eq!(Label::new("run-7").expect("label").to_string(), "run-7");
        assert_eq!(
            Label::new("x".repeat(MAX_LABEL_LENGTH + 1)),
            Err(ValueError::TooLong {
                kind: "label",
                length: 101,
                limit: 100,
            })
        );
        assert_eq!(
            Comment::new("y".repeat(MAX_COMMENT_LENGTH))
                .expect("comment")
                .as_str()
                .len(),
            MAX_COMMENT_LENGTH
        );
        assert_eq!(Comment::new("note").expect("comment").to_string(), "note");
        assert_eq!(
            Comment::new("y".repeat(MAX_COMMENT_LENGTH + 1)),
            Err(ValueError::TooLong {
                kind: "comment",
                length: 513,
                limit: 512,
            })
        );
    }

    #[test]
    fn rejections_of_an_order_read_as_sentences() {
        let one = Decimal::ONE;
        let cases = [
            (
                ValueError::TooLong {
                    kind: "label",
                    length: 101,
                    limit: 100,
                },
                "label has 101 characters, more than the 100 allowed",
            ),
            (
                ValueError::BelowMinimum {
                    kind: "volume",
                    value: Decimal::ZERO,
                    minimum: one,
                },
                "volume must be at least 1, got 0",
            ),
            (
                ValueError::AboveMaximum {
                    kind: "volume",
                    value: Decimal::TEN,
                    maximum: one,
                },
                "volume must be at most 1, got 10",
            ),
            (
                ValueError::OffStep {
                    kind: "volume",
                    value: decimal("1.5"),
                    step: one,
                },
                "volume must be a multiple of 1, got 1.5",
            ),
            (
                ValueError::TooFine {
                    kind: "price",
                    value: decimal("1.005"),
                    digits: 2,
                },
                "price must have at most 2 decimal places, got 1.005",
            ),
            (
                ValueError::Mismatch {
                    kind: "symbol of the order",
                },
                "symbol of the order does not match",
            ),
            (
                ValueError::Unsupported {
                    kind: "expiry of the order",
                },
                "expiry of the order is not supported here",
            ),
        ];
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
        }
    }

    #[test]
    fn enumerations_follow_the_wire_numbers() {
        assert_eq!(TradeSide::from_wire(1), Ok(TradeSide::Buy));
        assert_eq!(TradeSide::from_wire(2), Ok(TradeSide::Sell));
        assert_eq!(TradeSide::from_wire(3), Err(out_of_range("trade side")));
        assert_eq!(TradeSide::Buy.opposite(), TradeSide::Sell);
        assert_eq!(TradeSide::Sell.opposite(), TradeSide::Buy);
        assert_eq!(TradeSide::Buy.to_wire() as i32, 1);
        assert_eq!(TradeSide::Sell.to_wire() as i32, 2);
        let kinds = [
            OrderType::Market,
            OrderType::Limit,
            OrderType::Stop,
            OrderType::StopLossTakeProfit,
            OrderType::MarketRange,
            OrderType::StopLimit,
        ];
        for (raw, kind) in (1..).zip(kinds) {
            assert_eq!(OrderType::from_wire(raw), Ok(kind));
        }
        assert_eq!(OrderType::from_wire(0), Err(out_of_range("order type")));
        let statuses = [
            OrderStatus::Accepted,
            OrderStatus::Filled,
            OrderStatus::Rejected,
            OrderStatus::Expired,
            OrderStatus::Cancelled,
        ];
        for (raw, status) in (1..).zip(statuses) {
            assert_eq!(OrderStatus::from_wire(raw), Ok(status));
        }
        assert_eq!(OrderStatus::from_wire(6), Err(out_of_range("order status")));
        let positions = [
            PositionStatus::Open,
            PositionStatus::Closed,
            PositionStatus::Created,
            PositionStatus::Error,
        ];
        for (raw, status) in (1..).zip(positions) {
            assert_eq!(PositionStatus::from_wire(raw), Ok(status));
        }
        assert_eq!(
            PositionStatus::from_wire(5),
            Err(out_of_range("position status"))
        );
        let deals = [
            DealStatus::Filled,
            DealStatus::PartiallyFilled,
            DealStatus::Rejected,
            DealStatus::InternallyRejected,
            DealStatus::Error,
            DealStatus::Missed,
        ];
        for (raw, status) in (2..).zip(deals) {
            assert_eq!(DealStatus::from_wire(raw), Ok(status));
        }
        assert_eq!(DealStatus::from_wire(1), Err(out_of_range("deal status")));
        let executions = [
            ExecutionType::Accepted,
            ExecutionType::Filled,
            ExecutionType::Replaced,
            ExecutionType::Cancelled,
            ExecutionType::Expired,
            ExecutionType::Rejected,
            ExecutionType::CancelRejected,
            ExecutionType::Swap,
            ExecutionType::DepositWithdraw,
            ExecutionType::PartiallyFilled,
            ExecutionType::BonusDepositWithdraw,
        ];
        for (raw, kind) in (2..).zip(executions) {
            assert_eq!(ExecutionType::from_wire(raw), Ok(kind));
        }
        assert_eq!(
            ExecutionType::from_wire(13),
            Err(out_of_range("execution type"))
        );
    }

    #[test]
    fn time_in_force_takes_its_moment_from_the_expiry() {
        let expiry = Timestamp::from_millis(1_800_000_000_000).expect("timestamp");
        assert_eq!(
            TimeInForce::from_wire(Some(1), Some(1_800_000_000_000)),
            Ok(TimeInForce::GoodTillDate(expiry))
        );
        assert_eq!(
            TimeInForce::from_wire(Some(1), None),
            Err(ValueError::Missing {
                kind: "expiry of the order"
            })
        );
        assert_eq!(
            TimeInForce::from_wire(Some(1), Some(-1)),
            Err(out_of_range("timestamp"))
        );
        assert_eq!(
            TimeInForce::from_wire(Some(2), Some(5)),
            Ok(TimeInForce::GoodTillCancel)
        );
        assert_eq!(
            TimeInForce::from_wire(Some(3), None),
            Ok(TimeInForce::ImmediateOrCancel)
        );
        assert_eq!(
            TimeInForce::from_wire(None, None),
            Ok(TimeInForce::ImmediateOrCancel)
        );
        assert_eq!(
            TimeInForce::from_wire(Some(4), None),
            Ok(TimeInForce::FillOrKill)
        );
        assert_eq!(
            TimeInForce::from_wire(Some(5), None),
            Ok(TimeInForce::MarketOnOpen)
        );
        assert_eq!(
            TimeInForce::from_wire(Some(6), None),
            Err(out_of_range("time in force"))
        );
    }

    #[test]
    fn doubles_and_scaled_integers_become_exact_decimals() {
        assert_eq!(
            double_price(64_000.25).map(|price| price.to_string()),
            Ok("64000.25".to_owned())
        );
        assert_eq!(
            double_price(1.234_564_9).map(|price| price.to_string()),
            Ok("1.23456".to_owned())
        );
        assert_eq!(double_price(f64::NAN), Err(out_of_range("price")));
        assert_eq!(double_price(f64::INFINITY), Err(out_of_range("price")));
        assert!(matches!(
            double_price(-1.0),
            Err(ValueError::Negative { .. })
        ));
        assert_eq!(money(-12_345, Some(3)), Ok(decimal("-12.345")));
        assert_eq!(money(12_345, None), Ok(decimal("123.45")));
        assert_eq!(money(1, Some(40)), Err(out_of_range("amount of money")));
        assert_eq!(
            relative_price(Some(150_000)).map(|price| price.map(|price| price.to_string())),
            Ok(Some("1.50000".to_owned()))
        );
        assert_eq!(relative_price(None), Ok(None));
        assert_eq!(
            relative_price(Some(-1)),
            Err(out_of_range("distance of the protection"))
        );
    }

    #[test]
    fn order_keeps_every_field_of_the_wire() {
        let order = Order::try_from(order()).expect("order");
        assert_eq!(order.id.get(), 9);
        assert_eq!(order.symbol.get(), 7);
        assert_eq!(order.side, TradeSide::Sell);
        assert_eq!(order.volume.to_string(), "2.50");
        assert_eq!(order.kind, OrderType::StopLimit);
        assert_eq!(order.status, OrderStatus::Accepted);
        assert_eq!(
            order.time_in_force,
            TimeInForce::GoodTillDate(Timestamp::from_millis(1_800_000_000_000).expect("moment"))
        );
        let text = |price: Option<crate::common::decimals::Price>| {
            price.map(|price| price.value().normalize().to_string())
        };
        assert_eq!(text(order.limit_price), Some("100.12345".to_owned()));
        assert_eq!(text(order.stop_price), Some("99.5".to_owned()));
        assert_eq!(text(order.stop_loss), Some("90".to_owned()));
        assert_eq!(text(order.take_profit), Some("120.25".to_owned()));
        assert_eq!(text(order.relative_stop_loss), Some("1.5".to_owned()));
        assert_eq!(text(order.relative_take_profit), Some("3".to_owned()));
        assert_eq!(text(order.execution_price), Some("101.5".to_owned()));
        assert_eq!(order.slippage_points, Some(30));
        assert_eq!(
            order.executed_volume.map(|volume| volume.to_string()),
            Some("1.00".to_owned())
        );
        assert_eq!(order.position, None);
        assert_eq!(order.label.as_deref(), Some("run"));
        assert_eq!(order.comment.as_deref(), Some("note"));
        assert_eq!(order.client_order_id.as_deref(), Some("mine"));
        assert!(order.closing);
        assert!(order.trailing_stop_loss);
        assert_eq!(
            order.opened.map(Timestamp::as_millis),
            Some(1_700_000_000_000)
        );
        assert_eq!(
            order.updated.map(Timestamp::as_millis),
            Some(1_700_000_000_500)
        );
    }

    #[test]
    fn order_with_a_value_out_of_range_is_refused() {
        let broken = |change: fn(&mut ProtoOaOrder)| {
            let mut order = order();
            change(&mut order);
            Order::try_from(order).err()
        };
        assert!(matches!(
            broken(|order| order.order_id = 0),
            Some(ValueError::InvalidId { .. })
        ));
        assert!(matches!(
            broken(|order| order.trade_data.symbol_id = 0),
            Some(ValueError::InvalidId { .. })
        ));
        assert_eq!(
            broken(|order| order.trade_data.trade_side = 0),
            Some(out_of_range("trade side"))
        );
        assert!(broken(|order| order.trade_data.volume = -1).is_some());
        assert_eq!(
            broken(|order| order.order_type = 0),
            Some(out_of_range("order type"))
        );
        assert_eq!(
            broken(|order| order.order_status = 0),
            Some(out_of_range("order status"))
        );
        assert_eq!(
            broken(|order| order.time_in_force = Some(0)),
            Some(out_of_range("time in force"))
        );
        assert_eq!(
            broken(|order| order.limit_price = Some(f64::NAN)),
            Some(out_of_range("price"))
        );
        assert_eq!(
            broken(|order| order.slippage_in_points = Some(-1)),
            Some(out_of_range("slippage in points"))
        );
        assert!(matches!(
            broken(|order| order.position_id = Some(-4)),
            Some(ValueError::InvalidId { .. })
        ));
        assert_eq!(
            broken(|order| order.utc_last_update_timestamp = Some(-1)),
            Some(out_of_range("timestamp"))
        );
    }

    #[test]
    fn position_scales_its_money_by_its_own_digits() {
        let position = Position::try_from(position()).expect("position");
        assert_eq!(position.id.get(), 5);
        assert_eq!(position.symbol.get(), 7);
        assert_eq!(position.side, TradeSide::Sell);
        assert_eq!(position.volume.to_string(), "2.50");
        assert_eq!(position.status, PositionStatus::Open);
        assert_eq!(
            position.price.map(|price| price.value().normalize()),
            Some(decimal("100.5"))
        );
        assert_eq!(
            position.stop_loss.map(|price| price.value().normalize()),
            Some(decimal("90"))
        );
        assert_eq!(
            position.take_profit.map(|price| price.value().normalize()),
            Some(decimal("120"))
        );
        assert_eq!(position.swap, decimal("-1.234"));
        assert_eq!(position.commission, Some(decimal("-0.007")));
        assert_eq!(position.used_margin, Some(decimal("2.500")));
        assert_eq!(position.label.as_deref(), Some("run"));
        assert_eq!(position.comment.as_deref(), Some("note"));
        assert!(position.guaranteed_stop_loss);
        assert!(position.trailing_stop_loss);
        assert_eq!(
            position.opened.map(Timestamp::as_millis),
            Some(1_700_000_000_000)
        );
        assert_eq!(
            position.updated.map(Timestamp::as_millis),
            Some(1_700_000_000_900)
        );
        let mut huge = self::position();
        huge.used_margin = Some(u64::MAX);
        assert_eq!(
            Position::try_from(huge),
            Err(out_of_range("amount of money"))
        );
        let mut unknown = self::position();
        unknown.position_status = 9;
        assert_eq!(
            Position::try_from(unknown),
            Err(out_of_range("position status"))
        );
    }

    #[test]
    fn deal_carries_what_closing_did_to_the_account() {
        let deal = Deal::try_from(deal()).expect("deal");
        assert_eq!(deal.id.get(), 3);
        assert_eq!(deal.order.get(), 9);
        assert_eq!(deal.position.get(), 5);
        assert_eq!(deal.symbol.get(), 7);
        assert_eq!(deal.side, TradeSide::Buy);
        assert_eq!(deal.volume.to_string(), "2.50");
        assert_eq!(deal.filled_volume.to_string(), "1.00");
        assert_eq!(deal.status, DealStatus::PartiallyFilled);
        assert_eq!(deal.created.as_millis(), 1_700_000_000_000);
        assert_eq!(deal.executed.as_millis(), 1_700_000_000_010);
        assert_eq!(
            deal.execution_price.map(|price| price.value().normalize()),
            Some(decimal("101.5"))
        );
        assert_eq!(deal.commission, Some(decimal("-0.12")));
        let closing: ClosingDetail = deal.closing.expect("closing detail");
        assert_eq!(closing.entry_price.value().normalize(), decimal("100.5"));
        assert_eq!(closing.gross_profit, decimal("10.00"));
        assert_eq!(closing.swap, decimal("-0.10"));
        assert_eq!(closing.commission, decimal("-0.24"));
        assert_eq!(closing.balance, decimal("10009.66"));
        assert_eq!(
            closing.closed_volume.map(|volume| volume.to_string()),
            Some("1.00".to_owned())
        );
        let mut unknown = self::deal();
        unknown.deal_status = 1;
        assert_eq!(Deal::try_from(unknown), Err(out_of_range("deal status")));
    }

    #[test]
    fn first_answer_to_a_trade_is_an_execution_or_a_typed_refusal() {
        let event = ProtoOaExecutionEvent {
            ctid_trader_account_id: 101,
            execution_type: 2,
            order: Some(order()),
            position: Some(position()),
            deal: Some(deal()),
            is_server_event: Some(true),
            ..ProtoOaExecutionEvent::default()
        };
        let execution = traded(&event.to_message(None)).expect("execution");
        assert_eq!(execution.kind, ExecutionType::Accepted);
        assert!(execution.server_event);
        assert!(execution.order.is_some() && execution.position.is_some());
        assert!(execution.deal.is_some());
        assert_eq!(
            Execution::try_from(ProtoOaExecutionEvent {
                ctid_trader_account_id: 0,
                ..event.clone()
            })
            .err()
            .map(|error| error.to_string()),
            Some("account id must be a positive integer, got 0".to_owned())
        );
        let rejected = ProtoOaExecutionEvent {
            execution_type: 7,
            error_code: Some("MARKET_CLOSED".to_owned()),
            ..event
        };
        match traded(&rejected.to_message(None)) {
            Err(ConnectorError::Server(error)) => {
                assert_eq!(error.code, ErrorCode::MarketClosed);
                assert_eq!(error.description.as_deref(), Some("the order was refused"));
                assert_eq!(
                    error.account_id.map(crate::common::ids::AccountId::get),
                    Some(101)
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
        let error = ProtoOaOrderErrorEvent {
            payload_type: None,
            ctid_trader_account_id: 101,
            error_code: "ORDER_NOT_FOUND".to_owned(),
            order_id: Some(9),
            position_id: None,
            description: Some("no such order".to_owned()),
        };
        match traded(&error.to_message(None)) {
            Err(ConnectorError::Server(error)) => {
                assert_eq!(error.code, ErrorCode::OrderNotFound);
                assert_eq!(error.description.as_deref(), Some("no such order"));
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
}
