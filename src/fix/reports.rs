//! State of the account as the trade session reports it: orders and positions.

use crate::common::decimals::{Price, Volume};
use crate::common::errors::{ConnectorError, FixError};
use crate::common::ids::{PositionId, SymbolId};
use crate::fix::client::FixClient;
use crate::fix::messages::{
    ExecType, Message, OrderMassStatusRequest, OrderStatusRequest, PositionReport,
    RequestForPositions, Side,
};
use crate::fix::trading::OrderReport;

const MASS_PREFIX: &str = "m";
const POSITIONS_PREFIX: &str = "p";
const ALL_ORDERS: u32 = 7;
const NO_ORDERS: &str = "ORDER_NOT_FOUND";
const VALID_REQUEST: u32 = 0;
const NO_POSITIONS: u32 = 2;
const TAG_POSITION_ID: u32 = 721;
const TAG_SYMBOL: u32 = 55;
const TAG_LONG_QTY: u32 = 704;

/// An open position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    /// Identifier of the position, the same the other API of the server uses.
    pub id: PositionId,
    /// Symbol of the position.
    pub symbol: SymbolId,
    /// Side of the position: bought or sold.
    pub side: Side,
    /// Quantity held, in units of the base asset.
    pub quantity: Volume,
    /// Price the position was opened at.
    pub entry_price: Option<Price>,
}

impl TryFrom<&PositionReport> for Position {
    type Error = FixError;

    fn try_from(report: &PositionReport) -> Result<Self, FixError> {
        let missing = |tag| FixError::MissingField {
            msg_type: PositionReport::MSG_TYPE.to_owned(),
            tag,
        };
        let held = |quantity: Option<Volume>| quantity.filter(|held| !held.value().is_zero());
        let (side, quantity) = match (held(report.long_qty), held(report.short_qty)) {
            (Some(long), _) => (Side::Buy, long),
            (None, Some(short)) => (Side::Sell, short),
            (None, None) => return Err(missing(TAG_LONG_QTY)),
        };
        Ok(Self {
            id: report.position_id.ok_or_else(|| missing(TAG_POSITION_ID))?,
            symbol: report.symbol.ok_or_else(|| missing(TAG_SYMBOL))?,
            side,
            quantity,
            entry_price: report.settl_price,
        })
    }
}

fn complete<T>(collected: &[T], total: u32) -> bool {
    u32::try_from(collected.len()).is_ok_and(|count| count >= total)
}

impl FixClient {
    /// Asks for the state of the order the client gave the identifier `cl_ord_id`.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] with [`FixError::BusinessRejected`] when the server
    /// knows no such order, and otherwise the errors of [`FixClient::request`].
    pub async fn order_status(&self, cl_ord_id: &str) -> Result<OrderReport, ConnectorError> {
        let request = OrderStatusRequest {
            cl_ord_id: cl_ord_id.to_owned(),
            side: None,
        };
        self.request(request.into(), cl_ord_id, |message| match message {
            Message::ExecutionReport(report) if report.cl_ord_id.as_deref() == Some(cl_ord_id) => {
                Some(OrderReport::try_from(report.as_ref()).map_err(ConnectorError::from))
            }
            _ => None,
        })
        .await
    }

    /// Lists the waiting orders of the account.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`FixClient::request`]; when a report cannot be read or the
    /// list stays incomplete no order is returned.
    pub async fn open_orders(&self) -> Result<Vec<OrderReport>, ConnectorError> {
        let id = self.next_id(MASS_PREFIX);
        let request = OrderMassStatusRequest {
            mass_status_req_id: id.clone(),
            request_type: ALL_ORDERS,
            issue_date: None,
        };
        let mut orders = Vec::new();
        let listed = self
            .request(request.into(), &id, |message| match message {
                Message::ExecutionReport(report)
                    if report.exec_type == ExecType::OrderStatus
                        && report.mass_status_req_id.as_deref() == Some(&id) =>
                {
                    match OrderReport::try_from(report.as_ref()) {
                        Ok(order) => orders.push(order),
                        Err(error) => return Some(Err(error.into())),
                    }
                    complete(&orders, report.tot_num_reports.unwrap_or(1)).then_some(Ok(()))
                }
                _ => None,
            })
            .await;
        match listed {
            Ok(()) => Ok(orders),
            Err(ConnectorError::Fix(FixError::BusinessRejected {
                text: Some(text), ..
            })) if text.starts_with(NO_ORDERS) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// Lists the open positions of the account.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] with [`FixError::BusinessRejected`] when the server
    /// reports the request as invalid, and otherwise the errors of [`FixClient::request`];
    /// when a report cannot be read or the list stays incomplete no position is returned.
    pub async fn positions(&self) -> Result<Vec<Position>, ConnectorError> {
        let id = self.next_id(POSITIONS_PREFIX);
        let request = RequestForPositions {
            pos_req_id: id.clone(),
            position_id: None,
        };
        let mut positions = Vec::new();
        self.request(request.into(), &id, |message| match message {
            Message::PositionReport(report) if report.pos_req_id == id => {
                if report.result == NO_POSITIONS || report.total_reports == 0 {
                    return Some(Ok(()));
                }
                if report.result != VALID_REQUEST {
                    return Some(Err(FixError::BusinessRejected {
                        ref_id: Some(id.clone()),
                        reason: Some(report.result),
                        text: None,
                    }
                    .into()));
                }
                match Position::try_from(report) {
                    Ok(position) => positions.push(position),
                    Err(error) => return Some(Err(error.into())),
                }
                complete(&positions, report.total_reports).then_some(Ok(()))
            }
            _ => None,
        })
        .await?;
        Ok(positions)
    }
}
