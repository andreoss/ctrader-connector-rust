//! Changes to waiting orders of the trade session: cancelling and replacing.
//!
//! Both requests name the order by the identifier the client gave it. The report of a
//! replacement carries the identifier of the replacement request, but the order goes on
//! answering to the identifier it was placed under.
//!
//! The server refuses a change with an order cancel reject or, for an order it does not
//! know, with a business message reject; both are reported as
//! [`FixError::CancelRejected`].

use crate::common::decimals::{Price, Volume};
use crate::common::errors::{ConnectorError, FixError, RefusedChange};
use crate::common::time::Timestamp;
use crate::fix::client::FixClient;
use crate::fix::messages::{
    CancelRejectResponse, ExecType, Message, OrderCancelReplaceRequest, OrderCancelRequest,
};
use crate::fix::trading::{OrderReport, refused};

const CANCEL_PREFIX: &str = "c";
const REPLACE_PREFIX: &str = "r";

/// New terms of a waiting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replacement {
    quantity: Volume,
    price: Option<Price>,
    stop_price: Option<Price>,
    expiry: Option<Timestamp>,
}

impl Replacement {
    /// Starts the new terms with the quantity, which every replacement states.
    #[must_use]
    pub const fn new(quantity: Volume) -> Self {
        Self {
            quantity,
            price: None,
            stop_price: None,
            expiry: None,
        }
    }

    /// Sets the limit price of a limit order.
    #[must_use]
    pub const fn price(mut self, price: Price) -> Self {
        self.price = Some(price);
        self
    }

    /// Sets the stop price of a stop order.
    #[must_use]
    pub const fn stop_price(mut self, price: Price) -> Self {
        self.stop_price = Some(price);
        self
    }

    /// Sets the moment the order expires.
    #[must_use]
    pub const fn expires(mut self, at: Timestamp) -> Self {
        self.expiry = Some(at);
        self
    }
}

const fn change(response_to: CancelRejectResponse) -> RefusedChange {
    match response_to {
        CancelRejectResponse::Cancel => RefusedChange::Cancel,
        CancelRejectResponse::Replace => RefusedChange::Replace,
    }
}

fn unchanged(
    error: ConnectorError,
    response_to: CancelRejectResponse,
    cl_ord_id: &str,
    orig_cl_ord_id: &str,
) -> ConnectorError {
    match error {
        ConnectorError::Fix(FixError::BusinessRejected { text, .. }) => FixError::CancelRejected {
            cl_ord_id: cl_ord_id.to_owned(),
            orig_cl_ord_id: Some(orig_cl_ord_id.to_owned()),
            change: change(response_to),
            text,
        }
        .into(),
        other => other,
    }
}

impl FixClient {
    async fn amend(
        &self,
        request: Message,
        id: &str,
        orig_cl_ord_id: &str,
        response_to: CancelRejectResponse,
    ) -> Result<OrderReport, ConnectorError> {
        let wanted = match response_to {
            CancelRejectResponse::Cancel => ExecType::Canceled,
            CancelRejectResponse::Replace => ExecType::Replaced,
        };
        let pick = |message: &Message| match message {
            Message::ExecutionReport(report) if report.cl_ord_id.as_deref() == Some(id) => {
                if report.exec_type == wanted {
                    Some(OrderReport::try_from(report.as_ref()).map_err(ConnectorError::from))
                } else if report.exec_type == ExecType::Rejected {
                    Some(Err(refused(report)))
                } else {
                    None
                }
            }
            Message::OrderCancelReject(reject) if reject.cl_ord_id == id => {
                Some(Err(FixError::CancelRejected {
                    cl_ord_id: reject.cl_ord_id.clone(),
                    orig_cl_ord_id: reject.orig_cl_ord_id.clone(),
                    change: change(reject.response_to),
                    text: reject.text.clone(),
                }
                .into()))
            }
            _ => None,
        };
        let outcome = match response_to {
            CancelRejectResponse::Cancel => self.request(request, id, pick).await,
            CancelRejectResponse::Replace => self.request_once(request, id, pick).await,
        };
        outcome.map_err(|error| unchanged(error, response_to, id, orig_cl_ord_id))
    }

    /// Cancels the waiting order the client gave the identifier `cl_ord_id` and returns the
    /// report of the cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] with [`FixError::CancelRejected`] when the server
    /// refuses, among others because it knows no such order, and otherwise the errors of
    /// [`FixClient::request`]. A cancellation may be repeated after a
    /// [`ConnectorError::Timeout`].
    pub async fn cancel(&self, cl_ord_id: &str) -> Result<OrderReport, ConnectorError> {
        let id = self.next_id(CANCEL_PREFIX);
        let request = OrderCancelRequest {
            orig_cl_ord_id: cl_ord_id.to_owned(),
            order_id: None,
            cl_ord_id: id.clone(),
        };
        self.amend(request.into(), &id, cl_ord_id, CancelRejectResponse::Cancel)
            .await
    }

    /// Replaces the terms of the waiting order the client gave the identifier `cl_ord_id`
    /// and returns the report of the order as replaced. The report carries the identifier of
    /// the request; the order goes on answering to the identifier it was placed under.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] with [`FixError::CancelRejected`] when the server
    /// refuses, among others because it knows no such order,
    /// [`ConnectorError::OutcomeUnknown`] when the answer does not arrive in time or the
    /// session is lost first, and otherwise the errors of [`FixClient::request_once`].
    pub async fn replace(
        &self,
        cl_ord_id: &str,
        terms: Replacement,
    ) -> Result<OrderReport, ConnectorError> {
        let id = self.next_id(REPLACE_PREFIX);
        let request = OrderCancelReplaceRequest {
            orig_cl_ord_id: cl_ord_id.to_owned(),
            order_id: None,
            cl_ord_id: id.clone(),
            order_qty: terms.quantity,
            price: terms.price,
            stop_px: terms.stop_price,
            expire_time: terms.expiry,
        };
        self.amend(
            request.into(),
            &id,
            cl_ord_id,
            CancelRejectResponse::Replace,
        )
        .await
    }
}
