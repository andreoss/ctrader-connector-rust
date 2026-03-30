//! Deals and orders of an account inside a range of time.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::common::time::{TimeRange, Timestamp};
//! use ctrader_connector::open_api::client::OpenApiClient;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let day = TimeRange::ending_at(Timestamp::now(), Duration::from_secs(86_400));
//! for deal in client.deal_history(account, day).await? {
//!     println!("{} {:?} {}", deal.id, deal.side, deal.filled_volume);
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::AccountId;
use crate::common::time::{TimeRange, Timestamp};
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{ProtoOaDealListReq, ProtoOaOrderListReq};
use crate::open_api::trading::{Deal, Order};

const MAX_RANGE_MILLIS: i64 = 604_800_000;

const HISTORY_TIMESTAMP: &str = "end of the range of history";

/// Longest range of time the server accepts in one request for deals, orders or cash flow.
pub const MAX_HISTORY_RANGE: Duration = Duration::from_millis(MAX_RANGE_MILLIS.unsigned_abs());

/// Parts of a range of time still to be asked for, each short enough for one request.
///
/// The server includes both ends of a request and refuses one whose ends are the same
/// millisecond, so a part that is a single millisecond is asked for together with the
/// millisecond before it, or after it at the Unix epoch.
#[derive(Debug)]
pub(crate) struct Windows {
    pending: Vec<(i64, i64)>,
}

impl Windows {
    /// Cuts the range into parts of at most [`MAX_HISTORY_RANGE`], latest first.
    pub(crate) fn new(range: TimeRange) -> Result<Self, ValueError> {
        let (from, to) = (range.from().as_millis(), range.to().as_millis());
        let mut pending = Vec::new();
        let mut start = from;
        loop {
            let end = start.saturating_add(MAX_RANGE_MILLIS).min(to);
            pending.push(widened(start, end)?);
            if end >= to {
                break;
            }
            start = end.saturating_add(1);
        }
        Ok(Self { pending })
    }

    /// Returns the next part to ask for, as the two timestamps of the request.
    pub(crate) fn next(&mut self) -> Option<(i64, i64)> {
        self.pending.pop()
    }

    /// Replaces a part the server did not answer whole by its two halves, which share their
    /// middle millisecond.
    ///
    /// Fails with [`ConnectorError::Incomplete`] when the part is too short to be halved.
    pub(crate) fn split(&mut self, (from, to): (i64, i64)) -> Result<(), ConnectorError> {
        let middle = to
            .checked_sub(from)
            .map(|length| length / 2)
            .and_then(|half| from.checked_add(half))
            .filter(|middle| from < *middle && *middle < to);
        let Some(middle) = middle else {
            return Err(ConnectorError::Incomplete {
                from: Timestamp::from_millis(from)?,
                to: Timestamp::from_millis(to)?,
            });
        };
        self.pending.push((from, middle));
        self.pending.push((middle, to));
        Ok(())
    }
}

/// Tells whether a moment of last update, when the server states one, lies inside the range.
fn inside(range: TimeRange, updated: Option<i64>) -> bool {
    updated.is_none_or(|at| range.from().as_millis() <= at && at <= range.to().as_millis())
}

fn widened(start: i64, end: i64) -> Result<(i64, i64), ValueError> {
    if start < end {
        return Ok((start, end));
    }
    match start.checked_sub(1).filter(|before| *before >= 0) {
        Some(before) => Ok((before, end)),
        None => end
            .checked_add(1)
            .map(|after| (start, after))
            .ok_or(ValueError::OutOfRange {
                kind: HISTORY_TIMESTAMP,
            }),
    }
}

impl OpenApiClient {
    /// Fetches the deals of an authorised account inside a range of time, oldest first.
    ///
    /// The server selects a deal by the moment it was last updated, which for a deal is the
    /// moment it was created, and includes both ends of the range. The server refuses a request
    /// whose ends are the same, so a single millisecond is asked for together with the one
    /// before it, and what belongs to that other millisecond is left out.
    ///
    /// A range longer than [`MAX_HISTORY_RANGE`] is fetched with several requests, each of
    /// which counts against the limit for historical data. The server sends a limited number
    /// of deals per request; a part of the range it reports to hold more is halved and asked
    /// for again, until every part is answered whole. Every deal of the range is thus
    /// returned, and returned once.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a request,
    /// [`ConnectorError::Value`] when a deal carries a value out of range,
    /// [`ConnectorError::Incomplete`] when the server reports more deals than it sends for a
    /// part too short to be halved, in which case no part of the range is returned, and the
    /// errors of [`Connection::request`](crate::open_api::connection::Connection::request)
    /// otherwise.
    pub async fn deal_history(
        &self,
        account: AccountId,
        range: TimeRange,
    ) -> Result<Vec<Deal>, ConnectorError> {
        let mut windows = Windows::new(range)?;
        let mut deals = BTreeMap::new();
        while let Some((from, to)) = windows.next() {
            let request = ProtoOaDealListReq {
                payload_type: None,
                ctid_trader_account_id: account.get(),
                from_timestamp: from,
                to_timestamp: to,
                max_rows: None,
            };
            let response = self.connection().request(&request).await?;
            if response.has_more {
                windows.split((from, to))?;
                continue;
            }
            for deal in response.deal {
                let updated = deal.utc_last_update_timestamp;
                let deal = Deal::try_from(deal)?;
                if inside(range, updated) {
                    deals.insert(deal.id, deal);
                }
            }
        }
        let mut deals: Vec<Deal> = deals.into_values().collect();
        deals.sort_by_key(|deal| (deal.executed, deal.id));
        Ok(deals)
    }

    /// Fetches the orders of an authorised account that were last updated inside a range of
    /// time, in the order of their last update, oldest first.
    ///
    /// The orders are the ones in force and the ones that are filled, cancelled, expired or
    /// refused. Both ends of the range are included, and the range is cut into requests and
    /// answered whole as described for [`OpenApiClient::deal_history`].
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::deal_history`].
    pub async fn order_history(
        &self,
        account: AccountId,
        range: TimeRange,
    ) -> Result<Vec<Order>, ConnectorError> {
        let mut windows = Windows::new(range)?;
        let mut orders = BTreeMap::new();
        while let Some((from, to)) = windows.next() {
            let request = ProtoOaOrderListReq {
                payload_type: None,
                ctid_trader_account_id: account.get(),
                from_timestamp: from,
                to_timestamp: to,
            };
            let response = self.connection().request(&request).await?;
            if response.has_more {
                windows.split((from, to))?;
                continue;
            }
            for order in response.order {
                let updated = order.utc_last_update_timestamp;
                let order = Order::try_from(order)?;
                if inside(range, updated) {
                    orders.insert(order.id, order);
                }
            }
        }
        let mut orders: Vec<Order> = orders.into_values().collect();
        orders.sort_by_key(|order| (order.updated, order.id));
        Ok(orders)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{MAX_HISTORY_RANGE, MAX_RANGE_MILLIS, Windows, widened};
    use crate::common::errors::ConnectorError;
    use crate::common::time::{TimeRange, Timestamp};

    fn range(from: i64, to: i64) -> TimeRange {
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        TimeRange::new(at(from), at(to)).expect("range")
    }

    fn all(mut windows: Windows) -> Vec<(i64, i64)> {
        std::iter::from_fn(|| windows.next()).collect()
    }

    #[test]
    fn range_is_cut_into_parts_the_server_accepts_latest_first() {
        assert_eq!(MAX_HISTORY_RANGE, Duration::from_secs(7 * 86_400));
        let week = MAX_RANGE_MILLIS;
        assert_eq!(
            all(Windows::new(range(10, 20)).expect("windows")),
            [(10, 20)]
        );
        assert_eq!(
            all(Windows::new(range(10, 10 + week)).expect("windows")),
            [(10, 10 + week)]
        );
        assert_eq!(
            all(Windows::new(range(10, 12 + 2 * week)).expect("windows")),
            [
                (11 + 2 * week, 12 + 2 * week),
                (11 + week, 11 + 2 * week),
                (10, 10 + week)
            ]
        );
    }

    #[test]
    fn part_of_one_millisecond_is_widened_to_two() {
        let week = MAX_RANGE_MILLIS;
        assert_eq!(
            all(Windows::new(range(10, 10)).expect("windows")),
            [(9, 10)]
        );
        assert_eq!(all(Windows::new(range(0, 0)).expect("windows")), [(0, 1)]);
        assert_eq!(
            all(Windows::new(range(10, 11 + week)).expect("windows")),
            [(10 + week, 11 + week), (10, 10 + week)]
        );
        assert_eq!(
            widened(-5, -5),
            Ok((-5, -4)),
            "a moment before the epoch is widened forwards"
        );
        assert_eq!(
            widened(i64::MAX, i64::MAX).map_err(|error| error.to_string()),
            Ok((i64::MAX - 1, i64::MAX))
        );
        assert_eq!(
            widened(-1, i64::MAX).map_err(|error| error.to_string()),
            Ok((-1, i64::MAX))
        );
        assert!(matches!(
            widened(i64::MIN, i64::MIN),
            Ok((i64::MIN, end)) if end == i64::MIN + 1
        ));
    }

    #[test]
    fn part_is_halved_until_it_is_too_short() {
        let mut windows = Windows::new(range(10, 20)).expect("windows");
        let whole = windows.next().expect("window");
        windows.split(whole).expect("halves");
        assert_eq!(windows.next(), Some((15, 20)));
        assert_eq!(windows.next(), Some((10, 15)));
        windows.split((10, 12)).expect("halves");
        assert_eq!(all(windows), [(11, 12), (10, 11)]);
        let mut windows = Windows::new(range(10, 11)).expect("windows");
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        assert_eq!(
            windows.split((10, 11)),
            Err(ConnectorError::Incomplete {
                from: at(10),
                to: at(11)
            })
        );
        assert!(windows.split((i64::MIN, i64::MAX)).is_err());
    }
}
