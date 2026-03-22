//! Tick history: every price change of one side of a symbol within a range of time.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::common::ids::SymbolId;
//! use ctrader_connector::common::time::{TimeRange, Timestamp};
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::quotes::QuoteSide;
//! use ctrader_connector::open_api::ticks::TickDataParams;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let minute = TimeRange::ending_at(Timestamp::now(), Duration::from_secs(60));
//! let params = TickDataParams::new(account, SymbolId::new(1)?, QuoteSide::Bid, minute);
//! for tick in client.tick_data(&params).await? {
//!     println!("{} {}", tick.timestamp, tick.price);
//! }
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use crate::common::decimals::Price;
use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, SymbolId};
use crate::common::time::{TimeRange, Timestamp};
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{ProtoOaGetTickDataReq, ProtoOaQuoteType, ProtoOaTickData};
use crate::open_api::quotes::{QuoteSide, wire_price};

const TICK_TIMESTAMP: &str = "tick timestamp";
const TICK_PRICE: &str = "tick price";
const MAX_RANGE_MILLIS: i64 = 604_800_000;

/// One price change of one side of a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Tick {
    /// Moment the price changed.
    pub timestamp: Timestamp,
    /// Price from that moment on.
    pub price: Price,
}

/// Parameters of [`OpenApiClient::tick_data`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct TickDataParams {
    account: AccountId,
    symbol: SymbolId,
    side: QuoteSide,
    range: TimeRange,
    limit: Option<usize>,
}

impl TickDataParams {
    /// Longest span of time the server accepts in one request: one week.
    pub const MAX_RANGE: Duration = Duration::from_millis(MAX_RANGE_MILLIS.unsigned_abs());

    /// Asks for the ticks of one side of the symbol that fall inside the range.
    pub const fn new(
        account: AccountId,
        symbol: SymbolId,
        side: QuoteSide,
        range: TimeRange,
    ) -> Self {
        Self {
            account,
            symbol,
            side,
            range,
            limit: None,
        }
    }

    /// Limits the answer to the latest `limit` ticks of the range.
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Returns the account the ticks are asked for.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Returns the symbol the ticks belong to.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Returns the side of the quote the ticks belong to.
    #[must_use]
    pub const fn side(&self) -> QuoteSide {
        self.side
    }

    /// Returns the range of time the ticks fall inside.
    #[must_use]
    pub const fn range(&self) -> TimeRange {
        self.range
    }

    /// Returns the largest number of ticks in the answer, when one is set.
    #[must_use]
    pub const fn max_ticks(&self) -> Option<usize> {
        self.limit
    }
}

const fn wire_side(side: QuoteSide) -> ProtoOaQuoteType {
    match side {
        QuoteSide::Bid => ProtoOaQuoteType::Bid,
        QuoteSide::Ask => ProtoOaQuoteType::Ask,
    }
}

fn decode(page: &[ProtoOaTickData]) -> Result<Vec<Tick>, ValueError> {
    let mut ticks = Vec::with_capacity(page.len());
    let (mut millis, mut raw) = (0_i64, 0_i64);
    for wire in page {
        millis = millis
            .checked_add(wire.timestamp)
            .ok_or(ValueError::OutOfRange {
                kind: TICK_TIMESTAMP,
            })?;
        raw = raw
            .checked_add(wire.tick)
            .ok_or(ValueError::OutOfRange { kind: TICK_PRICE })?;
        let timestamp = Timestamp::from_millis(millis).map_err(|_| ValueError::OutOfRange {
            kind: TICK_TIMESTAMP,
        })?;
        let price = u64::try_from(raw)
            .map_err(|_| ValueError::OutOfRange { kind: TICK_PRICE })
            .and_then(wire_price)?;
        ticks.push(Tick { timestamp, price });
    }
    Ok(ticks)
}

impl OpenApiClient {
    /// Fetches the ticks of one side of a symbol inside a range of time, oldest first.
    ///
    /// The server answers with a limited number of ticks per request and accepts at most
    /// [`TickDataParams::MAX_RANGE`] per request. A longer or busier range is fetched with
    /// several requests, latest part first, each of which counts against the limit for
    /// historical data. Of several ticks that share the millisecond at which one answer ends
    /// and the next begins, only those of the later answer are returned. When such a
    /// millisecond holds so many ticks that an answer brings nothing older, the next request
    /// ends one millisecond earlier, so the older ticks of the range are fetched all the same.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a request,
    /// [`ConnectorError::Value`] when a tick carries a value out of range,
    /// [`ConnectorError::Incomplete`] when the server announces more ticks but sends none of
    /// them, in which case no part of the range is returned, and the errors of
    /// [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn tick_data(&self, params: &TickDataParams) -> Result<Vec<Tick>, ConnectorError> {
        let from = params.range.from().as_millis();
        let to = params.range.to().as_millis();
        let wanted = params.limit.unwrap_or(usize::MAX);
        let mut bound = to.saturating_add(1);
        let mut ticks: Vec<Tick> = Vec::new();
        let mut stepped = false;
        while bound > from && ticks.len() < wanted {
            let end = if stepped {
                bound.saturating_sub(1)
            } else {
                bound.min(to)
            };
            let start = end.saturating_sub(MAX_RANGE_MILLIS).max(from);
            let request = ProtoOaGetTickDataReq {
                payload_type: None,
                ctid_trader_account_id: params.account.get(),
                symbol_id: params.symbol.get(),
                r#type: i32::from(wire_side(params.side)),
                from_timestamp: start,
                to_timestamp: end,
            };
            let response = self.connection().request(&request).await?;
            let known = ticks.len();
            let mut oldest = bound;
            for tick in decode(&response.tick_data)? {
                let millis = tick.timestamp.as_millis();
                if millis >= from && millis < bound {
                    oldest = oldest.min(millis);
                    ticks.push(tick);
                }
            }
            if !response.has_more {
                bound = start;
                stepped = false;
            } else if ticks.len() > known {
                bound = oldest;
                stepped = false;
            } else if end == bound {
                stepped = true;
            } else {
                return Err(ConnectorError::Incomplete {
                    from: Timestamp::from_millis(start)?,
                    to: Timestamp::from_millis(end)?,
                });
            }
        }
        ticks.sort_by_key(|tick| std::cmp::Reverse(tick.timestamp));
        ticks.truncate(wanted);
        ticks.reverse();
        Ok(ticks)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Tick, TickDataParams, decode, wire_side};
    use crate::common::errors::ValueError;
    use crate::common::ids::{AccountId, SymbolId};
    use crate::common::time::{TimeRange, Timestamp};
    use crate::open_api::messages::{ProtoOaQuoteType, ProtoOaTickData};
    use crate::open_api::quotes::QuoteSide;

    fn wire(timestamp: i64, tick: i64) -> ProtoOaTickData {
        ProtoOaTickData { timestamp, tick }
    }

    #[test]
    fn ticks_after_the_first_are_differences_to_the_one_before() {
        let ticks = decode(&[wire(1_000, 108_500), wire(-10, 3), wire(-5, -8)]).expect("ticks");
        let seen: Vec<(i64, String)> = ticks
            .iter()
            .map(|tick: &Tick| (tick.timestamp.as_millis(), tick.price.to_string()))
            .collect();
        assert_eq!(
            seen,
            [
                (1_000, "1.08500".to_owned()),
                (990, "1.08503".to_owned()),
                (985, "1.08495".to_owned())
            ]
        );
        assert_eq!(decode(&[]), Ok(Vec::new()));
    }

    #[test]
    fn ticks_that_leave_the_range_of_values_are_rejected() {
        let timestamp = Err(ValueError::OutOfRange {
            kind: "tick timestamp",
        });
        let price = Err(ValueError::OutOfRange { kind: "tick price" });
        assert_eq!(decode(&[wire(-1, 1)]), timestamp);
        assert_eq!(decode(&[wire(5, 1), wire(-6, 0)]), timestamp);
        assert_eq!(decode(&[wire(i64::MAX, 1), wire(1, 0)]), timestamp);
        assert_eq!(decode(&[wire(5, -1)]), price);
        assert_eq!(decode(&[wire(5, 1), wire(0, -2)]), price);
        assert_eq!(decode(&[wire(5, i64::MAX), wire(0, 1)]), price);
    }

    #[test]
    fn parameters_keep_what_they_were_built_from() {
        let account = AccountId::new(7).expect("account id");
        let symbol = SymbolId::new(1).expect("symbol id");
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        let range = TimeRange::new(at(10), at(20)).expect("range");
        let params = TickDataParams::new(account, symbol, QuoteSide::Ask, range);
        assert_eq!(params.account(), account);
        assert_eq!(params.symbol(), symbol);
        assert_eq!(params.side(), QuoteSide::Ask);
        assert_eq!(params.range(), range);
        assert_eq!(params.max_ticks(), None);
        assert_eq!(params.limit(3).max_ticks(), Some(3));
        assert_eq!(TickDataParams::MAX_RANGE, Duration::from_secs(7 * 86_400));
        assert_eq!(wire_side(QuoteSide::Bid), ProtoOaQuoteType::Bid);
        assert_eq!(wire_side(QuoteSide::Ask), ProtoOaQuoteType::Ask);
    }
}
