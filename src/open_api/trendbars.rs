//! Trendbars (candles) of a symbol: their history and the bar that is forming now.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::common::ids::SymbolId;
//! use ctrader_connector::common::time::{TimeRange, Timestamp};
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::trendbars::{Period, TrendbarsParams};
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let symbol = SymbolId::new(1)?;
//! let day = TimeRange::ending_at(Timestamp::now(), Duration::from_secs(86_400));
//! let params = TrendbarsParams::new(account, symbol, Period::H1, day).count(10);
//! for bar in client.trendbars(&params).await? {
//!     println!("{} {} {} {} {}", bar.timestamp, bar.open, bar.high, bar.low, bar.close);
//! }
//! let mut live = client.subscribe_live_trendbars(account, symbol, Period::M1).await?;
//! println!("{:?}", live.next().await?);
//! live.unsubscribe().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::common::decimals::Price;
use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, SymbolId};
use crate::common::time::{TimeRange, Timestamp};
use crate::open_api::client::OpenApiClient;
use crate::open_api::connection::{ReplayKey, ReplayStage};
use crate::open_api::messages::{
    ProtoOaGetTrendbarsReq, ProtoOaSpotEvent, ProtoOaSubscribeLiveTrendbarReq, ProtoOaTrendbar,
    ProtoOaTrendbarPeriod, ProtoOaUnsubscribeLiveTrendbarReq,
};
use crate::open_api::quotes::wire_price;
use crate::open_api::stream::Feed;

const MINUTE: u64 = 60;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;
const WEEK: u64 = 7 * DAY;

/// Length of time one trendbar covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Period {
    /// One minute.
    M1,
    /// Two minutes.
    M2,
    /// Three minutes.
    M3,
    /// Four minutes.
    M4,
    /// Five minutes.
    M5,
    /// Ten minutes.
    M10,
    /// Fifteen minutes.
    M15,
    /// Thirty minutes.
    M30,
    /// One hour.
    H1,
    /// Four hours.
    H4,
    /// Twelve hours.
    H12,
    /// One day.
    D1,
    /// One week.
    W1,
    /// One month.
    Mn1,
}

impl Period {
    /// Every period, shortest first.
    pub const ALL: [Self; 14] = [
        Self::M1,
        Self::M2,
        Self::M3,
        Self::M4,
        Self::M5,
        Self::M10,
        Self::M15,
        Self::M30,
        Self::H1,
        Self::H4,
        Self::H12,
        Self::D1,
        Self::W1,
        Self::Mn1,
    ];

    const fn wire(self) -> ProtoOaTrendbarPeriod {
        match self {
            Self::M1 => ProtoOaTrendbarPeriod::M1,
            Self::M2 => ProtoOaTrendbarPeriod::M2,
            Self::M3 => ProtoOaTrendbarPeriod::M3,
            Self::M4 => ProtoOaTrendbarPeriod::M4,
            Self::M5 => ProtoOaTrendbarPeriod::M5,
            Self::M10 => ProtoOaTrendbarPeriod::M10,
            Self::M15 => ProtoOaTrendbarPeriod::M15,
            Self::M30 => ProtoOaTrendbarPeriod::M30,
            Self::H1 => ProtoOaTrendbarPeriod::H1,
            Self::H4 => ProtoOaTrendbarPeriod::H4,
            Self::H12 => ProtoOaTrendbarPeriod::H12,
            Self::D1 => ProtoOaTrendbarPeriod::D1,
            Self::W1 => ProtoOaTrendbarPeriod::W1,
            Self::Mn1 => ProtoOaTrendbarPeriod::Mn1,
        }
    }

    /// Returns the nominal length of one bar; a month counts thirty days.
    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(match self {
            Self::M1 => MINUTE,
            Self::M2 => 2 * MINUTE,
            Self::M3 => 3 * MINUTE,
            Self::M4 => 4 * MINUTE,
            Self::M5 => 5 * MINUTE,
            Self::M10 => 10 * MINUTE,
            Self::M15 => 15 * MINUTE,
            Self::M30 => 30 * MINUTE,
            Self::H1 => HOUR,
            Self::H4 => 4 * HOUR,
            Self::H12 => 12 * HOUR,
            Self::D1 => DAY,
            Self::W1 => WEEK,
            Self::Mn1 => 30 * DAY,
        })
    }

    /// Returns the longest span of time one request for bars of the period may cover.
    ///
    /// [`OpenApiClient::trendbars`] splits a longer range into requests of at most this span.
    #[must_use]
    pub const fn max_range(self) -> Duration {
        Duration::from_secs(match self {
            Self::M1 | Self::M2 | Self::M3 | Self::M4 | Self::M5 => 5 * WEEK,
            Self::M10 | Self::M15 | Self::M30 | Self::H1 => 35 * WEEK,
            Self::H4 | Self::H12 | Self::D1 => 366 * DAY,
            Self::W1 | Self::Mn1 => 1830 * DAY,
        })
    }
}

impl fmt::Display for Period {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.wire().as_str_name())
    }
}

const LOW: &str = "trendbar low";
const OPEN: &str = "trendbar open";
const HIGH: &str = "trendbar high";
const CLOSE: &str = "trendbar close";
const START: &str = "trendbar timestamp";
const VOLUME: &str = "trendbar volume";

/// Prices of a symbol over one period.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Trendbar {
    /// Symbol the bar belongs to.
    pub symbol: SymbolId,
    /// Length of time the bar covers.
    pub period: Period,
    /// Moment the bar starts.
    pub timestamp: Timestamp,
    /// First price of the period.
    pub open: Price,
    /// Highest price of the period.
    pub high: Price,
    /// Lowest price of the period.
    pub low: Price,
    /// Last price of the period.
    pub close: Price,
    /// Number of price changes within the period.
    pub volume: u64,
}

impl Trendbar {
    fn parse(
        symbol: SymbolId,
        period: Period,
        bar: &ProtoOaTrendbar,
        last: Option<u64>,
    ) -> Result<Self, ValueError> {
        let low = bar.low.ok_or(ValueError::Missing { kind: LOW })?;
        let low = u64::try_from(low).map_err(|_| ValueError::OutOfRange { kind: LOW })?;
        let above = |delta: Option<u64>, kind: &'static str| {
            delta
                .ok_or(ValueError::Missing { kind })?
                .checked_add(low)
                .ok_or(ValueError::OutOfRange { kind })
        };
        let close = match (bar.delta_close, last) {
            (None, Some(last)) => last,
            (delta, _) => above(delta, CLOSE)?,
        };
        let minutes = bar
            .utc_timestamp_in_minutes
            .ok_or(ValueError::Missing { kind: START })?;
        Ok(Self {
            symbol,
            period,
            timestamp: Timestamp::from_minutes(minutes),
            open: wire_price(above(bar.delta_open, OPEN)?)?,
            high: wire_price(above(bar.delta_high, HIGH)?)?,
            low: wire_price(low)?,
            close: wire_price(close)?,
            volume: u64::try_from(bar.volume)
                .map_err(|_| ValueError::OutOfRange { kind: VOLUME })?,
        })
    }
}

/// Parameters of [`OpenApiClient::trendbars`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct TrendbarsParams {
    account: AccountId,
    symbol: SymbolId,
    period: Period,
    range: TimeRange,
    count: Option<usize>,
}

impl TrendbarsParams {
    /// Asks for the bars of the symbol and period that overlap the range.
    pub const fn new(
        account: AccountId,
        symbol: SymbolId,
        period: Period,
        range: TimeRange,
    ) -> Self {
        Self {
            account,
            symbol,
            period,
            range,
            count: None,
        }
    }

    /// Limits the answer to the latest `count` bars of the range.
    pub const fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Returns the account the bars are asked for.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Returns the symbol the bars belong to.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Returns the length of time one bar covers.
    #[must_use]
    pub const fn period(&self) -> Period {
        self.period
    }

    /// Returns the range of time the bars overlap.
    #[must_use]
    pub const fn range(&self) -> TimeRange {
        self.range
    }

    /// Returns the largest number of bars in the answer, when one is set.
    #[must_use]
    pub const fn limit(&self) -> Option<usize> {
        self.count
    }
}

pub(crate) fn trendbar_key(account: AccountId, symbol: SymbolId, period: Period) -> ReplayKey {
    ReplayKey::new(
        ReplayStage::Subscription,
        format!("trendbars/{account}/{symbol}/{period}"),
    )
}

fn span_millis(span: Duration) -> i64 {
    i64::try_from(span.as_millis()).unwrap_or(i64::MAX)
}

/// The forming bar of one symbol and period, sent anew whenever a price changes it.
///
/// The subscription behind the stream is restored after a reconnection.
#[derive(Debug)]
pub struct TrendbarStream {
    client: OpenApiClient,
    feed: Feed,
    account: AccountId,
    symbol: SymbolId,
    period: Period,
    bid: Option<u64>,
}

impl TrendbarStream {
    /// Returns the account the bars belong to.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Returns the symbol the bars belong to.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Returns the length of time one bar covers.
    #[must_use]
    pub const fn period(&self) -> Period {
        self.period
    }

    /// Waits for the next state of the forming bar.
    ///
    /// The server leaves the last price out of a live bar; it is taken from the bid that
    /// arrives with the bar or, failing that, from the last bid the stream has seen. A bar that
    /// arrives before any bid is passed over.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down and
    /// [`ConnectorError::Value`] when the server sends a value out of range.
    pub async fn next(&mut self) -> Result<Trendbar, ConnectorError> {
        let period = i32::from(self.period.wire());
        loop {
            let event = self.feed.next().await?;
            let Some(spot) = event.decode::<ProtoOaSpotEvent>() else {
                continue;
            };
            if spot.ctid_trader_account_id != self.account.get()
                || spot.symbol_id != self.symbol.get()
            {
                continue;
            }
            self.bid = spot.bid.or(self.bid);
            let Some(bar) = spot.trendbar.iter().find(|bar| bar.period == Some(period)) else {
                continue;
            };
            if bar.delta_close.is_none() && self.bid.is_none() {
                continue;
            }
            return Ok(Trendbar::parse(self.symbol, self.period, bar, self.bid)?);
        }
    }

    /// Ends the subscription to the live bars; the spot subscription of the symbol stays.
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::unsubscribe_live_trendbars`].
    pub async fn unsubscribe(self) -> Result<(), ConnectorError> {
        self.client
            .unsubscribe_live_trendbars(self.account, self.symbol, self.period)
            .await
    }
}

impl OpenApiClient {
    /// Fetches the bars of a symbol and period that overlap a range of time, oldest first.
    ///
    /// The server answers one request with a limited number of bars and accepts a limited span
    /// of time per period, see [`Period::max_range`]. A longer range is fetched with several
    /// requests, latest part first, each of which counts against the limit for historical data.
    /// The bar that is still forming is not part of the answer.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a request,
    /// [`ConnectorError::Value`] when a bar lacks a field or carries a value out of range, and
    /// the errors of [`Connection::request`](crate::open_api::connection::Connection::request)
    /// otherwise.
    pub async fn trendbars(
        &self,
        params: &TrendbarsParams,
    ) -> Result<Vec<Trendbar>, ConnectorError> {
        let from = params.range.from().as_millis();
        let span = span_millis(params.period.max_range());
        let wanted = params.count.unwrap_or(usize::MAX);
        let mut cursor = params.range.to().as_millis();
        let mut bars = BTreeMap::new();
        while cursor >= from && bars.len() < wanted {
            let start = cursor.saturating_sub(span).max(from);
            let request = ProtoOaGetTrendbarsReq {
                payload_type: None,
                ctid_trader_account_id: params.account.get(),
                from_timestamp: start,
                to_timestamp: cursor,
                period: i32::from(params.period.wire()),
                symbol_id: params.symbol.get(),
                count: None,
            };
            let response = self.connection().request(&request).await?;
            let mut earliest = start;
            for (index, wire) in response.trendbar.iter().enumerate() {
                let bar = Trendbar::parse(params.symbol, params.period, wire, None)?;
                let opened = bar.timestamp.as_millis();
                earliest = if index == 0 {
                    opened
                } else {
                    earliest.min(opened)
                };
                bars.insert(bar.timestamp, bar);
            }
            cursor = earliest.min(cursor).saturating_sub(1);
        }
        let skipped = bars.len().saturating_sub(wanted);
        Ok(bars.into_values().skip(skipped).collect())
    }

    /// Subscribes to the forming bar of a symbol and period on an authorised account.
    ///
    /// The server sends live bars only for symbols whose spot prices are subscribed, so the
    /// spot subscription is requested first unless this client holds it already. Both
    /// subscriptions are restored after a reconnection, in that order.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a subscription, and the
    /// errors of [`Connection::request`](crate::open_api::connection::Connection::request)
    /// otherwise.
    pub async fn subscribe_live_trendbars(
        &self,
        account: AccountId,
        symbol: SymbolId,
        period: Period,
    ) -> Result<TrendbarStream, ConnectorError> {
        let stream = TrendbarStream {
            client: self.clone(),
            feed: Feed::new(self.connection()),
            account,
            symbol,
            period,
            bid: None,
        };
        if !self.has_spots(account, symbol) {
            self.subscribe_spots(account, &[symbol]).await?;
        }
        let request = ProtoOaSubscribeLiveTrendbarReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            period: i32::from(period.wire()),
            symbol_id: symbol.get(),
        };
        self.connection().request(&request).await?;
        self.connection()
            .register_replay(trendbar_key(account, symbol, period), &request)
            .await?;
        self.track_trendbars(account, symbol, period, true);
        Ok(stream)
    }

    /// Ends the subscription to the forming bar of a symbol and period.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the request, and the errors
    /// of [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn unsubscribe_live_trendbars(
        &self,
        account: AccountId,
        symbol: SymbolId,
        period: Period,
    ) -> Result<(), ConnectorError> {
        let request = ProtoOaUnsubscribeLiveTrendbarReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            period: i32::from(period.wire()),
            symbol_id: symbol.get(),
        };
        self.connection().request(&request).await?;
        self.connection()
            .remove_replay(trendbar_key(account, symbol, period))
            .await?;
        self.track_trendbars(account, symbol, period, false);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Period, Trendbar, TrendbarsParams, trendbar_key};
    use crate::common::errors::ValueError;
    use crate::common::ids::{AccountId, SymbolId};
    use crate::common::time::{TimeRange, Timestamp};
    use crate::open_api::messages::{ProtoOaTrendbar, ProtoOaTrendbarPeriod};

    fn symbol() -> SymbolId {
        SymbolId::new(1).expect("symbol id")
    }

    fn wire() -> ProtoOaTrendbar {
        ProtoOaTrendbar {
            volume: 42,
            period: None,
            low: Some(108_500),
            delta_open: Some(5),
            delta_close: Some(7),
            delta_high: Some(12),
            utc_timestamp_in_minutes: Some(3),
        }
    }

    fn parse(bar: &ProtoOaTrendbar, last: Option<u64>) -> Result<Trendbar, ValueError> {
        Trendbar::parse(symbol(), Period::M5, bar, last)
    }

    #[test]
    fn periods_map_to_the_wire_and_grow_in_length() {
        let names: Vec<String> = Period::ALL.iter().map(ToString::to_string).collect();
        assert_eq!(
            names,
            [
                "M1", "M2", "M3", "M4", "M5", "M10", "M15", "M30", "H1", "H4", "H12", "D1", "W1",
                "MN1"
            ]
        );
        for (index, period) in Period::ALL.iter().enumerate() {
            assert_eq!(
                ProtoOaTrendbarPeriod::try_from(i32::try_from(index + 1).expect("number")),
                Ok(period.wire())
            );
            assert!(period.max_range() > period.duration());
        }
        assert!(
            Period::ALL
                .windows(2)
                .all(|pair| pair[0].duration() < pair[1].duration() && pair[0] < pair[1])
        );
        assert!(
            Period::ALL
                .windows(2)
                .all(|pair| pair[0].max_range() <= pair[1].max_range())
        );
        assert_eq!(Period::M1.duration(), Duration::from_secs(60));
        assert_eq!(Period::Mn1.duration(), Duration::from_secs(2_592_000));
        assert_eq!(Period::M5.max_range(), Duration::from_millis(3_024_000_000));
        assert_eq!(
            Period::H1.max_range(),
            Duration::from_millis(21_168_000_000)
        );
        assert_eq!(
            Period::D1.max_range(),
            Duration::from_millis(31_622_400_000)
        );
        assert_eq!(
            Period::W1.max_range(),
            Duration::from_millis(158_112_000_000)
        );
    }

    #[test]
    fn bar_prices_are_the_low_plus_their_deltas() {
        let bar = parse(&wire(), None).expect("bar");
        assert_eq!(bar.symbol, symbol());
        assert_eq!(bar.period, Period::M5);
        assert_eq!(bar.timestamp.as_millis(), 180_000);
        let prices = [bar.open, bar.high, bar.low, bar.close].map(|price| price.to_string());
        assert_eq!(prices, ["1.08505", "1.08512", "1.08500", "1.08507"]);
        assert_eq!(bar.volume, 42);
        assert_eq!(parse(&wire(), Some(1)), Ok(bar));
    }

    #[test]
    fn a_live_bar_takes_its_close_from_the_last_price() {
        let live = ProtoOaTrendbar {
            delta_close: None,
            ..wire()
        };
        let bar = parse(&live, Some(108_509)).expect("bar");
        assert_eq!(bar.close.to_string(), "1.08509");
        assert_eq!(
            parse(&live, None),
            Err(ValueError::Missing {
                kind: "trendbar close"
            })
        );
    }

    #[test]
    fn bars_with_absent_or_impossible_fields_are_rejected() {
        let missing = |kind| Err(ValueError::Missing { kind });
        let out_of_range = |kind| Err(ValueError::OutOfRange { kind });
        let cases = [
            (
                ProtoOaTrendbar {
                    low: None,
                    ..wire()
                },
                missing("trendbar low"),
            ),
            (
                ProtoOaTrendbar {
                    delta_open: None,
                    ..wire()
                },
                missing("trendbar open"),
            ),
            (
                ProtoOaTrendbar {
                    delta_high: None,
                    ..wire()
                },
                missing("trendbar high"),
            ),
            (
                ProtoOaTrendbar {
                    utc_timestamp_in_minutes: None,
                    ..wire()
                },
                missing("trendbar timestamp"),
            ),
            (
                ProtoOaTrendbar {
                    low: Some(-1),
                    ..wire()
                },
                out_of_range("trendbar low"),
            ),
            (
                ProtoOaTrendbar {
                    delta_high: Some(u64::MAX),
                    ..wire()
                },
                out_of_range("trendbar high"),
            ),
            (
                ProtoOaTrendbar {
                    volume: -1,
                    ..wire()
                },
                out_of_range("trendbar volume"),
            ),
        ];
        for (bar, error) in cases {
            assert_eq!(parse(&bar, None), error);
        }
        assert_eq!(
            ValueError::Missing {
                kind: "trendbar low"
            }
            .to_string(),
            "trendbar low is missing"
        );
    }

    #[test]
    fn parameters_keep_what_they_were_built_from() {
        let account = AccountId::new(7).expect("account id");
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        let range = TimeRange::new(at(10), at(20)).expect("range");
        let params = TrendbarsParams::new(account, symbol(), Period::H4, range);
        assert_eq!(params.account(), account);
        assert_eq!(params.symbol(), symbol());
        assert_eq!(params.period(), Period::H4);
        assert_eq!(params.range(), range);
        assert_eq!(params.limit(), None);
        assert_eq!(params.count(3).limit(), Some(3));
        assert_eq!(
            trendbar_key(account, symbol(), Period::H4).id(),
            "trendbars/7/1/H4"
        );
    }
}
