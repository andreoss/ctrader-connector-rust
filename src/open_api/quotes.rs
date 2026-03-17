//! Stream of spot quotes of subscribed symbols.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::common::ids::SymbolId;
//! use ctrader_connector::open_api::client::OpenApiClient;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let mut quotes = client.subscribe_quotes(account, &[SymbolId::new(1)?]).await?;
//! for _ in 0..10 {
//!     let quote = quotes.next().await?;
//!     println!("{}: {:?} / {:?}", quote.symbol, quote.bid, quote.ask);
//! }
//! quotes.unsubscribe().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, BTreeSet};

use crate::common::decimals::{PRICE_SCALE, Price};
use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, SymbolId};
use crate::common::time::Timestamp;
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::ProtoOaSpotEvent;
use crate::open_api::stream::{Feed, starts_replay};

/// Side of a quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum QuoteSide {
    /// Price at which the market buys.
    Bid,
    /// Price at which the market sells.
    Ask,
}

/// Converts a price of a spot, tick or bar message, an integer in 1/100000 of a unit.
pub(crate) fn wire_price(raw: u64) -> Result<Price, ValueError> {
    Price::from_wire(raw, PRICE_SCALE)
}

/// Latest known prices of a symbol.
///
/// The server sends only the side that changed; the other side keeps its last known value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Quote {
    /// Symbol the prices belong to.
    pub symbol: SymbolId,
    /// Last known bid, absent until the server has sent one.
    pub bid: Option<Price>,
    /// Last known ask, absent until the server has sent one.
    pub ask: Option<Price>,
    /// Moment of the change, when the server reports it.
    pub timestamp: Option<Timestamp>,
}

impl Quote {
    /// Returns the last known price of the given side.
    #[must_use]
    pub const fn price(&self, side: QuoteSide) -> Option<Price> {
        match side {
            QuoteSide::Bid => self.bid,
            QuoteSide::Ask => self.ask,
        }
    }

    pub(crate) fn updated(
        previous: Option<&Self>,
        symbol: SymbolId,
        spot: &ProtoOaSpotEvent,
    ) -> Result<Self, ValueError> {
        let bid = spot.bid.map(wire_price).transpose()?;
        let ask = spot.ask.map(wire_price).transpose()?;
        Ok(Self {
            symbol,
            bid: bid.or(previous.and_then(|quote| quote.bid)),
            ask: ask.or(previous.and_then(|quote| quote.ask)),
            timestamp: spot.timestamp.map(Timestamp::from_millis).transpose()?,
        })
    }
}

/// Quotes of the symbols of one account, in the order the server sends them.
///
/// The subscription behind the stream is restored after a reconnection, so the stream goes on
/// without further calls. The last known prices are forgotten the moment the loss of the
/// connection is reported, so a price from before the loss is never combined with one from
/// after it.
#[derive(Debug)]
pub struct QuoteStream {
    client: OpenApiClient,
    feed: Feed,
    account: AccountId,
    symbols: BTreeSet<SymbolId>,
    last: BTreeMap<SymbolId, Quote>,
}

impl QuoteStream {
    fn new(client: &OpenApiClient, account: AccountId, symbols: &[SymbolId]) -> Self {
        Self {
            client: client.clone(),
            feed: Feed::new(client.connection()),
            account,
            symbols: symbols.iter().copied().collect(),
            last: BTreeMap::new(),
        }
    }

    /// Returns the account the quotes belong to.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Returns the symbols the stream delivers quotes of, in ascending order.
    pub fn symbols(&self) -> impl Iterator<Item = SymbolId> + '_ {
        self.symbols.iter().copied()
    }

    /// Returns the latest quote the stream has delivered for the symbol.
    #[must_use]
    pub fn last(&self, symbol: SymbolId) -> Option<&Quote> {
        self.last.get(&symbol)
    }

    /// Waits for the next quote of one of the symbols of the stream.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down and
    /// [`ConnectorError::Value`] when the server sends a value out of range.
    pub async fn next(&mut self) -> Result<Quote, ConnectorError> {
        loop {
            let event = self.feed.next().await?;
            if starts_replay(&event) {
                self.last.clear();
                continue;
            }
            let Some(spot) = event.decode::<ProtoOaSpotEvent>() else {
                continue;
            };
            if spot.ctid_trader_account_id != self.account.get()
                || (spot.bid.is_none() && spot.ask.is_none())
            {
                continue;
            }
            let Some(symbol) = self
                .symbols
                .iter()
                .copied()
                .find(|symbol| symbol.get() == spot.symbol_id)
            else {
                continue;
            };
            let quote = Quote::updated(self.last.get(&symbol), symbol, &spot)?;
            self.last.insert(symbol, quote);
            return Ok(quote);
        }
    }

    /// Ends the subscription to the spot prices of the symbols of the stream.
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::unsubscribe_spots`].
    pub async fn unsubscribe(self) -> Result<(), ConnectorError> {
        let symbols: Vec<SymbolId> = self.symbols.into_iter().collect();
        self.client.unsubscribe_spots(self.account, &symbols).await
    }
}

impl OpenApiClient {
    /// Subscribes to the spot prices of the given symbols on an authorised account and returns
    /// their quotes as a stream.
    ///
    /// The stream starts to listen before the subscription is requested, so the first prices
    /// the server sends are not missed. The subscription is restored after a reconnection.
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::subscribe_spots`].
    pub async fn subscribe_quotes(
        &self,
        account: AccountId,
        symbols: &[SymbolId],
    ) -> Result<QuoteStream, ConnectorError> {
        let stream = QuoteStream::new(self, account, symbols);
        self.subscribe_spots(account, symbols).await?;
        Ok(stream)
    }

    /// Returns the quotes of symbols that are subscribed already, without subscribing.
    ///
    /// The stream delivers quotes that arrive after this call and stays silent for symbols
    /// without a subscription.
    #[must_use]
    pub fn quotes(&self, account: AccountId, symbols: &[SymbolId]) -> QuoteStream {
        QuoteStream::new(self, account, symbols)
    }
}

#[cfg(test)]
mod tests {
    use super::{Quote, QuoteSide};
    use crate::common::errors::ValueError;
    use crate::common::ids::SymbolId;
    use crate::common::time::Timestamp;
    use crate::open_api::messages::ProtoOaSpotEvent;

    fn spot(bid: Option<u64>, ask: Option<u64>, timestamp: Option<i64>) -> ProtoOaSpotEvent {
        ProtoOaSpotEvent {
            ctid_trader_account_id: 7,
            symbol_id: 1,
            bid,
            ask,
            timestamp,
            ..ProtoOaSpotEvent::default()
        }
    }

    fn text(quote: &Quote, side: QuoteSide) -> Option<String> {
        quote.price(side).map(|price| price.to_string())
    }

    #[test]
    fn a_side_the_event_leaves_out_keeps_its_last_known_price() {
        let symbol = SymbolId::new(1).expect("symbol id");
        let first =
            Quote::updated(None, symbol, &spot(Some(108_512), None, Some(1_000))).expect("quote");
        assert_eq!(first.symbol, symbol);
        assert_eq!(text(&first, QuoteSide::Bid).as_deref(), Some("1.08512"));
        assert_eq!(first.ask, None);
        assert_eq!(first.timestamp.map(Timestamp::as_millis), Some(1_000));
        let second =
            Quote::updated(Some(&first), symbol, &spot(None, Some(108_520), None)).expect("quote");
        assert_eq!(text(&second, QuoteSide::Bid).as_deref(), Some("1.08512"));
        assert_eq!(text(&second, QuoteSide::Ask).as_deref(), Some("1.08520"));
        assert_eq!(second.timestamp, None);
        let third =
            Quote::updated(Some(&second), symbol, &spot(Some(108_500), None, None)).expect("quote");
        assert_eq!(text(&third, QuoteSide::Bid).as_deref(), Some("1.08500"));
        assert_eq!(text(&third, QuoteSide::Ask).as_deref(), Some("1.08520"));
    }

    #[test]
    fn values_out_of_range_are_rejected() {
        let symbol = SymbolId::new(1).expect("symbol id");
        assert_eq!(
            Quote::updated(None, symbol, &spot(Some(1), None, Some(-1))),
            Err(ValueError::OutOfRange { kind: "timestamp" })
        );
        assert!(Quote::updated(None, symbol, &spot(Some(u64::MAX), None, None)).is_ok());
    }
}
