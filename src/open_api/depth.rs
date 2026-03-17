//! Stream of market depth and the order book rebuilt from it.
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
//! let symbol = SymbolId::new(1)?;
//! let mut depth = client.subscribe_depth(account, &[symbol]).await?;
//! depth.next().await?;
//! if let Some(book) = depth.book(symbol) {
//!     println!("{:?} / {:?}", book.best_bid(), book.best_ask());
//! }
//! depth.unsubscribe().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::common::decimals::{Price, Volume};
use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, SymbolId};
use crate::open_api::client::OpenApiClient;
use crate::open_api::connection::{ReplayKey, ReplayStage};
use crate::open_api::messages::{
    ProtoOaDepthEvent, ProtoOaDepthQuote, ProtoOaSubscribeDepthQuotesReq,
    ProtoOaUnsubscribeDepthQuotesReq,
};
use crate::open_api::quotes::{QuoteSide, wire_price};
use crate::open_api::stream::{Feed, starts_replay};

const DEPTH_SIZE: &str = "depth size";
const DEPTH_SIDE: &str = "depth quote side";

/// Identifier of one entry of the market depth of a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DepthQuoteId(u64);

impl DepthQuoteId {
    /// Wraps the identifier the server assigned.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the identifier the server assigned.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for DepthQuoteId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One entry of the market depth: a volume on offer at a price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct DepthQuote {
    /// Identifier of the entry, unique within the depth of its symbol.
    pub id: DepthQuoteId,
    /// Side of the book the entry belongs to.
    pub side: QuoteSide,
    /// Price of the entry.
    pub price: Price,
    /// Volume on offer at the price.
    pub size: Volume,
}

impl TryFrom<ProtoOaDepthQuote> for DepthQuote {
    type Error = ValueError;

    fn try_from(quote: ProtoOaDepthQuote) -> Result<Self, Self::Error> {
        let (side, raw) = match (quote.bid, quote.ask) {
            (Some(bid), None) => (QuoteSide::Bid, bid),
            (None, Some(ask)) => (QuoteSide::Ask, ask),
            _ => return Err(ValueError::OutOfRange { kind: DEPTH_SIDE }),
        };
        let volume =
            i64::try_from(quote.size).map_err(|_| ValueError::OutOfRange { kind: DEPTH_SIZE })?;
        Ok(Self {
            id: DepthQuoteId(quote.id),
            side,
            price: wire_price(raw)?,
            size: Volume::from_wire(volume)?,
        })
    }
}

/// Change of the market depth of a symbol.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct DepthUpdate {
    /// Symbol the change belongs to.
    pub symbol: SymbolId,
    /// Entries that are new or replace the entry with the same identifier.
    pub new_quotes: Vec<DepthQuote>,
    /// Identifiers of the entries that are gone.
    pub deleted_quotes: Vec<DepthQuoteId>,
}

impl DepthUpdate {
    fn parse(symbol: SymbolId, event: ProtoOaDepthEvent) -> Result<Self, ValueError> {
        Ok(Self {
            symbol,
            new_quotes: event
                .new_quotes
                .into_iter()
                .map(DepthQuote::try_from)
                .collect::<Result<_, _>>()?,
            deleted_quotes: event.deleted_quotes.into_iter().map(DepthQuoteId).collect(),
        })
    }
}

/// Order book of a symbol, rebuilt from the changes of its market depth.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DepthBook {
    quotes: BTreeMap<DepthQuoteId, DepthQuote>,
}

impl DepthBook {
    /// Creates an empty book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a change: drops the deleted entries, then stores the new ones.
    pub fn apply(&mut self, update: &DepthUpdate) {
        for id in &update.deleted_quotes {
            self.quotes.remove(id);
        }
        for quote in &update.new_quotes {
            self.quotes.insert(quote.id, *quote);
        }
    }

    fn side(&self, side: QuoteSide) -> Vec<DepthQuote> {
        let mut quotes: Vec<DepthQuote> = self
            .quotes
            .values()
            .filter(|quote| quote.side == side)
            .copied()
            .collect();
        quotes.sort_by_key(|quote| (quote.price, quote.id));
        quotes
    }

    /// Returns the bids, best (highest) price first.
    #[must_use]
    pub fn bids(&self) -> Vec<DepthQuote> {
        let mut bids = self.side(QuoteSide::Bid);
        bids.reverse();
        bids
    }

    /// Returns the asks, best (lowest) price first.
    #[must_use]
    pub fn asks(&self) -> Vec<DepthQuote> {
        self.side(QuoteSide::Ask)
    }

    fn prices(&self, side: QuoteSide) -> impl Iterator<Item = Price> + '_ {
        self.quotes
            .values()
            .filter(move |quote| quote.side == side)
            .map(|quote| quote.price)
    }

    /// Returns the highest bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<Price> {
        self.prices(QuoteSide::Bid).max()
    }

    /// Returns the lowest ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<Price> {
        self.prices(QuoteSide::Ask).min()
    }

    /// Returns the number of entries on both sides.
    #[must_use]
    pub fn len(&self) -> usize {
        self.quotes.len()
    }

    /// Tells whether the book holds no entry.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.quotes.is_empty()
    }
}

fn depth_key(account: AccountId, symbol: SymbolId) -> ReplayKey {
    ReplayKey::new(
        ReplayStage::Subscription,
        format!("depth/{account}/{symbol}"),
    )
}

/// Changes of the market depth of the symbols of one account, with the books they build.
///
/// The subscription behind the stream is restored after a reconnection. The books are emptied
/// the moment the loss of the connection is reported, which is before anything the server sends
/// for the restored subscription, so the whole depth the server then sends anew builds them
/// afresh.
#[derive(Debug)]
pub struct DepthStream {
    client: OpenApiClient,
    feed: Feed,
    account: AccountId,
    symbols: BTreeSet<SymbolId>,
    books: BTreeMap<SymbolId, DepthBook>,
}

impl DepthStream {
    /// Returns the account the depth belongs to.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Returns the symbols the stream delivers the depth of, in ascending order.
    pub fn symbols(&self) -> impl Iterator<Item = SymbolId> + '_ {
        self.symbols.iter().copied()
    }

    /// Returns the book of the symbol as of the last change the stream delivered.
    #[must_use]
    pub fn book(&self, symbol: SymbolId) -> Option<&DepthBook> {
        self.books.get(&symbol)
    }

    /// Waits for the next change of the depth of one of the symbols and applies it to the book
    /// of its symbol.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Closed`] when the connection is shut down and
    /// [`ConnectorError::Value`] when the server sends a value out of range.
    pub async fn next(&mut self) -> Result<DepthUpdate, ConnectorError> {
        loop {
            let event = self.feed.next().await?;
            if starts_replay(&event) {
                self.books.clear();
                continue;
            }
            let Some(depth) = event.decode::<ProtoOaDepthEvent>() else {
                continue;
            };
            if depth.ctid_trader_account_id != self.account.get() {
                continue;
            }
            let Some(symbol) = self
                .symbols
                .iter()
                .copied()
                .find(|symbol| u64::try_from(symbol.get()) == Ok(depth.symbol_id))
            else {
                continue;
            };
            let update = DepthUpdate::parse(symbol, depth)?;
            self.books.entry(symbol).or_default().apply(&update);
            return Ok(update);
        }
    }

    /// Ends the subscription to the depth of the symbols of the stream.
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::unsubscribe_depth`].
    pub async fn unsubscribe(self) -> Result<(), ConnectorError> {
        let symbols: Vec<SymbolId> = self.symbols.into_iter().collect();
        self.client.unsubscribe_depth(self.account, &symbols).await
    }
}

impl OpenApiClient {
    /// Subscribes to the market depth of the given symbols on an authorised account and
    /// returns its changes as a stream.
    ///
    /// The stream starts to listen before the subscription is requested, so the first entries
    /// the server sends are not missed. The subscription is restored after a reconnection.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the subscription, and the
    /// errors of [`Connection::request`](crate::open_api::connection::Connection::request)
    /// otherwise.
    pub async fn subscribe_depth(
        &self,
        account: AccountId,
        symbols: &[SymbolId],
    ) -> Result<DepthStream, ConnectorError> {
        let stream = DepthStream {
            client: self.clone(),
            feed: Feed::new(self.connection()),
            account,
            symbols: symbols.iter().copied().collect(),
            books: BTreeMap::new(),
        };
        let request = |symbols: &[SymbolId]| ProtoOaSubscribeDepthQuotesReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            symbol_id: symbols.iter().map(|symbol| symbol.get()).collect(),
        };
        self.connection().request(&request(symbols)).await?;
        for symbol in symbols {
            self.connection()
                .register_replay(depth_key(account, *symbol), &request(&[*symbol]))
                .await?;
        }
        Ok(stream)
    }

    /// Ends the subscription to the market depth of the given symbols.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the request, and the errors
    /// of [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn unsubscribe_depth(
        &self,
        account: AccountId,
        symbols: &[SymbolId],
    ) -> Result<(), ConnectorError> {
        let request = ProtoOaUnsubscribeDepthQuotesReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            symbol_id: symbols.iter().map(|symbol| symbol.get()).collect(),
        };
        self.connection().request(&request).await?;
        for symbol in symbols {
            self.connection()
                .remove_replay(depth_key(account, *symbol))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{DepthBook, DepthQuote, DepthQuoteId, DepthUpdate};
    use crate::common::errors::ValueError;
    use crate::common::ids::SymbolId;
    use crate::open_api::messages::{ProtoOaDepthEvent, ProtoOaDepthQuote};
    use crate::open_api::quotes::QuoteSide;

    fn bid(id: u64, price: u64, size: u64) -> ProtoOaDepthQuote {
        ProtoOaDepthQuote {
            id,
            size,
            bid: Some(price),
            ask: None,
        }
    }

    fn ask(id: u64, price: u64, size: u64) -> ProtoOaDepthQuote {
        ProtoOaDepthQuote {
            id,
            size,
            bid: None,
            ask: Some(price),
        }
    }

    fn update(new: Vec<ProtoOaDepthQuote>, deleted: Vec<u64>) -> DepthUpdate {
        let event = ProtoOaDepthEvent {
            payload_type: None,
            ctid_trader_account_id: 7,
            symbol_id: 1,
            new_quotes: new,
            deleted_quotes: deleted,
        };
        DepthUpdate::parse(SymbolId::new(1).expect("symbol id"), event).expect("update")
    }

    #[test]
    fn depth_quote_is_parsed_from_the_wire() {
        let quote = DepthQuote::try_from(bid(3, 108_512, 150_000)).expect("quote");
        assert_eq!(quote.id, DepthQuoteId::new(3));
        assert_eq!(quote.id.get(), 3);
        assert_eq!(quote.id.to_string(), "3");
        assert_eq!(quote.side, QuoteSide::Bid);
        assert_eq!(quote.price.to_string(), "1.08512");
        assert_eq!(quote.size.to_string(), "1500.00");
        let quote = DepthQuote::try_from(ask(4, 108_520, 0)).expect("quote");
        assert_eq!(quote.side, QuoteSide::Ask);
        assert_eq!(quote.size.to_string(), "0.00");
    }

    #[test]
    fn depth_quotes_without_one_side_or_with_a_huge_size_are_rejected() {
        let side = Err(ValueError::OutOfRange {
            kind: "depth quote side",
        });
        let neither = ProtoOaDepthQuote {
            id: 1,
            size: 1,
            bid: None,
            ask: None,
        };
        assert_eq!(DepthQuote::try_from(neither), side);
        let both = ProtoOaDepthQuote {
            ask: Some(2),
            ..bid(1, 1, 1)
        };
        assert_eq!(DepthQuote::try_from(both), side);
        assert_eq!(
            DepthQuote::try_from(bid(1, 1, u64::MAX)),
            Err(ValueError::OutOfRange { kind: "depth size" })
        );
    }

    #[test]
    fn book_applies_new_replaced_and_deleted_entries() {
        let mut book = DepthBook::new();
        assert!(book.is_empty());
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
        book.apply(&update(
            vec![
                bid(1, 100, 10),
                bid(2, 99, 20),
                ask(3, 102, 30),
                ask(4, 103, 40),
            ],
            Vec::new(),
        ));
        assert_eq!(book.len(), 4);
        let ids = |quotes: Vec<DepthQuote>| quotes.iter().map(|q| q.id.get()).collect::<Vec<_>>();
        assert_eq!(ids(book.bids()), vec![1, 2]);
        assert_eq!(ids(book.asks()), vec![3, 4]);
        assert_eq!(
            book.best_bid().map(|p| p.to_string()).as_deref(),
            Some("0.00100")
        );
        assert_eq!(
            book.best_ask().map(|p| p.to_string()).as_deref(),
            Some("0.00102")
        );
        book.apply(&update(vec![bid(2, 101, 5), ask(5, 102, 1)], vec![3, 9]));
        assert_eq!(ids(book.bids()), vec![2, 1]);
        assert_eq!(ids(book.asks()), vec![5, 4]);
        assert_eq!(
            book.best_bid().map(|p| p.to_string()).as_deref(),
            Some("0.00101")
        );
        book.apply(&update(Vec::new(), vec![1, 2, 4, 5]));
        assert!(book.is_empty());
    }
}
