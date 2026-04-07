//! Prices of the quote session: spot prices and the depth of the market.
//!
//! A subscription is a stream of its own. A [`SpotStream`] yields the best bid and offer of a
//! symbol each time the server reports them; a [`DepthStream`] keeps the [`Book`] of a symbol
//! up to date and yields it after every change. The server tags every report with the
//! identifier of the subscription it belongs to, so that streams of the same symbol do not
//! mix. It sends the best prices as full snapshots, and the depth as an empty snapshot
//! followed by changes.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationFix;
//! use ctrader_connector::common::ids::SymbolId;
//! use ctrader_connector::fix::client::FixClient;
//! use ctrader_connector::fix::session::SessionKind;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = FixClient::connect(ConfigurationFix::from_env()?, SessionKind::Quote).await?;
//! let mut spots = client.subscribe_spot(SymbolId::new(1)?).await?;
//! let quote = spots.next().await?;
//! println!("{:?} / {:?}", quote.bid, quote.offer);
//! spots.unsubscribe().await?;
//! client.logout().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;

use crate::common::decimals::{Price, Volume};
use crate::common::errors::{ConnectorError, FixError, TransportError};
use crate::common::ids::SymbolId;
use crate::fix::client::FixClient;
use crate::fix::messages::{
    BookSide, BookUpdate, MarketDataRequest, MarketDataSnapshot, MarketDepth, Message,
};
use crate::fix::session::Event;

const REQUEST_PREFIX: &str = "md";

/// Best prices of a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpotQuote {
    /// Symbol the prices belong to.
    pub symbol: SymbolId,
    /// Best bid, absent while the server reports none.
    pub bid: Option<Price>,
    /// Best offer, absent while the server reports none.
    pub offer: Option<Price>,
}

/// One entry of a book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookEntry {
    /// Identifier of the entry, by which the server removes it.
    pub id: String,
    /// Side of the entry.
    pub side: BookSide,
    /// Price.
    pub price: Price,
    /// Quantity offered at the price.
    pub size: Option<Volume>,
}

/// Depth of the market of one symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    symbol: SymbolId,
    entries: BTreeMap<String, BookEntry>,
}

impl Book {
    /// Creates the empty book of a symbol.
    #[must_use]
    pub const fn new(symbol: SymbolId) -> Self {
        Self {
            symbol,
            entries: BTreeMap::new(),
        }
    }

    /// Returns the symbol the book belongs to.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Returns the number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Tells whether the book holds no entry.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the bids, the highest price first.
    #[must_use]
    pub fn bids(&self) -> Vec<&BookEntry> {
        let mut bids = self.side(BookSide::Bid);
        bids.sort_by(|a, b| b.price.cmp(&a.price));
        bids
    }

    /// Returns the offers, the lowest price first.
    #[must_use]
    pub fn offers(&self) -> Vec<&BookEntry> {
        let mut offers = self.side(BookSide::Offer);
        offers.sort_by(|a, b| a.price.cmp(&b.price));
        offers
    }

    fn side(&self, side: BookSide) -> Vec<&BookEntry> {
        self.entries
            .values()
            .filter(|entry| entry.side == side)
            .collect()
    }

    /// Replaces the content of the book by a snapshot of the same symbol; a snapshot of
    /// another symbol is ignored.
    pub fn replace(&mut self, snapshot: &MarketDataSnapshot) {
        if snapshot.symbol != self.symbol {
            return;
        }
        self.entries.clear();
        for (position, entry) in snapshot.entries.iter().enumerate() {
            let id = entry
                .id
                .clone()
                .unwrap_or_else(|| format!("{}{position}", entry.side.as_str()));
            self.entries.insert(
                id.clone(),
                BookEntry {
                    id,
                    side: entry.side,
                    price: entry.price,
                    size: entry.size,
                },
            );
        }
    }

    /// Applies one change; a change of another symbol is ignored.
    pub fn apply(&mut self, update: &BookUpdate) {
        match update {
            BookUpdate::New {
                side,
                id,
                symbol,
                price,
                size,
            } if *symbol == self.symbol => {
                self.entries.insert(
                    id.clone(),
                    BookEntry {
                        id: id.clone(),
                        side: *side,
                        price: *price,
                        size: *size,
                    },
                );
            }
            BookUpdate::Delete { id, symbol } if *symbol == self.symbol => {
                self.entries.remove(id);
            }
            _ => {}
        }
    }
}

enum Step {
    Snapshot(MarketDataSnapshot),
    Changes(Vec<BookUpdate>),
}

#[derive(Debug)]
struct Feed {
    client: FixClient,
    events: broadcast::Receiver<Event>,
    md_req_id: String,
    symbol: SymbolId,
    depth: MarketDepth,
    seq: u64,
}

impl Feed {
    fn request(&self, subscribe: bool) -> Message {
        MarketDataRequest {
            md_req_id: self.md_req_id.clone(),
            subscribe,
            depth: self.depth,
            symbols: vec![self.symbol],
            sides: vec![BookSide::Bid, BookSide::Offer],
        }
        .into()
    }

    async fn open(
        client: &FixClient,
        symbol: SymbolId,
        depth: MarketDepth,
    ) -> Result<Self, ConnectorError> {
        let mut feed = Self {
            client: client.clone(),
            events: client.events(),
            md_req_id: client.next_id(REQUEST_PREFIX),
            symbol,
            depth,
            seq: 0,
        };
        feed.seq = client.send(feed.request(true)).await?;
        Ok(feed)
    }

    fn mine(&self, md_req_id: Option<&str>) -> bool {
        md_req_id == Some(self.md_req_id.as_str())
    }

    async fn step(&mut self) -> Result<Step, ConnectorError> {
        loop {
            let inbound = match self.events.recv().await {
                Ok(Event::Message(inbound)) => inbound,
                Ok(Event::Disconnected(error)) => return Err(error.into()),
                Ok(Event::LoggedOut { .. }) | Err(RecvError::Closed) => {
                    return Err(TransportError::Closed.into());
                }
                Ok(_) => continue,
                Err(RecvError::Lagged(skipped)) => {
                    return Err(ConnectorError::Lagged { skipped });
                }
            };
            match inbound.message {
                Message::MarketDataSnapshot(snapshot)
                    if self.mine(snapshot.md_req_id.as_deref())
                        && snapshot.symbol == self.symbol =>
                {
                    return Ok(Step::Snapshot(snapshot));
                }
                Message::MarketDataIncremental(changes)
                    if self.mine(changes.md_req_id.as_deref()) =>
                {
                    return Ok(Step::Changes(changes.entries));
                }
                Message::MarketDataReject(reject) if reject.md_req_id == self.md_req_id => {
                    return Err(FixError::MarketDataRejected {
                        reason: reject.reason,
                        text: reject.text,
                    }
                    .into());
                }
                Message::Reject(reject) if reject.ref_seq_num == self.seq => {
                    return Err(FixError::Rejected {
                        ref_seq_num: self.seq,
                        tag: reject.ref_tag_id,
                        reason: reject.reason,
                        text: reject.text,
                    }
                    .into());
                }
                Message::BusinessReject(reject)
                    if reject.ref_seq_num == Some(self.seq)
                        || reject.ref_id.as_deref() == Some(self.md_req_id.as_str()) =>
                {
                    return Err(FixError::BusinessRejected {
                        ref_id: reject.ref_id,
                        reason: reject.reason,
                        text: reject.text,
                    }
                    .into());
                }
                _ => {}
            }
        }
    }

    async fn first(&mut self) -> Result<Step, ConnectorError> {
        let limit = self.client.timeout();
        timeout(limit, self.step())
            .await
            .map_err(|_| ConnectorError::Timeout(limit))?
    }

    async fn close(&self) -> Result<(), ConnectorError> {
        self.client.send(self.request(false)).await.map(|_| ())
    }
}

/// Stream of the best prices of one symbol.
#[derive(Debug)]
pub struct SpotStream {
    feed: Feed,
    last: SpotQuote,
}

impl SpotStream {
    /// Returns the symbol the stream reports.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.feed.symbol
    }

    /// Returns the prices reported last.
    #[must_use]
    pub const fn last(&self) -> SpotQuote {
        self.last
    }

    fn take(&mut self, step: Step) -> Option<SpotQuote> {
        let Step::Snapshot(snapshot) = step else {
            return None;
        };
        let best = |side: BookSide| {
            snapshot
                .entries
                .iter()
                .find(|entry| entry.side == side)
                .map(|entry| entry.price)
        };
        self.last = SpotQuote {
            symbol: snapshot.symbol,
            bid: best(BookSide::Bid),
            offer: best(BookSide::Offer),
        };
        Some(self.last)
    }

    /// Waits for the next report of the prices.
    ///
    /// The wait has no deadline of its own: a market may stand still. The session notices a
    /// server that has gone silent and ends the wait with an error.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the session ends,
    /// [`ConnectorError::Lagged`] when the reader fell behind and reports were skipped, and
    /// [`ConnectorError::Fix`] when the server refuses the subscription.
    pub async fn next(&mut self) -> Result<SpotQuote, ConnectorError> {
        loop {
            let step = self.feed.step().await?;
            if let Some(quote) = self.take(step) {
                return Ok(quote);
            }
        }
    }

    /// Ends the subscription.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the request cannot be written and
    /// [`ConnectorError::Closed`] when the session has ended already.
    pub async fn unsubscribe(self) -> Result<(), ConnectorError> {
        self.feed.close().await
    }
}

/// Stream of the depth of the market of one symbol.
#[derive(Debug)]
pub struct DepthStream {
    feed: Feed,
    book: Book,
}

impl DepthStream {
    /// Returns the symbol the stream reports.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.feed.symbol
    }

    /// Returns the book as of the last change.
    #[must_use]
    pub const fn book(&self) -> &Book {
        &self.book
    }

    fn take(&mut self, step: &Step) {
        match step {
            Step::Snapshot(snapshot) => self.book.replace(snapshot),
            Step::Changes(changes) => {
                for change in changes {
                    self.book.apply(change);
                }
            }
        }
    }

    /// Waits for the next change and returns the book after it.
    ///
    /// The wait has no deadline of its own: a market may stand still. The session notices a
    /// server that has gone silent and ends the wait with an error.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the session ends,
    /// [`ConnectorError::Lagged`] when the reader fell behind, and [`ConnectorError::Fix`]
    /// when the server refuses the subscription. After changes were skipped the book is no
    /// longer that of the server; the subscription has to be opened anew.
    pub async fn next(&mut self) -> Result<&Book, ConnectorError> {
        let step = self.feed.step().await?;
        self.take(&step);
        Ok(&self.book)
    }

    /// Ends the subscription.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the request cannot be written and
    /// [`ConnectorError::Closed`] when the session has ended already.
    pub async fn unsubscribe(self) -> Result<(), ConnectorError> {
        self.feed.close().await
    }
}

impl FixClient {
    /// Subscribes to the best prices of a symbol and waits for the first report.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Fix`] when the server refuses the subscription,
    /// [`ConnectorError::Timeout`] when no price arrives in time, and
    /// [`ConnectorError::Transport`] or [`ConnectorError::Closed`] when the session ends.
    pub async fn subscribe_spot(&self, symbol: SymbolId) -> Result<SpotStream, ConnectorError> {
        let mut stream = SpotStream {
            feed: Feed::open(self, symbol, MarketDepth::Spot).await?,
            last: SpotQuote {
                symbol,
                bid: None,
                offer: None,
            },
        };
        loop {
            let step = stream.feed.first().await?;
            if stream.take(step).is_some() {
                return Ok(stream);
            }
        }
    }

    /// Subscribes to the depth of the market of a symbol and waits for the first book, which
    /// the server sends empty; the entries follow as changes.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`FixClient::subscribe_spot`].
    pub async fn subscribe_depth(&self, symbol: SymbolId) -> Result<DepthStream, ConnectorError> {
        let mut stream = DepthStream {
            feed: Feed::open(self, symbol, MarketDepth::Full).await?,
            book: Book::new(symbol),
        };
        let step = stream.feed.first().await?;
        stream.take(&step);
        Ok(stream)
    }
}
