//! Subscribes to the market depth of a symbol and prints the best levels of the book rebuilt
//! from the updates.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_depth --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::open_api::depth::DepthQuote;

use support::guard::Access;
use support::open_api::{session, symbol};
use support::{Failure, QUOTES, within};

const UPDATES: usize = 5;
const LEVELS: usize = 3;

fn levels(quotes: &[DepthQuote]) -> String {
    quotes
        .iter()
        .take(LEVELS)
        .map(|quote| format!("{} x {}", quote.price, quote.size))
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session(Access::ReadOnly).await?;
    let (symbol, name) = symbol(&client, account).await?;
    let mut depth = client.subscribe_depth(account, &[symbol]).await?;
    let filled = within(QUOTES, "a book with both sides", async {
        let mut updates = 0_usize;
        loop {
            depth.next().await?;
            updates += 1;
            let both = depth
                .book(symbol)
                .is_some_and(|book| book.best_bid().is_some() && book.best_ask().is_some());
            if updates >= UPDATES && both {
                return Ok::<_, Failure>(updates);
            }
        }
    })
    .await;
    let book = depth.book(symbol).cloned().unwrap_or_default();
    depth.unsubscribe().await?;
    client.shutdown().await;
    let updates = filled??;
    println!(
        "{name} after {updates} updates: {} bids and {} asks",
        book.bids().len(),
        book.asks().len()
    );
    println!("best bids: {}", levels(&book.bids()));
    println!("best asks: {}", levels(&book.asks()));
    Ok(())
}
