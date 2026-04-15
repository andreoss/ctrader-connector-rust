//! Subscribes to the spot quotes of a symbol, prints a few and ends the subscription.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_quotes --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::decimals::Price;

use support::guard::Access;
use support::open_api::{session, symbol};
use support::{Failure, QUOTES, within};

const QUOTES_SHOWN: usize = 5;

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "-".to_owned(), |price| price.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session(Access::ReadOnly).await?;
    let (symbol, name) = symbol(&client, account).await?;
    let mut quotes = client.subscribe_quotes(account, &[symbol]).await?;
    for _ in 0..QUOTES_SHOWN {
        let quote = within(QUOTES, "a quote", quotes.next()).await??;
        println!(
            "{name} bid {} ask {} at {}",
            shown(quote.bid),
            shown(quote.ask),
            quote
                .timestamp
                .map_or_else(|| "-".to_owned(), |moment| moment.to_string())
        );
    }
    quotes.unsubscribe().await?;
    println!("subscription ended after {QUOTES_SHOWN} quotes");
    client.shutdown().await;
    Ok(())
}
