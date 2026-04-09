//! Reads the deals and the orders of an account over the last day.
//!
//! ```sh
//! cargo run --example open_api_history --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use ctrader_connector::common::time::{TimeRange, Timestamp};

use support::Failure;
use support::open_api::session;

const SPAN: Duration = Duration::from_secs(86_400);
const SHOWN: usize = 3;

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session().await?;
    let day = TimeRange::ending_at(Timestamp::now(), SPAN);
    let deals = client.deal_history(account, day).await?;
    let orders = client.order_history(account, day).await?;
    client.shutdown().await;
    println!("{} deals in the last day; the latest:", deals.len());
    for deal in deals.iter().rev().take(SHOWN) {
        println!(
            "deal: {:?} {} of symbol {} at {}, {:?}, executed at {}",
            deal.side,
            deal.filled_volume,
            deal.symbol,
            deal.execution_price
                .map_or_else(|| "no price".to_owned(), |price| price.to_string()),
            deal.status,
            deal.executed
        );
    }
    println!("{} orders in the last day; the latest:", orders.len());
    for order in orders.iter().rev().take(SHOWN) {
        println!(
            "order: {:?} {:?} {} of symbol {}, {:?}",
            order.kind, order.side, order.volume, order.symbol, order.status
        );
    }
    Ok(())
}
