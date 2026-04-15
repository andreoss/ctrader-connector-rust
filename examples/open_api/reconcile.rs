//! Reads the open positions and the waiting orders of an account.
//!
//! ```sh
//! cargo run --example open_api_reconcile --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::decimals::Price;

use support::Failure;
use support::guard::Access;
use support::open_api::session;

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "none".to_owned(), |price| price.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session(Access::ReadOnly).await?;
    let reconciled = client.reconcile(account).await?;
    client.shutdown().await;
    println!(
        "{} open positions and {} waiting orders",
        reconciled.positions.len(),
        reconciled.orders.len()
    );
    for position in &reconciled.positions {
        println!(
            "position: {:?} {} of symbol {} at {}, stop loss {} take profit {}, label {:?}",
            position.side,
            position.volume,
            position.symbol,
            shown(position.price),
            shown(position.stop_loss),
            shown(position.take_profit),
            position.label
        );
    }
    for order in &reconciled.orders {
        println!(
            "order: {:?} {:?} {} of symbol {}, limit {} stop {}, {:?}, label {:?}",
            order.kind,
            order.side,
            order.volume,
            order.symbol,
            shown(order.limit_price),
            shown(order.stop_price),
            order.time_in_force,
            order.label
        );
    }
    Ok(())
}
