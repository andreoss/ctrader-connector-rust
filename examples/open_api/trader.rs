//! Reads the details of a trading account: balance, deposit currency, leverage and kind.
//!
//! ```sh
//! cargo run --example open_api_trader --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use support::Failure;
use support::guard::Access;
use support::open_api::session;

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session(Access::ReadOnly).await?;
    let trader = client.trader(account).await?;
    client.shutdown().await;
    println!("balance: {} {}", trader.balance, trader.currency);
    println!(
        "leverage: {}",
        trader.leverage.map_or_else(
            || "not stated".to_owned(),
            |leverage| format!("1:{leverage}")
        )
    );
    println!(
        "account: {:?}, access {:?}, swap free {}, limited risk {}",
        trader.account_type, trader.access_rights, trader.swap_free, trader.limited_risk
    );
    Ok(())
}
