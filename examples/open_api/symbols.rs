//! Reads the reference data of an account: its assets, its symbols and the trading details
//! of one symbol.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_symbols --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::open_api::symbols::SymbolsParams;

use support::Failure;
use support::open_api::{session, symbol_details};

fn shown<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "not stated".to_owned(), |value| value.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session().await?;
    let assets = client.assets(account).await?;
    println!("{} assets", assets.len());
    let list = client
        .symbols(SymbolsParams::new(account).include_archived(true))
        .await?;
    let enabled = list.symbols.iter().filter(|symbol| symbol.enabled).count();
    println!(
        "{} symbols, {enabled} of them enabled, and {} archived ones",
        list.symbols.len(),
        list.archived.len()
    );
    let (symbol, name) = symbol_details(&client, account).await?;
    println!(
        "{name} is symbol {}: {} digits, pip at position {}, trading {:?}",
        symbol.id, symbol.digits, symbol.pip_position, symbol.trading_mode
    );
    println!(
        "{name} trades volumes from {} to {} in steps of {}; one lot is {}",
        shown(symbol.min_volume),
        shown(symbol.max_volume),
        shown(symbol.step_volume),
        shown(symbol.lot_size)
    );
    client.shutdown().await;
    Ok(())
}
