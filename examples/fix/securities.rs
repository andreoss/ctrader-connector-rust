//! Lists the symbols of the account over the quote session.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol whose entry is printed; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example fix_securities --features fix
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::fix::session::SessionKind;

use support::fix::{leave, session};
use support::{Failure, STEP, symbol_name, within};

const SHOWN: usize = 5;

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let quote = session(SessionKind::Quote).await?;
    let securities = within(STEP, "the security list", quote.securities()).await?;
    leave(&quote).await?;
    let securities = securities?;
    println!("{} symbols; the first:", securities.len());
    for security in securities.iter().take(SHOWN) {
        println!(
            "symbol {}: {} with {} digits",
            security.id, security.name, security.digits
        );
    }
    let name = symbol_name();
    match securities.iter().find(|security| security.name == name) {
        Some(security) => println!(
            "{name} is symbol {} with {} digits",
            security.id, security.digits
        ),
        None => println!("no symbol is named {name}"),
    }
    Ok(())
}
