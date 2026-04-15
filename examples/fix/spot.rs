//! Follows the bid and the offer of a symbol over the quote session.
//!
//! The server sends every change of the price as a full snapshot. A market that is closed
//! sends no price, and the example fails after ninety seconds without one.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example fix_spot --features fix
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::decimals::Price;
use ctrader_connector::fix::session::SessionKind;

use support::fix::{leave, session, symbol};
use support::guard::Access;
use support::{Failure, QUOTES, STEP, within};

const UPDATES: usize = 3;

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "none".to_owned(), |price| price.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let quote = session(SessionKind::Quote, Access::ReadOnly).await?;
    let security = symbol(&quote).await?;
    let mut spots = within(STEP, "the first prices", quote.subscribe_spot(security.id)).await??;
    let first = spots.last();
    println!(
        "{}: bid {} offer {}",
        security.name,
        shown(first.bid),
        shown(first.offer)
    );
    for _ in 0..UPDATES {
        let next = within(QUOTES, "the next prices", spots.next()).await??;
        println!(
            "{}: bid {} offer {}",
            security.name,
            shown(next.bid),
            shown(next.offer)
        );
    }
    spots.unsubscribe().await?;
    leave(&quote).await?;
    println!("unsubscribed and logged out");
    Ok(())
}
