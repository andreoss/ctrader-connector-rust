//! Reads the tick history of both sides of a symbol over the last minutes.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_ticks --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use ctrader_connector::common::time::{TimeRange, Timestamp};
use ctrader_connector::open_api::quotes::QuoteSide;
use ctrader_connector::open_api::ticks::TickDataParams;

use support::Failure;
use support::open_api::{session, symbol};

const SPAN: Duration = Duration::from_secs(300);

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session().await?;
    let (symbol, name) = symbol(&client, account).await?;
    let range = TimeRange::ending_at(Timestamp::now(), SPAN);
    for side in [QuoteSide::Bid, QuoteSide::Ask] {
        let params = TickDataParams::new(account, symbol, side, range);
        let ticks = client.tick_data(&params).await?;
        match (ticks.first(), ticks.last()) {
            (Some(first), Some(last)) => println!(
                "{name} {side:?}: {} ticks in {SPAN:?}, from {} at {} to {} at {}",
                ticks.len(),
                first.price,
                first.timestamp,
                last.price,
                last.timestamp
            ),
            _ => println!("{name} {side:?}: no tick in {SPAN:?}"),
        }
    }
    client.shutdown().await;
    Ok(())
}
