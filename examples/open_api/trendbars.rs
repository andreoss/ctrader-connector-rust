//! Reads the latest trendbars of a symbol and then the bar that is forming now.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_trendbars --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use ctrader_connector::common::time::{TimeRange, Timestamp};
use ctrader_connector::open_api::trendbars::{Period, Trendbar, TrendbarsParams};

use support::open_api::{session, symbol};
use support::{Failure, QUOTES, within};

const BARS: usize = 5;

fn show(name: &str, bar: &Trendbar) {
    println!(
        "{name} {} bar at {}: open {} high {} low {} close {} volume {}",
        bar.period, bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume
    );
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session().await?;
    let (symbol, name) = symbol(&client, account).await?;
    let day = TimeRange::ending_at(Timestamp::now(), Duration::from_secs(86_400));
    let params = TrendbarsParams::new(account, symbol, Period::H1, day).count(BARS);
    let bars = client.trendbars(&params).await?;
    println!("the latest {} hourly bars of the last day:", bars.len());
    for bar in &bars {
        show(&name, bar);
    }
    let mut live = client
        .subscribe_live_trendbars(account, symbol, Period::M1)
        .await?;
    let forming = within(QUOTES, "the forming bar", live.next()).await;
    live.unsubscribe().await?;
    client.shutdown().await;
    println!("the bar that is forming now:");
    show(&name, &forming??);
    Ok(())
}
