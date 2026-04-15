//! Reads the margin call thresholds of an account and the changes of its balance over the
//! last four weeks.
//!
//! ```sh
//! cargo run --example open_api_margin --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use ctrader_connector::common::time::{TimeRange, Timestamp};

use support::Failure;
use support::guard::Access;
use support::open_api::session;

const SPAN: Duration = Duration::from_secs(28 * 86_400);

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session(Access::ReadOnly).await?;
    let calls = client.margin_calls(account).await?;
    let weeks = TimeRange::ending_at(Timestamp::now(), SPAN);
    let flows = client.cash_flow_history(account, weeks).await?;
    client.shutdown().await;
    println!("{} margin calls", calls.len());
    for call in &calls {
        println!(
            "margin call: {:?} at a margin level of {}",
            call.level, call.threshold
        );
    }
    println!(
        "{} changes of the balance in the last four weeks",
        flows.len()
    );
    for flow in &flows {
        println!(
            "cash flow: {:?} of {}, balance {} afterwards, at {}",
            flow.kind, flow.amount, flow.balance, flow.at
        );
    }
    Ok(())
}
