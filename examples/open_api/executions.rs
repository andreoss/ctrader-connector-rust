//! Follows the life of an order on the stream of execution events.
//!
//! The example places a limit order far from the market, cancels it and prints the events
//! the server reports for it. A second cancellation is refused, which the stream reports as
//! an order error. Whatever still carries the label of this run at the end is cancelled.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_executions --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::errors::ConnectorError;
use ctrader_connector::common::ids::AccountId;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::executions::TradeEvent;
use ctrader_connector::open_api::symbols::Symbol;
use ctrader_connector::open_api::trading::{ExecutionType, Label, NewOrder, TradeSide};

use support::open_api::{market, session, sweep, symbol_details};
use support::{Failure, STEP, run_label, scaled, within};

async fn follow(
    client: &OpenApiClient,
    account: AccountId,
    symbol: &Symbol,
    label: Label,
) -> Result<(), Failure> {
    let (bid, _) = market(client, account, symbol.id).await?;
    let volume = symbol
        .min_volume
        .ok_or_else(|| Failure::new("the symbol states no smallest volume"))?;
    let mut executions = client.executions(account);
    let below = scaled(bid, 50, symbol.digits)?;
    let order = NewOrder::limit(account, symbol.id, TradeSide::Buy, volume, below).label(label);
    let placed = client
        .place_order(&order, symbol)
        .await?
        .order
        .ok_or_else(|| Failure::new("the acceptance names no order"))?;
    client.cancel_order(account, placed.id).await?;
    let refused = client.cancel_order(account, placed.id).await;
    if !matches!(refused, Err(ConnectorError::Server(_))) {
        return Err(Failure::new("the second cancellation was not refused"));
    }
    let mut cancelled = false;
    loop {
        let event = within(STEP, "the next event of the order", executions.next()).await??;
        if event.order() != Some(placed.id) {
            continue;
        }
        match event {
            TradeEvent::Execution(execution) => {
                println!(
                    "execution event: {:?}, order is {:?}",
                    execution.kind,
                    execution.order.as_ref().map(|order| order.status)
                );
                cancelled |= execution.kind == ExecutionType::Cancelled;
            }
            TradeEvent::OrderError(error) => {
                println!("order error event: {}", error.code);
                if cancelled {
                    return Ok(());
                }
            }
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session().await?;
    let (symbol, name) = symbol_details(&client, account).await?;
    let label = run_label("executions");
    println!("following a limit order of {name} under the label {label}");
    let outcome = follow(&client, account, &symbol, Label::new(label.clone())?).await;
    client.shutdown().await;
    let swept = sweep(&label).await;
    outcome?;
    swept?;
    println!("the account holds no order and no position of this run");
    Ok(())
}
