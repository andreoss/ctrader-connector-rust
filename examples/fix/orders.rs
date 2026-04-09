//! Places, replaces and cancels a limit order over the trade session.
//!
//! The order waits far below the market, so it is not filled. With `--market`, or with
//! `BROKER_EXAMPLE_MARKET=1`, the example also buys at market, finds the position among the
//! open positions and closes it with an opposite order that names it. Whatever the run
//! created and the account still holds at the end, also after a failure, is cancelled or
//! closed over a new session.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//! `BROKER_EXAMPLE_QUANTITY` is the quantity in units of the base asset; the default is
//! `0.01`.
//!
//! ```sh
//! cargo run --example fix_orders --features fix
//! cargo run --example fix_orders --features fix -- --market
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::fix::amend::Replacement;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::Side;
use ctrader_connector::fix::securities::Security;
use ctrader_connector::fix::session::SessionKind;
use ctrader_connector::fix::trading::NewOrder;

use support::fix::{Ledger, clear, leave, quantity, session, symbol_and_bid};
use support::{Failure, STEP, flag, run_label, scaled, within};

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "none".to_owned(), |price| price.to_string())
}

async fn waiting_order(
    trade: &FixClient,
    ledger: &mut Ledger,
    security: &Security,
    quantity: Volume,
    bid: Price,
    label: &str,
) -> Result<(), Failure> {
    let below = scaled(bid, 50, security.digits)?;
    let order = NewOrder::limit(security.id, Side::Buy, quantity, below).label(label);
    let placed = within(STEP, "the report of the order", trade.place(order)).await??;
    ledger.note(&placed);
    println!(
        "placed {}: {:?}, waiting to buy {} at {}",
        placed.cl_ord_id,
        placed.status,
        placed.quantity,
        shown(placed.price)
    );
    let lower = scaled(bid, 45, security.digits)?;
    let terms = Replacement::new(quantity).price(lower);
    let replaced = within(
        STEP,
        "the report of the replacement",
        trade.replace(&placed.cl_ord_id, terms),
    )
    .await??;
    println!(
        "replaced by {}: {:?}, now at {}",
        replaced.cl_ord_id,
        replaced.exec_type,
        shown(replaced.price)
    );
    let cancelled = within(
        STEP,
        "the report of the cancellation",
        trade.cancel(&placed.cl_ord_id),
    )
    .await??;
    println!(
        "cancelled under {}: {:?}",
        placed.cl_ord_id, cancelled.status
    );
    Ok(())
}

async fn market_order(
    trade: &FixClient,
    ledger: &mut Ledger,
    security: &Security,
    quantity: Volume,
    label: &str,
) -> Result<(), Failure> {
    let order = NewOrder::market(security.id, Side::Buy, quantity).label(label);
    let bought = within(STEP, "the report of the market order", trade.place(order)).await??;
    ledger.note(&bought);
    println!(
        "market order: {:?}, bought {} at {}",
        bought.status,
        bought.quantity,
        shown(bought.average_price)
    );
    let opened = bought
        .position
        .ok_or_else(|| Failure::new("the fill names no position"))?;
    let held = within(STEP, "the open positions", trade.positions()).await??;
    let position = held
        .iter()
        .find(|position| position.id == opened)
        .ok_or_else(|| Failure::new("the position of the fill is not listed"))?;
    println!(
        "position: {:?} {} at {}",
        position.side,
        position.quantity,
        shown(position.entry_price)
    );
    let closing = NewOrder::market(security.id, Side::Sell, position.quantity)
        .label(label)
        .position(opened);
    let sold = within(
        STEP,
        "the report of the closing order",
        trade.place(closing),
    )
    .await??;
    println!(
        "closing order: {:?}, sold {} at {}",
        sold.status,
        sold.quantity,
        shown(sold.average_price)
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let with_market = flag("--market", "BROKER_EXAMPLE_MARKET");
    let quantity = quantity()?;
    let (security, bid) = symbol_and_bid().await?;
    let label = run_label("fix-orders");
    println!("trading {} at a bid of {bid}", security.name);
    let trade = session(SessionKind::Trade).await?;
    let mut ledger = Ledger::default();
    let mut outcome = waiting_order(&trade, &mut ledger, &security, quantity, bid, &label).await;
    if outcome.is_ok() && with_market {
        outcome = market_order(&trade, &mut ledger, &security, quantity, &label).await;
    }
    let left = leave(&trade).await;
    let cleared = clear(&ledger).await;
    outcome?;
    left?;
    cleared?;
    println!("the account holds no order and no position of this run");
    Ok(())
}
