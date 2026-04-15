//! Asks the trade session for the state of one order, of all waiting orders and of the
//! open positions.
//!
//! To have an order to ask about, the example places a limit order far below the market
//! and cancels it afterwards. Whatever the run created and the account still holds at the
//! end, also after a failure, is cancelled over a new session.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//! `BROKER_EXAMPLE_QUANTITY` is the quantity in units of the base asset; the default is
//! `0.01`.
//!
//! ```sh
//! cargo run --example fix_reports --features fix
//! ```

#[path = "../support/mod.rs"]
mod support;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::Side;
use ctrader_connector::fix::securities::Security;
use ctrader_connector::fix::session::SessionKind;
use ctrader_connector::fix::trading::NewOrder;

use support::fix::{Ledger, clear, leave, quantity, session, symbol_and_bid};
use support::guard::Access;
use support::{Failure, STEP, run_label, scaled, within};

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "none".to_owned(), |price| price.to_string())
}

async fn report(
    trade: &FixClient,
    ledger: &mut Ledger,
    security: &Security,
    quantity: Volume,
    bid: Price,
) -> Result<(), Failure> {
    let below = scaled(bid, 50, security.digits)?;
    let order =
        NewOrder::limit(security.id, Side::Buy, quantity, below).label(run_label("fix-reports"));
    let placed = within(STEP, "the report of the order", trade.place(order)).await??;
    ledger.note(&placed);
    let status = within(
        STEP,
        "the state of the order",
        trade.order_status(&placed.cl_ord_id),
    )
    .await??;
    println!(
        "order status of {}: {:?}, {:?} {} at {}",
        status.cl_ord_id,
        status.status,
        status.side,
        status.quantity,
        shown(status.price)
    );
    let waiting = within(STEP, "the waiting orders", trade.open_orders()).await??;
    println!("mass status: {} waiting orders", waiting.len());
    for order in &waiting {
        println!(
            "waiting order: {:?} {:?} {} of symbol {}, limit {} stop {}",
            order.kind,
            order.side,
            order.quantity,
            order.symbol,
            shown(order.price),
            shown(order.stop_price)
        );
    }
    let held = within(STEP, "the open positions", trade.positions()).await??;
    println!("positions: {} open", held.len());
    for position in &held {
        println!(
            "position: {:?} {} of symbol {} at {}",
            position.side,
            position.quantity,
            position.symbol,
            shown(position.entry_price)
        );
    }
    let cancelled = within(
        STEP,
        "the report of the cancellation",
        trade.cancel(&placed.cl_ord_id),
    )
    .await??;
    println!("cancelled the order: {:?}", cancelled.status);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let quantity = quantity()?;
    let (security, bid) = symbol_and_bid(Access::Trading).await?;
    let trade = session(SessionKind::Trade, Access::Trading).await?;
    let mut ledger = Ledger::default();
    let outcome = report(&trade, &mut ledger, &security, quantity, bid).await;
    let left = leave(&trade).await;
    let cleared = clear(&ledger).await;
    outcome?;
    left?;
    cleared?;
    println!("the account holds no order of this run");
    Ok(())
}
