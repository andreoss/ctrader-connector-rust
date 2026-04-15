//! Places, amends, cancels and closes orders on a demo account and leaves nothing behind.
//!
//! The example buys the smallest volume at market with a stop loss and a take profit, places
//! a limit, a stop and a stop-limit order far from the market, amends the limit order and the
//! protection of the position, cancels the three waiting orders and closes the position.
//! Everything carries a label of this run, and whatever still carries it at the end, also
//! after a failure, is cancelled or closed over a new connection.
//!
//! `BROKER_EXAMPLE_SYMBOL` names the symbol; the default is `BTCUSD`.
//!
//! ```sh
//! cargo run --example open_api_trading --features open-api
//! ```

#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use ctrader_connector::common::decimals::Price;
use ctrader_connector::common::ids::AccountId;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::symbols::Symbol;
use ctrader_connector::open_api::trading::{
    ExecutionType, Label, NewOrder, Order, OrderAmendment, Position, PositionAmendment, TradeSide,
};

use support::guard::Access;
use support::open_api::{execution_of, market, session, sweep, symbol_details};
use support::{Failure, run_label, scaled, seconds_from_now};

const SLIPPAGE_POINTS: u32 = 40;
const PROTECTION_ROUNDS: usize = 5;

fn shown(price: Option<Price>) -> String {
    price.map_or_else(|| "none".to_owned(), |price| price.to_string())
}

struct Desk {
    client: OpenApiClient,
    account: AccountId,
    symbol: Symbol,
    label: Label,
}

impl Desk {
    async fn place(&self, order: NewOrder) -> Result<Order, Failure> {
        let accepted = self
            .client
            .place_order(&order.label(self.label.clone()), &self.symbol)
            .await?;
        accepted
            .order
            .ok_or_else(|| Failure::new("the acceptance names no order"))
    }

    async fn open(&self, bid: Price) -> Result<Position, Failure> {
        let mut executions = self.client.executions(self.account);
        let volume = self
            .symbol
            .min_volume
            .ok_or_else(|| Failure::new("the symbol states no smallest volume"))?;
        let distance = scaled(bid, 20, self.symbol.digits)?;
        let order = NewOrder::market(self.account, self.symbol.id, TradeSide::Buy, volume)
            .stop_loss_distance(distance)
            .take_profit_distance(distance);
        let accepted = self.place(order).await?;
        let filled = execution_of(&mut executions, accepted.id, ExecutionType::Filled).await?;
        let opened = filled
            .position
            .ok_or_else(|| Failure::new("the fill names no position"))?;
        let position = self.protected(&opened).await?;
        println!(
            "market order: bought {volume} at {}, stop loss {} and take profit {}",
            shown(position.price),
            shown(position.stop_loss),
            shown(position.take_profit)
        );
        Ok(position)
    }

    async fn protected(&self, opened: &Position) -> Result<Position, Failure> {
        for round in 0..PROTECTION_ROUNDS {
            if round > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let reconciled = self.client.reconcile(self.account).await?;
            let read = reconciled
                .positions
                .into_iter()
                .find(|position| position.id == opened.id);
            if let Some(position) = read.filter(|position| position.stop_loss.is_some()) {
                return Ok(position);
            }
        }
        Err(Failure::new("the position carries no stop loss"))
    }

    async fn pending(&self, bid: Price, ask: Price) -> Result<Vec<Order>, Failure> {
        let (account, symbol, digits) = (self.account, self.symbol.id, self.symbol.digits);
        let volume = self
            .symbol
            .min_volume
            .ok_or_else(|| Failure::new("the symbol states no smallest volume"))?;
        let below = scaled(bid, 50, digits)?;
        let above = scaled(ask, 150, digits)?;
        let limit = NewOrder::limit(account, symbol, TradeSide::Buy, volume, below)
            .good_till(seconds_from_now(3_600)?);
        let stop = NewOrder::stop(account, symbol, TradeSide::Buy, volume, above);
        let stop_limit = NewOrder::stop_limit(
            account,
            symbol,
            TradeSide::Buy,
            volume,
            above,
            SLIPPAGE_POINTS,
        );
        let mut placed = Vec::new();
        for order in [limit, stop, stop_limit] {
            let order = self.place(order).await?;
            println!(
                "{:?} order: waiting to buy {} at limit {} stop {} slippage {:?}",
                order.kind,
                order.volume,
                shown(order.limit_price),
                shown(order.stop_price),
                order.slippage_points
            );
            placed.push(order);
        }
        Ok(placed)
    }

    async fn amend(&self, limit: &Order, position: &Position, bid: Price) -> Result<(), Failure> {
        let digits = self.symbol.digits;
        let lower = scaled(bid, 45, digits)?;
        let amendment = OrderAmendment::of(self.account, limit)
            .price(lower)
            .stop_loss(Some(scaled(bid, 40, digits)?));
        let replaced = self.client.amend_order(&amendment, &self.symbol).await?;
        let order = replaced.order.as_ref();
        println!(
            "amended limit order: {:?}, limit {} stop loss {}",
            replaced.kind,
            shown(order.and_then(|order| order.limit_price)),
            shown(order.and_then(|order| order.stop_loss))
        );
        let amendment = PositionAmendment::of(self.account, position)
            .stop_loss(Some(scaled(bid, 60, digits)?))
            .take_profit(None);
        let protected = self.client.amend_position(&amendment, &self.symbol).await?;
        let position = protected.position.as_ref();
        println!(
            "amended position: {:?}, stop loss {} take profit {}",
            protected.kind,
            shown(position.and_then(|position| position.stop_loss)),
            shown(position.and_then(|position| position.take_profit))
        );
        Ok(())
    }

    async fn close(&self, position: &Position) -> Result<(), Failure> {
        let mut executions = self.client.executions(self.account);
        let accepted = self
            .client
            .close_position(self.account, position, position.volume, &self.symbol)
            .await?;
        let closing = accepted
            .order
            .ok_or_else(|| Failure::new("the acceptance names no closing order"))?;
        let filled = execution_of(&mut executions, closing.id, ExecutionType::Filled).await?;
        println!(
            "closed position: sold {} at {}",
            position.volume,
            shown(filled.deal.and_then(|deal| deal.execution_price))
        );
        Ok(())
    }

    async fn run(&self) -> Result<(), Failure> {
        let (bid, ask) = market(&self.client, self.account, self.symbol.id).await?;
        println!("market: bid {bid} ask {ask}");
        let position = self.open(bid).await?;
        let pending = self.pending(bid, ask).await?;
        if let Some(limit) = pending.first() {
            self.amend(limit, &position, bid).await?;
        }
        for order in &pending {
            let cancelled = self.client.cancel_order(self.account, order.id).await?;
            println!("cancelled {:?} order: {:?}", order.kind, cancelled.kind);
        }
        self.close(&position).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let (client, account) = session(Access::Trading).await?;
    let (symbol, name) = symbol_details(&client, account).await?;
    let label = run_label("trading");
    println!("trading {name} under the label {label}");
    let desk = Desk {
        client,
        account,
        symbol,
        label: Label::new(label.clone())?,
    };
    let outcome = desk.run().await;
    desk.client.shutdown().await;
    let swept = sweep(&label).await;
    outcome?;
    swept?;
    println!("the account holds no order and no position of this run");
    Ok(())
}
