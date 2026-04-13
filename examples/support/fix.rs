//! Steps the FIX examples share: the logged-on session, the symbol and its price.

use ctrader_connector::common::config::ConfigurationFix;
use std::time::Duration;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::ids::{OrderId, PositionId};
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::Side;
use ctrader_connector::fix::securities::Security;
use ctrader_connector::fix::session::SessionKind;
use ctrader_connector::fix::trading::{NewOrder, OrderReport};

use super::demo::demo_only;
use super::{Failure, QUOTES, STEP, setting, symbol_name, veil, within};

/// Variable naming the quantity the trading examples order, in units of the base asset.
pub const ENV_QUANTITY: &str = "BROKER_EXAMPLE_QUANTITY";

/// Quantity the trading examples order unless [`ENV_QUANTITY`] names another: the smallest
/// one of the default symbol.
pub const DEFAULT_QUANTITY: &str = "0.01";

const CLEAR_ROUNDS: usize = 10;
const SESSION_ROUNDS: usize = 5;

/// Reads the configuration from the environment and refuses any account but a demo one.
///
/// # Errors
///
/// Returns a [`Failure`] when a variable is absent or rejected, or when the sender identifier
/// or the host names another environment than demo; see [`demo_only`].
pub fn configuration() -> Result<ConfigurationFix, Failure> {
    let configuration = ConfigurationFix::from_env()?;
    veil(configuration.sender_comp_id());
    veil(configuration.username());
    demo_only(configuration.sender_comp_id(), configuration.host()).map_err(|reason| {
        Failure::new(format!(
            "the examples run against a demo account only: {reason}"
        ))
    })?;
    Ok(configuration)
}

/// Logs on to one session of the demo account the environment names.
///
/// Every FIX example reaches the server through this function, so none of them logs on to
/// an account that is not a demo one.
///
/// # Errors
///
/// Returns a [`Failure`] when the configuration is refused by [`configuration`] or the logon
/// fails.
pub async fn session(kind: SessionKind) -> Result<FixClient, Failure> {
    let configuration = configuration()?;
    let logon = Box::pin(FixClient::connect(configuration, kind));
    Ok(within(STEP, "the logon", logon).await??)
}

/// Logs out and waits for the session to end.
///
/// # Errors
///
/// Returns a [`Failure`] when the logout fails or the session does not end in time.
pub async fn leave(client: &FixClient) -> Result<(), Failure> {
    client.logout().await?;
    within(STEP, "the end of the session", client.closed()).await
}

/// Finds the symbol the example works with in the security list of the quote session.
///
/// # Errors
///
/// Returns a [`Failure`] when the list cannot be read or names no such symbol.
pub async fn symbol(quote: &FixClient) -> Result<Security, Failure> {
    let name = symbol_name();
    let securities = within(STEP, "the security list", quote.securities()).await??;
    securities
        .into_iter()
        .find(|security| security.name == name)
        .ok_or_else(|| Failure::new(format!("the server lists no symbol named {name}")))
}

/// Reads the current bid of the symbol over a quote session of its own.
///
/// # Errors
///
/// Returns a [`Failure`] when the session, the symbol or the price cannot be had.
pub async fn symbol_and_bid() -> Result<(Security, Price), Failure> {
    let quote = session(SessionKind::Quote).await?;
    let security = symbol(&quote).await?;
    let mut spots = within(STEP, "the first prices", quote.subscribe_spot(security.id)).await??;
    let bid = within(QUOTES, "a bid", async {
        loop {
            if let Some(bid) = spots.last().bid {
                return Ok::<_, Failure>(bid);
            }
            spots.next().await?;
        }
    })
    .await;
    spots.unsubscribe().await?;
    leave(&quote).await?;
    Ok((security, bid??))
}

/// Returns the quantity the trading examples order.
///
/// # Errors
///
/// Returns a [`Failure`] when [`ENV_QUANTITY`] holds no quantity.
pub fn quantity() -> Result<Volume, Failure> {
    Ok(setting(ENV_QUANTITY, DEFAULT_QUANTITY).parse::<Volume>()?)
}

/// What one run of a trading example created: the orders it placed and the positions it
/// opened.
#[derive(Default)]
pub struct Ledger {
    orders: Vec<(OrderId, String)>,
    positions: Vec<PositionId>,
}

impl Ledger {
    /// Notes an order the example placed and, when it was filled, the position it opened.
    pub fn note(&mut self, report: &OrderReport) {
        self.orders
            .push((report.order_id, report.cl_ord_id.clone()));
        if let Some(position) = report.position {
            if !self.positions.contains(&position) {
                self.positions.push(position);
            }
        }
    }
}

async fn clear_once(trade: &FixClient, ledger: &Ledger) -> Result<usize, Failure> {
    let waiting = within(STEP, "the waiting orders", trade.open_orders()).await??;
    let held = within(STEP, "the open positions", trade.positions()).await??;
    let mut left = 0_usize;
    for (_, cl_ord_id) in ledger
        .orders
        .iter()
        .filter(|(id, _)| waiting.iter().any(|order| order.order_id == *id))
    {
        left += 1;
        let outcome = within(STEP, "the cancellation", trade.cancel(cl_ord_id)).await?;
        println!(
            "clean-up: cancelling order {cl_ord_id} answered {:?}",
            outcome.map(|report| report.status)
        );
    }
    for position in held
        .iter()
        .filter(|position| ledger.positions.contains(&position.id))
    {
        left += 1;
        let side = match position.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let closing =
            NewOrder::market(position.symbol, side, position.quantity).position(position.id);
        let outcome = within(STEP, "the closing order", trade.place(closing)).await?;
        println!(
            "clean-up: closing a position answered {:?}",
            outcome.map(|report| report.status)
        );
    }
    Ok(left)
}

async fn session_for_clearing() -> Result<FixClient, Failure> {
    let mut last = Failure::new("the clean-up did not run");
    for round in 0..SESSION_ROUNDS {
        if round > 0 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        match session(SessionKind::Trade).await {
            Ok(trade) => return Ok(trade),
            Err(failure) => last = failure,
        }
    }
    Err(last)
}

/// Cancels the orders and closes the positions of `ledger` that the account still holds,
/// over a trade session of its own, and makes sure none is left.
///
/// # Errors
///
/// Returns a [`Failure`] when the session fails or something of the run is still listed
/// after several rounds.
pub async fn clear(ledger: &Ledger) -> Result<(), Failure> {
    let trade = session_for_clearing().await?;
    let mut outcome = Err(Failure::new("the clean-up did not run"));
    for round in 0..CLEAR_ROUNDS {
        if round > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        match clear_once(&trade, ledger).await {
            Ok(0) => {
                outcome = Ok(());
                break;
            }
            Ok(left) => {
                outcome = Err(Failure::new(format!(
                    "{left} orders or positions of this run were still listed"
                )));
            }
            Err(failure) => {
                outcome = Err(failure);
                break;
            }
        }
    }
    let left = leave(&trade).await;
    outcome?;
    left
}
