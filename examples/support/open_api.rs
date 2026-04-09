//! Steps the Open API examples share: the authorised session, the symbol and the clean-up.

use std::time::Duration;

use ctrader_connector::common::config::{ConfigurationOpenApi, Environment};
use ctrader_connector::common::decimals::Price;
use ctrader_connector::common::ids::{AccountId, OrderId, SymbolId};
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::executions::{ExecutionStream, TradeEvent};
use ctrader_connector::open_api::symbols::{Symbol, SymbolsParams};
use ctrader_connector::open_api::trading::{Execution, ExecutionType};

use super::{Failure, QUOTES, STEP, symbol_name, veil, within};

const SWEEP_ROUNDS: usize = 10;
const SESSION_ROUNDS: usize = 5;

/// Reads the configuration from the environment and refuses any environment but demo.
///
/// # Errors
///
/// Returns a [`Failure`] when a variable is absent or rejected, or the environment is live.
pub fn configuration() -> Result<ConfigurationOpenApi, Failure> {
    let configuration = ConfigurationOpenApi::from_env()?;
    if configuration.environment() == Environment::Demo {
        Ok(configuration)
    } else {
        Err(Failure::new(
            "the examples run against the demo environment only",
        ))
    }
}

/// Returns the first demo account the access token grants access to.
///
/// # Errors
///
/// Returns a [`Failure`] when the list cannot be read or holds no demo account.
pub async fn demo_account(client: &OpenApiClient) -> Result<AccountId, Failure> {
    let accounts = client.accounts().await?;
    for account in &accounts {
        veil(account.id);
        if let Some(login) = account.login {
            veil(login);
        }
    }
    accounts
        .iter()
        .find(|account| !account.is_live)
        .map(|account| account.id)
        .ok_or_else(|| Failure::new("the access token grants access to no demo account"))
}

/// Connects, authorises the application and authorises the first demo account.
///
/// # Errors
///
/// Returns a [`Failure`] when the configuration, the connection or an authorisation fails.
pub async fn session() -> Result<(OpenApiClient, AccountId), Failure> {
    let client = OpenApiClient::connect(configuration()?).await?;
    client.authorise_application().await?;
    let account = demo_account(&client).await?;
    client.authorise_account(account).await?;
    Ok((client, account))
}

/// Finds the symbol the example works with among the symbols of the account.
///
/// # Errors
///
/// Returns a [`Failure`] when the account lists no enabled symbol of that name.
pub async fn symbol(
    client: &OpenApiClient,
    account: AccountId,
) -> Result<(SymbolId, String), Failure> {
    let name = symbol_name();
    let list = client.symbols(SymbolsParams::new(account)).await?;
    list.symbols
        .iter()
        .find(|symbol| symbol.enabled && symbol.name.as_deref() == Some(name.as_str()))
        .map(|symbol| (symbol.id, name.clone()))
        .ok_or_else(|| Failure::new(format!("the account lists no enabled symbol named {name}")))
}

/// Reads the trading details of the symbol the example works with.
///
/// # Errors
///
/// Returns a [`Failure`] when the symbol is not listed or the server sends no details of it.
pub async fn symbol_details(
    client: &OpenApiClient,
    account: AccountId,
) -> Result<(Symbol, String), Failure> {
    let (id, name) = symbol(client, account).await?;
    let details = client.symbol_details(account, &[id]).await?.pop();
    details
        .map(|details| (details, name.clone()))
        .ok_or_else(|| Failure::new(format!("the server sent no details of {name}")))
}

/// Reads the current bid and ask of a symbol from a spot subscription it ends again.
///
/// # Errors
///
/// Returns a [`Failure`] when the subscription fails or no quote with both sides arrives.
pub async fn market(
    client: &OpenApiClient,
    account: AccountId,
    symbol: SymbolId,
) -> Result<(Price, Price), Failure> {
    let mut quotes = client.subscribe_quotes(account, &[symbol]).await?;
    let both = within(QUOTES, "a quote with both sides", async {
        loop {
            let quote = quotes.next().await?;
            if let (Some(bid), Some(ask)) = (quote.bid, quote.ask) {
                return Ok::<_, Failure>((bid, ask));
            }
        }
    })
    .await;
    quotes.unsubscribe().await?;
    both?
}

/// Reads the execution events of `order` until one of the `wanted` kind arrives.
///
/// # Errors
///
/// Returns a [`Failure`] when the stream fails, the server refuses the order or the event
/// does not arrive in time.
pub async fn execution_of(
    stream: &mut ExecutionStream,
    order: OrderId,
    wanted: ExecutionType,
) -> Result<Execution, Failure> {
    within(STEP, "the execution event", async {
        loop {
            let event = stream.next().await?;
            if event.order() != Some(order) {
                continue;
            }
            match event {
                TradeEvent::Execution(execution) if execution.kind == wanted => {
                    return Ok(*execution);
                }
                TradeEvent::Execution(execution) if execution.kind == ExecutionType::Rejected => {
                    return Err(Failure::new(format!(
                        "the server rejected the order: {:?}",
                        execution.error_code
                    )));
                }
                TradeEvent::OrderError(error) => {
                    return Err(Failure::new(format!(
                        "the server refused the order: {}",
                        error.code
                    )));
                }
                _ => {}
            }
        }
    })
    .await?
}

/// Cancels the orders and closes the positions that carry `label`, over a connection of its
/// own, until the account holds none of them.
///
/// # Errors
///
/// Returns a [`Failure`] when the account still holds an order or a position of the label
/// after the last round.
pub async fn sweep(label: &str) -> Result<(), Failure> {
    let mut last = Failure::new("the clean-up did not run");
    for round in 0..SESSION_ROUNDS {
        if round > 0 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        match session().await {
            Ok((client, account)) => {
                let outcome = sweep_over(&client, account, label).await;
                client.shutdown().await;
                return outcome;
            }
            Err(failure) => last = failure,
        }
    }
    Err(last)
}

async fn sweep_over(
    client: &OpenApiClient,
    account: AccountId,
    label: &str,
) -> Result<(), Failure> {
    let own = Some(label);
    let mut left = (0, 0);
    for round in 0..SWEEP_ROUNDS {
        if round > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let reconciled = client.reconcile(account).await?;
        let orders: Vec<_> = reconciled
            .orders
            .iter()
            .filter(|order| order.label.as_deref() == own)
            .collect();
        let positions: Vec<_> = reconciled
            .positions
            .iter()
            .filter(|position| position.label.as_deref() == own)
            .collect();
        left = (orders.len(), positions.len());
        if left == (0, 0) {
            return Ok(());
        }
        for order in orders.iter().filter(|order| !order.closing) {
            let outcome = client.cancel_order(account, order.id).await;
            println!(
                "clean-up: cancelling a left order answered {:?}",
                outcome
                    .map(|execution| execution.kind)
                    .map_err(Failure::from)
            );
        }
        for position in positions {
            let Some(symbol) = client
                .symbol_details(account, &[position.symbol])
                .await?
                .pop()
            else {
                continue;
            };
            let outcome = client
                .close_position(account, position, position.volume, &symbol)
                .await;
            println!(
                "clean-up: closing a left position answered {:?}",
                outcome
                    .map(|execution| execution.kind)
                    .map_err(Failure::from)
            );
        }
    }
    Err(Failure::new(format!(
        "the account still holds {} orders and {} positions labelled {label}",
        left.0, left.1
    )))
}
