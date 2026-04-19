//! Steps the Open API examples share: the authorised session, the symbol and the clean-up.

use std::time::Duration;

use ctrader_connector::common::config::{ConfigurationOpenApi, Environment};
use ctrader_connector::common::decimals::Price;
use ctrader_connector::common::ids::{AccountId, OrderId, SymbolId};
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::executions::{ExecutionStream, TradeEvent};
use ctrader_connector::open_api::symbols::{Symbol, SymbolsParams};
use ctrader_connector::open_api::token::TokenPair;
use ctrader_connector::open_api::trading::{Execution, ExecutionType};

use super::guard::{self, Access, Checked};
use super::{Failure, QUOTES, STEP, symbol_name, veil, within};

const SWEEP_ROUNDS: usize = 10;
const SESSION_ROUNDS: usize = 5;

/// Reads the configuration from the environment through the guard.
///
/// # Errors
///
/// Returns a [`Failure`] when a variable is absent or rejected, or the guard refuses the
/// configuration for `access`; see [`guard`].
pub fn configuration(access: Access) -> Result<Checked<ConfigurationOpenApi>, Failure> {
    Ok(guard::open_api(access)?)
}

/// Opens a connection with a configuration the guard let through.
///
/// # Errors
///
/// Returns a [`Failure`] when the connection cannot be established.
pub async fn open(checked: &Checked<ConfigurationOpenApi>) -> Result<OpenApiClient, Failure> {
    Ok(guard::open_api_client(checked).await?)
}

/// Returns `checked` with a new token pair; the environment and the host stay the same.
///
/// # Errors
///
/// Returns a [`Failure`] when a token of the pair is empty.
pub fn renewed(
    checked: &Checked<ConfigurationOpenApi>,
    access_token: &str,
    refresh_token: &str,
) -> Result<Checked<ConfigurationOpenApi>, Failure> {
    Ok(guard::open_api_renewed(
        checked,
        access_token,
        refresh_token,
    )?)
}

/// Tells whether the configuration holds a refresh token.
pub fn refreshable(checked: &Checked<ConfigurationOpenApi>) -> bool {
    checked.get().refresh_token().is_some()
}

/// Exchanges the refresh token for a new pair, once the guard permits the rotation.
///
/// # Errors
///
/// Returns a [`Failure`] when the guard refuses the rotation, which outside demo takes the
/// full permit, or the server refuses the refresh.
pub async fn rotate(
    checked: &Checked<ConfigurationOpenApi>,
    client: &OpenApiClient,
) -> Result<TokenPair, Failure> {
    guard::open_api_rotation(checked)?;
    Ok(client.refresh_token().await?)
}

/// Returns the first account of `environment` the access token grants access to.
///
/// # Errors
///
/// Returns a [`Failure`] when the list cannot be read or holds no such account.
pub async fn account(
    client: &OpenApiClient,
    environment: Environment,
) -> Result<AccountId, Failure> {
    let accounts = client.accounts().await?;
    for account in &accounts {
        veil(account.id);
        if let Some(login) = account.login {
            veil(login);
        }
    }
    let live = environment == Environment::Live;
    accounts
        .iter()
        .find(|account| account.is_live == live)
        .map(|account| account.id)
        .ok_or_else(|| {
            Failure::new(format!(
                "the access token grants access to no {environment} account"
            ))
        })
}

/// Connects, authorises the application and authorises the first account of the
/// environment, which is demo unless the guard was told otherwise.
///
/// Every Open API example reaches the server through [`configuration`] and [`open`], so none
/// of them connects with a configuration the guard did not check.
///
/// # Errors
///
/// Returns a [`Failure`] when the guard refuses the configuration for `access`, or the
/// connection or an authorisation fails.
pub async fn session(access: Access) -> Result<(OpenApiClient, AccountId), Failure> {
    let checked = configuration(access)?;
    let client = open(&checked).await?;
    client.authorise_application().await?;
    let account = account(&client, checked.get().environment()).await?;
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
        match session(Access::Trading).await {
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
