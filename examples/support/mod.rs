//! Pieces the examples share: the error `main` returns, bounded waits and optional settings.
//!
//! Every example is a program of its own and includes this module by path, so each of them
//! uses only a part of it.

#![allow(dead_code)]

use std::fmt;
use std::future::Future;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use ctrader_connector::common::decimals::Price;
use ctrader_connector::common::errors::ConnectorError;
use ctrader_connector::common::time::Timestamp;
use rust_decimal::Decimal;

#[cfg(feature = "fix")]
pub mod fix;
#[cfg(feature = "open-api")]
pub mod open_api;

/// Variable naming the symbol the examples work with.
pub const ENV_SYMBOL: &str = "BROKER_EXAMPLE_SYMBOL";

/// Symbol the examples work with unless [`ENV_SYMBOL`] names another; it quotes every day.
pub const DEFAULT_SYMBOL: &str = "BTCUSD";

/// Longest wait for one answer or one event of the server.
pub const STEP: Duration = Duration::from_secs(30);

/// Longest wait for prices; a market that is closed sends none.
pub const QUOTES: Duration = Duration::from_secs(90);

static KNOWN: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Keeps `identifier` out of every failure reported from now on.
pub fn veil(identifier: impl fmt::Display) {
    let identifier = identifier.to_string();
    if !identifier.is_empty() {
        KNOWN
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(identifier);
    }
}

fn veiled(text: &str) -> String {
    KNOWN
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .fold(text.to_owned(), |text, known| {
            text.replace(known, "<account>")
        })
}

/// Failure of an example: the kind of the error and its message, without account identifiers.
///
/// `main` returns it, so a failed example prints the message and exits with a non-zero status.
pub struct Failure(String);

impl Failure {
    /// Describes a failure the library has no error for, such as a market that sends no price.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&veiled(&self.0))
    }
}

impl<E: Into<ConnectorError>> From<E> for Failure {
    fn from(error: E) -> Self {
        let error = error.into();
        Self(format!("{}: {error}", kind(&error)))
    }
}

const fn kind(error: &ConnectorError) -> &'static str {
    match error {
        ConnectorError::Config(_) => "Config",
        ConnectorError::Server(_) => "Server",
        ConnectorError::Value(_) => "Value",
        ConnectorError::Transport(_) => "Transport",
        ConnectorError::Token(_) => "Token",
        ConnectorError::Fix(_) => "Fix",
        ConnectorError::Timeout(_) => "Timeout",
        ConnectorError::OutcomeUnknown(_) => "OutcomeUnknown",
        ConnectorError::SessionEnded(_) => "SessionEnded",
        ConnectorError::Closed => "Closed",
        ConnectorError::Lagged { .. } => "Lagged",
        ConnectorError::Incomplete { .. } => "Incomplete",
        ConnectorError::UnexpectedResponse { .. } => "UnexpectedResponse",
        _ => "Other",
    }
}

/// Waits for `work` no longer than `limit`.
///
/// # Errors
///
/// Returns a [`Failure`] naming `what` when the time runs out.
pub async fn within<T>(
    limit: Duration,
    what: &str,
    work: impl Future<Output = T>,
) -> Result<T, Failure> {
    tokio::time::timeout(limit, work)
        .await
        .map_err(|_| Failure::new(format!("{what} did not arrive within {limit:?}")))
}

/// Returns the value of the variable `name`, or `default` when it is absent or empty.
pub fn setting(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// Tells whether an option is switched on, by the command line `argument` or by the
/// `variable` set to `1`, `true` or `yes`.
pub fn flag(argument: &str, variable: &str) -> bool {
    std::env::args().any(|given| given == argument)
        || matches!(setting(variable, "").as_str(), "1" | "true" | "yes")
}

/// Returns the name of the symbol the example works with.
pub fn symbol_name() -> String {
    setting(ENV_SYMBOL, DEFAULT_SYMBOL)
}

/// Returns a label that marks what one run of the example `group` creates.
pub fn run_label(group: &str) -> String {
    format!("example-{group}-{}", Timestamp::now())
}

/// Returns `percent` hundredths of `price`, rounded to the `digits` of the symbol.
///
/// # Errors
///
/// Returns a [`Failure`] when the result is no price.
pub fn scaled(price: Price, percent: i64, digits: u32) -> Result<Price, Failure> {
    let scaled = price
        .value()
        .checked_mul(Decimal::new(percent, 2))
        .ok_or_else(|| Failure::new("the scaled price is out of range"))?
        .round_dp(digits);
    Ok(Price::new(scaled)?)
}

/// Returns the moment `seconds` whole seconds from now.
///
/// # Errors
///
/// Returns a [`Failure`] when the moment is out of range.
pub fn seconds_from_now(seconds: i64) -> Result<Timestamp, Failure> {
    let second = Timestamp::now().as_millis() / 1_000 + seconds;
    Ok(Timestamp::from_millis(second * 1_000)?)
}
