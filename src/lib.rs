//! Client library for the cTrader Open API and the cTrader FIX 4.4 API.
//!
//! # Overview
//!
//! The crate talks to a cTrader server over either of its two programming interfaces:
//!
//! - [`open_api`]: Protobuf messages over TLS. One connection serves an application and
//!   every trading account its access token grants: symbols, prices, depth, bars, ticks,
//!   orders, positions, history and account data.
//! - [`fix`]: FIX 4.4. One trading account has a quote session for prices and a trade
//!   session for orders, each a connection of its own.
//!
//! The [`common`] module holds what both share: [configuration](common::config),
//! [errors](common::errors), [typed identifiers](common::ids),
//! [exact prices and volumes](common::decimals) and [timestamps](common::time). Prices,
//! volumes and money are decimals throughout, never floats.
//!
//! Both clients are asynchronous and run on a tokio runtime the application provides.
//!
//! # Features
//!
//! No feature is on by default.
//!
//! | Feature | Enables |
//! |---|---|
//! | `open-api` | The [`open_api`] module. |
//! | `fix` | The [`fix`] module. |
//! | `logging` | Diagnostic events of the Open API connection, emitted through `tracing`. |
//!
//! # Quick start
//!
//! Both clients are built from a configuration, which is either read from the environment
//! or put together with a builder. The demo environment is the target unless the live one
//! is asked for. The variables are listed in the documentation of each protocol module.
//!
//! ## Open API
//!
//! ```no_run
//! # #[cfg(feature = "open-api")]
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//!
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let accounts = client.accounts().await?;
//! if let Some(account) = accounts.iter().find(|account| !account.is_live) {
//!     client.authorise_account(account.id).await?;
//!     let trader = client.trader(account.id).await?;
//!     println!("balance: {} {}", trader.balance, trader.currency);
//! }
//! client.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## FIX
//!
//! ```no_run
//! # #[cfg(feature = "fix")]
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! use ctrader_connector::common::config::ConfigurationFix;
//! use ctrader_connector::fix::client::FixClient;
//! use ctrader_connector::fix::session::SessionKind;
//!
//! let quote = FixClient::connect(ConfigurationFix::from_env()?, SessionKind::Quote).await?;
//! let securities = quote.securities().await?;
//! if let Some(security) = securities.first() {
//!     let mut spots = quote.subscribe_spot(security.id).await?;
//!     let prices = spots.next().await?;
//!     println!("{}: {:?} / {:?}", security.name, prices.bid, prices.offer);
//!     spots.unsubscribe().await?;
//! }
//! quote.logout().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Errors
//!
//! Every fallible call returns [`ConnectorError`](common::errors::ConnectorError), one
//! family for both protocols. Each protocol module explains which of its variants call for
//! which reaction; the one to know before trading is
//! [`OutcomeUnknown`](common::errors::ConnectorError::OutcomeUnknown): an order is written
//! to the wire at most once and is never sent again by this library.
//!
//! # Examples
//!
//! The `examples` directory of the repository holds one runnable program per group of
//! requests, `examples/open_api` and `examples/fix`, each behind the feature of its
//! protocol.

pub mod common;

#[cfg(feature = "fix")]
pub mod fix;

#[cfg(feature = "open-api")]
pub mod open_api;

#[cfg(test)]
mod proto_text;

/// Version of this crate as recorded in its manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_matches_the_manifest() {
        let manifest = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        assert!(manifest.contains(&format!("version = \"{VERSION}\"")));
    }
}
