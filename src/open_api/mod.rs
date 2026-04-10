//! Client for the cTrader Open API, which exchanges Protobuf messages over TLS.
//!
//! [`client::OpenApiClient`] is the entry point: it opens one connection, authorises the
//! application and its trading accounts on it, and carries every request. The other modules
//! hold the requests of one group each and the values they return.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::symbols::SymbolsParams;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let listed = client.symbols(SymbolsParams::new(account)).await?;
//! println!("{} symbols", listed.symbols.len());
//! client.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Configuration
//!
//! [`ConfigurationOpenApi`](crate::common::config::ConfigurationOpenApi) is built with
//! [`demo`](crate::common::config::ConfigurationOpenApi::demo),
//! [`live`](crate::common::config::ConfigurationOpenApi::live) or read from the environment
//! with [`from_env`](crate::common::config::ConfigurationOpenApi::from_env).
//!
//! | Variable | Builder option | Required | Default |
//! |---|---|---|---|
//! | `BROKER_CLIENT_ID` | `client_id` | yes | none |
//! | `BROKER_CLIENT_SECRET` | `client_secret` | yes | none |
//! | `BROKER_ACCESS_TOKEN` | `access_token` | yes | none |
//! | `BROKER_REFRESH_TOKEN` | `refresh_token` | no | none; a refresh then fails |
//! | `BROKER_ENVIRONMENT` | `demo()` or `live()` | no | `demo` |
//! | `BROKER_DEMO_HOST` | `host` | no | `demo.ctraderapi.com`; read for demo only |
//! | `BROKER_TOKEN_URL` | `token_url` | no | `https://connect.spotware.com/apps/token` |
//! | none | `host` | no | `live.ctraderapi.com` for live |
//! | none | `port` | no | 5035 |
//! | none | `timeout` | no | 10 s for one response |
//! | none | `retries` | no | 3 after a limit error |
//! | none | `backoff` | no | 1 s before the first retry |
//! | none | `reconnect_delay` | no | 5 s before the first attempt |
//!
//! An empty variable counts as absent. Secrets are held in types that do not print their
//! value.
//!
//! [`OpenApiClient::connect_with`](client::OpenApiClient::connect_with) also takes
//! [`ConnectionOptions`](connection::ConnectionOptions) for what the configuration does not
//! cover:
//!
//! | Option | Default |
//! |---|---|
//! | `heartbeat_interval` | 10 s |
//! | `inbound_timeout` | 75 s |
//! | `reconnect_attempts` | unlimited |
//! | `request_limit` | 50 per second |
//! | `historical_limit` | 5 per second |
//! | `command_capacity` | 64 requests waiting for the connection |
//! | `event_capacity` | 1024 events held for a reader |
//!
//! # Connection lifecycle
//!
//! One task owns the socket; see [`connection`] for the whole account.
//!
//! - Heartbeat: the task sends one whenever nothing was written for the heartbeat interval.
//! - Inbound silence: a server that sends nothing, not even its own heartbeat, for the
//!   inbound timeout counts as lost, as does a write that does not complete in time.
//! - Reconnect: after a loss the requests in flight fail,
//!   [`Event::Disconnected`](connection::Event::Disconnected) is reported and the task
//!   connects again, waiting twice as long before every further attempt, up to a minute.
//! - Replay: once connected it authorises the application and the accounts again, restores
//!   the subscriptions and reports [`Event::Reconnected`](connection::Event::Reconnected).
//!   Requests made while the connection is down fail at once.
//! - Never sent again: orders. A request that trades is written at most once; it is neither
//!   retried after a limit error nor replayed after a reconnection.
//! - [`OpenApiClient::shutdown`](client::OpenApiClient::shutdown) ends the task; later
//!   requests fail with [`ConnectorError::Closed`](crate::common::errors::ConnectorError::Closed).
//!
//! # Error handling
//!
//! Every call returns [`ConnectorError`](crate::common::errors::ConnectorError).
//!
//! | Variant | Meaning | Reaction |
//! |---|---|---|
//! | `Config`, `Value` | Rejected before anything was sent. | Correct the input. |
//! | `Server` | The server refused; the error carries its code and text. | Decide by the code. |
//! | `Transport` | The connection failed or is down. | Wait for `Event::Reconnected`, then repeat a request that reads. |
//! | `Timeout` | No answer in time to a request that may be repeated. | Repeat it. |
//! | `OutcomeUnknown` | A request that trades was written and no answer came: the server may have acted on it. | Look the order up with [`reconcile`](client::OpenApiClient::reconcile) or by its label before placing it again. |
//! | `SessionEnded` | The server ended the authorisation of the account or the application. | Authorise again, see [`client`]; then subscribe again. |
//! | `Lagged` | A reader fell behind and events were dropped. | Go on reading; refresh the state from the server. |
//! | `Incomplete` | The server announced more history than it sent. | Ask again for the range the error names. |
//! | `Token` | A token refresh failed. | Obtain a new pair. |
//! | `Closed` | The client was shut down. | Connect anew. |
//!
//! # Streams
//!
//! Subscriptions return typed streams, each read with `next`:
//! [`QuoteStream`](quotes::QuoteStream), [`DepthStream`](depth::DepthStream),
//! [`TrendbarStream`](trendbars::TrendbarStream),
//! [`ExecutionStream`](executions::ExecutionStream) and
//! [`MarginStream`](margin::MarginStream).
//! [`OpenApiClient::events`](client::OpenApiClient::events) gives every event of the
//! connection untyped. Streams are bounded: a stream read too slowly loses its oldest events
//! and reports the loss once with `Lagged`, after which it goes on. Subscriptions are
//! restored after a reconnection, and a stream reports `SessionEnded` when the
//! authorisation of its account ends.
//!
//! # Rate limits
//!
//! The server accepts 50 requests per second on a connection and 5 per second for
//! historical data. The task holds requests back so that neither limit is exceeded. When
//! the server still answers that a limit was exceeded, the task pauses and sends the
//! request again, up to the configured number of retries, with a pause that doubles up to
//! thirty seconds. Long ranges of bars and ticks take several requests, each counted.
//!
//! # Logging
//!
//! With the `logging` feature the connection task emits `tracing` events at debug level:
//! connected, connection lost, reconnected, reconnection failed, replayed request failed
//! and authorisation ended. They carry no secret. Without the feature nothing is emitted
//! and `tracing` is not compiled in. The application installs the subscriber.
//!
//! # Testing
//!
//! - Mock servers: the tests named `open_api_*_mock` and `open_api_mock` run against
//!   scripted servers on the loopback interface, reached through
//!   [`Plain`](crate::common::transport::Plain) and
//!   [`connect_with`](client::OpenApiClient::connect_with). They need no credentials.
//! - Live tests: `open_api_live` runs against the demo environment only and refuses any
//!   other. It reads the variables of the table above and `BROKER_ENV_FILE`, the absolute
//!   path of a file that holds the token pair as `export BROKER_ACCESS_TOKEN=` and
//!   `export BROKER_REFRESH_TOKEN=` lines.
//! - Token rotation: a refresh makes the old pair useless at once. The live test of the
//!   refresh writes the new pair back to that file; a run that cannot write it loses the
//!   pair, and a session opened with the old access token ends with `SessionEnded`.
//! - Live tests trade the smallest volume, label what they create and remove it afterwards.
//!
//! # Protocol facts
//!
//! - Prices travel as integers in 1/100000 of a unit and volumes in 0.01 of a unit; the
//!   library takes and returns decimals in units, see [`crate::common::decimals`].
//! - Ticks: on the wire `toTimestamp` is exclusive. [`ticks`] takes a range with both ends
//!   included and asks for one millisecond more.
//! - Bars: on the wire `toTimestamp` is inclusive; [`trendbars`] returns the bars that
//!   overlap the range, oldest first, without the bar still forming.
//! - A stop-limit order is a stop price and a slippage in points, not two prices.
//! - An order at market takes its stop loss and take profit as distances from the fill
//!   price; absolute prices are refused before anything is sent.
//! - A request that trades is answered by the first execution event for it, for an order at
//!   market its acceptance; the fill follows on the [`executions`] stream.

pub mod errors;

#[allow(missing_docs, clippy::all, clippy::pedantic)]
#[rustfmt::skip]
pub mod messages;

pub mod account;
pub mod client;
pub mod connection;
pub mod depth;
pub mod executions;
pub mod history;
pub mod margin;
pub mod payload;
pub mod quotes;
mod stream;
pub mod symbols;
pub mod ticks;
pub mod token;
pub mod trading;
pub mod transport;
pub mod trendbars;

#[cfg(test)]
mod messages_tests;
