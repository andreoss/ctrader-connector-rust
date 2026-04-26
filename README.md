# ctrader-connector

[![ci](https://github.com/andreoss/ctrader-connector-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/andreoss/ctrader-connector-rust/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/ctrader-connector.svg)](https://crates.io/crates/ctrader-connector)
[![docs.rs](https://img.shields.io/docsrs/ctrader-connector)](https://docs.rs/ctrader-connector)
[![licence](https://img.shields.io/crates/l/ctrader-connector.svg)](COPYING.LESSER)
[![rustc](https://img.shields.io/crates/msrv/ctrader-connector.svg)](Cargo.toml)

A Rust library for the cTrader Open API (Protobuf over TLS) and the cTrader
FIX 4.4 API: connection and authorisation, market data, trading and account
data.

In the typed API prices, volumes and money are exact decimals, never floats;
the generated Protobuf messages in `open_api::messages` keep the types of the
wire. An order is written to the wire at most once and is never sent again
by the library.

## Features

No feature is on by default.

| Feature | Enables |
| --- | --- |
| `open-api` | The Open API client: application and account authorisation, token refresh, symbols, spot quotes, depth, trendbars, ticks, orders and positions, reconcile, execution events, trader details, deal and order history, margin calls and cash flow. |
| `fix` | The FIX 4.4 client: quote session (security list, spot prices, depth) and trade session (orders, replacement, cancellation, order status, mass status, positions), with reconnection. |
| `logging` | Diagnostic events of the Open API connection through `tracing`. |

## Requirements

- Rust 1.86 or later (the minimum supported version), edition 2024.
- A tokio runtime in the calling application.

## Use

The crate is a library. Add it to an application with the protocol features
needed:

```toml
[dependencies]
ctrader-connector = { version = "0.3", features = ["open-api", "fix"] }
```

### Open API

```rust
use ctrader_connector::common::config::ConfigurationOpenApi;
use ctrader_connector::open_api::client::OpenApiClient;

let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
client.authorise_application().await?;
for account in client.accounts().await? {
    client.authorise_account(account.id).await?;
}
client.shutdown().await;
```

One connection serves the application and every account of the access token.
The connection sends heartbeats, keeps within the request limits of the
server, reconnects after a loss and restores authorisations and
subscriptions.

### FIX

```rust
use ctrader_connector::common::config::ConfigurationFix;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::session::SessionKind;

let quote = FixClient::connect(ConfigurationFix::from_env()?, SessionKind::Quote).await?;
let securities = quote.securities().await?;
quote.logout().await?;
```

An account has a quote session and a trade session, each a client of its
own. A session answers heartbeats, closes gaps in the sequence numbers and
logs on again after a loss. What happens to a message it cannot use depends
on the case: it is reported or not, counted or not, answered with a reject
(3), asked for again or dropped, or the connection ends. The table "Messages
that cannot be used" in the module documentation of `fix` holds every case
and the tags of every reject. A business message reject (j) is never sent:
the server refuses one with a reject and closes the connection, so
`FixClient::send` refuses one before anything is written.

The module documentation covers configuration, connection lifecycle, error
handling, streams, rate limits, logging, testing and protocol facts for each
protocol:

```sh
cargo doc --all-features --no-deps --open
```

## Environment variables

Both clients can be configured from the environment. Values are secrets or
account details and belong in no file under version control.

| Variable | Required | Meaning |
| --- | --- | --- |
| `BROKER_CLIENT_ID` | Open API | Client identifier of the application. |
| `BROKER_CLIENT_SECRET` | Open API | Client secret of the application. |
| `BROKER_ACCESS_TOKEN` | Open API | Access token that grants the trading accounts. |
| `BROKER_REFRESH_TOKEN` | no | Refresh token; needed to refresh the pair. |
| `BROKER_ENVIRONMENT` | no | `demo` or `live`; `demo` when absent. |
| `BROKER_DEMO_HOST` | no | Host of the demo environment in place of the default one. |
| `BROKER_TOKEN_URL` | no | Token endpoint in place of the default one. |
| `BROKER_FIX_HOST` | FIX | Host of the FIX sessions. |
| `BROKER_FIX_SENDER_COMP_ID` | FIX | Sender identifier of the sessions. |
| `BROKER_FIX_USERNAME` | FIX | User name of the sessions. |
| `BROKER_FIX_PASSWORD` | FIX | Password of the sessions. |
| `BROKER_FIX_QUOTE_PORT` | no | TLS port of the quote session; 5211 when absent. |
| `BROKER_FIX_TRADE_PORT` | no | TLS port of the trade session; 5212 when absent. |
| `BROKER_FIX_TARGET_COMP_ID` | no | Target identifier; `CSERVER` when absent. |
| `BROKER_ENV_FILE` | live tests of the Open API | Absolute path of the file that holds the token pair as `export` lines; the live tests that use the Open API (`open_api_live`, and `fix_live_trading` for its read-back) read the pair from it, and the refresh test writes the new pair back. `fix_live` does not use it. The token refresh example needs it when asked to rotate. |
| `BROKER_EXAMPLE_SYMBOL` | no | Symbol the examples work with; `BTCUSD` when absent. |
| `BROKER_EXAMPLE_QUANTITY` | no | Quantity the FIX trading examples order, in units; `0.01` when absent. |
| `BROKER_EXAMPLE_ROTATE` | no | Set to `1`, `true` or `yes` to let the token refresh example rotate the pair. |
| `BROKER_EXAMPLE_MARKET` | no | Set to `1`, `true` or `yes` to let the FIX order example also trade at market. |
| `BROKER_EXAMPLE_ALLOW_LIVE` | no | Lets the examples leave the demo environment: `read-only` for the examples that place no order, `yes` for every example and for rotating the token pair. Absent or any other value: demo only. |

## Examples

One runnable program per group of requests, each behind the feature of its
protocol:

```text
examples/
├── open_api/
│   ├── connect.rs         connect and authorise
│   ├── token_refresh.rs   token pair; rotates only when asked to
│   ├── symbols.rs         assets, symbols and trading details
│   ├── quotes.rs          spot quotes
│   ├── depth.rs           market depth
│   ├── trendbars.rs       historical and live bars
│   ├── ticks.rs           tick history
│   ├── trading.rs         market, limit, stop, stop-limit, amend, cancel, close
│   ├── reconcile.rs       open positions and waiting orders
│   ├── executions.rs      execution events
│   ├── trader.rs          account details
│   ├── history.rs         deal and order history
│   └── margin.rs          margin calls and cash flow
├── fix/
│   ├── spot.rs            spot prices of the quote session
│   ├── depth.rs           depth of the quote session
│   ├── securities.rs      security list
│   ├── orders.rs          limit order: place, replace, cancel; market on request
│   ├── reports.rs         order status, mass status and positions
│   └── reconnect.rs       recovery after a lost connection
└── support/               pieces the examples share; guard.rs is the demo guard
```

An example is named after its protocol and its file:

```sh
cargo run --example open_api_connect --features open-api
cargo run --example fix_spot --features fix
```

Every example reads its configuration from the environment, prints a short
result without secrets and exits with a non-zero status and a message that
names the cause on failure, with the kind of the error when the library
reported it. The trading examples run against a demo account, order the
smallest volume, place waiting orders far from the market and remove what
they created.

Every example, the read-only ones included, takes its configuration and its
client from one guard, `examples/support/guard.rs`, which checks the
configuration before any connection is opened. The guard is an allowlist:

- an Open API example runs when `BROKER_ENVIRONMENT` is `demo` or absent and
  the host is `demo.ctraderapi.com`;
- a FIX example runs when `BROKER_FIX_SENDER_COMP_ID` starts with `demo.`,
  one label of `BROKER_FIX_HOST` has the part `demo` and no label has a part
  that starts with `live`; the parts of a label are separated by `-`,
  `demo` is matched in lower case only and `live` in any case.

Anything else is refused unless `BROKER_EXAMPLE_ALLOW_LIVE` permits it:
`read-only` lets the examples that place no order leave demo, `yes` lets
every example leave it, the trading ones included, and any other value
permits nothing. Outside demo the token refresh example rotates the pair
only under `yes`; `read-only` lets it check the pair and no more. Outside
demo an Open API example works with the first account of the environment it
connects to.

The manifest lists every example and turns the discovery of examples off. A
test walks every `.rs` file under `examples/`, at any depth: a file outside
`examples/support` has to be the file of a listed example whose access level
the test pins, and any other file fails the test. The test scans the sources
of the examples and holds each of them to an allowlist: of the library an
example imports only the items the test lists, by a `use` that neither
renames nor globs, and names the library nowhere else; of the guard it
imports only the access level; and it names nothing that builds a
configuration, opens a connection or a transport, names a host, exchanges a
token pair or changes the environment. The same test pins the access level
every example asks for, and an example pinned read-only names nothing of the
library that places, changes, cancels or closes an order or a position.

## Known limitations

- The guard of the examples checks neither the ports nor the token endpoint:
  `BROKER_FIX_QUOTE_PORT`, `BROKER_FIX_TRADE_PORT` and `BROKER_TOKEN_URL`
  are used as given.
- The guard matches `demo` in `BROKER_FIX_HOST` in lower case only, so it
  refuses a demo host written in another case; it fails closed.
- A FIX reconnect delay above one hour is shortened to one hour without a
  message; a heartbeat interval above one hour is refused instead. The delay
  is the pause before the first of at most five attempts and the pause
  before the n-th attempt is n times the delay, so a session pauses for up
  to five hours before its last attempt.

## Build

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-features
cargo build --all-features --examples
```

## Test

```sh
cargo test --all-features
cargo llvm-cov --all-features --fail-under-lines 85
```

- Unit tests and the tests against scripted mock servers need no
  credentials and no network beyond the loopback interface.
- Live tests need the variables above and refuse a configuration that is
  not a demo one: `open_api_live` and `fix_live_trading` check the
  environment of the Open API, `fix_live` and `fix_live_trading` check that
  the sender identifier starts with `demo.`. `open_api_live` and
  `fix_live_trading` also need `BROKER_ENV_FILE`; `fix_live` needs the
  variables of the FIX sessions only. `open_api_live` and `fix_live_trading`
  trade the smallest volume, label what they create and remove it
  afterwards; `fix_live` places no order.
- The live test of the token refresh rotates the pair and writes the new one
  to the file `BROKER_ENV_FILE` names; the old pair stops working at once.

## Changes

[CHANGELOG.adoc](CHANGELOG.adoc) lists what each version added, changed
and fixed.

## Licence

This library is free software: it can be redistributed and modified under
the terms of the GNU Lesser General Public License, version 3 or (at your
option) any later version. See [COPYING.LESSER](COPYING.LESSER) and
[COPYING](COPYING).
