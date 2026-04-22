//! Client for the cTrader FIX 4.4 API.
//!
//! A trading account has two sessions: the quote session carries prices and the list of
//! symbols, the trade session carries orders and the state of the account. Each is a
//! connection of its own and a [`client::FixClient`] of its own.
//!
//! [`codec`] turns messages into the bytes of the wire and back, and frames them on a byte
//! stream. [`messages`] gives every supported message a type. [`session`] keeps one logged-on
//! session alive, and [`client`] is the handle through which a program uses it. The requests
//! live in [`securities`], [`quotes`], [`trading`], [`amend`] and [`reports`].
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationFix;
//! use ctrader_connector::fix::client::FixClient;
//! use ctrader_connector::fix::session::SessionKind;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let trade = FixClient::connect(ConfigurationFix::from_env()?, SessionKind::Trade).await?;
//! for position in trade.positions().await? {
//!     println!("{:?} {} of symbol {}", position.side, position.quantity, position.symbol);
//! }
//! trade.logout().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Configuration
//!
//! [`ConfigurationFix`](crate::common::config::ConfigurationFix) is put together with
//! [`builder`](crate::common::config::ConfigurationFix::builder) or read from the
//! environment with [`from_env`](crate::common::config::ConfigurationFix::from_env).
//!
//! | Variable | Builder option | Required | Default |
//! |---|---|---|---|
//! | `BROKER_FIX_HOST` | `host` | yes | none |
//! | `BROKER_FIX_SENDER_COMP_ID` | `sender_comp_id` | yes | none |
//! | `BROKER_FIX_USERNAME` | `username` | yes | none |
//! | `BROKER_FIX_PASSWORD` | `password` | yes | none |
//! | `BROKER_FIX_QUOTE_PORT` | `quote_port` | no | 5211 |
//! | `BROKER_FIX_TRADE_PORT` | `trade_port` | no | 5212 |
//! | `BROKER_FIX_TARGET_COMP_ID` | `target_comp_id` | no | `CSERVER` |
//! | none | `unencrypted` | no | TLS; once it is called the ports default to 5201 and 5202 |
//! | none | `heartbeat` | no | 30 s; whole seconds from 1 s to 3600 s |
//! | none | `timeout` | no | 10 s for one answer |
//! | none | `reconnect_delay` | no | 5 s before the first attempt; 3600 s at most, a longer one is shortened to that |
//!
//! An empty variable counts as absent. The password is held in a type that does not print
//! its value. Sessions read from the environment always run over TLS.
//!
//! The heartbeat interval travels in the logon as a whole number of seconds (108), so a
//! fraction of a second is dropped. An interval below one second is refused with
//! `ConfigError::ZeroDuration`: zero would ask for a session without heartbeats, and the
//! library always keeps a session alive and watches the server by them. An interval above
//! [`MAX_FIX_HEARTBEAT`](crate::common::config::MAX_FIX_HEARTBEAT), one hour, is refused
//! with `ConfigError::TooLong`. A timeout of [`Duration::MAX`](std::time::Duration::MAX)
//! stands for a wait without an end.
//!
//! # Connection lifecycle
//!
//! One task owns the socket of a session; see [`session`] for the whole account.
//!
//! - Logon: [`FixClient::connect`](client::FixClient::connect) returns once the server has
//!   accepted the logon; a refusal is
//!   [`FixError::LogonRefused`](crate::common::errors::FixError::LogonRefused).
//! - Heartbeat: the task sends one whenever it has sent nothing for the heartbeat interval
//!   and answers the test requests of the server.
//! - Inbound silence: after one and a half intervals without a message the task sends a
//!   test request; after two and a half the connection counts as lost.
//! - Reconnect: after a loss the task reports
//!   [`Event::Disconnected`](session::Event::Disconnected), then connects and logs on
//!   again, five times at most with a pause that grows by the reconnect delay each time,
//!   and reports [`Event::Reconnected`](session::Event::Reconnected). When no attempt
//!   succeeds, or as soon as the server refuses a logon, it reports
//!   [`Event::Ended`](session::Event::Ended) and the session is over.
//! - Replay: both sides count from one again after every logon. The server has forgotten
//!   the subscriptions; the price streams subscribe anew by themselves. A gap in the
//!   messages of the server is closed with a resend request: the message that arrived
//!   ahead of its turn is dropped and read when the server repeats it. A logout or a resend
//!   request that arrives ahead of its turn is the exception: it is acted on at once.
//! - Unreadable messages: one the library cannot read is reported as
//!   [`Event::Unparsed`](session::Event::Unparsed) and the session goes on. One whose bytes
//!   are intact and whose turn it is is counted and answered with a reject (3) that names
//!   the message (45), its type where it is known (372), the tag (371) and the reason
//!   (373): 11 for a type the library does not know, 1 for a missing tag. The session never
//!   sends a business message reject (j): the server refuses one with a reject and closes
//!   the connection. A reject or a business message reject that cannot be read is
//!   counted and never answered, so that no reject answers a reject. One whose bytes are
//!   damaged is not counted and is asked for again. While the session logs out an
//!   unreadable message is reported and not answered.
//! - Never sent again: orders, and every other message. When the server asks for messages
//!   again the task answers with a sequence reset that skips them, reported as
//!   [`Event::Skipped`](session::Event::Skipped).
//! - [`FixClient::logout`](client::FixClient::logout) ends the session in order;
//!   [`FixClient::closed`](client::FixClient::closed) waits for the end.
//!
//! # Error handling
//!
//! Every call returns [`ConnectorError`](crate::common::errors::ConnectorError); what the
//! protocol itself refuses is a [`FixError`](crate::common::errors::FixError) inside its
//! `Fix` variant.
//!
//! | Error | Meaning | Reaction |
//! |---|---|---|
//! | `Config`, `Value` | Rejected before anything was sent. | Correct the input. |
//! | `Fix(LogonRefused)` | The server answered the logon with a logout. | Check the credentials and the session kind. |
//! | `Fix(Rejected)`, `Fix(BusinessRejected)` | The server refused a message or a request. | Decide by the reason and the text. |
//! | `Fix(MarketDataRejected)` | The server refused a subscription to prices. | Check the symbol. |
//! | `Fix(OrderRejected)` | The server refused an order, or cancelled or expired it right after accepting it, or answered a cancellation or a replacement with a rejecting report. | Decide by the text. |
//! | `Fix(CancelRejected)` | The server refused to cancel or replace; the order is as it was. | Read the state of the order. |
//! | `Timeout` | No answer in time to a request that may be repeated. | Repeat it. |
//! | `OutcomeUnknown` | An order or a replacement was sent and no answer came or its write failed, or the server asked again for a request of any kind, which is never sent twice. | Read [`open_orders`](client::FixClient::open_orders) and [`positions`](client::FixClient::positions) before placing it again. |
//! | `Transport` | The connection was lost. | Go on after `Event::Reconnected`; a price stream reports the loss once and continues. |
//! | `Lagged` | The reader of the events fell behind; an answer may have been skipped. | Read the state again. |
//! | `Closed` | The session has ended, or it has no connection at the moment: a request made between `Event::Disconnected` and `Event::Reconnected` is not sent. | Ended: connect anew. Between two connections: go on after `Event::Reconnected`. |
//!
//! The variant `SessionEnded` belongs to the Open API and does not occur over FIX: a
//! session the server ends is reported as [`Event::LoggedOut`](session::Event::LoggedOut).
//! A call that waits for its answer at that moment fails with `Transport`, an order or a
//! replacement with `OutcomeUnknown`, and every later call with `Closed`.
//!
//! `Closed` has two meanings, told apart by the events and by
//! [`FixClient::closed`](client::FixClient::closed). The session has ended after
//! [`Event::LoggedOut`](session::Event::LoggedOut), after
//! [`Event::Ended`](session::Event::Ended) and after a logout or the drop of the last
//! client: `closed` has returned and every later call fails with `Closed`. The session is
//! between two connections after [`Event::Disconnected`](session::Event::Disconnected) and
//! until [`Event::Reconnected`](session::Event::Reconnected): `closed` has not returned, a
//! message handed over meanwhile is refused with `Closed` without being sent, and calls
//! work again after `Event::Reconnected`.
//!
//! # Streams
//!
//! [`SpotStream`](quotes::SpotStream) yields the best bid and offer of a symbol,
//! [`DepthStream`](quotes::DepthStream) keeps its [`Book`](quotes::Book). Both outlive a
//! lost connection: they report the loss once as an error and subscribe anew after the
//! logon; the depth starts over from an empty book.
//! [`FixClient::events`](client::FixClient::events) gives every message and every change of
//! the session; it holds 4096 events for a reader and drops the oldest for one that falls
//! behind.
//!
//! # Rate limits
//!
//! The library does not pace the requests of a FIX session. The limits of the server apply
//! as they are.
//!
//! # Logging
//!
//! The FIX sessions emit no `tracing` events, with or without the `logging` feature. What
//! happens to a session is read from [`FixClient::events`](client::FixClient::events).
//!
//! # Testing
//!
//! - Mock servers: the tests named `fix_*_mock` and `fix_mock` run against scripted servers
//!   on the loopback interface over unencrypted sessions; `fix_clock_mock` runs over a
//!   stream held in memory, on a paused clock. They need no credentials.
//! - Live tests: `fix_live` needs the variables of the table above. `fix_live_trading`
//!   also needs the variables of the Open API and `BROKER_ENV_FILE`, since it reads back
//!   over the other interface what it did over FIX; both must name the same demo account.
//! - Token rotation: the FIX sessions use no token, but the read-back does. The pair is
//!   taken from the file `BROKER_ENV_FILE` names, which the Open API tests rewrite.
//! - Demo only: both refuse a sender identifier that does not start with `demo.`.
//!   `fix_live_trading` trades the smallest quantity, labels what it creates and removes it
//!   afterwards; `fix_live` places no order.
//!
//! # Protocol facts
//!
//! - Quantities are in units of the base asset, not in lots. Prices are decimals.
//! - The symbol of a message is the numeric identifier of the symbol, the same the Open API
//!   uses; [`securities`] lists them with their names.
//! - Spot prices arrive as full snapshots, one per change.
//! - Depth arrives as an empty snapshot followed by incremental changes.
//! - `ClOrdID` after a replacement: the report of the replacement carries the identifier of
//!   the replacement request, but the order goes on answering to the identifier it was
//!   placed under. Cancel and replace it under the first identifier.
//! - A cancellation of an order the server does not know is answered with a business
//!   message reject, not with an order cancel reject; both are reported as
//!   `Fix(CancelRejected)`.
//! - Order kinds: [`NewOrder`](trading::NewOrder) places market, limit and stop orders;
//!   the server offers no stop-limit order over FIX.
//!   [`OrderKind::StopLimit`](messages::OrderKind::StopLimit) exists because the dictionary
//!   enumerates it under tag 40: it is read from an execution report that carries it, and
//!   no typed request sends it. A message put together by hand goes out through
//!   [`FixClient::send`](client::FixClient::send) as it is, whatever its kind.
//! - A market order is answered with its fill, which names the position. A position is
//!   closed with an opposite market order that names it.

pub mod amend;
pub mod client;
pub mod codec;
pub mod messages;
pub mod quotes;
pub mod reports;
pub mod securities;
pub mod session;
pub mod trading;
