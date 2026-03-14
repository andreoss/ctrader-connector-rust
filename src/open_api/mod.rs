//! Client for the cTrader Open API, which exchanges Protobuf messages over TLS.

pub mod errors;

#[allow(missing_docs, clippy::all, clippy::pedantic)]
#[rustfmt::skip]
pub mod messages;

#[cfg(test)]
mod messages_tests;
