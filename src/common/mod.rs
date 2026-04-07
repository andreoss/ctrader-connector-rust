//! Pieces shared by every protocol: configuration, errors, typed identifiers, exact values and
//! timestamps.

pub mod config;
pub mod decimals;
pub mod errors;
pub mod ids;
pub mod time;
#[cfg(any(feature = "open-api", feature = "fix"))]
pub mod transport;
