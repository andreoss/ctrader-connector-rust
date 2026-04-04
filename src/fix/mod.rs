//! Client for the cTrader FIX 4.4 API.
//!
//! [`codec`] turns messages into the bytes of the wire and back, and frames them on a byte
//! stream. [`messages`] gives every supported message a type.

pub mod codec;
pub mod messages;
