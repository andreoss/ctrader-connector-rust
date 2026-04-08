//! Client for the cTrader FIX 4.4 API.
//!
//! [`codec`] turns messages into the bytes of the wire and back, and frames them on a byte
//! stream. [`messages`] gives every supported message a type. [`session`] keeps one logged-on
//! session alive, and [`client`] is the handle through which a program uses it.

pub mod client;
pub mod codec;
pub mod messages;
pub mod quotes;
pub mod securities;
pub mod session;
pub mod trading;
