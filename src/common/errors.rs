//! Error types shared by every protocol.

use std::fmt;
use std::time::Duration;

use rust_decimal::Decimal;
use thiserror::Error;

use crate::common::ids::AccountId;
use crate::common::time::Timestamp;

/// Reasons a configuration is rejected before any connection is attempted.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required option or environment variable is absent or empty.
    #[error("{0} is not set")]
    Missing(&'static str),
    /// The environment name is neither `demo` nor `live`.
    #[error("environment must be demo or live, got {0}")]
    InvalidEnvironment(String),
    /// The host is empty or is not a bare host name.
    #[error("host is not a bare host name: {0}")]
    InvalidHost(String),
    /// The port is zero.
    #[error("port must not be zero")]
    InvalidPort,
    /// The token URL is not an `https` URL with a host.
    #[error("token URL must be an https URL: {0}")]
    InvalidTokenUrl(String),
    /// A duration option is zero.
    #[error("{0} must be longer than zero")]
    ZeroDuration(&'static str),
}

macro_rules! error_codes {
    ($($(#[$doc:meta])* $variant:ident => $wire:literal,)*) => {
        /// Error code reported by the server.
        ///
        /// The server sends the code as text. Every code named in the proto files has a variant;
        /// any other text is kept in [`ErrorCode::Unknown`] exactly as received.
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ErrorCode {
            $($(#[$doc])* $variant,)*
            /// A code this version of the library does not name, kept as received.
            Unknown(String),
        }

        impl ErrorCode {
            /// Maps the text sent by the server to a code.
            #[must_use]
            pub fn from_wire(text: &str) -> Self {
                match text {
                    $($wire => Self::$variant,)*
                    other => Self::Unknown(other.to_owned()),
                }
            }

            /// Returns the text the server uses for the code.
            #[must_use]
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)*
                    Self::Unknown(text) => text,
                }
            }
        }
    };
}

error_codes! {
    /// The server failed for a reason it does not disclose.
    UnknownError => "UNKNOWN_ERROR",
    /// The message type is not supported.
    UnsupportedMessage => "UNSUPPORTED_MESSAGE",
    /// The request is malformed or carries invalid values.
    InvalidRequest => "INVALID_REQUEST",
    /// The server did not process the request in time.
    TimeoutError => "TIMEOUT_ERROR",
    /// The entity named by the request does not exist.
    EntityNotFound => "ENTITY_NOT_FOUND",
    /// The server could not route the request.
    CantRouteRequest => "CANT_ROUTE_REQUEST",
    /// The frame exceeds the size limit.
    FrameTooLong => "FRAME_TOO_LONG",
    /// The market is closed.
    MarketClosed => "MARKET_CLOSED",
    /// The entity was changed by another request at the same time.
    ConcurrentModification => "CONCURRENT_MODIFICATION",
    /// The payload type is blocked.
    BlockedPayloadType => "BLOCKED_PAYLOAD_TYPE",
    /// The access token has expired.
    OaAuthTokenExpired => "OA_AUTH_TOKEN_EXPIRED",
    /// The account is not authorised on this connection.
    AccountNotAuthorized => "ACCOUNT_NOT_AUTHORIZED",
    /// No account has this login.
    RetNoSuchLogin => "RET_NO_SUCH_LOGIN",
    /// The account is already authorised on this connection.
    AlreadyLoggedIn => "ALREADY_LOGGED_IN",
    /// The account is disabled.
    RetAccountDisabled => "RET_ACCOUNT_DISABLED",
    /// The application credentials were refused.
    ChClientAuthFailure => "CH_CLIENT_AUTH_FAILURE",
    /// The application is not authorised on this connection.
    ChClientNotAuthenticated => "CH_CLIENT_NOT_AUTHENTICATED",
    /// The application is already authorised on this connection.
    ChClientAlreadyAuthenticated => "CH_CLIENT_ALREADY_AUTHENTICATED",
    /// The access token is invalid.
    ChAccessTokenInvalid => "CH_ACCESS_TOKEN_INVALID",
    /// The trading server is not reachable.
    ChServerNotReachable => "CH_SERVER_NOT_REACHABLE",
    /// The trading account was not found.
    ChCtidTraderAccountNotFound => "CH_CTID_TRADER_ACCOUNT_NOT_FOUND",
    /// The application was not found.
    ChOaClientNotFound => "CH_OA_CLIENT_NOT_FOUND",
    /// Requests are sent more often than the limit allows.
    RequestFrequencyExceeded => "REQUEST_FREQUENCY_EXCEEDED",
    /// The server is under maintenance.
    ServerIsUnderMaintenance => "SERVER_IS_UNDER_MAINTENANCE",
    /// The channel is blocked.
    ChannelIsBlocked => "CHANNEL_IS_BLOCKED",
    /// The limit of open connections is reached.
    ConnectionsLimitExceeded => "CONNECTIONS_LIMIT_EXCEEDED",
    /// A guaranteed stop loss may not be moved to a worse level.
    WorseGslNotAllowed => "WORSE_GSL_NOT_ALLOWED",
    /// Trading is suspended because the symbol has a holiday.
    SymbolHasHoliday => "SYMBOL_HAS_HOLIDAY",
    /// The connection is not subscribed to spot prices of the symbol.
    NotSubscribedToSpots => "NOT_SUBSCRIBED_TO_SPOTS",
    /// The subscription already exists.
    AlreadySubscribed => "ALREADY_SUBSCRIBED",
    /// The symbol was not found.
    SymbolNotFound => "SYMBOL_NOT_FOUND",
    /// The symbol is not known to the server.
    UnknownSymbol => "UNKNOWN_SYMBOL",
    /// The requested time range is not valid.
    IncorrectBoundaries => "INCORRECT_BOUNDARIES",
    /// There are no quotes to trade on.
    NoQuotes => "NO_QUOTES",
    /// The account lacks the funds for the trade.
    NotEnoughMoney => "NOT_ENOUGH_MONEY",
    /// The exposure limit is reached.
    MaxExposureReached => "MAX_EXPOSURE_REACHED",
    /// The position was not found.
    PositionNotFound => "POSITION_NOT_FOUND",
    /// The order was not found.
    OrderNotFound => "ORDER_NOT_FOUND",
    /// The position is not open.
    PositionNotOpen => "POSITION_NOT_OPEN",
    /// The position is locked.
    PositionLocked => "POSITION_LOCKED",
    /// The limit of open positions is reached.
    TooManyPositions => "TOO_MANY_POSITIONS",
    /// The volume is not valid.
    TradingBadVolume => "TRADING_BAD_VOLUME",
    /// The stop levels are not valid.
    TradingBadStops => "TRADING_BAD_STOPS",
    /// The prices are not valid.
    TradingBadPrices => "TRADING_BAD_PRICES",
    /// The stake is not valid.
    TradingBadStake => "TRADING_BAD_STAKE",
    /// The protection level is too close to the market price.
    ProtectionIsTooCloseToMarket => "PROTECTION_IS_TOO_CLOSE_TO_MARKET",
    /// The expiration date is not valid.
    TradingBadExpirationDate => "TRADING_BAD_EXPIRATION_DATE",
    /// The position has an execution pending.
    PendingExecution => "PENDING_EXECUTION",
    /// Trading is disabled for the symbol.
    TradingDisabled => "TRADING_DISABLED",
    /// The account may not trade.
    TradingNotAllowed => "TRADING_NOT_ALLOWED",
    /// The order cannot be cancelled.
    UnableToCancelOrder => "UNABLE_TO_CANCEL_ORDER",
    /// The order cannot be amended.
    UnableToAmendOrder => "UNABLE_TO_AMEND_ORDER",
    /// Short selling is not allowed.
    ShortSellingNotAllowed => "SHORT_SELLING_NOT_ALLOWED",
}

impl ErrorCode {
    /// Tells whether the code is one the proto files name.
    #[must_use]
    pub const fn is_known(&self) -> bool {
        !matches!(self, Self::Unknown(_))
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for ErrorCode {
    fn from(text: &str) -> Self {
        Self::from_wire(text)
    }
}

/// Error reported by the server in reply to a request.
///
/// The `Debug` output masks the account: it prints `<masked>` in place of the identifier, so
/// an error printed to a log or to the output of a test names no account. The `Display`
/// output carries the code and the explanation only. The [`account_id`](Self::account_id)
/// field itself stays readable.
#[derive(Clone, PartialEq, Eq, Error)]
#[error("server error {code}{}", detail(.description.as_deref()))]
#[non_exhaustive]
pub struct ServerError {
    /// Code of the error, preserved as sent.
    pub code: ErrorCode,
    /// Explanation given by the server, when there is one.
    pub description: Option<String>,
    /// Account the error concerns, when the server names one.
    pub account_id: Option<AccountId>,
    /// End of the maintenance window in milliseconds since the Unix epoch, when announced.
    pub maintenance_end_timestamp: Option<i64>,
}

struct Masked;

impl fmt::Debug for Masked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<masked>")
    }
}

impl fmt::Debug for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerError")
            .field("code", &self.code)
            .field("description", &self.description)
            .field("account_id", &self.account_id.map(|_| Masked))
            .field("maintenance_end_timestamp", &self.maintenance_end_timestamp)
            .finish()
    }
}

impl ServerError {
    /// Describes an error with the given code and no further detail.
    #[must_use]
    pub const fn new(code: ErrorCode) -> Self {
        Self {
            code,
            description: None,
            account_id: None,
            maintenance_end_timestamp: None,
        }
    }

    /// Adds the explanation given by the server.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds the account the error concerns.
    #[must_use]
    pub const fn with_account_id(mut self, account_id: AccountId) -> Self {
        self.account_id = Some(account_id);
        self
    }

    /// Adds the end of the maintenance window, in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn with_maintenance_end_timestamp(mut self, timestamp: i64) -> Self {
        self.maintenance_end_timestamp = Some(timestamp);
        self
    }
}

fn detail(description: Option<&str>) -> String {
    description.map_or_else(String::new, |text| format!(": {text}"))
}

/// Reasons an identifier, a price, a volume or a moment in time is rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ValueError {
    /// The identifier is not a positive integer.
    #[error("{kind} must be a positive integer, got {value}")]
    InvalidId {
        /// Kind of the identifier.
        kind: &'static str,
        /// The rejected value as given.
        value: String,
    },
    /// The text is not a decimal number.
    #[error("{kind} is not a decimal number: {text}")]
    InvalidDecimal {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected text.
        text: String,
    },
    /// The value is below zero.
    #[error("{kind} must not be negative, got {value}")]
    Negative {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected value.
        value: Decimal,
    },
    /// The symbol has more digits than a wire price carries.
    #[error("symbol digits must be at most 5, got {digits}")]
    TooManyDigits {
        /// The rejected number of digits.
        digits: u32,
    },
    /// The value is finer than the wire step or too large for the wire integer.
    #[error("{kind} cannot be carried on the wire: {value}")]
    Unrepresentable {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected value.
        value: Decimal,
    },
    /// The wire integer does not fit a decimal.
    #[error("{kind} is out of range")]
    OutOfRange {
        /// Kind of the value.
        kind: &'static str,
    },
    /// A message of the server lacks a field the value is built from.
    #[error("{kind} is missing")]
    Missing {
        /// Kind of the value.
        kind: &'static str,
    },
    /// A range of time ends before it starts.
    #[error("range from {from} ends before it starts, at {to}")]
    InvertedRange {
        /// Start of the range, in milliseconds since the Unix epoch.
        from: i64,
        /// End of the range, in milliseconds since the Unix epoch.
        to: i64,
    },
    /// The text has more characters than the server keeps.
    #[error("{kind} has {length} characters, more than the {limit} allowed")]
    TooLong {
        /// Kind of the text.
        kind: &'static str,
        /// Number of characters of the rejected text.
        length: usize,
        /// Largest number of characters allowed.
        limit: usize,
    },
    /// The value is smaller than the symbol allows.
    #[error("{kind} must be at least {minimum}, got {value}")]
    BelowMinimum {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected value.
        value: Decimal,
        /// Smallest value allowed.
        minimum: Decimal,
    },
    /// The value is larger than the symbol allows.
    #[error("{kind} must be at most {maximum}, got {value}")]
    AboveMaximum {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected value.
        value: Decimal,
        /// Largest value allowed.
        maximum: Decimal,
    },
    /// The value is not a multiple of the step of the symbol.
    #[error("{kind} must be a multiple of {step}, got {value}")]
    OffStep {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected value.
        value: Decimal,
        /// Step the value has to be a multiple of.
        step: Decimal,
    },
    /// The value has more decimal places than the symbol has digits.
    #[error("{kind} must have at most {digits} decimal places, got {value}")]
    TooFine {
        /// Kind of the value.
        kind: &'static str,
        /// The rejected value.
        value: Decimal,
        /// Digits of the symbol.
        digits: u32,
    },
    /// Two values that have to agree do not.
    #[error("{kind} does not match")]
    Mismatch {
        /// Kind of the value.
        kind: &'static str,
    },
    /// The value cannot be given for this kind of request.
    #[error("{kind} is not supported here")]
    Unsupported {
        /// Kind of the value.
        kind: &'static str,
    },
}

/// Reasons a connection fails below the level of protocol messages.
///
/// The variants carry text instead of the underlying error values so the type stays comparable
/// and cloneable.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Reading from or writing to the socket failed.
    #[error("input or output failed ({kind:?}): {message}")]
    Io {
        /// Kind of the failure.
        kind: std::io::ErrorKind,
        /// Explanation given by the operating system or the runtime.
        message: String,
    },
    /// The TLS layer refused the configuration or the handshake.
    #[error("TLS failed: {0}")]
    Tls(String),
    /// The host is not a name a certificate can be checked against.
    #[error("host is not a valid server name: {0}")]
    InvalidHost(String),
    /// The connection was not established in time.
    #[error("connecting timed out after {0:?}")]
    ConnectTimeout(Duration),
    /// A write to the socket did not complete in time.
    #[error("writing timed out after {0:?}")]
    WriteTimeout(Duration),
    /// The peer sent nothing, not even a heartbeat, for the time allowed.
    #[error("nothing was received for {0:?}")]
    Idle(Duration),
    /// A frame is longer than the limit.
    #[error("frame of {length} bytes exceeds the limit of {limit} bytes")]
    FrameTooLong {
        /// Length announced by the frame.
        length: usize,
        /// Longest frame accepted.
        limit: usize,
    },
    /// A frame does not hold a valid message.
    #[error("frame cannot be decoded: {0}")]
    Decode(String),
    /// The peer closed the connection.
    #[error("connection closed by the peer")]
    Closed,
}

impl From<std::io::Error> for TransportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

/// Reasons a token refresh fails.
///
/// The variants never carry a token or the reply of the token endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TokenError {
    /// The configuration holds no refresh token.
    #[error("no refresh token is configured")]
    NoRefreshToken,
    /// The token endpoint refused the refresh.
    #[error("token refresh refused: {code}{}", detail(Some(.description.as_str()).filter(|text| !text.is_empty())))]
    Refused {
        /// Code of the refusal as sent.
        code: String,
        /// Explanation given by the endpoint; empty when there is none.
        description: String,
    },
    /// The reply lacks the named part or the part is not valid.
    #[error("token response is malformed: {0}")]
    Malformed(&'static str),
    /// The token endpoint answered with an HTTP status that signals a failure.
    #[error("token endpoint answered with status {0}")]
    Status(u16),
}

/// What kept the answer to a request that was sent from arriving.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum Interruption {
    /// The server did not answer in time.
    #[error("no response within {0:?}")]
    Timeout(Duration),
    /// The connection failed while the answer was awaited.
    #[error("transport failed: {0}")]
    Transport(TransportError),
    /// The server asked for the request again and was told to skip it, because no request
    /// is sent twice.
    #[error("the server asked for the request again and it was not repeated")]
    Skipped,
}

/// What ended the authorisation of a session.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum SessionEndCause {
    /// The server announced that the access token of the account is no longer valid, as
    /// happens once the token pair was refreshed elsewhere or the access was revoked.
    #[error("the access token was invalidated{}", detail(.reason.as_deref()))]
    TokenInvalidated {
        /// Explanation given by the server, when it gave one.
        reason: Option<String>,
    },
    /// The server announced that it dropped the session of the account.
    #[error("the server dropped the session of the account")]
    AccountDisconnected,
    /// The server announced that it ends the session of the application.
    #[error("the server ended the session of the application{}", detail(.reason.as_deref()))]
    ClientDisconnected {
        /// Explanation given by the server, when it gave one.
        reason: Option<String>,
    },
    /// The server refused the access token when the account was authorised again after a
    /// reconnection.
    #[error("the access token was refused after a reconnection: {0}")]
    TokenRefused(ErrorCode),
}

/// A session that is no longer authorised on its connection.
///
/// Requests on the session fail with this error, without being sent, from the moment the
/// server ends the authorisation until the session is authorised again.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{} is no longer authorised: {cause}", subject(.account.as_ref()))]
#[non_exhaustive]
pub struct SessionEnded {
    /// Account whose session ended; `None` when the session of the application ended, which
    /// ends the sessions of all its accounts.
    pub account: Option<AccountId>,
    /// What ended the authorisation.
    pub cause: SessionEndCause,
}

impl SessionEnded {
    /// Describes the end of the session of `account`, or of the application for `None`.
    #[must_use]
    pub const fn new(account: Option<AccountId>, cause: SessionEndCause) -> Self {
        Self { account, cause }
    }
}

fn subject(account: Option<&AccountId>) -> String {
    account.map_or_else(
        || "the session of the application".to_owned(),
        |account| format!("the session of account {account}"),
    )
}

/// Reasons a FIX message is rejected by this library.
///
/// No variant holds the value of a field, so no credential of a logon can reach a log through
/// an error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum FixError {
    /// The bytes are not a FIX message.
    #[error("the bytes are not a FIX message: {0}")]
    Garbled(&'static str),
    /// The checksum of a message does not match its bytes.
    #[error("checksum is {found}, the bytes sum to {computed}")]
    Checksum {
        /// Checksum computed over the bytes received.
        computed: u8,
        /// Checksum the message carries.
        found: u8,
    },
    /// The body of a message is longer than the limit.
    #[error("body of {length} bytes exceeds the limit of {limit} bytes")]
    TooLong {
        /// Length of the body in bytes.
        length: usize,
        /// Largest length accepted.
        limit: usize,
    },
    /// A required field is absent.
    #[error("message {msg_type} lacks the required tag {tag}")]
    MissingField {
        /// Type of the message.
        msg_type: String,
        /// Tag that is absent.
        tag: u32,
    },
    /// A field holds a value of the wrong form.
    #[error("message {msg_type} holds an invalid value under tag {tag}")]
    InvalidField {
        /// Type of the message.
        msg_type: String,
        /// Tag whose value is invalid.
        tag: u32,
    },
    /// The type of the message is not known to this library.
    #[error("message type {0} is not supported")]
    UnsupportedMessage(String),
    /// The server answered the logon with a logout.
    #[error("the server refused the logon: {}", explained(text.as_deref()))]
    LogonRefused {
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// A message of this type arrived where another was due.
    #[error("message of type {0} arrived where another was due")]
    UnexpectedMessage(String),
    /// The server rejected a message at the session level.
    #[error("the server rejected message {ref_seq_num}: {}", explained(text.as_deref()))]
    Rejected {
        /// Sequence number of the message rejected.
        ref_seq_num: u64,
        /// Tag the server objects to.
        tag: Option<u32>,
        /// Reason code of the server.
        reason: Option<u32>,
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// The server refused a request at the application level.
    #[error("the server refused the request: {}", explained(text.as_deref()))]
    BusinessRejected {
        /// Identifier of the request refused.
        ref_id: Option<String>,
        /// Reason code of the server.
        reason: Option<u32>,
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// The server refused a subscription to prices.
    #[error("the server refused the market data request: {}", explained(text.as_deref()))]
    MarketDataRejected {
        /// Reason code of the server.
        reason: Option<u32>,
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// The server took an order and then refused, cancelled or expired it.
    #[error("the server refused the order: {}", explained(text.as_deref()))]
    OrderRejected {
        /// Identifier the client gave the order.
        cl_ord_id: Option<String>,
        /// Reason code of the server.
        reason: Option<u32>,
        /// Explanation the server gave.
        text: Option<String>,
    },
    /// The server refused to cancel or to replace an order, which stays as it was.
    #[error("the server refused to change the order: {}", explained(text.as_deref()))]
    CancelRejected {
        /// Identifier the client gave the refused request.
        cl_ord_id: String,
        /// Identifier of the order the request named.
        orig_cl_ord_id: Option<String>,
        /// Whether a cancellation or a replacement was refused.
        change: RefusedChange,
        /// Explanation the server gave.
        text: Option<String>,
    },
}

/// Change of an order the server refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedChange {
    /// A cancellation.
    Cancel,
    /// A replacement of the terms.
    Replace,
}

fn explained(text: Option<&str>) -> &str {
    text.unwrap_or("no explanation given")
}

/// Any failure reported by this library.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConnectorError {
    /// The configuration was rejected.
    #[error("configuration rejected: {0}")]
    Config(#[from] ConfigError),
    /// The server refused a request.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// An identifier, a price or a volume was rejected.
    #[error("value rejected: {0}")]
    Value(#[from] ValueError),
    /// The connection failed below the level of protocol messages.
    #[error("transport failed: {0}")]
    Transport(#[from] TransportError),
    /// A token refresh failed.
    #[error(transparent)]
    Token(#[from] TokenError),
    /// A FIX message was rejected, by this library or by the server.
    #[error("FIX failure: {0}")]
    Fix(#[from] FixError),
    /// The server did not answer a request in time.
    #[error("no response within {0:?}")]
    Timeout(Duration),
    /// A request that must not be sent twice was sent, and the connection failed or the time
    /// allowed ran out before the server answered: the server may or may not have acted on it.
    #[error("the request was sent and its outcome is unknown: {0}")]
    OutcomeUnknown(Interruption),
    /// The server ended the authorisation of the session the request belongs to.
    #[error(transparent)]
    SessionEnded(#[from] SessionEnded),
    /// The connection is shut down and accepts no further requests.
    #[error("connection is shut down")]
    Closed,
    /// A reader of the events of the connection fell behind and events were dropped unread.
    #[error("{skipped} events were dropped because their reader fell behind")]
    Lagged {
        /// Number of events dropped unread.
        skipped: u64,
    },
    /// The server announced more historical data than it sent and the rest cannot be asked for,
    /// so data between the two moments, both included, is missing.
    #[error("the server announced more data between {from} and {to} than it sent")]
    Incomplete {
        /// Start of the part that is missing.
        from: Timestamp,
        /// End of the part that is missing.
        to: Timestamp,
    },
    /// The server answered with a message of another type than the request calls for.
    #[error("expected payload type {expected}, received {received}")]
    UnexpectedResponse {
        /// Payload type of the response the request calls for.
        expected: u32,
        /// Payload type the server sent.
        received: u32,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::error::Error;

    use std::time::Duration;

    use super::{
        ConfigError, ConnectorError, ErrorCode, FixError, Interruption, ServerError,
        SessionEndCause, SessionEnded, TransportError, ValueError,
    };
    use crate::common::ids::AccountId;
    use crate::common::time::Timestamp;
    use crate::proto_text;

    fn proto_codes(name: &str) -> Vec<String> {
        proto_text::enums()
            .remove(name)
            .expect("enum is present in the proto files")
            .into_iter()
            .map(|(text, _)| text)
            .collect()
    }

    #[test]
    fn every_code_in_the_proto_files_has_a_variant() {
        let common = proto_codes("protoerrorcode");
        let open_api = proto_codes("protooaerrorcode");
        assert_eq!(common.len(), 10);
        assert_eq!(open_api.len(), 43);
        let mut seen = HashSet::new();
        for text in common.iter().chain(&open_api) {
            let code = ErrorCode::from_wire(text);
            assert!(code.is_known(), "{text} has no variant");
            assert_eq!(code.as_str(), text);
            assert_eq!(code.to_string(), *text);
            assert_eq!(ErrorCode::from(text.as_str()), code);
            assert!(seen.insert(code), "{text} shares a variant");
        }
        assert_eq!(seen.len(), 53);
    }

    #[test]
    fn selected_codes_map_to_their_variants() {
        assert_eq!(
            ErrorCode::from_wire("MARKET_CLOSED"),
            ErrorCode::MarketClosed
        );
        assert_eq!(
            ErrorCode::from_wire("OA_AUTH_TOKEN_EXPIRED"),
            ErrorCode::OaAuthTokenExpired
        );
        assert_eq!(
            ErrorCode::from_wire("CH_ACCESS_TOKEN_INVALID"),
            ErrorCode::ChAccessTokenInvalid
        );
        assert_eq!(
            ErrorCode::from_wire("NOT_ENOUGH_MONEY"),
            ErrorCode::NotEnoughMoney
        );
        assert_eq!(
            ErrorCode::from_wire("SHORT_SELLING_NOT_ALLOWED"),
            ErrorCode::ShortSellingNotAllowed
        );
    }

    #[test]
    fn unknown_codes_are_preserved_as_text() {
        for text in ["BRAND_NEW_CODE", "market_closed", "", "104"] {
            let code = ErrorCode::from_wire(text);
            assert_eq!(code, ErrorCode::Unknown(text.to_owned()));
            assert!(!code.is_known());
            assert_eq!(code.as_str(), text);
            assert_eq!(code.to_string(), text);
        }
    }

    #[test]
    fn server_error_prints_code_and_description() {
        let bare = ServerError::new(ErrorCode::MarketClosed);
        assert_eq!(bare.to_string(), "server error MARKET_CLOSED");
        assert_eq!(bare.description, None);
        assert_eq!(bare.maintenance_end_timestamp, None);
        let detailed = ServerError::new(ErrorCode::from_wire("BRAND_NEW_CODE"))
            .with_description("try later")
            .with_maintenance_end_timestamp(1_700_000_000_000);
        assert_eq!(
            detailed.to_string(),
            "server error BRAND_NEW_CODE: try later"
        );
        assert_eq!(detailed.maintenance_end_timestamp, Some(1_700_000_000_000));
        assert_eq!(detailed.account_id, None);
        let account = AccountId::new(7).expect("positive id");
        assert_eq!(detailed.with_account_id(account).account_id, Some(account));
    }

    #[test]
    fn server_error_debug_masks_the_account_and_keeps_the_field() {
        let account = AccountId::new(987_654_321).expect("positive id");
        let error = ServerError::new(ErrorCode::MarketClosed)
            .with_description("closed")
            .with_account_id(account);
        let printed = format!("{error:?} {error:#?} {error}");
        assert!(!printed.contains("987654321"), "{printed}");
        assert!(printed.contains("account_id: Some(<masked>)"), "{printed}");
        assert!(printed.contains("MarketClosed"), "{printed}");
        assert!(printed.contains("closed"), "{printed}");
        assert_eq!(error.account_id, Some(account));
        let wrapped = format!("{:?}", ConnectorError::from(error));
        assert!(!wrapped.contains("987654321"), "{wrapped}");
        let bare = format!("{:?}", ServerError::new(ErrorCode::MarketClosed));
        assert!(bare.contains("account_id: None"), "{bare}");
    }

    #[test]
    fn session_end_names_the_session_and_the_cause() {
        let account = AccountId::new(7).expect("positive id");
        let invalidated = SessionEnded::new(
            Some(account),
            SessionEndCause::TokenInvalidated {
                reason: Some("refreshed".to_owned()),
            },
        );
        assert_eq!(
            invalidated.to_string(),
            "the session of account 7 is no longer authorised: the access token was invalidated: refreshed"
        );
        assert_eq!(
            ConnectorError::from(invalidated.clone()),
            ConnectorError::SessionEnded(invalidated.clone())
        );
        assert_eq!(
            ConnectorError::from(invalidated.clone()).to_string(),
            invalidated.to_string()
        );
        assert_eq!(
            SessionEnded::new(Some(account), SessionEndCause::AccountDisconnected).to_string(),
            "the session of account 7 is no longer authorised: the server dropped the session of the account"
        );
        assert_eq!(
            SessionEnded::new(None, SessionEndCause::ClientDisconnected { reason: None })
                .to_string(),
            "the session of the application is no longer authorised: the server ended the session of the application"
        );
        assert_eq!(
            SessionEnded::new(
                Some(account),
                SessionEndCause::TokenRefused(ErrorCode::ChAccessTokenInvalid)
            )
            .to_string(),
            "the session of account 7 is no longer authorised: the access token was refused after a reconnection: CH_ACCESS_TOKEN_INVALID"
        );
    }

    #[test]
    fn connector_error_wraps_its_sources() {
        let server = ServerError::new(ErrorCode::TradingBadVolume).with_description("too small");
        let error = ConnectorError::from(server.clone());
        assert_eq!(error, ConnectorError::Server(server));
        assert_eq!(
            error.to_string(),
            "server error TRADING_BAD_VOLUME: too small"
        );
        let config = ConnectorError::from(ConfigError::InvalidPort);
        assert_eq!(
            config.to_string(),
            "configuration rejected: port must not be zero"
        );
        let value = ConnectorError::from(ValueError::TooManyDigits { digits: 9 });
        assert_eq!(
            value.to_string(),
            "value rejected: symbol digits must be at most 5, got 9"
        );
        assert_eq!(
            config.source().map(ToString::to_string),
            Some("port must not be zero".to_owned())
        );
    }

    #[test]
    fn transport_error_keeps_the_kind_of_an_io_failure() {
        let source = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
        let error = TransportError::from(source);
        assert_eq!(
            error,
            TransportError::Io {
                kind: std::io::ErrorKind::ConnectionReset,
                message: "reset by peer".to_owned(),
            }
        );
        assert_eq!(
            ConnectorError::from(error).to_string(),
            "transport failed: input or output failed (ConnectionReset): reset by peer"
        );
        assert_eq!(
            TransportError::FrameTooLong {
                length: 9,
                limit: 8
            }
            .to_string(),
            "frame of 9 bytes exceeds the limit of 8 bytes"
        );
        assert_eq!(
            TransportError::ConnectTimeout(Duration::from_secs(15)).to_string(),
            "connecting timed out after 15s"
        );
        assert_eq!(
            TransportError::Closed.to_string(),
            "connection closed by the peer"
        );
        assert_eq!(
            TransportError::WriteTimeout(Duration::from_secs(10)).to_string(),
            "writing timed out after 10s"
        );
        assert_eq!(
            TransportError::Idle(Duration::from_secs(40)).to_string(),
            "nothing was received for 40s"
        );
        let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
        assert_eq!(
            ConnectorError::Incomplete {
                from: at(5),
                to: at(9)
            }
            .to_string(),
            "the server announced more data between 5 and 9 than it sent"
        );
        assert_eq!(
            ConnectorError::OutcomeUnknown(Interruption::Timeout(Duration::from_secs(2)))
                .to_string(),
            "the request was sent and its outcome is unknown: no response within 2s"
        );
        assert_eq!(
            ConnectorError::OutcomeUnknown(Interruption::Transport(TransportError::Closed))
                .to_string(),
            "the request was sent and its outcome is unknown: transport failed: connection closed by the peer"
        );
        assert_eq!(
            ConnectorError::Lagged { skipped: 3 }.to_string(),
            "3 events were dropped because their reader fell behind"
        );
    }

    #[test]
    fn fix_refusals_carry_the_explanation_of_the_server_or_say_that_none_came() {
        assert_eq!(
            FixError::LogonRefused {
                text: Some("RET_INVALID_DATA".to_owned())
            }
            .to_string(),
            "the server refused the logon: RET_INVALID_DATA"
        );
        assert_eq!(
            FixError::LogonRefused { text: None }.to_string(),
            "the server refused the logon: no explanation given"
        );
        assert_eq!(
            FixError::UnexpectedMessage("0".to_owned()).to_string(),
            "message of type 0 arrived where another was due"
        );
        assert_eq!(
            FixError::Rejected {
                ref_seq_num: 7,
                tag: Some(55),
                reason: Some(5),
                text: Some("Value is incorrect".to_owned())
            }
            .to_string(),
            "the server rejected message 7: Value is incorrect"
        );
        assert_eq!(
            FixError::BusinessRejected {
                ref_id: Some("a".to_owned()),
                reason: None,
                text: None
            }
            .to_string(),
            "the server refused the request: no explanation given"
        );
    }

    #[test]
    fn fix_errors_name_the_message_and_the_tag_and_never_a_value() {
        let missing = FixError::MissingField {
            msg_type: "A".to_owned(),
            tag: 108,
        };
        assert_eq!(missing.to_string(), "message A lacks the required tag 108");
        assert_eq!(
            ConnectorError::from(missing).to_string(),
            "FIX failure: message A lacks the required tag 108"
        );
        assert_eq!(
            FixError::InvalidField {
                msg_type: "A".to_owned(),
                tag: 554
            }
            .to_string(),
            "message A holds an invalid value under tag 554"
        );
        assert_eq!(
            FixError::Checksum {
                computed: 7,
                found: 8
            }
            .to_string(),
            "checksum is 8, the bytes sum to 7"
        );
        assert_eq!(
            FixError::TooLong {
                length: 9,
                limit: 8
            }
            .to_string(),
            "body of 9 bytes exceeds the limit of 8 bytes"
        );
        assert_eq!(
            FixError::Garbled("no opening").to_string(),
            "the bytes are not a FIX message: no opening"
        );
        assert_eq!(
            FixError::UnsupportedMessage("ZZ".to_owned()).to_string(),
            "message type ZZ is not supported"
        );
    }
}
