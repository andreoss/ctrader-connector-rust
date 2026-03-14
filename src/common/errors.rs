//! Error types shared by every protocol.

use std::fmt;

use thiserror::Error;

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
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("server error {code}{}", detail(.description.as_deref()))]
#[non_exhaustive]
pub struct ServerError {
    /// Code of the error, preserved as sent.
    pub code: ErrorCode,
    /// Explanation given by the server, when there is one.
    pub description: Option<String>,
    /// End of the maintenance window in milliseconds since the Unix epoch, when announced.
    pub maintenance_end_timestamp: Option<i64>,
}

impl ServerError {
    /// Describes an error with the given code and no further detail.
    #[must_use]
    pub const fn new(code: ErrorCode) -> Self {
        Self {
            code,
            description: None,
            maintenance_end_timestamp: None,
        }
    }

    /// Adds the explanation given by the server.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::error::Error;

    use super::{ConfigError, ConnectorError, ErrorCode, ServerError};
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
        assert_eq!(
            config.source().map(ToString::to_string),
            Some("port must not be zero".to_owned())
        );
    }
}
