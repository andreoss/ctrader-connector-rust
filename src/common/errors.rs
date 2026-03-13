//! Error types shared by every protocol.

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
