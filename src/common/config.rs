//! Connection configuration and its builders.
//!
//! A configuration targets the demo environment unless the live one is requested:
//!
//! ```
//! use ctrader_connector::common::config::{ConfigurationOpenApi, Environment};
//!
//! let configuration = ConfigurationOpenApi::builder()
//!     .client_id("id")
//!     .client_secret("secret")
//!     .access_token("token")
//!     .build()?;
//! assert_eq!(configuration.environment(), Environment::Demo);
//! # Ok::<(), ctrader_connector::common::errors::ConfigError>(())
//! ```

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::common::errors::ConfigError;

/// Host of the demo environment.
pub const DEMO_HOST: &str = "demo.ctraderapi.com";
/// Host of the live environment.
pub const LIVE_HOST: &str = "live.ctraderapi.com";
/// Port accepting Protobuf messages over TLS.
pub const DEFAULT_PORT: u16 = 5035;
/// Endpoint that exchanges a refresh token for a new access token.
pub const DEFAULT_TOKEN_URL: &str = "https://connect.spotware.com/apps/token";
/// Default time allowed for a single request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default number of times a failed request is retried.
pub const DEFAULT_RETRIES: u32 = 3;
/// Default pause before a retry.
pub const DEFAULT_BACKOFF: Duration = Duration::from_secs(1);
/// Default pause before a dropped connection is opened again.
pub const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Environment variable holding the application client id.
pub const ENV_CLIENT_ID: &str = "BROKER_CLIENT_ID";
/// Environment variable holding the application client secret.
pub const ENV_CLIENT_SECRET: &str = "BROKER_CLIENT_SECRET";
/// Environment variable holding the access token.
pub const ENV_ACCESS_TOKEN: &str = "BROKER_ACCESS_TOKEN";
/// Environment variable holding the refresh token.
pub const ENV_REFRESH_TOKEN: &str = "BROKER_REFRESH_TOKEN";
/// Environment variable overriding the host of the demo environment.
pub const ENV_DEMO_HOST: &str = "BROKER_DEMO_HOST";
/// Environment variable overriding the token URL.
pub const ENV_TOKEN_URL: &str = "BROKER_TOKEN_URL";
/// Environment variable selecting the environment, `demo` or `live`.
pub const ENV_ENVIRONMENT: &str = "BROKER_ENVIRONMENT";

const CLIENT_ID: &str = "client_id";
const CLIENT_SECRET: &str = "client_secret";
const ACCESS_TOKEN: &str = "access_token";
const REFRESH_TOKEN: &str = "refresh_token";
const FIX_PASSWORD: &str = "fix_password";
const TIMEOUT: &str = "timeout";
const BACKOFF: &str = "backoff";
const RECONNECT_DELAY: &str = "reconnect_delay";

/// Trading environment a connection targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Environment {
    /// Demo accounts; the default.
    #[default]
    Demo,
    /// Live accounts.
    Live,
}

impl Environment {
    /// Returns the default host of the environment.
    #[must_use]
    pub const fn host(self) -> &'static str {
        match self {
            Self::Demo => DEMO_HOST,
            Self::Live => LIVE_HOST,
        }
    }

    /// Returns the lower-case name of the environment.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Demo => "demo",
            Self::Live => "live",
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Environment {
    type Err = ConfigError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "demo" => Ok(Self::Demo),
            "live" => Ok(Self::Live),
            other => Err(ConfigError::InvalidEnvironment(other.to_owned())),
        }
    }
}

/// Identifier of the application registered with the broker.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(String);

impl ClientId {
    /// Wraps a client id.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Missing`] when the value is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        filled(value.into(), CLIENT_ID).map(Self)
    }

    /// Returns the client id.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! secret {
    ($(#[$doc:meta])* $name:ident, $option:expr) => {
        $(#[$doc])*
        ///
        /// The value never appears in `Debug` output.
        #[derive(Clone, PartialEq, Eq)]
        pub struct $name(String);

        impl $name {
            /// Wraps a secret value.
            ///
            /// # Errors
            ///
            /// Returns [`ConfigError::Missing`] when the value is empty.
            pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
                filled(value.into(), $option).map(Self)
            }

            /// Returns the secret value; the caller must keep it out of logs.
            #[must_use]
            pub fn expose(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "(***)"))
            }
        }
    };
}

secret!(
    /// Secret of the application registered with the broker.
    ClientSecret,
    CLIENT_SECRET
);
secret!(
    /// Token authorising access to trading accounts.
    AccessToken,
    ACCESS_TOKEN
);
secret!(
    /// Token exchanged for a new access token.
    RefreshToken,
    REFRESH_TOKEN
);
secret!(
    /// Password of the FIX sessions of a trading account.
    FixPassword,
    FIX_PASSWORD
);

fn filled(value: String, option: &'static str) -> Result<String, ConfigError> {
    if value.trim().is_empty() {
        Err(ConfigError::Missing(option))
    } else {
        Ok(value)
    }
}

fn host_name(value: String) -> Result<String, ConfigError> {
    let bare = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-');
    if bare {
        Ok(value)
    } else {
        Err(ConfigError::InvalidHost(value))
    }
}

fn https_url(value: String) -> Result<String, ConfigError> {
    let valid = value.strip_prefix("https://").is_some_and(|rest| {
        !rest.is_empty() && !rest.starts_with('/') && !rest.contains(char::is_whitespace)
    });
    if valid {
        Ok(value)
    } else {
        Err(ConfigError::InvalidTokenUrl(value))
    }
}

fn positive(value: Duration, option: &'static str) -> Result<Duration, ConfigError> {
    if value.is_zero() {
        Err(ConfigError::ZeroDuration(option))
    } else {
        Ok(value)
    }
}

/// Validated configuration of an Open API connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationOpenApi {
    environment: Environment,
    host: String,
    port: u16,
    client_id: ClientId,
    client_secret: ClientSecret,
    access_token: AccessToken,
    refresh_token: Option<RefreshToken>,
    token_url: String,
    timeout: Duration,
    retries: u32,
    backoff: Duration,
    reconnect_delay: Duration,
}

impl ConfigurationOpenApi {
    /// Starts a configuration for the demo environment.
    pub fn builder() -> ConfigurationOpenApiBuilder {
        Self::demo()
    }

    /// Starts a configuration for the demo environment.
    pub fn demo() -> ConfigurationOpenApiBuilder {
        ConfigurationOpenApiBuilder::new(Environment::Demo)
    }

    /// Starts a configuration for the live environment.
    pub fn live() -> ConfigurationOpenApiBuilder {
        ConfigurationOpenApiBuilder::new(Environment::Live)
    }

    /// Builds a configuration from the process environment.
    ///
    /// See [`ConfigurationOpenApi::from_lookup`] for the variables read.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] when a required variable is absent or a value is rejected.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Builds a configuration from variables resolved by `lookup`.
    ///
    /// [`ENV_CLIENT_ID`], [`ENV_CLIENT_SECRET`] and [`ENV_ACCESS_TOKEN`] are required.
    /// [`ENV_REFRESH_TOKEN`], [`ENV_TOKEN_URL`] and [`ENV_ENVIRONMENT`] are optional, and
    /// [`ENV_DEMO_HOST`] replaces the host only when the environment is demo. An empty value
    /// counts as absent.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] when a required variable is absent or a value is rejected.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let optional = |key: &str| lookup(key).filter(|value| !value.trim().is_empty());
        let required = |key: &'static str| optional(key).ok_or(ConfigError::Missing(key));
        let environment = match optional(ENV_ENVIRONMENT) {
            Some(name) => name.parse()?,
            None => Environment::Demo,
        };
        let mut builder = ConfigurationOpenApiBuilder::new(environment)
            .client_id(required(ENV_CLIENT_ID)?)
            .client_secret(required(ENV_CLIENT_SECRET)?)
            .access_token(required(ENV_ACCESS_TOKEN)?);
        if let Some(token) = optional(ENV_REFRESH_TOKEN) {
            builder = builder.refresh_token(token);
        }
        if let Some(url) = optional(ENV_TOKEN_URL) {
            builder = builder.token_url(url);
        }
        if environment == Environment::Demo {
            if let Some(host) = optional(ENV_DEMO_HOST) {
                builder = builder.host(host);
            }
        }
        builder.build()
    }

    /// Returns the environment the connection targets.
    #[must_use]
    pub const fn environment(&self) -> Environment {
        self.environment
    }

    /// Returns the host to connect to.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the port to connect to.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns the application client id.
    #[must_use]
    pub const fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Returns the application client secret.
    #[must_use]
    pub const fn client_secret(&self) -> &ClientSecret {
        &self.client_secret
    }

    /// Returns the access token.
    #[must_use]
    pub const fn access_token(&self) -> &AccessToken {
        &self.access_token
    }

    /// Returns the refresh token, when one was given.
    #[must_use]
    pub const fn refresh_token(&self) -> Option<&RefreshToken> {
        self.refresh_token.as_ref()
    }

    /// Returns the endpoint that refreshes the access token.
    #[must_use]
    pub fn token_url(&self) -> &str {
        &self.token_url
    }

    /// Returns the time allowed for a single request.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the number of times a failed request is retried.
    #[must_use]
    pub const fn retries(&self) -> u32 {
        self.retries
    }

    /// Returns the pause before a retry.
    #[must_use]
    pub const fn backoff(&self) -> Duration {
        self.backoff
    }

    /// Returns the pause before a dropped connection is opened again.
    #[must_use]
    pub const fn reconnect_delay(&self) -> Duration {
        self.reconnect_delay
    }
}

/// Builder of a [`ConfigurationOpenApi`].
///
/// Values are checked by [`ConfigurationOpenApiBuilder::build`], not by the setters.
#[derive(Debug, Clone)]
#[must_use]
pub struct ConfigurationOpenApiBuilder {
    environment: Environment,
    host: Option<String>,
    port: u16,
    client_id: Option<String>,
    client_secret: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    token_url: String,
    timeout: Duration,
    retries: u32,
    backoff: Duration,
    reconnect_delay: Duration,
}

impl ConfigurationOpenApiBuilder {
    fn new(environment: Environment) -> Self {
        Self {
            environment,
            host: None,
            port: DEFAULT_PORT,
            client_id: None,
            client_secret: None,
            access_token: None,
            refresh_token: None,
            token_url: DEFAULT_TOKEN_URL.to_owned(),
            timeout: DEFAULT_TIMEOUT,
            retries: DEFAULT_RETRIES,
            backoff: DEFAULT_BACKOFF,
            reconnect_delay: DEFAULT_RECONNECT_DELAY,
        }
    }

    /// Sets the host, replacing the default of the environment.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Sets the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the application client id.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the application client secret.
    pub fn client_secret(mut self, client_secret: impl Into<String>) -> Self {
        self.client_secret = Some(client_secret.into());
        self
    }

    /// Sets the access token.
    pub fn access_token(mut self, access_token: impl Into<String>) -> Self {
        self.access_token = Some(access_token.into());
        self
    }

    /// Sets the refresh token.
    pub fn refresh_token(mut self, refresh_token: impl Into<String>) -> Self {
        self.refresh_token = Some(refresh_token.into());
        self
    }

    /// Sets the endpoint that refreshes the access token.
    pub fn token_url(mut self, token_url: impl Into<String>) -> Self {
        self.token_url = token_url.into();
        self
    }

    /// Sets the time allowed for a single request.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the number of times a failed request is retried; zero disables retries.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Sets the pause before a retry.
    pub fn backoff(mut self, backoff: Duration) -> Self {
        self.backoff = backoff;
        self
    }

    /// Sets the pause before a dropped connection is opened again.
    pub fn reconnect_delay(mut self, reconnect_delay: Duration) -> Self {
        self.reconnect_delay = reconnect_delay;
        self
    }

    /// Checks every value and returns the configuration.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] when the client id, client secret or access token is absent
    /// or empty, the refresh token is empty, the host is not a bare host name, the port is
    /// zero, the token URL is not an `https` URL, or a duration is zero.
    pub fn build(self) -> Result<ConfigurationOpenApi, ConfigError> {
        let host = self
            .host
            .unwrap_or_else(|| self.environment.host().to_owned());
        Ok(ConfigurationOpenApi {
            environment: self.environment,
            host: host_name(host)?,
            port: if self.port == 0 {
                return Err(ConfigError::InvalidPort);
            } else {
                self.port
            },
            client_id: ClientId::new(self.client_id.unwrap_or_default())?,
            client_secret: ClientSecret::new(self.client_secret.unwrap_or_default())?,
            access_token: AccessToken::new(self.access_token.unwrap_or_default())?,
            refresh_token: self.refresh_token.map(RefreshToken::new).transpose()?,
            token_url: https_url(self.token_url)?,
            timeout: positive(self.timeout, TIMEOUT)?,
            retries: self.retries,
            backoff: positive(self.backoff, BACKOFF)?,
            reconnect_delay: positive(self.reconnect_delay, RECONNECT_DELAY)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::{
        AccessToken, ClientId, ClientSecret, ConfigurationOpenApi, ConfigurationOpenApiBuilder,
        DEFAULT_BACKOFF, DEFAULT_PORT, DEFAULT_RECONNECT_DELAY, DEFAULT_RETRIES, DEFAULT_TIMEOUT,
        DEFAULT_TOKEN_URL, DEMO_HOST, ENV_ACCESS_TOKEN, ENV_CLIENT_ID, ENV_CLIENT_SECRET,
        ENV_DEMO_HOST, ENV_ENVIRONMENT, ENV_REFRESH_TOKEN, ENV_TOKEN_URL, Environment, LIVE_HOST,
        RefreshToken,
    };
    use crate::common::errors::ConfigError;

    fn filled(builder: ConfigurationOpenApiBuilder) -> ConfigurationOpenApiBuilder {
        builder
            .client_id("id")
            .client_secret("secret")
            .access_token("access")
    }

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        move |key| map.get(key).cloned()
    }

    const REQUIRED: [(&str, &str); 3] = [
        (ENV_CLIENT_ID, "id"),
        (ENV_CLIENT_SECRET, "secret"),
        (ENV_ACCESS_TOKEN, "access"),
    ];

    fn with(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
        REQUIRED.iter().chain(extra).copied().collect()
    }

    #[test]
    fn builder_defaults_to_demo() -> Result<(), ConfigError> {
        let configuration = filled(ConfigurationOpenApi::builder()).build()?;
        assert_eq!(configuration.environment(), Environment::Demo);
        assert_eq!(configuration.host(), DEMO_HOST);
        assert_eq!(configuration.host(), "demo.ctraderapi.com");
        assert_eq!(configuration.port(), DEFAULT_PORT);
        assert_eq!(configuration.port(), 5035);
        assert_eq!(configuration.client_id().as_str(), "id");
        assert_eq!(configuration.client_secret().expose(), "secret");
        assert_eq!(configuration.access_token().expose(), "access");
        assert_eq!(configuration.refresh_token(), None);
        assert_eq!(configuration.token_url(), DEFAULT_TOKEN_URL);
        assert_eq!(configuration.timeout(), DEFAULT_TIMEOUT);
        assert_eq!(configuration.retries(), DEFAULT_RETRIES);
        assert_eq!(configuration.backoff(), DEFAULT_BACKOFF);
        assert_eq!(configuration.reconnect_delay(), DEFAULT_RECONNECT_DELAY);
        Ok(())
    }

    #[test]
    fn demo_constructor_equals_the_default_builder() -> Result<(), ConfigError> {
        let demo = filled(ConfigurationOpenApi::demo()).build()?;
        let default = filled(ConfigurationOpenApi::builder()).build()?;
        assert_eq!(demo, default);
        Ok(())
    }

    #[test]
    fn live_constructor_targets_the_live_host() -> Result<(), ConfigError> {
        let configuration = filled(ConfigurationOpenApi::live()).build()?;
        assert_eq!(configuration.environment(), Environment::Live);
        assert_eq!(configuration.host(), LIVE_HOST);
        assert_eq!(configuration.host(), "live.ctraderapi.com");
        assert_eq!(configuration.port(), 5035);
        Ok(())
    }

    #[test]
    fn overrides_replace_every_default() -> Result<(), ConfigError> {
        let configuration = filled(ConfigurationOpenApi::builder())
            .host("proxy.example.test")
            .port(6000)
            .refresh_token("refresh")
            .token_url("https://auth.example.test/token")
            .timeout(Duration::from_millis(250))
            .retries(0)
            .backoff(Duration::from_millis(40))
            .reconnect_delay(Duration::from_secs(9))
            .build()?;
        assert_eq!(configuration.host(), "proxy.example.test");
        assert_eq!(configuration.port(), 6000);
        assert_eq!(
            configuration.refresh_token().map(RefreshToken::expose),
            Some("refresh")
        );
        assert_eq!(configuration.token_url(), "https://auth.example.test/token");
        assert_eq!(configuration.timeout(), Duration::from_millis(250));
        assert_eq!(configuration.retries(), 0);
        assert_eq!(configuration.backoff(), Duration::from_millis(40));
        assert_eq!(configuration.reconnect_delay(), Duration::from_secs(9));
        Ok(())
    }

    #[test]
    fn missing_credentials_are_rejected() {
        let no_id = ConfigurationOpenApi::builder()
            .client_secret("secret")
            .access_token("access")
            .build();
        assert_eq!(no_id, Err(ConfigError::Missing("client_id")));
        let no_secret = ConfigurationOpenApi::builder()
            .client_id("id")
            .access_token("access")
            .build();
        assert_eq!(no_secret, Err(ConfigError::Missing("client_secret")));
        let no_token = ConfigurationOpenApi::builder()
            .client_id("id")
            .client_secret("secret")
            .build();
        assert_eq!(no_token, Err(ConfigError::Missing("access_token")));
    }

    #[test]
    fn blank_credentials_are_rejected() {
        let builder = || filled(ConfigurationOpenApi::builder());
        assert_eq!(
            builder().client_id(" ").build(),
            Err(ConfigError::Missing("client_id"))
        );
        assert_eq!(
            builder().client_secret("").build(),
            Err(ConfigError::Missing("client_secret"))
        );
        assert_eq!(
            builder().access_token("").build(),
            Err(ConfigError::Missing("access_token"))
        );
        assert_eq!(
            builder().refresh_token("").build(),
            Err(ConfigError::Missing("refresh_token"))
        );
    }

    #[test]
    fn bad_hosts_and_ports_are_rejected() {
        let builder = || filled(ConfigurationOpenApi::builder());
        for host in ["", "host name", "https://host", "host:5035", "host/path"] {
            assert_eq!(
                builder().host(host).build(),
                Err(ConfigError::InvalidHost(host.to_owned()))
            );
        }
        assert_eq!(builder().port(0).build(), Err(ConfigError::InvalidPort));
    }

    #[test]
    fn bad_token_urls_are_rejected() {
        let builder = || filled(ConfigurationOpenApi::builder());
        for url in [
            "",
            "http://auth.example.test/token",
            "https://",
            "https:///token",
            "https://a b",
        ] {
            assert_eq!(
                builder().token_url(url).build(),
                Err(ConfigError::InvalidTokenUrl(url.to_owned()))
            );
        }
    }

    #[test]
    fn zero_durations_are_rejected() {
        let builder = || filled(ConfigurationOpenApi::builder());
        assert_eq!(
            builder().timeout(Duration::ZERO).build(),
            Err(ConfigError::ZeroDuration("timeout"))
        );
        assert_eq!(
            builder().backoff(Duration::ZERO).build(),
            Err(ConfigError::ZeroDuration("backoff"))
        );
        assert_eq!(
            builder().reconnect_delay(Duration::ZERO).build(),
            Err(ConfigError::ZeroDuration("reconnect_delay"))
        );
    }

    #[test]
    fn secrets_are_redacted_in_debug_output() -> Result<(), ConfigError> {
        let configuration = filled(ConfigurationOpenApi::builder())
            .client_secret("s3cr3t-value")
            .access_token("acc3ss-value")
            .refresh_token("r3fr3sh-value")
            .build()?;
        let printed = format!("{configuration:?}");
        assert!(!printed.contains("s3cr3t-value"));
        assert!(!printed.contains("acc3ss-value"));
        assert!(!printed.contains("r3fr3sh-value"));
        assert!(printed.contains("ClientSecret(***)"));
        assert!(printed.contains("AccessToken(***)"));
        assert!(printed.contains("RefreshToken(***)"));
        assert_eq!(
            format!("{:?}", ClientSecret::new("x")?),
            "ClientSecret(***)"
        );
        assert_eq!(format!("{:?}", AccessToken::new("x")?), "AccessToken(***)");
        assert_eq!(format!("{:?}", ClientId::new("x")?), "ClientId(\"x\")");
        Ok(())
    }

    #[test]
    fn environment_parses_and_prints() {
        assert_eq!("demo".parse(), Ok(Environment::Demo));
        assert_eq!("live".parse(), Ok(Environment::Live));
        assert_eq!(
            "paper".parse::<Environment>(),
            Err(ConfigError::InvalidEnvironment("paper".to_owned()))
        );
        assert_eq!(Environment::Demo.to_string(), "demo");
        assert_eq!(Environment::Live.to_string(), "live");
        assert_eq!(Environment::default(), Environment::Demo);
    }

    #[test]
    fn lookup_with_required_values_gives_demo_defaults() -> Result<(), ConfigError> {
        let configuration = ConfigurationOpenApi::from_lookup(lookup(&REQUIRED))?;
        let expected = filled(ConfigurationOpenApi::builder()).build()?;
        assert_eq!(configuration, expected);
        Ok(())
    }

    #[test]
    fn lookup_applies_optional_values() -> Result<(), ConfigError> {
        let configuration = ConfigurationOpenApi::from_lookup(lookup(&with(&[
            (ENV_REFRESH_TOKEN, "refresh"),
            (ENV_DEMO_HOST, "demo.example.test"),
            (ENV_TOKEN_URL, "https://auth.example.test/token"),
            (ENV_ENVIRONMENT, "demo"),
        ])))?;
        assert_eq!(configuration.environment(), Environment::Demo);
        assert_eq!(configuration.host(), "demo.example.test");
        assert_eq!(configuration.token_url(), "https://auth.example.test/token");
        assert_eq!(
            configuration.refresh_token().map(RefreshToken::expose),
            Some("refresh")
        );
        Ok(())
    }

    #[test]
    fn lookup_ignores_the_demo_host_when_live() -> Result<(), ConfigError> {
        let configuration = ConfigurationOpenApi::from_lookup(lookup(&with(&[
            (ENV_DEMO_HOST, "demo.example.test"),
            (ENV_ENVIRONMENT, "live"),
        ])))?;
        assert_eq!(configuration.environment(), Environment::Live);
        assert_eq!(configuration.host(), LIVE_HOST);
        Ok(())
    }

    #[test]
    fn lookup_treats_empty_values_as_absent() -> Result<(), ConfigError> {
        let configuration = ConfigurationOpenApi::from_lookup(lookup(&with(&[
            (ENV_REFRESH_TOKEN, ""),
            (ENV_DEMO_HOST, ""),
            (ENV_TOKEN_URL, ""),
            (ENV_ENVIRONMENT, ""),
        ])))?;
        let expected = filled(ConfigurationOpenApi::builder()).build()?;
        assert_eq!(configuration, expected);
        Ok(())
    }

    #[test]
    fn lookup_names_the_missing_variable() {
        for missing in [ENV_CLIENT_ID, ENV_CLIENT_SECRET, ENV_ACCESS_TOKEN] {
            let pairs: Vec<_> = REQUIRED
                .iter()
                .filter(|(key, _)| *key != missing)
                .copied()
                .collect();
            assert_eq!(
                ConfigurationOpenApi::from_lookup(lookup(&pairs)),
                Err(ConfigError::Missing(missing))
            );
        }
    }

    #[test]
    fn lookup_rejects_bad_values() {
        assert_eq!(
            ConfigurationOpenApi::from_lookup(lookup(&with(&[(ENV_ENVIRONMENT, "paper")]))),
            Err(ConfigError::InvalidEnvironment("paper".to_owned()))
        );
        assert_eq!(
            ConfigurationOpenApi::from_lookup(lookup(&with(&[(ENV_DEMO_HOST, "a b")]))),
            Err(ConfigError::InvalidHost("a b".to_owned()))
        );
        assert_eq!(
            ConfigurationOpenApi::from_lookup(lookup(&with(&[(ENV_TOKEN_URL, "ftp://x")]))),
            Err(ConfigError::InvalidTokenUrl("ftp://x".to_owned()))
        );
    }

    #[test]
    fn process_environment_is_read_through_the_same_lookup() {
        let direct = ConfigurationOpenApi::from_lookup(|key| std::env::var(key).ok());
        assert_eq!(ConfigurationOpenApi::from_env(), direct);
    }

    #[test]
    fn errors_describe_the_rejected_value() {
        assert_eq!(
            ConfigError::Missing("client_id").to_string(),
            "client_id is not set"
        );
        assert_eq!(
            ConfigError::InvalidEnvironment("paper".to_owned()).to_string(),
            "environment must be demo or live, got paper"
        );
        assert_eq!(
            ConfigError::InvalidHost("a b".to_owned()).to_string(),
            "host is not a bare host name: a b"
        );
        assert_eq!(
            ConfigError::InvalidPort.to_string(),
            "port must not be zero"
        );
        assert_eq!(
            ConfigError::InvalidTokenUrl("x".to_owned()).to_string(),
            "token URL must be an https URL: x"
        );
        assert_eq!(
            ConfigError::ZeroDuration("timeout").to_string(),
            "timeout must be longer than zero"
        );
    }
}
