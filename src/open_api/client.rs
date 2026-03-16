//! Client that authorises an application and its trading accounts over one connection.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! for account in client.accounts().await? {
//!     client.authorise_account(account.id).await?;
//! }
//! client.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::broadcast;

use crate::common::config::{AccessToken, ConfigurationOpenApi, RefreshToken};
use crate::common::errors::{ConnectorError, TokenError, ValueError};
use crate::common::ids::AccountId;
use crate::open_api::connection::{Connection, ConnectionOptions, Event};
use crate::open_api::messages::{
    ProtoOaAccountAuthReq, ProtoOaApplicationAuthReq, ProtoOaCtidTraderAccount,
    ProtoOaGetAccountListByAccessTokenReq, ProtoOaRefreshTokenReq, ProtoOaVersionReq,
};
use crate::open_api::token::{TokenPair, refresh_over_http};
use crate::open_api::transport::{Connect, Tls};

/// Trading account an access token grants access to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Account {
    /// Identifier of the account.
    pub id: AccountId,
    /// Tells whether the account belongs to the live environment.
    pub is_live: bool,
    /// Login number of the account at the broker, when the server reports it.
    pub login: Option<i64>,
}

impl TryFrom<ProtoOaCtidTraderAccount> for Account {
    type Error = ValueError;

    fn try_from(account: ProtoOaCtidTraderAccount) -> Result<Self, Self::Error> {
        let raw = account.ctid_trader_account_id;
        let id = i64::try_from(raw).map_err(|_| ValueError::InvalidId {
            kind: "account id",
            value: raw.to_string(),
        })?;
        Ok(Self {
            id: AccountId::new(id)?,
            is_live: account.is_live.unwrap_or(false),
            login: account.trader_login,
        })
    }
}

#[derive(Debug)]
struct Tokens {
    access: AccessToken,
    refresh: Option<RefreshToken>,
}

/// Cloneable client of the Open API.
#[derive(Debug, Clone)]
pub struct OpenApiClient {
    connection: Connection,
    configuration: Arc<ConfigurationOpenApi>,
    tokens: Arc<Mutex<Tokens>>,
}

impl OpenApiClient {
    /// Connects over TLS to the host named by the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the connection cannot be established.
    pub async fn connect(configuration: ConfigurationOpenApi) -> Result<Self, ConnectorError> {
        let options = ConnectionOptions::from(&configuration);
        Self::connect_with(configuration, Tls::new()?, options).await
    }

    /// Connects through the given connector with the given connection options.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Transport`] when the connection cannot be established.
    pub async fn connect_with<C: Connect>(
        configuration: ConfigurationOpenApi,
        connector: C,
        options: ConnectionOptions,
    ) -> Result<Self, ConnectorError> {
        let connection = Connection::open(
            connector,
            configuration.host(),
            configuration.port(),
            options,
        )
        .await?;
        Ok(Self {
            connection,
            tokens: Arc::new(Mutex::new(Tokens {
                access: configuration.access_token().clone(),
                refresh: configuration.refresh_token().cloned(),
            })),
            configuration: Arc::new(configuration),
        })
    }

    /// Returns the connection the client sends its requests over.
    #[must_use]
    pub const fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Returns the configuration the client was built from.
    #[must_use]
    pub fn configuration(&self) -> &ConfigurationOpenApi {
        &self.configuration
    }

    /// Subscribes to the events of the connection.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.connection.events()
    }

    /// Authorises the application with its client id and secret.
    ///
    /// This must succeed before any other request is sent.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the credentials are refused, and the errors of
    /// [`Connection::request`] otherwise.
    pub async fn authorise_application(&self) -> Result<(), ConnectorError> {
        let request = ProtoOaApplicationAuthReq {
            payload_type: None,
            client_id: self.configuration.client_id().as_str().to_owned(),
            client_secret: self.configuration.client_secret().expose().to_owned(),
        };
        self.connection.request(&request).await?;
        Ok(())
    }

    /// Lists the trading accounts the access token grants access to.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the token is refused,
    /// [`ConnectorError::Value`] when the server reports an account id out of range, and the
    /// errors of [`Connection::request`] otherwise.
    pub async fn accounts(&self) -> Result<Vec<Account>, ConnectorError> {
        let request = ProtoOaGetAccountListByAccessTokenReq {
            payload_type: None,
            access_token: self.access_token().expose().to_owned(),
        };
        let response = self.connection.request(&request).await?;
        response
            .ctid_trader_account
            .into_iter()
            .map(|account| Account::try_from(account).map_err(ConnectorError::from))
            .collect()
    }

    /// Authorises a trading account with the access token.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the token or the account is refused, and the
    /// errors of [`Connection::request`] otherwise.
    pub async fn authorise_account(&self, account: AccountId) -> Result<(), ConnectorError> {
        let request = ProtoOaAccountAuthReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            access_token: self.access_token().expose().to_owned(),
        };
        self.connection.request(&request).await?;
        Ok(())
    }

    /// Returns the access token in use, which differs from the configured one after a refresh.
    #[must_use]
    pub fn access_token(&self) -> AccessToken {
        self.tokens().access.clone()
    }

    /// Replaces the token pair the client uses for its next requests.
    pub fn set_tokens(&self, pair: &TokenPair) {
        let mut tokens = self.tokens();
        tokens.access = pair.access.clone();
        tokens.refresh = Some(pair.refresh.clone());
    }

    /// Exchanges the refresh token for a new pair over the connection.
    ///
    /// The old pair stops working once the server has answered and the client uses the new one
    /// from then on. Accounts authorised earlier stay authorised on this connection. Store the
    /// returned pair before doing anything else: it is the only copy.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError::NoRefreshToken`] when no refresh token is known,
    /// [`ConnectorError::Server`] when the server refuses the refresh,
    /// [`TokenError::Malformed`] when the response lacks a token, and the errors of
    /// [`Connection::request`] otherwise.
    pub async fn refresh_token(&self) -> Result<TokenPair, ConnectorError> {
        let request = ProtoOaRefreshTokenReq {
            payload_type: None,
            refresh_token: self.current_refresh_token()?.expose().to_owned(),
        };
        let pair = TokenPair::try_from(self.connection.request(&request).await?)?;
        self.set_tokens(&pair);
        Ok(pair)
    }

    /// Exchanges the refresh token for a new pair at the token endpoint, over TLS.
    ///
    /// Behaves like [`OpenApiClient::refresh_token`] but does not use the connection.
    ///
    /// # Errors
    ///
    /// Fails like [`OpenApiClient::refresh_token_over_http_with`].
    pub async fn refresh_token_over_http(&self) -> Result<TokenPair, ConnectorError> {
        self.refresh_token_over_http_with(&Tls::new()?).await
    }

    /// Exchanges the refresh token for a new pair at the token endpoint through `connector`.
    ///
    /// # Errors
    ///
    /// Returns [`TokenError::NoRefreshToken`] when no refresh token is known and the errors of
    /// [`refresh_over_http`] otherwise.
    pub async fn refresh_token_over_http_with<C: Connect>(
        &self,
        connector: &C,
    ) -> Result<TokenPair, ConnectorError> {
        let refresh = self.current_refresh_token()?;
        let pair = refresh_over_http(connector, &self.configuration, &refresh).await?;
        self.set_tokens(&pair);
        Ok(pair)
    }

    fn tokens(&self) -> std::sync::MutexGuard<'_, Tokens> {
        self.tokens.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn current_refresh_token(&self) -> Result<RefreshToken, TokenError> {
        self.tokens()
            .refresh
            .clone()
            .ok_or(TokenError::NoRefreshToken)
    }

    /// Asks the server for the version of the protocol it speaks.
    ///
    /// # Errors
    ///
    /// Fails like [`Connection::request`].
    pub async fn version(&self) -> Result<String, ConnectorError> {
        let response = self
            .connection
            .request(&ProtoOaVersionReq { payload_type: None })
            .await?;
        Ok(response.version)
    }

    /// Stops the connection task; see [`Connection::shutdown`].
    pub async fn shutdown(&self) {
        self.connection.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::Account;
    use crate::common::errors::ValueError;
    use crate::common::ids::AccountId;
    use crate::open_api::messages::ProtoOaCtidTraderAccount;

    fn wire(id: u64) -> ProtoOaCtidTraderAccount {
        ProtoOaCtidTraderAccount {
            ctid_trader_account_id: id,
            is_live: Some(true),
            trader_login: Some(9),
            last_closing_deal_timestamp: None,
            last_balance_update_timestamp: None,
        }
    }

    #[test]
    fn account_is_parsed_from_the_wire() {
        let account = Account::try_from(wire(42)).expect("account");
        assert_eq!(account.id, AccountId::new(42).expect("id"));
        assert!(account.is_live);
        assert_eq!(account.login, Some(9));
    }

    #[test]
    fn account_ids_out_of_range_are_rejected() {
        for id in [0, u64::MAX] {
            assert_eq!(
                Account::try_from(wire(id)),
                Err(ValueError::InvalidId {
                    kind: "account id",
                    value: id.to_string(),
                })
            );
        }
    }
}
