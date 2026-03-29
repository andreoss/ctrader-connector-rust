//! Details of a trading account: its balance, deposit currency, leverage and kind.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! let account = client.accounts().await?[0].id;
//! client.authorise_account(account).await?;
//! let trader = client.trader(account).await?;
//! println!("{} {}", trader.balance, trader.currency);
//! # Ok(())
//! # }
//! ```

use rust_decimal::Decimal;

use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, AssetId};
use crate::common::time::Timestamp;
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{
    ProtoOaAccessRights, ProtoOaAccountType, ProtoOaTrader, ProtoOaTraderReq,
};
use crate::open_api::symbols::Asset;
use crate::open_api::trading::{DEFAULT_MONEY_DIGITS, money};

const LEVERAGE_SCALE: u32 = 2;

const ACCOUNT_TYPE: &str = "account type";
const ACCESS_RIGHTS: &str = "access rights";
const DEPOSIT_ASSET: &str = "deposit asset";

/// How the positions of an account are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AccountType {
    /// Several positions of a symbol, in either direction, exist side by side.
    Hedged,
    /// One position per symbol, the net of all trades.
    Netted,
    /// Spread betting.
    SpreadBetting,
}

impl AccountType {
    fn from_wire(raw: Option<i32>) -> Result<Self, ValueError> {
        let Some(raw) = raw else {
            return Ok(Self::Hedged);
        };
        match ProtoOaAccountType::try_from(raw) {
            Ok(ProtoOaAccountType::Hedged) => Ok(Self::Hedged),
            Ok(ProtoOaAccountType::Netted) => Ok(Self::Netted),
            Ok(ProtoOaAccountType::SpreadBetting) => Ok(Self::SpreadBetting),
            Err(_) => Err(ValueError::OutOfRange { kind: ACCOUNT_TYPE }),
        }
    }
}

/// What the account is allowed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AccessRights {
    /// Trading without restriction.
    FullAccess,
    /// Positions can be closed and nothing opened.
    CloseOnly,
    /// No trading; the account can be read.
    NoTrading,
    /// The account cannot be used.
    NoLogin,
}

impl AccessRights {
    fn from_wire(raw: Option<i32>) -> Result<Self, ValueError> {
        let Some(raw) = raw else {
            return Ok(Self::FullAccess);
        };
        match ProtoOaAccessRights::try_from(raw) {
            Ok(ProtoOaAccessRights::FullAccess) => Ok(Self::FullAccess),
            Ok(ProtoOaAccessRights::CloseOnly) => Ok(Self::CloseOnly),
            Ok(ProtoOaAccessRights::NoTrading) => Ok(Self::NoTrading),
            Ok(ProtoOaAccessRights::NoLogin) => Ok(Self::NoLogin),
            Err(_) => Err(ValueError::OutOfRange {
                kind: ACCESS_RIGHTS,
            }),
        }
    }
}

/// Trading account as the server knows it.
///
/// Amounts of money are in the deposit currency, scaled by the money digits of the account.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Trader {
    /// Identifier of the account.
    pub account: AccountId,
    /// Balance of the account.
    pub balance: Decimal,
    /// Number of decimal places the amounts of the account are kept with.
    pub money_digits: u32,
    /// Asset the account is kept in.
    pub deposit_asset: AssetId,
    /// Name of the deposit asset, such as `USD`.
    pub currency: String,
    /// Leverage of the account, such as `100` for 1:100.
    pub leverage: Option<Decimal>,
    /// Largest leverage the account can be given.
    pub max_leverage: Option<u32>,
    /// How the positions of the account are kept.
    pub account_type: AccountType,
    /// What the account is allowed to do.
    pub access_rights: AccessRights,
    /// Tells whether no swap is charged on the account.
    pub swap_free: bool,
    /// Tells whether the loss of every position of the account is limited by a guaranteed stop
    /// loss.
    pub limited_risk: bool,
    /// Bonus given by the manager of the account.
    pub manager_bonus: Option<Decimal>,
    /// Bonus given by the introducing broker.
    pub ib_bonus: Option<Decimal>,
    /// Part of the bonus that cannot be withdrawn.
    pub non_withdrawable_bonus: Option<Decimal>,
    /// Name of the broker the account is held with.
    pub broker_name: Option<String>,
    /// Moment the account was registered.
    pub registered: Option<Timestamp>,
}

impl Trader {
    /// Builds the account from its wire form and the assets of the account, among which the
    /// deposit asset is looked up.
    fn parse(trader: ProtoOaTrader, assets: &[Asset]) -> Result<Self, ValueError> {
        let digits = trader.money_digits;
        let amount = |raw: Option<i64>| raw.map(|raw| money(raw, digits)).transpose();
        let deposit_asset = AssetId::new(trader.deposit_asset_id)?;
        let currency = assets
            .iter()
            .find(|asset| asset.id == deposit_asset)
            .map(|asset| asset.name.clone())
            .ok_or(ValueError::OutOfRange {
                kind: DEPOSIT_ASSET,
            })?;
        Ok(Self {
            account: AccountId::new(trader.ctid_trader_account_id)?,
            balance: money(trader.balance, digits)?,
            money_digits: digits.unwrap_or(DEFAULT_MONEY_DIGITS),
            deposit_asset,
            currency,
            leverage: trader
                .leverage_in_cents
                .map(|cents| Decimal::new(i64::from(cents), LEVERAGE_SCALE).normalize()),
            max_leverage: trader.max_leverage,
            account_type: AccountType::from_wire(trader.account_type)?,
            access_rights: AccessRights::from_wire(trader.access_rights)?,
            swap_free: trader.swap_free.unwrap_or(false),
            limited_risk: trader.is_limited_risk.unwrap_or(false),
            manager_bonus: amount(trader.manager_bonus)?,
            ib_bonus: amount(trader.ib_bonus)?,
            non_withdrawable_bonus: amount(trader.non_withdrawable_bonus)?,
            broker_name: trader.broker_name,
            registered: trader
                .registration_timestamp
                .map(Timestamp::from_millis)
                .transpose()?,
        })
    }
}

impl OpenApiClient {
    /// Reads the details of an authorised account: its balance, deposit currency, leverage and
    /// kind.
    ///
    /// Two requests are made: one for the account and one for the assets of the account, among
    /// which the name of the deposit asset is looked up.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses a request,
    /// [`ConnectorError::Value`] when the account carries a value out of range or a deposit
    /// asset the server does not list, and the errors of
    /// [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn trader(&self, account: AccountId) -> Result<Trader, ConnectorError> {
        let request = ProtoOaTraderReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
        };
        let trader = self.connection().request(&request).await?.trader;
        let assets = self.assets(account).await?;
        Ok(Trader::parse(trader, &assets)?)
    }
}
