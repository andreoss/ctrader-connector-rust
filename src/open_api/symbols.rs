//! Reference data of a trading account: its assets, its symbols and the details of a symbol.
//!
//! ```no_run
//! use ctrader_connector::common::config::ConfigurationOpenApi;
//! use ctrader_connector::open_api::client::OpenApiClient;
//! use ctrader_connector::open_api::symbols::SymbolsParams;
//!
//! # async fn run() -> Result<(), ctrader_connector::common::errors::ConnectorError> {
//! let client = OpenApiClient::connect(ConfigurationOpenApi::from_env()?).await?;
//! client.authorise_application().await?;
//! for account in client.accounts().await? {
//!     client.authorise_account(account.id).await?;
//!     let list = client.symbols(SymbolsParams::new(account.id)).await?;
//!     let ids: Vec<_> = list.symbols.iter().take(10).map(|symbol| symbol.id).collect();
//!     for symbol in client.symbol_details(account.id, &ids).await? {
//!         println!("{} has {} digits", symbol.id, symbol.digits);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::common::decimals::{PRICE_SCALE, Price, Volume};
use crate::common::errors::{ConnectorError, ValueError};
use crate::common::ids::{AccountId, AssetId, SymbolCategoryId, SymbolId};
use crate::common::time::Timestamp;
use crate::open_api::client::OpenApiClient;
use crate::open_api::messages::{
    ProtoOaArchivedSymbol, ProtoOaAsset, ProtoOaAssetListReq, ProtoOaLightSymbol, ProtoOaSymbol,
    ProtoOaSymbolByIdReq, ProtoOaSymbolsListReq, ProtoOaTradingMode,
};

const DIGITS: &str = "digits";

fn digits(raw: i32) -> Result<u32, ValueError> {
    u32::try_from(raw).map_err(|_| ValueError::OutOfRange { kind: DIGITS })
}

fn collect<W, T>(items: Vec<W>) -> Result<Vec<T>, ConnectorError>
where
    T: TryFrom<W, Error = ValueError>,
{
    items
        .into_iter()
        .map(|item| T::try_from(item).map_err(ConnectorError::from))
        .collect()
}

/// Asset an account can hold or a symbol is quoted in, such as a currency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Asset {
    /// Identifier of the asset.
    pub id: AssetId,
    /// Short name of the asset, such as `EUR`.
    pub name: String,
    /// Name shown to users, when the server reports one.
    pub display_name: Option<String>,
    /// Number of decimal places amounts of the asset are shown with.
    pub digits: Option<u32>,
}

impl TryFrom<ProtoOaAsset> for Asset {
    type Error = ValueError;

    fn try_from(asset: ProtoOaAsset) -> Result<Self, Self::Error> {
        Ok(Self {
            id: AssetId::new(asset.asset_id)?,
            name: asset.name,
            display_name: asset.display_name,
            digits: asset.digits.map(digits).transpose()?,
        })
    }
}

/// Symbol as the list of symbols carries it, without its trading details.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct LightSymbol {
    /// Identifier of the symbol.
    pub id: SymbolId,
    /// Name of the symbol, such as `EURUSD`.
    pub name: Option<String>,
    /// Tells whether the symbol can be traded and quoted.
    pub enabled: bool,
    /// Asset bought or sold.
    pub base_asset: Option<AssetId>,
    /// Asset the price is expressed in.
    pub quote_asset: Option<AssetId>,
    /// Category the symbol is listed under.
    pub category: Option<SymbolCategoryId>,
    /// Description of the symbol.
    pub description: Option<String>,
}

impl TryFrom<ProtoOaLightSymbol> for LightSymbol {
    type Error = ValueError;

    fn try_from(symbol: ProtoOaLightSymbol) -> Result<Self, Self::Error> {
        Ok(Self {
            id: SymbolId::new(symbol.symbol_id)?,
            name: symbol.symbol_name,
            enabled: symbol.enabled.unwrap_or(false),
            base_asset: symbol.base_asset_id.map(AssetId::new).transpose()?,
            quote_asset: symbol.quote_asset_id.map(AssetId::new).transpose()?,
            category: symbol
                .symbol_category_id
                .map(SymbolCategoryId::new)
                .transpose()?,
            description: symbol.description,
        })
    }
}

/// Symbol that is no longer offered.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ArchivedSymbol {
    /// Identifier of the symbol.
    pub id: SymbolId,
    /// Name of the symbol.
    pub name: String,
    /// Moment the symbol was last changed.
    pub last_update: Timestamp,
    /// Description of the symbol.
    pub description: Option<String>,
}

impl TryFrom<ProtoOaArchivedSymbol> for ArchivedSymbol {
    type Error = ValueError;

    fn try_from(symbol: ProtoOaArchivedSymbol) -> Result<Self, Self::Error> {
        Ok(Self {
            id: SymbolId::new(symbol.symbol_id)?,
            name: symbol.name,
            last_update: Timestamp::from_millis(symbol.utc_last_update_timestamp)?,
            description: symbol.description,
        })
    }
}

/// Symbols of an account.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct SymbolList {
    /// Symbols on offer.
    pub symbols: Vec<LightSymbol>,
    /// Symbols no longer offered; empty unless they were asked for.
    pub archived: Vec<ArchivedSymbol>,
}

/// Tells which orders the server accepts for a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TradingMode {
    /// Trading is enabled.
    Enabled,
    /// Trading is disabled and pending orders are not executed.
    DisabledWithoutPendingsExecution,
    /// Trading is disabled and pending orders are still executed.
    DisabledWithPendingsExecution,
    /// Positions can only be closed.
    CloseOnly,
}

impl From<ProtoOaTradingMode> for TradingMode {
    fn from(mode: ProtoOaTradingMode) -> Self {
        match mode {
            ProtoOaTradingMode::Enabled => Self::Enabled,
            ProtoOaTradingMode::DisabledWithoutPendingsExecution => {
                Self::DisabledWithoutPendingsExecution
            }
            ProtoOaTradingMode::DisabledWithPendingsExecution => {
                Self::DisabledWithPendingsExecution
            }
            ProtoOaTradingMode::CloseOnlyMode => Self::CloseOnly,
        }
    }
}

/// Trading details of a symbol.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Symbol {
    /// Identifier of the symbol.
    pub id: SymbolId,
    /// Number of decimal places of a price of the symbol.
    pub digits: u32,
    /// Decimal place of one pip; negative when a pip is larger than one unit.
    pub pip_position: i32,
    /// Smallest volume of an order.
    pub min_volume: Option<Volume>,
    /// Largest volume of an order.
    pub max_volume: Option<Volume>,
    /// Step the volume of an order moves in.
    pub step_volume: Option<Volume>,
    /// Volume of one lot.
    pub lot_size: Option<Volume>,
    /// Orders the server accepts for the symbol.
    pub trading_mode: TradingMode,
    /// Tells whether the symbol can be sold short.
    pub short_selling: bool,
    /// Units the volume is measured in, when the server reports them.
    pub measurement_units: Option<String>,
    /// Time zone of the trading schedule, when the server reports it.
    pub schedule_time_zone: Option<String>,
}

impl Symbol {
    /// Converts a wire price, in 1/100000 of a unit, to a price with the digits of the symbol.
    ///
    /// # Errors
    ///
    /// Returns [`ValueError::TooManyDigits`] when the symbol has more digits than a wire price
    /// carries.
    pub fn price(&self, raw: u64) -> Result<Price, ValueError> {
        Price::from_wire(raw, self.digits)
    }

    /// Tells whether prices of the symbol can be carried on the wire without loss.
    #[must_use]
    pub const fn has_wire_precision(&self) -> bool {
        self.digits <= PRICE_SCALE
    }
}

impl TryFrom<ProtoOaSymbol> for Symbol {
    type Error = ValueError;

    fn try_from(symbol: ProtoOaSymbol) -> Result<Self, Self::Error> {
        let volume = |raw: Option<i64>| raw.map(Volume::from_wire).transpose();
        Ok(Self {
            id: SymbolId::new(symbol.symbol_id)?,
            digits: digits(symbol.digits)?,
            pip_position: symbol.pip_position,
            min_volume: volume(symbol.min_volume)?,
            max_volume: volume(symbol.max_volume)?,
            step_volume: volume(symbol.step_volume)?,
            lot_size: volume(symbol.lot_size)?,
            trading_mode: symbol.trading_mode().into(),
            short_selling: symbol.enable_short_selling.unwrap_or(false),
            measurement_units: symbol.measurement_units,
            schedule_time_zone: symbol.schedule_time_zone,
        })
    }
}

/// Parameters of [`OpenApiClient::symbols`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct SymbolsParams {
    account: AccountId,
    include_archived: bool,
}

impl SymbolsParams {
    /// Asks for the symbols on offer to the given authorised account.
    pub const fn new(account: AccountId) -> Self {
        Self {
            account,
            include_archived: false,
        }
    }

    /// Chooses whether symbols no longer offered are listed as well; they are not by default.
    pub const fn include_archived(mut self, include: bool) -> Self {
        self.include_archived = include;
        self
    }

    /// Returns the account the symbols are listed for.
    #[must_use]
    pub const fn account(&self) -> AccountId {
        self.account
    }

    /// Tells whether symbols no longer offered are listed as well.
    #[must_use]
    pub const fn includes_archived(&self) -> bool {
        self.include_archived
    }
}

impl OpenApiClient {
    /// Lists the assets known to an authorised account.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the request,
    /// [`ConnectorError::Value`] when an asset carries a value out of range, and the errors of
    /// [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn assets(&self, account: AccountId) -> Result<Vec<Asset>, ConnectorError> {
        let request = ProtoOaAssetListReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
        };
        collect(self.connection().request(&request).await?.asset)
    }

    /// Lists the symbols of an authorised account, without their trading details.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the request,
    /// [`ConnectorError::Value`] when a symbol carries a value out of range, and the errors of
    /// [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn symbols(&self, params: SymbolsParams) -> Result<SymbolList, ConnectorError> {
        let request = ProtoOaSymbolsListReq {
            payload_type: None,
            ctid_trader_account_id: params.account.get(),
            include_archived_symbols: Some(params.include_archived),
        };
        let response = self.connection().request(&request).await?;
        Ok(SymbolList {
            symbols: collect(response.symbol)?,
            archived: collect(response.archived_symbol)?,
        })
    }

    /// Fetches the trading details of the given symbols of an authorised account.
    ///
    /// Symbols the server does not know are left out of the answer.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::Server`] when the server refuses the request,
    /// [`ConnectorError::Value`] when a symbol carries a value out of range, and the errors of
    /// [`Connection::request`](crate::open_api::connection::Connection::request) otherwise.
    pub async fn symbol_details(
        &self,
        account: AccountId,
        symbols: &[SymbolId],
    ) -> Result<Vec<Symbol>, ConnectorError> {
        let request = ProtoOaSymbolByIdReq {
            payload_type: None,
            ctid_trader_account_id: account.get(),
            symbol_id: symbols.iter().map(|symbol| symbol.get()).collect(),
        };
        collect(self.connection().request(&request).await?.symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArchivedSymbol, Asset, LightSymbol, Symbol, SymbolsParams, TradingMode};
    use crate::common::errors::ValueError;
    use crate::common::ids::{AccountId, AssetId, SymbolCategoryId};
    use crate::open_api::messages::{
        ProtoOaArchivedSymbol, ProtoOaAsset, ProtoOaLightSymbol, ProtoOaSymbol, ProtoOaTradingMode,
    };

    fn invalid(kind: &'static str, value: i64) -> ValueError {
        ValueError::InvalidId {
            kind,
            value: value.to_string(),
        }
    }

    fn wire_asset() -> ProtoOaAsset {
        ProtoOaAsset {
            asset_id: 4,
            name: "EUR".to_owned(),
            display_name: Some("Euro".to_owned()),
            digits: Some(2),
        }
    }

    #[test]
    fn asset_is_parsed_from_the_wire() {
        let asset = Asset::try_from(wire_asset()).expect("asset");
        assert_eq!(asset.id.get(), 4);
        assert_eq!(asset.name, "EUR");
        assert_eq!(asset.display_name.as_deref(), Some("Euro"));
        assert_eq!(asset.digits, Some(2));
        let bare = ProtoOaAsset {
            display_name: None,
            digits: None,
            ..wire_asset()
        };
        assert_eq!(Asset::try_from(bare).expect("asset").digits, None);
    }

    #[test]
    fn asset_values_out_of_range_are_rejected() {
        let no_id = ProtoOaAsset {
            asset_id: 0,
            ..wire_asset()
        };
        assert_eq!(Asset::try_from(no_id), Err(invalid("asset id", 0)));
        let negative = ProtoOaAsset {
            digits: Some(-1),
            ..wire_asset()
        };
        assert_eq!(
            Asset::try_from(negative),
            Err(ValueError::OutOfRange { kind: "digits" })
        );
    }

    fn wire_light() -> ProtoOaLightSymbol {
        ProtoOaLightSymbol {
            symbol_id: 1,
            symbol_name: Some("EURUSD".to_owned()),
            enabled: Some(true),
            base_asset_id: Some(4),
            quote_asset_id: Some(11),
            symbol_category_id: Some(2),
            description: Some("Euro vs US Dollar".to_owned()),
            sorting_number: Some(1.0),
        }
    }

    #[test]
    fn light_symbol_is_parsed_from_the_wire() {
        let symbol = LightSymbol::try_from(wire_light()).expect("symbol");
        assert_eq!(symbol.id.get(), 1);
        assert_eq!(symbol.name.as_deref(), Some("EURUSD"));
        assert!(symbol.enabled);
        assert_eq!(symbol.base_asset.map(AssetId::get), Some(4));
        assert_eq!(symbol.quote_asset.map(AssetId::get), Some(11));
        assert_eq!(symbol.category.map(SymbolCategoryId::get), Some(2));
        assert_eq!(symbol.description.as_deref(), Some("Euro vs US Dollar"));
        let bare = LightSymbol::try_from(ProtoOaLightSymbol {
            symbol_id: 9,
            ..ProtoOaLightSymbol::default()
        })
        .expect("symbol");
        assert!(!bare.enabled);
        assert_eq!(bare.base_asset, None);
        assert_eq!(bare.category, None);
    }

    #[test]
    fn light_symbol_ids_out_of_range_are_rejected() {
        let cases = [
            (
                ProtoOaLightSymbol {
                    symbol_id: -3,
                    ..wire_light()
                },
                invalid("symbol id", -3),
            ),
            (
                ProtoOaLightSymbol {
                    base_asset_id: Some(0),
                    ..wire_light()
                },
                invalid("asset id", 0),
            ),
            (
                ProtoOaLightSymbol {
                    quote_asset_id: Some(-1),
                    ..wire_light()
                },
                invalid("asset id", -1),
            ),
            (
                ProtoOaLightSymbol {
                    symbol_category_id: Some(0),
                    ..wire_light()
                },
                invalid("symbol category id", 0),
            ),
        ];
        for (wire, error) in cases {
            assert_eq!(LightSymbol::try_from(wire), Err(error));
        }
    }

    #[test]
    fn archived_symbol_is_parsed_from_the_wire() {
        let wire = ProtoOaArchivedSymbol {
            symbol_id: 5,
            name: "OLD".to_owned(),
            utc_last_update_timestamp: 1_700_000_000_000,
            description: None,
        };
        let symbol = ArchivedSymbol::try_from(wire.clone()).expect("symbol");
        assert_eq!(symbol.id.get(), 5);
        assert_eq!(symbol.name, "OLD");
        assert_eq!(symbol.last_update.as_millis(), 1_700_000_000_000);
        let early = ProtoOaArchivedSymbol {
            utc_last_update_timestamp: -1,
            ..wire.clone()
        };
        assert_eq!(
            ArchivedSymbol::try_from(early),
            Err(ValueError::OutOfRange { kind: "timestamp" })
        );
        let no_id = ProtoOaArchivedSymbol {
            symbol_id: 0,
            ..wire
        };
        assert_eq!(
            ArchivedSymbol::try_from(no_id),
            Err(invalid("symbol id", 0))
        );
    }

    fn wire_symbol() -> ProtoOaSymbol {
        ProtoOaSymbol {
            symbol_id: 1,
            digits: 3,
            pip_position: 2,
            enable_short_selling: Some(true),
            max_volume: Some(1_000_000_000),
            min_volume: Some(100_000),
            step_volume: Some(100_000),
            lot_size: Some(10_000_000),
            trading_mode: Some(ProtoOaTradingMode::CloseOnlyMode as i32),
            measurement_units: Some("EUR".to_owned()),
            schedule_time_zone: Some("UTC".to_owned()),
            ..ProtoOaSymbol::default()
        }
    }

    #[test]
    fn symbol_details_are_parsed_from_the_wire() {
        let symbol = Symbol::try_from(wire_symbol()).expect("symbol");
        assert_eq!(symbol.id.get(), 1);
        assert_eq!(symbol.digits, 3);
        assert_eq!(symbol.pip_position, 2);
        let text = |volume: Option<crate::common::decimals::Volume>| {
            volume.map(|volume| volume.to_string())
        };
        assert_eq!(text(symbol.min_volume).as_deref(), Some("1000.00"));
        assert_eq!(text(symbol.max_volume).as_deref(), Some("10000000.00"));
        assert_eq!(text(symbol.step_volume).as_deref(), Some("1000.00"));
        assert_eq!(text(symbol.lot_size).as_deref(), Some("100000.00"));
        assert_eq!(symbol.trading_mode, TradingMode::CloseOnly);
        assert!(symbol.short_selling);
        assert_eq!(symbol.measurement_units.as_deref(), Some("EUR"));
        assert_eq!(symbol.schedule_time_zone.as_deref(), Some("UTC"));
        assert!(symbol.has_wire_precision());
        assert_eq!(
            symbol.price(123_456).map(|price| price.to_string()),
            Ok("1.235".to_owned())
        );
        let bare = Symbol::try_from(ProtoOaSymbol {
            symbol_id: 2,
            digits: 6,
            ..ProtoOaSymbol::default()
        })
        .expect("symbol");
        assert_eq!(bare.min_volume, None);
        assert_eq!(bare.trading_mode, TradingMode::Enabled);
        assert!(!bare.short_selling);
        assert!(!bare.has_wire_precision());
        assert_eq!(bare.price(1), Err(ValueError::TooManyDigits { digits: 6 }));
    }

    #[test]
    fn symbol_details_out_of_range_are_rejected() {
        let no_id = ProtoOaSymbol {
            symbol_id: 0,
            ..wire_symbol()
        };
        assert_eq!(Symbol::try_from(no_id), Err(invalid("symbol id", 0)));
        let negative_digits = ProtoOaSymbol {
            digits: -1,
            ..wire_symbol()
        };
        assert_eq!(
            Symbol::try_from(negative_digits),
            Err(ValueError::OutOfRange { kind: "digits" })
        );
        for raw in [
            ProtoOaSymbol {
                min_volume: Some(-1),
                ..wire_symbol()
            },
            ProtoOaSymbol {
                max_volume: Some(-1),
                ..wire_symbol()
            },
            ProtoOaSymbol {
                step_volume: Some(-1),
                ..wire_symbol()
            },
            ProtoOaSymbol {
                lot_size: Some(-1),
                ..wire_symbol()
            },
        ] {
            assert!(matches!(
                Symbol::try_from(raw),
                Err(ValueError::Negative { kind: "volume", .. })
            ));
        }
    }

    #[test]
    fn every_trading_mode_maps_to_a_variant() {
        let modes = [
            (ProtoOaTradingMode::Enabled, TradingMode::Enabled),
            (
                ProtoOaTradingMode::DisabledWithoutPendingsExecution,
                TradingMode::DisabledWithoutPendingsExecution,
            ),
            (
                ProtoOaTradingMode::DisabledWithPendingsExecution,
                TradingMode::DisabledWithPendingsExecution,
            ),
            (ProtoOaTradingMode::CloseOnlyMode, TradingMode::CloseOnly),
        ];
        for (wire, mode) in modes {
            assert_eq!(TradingMode::from(wire), mode);
        }
    }

    #[test]
    fn symbols_parameters_leave_archived_symbols_out_by_default() {
        let account = AccountId::new(7).expect("account id");
        let params = SymbolsParams::new(account);
        assert_eq!(params.account(), account);
        assert!(!params.includes_archived());
        assert!(params.include_archived(true).includes_archived());
    }
}
