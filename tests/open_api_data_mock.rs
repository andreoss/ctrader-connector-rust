//! Behaviour of the reference and market data calls against the mock server.

mod support;

use std::time::Duration;

use ctrader_connector::common::errors::{ConnectorError, ErrorCode, ValueError};
use ctrader_connector::common::ids::{AccountId, SymbolId};
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::connection::ConnectionOptions;
use ctrader_connector::open_api::messages::{
    ProtoOaArchivedSymbol, ProtoOaAsset, ProtoOaAssetListReq, ProtoOaAssetListRes,
    ProtoOaLightSymbol, ProtoOaSymbol, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes,
    ProtoOaSymbolsListReq, ProtoOaSymbolsListRes,
};
use ctrader_connector::open_api::symbols::{SymbolsParams, TradingMode};
use ctrader_connector::open_api::transport::Plain;
use prost::Message as _;
use support::{
    ACCESS_TOKEN, Answer, CLIENT_ID, CLIENT_SECRET, HOST, MockOptions, MockServer, REFRESH_TOKEN,
    Responder, answer, refuse,
};

const ASSET_LIST_REQ: u32 = 2112;
const ASSET_LIST_RES: u32 = 2113;
const SYMBOLS_LIST_REQ: u32 = 2114;
const SYMBOLS_LIST_RES: u32 = 2115;
const SYMBOL_BY_ID_REQ: u32 = 2116;
const SYMBOL_BY_ID_RES: u32 = 2117;

fn account(id: i64) -> AccountId {
    AccountId::new(id).expect("account id")
}

fn symbol(id: i64) -> SymbolId {
    SymbolId::new(id).expect("symbol id")
}

async fn session(responder: impl Fn(u32, &[u8]) -> Answer + Send + Sync + 'static) -> Session {
    let server = MockServer::start(MockOptions {
        responder: Some(Responder::new(responder)),
        ..MockOptions::default()
    })
    .await;
    let configuration = ctrader_connector::common::config::ConfigurationOpenApi::demo()
        .host(HOST)
        .port(server.port())
        .client_id(CLIENT_ID)
        .client_secret(CLIENT_SECRET)
        .access_token(ACCESS_TOKEN)
        .refresh_token(REFRESH_TOKEN)
        .timeout(Duration::from_secs(5))
        .reconnect_delay(Duration::from_millis(40))
        .build()
        .expect("configuration");
    let options = ConnectionOptions::from(&configuration);
    let client = OpenApiClient::connect_with(configuration, Plain, options)
        .await
        .expect("client");
    client.authorise_application().await.expect("application");
    client
        .authorise_account(account(101))
        .await
        .expect("account");
    Session { server, client }
}

struct Session {
    server: MockServer,
    client: OpenApiClient,
}

fn server_code<T>(outcome: Result<T, ConnectorError>) -> Option<ErrorCode> {
    match outcome {
        Err(ConnectorError::Server(error)) => Some(error.code),
        _ => None,
    }
}

fn reference_data(payload_type: u32, payload: &[u8]) -> Answer {
    match payload_type {
        ASSET_LIST_REQ => {
            let request = ProtoOaAssetListReq::decode(payload).ok()?;
            answer(
                ASSET_LIST_RES,
                &ProtoOaAssetListRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    asset: vec![
                        ProtoOaAsset {
                            asset_id: 4,
                            name: "EUR".to_owned(),
                            display_name: Some("Euro".to_owned()),
                            digits: Some(2),
                        },
                        ProtoOaAsset {
                            asset_id: 11,
                            name: "USD".to_owned(),
                            display_name: None,
                            digits: None,
                        },
                    ],
                },
            )
        }
        SYMBOLS_LIST_REQ => {
            let request = ProtoOaSymbolsListReq::decode(payload).ok()?;
            let archived = if request.include_archived_symbols == Some(true) {
                vec![ProtoOaArchivedSymbol {
                    symbol_id: 9,
                    name: "OLD".to_owned(),
                    utc_last_update_timestamp: 1_700_000_000_000,
                    description: None,
                }]
            } else {
                Vec::new()
            };
            answer(
                SYMBOLS_LIST_RES,
                &ProtoOaSymbolsListRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    symbol: vec![ProtoOaLightSymbol {
                        symbol_id: 1,
                        symbol_name: Some("EURUSD".to_owned()),
                        enabled: Some(true),
                        base_asset_id: Some(4),
                        quote_asset_id: Some(11),
                        symbol_category_id: Some(2),
                        description: None,
                        sorting_number: None,
                    }],
                    archived_symbol: archived,
                },
            )
        }
        SYMBOL_BY_ID_REQ => {
            let request = ProtoOaSymbolByIdReq::decode(payload).ok()?;
            if request.symbol_id.contains(&666) {
                return refuse("SYMBOL_NOT_FOUND", "unknown symbol");
            }
            answer(
                SYMBOL_BY_ID_RES,
                &ProtoOaSymbolByIdRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    symbol: request
                        .symbol_id
                        .iter()
                        .map(|id| ProtoOaSymbol {
                            symbol_id: *id,
                            digits: if *id == 13 { -1 } else { 5 },
                            pip_position: 4,
                            min_volume: Some(100_000),
                            max_volume: Some(1_000_000_000),
                            step_volume: Some(100_000),
                            lot_size: Some(10_000_000),
                            ..ProtoOaSymbol::default()
                        })
                        .collect(),
                    archived_symbol: Vec::new(),
                },
            )
        }
        _ => None,
    }
}

#[tokio::test]
async fn assets_symbols_and_details_arrive_as_typed_values() {
    let Session { server, client } = session(reference_data).await;
    let assets = client.assets(account(101)).await.expect("assets");
    assert_eq!(
        assets
            .iter()
            .map(|asset| (asset.id.get(), asset.name.as_str(), asset.digits))
            .collect::<Vec<_>>(),
        vec![(4, "EUR", Some(2)), (11, "USD", None)]
    );
    let list = client
        .symbols(SymbolsParams::new(account(101)))
        .await
        .expect("symbols");
    assert_eq!(list.symbols.len(), 1);
    assert_eq!(list.symbols[0].id, symbol(1));
    assert_eq!(list.symbols[0].name.as_deref(), Some("EURUSD"));
    assert!(list.archived.is_empty());
    let with_archived = client
        .symbols(SymbolsParams::new(account(101)).include_archived(true))
        .await
        .expect("symbols");
    assert_eq!(with_archived.archived.len(), 1);
    assert_eq!(with_archived.archived[0].id, symbol(9));
    let details = client
        .symbol_details(account(101), &[symbol(1), symbol(2)])
        .await
        .expect("details");
    assert_eq!(details.len(), 2);
    assert_eq!(details[1].id, symbol(2));
    assert_eq!(details[0].digits, 5);
    assert_eq!(details[0].pip_position, 4);
    assert_eq!(details[0].trading_mode, TradingMode::Enabled);
    assert_eq!(
        details[0].min_volume.map(|volume| volume.to_string()),
        Some("1000.00".to_owned())
    );
    assert_eq!(
        details[0].price(108_512).map(|price| price.to_string()),
        Ok("1.08512".to_owned())
    );
    let sent = ProtoOaSymbolByIdReq::decode(
        server
            .received()
            .iter()
            .rfind(|message| message.payload_type == SYMBOL_BY_ID_REQ)
            .expect("request")
            .payload
            .as_slice(),
    )
    .expect("details request");
    assert_eq!(sent.ctid_trader_account_id, 101);
    assert_eq!(sent.symbol_id, vec![1, 2]);
    client.shutdown().await;
}

#[tokio::test]
async fn refused_and_malformed_reference_data_yield_typed_errors() {
    let Session {
        server: _server,
        client,
    } = session(reference_data).await;
    assert_eq!(
        server_code(client.symbol_details(account(101), &[symbol(666)]).await),
        Some(ErrorCode::SymbolNotFound)
    );
    assert_eq!(
        client
            .symbol_details(account(101), &[symbol(13)])
            .await
            .err(),
        Some(ConnectorError::Value(ValueError::OutOfRange {
            kind: "digits"
        }))
    );
    client.shutdown().await;
}
