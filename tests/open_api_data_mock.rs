//! Behaviour of the reference and market data calls against the mock server.

mod support;

use std::time::Duration;

use ctrader_connector::common::errors::{ConnectorError, ErrorCode, ValueError};
use ctrader_connector::common::ids::{AccountId, SymbolId};
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::connection::{ConnectionOptions, Event};
use ctrader_connector::open_api::messages::{
    ProtoMessage, ProtoOaArchivedSymbol, ProtoOaAsset, ProtoOaAssetListReq, ProtoOaAssetListRes,
    ProtoOaDepthEvent, ProtoOaDepthQuote, ProtoOaLightSymbol, ProtoOaSpotEvent,
    ProtoOaSubscribeDepthQuotesReq, ProtoOaSubscribeDepthQuotesRes, ProtoOaSubscribeSpotsReq,
    ProtoOaSymbol, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq,
    ProtoOaSymbolsListRes, ProtoOaUnsubscribeDepthQuotesReq, ProtoOaUnsubscribeDepthQuotesRes,
    ProtoOaUnsubscribeSpotsReq,
};
use ctrader_connector::open_api::quotes::QuoteSide;
use ctrader_connector::open_api::symbols::{SymbolsParams, TradingMode};
use ctrader_connector::open_api::transport::Plain;
use prost::Message as _;
use support::{
    ACCESS_TOKEN, Answer, CLIENT_ID, CLIENT_SECRET, HOST, MockOptions, MockServer, REFRESH_TOKEN,
    Responder, SPOT_EVENT, SUBSCRIBE_SPOTS_REQ, UNSUBSCRIBE_SPOTS_REQ, answer, envelope, refuse,
};
use tokio::time::timeout;

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

fn silent(_: u32, _: &[u8]) -> Answer {
    None
}

fn spot(
    account: i64,
    symbol: i64,
    bid: Option<u64>,
    ask: Option<u64>,
    timestamp: Option<i64>,
) -> ProtoMessage {
    envelope(
        SPOT_EVENT,
        &ProtoOaSpotEvent {
            ctid_trader_account_id: account,
            symbol_id: symbol,
            bid,
            ask,
            timestamp,
            ..ProtoOaSpotEvent::default()
        },
        None,
    )
}

async fn soon<T>(what: &str, future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(5), future)
        .await
        .unwrap_or_else(|_| panic!("nothing arrived in time: {what}"))
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
    for _ in 0..500 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached: {what}");
}

fn last_request<M: prost::Message + Default>(server: &MockServer, payload_type: u32) -> M {
    let received = server.received();
    let message = received
        .iter()
        .rfind(|message| message.payload_type == payload_type)
        .unwrap_or_else(|| panic!("no request of type {payload_type}"));
    M::decode(message.payload.as_slice()).expect("request")
}

#[tokio::test]
async fn quotes_keep_the_last_known_side_and_end_with_the_subscription() {
    let Session { server, client } = session(silent).await;
    let mut quotes = client
        .subscribe_quotes(account(101), &[symbol(1), symbol(2)])
        .await
        .expect("subscription");
    assert_eq!(quotes.account(), account(101));
    assert_eq!(quotes.symbols().collect::<Vec<_>>(), [symbol(1), symbol(2)]);
    let sent: ProtoOaSubscribeSpotsReq = last_request(&server, SUBSCRIBE_SPOTS_REQ);
    assert_eq!(sent.symbol_id, vec![1, 2]);
    assert_eq!(sent.subscribe_to_spot_timestamp, Some(true));
    server.push(spot(102, 1, Some(5), None, None));
    server.push(spot(101, 3, Some(5), None, None));
    server.push(spot(101, 1, None, None, None));
    server.push(spot(101, 1, Some(108_512), None, Some(1_000)));
    server.push(spot(101, 2, Some(7), Some(9), None));
    server.push(spot(101, 1, None, Some(108_520), Some(1_001)));
    let first = soon("first quote", quotes.next()).await.expect("quote");
    assert_eq!(first.symbol, symbol(1));
    assert_eq!(
        first.bid.map(|price| price.to_string()).as_deref(),
        Some("1.08512")
    );
    assert_eq!(first.ask, None);
    assert_eq!(first.timestamp.map(Timestamp::as_millis), Some(1_000));
    let second = soon("second quote", quotes.next()).await.expect("quote");
    assert_eq!(second.symbol, symbol(2));
    assert_eq!(second.price(QuoteSide::Ask), "0.00009".parse().ok());
    let third = soon("third quote", quotes.next()).await.expect("quote");
    assert_eq!(third.bid, first.bid);
    assert_eq!(third.price(QuoteSide::Ask), "1.08520".parse().ok());
    assert_eq!(quotes.last(symbol(1)), Some(&third));
    assert_eq!(quotes.last(symbol(3)), None);
    quotes.unsubscribe().await.expect("end of subscription");
    let ended: ProtoOaUnsubscribeSpotsReq = last_request(&server, UNSUBSCRIBE_SPOTS_REQ);
    assert_eq!(ended.symbol_id, vec![1, 2]);
    client.shutdown().await;
}

#[tokio::test]
async fn quotes_resume_after_a_reconnection_and_end_when_the_connection_closes() {
    let Session { server, client } = session(silent).await;
    let mut quotes = client
        .subscribe_quotes(account(101), &[symbol(1)])
        .await
        .expect("subscription");
    let mut listener = client.quotes(account(101), &[symbol(1)]);
    server.drop_connections();
    eventually("the subscription is replayed", || {
        server.count(SUBSCRIBE_SPOTS_REQ) == 2
    })
    .await;
    server.push(spot(101, 1, Some(11), None, None));
    let quote = soon("quote after the reconnection", quotes.next())
        .await
        .expect("quote");
    assert_eq!(quote.symbol, symbol(1));
    assert_eq!(soon("same quote", listener.next()).await, Ok(quote));
    server.push(spot(101, 1, Some(11), None, Some(-1)));
    assert_eq!(
        soon("malformed quote", quotes.next()).await,
        Err(ConnectorError::Value(ValueError::OutOfRange {
            kind: "timestamp"
        }))
    );
    assert!(matches!(
        soon("malformed quote at the listener", listener.next()).await,
        Err(ConnectorError::Value(_))
    ));
    client.shutdown().await;
    assert_eq!(
        soon("end of the stream", quotes.next()).await,
        Err(ConnectorError::Closed)
    );
    assert_eq!(
        soon("end of the listener", listener.next()).await,
        Err(ConnectorError::Closed)
    );
}

const DEPTH_EVENT: u32 = 2155;
const SUBSCRIBE_DEPTH_REQ: u32 = 2156;
const SUBSCRIBE_DEPTH_RES: u32 = 2157;
const UNSUBSCRIBE_DEPTH_REQ: u32 = 2158;
const UNSUBSCRIBE_DEPTH_RES: u32 = 2159;

fn depth_subscriptions(payload_type: u32, payload: &[u8]) -> Answer {
    match payload_type {
        SUBSCRIBE_DEPTH_REQ => {
            let request = ProtoOaSubscribeDepthQuotesReq::decode(payload).ok()?;
            if request.symbol_id.contains(&666) {
                return refuse("SYMBOL_NOT_FOUND", "unknown symbol");
            }
            answer(
                SUBSCRIBE_DEPTH_RES,
                &ProtoOaSubscribeDepthQuotesRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                },
            )
        }
        UNSUBSCRIBE_DEPTH_REQ => {
            let request = ProtoOaUnsubscribeDepthQuotesReq::decode(payload).ok()?;
            if request.symbol_id.contains(&2) {
                return refuse("INVALID_REQUEST", "not subscribed");
            }
            answer(
                UNSUBSCRIBE_DEPTH_RES,
                &ProtoOaUnsubscribeDepthQuotesRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                },
            )
        }
        _ => None,
    }
}

fn level(id: u64, bid: Option<u64>, ask: Option<u64>) -> ProtoOaDepthQuote {
    ProtoOaDepthQuote {
        id,
        size: 100,
        bid,
        ask,
    }
}

fn depth(
    account: i64,
    symbol: u64,
    new: Vec<ProtoOaDepthQuote>,
    deleted: Vec<u64>,
) -> ProtoMessage {
    envelope(
        DEPTH_EVENT,
        &ProtoOaDepthEvent {
            payload_type: None,
            ctid_trader_account_id: account,
            symbol_id: symbol,
            new_quotes: new,
            deleted_quotes: deleted,
        },
        None,
    )
}

#[tokio::test]
async fn depth_updates_build_a_book_that_starts_again_after_a_reconnection() {
    let Session { server, client } = session(depth_subscriptions).await;
    assert_eq!(
        server_code(client.subscribe_depth(account(101), &[symbol(666)]).await),
        Some(ErrorCode::SymbolNotFound)
    );
    let mut stream = client
        .subscribe_depth(account(101), &[symbol(1)])
        .await
        .expect("subscription");
    assert_eq!(stream.account(), account(101));
    assert_eq!(stream.symbols().collect::<Vec<_>>(), [symbol(1)]);
    assert!(stream.book(symbol(1)).is_none());
    server.push(depth(102, 1, vec![level(9, Some(1), None)], Vec::new()));
    server.push(depth(101, 2, vec![level(9, Some(1), None)], Vec::new()));
    server.push(spot(101, 1, Some(5), None, None));
    server.push(depth(
        101,
        1,
        vec![level(1, None, Some(108_520)), level(2, None, Some(108_530))],
        Vec::new(),
    ));
    server.push(depth(
        101,
        1,
        vec![level(3, Some(108_510), None), level(4, Some(108_500), None)],
        vec![2],
    ));
    let first = soon("first update", stream.next()).await.expect("update");
    assert_eq!(first.symbol, symbol(1));
    assert_eq!(first.new_quotes.len(), 2);
    assert_eq!(first.new_quotes[0].side, QuoteSide::Ask);
    let second = soon("second update", stream.next()).await.expect("update");
    assert_eq!(second.deleted_quotes.len(), 1);
    let book = stream.book(symbol(1)).expect("book");
    assert_eq!(book.len(), 3);
    assert_eq!(book.best_bid(), "1.08510".parse().ok());
    assert_eq!(book.best_ask(), "1.08520".parse().ok());
    assert_eq!(book.bids().len(), 2);
    assert_eq!(book.asks().len(), 1);
    server.drop_connections();
    eventually("the subscription is replayed", || {
        server.count(SUBSCRIBE_DEPTH_REQ) == 3
    })
    .await;
    let replayed: ProtoOaSubscribeDepthQuotesReq = last_request(&server, SUBSCRIBE_DEPTH_REQ);
    assert_eq!(replayed.symbol_id, vec![1]);
    server.push(depth(
        101,
        1,
        vec![level(7, Some(108_490), None)],
        Vec::new(),
    ));
    soon("update after the reconnection", stream.next())
        .await
        .expect("update");
    let book = stream.book(symbol(1)).expect("book");
    assert_eq!(book.len(), 1);
    assert_eq!(book.best_ask(), None);
    server.push(depth(101, 1, vec![level(8, None, None)], Vec::new()));
    assert_eq!(
        soon("malformed update", stream.next()).await,
        Err(ConnectorError::Value(ValueError::OutOfRange {
            kind: "depth quote side"
        }))
    );
    stream.unsubscribe().await.expect("end of subscription");
    let ended: ProtoOaUnsubscribeDepthQuotesReq = last_request(&server, UNSUBSCRIBE_DEPTH_REQ);
    assert_eq!(ended.symbol_id, vec![1]);
    assert_eq!(
        server_code(client.unsubscribe_depth(account(101), &[symbol(2)]).await),
        Some(ErrorCode::InvalidRequest)
    );
    let mut events = client.events();
    server.drop_connections();
    soon("reconnection", async {
        while !matches!(events.recv().await, Ok(Event::Reconnected { .. })) {}
    })
    .await;
    assert_eq!(client.version().await.as_deref(), Ok("mock-1"));
    assert_eq!(server.count(SUBSCRIBE_DEPTH_REQ), 3);
    client.shutdown().await;
}
