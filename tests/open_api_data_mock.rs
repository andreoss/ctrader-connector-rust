//! Behaviour of the reference and market data calls against the mock server.

mod support;

use std::time::Duration;

use ctrader_connector::common::errors::{ConnectorError, ErrorCode, ValueError};
use ctrader_connector::common::ids::{AccountId, SymbolId};
use ctrader_connector::common::time::{TimeRange, Timestamp};
use ctrader_connector::open_api::account::{AccessRights, AccountType};
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::connection::{ConnectionOptions, Event};
use ctrader_connector::open_api::depth::DepthBook;
use ctrader_connector::open_api::messages::{
    ProtoMessage, ProtoOaArchivedSymbol, ProtoOaAsset, ProtoOaAssetListReq, ProtoOaAssetListRes,
    ProtoOaDepthEvent, ProtoOaDepthQuote, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes,
    ProtoOaGetTrendbarsReq, ProtoOaGetTrendbarsRes, ProtoOaLightSymbol, ProtoOaSpotEvent,
    ProtoOaSubscribeDepthQuotesReq, ProtoOaSubscribeDepthQuotesRes,
    ProtoOaSubscribeLiveTrendbarReq, ProtoOaSubscribeLiveTrendbarRes, ProtoOaSubscribeSpotsReq,
    ProtoOaSymbol, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq,
    ProtoOaSymbolsListRes, ProtoOaTickData, ProtoOaTrader, ProtoOaTraderReq, ProtoOaTraderRes,
    ProtoOaTrendbar, ProtoOaUnsubscribeDepthQuotesReq, ProtoOaUnsubscribeDepthQuotesRes,
    ProtoOaUnsubscribeLiveTrendbarReq, ProtoOaUnsubscribeLiveTrendbarRes,
    ProtoOaUnsubscribeSpotsReq,
};
use ctrader_connector::open_api::quotes::QuoteSide;
use ctrader_connector::open_api::symbols::{SymbolsParams, TradingMode};
use ctrader_connector::open_api::ticks::{Tick, TickDataParams};
use ctrader_connector::open_api::transport::Plain;
use ctrader_connector::open_api::trendbars::{Period, TrendbarsParams};
use prost::Message as _;
use support::{
    ACCESS_TOKEN, ACCOUNT_AUTH_REQ, APPLICATION_AUTH_REQ, Answer, CLIENT_ID, CLIENT_SECRET, HOST,
    MockOptions, MockServer, REFRESH_TOKEN, Responder, SPOT_EVENT, SUBSCRIBE_SPOTS_REQ,
    UNSUBSCRIBE_SPOTS_REQ, answer, envelope, refuse,
};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;

const ASSET_LIST_REQ: u32 = 2112;
const TRADER_REQ: u32 = 2121;
const TRADER_RES: u32 = 2122;
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
    session_with(responder, |options| options).await
}

async fn session_with(
    responder: impl Fn(u32, &[u8]) -> Answer + Send + Sync + 'static,
    adjust: impl FnOnce(ConnectionOptions) -> ConnectionOptions,
) -> Session {
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
    let options = adjust(ConnectionOptions::from(&configuration).historical_limit(50));
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
    server.push(spot(101, 1, Some(9), Some(12), None));
    let known = soon("quote before the loss", quotes.next())
        .await
        .expect("quote");
    assert_eq!(known.ask, "0.00012".parse().ok());
    assert_eq!(soon("same quote", listener.next()).await, Ok(known));
    server.precede(
        SUBSCRIBE_SPOTS_REQ,
        vec![spot(101, 1, Some(10), None, None)],
    );
    let mut events = client.events();
    server.drop_connections();
    let mut order = Vec::new();
    while order.last() != Some(&"reconnected") {
        order.push(next_marker(&mut events).await);
    }
    assert_eq!(order, ["disconnected", "spot", "reconnected"]);
    let replayed = soon("quote of the replay", quotes.next())
        .await
        .expect("quote");
    assert_eq!(replayed.bid, "0.00010".parse().ok());
    assert_eq!(replayed.ask, None);
    assert_eq!(soon("same quote", listener.next()).await, Ok(replayed));
    eventually("the subscription is replayed", || {
        server.count(SUBSCRIBE_SPOTS_REQ) == 2
    })
    .await;
    server.push(spot(101, 1, Some(11), None, None));
    let quote = soon("quote after the reconnection", quotes.next())
        .await
        .expect("quote");
    assert_eq!(quote.symbol, symbol(1));
    assert_eq!(quote.ask, None);
    assert_eq!(quotes.last(symbol(1)), Some(&quote));
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

async fn next_marker(events: &mut tokio::sync::broadcast::Receiver<Event>) -> &'static str {
    soon("event of the connection", async {
        loop {
            match events.recv().await.expect("event") {
                Event::Disconnected(_) => return "disconnected",
                Event::Reconnected { .. } => return "reconnected",
                Event::Message(message) if message.payload_type == DEPTH_EVENT => return "depth",
                Event::Message(message) if message.payload_type == SPOT_EVENT => return "spot",
                _ => {}
            }
        }
    })
    .await
}

#[tokio::test]
async fn depth_sent_during_the_replay_survives_the_reconnection() {
    let Session { server, client } = session(depth_subscriptions).await;
    let mut stream = client
        .subscribe_depth(account(101), &[symbol(1)])
        .await
        .expect("subscription");
    server.push(depth(
        101,
        1,
        vec![level(1, None, Some(108_520)), level(2, Some(108_500), None)],
        Vec::new(),
    ));
    soon("first update", stream.next()).await.expect("update");
    assert_eq!(stream.book(symbol(1)).map(DepthBook::len), Some(2));
    server.precede(
        SUBSCRIBE_DEPTH_REQ,
        vec![depth(
            101,
            1,
            vec![level(7, Some(108_490), None), level(8, None, Some(108_510))],
            Vec::new(),
        )],
    );
    let mut events = client.events();
    server.drop_connections();
    let mut order = Vec::new();
    while order.last() != Some(&"reconnected") {
        order.push(next_marker(&mut events).await);
    }
    assert_eq!(order, ["disconnected", "depth", "reconnected"]);
    let snapshot = soon("snapshot of the replay", stream.next())
        .await
        .expect("update");
    assert_eq!(snapshot.new_quotes.len(), 2);
    let book = stream.book(symbol(1)).expect("book");
    assert_eq!(book.len(), 2);
    assert_eq!(book.best_bid(), "1.08490".parse().ok());
    assert_eq!(book.best_ask(), "1.08510".parse().ok());
    server.push(depth(101, 1, vec![level(9, Some(108_495), None)], vec![7]));
    let change = soon("change after the reconnection", stream.next())
        .await
        .expect("update");
    assert_eq!(change.deleted_quotes.len(), 1);
    let book = stream.book(symbol(1)).expect("book");
    assert_eq!(book.len(), 2);
    assert_eq!(book.best_bid(), "1.08495".parse().ok());
    assert_eq!(book.best_ask(), "1.08510".parse().ok());
    client.shutdown().await;
}

const SUBSCRIBE_LIVE_TRENDBAR_REQ: u32 = 2135;
const UNSUBSCRIBE_LIVE_TRENDBAR_REQ: u32 = 2136;
const GET_TRENDBARS_REQ: u32 = 2137;
const GET_TRENDBARS_RES: u32 = 2138;
const SUBSCRIBE_LIVE_TRENDBAR_RES: u32 = 2165;
const UNSUBSCRIBE_LIVE_TRENDBAR_RES: u32 = 2166;
const MINUTE: i64 = 60_000;
const BARS_PER_RESPONSE: usize = 4;
const OLD_BARS: std::ops::Range<u32> = 1_000..1_010;
const NEW_BARS: std::ops::Range<u32> = 200_000..200_006;

fn history_bar(minute: u32) -> ProtoOaTrendbar {
    ProtoOaTrendbar {
        volume: i64::from(minute),
        period: None,
        low: Some(100_000 + i64::from(minute)),
        delta_open: Some(5),
        delta_close: Some(7),
        delta_high: Some(10),
        utc_timestamp_in_minutes: Some(minute),
    }
}

fn trendbar_data(payload_type: u32, payload: &[u8]) -> Answer {
    match payload_type {
        GET_TRENDBARS_REQ => {
            let request = ProtoOaGetTrendbarsReq::decode(payload).ok()?;
            if request.symbol_id == 666 {
                return refuse("SYMBOL_NOT_FOUND", "unknown symbol");
            }
            let mut bars: Vec<ProtoOaTrendbar> = OLD_BARS
                .chain(NEW_BARS)
                .filter(|minute| {
                    let opened = i64::from(*minute) * MINUTE;
                    opened + MINUTE > request.from_timestamp && opened <= request.to_timestamp
                })
                .map(history_bar)
                .collect();
            let surplus = bars.len().saturating_sub(BARS_PER_RESPONSE);
            bars.drain(..surplus);
            if request.symbol_id == 13 {
                bars = vec![ProtoOaTrendbar {
                    low: None,
                    ..history_bar(5)
                }];
            }
            answer(
                GET_TRENDBARS_RES,
                &ProtoOaGetTrendbarsRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    period: request.period,
                    timestamp: request.to_timestamp,
                    trendbar: bars,
                    symbol_id: Some(request.symbol_id),
                },
            )
        }
        SUBSCRIBE_LIVE_TRENDBAR_REQ => {
            let request = ProtoOaSubscribeLiveTrendbarReq::decode(payload).ok()?;
            if request.symbol_id == 666 {
                return refuse("INVALID_REQUEST", "no such symbol");
            }
            answer(
                SUBSCRIBE_LIVE_TRENDBAR_RES,
                &ProtoOaSubscribeLiveTrendbarRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                },
            )
        }
        UNSUBSCRIBE_LIVE_TRENDBAR_REQ => {
            let request = ProtoOaUnsubscribeLiveTrendbarReq::decode(payload).ok()?;
            answer(
                UNSUBSCRIBE_LIVE_TRENDBAR_RES,
                &ProtoOaUnsubscribeLiveTrendbarRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                },
            )
        }
        _ => None,
    }
}

fn minutes(range: std::ops::RangeInclusive<i64>) -> TimeRange {
    let at = |minute: i64| Timestamp::from_millis(minute * MINUTE).expect("timestamp");
    TimeRange::new(at(*range.start()), at(*range.end())).expect("range")
}

#[tokio::test]
async fn long_trendbar_ranges_are_fetched_in_parts_and_returned_in_order() {
    let Session { server, client } = session(trendbar_data).await;
    let whole = TrendbarsParams::new(account(101), symbol(1), Period::M1, minutes(0..=200_010));
    let bars = client.trendbars(&whole).await.expect("bars");
    let opened: Vec<i64> = bars.iter().map(|bar| bar.timestamp.as_millis()).collect();
    let expected: Vec<i64> = OLD_BARS
        .chain(NEW_BARS)
        .map(|minute| i64::from(minute) * MINUTE)
        .collect();
    assert_eq!(opened, expected);
    assert!(
        bars.iter()
            .all(|bar| bar.low <= bar.open && bar.open <= bar.close && bar.close <= bar.high)
    );
    assert_eq!(bars[0].low.to_string(), "1.01000");
    assert_eq!(bars[0].volume, 1_000);
    assert_eq!(bars[0].period, Period::M1);
    let requests: Vec<ProtoOaGetTrendbarsReq> = server
        .received()
        .iter()
        .filter(|message| message.payload_type == GET_TRENDBARS_REQ)
        .map(|message| ProtoOaGetTrendbarsReq::decode(message.payload.as_slice()).expect("request"))
        .collect();
    assert_eq!(requests.len(), 9);
    assert_eq!(requests[0].to_timestamp, 200_010 * MINUTE);
    assert_eq!(requests[0].from_timestamp, 200_010 * MINUTE - 3_024_000_000);
    assert_eq!(requests[1].to_timestamp, 200_002 * MINUTE - 1);
    assert_eq!(requests[0].period, 1);
    assert_eq!(requests[0].count, None);
    assert!(requests.iter().all(|request| {
        request.from_timestamp >= 0
            && request.from_timestamp <= request.to_timestamp
            && request.to_timestamp - request.from_timestamp <= 3_024_000_000
    }));
    assert!(
        requests
            .windows(2)
            .all(|pair| pair[1].to_timestamp < pair[0].to_timestamp)
    );
    let latest = client.trendbars(&whole.count(5)).await.expect("bars");
    assert_eq!(
        latest
            .iter()
            .map(|bar| bar.timestamp.as_millis() / MINUTE)
            .collect::<Vec<_>>(),
        (200_001..200_006).collect::<Vec<_>>()
    );
    assert_eq!(server.count(GET_TRENDBARS_REQ), 11);
    let inside = TrendbarsParams::new(account(101), symbol(1), Period::M1, minutes(1_003..=1_004));
    assert_eq!(client.trendbars(&inside).await.expect("bars").len(), 2);
    assert_eq!(server.count(GET_TRENDBARS_REQ), 12);
    let nothing = TrendbarsParams::new(account(101), symbol(1), Period::M1, minutes(0..=0));
    assert!(
        client
            .trendbars(&nothing.count(0))
            .await
            .expect("bars")
            .is_empty()
    );
    assert_eq!(server.count(GET_TRENDBARS_REQ), 12);
    assert!(client.trendbars(&nothing).await.expect("bars").is_empty());
    assert_eq!(server.count(GET_TRENDBARS_REQ), 13);
    client.shutdown().await;
}

#[tokio::test]
async fn refused_and_malformed_trendbars_yield_typed_errors() {
    let Session {
        server: _server,
        client,
    } = session(trendbar_data).await;
    let params = |id| TrendbarsParams::new(account(101), symbol(id), Period::H1, minutes(0..=10));
    assert_eq!(
        server_code(client.trendbars(&params(666)).await),
        Some(ErrorCode::SymbolNotFound)
    );
    assert_eq!(
        client.trendbars(&params(13)).await,
        Err(ConnectorError::Value(ValueError::Missing {
            kind: "trendbar low"
        }))
    );
    assert_eq!(
        server_code(
            client
                .subscribe_live_trendbars(account(101), symbol(666), Period::M1)
                .await
        ),
        Some(ErrorCode::InvalidRequest)
    );
    client.shutdown().await;
}

fn live_bar(period: i32, low: i64) -> ProtoOaTrendbar {
    ProtoOaTrendbar {
        volume: 3,
        period: Some(period),
        low: Some(low),
        delta_open: Some(1),
        delta_close: None,
        delta_high: Some(20),
        utc_timestamp_in_minutes: Some(10),
    }
}

fn spot_with_bars(symbol: i64, bid: Option<u64>, bars: Vec<ProtoOaTrendbar>) -> ProtoMessage {
    envelope(
        SPOT_EVENT,
        &ProtoOaSpotEvent {
            ctid_trader_account_id: 101,
            symbol_id: symbol,
            bid,
            ask: bid.is_none().then_some(1),
            trendbar: bars,
            ..ProtoOaSpotEvent::default()
        },
        None,
    )
}

async fn reconnected(server: &MockServer, client: &OpenApiClient) {
    let mut events = client.events();
    server.drop_connections();
    soon("reconnection", async {
        while !matches!(events.recv().await, Ok(Event::Reconnected { .. })) {}
    })
    .await;
}

#[tokio::test]
async fn live_trendbars_follow_the_spot_subscription_and_close_at_the_last_bid() {
    let Session { server, client } = session(trendbar_data).await;
    let mut one = client
        .subscribe_live_trendbars(account(101), symbol(1), Period::M1)
        .await
        .expect("subscription");
    let mut five = client
        .subscribe_live_trendbars(account(101), symbol(1), Period::M5)
        .await
        .expect("subscription");
    assert_eq!(
        (one.account(), one.symbol(), one.period()),
        (account(101), symbol(1), Period::M1)
    );
    assert_eq!(
        server.types_on(1)[2..],
        [
            SUBSCRIBE_SPOTS_REQ,
            SUBSCRIBE_LIVE_TRENDBAR_REQ,
            SUBSCRIBE_LIVE_TRENDBAR_REQ
        ]
    );
    server.push(spot_with_bars(1, None, vec![live_bar(1, 100_000)]));
    server.push(spot_with_bars(2, Some(7), vec![live_bar(1, 1)]));
    server.push(spot(102, 1, Some(7), None, None));
    server.push(spot_with_bars(1, Some(100_008), vec![live_bar(1, 100_001)]));
    server.push(spot_with_bars(1, Some(100_009), Vec::new()));
    server.push(spot_with_bars(
        1,
        None,
        vec![live_bar(5, 100_002), live_bar(1, 100_003)],
    ));
    let first = soon("first live bar", one.next()).await.expect("bar");
    assert_eq!(first.period, Period::M1);
    assert_eq!(first.timestamp.as_millis(), 10 * MINUTE);
    assert_eq!(
        [first.open, first.high, first.low, first.close].map(|price| price.to_string()),
        ["1.00002", "1.00021", "1.00001", "1.00008"]
    );
    let second = soon("second live bar", one.next()).await.expect("bar");
    assert_eq!(second.low.to_string(), "1.00003");
    assert_eq!(second.close.to_string(), "1.00009");
    let other = soon("bar of the other period", five.next())
        .await
        .expect("bar");
    assert_eq!(other.period, Period::M5);
    assert_eq!(other.low.to_string(), "1.00002");
    assert_eq!(other.close.to_string(), "1.00009");
    reconnected(&server, &client).await;
    server.push(spot_with_bars(1, None, vec![live_bar(1, 100_004)]));
    server.push(spot_with_bars(1, Some(100_020), vec![live_bar(1, 100_005)]));
    let resumed = soon("live bar after the reconnection", one.next())
        .await
        .expect("bar");
    assert_eq!(resumed.low.to_string(), "1.00005");
    assert_eq!(resumed.close.to_string(), "1.00020");
    assert_eq!(
        server.types_on(2),
        [
            APPLICATION_AUTH_REQ,
            ACCOUNT_AUTH_REQ,
            SUBSCRIBE_SPOTS_REQ,
            SUBSCRIBE_LIVE_TRENDBAR_REQ,
            SUBSCRIBE_LIVE_TRENDBAR_REQ
        ]
    );
    one.unsubscribe().await.expect("end of subscription");
    let ended: ProtoOaUnsubscribeLiveTrendbarReq =
        last_request(&server, UNSUBSCRIBE_LIVE_TRENDBAR_REQ);
    assert_eq!((ended.symbol_id, ended.period), (1, 1));
    reconnected(&server, &client).await;
    let replayed: ProtoOaSubscribeLiveTrendbarReq =
        last_request(&server, SUBSCRIBE_LIVE_TRENDBAR_REQ);
    assert_eq!(replayed.period, 5);
    assert_eq!(server.types_on(3).len(), 4);
    client
        .unsubscribe_spots(account(101), &[symbol(1)])
        .await
        .expect("end of spot subscription");
    reconnected(&server, &client).await;
    assert_eq!(server.types_on(4), [APPLICATION_AUTH_REQ, ACCOUNT_AUTH_REQ]);
    client
        .subscribe_live_trendbars(account(101), symbol(1), Period::M5)
        .await
        .expect("subscription");
    assert_eq!(
        server.types_on(4)[2..],
        [SUBSCRIBE_SPOTS_REQ, SUBSCRIBE_LIVE_TRENDBAR_REQ]
    );
    client.shutdown().await;
}

const EVENT_CAPACITY: usize = 8;
const FLOOD: u64 = 24;

async fn flooded(server: &MockServer, client: &OpenApiClient, tail: [ProtoMessage; 2]) {
    let mut watch = client.events();
    for id in 0..FLOOD - 2 {
        server.push(depth(
            101,
            1,
            vec![level(100 + id, Some(108_400), None)],
            Vec::new(),
        ));
    }
    for message in tail {
        server.push(message);
    }
    let mut seen = 0_u64;
    soon("the flood reaches the client", async {
        while seen < FLOOD {
            match watch.recv().await {
                Ok(_) => seen += 1,
                Err(RecvError::Lagged(skipped)) => seen += skipped,
                Err(RecvError::Closed) => panic!("connection closed"),
            }
        }
    })
    .await;
}

fn skipped<T: std::fmt::Debug>(outcome: Result<T, ConnectorError>) -> u64 {
    match outcome {
        Err(ConnectorError::Lagged { skipped }) => skipped,
        other => panic!("expected the loss to be reported, got {other:?}"),
    }
}

#[tokio::test]
async fn a_reader_that_falls_behind_is_told_and_its_stream_drops_what_it_knew() {
    let Session { server, client } = session_with(
        |payload_type, payload| {
            depth_subscriptions(payload_type, payload)
                .or_else(|| trendbar_data(payload_type, payload))
        },
        |options| options.event_capacity(EVENT_CAPACITY),
    )
    .await;
    let mut quotes = client
        .subscribe_quotes(account(101), &[symbol(1)])
        .await
        .expect("subscription");
    let mut book = client
        .subscribe_depth(account(101), &[symbol(1)])
        .await
        .expect("subscription");
    let mut bars = client
        .subscribe_live_trendbars(account(101), symbol(1), Period::M1)
        .await
        .expect("subscription");
    let mut raw = client.events();
    server.push(depth(
        101,
        1,
        vec![level(1, None, Some(108_520)), level(2, Some(108_500), None)],
        Vec::new(),
    ));
    server.push(spot_with_bars(1, Some(100_008), vec![live_bar(1, 100_001)]));
    let known = soon("quote", quotes.next()).await.expect("quote");
    assert_eq!(known.bid, "1.00008".parse().ok());
    soon("update", book.next()).await.expect("update");
    assert_eq!(book.book(symbol(1)).map(DepthBook::len), Some(2));
    let bar = soon("bar", bars.next()).await.expect("bar");
    assert_eq!(bar.close.to_string(), "1.00008");
    let late = FLOOD - u64::try_from(EVENT_CAPACITY).expect("capacity");
    flooded(
        &server,
        &client,
        [
            depth(101, 1, Vec::new(), vec![1]),
            spot_with_bars(1, None, vec![live_bar(1, 100_004)]),
        ],
    )
    .await;
    assert!(matches!(
        raw.recv().await,
        Err(RecvError::Lagged(lost)) if lost == late + 2
    ));
    assert!(raw.recv().await.is_ok());

    assert_eq!(skipped(soon("lag", quotes.next()).await), late);
    assert_eq!(quotes.last(symbol(1)), None);
    let partial = soon("quote after the lag", quotes.next())
        .await
        .expect("quote");
    assert_eq!((partial.bid, partial.ask), (None, "0.00001".parse().ok()));

    assert_eq!(skipped(soon("lag", bars.next()).await), late);
    server.push(spot_with_bars(1, Some(100_030), vec![live_bar(1, 100_005)]));
    let bar = soon("bar after the lag", bars.next()).await.expect("bar");
    assert_eq!(bar.low.to_string(), "1.00005");
    assert_eq!(bar.close.to_string(), "1.00030");

    assert_eq!(skipped(soon("lag", book.next()).await), late + 2);
    assert!(book.is_stale());
    assert!(book.book(symbol(1)).is_none());
    let unapplied = soon("change after the lag", book.next())
        .await
        .expect("update");
    assert_eq!(unapplied.new_quotes.len(), 1);
    assert!(book.book(symbol(1)).is_none());
    server.precede(
        SUBSCRIBE_DEPTH_REQ,
        vec![depth(
            101,
            1,
            vec![level(7, Some(108_490), None), level(8, None, Some(108_510))],
            Vec::new(),
        )],
    );
    let sent = (
        server.count(UNSUBSCRIBE_DEPTH_REQ),
        server.count(SUBSCRIBE_DEPTH_REQ),
    );
    book.resync().await.expect("fresh subscription");
    assert_eq!(
        (
            server.count(UNSUBSCRIBE_DEPTH_REQ),
            server.count(SUBSCRIBE_DEPTH_REQ)
        ),
        (sent.0 + 1, sent.1 + 1)
    );
    assert!(!book.is_stale());
    let snapshot = soon("fresh depth", book.next()).await.expect("update");
    assert_eq!(snapshot.new_quotes.len(), 2);
    let fresh = book.book(symbol(1)).expect("book");
    assert_eq!(fresh.len(), 2);
    assert_eq!(fresh.best_bid(), "1.08490".parse().ok());
    assert_eq!(fresh.best_ask(), "1.08510".parse().ok());
    client.shutdown().await;
}

const GET_TICKDATA_REQ: u32 = 2145;
const GET_TICKDATA_RES: u32 = 2146;
const WEEK: i64 = 604_800_000;
const TICKS_PER_RESPONSE: usize = 3;
const TICK_TIMES: [i64; 9] = [
    1_000,
    2_000,
    2_000,
    3_000,
    4_000,
    5_000,
    WEEK + 6_000,
    3 * WEEK,
    3 * WEEK + 500,
];

const CROWDED_TICK_TIMES: [i64; 9] = [900, 1_000, 1_000, 1_000, 1_000, 1_000, 1_100, 1_200, 1_300];

const SPLIT_TICK_TIMES: [i64; 5] = [900, 1_000, 1_000, 1_100, 1_200];

fn tick_price(side: i32, at: i64) -> i64 {
    100_000 + i64::from(side) * 1_000 + (at / 500) % 50
}

fn tick_data(payload_type: u32, payload: &[u8]) -> Answer {
    if payload_type != GET_TICKDATA_REQ {
        return None;
    }
    let request = ProtoOaGetTickDataReq::decode(payload).ok()?;
    if request.to_timestamp - request.from_timestamp > WEEK {
        return refuse("INCORRECT_BOUNDARIES", "range too long");
    }
    let page = match request.symbol_id {
        666 => return refuse("SYMBOL_NOT_FOUND", "unknown symbol"),
        13 => vec![
            ProtoOaTickData {
                timestamp: 5,
                tick: 1,
            },
            ProtoOaTickData {
                timestamp: 0,
                tick: -2,
            },
        ],
        14 => {
            return answer(
                GET_TICKDATA_RES,
                &ProtoOaGetTickDataRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    tick_data: vec![ProtoOaTickData {
                        timestamp: request.to_timestamp - 1,
                        tick: 7,
                    }],
                    has_more: true,
                },
            );
        }
        16 => {
            return answer(
                GET_TICKDATA_RES,
                &ProtoOaGetTickDataRes {
                    payload_type: None,
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    tick_data: Vec::new(),
                    has_more: true,
                },
            );
        }
        _ => Vec::new(),
    };
    if !page.is_empty() {
        return answer(
            GET_TICKDATA_RES,
            &ProtoOaGetTickDataRes {
                payload_type: None,
                ctid_trader_account_id: request.ctid_trader_account_id,
                tick_data: page,
                has_more: false,
            },
        );
    }
    let times: &[i64] = match request.symbol_id {
        15 => &CROWDED_TICK_TIMES,
        17 => &SPLIT_TICK_TIMES,
        _ => &TICK_TIMES,
    };
    let matching: Vec<i64> = times
        .iter()
        .rev()
        .copied()
        .filter(|at| *at >= request.from_timestamp && *at < request.to_timestamp)
        .collect();
    let mut previous = (0, 0);
    let tick_data = matching
        .iter()
        .take(TICKS_PER_RESPONSE)
        .map(|at| {
            let price = tick_price(request.r#type, *at);
            let wire = ProtoOaTickData {
                timestamp: at - previous.0,
                tick: price - previous.1,
            };
            previous = (*at, price);
            wire
        })
        .collect();
    answer(
        GET_TICKDATA_RES,
        &ProtoOaGetTickDataRes {
            payload_type: None,
            ctid_trader_account_id: request.ctid_trader_account_id,
            tick_data,
            has_more: matching.len() > TICKS_PER_RESPONSE,
        },
    )
}

fn millis(from: i64, to: i64) -> TimeRange {
    let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
    TimeRange::new(at(from), at(to)).expect("range")
}

fn tick_requests(server: &MockServer) -> Vec<ProtoOaGetTickDataReq> {
    server
        .received()
        .iter()
        .filter(|message| message.payload_type == GET_TICKDATA_REQ)
        .map(|message| ProtoOaGetTickDataReq::decode(message.payload.as_slice()).expect("request"))
        .collect()
}

#[tokio::test]
async fn tick_history_is_paged_split_by_week_and_decoded_in_order() {
    let Session { server, client } = session(tick_data).await;
    let range = millis(1_500, 3 * WEEK + 100);
    let bids = TickDataParams::new(account(101), symbol(1), QuoteSide::Bid, range);
    let ticks = client.tick_data(&bids).await.expect("ticks");
    let seen: Vec<(i64, String)> = ticks
        .iter()
        .map(|tick| (tick.timestamp.as_millis(), tick.price.to_string()))
        .collect();
    assert_eq!(
        seen,
        [
            (2_000, "1.01004".to_owned()),
            (2_000, "1.01004".to_owned()),
            (3_000, "1.01006".to_owned()),
            (4_000, "1.01008".to_owned()),
            (5_000, "1.01010".to_owned()),
            (WEEK + 6_000, "1.01012".to_owned()),
            (3 * WEEK, "1.01000".to_owned()),
        ]
    );
    let requests = tick_requests(&server);
    let spans: Vec<(i64, i64)> = requests
        .iter()
        .map(|request| (request.from_timestamp, request.to_timestamp))
        .collect();
    assert_eq!(
        spans,
        [
            (2 * WEEK + 101, 3 * WEEK + 101),
            (WEEK + 101, 2 * WEEK + 101),
            (1_500, WEEK + 101),
            (1_500, 3_001),
        ]
    );
    assert!(requests.iter().all(|request| request.r#type == 1));
    let asks = TickDataParams::new(account(101), symbol(1), QuoteSide::Ask, range).limit(2);
    let latest = client.tick_data(&asks).await.expect("ticks");
    assert_eq!(
        latest
            .iter()
            .map(|tick| (tick.timestamp.as_millis(), tick.price.to_string()))
            .collect::<Vec<_>>(),
        [
            (WEEK + 6_000, "1.02012".to_owned()),
            (3 * WEEK, "1.02000".to_owned())
        ]
    );
    let requests = tick_requests(&server);
    assert_eq!(requests.len(), 6);
    assert_eq!(requests[5].r#type, 2);
    let none = TickDataParams::new(account(101), symbol(1), QuoteSide::Bid, range).limit(0);
    assert!(client.tick_data(&none).await.expect("ticks").is_empty());
    assert_eq!(server.count(GET_TICKDATA_REQ), 6);
    client.shutdown().await;
}

#[tokio::test]
async fn refused_malformed_and_endless_tick_answers_end_the_request() {
    let Session { server, client } = session(tick_data).await;
    let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
    let params =
        |id| TickDataParams::new(account(101), symbol(id), QuoteSide::Bid, millis(100, 200));
    assert_eq!(
        server_code(client.tick_data(&params(666)).await),
        Some(ErrorCode::SymbolNotFound)
    );
    assert_eq!(
        client.tick_data(&params(13)).await,
        Err(ConnectorError::Value(ValueError::OutOfRange {
            kind: "tick price"
        }))
    );
    let brief = TickDataParams::new(account(101), symbol(14), QuoteSide::Bid, millis(100, 102));
    assert_eq!(
        client.tick_data(&brief).await,
        Err(ConnectorError::Incomplete {
            from: at(100),
            to: at(102)
        })
    );
    assert_eq!(server.count(GET_TICKDATA_REQ), 3);
    let endless = millis(i64::MAX - 5, i64::MAX);
    let last = TickDataParams::new(account(101), symbol(1), QuoteSide::Bid, endless);
    assert_eq!(
        client.tick_data(&last).await,
        Err(ConnectorError::Value(ValueError::OutOfRange {
            kind: "tick timestamp"
        }))
    );
    assert_eq!(server.count(GET_TICKDATA_REQ), 3);
    client.shutdown().await;
}

fn tick_times(ticks: &[Tick]) -> Vec<i64> {
    ticks
        .iter()
        .map(|tick| tick.timestamp.as_millis())
        .collect()
}

fn tick_spans(server: &MockServer) -> Vec<(i64, i64)> {
    tick_requests(server)
        .iter()
        .map(|request| (request.from_timestamp, request.to_timestamp))
        .collect()
}

#[tokio::test]
async fn a_millisecond_split_between_two_answers_is_returned_whole_and_once() {
    let Session { server, client } = session(tick_data).await;
    let split = TickDataParams::new(account(101), symbol(17), QuoteSide::Bid, millis(500, 2_000));
    let ticks = client.tick_data(&split).await.expect("ticks");
    assert_eq!(tick_times(&ticks), SPLIT_TICK_TIMES);
    assert_eq!(tick_spans(&server), [(500, 2_001), (500, 1_001)]);
    let edges = TickDataParams::new(
        account(101),
        symbol(17),
        QuoteSide::Bid,
        millis(1_000, 1_200),
    );
    let ticks = client.tick_data(&edges).await.expect("ticks");
    assert_eq!(tick_times(&ticks), [1_000, 1_000, 1_100, 1_200]);
    assert_eq!(tick_spans(&server)[2..], [(1_000, 1_201), (1_000, 1_001)]);
    let latest = client.tick_data(&split.limit(3)).await.expect("ticks");
    assert_eq!(tick_times(&latest), [1_000, 1_100, 1_200]);
    client.shutdown().await;
}

#[tokio::test]
async fn a_millisecond_holding_more_ticks_than_an_answer_is_a_typed_error() {
    let Session { server, client } = session(tick_data).await;
    let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
    let crowded = TickDataParams::new(account(101), symbol(15), QuoteSide::Bid, millis(500, 2_000));
    assert_eq!(
        client.tick_data(&crowded).await,
        Err(ConnectorError::Incomplete {
            from: at(500),
            to: at(1_000)
        })
    );
    assert_eq!(
        tick_spans(&server),
        [(500, 2_001), (500, 1_101), (500, 1_001)]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn more_ticks_announced_but_never_sent_are_a_typed_error() {
    let Session { server, client } = session(tick_data).await;
    let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
    let hollow = TickDataParams::new(account(101), symbol(16), QuoteSide::Bid, millis(500, 2_000));
    assert_eq!(
        client.tick_data(&hollow).await,
        Err(ConnectorError::Incomplete {
            from: at(500),
            to: at(2_000)
        })
    );
    assert_eq!(server.count(GET_TICKDATA_REQ), 1);
    client.shutdown().await;
}

fn trader_data(payload_type: u32, payload: &[u8]) -> Answer {
    if payload_type != TRADER_REQ {
        return None;
    }
    let request = ProtoOaTraderReq::decode(payload).ok()?;
    let account = request.ctid_trader_account_id;
    let plain = ProtoOaTrader {
        ctid_trader_account_id: account,
        balance: 1_234_567,
        deposit_asset_id: 11,
        ..ProtoOaTrader::default()
    };
    let trader = match account {
        101 => ProtoOaTrader {
            leverage_in_cents: Some(10_000),
            max_leverage: Some(500),
            account_type: Some(1),
            access_rights: Some(1),
            swap_free: Some(true),
            is_limited_risk: Some(true),
            broker_name: Some("Mock Broker".to_owned()),
            registration_timestamp: Some(1_600_000_000_000),
            ..plain
        },
        102 => ProtoOaTrader {
            deposit_asset_id: 4,
            money_digits: Some(8),
            manager_bonus: Some(150_000_000),
            ib_bonus: Some(1),
            non_withdrawable_bonus: Some(0),
            account_type: Some(2),
            access_rights: Some(3),
            ..plain
        },
        103 => ProtoOaTrader {
            deposit_asset_id: 99,
            ..plain
        },
        104 => ProtoOaTrader {
            account_type: Some(9),
            ..plain
        },
        105 => ProtoOaTrader {
            access_rights: Some(7),
            ..plain
        },
        106 => ProtoOaTrader {
            access_rights: Some(2),
            account_type: Some(0),
            ..plain
        },
        _ => plain,
    };
    answer(
        TRADER_RES,
        &ProtoOaTraderRes {
            payload_type: None,
            ctid_trader_account_id: account,
            trader,
        },
    )
}

#[tokio::test]
async fn trader_carries_scaled_money_and_the_name_of_its_deposit_asset() {
    let Session { server, client } = session(|payload_type, payload| {
        trader_data(payload_type, payload).or_else(|| reference_data(payload_type, payload))
    })
    .await;
    let trader = client.trader(account(101)).await.expect("trader");
    assert_eq!(trader.account, account(101));
    assert_eq!(trader.balance.to_string(), "12345.67");
    assert_eq!(trader.money_digits, 2);
    assert_eq!(trader.deposit_asset.get(), 11);
    assert_eq!(trader.currency, "USD");
    assert_eq!(
        trader
            .leverage
            .map(|leverage| leverage.to_string())
            .as_deref(),
        Some("100")
    );
    assert_eq!(trader.max_leverage, Some(500));
    assert_eq!(trader.account_type, AccountType::Netted);
    assert_eq!(trader.access_rights, AccessRights::CloseOnly);
    assert!(trader.swap_free && trader.limited_risk);
    assert_eq!(trader.broker_name.as_deref(), Some("Mock Broker"));
    assert_eq!(
        trader.registered,
        Timestamp::from_millis(1_600_000_000_000).ok()
    );
    let other = client.trader(account(102)).await.expect("trader");
    assert_eq!(other.balance.to_string(), "0.01234567");
    assert_eq!(other.money_digits, 8);
    assert_eq!(other.currency, "EUR");
    assert_eq!(
        other
            .manager_bonus
            .map(|bonus| bonus.to_string())
            .as_deref(),
        Some("1.50000000")
    );
    assert_eq!(
        other.ib_bonus.map(|bonus| bonus.to_string()).as_deref(),
        Some("0.00000001")
    );
    assert_eq!(
        other.non_withdrawable_bonus,
        Some(rust_decimal::Decimal::ZERO)
    );
    assert_eq!(other.account_type, AccountType::SpreadBetting);
    assert_eq!(other.access_rights, AccessRights::NoLogin);
    assert_eq!((other.leverage, other.registered), (None, None));
    assert!(!other.swap_free && !other.limited_risk);
    let bare = client.trader(account(106)).await.expect("trader");
    assert_eq!(bare.account_type, AccountType::Hedged);
    assert_eq!(bare.access_rights, AccessRights::NoTrading);
    let plain = client.trader(account(107)).await.expect("trader");
    assert_eq!(plain.account_type, AccountType::Hedged);
    assert_eq!(plain.access_rights, AccessRights::FullAccess);
    for (id, kind) in [
        (103, "deposit asset"),
        (104, "account type"),
        (105, "access rights"),
    ] {
        assert_eq!(
            client.trader(account(id)).await,
            Err(ConnectorError::Value(ValueError::OutOfRange { kind })),
        );
    }
    assert_eq!(server.count(TRADER_REQ), 7);
    client.shutdown().await;
}
