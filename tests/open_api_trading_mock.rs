//! Behaviour of the trading calls against the mock server.

mod support;

use std::time::Duration;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::errors::{
    ConnectorError, ErrorCode, Interruption, TransportError, ValueError,
};
use ctrader_connector::common::ids::{AccountId, PositionId, SymbolId};
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::connection::{ConnectionOptions, Event};
use ctrader_connector::open_api::messages::{
    ProtoOaAmendOrderReq, ProtoOaAmendPositionSltpReq, ProtoOaCancelOrderReq, ProtoOaDeal,
    ProtoOaExecutionEvent, ProtoOaNewOrderReq, ProtoOaOrder, ProtoOaOrderErrorEvent,
    ProtoOaPosition, ProtoOaSymbol, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaTradeData,
    ProtoOaVersionRes,
};
use ctrader_connector::open_api::symbols::Symbol;
use ctrader_connector::open_api::trading::{
    Comment, Execution, ExecutionType, Label, NewOrder, Order, OrderAmendment, OrderStatus,
    OrderType, Position, PositionAmendment, TimeInForce, TradeSide,
};
use ctrader_connector::open_api::transport::Plain;
use prost::Message as _;
use support::{
    ACCESS_TOKEN, Answer, CLIENT_ID, CLIENT_SECRET, HOST, MockOptions, MockServer, REFRESH_TOKEN,
    Responder, answer, envelope, refuse,
};
use tokio::sync::broadcast;
use tokio::time::timeout;

const SYMBOL_BY_ID_REQ: u32 = 2116;
const SYMBOL_BY_ID_RES: u32 = 2117;
const NEW_ORDER_REQ: u32 = 2106;
const CANCEL_ORDER_REQ: u32 = 2108;
const AMEND_ORDER_REQ: u32 = 2109;
const AMEND_POSITION_REQ: u32 = 2110;
const MISSING: i64 = 404;
const EXECUTION_EVENT: u32 = 2126;
const ORDER_ERROR_EVENT: u32 = 2132;
const VERSION_RES: u32 = 2105;

const ACCEPTED: i32 = 2;
const FILLED: i32 = 3;
const REPLACED: i32 = 4;
const REJECTED: i32 = 7;

fn account(id: i64) -> AccountId {
    AccountId::new(id).expect("account id")
}

fn symbol_id(id: i64) -> SymbolId {
    SymbolId::new(id).expect("symbol id")
}

fn volume(text: &str) -> Volume {
    text.parse().expect("volume")
}

fn price(text: &str) -> Price {
    text.parse().expect("price")
}

struct Session {
    server: MockServer,
    client: OpenApiClient,
    symbol: Symbol,
}

fn details(payload_type: u32, payload: &[u8]) -> Answer {
    if payload_type != SYMBOL_BY_ID_REQ {
        return None;
    }
    let request = ProtoOaSymbolByIdReq::decode(payload).ok()?;
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
                    digits: 2,
                    pip_position: 1,
                    min_volume: Some(100),
                    max_volume: Some(100_000),
                    step_volume: Some(50),
                    ..ProtoOaSymbol::default()
                })
                .collect(),
            archived_symbol: Vec::new(),
        },
    )
}

async fn session(responder: impl Fn(u32, &[u8]) -> Answer + Send + Sync + 'static) -> Session {
    session_with(responder, Vec::new(), |options| options).await
}

async fn session_with(
    responder: impl Fn(u32, &[u8]) -> Answer + Send + Sync + 'static,
    silent: Vec<u32>,
    adjust: impl FnOnce(ConnectionOptions) -> ConnectionOptions,
) -> Session {
    let server = MockServer::start(MockOptions {
        responder: Some(Responder::new(move |payload_type, payload| {
            details(payload_type, payload).or_else(|| responder(payload_type, payload))
        })),
        silent,
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
    let options = adjust(ConnectionOptions::from(&configuration));
    let client = OpenApiClient::connect_with(configuration, Plain, options)
        .await
        .expect("client");
    client.authorise_application().await.expect("application");
    client
        .authorise_account(account(101))
        .await
        .expect("account");
    let symbol = client
        .symbol_details(account(101), &[symbol_id(7)])
        .await
        .expect("details")
        .pop()
        .expect("symbol");
    Session {
        server,
        client,
        symbol,
    }
}

fn server_code<T>(outcome: Result<T, ConnectorError>) -> Option<ErrorCode> {
    match outcome {
        Err(ConnectorError::Server(error)) => Some(error.code),
        _ => None,
    }
}

fn value_error<T>(outcome: Result<T, ConnectorError>) -> Option<ValueError> {
    match outcome {
        Err(ConnectorError::Value(error)) => Some(error),
        _ => None,
    }
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

async fn next_execution(events: &mut broadcast::Receiver<Event>) -> ProtoOaExecutionEvent {
    loop {
        let event = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("event in time")
            .expect("event");
        if let Event::Message(message) = event {
            if message.payload_type == EXECUTION_EVENT {
                return ProtoOaExecutionEvent::decode(
                    message.payload.as_deref().unwrap_or_default(),
                )
                .expect("execution event");
            }
        }
    }
}

fn wire_order(request: &ProtoOaNewOrderReq, status: i32) -> ProtoOaOrder {
    ProtoOaOrder {
        order_id: 900,
        trade_data: ProtoOaTradeData {
            symbol_id: request.symbol_id,
            volume: request.volume,
            trade_side: request.trade_side,
            open_timestamp: Some(1_700_000_000_000),
            label: request.label.clone(),
            comment: request.comment.clone(),
            ..ProtoOaTradeData::default()
        },
        order_type: request.order_type,
        order_status: status,
        time_in_force: request.time_in_force.or(Some(3)),
        expiration_timestamp: request.expiration_timestamp,
        limit_price: request.limit_price,
        stop_price: request.stop_price,
        slippage_in_points: request.slippage_in_points.map(i64::from),
        stop_loss: request.stop_loss,
        take_profit: request.take_profit,
        relative_stop_loss: request.relative_stop_loss,
        relative_take_profit: request.relative_take_profit,
        position_id: Some(500),
        utc_last_update_timestamp: Some(1_700_000_000_100),
        ..ProtoOaOrder::default()
    }
}

fn wire_position(request: &ProtoOaNewOrderReq, status: i32) -> ProtoOaPosition {
    ProtoOaPosition {
        position_id: 500,
        trade_data: ProtoOaTradeData {
            symbol_id: request.symbol_id,
            volume: request.volume,
            trade_side: request.trade_side,
            open_timestamp: Some(1_700_000_000_000),
            label: request.label.clone(),
            comment: request.comment.clone(),
            ..ProtoOaTradeData::default()
        },
        position_status: status,
        swap: -125,
        price: Some(64_000.25),
        commission: Some(-40),
        used_margin: Some(6_400),
        money_digits: Some(2),
        ..ProtoOaPosition::default()
    }
}

fn execution(request: &ProtoOaNewOrderReq, kind: i32) -> ProtoOaExecutionEvent {
    let filled = kind == FILLED;
    ProtoOaExecutionEvent {
        payload_type: None,
        ctid_trader_account_id: request.ctid_trader_account_id,
        execution_type: kind,
        position: Some(wire_position(request, if filled { 1 } else { 3 })),
        order: Some(wire_order(request, if filled { 2 } else { 1 })),
        deal: filled.then(|| ProtoOaDeal {
            deal_id: 700,
            order_id: 900,
            position_id: 500,
            volume: request.volume,
            filled_volume: request.volume,
            symbol_id: request.symbol_id,
            create_timestamp: 1_700_000_000_050,
            execution_timestamp: 1_700_000_000_060,
            execution_price: Some(64_000.25),
            trade_side: request.trade_side,
            deal_status: 2,
            commission: Some(-40),
            money_digits: Some(2),
            ..ProtoOaDeal::default()
        }),
        ..ProtoOaExecutionEvent::default()
    }
}

fn accept(payload_type: u32, payload: &[u8]) -> Answer {
    if payload_type != NEW_ORDER_REQ {
        return None;
    }
    let request = ProtoOaNewOrderReq::decode(payload).ok()?;
    answer(EXECUTION_EVENT, &execution(&request, ACCEPTED))
}

fn market(symbol: &Symbol, side: TradeSide, units: &str) -> NewOrder {
    NewOrder::market(account(101), symbol.id, side, volume(units))
}

fn sent_orders(server: &MockServer) -> Vec<ProtoOaNewOrderReq> {
    server
        .received()
        .iter()
        .filter(|received| received.payload_type == NEW_ORDER_REQ)
        .map(|received| ProtoOaNewOrderReq::decode(received.payload.as_slice()).expect("order"))
        .collect()
}

#[tokio::test]
async fn market_order_returns_the_acceptance_and_the_fill_follows_as_an_event() {
    let Session {
        server,
        client,
        symbol,
    } = session(accept).await;
    let mut events = client.events();
    let order = market(&symbol, TradeSide::Buy, "1.5")
        .label(Label::new("run-1").expect("label"))
        .comment(Comment::new("first").expect("comment"))
        .stop_loss_distance(price("250.5"))
        .take_profit_distance(price("400"));
    assert_eq!(order.account(), account(101));
    assert_eq!(order.symbol(), symbol_id(7));
    assert_eq!(order.side(), TradeSide::Buy);
    assert_eq!(order.volume(), volume("1.5"));
    assert_eq!(order.kind(), OrderType::Market);
    let accepted: Execution = client.place_order(&order, &symbol).await.expect("accepted");
    let sent = sent_orders(&server);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].ctid_trader_account_id, 101);
    assert_eq!(sent[0].symbol_id, 7);
    assert_eq!(sent[0].order_type, 1);
    assert_eq!(sent[0].trade_side, 1);
    assert_eq!(sent[0].volume, 150);
    assert_eq!(sent[0].label.as_deref(), Some("run-1"));
    assert_eq!(sent[0].comment.as_deref(), Some("first"));
    assert_eq!(sent[0].relative_stop_loss, Some(25_050_000));
    assert_eq!(sent[0].relative_take_profit, Some(40_000_000));
    assert_eq!(sent[0].stop_loss, None);
    assert_eq!(sent[0].take_profit, None);
    assert_eq!(sent[0].limit_price, None);
    assert_eq!(sent[0].stop_price, None);
    assert_eq!(accepted.account, account(101));
    assert_eq!(accepted.kind, ExecutionType::Accepted);
    assert!(accepted.deal.is_none());
    assert!(!accepted.server_event);
    assert_eq!(accepted.error_code, None);
    let placed = accepted.order.expect("order");
    assert_eq!(placed.id.get(), 900);
    assert_eq!(placed.symbol, symbol_id(7));
    assert_eq!(placed.side, TradeSide::Buy);
    assert_eq!(placed.volume, volume("1.5"));
    assert_eq!(placed.kind, OrderType::Market);
    assert_eq!(placed.status, OrderStatus::Accepted);
    assert_eq!(placed.time_in_force, TimeInForce::ImmediateOrCancel);
    assert_eq!(placed.relative_stop_loss, Some(price("250.5")));
    assert_eq!(placed.relative_take_profit, Some(price("400")));
    assert_eq!(placed.label.as_deref(), Some("run-1"));
    assert_eq!(placed.comment.as_deref(), Some("first"));
    assert_eq!(placed.position.map(PositionId::get), Some(500));
    let position = accepted.position.expect("position");
    assert_eq!(position.id.get(), 500);
    assert_eq!(position.price, Some(price("64000.25")));
    assert_eq!(position.swap.to_string(), "-1.25");
    assert_eq!(
        position.commission.map(|money| money.to_string()),
        Some("-0.40".to_owned())
    );
    assert_eq!(
        position.used_margin.map(|money| money.to_string()),
        Some("64.00".to_owned())
    );
    server.push(envelope(
        EXECUTION_EVENT,
        &execution(&sent[0], FILLED),
        None,
    ));
    let first = next_execution(&mut events).await;
    let second = next_execution(&mut events).await;
    assert_eq!(first.execution_type, ACCEPTED);
    assert_eq!(second.execution_type, FILLED);
    let filled = Execution::try_from(second).expect("fill");
    assert_eq!(filled.kind, ExecutionType::Filled);
    let deal = filled.deal.expect("deal");
    assert_eq!(deal.id.get(), 700);
    assert_eq!(deal.order.get(), 900);
    assert_eq!(deal.position.get(), 500);
    assert_eq!(deal.filled_volume, volume("1.5"));
    assert_eq!(deal.execution_price, Some(price("64000.25")));
    assert_eq!(
        deal.commission.map(|money| money.to_string()),
        Some("-0.40".to_owned())
    );
    client.shutdown().await;
}

#[tokio::test]
async fn order_in_flight_when_the_connection_drops_is_not_sent_again() {
    let Session {
        server,
        client,
        symbol,
    } = session_with(|_, _| None, vec![NEW_ORDER_REQ], |options| options).await;
    let order = market(&symbol, TradeSide::Sell, "1");
    let placing = {
        let client = client.clone();
        let symbol = symbol.clone();
        tokio::spawn(async move { client.place_order(&order, &symbol).await })
    };
    eventually("order received", || server.count(NEW_ORDER_REQ) == 1).await;
    server.drop_connections();
    let outcome = timeout(Duration::from_secs(5), placing)
        .await
        .expect("outcome in time")
        .expect("task");
    assert!(
        matches!(
            outcome,
            Err(ConnectorError::OutcomeUnknown(Interruption::Transport(_)))
        ),
        "{outcome:?}"
    );
    eventually("reconnected", || {
        client.connection().stats().reconnects == 1
    })
    .await;
    assert_eq!(client.version().await.as_deref(), Ok("mock-1"));
    assert_eq!(server.connections(), 2);
    assert_eq!(server.count(NEW_ORDER_REQ), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn order_without_an_answer_times_out_with_an_unknown_outcome_and_is_not_sent_again() {
    let Session {
        server,
        client,
        symbol,
    } = session_with(
        |_, _| None,
        vec![NEW_ORDER_REQ],
        |options| options.timeout(Duration::from_millis(300)),
    )
    .await;
    let outcome = client
        .place_order(&market(&symbol, TradeSide::Buy, "1"), &symbol)
        .await;
    assert_eq!(
        outcome,
        Err(ConnectorError::OutcomeUnknown(Interruption::Timeout(
            Duration::from_millis(300)
        )))
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(server.count(NEW_ORDER_REQ), 1);
    assert_eq!(client.version().await.as_deref(), Ok("mock-1"));
    client.shutdown().await;
}

#[tokio::test]
async fn order_refused_for_the_request_limit_is_not_retried() {
    let Session {
        server,
        client,
        symbol,
    } = session_with(accept, Vec::new(), |options| {
        options.retries(4).backoff(Duration::from_millis(20))
    })
    .await;
    server.set_limit(Some(0));
    let outcome = client
        .place_order(&market(&symbol, TradeSide::Buy, "1"), &symbol)
        .await;
    assert_eq!(
        server_code(outcome),
        Some(ErrorCode::RequestFrequencyExceeded)
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.count(NEW_ORDER_REQ), 1);
    assert_eq!(client.connection().stats().throttle_retries, 0);
    client.shutdown().await;
}

fn refuse_by_volume(payload_type: u32, payload: &[u8]) -> Answer {
    if payload_type != NEW_ORDER_REQ {
        return None;
    }
    let request = ProtoOaNewOrderReq::decode(payload).ok()?;
    match request.volume {
        100 => answer(
            ORDER_ERROR_EVENT,
            &ProtoOaOrderErrorEvent {
                payload_type: None,
                ctid_trader_account_id: request.ctid_trader_account_id,
                error_code: "TRADING_BAD_VOLUME".to_owned(),
                order_id: Some(900),
                position_id: None,
                description: Some("volume is not allowed".to_owned()),
            },
        ),
        150 => answer(
            ORDER_ERROR_EVENT,
            &ProtoOaOrderErrorEvent {
                payload_type: None,
                ctid_trader_account_id: 0,
                error_code: "SOMETHING_NEW".to_owned(),
                order_id: None,
                position_id: None,
                description: None,
            },
        ),
        200 => refuse("MARKET_CLOSED", "closed for the weekend"),
        250 => answer(
            EXECUTION_EVENT,
            &ProtoOaExecutionEvent {
                error_code: Some("NOT_ENOUGH_MONEY".to_owned()),
                ..execution(&request, REJECTED)
            },
        ),
        300 => answer(EXECUTION_EVENT, &execution(&request, REJECTED)),
        350 => answer(EXECUTION_EVENT, &execution(&request, 8)),
        400 => Some((ORDER_ERROR_EVENT, vec![0xff])),
        450 => answer(EXECUTION_EVENT, &execution(&request, 99)),
        500 => Some((EXECUTION_EVENT, vec![0xff])),
        _ => answer(
            VERSION_RES,
            &ProtoOaVersionRes {
                payload_type: None,
                version: "mock-1".to_owned(),
            },
        ),
    }
}

#[tokio::test]
async fn refusals_of_an_order_are_typed_server_errors() {
    let Session {
        server,
        client,
        symbol,
    } = session(refuse_by_volume).await;
    let place = |units: &'static str| {
        let client = client.clone();
        let symbol = symbol.clone();
        async move {
            client
                .place_order(&market(&symbol, TradeSide::Buy, units), &symbol)
                .await
        }
    };
    let bad_volume = place("1").await;
    match &bad_volume {
        Err(ConnectorError::Server(error)) => {
            assert_eq!(error.code, ErrorCode::TradingBadVolume);
            assert_eq!(error.description.as_deref(), Some("volume is not allowed"));
            assert_eq!(error.account_id, Some(account(101)));
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    match place("1.5").await {
        Err(ConnectorError::Server(error)) => {
            assert_eq!(error.code, ErrorCode::Unknown("SOMETHING_NEW".to_owned()));
            assert_eq!(error.description, None);
            assert_eq!(error.account_id, None);
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert_eq!(server_code(place("2").await), Some(ErrorCode::MarketClosed));
    assert_eq!(
        server_code(place("2.5").await),
        Some(ErrorCode::NotEnoughMoney)
    );
    assert_eq!(server_code(place("3").await), Some(ErrorCode::UnknownError));
    match place("3.5").await {
        Err(ConnectorError::Server(error)) => {
            assert_eq!(error.code, ErrorCode::UnknownError);
            assert_eq!(
                error.description.as_deref(),
                Some("the request to cancel the order was refused")
            );
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    assert!(matches!(
        place("4").await,
        Err(ConnectorError::Transport(TransportError::Decode(_)))
    ));
    assert_eq!(
        value_error(place("4.5").await),
        Some(ValueError::OutOfRange {
            kind: "execution type"
        })
    );
    assert!(matches!(
        place("5").await,
        Err(ConnectorError::Transport(TransportError::Decode(_)))
    ));
    assert_eq!(
        place("5.5").await,
        Err(ConnectorError::UnexpectedResponse {
            expected: EXECUTION_EVENT,
            received: VERSION_RES,
        })
    );
    assert_eq!(server.count(NEW_ORDER_REQ), 10);
    client.shutdown().await;
}

#[tokio::test]
async fn order_that_does_not_fit_the_symbol_is_refused_before_anything_is_sent() {
    let Session {
        server,
        client,
        symbol,
    } = session(accept).await;
    let place = |order: NewOrder| {
        let client = client.clone();
        let symbol = symbol.clone();
        async move { value_error(client.place_order(&order, &symbol).await) }
    };
    let decimal = |text: &str| volume(text).value();
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "0.5")).await,
        Some(ValueError::BelowMinimum {
            kind: "volume",
            value: decimal("0.5"),
            minimum: decimal("1"),
        })
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "0")).await,
        Some(ValueError::BelowMinimum {
            kind: "volume",
            value: decimal("0"),
            minimum: decimal("1"),
        })
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1000.5")).await,
        Some(ValueError::AboveMaximum {
            kind: "volume",
            value: decimal("1000.5"),
            maximum: decimal("1000"),
        })
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1.25")).await,
        Some(ValueError::OffStep {
            kind: "volume",
            value: decimal("1.25"),
            step: decimal("0.5"),
        })
    );
    assert_eq!(
        place(NewOrder::market(
            account(101),
            symbol_id(8),
            TradeSide::Buy,
            volume("1")
        ))
        .await,
        Some(ValueError::Mismatch {
            kind: "symbol of the order"
        })
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1").stop_loss_distance(price("1.005"))).await,
        Some(ValueError::TooFine {
            kind: "distance of the protection",
            value: price("1.005").value(),
            digits: 2,
        })
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1").take_profit_distance(price("0"))).await,
        Some(ValueError::BelowMinimum {
            kind: "distance of the protection",
            value: price("0").value(),
            minimum: price("0.01").value(),
        })
    );
    assert_eq!(server.count(NEW_ORDER_REQ), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn order_with_terms_its_kind_does_not_take_is_refused_before_anything_is_sent() {
    let Session {
        server,
        client,
        symbol,
    } = session(accept).await;
    let place = |order: NewOrder| {
        let client = client.clone();
        let symbol = symbol.clone();
        async move { value_error(client.place_order(&order, &symbol).await) }
    };
    let unsupported = |kind| Some(ValueError::Unsupported { kind });
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1").stop_loss(price("60000"))).await,
        unsupported("protection given as a price")
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1").take_profit(price("60000"))).await,
        unsupported("protection given as a price")
    );
    assert_eq!(
        place(market(&symbol, TradeSide::Buy, "1").good_till(Timestamp::EPOCH)).await,
        unsupported("expiry of the order")
    );
    let pending = |text: &str| {
        NewOrder::limit(
            account(101),
            symbol.id,
            TradeSide::Buy,
            volume("1"),
            price(text),
        )
    };
    assert_eq!(
        place(pending("60000.005")).await,
        Some(ValueError::TooFine {
            kind: "price",
            value: price("60000.005").value(),
            digits: 2,
        })
    );
    assert_eq!(
        place(pending("0")).await,
        Some(ValueError::BelowMinimum {
            kind: "price",
            value: price("0").value(),
            minimum: price("0.01").value(),
        })
    );
    assert_eq!(
        place(pending("60000").stop_loss(price("59000.001"))).await,
        Some(ValueError::TooFine {
            kind: "protection given as a price",
            value: price("59000.001").value(),
            digits: 2,
        })
    );
    assert_eq!(
        place(NewOrder::stop(
            account(101),
            symbol.id,
            TradeSide::Sell,
            volume("1"),
            price("0.001")
        ))
        .await,
        Some(ValueError::TooFine {
            kind: "price",
            value: price("0.001").value(),
            digits: 2,
        })
    );
    assert_eq!(server.count(NEW_ORDER_REQ), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn limit_and_stop_orders_carry_their_price_and_how_long_they_wait() {
    let Session {
        server,
        client,
        symbol,
    } = session(accept).await;
    let expiry = Timestamp::from_millis(1_900_000_000_000).expect("moment");
    let limit = NewOrder::limit(
        account(101),
        symbol.id,
        TradeSide::Buy,
        volume("1"),
        price("60000.5"),
    )
    .good_till(expiry)
    .stop_loss_distance(price("5"))
    .stop_loss(price("59000"))
    .take_profit(price("61000.25"));
    assert_eq!(limit.kind(), OrderType::Limit);
    let placed = client
        .place_order(&limit, &symbol)
        .await
        .expect("limit order")
        .order
        .expect("order");
    assert_eq!(placed.kind, OrderType::Limit);
    assert_eq!(placed.limit_price, Some(price("60000.5")));
    assert_eq!(placed.stop_price, None);
    assert_eq!(placed.time_in_force, TimeInForce::GoodTillDate(expiry));
    assert_eq!(placed.stop_loss, Some(price("59000")));
    assert_eq!(placed.take_profit, Some(price("61000.25")));
    let stop = NewOrder::stop(
        account(101),
        symbol.id,
        TradeSide::Sell,
        volume("2"),
        price("50000"),
    )
    .stop_loss(price("51000"))
    .stop_loss_distance(price("100"))
    .take_profit(price("49000"))
    .take_profit_distance(price("200.5"));
    assert_eq!(stop.kind(), OrderType::Stop);
    let placed = client
        .place_order(&stop, &symbol)
        .await
        .expect("stop order")
        .order
        .expect("order");
    assert_eq!(placed.kind, OrderType::Stop);
    assert_eq!(placed.side, TradeSide::Sell);
    assert_eq!(placed.stop_price, Some(price("50000")));
    assert_eq!(placed.limit_price, None);
    assert_eq!(placed.time_in_force, TimeInForce::GoodTillCancel);
    assert_eq!(placed.relative_stop_loss, Some(price("100")));
    assert_eq!(placed.relative_take_profit, Some(price("200.5")));
    let sent = sent_orders(&server);
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].order_type, 2);
    assert_eq!(sent[0].limit_price, Some(60_000.5));
    assert_eq!(sent[0].stop_price, None);
    assert_eq!(sent[0].time_in_force, Some(1));
    assert_eq!(sent[0].expiration_timestamp, Some(1_900_000_000_000));
    assert_eq!(sent[0].stop_loss, Some(59_000.0));
    assert_eq!(sent[0].take_profit, Some(61_000.25));
    assert_eq!(sent[0].relative_stop_loss, None);
    assert_eq!(sent[0].relative_take_profit, None);
    assert_eq!(sent[1].order_type, 3);
    assert_eq!(sent[1].trade_side, 2);
    assert_eq!(sent[1].volume, 200);
    assert_eq!(sent[1].limit_price, None);
    assert_eq!(sent[1].stop_price, Some(50_000.0));
    assert_eq!(sent[1].time_in_force, Some(2));
    assert_eq!(sent[1].expiration_timestamp, None);
    assert_eq!(sent[1].stop_loss, None);
    assert_eq!(sent[1].take_profit, None);
    assert_eq!(sent[1].relative_stop_loss, Some(10_000_000));
    assert_eq!(sent[1].relative_take_profit, Some(20_050_000));
    client.shutdown().await;
}

#[tokio::test]
async fn stop_limit_order_carries_its_stop_price_and_slippage_and_no_limit_price() {
    let Session {
        server,
        client,
        symbol,
    } = session(accept).await;
    let stop_limit = |slippage: u32| {
        NewOrder::stop_limit(
            account(101),
            symbol.id,
            TradeSide::Buy,
            volume("1"),
            price("70000.5"),
            slippage,
        )
    };
    assert_eq!(stop_limit(25).kind(), OrderType::StopLimit);
    let placed = client
        .place_order(&stop_limit(25), &symbol)
        .await
        .expect("stop-limit order")
        .order
        .expect("order");
    assert_eq!(placed.kind, OrderType::StopLimit);
    assert_eq!(placed.stop_price, Some(price("70000.5")));
    assert_eq!(placed.limit_price, None);
    assert_eq!(placed.slippage_points, Some(25));
    assert_eq!(placed.time_in_force, TimeInForce::GoodTillCancel);
    assert_eq!(
        value_error(client.place_order(&stop_limit(u32::MAX), &symbol).await),
        Some(ValueError::OutOfRange {
            kind: "slippage in points"
        })
    );
    let sent = sent_orders(&server);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].order_type, 6);
    assert_eq!(sent[0].stop_price, Some(70_000.5));
    assert_eq!(sent[0].limit_price, None);
    assert_eq!(sent[0].slippage_in_points, Some(25));
    assert_eq!(sent[0].base_slippage_price, None);
    assert_eq!(sent[0].time_in_force, Some(2));
    client.shutdown().await;
}

fn amended_order(request: &ProtoOaAmendOrderReq) -> ProtoOaOrder {
    let kind = match (request.limit_price, request.slippage_in_points) {
        (Some(_), _) => 2,
        (None, Some(_)) => 6,
        (None, None) => 3,
    };
    ProtoOaOrder {
        order_id: request.order_id,
        trade_data: ProtoOaTradeData {
            symbol_id: 7,
            volume: request.volume.unwrap_or(100),
            trade_side: 1,
            ..ProtoOaTradeData::default()
        },
        order_type: kind,
        order_status: 1,
        time_in_force: Some(if request.expiration_timestamp.is_some() {
            1
        } else {
            2
        }),
        expiration_timestamp: request.expiration_timestamp,
        limit_price: request.limit_price,
        stop_price: request.stop_price,
        slippage_in_points: request.slippage_in_points.map(i64::from),
        stop_loss: request.stop_loss,
        take_profit: request.take_profit,
        relative_stop_loss: request.relative_stop_loss,
        relative_take_profit: request.relative_take_profit,
        ..ProtoOaOrder::default()
    }
}

fn amended_position(request: &ProtoOaAmendPositionSltpReq) -> ProtoOaPosition {
    ProtoOaPosition {
        position_id: request.position_id,
        trade_data: ProtoOaTradeData {
            symbol_id: 7,
            volume: 100,
            trade_side: 1,
            ..ProtoOaTradeData::default()
        },
        position_status: 1,
        swap: 0,
        price: Some(64_000.25),
        stop_loss: request.stop_loss,
        take_profit: request.take_profit,
        ..ProtoOaPosition::default()
    }
}

fn not_found(account: i64, code: &str) -> Answer {
    answer(
        ORDER_ERROR_EVENT,
        &ProtoOaOrderErrorEvent {
            payload_type: None,
            ctid_trader_account_id: account,
            error_code: code.to_owned(),
            order_id: None,
            position_id: None,
            description: Some("nothing with that id".to_owned()),
        },
    )
}

fn amend(payload_type: u32, payload: &[u8]) -> Answer {
    match payload_type {
        AMEND_ORDER_REQ => {
            let request = ProtoOaAmendOrderReq::decode(payload).ok()?;
            if request.order_id == MISSING {
                return not_found(request.ctid_trader_account_id, "ORDER_NOT_FOUND");
            }
            answer(
                EXECUTION_EVENT,
                &ProtoOaExecutionEvent {
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    execution_type: REPLACED,
                    order: Some(amended_order(&request)),
                    ..ProtoOaExecutionEvent::default()
                },
            )
        }
        AMEND_POSITION_REQ => {
            let request = ProtoOaAmendPositionSltpReq::decode(payload).ok()?;
            if request.position_id == MISSING {
                return not_found(request.ctid_trader_account_id, "POSITION_NOT_FOUND");
            }
            let first = request.take_profit.is_some() && request.stop_loss.is_some();
            answer(
                EXECUTION_EVENT,
                &ProtoOaExecutionEvent {
                    ctid_trader_account_id: request.ctid_trader_account_id,
                    execution_type: if first { ACCEPTED } else { REPLACED },
                    position: Some(amended_position(&request)),
                    ..ProtoOaExecutionEvent::default()
                },
            )
        }
        _ => None,
    }
}

fn sent<T: prost::Message + Default>(server: &MockServer, payload_type: u32) -> Vec<T> {
    server
        .received()
        .iter()
        .filter(|received| received.payload_type == payload_type)
        .map(|received| T::decode(received.payload.as_slice()).expect("request"))
        .collect()
}

fn pending(id: i64, kind: i32, symbol: i64) -> Order {
    let waits_at = Some(60_000.5);
    Order::try_from(ProtoOaOrder {
        order_id: id,
        trade_data: ProtoOaTradeData {
            symbol_id: symbol,
            volume: 100,
            trade_side: 1,
            ..ProtoOaTradeData::default()
        },
        order_type: kind,
        order_status: 1,
        time_in_force: Some(1),
        expiration_timestamp: Some(1_800_000_000_000),
        limit_price: if kind == 2 { waits_at } else { None },
        stop_price: if kind == 3 || kind == 6 {
            waits_at
        } else {
            None
        },
        slippage_in_points: (kind == 6).then_some(25),
        stop_loss: Some(50_000.0),
        relative_take_profit: Some(150_000),
        ..ProtoOaOrder::default()
    })
    .expect("order")
}

fn open(id: i64, symbol: i64) -> Position {
    Position::try_from(ProtoOaPosition {
        position_id: id,
        trade_data: ProtoOaTradeData {
            symbol_id: symbol,
            volume: 100,
            trade_side: 1,
            ..ProtoOaTradeData::default()
        },
        position_status: 1,
        swap: 0,
        price: Some(64_000.25),
        ..ProtoOaPosition::default()
    })
    .expect("position")
}

#[tokio::test]
async fn amendment_of_a_pending_order_states_the_whole_order() {
    let Session {
        server,
        client,
        symbol,
    } = session(amend).await;
    let later = Timestamp::from_millis(1_800_000_600_000).expect("timestamp");
    let limit = OrderAmendment::of(account(101), &pending(900, 2, 7));
    assert_eq!(limit.order().get(), 900);
    let untouched = client
        .amend_order(&limit, &symbol)
        .await
        .expect("amended order");
    assert_eq!(untouched.kind, ExecutionType::Replaced);
    let changed = limit
        .clone()
        .price(price("59000.25"))
        .volume(volume("1.5"))
        .good_till(later)
        .stop_loss(None)
        .take_profit(Some(price("70000")));
    let order = client
        .amend_order(&changed, &symbol)
        .await
        .expect("amended order")
        .order
        .expect("order");
    assert_eq!(order.volume, volume("1.5"));
    assert_eq!(order.limit_price, Some(price("59000.25")));
    assert_eq!(order.time_in_force, TimeInForce::GoodTillDate(later));
    assert_eq!(
        (order.stop_loss, order.take_profit),
        (None, Some(price("70000")))
    );
    let stop = OrderAmendment::of(account(101), &pending(901, 3, 7))
        .price(price("61000"))
        .good_till_cancelled();
    client
        .amend_order(&stop, &symbol)
        .await
        .expect("amended stop order");
    let stop_limit = OrderAmendment::of(account(101), &pending(902, 6, 7)).slippage(30);
    client
        .amend_order(&stop_limit, &symbol)
        .await
        .expect("amended stop-limit order");
    let sent: Vec<ProtoOaAmendOrderReq> = sent(&server, AMEND_ORDER_REQ);
    assert_eq!(sent.len(), 4);
    assert_eq!(
        (sent[0].ctid_trader_account_id, sent[0].order_id),
        (101, 900)
    );
    assert_eq!(sent[0].volume, Some(100));
    assert_eq!(
        (sent[0].limit_price, sent[0].stop_price),
        (Some(60_000.5), None)
    );
    assert_eq!(sent[0].expiration_timestamp, Some(1_800_000_000_000));
    assert_eq!(
        (sent[0].stop_loss, sent[0].take_profit),
        (Some(50_000.0), None)
    );
    assert_eq!(sent[0].relative_stop_loss, None);
    assert_eq!(sent[0].relative_take_profit, Some(150_000));
    assert_eq!(sent[0].slippage_in_points, None);
    assert_eq!(sent[1].volume, Some(150));
    assert_eq!(sent[1].limit_price, Some(59_000.25));
    assert_eq!(sent[1].expiration_timestamp, Some(1_800_000_600_000));
    assert_eq!(
        (sent[1].stop_loss, sent[1].relative_stop_loss),
        (None, None)
    );
    assert_eq!(sent[1].take_profit, Some(70_000.0));
    assert_eq!(sent[1].relative_take_profit, None);
    assert_eq!(
        (sent[2].limit_price, sent[2].stop_price),
        (None, Some(61_000.0))
    );
    assert_eq!(sent[2].expiration_timestamp, None);
    assert_eq!(
        (sent[3].limit_price, sent[3].stop_price),
        (None, Some(60_000.5))
    );
    assert_eq!(sent[3].slippage_in_points, Some(30));
    client.shutdown().await;
}

#[tokio::test]
async fn amendment_that_does_not_fit_is_refused_before_anything_is_sent() {
    let Session {
        server,
        client,
        symbol,
    } = session(amend).await;
    let limit = OrderAmendment::of(account(101), &pending(900, 2, 7));
    let refused = |amendment: OrderAmendment| {
        let (client, symbol) = (client.clone(), symbol.clone());
        async move { value_error(client.amend_order(&amendment, &symbol).await) }
    };
    assert!(matches!(
        refused(OrderAmendment::of(account(101), &pending(900, 1, 7))).await,
        Some(ValueError::Unsupported { .. })
    ));
    assert!(matches!(
        refused(OrderAmendment::of(account(101), &pending(900, 2, 8))).await,
        Some(ValueError::Mismatch { .. })
    ));
    assert!(matches!(
        refused(limit.clone().volume(volume("1.25"))).await,
        Some(ValueError::OffStep { .. })
    ));
    assert!(matches!(
        refused(limit.clone().price(price("59000.255"))).await,
        Some(ValueError::TooFine { .. })
    ));
    assert!(matches!(
        refused(limit.clone().stop_loss(Some(price("0.001")))).await,
        Some(ValueError::TooFine { .. })
    ));
    assert!(matches!(
        refused(OrderAmendment::of(account(101), &pending(902, 6, 7)).slippage(u32::MAX)).await,
        Some(ValueError::OutOfRange { .. })
    ));
    let position = PositionAmendment::of(account(101), &open(500, 7));
    assert!(matches!(
        value_error(
            client
                .amend_position(&PositionAmendment::of(account(101), &open(500, 8)), &symbol)
                .await
        ),
        Some(ValueError::Mismatch { .. })
    ));
    assert!(matches!(
        value_error(
            client
                .amend_position(&position.take_profit(Some(price("70000.001"))), &symbol)
                .await
        ),
        Some(ValueError::TooFine { .. })
    ));
    assert_eq!(server.count(AMEND_ORDER_REQ), 0);
    assert_eq!(server.count(AMEND_POSITION_REQ), 0);
    assert_eq!(
        server_code(
            client
                .amend_order(
                    &OrderAmendment::of(account(101), &pending(MISSING, 2, 7)),
                    &symbol
                )
                .await
        ),
        Some(ErrorCode::OrderNotFound)
    );
    assert_eq!(
        server_code(
            client
                .amend_position(
                    &PositionAmendment::of(account(101), &open(MISSING, 7)),
                    &symbol
                )
                .await
        ),
        Some(ErrorCode::PositionNotFound)
    );
    assert_eq!(server.count(AMEND_ORDER_REQ), 1);
    assert_eq!(server.count(AMEND_POSITION_REQ), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn amendment_of_a_position_states_both_protection_prices() {
    let Session {
        server,
        client,
        symbol,
    } = session(amend).await;
    let opened = open(500, 7);
    let amendment = PositionAmendment::of(account(101), &opened)
        .stop_loss(Some(price("50000.5")))
        .take_profit(Some(price("70000")));
    assert_eq!(amendment.position(), opened.id);
    let protected = client
        .amend_position(&amendment, &symbol)
        .await
        .expect("amended position");
    assert_eq!(protected.kind, ExecutionType::Accepted);
    let position = protected.position.expect("position");
    assert_eq!(position.stop_loss, Some(price("50000.5")));
    assert_eq!(position.take_profit, Some(price("70000")));
    let replaced = client
        .amend_position(
            &PositionAmendment::of(account(101), &position).take_profit(None),
            &symbol,
        )
        .await
        .expect("amended position");
    assert_eq!(replaced.kind, ExecutionType::Replaced);
    let position = replaced.position.expect("position");
    assert_eq!(
        (position.stop_loss, position.take_profit),
        (Some(price("50000.5")), None)
    );
    let sent: Vec<ProtoOaAmendPositionSltpReq> = sent(&server, AMEND_POSITION_REQ);
    assert_eq!(sent.len(), 2);
    assert_eq!(
        (sent[0].ctid_trader_account_id, sent[0].position_id),
        (101, 500)
    );
    assert_eq!(
        (sent[0].stop_loss, sent[0].take_profit),
        (Some(50_000.5), Some(70_000.0))
    );
    assert_eq!(
        (sent[1].stop_loss, sent[1].take_profit),
        (Some(50_000.5), None)
    );
    assert_eq!(sent[1].trailing_stop_loss, None);
    client.shutdown().await;
}

fn cancel(payload_type: u32, payload: &[u8]) -> Answer {
    if payload_type != CANCEL_ORDER_REQ {
        return None;
    }
    let request = ProtoOaCancelOrderReq::decode(payload).ok()?;
    let event = |kind: i32, status: i32| ProtoOaExecutionEvent {
        ctid_trader_account_id: request.ctid_trader_account_id,
        execution_type: kind,
        order: Some(ProtoOaOrder {
            order_id: request.order_id,
            trade_data: ProtoOaTradeData {
                symbol_id: 7,
                volume: 100,
                trade_side: 2,
                ..ProtoOaTradeData::default()
            },
            order_type: 2,
            order_status: status,
            limit_price: Some(60_000.5),
            ..ProtoOaOrder::default()
        }),
        ..ProtoOaExecutionEvent::default()
    };
    match request.order_id {
        MISSING => not_found(request.ctid_trader_account_id, "ORDER_NOT_FOUND"),
        901 => answer(EXECUTION_EVENT, &event(8, 1)),
        _ => answer(EXECUTION_EVENT, &event(5, 5)),
    }
}

#[tokio::test]
async fn cancellation_names_the_order_and_returns_it_as_cancelled() {
    let Session { server, client, .. } = session(cancel).await;
    let order = pending(900, 2, 7);
    let cancelled = client
        .cancel_order(account(101), order.id)
        .await
        .expect("cancelled order");
    assert_eq!(cancelled.kind, ExecutionType::Cancelled);
    assert_eq!(cancelled.account, account(101));
    let last = cancelled.order.expect("order");
    assert_eq!(last.id, order.id);
    assert_eq!(last.status, OrderStatus::Cancelled);
    assert_eq!(last.side, TradeSide::Sell);
    assert_eq!(
        server_code(
            client
                .cancel_order(account(101), pending(MISSING, 2, 7).id)
                .await
        ),
        Some(ErrorCode::OrderNotFound)
    );
    assert_eq!(
        server_code(
            client
                .cancel_order(account(101), pending(901, 2, 7).id)
                .await
        ),
        Some(ErrorCode::UnknownError)
    );
    let sent: Vec<ProtoOaCancelOrderReq> = sent(&server, CANCEL_ORDER_REQ);
    assert_eq!(
        sent.iter()
            .map(|request| (request.ctid_trader_account_id, request.order_id))
            .collect::<Vec<_>>(),
        vec![(101, 900), (101, MISSING), (101, 901)]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn cancellation_without_an_answer_has_an_unknown_outcome_and_is_not_sent_again() {
    let Session { server, client, .. } = session_with(
        |_, _| None,
        vec![CANCEL_ORDER_REQ],
        |options| options.timeout(Duration::from_millis(300)),
    )
    .await;
    assert_eq!(
        client
            .cancel_order(account(101), pending(900, 2, 7).id)
            .await,
        Err(ConnectorError::OutcomeUnknown(Interruption::Timeout(
            Duration::from_millis(300)
        )))
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(server.count(CANCEL_ORDER_REQ), 1);
    client.shutdown().await;
}
