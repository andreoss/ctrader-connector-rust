//! Behaviour of the Open API client against the mock server.

mod support;

use std::time::Duration;

use ctrader_connector::common::config::{ConfigurationOpenApi, ConfigurationOpenApiBuilder};
use ctrader_connector::common::errors::{ConnectorError, ErrorCode, TokenError, TransportError};
use ctrader_connector::common::ids::AccountId;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::connection::{Connection, ConnectionOptions, Event};
use ctrader_connector::open_api::messages::{
    ProtoHeartbeatEvent, ProtoOaApplicationAuthReq, ProtoOaGetAccountListByAccessTokenReq,
    ProtoOaSpotEvent, ProtoOaVersionReq,
};
use ctrader_connector::open_api::token::TokenPair;
use ctrader_connector::open_api::transport::{Connect, Framed, Plain, Tls};
use support::{
    ACCESS_TOKEN, ACCOUNT_AUTH_REQ, ACCOUNT_LIST_REQ, APPLICATION_AUTH_REQ, APPLICATION_AUTH_RES,
    CLIENT_ID, CLIENT_SECRET, HEARTBEAT, HOST, HttpMock, MockOptions, MockServer, OA_ERROR,
    REFRESH_TOKEN, REFRESH_TOKEN_REQ, VERSION_REQ, VERSION_RES, envelope,
};
use tokio::sync::broadcast;
use tokio::time::timeout;

#[tokio::test]
async fn framed_round_trip_against_the_mock_server() {
    let server = MockServer::start(MockOptions::default()).await;
    let stream = Plain.connect(HOST, server.port()).await.expect("connect");
    let mut framed = Framed::new(stream);
    framed
        .send(&envelope(
            VERSION_REQ,
            &ProtoOaVersionReq { payload_type: None },
            Some("early".to_owned()),
        ))
        .await
        .expect("send");
    let refused = framed.receive().await.expect("reply");
    assert_eq!(refused.payload_type, OA_ERROR);
    assert_eq!(refused.client_msg_id.as_deref(), Some("early"));
    framed
        .send(&envelope(
            APPLICATION_AUTH_REQ,
            &ProtoOaApplicationAuthReq {
                payload_type: None,
                client_id: CLIENT_ID.to_owned(),
                client_secret: CLIENT_SECRET.to_owned(),
            },
            Some("auth".to_owned()),
        ))
        .await
        .expect("send");
    let reply = framed.receive().await.expect("reply");
    assert_eq!(reply.payload_type, APPLICATION_AUTH_RES);
    assert_eq!(reply.client_msg_id.as_deref(), Some("auth"));
    assert_eq!(server.types_on(1), vec![VERSION_REQ, APPLICATION_AUTH_REQ]);
    server.drop_connections();
    assert_eq!(framed.receive().await, Err(TransportError::Closed));
}

#[tokio::test]
async fn tls_handshake_with_a_plain_server_fails_with_a_typed_error() {
    let server = MockServer::start(MockOptions::default()).await;
    let tls = Tls::new().expect("tls");
    let error = tls.connect("localhost", server.port()).await.err();
    assert!(
        matches!(error, Some(TransportError::Tls(_))),
        "unexpected outcome: {error:?}"
    );
}

#[tokio::test]
async fn connecting_to_a_closed_port_fails_with_an_io_error() {
    let server = MockServer::start(MockOptions::default()).await;
    let port = server.port();
    drop(server);
    tokio::task::yield_now().await;
    let error = Plain.connect(HOST, port).await.err();
    assert!(
        matches!(error, Some(TransportError::Io { .. })),
        "unexpected outcome: {error:?}"
    );
}

fn options() -> ConnectionOptions {
    ConnectionOptions::new()
        .timeout(Duration::from_secs(5))
        .heartbeat_interval(Duration::from_secs(5))
}

async fn open(server: &MockServer, options: ConnectionOptions) -> Connection {
    Connection::open(Plain, HOST, server.port(), options)
        .await
        .expect("connection")
}

fn application_auth(secret: &str) -> ProtoOaApplicationAuthReq {
    ProtoOaApplicationAuthReq {
        payload_type: None,
        client_id: CLIENT_ID.to_owned(),
        client_secret: secret.to_owned(),
    }
}

fn account_list() -> ProtoOaGetAccountListByAccessTokenReq {
    ProtoOaGetAccountListByAccessTokenReq {
        payload_type: None,
        access_token: ACCESS_TOKEN.to_owned(),
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

async fn next_event(events: &mut broadcast::Receiver<Event>) -> Event {
    timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("event in time")
        .expect("event")
}

#[tokio::test]
async fn concurrent_requests_resolve_to_their_own_responses() {
    let server = MockServer::start(MockOptions {
        reverse_batch: 6,
        ..MockOptions::default()
    })
    .await;
    let connection = open(&server, options()).await;
    connection
        .request(&application_auth(CLIENT_SECRET))
        .await
        .expect("application authorisation");
    let version = ProtoOaVersionReq::default();
    let accounts = account_list();
    let (v1, a1, v2, a2, v3, a3) = tokio::join!(
        connection.request(&version),
        connection.request(&accounts),
        connection.request(&version),
        connection.request(&accounts),
        connection.request(&version),
        connection.request(&accounts),
    );
    for response in [v1, v2, v3] {
        assert_eq!(response.expect("version").version, "mock-1");
    }
    for response in [a1, a2, a3] {
        assert_eq!(response.expect("accounts").ctid_trader_account.len(), 2);
    }
    assert_eq!(connection.stats().requests_sent, 7);
    connection.shutdown().await;
}

#[tokio::test]
async fn server_error_is_delivered_to_the_request_it_answers() {
    let server = MockServer::start(MockOptions::default()).await;
    let connection = open(&server, options()).await;
    let early = connection.request(&ProtoOaVersionReq::default()).await;
    assert!(matches!(
        early,
        Err(ConnectorError::Server(error)) if error.code == ErrorCode::ChClientNotAuthenticated
    ));
    let refused = connection.request(&application_auth("wrong")).await;
    assert!(matches!(
        refused,
        Err(ConnectorError::Server(error)) if error.code == ErrorCode::ChClientAuthFailure
    ));
    connection
        .request(&application_auth(CLIENT_SECRET))
        .await
        .expect("application authorisation");
    let unsupported = connection.request_message(9999, Vec::new()).await;
    assert!(matches!(
        unsupported,
        Err(ConnectorError::Server(error)) if error.code == ErrorCode::UnsupportedMessage
    ));
    let mismatched = connection
        .request_message(VERSION_REQ, Vec::new())
        .await
        .expect("version envelope");
    assert_eq!(mismatched.payload_type, VERSION_RES);
}

#[tokio::test]
async fn heartbeats_are_sent_while_the_connection_is_idle() {
    let server = MockServer::start(MockOptions::default()).await;
    let connection = open(
        &server,
        options().heartbeat_interval(Duration::from_millis(40)),
    )
    .await;
    eventually("three heartbeats", || server.count(HEARTBEAT) >= 3).await;
    assert!(connection.stats().heartbeats_sent >= 3);
    server.push(envelope(HEARTBEAT, &ProtoHeartbeatEvent::default(), None));
    connection
        .request(&application_auth(CLIENT_SECRET))
        .await
        .expect("request after idling");
    eventually("server heartbeat counted", || {
        connection.stats().heartbeats_received == 1
    })
    .await;
}

#[tokio::test]
async fn unanswered_request_times_out() {
    let server = MockServer::start(MockOptions {
        silent: vec![VERSION_REQ],
        ..MockOptions::default()
    })
    .await;
    let connection = open(&server, options().timeout(Duration::from_millis(80))).await;
    let outcome = connection.request(&ProtoOaVersionReq::default()).await;
    assert_eq!(
        outcome.err(),
        Some(ConnectorError::Timeout(Duration::from_millis(80)))
    );
    connection
        .request(&application_auth(CLIENT_SECRET))
        .await
        .expect("later request");
}

#[tokio::test]
async fn pushed_messages_reach_every_subscriber() {
    let server = MockServer::start(MockOptions::default()).await;
    let connection = open(&server, options()).await;
    let mut first = connection.events();
    let mut second = connection.clone().events();
    connection
        .request(&application_auth(CLIENT_SECRET))
        .await
        .expect("application authorisation");
    server.push_spot(101, 1, 123_456);
    for events in [&mut first, &mut second] {
        let spot = next_event(events)
            .await
            .decode::<ProtoOaSpotEvent>()
            .expect("spot event");
        assert_eq!(spot.symbol_id, 1);
        assert_eq!(spot.bid, Some(123_456));
    }
}

#[tokio::test]
async fn shutdown_fails_pending_and_later_requests() {
    let server = MockServer::start(MockOptions {
        silent: vec![VERSION_REQ],
        ..MockOptions::default()
    })
    .await;
    let connection = open(&server, options()).await;
    let pending = {
        let connection = connection.clone();
        tokio::spawn(async move { connection.request(&ProtoOaVersionReq::default()).await })
    };
    eventually("request reached the server", || {
        server.count(VERSION_REQ) == 1
    })
    .await;
    connection.shutdown().await;
    assert_eq!(
        pending.await.expect("task").err(),
        Some(ConnectorError::Closed)
    );
    let later = connection.request(&ProtoOaVersionReq::default()).await;
    assert_eq!(later.err(), Some(ConnectorError::Closed));
    connection.shutdown().await;
}

#[tokio::test]
async fn dropping_every_handle_closes_the_socket() {
    let server = MockServer::start(MockOptions::default()).await;
    let connection = open(&server, options()).await;
    let stream = Plain.connect(HOST, server.port()).await.expect("probe");
    drop(connection);
    drop(stream);
    eventually("server saw both connections", || server.connections() == 2).await;
    eventually("server saw the sockets close", || {
        server.open_connections() == 0
    })
    .await;
}

#[tokio::test]
async fn oversized_request_fails_alone() {
    let server = MockServer::start(MockOptions::default()).await;
    let connection = open(&server, options()).await;
    let huge = connection
        .request_message(VERSION_REQ, vec![0; 16 * 1024 * 1024 + 1])
        .await;
    assert!(matches!(
        huge,
        Err(ConnectorError::Transport(
            TransportError::FrameTooLong { .. }
        ))
    ));
    connection
        .request(&application_auth(CLIENT_SECRET))
        .await
        .expect("connection still usable");
}

#[tokio::test]
async fn opening_fails_when_nothing_listens() {
    let server = MockServer::start(MockOptions::default()).await;
    let port = server.port();
    drop(server);
    tokio::task::yield_now().await;
    let outcome = Connection::open(Plain, HOST, port, options()).await;
    assert!(matches!(
        outcome,
        Err(ConnectorError::Transport(TransportError::Io { .. }))
    ));
}

#[tokio::test]
async fn lost_connection_fails_pending_requests_and_is_reported() {
    let server = MockServer::start(MockOptions {
        silent: vec![VERSION_REQ],
        ..MockOptions::default()
    })
    .await;
    let connection = open(&server, options()).await;
    let mut events = connection.events();
    let pending = {
        let connection = connection.clone();
        tokio::spawn(async move { connection.request(&ProtoOaVersionReq::default()).await })
    };
    eventually("request reached the server", || {
        server.count(VERSION_REQ) == 1
    })
    .await;
    server.drop_connections();
    assert_eq!(
        pending.await.expect("task").err(),
        Some(ConnectorError::Transport(TransportError::Closed))
    );
    assert_eq!(
        next_event(&mut events).await,
        Event::Disconnected(TransportError::Closed)
    );
}

fn builder(server: &MockServer) -> ConfigurationOpenApiBuilder {
    ConfigurationOpenApi::demo()
        .host(HOST)
        .port(server.port())
        .client_id(CLIENT_ID)
        .client_secret(CLIENT_SECRET)
        .access_token(ACCESS_TOKEN)
        .refresh_token(REFRESH_TOKEN)
        .timeout(Duration::from_secs(5))
}

async fn client(configuration: ConfigurationOpenApiBuilder) -> OpenApiClient {
    let configuration = configuration.build().expect("configuration");
    let options = ConnectionOptions::from(&configuration);
    OpenApiClient::connect_with(configuration, Plain, options)
        .await
        .expect("client")
}

fn account(id: i64) -> AccountId {
    AccountId::new(id).expect("account id")
}

fn code(outcome: Result<(), ConnectorError>) -> Option<ErrorCode> {
    match outcome {
        Err(ConnectorError::Server(error)) => Some(error.code),
        _ => None,
    }
}

#[tokio::test]
async fn application_and_account_are_authorised_in_order() {
    let server = MockServer::start(MockOptions::default()).await;
    let client = client(builder(&server)).await;
    assert_eq!(client.configuration().port(), server.port());
    client.authorise_application().await.expect("application");
    assert_eq!(client.version().await.as_deref(), Ok("mock-1"));
    let accounts = client.accounts().await.expect("accounts");
    assert_eq!(
        accounts.iter().map(|a| a.id.get()).collect::<Vec<_>>(),
        vec![101, 102]
    );
    assert!(accounts.iter().all(|a| !a.is_live && a.login == Some(7)));
    client
        .authorise_account(account(101))
        .await
        .expect("account");
    assert_eq!(
        server.types_on(1),
        vec![
            APPLICATION_AUTH_REQ,
            VERSION_REQ,
            ACCOUNT_LIST_REQ,
            ACCOUNT_AUTH_REQ
        ]
    );
    assert_eq!(
        code(client.authorise_account(account(999)).await),
        Some(ErrorCode::ChCtidTraderAccountNotFound)
    );
    client.shutdown().await;
}

#[tokio::test]
async fn bad_secret_yields_the_typed_error() {
    let server = MockServer::start(MockOptions::default()).await;
    let client = client(builder(&server).client_secret("wrong")).await;
    assert_eq!(
        code(client.authorise_application().await),
        Some(ErrorCode::ChClientAuthFailure)
    );
    assert_eq!(
        code(client.authorise_account(account(101)).await),
        Some(ErrorCode::ChClientNotAuthenticated)
    );
}

#[tokio::test]
async fn bad_token_yields_the_typed_error() {
    let server = MockServer::start(MockOptions::default()).await;
    let client = client(builder(&server).access_token("stale")).await;
    client.authorise_application().await.expect("application");
    assert_eq!(
        code(client.authorise_account(account(101)).await),
        Some(ErrorCode::ChAccessTokenInvalid)
    );
    assert_eq!(
        code(client.accounts().await.map(|_| ())),
        Some(ErrorCode::ChAccessTokenInvalid)
    );
}

#[tokio::test]
async fn account_id_out_of_range_is_a_value_error() {
    let server = MockServer::start(MockOptions {
        accounts: vec![0],
        ..MockOptions::default()
    })
    .await;
    let client = client(builder(&server)).await;
    client.authorise_application().await.expect("application");
    assert!(matches!(
        client.accounts().await,
        Err(ConnectorError::Value(_))
    ));
}

#[tokio::test]
async fn default_connect_uses_tls_and_fails_against_a_plain_server() {
    let server = MockServer::start(MockOptions::default()).await;
    let configuration = builder(&server).build().expect("configuration");
    let outcome = OpenApiClient::connect(configuration).await;
    assert!(matches!(
        outcome.map(|_| ()),
        Err(ConnectorError::Transport(TransportError::Tls(_)))
    ));
}

#[tokio::test]
async fn refresh_rotates_the_pair_and_the_new_token_authorises() {
    let server = MockServer::start(MockOptions::default()).await;
    let client = client(builder(&server)).await;
    client.authorise_application().await.expect("application");
    assert_eq!(client.access_token().expose(), ACCESS_TOKEN);
    let pair = client.refresh_token().await.expect("refresh");
    assert_eq!(pair.access.expose(), "mock-access-1");
    assert_eq!(pair.refresh.expose(), "mock-refresh-1");
    assert_eq!(pair.expires_in, Duration::from_secs(2_628_000));
    assert_eq!(client.access_token(), pair.access);
    assert_eq!(server.access_token(), "mock-access-1");
    client
        .authorise_account(account(101))
        .await
        .expect("account with the new token");
    let second = client.refresh_token().await.expect("second refresh");
    assert_eq!(second.refresh.expose(), "mock-refresh-2");
    assert_eq!(server.count(REFRESH_TOKEN_REQ), 2);
    let stale = self::client(builder(&server)).await;
    stale.authorise_application().await.expect("application");
    assert_eq!(
        code(stale.authorise_account(account(101)).await),
        Some(ErrorCode::ChAccessTokenInvalid)
    );
    assert_eq!(
        code(stale.refresh_token().await.map(|_| ())),
        Some(ErrorCode::Unknown("ACCESS_DENIED".to_owned()))
    );
    stale.set_tokens(&second);
    stale
        .authorise_account(account(101))
        .await
        .expect("account with the adopted pair");
    client.shutdown().await;
    stale.shutdown().await;
}

#[tokio::test]
async fn refresh_without_a_refresh_token_is_refused_locally() {
    let server = MockServer::start(MockOptions::default()).await;
    let configuration = ConfigurationOpenApi::demo()
        .host(HOST)
        .port(server.port())
        .client_id(CLIENT_ID)
        .client_secret(CLIENT_SECRET)
        .access_token(ACCESS_TOKEN);
    let client = client(configuration).await;
    let missing = Err(ConnectorError::Token(TokenError::NoRefreshToken));
    assert_eq!(client.refresh_token().await, missing);
    assert_eq!(client.refresh_token_over_http_with(&Plain).await, missing);
    assert_eq!(client.refresh_token_over_http().await, missing);
    assert_eq!(server.count(REFRESH_TOKEN_REQ), 0);
    client.shutdown().await;
}

async fn http_refresh(response: &str) -> (Result<TokenPair, ConnectorError>, Vec<String>) {
    let server = MockServer::start(MockOptions::default()).await;
    let http = HttpMock::start(response).await;
    let configuration = builder(&server)
        .client_secret("s e/c+ret")
        .token_url(http.url())
        .timeout(Duration::from_millis(500));
    let client = client(configuration).await;
    let outcome = client.refresh_token_over_http_with(&Plain).await;
    if let Ok(pair) = &outcome {
        assert_eq!(client.access_token(), pair.access);
    } else {
        assert_eq!(client.access_token().expose(), ACCESS_TOKEN);
    }
    client.shutdown().await;
    (outcome, http.requests())
}

#[tokio::test]
async fn refresh_over_http_sends_the_documented_query_and_reads_the_pair() {
    let body = r#"{"accessToken":"http-access","refreshToken":"http-refresh","expiresIn":60,"tokenType":"bearer","errorCode":null,"description":null}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let (outcome, requests) = http_refresh(&response).await;
    let pair = outcome.expect("pair");
    assert_eq!(pair.access.expose(), "http-access");
    assert_eq!(pair.refresh.expose(), "http-refresh");
    assert_eq!(pair.expires_in, Duration::from_secs(60));
    assert_eq!(requests.len(), 1);
    let expected = format!(
        "GET /apps/token?grant_type=refresh_token&refresh_token={REFRESH_TOKEN}&client_id={CLIENT_ID}&client_secret=s%20e%2Fc%2Bret HTTP/1.1\r\nHost: 127.0.0.1\r\n"
    );
    assert!(requests[0].starts_with(&expected), "{}", requests[0]);
    assert!(requests[0].contains("\r\nConnection: close\r\n"));
}

#[tokio::test]
async fn refresh_over_http_reports_refusals_and_broken_replies() {
    let refused = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3b\r\n{\"errorCode\":\"ACCESS_DENIED\",\"description\":\"Access denied\"}\r\n0\r\n\r\n";
    assert_eq!(
        http_refresh(refused).await.0,
        Err(ConnectorError::Token(TokenError::Refused {
            code: "ACCESS_DENIED".to_owned(),
            description: "Access denied".to_owned(),
        }))
    );
    assert_eq!(
        http_refresh("HTTP/1.1 503 Service Unavailable\r\n\r\n<html>")
            .await
            .0,
        Err(ConnectorError::Token(TokenError::Status(503)))
    );
    assert_eq!(
        http_refresh("HTTP/1.1 200 OK\r\n\r\n{}").await.0,
        Err(ConnectorError::Token(TokenError::Malformed("expiresIn")))
    );
    assert_eq!(
        http_refresh("garbage").await.0,
        Err(ConnectorError::Token(TokenError::Malformed(
            "HTTP response"
        )))
    );
    assert_eq!(
        http_refresh("").await.0,
        Err(ConnectorError::Timeout(Duration::from_millis(500)))
    );
}

#[tokio::test]
async fn refresh_over_http_fails_when_the_endpoint_is_unreachable() {
    let server = MockServer::start(MockOptions::default()).await;
    let http = HttpMock::start("x").await;
    let url = http.url();
    drop(http);
    tokio::task::yield_now().await;
    let client = client(builder(&server).token_url(url)).await;
    assert!(matches!(
        client.refresh_token_over_http_with(&Plain).await,
        Err(ConnectorError::Transport(TransportError::Io { .. }))
    ));
    client.shutdown().await;
}
