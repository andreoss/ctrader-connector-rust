//! Verification against the demo environment.
//!
//! The tests need the `BROKER_*` credentials in the process environment and fail when they are
//! absent. They share one lock so that only one of them talks to the server at a time.
//!
//! A refresh replaces the token pair, so the pair is read from the file named by
//! `BROKER_ENV_FILE` and the refresh test writes the new pair back there before it checks
//! anything else.

use std::path::PathBuf;
use std::time::Duration;

use ctrader_connector::common::config::{
    ConfigurationOpenApi, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN, Environment,
};
use ctrader_connector::common::errors::{ConnectorError, ErrorCode, TokenError};
use ctrader_connector::common::ids::SymbolId;
use ctrader_connector::open_api::client::{Account, OpenApiClient};
use ctrader_connector::open_api::connection::{Connection, ConnectionOptions, Event};
use ctrader_connector::open_api::messages::{
    ProtoMessage, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes,
    ProtoOaGetAccountListByAccessTokenReq, ProtoOaSpotEvent, ProtoOaVersionReq,
};
use ctrader_connector::open_api::transport::{Connect, Framed, Tls};
use prost::Message;
use tokio::sync::Mutex;
use tokio::sync::broadcast;
use tokio::time::timeout;

static LIVE: Mutex<()> = Mutex::const_new(());

const STEP: Duration = Duration::from_secs(20);

fn env_file() -> PathBuf {
    let path = PathBuf::from(std::env::var("BROKER_ENV_FILE").unwrap_or_else(|_| {
        panic!("live tests need BROKER_ENV_FILE to name the file that holds the token pair")
    }));
    assert!(
        path.is_absolute(),
        "BROKER_ENV_FILE must be an absolute path"
    );
    path
}

fn file_value(text: &str, key: &str) -> Option<String> {
    let prefix = format!("export {key}=");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .map(|value| value.trim().trim_matches('"').to_owned())
        .filter(|value| !value.is_empty())
}

fn store_value(text: &str, key: &str, value: &str) -> String {
    let prefix = format!("export {key}=");
    let mut stored = String::with_capacity(text.len() + value.len());
    for line in text.split_inclusive('\n') {
        match line.strip_prefix(&prefix) {
            Some(old) => {
                let quote = if old.trim_start().starts_with('"') {
                    "\""
                } else {
                    ""
                };
                let end = if line.ends_with('\n') { "\n" } else { "" };
                for part in [prefix.as_str(), quote, value, quote, end] {
                    stored.push_str(part);
                }
            }
            None => stored.push_str(line),
        }
    }
    stored
}

#[test]
fn env_file_lines_are_rewritten_in_place() {
    let text = "export A=\"1\"\nexport BROKER_ACCESS_TOKEN=\"old\"\nexport B=2\nexport BROKER_REFRESH_TOKEN=old\n";
    assert_eq!(file_value(text, ENV_ACCESS_TOKEN).as_deref(), Some("old"));
    assert_eq!(file_value(text, "MISSING"), None);
    let stored = store_value(text, ENV_ACCESS_TOKEN, "new");
    let stored = store_value(&stored, ENV_REFRESH_TOKEN, "next");
    assert_eq!(
        stored,
        "export A=\"1\"\nexport BROKER_ACCESS_TOKEN=\"new\"\nexport B=2\nexport BROKER_REFRESH_TOKEN=next\n"
    );
    assert_eq!(store_value("export X=1", "X", "2"), "export X=2");
}

fn configuration() -> ConfigurationOpenApi {
    let stored = std::fs::read_to_string(env_file())
        .unwrap_or_else(|error| panic!("the file named by BROKER_ENV_FILE is unreadable: {error}"));
    let configuration = ConfigurationOpenApi::from_lookup(|key| {
        let current = if key == ENV_ACCESS_TOKEN || key == ENV_REFRESH_TOKEN {
            file_value(&stored, key)
        } else {
            None
        };
        current.or_else(|| std::env::var(key).ok())
    })
    .unwrap_or_else(|error| {
        panic!("live tests need the BROKER_* credentials in the environment: {error}")
    });
    assert_eq!(
        configuration.environment(),
        Environment::Demo,
        "live tests run against the demo environment only"
    );
    configuration
}

#[tokio::test]
async fn live_framed_round_trip_over_tls() {
    let _guard = LIVE.lock().await;
    let configuration = configuration();
    let tls = Tls::new().expect("tls configuration");
    let stream = tls
        .connect(configuration.host(), configuration.port())
        .await
        .expect("connect to the demo host");
    let mut framed = Framed::new(stream);
    let request = ProtoOaApplicationAuthReq {
        payload_type: None,
        client_id: configuration.client_id().as_str().to_owned(),
        client_secret: configuration.client_secret().expose().to_owned(),
    };
    framed
        .send(&ProtoMessage {
            payload_type: 2100,
            payload: Some(request.encode_to_vec()),
            client_msg_id: Some("round-trip".to_owned()),
        })
        .await
        .expect("send");
    let reply = timeout(STEP, framed.receive())
        .await
        .expect("reply in time")
        .expect("reply");
    println!(
        "live round trip: payload type {} id {:?} body {} bytes",
        reply.payload_type,
        reply.client_msg_id,
        reply.payload.as_ref().map_or(0, Vec::len)
    );
    assert_eq!(reply.payload_type, 2101);
    assert_eq!(reply.client_msg_id.as_deref(), Some("round-trip"));
    ProtoOaApplicationAuthRes::decode(reply.payload.unwrap_or_default().as_slice())
        .expect("application authorisation response");
    framed.close().await;
}

async fn open(configuration: &ConfigurationOpenApi) -> Connection {
    let tls = Tls::new().expect("tls configuration");
    Connection::open(
        tls,
        configuration.host(),
        configuration.port(),
        ConnectionOptions::from(configuration),
    )
    .await
    .expect("connect to the demo host")
}

#[tokio::test]
async fn live_concurrent_requests_and_heartbeats_over_an_idle_period() {
    let _guard = LIVE.lock().await;
    let configuration = configuration();
    let connection = open(&configuration).await;
    connection
        .request(&ProtoOaApplicationAuthReq {
            payload_type: None,
            client_id: configuration.client_id().as_str().to_owned(),
            client_secret: configuration.client_secret().expose().to_owned(),
        })
        .await
        .expect("application authorisation");
    let version = ProtoOaVersionReq::default();
    let accounts = ProtoOaGetAccountListByAccessTokenReq {
        payload_type: None,
        access_token: configuration.access_token().expose().to_owned(),
    };
    let (v1, a1, v2, a2, v3, a3) = tokio::join!(
        connection.request(&version),
        connection.request(&accounts),
        connection.request(&version),
        connection.request(&accounts),
        connection.request(&version),
        connection.request(&accounts),
    );
    let versions: Vec<String> = [v1, v2, v3]
        .into_iter()
        .map(|response| response.expect("version").version)
        .collect();
    let counts: Vec<usize> = [a1, a2, a3]
        .into_iter()
        .map(|response| response.expect("account list").ctid_trader_account.len())
        .collect();
    println!("live concurrent requests: versions {versions:?} account counts {counts:?}");
    assert!(versions.iter().all(|text| !text.is_empty()));
    assert!(counts.iter().all(|count| *count == counts[0] && *count > 0));
    let idle = Duration::from_secs(33);
    tokio::time::sleep(idle).await;
    let stats = connection.stats();
    let after = timeout(STEP, connection.request(&version))
        .await
        .expect("reply in time")
        .expect("request after idling");
    println!(
        "live idle {idle:?}: heartbeats sent {} received {} then version {}",
        stats.heartbeats_sent, stats.heartbeats_received, after.version
    );
    assert!(stats.heartbeats_sent >= 3);
    connection.shutdown().await;
}

fn code(outcome: Result<(), ConnectorError>) -> Option<ErrorCode> {
    match outcome {
        Err(ConnectorError::Server(error)) => Some(error.code),
        _ => None,
    }
}

async fn demo_account(client: &OpenApiClient) -> Account {
    let accounts = client.accounts().await.expect("account list");
    let demo: Vec<&Account> = accounts.iter().filter(|a| !a.is_live).collect();
    println!(
        "live accounts: {} in total, {} demo",
        accounts.len(),
        demo.len()
    );
    **demo
        .first()
        .expect("the access token grants at least one demo account")
}

#[tokio::test]
async fn live_application_and_demo_account_are_authorised() {
    let _guard = LIVE.lock().await;
    let client = OpenApiClient::connect(configuration())
        .await
        .expect("connect to the demo host");
    client
        .authorise_application()
        .await
        .expect("application authorisation");
    let account = demo_account(&client).await;
    client
        .authorise_account(account.id)
        .await
        .expect("account authorisation");
    println!(
        "live authorisation: application and account {} authorised",
        account.id
    );
    client.shutdown().await;
}

#[tokio::test]
async fn live_bad_secret_yields_the_typed_error() {
    let _guard = LIVE.lock().await;
    let good = configuration();
    let bad = ConfigurationOpenApi::demo()
        .host(good.host())
        .port(good.port())
        .client_id(good.client_id().as_str())
        .client_secret("not-the-secret-of-this-application")
        .access_token(good.access_token().expose())
        .build()
        .expect("configuration");
    let client = OpenApiClient::connect(bad)
        .await
        .expect("connect to the demo host");
    let refused = code(client.authorise_application().await);
    println!("live bad secret: {refused:?}");
    assert_eq!(refused, Some(ErrorCode::ChClientAuthFailure));
    client.shutdown().await;
}

fn writable_token_file() -> (PathBuf, String) {
    let path = env_file();
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("refusing to refresh: the token file is unreadable: {error}")
    });
    for key in [ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN] {
        assert!(
            file_value(&text, key).is_some(),
            "refusing to refresh: the token file has no `export {key}=` line"
        );
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap_or_else(|error| {
            panic!("refusing to refresh: the token file is not writable: {error}")
        });
    (path, text)
}

#[tokio::test]
async fn live_refresh_rotates_the_pair_and_the_new_token_authorises() {
    let _guard = LIVE.lock().await;
    let (path, text) = writable_token_file();
    let before = configuration();
    let client = OpenApiClient::connect(before.clone())
        .await
        .expect("connect to the demo host");
    client
        .authorise_application()
        .await
        .expect("application authorisation");
    let pair = match client.refresh_token().await {
        Ok(pair) => pair,
        Err(error) => panic!("live refresh failed, the stored pair is unchanged: {error}"),
    };
    let stored = store_value(&text, ENV_ACCESS_TOKEN, pair.access.expose());
    let stored = store_value(&stored, ENV_REFRESH_TOKEN, pair.refresh.expose());
    std::fs::write(&path, stored)
        .unwrap_or_else(|error| panic!("the new token pair could not be stored: {error}"));
    let after = configuration();
    let persisted =
        after.access_token() == &pair.access && after.refresh_token() == Some(&pair.refresh);
    let rotated =
        before.access_token() != &pair.access && before.refresh_token() != Some(&pair.refresh);
    client.shutdown().await;
    let fresh = OpenApiClient::connect(after)
        .await
        .expect("connect to the demo host");
    fresh
        .authorise_application()
        .await
        .expect("application authorisation");
    let account = demo_account(&fresh).await;
    let authorised = fresh.authorise_account(account.id).await;
    println!(
        "live refresh: token type {} expires in {} s, rotated {rotated}, persisted {persisted}, account {} with the new token: {}",
        pair.token_type,
        pair.expires_in.as_secs(),
        account.id,
        authorised
            .as_ref()
            .map_or_else(ToString::to_string, |()| "authorised".to_owned())
    );
    assert!(persisted, "the stored pair differs from the pair received");
    assert!(pair.expires_in > Duration::from_secs(60));
    assert!(
        authorised.is_ok(),
        "the new token did not authorise the account"
    );
    fresh.shutdown().await;
}

#[tokio::test]
async fn live_refresh_over_http_with_an_unknown_token_is_refused() {
    let _guard = LIVE.lock().await;
    let good = configuration();
    let unknown = ConfigurationOpenApi::demo()
        .host(good.host())
        .port(good.port())
        .client_id(good.client_id().as_str())
        .client_secret(good.client_secret().expose())
        .access_token(good.access_token().expose())
        .refresh_token("not-a-refresh-token-of-this-application")
        .token_url(good.token_url())
        .build()
        .expect("configuration");
    let client = OpenApiClient::connect(unknown)
        .await
        .expect("connect to the demo host");
    let outcome = client.refresh_token_over_http().await.map(|_| ());
    println!("live refresh over HTTP with an unknown token: {outcome:?}");
    assert!(
        matches!(
            outcome,
            Err(ConnectorError::Token(
                TokenError::Refused { .. } | TokenError::Status(_)
            ))
        ),
        "{outcome:?}"
    );
    assert!(client.access_token() == *good.access_token());
    client.shutdown().await;
}

async fn next_spot(events: &mut broadcast::Receiver<Event>, symbol: SymbolId) -> ProtoOaSpotEvent {
    timeout(STEP, async {
        loop {
            let event = events.recv().await.expect("event");
            if let Some(spot) = event.decode::<ProtoOaSpotEvent>() {
                if spot.symbol_id == symbol.get() {
                    return spot;
                }
            }
        }
    })
    .await
    .expect("spot event in time")
}

#[tokio::test]
async fn live_forced_disconnect_recovers_authorisation_and_spot_subscription() {
    let _guard = LIVE.lock().await;
    let client = OpenApiClient::connect(configuration())
        .await
        .expect("connect to the demo host");
    client
        .authorise_application()
        .await
        .expect("application authorisation");
    let account = demo_account(&client).await;
    client
        .authorise_account(account.id)
        .await
        .expect("account authorisation");
    let symbol = SymbolId::new(1).expect("symbol id");
    let mut events = client.events();
    client
        .subscribe_spots(account.id, &[symbol])
        .await
        .expect("spot subscription");
    let before = next_spot(&mut events, symbol).await;
    client
        .connection()
        .force_reconnect()
        .await
        .expect("forced reconnection");
    let mut disconnected = false;
    let mut failures = Vec::new();
    let attempt = timeout(Duration::from_secs(90), async {
        loop {
            match events.recv().await.expect("event") {
                Event::Disconnected(_) => disconnected = true,
                Event::ReconnectFailed { reason, .. } => failures.push(reason.to_string()),
                Event::ReplayFailed { key, error } => failures.push(format!("{key:?}: {error}")),
                Event::Reconnected { attempt } => break attempt,
                _ => {}
            }
        }
    })
    .await
    .expect("reconnection in time");
    let after = next_spot(&mut events, symbol).await;
    let version = client.version().await.expect("version after reconnection");
    let reconnects = client.connection().stats().reconnects;
    println!(
        "live reconnect: disconnected {disconnected}, reconnected on attempt {attempt}, failures {failures:?}, reconnects {reconnects}, spot for symbol {symbol} before {} and after {}, version {version}",
        before.bid.is_some() || before.ask.is_some(),
        after.bid.is_some() || after.ask.is_some()
    );
    assert!(disconnected);
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(reconnects, 1);
    client.shutdown().await;
}
