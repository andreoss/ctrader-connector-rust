//! Verification against the demo environment.
//!
//! The tests need the `BROKER_*` credentials in the process environment and fail when they are
//! absent. They share one lock so that only one of them talks to the server at a time.
//!
//! A refresh replaces the token pair, so the pair is read from the file named by
//! `BROKER_ENV_FILE` and the refresh test writes the new pair back there before it checks
//! anything else.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use ctrader_connector::common::config::{
    ConfigurationOpenApi, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN, Environment,
};
use ctrader_connector::common::errors::{ConnectorError, ErrorCode, TokenError};
use ctrader_connector::common::ids::SymbolId;
use ctrader_connector::common::time::{TimeRange, Timestamp};
use ctrader_connector::open_api::client::{Account, OpenApiClient};
use ctrader_connector::open_api::connection::{Connection, ConnectionOptions, Event};
use ctrader_connector::open_api::depth::{DepthBook, DepthStream, DepthUpdate};
use ctrader_connector::open_api::messages::{
    ProtoMessage, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes, ProtoOaDepthEvent,
    ProtoOaGetAccountListByAccessTokenReq, ProtoOaGetTickDataReq, ProtoOaSpotEvent,
    ProtoOaVersionReq,
};
use ctrader_connector::open_api::quotes::QuoteSide;
use ctrader_connector::open_api::symbols::SymbolsParams;
use ctrader_connector::open_api::ticks::TickDataParams;
use ctrader_connector::open_api::transport::{Connect, Framed, Tls};
use ctrader_connector::open_api::trendbars::{Period, TrendbarsParams};
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
    let idle = Duration::from_secs(45);
    tokio::time::sleep(idle).await;
    let stats = connection.stats();
    assert!(stats.heartbeats_received >= 1);
    assert_eq!(
        stats.reconnects, 0,
        "an idle connection must outlast the period of silence allowed by default"
    );
    let after = timeout(STEP, connection.request(&version))
        .await
        .expect("reply in time")
        .expect("request after idling");
    println!(
        "live idle {idle:?}: heartbeats sent {} received {} reconnects {} then version {}",
        stats.heartbeats_sent, stats.heartbeats_received, stats.reconnects, after.version
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
    assert!(
        rotated,
        "the refresh returned a token of the pair it was asked to replace"
    );
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

#[tokio::test]
async fn live_burst_above_the_limit_completes_without_a_limit_error() {
    const BURST: usize = 120;
    let _guard = LIVE.lock().await;
    let client = OpenApiClient::connect(configuration())
        .await
        .expect("connect to the demo host");
    client
        .authorise_application()
        .await
        .expect("application authorisation");
    let started = std::time::Instant::now();
    let tasks: Vec<_> = (0..BURST)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.version().await })
        })
        .collect();
    let mut failures = Vec::new();
    let mut answered = 0_usize;
    for task in tasks {
        match task.await.expect("task") {
            Ok(_) => answered += 1,
            Err(error) => failures.push(error),
        }
    }
    let elapsed = started.elapsed();
    let stats = client.connection().stats();
    let limited = failures
        .iter()
        .filter(|error| {
            matches!(
                error,
                ConnectorError::Server(refusal) if matches!(
                    refusal.code,
                    ErrorCode::RequestFrequencyExceeded | ErrorCode::BlockedPayloadType
                )
            )
        })
        .count();
    println!(
        "live burst: {answered} of {BURST} version requests answered in {elapsed:.1?}, failures {failures:?}, limit errors {limited}, requests sent {}, throttle retries {}",
        stats.requests_sent, stats.throttle_retries
    );
    assert_eq!(limited, 0, "a limit error reached the caller: {failures:?}");
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(answered, BURST, "{answered} of {BURST} requests answered");
    let burst = u64::try_from(BURST).expect("burst size");
    assert_eq!(
        stats.requests_sent,
        burst + 1 + stats.throttle_retries,
        "every request is sent once, and once more for each retry after a limit error"
    );
    assert!(elapsed >= Duration::from_secs(2));
    client.shutdown().await;
}

async fn authorised() -> (OpenApiClient, Account) {
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
    (client, account)
}

#[tokio::test]
async fn live_accounts_assets_symbols_and_details_parse_into_typed_values() {
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let assets = client.assets(account.id).await.expect("assets");
    let list = client
        .symbols(SymbolsParams::new(account.id).include_archived(true))
        .await
        .expect("symbols");
    let named = |name: &str| {
        list.symbols
            .iter()
            .find(|symbol| symbol.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("the demo account lists no symbol named {name}"))
    };
    let light = named("EURUSD");
    let ids = [light.id, named("BTCUSD").id];
    let details = client
        .symbol_details(account.id, &ids)
        .await
        .expect("symbol details");
    let asset_name = |id| {
        assets
            .iter()
            .find(|asset| Some(asset.id) == id)
            .map(|asset| asset.name.clone())
    };
    println!(
        "live reference data: account {}, {} assets, {} symbols ({} enabled, {} archived), EURUSD id {} base {:?} quote {:?}",
        account.id,
        assets.len(),
        list.symbols.len(),
        list.symbols.iter().filter(|symbol| symbol.enabled).count(),
        list.archived.len(),
        light.id,
        asset_name(light.base_asset),
        asset_name(light.quote_asset),
    );
    for symbol in &details {
        println!(
            "live symbol details: id {} digits {} pip position {} volume min {:?} max {:?} step {:?} lot {:?} mode {:?} units {:?}",
            symbol.id,
            symbol.digits,
            symbol.pip_position,
            symbol.min_volume.map(|volume| volume.to_string()),
            symbol.max_volume.map(|volume| volume.to_string()),
            symbol.step_volume.map(|volume| volume.to_string()),
            symbol.lot_size.map(|volume| volume.to_string()),
            symbol.trading_mode,
            symbol.measurement_units,
        );
    }
    assert!(!assets.is_empty());
    assert!(list.symbols.len() > 1);
    assert_eq!(asset_name(light.base_asset).as_deref(), Some("EUR"));
    assert_eq!(asset_name(light.quote_asset).as_deref(), Some("USD"));
    assert_eq!(details.len(), 2);
    let euro = details
        .iter()
        .find(|symbol| symbol.id == light.id)
        .expect("details of EURUSD");
    assert_eq!(euro.digits, 5);
    assert_eq!(euro.pip_position, 4);
    assert!(euro.min_volume <= euro.max_volume);
    assert!(euro.min_volume.is_some() && euro.step_volume.is_some());
    client.shutdown().await;
}

const QUOTING_CANDIDATES: [&str; 3] = ["BTCUSD", "ETHUSD", "EURUSD"];

async fn candidates(client: &OpenApiClient, account: &Account) -> Vec<(SymbolId, String)> {
    let list = client
        .symbols(SymbolsParams::new(account.id))
        .await
        .expect("symbols");
    let found: Vec<(SymbolId, String)> = QUOTING_CANDIDATES
        .iter()
        .filter_map(|name| {
            list.symbols
                .iter()
                .find(|symbol| symbol.enabled && symbol.name.as_deref() == Some(*name))
                .map(|symbol| (symbol.id, (*name).to_owned()))
        })
        .collect();
    assert!(
        !found.is_empty(),
        "the demo account lists none of {QUOTING_CANDIDATES:?}"
    );
    found
}

#[tokio::test]
async fn live_quotes_arrive_while_subscribed_and_stop_after_unsubscribing() {
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let candidates = candidates(&client, &account).await;
    let ids: Vec<SymbolId> = candidates.iter().map(|(id, _)| *id).collect();
    let mut quotes = client
        .subscribe_quotes(account.id, &ids)
        .await
        .expect("spot subscription");
    let mut received = 0_usize;
    let mut sample = None;
    let window = tokio::time::Instant::now() + Duration::from_secs(60);
    while received < 5 || sample.is_none() {
        let quote = tokio::time::timeout_at(window, quotes.next())
            .await
            .unwrap_or_else(|_| {
                panic!("none of {QUOTING_CANDIDATES:?} quoted both sides five times in 60 s (received {received}); the markets may be closed")
            })
            .expect("quote");
        received += 1;
        if quote.bid.is_some() && quote.ask.is_some() {
            sample = Some(quote);
        }
    }
    let sample = sample.expect("a quote with both sides");
    let name = candidates
        .iter()
        .find(|(id, _)| *id == sample.symbol)
        .map_or("?", |(_, name)| name.as_str());
    quotes.unsubscribe().await.expect("end of subscription");
    let mut after = client.quotes(account.id, &ids);
    let quiet = Duration::from_secs(8);
    let late = timeout(quiet, after.next()).await.ok();
    println!(
        "live quotes: {received} quotes while subscribed to {ids:?}; sample {name} ({}) bid {} ask {} at {:?}; after unsubscribing {} in {quiet:?}",
        sample.symbol,
        sample
            .bid
            .map_or_else(String::new, |price| price.to_string()),
        sample
            .ask
            .map_or_else(String::new, |price| price.to_string()),
        sample.timestamp.map(Timestamp::as_millis),
        late.as_ref().map_or("none", |_| "a quote arrived"),
    );
    assert!(sample.bid <= sample.ask, "bid above ask: {sample:?}");
    assert!(sample.timestamp.is_some());
    assert!(late.is_none(), "quote after unsubscribing: {late:?}");
    client.shutdown().await;
}

#[tokio::test]
async fn live_depth_updates_rebuild_a_book_with_bids_below_asks() {
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let candidates = candidates(&client, &account).await;
    let ids: Vec<SymbolId> = candidates.iter().map(|(id, _)| *id).collect();
    let mut depth = client
        .subscribe_depth(account.id, &ids)
        .await
        .expect("depth subscription");
    let window = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut updates = 0_usize;
    let (mut added, mut deleted) = (0_usize, 0_usize);
    let symbol = loop {
        let update = tokio::time::timeout_at(window, depth.next())
            .await
            .unwrap_or_else(|_| {
                panic!("none of {QUOTING_CANDIDATES:?} sent five depth updates with both sides in 60 s (received {updates}); the markets may be closed")
            })
            .expect("depth update");
        updates += 1;
        added += update.new_quotes.len();
        deleted += update.deleted_quotes.len();
        let book = depth.book(update.symbol).expect("book of the symbol");
        if updates >= 5 && book.best_bid().is_some() && book.best_ask().is_some() {
            break update.symbol;
        }
    };
    let book = depth.book(symbol).expect("book of the symbol").clone();
    let name = candidates
        .iter()
        .find(|(id, _)| *id == symbol)
        .map_or("?", |(_, name)| name.as_str());
    let (bids, asks) = (book.bids(), book.asks());
    println!(
        "live depth: {updates} updates ({added} new, {deleted} deleted quotes); book of {name} ({symbol}) holds {} bids and {} asks; best bid {} x {} / best ask {} x {}",
        bids.len(),
        asks.len(),
        bids[0].price,
        bids[0].size,
        asks[0].price,
        asks[0].size,
    );
    assert!(book.best_bid() <= book.best_ask(), "crossed book: {book:?}");
    assert!(bids.windows(2).all(|pair| pair[0].price >= pair[1].price));
    assert!(asks.windows(2).all(|pair| pair[0].price <= pair[1].price));
    depth.resync().await.expect("fresh subscription");
    fresh_depth(&mut depth).await;
    depth.unsubscribe().await.expect("end of subscription");
    client.shutdown().await;
}

async fn fresh_depth(depth: &mut DepthStream) {
    let window = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut seen = BTreeSet::new();
    loop {
        let first = tokio::time::timeout_at(window, depth.next())
            .await
            .expect("whole depth of a symbol within 30 s of the fresh subscription")
            .expect("depth update");
        if !seen.insert(first.symbol) {
            continue;
        }
        let entries = depth.book(first.symbol).map_or(0, DepthBook::len);
        println!(
            "live depth resync: the first update of {} carries {} new and {} deleted quotes; its book holds {entries} entries",
            first.symbol,
            first.new_quotes.len(),
            first.deleted_quotes.len(),
        );
        assert!(first.deleted_quotes.is_empty());
        assert_eq!(entries, first.new_quotes.len());
        if entries > 0 {
            break;
        }
    }
}

#[tokio::test]
async fn live_trendbars_are_ordered_and_a_live_bar_arrives() {
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let now = Timestamp::now();
    let recent = TimeRange::ending_at(now, Duration::from_secs(600));
    let mut quoting = None;
    for (id, name) in candidates(&client, &account).await {
        let params = TrendbarsParams::new(account.id, id, Period::M1, recent);
        if !client
            .trendbars(&params)
            .await
            .expect("recent bars")
            .is_empty()
        {
            quoting = Some((id, name));
            break;
        }
    }
    let (symbol, name) = quoting.unwrap_or_else(|| {
        panic!("none of {QUOTING_CANDIDATES:?} has a one minute bar in the last ten minutes; the markets may be closed")
    });
    let settled = now.checked_sub(Duration::from_secs(120)).expect("moment");
    let range = TimeRange::ending_at(settled, Duration::from_secs(6 * 3_600));
    let params = TrendbarsParams::new(account.id, symbol, Period::M1, range);
    let bars = client.trendbars(&params).await.expect("history");
    assert!(bars.len() > 60, "only {} bars in six hours", bars.len());
    assert!(
        bars.windows(2)
            .all(|pair| pair[0].timestamp < pair[1].timestamp)
    );
    for bar in &bars {
        assert!(
            bar.low <= bar.open && bar.low <= bar.close,
            "low above the body: {bar:?}"
        );
        assert!(
            bar.open <= bar.high && bar.close <= bar.high,
            "high below the body: {bar:?}"
        );
        assert_eq!((bar.symbol, bar.period), (symbol, Period::M1));
        assert!(
            bar.timestamp.as_millis() + 60_000 > range.from().as_millis()
                && bar.timestamp <= range.to(),
            "bar outside the range: {bar:?}"
        );
    }
    let latest = client.trendbars(&params.count(10)).await.expect("history");
    assert_eq!(latest.as_slice(), &bars[bars.len() - 10..]);
    let (first, last) = (&bars[0], &bars[bars.len() - 1]);
    println!(
        "live trendbars: {} M1 bars of {name} ({symbol}) from {} to {}; last bar open {} high {} low {} close {} volume {}",
        bars.len(),
        first.timestamp,
        last.timestamp,
        last.open,
        last.high,
        last.low,
        last.close,
        last.volume,
    );
    let mut stream = client
        .subscribe_live_trendbars(account.id, symbol, Period::M1)
        .await
        .expect("live trendbar subscription");
    let bar = timeout(Duration::from_secs(90), stream.next())
        .await
        .unwrap_or_else(|_| panic!("no live M1 bar of {name} in 90 s; the market may be closed"))
        .expect("live bar");
    println!(
        "live trendbar: {name} ({symbol}) M1 at {} open {} high {} low {} close {} volume {}",
        bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume,
    );
    assert_eq!((bar.symbol, bar.period), (symbol, Period::M1));
    assert!(bar.low <= bar.open && bar.open <= bar.high);
    assert!(bar.low <= bar.close && bar.close <= bar.high);
    assert!(bar.timestamp >= last.timestamp);
    stream.unsubscribe().await.expect("end of subscription");
    client
        .unsubscribe_spots(account.id, &[symbol])
        .await
        .expect("end of spot subscription");
    client.shutdown().await;
}

#[tokio::test]
async fn live_tick_history_of_both_sides_falls_inside_the_range() {
    const PART_MILLIS: i64 = 1_200_000;
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let settled = Timestamp::now()
        .checked_sub(Duration::from_secs(60))
        .expect("a clock past the epoch");
    let range = TimeRange::ending_at(settled, Duration::from_secs(4 * 3_600));
    let mut evidence = None;
    for (symbol, name) in candidates(&client, &account).await {
        let mut sides = Vec::new();
        for side in [QuoteSide::Bid, QuoteSide::Ask] {
            let params = TickDataParams::new(account.id, symbol, side, range);
            let ticks = client.tick_data(&params).await.expect("tick history");
            for tick in &ticks {
                assert!(
                    range.contains(tick.timestamp),
                    "{side:?} tick outside {range:?}: {tick:?}"
                );
            }
            assert!(
                ticks
                    .windows(2)
                    .all(|pair| pair[0].timestamp <= pair[1].timestamp)
            );
            let latest = client.tick_data(&params.limit(25)).await.expect("ticks");
            assert_eq!(
                latest.as_slice(),
                &ticks[ticks.len().saturating_sub(25)..],
                "the latest ticks differ from the end of the whole range"
            );
            let mut parts = Vec::new();
            let mut first = range.from().as_millis();
            while first <= range.to().as_millis() {
                let last = (first + PART_MILLIS - 1).min(range.to().as_millis());
                let at = |millis| Timestamp::from_millis(millis).expect("timestamp");
                let part = TimeRange::new(at(first), at(last)).expect("range");
                let params = TickDataParams::new(account.id, symbol, side, part);
                parts.extend(client.tick_data(&params).await.expect("part of the ticks"));
                first = last + 1;
            }
            assert!(
                parts == ticks,
                "{side:?}: {} ticks fetched in one piece, {} fetched in parts",
                ticks.len(),
                parts.len()
            );
            sides.push((side, ticks));
        }
        if sides.iter().all(|(_, ticks)| !ticks.is_empty()) {
            evidence = Some((symbol, name, sides));
            break;
        }
    }
    let (symbol, name, sides) = evidence.unwrap_or_else(|| {
        panic!("none of {QUOTING_CANDIDATES:?} has bid and ask ticks in the last four hours; the markets may be closed")
    });
    for (side, ticks) in &sides {
        let (first, last) = (&ticks[0], &ticks[ticks.len() - 1]);
        println!(
            "live ticks: {} {side:?} ticks of {name} ({symbol}) in {}..{}; first {} at {}, last {} at {}",
            ticks.len(),
            range.from(),
            range.to(),
            first.price,
            first.timestamp,
            last.price,
            last.timestamp,
        );
    }
    let (bids, asks) = (&sides[0].1, &sides[1].1);
    assert!(bids[bids.len() - 1].price < asks[asks.len() - 1].price);
    client.shutdown().await;
}

async fn raw_ticks(
    client: &OpenApiClient,
    account: &Account,
    symbol: SymbolId,
    span: (i64, i64),
) -> (Vec<i64>, bool) {
    let request = ProtoOaGetTickDataReq {
        payload_type: None,
        ctid_trader_account_id: account.id.get(),
        symbol_id: symbol.get(),
        r#type: 1,
        from_timestamp: span.0,
        to_timestamp: span.1,
    };
    let response = client
        .connection()
        .request(&request)
        .await
        .expect("tick data");
    let mut at = 0_i64;
    let times = response
        .tick_data
        .iter()
        .map(|tick| {
            at += tick.timestamp;
            at
        })
        .collect();
    (times, response.has_more)
}

#[tokio::test]
async fn live_tick_requests_end_before_the_millisecond_named_as_their_end() {
    const MINUTE: i64 = 60_000;
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let now = Timestamp::now().as_millis();
    let mut observed = false;
    for (symbol, name) in candidates(&client, &account).await {
        let span = (now - 240 * MINUTE, now);
        let (page, more) = raw_ticks(&client, &account, symbol, span).await;
        if page.len() < 3 {
            continue;
        }
        assert!(
            page.windows(2).all(|pair| pair[0] >= pair[1]),
            "an answer does not start with its latest tick"
        );
        assert!(page.iter().all(|at| span.0 <= *at && *at < span.1));
        let probe = page[page.len() / 2];
        assert!(
            probe > page[page.len() - 1],
            "one millisecond fills half an answer"
        );
        let held = page.iter().filter(|at| **at == probe).count();
        let (short, _) = raw_ticks(&client, &account, symbol, (probe - MINUTE, probe)).await;
        assert!(
            short.iter().all(|at| *at < probe),
            "a request includes the millisecond named as its end"
        );
        let (ending, _) = raw_ticks(&client, &account, symbol, (probe - MINUTE, probe + 1)).await;
        assert_eq!(ending[..held], vec![probe; held]);
        assert_eq!(ending[held..], short);
        let (single, _) = raw_ticks(&client, &account, symbol, (probe, probe + 1)).await;
        assert_eq!(single, vec![probe; held]);
        let moment = Timestamp::from_millis(probe).expect("timestamp");
        let range = TimeRange::new(moment, moment).expect("range");
        let params = TickDataParams::new(account.id, symbol, QuoteSide::Bid, range);
        let ticks = client.tick_data(&params).await.expect("tick history");
        assert_eq!(ticks.len(), held);
        assert!(ticks.iter().all(|tick| tick.timestamp == moment));
        println!(
            "live tick bounds: {name} ({symbol}) answer of {} ticks, more announced: {more}; fromTimestamp {probe} toTimestamp {probe} + 1 returned the {held} tick(s) of that millisecond, toTimestamp {probe} none of them",
            page.len()
        );
        observed = true;
        break;
    }
    assert!(
        observed,
        "none of {QUOTING_CANDIDATES:?} has three bid ticks in the last four hours; the markets may be closed"
    );
    client.shutdown().await;
}

fn depth_ids(event: &ProtoOaDepthEvent) -> (u64, Vec<u64>, Vec<u64>) {
    (
        event.symbol_id,
        event.new_quotes.iter().map(|quote| quote.id).collect(),
        event.deleted_quotes.clone(),
    )
}

fn update_ids(update: &DepthUpdate) -> (u64, Vec<u64>, Vec<u64>) {
    (
        u64::try_from(update.symbol.get()).expect("symbol id"),
        update
            .new_quotes
            .iter()
            .map(|quote| quote.id.get())
            .collect(),
        update.deleted_quotes.iter().map(|id| id.get()).collect(),
    )
}

async fn depth_since_the_loss(
    events: &mut broadcast::Receiver<Event>,
    settle: Duration,
) -> (Vec<&'static str>, Vec<ProtoOaDepthEvent>, usize) {
    let mut order = Vec::new();
    let mut replayed: Vec<ProtoOaDepthEvent> = Vec::new();
    let mut during = 0_usize;
    timeout(Duration::from_secs(90), async {
        loop {
            match events.recv().await.expect("event") {
                Event::Disconnected(_) => {
                    order.push("disconnected");
                    replayed.clear();
                }
                Event::Reconnected { .. } => {
                    order.push("reconnected");
                    during = replayed.len();
                    break;
                }
                Event::ReconnectFailed { .. } => {
                    order.push("failed");
                    replayed.clear();
                }
                event => {
                    if let Some(event) = event.decode::<ProtoOaDepthEvent>() {
                        if order.last() != Some(&"depth") {
                            order.push("depth");
                        }
                        replayed.push(event);
                    }
                }
            }
        }
    })
    .await
    .expect("reconnection in time");
    let settled = tokio::time::Instant::now() + settle;
    while let Ok(event) = tokio::time::timeout_at(settled, events.recv()).await {
        if let Some(event) = event.expect("event").decode::<ProtoOaDepthEvent>() {
            replayed.push(event);
        }
    }
    (order, replayed, during)
}

#[tokio::test]
async fn live_depth_book_holds_the_replayed_depth_after_a_forced_reconnect() {
    const SETTLE: Duration = Duration::from_secs(5);
    let _guard = LIVE.lock().await;
    let (client, account) = authorised().await;
    let candidates = candidates(&client, &account).await;
    let ids: Vec<SymbolId> = candidates.iter().map(|(id, _)| *id).collect();
    let mut depth = client
        .subscribe_depth(account.id, &ids)
        .await
        .expect("depth subscription");
    timeout(Duration::from_secs(60), async {
        loop {
            let update = depth.next().await.expect("depth update");
            if depth
                .book(update.symbol)
                .is_some_and(|book| !book.is_empty())
            {
                break;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("none of {QUOTING_CANDIDATES:?} sent depth in 60 s"));
    let mut events = client.events();
    client
        .connection()
        .force_reconnect()
        .await
        .expect("forced reconnection");
    let (order, replayed, during) = depth_since_the_loss(&mut events, SETTLE).await;
    assert!(
        !replayed.is_empty(),
        "no depth arrived after the reconnection; the markets may be closed"
    );
    let mut expected: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
    let first = depth_ids(&replayed[0]);
    timeout(STEP, async {
        let mut update = depth.next().await.expect("depth update");
        while update_ids(&update) != first {
            update = depth.next().await.expect("depth update");
        }
        for (index, event) in replayed.iter().enumerate() {
            if index > 0 {
                update = depth.next().await.expect("depth update");
            }
            assert_eq!(update_ids(&update), depth_ids(event));
            let ids = expected.entry(event.symbol_id).or_default();
            for id in &event.deleted_quotes {
                ids.remove(id);
            }
            ids.extend(event.new_quotes.iter().map(|quote| quote.id));
            let book = depth.book(update.symbol).expect("book of the symbol");
            let held: BTreeSet<u64> = book
                .bids()
                .iter()
                .chain(book.asks().iter())
                .map(|quote| quote.id.get())
                .collect();
            assert_eq!(
                &held, ids,
                "book of {} differs from the depth sent since the loss, at event {index}",
                update.symbol
            );
        }
    })
    .await
    .expect("the stream delivers what the connection delivered");
    let entries: usize = expected.values().map(BTreeSet::len).sum();
    println!(
        "live depth reconnect: order {order:?}, {during} depth events during the replay, {} until {SETTLE:?} after it, books of {} symbols hold {entries} entries",
        replayed.len(),
        expected.len()
    );
    assert!(entries > 0, "the books are empty after the reconnection");
    depth.unsubscribe().await.expect("end of subscription");
    client.shutdown().await;
}
