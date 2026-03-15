//! Verification against the demo environment.
//!
//! The tests need the `BROKER_*` credentials in the process environment and fail when they are
//! absent. They share one lock so that only one of them talks to the server at a time.

use std::time::Duration;

use ctrader_connector::common::config::{ConfigurationOpenApi, Environment};
use ctrader_connector::common::errors::{ConnectorError, ErrorCode};
use ctrader_connector::open_api::client::{Account, OpenApiClient};
use ctrader_connector::open_api::connection::{Connection, ConnectionOptions};
use ctrader_connector::open_api::messages::{
    ProtoMessage, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes,
    ProtoOaGetAccountListByAccessTokenReq, ProtoOaVersionReq,
};
use ctrader_connector::open_api::transport::{Connect, Framed, Tls};
use prost::Message;
use tokio::sync::Mutex;
use tokio::time::timeout;

static LIVE: Mutex<()> = Mutex::const_new(());

const STEP: Duration = Duration::from_secs(20);

fn configuration() -> ConfigurationOpenApi {
    let configuration = ConfigurationOpenApi::from_env().unwrap_or_else(|error| {
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
