//! Verification of the FIX sessions against the demo server, read back through the Open API.
//!
//! The tests need the variables of the FIX sessions and the `BROKER_*` credentials of the
//! Open API in the environment, and run one at a time. Both interfaces reach the same demo
//! account: the tests pick the account whose login is the user name of the FIX sessions.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use ctrader_connector::common::config::{
    ConfigurationFix, ConfigurationOpenApi, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN, Environment,
};
use ctrader_connector::common::errors::ConnectorError;
use ctrader_connector::common::ids::{AccountId, SymbolId};
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::session::SessionKind;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::symbols::SymbolsParams;
use tokio::sync::Mutex;
use tokio::time::timeout;

static LIVE: Mutex<()> = Mutex::const_new(());

const STEP: Duration = Duration::from_secs(20);
const HEARTBEAT: Duration = Duration::from_secs(5);
const TRADED: &str = "BTCUSD";
const DETAILS_CHUNK: usize = 200;

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

fn open_api_configuration() -> ConfigurationOpenApi {
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

fn fix_configuration() -> ConfigurationFix {
    let read = ConfigurationFix::from_env().expect("variables of the FIX sessions");
    assert!(read.encrypted());
    ConfigurationFix::builder()
        .host(read.host())
        .quote_port(read.quote_port())
        .trade_port(read.trade_port())
        .sender_comp_id(read.sender_comp_id())
        .target_comp_id(read.target_comp_id())
        .username(read.username())
        .password(read.password().expose())
        .heartbeat(HEARTBEAT)
        .build()
        .expect("configuration")
}

async fn session(kind: SessionKind) -> FixClient {
    timeout(STEP, FixClient::connect(fix_configuration(), kind))
        .await
        .expect("logon in time")
        .expect("logon")
}

async fn leave(client: &FixClient) {
    assert_eq!(client.logout().await, Ok(()));
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
}

async fn shared_account(client: &OpenApiClient) -> Result<Option<AccountId>, ConnectorError> {
    let login = fix_configuration().username().parse::<i64>().ok();
    let accounts = client.accounts().await?;
    Ok(accounts
        .iter()
        .find(|account| !account.is_live && login.is_some() && account.login == login)
        .map(|account| account.id))
}

async fn reader() -> Result<(OpenApiClient, AccountId), ConnectorError> {
    let client = OpenApiClient::connect(open_api_configuration()).await?;
    let authorised = async {
        client.authorise_application().await?;
        let account = shared_account(&client)
            .await?
            .ok_or(ConnectorError::Closed)?;
        client.authorise_account(account).await?;
        Ok(account)
    }
    .await;
    match authorised {
        Ok(account) => Ok((client, account)),
        Err(error) => {
            client.shutdown().await;
            Err(error)
        }
    }
}

#[tokio::test]
async fn live_security_list_matches_the_symbols_of_the_same_account() {
    let _guard = LIVE.lock().await;
    let (open, account) = reader()
        .await
        .expect("the demo account of the FIX sessions");
    let listed = open
        .symbols(SymbolsParams::new(account))
        .await
        .expect("symbols");
    let ids: Vec<SymbolId> = listed.symbols.iter().map(|symbol| symbol.id).collect();
    let mut digits = BTreeMap::new();
    for chunk in ids.chunks(DETAILS_CHUNK) {
        let details = timeout(STEP, open.symbol_details(account, chunk))
            .await
            .expect("details in time")
            .expect("symbol details");
        digits.extend(details.into_iter().map(|symbol| (symbol.id, symbol.digits)));
    }
    open.shutdown().await;
    let expected: BTreeMap<SymbolId, (String, u32)> = listed
        .symbols
        .iter()
        .map(|symbol| {
            let name = symbol.name.clone().expect("name of the symbol");
            let digits = *digits.get(&symbol.id).expect("digits of the symbol");
            (symbol.id, (name, digits))
        })
        .collect();

    let quote = session(SessionKind::Quote).await;
    let securities = timeout(STEP, quote.securities())
        .await
        .expect("security list in time")
        .expect("security list");
    leave(&quote).await;
    let found: BTreeMap<SymbolId, (String, u32)> = securities
        .iter()
        .map(|security| (security.id, (security.name.clone(), security.digits)))
        .collect();
    assert_eq!(found.len(), securities.len(), "a symbol is listed twice");
    let traded = securities
        .iter()
        .find(|security| security.name == TRADED)
        .expect("the traded symbol");
    println!(
        "live security list: {} symbols over FIX, {} enabled and not archived over the Open API; {TRADED} is {} with {} digits",
        found.len(),
        expected.len(),
        traded.id,
        traded.digits
    );
    assert_eq!(found, expected);
}
