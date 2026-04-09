//! Verification of the FIX sessions against the demo server, read back through the Open API.
//!
//! The tests need the variables of the FIX sessions and the `BROKER_*` credentials of the
//! Open API in the environment, and run one at a time. Both interfaces reach the same demo
//! account: the tests pick the account whose login is the user name of the FIX sessions.
//! Whatever a test creates carries a label of its own and is swept away afterwards over a
//! fresh connection, also when the test fails.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use ctrader_connector::common::config::{
    ConfigurationFix, ConfigurationOpenApi, ENV_ACCESS_TOKEN, ENV_REFRESH_TOKEN, Environment,
};
use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::errors::{ConnectorError, FixError, RefusedChange};
use ctrader_connector::common::ids::{AccountId, OrderId, SymbolId};
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::fix::amend::Replacement;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::{ExecType, OrderKind, OrderStatus, Side};
use ctrader_connector::fix::session::SessionKind;
use ctrader_connector::fix::trading::NewOrder;
use ctrader_connector::open_api::client::OpenApiClient;
use ctrader_connector::open_api::symbols::{Symbol, SymbolsParams};
use ctrader_connector::open_api::trading::{Order, OrderType, Position, TimeInForce, TradeSide};
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tokio::time::timeout;

static LIVE: Mutex<()> = Mutex::const_new(());

const STEP: Duration = Duration::from_secs(20);
const HEARTBEAT: Duration = Duration::from_secs(5);
const TRADED: &str = "BTCUSD";
const SWEEP_ROUNDS: usize = 10;
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

#[derive(Clone)]
struct Desk {
    open: OpenApiClient,
    account: AccountId,
    symbol: Symbol,
    label: String,
}

impl Desk {
    async fn own(&self) -> (Vec<Position>, Vec<Order>) {
        let reconciled = self.open.reconcile(self.account).await.expect("reconcile");
        let label = Some(self.label.as_str());
        let positions = reconciled
            .positions
            .into_iter()
            .filter(|position| position.label.as_deref() == label)
            .collect();
        let orders = reconciled
            .orders
            .into_iter()
            .filter(|order| order.label.as_deref() == label)
            .collect();
        (positions, orders)
    }
}

async fn sweep_once(
    client: &OpenApiClient,
    account: AccountId,
    label: &str,
) -> Result<usize, ConnectorError> {
    let reconciled = client.reconcile(account).await?;
    let own = Some(label);
    let orders: Vec<&Order> = reconciled
        .orders
        .iter()
        .filter(|order| order.label.as_deref() == own)
        .collect();
    let positions: Vec<&Position> = reconciled
        .positions
        .iter()
        .filter(|position| position.label.as_deref() == own)
        .collect();
    for order in orders.iter().filter(|order| !order.closing) {
        let outcome = client.cancel_order(account, order.id).await;
        println!(
            "live sweep: cancel order {} answered {:?}",
            order.id,
            outcome.map(|execution| execution.kind)
        );
    }
    for position in &positions {
        let Some(symbol) = client
            .symbol_details(account, &[position.symbol])
            .await?
            .pop()
        else {
            continue;
        };
        let outcome = client
            .close_position(account, position, position.volume, &symbol)
            .await;
        println!(
            "live sweep: close position {} answered {:?}",
            position.id,
            outcome.map(|execution| execution.kind)
        );
    }
    Ok(orders.len() + positions.len())
}

async fn sweep(label: &str) -> Result<(), String> {
    let mut last = "the sweep did not run".to_owned();
    let mut link: Option<(OpenApiClient, AccountId)> = None;
    for round in 0..SWEEP_ROUNDS {
        if round > 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let (client, account) = match link.take() {
            Some(open) => open,
            None => match reader().await {
                Ok(open) => open,
                Err(error) => {
                    last = format!("no fresh connection for the sweep: {error}");
                    continue;
                }
            },
        };
        match sweep_once(&client, account, label).await {
            Ok(0) => {
                client.shutdown().await;
                return Ok(());
            }
            Ok(left) => {
                last = format!("{left} orders or positions labelled {label} were still listed");
                link = Some((client, account));
            }
            Err(error) => {
                last = format!("the sweep of label {label} failed: {error}");
                client.shutdown().await;
            }
        }
    }
    if let Some((client, _)) = link {
        client.shutdown().await;
    }
    Err(last)
}

fn panic_text(failure: tokio::task::JoinError) -> String {
    let Ok(payload) = failure.try_into_panic() else {
        return "the test was cancelled".to_owned();
    };
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .unwrap_or_else(|| "the test panicked".to_owned())
}

async fn trading<B, F>(item: &str, body: B)
where
    B: FnOnce(Desk) -> F,
    F: Future<Output = ()> + Send + 'static,
{
    let _guard = LIVE.lock().await;
    let (open, account) = reader()
        .await
        .expect("the demo account of the FIX sessions");
    let list = open
        .symbols(SymbolsParams::new(account))
        .await
        .expect("symbols");
    let traded = list
        .symbols
        .iter()
        .find(|symbol| symbol.name.as_deref() == Some(TRADED))
        .unwrap_or_else(|| panic!("the demo account lists no symbol named {TRADED}"))
        .id;
    let symbol = open
        .symbol_details(account, &[traded])
        .await
        .expect("symbol details")
        .pop()
        .expect("details of the traded symbol");
    let desk = Desk {
        open,
        account,
        symbol,
        label: format!("{item}-{}", Timestamp::now().as_millis()),
    };
    let outcome = tokio::spawn(body(desk.clone())).await;
    desk.open.shutdown().await;
    let swept = sweep(&desk.label).await;
    match (outcome, swept) {
        (Ok(()), swept) => assert_eq!(swept, Ok(()), "the test left orders or positions behind"),
        (Err(failure), Ok(())) => panic!("{}", panic_text(failure)),
        (Err(failure), Err(left)) => panic!(
            "{}; and the sweep after the failure did not leave the account clean: {left}",
            panic_text(failure)
        ),
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

async fn bid(symbol: SymbolId) -> Price {
    let quote = session(SessionKind::Quote).await;
    let spots = timeout(STEP, quote.subscribe_spot(symbol))
        .await
        .expect("prices in time")
        .expect("spot subscription");
    let bid = spots.last().bid.expect("bid");
    spots.unsubscribe().await.expect("end of subscription");
    leave(&quote).await;
    bid
}

fn scaled(price: Price, by: Decimal) -> Price {
    Price::new((price.value() * by).round_dp(0)).expect("price")
}

fn whole_seconds_from_now(seconds: i64) -> Timestamp {
    let second = Timestamp::now().as_millis() / 1_000 + seconds;
    Timestamp::from_millis(second * 1_000).expect("moment")
}

async fn places_waiting_orders(desk: &Desk, trade: &FixClient, bid: Price) -> [OrderId; 2] {
    let symbol = desk.symbol.id;
    let quantity = desk
        .symbol
        .min_volume
        .expect("the traded symbol states its smallest volume");
    let below = scaled(bid, Decimal::new(5, 1));
    let limit = timeout(
        STEP,
        trade.place(NewOrder::limit(symbol, Side::Buy, quantity, below).label(&desk.label)),
    )
    .await
    .expect("report in time")
    .expect("limit order");
    assert_eq!(
        (limit.exec_type, limit.status),
        (ExecType::New, OrderStatus::New)
    );
    assert_eq!(
        (limit.symbol, limit.side, limit.quantity),
        (symbol, Side::Buy, quantity)
    );
    assert_eq!(
        (limit.kind, limit.price),
        (Some(OrderKind::Limit), Some(below))
    );
    assert_eq!(limit.label.as_deref(), Some(desk.label.as_str()));

    let above = scaled(bid, Decimal::TWO);
    let expiry = whole_seconds_from_now(3_600);
    let stop = timeout(
        STEP,
        trade.place(
            NewOrder::stop(symbol, Side::Buy, quantity, above)
                .label(&desk.label)
                .expires(expiry),
        ),
    )
    .await
    .expect("report in time")
    .expect("stop order");
    assert_eq!(
        (stop.exec_type, stop.status),
        (ExecType::New, OrderStatus::New)
    );
    assert_eq!(
        (stop.kind, stop.stop_price),
        (Some(OrderKind::Stop), Some(above))
    );
    assert_eq!(stop.expiry, Some(expiry));

    let (positions, orders) = desk.own().await;
    assert!(positions.is_empty(), "{positions:?}");
    assert_eq!(orders.len(), 2, "{orders:?}");
    let read = |id| {
        orders
            .iter()
            .find(|order| order.id == id)
            .expect("the order is listed")
    };
    let waiting = read(limit.order_id);
    assert_eq!((waiting.symbol, waiting.side), (symbol, TradeSide::Buy));
    assert_eq!((waiting.kind, waiting.volume), (OrderType::Limit, quantity));
    assert_eq!(waiting.limit_price, Some(below));
    let stopped = read(stop.order_id);
    assert_eq!((stopped.symbol, stopped.side), (symbol, TradeSide::Buy));
    assert_eq!((stopped.kind, stopped.volume), (OrderType::Stop, quantity));
    assert_eq!(stopped.stop_price, Some(above));
    assert_eq!(stopped.time_in_force, TimeInForce::GoodTillDate(expiry));
    println!(
        "live FIX orders: quantity {quantity} of {TRADED} (lot size {:?}) sent as OrderQty and read back as volume {} and {}; limit {} at {below} and stop {} at {above} until {} are listed by reconcile with the same side, kind, volume and price",
        desk.symbol.lot_size.map(|lot| lot.to_string()),
        waiting.volume,
        stopped.volume,
        limit.order_id,
        stop.order_id,
        expiry.as_millis()
    );

    [limit.order_id, stop.order_id]
}

async fn fills_and_closes_a_position(desk: &Desk, trade: &FixClient) {
    let symbol = desk.symbol.id;
    let quantity = desk
        .symbol
        .min_volume
        .expect("the traded symbol states its smallest volume");
    let bought = timeout(
        STEP,
        trade.place(NewOrder::market(symbol, Side::Buy, quantity).label(&desk.label)),
    )
    .await
    .expect("report in time")
    .expect("market order");
    assert_eq!(
        (bought.exec_type, bought.status),
        (ExecType::Trade, OrderStatus::Filled)
    );
    assert_eq!(
        (bought.kind, bought.side),
        (Some(OrderKind::Market), Side::Buy)
    );
    assert_eq!(
        (bought.filled, bought.last_quantity),
        (Some(quantity), Some(quantity))
    );
    let opened = bought.position.expect("position of the fill");
    let (positions, _) = desk.own().await;
    assert_eq!(positions.len(), 1, "{positions:?}");
    let held = positions.first().expect("position");
    assert_eq!((held.id, held.symbol), (opened, symbol));
    assert_eq!((held.side, held.volume), (TradeSide::Buy, quantity));
    assert_eq!(held.price, bought.average_price);

    let sold = timeout(
        STEP,
        trade.place(
            NewOrder::market(symbol, Side::Sell, quantity)
                .label(&desk.label)
                .position(opened),
        ),
    )
    .await
    .expect("report in time")
    .expect("closing order");
    assert_eq!(
        (sold.exec_type, sold.status),
        (ExecType::Trade, OrderStatus::Filled)
    );
    assert_eq!(sold.position, Some(opened));
    let (positions, orders) = desk.own().await;
    assert!(positions.is_empty(), "{positions:?}");
    assert_eq!(orders.len(), 2, "{orders:?}");
    println!(
        "live FIX market order: order {} filled {quantity} at {:?} and opened position {opened}, read back as a buy position of volume {} at {:?}; order {} sold it at {:?} and reconcile lists the position no longer",
        bought.order_id,
        bought.average_price.map(|price| price.to_string()),
        held.volume,
        held.price.map(|price| price.to_string()),
        sold.order_id,
        sold.average_price.map(|price| price.to_string())
    );
}

#[tokio::test]
async fn live_orders_placed_over_fix_are_read_back_through_the_open_api() {
    trading("fix33", |desk| async move {
        let bid = bid(desk.symbol.id).await;
        let trade = session(SessionKind::Trade).await;
        let waiting = places_waiting_orders(&desk, &trade, bid).await;
        fills_and_closes_a_position(&desk, &trade).await;
        for order in waiting {
            desk.open
                .cancel_order(desk.account, order)
                .await
                .expect("cancellation");
        }
        let (positions, orders) = desk.own().await;
        assert!(
            positions.is_empty() && orders.is_empty(),
            "{positions:?} {orders:?}"
        );
        leave(&trade).await;
    })
    .await;
}

#[tokio::test]
async fn live_replace_and_cancel_over_fix_are_read_back_through_the_open_api() {
    trading("fix34", |desk| async move {
        let symbol = desk.symbol.id;
        let quantity = desk
            .symbol
            .min_volume
            .expect("the traded symbol states its smallest volume");
        let doubled = Volume::new(quantity.value() * Decimal::TWO).expect("volume");
        let bid = bid(symbol).await;
        let trade = session(SessionKind::Trade).await;
        let first = scaled(bid, Decimal::new(5, 1));
        let second = scaled(bid, Decimal::new(4, 1));
        let placed = timeout(
            STEP,
            trade.place(NewOrder::limit(symbol, Side::Buy, quantity, first).label(&desk.label)),
        )
        .await
        .expect("report in time")
        .expect("limit order");

        let replaced = timeout(
            STEP,
            trade.replace(&placed.cl_ord_id, Replacement::new(doubled).price(second)),
        )
        .await
        .expect("report in time")
        .expect("replacement");
        assert_eq!(replaced.exec_type, ExecType::Replaced);
        assert_eq!(replaced.order_id, placed.order_id);
        assert_eq!((replaced.quantity, replaced.price), (doubled, Some(second)));
        let (_, orders) = desk.own().await;
        assert_eq!(orders.len(), 1, "{orders:?}");
        let read = orders.first().expect("order");
        assert_eq!((read.id, read.kind), (placed.order_id, OrderType::Limit));
        assert_eq!((read.volume, read.limit_price), (doubled, Some(second)));

        let refused = timeout(STEP, trade.cancel(&replaced.cl_ord_id))
            .await
            .expect("answer in time")
            .expect_err("refusal");
        assert!(
            matches!(
                &refused,
                ConnectorError::Fix(FixError::CancelRejected {
                    change: RefusedChange::Cancel,
                    ..
                })
            ),
            "{refused:?}"
        );
        let (_, orders) = desk.own().await;
        assert_eq!(orders.len(), 1, "{orders:?}");

        let cancelled = timeout(STEP, trade.cancel(&placed.cl_ord_id))
            .await
            .expect("report in time")
            .expect("cancellation");
        assert_eq!(
            (cancelled.exec_type, cancelled.status),
            (ExecType::Canceled, OrderStatus::Canceled)
        );
        assert_eq!(cancelled.order_id, placed.order_id);
        let (positions, orders) = desk.own().await;
        assert!(positions.is_empty() && orders.is_empty(), "{positions:?} {orders:?}");
        println!(
            "live FIX change: order {} placed as {} for {quantity} at {first}, replaced under {} and read back by reconcile with volume {} at {:?}; cancel under the identifier of the replacement refused with {refused}; cancel under the first identifier reported {:?} and reconcile lists the order no longer",
            placed.order_id,
            placed.cl_ord_id,
            replaced.cl_ord_id,
            read.volume,
            read.limit_price.map(|price| price.to_string()),
            cancelled.status
        );
        leave(&trade).await;
    })
    .await;
}

#[tokio::test]
async fn live_reports_of_orders_and_positions_match_what_the_test_created() {
    trading("fix35", |desk| async move {
        let symbol = desk.symbol.id;
        let quantity = desk
            .symbol
            .min_volume
            .expect("the traded symbol states its smallest volume");
        let bid = bid(symbol).await;
        let trade = session(SessionKind::Trade).await;
        let below = scaled(bid, Decimal::new(5, 1));
        let place = |order: NewOrder| async {
            timeout(STEP, trade.place(order.label(&desk.label)))
                .await
                .expect("report in time")
                .expect("order")
        };
        let limit = place(NewOrder::limit(symbol, Side::Buy, quantity, below)).await;
        let stop = place(NewOrder::stop(symbol, Side::Sell, quantity, below)).await;
        let bought = place(NewOrder::market(symbol, Side::Buy, quantity)).await;
        let opened = bought.position.expect("position of the fill");

        let listed = timeout(STEP, trade.open_orders())
            .await
            .expect("orders in time")
            .expect("orders");
        let own: Vec<_> = listed
            .iter()
            .filter(|order| [limit.order_id, stop.order_id].contains(&order.order_id))
            .collect();
        assert_eq!(own.len(), 2, "{listed:?}");
        for (placed, kind) in [(&limit, OrderKind::Limit), (&stop, OrderKind::Stop)] {
            let report = own
                .iter()
                .find(|order| order.order_id == placed.order_id)
                .expect("the order is listed");
            assert_eq!(report.exec_type, ExecType::OrderStatus);
            assert_eq!((report.symbol, report.side), (symbol, placed.side));
            assert_eq!((report.quantity, report.kind), (quantity, Some(kind)));
            assert_eq!((report.price, report.stop_price), (placed.price, placed.stop_price));
        }

        let status = timeout(STEP, trade.order_status(&limit.cl_ord_id))
            .await
            .expect("status in time")
            .expect("status");
        assert_eq!((status.order_id, status.status), (limit.order_id, OrderStatus::New));
        assert_eq!((status.side, status.quantity), (Side::Buy, quantity));
        assert_eq!((status.kind, status.price), (Some(OrderKind::Limit), Some(below)));

        let held = timeout(STEP, trade.positions())
            .await
            .expect("positions in time")
            .expect("positions");
        let position = held
            .iter()
            .find(|position| position.id == opened)
            .unwrap_or_else(|| panic!("position {opened} is not among {held:?}"));
        assert_eq!((position.symbol, position.side), (symbol, Side::Buy));
        assert_eq!(position.quantity, quantity);
        assert_eq!(position.entry_price, bought.average_price);

        let (positions, orders) = desk.own().await;
        assert_eq!((positions.len(), orders.len()), (1, 2), "{positions:?} {orders:?}");
        println!(
            "live FIX reports: mass status listed {} orders, among them limit {} and stop {} with side, kind, quantity and price as placed; status of {} is {:?}; positions listed {} with position {opened} bought {} at {:?}; reconcile lists the same 2 orders and 1 position",
            listed.len(),
            limit.order_id,
            stop.order_id,
            limit.cl_ord_id,
            status.status,
            held.len(),
            position.quantity,
            position.entry_price.map(|price| price.to_string())
        );

        place(NewOrder::market(symbol, Side::Sell, quantity).position(opened)).await;
        for order in [&limit, &stop] {
            timeout(STEP, trade.cancel(&order.cl_ord_id))
                .await
                .expect("report in time")
                .expect("cancellation");
        }
        let none = timeout(STEP, trade.positions())
            .await
            .expect("positions in time")
            .expect("positions");
        assert!(none.iter().all(|position| position.id != opened), "{none:?}");
        let (positions, orders) = desk.own().await;
        assert!(positions.is_empty() && orders.is_empty(), "{positions:?} {orders:?}");
        leave(&trade).await;
    })
    .await;
}
