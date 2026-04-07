//! Tests of the FIX sessions against the demo server.
//!
//! They need the variables of the FIX sessions in the environment and run one at a time.

use std::time::Duration;

use ctrader_connector::common::config::ConfigurationFix;
use ctrader_connector::common::errors::ConnectorError;
use ctrader_connector::common::ids::SymbolId;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::{Message, SecurityListRequest};
use ctrader_connector::fix::session::{Event, SessionKind};
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};

static LIVE: Mutex<()> = Mutex::const_new(());

const STEP: Duration = Duration::from_secs(20);
const HEARTBEAT: Duration = Duration::from_secs(5);

fn configuration() -> ConfigurationFix {
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
    timeout(STEP, FixClient::connect(configuration(), kind))
        .await
        .expect("logon in time")
        .expect("logon")
}

async fn leave(client: &FixClient) {
    assert_eq!(client.logout().await, Ok(()));
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
    assert_eq!(client.logout().await, Err(ConnectorError::Closed));
}

#[tokio::test]
async fn live_sessions_log_on_stay_up_across_heartbeats_and_log_out() {
    let _guard = LIVE.lock().await;
    for kind in [SessionKind::Quote, SessionKind::Trade] {
        let client = session(kind).await;
        let mut events = client.events();
        let started = Instant::now();
        let watch = HEARTBEAT * 2 + HEARTBEAT / 2;
        let mut beats = Vec::new();
        while let Ok(event) = timeout(watch.saturating_sub(started.elapsed()), events.recv()).await
        {
            match event.expect("event") {
                Event::Message(inbound) => {
                    assert!(
                        matches!(inbound.message, Message::Heartbeat(_)),
                        "{:?}",
                        inbound.message
                    );
                    beats.push((inbound.seq, started.elapsed().as_millis()));
                }
                other => panic!("the session did not stay up: {other:?}"),
            }
        }
        assert!(beats.len() >= 2, "{beats:?}");
        assert!(
            beats
                .iter()
                .map(|(seq, _)| *seq)
                .eq((2_u64..).take(beats.len())),
            "{beats:?}"
        );
        println!(
            "{} session up for {:?} with a heartbeat of {HEARTBEAT:?}; heartbeats of the server (sequence number, milliseconds): {beats:?}",
            kind.as_str(),
            started.elapsed()
        );
        leave(&client).await;
    }
}

const QUOTED: &str = "BTCUSD";

async fn symbol_named(client: &FixClient, name: &str) -> SymbolId {
    let id = client.next_id("list");
    let request = SecurityListRequest {
        security_req_id: id.clone(),
        request_type: 0,
        symbol: None,
    };
    let list = client
        .request(request.into(), &id, |message| match message {
            Message::SecurityList(list) if list.security_req_id == id => Some(Ok(list.clone())),
            _ => None,
        })
        .await
        .expect("security list");
    list.symbols
        .iter()
        .find(|listed| listed.name.as_deref() == Some(name))
        .unwrap_or_else(|| panic!("the server lists no symbol named {name}"))
        .symbol
}

#[tokio::test]
async fn live_spot_prices_arrive_as_snapshots_until_unsubscribed() {
    let _guard = LIVE.lock().await;
    let client = session(SessionKind::Quote).await;
    let symbol = symbol_named(&client, QUOTED).await;
    let mut events = client.events();
    let mut spots = timeout(STEP, client.subscribe_spot(symbol))
        .await
        .expect("first prices in time")
        .expect("spot subscription");
    let first = spots.last();
    assert_eq!(first.symbol, symbol);
    let mut later = Vec::new();
    while later.len() < 3 {
        let quote = timeout(Duration::from_secs(60), spots.next())
            .await
            .unwrap_or_else(|_| {
                panic!("{QUOTED} sent no further prices in 60 s; the market may be closed")
            })
            .expect("prices");
        assert_eq!(quote.symbol, symbol);
        let (bid, offer) = (quote.bid.expect("bid"), quote.offer.expect("offer"));
        assert!(bid <= offer, "bid above offer: {quote:?}");
        later.push(quote);
    }
    let mut snapshots = 0_usize;
    let mut incrementals = 0_usize;
    while let Ok(event) = events.try_recv() {
        match event {
            Event::Message(inbound) => match inbound.message {
                Message::MarketDataSnapshot(_) => snapshots += 1,
                Message::MarketDataIncremental(_) => incrementals += 1,
                _ => {}
            },
            other => panic!("unexpected event: {other:?}"),
        }
    }
    spots.unsubscribe().await.expect("end of subscription");
    let quiet = Duration::from_secs(8);
    let settle = Duration::from_secs(2);
    let ended = Instant::now();
    let mut late = 0_usize;
    while let Ok(event) = timeout(quiet.saturating_sub(ended.elapsed()), events.recv()).await {
        if let Event::Message(inbound) = event.expect("event") {
            if matches!(inbound.message, Message::MarketDataSnapshot(_)) && ended.elapsed() > settle
            {
                late += 1;
            }
        }
    }
    println!(
        "live spot: {QUOTED} is symbol {symbol}; first bid {:?} offer {:?}; {} later reports; {snapshots} snapshots and {incrementals} incremental messages while subscribed; {late} price messages later than {settle:?} after unsubscribing, watched for {quiet:?}",
        first.bid.map(|price| price.to_string()),
        first.offer.map(|price| price.to_string()),
        later.len()
    );
    assert!(snapshots >= 4, "{snapshots}");
    assert_eq!(incrementals, 0);
    assert_eq!(late, 0);
    leave(&client).await;
}

#[tokio::test]
async fn live_depth_fills_a_book_by_changes_until_unsubscribed() {
    let _guard = LIVE.lock().await;
    let client = session(SessionKind::Quote).await;
    let symbol = symbol_named(&client, QUOTED).await;
    let mut events = client.events();
    let mut depth = timeout(STEP, client.subscribe_depth(symbol))
        .await
        .expect("first book in time")
        .expect("depth subscription");
    let first = depth.book().len();
    let window = Instant::now() + Duration::from_secs(60);
    let mut changes = 0_usize;
    while changes < 5 || depth.book().bids().is_empty() || depth.book().offers().is_empty() {
        tokio::time::timeout_at(window, depth.next())
            .await
            .unwrap_or_else(|_| panic!("{QUOTED} sent no five changes with both sides in 60 s ({changes} received); the market may be closed"))
            .expect("book");
        changes += 1;
    }
    let book = depth.book().clone();
    let (bids, offers) = (book.bids(), book.offers());
    let best_bid = bids.first().expect("best bid");
    let best_offer = offers.first().expect("best offer");
    assert!(
        best_bid.price < best_offer.price,
        "{best_bid:?} {best_offer:?}"
    );
    assert!(
        bids.windows(2)
            .all(|pair| matches!(pair, [above, below] if above.price >= below.price))
    );
    assert!(
        offers
            .windows(2)
            .all(|pair| matches!(pair, [below, above] if below.price <= above.price))
    );
    assert!(
        book.bids()
            .iter()
            .chain(&book.offers())
            .all(|entry| entry.size.is_some())
    );
    let mut snapshots = 0_usize;
    let mut incrementals = 0_usize;
    while let Ok(event) = events.try_recv() {
        if let Event::Message(inbound) = event {
            match inbound.message {
                Message::MarketDataSnapshot(_) => snapshots += 1,
                Message::MarketDataIncremental(_) => incrementals += 1,
                _ => {}
            }
        }
    }
    depth.unsubscribe().await.expect("end of subscription");
    let quiet = Duration::from_secs(8);
    let settle = Duration::from_secs(2);
    let ended = Instant::now();
    let mut late = 0_usize;
    while let Ok(event) = timeout(quiet.saturating_sub(ended.elapsed()), events.recv()).await {
        if let Event::Message(inbound) = event.expect("event") {
            if matches!(inbound.message, Message::MarketDataIncremental(_))
                && ended.elapsed() > settle
            {
                late += 1;
            }
        }
    }
    println!(
        "live depth: {QUOTED} is symbol {symbol}; first book of {first} entries; after {changes} changes {} bids and {} offers, best bid {} below best offer {}; {snapshots} snapshots and {incrementals} incremental messages while subscribed; {late} changes later than {settle:?} after unsubscribing, watched for {quiet:?}",
        bids.len(),
        offers.len(),
        best_bid.price,
        best_offer.price
    );
    assert_eq!(first, 0);
    assert_eq!(snapshots, 1);
    assert!(incrementals >= 5, "{incrementals}");
    assert_eq!(late, 0);
    leave(&client).await;
}
