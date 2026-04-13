//! Tests of the orders of the trade session against a scripted server.

mod faulty;
mod fix_support;

use std::time::Duration;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::errors::{ConnectorError, FixError, Interruption, TransportError};
use ctrader_connector::common::ids::{OrderId, PositionId, SymbolId};
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::{
    BusinessReject, ExecType, Logout, Message, NewOrderSingle, OrderKind, OrderStatus, Reject,
    Side, TestRequest, TimeInForce,
};
use ctrader_connector::fix::session::{Event, SessionKind};
use ctrader_connector::fix::trading::NewOrder;
use faulty::Faulty;
use fix_support::{Peer, Server, session, wire};

fn symbol() -> SymbolId {
    SymbolId::new(22_395).expect("symbol")
}

fn price(text: &str) -> Price {
    text.parse().expect("price")
}

fn volume(text: &str) -> Volume {
    text.parse().expect("volume")
}

async fn order(peer: &mut Peer) -> (u64, NewOrderSingle) {
    loop {
        let (header, message) = peer.request().await;
        match message {
            Message::NewOrderSingle(order) => return (header.seq, order),
            Message::TestRequest(_) => {}
            other => panic!("expected a new order, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn limit_order_is_sent_with_its_terms_and_answered_by_a_typed_report() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let before = Timestamp::now();
    let placing = NewOrder::limit(symbol(), Side::Buy, volume("0.01"), price("20000"))
        .label("run-1")
        .position(PositionId::new(77).expect("position"));
    let (report, sent) = tokio::join!(client.place(placing), async {
        let (_, sent) = order(&mut peer).await;
        let id = &sent.cl_ord_id;
        peer.send(wire(
            "8",
            "11=other|14=0|37=1|38=0.01|39=8|54=1|55=22395|150=8",
        ))
        .await;
        peer.send(wire(
            "8",
            &format!(
                "11={id}|14=0|37=328438181|38=0.01|39=0|40=2|44=20000|54=1|55=22395|59=1|60=20261112-21:30:37.223|150=0|151=0.01|494=run-1|721=302711675"
            ),
        ))
        .await;
        sent
    });
    assert!(
        sent.transact_time
            >= before
                .checked_sub(std::time::Duration::from_secs(1))
                .expect("moment")
    );
    assert_eq!(
        sent,
        NewOrderSingle {
            cl_ord_id: sent.cl_ord_id.clone(),
            symbol: symbol(),
            side: Side::Buy,
            transact_time: sent.transact_time,
            order_qty: volume("0.01"),
            ord_type: OrderKind::Limit,
            price: Some(price("20000")),
            stop_px: None,
            time_in_force: None,
            expire_time: None,
            position_id: Some(PositionId::new(77).expect("position")),
            designation: Some("run-1".to_owned()),
        }
    );
    let report = report.expect("report");
    assert_eq!(report.order_id, OrderId::new(328_438_181).expect("order"));
    assert_eq!(report.cl_ord_id, sent.cl_ord_id);
    assert_eq!(report.exec_type, ExecType::New);
    assert_eq!(report.status, OrderStatus::New);
    assert_eq!(report.symbol, symbol());
    assert_eq!(report.side, Side::Buy);
    assert_eq!(report.quantity, volume("0.01"));
    assert_eq!(report.kind, Some(OrderKind::Limit));
    assert_eq!(report.price, Some(price("20000")));
    assert_eq!(report.stop_price, None);
    assert_eq!(report.time_in_force, Some(TimeInForce::GoodTillCancel));
    assert_eq!(report.expiry, None);
    assert_eq!(report.filled, Some(volume("0")));
    assert_eq!(report.remaining, Some(volume("0.01")));
    assert_eq!(report.last_quantity, None);
    assert_eq!(report.average_price, None);
    assert_eq!(
        report.position,
        Some(PositionId::new(302_711_675).expect("position"))
    );
    assert_eq!(report.label.as_deref(), Some("run-1"));
    assert!(report.time.is_some());
    assert_eq!(report.text, None);
}

#[tokio::test]
async fn stop_order_carries_its_expiry_and_a_market_order_waits_for_its_fill() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let expiry = Timestamp::from_millis(1_794_605_434_000).expect("moment");
    let placing =
        NewOrder::stop(symbol(), Side::Sell, volume("0.01"), price("200000")).expires(expiry);
    assert_eq!(placing.kind(), OrderKind::Stop);
    let (report, sent) = tokio::join!(client.place(placing), async {
        let (_, sent) = order(&mut peer).await;
        let id = &sent.cl_ord_id;
        peer.send(wire(
            "8",
            &format!(
                "11={id}|14=0|37=5|38=0.01|39=0|40=3|54=2|55=22395|59=6|99=200000|126=20261113-21:30:34.000|150=0|151=0.01"
            ),
        ))
        .await;
        sent
    });
    assert_eq!(sent.ord_type, OrderKind::Stop);
    assert_eq!(sent.price, None);
    assert_eq!(sent.stop_px, Some(price("200000")));
    assert_eq!(sent.time_in_force, Some(TimeInForce::GoodTillDate));
    assert_eq!(sent.expire_time, Some(expiry));
    let report = report.expect("report");
    assert_eq!(report.stop_price, Some(price("200000")));
    assert_eq!(report.time_in_force, Some(TimeInForce::GoodTillDate));
    assert_eq!(report.expiry, Some(expiry));

    let placing = NewOrder::market(symbol(), Side::Buy, volume("0.01"));
    let (report, sent) = tokio::join!(client.place(placing), async {
        let (_, sent) = order(&mut peer).await;
        let id = &sent.cl_ord_id;
        peer.send(wire(
            "8",
            &format!("11={id}|14=0|37=6|38=0.01|39=0|40=1|54=1|55=22395|59=3|150=0|151=0.01|721=9"),
        ))
        .await;
        peer.send(wire(
            "8",
            &format!(
                "6=71583.1|11={id}|14=0.01|32=0.01|37=6|38=0.01|39=2|40=1|54=1|55=22395|59=3|150=F|151=0|721=9"
            ),
        ))
        .await;
        sent
    });
    assert_eq!(sent.ord_type, OrderKind::Market);
    assert_eq!(
        (sent.price, sent.stop_px, sent.time_in_force),
        (None, None, None)
    );
    let report = report.expect("report");
    assert_eq!(report.exec_type, ExecType::Trade);
    assert_eq!(report.status, OrderStatus::Filled);
    assert_eq!(report.filled, Some(volume("0.01")));
    assert_eq!(report.remaining, Some(volume("0")));
    assert_eq!(report.last_quantity, Some(volume("0.01")));
    assert_eq!(report.average_price, Some(price("71583.1")));
    assert_eq!(report.position, Some(PositionId::new(9).expect("position")));
}

#[tokio::test]
async fn refused_orders_are_typed_errors() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let server = tokio::spawn(async move {
        let (_, sent) = order(&mut peer).await;
        peer.send(BusinessReject {
            ref_seq_num: None,
            ref_msg_type: Some("D".to_owned()),
            ref_id: Some(sent.cl_ord_id),
            reason: Some(0),
            text: Some("TRADING_BAD_VOLUME:Order volume is too small".to_owned()),
        })
        .await;
        let (seq, _) = order(&mut peer).await;
        peer.send(Reject {
            ref_seq_num: seq,
            text: Some("Required tag missing".to_owned()),
            ref_tag_id: Some(44),
            ref_msg_type: Some("D".to_owned()),
            reason: Some(1),
        })
        .await;
        let (_, sent) = order(&mut peer).await;
        let id = sent.cl_ord_id;
        peer.send(wire(
            "8",
            &format!("11={id}|37=7|38=0.01|39=0|40=1|54=1|55=22395|150=0"),
        ))
        .await;
        peer.send(wire(
            "8",
            &format!(
                "11={id}|37=7|38=0.01|39=8|40=1|54=1|55=22395|58=NOT_ENOUGH_MONEY|103=99|150=8"
            ),
        ))
        .await;
        let (_, sent) = order(&mut peer).await;
        let id = sent.cl_ord_id;
        peer.send(wire("8", &format!("11={id}|37=8|39=0|150=0")))
            .await;
        peer
    });
    let limit = || NewOrder::limit(symbol(), Side::Buy, volume("0.01"), price("1"));
    let market = || NewOrder::market(symbol(), Side::Buy, volume("0.01"));
    assert!(matches!(
        client.place(limit()).await,
        Err(ConnectorError::Fix(FixError::BusinessRejected { reason: Some(0), text: Some(text), .. }))
            if text.starts_with("TRADING_BAD_VOLUME")
    ));
    assert!(matches!(
        client.place(limit()).await,
        Err(ConnectorError::Fix(FixError::Rejected {
            tag: Some(44),
            reason: Some(1),
            ..
        }))
    ));
    let refused = client.place(market()).await.expect_err("refusal");
    assert!(
        matches!(
            &refused,
            ConnectorError::Fix(FixError::OrderRejected { cl_ord_id: Some(_), reason: Some(99), text: Some(text) })
                if text == "NOT_ENOUGH_MONEY"
        ),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains("NOT_ENOUGH_MONEY"),
        "{refused}"
    );
    assert_eq!(
        client.place(limit()).await,
        Err(ConnectorError::Fix(FixError::MissingField {
            msg_type: "8".to_owned(),
            tag: 55,
        }))
    );
    drop(server.await.expect("server"));
}

#[tokio::test]
async fn order_without_an_answer_has_an_unknown_outcome_and_is_sent_once() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let market = || NewOrder::market(symbol(), Side::Buy, volume("0.01"));
    let (outcome, id) = tokio::join!(client.place(market()), async {
        let (_, sent) = order(&mut peer).await;
        let id = sent.cl_ord_id;
        peer.send(wire(
            "8",
            &format!("11={id}|14=0|37=6|38=0.01|39=0|40=1|54=1|55=22395|150=0"),
        ))
        .await;
        id
    });
    assert!(matches!(
        outcome,
        Err(ConnectorError::OutcomeUnknown(Interruption::Timeout(_)))
    ));
    let (outcome, ()) = tokio::join!(client.place(market()), async {
        let (_, sent) = order(&mut peer).await;
        assert_ne!(sent.cl_ord_id, id);
        drop(peer);
    });
    assert_eq!(
        outcome,
        Err(ConnectorError::OutcomeUnknown(Interruption::Transport(
            TransportError::Closed
        )))
    );
}

async fn session_through(faulty: &Faulty) -> (Server, FixClient, Peer) {
    let server = Server::bind().await;
    let configuration = server
        .builder()
        .reconnect_delay(Duration::from_millis(300))
        .build()
        .expect("configuration");
    let (client, peer) = tokio::join!(
        FixClient::connect_with(configuration, SessionKind::Trade, faulty.clone()),
        async {
            let mut peer = server.accept().await;
            peer.logon().await;
            peer
        }
    );
    (server, client.expect("logon"), peer)
}

#[tokio::test]
async fn order_whose_write_fails_has_an_unknown_outcome_and_is_not_sent_again() {
    let faulty = Faulty::default();
    let (server, client, mut old) = session_through(&faulty).await;
    let mut events = client.events();
    faulty.fail_writes(true);
    let outcome = client
        .place(NewOrder::market(symbol(), Side::Buy, volume("0.01")))
        .await;
    assert_eq!(
        outcome,
        Err(ConnectorError::OutcomeUnknown(Interruption::Transport(
            Faulty::error()
        )))
    );
    assert_eq!(
        events.recv().await,
        Ok(Event::Disconnected(Faulty::error()))
    );
    faulty.fail_writes(false);
    let mut fresh = server.accept().await;
    fresh.logon().await;
    assert_eq!(events.recv().await, Ok(Event::Reconnected { attempt: 1 }));
    let (left, ()) = tokio::join!(client.logout(), async {
        let (_, message) = fresh.request().await;
        assert_eq!(message, Logout { text: None }.into());
        fresh.send(Logout { text: None }).await;
    });
    assert_eq!(left, Ok(()));
    while let Some(frame) = old.frame().await {
        assert_eq!(frame.header.msg_type, "0", "{frame:?}");
    }
}

#[tokio::test]
async fn request_that_may_repeat_reports_the_failed_write_as_a_transport_error() {
    let faulty = Faulty::default();
    let (_server, client, _peer) = session_through(&faulty).await;
    faulty.fail_writes(true);
    let probe = TestRequest {
        test_req_id: "probe".to_owned(),
    };
    assert_eq!(
        client
            .request(probe.into(), "probe", |_| Some(Ok(())))
            .await,
        Err(ConnectorError::Transport(Faulty::error()))
    );
}
