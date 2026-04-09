//! Tests of cancelling and replacing orders of the trade session against a scripted server.

mod fix_support;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::errors::{ConnectorError, FixError, Interruption, RefusedChange};
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::fix::amend::Replacement;
use ctrader_connector::fix::messages::{
    BusinessReject, CancelRejectResponse, ExecType, Message, OrderCancelReject,
    OrderCancelReplaceRequest, OrderCancelRequest, OrderStatus, Reject,
};
use ctrader_connector::fix::session::SessionKind;
use fix_support::{Peer, session, wire};

fn price(text: &str) -> Price {
    text.parse().expect("price")
}

fn volume(text: &str) -> Volume {
    text.parse().expect("volume")
}

async fn cancellation(peer: &mut Peer) -> (u64, OrderCancelRequest) {
    let (header, message) = peer.request().await;
    let Message::OrderCancelRequest(request) = message else {
        panic!("expected a cancel request, got {message:?}");
    };
    (header.seq, request)
}

async fn replacement(peer: &mut Peer) -> (u64, OrderCancelReplaceRequest) {
    let (header, message) = peer.request().await;
    let Message::OrderCancelReplaceRequest(request) = message else {
        panic!("expected a replace request, got {message:?}");
    };
    (header.seq, request)
}

#[tokio::test]
async fn cancel_names_the_order_and_returns_the_report_of_the_cancellation() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let (report, sent) = tokio::join!(client.cancel("o1"), async {
        let (_, sent) = cancellation(&mut peer).await;
        let id = &sent.cl_ord_id;
        peer.send(wire(
            "8",
            "11=other|37=4|38=0.01|39=4|41=o9|54=1|55=22395|150=4",
        ))
        .await;
        peer.send(wire(
            "8",
            &format!(
                "11={id}|14=0|37=5|38=0.01|39=4|40=2|41=o1|44=20000|54=1|55=22395|150=4|151=0"
            ),
        ))
        .await;
        sent
    });
    assert_eq!(
        sent,
        OrderCancelRequest {
            orig_cl_ord_id: "o1".to_owned(),
            order_id: None,
            cl_ord_id: sent.cl_ord_id.clone(),
        }
    );
    assert_ne!(sent.cl_ord_id, "o1");
    let report = report.expect("report");
    assert_eq!(report.cl_ord_id, sent.cl_ord_id);
    assert_eq!(report.exec_type, ExecType::Canceled);
    assert_eq!(report.status, OrderStatus::Canceled);
    assert_eq!(report.order_id.get(), 5);
}

#[tokio::test]
async fn replace_states_the_new_terms_and_returns_the_order_as_replaced() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let expiry = Timestamp::from_millis(1_794_605_434_000).expect("moment");
    let terms = Replacement::new(volume("0.02")).price(price("21000"));
    let (report, sent) = tokio::join!(client.replace("o1", terms), async {
        let (_, sent) = replacement(&mut peer).await;
        let id = &sent.cl_ord_id;
        peer.send(wire(
            "8",
            &format!("11={id}|14=0|37=5|38=0.02|39=0|40=2|44=21000|54=1|55=22395|150=5|151=0.02"),
        ))
        .await;
        sent
    });
    assert_eq!(
        sent,
        OrderCancelReplaceRequest {
            orig_cl_ord_id: "o1".to_owned(),
            order_id: None,
            cl_ord_id: sent.cl_ord_id.clone(),
            order_qty: volume("0.02"),
            price: Some(price("21000")),
            stop_px: None,
            expire_time: None,
        }
    );
    let report = report.expect("report");
    assert_eq!(report.cl_ord_id, sent.cl_ord_id);
    assert_eq!(report.exec_type, ExecType::Replaced);
    assert_eq!(report.quantity, volume("0.02"));
    assert_eq!(report.price, Some(price("21000")));

    let terms = Replacement::new(volume("0.01"))
        .stop_price(price("190000"))
        .expires(expiry);
    let (outcome, sent) = tokio::join!(client.replace("o2", terms), async {
        replacement(&mut peer).await.1
    });
    assert_eq!(
        (sent.price, sent.stop_px, sent.expire_time),
        (None, Some(price("190000")), Some(expiry))
    );
    assert!(matches!(
        outcome,
        Err(ConnectorError::OutcomeUnknown(Interruption::Timeout(_)))
    ));
}

async fn unknown(peer: &mut Peer, msg_type: &str, id: String) {
    peer.send(BusinessReject {
        ref_seq_num: None,
        ref_msg_type: Some(msg_type.to_owned()),
        ref_id: Some(id),
        reason: Some(0),
        text: Some("ORDER_NOT_FOUND:Order not found or not belong to trader".to_owned()),
    })
    .await;
}

#[tokio::test]
async fn refused_changes_are_typed_cancel_rejects_whatever_message_carries_them() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let server = tokio::spawn(async move {
        let (_, sent) = cancellation(&mut peer).await;
        peer.send(OrderCancelReject {
            order_id: None,
            cl_ord_id: sent.cl_ord_id,
            orig_cl_ord_id: Some("o1".to_owned()),
            ord_status: OrderStatus::Filled,
            response_to: CancelRejectResponse::Cancel,
            text: Some("the order is filled".to_owned()),
        })
        .await;
        let (_, sent) = cancellation(&mut peer).await;
        unknown(&mut peer, "F", sent.cl_ord_id).await;
        let (_, sent) = replacement(&mut peer).await;
        unknown(&mut peer, "G", sent.cl_ord_id).await;
        let (_, sent) = replacement(&mut peer).await;
        let id = sent.cl_ord_id;
        peer.send(wire(
            "8",
            &format!("11={id}|37=5|38=0.01|39=0|54=1|55=22395|150=I"),
        ))
        .await;
        peer.send(wire(
            "8",
            &format!("11={id}|37=5|38=0.01|39=8|54=1|55=22395|58=TRADING_BAD_VOLUME|150=8"),
        ))
        .await;
        let (seq, _) = cancellation(&mut peer).await;
        peer.send(Reject {
            ref_seq_num: seq,
            text: Some("Tag not defined for this message type".to_owned()),
            ref_tag_id: Some(37),
            ref_msg_type: Some("F".to_owned()),
            reason: Some(2),
        })
        .await;
        cancellation(&mut peer).await;
        peer
    });
    let refused = client.cancel("o1").await.expect_err("refusal");
    assert!(
        matches!(
            &refused,
            ConnectorError::Fix(FixError::CancelRejected {
                orig_cl_ord_id: Some(orig),
                change: RefusedChange::Cancel,
                text: Some(text),
                ..
            }) if orig == "o1" && text == "the order is filled"
        ),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains("the order is filled"),
        "{refused}"
    );
    let missing = |response: RefusedChange, outcome: Result<_, ConnectorError>| {
        assert!(
            matches!(
                &outcome,
                Err(ConnectorError::Fix(FixError::CancelRejected {
                    orig_cl_ord_id: Some(orig),
                    change,
                    text: Some(text),
                    ..
                })) if orig == "o1" && *change == response && text.starts_with("ORDER_NOT_FOUND")
            ),
            "{outcome:?}"
        );
    };
    missing(RefusedChange::Cancel, client.cancel("o1").await);
    let terms = Replacement::new(volume("0.01")).price(price("1"));
    missing(RefusedChange::Replace, client.replace("o1", terms).await);
    assert!(matches!(
        client.replace("o1", terms).await,
        Err(ConnectorError::Fix(FixError::OrderRejected { text: Some(text), .. })) if text == "TRADING_BAD_VOLUME"
    ));
    assert!(matches!(
        client.cancel("o1").await,
        Err(ConnectorError::Fix(FixError::Rejected {
            tag: Some(37),
            reason: Some(2),
            ..
        }))
    ));
    assert!(matches!(
        client.cancel("o1").await,
        Err(ConnectorError::Timeout(_))
    ));
    drop(server.await.expect("server"));
}
