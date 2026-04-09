//! Tests of the reports of orders and positions of the trade session against a scripted server.

mod fix_support;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::errors::{ConnectorError, FixError};
use ctrader_connector::fix::messages::{
    BusinessReject, ExecType, Message, OrderMassStatusRequest, OrderStatus, OrderStatusRequest,
    RequestForPositions, Side,
};
use ctrader_connector::fix::session::SessionKind;
use fix_support::{Peer, session, wire};

fn price(text: &str) -> Price {
    text.parse().expect("price")
}

fn volume(text: &str) -> Volume {
    text.parse().expect("volume")
}

async fn mass(peer: &mut Peer) -> String {
    let (_, message) = peer.request().await;
    let Message::OrderMassStatusRequest(request) = message else {
        panic!("expected a mass status request, got {message:?}");
    };
    assert_eq!(
        request,
        OrderMassStatusRequest {
            mass_status_req_id: request.mass_status_req_id.clone(),
            request_type: 7,
            issue_date: None,
        }
    );
    request.mass_status_req_id
}

async fn asked(peer: &mut Peer) -> String {
    let (_, message) = peer.request().await;
    let Message::RequestForPositions(request) = message else {
        panic!("expected a request for positions, got {message:?}");
    };
    assert_eq!(
        request,
        RequestForPositions {
            pos_req_id: request.pos_req_id.clone(),
            position_id: None,
        }
    );
    request.pos_req_id
}

#[tokio::test]
async fn order_status_is_asked_by_the_identifier_of_the_client_alone() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let (report, sent) = tokio::join!(client.order_status("o1"), async {
        let (_, sent) = peer.request().await;
        peer.send(wire("8", "11=o2|37=6|38=0.01|39=0|54=1|55=22395|150=I"))
            .await;
        peer.send(wire(
            "8",
            "11=o1|14=0|37=5|38=0.01|39=0|40=2|44=20000|54=1|55=22395|59=1|150=I|151=0.01|494=run|721=9",
        ))
        .await;
        sent
    });
    assert_eq!(
        sent,
        OrderStatusRequest {
            cl_ord_id: "o1".to_owned(),
            side: None,
        }
        .into()
    );
    let report = report.expect("report");
    assert_eq!(report.cl_ord_id, "o1");
    assert_eq!(report.order_id.get(), 5);
    assert_eq!(report.exec_type, ExecType::OrderStatus);
    assert_eq!(report.status, OrderStatus::New);
    assert_eq!(report.price, Some(price("20000")));

    let (outcome, ()) = tokio::join!(client.order_status("o7"), async {
        peer.request().await;
        peer.send(BusinessReject {
            ref_seq_num: None,
            ref_msg_type: Some("H".to_owned()),
            ref_id: Some("o7".to_owned()),
            reason: Some(0),
            text: Some("ORDER_NOT_FOUND:Order not found or not belong to trader".to_owned()),
        })
        .await;
    });
    assert!(matches!(
        outcome,
        Err(ConnectorError::Fix(FixError::BusinessRejected { ref_id: Some(id), .. })) if id == "o7"
    ));
}

#[tokio::test]
async fn open_orders_are_listed_whole_or_not_at_all() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let server = tokio::spawn(async move {
        let id = mass(&mut peer).await;
        peer.send(wire("8", "11=o0|37=4|38=0.01|39=0|54=1|55=22395|150=0"))
            .await;
        peer.send(wire(
            "8",
            "11=o0|37=4|38=0.01|39=0|54=1|55=22395|150=I|584=other|911=1",
        ))
        .await;
        peer.send(wire(
            "8",
            &format!("11=o1|37=5|38=0.01|39=0|40=2|44=20000|54=1|55=22395|150=I|584={id}|911=2"),
        ))
        .await;
        peer.send(wire(
            "8",
            &format!("11=o2|37=6|38=0.02|39=0|40=3|99=200000|54=2|55=22395|150=I|584={id}|911=2"),
        ))
        .await;
        let id = mass(&mut peer).await;
        peer.send(BusinessReject {
            ref_seq_num: None,
            ref_msg_type: Some("AF".to_owned()),
            ref_id: Some(id),
            reason: Some(0),
            text: Some("ORDER_NOT_FOUND:no orders found".to_owned()),
        })
        .await;
        let id = mass(&mut peer).await;
        peer.send(BusinessReject {
            ref_seq_num: None,
            ref_msg_type: Some("AF".to_owned()),
            ref_id: Some(id),
            reason: Some(4),
            text: Some("Application not available".to_owned()),
        })
        .await;
        let id = mass(&mut peer).await;
        peer.send(wire(
            "8",
            &format!("11=o1|37=5|38=0.01|39=0|54=1|55=22395|150=I|584={id}|911=2"),
        ))
        .await;
        peer.send(wire("8", &format!("11=o2|37=6|39=0|150=I|584={id}|911=2")))
            .await;
        let id = mass(&mut peer).await;
        peer.send(wire(
            "8",
            &format!("11=o1|37=5|38=0.01|39=0|54=1|55=22395|150=I|584={id}|911=2"),
        ))
        .await;
        peer
    });
    let orders = client.open_orders().await.expect("orders");
    let seen: Vec<(&str, i64, Side, Volume)> = orders
        .iter()
        .map(|order| {
            (
                order.cl_ord_id.as_str(),
                order.order_id.get(),
                order.side,
                order.quantity,
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            ("o1", 5, Side::Buy, volume("0.01")),
            ("o2", 6, Side::Sell, volume("0.02"))
        ]
    );
    assert_eq!(client.open_orders().await, Ok(Vec::new()));
    assert!(matches!(
        client.open_orders().await,
        Err(ConnectorError::Fix(FixError::BusinessRejected {
            reason: Some(4),
            ..
        }))
    ));
    assert_eq!(
        client.open_orders().await,
        Err(ConnectorError::Fix(FixError::MissingField {
            msg_type: "8".to_owned(),
            tag: 55,
        }))
    );
    assert!(matches!(
        client.open_orders().await,
        Err(ConnectorError::Timeout(_))
    ));
    drop(server.await.expect("server"));
}

#[tokio::test]
async fn positions_are_listed_whole_or_not_at_all() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let server = tokio::spawn(async move {
        let id = asked(&mut peer).await;
        peer.send(wire(
            "AP",
            "55=1|710=other|721=1|727=1|728=0|730=1.1|702=1|704=1000|705=0",
        ))
        .await;
        peer.send(wire(
            "AP",
            &format!("55=22395|710={id}|721=8|727=2|728=0|730=71583.1|702=1|704=0.01|705=0"),
        ))
        .await;
        peer.send(wire(
            "AP",
            &format!("55=1|710={id}|721=9|727=2|728=0|730=1.1|702=1|704=0|705=1000"),
        ))
        .await;
        let id = asked(&mut peer).await;
        peer.send(wire("AP", &format!("710={id}|727=0|728=2")))
            .await;
        let id = asked(&mut peer).await;
        peer.send(wire("AP", &format!("710={id}|727=1|728=1")))
            .await;
        let id = asked(&mut peer).await;
        peer.send(wire(
            "AP",
            &format!("55=1|710={id}|721=9|727=1|728=0|702=1|704=0|705=0"),
        ))
        .await;
        let id = asked(&mut peer).await;
        peer.send(wire(
            "AP",
            &format!("55=1|710={id}|727=1|728=0|702=1|704=1|705=0"),
        ))
        .await;
        let id = asked(&mut peer).await;
        peer.send(wire(
            "AP",
            &format!("710={id}|721=9|727=1|728=0|702=1|704=1|705=0"),
        ))
        .await;
        peer
    });
    let positions = client.positions().await.expect("positions");
    let seen: Vec<(i64, i64, Side, Volume, Option<Price>)> = positions
        .iter()
        .map(|position| {
            (
                position.id.get(),
                position.symbol.get(),
                position.side,
                position.quantity,
                position.entry_price,
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            (8, 22_395, Side::Buy, volume("0.01"), Some(price("71583.1"))),
            (9, 1, Side::Sell, volume("1000"), Some(price("1.1")))
        ]
    );
    assert_eq!(client.positions().await, Ok(Vec::new()));
    assert!(matches!(
        client.positions().await,
        Err(ConnectorError::Fix(FixError::BusinessRejected {
            reason: Some(1),
            ..
        }))
    ));
    let lacking = |tag| {
        Err(ConnectorError::Fix(FixError::MissingField {
            msg_type: "AP".to_owned(),
            tag,
        }))
    };
    assert_eq!(client.positions().await, lacking(704));
    assert_eq!(client.positions().await, lacking(721));
    assert_eq!(client.positions().await, lacking(55));
    drop(server.await.expect("server"));
}
