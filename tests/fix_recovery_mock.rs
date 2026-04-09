//! Tests of the recovery of a FIX session against a scripted server: lost connections,
//! missing messages and messages asked for again.

mod fix_support;

use ctrader_connector::common::errors::{ConnectorError, FixError, Interruption, TransportError};
use ctrader_connector::common::ids::SymbolId;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::{
    BookSide, Heartbeat, Logout, MarketDataSnapshot, Message, ResendRequest, SequenceReset, Side,
    SnapshotEntry, TestRequest,
};
use ctrader_connector::fix::session::{Event, SessionKind};
use ctrader_connector::fix::trading::NewOrder;
use fix_support::{Peer, STEP, Server, session};
use tokio::sync::broadcast::Receiver;
use tokio::time::timeout;

fn symbol() -> SymbolId {
    SymbolId::new(7).expect("symbol")
}

fn order() -> NewOrder {
    NewOrder::limit(
        symbol(),
        Side::Buy,
        "0.01".parse().expect("volume"),
        "20000".parse().expect("price"),
    )
}

fn mark(id: &str) -> Message {
    Heartbeat {
        test_req_id: Some(id.to_owned()),
    }
    .into()
}

fn probe(id: &str) -> Message {
    TestRequest {
        test_req_id: id.to_owned(),
    }
    .into()
}

fn answers(id: &str) -> impl FnMut(&Message) -> Option<Result<(), ConnectorError>> {
    let id = id.to_owned();
    move |message| match message {
        Message::Heartbeat(heartbeat) if heartbeat.test_req_id.as_deref() == Some(&id) => {
            Some(Ok(()))
        }
        _ => None,
    }
}

async fn next(events: &mut Receiver<Event>) -> Event {
    timeout(STEP, events.recv())
        .await
        .expect("an event in time")
        .expect("event")
}

async fn until(events: &mut Receiver<Event>, last: impl Fn(&Event) -> bool) -> Vec<Event> {
    let mut seen = Vec::new();
    loop {
        let event = next(events).await;
        let done = last(&event);
        seen.push(event);
        if done {
            return seen;
        }
    }
}

fn marks(events: &[Event]) -> Vec<(u64, bool, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Message(inbound) => match &inbound.message {
                Message::Heartbeat(heartbeat) => heartbeat
                    .test_req_id
                    .clone()
                    .map(|id| (inbound.seq, inbound.poss_dup, id)),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn is_mark(event: &Event, id: &str) -> bool {
    matches!(event, Event::Message(inbound) if inbound.message == mark(id))
}

async fn started(kind: SessionKind) -> (Server, FixClient, Peer) {
    let server = Server::bind().await;
    let (client, peer) = tokio::join!(FixClient::connect(server.configuration(), kind), async {
        let mut peer = server.accept().await;
        peer.logon().await;
        peer
    });
    (server, client.expect("logon"), peer)
}

async fn probed(client: &FixClient, peer: &mut Peer, id: &str) -> u64 {
    let (answer, seq) = tokio::join!(client.request(probe(id), "", answers(id)), async {
        let (header, message) = peer.request().await;
        assert_eq!(message, probe(id));
        peer.send(mark(id)).await;
        header.seq
    });
    assert_eq!(answer, Ok(()));
    seq
}

#[tokio::test]
async fn lost_connection_is_replaced_and_both_sides_count_from_one_again() {
    let (server, client, mut peer) = started(SessionKind::Quote).await;
    let quote = |id: &str, bid: &str| MarketDataSnapshot {
        md_req_id: Some(id.to_owned()),
        symbol: symbol(),
        entries: vec![SnapshotEntry {
            side: BookSide::Bid,
            price: bid.parse().expect("price"),
            size: None,
            id: None,
        }],
    };
    let (stream, id) = tokio::join!(client.subscribe_spot(symbol()), async {
        let (_, message) = peer.request().await;
        let Message::MarketDataRequest(request) = message else {
            panic!("expected a market data request, got {message:?}");
        };
        peer.send(quote(&request.md_req_id, "1.1")).await;
        request.md_req_id
    });
    let mut stream = stream.expect("subscription");
    assert_eq!(probed(&client, &mut peer, "before").await, 3);
    let mut events = client.events();
    drop(peer);

    let mut peer = server.accept().await;
    let (header, logon) = peer.logon().await;
    assert_eq!((header.seq, logon.reset_seq_num), (1, Some(true)));
    let seen = until(&mut events, |event| {
        matches!(event, Event::Reconnected { .. })
    })
    .await;
    assert_eq!(
        seen.first(),
        Some(&Event::Disconnected(TransportError::Closed))
    );
    assert_eq!(seen.last(), Some(&Event::Reconnected { attempt: 1 }));
    assert_eq!(
        stream.next().await,
        Err(ConnectorError::Transport(TransportError::Closed))
    );

    let (again, (seq, subscribed)) = tokio::join!(timeout(STEP, stream.next()), async {
        let (header, message) = peer.request().await;
        let Message::MarketDataRequest(request) = message else {
            panic!("expected the subscription again, got {message:?}");
        };
        peer.send(quote(&request.md_req_id, "1.2")).await;
        (header.seq, request)
    });
    assert_eq!(seq, 2);
    assert!(subscribed.subscribe && subscribed.md_req_id == id);
    let again = again
        .expect("in time")
        .expect("prices after the reconnection");
    assert_eq!(again.bid, Some("1.2".parse().expect("price")));
    assert_eq!(probed(&client, &mut peer, "after").await, 3);
    let (left, ()) = tokio::join!(client.logout(), async {
        let (_, message) = peer.request().await;
        assert_eq!(message, Logout { text: None }.into());
        peer.send(Logout { text: None }).await;
    });
    assert_eq!(left, Ok(()));
}

#[tokio::test]
async fn order_in_flight_when_the_connection_drops_is_unknown_and_never_sent_again() {
    let (server, client, mut peer) = started(SessionKind::Trade).await;
    let mut events = client.events();
    let again = |event: &Event| matches!(event, Event::Reconnected { .. });
    let (placed, ()) = tokio::join!(client.place(order()), async {
        let (header, message) = peer.request().await;
        assert!(matches!(message, Message::NewOrderSingle(_)), "{message:?}");
        assert_eq!(header.seq, 2);
        drop(peer);
    });
    assert_eq!(
        placed,
        Err(ConnectorError::OutcomeUnknown(Interruption::Transport(
            TransportError::Closed
        )))
    );
    let mut peer = server.accept().await;
    peer.logon().await;
    until(&mut events, again).await;
    assert_eq!(probed(&client, &mut peer, "first").await, 2);

    assert_eq!(client.force_reconnect().await, Ok(()));
    assert_eq!(peer.frame().await.map(|frame| frame.header.msg_type), None);
    let mut peer = server.accept().await;
    peer.logon().await;
    until(&mut events, again).await;
    assert_eq!(probed(&client, &mut peer, "second").await, 2);
}

#[tokio::test]
async fn reconnection_gives_up_after_a_handful_of_attempts_or_a_refused_logon() {
    let (client, peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    drop(peer);
    let seen = until(&mut events, |event| *event == Event::Ended).await;
    let failed: Vec<u32> = seen
        .iter()
        .filter_map(|event| match event {
            Event::ReconnectFailed {
                attempt,
                error: ConnectorError::Transport(_),
            } => Some(*attempt),
            _ => None,
        })
        .collect();
    assert_eq!(failed, [1, 2, 3, 4, 5]);
    assert_eq!(seen.len(), 7, "{seen:?}");
    timeout(STEP, client.closed())
        .await
        .expect("the end in time");
    assert_eq!(
        client.send(probe("late")).await,
        Err(ConnectorError::Closed)
    );
    assert_eq!(client.force_reconnect().await, Err(ConnectorError::Closed));
    assert_eq!(
        client.request(probe("late"), "", answers("late")).await,
        Err(ConnectorError::Closed)
    );

    let (server, client, peer) = started(SessionKind::Trade).await;
    let mut events = client.events();
    drop(peer);
    let mut peer = server.accept().await;
    peer.read().await;
    peer.send(Logout {
        text: Some("RET_INVALID_DATA".to_owned()),
    })
    .await;
    let seen = until(&mut events, |event| *event == Event::Ended).await;
    assert_eq!(
        seen.get(1..),
        Some(
            &[
                Event::ReconnectFailed {
                    attempt: 1,
                    error: FixError::LogonRefused {
                        text: Some("RET_INVALID_DATA".to_owned()),
                    }
                    .into(),
                },
                Event::Ended,
            ][..]
        )
    );
    timeout(STEP, client.closed())
        .await
        .expect("the end in time");
}

#[tokio::test]
async fn commands_between_two_connections_are_answered_and_a_logout_ends_the_wait() {
    let (server, client, peer) = started(SessionKind::Trade).await;
    let mut events = client.events();
    drop(peer);
    let mut peer = server.accept().await;
    peer.read().await;
    assert_eq!(
        next(&mut events).await,
        Event::Disconnected(TransportError::Closed)
    );
    assert_eq!(
        client.send(probe("early")).await,
        Err(ConnectorError::Closed)
    );
    assert_eq!(client.place(order()).await, Err(ConnectorError::Closed));
    assert_eq!(client.force_reconnect().await, Ok(()));
    assert_eq!(client.logout().await, Ok(()));
    timeout(STEP, client.closed())
        .await
        .expect("the end in time");
    assert_eq!(peer.frame().await.map(|frame| frame.header.msg_type), None);
}

#[tokio::test]
async fn gap_in_the_messages_of_the_server_is_asked_for_again_and_filled_in_order() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let repeated = |header: &mut ctrader_connector::fix::codec::Header| {
        header.poss_dup = true;
        header.orig_sending_time = Some(header.sending_time);
    };
    peer.send(mark("m2")).await;
    peer.send_with(5, |_| {}, mark("m5")).await;
    peer.send_with(6, |_| {}, mark("m6")).await;
    let (header, message) = peer.request().await;
    assert_eq!(
        message,
        ResendRequest {
            begin_seq_no: 3,
            end_seq_no: 0,
        }
        .into()
    );
    assert_eq!(header.seq, 2);
    peer.send_with(3, repeated, mark("m3")).await;
    let fill = SequenceReset {
        gap_fill: Some(true),
        new_seq_no: 5,
    };
    peer.send_with(3, repeated, mark("m3")).await;
    peer.send_with(4, repeated, fill).await;
    peer.send_with(5, repeated, mark("m5")).await;
    peer.send_with(6, repeated, mark("m6")).await;
    peer.set_next_seq(7);
    peer.send(mark("m7")).await;
    let reset = SequenceReset {
        gap_fill: None,
        new_seq_no: 20,
    };
    peer.send_with(1, |_| {}, reset).await;
    peer.set_next_seq(20);
    peer.send(mark("m20")).await;
    let seen = until(&mut events, |event| is_mark(event, "m20")).await;
    let read = |seq, dup, id: &str| (seq, dup, id.to_owned());
    assert_eq!(
        marks(&seen),
        [
            read(2, false, "m2"),
            read(3, true, "m3"),
            read(5, true, "m5"),
            read(6, true, "m6"),
            read(7, false, "m7"),
            read(20, false, "m20"),
        ]
    );
    let gaps: Vec<_> = seen
        .iter()
        .filter(|event| matches!(event, Event::Gap { .. }))
        .collect();
    assert_eq!(
        gaps,
        [&Event::Gap {
            expected: 3,
            received: 5,
        }]
    );
    assert_eq!(probed(&client, &mut peer, "whole").await, 3);

    peer.send_with(4, |_| {}, mark("stale")).await;
    let lost = until(&mut events, |event| matches!(event, Event::Disconnected(_))).await;
    assert!(
        matches!(lost.last(), Some(Event::Disconnected(TransportError::Decode(text))) if text.contains("below the expected 22")),
        "{lost:?}"
    );
}

#[tokio::test]
async fn gap_the_server_does_not_fill_ends_the_connection_typed() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    peer.send_with(9, |_| {}, mark("m9")).await;
    let (_, message) = peer.request().await;
    assert!(matches!(message, Message::ResendRequest(_)), "{message:?}");
    let lost = until(&mut events, |event| matches!(event, Event::Disconnected(_))).await;
    assert!(marks(&lost).is_empty(), "{lost:?}");
    assert!(
        matches!(lost.last(), Some(Event::Disconnected(TransportError::Decode(text))) if text.contains("from 2 on were not sent again")),
        "{lost:?}"
    );
}

#[tokio::test]
async fn resend_request_of_the_server_is_answered_by_a_gap_fill_and_no_order_is_repeated() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let again = |begin_seq_no| ResendRequest {
        begin_seq_no,
        end_seq_no: 0,
    };
    let (placed, ()) = tokio::join!(client.place(order()), async {
        let (header, message) = peer.request().await;
        assert!(matches!(message, Message::NewOrderSingle(_)), "{message:?}");
        assert_eq!(header.seq, 2);
        peer.send(again(1)).await;
        let (header, message) = peer.request().await;
        assert_eq!(
            message,
            SequenceReset {
                gap_fill: Some(true),
                new_seq_no: 3,
            }
            .into()
        );
        assert_eq!((header.seq, header.poss_dup), (1, true));
        assert!(header.orig_sending_time.is_some());
    });
    assert_eq!(
        placed,
        Err(ConnectorError::OutcomeUnknown(Interruption::Skipped))
    );
    let seen = until(&mut events, |event| matches!(event, Event::Skipped { .. })).await;
    assert_eq!(seen.last(), Some(&Event::Skipped { from: 1, to: 2 }));

    let (asked, ()) = tokio::join!(
        client.request(probe("asked"), "", answers("asked")),
        async {
            let (header, message) = peer.request().await;
            assert_eq!((header.seq, message), (3, probe("asked")));
            peer.send(again(3)).await;
            let (header, message) = peer.request().await;
            assert!(matches!(message, Message::SequenceReset(_)), "{message:?}");
            assert_eq!(header.seq, 3);
        }
    );
    assert_eq!(
        asked,
        Err(ConnectorError::OutcomeUnknown(Interruption::Skipped))
    );

    peer.send(again(50)).await;
    assert_eq!(probed(&client, &mut peer, "next").await, 4);
}
