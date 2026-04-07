//! Tests of the prices and of the symbols of the quote session against a scripted server.

mod fix_support;

use std::time::Duration;

use ctrader_connector::common::decimals::{Price, Volume};
use ctrader_connector::common::errors::{ConnectorError, FixError, TransportError};
use ctrader_connector::common::ids::SymbolId;
use ctrader_connector::fix::messages::{
    BookSide, BookUpdate, BusinessReject, Logout, MarketDataIncremental, MarketDataReject,
    MarketDataRequest, MarketDataSnapshot, MarketDepth, Message, Reject, SnapshotEntry,
};
use ctrader_connector::fix::quotes::{Book, SpotQuote};
use ctrader_connector::fix::securities::Security;
use ctrader_connector::fix::session::{Event, SessionKind};
use fix_support::{Peer, STEP, session, wire};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::timeout;

fn symbol(id: i64) -> SymbolId {
    SymbolId::new(id).expect("symbol")
}

fn price(text: &str) -> Price {
    text.parse().expect("price")
}

fn volume(text: &str) -> Volume {
    text.parse().expect("volume")
}

fn spot(id: &str, of: i64, bid: &str, offer: &str) -> MarketDataSnapshot {
    let entry = |side, text: &str| SnapshotEntry {
        side,
        price: price(text),
        size: None,
        id: None,
    };
    MarketDataSnapshot {
        md_req_id: Some(id.to_owned()),
        symbol: symbol(of),
        entries: vec![entry(BookSide::Bid, bid), entry(BookSide::Offer, offer)],
    }
}

fn added(side: BookSide, id: &str, of: i64, at: &str, amount: &str) -> BookUpdate {
    BookUpdate::New {
        side,
        id: id.to_owned(),
        symbol: symbol(of),
        price: price(at),
        size: Some(volume(amount)),
    }
}

fn changes(id: &str, entries: Vec<BookUpdate>) -> MarketDataIncremental {
    MarketDataIncremental {
        md_req_id: Some(id.to_owned()),
        entries,
    }
}

async fn subscription(peer: &mut Peer, depth: MarketDepth, of: i64) -> (u64, String) {
    let (header, message) = peer.request().await;
    let Message::MarketDataRequest(request) = message else {
        panic!("expected a market data request, got {message:?}");
    };
    assert_eq!(
        request,
        MarketDataRequest {
            md_req_id: request.md_req_id.clone(),
            subscribe: true,
            depth,
            symbols: vec![symbol(of)],
            sides: vec![BookSide::Bid, BookSide::Offer],
        }
    );
    (header.seq, request.md_req_id)
}

#[tokio::test]
async fn spot_subscription_yields_each_report_of_its_own_and_unsubscribes() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let (stream, id) = tokio::join!(client.subscribe_spot(symbol(7)), async {
        let (_, id) = subscription(&mut peer, MarketDepth::Spot, 7).await;
        peer.send(spot("another", 7, "9", "10")).await;
        peer.send(spot(&id, 7, "1.10001", "1.10004")).await;
        id
    });
    let mut stream = stream.expect("subscription");
    assert_eq!(stream.symbol(), symbol(7));
    let first = SpotQuote {
        symbol: symbol(7),
        bid: Some(price("1.10001")),
        offer: Some(price("1.10004")),
    };
    assert_eq!(stream.last(), first);
    peer.send(spot("another", 7, "9", "10")).await;
    peer.send(MarketDataSnapshot {
        md_req_id: Some(id.clone()),
        symbol: symbol(7),
        entries: vec![SnapshotEntry {
            side: BookSide::Offer,
            price: price("1.10005"),
            size: None,
            id: None,
        }],
    })
    .await;
    let second = SpotQuote {
        symbol: symbol(7),
        bid: None,
        offer: Some(price("1.10005")),
    };
    assert_eq!(
        timeout(STEP, stream.next()).await.expect("in time"),
        Ok(second)
    );
    assert_eq!(stream.last(), second);
    assert_eq!(stream.unsubscribe().await, Ok(()));
    let (_, message) = peer.request().await;
    assert_eq!(
        message,
        MarketDataRequest {
            md_req_id: id,
            subscribe: false,
            depth: MarketDepth::Spot,
            symbols: vec![symbol(7)],
            sides: vec![BookSide::Bid, BookSide::Offer],
        }
        .into()
    );
}

#[tokio::test]
async fn depth_subscription_keeps_the_book_of_the_server() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let (stream, id) = tokio::join!(client.subscribe_depth(symbol(7)), async {
        let (_, id) = subscription(&mut peer, MarketDepth::Full, 7).await;
        peer.send(MarketDataSnapshot {
            md_req_id: Some(id.clone()),
            symbol: symbol(7),
            entries: Vec::new(),
        })
        .await;
        id
    });
    let mut stream = stream.expect("subscription");
    assert_eq!(stream.symbol(), symbol(7));
    assert!(stream.book().is_empty());
    peer.send(changes(
        "another",
        vec![added(BookSide::Bid, "x", 7, "5", "5")],
    ))
    .await;
    peer.send(changes(
        &id,
        vec![
            added(BookSide::Bid, "b1", 7, "1.1", "100"),
            added(BookSide::Bid, "b2", 7, "1.2", "200"),
            added(BookSide::Offer, "o1", 7, "1.4", "300"),
            added(BookSide::Offer, "o2", 7, "1.3", "400"),
        ],
    ))
    .await;
    let book = timeout(STEP, stream.next())
        .await
        .expect("in time")
        .expect("book");
    assert_eq!(book.symbol(), symbol(7));
    assert_eq!(book.len(), 4);
    let ids = |entries: Vec<&ctrader_connector::fix::quotes::BookEntry>| -> Vec<String> {
        entries.into_iter().map(|entry| entry.id.clone()).collect()
    };
    assert_eq!(ids(book.bids()), ["b2", "b1"]);
    assert_eq!(ids(book.offers()), ["o2", "o1"]);
    assert_eq!(
        book.bids().first().map(|best| best.price),
        Some(price("1.2"))
    );
    assert_eq!(
        book.offers().first().and_then(|best| best.size),
        Some(volume("400"))
    );
    peer.send(changes(
        &id,
        vec![
            added(BookSide::Bid, "b3", 7, "1.25", "50"),
            BookUpdate::Delete {
                id: "b2".to_owned(),
                symbol: symbol(7),
            },
        ],
    ))
    .await;
    let book = timeout(STEP, stream.next())
        .await
        .expect("in time")
        .expect("book");
    assert_eq!(ids(book.bids()), ["b3", "b1"]);
    assert_eq!(ids(stream.book().offers()), ["o2", "o1"]);
    assert_eq!(stream.unsubscribe().await, Ok(()));
    let (_, message) = peer.request().await;
    assert!(
        matches!(&message, Message::MarketDataRequest(request) if !request.subscribe && request.md_req_id == id),
        "{message:?}"
    );
}

#[tokio::test]
async fn refused_subscriptions_are_typed_errors() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let server = tokio::spawn(async move {
        let (_, id) = subscription(&mut peer, MarketDepth::Spot, 9).await;
        peer.send(MarketDataReject {
            md_req_id: id,
            reason: Some(0),
            text: Some("SYMBOL_NOT_FOUND:Symbol not found with id 9".to_owned()),
        })
        .await;
        let (seq, _) = subscription(&mut peer, MarketDepth::Full, 9).await;
        peer.send(Reject {
            ref_seq_num: seq,
            text: Some("Required tag missing".to_owned()),
            ref_tag_id: Some(267),
            ref_msg_type: Some("V".to_owned()),
            reason: Some(1),
        })
        .await;
        let (_, id) = subscription(&mut peer, MarketDepth::Spot, 9).await;
        peer.send(BusinessReject {
            ref_seq_num: None,
            ref_msg_type: Some("V".to_owned()),
            ref_id: Some(id),
            reason: Some(4),
            text: Some("Application not available".to_owned()),
        })
        .await;
        subscription(&mut peer, MarketDepth::Spot, 9).await;
        peer
    });
    let refused = client.subscribe_spot(symbol(9)).await.expect_err("refusal");
    assert_eq!(
        refused,
        ConnectorError::Fix(FixError::MarketDataRejected {
            reason: Some(0),
            text: Some("SYMBOL_NOT_FOUND:Symbol not found with id 9".to_owned()),
        })
    );
    assert_eq!(
        refused.to_string(),
        "FIX failure: the server refused the market data request: SYMBOL_NOT_FOUND:Symbol not found with id 9"
    );
    assert_eq!(
        client.subscribe_depth(symbol(9)).await.err(),
        Some(ConnectorError::Fix(FixError::Rejected {
            ref_seq_num: 3,
            tag: Some(267),
            reason: Some(1),
            text: Some("Required tag missing".to_owned()),
        }))
    );
    assert!(matches!(
        client.subscribe_spot(symbol(9)).await.err(),
        Some(ConnectorError::Fix(FixError::BusinessRejected {
            reason: Some(4),
            ..
        }))
    ));
    assert_eq!(
        client.subscribe_spot(symbol(9)).await.err(),
        Some(ConnectorError::Timeout(Duration::from_secs(2)))
    );
    drop(server.await.expect("server"));
}

#[tokio::test]
async fn streams_end_typed_when_the_session_ends_or_the_reader_falls_behind() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let (stream, id) = tokio::join!(client.subscribe_spot(symbol(7)), async {
        let (_, id) = subscription(&mut peer, MarketDepth::Spot, 7).await;
        peer.send(spot(&id, 7, "1", "2")).await;
        id
    });
    let mut stream = stream.expect("subscription");
    let mut burst = Vec::new();
    let from = peer.next_seq();
    for seq in from..from + 5_000 {
        burst.extend(peer.bytes(seq, spot(&id, 7, "1", "2")));
    }
    peer.set_next_seq(from + 5_000);
    peer.raw(&burst).await;
    peer.send(spot(&id, 7, "3", "4")).await;
    let mut events = client.events();
    peer.send(spot("mark", 8, "3", "4")).await;
    timeout(STEP, async {
        loop {
            match events.recv().await {
                Ok(Event::Message(inbound)) => {
                    if matches!(&inbound.message, Message::MarketDataSnapshot(mark) if mark.md_req_id.as_deref() == Some("mark"))
                    {
                        break;
                    }
                }
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => panic!("the session ended"),
            }
        }
    })
    .await
    .expect("the mark in time");
    assert!(matches!(
        stream.next().await,
        Err(ConnectorError::Lagged { skipped }) if skipped > 0
    ));
    assert!(stream.next().await.is_ok());
    drop(peer);
    let ended = timeout(STEP, async {
        loop {
            if let Err(error) = stream.next().await {
                return error;
            }
        }
    })
    .await
    .expect("in time");
    assert_eq!(ended, ConnectorError::Transport(TransportError::Closed));
    assert_eq!(stream.unsubscribe().await, Err(ConnectorError::Closed));

    let (client, mut peer) = session(SessionKind::Quote).await;
    let (stream, ()) = tokio::join!(client.subscribe_depth(symbol(7)), async {
        let (_, id) = subscription(&mut peer, MarketDepth::Full, 7).await;
        peer.send(MarketDataSnapshot {
            md_req_id: Some(id),
            symbol: symbol(7),
            entries: Vec::new(),
        })
        .await;
    });
    let mut stream = stream.expect("subscription");
    peer.send(Logout { text: None }).await;
    assert_eq!(
        timeout(STEP, stream.next()).await.expect("in time").err(),
        Some(ConnectorError::Transport(TransportError::Closed))
    );
}

#[test]
fn book_takes_snapshots_and_changes_of_its_own_symbol_only() {
    let entry = |side, at: &str, id: Option<&str>| SnapshotEntry {
        side,
        price: price(at),
        size: Some(volume("10")),
        id: id.map(str::to_owned),
    };
    let mut book = Book::new(symbol(7));
    assert!(book.is_empty());
    book.replace(&MarketDataSnapshot {
        md_req_id: None,
        symbol: symbol(7),
        entries: vec![
            entry(BookSide::Bid, "1.1", Some("b1")),
            entry(BookSide::Bid, "1.2", None),
            entry(BookSide::Offer, "1.3", None),
        ],
    });
    assert_eq!(book.len(), 3);
    assert_eq!(
        book.bids().first().map(|best| best.price),
        Some(price("1.2"))
    );
    assert_eq!(book.offers().len(), 1);
    let before = book.clone();
    book.replace(&MarketDataSnapshot {
        md_req_id: None,
        symbol: symbol(8),
        entries: Vec::new(),
    });
    book.apply(&added(BookSide::Bid, "b9", 8, "1.25", "5"));
    book.apply(&BookUpdate::Delete {
        id: "b1".to_owned(),
        symbol: symbol(8),
    });
    assert_eq!(book, before);
    book.apply(&added(BookSide::Bid, "b1", 7, "1.15", "5"));
    assert_eq!(book.len(), 3);
    assert_eq!(
        book.bids().last().map(|worst| (worst.price, worst.size)),
        Some((price("1.15"), Some(volume("5"))))
    );
    book.replace(&MarketDataSnapshot {
        md_req_id: None,
        symbol: symbol(7),
        entries: Vec::new(),
    });
    assert!(book.is_empty());
}

#[tokio::test]
async fn securities_are_listed_whole_or_not_at_all() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let server = tokio::spawn(async move {
        let mut ids = Vec::new();
        for answer in [
            "322=responce:1|560=0|146=2|55=1|1007=EURUSD|1008=5|55=22395|1007=BTCUSD|1008=3",
            "322=responce:2|560=2",
            "322=responce:3|560=0|146=2|55=1|1007=EURUSD|1008=5|55=2|1008=3",
            "322=responce:4|560=0|146=1|55=1|1007=EURUSD",
        ] {
            let (_, message) = peer.request().await;
            let Message::SecurityListRequest(request) = message else {
                panic!("expected a security list request, got {message:?}");
            };
            assert_eq!((request.request_type, request.symbol), (0, None));
            peer.send(wire("y", &format!("320=another|{answer}"))).await;
            peer.send(wire(
                "y",
                &format!("320={}|{answer}", request.security_req_id),
            ))
            .await;
            ids.push(request.security_req_id);
        }
        (peer, ids)
    });
    assert_eq!(
        client.securities().await,
        Ok(vec![
            Security {
                id: symbol(1),
                name: "EURUSD".to_owned(),
                digits: 5,
            },
            Security {
                id: symbol(22_395),
                name: "BTCUSD".to_owned(),
                digits: 3,
            },
        ])
    );
    assert!(matches!(
        client.securities().await,
        Err(ConnectorError::Fix(FixError::BusinessRejected {
            reason: Some(2),
            text: None,
            ..
        }))
    ));
    let lacking = |tag| {
        Err(ConnectorError::Fix(FixError::MissingField {
            msg_type: "y".to_owned(),
            tag,
        }))
    };
    assert_eq!(client.securities().await, lacking(1007));
    assert_eq!(client.securities().await, lacking(1008));
    let (_peer, ids) = server.await.expect("server");
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        4
    );
}
