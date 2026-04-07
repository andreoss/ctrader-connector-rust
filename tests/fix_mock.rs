//! Tests of the FIX session against a scripted server on the local machine.

mod fix_support;

use std::time::Duration;

use ctrader_connector::common::config::FixPassword;
use ctrader_connector::common::errors::{ConnectorError, FixError, Interruption, TransportError};
use ctrader_connector::common::transport::Plain;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::{
    BusinessReject, Heartbeat, Logout, Message, Reject, TestRequest,
};
use ctrader_connector::fix::session::{Event, SessionKind};
use fix_support::{PASSWORD, SENDER, STEP, Server, USERNAME, session};
use tokio::sync::broadcast::Receiver;
use tokio::time::{Instant, timeout};

async fn next(events: &mut Receiver<Event>) -> Event {
    timeout(STEP, events.recv())
        .await
        .expect("an event in time")
        .expect("event")
}

async fn next_matching(events: &mut Receiver<Event>, wanted: impl Fn(&Event) -> bool) -> Event {
    loop {
        let event = next(events).await;
        if wanted(&event) {
            return event;
        }
    }
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

#[tokio::test]
async fn logon_carries_the_credentials_and_asks_for_a_sequence_reset() {
    let server = Server::bind().await;
    let configuration = server.configuration();
    let shown = format!("{configuration:?}");
    assert!(!shown.contains(PASSWORD), "{shown}");
    let (client, logon) = tokio::join!(
        FixClient::connect_with(configuration, SessionKind::Trade, Plain),
        async {
            let mut peer = server.accept().await;
            let logon = peer.logon().await;
            (peer, logon)
        }
    );
    let client = client.expect("logon");
    let (mut peer, (header, logon)) = logon;
    assert_eq!(client.kind(), SessionKind::Trade);
    assert_eq!(header.msg_type, "A");
    assert_eq!(header.seq, 1);
    assert_eq!(header.sender_comp_id, SENDER);
    assert_eq!(header.target_comp_id, "CSERVER");
    assert_eq!(header.target_sub_id.as_deref(), Some("TRADE"));
    assert_eq!(header.sender_sub_id.as_deref(), Some("TRADE"));
    assert_eq!(logon.encrypt_method, 0);
    assert_eq!(logon.heart_bt_int, 1);
    assert_eq!(logon.reset_seq_num, Some(true));
    assert_eq!(logon.username.as_deref(), Some(USERNAME));
    assert_eq!(
        logon.password.as_ref().map(FixPassword::expose),
        Some(PASSWORD)
    );
    let (left, ()) = tokio::join!(client.logout(), async {
        let (header, message) = peer.request().await;
        assert_eq!(message, Logout { text: None }.into());
        assert_eq!(header.seq, 2);
        peer.send(Logout { text: None }).await;
        assert!(peer.frame().await.is_none());
    });
    assert_eq!(left, Ok(()));
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
    assert_eq!(
        client.send(probe("late")).await,
        Err(ConnectorError::Closed)
    );
    assert_eq!(client.logout().await, Err(ConnectorError::Closed));
}

#[tokio::test]
async fn wrong_password_is_refused_with_a_typed_error_that_holds_no_secret() {
    let server = Server::bind().await;
    let configuration = server.configuration();
    let (client, ()) = tokio::join!(
        FixClient::connect(configuration, SessionKind::Quote),
        async {
            let mut peer = server.accept().await;
            let (header, message) = peer.read().await;
            assert!(matches!(message, Message::Logon(_)));
            assert_eq!(header.target_sub_id.as_deref(), Some("QUOTE"));
            peer.send(Logout {
                text: Some("RET_INVALID_DATA".to_owned()),
            })
            .await;
        }
    );
    let error = client.expect_err("refused logon");
    assert_eq!(
        error,
        ConnectorError::Fix(FixError::LogonRefused {
            text: Some("RET_INVALID_DATA".to_owned())
        })
    );
    let shown = format!("{error} {error:?}");
    assert!(shown.contains("RET_INVALID_DATA"), "{shown}");
    assert!(!shown.contains(PASSWORD), "{shown}");
}

#[tokio::test]
async fn logon_fails_typed_when_the_server_is_silent_closes_or_answers_otherwise() {
    let server = Server::bind().await;
    let configuration = server
        .builder()
        .timeout(Duration::from_millis(300))
        .build()
        .expect("configuration");
    let (silent, _peer) = tokio::join!(
        FixClient::connect(configuration.clone(), SessionKind::Quote),
        async {
            let mut peer = server.accept().await;
            peer.read().await;
            peer
        }
    );
    assert_eq!(
        silent.expect_err("silence"),
        ConnectorError::Timeout(Duration::from_millis(300))
    );
    let (closed, ()) = tokio::join!(
        FixClient::connect(configuration.clone(), SessionKind::Quote),
        async {
            let mut peer = server.accept().await;
            peer.read().await;
        }
    );
    assert_eq!(
        closed.expect_err("closed"),
        ConnectorError::Transport(TransportError::Closed)
    );
    let (other, _peer) = tokio::join!(
        FixClient::connect(configuration.clone(), SessionKind::Quote),
        async {
            let mut peer = server.accept().await;
            peer.read().await;
            peer.send(Heartbeat { test_req_id: None }).await;
            peer
        }
    );
    assert_eq!(
        other.expect_err("heartbeat instead of logon"),
        ConnectorError::Fix(FixError::UnexpectedMessage("0".to_owned()))
    );
    let (garbled, _peer) = tokio::join!(
        FixClient::connect(configuration, SessionKind::Quote),
        async {
            let mut peer = server.accept().await;
            peer.read().await;
            peer.raw(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            peer
        }
    );
    assert!(matches!(
        garbled.expect_err("garbage"),
        ConnectorError::Transport(TransportError::Decode(_))
    ));
    drop(server);
    let unreachable = Server::bind().await;
    let configuration = unreachable.configuration();
    drop(unreachable);
    assert!(matches!(
        FixClient::connect(configuration, SessionKind::Quote).await,
        Err(ConnectorError::Transport(_))
    ));
}

#[tokio::test]
async fn heartbeats_flow_while_idle_and_a_test_request_is_answered_in_sequence() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let mut events = client.events();
    let started = Instant::now();
    let (first, message) = peer.read().await;
    assert_eq!(message, Heartbeat { test_req_id: None }.into());
    assert_eq!(first.seq, 2);
    assert!(started.elapsed() >= Duration::from_millis(700));
    peer.send(probe("are-you-there")).await;
    let (second, message) = peer.read().await;
    assert_eq!(
        message,
        Heartbeat {
            test_req_id: Some("are-you-there".to_owned())
        }
        .into()
    );
    assert_eq!(second.seq, 3);
    let Event::Message(inbound) = next(&mut events).await else {
        panic!("not a message");
    };
    assert_eq!(inbound.seq, 2);
    assert!(!inbound.poss_dup);
    assert_eq!(inbound.message, probe("are-you-there"));
    peer.send(Heartbeat { test_req_id: None }).await;
    let (third, _) = peer.read().await;
    assert_eq!(third.seq, 4);
    drop(events);
    drop(client);
    let (_, message) = peer.request().await;
    assert_eq!(message, Logout { text: None }.into());
    assert!(peer.frame().await.is_none());
}

#[tokio::test]
async fn silent_server_is_probed_and_then_reported_idle() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let mut events = client.events();
    let (_, heartbeat) = peer.read().await;
    assert_eq!(heartbeat, Heartbeat { test_req_id: None }.into());
    let (_, message) = peer.request().await;
    assert!(matches!(message, Message::TestRequest(_)), "{message:?}");
    assert_eq!(
        next(&mut events).await,
        Event::Disconnected(TransportError::Idle(Duration::from_millis(2_500)))
    );
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
    assert_eq!(
        client.send(probe("late")).await,
        Err(ConnectorError::Closed)
    );
}

#[tokio::test]
async fn logout_of_the_server_ends_the_session_with_its_text() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    peer.send(Logout {
        text: Some("maintenance".to_owned()),
    })
    .await;
    let (_, message) = peer.request().await;
    assert_eq!(message, Logout { text: None }.into());
    let ended = next_matching(&mut events, |event| {
        matches!(event, Event::LoggedOut { .. })
    })
    .await;
    assert_eq!(
        ended,
        Event::LoggedOut {
            text: Some("maintenance".to_owned())
        }
    );
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
}

#[tokio::test]
async fn request_returns_its_answer_and_rejects_are_typed_errors() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let server = tokio::spawn(async move {
        let (_, message) = peer.request().await;
        assert_eq!(message, probe("one"));
        peer.send(Heartbeat {
            test_req_id: Some("other".to_owned()),
        })
        .await;
        peer.send(Heartbeat {
            test_req_id: Some("one".to_owned()),
        })
        .await;
        let (header, _) = peer.request().await;
        peer.send(Reject {
            ref_seq_num: header.seq,
            text: Some("Tag not defined for this message type".to_owned()),
            ref_tag_id: Some(112),
            ref_msg_type: Some("1".to_owned()),
            reason: Some(2),
        })
        .await;
        let (header, _) = peer.request().await;
        peer.send(BusinessReject {
            ref_seq_num: Some(header.seq),
            ref_msg_type: Some("1".to_owned()),
            ref_id: None,
            reason: Some(0),
            text: Some("ORDER_NOT_FOUND:nothing".to_owned()),
        })
        .await;
        peer.request().await;
        peer.send(BusinessReject {
            ref_seq_num: None,
            ref_msg_type: None,
            ref_id: Some("four".to_owned()),
            reason: None,
            text: None,
        })
        .await;
        peer.request().await;
        peer
    });
    assert_eq!(
        client.request(probe("one"), "one", answers("one")).await,
        Ok(())
    );
    assert_eq!(
        client.request(probe("two"), "two", answers("two")).await,
        Err(ConnectorError::Fix(FixError::Rejected {
            ref_seq_num: 3,
            tag: Some(112),
            reason: Some(2),
            text: Some("Tag not defined for this message type".to_owned()),
        }))
    );
    let refused = client
        .request(probe("three"), "three", answers("three"))
        .await
        .expect_err("business reject");
    assert_eq!(
        refused,
        ConnectorError::Fix(FixError::BusinessRejected {
            ref_id: None,
            reason: Some(0),
            text: Some("ORDER_NOT_FOUND:nothing".to_owned()),
        })
    );
    assert_eq!(
        refused.to_string(),
        "FIX failure: the server refused the request: ORDER_NOT_FOUND:nothing"
    );
    assert_eq!(
        client.request(probe("four"), "four", answers("four")).await,
        Err(ConnectorError::Fix(FixError::BusinessRejected {
            ref_id: Some("four".to_owned()),
            reason: None,
            text: None,
        }))
    );
    assert_eq!(
        client.request(probe("five"), "", answers("five")).await,
        Err(ConnectorError::Timeout(Duration::from_secs(2)))
    );
    drop(server.await.expect("server"));
}

#[tokio::test]
async fn request_that_must_not_repeat_reports_an_unknown_outcome_when_cut_short() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let server = tokio::spawn(async move {
        peer.request().await;
        peer.send(Heartbeat {
            test_req_id: Some("one".to_owned()),
        })
        .await;
        peer.request().await;
        peer
    });
    assert_eq!(
        client
            .request_once(probe("one"), "one", answers("one"))
            .await,
        Ok(())
    );
    assert_eq!(
        client
            .request_once(probe("two"), "two", answers("two"))
            .await,
        Err(ConnectorError::OutcomeUnknown(Interruption::Timeout(
            Duration::from_secs(2)
        )))
    );
    let peer = server.await.expect("server");
    let (lost, ()) = tokio::join!(
        client.request_once(probe("three"), "three", answers("three")),
        async {
            let mut peer = peer;
            peer.request().await;
        }
    );
    assert_eq!(
        lost,
        Err(ConnectorError::OutcomeUnknown(Interruption::Transport(
            TransportError::Closed
        )))
    );
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
    assert_eq!(
        client.request(probe("four"), "four", answers("four")).await,
        Err(ConnectorError::Closed)
    );
}

#[tokio::test]
async fn request_ends_typed_when_the_server_logs_out_or_drops_the_connection() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let (ended, ()) = tokio::join!(client.request(probe("one"), "one", answers("one")), async {
        peer.request().await;
        peer.send(Logout { text: None }).await;
    });
    assert_eq!(
        ended,
        Err(ConnectorError::Transport(TransportError::Closed))
    );
    let (client, mut peer) = session(SessionKind::Quote).await;
    let (dropped, ()) = tokio::join!(client.request(probe("one"), "one", answers("one")), async {
        peer.request().await;
        drop(peer);
    });
    assert_eq!(
        dropped,
        Err(ConnectorError::Transport(TransportError::Closed))
    );
}

#[tokio::test]
async fn unreadable_messages_are_skipped_and_a_broken_stream_ends_the_session() {
    let (client, mut peer) = session(SessionKind::Quote).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    peer.send_with(
        seq,
        |header| header.msg_type = "ZZ".to_owned(),
        Heartbeat { test_req_id: None },
    )
    .await;
    assert_eq!(
        next(&mut events).await,
        Event::Unparsed(FixError::UnsupportedMessage("ZZ".to_owned()))
    );
    let mut bytes = peer.bytes(seq + 1, probe("sum"));
    let at = bytes.len() - 2;
    let digit = bytes.get_mut(at).expect("digit");
    *digit = if *digit == b'9' { b'0' } else { *digit + 1 };
    peer.raw(&bytes).await;
    assert!(matches!(
        next(&mut events).await,
        Event::Unparsed(FixError::Checksum { .. })
    ));
    peer.set_next_seq(seq + 1);
    peer.send(probe("after")).await;
    let Event::Message(inbound) = next(&mut events).await else {
        panic!("not a message");
    };
    assert_eq!(inbound.message, probe("after"));
    peer.send_with(1, |_| {}, Heartbeat { test_req_id: None })
        .await;
    assert!(matches!(
        next(&mut events).await,
        Event::Disconnected(TransportError::Decode(_))
    ));
    let (client, mut peer) = session(SessionKind::Quote).await;
    let mut events = client.events();
    peer.raw(b"not a message at all").await;
    assert!(matches!(
        next(&mut events).await,
        Event::Disconnected(TransportError::Decode(_))
    ));
}
