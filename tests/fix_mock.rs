//! Tests of the FIX session against a scripted server on the local machine.
//!
//! A comment that names rows refers to the table "Messages that cannot be used" in the
//! documentation of the module `fix`: the test below it pins those rows.

mod fix_support;

use std::time::Duration;

use ctrader_connector::common::config::FixPassword;
use ctrader_connector::common::errors::{ConnectorError, FixError, Interruption, TransportError};
use ctrader_connector::common::transport::Plain;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::{
    BusinessReject, Heartbeat, Logout, Message, Reject, ResendRequest, TestRequest,
};
use ctrader_connector::fix::session::{Event, Inbound, SessionKind};
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

// Row 18, the stream that lost its framing.
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

fn sealed(body: &str) -> Vec<u8> {
    let body = body.replace('|', "\u{1}");
    let mut bytes = format!("8=FIX.4.4\u{1}9={}\u{1}{body}", body.len()).into_bytes();
    let sum = bytes.iter().fold(0_u8, |sum, byte| sum.wrapping_add(*byte));
    bytes.extend_from_slice(format!("10={sum:03}\u{1}").as_bytes());
    bytes
}

fn without_sending_time(seq: u64) -> Vec<u8> {
    sealed(&format!("35=8|49=CSERVER|56={SENDER}|34={seq}|"))
}

async fn reject_naming(peer: &mut fix_support::Peer, msg_type: &str) -> (u64, Message) {
    let frame = peer.frame().await.expect("a reject, not the end");
    let named: Vec<&str> = frame
        .body
        .iter()
        .filter(|field| field.tag == 372)
        .map(|field| field.value.as_str())
        .collect();
    assert_eq!(named, [msg_type]);
    let message = Message::from_frame(&frame).expect("message of the client");
    (frame.header.seq, message)
}

// Rows 11, 5, 4 and 3, in that order.
#[tokio::test]
async fn message_with_an_unreadable_header_is_rejected_and_the_numbering_goes_on() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let missing = FixError::MissingField {
        msg_type: "8".to_owned(),
        tag: 52,
    };
    peer.raw(&without_sending_time(seq)).await;
    assert_eq!(next(&mut events).await, Event::Unparsed(missing.clone()));
    let (sent, reject) = reject_naming(&mut peer, "8").await;
    assert_eq!(
        reject,
        Message::from(Reject {
            ref_seq_num: seq,
            text: Some(missing.to_string()),
            ref_tag_id: Some(52),
            ref_msg_type: Some("8".to_owned()),
            reason: Some(1),
        })
    );
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        sent + 1
    );
    peer.raw(&without_sending_time(seq)).await;
    assert_eq!(next(&mut events).await, Event::Unparsed(missing.clone()));
    peer.raw(&without_sending_time(seq + 3)).await;
    assert_eq!(
        next(&mut events).await,
        Event::Gap {
            expected: seq + 2,
            received: seq + 3,
        }
    );
    assert_eq!(
        peer.read().await.1,
        Message::from(ResendRequest {
            begin_seq_no: seq + 2,
            end_seq_no: 0,
        })
    );
    peer.raw(&sealed(&format!(
        "35=8|49=CSERVER|56={SENDER}|34={}|34={}|",
        seq + 2,
        seq + 2
    )))
    .await;
    assert!(matches!(
        next(&mut events).await,
        Event::Unparsed(FixError::InvalidField { tag: 34, .. })
    ));
    peer.set_next_seq(seq + 2);
    peer.send(probe("again")).await;
    assert_eq!(
        next(&mut events).await,
        Event::Message(Inbound {
            seq: seq + 2,
            poss_dup: false,
            message: probe("again"),
        })
    );
    assert_eq!(
        peer.read().await.1,
        Message::from(Heartbeat {
            test_req_id: Some("again".to_owned()),
        })
    );
}

// Rows 10, 2, 8 and 1, in that order.
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

async fn answered_after(
    peer: &mut fix_support::Peer,
    events: &mut Receiver<Event>,
    seq: u64,
) -> u64 {
    peer.set_next_seq(seq);
    peer.send(probe("after")).await;
    assert_eq!(
        next(events).await,
        Event::Message(Inbound {
            seq,
            poss_dup: false,
            message: probe("after"),
        })
    );
    let (header, message) = peer.read().await;
    assert_eq!(
        message,
        Message::from(Heartbeat {
            test_req_id: Some("after".to_owned()),
        })
    );
    header.seq
}

// Row 10.
#[tokio::test]
async fn message_of_an_unsupported_type_is_answered_with_a_session_reject() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let unsupported = FixError::UnsupportedMessage("ZZ".to_owned());
    peer.send_with(
        seq,
        |header| header.msg_type = "ZZ".to_owned(),
        Heartbeat { test_req_id: None },
    )
    .await;
    assert_eq!(
        next(&mut events).await,
        Event::Unparsed(unsupported.clone())
    );
    let (sent, reject) = reject_naming(&mut peer, "ZZ").await;
    assert_eq!(
        reject,
        Message::from(Reject {
            ref_seq_num: seq,
            text: Some(unsupported.to_string()),
            ref_tag_id: None,
            ref_msg_type: Some("ZZ".to_owned()),
            reason: Some(11),
        })
    );
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        sent + 1
    );
    drop(client);
}

fn refusal() -> Message {
    BusinessReject {
        ref_seq_num: Some(1),
        ref_msg_type: Some("0".to_owned()),
        ref_id: None,
        reason: Some(3),
        text: None,
    }
    .into()
}

#[tokio::test]
async fn business_reject_of_the_application_is_refused_and_takes_no_number() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let first = client.send(probe("before")).await.expect("sent");
    assert_eq!(
        client.send(refusal()).await,
        Err(ConnectorError::Fix(FixError::Unsendable("j".to_owned())))
    );
    assert_eq!(client.send(probe("after")).await, Ok(first + 1));
    for (seq, id) in [(first, "before"), (first + 1, "after")] {
        let (header, message) = peer.read().await;
        assert_eq!((header.seq, message), (seq, probe(id)));
    }
    drop(client);
}

// Rows 9, 10 and 11.
#[tokio::test]
async fn no_refusal_is_ever_written_as_a_business_reject() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    for (at, msg_type) in [(seq, "ZZ"), (seq + 1, "1")] {
        peer.send_with(
            at,
            |header| header.msg_type = msg_type.to_owned(),
            Heartbeat { test_req_id: None },
        )
        .await;
    }
    peer.raw(&without_sending_time(seq + 2)).await;
    for (at, msg_type) in [(seq + 3, "ZZ"), (seq + 4, "j")] {
        peer.raw(&sealed(&format!(
            "35={msg_type}|49=CSERVER|56={SENDER}|34={at}|"
        )))
        .await;
    }
    for _ in 0..5 {
        assert!(matches!(next(&mut events).await, Event::Unparsed(_)));
    }
    let unsendable = ConnectorError::Fix(FixError::Unsendable("j".to_owned()));
    assert_eq!(client.send(refusal()).await, Err(unsendable.clone()));
    assert_eq!(
        client.request(refusal(), "", answers("never")).await,
        Err(unsendable.clone())
    );
    assert_eq!(
        client.request_once(refusal(), "", answers("never")).await,
        Err(unsendable)
    );
    peer.set_next_seq(seq + 5);
    peer.send(Logout { text: None }).await;
    let mut written = Vec::new();
    while let Some(frame) = peer.frame().await {
        written.push((frame.header.seq, frame.header.msg_type));
    }
    let numbered = |seq: u64, msg_type: &str| (seq, msg_type.to_owned());
    assert_eq!(
        written,
        [
            numbered(2, "3"),
            numbered(3, "3"),
            numbered(4, "3"),
            numbered(5, "3"),
            numbered(6, "5"),
        ]
    );
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
}

// Rows 11 and 12.
#[tokio::test]
async fn message_with_a_malformed_body_is_answered_with_a_reject() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let missing = FixError::MissingField {
        msg_type: "1".to_owned(),
        tag: 112,
    };
    peer.send_with(
        seq,
        |header| header.msg_type = "1".to_owned(),
        Heartbeat { test_req_id: None },
    )
    .await;
    assert_eq!(next(&mut events).await, Event::Unparsed(missing.clone()));
    let (sent, reject) = reject_naming(&mut peer, "1").await;
    assert_eq!(
        reject,
        Message::from(Reject {
            ref_seq_num: seq,
            text: Some(missing.to_string()),
            ref_tag_id: Some(112),
            ref_msg_type: Some("1".to_owned()),
            reason: Some(1),
        })
    );
    let invalid = FixError::InvalidField {
        msg_type: "2".to_owned(),
        tag: 7,
    };
    let mut frame = Message::from(ResendRequest {
        begin_seq_no: 1,
        end_seq_no: 0,
    })
    .to_frame(peer.header(seq + 1));
    for field in &mut frame.body {
        if field.tag == 7 {
            field.value = "first".to_owned();
        }
    }
    peer.send_frame(&frame).await;
    assert_eq!(next(&mut events).await, Event::Unparsed(invalid.clone()));
    let (again, reject) = reject_naming(&mut peer, "2").await;
    assert_eq!(again, sent + 1);
    assert_eq!(
        reject,
        Message::from(Reject {
            ref_seq_num: seq + 1,
            text: Some(invalid.to_string()),
            ref_tag_id: Some(7),
            ref_msg_type: Some("2".to_owned()),
            reason: None,
        })
    );
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 2).await,
        sent + 2
    );
    drop(client);
}

// Row 9.
#[tokio::test]
async fn reject_that_cannot_be_read_is_never_answered_with_a_reject() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    peer.send_with(
        seq,
        |header| header.msg_type = "3".to_owned(),
        Heartbeat { test_req_id: None },
    )
    .await;
    assert_eq!(
        next(&mut events).await,
        Event::Unparsed(FixError::MissingField {
            msg_type: "3".to_owned(),
            tag: 45,
        })
    );
    let mut frame = Message::from(BusinessReject {
        ref_seq_num: None,
        ref_msg_type: None,
        ref_id: None,
        reason: Some(3),
        text: None,
    })
    .to_frame(peer.header(seq + 1));
    for field in &mut frame.body {
        if field.tag == 380 {
            field.value = "three".to_owned();
        }
    }
    peer.send_frame(&frame).await;
    assert_eq!(
        next(&mut events).await,
        Event::Unparsed(FixError::InvalidField {
            msg_type: "j".to_owned(),
            tag: 380,
        })
    );
    assert_eq!(answered_after(&mut peer, &mut events, seq + 2).await, 2);
    drop(client);
}

// Row 9.
#[tokio::test]
async fn reject_with_an_unreadable_header_gets_no_reply_and_no_number() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    for (at, msg_type, body) in [(seq + 1, "3", "45=1"), (seq + 2, "j", "380=3")] {
        peer.raw(&sealed(&format!(
            "35={msg_type}|49=CSERVER|56={SENDER}|34={at}|{body}|"
        )))
        .await;
        assert_eq!(
            next(&mut events).await,
            Event::Unparsed(FixError::MissingField {
                msg_type: msg_type.to_owned(),
                tag: 52,
            })
        );
    }
    for (at, msg_type) in [(seq + 3, "3"), (seq + 4, "j")] {
        peer.raw(&sealed(&format!(
            "35={msg_type}|49=CSERVER|56={SENDER}|34={at}|58=|"
        )))
        .await;
        assert!(matches!(
            next(&mut events).await,
            Event::Unparsed(FixError::Garbled(_))
        ));
    }
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 5).await,
        before + 1
    );
    drop(client);
}

const SENT: &str = "20260101-00:00:00";

fn headed(msg_type: &str, seq: u64, rest: &str) -> Vec<u8> {
    sealed(&format!(
        "35={msg_type}|49=CSERVER|56={SENDER}|34={seq}|52={SENT}|{rest}"
    ))
}

fn damaged(bytes: &[u8]) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    let at = bytes.len() - 2;
    let digit = bytes.get_mut(at).expect("digit");
    *digit = if *digit == b'9' { b'0' } else { *digit + 1 };
    bytes
}

fn unclosed(bytes: &[u8]) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    let at = bytes.len() - 2;
    *bytes.get_mut(at).expect("digit") = b'x';
    bytes
}

async fn reject_fields(peer: &mut fix_support::Peer) -> (u64, Vec<(u32, String)>) {
    let frame = peer.frame().await.expect("a reject, not the end");
    assert_eq!(frame.header.msg_type, "3");
    let mut fields: Vec<(u32, String)> = frame
        .body
        .into_iter()
        .map(|field| (field.tag, field.value))
        .collect();
    fields.sort();
    (frame.header.seq, fields)
}

struct Unusable {
    body: String,
    error: FixError,
    reject: Option<Vec<(u32, &'static str)>>,
}

fn unusable(body: &str, error: FixError, reject: Option<&[(u32, &'static str)]>) -> Unusable {
    Unusable {
        body: body.replace("{to}", SENDER).replace("{at}", SENT),
        error,
        reject: reject.map(<[(u32, &str)]>::to_vec),
    }
}

fn missing(msg_type: &str, tag: u32) -> FixError {
    FixError::MissingField {
        msg_type: msg_type.to_owned(),
        tag,
    }
}

fn invalid(msg_type: &str, tag: u32) -> FixError {
    FixError::InvalidField {
        msg_type: msg_type.to_owned(),
        tag,
    }
}

const EMPTY_VALUE: FixError = FixError::Garbled("a field has an empty value");
const NO_VALUE: FixError = FixError::Garbled("a field has no value");
const TYPE_NOT_FIRST: FixError = FixError::Garbled("the message type is not the third field");

fn unusable_in_turn() -> Vec<Unusable> {
    vec![
        // Row 9: a reject or a business message reject, unreadable in its body, its header
        // or its fields.
        unusable(
            "35=3|49=CSERVER|56={to}|34={n}|52={at}|",
            missing("3", 45),
            None,
        ),
        unusable(
            "35=j|49=CSERVER|56={to}|34={n}|52={at}|380=three|",
            invalid("j", 380),
            None,
        ),
        unusable(
            "35=3|49=CSERVER|56={to}|34={n}|45=1|",
            missing("3", 52),
            None,
        ),
        unusable("35=j|49=CSERVER|56={to}|34={n}|58=|", EMPTY_VALUE, None),
        // Row 10: a type the library does not know.
        unusable(
            "35=ZZ|49=CSERVER|56={to}|34={n}|52={at}|",
            FixError::UnsupportedMessage("ZZ".to_owned()),
            Some(&[(372, "ZZ"), (373, "11")]),
        ),
        // Row 11: a required tag is missing, in the header or in the body.
        unusable(
            "35=8|49=CSERVER|56={to}|34={n}|",
            missing("8", 52),
            Some(&[(371, "52"), (372, "8"), (373, "1")]),
        ),
        unusable(
            "35=8|56={to}|34={n}|52={at}|",
            missing("8", 49),
            Some(&[(371, "49"), (372, "8"), (373, "1")]),
        ),
        unusable(
            "35=1|49=CSERVER|56={to}|34={n}|52={at}|",
            missing("1", 112),
            Some(&[(371, "112"), (372, "1"), (373, "1")]),
        ),
        // Row 12: a value of the wrong form, or a tag of the header twice.
        unusable(
            "35=8|49=CSERVER|56={to}|34={n}|52=yesterday|",
            invalid("8", 52),
            Some(&[(371, "52"), (372, "8")]),
        ),
        unusable(
            "35=8|49=CSERVER|49=CSERVER|56={to}|34={n}|52={at}|",
            invalid("8", 49),
            Some(&[(371, "49"), (372, "8")]),
        ),
        unusable(
            "35=2|49=CSERVER|56={to}|34={n}|52={at}|7=first|16=0|",
            invalid("2", 7),
            Some(&[(371, "7"), (372, "2")]),
        ),
        // Row 13: broken fields, the type shows.
        unusable(
            "35=8|49=CSERVER|56={to}|34={n}|52={at}|58=|",
            EMPTY_VALUE,
            Some(&[(372, "8")]),
        ),
        unusable(
            "35=8|49=CSERVER|56={to}|34={n}|52={at}|58|",
            NO_VALUE,
            Some(&[(372, "8")]),
        ),
        unusable(
            "49=CSERVER|35=8|56={to}|34={n}|52={at}|",
            TYPE_NOT_FIRST,
            Some(&[(372, "8")]),
        ),
        // Row 14: broken fields, no type shows.
        unusable(
            "49=CSERVER|56={to}|34={n}|52={at}|",
            TYPE_NOT_FIRST,
            Some(&[]),
        ),
        // Row 15: broken fields, the type is empty.
        unusable("35=|49=CSERVER|56={to}|34={n}|52={at}|", EMPTY_VALUE, None),
    ]
}

// Rows 9 to 15; each case names its row.
#[tokio::test]
async fn unusable_message_in_turn_is_counted_and_its_reject_carries_the_tags_of_its_row() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let mut inbound = peer.next_seq();
    let mut outbound = answered_after(&mut peer, &mut events, inbound).await + 1;
    for case in unusable_in_turn() {
        inbound += 1;
        let number = inbound.to_string();
        let text = case.error.to_string();
        peer.raw(&sealed(&case.body.replace("{n}", &number))).await;
        assert_eq!(next(&mut events).await, Event::Unparsed(case.error));
        if let Some(named) = case.reject {
            let mut carried = vec![(45, number), (58, text)];
            carried.extend(
                named
                    .into_iter()
                    .map(|(tag, value)| (tag, value.to_owned())),
            );
            carried.sort();
            assert_eq!(reject_fields(&mut peer).await, (outbound, carried));
            outbound += 1;
        }
        inbound += 1;
        assert_eq!(
            answered_after(&mut peer, &mut events, inbound).await,
            outbound,
            "{}",
            case.body
        );
        outbound += 1;
    }
    drop(client);
}

// Row 1.
#[tokio::test]
async fn stream_that_lost_its_framing_ends_the_connection_and_is_not_answered() {
    let lost: [&[u8]; 3] = [
        b"not a message at all",
        b"8=FIX.4.4\x019=abc\x01",
        b"8=FIX.4.4\x019=4194305\x01",
    ];
    for bytes in lost {
        let (client, mut peer) = session(SessionKind::Trade).await;
        let mut events = client.events();
        peer.raw(bytes).await;
        assert!(matches!(
            next(&mut events).await,
            Event::Disconnected(TransportError::Decode(_))
        ));
        assert!(peer.frame().await.is_none());
    }
}

// Row 2.
#[tokio::test]
async fn damaged_message_is_reported_and_neither_counted_nor_answered() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    let whole = peer.bytes(seq + 1, probe("sum"));
    peer.raw(&damaged(&whole)).await;
    assert!(matches!(
        next(&mut events).await,
        Event::Unparsed(FixError::Checksum { .. })
    ));
    peer.raw(&unclosed(&whole)).await;
    assert_eq!(
        next(&mut events).await,
        Event::Unparsed(FixError::Garbled(
            "the checksum field does not close the message"
        ))
    );
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        before + 1
    );
    drop(client);
}

// Row 3.
#[tokio::test]
async fn message_without_a_single_sequence_number_is_reported_and_not_answered() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    let at = seq + 1;
    let numbered = [
        (String::new(), missing("8", 34)),
        (format!("34={at}|34={at}|"), invalid("8", 34)),
        ("34=two|".to_owned(), invalid("8", 34)),
        ("58=|".to_owned(), EMPTY_VALUE),
    ];
    for (number, error) in numbered {
        peer.raw(&sealed(&format!(
            "35=8|49=CSERVER|56={SENDER}|52={SENT}|{number}"
        )))
        .await;
        assert_eq!(next(&mut events).await, Event::Unparsed(error));
    }
    assert_eq!(answered_after(&mut peer, &mut events, at).await, before + 1);
    drop(client);
}

// Row 4.
#[tokio::test]
async fn unreadable_header_ahead_of_its_turn_opens_a_gap_and_is_not_reported() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    peer.raw(&without_sending_time(seq + 3)).await;
    assert_eq!(
        next(&mut events).await,
        Event::Gap {
            expected: seq + 1,
            received: seq + 3,
        }
    );
    let (header, message) = peer.read().await;
    let again = ResendRequest {
        begin_seq_no: seq + 1,
        end_seq_no: 0,
    };
    assert_eq!((header.seq, message), (before + 1, again.into()));
    peer.raw(&without_sending_time(seq + 4)).await;
    peer.raw(&sealed(&format!(
        "35=3|49=CSERVER|56={SENDER}|34={}|58=|",
        seq + 5
    )))
    .await;
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        before + 2
    );
    drop(client);
}

// Row 5.
#[tokio::test]
async fn unreadable_header_behind_its_turn_is_reported_and_neither_counted_nor_answered() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    let behind = [
        (format!("34={seq}|"), missing("8", 52)),
        (format!("34={seq}|43=Y|"), missing("8", 52)),
        (format!("34={seq}|52={SENT}|58=|"), EMPTY_VALUE),
    ];
    for (rest, error) in behind {
        peer.raw(&sealed(&format!("35=8|49=CSERVER|56={SENDER}|{rest}")))
            .await;
        assert_eq!(next(&mut events).await, Event::Unparsed(error));
    }
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        before + 1
    );
    drop(client);
}

// Row 6.
#[tokio::test]
async fn unreadable_body_ahead_of_its_turn_opens_a_gap_and_is_not_reported() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    peer.raw(&headed("ZZ", seq + 3, "")).await;
    assert_eq!(
        next(&mut events).await,
        Event::Gap {
            expected: seq + 1,
            received: seq + 3,
        }
    );
    let (header, message) = peer.read().await;
    let again = ResendRequest {
        begin_seq_no: seq + 1,
        end_seq_no: 0,
    };
    assert_eq!((header.seq, message), (before + 1, again.into()));
    peer.raw(&headed("1", seq + 4, "")).await;
    peer.raw(&headed("3", seq + 5, "")).await;
    peer.raw(&headed("j", seq + 6, "380=three|")).await;
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        before + 2
    );
    drop(client);
}

// Row 7.
#[tokio::test]
async fn repetition_behind_its_turn_is_dropped_without_an_event_or_an_answer() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let seq = peer.next_seq();
    let before = answered_after(&mut peer, &mut events, seq).await;
    peer.raw(&headed("ZZ", seq, "43=Y|")).await;
    peer.raw(&headed("1", seq, "43=Y|")).await;
    peer.raw(&headed("3", seq, "43=Y|")).await;
    peer.raw(&headed("1", seq, "43=Y|112=again|")).await;
    assert_eq!(
        answered_after(&mut peer, &mut events, seq + 1).await,
        before + 1
    );
    drop(client);
}

// Row 8.
#[tokio::test]
async fn message_behind_its_turn_that_is_no_repetition_ends_the_connection_unanswered() {
    for (msg_type, rest) in [("ZZ", ""), ("1", ""), ("3", ""), ("1", "112=stale|")] {
        let (client, mut peer) = session(SessionKind::Trade).await;
        let mut events = client.events();
        peer.raw(&headed(msg_type, 1, rest)).await;
        let lost = next(&mut events).await;
        assert!(
            matches!(&lost, Event::Disconnected(TransportError::Decode(text)) if text.contains("below the expected 2")),
            "{lost:?}"
        );
        assert!(peer.frame().await.is_none());
    }
}

fn unusable_while_leaving(seq: u64) -> Vec<(Vec<u8>, Option<FixError>)> {
    let unsupported = || Some(FixError::UnsupportedMessage("ZZ".to_owned()));
    vec![
        (damaged(&headed("0", seq, "")), None),
        (
            sealed(&format!("35=8|49=CSERVER|56={SENDER}|52={SENT}|")),
            Some(missing("8", 34)),
        ),
        (headed("ZZ", seq, ""), unsupported()),
        (headed("ZZ", seq + 50, ""), unsupported()),
        (headed("ZZ", 1, ""), unsupported()),
        (headed("1", seq, ""), Some(missing("1", 112))),
        (headed("2", seq, "7=first|16=0|"), Some(invalid("2", 7))),
        (headed("3", seq, ""), Some(missing("3", 45))),
        (without_sending_time(seq), Some(missing("8", 52))),
        (without_sending_time(seq + 50), Some(missing("8", 52))),
        (without_sending_time(1), Some(missing("8", 52))),
        (headed("8", seq, "58=|"), Some(EMPTY_VALUE)),
    ]
}

// Row 16.
#[tokio::test]
async fn unusable_message_during_the_logout_is_reported_and_never_answered() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let mut events = client.events();
    let cases = unusable_while_leaving(peer.next_seq());
    let expected: Vec<Option<FixError>> = cases.iter().map(|(_, error)| error.clone()).collect();
    let (left, written) = tokio::join!(client.logout(), async {
        let (header, message) = peer.request().await;
        assert_eq!(message, Logout { text: None }.into());
        for (bytes, _) in &cases {
            peer.raw(bytes).await;
        }
        peer.send(Logout { text: None }).await;
        let mut written = vec![header.seq];
        while let Some(frame) = peer.frame().await {
            written.push(frame.header.seq);
        }
        written
    });
    assert_eq!(left, Ok(()));
    assert_eq!(written, [2]);
    for error in expected {
        match (next(&mut events).await, error) {
            (Event::Unparsed(FixError::Checksum { .. }), None) => {}
            (event, error) => assert_eq!(Some(event), error.map(Event::Unparsed)),
        }
    }
    timeout(STEP, client.closed())
        .await
        .expect("closed in time");
}

// Row 17.
#[tokio::test]
async fn stream_that_loses_its_framing_during_the_logout_fails_the_logout() {
    let (client, mut peer) = session(SessionKind::Trade).await;
    let (left, written) = tokio::join!(client.logout(), async {
        let (header, message) = peer.request().await;
        assert_eq!(message, Logout { text: None }.into());
        peer.raw(b"not a message at all").await;
        let mut written = vec![header.seq];
        while let Some(frame) = peer.frame().await {
            written.push(frame.header.seq);
        }
        written
    });
    assert!(
        matches!(
            left,
            Err(ConnectorError::Transport(TransportError::Decode(_)))
        ),
        "{left:?}"
    );
    assert_eq!(written, [2]);
}

fn unusable_logon_answers() -> Vec<(Vec<u8>, Option<FixError>)> {
    vec![
        (damaged(&headed("A", 1, "98=0|108=1|")), None),
        (
            sealed(&format!(
                "35=A|49=CSERVER|56={SENDER}|52={SENT}|98=0|108=1|"
            )),
            Some(missing("A", 34)),
        ),
        (
            sealed(&format!("35=A|49=CSERVER|56={SENDER}|34=1|98=0|108=1|")),
            Some(missing("A", 52)),
        ),
        (headed("A", 1, "98=0|108=|"), Some(EMPTY_VALUE)),
        (headed("A", 1, "98=0|"), Some(missing("A", 108))),
        (headed("A", 1, "98=0|108=soon|"), Some(invalid("A", 108))),
        (
            headed("ZZ", 1, ""),
            Some(FixError::UnsupportedMessage("ZZ".to_owned())),
        ),
        (headed("3", 1, ""), Some(missing("3", 45))),
    ]
}

// Row 18.
#[tokio::test]
async fn unusable_answer_to_the_logon_fails_the_logon_and_is_not_answered() {
    for (bytes, error) in unusable_logon_answers() {
        let server = Server::bind().await;
        let (client, written) = tokio::join!(
            FixClient::connect(server.configuration(), SessionKind::Trade),
            async {
                let mut peer = server.accept().await;
                let (header, message) = peer.read().await;
                assert!(matches!(message, Message::Logon(_)));
                peer.raw(&bytes).await;
                let mut written = vec![header.seq];
                while let Some(frame) = peer.frame().await {
                    written.push(frame.header.seq);
                }
                written
            }
        );
        match (client.expect_err("no logon"), error) {
            (ConnectorError::Fix(FixError::Checksum { .. }), None) => {}
            (failed, error) => assert_eq!(Some(failed), error.map(ConnectorError::Fix)),
        }
        assert_eq!(written, [1]);
    }
}

// Row 18, logging on again.
#[tokio::test]
async fn unusable_answer_to_a_later_logon_fails_that_attempt_and_the_next_one_follows() {
    let server = Server::bind().await;
    let (client, peer) = tokio::join!(
        FixClient::connect(server.configuration(), SessionKind::Trade),
        async {
            let mut peer = server.accept().await;
            peer.logon().await;
            peer
        }
    );
    let client = client.expect("logon");
    let mut events = client.events();
    drop(peer);
    let mut peer = server.accept().await;
    let (header, message) = peer.read().await;
    assert!(matches!(message, Message::Logon(_)));
    peer.raw(&headed("ZZ", 1, "")).await;
    let mut written = vec![header.seq];
    while let Some(frame) = peer.frame().await {
        written.push(frame.header.seq);
    }
    assert_eq!(written, [1]);
    assert_eq!(
        next(&mut events).await,
        Event::Disconnected(TransportError::Closed)
    );
    assert_eq!(
        next(&mut events).await,
        Event::ReconnectFailed {
            attempt: 1,
            error: FixError::UnsupportedMessage("ZZ".to_owned()).into(),
        }
    );
    let mut peer = server.accept().await;
    peer.logon().await;
    assert_eq!(next(&mut events).await, Event::Reconnected { attempt: 2 });
    drop(client);
}
