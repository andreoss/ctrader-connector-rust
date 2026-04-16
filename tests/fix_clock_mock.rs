//! Timing of the FIX session keep-alive, on a clock the test controls.
//!
//! The session runs over a stream held in memory, so that nothing but the timers of the
//! session moves the paused clock.

use std::sync::Mutex;
use std::time::Duration;

use ctrader_connector::common::config::{ConfigurationFix, DEFAULT_TIMEOUT, MAX_FIX_HEARTBEAT};
use ctrader_connector::common::errors::TransportError;
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::common::transport::Connect;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::codec::{Header, decode, encode};
use ctrader_connector::fix::messages::{Heartbeat, Logon, Message, ResendRequest, TestRequest};
use ctrader_connector::fix::session::SessionKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::time::{Instant, sleep};

const SENDER: &str = "demo.broker.4242";
const HEARTBEAT: Duration = Duration::from_secs(30);

struct Memory(Mutex<Option<DuplexStream>>);

impl Connect for Memory {
    type Stream = DuplexStream;

    async fn connect(&self, _host: &str, _port: u16) -> Result<Self::Stream, TransportError> {
        self.0
            .lock()
            .ok()
            .and_then(|mut stream| stream.take())
            .ok_or(TransportError::Closed)
    }
}

struct Peer {
    stream: DuplexStream,
    buffer: Vec<u8>,
    seq: u64,
}

impl Peer {
    async fn read(&mut self) -> (Header, Message, Instant) {
        loop {
            if let Some(frame) = decode(&mut self.buffer).expect("frame of the client") {
                let message = Message::from_frame(&frame).expect("message of the client");
                return (frame.header, message, Instant::now());
            }
            let mut chunk = [0_u8; 4096];
            let read = self.stream.read(&mut chunk).await.expect("read");
            assert!(read > 0, "the client closed the stream");
            self.buffer
                .extend_from_slice(chunk.get(..read).expect("chunk"));
        }
    }

    async fn send(&mut self, message: impl Into<Message>) {
        let header = Header::new("", "CSERVER", SENDER, self.seq, Timestamp::now())
            .qualified("TRADE", "TRADE");
        self.seq += 1;
        let bytes = encode(&message.into().to_frame(header)).expect("encode");
        self.stream.write_all(&bytes).await.expect("write");
    }
}

async fn session() -> (FixClient, Peer) {
    session_with(HEARTBEAT, DEFAULT_TIMEOUT).await
}

async fn session_with(heartbeat: Duration, patience: Duration) -> (FixClient, Peer) {
    let (near, far) = duplex(64 * 1024);
    let configuration = ConfigurationFix::builder()
        .host("server.invalid")
        .sender_comp_id(SENDER)
        .username("4242")
        .password("open-sesame")
        .heartbeat(heartbeat)
        .timeout(patience)
        .build()
        .expect("configuration");
    let mut peer = Peer {
        stream: far,
        buffer: Vec::new(),
        seq: 1,
    };
    let connector = Memory(Mutex::new(Some(near)));
    let (client, ()) = tokio::join!(
        FixClient::connect_with(configuration, SessionKind::Trade, connector),
        async {
            let (_, logon, _) = peer.read().await;
            assert!(matches!(logon, Message::Logon(_)), "{logon:?}");
            peer.send(Logon {
                encrypt_method: 0,
                heart_bt_int: u32::try_from(heartbeat.as_secs()).expect("seconds"),
                reset_seq_num: Some(true),
                username: None,
                password: None,
            })
            .await;
        }
    );
    (client.expect("logon"), peer)
}

#[tokio::test(start_paused = true)]
async fn heartbeat_is_sent_when_the_interval_ends_and_a_test_request_is_answered_at_once() {
    let (client, mut peer) = session().await;
    let offset = Duration::from_millis(300);
    sleep(offset).await;
    let asked = Instant::now();
    peer.send(TestRequest {
        test_req_id: "probe".to_owned(),
    })
    .await;
    let (header, answer, answered) = peer.read().await;
    assert_eq!(
        answer,
        Message::from(Heartbeat {
            test_req_id: Some("probe".to_owned()),
        })
    );
    assert_eq!(header.seq, 2);
    assert_eq!(answered.duration_since(asked), Duration::ZERO);
    for seq in 3..6 {
        let before = Instant::now();
        peer.send(Heartbeat { test_req_id: None }).await;
        let (header, beat, at) = peer.read().await;
        assert_eq!(beat, Message::from(Heartbeat { test_req_id: None }));
        assert_eq!(header.seq, seq);
        assert_eq!(at.duration_since(before), HEARTBEAT);
    }
    let silent_from = Instant::now();
    peer.send(Heartbeat { test_req_id: None }).await;
    let (_, beat, at) = peer.read().await;
    assert_eq!(beat, Message::from(Heartbeat { test_req_id: None }));
    assert_eq!(at.duration_since(silent_from), HEARTBEAT);
    let (_, probe, at) = peer.read().await;
    assert!(matches!(probe, Message::TestRequest(_)), "{probe:?}");
    assert_eq!(at.duration_since(silent_from), HEARTBEAT * 3 / 2);
    drop(client);
}

#[tokio::test(start_paused = true)]
async fn session_kept_alive_past_a_wait_that_never_ends_neither_hangs_nor_gives_up() {
    let (client, mut peer) = session_with(MAX_FIX_HEARTBEAT, Duration::MAX).await;
    let started = Instant::now();
    peer.seq += 1;
    peer.send(Heartbeat { test_req_id: None }).await;
    let (_, asked, at) = peer.read().await;
    assert_eq!(
        asked,
        Message::from(ResendRequest {
            begin_seq_no: 2,
            end_seq_no: 0,
        })
    );
    assert_eq!(at, started);
    let rounds = 366 * 24;
    for _ in 0..rounds {
        let (_, beat, _) = peer.read().await;
        assert_eq!(beat, Message::from(Heartbeat { test_req_id: None }));
        peer.send(Heartbeat { test_req_id: None }).await;
    }
    assert_eq!(started.elapsed(), MAX_FIX_HEARTBEAT * rounds);
    assert!(started.elapsed() > Duration::from_secs(365 * 86_400));
    drop(client);
}
