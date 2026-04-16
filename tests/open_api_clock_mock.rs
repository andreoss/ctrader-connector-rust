//! Timing of the Open API connection, on a clock the test controls.
//!
//! The connection runs over a stream held in memory, so that nothing but the timers of the
//! connection moves the paused clock.

use std::sync::Mutex;
use std::time::Duration;

use ctrader_connector::common::errors::TransportError;
use ctrader_connector::common::transport::Connect;
use ctrader_connector::open_api::connection::{Connection, ConnectionOptions};
use ctrader_connector::open_api::messages::{ProtoMessage, ProtoOaVersionReq, ProtoOaVersionRes};
use ctrader_connector::open_api::transport::Framed;
use prost::Message;
use tokio::io::{DuplexStream, duplex};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::{Instant, sleep};

const VERSION_REQ: u32 = 2104;
const VERSION_RES: u32 = 2105;
const YEAR: Duration = Duration::from_secs(365 * 86_400);
const YEARS: u32 = 62;

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

#[tokio::test(start_paused = true)]
async fn waits_set_to_never_are_not_acted_on_however_far_the_clock_runs() {
    let (near, far) = duplex(64 * 1024);
    let options = ConnectionOptions::new()
        .heartbeat_interval(Duration::MAX)
        .inbound_timeout(Duration::MAX)
        .timeout(Duration::MAX)
        .reconnect_attempts(Some(0));
    let connector = Memory(Mutex::new(Some(near)));
    let connection = Connection::open(connector, "server.invalid", 1, options)
        .await
        .expect("open");
    let mut events = connection.events();
    let mut server = Framed::new(far);
    let started = Instant::now();
    let asking = connection.clone();
    let pending = tokio::spawn(async move {
        asking
            .request_message(VERSION_REQ, ProtoOaVersionReq::default().encode_to_vec())
            .await
    });
    let request = server.receive().await.expect("request");
    assert_eq!(request.payload_type, VERSION_REQ);
    for _ in 0..YEARS {
        sleep(YEAR).await;
        assert_eq!(connection.stats().heartbeats_sent, 0);
        assert_eq!(events.try_recv(), Err(TryRecvError::Empty));
        assert!(!pending.is_finished());
    }
    let version = ProtoOaVersionRes {
        payload_type: None,
        version: "99".to_owned(),
    };
    server
        .send(&ProtoMessage {
            payload_type: VERSION_RES,
            payload: Some(version.encode_to_vec()),
            client_msg_id: request.client_msg_id,
        })
        .await
        .expect("answer");
    let answer = pending.await.expect("task").expect("answer");
    assert_eq!(answer.payload_type, VERSION_RES);
    assert_eq!(started.elapsed(), YEAR * YEARS);
    assert_eq!(connection.stats().reconnects, 0);
    connection.shutdown().await;
}
