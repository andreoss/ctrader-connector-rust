//! Tests of the FIX sessions against the demo server.
//!
//! They need the variables of the FIX sessions in the environment and run one at a time.

use std::time::Duration;

use ctrader_connector::common::config::ConfigurationFix;
use ctrader_connector::common::errors::ConnectorError;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::messages::Message;
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
