//! Mock Open API server for tests: it listens on a port chosen by the system, speaks the framed
//! protocol over plain TCP and answers the requests the client layer sends.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use ctrader_connector::open_api::messages::{
    ProtoErrorRes, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
    ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes, ProtoOaCtidTraderAccount,
    ProtoOaErrorRes, ProtoOaGetAccountListByAccessTokenReq, ProtoOaGetAccountListByAccessTokenRes,
    ProtoOaRefreshTokenReq, ProtoOaRefreshTokenRes, ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq,
    ProtoOaSubscribeSpotsRes, ProtoOaUnsubscribeSpotsReq, ProtoOaUnsubscribeSpotsRes,
    ProtoOaVersionRes,
};
use ctrader_connector::open_api::transport::Framed;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

pub const HEARTBEAT: u32 = 51;
pub const ERROR: u32 = 50;
pub const APPLICATION_AUTH_REQ: u32 = 2100;
pub const APPLICATION_AUTH_RES: u32 = 2101;
pub const ACCOUNT_AUTH_REQ: u32 = 2102;
pub const ACCOUNT_AUTH_RES: u32 = 2103;
pub const VERSION_REQ: u32 = 2104;
pub const VERSION_RES: u32 = 2105;
pub const SUBSCRIBE_SPOTS_REQ: u32 = 2127;
pub const SUBSCRIBE_SPOTS_RES: u32 = 2128;
pub const UNSUBSCRIBE_SPOTS_REQ: u32 = 2129;
pub const UNSUBSCRIBE_SPOTS_RES: u32 = 2130;
pub const SPOT_EVENT: u32 = 2131;
pub const OA_ERROR: u32 = 2142;
pub const ACCOUNT_LIST_REQ: u32 = 2149;
pub const ACCOUNT_LIST_RES: u32 = 2150;
pub const REFRESH_TOKEN_REQ: u32 = 2173;
pub const REFRESH_TOKEN_RES: u32 = 2174;

pub const CLIENT_ID: &str = "mock-client";
pub const CLIENT_SECRET: &str = "mock-secret";
pub const ACCESS_TOKEN: &str = "mock-access-0";
pub const REFRESH_TOKEN: &str = "mock-refresh-0";
pub const HOST: &str = "127.0.0.1";

/// Behaviour of the mock server.
#[derive(Debug, Clone)]
pub struct MockOptions {
    pub accounts: Vec<i64>,
    pub limit_per_second: Option<usize>,
    pub reverse_batch: usize,
    pub silent: Vec<u32>,
}

impl Default for MockOptions {
    fn default() -> Self {
        Self {
            accounts: vec![101, 102],
            limit_per_second: None,
            reverse_batch: 0,
            silent: Vec::new(),
        }
    }
}

/// One message the server received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub connection: usize,
    pub payload_type: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
enum Control {
    Drop,
    Push(ProtoMessage),
}

#[derive(Debug)]
struct State {
    options: MockOptions,
    received: Vec<Received>,
    connections: usize,
    open: usize,
    generation: u32,
    held: Vec<ProtoMessage>,
    recent: VecDeque<Instant>,
    refuse: bool,
    limited: usize,
}

/// Handle of a running mock server.
#[derive(Debug)]
pub struct MockServer {
    address: SocketAddr,
    state: Arc<Mutex<State>>,
    controls: broadcast::Sender<Control>,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = self.controls.send(Control::Drop);
    }
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

pub fn envelope<M: Message>(payload_type: u32, body: &M, id: Option<String>) -> ProtoMessage {
    ProtoMessage {
        payload_type,
        payload: Some(body.encode_to_vec()),
        client_msg_id: id,
    }
}

fn refusal(code: &str, description: &str, id: Option<String>) -> ProtoMessage {
    envelope(
        OA_ERROR,
        &ProtoOaErrorRes {
            payload_type: None,
            ctid_trader_account_id: None,
            error_code: code.to_owned(),
            description: Some(description.to_owned()),
            maintenance_end_timestamp: None,
        },
        id,
    )
}

impl MockServer {
    pub async fn start(options: MockOptions) -> Self {
        let listener = TcpListener::bind((HOST, 0)).await.expect("bind port zero");
        let address = listener.local_addr().expect("announced address");
        let state = Arc::new(Mutex::new(State {
            options,
            received: Vec::new(),
            connections: 0,
            open: 0,
            generation: 0,
            held: Vec::new(),
            recent: VecDeque::new(),
            refuse: false,
            limited: 0,
        }));
        let (controls, _) = broadcast::channel(256);
        let task = tokio::spawn(accept(listener, Arc::clone(&state), controls.clone()));
        Self {
            address,
            state,
            controls,
            task,
        }
    }

    pub fn port(&self) -> u16 {
        self.address.port()
    }

    pub fn received(&self) -> Vec<Received> {
        lock(&self.state).received.clone()
    }

    pub fn count(&self, payload_type: u32) -> usize {
        lock(&self.state)
            .received
            .iter()
            .filter(|received| received.payload_type == payload_type)
            .count()
    }

    pub fn types_on(&self, connection: usize) -> Vec<u32> {
        lock(&self.state)
            .received
            .iter()
            .filter(|received| received.connection == connection)
            .map(|received| received.payload_type)
            .filter(|payload_type| *payload_type != HEARTBEAT)
            .collect()
    }

    pub fn connections(&self) -> usize {
        lock(&self.state).connections
    }

    pub fn open_connections(&self) -> usize {
        lock(&self.state).open
    }

    pub fn limited(&self) -> usize {
        lock(&self.state).limited
    }

    pub fn access_token(&self) -> String {
        format!("mock-access-{}", lock(&self.state).generation)
    }

    pub fn refuse_connections(&self, refuse: bool) {
        lock(&self.state).refuse = refuse;
    }

    pub fn set_limit(&self, limit: Option<usize>) {
        lock(&self.state).options.limit_per_second = limit;
    }

    pub fn drop_connections(&self) {
        let _ = self.controls.send(Control::Drop);
    }

    pub fn push(&self, message: ProtoMessage) {
        let _ = self.controls.send(Control::Push(message));
    }

    pub fn push_spot(&self, account: i64, symbol: i64, bid: u64) {
        self.push(envelope(
            SPOT_EVENT,
            &ProtoOaSpotEvent {
                payload_type: None,
                ctid_trader_account_id: account,
                symbol_id: symbol,
                bid: Some(bid),
                ask: None,
                trendbar: Vec::new(),
                session_close: None,
                timestamp: None,
            },
            None,
        ));
    }
}

async fn accept(
    listener: TcpListener,
    state: Arc<Mutex<State>>,
    controls: broadcast::Sender<Control>,
) {
    while let Ok((stream, _)) = listener.accept().await {
        let number = {
            let mut state = lock(&state);
            if state.refuse {
                continue;
            }
            state.connections += 1;
            state.open += 1;
            state.connections
        };
        tokio::spawn(serve(
            stream,
            number,
            Arc::clone(&state),
            controls.subscribe(),
        ));
    }
}

async fn serve(
    stream: TcpStream,
    number: usize,
    state: Arc<Mutex<State>>,
    mut controls: broadcast::Receiver<Control>,
) {
    let mut framed = Framed::new(stream);
    let mut authorised = false;
    loop {
        tokio::select! {
            incoming = framed.receive() => {
                let Ok(message) = incoming else { break };
                let replies = lock(&state).handle(number, &mut authorised, message);
                for reply in replies {
                    if framed.send(&reply).await.is_err() {
                        break;
                    }
                }
            }
            control = controls.recv() => match control {
                Ok(Control::Push(message)) => {
                    if framed.send(&message).await.is_err() {
                        break;
                    }
                }
                Ok(Control::Drop) | Err(_) => break,
            },
        }
    }
    lock(&state).open -= 1;
}

impl State {
    fn over_limit(&mut self) -> bool {
        let Some(limit) = self.options.limit_per_second else {
            return false;
        };
        let now = Instant::now();
        while self
            .recent
            .front()
            .is_some_and(|sent| now.duration_since(*sent) >= Duration::from_secs(1))
        {
            self.recent.pop_front();
        }
        if self.recent.len() >= limit {
            self.limited += 1;
            true
        } else {
            self.recent.push_back(now);
            false
        }
    }

    fn handle(
        &mut self,
        connection: usize,
        authorised: &mut bool,
        message: ProtoMessage,
    ) -> Vec<ProtoMessage> {
        let payload = message.payload.unwrap_or_default();
        let id = message.client_msg_id;
        self.received.push(Received {
            connection,
            payload_type: message.payload_type,
            payload: payload.clone(),
        });
        if message.payload_type == HEARTBEAT || self.options.silent.contains(&message.payload_type)
        {
            return Vec::new();
        }
        if self.over_limit() {
            return vec![refusal(
                "REQUEST_FREQUENCY_EXCEEDED",
                "too many requests",
                id,
            )];
        }
        let reply = match message.payload_type {
            APPLICATION_AUTH_REQ => {
                let request = ProtoOaApplicationAuthReq::decode(payload.as_slice());
                match request {
                    Ok(request)
                        if request.client_id == CLIENT_ID
                            && request.client_secret == CLIENT_SECRET =>
                    {
                        *authorised = true;
                        envelope(
                            APPLICATION_AUTH_RES,
                            &ProtoOaApplicationAuthRes { payload_type: None },
                            id,
                        )
                    }
                    _ => refusal("CH_CLIENT_AUTH_FAILURE", "bad application credentials", id),
                }
            }
            _ if !*authorised => refusal(
                "CH_CLIENT_NOT_AUTHENTICATED",
                "application is not authorised",
                id,
            ),
            other => {
                let reply = self.session_reply(other, &payload, id);
                if self.options.reverse_batch > 0 {
                    self.held.push(reply);
                    if self.held.len() < self.options.reverse_batch {
                        return Vec::new();
                    }
                    let mut held = std::mem::take(&mut self.held);
                    held.reverse();
                    return held;
                }
                reply
            }
        };
        vec![reply]
    }

    #[allow(clippy::too_many_lines)]
    fn session_reply(
        &mut self,
        payload_type: u32,
        payload: &[u8],
        id: Option<String>,
    ) -> ProtoMessage {
        match payload_type {
            VERSION_REQ => envelope(
                VERSION_RES,
                &ProtoOaVersionRes {
                    payload_type: None,
                    version: "mock-1".to_owned(),
                },
                id,
            ),
            ACCOUNT_LIST_REQ => match ProtoOaGetAccountListByAccessTokenReq::decode(payload) {
                Ok(request) if request.access_token == self.token() => envelope(
                    ACCOUNT_LIST_RES,
                    &ProtoOaGetAccountListByAccessTokenRes {
                        payload_type: None,
                        access_token: request.access_token,
                        permission_scope: None,
                        ctid_trader_account: self
                            .options
                            .accounts
                            .iter()
                            .filter_map(|id| u64::try_from(*id).ok())
                            .map(|id| ProtoOaCtidTraderAccount {
                                ctid_trader_account_id: id,
                                is_live: Some(false),
                                trader_login: Some(7),
                                last_closing_deal_timestamp: None,
                                last_balance_update_timestamp: None,
                            })
                            .collect(),
                    },
                    id,
                ),
                _ => refusal("CH_ACCESS_TOKEN_INVALID", "unknown token", id),
            },
            ACCOUNT_AUTH_REQ => match ProtoOaAccountAuthReq::decode(payload) {
                Ok(request) if request.access_token != self.token() => {
                    refusal("CH_ACCESS_TOKEN_INVALID", "unknown token", id)
                }
                Ok(request)
                    if self
                        .options
                        .accounts
                        .contains(&request.ctid_trader_account_id) =>
                {
                    envelope(
                        ACCOUNT_AUTH_RES,
                        &ProtoOaAccountAuthRes {
                            payload_type: None,
                            ctid_trader_account_id: request.ctid_trader_account_id,
                        },
                        id,
                    )
                }
                _ => refusal("CH_CTID_TRADER_ACCOUNT_NOT_FOUND", "unknown account", id),
            },
            REFRESH_TOKEN_REQ => match ProtoOaRefreshTokenReq::decode(payload) {
                Ok(request)
                    if request.refresh_token == format!("mock-refresh-{}", self.generation) =>
                {
                    self.generation += 1;
                    envelope(
                        REFRESH_TOKEN_RES,
                        &ProtoOaRefreshTokenRes {
                            payload_type: None,
                            access_token: self.token(),
                            token_type: "bearer".to_owned(),
                            expires_in: 2_628_000,
                            refresh_token: format!("mock-refresh-{}", self.generation),
                        },
                        id,
                    )
                }
                _ => refusal("ACCESS_DENIED", "unknown refresh token", id),
            },
            SUBSCRIBE_SPOTS_REQ => match ProtoOaSubscribeSpotsReq::decode(payload) {
                Ok(request) => envelope(
                    SUBSCRIBE_SPOTS_RES,
                    &ProtoOaSubscribeSpotsRes {
                        payload_type: None,
                        ctid_trader_account_id: request.ctid_trader_account_id,
                    },
                    id,
                ),
                Err(_) => refusal("INVALID_REQUEST", "malformed", id),
            },
            UNSUBSCRIBE_SPOTS_REQ => match ProtoOaUnsubscribeSpotsReq::decode(payload) {
                Ok(request) => envelope(
                    UNSUBSCRIBE_SPOTS_RES,
                    &ProtoOaUnsubscribeSpotsRes {
                        payload_type: None,
                        ctid_trader_account_id: request.ctid_trader_account_id,
                    },
                    id,
                ),
                Err(_) => refusal("INVALID_REQUEST", "malformed", id),
            },
            _ => envelope(
                ERROR,
                &ProtoErrorRes {
                    payload_type: None,
                    error_code: "UNSUPPORTED_MESSAGE".to_owned(),
                    description: None,
                    maintenance_end_timestamp: None,
                },
                id,
            ),
        }
    }

    fn token(&self) -> String {
        format!("mock-access-{}", self.generation)
    }
}

pub struct HttpMock {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl HttpMock {
    pub async fn start(response: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let response = response.to_owned();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut byte = [0_u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte).await {
                        Ok(1) => head.push(byte[0]),
                        _ => break,
                    }
                }
                seen.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(String::from_utf8_lossy(&head).into_owned());
                if response.is_empty() {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                } else {
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                }
            }
        });
        Self {
            address,
            requests,
            task,
        }
    }

    pub fn url(&self) -> String {
        format!("https://{}/apps/token", self.address)
    }

    pub fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Drop for HttpMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
