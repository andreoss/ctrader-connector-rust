//! Scripted server for the tests of the FIX sessions.
//!
//! The server listens on a port chosen by the operating system and speaks through the codec
//! of the crate; every test scripts what it reads and what it answers.

#![allow(dead_code)]

use std::time::Duration;

use ctrader_connector::common::config::{ConfigurationFix, ConfigurationFixBuilder};
use ctrader_connector::common::time::Timestamp;
use ctrader_connector::fix::client::FixClient;
use ctrader_connector::fix::codec::{Field, Frame, Header, decode, encode};
use ctrader_connector::fix::messages::{Logon, Message};
use ctrader_connector::fix::session::SessionKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// Sender the scripted clients use.
pub const SENDER: &str = "demo.broker.4242";
/// Login the scripted clients use.
pub const USERNAME: &str = "4242";
/// Password the scripted clients use.
pub const PASSWORD: &str = "open-sesame";
/// Longest wait for anything a test expects.
pub const STEP: Duration = Duration::from_secs(8);

/// Logs a client on to a scripted server and returns both ends of the session.
pub async fn session(kind: SessionKind) -> (FixClient, Peer) {
    let server = Server::bind().await;
    let configuration = server.configuration();
    let (client, peer) = tokio::join!(FixClient::connect(configuration, kind), async {
        let mut peer = server.accept().await;
        peer.logon().await;
        peer
    });
    (client.expect("logon"), peer)
}

/// Reads a message of the given type from fields written as `tag=value` and joined by bars.
pub fn wire(msg_type: &str, fields: &str) -> Message {
    let body: Vec<Field> = fields
        .split('|')
        .map(|field| {
            let (tag, value) = field.split_once('=').expect("tag and value");
            Field::new(tag.parse().expect("tag"), value)
        })
        .collect();
    Message::parse(msg_type, &body).expect("message")
}

/// Listening side of the scripted server.
pub struct Server {
    listener: TcpListener,
    port: u16,
}

impl Server {
    /// Binds a port chosen by the operating system.
    pub async fn bind() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        Self { listener, port }
    }

    /// Starts a configuration that reaches the server without encryption.
    pub fn builder(&self) -> ConfigurationFixBuilder {
        ConfigurationFix::builder()
            .unencrypted()
            .host("127.0.0.1")
            .quote_port(self.port)
            .trade_port(self.port)
            .sender_comp_id(SENDER)
            .username(USERNAME)
            .password(PASSWORD)
            .heartbeat(Duration::from_secs(1))
            .timeout(Duration::from_secs(2))
            .reconnect_delay(Duration::from_millis(50))
    }

    /// Returns a configuration that reaches the server without encryption.
    pub fn configuration(&self) -> ConfigurationFix {
        self.builder().build().expect("configuration")
    }

    /// Waits for the next connection.
    pub async fn accept(&self) -> Peer {
        let (stream, _) = timeout(STEP, self.listener.accept())
            .await
            .expect("a connection in time")
            .expect("accept");
        Peer {
            stream,
            buffer: Vec::new(),
            seq: 1,
            qualifier: String::new(),
        }
    }
}

/// One connection of the scripted server.
pub struct Peer {
    stream: TcpStream,
    buffer: Vec<u8>,
    seq: u64,
    qualifier: String,
}

impl Peer {
    /// Reads the next frame; `None` once the client has closed the connection.
    pub async fn frame(&mut self) -> Option<Frame> {
        loop {
            if let Some(frame) = decode(&mut self.buffer).expect("frame of the client") {
                return Some(frame);
            }
            let mut chunk = [0_u8; 4096];
            let read = timeout(STEP, self.stream.read(&mut chunk))
                .await
                .expect("bytes in time")
                .unwrap_or(0);
            if read == 0 {
                return None;
            }
            self.buffer
                .extend_from_slice(chunk.get(..read).expect("chunk"));
        }
    }

    /// Reads the next message with its header.
    pub async fn read(&mut self) -> (Header, Message) {
        let frame = self.frame().await.expect("a message, not the end");
        let message = Message::from_frame(&frame).expect("message of the client");
        (frame.header, message)
    }

    /// Reads the next message that is not a heartbeat.
    pub async fn request(&mut self) -> (Header, Message) {
        loop {
            let (header, message) = self.read().await;
            if !matches!(message, Message::Heartbeat(_)) {
                return (header, message);
            }
        }
    }

    /// Returns the sequence number the next message of the server gets.
    pub const fn next_seq(&self) -> u64 {
        self.seq
    }

    /// Sets the sequence number the next message of the server gets.
    pub const fn set_next_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    fn header(&self, seq: u64) -> Header {
        let header = Header::new("", "CSERVER", SENDER, seq, Timestamp::now());
        if self.qualifier.is_empty() {
            header
        } else {
            header.qualified(&self.qualifier, &self.qualifier)
        }
    }

    /// Writes bytes as they are.
    pub async fn raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("write");
        self.stream.flush().await.expect("flush");
    }

    /// Sends a message under the next sequence number.
    pub async fn send(&mut self, message: impl Into<Message>) {
        let header = self.header(self.seq);
        self.seq += 1;
        self.send_as(header, message).await;
    }

    /// Sends a message under a header changed by `change`, without counting it.
    pub async fn send_with(
        &mut self,
        seq: u64,
        change: impl FnOnce(&mut Header),
        message: impl Into<Message>,
    ) {
        let mut frame = message.into().to_frame(self.header(seq));
        change(&mut frame.header);
        self.send_frame(&frame).await;
    }

    /// Returns the bytes of a message under the given sequence number, without sending it.
    pub fn bytes(&self, seq: u64, message: impl Into<Message>) -> Vec<u8> {
        encode(&message.into().to_frame(self.header(seq))).expect("encode")
    }

    /// Sends a frame as it is.
    pub async fn send_frame(&mut self, frame: &Frame) {
        let bytes = encode(frame).expect("encode");
        self.raw(&bytes).await;
    }

    async fn send_as(&mut self, header: Header, message: impl Into<Message>) {
        let frame = message.into().to_frame(header);
        self.send_frame(&frame).await;
    }

    /// Reads the logon of the client and accepts it.
    pub async fn logon(&mut self) -> (Header, Logon) {
        let (header, message) = self.read().await;
        let Message::Logon(logon) = message else {
            panic!("expected a logon, got {message:?}");
        };
        self.qualifier = header.sender_sub_id.clone().unwrap_or_default();
        self.send(Logon {
            encrypt_method: 0,
            heart_bt_int: logon.heart_bt_int,
            reset_seq_num: Some(true),
            username: None,
            password: None,
        })
        .await;
        (header, logon)
    }
}
