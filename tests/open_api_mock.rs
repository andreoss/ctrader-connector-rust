//! Behaviour of the Open API client against the mock server.

mod support;

use ctrader_connector::common::errors::TransportError;
use ctrader_connector::open_api::messages::{ProtoOaApplicationAuthReq, ProtoOaVersionReq};
use ctrader_connector::open_api::transport::{Connect, Framed, Plain, Tls};
use support::{
    APPLICATION_AUTH_REQ, APPLICATION_AUTH_RES, CLIENT_ID, CLIENT_SECRET, HOST, MockOptions,
    MockServer, OA_ERROR, VERSION_REQ, envelope,
};

#[tokio::test]
async fn framed_round_trip_against_the_mock_server() {
    let server = MockServer::start(MockOptions::default()).await;
    let stream = Plain.connect(HOST, server.port()).await.expect("connect");
    let mut framed = Framed::new(stream);
    framed
        .send(&envelope(
            VERSION_REQ,
            &ProtoOaVersionReq { payload_type: None },
            Some("early".to_owned()),
        ))
        .await
        .expect("send");
    let refused = framed.receive().await.expect("reply");
    assert_eq!(refused.payload_type, OA_ERROR);
    assert_eq!(refused.client_msg_id.as_deref(), Some("early"));
    framed
        .send(&envelope(
            APPLICATION_AUTH_REQ,
            &ProtoOaApplicationAuthReq {
                payload_type: None,
                client_id: CLIENT_ID.to_owned(),
                client_secret: CLIENT_SECRET.to_owned(),
            },
            Some("auth".to_owned()),
        ))
        .await
        .expect("send");
    let reply = framed.receive().await.expect("reply");
    assert_eq!(reply.payload_type, APPLICATION_AUTH_RES);
    assert_eq!(reply.client_msg_id.as_deref(), Some("auth"));
    assert_eq!(server.types_on(1), vec![VERSION_REQ, APPLICATION_AUTH_REQ]);
    server.drop_connections();
    assert_eq!(framed.receive().await, Err(TransportError::Closed));
}

#[tokio::test]
async fn tls_handshake_with_a_plain_server_fails_with_a_typed_error() {
    let server = MockServer::start(MockOptions::default()).await;
    let tls = Tls::new().expect("tls");
    let error = tls.connect("localhost", server.port()).await.err();
    assert!(
        matches!(error, Some(TransportError::Tls(_))),
        "unexpected outcome: {error:?}"
    );
}

#[tokio::test]
async fn connecting_to_a_closed_port_fails_with_an_io_error() {
    let server = MockServer::start(MockOptions::default()).await;
    let port = server.port();
    drop(server);
    tokio::task::yield_now().await;
    let error = Plain.connect(HOST, port).await.err();
    assert!(
        matches!(error, Some(TransportError::Io { .. })),
        "unexpected outcome: {error:?}"
    );
}
