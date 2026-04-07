//! Connectors that open the byte stream a connection runs on.
//!
//! A [`Connect`] implementation opens the stream: [`Tls`] for the real hosts and [`Plain`] for
//! servers that offer no encryption, such as the scripted servers of the tests.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::common::errors::TransportError;

/// Time allowed for opening a connection, including the TLS handshake.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Opens the byte stream a connection runs on.
pub trait Connect: Send + Sync + 'static {
    /// Stream produced by a successful connection.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Connects to `host` on `port`.
    ///
    /// # Errors
    ///
    /// Returns a [`TransportError`] when the connection cannot be established.
    fn connect(
        &self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = Result<Self::Stream, TransportError>> + Send;
}

async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, TransportError> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| TransportError::ConnectTimeout(CONNECT_TIMEOUT))??;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// Connector for unencrypted TCP, meant for servers on the local machine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Plain;

impl Connect for Plain {
    type Stream = TcpStream;

    async fn connect(&self, host: &str, port: u16) -> Result<Self::Stream, TransportError> {
        connect_tcp(host, port).await
    }
}

/// Connector for TLS over TCP, verifying the server against the bundled web roots.
#[derive(Clone)]
pub struct Tls {
    connector: TlsConnector,
}

impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tls").finish_non_exhaustive()
    }
}

impl Tls {
    /// Prepares a TLS client configuration.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Tls`] when the protocol versions cannot be configured.
    pub fn new() -> Result<Self, TransportError> {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| TransportError::Tls(error.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
        })
    }
}

impl Connect for Tls {
    type Stream = TlsStream<TcpStream>;

    async fn connect(&self, host: &str, port: u16) -> Result<Self::Stream, TransportError> {
        let name = ServerName::try_from(host.to_owned())
            .map_err(|_| TransportError::InvalidHost(host.to_owned()))?;
        let stream = connect_tcp(host, port).await?;
        timeout(CONNECT_TIMEOUT, self.connector.connect(name, stream))
            .await
            .map_err(|_| TransportError::ConnectTimeout(CONNECT_TIMEOUT))?
            .map_err(|error| TransportError::Tls(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Connect, Tls};
    use crate::common::errors::TransportError;

    #[tokio::test]
    async fn tls_rejects_a_host_that_is_not_a_server_name() {
        let tls = Tls::new().expect("tls");
        assert!(format!("{tls:?}").contains("Tls"));
        assert_eq!(
            tls.connect("not a host", 1).await.err(),
            Some(TransportError::InvalidHost("not a host".to_owned()))
        );
    }
}
