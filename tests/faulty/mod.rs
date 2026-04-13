//! Connector for the tests whose writes can be made to fail.
//!
//! It opens an unencrypted connection to a scripted server on the local machine; once
//! [`Faulty::fail_writes`] is called, every write of every stream it opened is refused with
//! an error, while reads go on.

#![allow(dead_code)]

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use ctrader_connector::common::errors::TransportError;
use ctrader_connector::common::transport::{Connect, Plain};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Explanation the refused writes carry.
pub const REFUSED: &str = "write refused by the test";

/// Connector whose streams refuse to write on demand.
#[derive(Debug, Clone, Default)]
pub struct Faulty {
    failing: Arc<AtomicBool>,
}

impl Faulty {
    /// Makes every write fail from now on, or work again.
    pub fn fail_writes(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }

    /// Returns the error a refused write is reported with.
    pub fn error() -> TransportError {
        TransportError::Io {
            kind: io::ErrorKind::BrokenPipe,
            message: REFUSED.to_owned(),
        }
    }
}

impl Connect for Faulty {
    type Stream = FaultyStream;

    async fn connect(&self, host: &str, port: u16) -> Result<Self::Stream, TransportError> {
        Ok(FaultyStream {
            inner: Plain.connect(host, port).await?,
            failing: Arc::clone(&self.failing),
        })
    }
}

/// Stream of a [`Faulty`] connector.
#[derive(Debug)]
pub struct FaultyStream {
    inner: TcpStream,
    failing: Arc<AtomicBool>,
}

impl AsyncRead for FaultyStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for FaultyStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.failing.load(Ordering::SeqCst) {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, REFUSED)))
        } else {
            Pin::new(&mut self.inner).poll_write(context, bytes)
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}
