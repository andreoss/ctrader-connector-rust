//! Length-prefixed framing, and the connectors shared with the other protocols.
//!
//! Every frame is a four-byte big-endian length followed by an encoded [`ProtoMessage`]. A
//! [`Framed`] stream reads and writes such frames over any byte stream, and a [`Connect`]
//! implementation opens the stream: [`Tls`] for the real hosts and [`Plain`] for local servers.

use std::fmt;

use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::common::errors::TransportError;
pub use crate::common::transport::{CONNECT_TIMEOUT, Connect, Plain, Tls};
use crate::open_api::messages::ProtoMessage;

/// Longest frame body accepted or produced, in bytes.
pub const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;

const PREFIX_LENGTH: usize = 4;
const READ_CHUNK: usize = 8 * 1024;

/// Encodes a message as one frame: the length prefix followed by the message.
///
/// # Errors
///
/// Returns [`TransportError::FrameTooLong`] when the encoded message exceeds
/// [`MAX_FRAME_LENGTH`].
pub fn encode_frame(message: &ProtoMessage) -> Result<Vec<u8>, TransportError> {
    let length = message.encoded_len();
    let too_long = || TransportError::FrameTooLong {
        length,
        limit: MAX_FRAME_LENGTH,
    };
    if length > MAX_FRAME_LENGTH {
        return Err(too_long());
    }
    let prefix = u32::try_from(length).map_err(|_| too_long())?;
    let mut frame = Vec::with_capacity(PREFIX_LENGTH + length);
    frame.extend_from_slice(&prefix.to_be_bytes());
    message
        .encode(&mut frame)
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    Ok(frame)
}

/// Takes one complete frame off the front of `buffer` and decodes it.
///
/// Returns `Ok(None)` and leaves the buffer untouched while the frame is incomplete.
///
/// # Errors
///
/// Returns [`TransportError::FrameTooLong`] when the announced length exceeds
/// [`MAX_FRAME_LENGTH`] and [`TransportError::Decode`] when the body is not a valid message.
pub fn decode_frame(buffer: &mut Vec<u8>) -> Result<Option<ProtoMessage>, TransportError> {
    let Some(prefix) = buffer.first_chunk::<PREFIX_LENGTH>() else {
        return Ok(None);
    };
    let length = usize::try_from(u32::from_be_bytes(*prefix)).unwrap_or(usize::MAX);
    if length > MAX_FRAME_LENGTH {
        return Err(TransportError::FrameTooLong {
            length,
            limit: MAX_FRAME_LENGTH,
        });
    }
    let end = PREFIX_LENGTH + length;
    let Some(body) = buffer.get(PREFIX_LENGTH..end) else {
        return Ok(None);
    };
    let decoded = ProtoMessage::decode(body);
    buffer.drain(..end);
    decoded
        .map(Some)
        .map_err(|error| TransportError::Decode(error.to_string()))
}

/// Byte stream that exchanges framed messages.
pub struct Framed<S> {
    stream: S,
    buffer: Vec<u8>,
}

impl<S> fmt::Debug for Framed<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Framed")
            .field("buffered", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Framed<S> {
    /// Wraps a connected stream.
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
        }
    }

    /// Writes one message as a frame and flushes the stream.
    ///
    /// # Errors
    ///
    /// Returns a [`TransportError`] when the message is too long or the stream fails.
    pub async fn send(&mut self, message: &ProtoMessage) -> Result<(), TransportError> {
        let frame = encode_frame(message)?;
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Reads the next message.
    ///
    /// The call may be dropped at any await point without losing data: bytes already read stay
    /// buffered for the next call.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Closed`] when the peer closes the stream, and another
    /// [`TransportError`] when the stream fails or a frame is invalid.
    pub async fn receive(&mut self) -> Result<ProtoMessage, TransportError> {
        loop {
            if let Some(message) = decode_frame(&mut self.buffer)? {
                return Ok(message);
            }
            let mut chunk = [0_u8; READ_CHUNK];
            let read = self.stream.read(&mut chunk).await?;
            match chunk.get(..read) {
                Some(bytes) if read > 0 => self.buffer.extend_from_slice(bytes),
                _ => return Err(TransportError::Closed),
            }
        }
    }

    /// Closes the writing side of the stream; failures are ignored because the stream is
    /// abandoned either way.
    pub async fn close(&mut self) {
        let _ = self.stream.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, duplex};

    use super::{Framed, MAX_FRAME_LENGTH, decode_frame, encode_frame};
    use crate::common::errors::TransportError;
    use crate::open_api::messages::ProtoMessage;

    fn message(payload_type: u32, payload: &[u8], id: &str) -> ProtoMessage {
        ProtoMessage {
            payload_type,
            payload: Some(payload.to_vec()),
            client_msg_id: Some(id.to_owned()),
        }
    }

    #[test]
    fn frame_starts_with_the_big_endian_length() {
        let frame = encode_frame(&message(51, b"", "a")).expect("frame");
        let body = frame.len() - 4;
        assert_eq!(
            frame.first_chunk::<4>().copied(),
            Some(u32::try_from(body).expect("short").to_be_bytes())
        );
    }

    #[test]
    fn frames_are_decoded_one_at_a_time() {
        let first = message(2100, b"abc", "1");
        let second = message(2101, b"", "2");
        let mut buffer = encode_frame(&first).expect("frame");
        buffer.extend(encode_frame(&second).expect("frame"));
        assert_eq!(decode_frame(&mut buffer), Ok(Some(first)));
        assert_eq!(decode_frame(&mut buffer), Ok(Some(second)));
        assert_eq!(decode_frame(&mut buffer), Ok(None));
        assert!(buffer.is_empty());
    }

    #[test]
    fn incomplete_frame_is_left_in_the_buffer() {
        let frame = encode_frame(&message(2100, b"abcdef", "1")).expect("frame");
        for cut in 0..frame.len() {
            let mut buffer = frame.get(..cut).expect("prefix").to_vec();
            assert_eq!(decode_frame(&mut buffer), Ok(None));
            assert_eq!(buffer.len(), cut);
        }
    }

    #[test]
    fn oversized_frames_are_rejected_in_both_directions() {
        let mut buffer = u32::MAX.to_be_bytes().to_vec();
        assert!(matches!(
            decode_frame(&mut buffer),
            Err(TransportError::FrameTooLong { limit, .. }) if limit == MAX_FRAME_LENGTH
        ));
        let huge = ProtoMessage {
            payload_type: 1,
            payload: Some(vec![0; MAX_FRAME_LENGTH]),
            client_msg_id: None,
        };
        assert!(matches!(
            encode_frame(&huge),
            Err(TransportError::FrameTooLong { .. })
        ));
    }

    #[test]
    fn invalid_body_is_a_decode_error_and_is_consumed() {
        let mut buffer = vec![0, 0, 0, 2, 0xff, 0xff];
        assert!(matches!(
            decode_frame(&mut buffer),
            Err(TransportError::Decode(_))
        ));
        assert!(buffer.is_empty());
    }

    #[tokio::test]
    async fn framed_stream_round_trips_messages_split_across_reads() {
        let (near, mut far) = duplex(4096);
        let mut framed = Framed::new(near);
        let expected = message(2104, &[7; 300], "split");
        let frame = encode_frame(&expected).expect("frame");
        let writer = tokio::spawn(async move {
            for part in frame.chunks(50) {
                far.write_all(part).await.expect("write");
                tokio::task::yield_now().await;
            }
            far
        });
        assert_eq!(framed.receive().await, Ok(expected.clone()));
        let far = writer.await.expect("writer");
        let mut peer = Framed::new(far);
        framed.send(&expected).await.expect("send");
        assert_eq!(peer.receive().await, Ok(expected));
        assert!(format!("{framed:?}").contains("Framed"));
        framed.close().await;
        assert_eq!(peer.receive().await, Err(TransportError::Closed));
    }
}
