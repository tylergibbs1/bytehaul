//! Wire protocol for ByteHaul.
//!
//! Defines the message formats and encoding/decoding routines for both the
//! control stream (QUIC stream 0) and data streams (QUIC streams 1..N).
//!
//! ## Control stream
//!
//! Control messages are length-prefixed: a 4-byte big-endian `u32` length
//! followed by that many bytes of bincode-encoded [`ControlMessage`].
//!
//! ## Data streams
//!
//! Each data stream carries a single chunk preceded by a fixed 76-byte
//! [`ChunkHeader`], followed by the raw chunk bytes.

use serde::{Deserialize, Serialize};
use tracing::trace;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Fixed size of a [`ChunkHeader`] on the wire, in bytes.
///
/// Layout: transfer_id (32) + chunk_index (8) + chunk_size (4) + chunk_blake3 (32) = 76.
pub const CHUNK_HEADER_SIZE: usize = 76;

/// Maximum allowed size for a single control message (64 MiB).
///
/// This limit is deliberately generous to accommodate large manifests that
/// describe many thousands of files.
pub const MAX_CONTROL_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Current protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors specific to wire-level encoding and decoding.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("quinn write error: {0}")]
    WriteError(#[from] quinn::WriteError),

    #[error("quinn read exact error: {0}")]
    ReadExactError(#[from] quinn::ReadExactError),

    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error(
        "control message size {size} exceeds maximum allowed size {MAX_CONTROL_MESSAGE_SIZE}"
    )]
    MessageTooLarge { size: usize },
}

pub type Result<T> = std::result::Result<T, WireError>;

// ---------------------------------------------------------------------------
// ErrorCode
// ---------------------------------------------------------------------------

/// Application-level error codes exchanged over the control stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ErrorCode {
    /// An I/O error occurred (disk, network, etc.).
    IoError = 1,
    /// A chunk or file failed BLAKE3 integrity verification.
    VerificationFailed = 2,
    /// A destination path attempted to escape its root (path traversal).
    PathViolation = 3,
    /// The requested transfer ID was not found.
    TransferNotFound = 4,
    /// A protocol-level framing or sequencing error.
    ProtocolError = 5,
    /// The transfer was cancelled by the remote peer.
    Cancelled = 6,
}

// ---------------------------------------------------------------------------
// ControlMessage
// ---------------------------------------------------------------------------

/// Messages exchanged over the control stream (QUIC stream 0).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Carries the bincode-serialized [`TransferManifest`](crate::manifest::TransferManifest).
    Manifest { manifest_bytes: Vec<u8> },

    /// Receiver tells the sender which blocks it already has, enabling
    /// resumption. Indices are sorted in ascending order.
    ResumeState { blocks_received: Vec<u64> },

    /// Receiver acknowledges a batch of successfully written blocks.
    ProgressAck { blocks_confirmed: Vec<u64> },

    /// Sender (or receiver) signals that the entire transfer is complete.
    /// Contains the BLAKE3 hash of the fully reassembled file.
    TransferComplete { file_blake3: [u8; 32] },

    /// Signals an application-level error to the peer.
    Error { code: ErrorCode, message: String },

    /// Request graceful cancellation of the current transfer.
    Cancel,
}

// ---------------------------------------------------------------------------
// ChunkHeader
// ---------------------------------------------------------------------------

/// Fixed-size header that precedes every chunk on a data stream.
///
/// **Wire layout** (76 bytes, all multi-byte integers are big-endian):
///
/// | Offset | Size | Field          |
/// |--------|------|----------------|
/// |  0     | 32   | transfer_id    |
/// | 32     |  8   | chunk_index    |
/// | 40     |  4   | chunk_size     |
/// | 44     | 32   | chunk_blake3   |
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    /// BLAKE3 transfer identifier (raw 32 bytes, **not** hex-encoded).
    pub transfer_id: [u8; 32],
    /// Zero-based index of this chunk within the global block numbering.
    pub chunk_index: u64,
    /// Size of the chunk payload in bytes.
    pub chunk_size: u32,
    /// BLAKE3 hash of the chunk payload.
    pub chunk_blake3: [u8; 32],
}

impl ChunkHeader {
    /// Serialize the header into a caller-provided 76-byte buffer.
    pub fn encode(&self, buf: &mut [u8; CHUNK_HEADER_SIZE]) {
        buf[0..32].copy_from_slice(&self.transfer_id);
        buf[32..40].copy_from_slice(&self.chunk_index.to_be_bytes());
        buf[40..44].copy_from_slice(&self.chunk_size.to_be_bytes());
        buf[44..76].copy_from_slice(&self.chunk_blake3);
    }

    /// Deserialize a header from a 76-byte buffer.
    pub fn decode(buf: &[u8; CHUNK_HEADER_SIZE]) -> Self {
        let mut transfer_id = [0u8; 32];
        transfer_id.copy_from_slice(&buf[0..32]);

        let chunk_index = u64::from_be_bytes(buf[32..40].try_into().unwrap());
        let chunk_size = u32::from_be_bytes(buf[40..44].try_into().unwrap());

        let mut chunk_blake3 = [0u8; 32];
        chunk_blake3.copy_from_slice(&buf[44..76]);

        Self {
            transfer_id,
            chunk_index,
            chunk_size,
            chunk_blake3,
        }
    }
}

// ---------------------------------------------------------------------------
// Control stream encoding / decoding
// ---------------------------------------------------------------------------

/// Write a [`ControlMessage`] to a QUIC send stream.
///
/// Format: `[4 bytes length (u32 big-endian)] [bincode payload]`.
pub async fn write_control_message(
    stream: &mut quinn::SendStream,
    msg: &ControlMessage,
) -> Result<()> {
    let payload = bincode::serialize(msg)?;
    let len = payload.len();

    if len > MAX_CONTROL_MESSAGE_SIZE {
        return Err(WireError::MessageTooLarge { size: len });
    }

    trace!(len, "writing control message");

    let len_bytes = (len as u32).to_be_bytes();
    stream.write_all(&len_bytes).await?;
    stream.write_all(&payload).await?;

    Ok(())
}

/// Read a [`ControlMessage`] from a QUIC receive stream.
///
/// Reads a 4-byte big-endian length prefix, then reads and deserializes that
/// many bytes of bincode.
pub async fn read_control_message(
    stream: &mut quinn::RecvStream,
) -> Result<ControlMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_CONTROL_MESSAGE_SIZE {
        return Err(WireError::MessageTooLarge { size: len });
    }

    trace!(len, "reading control message");

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;

    let msg = bincode::deserialize(&payload)?;
    Ok(msg)
}

// ---------------------------------------------------------------------------
// Data stream encoding / decoding
// ---------------------------------------------------------------------------

/// Write a [`ChunkHeader`] to a QUIC send stream.
///
/// Writes exactly [`CHUNK_HEADER_SIZE`] (76) bytes.
pub async fn write_chunk_header(
    stream: &mut quinn::SendStream,
    header: &ChunkHeader,
) -> Result<()> {
    let mut buf = [0u8; CHUNK_HEADER_SIZE];
    header.encode(&mut buf);

    trace!(
        chunk_index = header.chunk_index,
        chunk_size = header.chunk_size,
        "writing chunk header"
    );

    stream.write_all(&buf).await?;
    Ok(())
}

/// Read a [`ChunkHeader`] from a QUIC receive stream.
///
/// Reads exactly [`CHUNK_HEADER_SIZE`] (76) bytes and parses them.
pub async fn read_chunk_header(
    stream: &mut quinn::RecvStream,
) -> Result<ChunkHeader> {
    let mut buf = [0u8; CHUNK_HEADER_SIZE];
    stream.read_exact(&mut buf).await?;

    let header = ChunkHeader::decode(&buf);

    trace!(
        chunk_index = header.chunk_index,
        chunk_size = header.chunk_size,
        "read chunk header"
    );

    Ok(header)
}

/// Write raw chunk data to a QUIC send stream.
pub async fn write_chunk_data(
    stream: &mut quinn::SendStream,
    data: &[u8],
) -> Result<()> {
    stream.write_all(data).await?;
    Ok(())
}

/// Read exactly `size` bytes of chunk data from a QUIC receive stream.
pub async fn read_chunk_data(
    stream: &mut quinn::RecvStream,
    size: u32,
) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; size as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_header_encode_decode_round_trip() {
        let header = ChunkHeader {
            transfer_id: [0xAB; 32],
            chunk_index: 42,
            chunk_size: 4 * 1024 * 1024,
            chunk_blake3: [0xCD; 32],
        };

        let mut buf = [0u8; CHUNK_HEADER_SIZE];
        header.encode(&mut buf);

        let decoded = ChunkHeader::decode(&buf);
        assert_eq!(header, decoded);
    }

    #[test]
    fn chunk_header_wire_layout() {
        let header = ChunkHeader {
            transfer_id: [0x01; 32],
            chunk_index: 0x0000_0000_0000_0007,
            chunk_size: 0x0040_0000, // 4 MiB
            chunk_blake3: [0xFF; 32],
        };

        let mut buf = [0u8; CHUNK_HEADER_SIZE];
        header.encode(&mut buf);

        // transfer_id at offset 0..32
        assert_eq!(&buf[0..32], &[0x01; 32]);
        // chunk_index at offset 32..40 (big-endian)
        assert_eq!(&buf[32..40], &[0, 0, 0, 0, 0, 0, 0, 7]);
        // chunk_size at offset 40..44 (big-endian)
        assert_eq!(&buf[40..44], &[0x00, 0x40, 0x00, 0x00]);
        // chunk_blake3 at offset 44..76
        assert_eq!(&buf[44..76], &[0xFF; 32]);
    }

    #[test]
    fn chunk_header_size_constant_matches() {
        assert_eq!(CHUNK_HEADER_SIZE, 32 + 8 + 4 + 32);
    }

    #[test]
    fn control_message_bincode_round_trip() {
        let messages = vec![
            ControlMessage::Manifest {
                manifest_bytes: vec![1, 2, 3, 4],
            },
            ControlMessage::ResumeState {
                blocks_received: vec![0, 5, 10, 100],
            },
            ControlMessage::ProgressAck {
                blocks_confirmed: vec![3, 7],
            },
            ControlMessage::TransferComplete {
                file_blake3: [0xEE; 32],
            },
            ControlMessage::Error {
                code: ErrorCode::VerificationFailed,
                message: "bad hash".into(),
            },
            ControlMessage::Cancel,
        ];

        for msg in &messages {
            let encoded = bincode::serialize(msg).unwrap();
            let decoded: ControlMessage = bincode::deserialize(&encoded).unwrap();

            // Verify structural equality via debug representation.
            assert_eq!(format!("{:?}", msg), format!("{:?}", decoded));
        }
    }

    #[test]
    fn error_code_values() {
        assert_eq!(ErrorCode::IoError as u8, 1);
        assert_eq!(ErrorCode::VerificationFailed as u8, 2);
        assert_eq!(ErrorCode::PathViolation as u8, 3);
        assert_eq!(ErrorCode::TransferNotFound as u8, 4);
        assert_eq!(ErrorCode::ProtocolError as u8, 5);
        assert_eq!(ErrorCode::Cancelled as u8, 6);
    }
}
