//! Per-chunk zstd compression for ByteHaul.
//!
//! Compression is applied transparently at the chunk level: the sender
//! compresses each chunk before writing it to the wire, and the receiver
//! decompresses after reading. The [`TransferManifest`](crate::manifest::TransferManifest)
//! carries a `compressed` flag so both sides agree on whether compression is in
//! effect.

use std::io::Cursor;

/// Default zstd compression level (good speed/ratio trade-off).
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Errors specific to chunk compression/decompression.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("compression failed: {0}")]
    CompressFailed(std::io::Error),

    #[error("decompression failed: {0}")]
    DecompressFailed(std::io::Error),

    #[error(
        "decompressed size {actual} exceeds maximum allowed size {max}"
    )]
    DecompressedTooLarge { actual: usize, max: usize },
}

/// Configuration for per-chunk compression.
#[derive(Debug, Clone, Copy)]
pub struct CompressionConfig {
    /// Whether compression is enabled.
    pub enabled: bool,
    /// Zstd compression level (1..=22, default [`DEFAULT_COMPRESSION_LEVEL`]).
    pub level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            level: DEFAULT_COMPRESSION_LEVEL,
        }
    }
}

/// Compress a chunk using zstd at the given compression level.
///
/// Returns the compressed bytes. The caller should compare the compressed
/// length against the original to decide what goes on the wire (this module
/// always compresses; the decision to enable compression is made higher up).
pub fn compress_chunk(data: &[u8], level: i32) -> Result<Vec<u8>, CompressionError> {
    let cursor = Cursor::new(data);
    zstd::encode_all(cursor, level).map_err(CompressionError::CompressFailed)
}

/// Decompress a zstd-compressed chunk.
///
/// `max_size` is the upper bound on the decompressed output. If the
/// decompressed data exceeds this limit an error is returned to guard against
/// decompression bombs.
pub fn decompress_chunk(data: &[u8], max_size: usize) -> Result<Vec<u8>, CompressionError> {
    let cursor = Cursor::new(data);
    let decompressed = zstd::decode_all(cursor).map_err(CompressionError::DecompressFailed)?;
    if decompressed.len() > max_size {
        return Err(CompressionError::DecompressedTooLarge {
            actual: decompressed.len(),
            max: max_size,
        });
    }
    Ok(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_roundtrip() {
        let original = b"ByteHaul chunk data that should compress nicely. \
                         Repeated content repeated content repeated content \
                         repeated content repeated content repeated content.";
        let compressed = compress_chunk(original, DEFAULT_COMPRESSION_LEVEL).unwrap();
        let decompressed = decompress_chunk(&compressed, original.len() * 2).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn compressed_size_smaller_for_compressible_data() {
        // 64 KiB of highly compressible data (repeated pattern).
        let original: Vec<u8> = (0..65_536).map(|i| (i % 256) as u8).collect();
        let compressed = compress_chunk(&original, DEFAULT_COMPRESSION_LEVEL).unwrap();
        assert!(
            compressed.len() < original.len(),
            "compressed ({}) should be smaller than original ({})",
            compressed.len(),
            original.len(),
        );
    }

    #[test]
    fn incompressible_data_does_not_grow_too_much() {
        // Random-ish data: use a simple PRNG so the test is deterministic.
        let mut data = vec![0u8; 65_536];
        let mut state: u64 = 0xdeadbeefcafe1234;
        for byte in data.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        let compressed = compress_chunk(&data, DEFAULT_COMPRESSION_LEVEL).unwrap();
        // Zstd worst-case overhead is modest; allow up to 1% growth + frame overhead.
        let max_allowed = data.len() + data.len() / 100 + 128;
        assert!(
            compressed.len() <= max_allowed,
            "compressed ({}) should not exceed {} for random data",
            compressed.len(),
            max_allowed,
        );
    }

    #[test]
    fn decompress_rejects_oversized_output() {
        let original = vec![0u8; 1024];
        let compressed = compress_chunk(&original, DEFAULT_COMPRESSION_LEVEL).unwrap();
        // Set max_size smaller than the decompressed output.
        let result = decompress_chunk(&compressed, 512);
        assert!(result.is_err());
    }

    #[test]
    fn empty_data_roundtrip() {
        let compressed = compress_chunk(b"", DEFAULT_COMPRESSION_LEVEL).unwrap();
        let decompressed = decompress_chunk(&compressed, 1024).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn compression_config_default() {
        let cfg = CompressionConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.level, DEFAULT_COMPRESSION_LEVEL);
    }
}
