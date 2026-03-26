use std::path::Path;

use thiserror::Error;
use tokio::io::AsyncReadExt;

#[derive(Error, Debug)]
pub enum VerifyError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid hex string: {0}")]
    InvalidHex(String),
}

const HASH_BUF_SIZE: usize = 1024 * 1024; // 1 MB read buffer

/// BLAKE3 hash of a byte slice.
pub fn hash_bytes(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// BLAKE3 hash of a file, reading in 1 MB buffers.
pub async fn hash_file(path: &Path) -> Result<[u8; 32], VerifyError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; HASH_BUF_SIZE];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(*hasher.finalize().as_bytes())
}

/// Verify a chunk's data against its expected BLAKE3 hash.
pub fn verify_chunk(data: &[u8], expected_hash: &[u8; 32]) -> bool {
    let actual = hash_bytes(data);
    actual == *expected_hash
}

/// Verify an entire file against its expected BLAKE3 hash.
pub async fn verify_file(path: &Path, expected_hash: &[u8; 32]) -> Result<bool, VerifyError> {
    let actual = hash_file(path).await?;
    Ok(actual == *expected_hash)
}

/// Streaming BLAKE3 hasher for incremental data.
pub struct HashStream {
    hasher: blake3::Hasher,
}

impl HashStream {
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn finalize(self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}

impl Default for HashStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute a transfer ID from file hash, destination path, and block size.
/// Returns the hex string of BLAKE3(file_hash || dest_path || block_size_le_bytes).
pub fn compute_transfer_id(file_hash: &[u8; 32], dest_path: &str, block_size: u32) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(file_hash);
    hasher.update(dest_path.as_bytes());
    hasher.update(&block_size.to_le_bytes());
    let hash = hasher.finalize();
    hash_to_hex(hash.as_bytes())
}

/// Convert a 32-byte hash to a hex string.
pub fn hash_to_hex(hash: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut hex = String::with_capacity(64);
    for byte in hash {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Convert a hex string to a 32-byte hash.
pub fn hex_to_hash(hex: &str) -> Result<[u8; 32], VerifyError> {
    if hex.len() != 64 {
        return Err(VerifyError::InvalidHex(format!(
            "Expected 64 hex chars, got {}",
            hex.len()
        )));
    }
    let mut hash = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk)
            .map_err(|_| VerifyError::InvalidHex("Invalid UTF-8 in hex".to_string()))?;
        hash[i] = u8::from_str_radix(s, 16)
            .map_err(|_| VerifyError::InvalidHex(format!("Invalid hex byte: {}", s)))?;
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bytes() {
        let data = b"hello world";
        let hash1 = hash_bytes(data);
        let hash2 = hash_bytes(data);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, [0u8; 32]);
    }

    #[test]
    fn test_verify_chunk() {
        let data = b"test chunk data";
        let hash = hash_bytes(data);
        assert!(verify_chunk(data, &hash));
        assert!(!verify_chunk(b"wrong data", &hash));
    }

    #[test]
    fn test_hash_hex_roundtrip() {
        let hash = hash_bytes(b"test");
        let hex = hash_to_hex(&hash);
        assert_eq!(hex.len(), 64);
        let recovered = hex_to_hash(&hex).unwrap();
        assert_eq!(hash, recovered);
    }

    #[test]
    fn test_compute_transfer_id() {
        let file_hash = hash_bytes(b"file content");
        let id1 = compute_transfer_id(&file_hash, "/dest/path", 4_194_304);
        let id2 = compute_transfer_id(&file_hash, "/dest/path", 4_194_304);
        assert_eq!(id1, id2);

        // Different dest path -> different ID
        let id3 = compute_transfer_id(&file_hash, "/other/path", 4_194_304);
        assert_ne!(id1, id3);

        // Different block size -> different ID
        let id4 = compute_transfer_id(&file_hash, "/dest/path", 1_048_576);
        assert_ne!(id1, id4);
    }

    #[test]
    fn test_hash_stream() {
        let mut stream = HashStream::new();
        stream.update(b"hello ");
        stream.update(b"world");
        let hash1 = stream.finalize();
        let hash2 = hash_bytes(b"hello world");
        assert_eq!(hash1, hash2);
    }
}
