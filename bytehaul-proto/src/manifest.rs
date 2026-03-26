//! Transfer manifest for ByteHaul.
//!
//! A [`TransferManifest`] describes a pending transfer: the files involved,
//! their sizes and BLAKE3 content hashes, the block size that will be used on
//! the wire, and a deterministic transfer ID derived from the content.

use std::path::{Path, PathBuf};

use blake3::Hasher;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default block size: 4 MiB.
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;

/// Minimum allowed block size: 256 KiB.
pub const MIN_BLOCK_SIZE: u32 = 256 * 1024;

/// Maximum allowed block size: 64 MiB.
pub const MAX_BLOCK_SIZE: u32 = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur while building or (de)serializing a manifest.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("block size {0} is out of the allowed range [{MIN_BLOCK_SIZE}, {MAX_BLOCK_SIZE}]")]
    InvalidBlockSize(u32),

    #[error("file list must not be empty")]
    EmptyFileList,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("bincode serialization error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("file index {index} is out of range (manifest contains {count} files)")]
    FileIndexOutOfRange { index: usize, count: usize },
}

pub type Result<T> = std::result::Result<T, ManifestError>;

// ---------------------------------------------------------------------------
// FileEntry
// ---------------------------------------------------------------------------

/// Describes a single file that is part of a transfer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    /// Absolute or relative path on the source machine.
    pub source_path: PathBuf,

    /// Destination path on the receiver.
    pub dest_path: PathBuf,

    /// File size in bytes.
    pub size: u64,

    /// BLAKE3 hash of the full file content.
    pub blake3_hash: [u8; 32],
}

impl FileEntry {
    /// Number of blocks required to transfer this file at the given block size.
    #[inline]
    pub fn block_count(&self, block_size: u32) -> u64 {
        if self.size == 0 {
            return 0;
        }
        let bs = block_size as u64;
        self.size.div_ceil(bs)
    }
}

// ---------------------------------------------------------------------------
// TransferManifest
// ---------------------------------------------------------------------------

/// A manifest that fully describes a transfer session.
///
/// The `transfer_id` is a hex-encoded BLAKE3 hash derived from the file
/// content hashes, their destination paths, and the block size. Two manifests
/// that describe the *same* logical transfer will always produce the same ID,
/// making it safe to use for resumption.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferManifest {
    /// Deterministic, hex-encoded BLAKE3 transfer identifier.
    pub transfer_id: String,

    /// Ordered list of files in this transfer.
    pub files: Vec<FileEntry>,

    /// Block (chunk) size in bytes used for this transfer.
    pub block_size: u32,

    /// Timestamp of manifest creation.
    pub created_at: DateTime<Utc>,
}

impl TransferManifest {
    // -- Construction -------------------------------------------------------

    /// Create a new manifest from an already-known list of [`FileEntry`] values.
    ///
    /// Returns [`ManifestError::InvalidBlockSize`] if `block_size` is outside
    /// the allowed range and [`ManifestError::EmptyFileList`] if `files` is
    /// empty.
    pub fn new(files: Vec<FileEntry>, block_size: u32) -> Result<Self> {
        validate_block_size(block_size)?;
        if files.is_empty() {
            return Err(ManifestError::EmptyFileList);
        }

        let transfer_id = compute_transfer_id(&files, block_size);

        Ok(Self {
            transfer_id,
            files,
            block_size,
            created_at: Utc::now(),
        })
    }

    /// Build a manifest for a **single file** transfer.
    ///
    /// The file at `source_path` is read and hashed with BLAKE3. The manifest
    /// is created with the given `dest_path` and `block_size`.
    pub async fn from_file(
        source_path: impl AsRef<Path>,
        dest_path: impl AsRef<Path>,
        block_size: u32,
    ) -> Result<Self> {
        validate_block_size(block_size)?;

        let source_path = source_path.as_ref();
        let dest_path = dest_path.as_ref();

        let (size, blake3_hash) = hash_file(source_path).await?;

        let entry = FileEntry {
            source_path: source_path.to_path_buf(),
            dest_path: dest_path.to_path_buf(),
            size,
            blake3_hash,
        };

        Self::new(vec![entry], block_size)
    }

    // -- Serialization ------------------------------------------------------

    /// Serialize to a compact binary representation (bincode) suitable for the
    /// wire protocol.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(ManifestError::from)
    }

    /// Deserialize from bytes previously produced by [`to_bytes`](Self::to_bytes).
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(ManifestError::from)
    }

    /// Serialize to a pretty-printed JSON string (useful for logging/debug).
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(ManifestError::from)
    }

    /// Deserialize from a JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(ManifestError::from)
    }

    // -- Helpers ------------------------------------------------------------

    /// Total size of all files in the manifest, in bytes.
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// Total number of blocks across all files.
    pub fn total_blocks(&self) -> u64 {
        self.files
            .iter()
            .map(|f| f.block_count(self.block_size))
            .sum()
    }

    /// Returns the `(start_block, end_block)` **inclusive** range for the file
    /// at the given `file_index`.
    ///
    /// Blocks are numbered sequentially across all files in the manifest,
    /// starting at 0. For a zero-length file the range is `(start, start - 1)`
    /// (i.e. an empty range where `start > end`).
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError::FileIndexOutOfRange`] when `file_index >=
    /// self.files.len()`.
    pub fn block_range_for_file(&self, file_index: usize) -> Result<(u64, u64)> {
        if file_index >= self.files.len() {
            return Err(ManifestError::FileIndexOutOfRange {
                index: file_index,
                count: self.files.len(),
            });
        }

        let start: u64 = self.files[..file_index]
            .iter()
            .map(|f| f.block_count(self.block_size))
            .sum();

        let count = self.files[file_index].block_count(self.block_size);

        if count == 0 {
            // Empty file: return an intentionally empty range.
            return Ok((start, start.wrapping_sub(1)));
        }

        Ok((start, start + count - 1))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Validate that `block_size` is within the allowed range.
fn validate_block_size(block_size: u32) -> Result<()> {
    if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&block_size) {
        return Err(ManifestError::InvalidBlockSize(block_size));
    }
    Ok(())
}

/// Compute a deterministic transfer ID from file entries and block size.
///
/// The ID is the hex-encoded BLAKE3 hash of:
///   for each file: blake3_hash ++ dest_path (UTF-8 bytes)
///   then: block_size (little-endian u32)
fn compute_transfer_id(files: &[FileEntry], block_size: u32) -> String {
    let mut hasher = Hasher::new();
    for entry in files {
        hasher.update(&entry.blake3_hash);
        hasher.update(entry.dest_path.to_string_lossy().as_bytes());
    }
    hasher.update(&block_size.to_le_bytes());
    hasher.finalize().to_hex().to_string()
}

/// Read a file and return `(size, blake3_hash)`.
///
/// Uses buffered async reads so that large files do not need to be fully
/// resident in memory.
async fn hash_file(path: &Path) -> Result<(u64, [u8; 32])> {
    let mut file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    let size = metadata.len();

    let mut hasher = Hasher::new();
    let mut buf = vec![0u8; 256 * 1024]; // 256 KiB read buffer
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let hash: [u8; 32] = *hasher.finalize().as_bytes();
    Ok((size, hash))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    fn sample_entry(size: u64, hash_byte: u8, dest: &str) -> FileEntry {
        FileEntry {
            source_path: PathBuf::from("/src/file"),
            dest_path: PathBuf::from(dest),
            size,
            blake3_hash: [hash_byte; 32],
        }
    }

    #[test]
    fn block_size_validation() {
        assert!(TransferManifest::new(vec![sample_entry(100, 0, "/dst")], MIN_BLOCK_SIZE).is_ok());
        assert!(TransferManifest::new(vec![sample_entry(100, 0, "/dst")], MAX_BLOCK_SIZE).is_ok());
        assert!(TransferManifest::new(vec![sample_entry(100, 0, "/dst")], MIN_BLOCK_SIZE - 1).is_err());
        assert!(TransferManifest::new(vec![sample_entry(100, 0, "/dst")], MAX_BLOCK_SIZE + 1).is_err());
    }

    #[test]
    fn empty_file_list_rejected() {
        assert!(matches!(
            TransferManifest::new(vec![], DEFAULT_BLOCK_SIZE),
            Err(ManifestError::EmptyFileList)
        ));
    }

    #[test]
    fn transfer_id_is_deterministic() {
        let a = TransferManifest::new(
            vec![sample_entry(1024, 0xAB, "/dst/a.bin")],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();
        let b = TransferManifest::new(
            vec![sample_entry(1024, 0xAB, "/dst/a.bin")],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();
        assert_eq!(a.transfer_id, b.transfer_id);
    }

    #[test]
    fn transfer_id_changes_with_block_size() {
        let a = TransferManifest::new(
            vec![sample_entry(1024, 0xAB, "/dst/a.bin")],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();
        let b = TransferManifest::new(
            vec![sample_entry(1024, 0xAB, "/dst/a.bin")],
            MIN_BLOCK_SIZE,
        )
        .unwrap();
        assert_ne!(a.transfer_id, b.transfer_id);
    }

    #[test]
    fn total_size_and_blocks() {
        let bs = 1024 * 1024; // 1 MiB for easy math
        let manifest = TransferManifest::new(
            vec![
                sample_entry(2 * 1024 * 1024, 1, "/a"), // exactly 2 blocks
                sample_entry(2 * 1024 * 1024 + 1, 2, "/b"), // 3 blocks (partial last)
            ],
            bs,
        )
        .unwrap();

        assert_eq!(manifest.total_size(), 4 * 1024 * 1024 + 1);
        assert_eq!(manifest.total_blocks(), 5);
    }

    #[test]
    fn block_range_for_file_sequential() {
        let bs = 1024 * 1024;
        let manifest = TransferManifest::new(
            vec![
                sample_entry(2 * 1024 * 1024, 1, "/a"),     // 2 blocks: 0..=1
                sample_entry(3 * 1024 * 1024 + 1, 2, "/b"), // 4 blocks: 2..=5
                sample_entry(1024 * 1024, 3, "/c"),          // 1 block:  6..=6
            ],
            bs,
        )
        .unwrap();

        assert_eq!(manifest.block_range_for_file(0).unwrap(), (0, 1));
        assert_eq!(manifest.block_range_for_file(1).unwrap(), (2, 5));
        assert_eq!(manifest.block_range_for_file(2).unwrap(), (6, 6));
        assert!(manifest.block_range_for_file(3).is_err());
    }

    #[test]
    fn zero_size_file_block_range() {
        let manifest = TransferManifest::new(
            vec![
                sample_entry(DEFAULT_BLOCK_SIZE as u64, 1, "/a"),
                sample_entry(0, 2, "/empty"),
                sample_entry(DEFAULT_BLOCK_SIZE as u64, 3, "/b"),
            ],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();

        // /a occupies block 0
        assert_eq!(manifest.block_range_for_file(0).unwrap(), (0, 0));
        // /empty has 0 blocks -> empty range (1, 0)
        let (start, end) = manifest.block_range_for_file(1).unwrap();
        assert!(start > end);
        // /b occupies block 1
        assert_eq!(manifest.block_range_for_file(2).unwrap(), (1, 1));
    }

    #[test]
    fn bincode_round_trip() {
        let manifest = TransferManifest::new(
            vec![sample_entry(4096, 0xFF, "/dst/test.bin")],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();

        let bytes = manifest.to_bytes().unwrap();
        let restored = TransferManifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest.transfer_id, restored.transfer_id);
        assert_eq!(manifest.files.len(), restored.files.len());
        assert_eq!(manifest.files[0], restored.files[0]);
        assert_eq!(manifest.block_size, restored.block_size);
    }

    #[test]
    fn json_round_trip() {
        let manifest = TransferManifest::new(
            vec![sample_entry(4096, 0xAA, "/dst/test.bin")],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();

        let json = manifest.to_json().unwrap();
        let restored = TransferManifest::from_json(&json).unwrap();

        assert_eq!(manifest.transfer_id, restored.transfer_id);
        assert_eq!(manifest.files[0], restored.files[0]);
    }

    #[tokio::test]
    async fn from_file_hashes_correctly() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = b"hello bytehaul";
        tmp.write_all(content).unwrap();
        tmp.flush().unwrap();

        let manifest = TransferManifest::from_file(
            tmp.path(),
            "/remote/hello.txt",
            DEFAULT_BLOCK_SIZE,
        )
        .await
        .unwrap();

        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].size, content.len() as u64);
        assert_eq!(
            manifest.files[0].blake3_hash,
            *blake3::hash(content).as_bytes()
        );
        assert_eq!(manifest.total_blocks(), 1);
    }
}
