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

use crate::filter::FileFilter;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default block size: 16 MiB.
///
/// Benchmarks show 16 MB blocks give ~35% higher throughput than 4 MB
/// on high-BDP links (54 MB/s vs 40 MB/s at 1 GB over 85ms RTT).
pub const DEFAULT_BLOCK_SIZE: u32 = 16 * 1024 * 1024;

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

    /// Path relative to the transfer root directory.
    ///
    /// Used by the receiver to reconstruct directory structure. `None` for
    /// single-file transfers (backward compatible).
    pub relative_path: Option<PathBuf>,
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
            relative_path: None,
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

    /// Build a manifest by recursively walking a directory tree.
    ///
    /// Every regular file under `source_dir` is included. Symlinks are skipped.
    /// Each file's `dest_path` is `dest_dir` joined with the path relative to
    /// `source_dir`. Entries are sorted by relative path for a deterministic
    /// transfer ID.
    pub async fn from_directory(
        source_dir: &Path,
        dest_dir: &Path,
        block_size: u32,
    ) -> Result<Self> {
        validate_block_size(block_size)?;

        let source_dir = source_dir
            .canonicalize()
            .map_err(ManifestError::Io)?;

        let mut entries: Vec<(PathBuf, FileEntry)> = Vec::new();

        for dir_entry in walkdir::WalkDir::new(&source_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            // Skip symlinks
            if dir_entry.path_is_symlink() {
                continue;
            }

            let ft = dir_entry.file_type();
            if !ft.is_file() {
                continue;
            }

            let abs_path = dir_entry.path().to_path_buf();
            let rel_path = abs_path
                .strip_prefix(&source_dir)
                .expect("walkdir entry must be under source_dir")
                .to_path_buf();

            let (size, blake3_hash) = hash_file(&abs_path).await?;

            let entry = FileEntry {
                source_path: abs_path,
                dest_path: dest_dir.join(&rel_path),
                size,
                blake3_hash,
                relative_path: Some(rel_path.clone()),
            };

            entries.push((rel_path, entry));
        }

        // Sort by relative path for deterministic ordering.
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let files: Vec<FileEntry> = entries.into_iter().map(|(_, e)| e).collect();

        if files.is_empty() {
            return Err(ManifestError::EmptyFileList);
        }

        Self::new(files, block_size)
    }

    /// Build a manifest by recursively walking a directory tree, applying a
    /// [`FileFilter`] to skip files that don't match.
    ///
    /// Behaves like [`from_directory`](Self::from_directory) but skips files
    /// whose relative path does not pass the filter **before** hashing them,
    /// saving I/O on large trees.
    pub async fn from_directory_filtered(
        source_dir: &Path,
        dest_dir: &Path,
        block_size: u32,
        filter: &FileFilter,
    ) -> Result<Self> {
        validate_block_size(block_size)?;

        let source_dir = source_dir
            .canonicalize()
            .map_err(ManifestError::Io)?;

        let mut entries: Vec<(PathBuf, FileEntry)> = Vec::new();

        for dir_entry in walkdir::WalkDir::new(&source_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            // Skip symlinks
            if dir_entry.path_is_symlink() {
                continue;
            }

            let ft = dir_entry.file_type();
            if !ft.is_file() {
                continue;
            }

            let abs_path = dir_entry.path().to_path_buf();
            let rel_path = abs_path
                .strip_prefix(&source_dir)
                .expect("walkdir entry must be under source_dir")
                .to_path_buf();

            // Apply filter before hashing to save I/O.
            if !filter.matches(&rel_path) {
                continue;
            }

            let (size, blake3_hash) = hash_file(&abs_path).await?;

            let entry = FileEntry {
                source_path: abs_path,
                dest_path: dest_dir.join(&rel_path),
                size,
                blake3_hash,
                relative_path: Some(rel_path.clone()),
            };

            entries.push((rel_path, entry));
        }

        // Sort by relative path for deterministic ordering.
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let files: Vec<FileEntry> = entries.into_iter().map(|(_, e)| e).collect();

        if files.is_empty() {
            return Err(ManifestError::EmptyFileList);
        }

        Self::new(files, block_size)
    }

    /// Build a manifest from an explicit list of file paths.
    ///
    /// Each file's `dest_path` is `dest_dir` joined with just the filename
    /// component of the source path.
    pub async fn from_paths(
        paths: &[PathBuf],
        dest_dir: &Path,
        block_size: u32,
    ) -> Result<Self> {
        validate_block_size(block_size)?;

        let mut files = Vec::with_capacity(paths.len());

        for path in paths {
            let (size, blake3_hash) = hash_file(path).await?;

            let filename = path
                .file_name()
                .unwrap_or(path.as_os_str());

            let entry = FileEntry {
                source_path: path.clone(),
                dest_path: dest_dir.join(filename),
                size,
                blake3_hash,
                relative_path: None,
            };

            files.push(entry);
        }

        if files.is_empty() {
            return Err(ManifestError::EmptyFileList);
        }

        Self::new(files, block_size)
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

    /// Map a global block index to `(file_index, local_byte_offset)`.
    ///
    /// Returns `None` if `global_block_index` is beyond the total number of
    /// blocks in the manifest.
    pub fn file_and_offset_for_block(&self, global_block_index: u64) -> Option<(usize, u64)> {
        let bs = self.block_size as u64;
        let mut blocks_before: u64 = 0;

        for (i, file) in self.files.iter().enumerate() {
            let file_blocks = file.block_count(self.block_size);
            if global_block_index < blocks_before + file_blocks {
                let local_block = global_block_index - blocks_before;
                let local_byte_offset = local_block * bs;
                return Some((i, local_byte_offset));
            }
            blocks_before += file_blocks;
        }

        None
    }

    /// BLAKE3 hash of all file hashes concatenated.
    ///
    /// Computed as `BLAKE3(file0_hash || file1_hash || ...)`. Useful for
    /// multi-file `TransferComplete` verification.
    pub fn manifest_hash(&self) -> [u8; 32] {
        let mut hasher = Hasher::new();
        for file in &self.files {
            hasher.update(&file.blake3_hash);
        }
        *hasher.finalize().as_bytes()
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
            relative_path: None,
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

    // -- New tests for directory / multi-file support -----------------------

    #[tokio::test]
    async fn from_directory_with_nested_subdirs() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        // Create nested structure:
        //   root/a.txt
        //   root/sub/b.txt
        //   root/sub/deep/c.txt
        std::fs::write(root.join("a.txt"), b"aaa").unwrap();
        std::fs::create_dir_all(root.join("sub/deep")).unwrap();
        std::fs::write(root.join("sub/b.txt"), b"bbb").unwrap();
        std::fs::write(root.join("sub/deep/c.txt"), b"ccc").unwrap();

        let dest = Path::new("/remote/backup");
        let manifest = TransferManifest::from_directory(root, dest, MIN_BLOCK_SIZE)
            .await
            .unwrap();

        assert_eq!(manifest.files.len(), 3);

        // Entries are sorted by relative path.
        assert_eq!(
            manifest.files[0].relative_path.as_deref(),
            Some(Path::new("a.txt"))
        );
        assert_eq!(
            manifest.files[1].relative_path.as_deref(),
            Some(Path::new("sub/b.txt"))
        );
        assert_eq!(
            manifest.files[2].relative_path.as_deref(),
            Some(Path::new("sub/deep/c.txt"))
        );

        // dest_path should preserve subdirectory structure.
        assert_eq!(manifest.files[0].dest_path, dest.join("a.txt"));
        assert_eq!(manifest.files[1].dest_path, dest.join("sub/b.txt"));
        assert_eq!(manifest.files[2].dest_path, dest.join("sub/deep/c.txt"));

        // Hashes are correct.
        assert_eq!(manifest.files[0].blake3_hash, *blake3::hash(b"aaa").as_bytes());
        assert_eq!(manifest.files[1].blake3_hash, *blake3::hash(b"bbb").as_bytes());
    }

    #[tokio::test]
    async fn from_paths_with_multiple_files() {
        let mut tmp_a = NamedTempFile::new().unwrap();
        tmp_a.write_all(b"alpha").unwrap();
        tmp_a.flush().unwrap();

        let mut tmp_b = NamedTempFile::new().unwrap();
        tmp_b.write_all(b"beta").unwrap();
        tmp_b.flush().unwrap();

        let paths = vec![tmp_a.path().to_path_buf(), tmp_b.path().to_path_buf()];
        let dest = Path::new("/remote");

        let manifest = TransferManifest::from_paths(&paths, dest, MIN_BLOCK_SIZE)
            .await
            .unwrap();

        assert_eq!(manifest.files.len(), 2);

        // dest_path should be dest_dir joined with just the filename.
        let name_a = tmp_a.path().file_name().unwrap();
        let name_b = tmp_b.path().file_name().unwrap();
        assert_eq!(manifest.files[0].dest_path, dest.join(name_a));
        assert_eq!(manifest.files[1].dest_path, dest.join(name_b));

        assert_eq!(manifest.files[0].size, 5);
        assert_eq!(manifest.files[1].size, 4);
    }

    #[test]
    fn file_and_offset_for_block_correctness() {
        let bs: u32 = 1024 * 1024; // 1 MiB
        let manifest = TransferManifest::new(
            vec![
                sample_entry(2 * 1024 * 1024, 1, "/a"),     // 2 blocks: 0, 1
                sample_entry(3 * 1024 * 1024 + 1, 2, "/b"), // 4 blocks: 2, 3, 4, 5
                sample_entry(1024 * 1024, 3, "/c"),          // 1 block:  6
            ],
            bs,
        )
        .unwrap();

        // File 0: blocks 0 and 1
        assert_eq!(manifest.file_and_offset_for_block(0), Some((0, 0)));
        assert_eq!(
            manifest.file_and_offset_for_block(1),
            Some((0, 1024 * 1024))
        );

        // File 1: blocks 2..=5
        assert_eq!(manifest.file_and_offset_for_block(2), Some((1, 0)));
        assert_eq!(
            manifest.file_and_offset_for_block(5),
            Some((1, 3 * 1024 * 1024))
        );

        // File 2: block 6
        assert_eq!(manifest.file_and_offset_for_block(6), Some((2, 0)));

        // Out of range
        assert_eq!(manifest.file_and_offset_for_block(7), None);
        assert_eq!(manifest.file_and_offset_for_block(u64::MAX), None);
    }

    #[test]
    fn manifest_hash_determinism() {
        let entries = vec![
            sample_entry(100, 0xAA, "/a"),
            sample_entry(200, 0xBB, "/b"),
        ];

        let m1 = TransferManifest::new(entries.clone(), DEFAULT_BLOCK_SIZE).unwrap();
        let m2 = TransferManifest::new(entries, DEFAULT_BLOCK_SIZE).unwrap();

        assert_eq!(m1.manifest_hash(), m2.manifest_hash());

        // Changing a file hash must change the manifest hash.
        let different = TransferManifest::new(
            vec![
                sample_entry(100, 0xAA, "/a"),
                sample_entry(200, 0xCC, "/b"), // different hash byte
            ],
            DEFAULT_BLOCK_SIZE,
        )
        .unwrap();

        assert_ne!(m1.manifest_hash(), different.manifest_hash());

        // Verify it equals BLAKE3(hash_a || hash_b).
        let mut hasher = Hasher::new();
        hasher.update(&[0xAA; 32]);
        hasher.update(&[0xBB; 32]);
        let expected = *hasher.finalize().as_bytes();
        assert_eq!(m1.manifest_hash(), expected);
    }
}
