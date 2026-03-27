//! Resume and transfer state management for ByteHaul.
//!
//! The receiver maintains a transfer state file on disk at
//! `~/.bytehaul/state/<transfer_id>.json`. State is atomically persisted
//! (write-to-temp, fsync, rename) so that a crash never leaves a corrupt
//! state file. On restart any partially written chunk is discarded and
//! re-requested -- only fully written and verified chunks count as received.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use bitvec::prelude::*;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the resume/state layer.
#[derive(Debug, Error)]
pub enum ResumeError {
    /// An I/O error occurred while reading or writing state.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization failed.
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Could not determine the user's home directory.
    #[error("failed to determine home directory")]
    NoHomeDir,

    /// State directory is invalid or inaccessible.
    #[error("state directory error: {0}")]
    StateDir(String),

    /// Encryption or decryption failed.
    #[error("encryption error: {0}")]
    Crypto(String),

    /// A block index was outside the valid range.
    #[error("block index {index} out of range (total_blocks={total_blocks})")]
    BlockOutOfRange {
        /// The invalid block index.
        index: u64,
        /// Total number of blocks in the transfer.
        total_blocks: u64,
    },
}

/// Convenience type alias.
pub type Result<T> = std::result::Result<T, ResumeError>;

/// Default garbage-collection max age: 7 days.
pub const DEFAULT_GC_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

// ---------------------------------------------------------------------------
// TransferState
// ---------------------------------------------------------------------------

/// Persistent state for a single in-progress transfer.
///
/// `blocks_received` is stored as a sorted `Vec<u64>` of block indices so
/// that the JSON representation is human-readable. Helper methods convert
/// to/from `BitVec` for efficient in-memory operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferState {
    /// Deterministic transfer identifier (hex-encoded BLAKE3).
    pub transfer_id: String,
    /// Destination file path on the receiver.
    pub file_path: String,
    /// Total size of the file in bytes.
    pub total_size: u64,
    /// Block (chunk) size in bytes.
    pub block_size: u32,
    /// Total number of blocks in the transfer.
    pub total_blocks: u64,
    /// Sorted list of received block indices.
    pub blocks_received: Vec<u64>,
    /// Hex-encoded BLAKE3 hash of the complete source file.
    pub blake3_file: String,
    /// Timestamp of state creation.
    pub created_at: DateTime<Utc>,
    /// Timestamp of the most recent activity.
    pub last_activity: DateTime<Utc>,
}

impl TransferState {
    /// Create a new, empty transfer state.
    pub fn new(
        transfer_id: String,
        file_path: String,
        total_size: u64,
        block_size: u32,
        total_blocks: u64,
        blake3_file: String,
    ) -> Self {
        let now = Utc::now();
        Self {
            transfer_id,
            file_path,
            total_size,
            block_size,
            total_blocks,
            blocks_received: Vec::new(),
            blake3_file,
            created_at: now,
            last_activity: now,
        }
    }

    /// Mark a block as received. The block is inserted in sorted order and
    /// `last_activity` is bumped to now. Duplicate inserts are idempotent.
    pub fn mark_received(&mut self, block_index: u64) {
        self.last_activity = Utc::now();
        match self.blocks_received.binary_search(&block_index) {
            Ok(_) => {} // already present
            Err(pos) => self.blocks_received.insert(pos, block_index),
        }
    }

    /// Returns `true` if the given block has already been received.
    pub fn is_received(&self, block_index: u64) -> bool {
        self.blocks_received.binary_search(&block_index).is_ok()
    }

    /// Number of blocks received so far.
    pub fn received_count(&self) -> u64 {
        self.blocks_received.len() as u64
    }

    /// Returns all block indices that have **not** yet been received, in
    /// ascending order.
    pub fn remaining_blocks(&self) -> Vec<u64> {
        let mut remaining = Vec::new();
        let mut recv_iter = self.blocks_received.iter().peekable();
        for idx in 0..self.total_blocks {
            if recv_iter.peek() == Some(&&idx) {
                recv_iter.next();
            } else {
                remaining.push(idx);
            }
        }
        remaining
    }

    /// Convert `blocks_received` into a `BitVec` of length `total_blocks`.
    pub fn to_bitvec(&self) -> BitVec {
        let len = self.total_blocks as usize;
        let mut bv = bitvec![0; len];
        for &idx in &self.blocks_received {
            if (idx as usize) < len {
                bv.set(idx as usize, true);
            }
        }
        bv
    }

    /// Convert a `BitVec` back into a sorted `Vec<u64>` of set bit indices.
    pub fn from_bitvec(bitvec: &BitVec) -> Vec<u64> {
        bitvec
            .iter()
            .enumerate()
            .filter_map(|(i, bit)| if *bit { Some(i as u64) } else { None })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// StateManager
// ---------------------------------------------------------------------------

/// Manages the on-disk lifecycle of [`TransferState`] files.
///
/// When `encrypt` is true, state files are encrypted with ChaCha20-Poly1305
/// using a key derived from the transfer ID. Encrypted files use the `.enc`
/// extension instead of `.json`.
pub struct StateManager {
    state_dir: PathBuf,
    encrypt: bool,
}

impl StateManager {
    /// Create a new `StateManager`.
    ///
    /// If `state_dir` is `None`, defaults to `~/.bytehaul/state/`.
    pub fn new(state_dir: Option<PathBuf>) -> Result<Self> {
        Self::with_encryption(state_dir, false)
    }

    /// Create a `StateManager` with optional encryption.
    pub fn with_encryption(state_dir: Option<PathBuf>, encrypt: bool) -> Result<Self> {
        let state_dir = match state_dir {
            Some(d) => d,
            None => {
                let home = dirs_fallback_home()?;
                home.join(".bytehaul").join("state")
            }
        };
        fs::create_dir_all(&state_dir)?;
        Ok(Self { state_dir, encrypt })
    }

    /// Load state for `transfer_id` from disk. Returns `Ok(None)` when no
    /// state file exists. Transparently decrypts if the encrypted file is
    /// found.
    pub fn load(&self, transfer_id: &str) -> Result<Option<TransferState>> {
        // Try encrypted file first, then plaintext.
        let enc_path = self.state_path_enc(transfer_id);
        let json_path = self.state_path(transfer_id);

        if enc_path.exists() {
            let ciphertext = fs::read(&enc_path)?;
            let json = Self::decrypt(transfer_id, &ciphertext)?;
            let state: TransferState = serde_json::from_str(&json)?;
            return Ok(Some(state));
        }
        if json_path.exists() {
            let data = fs::read_to_string(&json_path)?;
            let state: TransferState = serde_json::from_str(&data)?;
            return Ok(Some(state));
        }
        Ok(None)
    }

    /// Atomically persist `state` to disk.
    ///
    /// Writes to a temporary file in the same directory, calls `fsync`, then
    /// renames over the target path. This guarantees that readers always see
    /// either the old or the new version -- never a partial write.
    ///
    /// When encryption is enabled, the JSON is encrypted with ChaCha20-Poly1305
    /// and written with an `.enc` extension.
    pub fn save(&self, state: &TransferState) -> Result<()> {
        let target = if self.encrypt {
            self.state_path_enc(&state.transfer_id)
        } else {
            self.state_path(&state.transfer_id)
        };
        let dir = target
            .parent()
            .ok_or_else(|| ResumeError::StateDir("cannot determine parent dir".into()))?;

        let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
        let json = serde_json::to_string_pretty(state)?;

        if self.encrypt {
            let ciphertext = Self::encrypt(&state.transfer_id, &json)?;
            tmp.write_all(&ciphertext)?;
        } else {
            tmp.write_all(json.as_bytes())?;
        }

        tmp.as_file().sync_all()?;
        tmp.persist(&target).map_err(|e| e.error)?;

        if let Ok(dir_file) = fs::File::open(dir) {
            let _ = dir_file.sync_all();
        }

        Ok(())
    }

    /// Delete the state file for `transfer_id`. No error if it does not exist.
    /// Removes both plaintext and encrypted variants.
    pub fn delete(&self, transfer_id: &str) -> Result<()> {
        for path in [
            self.state_path(transfer_id),
            self.state_path_enc(transfer_id),
        ] {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Count state files that would be garbage-collected (dry-run).
    pub fn gc_preview(&self, max_age: Duration) -> Result<usize> {
        self.gc_inner(max_age, false)
    }

    /// Garbage-collect state files whose `last_activity` is older than
    /// `max_age`. Returns the number of files deleted.
    pub fn gc(&self, max_age: Duration) -> Result<usize> {
        self.gc_inner(max_age, true)
    }

    fn gc_inner(&self, max_age: Duration, delete: bool) -> Result<usize> {
        let cutoff = Utc::now()
            - chrono::Duration::from_std(max_age)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));

        let mut count = 0usize;
        for entry in fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("json") && ext != Some("enc") {
                continue;
            }

            let state: TransferState = if ext == Some("enc") {
                let tid = match path.file_stem().and_then(|s| s.to_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let data = match fs::read(&path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let json = match Self::decrypt(&tid, &data) {
                    Ok(j) => j,
                    Err(_) => continue,
                };
                match serde_json::from_str(&json) {
                    Ok(s) => s,
                    Err(_) => continue,
                }
            } else {
                let data = match fs::read_to_string(&path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                match serde_json::from_str(&data) {
                    Ok(s) => s,
                    Err(_) => continue,
                }
            };
            if state.last_activity < cutoff {
                if delete {
                    let _ = fs::remove_file(&path);
                }
                count += 1;
            }
        }
        Ok(count)
    }

    // -- private helpers ----------------------------------------------------

    fn state_path(&self, transfer_id: &str) -> PathBuf {
        self.state_dir.join(format!("{transfer_id}.json"))
    }

    fn state_path_enc(&self, transfer_id: &str) -> PathBuf {
        self.state_dir.join(format!("{transfer_id}.enc"))
    }

    /// Derive a 256-bit encryption key from the transfer ID using BLAKE3.
    /// The transfer ID is already a BLAKE3 hash, but we re-hash it with a
    /// domain separator to produce a distinct key.
    fn derive_key(transfer_id: &str) -> [u8; 32] {
        blake3::derive_key("bytehaul-state-encryption-v1", transfer_id.as_bytes())
    }

    /// Encrypt JSON state with ChaCha20-Poly1305.
    /// Output format: 12-byte nonce || ciphertext+tag.
    fn encrypt(transfer_id: &str, plaintext: &str) -> Result<Vec<u8>> {
        let key = Self::derive_key(transfer_id);
        let cipher = ChaCha20Poly1305::new_from_slice(&key)
            .map_err(|e| ResumeError::Crypto(format!("key init: {e}")))?;

        // Use a random nonce for each save.
        let mut nonce_bytes = [0u8; 12];
        rand::Fill::fill(&mut nonce_bytes, &mut rand::rng());
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|e| ResumeError::Crypto(format!("encrypt: {e}")))?;

        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt state from nonce || ciphertext+tag.
    fn decrypt(transfer_id: &str, data: &[u8]) -> Result<String> {
        if data.len() < 12 {
            return Err(ResumeError::Crypto("encrypted state too short".into()));
        }

        let key = Self::derive_key(transfer_id);
        let cipher = ChaCha20Poly1305::new_from_slice(&key)
            .map_err(|e| ResumeError::Crypto(format!("key init: {e}")))?;

        let nonce = Nonce::from_slice(&data[..12]);
        let plaintext = cipher
            .decrypt(nonce, &data[12..])
            .map_err(|e| ResumeError::Crypto(format!("decrypt: {e}")))?;

        String::from_utf8(plaintext)
            .map_err(|e| ResumeError::Crypto(format!("utf8: {e}")))
    }
}

/// Best-effort home directory detection without pulling in the `dirs` crate.
fn dirs_fallback_home() -> Result<PathBuf> {
    // Try the HOME env var (always set on macOS/Linux, usually on Windows too).
    if let Ok(home) = std::env::var("HOME") {
        return Ok(PathBuf::from(home));
    }
    // Windows fallback
    if let Ok(profile) = std::env::var("USERPROFILE") {
        return Ok(PathBuf::from(profile));
    }
    Err(ResumeError::NoHomeDir)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_state() -> TransferState {
        TransferState::new(
            "abc123".into(),
            "/tmp/test.bin".into(),
            1024 * 1024,
            65536,
            16,
            "deadbeef".into(),
        )
    }

    #[test]
    fn mark_received_inserts_in_sorted_order() {
        let mut state = sample_state();
        assert!(!state.is_received(0));
        state.mark_received(3);
        state.mark_received(1);
        state.mark_received(3); // duplicate
        assert!(state.is_received(1));
        assert!(state.is_received(3));
        assert!(!state.is_received(2));
        assert_eq!(state.received_count(), 2);
        assert_eq!(state.blocks_received, vec![1, 3]);
    }

    #[test]
    fn remaining_blocks_excludes_received() {
        let mut state = sample_state();
        state.mark_received(0);
        state.mark_received(2);
        state.mark_received(15);
        let remaining = state.remaining_blocks();
        assert!(!remaining.contains(&0));
        assert!(remaining.contains(&1));
        assert!(!remaining.contains(&2));
        assert!(remaining.contains(&3));
        assert!(!remaining.contains(&15));
        assert_eq!(remaining.len(), 13);
    }

    #[test]
    fn bitvec_roundtrip_preserves_indices() {
        let mut state = sample_state();
        state.mark_received(0);
        state.mark_received(5);
        state.mark_received(15);

        let bv = state.to_bitvec();
        assert_eq!(bv.len(), 16);
        assert!(bv[0]);
        assert!(!bv[1]);
        assert!(bv[5]);
        assert!(bv[15]);

        let roundtripped = TransferState::from_bitvec(&bv);
        assert_eq!(roundtripped, vec![0, 5, 15]);
    }

    #[test]
    fn save_load_delete_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = StateManager::new(Some(tmp.path().to_path_buf())).unwrap();

        let mut state = sample_state();
        state.mark_received(4);
        state.mark_received(7);

        mgr.save(&state).unwrap();

        let loaded = mgr.load("abc123").unwrap().expect("state should exist");
        assert_eq!(loaded.transfer_id, "abc123");
        assert_eq!(loaded.blocks_received, vec![4, 7]);
        assert_eq!(loaded.total_blocks, 16);

        mgr.delete("abc123").unwrap();
        assert!(mgr.load("abc123").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = StateManager::new(Some(tmp.path().to_path_buf())).unwrap();
        mgr.delete("nonexistent").unwrap();
    }

    #[test]
    fn gc_removes_old_state_files() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = StateManager::new(Some(tmp.path().to_path_buf())).unwrap();

        // Create a state with old last_activity
        let mut old_state = sample_state();
        old_state.transfer_id = "old_transfer".into();
        old_state.last_activity = Utc::now() - chrono::Duration::days(10);
        mgr.save(&old_state).unwrap();

        // Create a recent state
        let recent_state = sample_state();
        mgr.save(&recent_state).unwrap();

        let deleted = mgr.gc(DEFAULT_GC_MAX_AGE).unwrap();
        assert_eq!(deleted, 1);

        // Old one should be gone, recent one should remain.
        assert!(mgr.load("old_transfer").unwrap().is_none());
        assert!(mgr.load("abc123").unwrap().is_some());
    }

    #[test]
    fn encrypted_save_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = StateManager::with_encryption(Some(tmp.path().to_path_buf()), true).unwrap();

        let mut state = sample_state();
        state.mark_received(4);
        state.mark_received(7);

        mgr.save(&state).unwrap();

        // Encrypted file should exist, not plaintext
        assert!(!tmp.path().join("abc123.json").exists());
        assert!(tmp.path().join("abc123.enc").exists());

        let loaded = mgr.load("abc123").unwrap().expect("state should exist");
        assert_eq!(loaded.transfer_id, "abc123");
        assert_eq!(loaded.blocks_received, vec![4, 7]);

        mgr.delete("abc123").unwrap();
        assert!(!tmp.path().join("abc123.enc").exists());
        assert!(mgr.load("abc123").unwrap().is_none());
    }

    #[test]
    fn unencrypted_manager_reads_encrypted_file() {
        let tmp = tempfile::tempdir().unwrap();
        let enc_mgr = StateManager::with_encryption(Some(tmp.path().to_path_buf()), true).unwrap();

        let mut state = sample_state();
        state.mark_received(1);
        enc_mgr.save(&state).unwrap();

        // An unencrypted manager should still find and decrypt the .enc file
        let plain_mgr = StateManager::new(Some(tmp.path().to_path_buf())).unwrap();
        let loaded = plain_mgr.load("abc123").unwrap().expect("should find encrypted state");
        assert_eq!(loaded.blocks_received, vec![1]);
    }

    #[test]
    fn json_serialization_roundtrip() {
        let mut state = sample_state();
        state.mark_received(2);
        state.mark_received(10);

        let json = serde_json::to_string_pretty(&state).unwrap();
        let deserialized: TransferState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);
    }
}
