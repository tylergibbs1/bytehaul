//! Encryption-at-rest for ByteHaul transfer state files.
//!
//! State files are encrypted with XChaCha20-Poly1305 so that transfer metadata
//! (file paths, hashes, block maps) is not stored in plaintext on disk.  The
//! encryption key lives in a separate key file with restrictive permissions.
//!
//! # Wire format
//!
//! ```text
//! [1 byte version = 0x01] [24-byte nonce] [ciphertext + 16-byte Poly1305 tag]
//! ```

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use chacha20poly1305::aead::generic_array::GenericArray;
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::XChaCha20Poly1305;
use rand::RngCore as _;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the crypto layer.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// An I/O error occurred while reading or writing the key file.
    #[error("key file I/O error: {0}")]
    KeyFileIo(#[from] std::io::Error),

    /// The key file did not contain exactly 32 bytes.
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLength(usize),

    /// AEAD encryption failed.
    #[error("encryption failed")]
    EncryptionFailed,

    /// AEAD decryption or authentication failed.
    #[error("decryption failed")]
    DecryptionFailed,

    /// The encrypted payload has an unrecognised version or is too short.
    #[error("invalid encrypted format")]
    InvalidFormat,
}

/// Convenience type alias.
pub type Result<T> = std::result::Result<T, CryptoError>;

/// Current envelope version byte.
const VERSION: u8 = 0x01;

/// Size of the XChaCha20-Poly1305 nonce in bytes.
const NONCE_LEN: usize = 24;

/// Minimum valid ciphertext length: version (1) + nonce (24) + tag (16).
const MIN_ENCRYPTED_LEN: usize = 1 + NONCE_LEN + 16;

// ---------------------------------------------------------------------------
// StateEncryptor
// ---------------------------------------------------------------------------

/// Encrypts and decrypts transfer state blobs using XChaCha20-Poly1305.
#[derive(Debug)]
pub struct StateEncryptor {
    key: [u8; 32],
}

impl StateEncryptor {
    /// Load (or create) an encryption key from a file on disk.
    ///
    /// * If `path` exists, reads 32 bytes from it.
    /// * If `path` does not exist, generates 32 cryptographically random bytes,
    ///   writes them to a new file with mode `0o600`, and returns the key.
    pub fn from_key_file(path: &Path) -> Result<Self> {
        if path.exists() {
            let data = fs::read(path)?;
            if data.len() != 32 {
                return Err(CryptoError::InvalidKeyLength(data.len()));
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&data);
            Ok(Self { key })
        } else {
            let mut key = [0u8; 32];
            rand::rng().fill_bytes(&mut key);

            // Write with restrictive permissions (owner read/write only).
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            file.write_all(&key)?;
            file.sync_all()?;

            Ok(Self { key })
        }
    }

    /// Encrypt `plaintext` and return the versioned envelope.
    ///
    /// Output layout: `[0x01] [24-byte nonce] [ciphertext || 16-byte tag]`
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&self.key));
        use chacha20poly1305::AeadCore;
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| CryptoError::EncryptionFailed)?;

        let mut output = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        output.push(VERSION);
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// Decrypt a versioned envelope produced by [`Self::encrypt`].
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < MIN_ENCRYPTED_LEN {
            return Err(CryptoError::InvalidFormat);
        }
        if data[0] != VERSION {
            return Err(CryptoError::InvalidFormat);
        }

        let nonce = GenericArray::from_slice(&data[1..1 + NONCE_LEN]);
        let ciphertext = &data[1 + NONCE_LEN..];

        let cipher = XChaCha20Poly1305::new(GenericArray::from_slice(&self.key));
        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| CryptoError::DecryptionFailed)?;

        Ok(plaintext)
    }
}

/// Heuristic check: does `data` look like an encrypted state envelope?
///
/// Encrypted envelopes start with the version byte `0x01`.  Unencrypted state
/// files are JSON and therefore start with `{`.
pub fn is_encrypted(data: &[u8]) -> bool {
    data.first() == Some(&VERSION)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Helper: create a `StateEncryptor` with a random in-memory key.
    fn random_encryptor() -> StateEncryptor {
        let mut key = [0u8; 32];
        rand::rng().fill_bytes(&mut key);
        StateEncryptor { key }
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let enc = random_encryptor();
        let plaintext = b"hello, bytehaul state";

        let ciphertext = enc.encrypt(plaintext).unwrap();
        assert_ne!(&ciphertext, plaintext);
        assert!(ciphertext.len() >= MIN_ENCRYPTED_LEN);

        let decrypted = enc.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_empty_plaintext() {
        let enc = random_encryptor();
        let ciphertext = enc.encrypt(b"").unwrap();
        let decrypted = enc.decrypt(&ciphertext).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn detect_encrypted_vs_plaintext() {
        let enc = random_encryptor();
        let ciphertext = enc.encrypt(b"test").unwrap();
        assert!(is_encrypted(&ciphertext));

        let json = b"{\"transfer_id\":\"abc\"}";
        assert!(!is_encrypted(json));

        assert!(!is_encrypted(&[]));
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let enc1 = random_encryptor();
        let enc2 = random_encryptor();

        let ciphertext = enc1.encrypt(b"secret data").unwrap();
        let result = enc2.decrypt(&ciphertext);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CryptoError::DecryptionFailed));
    }

    #[test]
    fn invalid_format_rejected() {
        let enc = random_encryptor();

        // Too short.
        assert!(matches!(
            enc.decrypt(&[0x01]),
            Err(CryptoError::InvalidFormat)
        ));

        // Wrong version byte.
        let mut bad = enc.encrypt(b"x").unwrap();
        bad[0] = 0xFF;
        assert!(matches!(
            enc.decrypt(&bad),
            Err(CryptoError::InvalidFormat)
        ));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let enc = random_encryptor();
        let mut ciphertext = enc.encrypt(b"important data").unwrap();
        // Flip a byte in the ciphertext region.
        let last = ciphertext.len() - 1;
        ciphertext[last] ^= 0xFF;
        assert!(matches!(
            enc.decrypt(&ciphertext),
            Err(CryptoError::DecryptionFailed)
        ));
    }

    #[test]
    fn key_file_creation_and_loading() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join("state.key");

        // First call creates the file.
        let enc1 = StateEncryptor::from_key_file(&key_path).unwrap();
        assert!(key_path.exists());

        // File should contain exactly 32 bytes.
        let raw = fs::read(&key_path).unwrap();
        assert_eq!(raw.len(), 32);

        // Second call loads the same key.
        let enc2 = StateEncryptor::from_key_file(&key_path).unwrap();
        assert_eq!(enc1.key, enc2.key);

        // Round-trip across instances.
        let ct = enc1.encrypt(b"persistence test").unwrap();
        let pt = enc2.decrypt(&ct).unwrap();
        assert_eq!(pt, b"persistence test");
    }

    #[test]
    fn key_file_wrong_length_rejected() {
        let dir = tempdir().unwrap();
        let key_path = dir.path().join("bad.key");
        fs::write(&key_path, &[0u8; 16]).unwrap();

        let result = StateEncryptor::from_key_file(&key_path);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CryptoError::InvalidKeyLength(16)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn key_file_has_restrictive_permissions() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempdir().unwrap();
        let key_path = dir.path().join("perms.key");
        StateEncryptor::from_key_file(&key_path).unwrap();

        let mode = fs::metadata(&key_path).unwrap().mode();
        // Lower 9 bits should be 0o600 (owner rw only).
        assert_eq!(mode & 0o777, 0o600);
    }
}
