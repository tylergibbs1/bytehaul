//! Storage abstraction for ByteHaul.
//!
//! Defines the [`Storage`] trait that abstracts over local filesystem, S3,
//! GCS, and other backends. The engine reads/writes chunks through this
//! trait, enabling transfers directly from/to object storage without
//! staging to local disk.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

/// Metadata about a file in storage.
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub path: PathBuf,
    pub size: u64,
}

/// Abstraction over storage backends (local fs, S3, GCS).
///
/// All methods are async to support network-backed storage.
/// Implementations must be Send + Sync for use in concurrent transfers.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Read `len` bytes from `path` starting at `offset`.
    async fn read_chunk(&self, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>, std::io::Error>;

    /// Write `data` to `path` at `offset`.
    async fn write_chunk(&self, path: &Path, offset: u64, data: &[u8]) -> Result<(), std::io::Error>;

    /// List all files under `prefix` (recursive).
    async fn list_files(&self, prefix: &Path) -> Result<Vec<FileMeta>, std::io::Error>;

    /// Get metadata for a single file.
    async fn metadata(&self, path: &Path) -> Result<FileMeta, std::io::Error>;

    /// Create a file with the given size (pre-allocate).
    async fn create_file(&self, path: &Path, size: u64) -> Result<(), std::io::Error>;
}

/// Local filesystem storage implementation.
#[derive(Debug, Clone, Default)]
pub struct LocalStorage;

#[async_trait]
impl Storage for LocalStorage {
    async fn read_chunk(&self, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>, std::io::Error> {
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn write_chunk(&self, path: &Path, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        file.flush().await?;
        Ok(())
    }

    async fn list_files(&self, prefix: &Path) -> Result<Vec<FileMeta>, std::io::Error> {
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(prefix).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_file() {
                files.push(FileMeta {
                    path: entry.path(),
                    size: meta.len(),
                });
            }
        }
        Ok(files)
    }

    async fn metadata(&self, path: &Path) -> Result<FileMeta, std::io::Error> {
        let meta = tokio::fs::metadata(path).await?;
        Ok(FileMeta {
            path: path.to_path_buf(),
            size: meta.len(),
        })
    }

    async fn create_file(&self, path: &Path, size: u64) -> Result<(), std::io::Error> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await?;
        if size > 0 {
            file.set_len(size).await?;
        }
        Ok(())
    }
}

/// Parse a storage URL and return the appropriate backend + path.
///
/// Supported URL schemes:
/// - `s3://bucket/prefix/path` → S3 (requires `s3` feature)
/// - `gs://bucket/prefix/path` → GCS (requires `gcs` feature)
/// - Everything else → local filesystem
pub fn parse_storage_url(url: &str) -> (StorageBackend, String) {
    if let Some(rest) = url.strip_prefix("s3://") {
        (StorageBackend::S3, rest.to_string())
    } else if let Some(rest) = url.strip_prefix("gs://") {
        (StorageBackend::Gcs, rest.to_string())
    } else {
        (StorageBackend::Local, url.to_string())
    }
}

/// Which storage backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackend {
    Local,
    S3,
    Gcs,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn local_storage_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.bin");
        let storage = LocalStorage;

        // Create file
        storage.create_file(&path, 1024).await.unwrap();

        // Write chunk
        let data = vec![0xAA; 512];
        storage.write_chunk(&path, 0, &data).await.unwrap();
        storage.write_chunk(&path, 512, &data).await.unwrap();

        // Read chunk
        let read = storage.read_chunk(&path, 256, 256).await.unwrap();
        assert_eq!(read.len(), 256);
        assert!(read.iter().all(|&b| b == 0xAA));

        // Metadata
        let meta = storage.metadata(&path).await.unwrap();
        assert_eq!(meta.size, 1024);
    }

    #[test]
    fn parse_urls() {
        assert_eq!(
            parse_storage_url("s3://my-bucket/data/file.bin"),
            (StorageBackend::S3, "my-bucket/data/file.bin".to_string())
        );
        assert_eq!(
            parse_storage_url("gs://bucket/prefix"),
            (StorageBackend::Gcs, "bucket/prefix".to_string())
        );
        assert_eq!(
            parse_storage_url("/tmp/local/file.bin"),
            (StorageBackend::Local, "/tmp/local/file.bin".to_string())
        );
    }
}
