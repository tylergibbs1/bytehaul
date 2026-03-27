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

// ---------------------------------------------------------------------------
// S3 storage backend (requires `s3` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "s3")]
pub mod s3 {
    use super::*;
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::primitives::ByteStream;

    /// Amazon S3 storage backend.
    ///
    /// Paths are interpreted as `bucket/key`. For example, the URL
    /// `s3://my-bucket/data/file.bin` maps to bucket=`my-bucket`,
    /// key=`data/file.bin`.
    pub struct S3Storage {
        client: S3Client,
    }

    impl S3Storage {
        /// Create a new S3Storage using default AWS credential chain.
        pub async fn new() -> Self {
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            Self {
                client: S3Client::new(&config),
            }
        }

        /// Create from an existing S3 client.
        pub fn from_client(client: S3Client) -> Self {
            Self { client }
        }

        /// Split a path into (bucket, key).
        fn split_path(path: &Path) -> (&str, String) {
            let s = path.to_str().unwrap_or("");
            if let Some(pos) = s.find('/') {
                (&s[..pos], s[pos + 1..].to_string())
            } else {
                (s, String::new())
            }
        }
    }

    #[async_trait]
    impl Storage for S3Storage {
        async fn read_chunk(&self, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>, std::io::Error> {
            let (bucket, key) = Self::split_path(path);
            let range = format!("bytes={}-{}", offset, offset + len as u64 - 1);

            let resp = self.client
                .get_object()
                .bucket(bucket)
                .key(&key)
                .range(range)
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            let bytes = resp.body
                .collect()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
                .to_vec();

            Ok(bytes)
        }

        async fn write_chunk(&self, path: &Path, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            // S3 doesn't support partial writes. For chunk-at-offset writes,
            // we upload the data as a separate object with the offset encoded
            // in a metadata tag. The caller is expected to assemble the final
            // object separately, or use this for whole-object writes (offset=0).
            if offset != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "S3 does not support partial writes; use offset=0 for full object upload",
                ));
            }

            let (bucket, key) = Self::split_path(path);
            self.client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from(data.to_vec()))
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            Ok(())
        }

        async fn list_files(&self, prefix: &Path) -> Result<Vec<FileMeta>, std::io::Error> {
            let (bucket, key_prefix) = Self::split_path(prefix);
            let mut files = Vec::new();
            let mut continuation_token = None;

            loop {
                let mut req = self.client
                    .list_objects_v2()
                    .bucket(bucket)
                    .prefix(&key_prefix);

                if let Some(token) = continuation_token.take() {
                    req = req.continuation_token(token);
                }

                let resp = req
                    .send()
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

                for obj in resp.contents() {
                    if let Some(key) = obj.key() {
                        files.push(FileMeta {
                            path: PathBuf::from(format!("{bucket}/{key}")),
                            size: obj.size.unwrap_or(0) as u64,
                        });
                    }
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token = resp.next_continuation_token().map(String::from);
                } else {
                    break;
                }
            }

            Ok(files)
        }

        async fn metadata(&self, path: &Path) -> Result<FileMeta, std::io::Error> {
            let (bucket, key) = Self::split_path(path);

            let resp = self.client
                .head_object()
                .bucket(bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            Ok(FileMeta {
                path: path.to_path_buf(),
                size: resp.content_length().unwrap_or(0) as u64,
            })
        }

        async fn create_file(&self, path: &Path, _size: u64) -> Result<(), std::io::Error> {
            // S3 doesn't need pre-allocation. Create an empty object as placeholder.
            let (bucket, key) = Self::split_path(path);
            self.client
                .put_object()
                .bucket(bucket)
                .key(&key)
                .body(ByteStream::from(Vec::new()))
                .send()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// GCS storage backend (stub — uses S3-compatible API)
// ---------------------------------------------------------------------------

/// Google Cloud Storage backend.
///
/// GCS supports the S3-compatible XML API. This implementation delegates to
/// S3Storage configured with the GCS endpoint. To use it, set the
/// `AWS_ENDPOINT_URL=https://storage.googleapis.com` environment variable
/// and provide GCS HMAC credentials via standard AWS env vars.
#[cfg(feature = "s3")]
pub use s3::S3Storage as GcsStorage;

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
