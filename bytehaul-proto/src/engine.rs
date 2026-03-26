//! Sender and receiver transfer engines for ByteHaul.
//!
//! The [`Sender`] reads a local file and streams chunks over QUIC, while the
//! [`Receiver`] reassembles incoming chunks and verifies integrity.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bitvec::prelude::*;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::chunking::{ChunkPlan, ChunkReader, ChunkScheduler, ChunkWriter};
use crate::congestion::BandwidthLimiter;
use crate::manifest::{FileEntry, TransferManifest};
use crate::resume::{StateManager, TransferState};
use crate::transport::QuicConnection;
use crate::verify;
use crate::wire::{self, ChunkHeader, ControlMessage, ErrorCode};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the transfer engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A BLAKE3 verification check failed.
    #[error("verification error: {0}")]
    Verify(#[from] crate::verify::VerifyError),

    /// A wire protocol encoding/decoding error.
    #[error("wire protocol error: {0}")]
    Wire(#[from] crate::wire::WireError),

    /// A QUIC transport-level error.
    #[error("transport error: {0}")]
    Transport(#[from] crate::transport::TransportError),

    /// A chunk I/O error.
    #[error("chunk error: {0}")]
    Chunk(#[from] crate::chunking::ChunkError),

    /// A resume/state persistence error.
    #[error("resume error: {0}")]
    Resume(#[from] crate::resume::ResumeError),

    /// A manifest construction or validation error.
    #[error("manifest error: {0}")]
    Manifest(#[from] crate::manifest::ManifestError),

    /// A raw I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Bincode (de)serialization failed.
    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    /// A tokio semaphore was closed unexpectedly.
    #[error("semaphore acquire error: {0}")]
    Semaphore(#[from] tokio::sync::AcquireError),

    /// A spawned task panicked.
    #[error("task join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    /// A protocol-level violation (unexpected message, invalid state, etc.).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The remote peer reported an application-level error.
    #[error("remote error ({code:?}): {message}")]
    RemoteError {
        /// The error code sent by the remote peer.
        code: ErrorCode,
        /// Human-readable error description.
        message: String,
    },

    /// File or chunk hash did not match after transfer.
    #[error("hash mismatch: {0}")]
    HashMismatch(String),
}

/// Convenience type alias for engine operations.
pub type Result<T> = std::result::Result<T, EngineError>;

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

/// Progress information for a transfer.
#[derive(Debug, Clone)]
pub struct TransferProgress {
    /// Bytes transferred so far.
    pub transferred_bytes: u64,
    /// Total bytes to transfer.
    pub total_bytes: u64,
    /// Chunks transferred so far.
    pub transferred_chunks: u64,
    /// Total number of chunks.
    pub total_chunks: u64,
    /// Current transfer speed in bytes per second.
    pub speed_bytes_per_sec: f64,
    /// Elapsed time in seconds.
    pub elapsed_secs: f64,
}

impl TransferProgress {
    /// Speed in megabytes per second.
    pub fn speed_mbps(&self) -> f64 {
        self.speed_bytes_per_sec / (1024.0 * 1024.0)
    }

    /// Fraction of transfer complete (0.0 to 1.0).
    pub fn fraction(&self) -> f64 {
        if self.total_bytes == 0 {
            1.0
        } else {
            self.transferred_bytes as f64 / self.total_bytes as f64
        }
    }
}

/// Callback type for progress updates.
pub type ProgressCallback = Box<dyn Fn(TransferProgress) + Send + Sync>;

/// What to do when the destination file already exists.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum OverwriteMode {
    /// Fail if the destination file exists (default).
    #[default]
    Fail,
    /// Rename the new file (append .1, .2, etc.) if the destination exists.
    Rename,
    /// Overwrite the existing file.
    Overwrite,
}

// ---------------------------------------------------------------------------
// EngineConfig
// ---------------------------------------------------------------------------

/// Configuration for a send/receive operation.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Maximum number of parallel QUIC streams for chunk transfer.
    pub max_parallel_streams: usize,
    /// Block (chunk) size in bytes.
    pub block_size: u32,
    /// Whether transfer resumption is enabled.
    pub resume_enabled: bool,
    /// Directory for resume state files (defaults to `~/.bytehaul/state/`).
    pub state_dir: Option<PathBuf>,
    /// How often (in chunks) to report progress.
    pub progress_interval_chunks: u64,
    /// How to handle existing destination files.
    pub overwrite_mode: OverwriteMode,
    /// Whether delta transfers are enabled (only send changed blocks).
    pub delta_enabled: bool,
    /// Whether to encrypt transfer state files at rest.
    pub encrypt_state: bool,
    /// Maximum bandwidth in bytes per second (0 = unlimited).
    pub max_bandwidth_bps: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_parallel_streams: 16,
            block_size: crate::manifest::DEFAULT_BLOCK_SIZE,
            resume_enabled: true,
            state_dir: None,
            progress_interval_chunks: 64,
            overwrite_mode: OverwriteMode::default(),
            delta_enabled: false,
            encrypt_state: false,
            max_bandwidth_bps: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

/// Sender engine: reads a local file and sends it over a QUIC connection.
pub struct Sender {
    config: EngineConfig,
    progress_cb: Option<ProgressCallback>,
}

impl Sender {
    /// Create a new sender with the given configuration.
    pub fn new(config: EngineConfig) -> Self {
        Self {
            config,
            progress_cb: None,
        }
    }

    /// Register a progress callback.
    pub fn on_progress<F: Fn(TransferProgress) + Send + Sync + 'static>(&mut self, cb: F) {
        self.progress_cb = Some(Box::new(cb));
    }

    /// Register an already-boxed progress callback.
    pub fn set_progress_callback(&mut self, cb: ProgressCallback) {
        self.progress_cb = Some(cb);
    }

    /// Send a single file over the given QUIC connection.
    pub async fn send_file(
        &self,
        conn: &QuicConnection,
        local_path: &Path,
        remote_dest: &str,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        // 1. Hash the file and build manifest
        info!("Hashing file: {}", local_path.display());
        let file_hash = verify::hash_file(local_path).await?;

        let file_size = tokio::fs::metadata(local_path).await?.len();

        let manifest = TransferManifest::new(
            vec![FileEntry {
                source_path: local_path.to_path_buf(),
                dest_path: PathBuf::from(remote_dest),
                size: file_size,
                blake3_hash: file_hash,
                relative_path: None,
            }],
            self.config.block_size,
        )?;

        info!(
            "Transfer {}: {} bytes, {} blocks",
            &manifest.transfer_id[..12],
            file_size,
            manifest.total_blocks()
        );

        // 2. Open control stream and send manifest
        let (mut ctrl_send, mut ctrl_recv) = conn.open_control_stream().await?;

        let manifest_bytes = bincode::serialize(&manifest)?;
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::Manifest { manifest_bytes },
        )
        .await?;

        // 3. Wait for resume state from receiver
        let resume_msg = wire::read_control_message(&mut ctrl_recv).await?;
        let received_blocks: Vec<u64> = match resume_msg {
            ControlMessage::ResumeState { blocks_received } => blocks_received,
            ControlMessage::Error { code, message } => {
                return Err(EngineError::RemoteError { code, message });
            }
            _ => {
                return Err(EngineError::Protocol(
                    "expected ResumeState message, got unexpected message".into(),
                ));
            }
        };

        let total_chunks = manifest.total_blocks();

        // Build bitfield from received blocks
        let mut bitfield = bitvec![0; total_chunks as usize];
        for idx in &received_blocks {
            if (*idx as usize) < bitfield.len() {
                bitfield.set(*idx as usize, true);
            }
        }

        let already_received = received_blocks.len() as u64;
        if already_received > 0 {
            info!(
                "Resuming: {}/{} chunks already received",
                already_received, total_chunks
            );
        }

        let chunk_plan = ChunkPlan::new(file_size, self.config.block_size);
        let mut scheduler = ChunkScheduler::new(chunk_plan, bitfield);

        if scheduler.is_complete() {
            info!("All chunks already received, sending completion");
            wire::write_control_message(
                &mut ctrl_send,
                &ControlMessage::TransferComplete {
                    file_blake3: file_hash,
                },
            )
            .await?;
            return Ok(());
        }

        // 4. Send chunks in parallel batches
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_streams));
        let mut sent_count = already_received;
        let mut rate_limiter = BandwidthLimiter::new(self.config.max_bandwidth_bps);

        let local_path = local_path.to_path_buf();
        let transfer_id_bytes =
            verify::hex_to_hash(&manifest.transfer_id).unwrap_or([0u8; 32]);

        while !scheduler.is_complete() {
            let batch = scheduler.next_batch(self.config.max_parallel_streams);
            if batch.is_empty() {
                break;
            }

            let mut handles = Vec::new();

            for chunk_idx in batch {
                // Apply bandwidth limiting before sending each chunk
                rate_limiter.wait_if_needed(self.config.block_size as u64).await;

                let permit = semaphore.clone().acquire_owned().await?;
                let chunk_meta = chunk_plan.chunk_meta(chunk_idx);
                let local_path = local_path.clone();
                let conn_inner = conn.inner().clone();
                let tid = transfer_id_bytes;

                let handle = tokio::spawn(async move {
                    let _permit = permit;

                    let meta = match chunk_meta {
                        Some(m) => m,
                        None => {
                            return Err(EngineError::Protocol(format!(
                                "invalid chunk index {chunk_idx}"
                            )))
                        }
                    };

                    // Read chunk data from file
                    let data =
                        ChunkReader::read_chunk(&local_path, meta.offset, meta.size).await?;

                    // Compute chunk hash
                    let chunk_hash = verify::hash_bytes(&data);

                    // Open a data stream and send
                    let (mut send, _recv) = conn_inner.open_bi().await.map_err(|e| {
                        EngineError::Protocol(format!("failed to open data stream: {e}"))
                    })?;

                    let header = ChunkHeader {
                        transfer_id: tid,
                        chunk_index: chunk_idx,
                        chunk_size: meta.size,
                        chunk_blake3: chunk_hash,
                    };

                    wire::write_chunk_header(&mut send, &header).await?;
                    wire::write_chunk_data(&mut send, &data).await?;
                    send.finish().map_err(|e| {
                        EngineError::Protocol(format!("failed to finish stream: {e}"))
                    })?;

                    debug!("Sent chunk {} ({} bytes)", chunk_idx, meta.size);
                    Ok::<u64, EngineError>(chunk_idx)
                });

                handles.push(handle);
            }

            // Wait for all in batch
            for handle in handles {
                match handle.await? {
                    Ok(idx) => {
                        scheduler.mark_received(idx);
                        sent_count += 1;

                        // Report progress
                        if let Some(ref cb) = self.progress_cb {
                            if sent_count.is_multiple_of(self.config.progress_interval_chunks)
                                || scheduler.is_complete()
                            {
                                let elapsed = start.elapsed().as_secs_f64();
                                let transferred =
                                    (sent_count * self.config.block_size as u64).min(file_size);
                                cb(TransferProgress {
                                    transferred_bytes: transferred,
                                    total_bytes: file_size,
                                    transferred_chunks: sent_count,
                                    total_chunks,
                                    speed_bytes_per_sec: if elapsed > 0.0 {
                                        transferred as f64 / elapsed
                                    } else {
                                        0.0
                                    },
                                    elapsed_secs: elapsed,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to send chunk: {}", e);
                        return Err(e);
                    }
                }
            }
        }

        // 5. Wait for receiver to confirm
        debug!("All chunks sent, waiting for receiver confirmation");

        loop {
            let msg = wire::read_control_message(&mut ctrl_recv).await?;
            match msg {
                ControlMessage::ProgressAck { .. } => {
                    continue;
                }
                ControlMessage::TransferComplete { file_blake3 } => {
                    if file_blake3 == file_hash {
                        info!("Transfer complete and verified by receiver");
                    } else {
                        return Err(EngineError::HashMismatch(
                            "file hash mismatch on receiver side".into(),
                        ));
                    }
                    break;
                }
                ControlMessage::Error { code, message } => {
                    return Err(EngineError::RemoteError { code, message });
                }
                _ => {
                    warn!("Unexpected control message while waiting for completion");
                }
            }
        }

        // 6. Send our own completion confirmation
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::TransferComplete {
                file_blake3: file_hash,
            },
        )
        .await?;

        let elapsed = start.elapsed();
        let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
            (file_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
        } else {
            0.0
        };
        info!(
            "Transfer complete: {} bytes in {:.1}s ({:.1} MB/s)",
            file_size,
            elapsed.as_secs_f64(),
            speed_mbps
        );

        Ok(())
    }

    /// Send a pre-built manifest (supports multi-file transfers).
    ///
    /// The manifest must already contain file entries with valid source paths
    /// and BLAKE3 hashes. Use `TransferManifest::from_directory` or
    /// `TransferManifest::from_paths` to build one.
    pub async fn send_transfer(
        &self,
        conn: &QuicConnection,
        manifest: &TransferManifest,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let total_size = manifest.total_size();
        let total_chunks = manifest.total_blocks();

        info!(
            "Transfer {}: {} files, {} bytes, {} blocks",
            &manifest.transfer_id[..12],
            manifest.files.len(),
            total_size,
            total_chunks
        );

        // 1. Open control stream and send manifest
        let (mut ctrl_send, mut ctrl_recv) = conn.open_control_stream().await?;
        let manifest_bytes = bincode::serialize(manifest)?;
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::Manifest { manifest_bytes },
        )
        .await?;

        // 2. Wait for resume state
        let resume_msg = wire::read_control_message(&mut ctrl_recv).await?;
        let received_blocks: Vec<u64> = match resume_msg {
            ControlMessage::ResumeState { blocks_received } => blocks_received,
            ControlMessage::Error { code, message } => {
                return Err(EngineError::RemoteError { code, message });
            }
            _ => {
                return Err(EngineError::Protocol(
                    "expected ResumeState message".into(),
                ));
            }
        };

        let mut bitfield = bitvec![0; total_chunks as usize];
        for idx in &received_blocks {
            if (*idx as usize) < bitfield.len() {
                bitfield.set(*idx as usize, true);
            }
        }

        // 3. Delta negotiation (if enabled)
        if self.config.delta_enabled {
            for (file_idx, _file) in manifest.files.iter().enumerate() {
                wire::write_control_message(
                    &mut ctrl_send,
                    &ControlMessage::DeltaRequest { file_index: file_idx },
                )
                .await?;

                let delta_resp = wire::read_control_message(&mut ctrl_recv).await?;
                match delta_resp {
                    ControlMessage::DeltaSignatures { file_index, signatures } => {
                        if file_index != file_idx {
                            continue;
                        }
                        // Compare source blocks against destination signatures
                        let (start_block, _) = manifest.block_range_for_file(file_idx)?;
                        let plan = crate::delta::compute_delta(
                            &manifest.files[file_idx].source_path,
                            &signatures,
                            manifest.block_size,
                            start_block,
                        )
                        .await?;

                        // Mark matching blocks as already received
                        for idx in &plan.matching_blocks {
                            if (*idx as usize) < bitfield.len() {
                                bitfield.set(*idx as usize, true);
                            }
                        }

                        if !plan.matching_blocks.is_empty() {
                            info!(
                                "Delta: file {} - {}/{} blocks match ({:.0}% savings)",
                                file_idx,
                                plan.matching_blocks.len(),
                                plan.total_blocks,
                                plan.skip_ratio() * 100.0
                            );
                        }
                    }
                    ControlMessage::DeltaNotAvailable { .. } => {
                        debug!("File {} not available at destination, full transfer", file_idx);
                    }
                    _ => {}
                }
            }
        }

        // 4. Build chunk plan for all files
        let already_received = bitfield.count_ones() as u64;
        if already_received > 0 {
            info!("Skipping {}/{} chunks (resume + delta)", already_received, total_chunks);
        }

        // We need a unified chunk plan that maps global indices to file+offset
        let chunk_plan = ChunkPlan::new(total_size, self.config.block_size);
        let mut scheduler = ChunkScheduler::new(chunk_plan, bitfield);

        if scheduler.is_complete() {
            let manifest_hash = manifest.manifest_hash();
            wire::write_control_message(
                &mut ctrl_send,
                &ControlMessage::TransferComplete { file_blake3: manifest_hash },
            )
            .await?;
            return Ok(());
        }

        // 5. Send chunks in parallel
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_streams));
        let mut sent_count = already_received;
        let mut rate_limiter = BandwidthLimiter::new(self.config.max_bandwidth_bps);
        let transfer_id_bytes =
            verify::hex_to_hash(&manifest.transfer_id).unwrap_or([0u8; 32]);

        while !scheduler.is_complete() {
            let batch = scheduler.next_batch(self.config.max_parallel_streams);
            if batch.is_empty() {
                break;
            }

            let mut handles = Vec::new();

            for chunk_idx in batch {
                rate_limiter.wait_if_needed(self.config.block_size as u64).await;
                let permit = semaphore.clone().acquire_owned().await?;

                // Resolve global chunk index to file + local offset
                let (file_idx, local_offset) = match manifest.file_and_offset_for_block(chunk_idx) {
                    Some(v) => v,
                    None => continue,
                };
                let source_path = manifest.files[file_idx].source_path.clone();
                let file_size = manifest.files[file_idx].size;
                let remaining = file_size - local_offset;
                let chunk_size = remaining.min(self.config.block_size as u64) as u32;

                let conn_inner = conn.inner().clone();
                let tid = transfer_id_bytes;

                let handle = tokio::spawn(async move {
                    let _permit = permit;

                    let data = ChunkReader::read_chunk(&source_path, local_offset, chunk_size).await?;
                    let chunk_hash = verify::hash_bytes(&data);

                    let (mut send, _recv) = conn_inner.open_bi().await.map_err(|e| {
                        EngineError::Protocol(format!("failed to open data stream: {e}"))
                    })?;

                    let header = ChunkHeader {
                        transfer_id: tid,
                        chunk_index: chunk_idx,
                        chunk_size,
                        chunk_blake3: chunk_hash,
                    };

                    wire::write_chunk_header(&mut send, &header).await?;
                    wire::write_chunk_data(&mut send, &data).await?;
                    send.finish().map_err(|e| {
                        EngineError::Protocol(format!("failed to finish stream: {e}"))
                    })?;

                    debug!("Sent chunk {} ({} bytes)", chunk_idx, chunk_size);
                    Ok::<u64, EngineError>(chunk_idx)
                });

                handles.push(handle);
            }

            for handle in handles {
                match handle.await? {
                    Ok(idx) => {
                        scheduler.mark_received(idx);
                        sent_count += 1;

                        if let Some(ref cb) = self.progress_cb {
                            if sent_count.is_multiple_of(self.config.progress_interval_chunks)
                                || scheduler.is_complete()
                            {
                                let elapsed = start.elapsed().as_secs_f64();
                                let transferred =
                                    (sent_count * self.config.block_size as u64).min(total_size);
                                cb(TransferProgress {
                                    transferred_bytes: transferred,
                                    total_bytes: total_size,
                                    transferred_chunks: sent_count,
                                    total_chunks,
                                    speed_bytes_per_sec: if elapsed > 0.0 {
                                        transferred as f64 / elapsed
                                    } else {
                                        0.0
                                    },
                                    elapsed_secs: elapsed,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to send chunk: {}", e);
                        return Err(e);
                    }
                }
            }
        }

        // 6. Wait for receiver confirmation
        let manifest_hash = manifest.manifest_hash();
        loop {
            let msg = wire::read_control_message(&mut ctrl_recv).await?;
            match msg {
                ControlMessage::ProgressAck { .. } => continue,
                ControlMessage::TransferComplete { file_blake3 } => {
                    if file_blake3 == manifest_hash {
                        info!("Transfer complete and verified by receiver");
                    } else {
                        return Err(EngineError::HashMismatch(
                            "manifest hash mismatch on receiver side".into(),
                        ));
                    }
                    break;
                }
                ControlMessage::Error { code, message } => {
                    return Err(EngineError::RemoteError { code, message });
                }
                _ => {}
            }
        }

        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::TransferComplete { file_blake3: manifest_hash },
        )
        .await?;

        let elapsed = start.elapsed();
        info!(
            "Transfer complete: {} files, {} bytes in {:.1}s ({:.1} MB/s)",
            manifest.files.len(),
            total_size,
            elapsed.as_secs_f64(),
            if elapsed.as_secs_f64() > 0.0 {
                (total_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
            } else {
                0.0
            }
        );

        Ok(())
    }

    /// Serve a pull request on an already-accepted connection.
    ///
    /// Unlike [`send_transfer`](Self::send_transfer), the control stream has
    /// already been accepted by the server and the first message
    /// (`PullRequest`) has already been consumed.  This method builds a
    /// manifest from `remote_path`, sends it over the provided control
    /// streams, then streams the file data exactly like a normal send.
    pub async fn serve_pull(
        &self,
        conn: &QuicConnection,
        ctrl_send: &mut quinn::SendStream,
        ctrl_recv: &mut quinn::RecvStream,
        remote_path: &Path,
        recursive: bool,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        // Build manifest from the requested path.
        // Use a dummy dest_dir — the client will place files wherever it likes.
        let dummy_dest = Path::new("/pull");

        let manifest = if remote_path.is_dir() {
            if !recursive {
                return Err(EngineError::Protocol(
                    "remote path is a directory but recursive was not requested".into(),
                ));
            }
            TransferManifest::from_directory(remote_path, dummy_dest, self.config.block_size)
                .await?
        } else if remote_path.is_file() {
            let file_hash = verify::hash_file(remote_path).await?;
            let file_size = tokio::fs::metadata(remote_path).await?.len();
            TransferManifest::new(
                vec![FileEntry {
                    source_path: remote_path.to_path_buf(),
                    dest_path: dummy_dest.join(
                        remote_path
                            .file_name()
                            .unwrap_or_else(|| std::ffi::OsStr::new("file")),
                    ),
                    size: file_size,
                    blake3_hash: file_hash,
                    relative_path: None,
                }],
                self.config.block_size,
            )?
        } else {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("remote path not found: {}", remote_path.display()),
            )));
        };

        let total_size = manifest.total_size();
        let total_chunks = manifest.total_blocks();

        info!(
            "Serving pull {}: {} files, {} bytes, {} blocks",
            &manifest.transfer_id[..12],
            manifest.files.len(),
            total_size,
            total_chunks
        );

        // Send manifest over the already-accepted control stream.
        let manifest_bytes = bincode::serialize(&manifest)?;
        wire::write_control_message(
            ctrl_send,
            &ControlMessage::Manifest { manifest_bytes },
        )
        .await?;

        // Wait for resume state from the pulling client.
        let resume_msg = wire::read_control_message(ctrl_recv).await?;
        let received_blocks: Vec<u64> = match resume_msg {
            ControlMessage::ResumeState { blocks_received } => blocks_received,
            ControlMessage::Error { code, message } => {
                return Err(EngineError::RemoteError { code, message });
            }
            _ => {
                return Err(EngineError::Protocol(
                    "expected ResumeState message from pull client".into(),
                ));
            }
        };

        let mut bitfield = bitvec![0; total_chunks as usize];
        for idx in &received_blocks {
            if (*idx as usize) < bitfield.len() {
                bitfield.set(*idx as usize, true);
            }
        }

        let already_received = received_blocks.len() as u64;
        if already_received > 0 {
            info!(
                "Pull resume: {}/{} chunks already received by client",
                already_received, total_chunks
            );
        }

        let chunk_plan = ChunkPlan::new(total_size, self.config.block_size);
        let mut scheduler = ChunkScheduler::new(chunk_plan, bitfield);

        if scheduler.is_complete() {
            let manifest_hash = manifest.manifest_hash();
            wire::write_control_message(
                ctrl_send,
                &ControlMessage::TransferComplete { file_blake3: manifest_hash },
            )
            .await?;
            return Ok(());
        }

        // Stream chunks in parallel — identical to send_transfer.
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_streams));
        let mut sent_count = already_received;
        let mut rate_limiter = BandwidthLimiter::new(self.config.max_bandwidth_bps);
        let transfer_id_bytes =
            verify::hex_to_hash(&manifest.transfer_id).unwrap_or([0u8; 32]);

        while !scheduler.is_complete() {
            let batch = scheduler.next_batch(self.config.max_parallel_streams);
            if batch.is_empty() {
                break;
            }

            let mut handles = Vec::new();

            for chunk_idx in batch {
                rate_limiter.wait_if_needed(self.config.block_size as u64).await;
                let permit = semaphore.clone().acquire_owned().await?;

                let (file_idx, local_offset) = match manifest.file_and_offset_for_block(chunk_idx) {
                    Some(v) => v,
                    None => continue,
                };
                let source_path = manifest.files[file_idx].source_path.clone();
                let file_size = manifest.files[file_idx].size;
                let remaining = file_size - local_offset;
                let chunk_size = remaining.min(self.config.block_size as u64) as u32;

                let conn_inner = conn.inner().clone();
                let tid = transfer_id_bytes;

                let handle = tokio::spawn(async move {
                    let _permit = permit;

                    let data = ChunkReader::read_chunk(&source_path, local_offset, chunk_size).await?;
                    let chunk_hash = verify::hash_bytes(&data);

                    let (mut send, _recv) = conn_inner.open_bi().await.map_err(|e| {
                        EngineError::Protocol(format!("failed to open data stream: {e}"))
                    })?;

                    let header = ChunkHeader {
                        transfer_id: tid,
                        chunk_index: chunk_idx,
                        chunk_size,
                        chunk_blake3: chunk_hash,
                    };

                    wire::write_chunk_header(&mut send, &header).await?;
                    wire::write_chunk_data(&mut send, &data).await?;
                    send.finish().map_err(|e| {
                        EngineError::Protocol(format!("failed to finish stream: {e}"))
                    })?;

                    debug!("Sent chunk {} ({} bytes)", chunk_idx, chunk_size);
                    Ok::<u64, EngineError>(chunk_idx)
                });

                handles.push(handle);
            }

            for handle in handles {
                match handle.await? {
                    Ok(idx) => {
                        scheduler.mark_received(idx);
                        sent_count += 1;

                        if let Some(ref cb) = self.progress_cb {
                            if sent_count.is_multiple_of(self.config.progress_interval_chunks)
                                || scheduler.is_complete()
                            {
                                let elapsed = start.elapsed().as_secs_f64();
                                let transferred =
                                    (sent_count * self.config.block_size as u64).min(total_size);
                                cb(TransferProgress {
                                    transferred_bytes: transferred,
                                    total_bytes: total_size,
                                    transferred_chunks: sent_count,
                                    total_chunks,
                                    speed_bytes_per_sec: if elapsed > 0.0 {
                                        transferred as f64 / elapsed
                                    } else {
                                        0.0
                                    },
                                    elapsed_secs: elapsed,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to send chunk during pull: {}", e);
                        return Err(e);
                    }
                }
            }
        }

        // Wait for client confirmation.
        let manifest_hash = manifest.manifest_hash();
        loop {
            let msg = wire::read_control_message(ctrl_recv).await?;
            match msg {
                ControlMessage::ProgressAck { .. } => continue,
                ControlMessage::TransferComplete { file_blake3 } => {
                    if file_blake3 == manifest_hash {
                        info!("Pull transfer complete and verified by client");
                    } else {
                        return Err(EngineError::HashMismatch(
                            "manifest hash mismatch on pull client side".into(),
                        ));
                    }
                    break;
                }
                ControlMessage::Error { code, message } => {
                    return Err(EngineError::RemoteError { code, message });
                }
                _ => {}
            }
        }

        wire::write_control_message(
            ctrl_send,
            &ControlMessage::TransferComplete { file_blake3: manifest_hash },
        )
        .await?;

        let elapsed = start.elapsed();
        info!(
            "Pull complete: {} files, {} bytes in {:.1}s ({:.1} MB/s)",
            manifest.files.len(),
            total_size,
            elapsed.as_secs_f64(),
            if elapsed.as_secs_f64() > 0.0 {
                (total_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
            } else {
                0.0
            }
        );

        Ok(())
    }

    /// Send an entire directory over the given QUIC connection.
    pub async fn send_directory(
        &self,
        conn: &QuicConnection,
        local_dir: &Path,
        remote_dest: &str,
    ) -> Result<()> {
        info!("Scanning directory: {}", local_dir.display());
        let manifest = TransferManifest::from_directory(
            local_dir,
            Path::new(remote_dest),
            self.config.block_size,
        )
        .await?;
        info!("Found {} files, {} total", manifest.files.len(), manifest.total_size());
        self.send_transfer(conn, &manifest).await
    }

    /// Send a directory with glob-based filtering over the given QUIC connection.
    pub async fn send_directory_filtered(
        &self,
        conn: &QuicConnection,
        local_dir: &Path,
        remote_dest: &str,
        filter: &crate::filter::FileFilter,
    ) -> Result<()> {
        info!("Scanning directory (filtered): {}", local_dir.display());
        let manifest = TransferManifest::from_directory_filtered(
            local_dir,
            Path::new(remote_dest),
            self.config.block_size,
            filter,
        )
        .await?;
        info!("Found {} files, {} total", manifest.files.len(), manifest.total_size());
        self.send_transfer(conn, &manifest).await
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

/// Receiver engine: accepts chunks over a QUIC connection and writes to disk.
pub struct Receiver {
    config: EngineConfig,
    state_manager: StateManager,
    progress_cb: Option<ProgressCallback>,
}

impl Receiver {
    /// Create a new receiver with the given configuration.
    pub fn new(config: EngineConfig) -> Result<Self> {
        let state_manager = StateManager::new(config.state_dir.clone())?;
        Ok(Self {
            config,
            state_manager,
            progress_cb: None,
        })
    }

    /// Register a progress callback.
    pub fn on_progress<F: Fn(TransferProgress) + Send + Sync + 'static>(&mut self, cb: F) {
        self.progress_cb = Some(Box::new(cb));
    }

    /// Register an already-boxed progress callback.
    pub fn set_progress_callback(&mut self, cb: ProgressCallback) {
        self.progress_cb = Some(cb);
    }

    /// Receive a file transfer over the given QUIC connection.
    pub async fn receive_file(&self, conn: &QuicConnection, dest_dir: &Path) -> Result<PathBuf> {
        let start = std::time::Instant::now();

        // 1. Accept control stream and read manifest
        let (mut ctrl_send, mut ctrl_recv) = conn.accept_control_stream().await?;

        let manifest_msg = wire::read_control_message(&mut ctrl_recv).await?;
        let manifest: TransferManifest = match manifest_msg {
            ControlMessage::Manifest { manifest_bytes } => bincode::deserialize(&manifest_bytes)?,
            _ => return Err(EngineError::Protocol("expected Manifest message".into())),
        };

        if manifest.files.is_empty() {
            return Err(EngineError::Protocol("empty manifest".into()));
        }

        let file_entry = &manifest.files[0];
        let dest_path = dest_dir.join(
            file_entry
                .dest_path
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("received_file")),
        );

        // Validate path safety
        validate_path(&dest_path, dest_dir)?;

        // Apply overwrite mode
        let dest_path = resolve_dest_path(&dest_path, self.config.overwrite_mode)?;

        // Check that the TLS handshake has completed before allowing file writes
        // (0-RTT safety). Manifest/resume-state exchange over the control stream
        // is fine during 0-RTT, but we must not write to disk until the handshake
        // is confirmed.
        if conn.inner().handshake_data().is_none() {
            return Err(EngineError::Protocol(
                "TLS handshake not yet available".into(),
            ));
        }
        debug!("Full TLS handshake confirmed, file writes permitted");

        info!(
            "Receiving: {} -> {} ({} bytes, {} blocks)",
            file_entry.source_path.display(),
            dest_path.display(),
            file_entry.size,
            manifest.total_blocks()
        );

        let total_chunks = manifest.total_blocks();
        let block_size = manifest.block_size;

        // 2. Check for resume state
        let mut state = match self.state_manager.load(&manifest.transfer_id)? {
            Some(existing) => {
                info!(
                    "Resuming transfer: {}/{} chunks",
                    existing.received_count(),
                    total_chunks
                );
                existing
            }
            None => {
                // Create the destination file (pre-allocate)
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&dest_path)
                    .await?;
                if file_entry.size > 0 {
                    file.set_len(file_entry.size).await?;
                }
                drop(file);

                TransferState::new(
                    manifest.transfer_id.clone(),
                    dest_path.to_string_lossy().into_owned(),
                    file_entry.size,
                    block_size,
                    total_chunks,
                    verify::hash_to_hex(&file_entry.blake3_hash),
                )
            }
        };

        // 3. Send resume state to sender
        let received_blocks = state.blocks_received.clone();
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::ResumeState {
                blocks_received: received_blocks,
            },
        )
        .await?;

        if state.received_count() < total_chunks {
            // 4. Receive chunks concurrently
            let chunk_plan = ChunkPlan::new(file_entry.size, block_size);
            let mut ack_batch = Vec::new();
            let mut set = JoinSet::new();

            loop {
                // If all chunks are received, drain remaining tasks and exit
                if state.received_count() >= total_chunks {
                    while let Some(result) = set.join_next().await {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);
                    }
                    break;
                }

                tokio::select! {
                    // Accept next data stream and spawn processing task
                    result = conn.accept_data_stream() => {
                        let (_send, mut recv) = result?;
                        let dp = dest_path.clone();
                        let cp = chunk_plan;

                        set.spawn(async move {
                            let header = wire::read_chunk_header(&mut recv).await?;
                            let data = wire::read_chunk_data(&mut recv, header.chunk_size).await?;

                            if !verify::verify_chunk(&data, &header.chunk_blake3) {
                                return Ok((header.chunk_index, false));
                            }

                            let offset = match cp.chunk_meta(header.chunk_index) {
                                Some(m) => m.offset,
                                None => return Ok((header.chunk_index, false)),
                            };

                            ChunkWriter::write_chunk(&dp, offset, &data).await?;
                            Ok::<(u64, bool), EngineError>((header.chunk_index, true))
                        });
                    }
                    // Collect completed task results
                    Some(result) = set.join_next() => {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);

                        // Periodically save state and send ACKs
                        if ack_batch.len() >= self.config.progress_interval_chunks as usize
                            || state.received_count() == total_chunks
                        {
                            self.flush_acks(
                                &mut ctrl_send,
                                &mut ack_batch,
                                &state,
                                file_entry,
                                total_chunks,
                                block_size,
                                start,
                            ).await?;
                        }
                    }
                }
            }

            // Flush any remaining ACKs
            if !ack_batch.is_empty() {
                self.flush_acks(
                    &mut ctrl_send,
                    &mut ack_batch,
                    &state,
                    file_entry,
                    total_chunks,
                    block_size,
                    start,
                )
                .await?;
            }
        }

        // 5. Verify complete file
        info!("All chunks received, verifying file integrity...");
        let file_hash = verify::hash_file(&dest_path).await?;

        if file_hash != file_entry.blake3_hash {
            error!("File verification failed!");
            wire::write_control_message(
                &mut ctrl_send,
                &ControlMessage::Error {
                    code: ErrorCode::VerificationFailed,
                    message: "File hash mismatch after assembly".to_string(),
                },
            )
            .await?;
            return Err(EngineError::HashMismatch(
                "file verification failed: hash mismatch".into(),
            ));
        }

        // 6. Send completion
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::TransferComplete {
                file_blake3: file_hash,
            },
        )
        .await?;

        // Wait for sender's completion confirmation
        match wire::read_control_message(&mut ctrl_recv).await {
            Ok(ControlMessage::TransferComplete { .. }) => {}
            Ok(_) => warn!("Expected TransferComplete from sender"),
            Err(e) => debug!("Control stream closed before sender confirmation: {}", e),
        }

        // 7. Clean up state file
        self.state_manager.delete(&manifest.transfer_id)?;

        let elapsed = start.elapsed();
        let speed_mbps = if elapsed.as_secs_f64() > 0.0 {
            (file_entry.size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
        } else {
            0.0
        };
        info!(
            "Receive complete: {} bytes in {:.1}s ({:.1} MB/s)",
            file_entry.size,
            elapsed.as_secs_f64(),
            speed_mbps
        );

        Ok(dest_path)
    }

    /// Process a completed chunk task result, updating state and ack batch.
    fn handle_chunk_result(
        &self,
        result: std::result::Result<
            std::result::Result<(u64, bool), EngineError>,
            tokio::task::JoinError,
        >,
        state: &mut TransferState,
        ack_batch: &mut Vec<u64>,
    ) {
        match result {
            Ok(Ok((chunk_idx, true))) => {
                state.mark_received(chunk_idx);
                ack_batch.push(chunk_idx);
                debug!(
                    "Received chunk {} ({}/{})",
                    chunk_idx,
                    state.received_count(),
                    state.total_blocks
                );
            }
            Ok(Ok((chunk_idx, false))) => {
                warn!("Chunk {} failed verification or invalid index", chunk_idx);
            }
            Ok(Err(e)) => {
                error!("Chunk processing error: {}", e);
            }
            Err(e) => {
                error!("Chunk task panicked: {}", e);
            }
        }
    }

    /// Save state, send ACKs over control stream, and report progress.
    #[allow(clippy::too_many_arguments)]
    async fn flush_acks(
        &self,
        ctrl_send: &mut quinn::SendStream,
        ack_batch: &mut Vec<u64>,
        state: &TransferState,
        file_entry: &FileEntry,
        total_chunks: u64,
        block_size: u32,
        start: std::time::Instant,
    ) -> Result<()> {
        self.state_manager.save(state)?;

        wire::write_control_message(
            ctrl_send,
            &ControlMessage::ProgressAck {
                blocks_confirmed: ack_batch.clone(),
            },
        )
        .await?;

        if let Some(ref cb) = self.progress_cb {
            let received = state.received_count();
            let transferred = (received * block_size as u64).min(file_entry.size);
            let elapsed = start.elapsed().as_secs_f64();
            cb(TransferProgress {
                transferred_bytes: transferred,
                total_bytes: file_entry.size,
                transferred_chunks: received,
                total_chunks,
                speed_bytes_per_sec: if elapsed > 0.0 {
                    transferred as f64 / elapsed
                } else {
                    0.0
                },
                elapsed_secs: elapsed,
            });
        }

        ack_batch.clear();
        Ok(())
    }

    /// Receive a multi-file transfer over the given QUIC connection.
    ///
    /// Returns the list of all received file paths.
    pub async fn receive_transfer(
        &self,
        conn: &QuicConnection,
        dest_dir: &Path,
    ) -> Result<Vec<PathBuf>> {
        let start = std::time::Instant::now();

        // 1. Accept control stream and read manifest
        let (mut ctrl_send, mut ctrl_recv) = conn.accept_control_stream().await?;

        let manifest_msg = wire::read_control_message(&mut ctrl_recv).await?;
        let manifest: TransferManifest = match manifest_msg {
            ControlMessage::Manifest { manifest_bytes } => bincode::deserialize(&manifest_bytes)?,
            _ => return Err(EngineError::Protocol("expected Manifest message".into())),
        };

        if manifest.files.is_empty() {
            return Err(EngineError::Protocol("empty manifest".into()));
        }

        // Wait for TLS handshake
        if conn.inner().handshake_data().is_none() {
            return Err(EngineError::Protocol("TLS handshake not yet available".into()));
        }

        let total_chunks = manifest.total_blocks();
        let total_size = manifest.total_size();
        let block_size = manifest.block_size;

        // 2. Resolve destination paths for all files
        let mut dest_paths = Vec::with_capacity(manifest.files.len());
        for file_entry in &manifest.files {
            let rel = file_entry
                .relative_path
                .as_deref()
                .or_else(|| file_entry.dest_path.file_name().map(Path::new))
                .unwrap_or(Path::new("received_file"));
            let dest_path = dest_dir.join(rel);
            validate_path(&dest_path, dest_dir)?;
            let dest_path = resolve_dest_path(&dest_path, self.config.overwrite_mode)?;
            dest_paths.push(dest_path);
        }

        info!(
            "Receiving: {} files, {} bytes, {} blocks",
            manifest.files.len(),
            total_size,
            total_chunks
        );

        // 3. Check for resume state
        let mut state = match self.state_manager.load(&manifest.transfer_id)? {
            Some(existing) => {
                info!(
                    "Resuming transfer: {}/{} chunks",
                    existing.received_count(),
                    total_chunks
                );
                existing
            }
            None => {
                // Create all destination directories and pre-allocate files
                for (i, dest_path) in dest_paths.iter().enumerate() {
                    if let Some(parent) = dest_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(dest_path)
                        .await?;
                    if manifest.files[i].size > 0 {
                        file.set_len(manifest.files[i].size).await?;
                    }
                }

                TransferState::new(
                    manifest.transfer_id.clone(),
                    dest_paths
                        .iter()
                        .map(|p| p.to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    total_size,
                    block_size,
                    total_chunks,
                    verify::hash_to_hex(&manifest.manifest_hash()),
                )
            }
        };

        // 4. Send resume state
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::ResumeState {
                blocks_received: state.blocks_received.clone(),
            },
        )
        .await?;

        // 5. Handle delta requests (if sender asks)
        // Use a short timeout: if no delta request arrives within 100ms,
        // the sender isn't using delta and we proceed to chunk receiving.
        loop {
            let msg = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                wire::read_control_message(&mut ctrl_recv),
            ).await;
            let msg = match msg {
                Ok(msg) => msg,
                Err(_) => break, // Timeout — no delta, proceed to chunks
            };
            match msg {
                Ok(ControlMessage::DeltaRequest { file_index }) => {
                    if file_index < dest_paths.len() && dest_paths[file_index].exists() {
                        let sigs = crate::delta::compute_signatures(
                            &dest_paths[file_index],
                            block_size,
                        )
                        .await
                        .unwrap_or_default();

                        // If delta, receiver also needs to copy matching blocks
                        // into the new file from the old version
                        wire::write_control_message(
                            &mut ctrl_send,
                            &ControlMessage::DeltaSignatures {
                                file_index,
                                signatures: sigs,
                            },
                        )
                        .await?;
                    } else {
                        wire::write_control_message(
                            &mut ctrl_send,
                            &ControlMessage::DeltaNotAvailable { file_index },
                        )
                        .await?;
                    }
                }
                Ok(other) => {
                    // Not a delta message — this must be something else (e.g., the sender
                    // started sending chunks). We need to handle it in the chunk loop.
                    // For now, if it's a TransferComplete (all matched), handle it.
                    if let ControlMessage::TransferComplete { file_blake3 } = other {
                        let manifest_hash = manifest.manifest_hash();
                        if file_blake3 == manifest_hash {
                            info!("All blocks matched via delta, nothing to transfer");
                            wire::write_control_message(
                                &mut ctrl_send,
                                &ControlMessage::TransferComplete { file_blake3: manifest_hash },
                            )
                            .await?;
                            self.state_manager.delete(&manifest.transfer_id)?;
                            return Ok(dest_paths);
                        }
                    }
                    break;
                }
                Err(_) => break,
            }
        }

        // 6. Receive chunks
        if state.received_count() < total_chunks {
            let mut ack_batch = Vec::new();
            let mut set = JoinSet::new();

            loop {
                if state.received_count() >= total_chunks {
                    while let Some(result) = set.join_next().await {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);
                    }
                    break;
                }

                tokio::select! {
                    result = conn.accept_data_stream() => {
                        let (_send, mut recv) = result?;
                        let manifest_ref = manifest.clone();
                        let dest_paths_ref = dest_paths.clone();
                        let bs = block_size;

                        set.spawn(async move {
                            let header = wire::read_chunk_header(&mut recv).await?;
                            let data = wire::read_chunk_data(&mut recv, header.chunk_size).await?;

                            if !verify::verify_chunk(&data, &header.chunk_blake3) {
                                return Ok((header.chunk_index, false));
                            }

                            // Resolve global index to file + offset
                            let (file_idx, local_offset) = match manifest_ref
                                .file_and_offset_for_block(header.chunk_index)
                            {
                                Some(v) => v,
                                None => return Ok((header.chunk_index, false)),
                            };

                            ChunkWriter::write_chunk(
                                &dest_paths_ref[file_idx],
                                local_offset,
                                &data,
                            )
                            .await?;

                            Ok::<(u64, bool), EngineError>((header.chunk_index, true))
                        });
                    }
                    Some(result) = set.join_next() => {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);

                        if ack_batch.len() >= self.config.progress_interval_chunks as usize
                            || state.received_count() == total_chunks
                        {
                            self.state_manager.save(&state)?;
                            wire::write_control_message(
                                &mut ctrl_send,
                                &ControlMessage::ProgressAck {
                                    blocks_confirmed: ack_batch.clone(),
                                },
                            )
                            .await?;

                            if let Some(ref cb) = self.progress_cb {
                                let received = state.received_count();
                                let transferred =
                                    (received * block_size as u64).min(total_size);
                                let elapsed = start.elapsed().as_secs_f64();
                                cb(TransferProgress {
                                    transferred_bytes: transferred,
                                    total_bytes: total_size,
                                    transferred_chunks: received,
                                    total_chunks,
                                    speed_bytes_per_sec: if elapsed > 0.0 {
                                        transferred as f64 / elapsed
                                    } else {
                                        0.0
                                    },
                                    elapsed_secs: elapsed,
                                });
                            }

                            ack_batch.clear();
                        }
                    }
                }
            }

            if !ack_batch.is_empty() {
                self.state_manager.save(&state)?;
                wire::write_control_message(
                    &mut ctrl_send,
                    &ControlMessage::ProgressAck {
                        blocks_confirmed: ack_batch,
                    },
                )
                .await?;
            }
        }

        // 7. Verify all files
        info!("All chunks received, verifying file integrity...");
        let mut all_hashes = Vec::new();
        for (i, dest_path) in dest_paths.iter().enumerate() {
            let hash = verify::hash_file(dest_path).await?;
            if hash != manifest.files[i].blake3_hash {
                return Err(EngineError::HashMismatch(format!(
                    "file {} hash mismatch",
                    dest_path.display()
                )));
            }
            all_hashes.extend_from_slice(&hash);
        }
        let manifest_hash = verify::hash_bytes(&all_hashes);

        // 8. Send completion
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::TransferComplete {
                file_blake3: manifest_hash,
            },
        )
        .await?;

        match wire::read_control_message(&mut ctrl_recv).await {
            Ok(ControlMessage::TransferComplete { .. }) => {}
            Ok(_) => warn!("Expected TransferComplete from sender"),
            Err(e) => debug!("Control stream closed before sender confirmation: {}", e),
        }

        self.state_manager.delete(&manifest.transfer_id)?;

        let elapsed = start.elapsed();
        info!(
            "Receive complete: {} files, {} bytes in {:.1}s ({:.1} MB/s)",
            manifest.files.len(),
            total_size,
            elapsed.as_secs_f64(),
            if elapsed.as_secs_f64() > 0.0 {
                (total_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
            } else {
                0.0
            }
        );

        Ok(dest_paths)
    }

    /// Receive files from the remote in response to a pull request.
    ///
    /// The caller (client) opens the control stream, sends a `PullRequest`,
    /// then the remote sends a `Manifest` followed by data chunks.  This
    /// method handles everything from the manifest onward — it mirrors
    /// [`receive_transfer`](Self::receive_transfer) but on a
    /// *client-opened* control stream.
    pub async fn pull_receive(
        &self,
        conn: &QuicConnection,
        remote_path: &str,
        recursive: bool,
        dest_dir: &Path,
    ) -> Result<Vec<PathBuf>> {
        let start = std::time::Instant::now();

        // 1. Open control stream and send the PullRequest.
        let (mut ctrl_send, mut ctrl_recv) = conn.open_control_stream().await?;

        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::PullRequest {
                remote_path: remote_path.to_string(),
                recursive,
            },
        )
        .await?;

        // 2. Read the manifest the remote built for us.
        let manifest_msg = wire::read_control_message(&mut ctrl_recv).await?;
        let manifest: TransferManifest = match manifest_msg {
            ControlMessage::Manifest { manifest_bytes } => bincode::deserialize(&manifest_bytes)?,
            ControlMessage::Error { code, message } => {
                return Err(EngineError::RemoteError { code, message });
            }
            _ => return Err(EngineError::Protocol("expected Manifest from remote".into())),
        };

        if manifest.files.is_empty() {
            return Err(EngineError::Protocol("empty manifest from remote".into()));
        }

        // Wait for TLS handshake
        if conn.inner().handshake_data().is_none() {
            return Err(EngineError::Protocol("TLS handshake not yet available".into()));
        }

        let total_chunks = manifest.total_blocks();
        let total_size = manifest.total_size();
        let block_size = manifest.block_size;

        // 3. Resolve destination paths for all files.
        let mut dest_paths = Vec::with_capacity(manifest.files.len());
        for file_entry in &manifest.files {
            let rel = file_entry
                .relative_path
                .as_deref()
                .or_else(|| file_entry.dest_path.file_name().map(Path::new))
                .unwrap_or(Path::new("received_file"));
            let dest_path = dest_dir.join(rel);
            validate_path(&dest_path, dest_dir)?;
            let dest_path = resolve_dest_path(&dest_path, self.config.overwrite_mode)?;
            dest_paths.push(dest_path);
        }

        info!(
            "Pull receiving: {} files, {} bytes, {} blocks -> {}",
            manifest.files.len(),
            total_size,
            total_chunks,
            dest_dir.display()
        );

        // 4. Resume state.
        let mut state = match self.state_manager.load(&manifest.transfer_id)? {
            Some(existing) => {
                info!(
                    "Resuming pull: {}/{} chunks",
                    existing.received_count(),
                    total_chunks
                );
                existing
            }
            None => {
                // Create directories and pre-allocate files.
                for (i, dest_path) in dest_paths.iter().enumerate() {
                    if let Some(parent) = dest_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let file = tokio::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(dest_path)
                        .await?;
                    if manifest.files[i].size > 0 {
                        file.set_len(manifest.files[i].size).await?;
                    }
                }

                TransferState::new(
                    manifest.transfer_id.clone(),
                    dest_paths
                        .iter()
                        .map(|p| p.to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    total_size,
                    block_size,
                    total_chunks,
                    verify::hash_to_hex(&manifest.manifest_hash()),
                )
            }
        };

        // 5. Send resume state back to the remote sender.
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::ResumeState {
                blocks_received: state.blocks_received.clone(),
            },
        )
        .await?;

        // 6. Receive chunks.
        if state.received_count() < total_chunks {
            let mut ack_batch = Vec::new();
            let mut set = JoinSet::new();

            loop {
                if state.received_count() >= total_chunks {
                    while let Some(result) = set.join_next().await {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);
                    }
                    break;
                }

                tokio::select! {
                    result = conn.accept_data_stream() => {
                        let (_send, mut recv) = result?;
                        let manifest_ref = manifest.clone();
                        let dest_paths_ref = dest_paths.clone();

                        set.spawn(async move {
                            let header = wire::read_chunk_header(&mut recv).await?;
                            let data = wire::read_chunk_data(&mut recv, header.chunk_size).await?;

                            if !verify::verify_chunk(&data, &header.chunk_blake3) {
                                return Ok((header.chunk_index, false));
                            }

                            let (file_idx, local_offset) = match manifest_ref
                                .file_and_offset_for_block(header.chunk_index)
                            {
                                Some(v) => v,
                                None => return Ok((header.chunk_index, false)),
                            };

                            ChunkWriter::write_chunk(
                                &dest_paths_ref[file_idx],
                                local_offset,
                                &data,
                            )
                            .await?;

                            Ok::<(u64, bool), EngineError>((header.chunk_index, true))
                        });
                    }
                    Some(result) = set.join_next() => {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);

                        if ack_batch.len() >= self.config.progress_interval_chunks as usize
                            || state.received_count() == total_chunks
                        {
                            self.state_manager.save(&state)?;
                            wire::write_control_message(
                                &mut ctrl_send,
                                &ControlMessage::ProgressAck {
                                    blocks_confirmed: ack_batch.clone(),
                                },
                            )
                            .await?;

                            if let Some(ref cb) = self.progress_cb {
                                let received = state.received_count();
                                let transferred =
                                    (received * block_size as u64).min(total_size);
                                let elapsed = start.elapsed().as_secs_f64();
                                cb(TransferProgress {
                                    transferred_bytes: transferred,
                                    total_bytes: total_size,
                                    transferred_chunks: received,
                                    total_chunks,
                                    speed_bytes_per_sec: if elapsed > 0.0 {
                                        transferred as f64 / elapsed
                                    } else {
                                        0.0
                                    },
                                    elapsed_secs: elapsed,
                                });
                            }

                            ack_batch.clear();
                        }
                    }
                }
            }

            if !ack_batch.is_empty() {
                self.state_manager.save(&state)?;
                wire::write_control_message(
                    &mut ctrl_send,
                    &ControlMessage::ProgressAck {
                        blocks_confirmed: ack_batch,
                    },
                )
                .await?;
            }
        }

        // 7. Verify all files.
        info!("All chunks received, verifying file integrity...");
        let mut all_hashes = Vec::new();
        for (i, dest_path) in dest_paths.iter().enumerate() {
            let hash = verify::hash_file(dest_path).await?;
            if hash != manifest.files[i].blake3_hash {
                return Err(EngineError::HashMismatch(format!(
                    "file {} hash mismatch",
                    dest_path.display()
                )));
            }
            all_hashes.extend_from_slice(&hash);
        }
        let manifest_hash = verify::hash_bytes(&all_hashes);

        // 8. Send completion.
        wire::write_control_message(
            &mut ctrl_send,
            &ControlMessage::TransferComplete {
                file_blake3: manifest_hash,
            },
        )
        .await?;

        match wire::read_control_message(&mut ctrl_recv).await {
            Ok(ControlMessage::TransferComplete { .. }) => {}
            Ok(_) => warn!("Expected TransferComplete from remote sender"),
            Err(e) => debug!("Control stream closed before sender confirmation: {}", e),
        }

        self.state_manager.delete(&manifest.transfer_id)?;

        let elapsed = start.elapsed();
        info!(
            "Pull receive complete: {} files, {} bytes in {:.1}s ({:.1} MB/s)",
            manifest.files.len(),
            total_size,
            elapsed.as_secs_f64(),
            if elapsed.as_secs_f64() > 0.0 {
                (total_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0)
            } else {
                0.0
            }
        );

        Ok(dest_paths)
    }
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Validate that a destination path is safe (no path traversal, within root).
fn validate_path(path: &Path, root: &Path) -> Result<()> {
    let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    // Check for null bytes
    let path_str = path.to_string_lossy();
    if path_str.contains('\0') {
        return Err(EngineError::Protocol("path contains null bytes".into()));
    }

    // Check for path traversal
    for component in path.components() {
        if let std::path::Component::ParentDir = component {
            return Err(EngineError::Protocol(
                "path traversal detected: contains '..'".into(),
            ));
        }
    }

    // Ensure path is within root (best effort before file creation)
    if let Ok(canonical_path) = path.canonicalize() {
        if !canonical_path.starts_with(&canonical_root) {
            return Err(EngineError::Protocol(format!(
                "path {} is outside root {}",
                canonical_path.display(),
                canonical_root.display()
            )));
        }
    }

    Ok(())
}

/// Resolve destination path according to overwrite mode.
///
/// - `Fail`: returns error if file exists
/// - `Rename`: appends .1, .2, etc. until a non-existing name is found
/// - `Overwrite`: returns the path unchanged (caller will truncate)
fn resolve_dest_path(path: &Path, mode: OverwriteMode) -> Result<PathBuf> {
    match mode {
        OverwriteMode::Overwrite => Ok(path.to_path_buf()),
        OverwriteMode::Fail => {
            if path.exists() {
                return Err(EngineError::Protocol(format!(
                    "destination file already exists: {} (use --overwrite to replace)",
                    path.display()
                )));
            }
            Ok(path.to_path_buf())
        }
        OverwriteMode::Rename => {
            if !path.exists() {
                return Ok(path.to_path_buf());
            }
            let stem = path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy();
            let ext = path
                .extension()
                .map(|e| format!(".{}", e.to_string_lossy()))
                .unwrap_or_default();
            let parent = path.parent().unwrap_or(Path::new("."));
            for i in 1..10_000 {
                let candidate = parent.join(format!("{stem}.{i}{ext}"));
                if !candidate.exists() {
                    info!("Destination exists, renaming to: {}", candidate.display());
                    return Ok(candidate);
                }
            }
            Err(EngineError::Protocol(format!(
                "could not find available rename for: {}",
                path.display()
            )))
        }
    }
}
