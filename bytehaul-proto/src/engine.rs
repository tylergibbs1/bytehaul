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

use crate::adaptive;
use crate::chunking::{ChunkPlan, ChunkReader, ChunkScheduler, ChunkWriter};
use crate::compress;
use crate::congestion::{BandwidthLimiter, ByteHaulCongestion, CongestionMode};
use crate::fec::{AdaptiveFecController, FecConfig, FecDecoder, FecEncoder};
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

    /// Bincode encode failed.
    #[error("bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

    /// Bincode decode failed.
    #[error("bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),

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

    /// A compression or decompression error.
    #[error("compression error: {0}")]
    Compression(#[from] crate::compress::CompressionError),
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
    /// Whether to compress chunk payloads with zstd before sending.
    pub compress: bool,
    /// Zstd compression level (1..=22).
    pub compress_level: i32,
    /// FEC group size (0 = disabled). Generates 1 parity chunk per N data chunks.
    pub fec_group_size: usize,
    /// Enable adaptive optimization (auto-detect loss, switch congestion, enable FEC).
    pub adaptive: bool,
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
            compress: false,
            compress_level: crate::compress::DEFAULT_COMPRESSION_LEVEL,
            fec_group_size: 0,
            adaptive: false,
        }
    }
}

fn finish_control_stream_shared(
    ctrl_send: &mut quinn::SendStream,
    context: &'static str,
) -> Result<()> {
    ctrl_send
        .finish()
        .map_err(|e| EngineError::Protocol(format!("{context}: {e}")))
}

async fn send_final_transfer_complete_shared(
    ctrl_send: &mut quinn::SendStream,
    file_blake3: [u8; 32],
    finish_context: &'static str,
) -> Result<()> {
    wire::write_control_message(
        ctrl_send,
        &ControlMessage::TransferComplete { file_blake3 },
    )
    .await?;
    finish_control_stream_shared(ctrl_send, finish_context)
}

async fn prepare_destination_file_shared(path: &Path, size: u64) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Preserve existing bytes until transfer negotiation completes. Delta
    // and resume both rely on the current destination contents remaining
    // available during setup.
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .await?;
    file.set_len(size).await?;
    Ok(())
}

#[derive(Debug)]
struct RuntimeAdaptiveState {
    cc: ByteHaulCongestion,
    fec: AdaptiveFecController,
    current_parallel: usize,
    current_fec_group: usize,
    last_sent_bytes: u64,
    last_lost_bytes: u64,
    last_sample_at: std::time::Instant,
    baseline_rtt: Option<std::time::Duration>,
    block_size: u64,
}

impl RuntimeAdaptiveState {
    fn new(config: &EngineConfig) -> Self {
        Self {
            cc: ByteHaulCongestion::new(CongestionMode::Aggressive),
            fec: AdaptiveFecController::new(),
            current_parallel: config.max_parallel_streams.max(8),
            current_fec_group: config.fec_group_size,
            last_sent_bytes: 0,
            last_lost_bytes: 0,
            last_sample_at: std::time::Instant::now(),
            baseline_rtt: None,
            block_size: config.block_size as u64,
        }
    }

    fn current_parallel(&self) -> usize {
        self.current_parallel
    }

    fn maybe_refresh(&mut self, conn: &QuicConnection, configured_parallel: usize) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_sample_at);
        if elapsed < std::time::Duration::from_millis(750) {
            return;
        }

        let stats = conn.inner().stats();
        let sent_bytes = stats.udp_tx.bytes;
        let lost_bytes = stats.path.lost_bytes;
        let delta_sent = sent_bytes.saturating_sub(self.last_sent_bytes);
        let delta_lost = lost_bytes.saturating_sub(self.last_lost_bytes);
        let rtt = stats.path.rtt;

        self.last_sent_bytes = sent_bytes;
        self.last_lost_bytes = lost_bytes;
        self.last_sample_at = now;

        if self.baseline_rtt.is_none() && rtt > std::time::Duration::ZERO {
            self.baseline_rtt = Some(rtt);
        }
        let baseline_rtt = self.baseline_rtt.unwrap_or(rtt);
        let rtt_inflation = if baseline_rtt > std::time::Duration::ZERO {
            (rtt.as_secs_f64() / baseline_rtt.as_secs_f64()).max(1.0)
        } else {
            1.0
        };

        let sent_packets = (delta_sent / 1200).max(1);
        let lost_packets = delta_lost / 1200;
        let burst_losses = if delta_lost == 0 {
            0
        } else {
            (delta_lost / self.block_size.max(1)).max(1)
        };
        let loss_rate = delta_lost as f64 / delta_sent.max(1) as f64;
        let loss_class = self.cc.classify_loss(loss_rate, burst_losses, rtt);

        let acked_bytes = delta_sent.saturating_sub(delta_lost);
        if acked_bytes > 0 {
            self.cc.on_ack(acked_bytes, rtt);
        }
        if delta_lost > 0 {
            self.cc.on_loss_event(delta_lost, loss_class);
        }

        let mut profile = adaptive::profile_from_quinn_stats(
            rtt,
            lost_packets,
            sent_packets,
            delta_sent,
            elapsed.max(std::time::Duration::from_millis(1)),
        );
        profile.burst_ratio = if lost_packets > 0 {
            (burst_losses as f64 / lost_packets.max(1) as f64).min(1.0)
        } else {
            0.0
        };
        profile.rtt_inflation = rtt_inflation;
        profile.loss_class = loss_class;

        let settings = adaptive::compute_adaptive_settings(&profile);
        let next_parallel = settings.parallel_streams.clamp(4, configured_parallel.max(4));
        let next_fec_group = self.fec.update(crate::fec::FecPathSignals {
            loss_rate: profile.loss_rate,
            burst_ratio: profile.burst_ratio,
            rtt_inflation: profile.rtt_inflation,
            retransmission_ratio: profile.loss_rate,
        });

        if next_parallel != self.current_parallel || next_fec_group != self.current_fec_group {
            info!(
                parallel = next_parallel,
                fec_group = next_fec_group,
                ?loss_class,
                loss_rate = format_args!("{:.3}", profile.loss_rate),
                rtt_ms = format_args!("{:.1}", rtt.as_secs_f64() * 1000.0),
                "adaptive runtime update"
            );
        }

        self.current_parallel = next_parallel;
        self.current_fec_group = next_fec_group;
    }

    fn current_fec_group(&self) -> usize {
        self.current_fec_group
    }
}

#[derive(Debug, Clone)]
struct PendingFecParity {
    chunk_indices: Vec<u64>,
    parity: Vec<u8>,
}

async fn read_manifest_chunk_data(
    manifest: &TransferManifest,
    chunk_idx: u64,
) -> Result<Vec<u8>> {
    let (file_idx, local_offset) = manifest
        .file_and_offset_for_block(chunk_idx)
        .ok_or_else(|| EngineError::Protocol(format!("invalid manifest chunk index {chunk_idx}")))?;
    let file_size = manifest.files[file_idx].size;
    let remaining = file_size.saturating_sub(local_offset);
    let chunk_size = remaining.min(manifest.block_size as u64) as u32;
    ChunkReader::read_chunk(
        &manifest.files[file_idx].source_path,
        local_offset,
        chunk_size,
    )
    .await
    .map_err(EngineError::from)
}

async fn build_manifest_fec_messages(
    manifest: &TransferManifest,
    batch: &[u64],
    group_size: usize,
) -> Result<Vec<ControlMessage>> {
    if group_size < 2 || batch.len() < 2 {
        return Ok(Vec::new());
    }

    let mut messages = Vec::new();
    for group in batch.chunks(group_size) {
        if group.len() < 2 {
            continue;
        }

        let mut encoder = FecEncoder::new(FecConfig::with_group_size(group.len()));
        let mut parity = None;
        for &chunk_idx in group {
            let data = read_manifest_chunk_data(manifest, chunk_idx).await?;
            if let Some(generated) = encoder.feed(&data) {
                parity = Some(generated);
            }
        }

        if parity.is_none() {
            parity = encoder.flush();
        }

        if let Some(parity) = parity {
            messages.push(ControlMessage::FecParity {
                chunk_indices: group.to_vec(),
                parity,
            });
        }
    }

    Ok(messages)
}

async fn recover_manifest_fec_chunks(
    manifest: &TransferManifest,
    dest_paths: &[PathBuf],
    state: &mut TransferState,
    parity_groups: &[PendingFecParity],
) -> Result<u64> {
    if parity_groups.is_empty() {
        return Ok(0);
    }

    let mut recovered = 0;
    let mut progress = true;

    while progress {
        progress = false;

        for parity_group in parity_groups {
            let missing: Vec<u64> = parity_group
                .chunk_indices
                .iter()
                .copied()
                .filter(|idx| !state.blocks_received.contains(idx))
                .collect();

            if missing.len() != 1 {
                continue;
            }

            let mut decoder = FecDecoder::new(parity_group.chunk_indices.len());
            for (position, &chunk_idx) in parity_group.chunk_indices.iter().enumerate() {
                if chunk_idx == missing[0] {
                    continue;
                }

                let (file_idx, local_offset) = manifest
                    .file_and_offset_for_block(chunk_idx)
                    .ok_or_else(|| {
                        EngineError::Protocol(format!(
                            "invalid manifest chunk index {chunk_idx} during FEC recovery"
                        ))
                    })?;
                let file_size = manifest.files[file_idx].size;
                let remaining = file_size.saturating_sub(local_offset);
                let chunk_size = remaining.min(manifest.block_size as u64) as u32;
                let data = ChunkReader::read_chunk(&dest_paths[file_idx], local_offset, chunk_size).await?;
                decoder.receive_chunk(position, data);
            }

            decoder.receive_parity(parity_group.parity.clone());
            let Some((missing_position, recovered_bytes)) = decoder.try_recover() else {
                continue;
            };
            let missing_idx = parity_group.chunk_indices[missing_position];
            let (file_idx, local_offset) = manifest
                .file_and_offset_for_block(missing_idx)
                .ok_or_else(|| {
                    EngineError::Protocol(format!(
                        "invalid recovered manifest chunk index {missing_idx}"
                    ))
                })?;

            ChunkWriter::write_chunk(&dest_paths[file_idx], local_offset, &recovered_bytes).await?;
            state.mark_received(missing_idx);
            recovered += 1;
            progress = true;

            info!(chunk_index = missing_idx, "recovered chunk from FEC parity");
        }
    }

    Ok(recovered)
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

    async fn wait_for_completion_confirmation(
        ctrl_recv: &mut quinn::RecvStream,
        mismatch_msg: &'static str,
    ) -> Result<()> {
        loop {
            let msg = wire::read_control_message(ctrl_recv).await?;
            match msg {
                ControlMessage::ProgressAck { .. } => continue,
                ControlMessage::TransferComplete { .. } => return Ok(()),
                ControlMessage::Error { code, message } => {
                    return Err(EngineError::RemoteError { code, message });
                }
                _ => return Err(EngineError::Protocol(mismatch_msg.into())),
            }
        }
    }

    fn finish_control_stream(
        ctrl_send: &mut quinn::SendStream,
        context: &'static str,
    ) -> Result<()> {
        ctrl_send
            .finish()
            .map_err(|e| EngineError::Protocol(format!("{context}: {e}")))
    }

    async fn send_final_transfer_complete(
        ctrl_send: &mut quinn::SendStream,
        file_blake3: [u8; 32],
        finish_context: &'static str,
    ) -> Result<()> {
        wire::write_control_message(
            ctrl_send,
            &ControlMessage::TransferComplete { file_blake3 },
        )
        .await?;
        Self::finish_control_stream(ctrl_send, finish_context)
    }

    /// Send a single file over the given QUIC connection.
    pub async fn send_file(
        &self,
        conn: &QuicConnection,
        local_path: &Path,
        remote_dest: &str,
    ) -> Result<()> {
        let file_size = tokio::fs::metadata(local_path).await?.len();
        let mut manifest = TransferManifest::new(
            vec![FileEntry {
                source_path: local_path.to_path_buf(),
                dest_path: PathBuf::from(remote_dest),
                size: file_size,
                blake3_hash: verify::hash_file(local_path).await?,
                relative_path: None,
            }],
            self.config.block_size,
        )?;
        manifest.compressed = self.config.compress;

        if self.config.delta_enabled || self.config.adaptive || self.config.fec_group_size > 0 {
            info!(
                "Using manifest transfer path for single-file send"
            );
            return self.send_transfer(conn, &manifest).await;
        }

        let start = std::time::Instant::now();

        info!(
            "Transfer {}: {} bytes, {} blocks{}",
            &manifest.transfer_id[..12],
            file_size,
            manifest.total_blocks(),
            if manifest.compressed { " (compressed)" } else { "" },
        );

        // 2. Open control stream and send manifest
        let (mut ctrl_send, mut ctrl_recv) = conn.open_control_stream().await?;

        let manifest_bytes = bincode::serde::encode_to_vec(&manifest, bincode::config::standard())?;
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
            Self::send_final_transfer_complete(
                &mut ctrl_send,
                manifest.manifest_hash(),
                "failed to finish sender control stream",
            )
            .await?;
            // Wait for receiver's confirmation before returning, so callers
            // that tear down the daemon after send exits don't race.
            Self::wait_for_completion_confirmation(
                &mut ctrl_recv,
                "expected TransferComplete from receiver",
            )
            .await?;
            return Ok(());
        }

        // 4. Send chunks in parallel batches
        let mut adaptive_state = RuntimeAdaptiveState::new(&self.config);
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_streams.max(64)));
        let mut sent_count = already_received;
        let mut rate_limiter = BandwidthLimiter::new(self.config.max_bandwidth_bps);

        let local_path = local_path.to_path_buf();
        let transfer_id_bytes =
            verify::hex_to_hash(&manifest.transfer_id).unwrap_or([0u8; 32]);

        while !scheduler.is_complete() {
            adaptive_state.maybe_refresh(conn, self.config.max_parallel_streams);
            let batch_limit = adaptive_state.current_parallel();
            let batch = scheduler.next_batch(batch_limit);
            if batch.is_empty() {
                break;
            }

            let fec_messages = build_manifest_fec_messages(
                &manifest,
                &batch,
                adaptive_state.current_fec_group(),
            )
            .await?;

            let mut handles = Vec::new();

            for chunk_idx in batch {
                // Apply bandwidth limiting before sending each chunk
                rate_limiter.wait_if_needed(self.config.block_size as u64).await;

                let permit = semaphore.clone().acquire_owned().await?;
                let chunk_meta = chunk_plan.chunk_meta(chunk_idx);
                let local_path = local_path.clone();
                let conn_inner = conn.inner().clone();
                let tid = transfer_id_bytes;
                let do_compress = self.config.compress;
                let compress_level = self.config.compress_level;

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

                    // Compute chunk hash (always over uncompressed data)
                    let chunk_hash = verify::hash_bytes(&data);

                    // Optionally compress the chunk payload
                    let wire_data = if do_compress {
                        compress::compress_chunk(&data, compress_level)?
                    } else {
                        data
                    };

                    // Open a data stream and send
                    let (mut send, _recv) = conn_inner.open_bi().await.map_err(|e| {
                        EngineError::Protocol(format!("failed to open data stream: {e}"))
                    })?;

                    let header = ChunkHeader {
                        transfer_id: tid,
                        chunk_index: chunk_idx,
                        chunk_size: wire_data.len() as u32,
                        chunk_blake3: chunk_hash,
                    };

                    wire::write_chunk_header(&mut send, &header).await?;
                    wire::write_chunk_data(&mut send, &wire_data).await?;
                    send.finish().map_err(|e| {
                        EngineError::Protocol(format!("failed to finish stream: {e}"))
                    })?;

                    debug!("Sent chunk {} ({} bytes)", chunk_idx, wire_data.len());
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

            for msg in fec_messages {
                wire::write_control_message(&mut ctrl_send, &msg).await?;
            }
        }

        if adaptive_state.current_fec_group() > 0 {
            info!(
                fec_group = adaptive_state.current_fec_group(),
                "adaptive fec selected a repair group size for this transfer"
            );
        }

        // 5. Wait for receiver to confirm
        debug!("All chunks sent, waiting for receiver confirmation");

        loop {
            let msg = wire::read_control_message(&mut ctrl_recv).await?;
            match msg {
                ControlMessage::ProgressAck { .. } => {
                    continue;
                }
                ControlMessage::TransferComplete { .. } => {
                    info!("Transfer complete and verified by receiver");
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

        // Apply compression setting from engine config to the manifest.
        let mut manifest = manifest.clone();
        manifest.compressed = self.config.compress;

        info!(
            "Transfer {}: {} files, {} bytes, {} blocks{}",
            &manifest.transfer_id[..12],
            manifest.files.len(),
            total_size,
            total_chunks,
            if manifest.compressed { " (compressed)" } else { "" },
        );

        // 1. Open control stream and send manifest
        let (mut ctrl_send, mut ctrl_recv) = conn.open_control_stream().await?;
        let manifest_bytes = bincode::serde::encode_to_vec(&manifest, bincode::config::standard())?;
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

        // Create a chunk plan sized to the manifest's total block count.
        // For multi-file transfers, total_blocks() counts blocks per-file,
        // so we cannot use ChunkPlan::new(total_size, block_size) which
        // would give a different count. Instead, construct a plan whose
        // total_chunks matches the manifest.
        let plan_size = total_chunks * self.config.block_size as u64;
        let chunk_plan = ChunkPlan::new(plan_size, self.config.block_size);
        let mut scheduler = ChunkScheduler::new(chunk_plan, bitfield);

        if scheduler.is_complete() {
            let manifest_hash = manifest.manifest_hash();
            Self::send_final_transfer_complete(
                &mut ctrl_send,
                manifest_hash,
                "failed to finish sender control stream",
            )
            .await?;
            // Wait for receiver confirmation before returning.
            Self::wait_for_completion_confirmation(
                &mut ctrl_recv,
                "expected TransferComplete from receiver",
            )
            .await?;
            return Ok(());
        }

        // 5. Send chunks in parallel
        let mut adaptive_state = RuntimeAdaptiveState::new(&self.config);
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_streams.max(64)));
        let mut sent_count = already_received;
        let mut rate_limiter = BandwidthLimiter::new(self.config.max_bandwidth_bps);
        let transfer_id_bytes =
            verify::hex_to_hash(&manifest.transfer_id).unwrap_or([0u8; 32]);

        while !scheduler.is_complete() {
            adaptive_state.maybe_refresh(conn, self.config.max_parallel_streams);
            let batch_limit = adaptive_state.current_parallel();
            let batch = scheduler.next_batch(batch_limit);
            if batch.is_empty() {
                break;
            }

            let fec_messages = build_manifest_fec_messages(
                &manifest,
                &batch,
                adaptive_state.current_fec_group(),
            )
            .await?;

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
                let do_compress = manifest.compressed;
                let compress_level = self.config.compress_level;

                let handle = tokio::spawn(async move {
                    let _permit = permit;

                    let data = ChunkReader::read_chunk(&source_path, local_offset, chunk_size).await?;
                    let chunk_hash = verify::hash_bytes(&data);

                    let wire_data = if do_compress {
                        compress::compress_chunk(&data, compress_level)?
                    } else {
                        data
                    };

                    let (mut send, _recv) = conn_inner.open_bi().await.map_err(|e| {
                        EngineError::Protocol(format!("failed to open data stream: {e}"))
                    })?;

                    let header = ChunkHeader {
                        transfer_id: tid,
                        chunk_index: chunk_idx,
                        chunk_size: wire_data.len() as u32,
                        chunk_blake3: chunk_hash,
                    };

                    wire::write_chunk_header(&mut send, &header).await?;
                    wire::write_chunk_data(&mut send, &wire_data).await?;
                    send.finish().map_err(|e| {
                        EngineError::Protocol(format!("failed to finish stream: {e}"))
                    })?;

                    debug!("Sent chunk {} ({} bytes)", chunk_idx, wire_data.len());
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

            for msg in fec_messages {
                wire::write_control_message(&mut ctrl_send, &msg).await?;
            }
        }

        if adaptive_state.current_fec_group() > 0 {
            info!(
                fec_group = adaptive_state.current_fec_group(),
                "adaptive fec selected a repair group size for this transfer"
            );
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

        let mut manifest = if remote_path.is_dir() {
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

        manifest.compressed = self.config.compress;

        let total_size = manifest.total_size();
        let total_chunks = manifest.total_blocks();

        info!(
            "Serving pull {}: {} files, {} bytes, {} blocks{}",
            &manifest.transfer_id[..12],
            manifest.files.len(),
            total_size,
            total_chunks,
            if manifest.compressed { " (compressed)" } else { "" },
        );

        // Send manifest over the already-accepted control stream.
        let manifest_bytes = bincode::serde::encode_to_vec(&manifest, bincode::config::standard())?;
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

        let plan_size = total_chunks * self.config.block_size as u64;
        let chunk_plan = ChunkPlan::new(plan_size, self.config.block_size);
        let mut scheduler = ChunkScheduler::new(chunk_plan, bitfield);

        if scheduler.is_complete() {
            let manifest_hash = manifest.manifest_hash();
            Self::send_final_transfer_complete(
                ctrl_send,
                manifest_hash,
                "failed to finish pull control stream",
            )
            .await?;
            Self::wait_for_completion_confirmation(
                ctrl_recv,
                "expected TransferComplete from pull client",
            )
            .await?;
            return Ok(());
        }

        // Stream chunks in parallel — identical to send_transfer.
        let mut adaptive_state = RuntimeAdaptiveState::new(&self.config);
        let semaphore = Arc::new(Semaphore::new(self.config.max_parallel_streams.max(64)));
        let mut sent_count = already_received;
        let mut rate_limiter = BandwidthLimiter::new(self.config.max_bandwidth_bps);
        let transfer_id_bytes =
            verify::hex_to_hash(&manifest.transfer_id).unwrap_or([0u8; 32]);

        while !scheduler.is_complete() {
            adaptive_state.maybe_refresh(conn, self.config.max_parallel_streams);
            let batch_limit = adaptive_state.current_parallel();
            let batch = scheduler.next_batch(batch_limit);
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
                let do_compress = manifest.compressed;
                let compress_level = self.config.compress_level;

                let handle = tokio::spawn(async move {
                    let _permit = permit;

                    let data = ChunkReader::read_chunk(&source_path, local_offset, chunk_size).await?;
                    let chunk_hash = verify::hash_bytes(&data);

                    let wire_data = if do_compress {
                        compress::compress_chunk(&data, compress_level)?
                    } else {
                        data
                    };

                    let (mut send, _recv) = conn_inner.open_bi().await.map_err(|e| {
                        EngineError::Protocol(format!("failed to open data stream: {e}"))
                    })?;

                    let header = ChunkHeader {
                        transfer_id: tid,
                        chunk_index: chunk_idx,
                        chunk_size: wire_data.len() as u32,
                        chunk_blake3: chunk_hash,
                    };

                    wire::write_chunk_header(&mut send, &header).await?;
                    wire::write_chunk_data(&mut send, &wire_data).await?;
                    send.finish().map_err(|e| {
                        EngineError::Protocol(format!("failed to finish stream: {e}"))
                    })?;

                    debug!("Sent chunk {} ({} bytes)", chunk_idx, wire_data.len());
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

        if adaptive_state.current_fec_group() > 0 {
            info!(
                fec_group = adaptive_state.current_fec_group(),
                "adaptive fec selected a repair group size for this transfer"
            );
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
            ControlMessage::Manifest { manifest_bytes } => bincode::serde::decode_from_slice(&manifest_bytes, bincode::config::standard())?.0,
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
                prepare_destination_file_shared(&dest_path, file_entry.size).await?;

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
                        let is_compressed = manifest.compressed;
                        let max_decomp_size = block_size as usize;

                        set.spawn(async move {
                            let header = wire::read_chunk_header(&mut recv).await?;
                            let wire_data = wire::read_chunk_data(&mut recv, header.chunk_size).await?;

                            let data = if is_compressed {
                                compress::decompress_chunk(&wire_data, max_decomp_size)?
                            } else {
                                wire_data
                            };

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

        // 6. Send completion (use manifest_hash for consistency with send_file)
        send_final_transfer_complete_shared(
            &mut ctrl_send,
            manifest.manifest_hash(),
            "failed to finish receiver control stream",
        )
        .await?;

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
        // Accept control stream and read manifest, then delegate.
        let (ctrl_send, mut ctrl_recv) = conn.accept_control_stream().await?;

        let manifest_msg = wire::read_control_message(&mut ctrl_recv).await?;
        let manifest_bytes = match manifest_msg {
            ControlMessage::Manifest { manifest_bytes } => manifest_bytes,
            _ => return Err(EngineError::Protocol("expected Manifest message".into())),
        };

        self.receive_transfer_preread(conn, dest_dir, manifest_bytes, ctrl_send, ctrl_recv)
            .await
    }

    /// Receive a multi-file transfer using a control stream and manifest
    /// bytes that have already been read by the caller.
    ///
    /// This enables the server to peek at the first control message (to
    /// distinguish send vs pull) and then hand off to this method for the
    /// normal receive path.
    pub async fn receive_transfer_preread(
        &self,
        conn: &QuicConnection,
        dest_dir: &Path,
        manifest_bytes: Vec<u8>,
        mut ctrl_send: quinn::SendStream,
        mut ctrl_recv: quinn::RecvStream,
    ) -> Result<Vec<PathBuf>> {
        let start = std::time::Instant::now();

        let manifest: TransferManifest = bincode::serde::decode_from_slice(&manifest_bytes, bincode::config::standard())?.0;

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
                for (i, dest_path) in dest_paths.iter().enumerate() {
                    prepare_destination_file_shared(dest_path, manifest.files[i].size).await?;
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
        // Use a short timeout: if no delta request arrives within 10ms,
        // the sender isn't using delta and we proceed to chunk receiving.
        // 10ms is ample on all links (any delta request is sent immediately
        // after resume state is read) while saving ~90ms of idle time.
        let mut parity_groups = Vec::new();
        loop {
            let msg = tokio::time::timeout(
                std::time::Duration::from_millis(10),
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
                    match other {
                        ControlMessage::TransferComplete { file_blake3 } => {
                            let manifest_hash = manifest.manifest_hash();
                            if file_blake3 == manifest_hash {
                                info!("All blocks matched via delta, nothing to transfer");
                                send_final_transfer_complete_shared(
                                    &mut ctrl_send,
                                    manifest_hash,
                                    "failed to finish receiver control stream",
                                )
                                .await?;
                                self.state_manager.delete(&manifest.transfer_id)?;
                                return Ok(dest_paths);
                            }
                        }
                        ControlMessage::FecParity { chunk_indices, parity } => {
                            parity_groups.push(PendingFecParity {
                                chunk_indices,
                                parity,
                            });
                            continue;
                        }
                        _ => {}
                    }
                    break;
                }
                Err(_) => break,
            }
        }

        // 6. Receive chunks
        let mut sender_reported_complete = false;
        if state.received_count() < total_chunks {
            let mut ack_batch = Vec::new();
            let mut set = JoinSet::new();
            let expected_manifest_hash = manifest.manifest_hash();
            let manifest_arc = Arc::new(manifest.clone());
            let dest_paths_arc = Arc::new(dest_paths.clone());

            loop {
                if state.received_count() >= total_chunks || sender_reported_complete {
                    while let Some(result) = set.join_next().await {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);
                    }
                    break;
                }

                tokio::select! {
                    control_msg = wire::read_control_message(&mut ctrl_recv) => {
                        match control_msg? {
                            ControlMessage::TransferComplete { file_blake3 } => {
                                if file_blake3 != expected_manifest_hash {
                                    return Err(EngineError::HashMismatch(
                                        "manifest hash mismatch on sender side".into(),
                                    ));
                                }

                                if !set.is_empty() {
                                    return Err(EngineError::Protocol(
                                        "sender reported completion while chunks were still in flight".into(),
                                    ));
                                }

                                sender_reported_complete = true;
                            }
                            ControlMessage::FecParity { chunk_indices, parity } => {
                                parity_groups.push(PendingFecParity {
                                    chunk_indices,
                                    parity,
                                });
                            }
                            ControlMessage::Error { code, message } => {
                                return Err(EngineError::RemoteError { code, message });
                            }
                            ControlMessage::Cancel => {
                                return Err(EngineError::Protocol(
                                    "sender canceled transfer while receiving chunks".into(),
                                ));
                            }
                            ControlMessage::ProgressAck { .. }
                            | ControlMessage::DeltaRequest { .. }
                            | ControlMessage::DeltaSignatures { .. }
                            | ControlMessage::DeltaNotAvailable { .. }
                            | ControlMessage::ResumeState { .. }
                            | ControlMessage::Manifest { .. }
                            | ControlMessage::PullRequest { .. } => {
                                return Err(EngineError::Protocol(
                                    "unexpected control message while receiving chunks".into(),
                                ));
                            }
                        }
                    }
                    result = conn.accept_data_stream() => {
                        let (_send, mut recv) = result?;
                        let manifest_ref = manifest_arc.clone();
                        let dest_paths_ref = dest_paths_arc.clone();
                        let bs = block_size;
                        let is_compressed = manifest_ref.compressed;

                        set.spawn(async move {
                            let header = wire::read_chunk_header(&mut recv).await?;
                            let wire_data = wire::read_chunk_data(&mut recv, header.chunk_size).await?;

                            let data = if is_compressed {
                                compress::decompress_chunk(&wire_data, bs as usize)?
                            } else {
                                wire_data
                            };

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

        if state.received_count() < total_chunks {
            let recovered = recover_manifest_fec_chunks(
                &manifest,
                &dest_paths,
                &mut state,
                &parity_groups,
            )
            .await?;
            if recovered > 0 {
                info!(recovered_chunks = recovered, "recovered missing chunks via FEC");
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
        send_final_transfer_complete_shared(
            &mut ctrl_send,
            manifest_hash,
            "failed to finish receiver control stream",
        )
        .await?;

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
            ControlMessage::Manifest { manifest_bytes } => bincode::serde::decode_from_slice(&manifest_bytes, bincode::config::standard())?.0,
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
                for (i, dest_path) in dest_paths.iter().enumerate() {
                    prepare_destination_file_shared(dest_path, manifest.files[i].size).await?;
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
        let mut sender_reported_complete = false;
        let mut parity_groups = Vec::new();
        if state.received_count() < total_chunks {
            let mut ack_batch = Vec::new();
            let mut set = JoinSet::new();
            let expected_manifest_hash = manifest.manifest_hash();

            loop {
                if state.received_count() >= total_chunks || sender_reported_complete {
                    while let Some(result) = set.join_next().await {
                        self.handle_chunk_result(result, &mut state, &mut ack_batch);
                    }
                    break;
                }

                tokio::select! {
                    control_msg = wire::read_control_message(&mut ctrl_recv) => {
                        match control_msg? {
                            ControlMessage::TransferComplete { file_blake3 } => {
                                if file_blake3 != expected_manifest_hash {
                                    return Err(EngineError::HashMismatch(
                                        "manifest hash mismatch on pull sender side".into(),
                                    ));
                                }
                                if !set.is_empty() {
                                    return Err(EngineError::Protocol(
                                        "pull sender reported completion while chunks were still in flight".into(),
                                    ));
                                }
                                sender_reported_complete = true;
                            }
                            ControlMessage::FecParity { chunk_indices, parity } => {
                                parity_groups.push(PendingFecParity {
                                    chunk_indices,
                                    parity,
                                });
                            }
                            ControlMessage::Error { code, message } => {
                                return Err(EngineError::RemoteError { code, message });
                            }
                            ControlMessage::Cancel => {
                                return Err(EngineError::Protocol(
                                    "remote sender canceled pull while receiving chunks".into(),
                                ));
                            }
                            ControlMessage::ProgressAck { .. }
                            | ControlMessage::DeltaRequest { .. }
                            | ControlMessage::DeltaSignatures { .. }
                            | ControlMessage::DeltaNotAvailable { .. }
                            | ControlMessage::ResumeState { .. }
                            | ControlMessage::Manifest { .. }
                            | ControlMessage::PullRequest { .. } => {
                                return Err(EngineError::Protocol(
                                    "unexpected control message while pull receiving chunks".into(),
                                ));
                            }
                        }
                    }
                    result = conn.accept_data_stream() => {
                        let (_send, mut recv) = result?;
                        let manifest_ref = manifest.clone();
                        let dest_paths_ref = dest_paths.clone();
                        let is_compressed = manifest.compressed;
                        let max_decomp_size = block_size as usize;

                        set.spawn(async move {
                            let header = wire::read_chunk_header(&mut recv).await?;
                            let wire_data = wire::read_chunk_data(&mut recv, header.chunk_size).await?;

                            let data = if is_compressed {
                                compress::decompress_chunk(&wire_data, max_decomp_size)?
                            } else {
                                wire_data
                            };

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

        if state.received_count() < total_chunks {
            let recovered = recover_manifest_fec_chunks(
                &manifest,
                &dest_paths,
                &mut state,
                &parity_groups,
            )
            .await?;
            if recovered > 0 {
                info!(recovered_chunks = recovered, "recovered missing pull chunks via FEC");
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
        send_final_transfer_complete_shared(
            &mut ctrl_send,
            manifest_hash,
            "failed to finish receiver control stream",
        )
        .await?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn manifest_fec_recovery_restores_one_missing_chunk() {
        let tmp = tempfile::tempdir().unwrap();
        let src_path = tmp.path().join("src.bin");
        let dest_path = tmp.path().join("dest.bin");
        let block_size = 256 * 1024u32;

        let mut source_data = vec![0u8; block_size as usize * 3];
        for (idx, byte) in source_data.iter_mut().enumerate() {
            *byte = (idx % 251) as u8;
        }
        tokio::fs::write(&src_path, &source_data).await.unwrap();

        let file_hash = verify::hash_file(&src_path).await.unwrap();
        let manifest = TransferManifest::new(
            vec![FileEntry {
                source_path: src_path.clone(),
                dest_path: PathBuf::from("/dest.bin"),
                size: source_data.len() as u64,
                blake3_hash: file_hash,
                relative_path: None,
            }],
            block_size,
        )
        .unwrap();

        let mut state = TransferState::new(
            manifest.transfer_id.clone(),
            dest_path.to_string_lossy().into_owned(),
            source_data.len() as u64,
            block_size,
            manifest.total_blocks(),
            verify::hash_to_hex(&manifest.manifest_hash()),
        );

        let chunk0 = &source_data[..block_size as usize];
        let chunk2 = &source_data[(block_size as usize * 2)..];
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&dest_path)
            .await
            .unwrap();
        file.set_len(source_data.len() as u64).await.unwrap();
        ChunkWriter::write_chunk(&dest_path, 0, chunk0).await.unwrap();
        ChunkWriter::write_chunk(&dest_path, block_size as u64 * 2, chunk2)
            .await
            .unwrap();
        state.mark_received(0);
        state.mark_received(2);

        let fec_messages = build_manifest_fec_messages(&manifest, &[0, 1, 2], 3)
            .await
            .unwrap();
        let parity_groups: Vec<PendingFecParity> = fec_messages
            .into_iter()
            .filter_map(|msg| match msg {
                ControlMessage::FecParity { chunk_indices, parity } => {
                    Some(PendingFecParity {
                        chunk_indices,
                        parity,
                    })
                }
                _ => None,
            })
            .collect();

        let recovered = recover_manifest_fec_chunks(
            &manifest,
            std::slice::from_ref(&dest_path),
            &mut state,
            &parity_groups,
        )
        .await
        .unwrap();

        assert_eq!(recovered, 1);
        assert_eq!(state.received_count(), 3);
        assert_eq!(verify::hash_file(&dest_path).await.unwrap(), file_hash);
    }
}
