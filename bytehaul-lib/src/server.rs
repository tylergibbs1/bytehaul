use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

use bytehaul_proto::engine::{EngineConfig, Receiver, Sender, TransferProgress};
use bytehaul_proto::transport::{QuicServer, TransportConfig};
use bytehaul_proto::wire::{self, ControlMessage, ErrorCode};

use crate::config::TransferConfig;

/// ByteHaul server (receiver) that listens for incoming transfers.
pub struct Server {
    quic_server: QuicServer,
    dest_dir: PathBuf,
    config: TransferConfig,
}

impl Server {
    /// Bind a new server to the given address.
    pub fn bind(addr: SocketAddr, dest_dir: PathBuf) -> Result<Self> {
        let transport_config = TransportConfig {
            bind_addr: addr,
            ..Default::default()
        };
        let quic_server =
            QuicServer::bind(transport_config).context("Failed to bind QUIC server")?;
        info!("Server listening on {}", quic_server.local_addr());

        Ok(Self {
            quic_server,
            dest_dir,
            config: TransferConfig::default(),
        })
    }

    /// Get the local address this server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.quic_server.local_addr()
    }

    /// Apply a transfer configuration.
    pub fn with_config(mut self, config: TransferConfig) -> Self {
        self.config = config;
        self
    }

    /// Accept and handle a single incoming transfer.
    ///
    /// Reads the first control message to decide whether this is a normal
    /// send (Manifest) or a pull request (PullRequest).
    ///  - **Manifest**: the server acts as Receiver (existing behaviour).
    ///  - **PullRequest**: the server builds a manifest from the requested
    ///    path and acts as Sender on the same connection.
    pub async fn accept_transfer(
        &self,
        progress_cb: Option<Box<dyn Fn(TransferProgress) + Send + Sync>>,
    ) -> Result<Vec<PathBuf>> {
        let conn = self
            .quic_server
            .accept()
            .await
            .context("Failed to accept connection")?;
        info!("Accepted connection from {}", conn.remote_addr());

        let engine_config = EngineConfig {
            max_parallel_streams: self.config.max_parallel_streams,
            block_size: self.config.block_size,
            resume_enabled: self.config.resume,
            state_dir: self.config.state_dir.clone(),
            overwrite_mode: self.config.overwrite_mode,
            delta_enabled: self.config.delta,
            encrypt_state: self.config.encrypt_state,
            compress: self.config.compress,
            compress_level: self.config.compress_level,
            ..Default::default()
        };

        // Accept the control stream and peek at the first message.
        let (mut ctrl_send, mut ctrl_recv) = conn
            .accept_control_stream()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let first_msg = wire::read_control_message(&mut ctrl_recv)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        match first_msg {
            ControlMessage::PullRequest { remote_path, recursive } => {
                info!(
                    "Pull request from {}: {} (recursive={})",
                    conn.remote_addr(),
                    remote_path,
                    recursive
                );

                let path = Path::new(&remote_path);
                if !path.exists() {
                    wire::write_control_message(
                        &mut ctrl_send,
                        &ControlMessage::Error {
                            code: ErrorCode::IoError,
                            message: format!("path not found: {}", remote_path),
                        },
                    )
                    .await
                    .ok();
                    anyhow::bail!("Requested pull path not found: {}", remote_path);
                }

                let mut sender = Sender::new(engine_config);
                if let Some(cb) = progress_cb {
                    sender.set_progress_callback(cb);
                }

                sender
                    .serve_pull(&conn, &mut ctrl_send, &mut ctrl_recv, path, recursive)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;

                // Return empty vec — the server didn't write any files locally.
                Ok(Vec::new())
            }
            ControlMessage::Manifest { manifest_bytes } => {
                // Normal send flow.  We already consumed the Manifest message,
                // so we cannot simply call receive_transfer (which expects to
                // read it).  Instead, use the lower-level
                // receive_transfer_with_manifest that accepts pre-read streams.
                let mut receiver =
                    Receiver::new(engine_config).context("Failed to create receiver")?;
                if let Some(cb) = progress_cb {
                    receiver.set_progress_callback(cb);
                }

                receiver
                    .receive_transfer_preread(
                        &conn,
                        &self.dest_dir,
                        manifest_bytes,
                        ctrl_send,
                        ctrl_recv,
                    )
                    .await
                    .map_err(|e| anyhow::anyhow!(e))
            }
            other => {
                warn!(
                    "Unexpected first control message from {}: {:?}",
                    conn.remote_addr(),
                    other
                );
                wire::write_control_message(
                    &mut ctrl_send,
                    &ControlMessage::Error {
                        code: ErrorCode::ProtocolError,
                        message: "expected Manifest or PullRequest as first message".into(),
                    },
                )
                .await
                .ok();
                anyhow::bail!("Unexpected first control message");
            }
        }
    }

    /// Run the server loop, accepting transfers until interrupted.
    ///
    /// Handles `SIGINT` / `Ctrl-C` for graceful shutdown.
    pub async fn run(&self) -> Result<()> {
        info!(
            "ByteHaul daemon running on {}, writing to {}",
            self.quic_server.local_addr(),
            self.dest_dir.display()
        );

        loop {
            tokio::select! {
                result = self.accept_transfer(None) => {
                    match result {
                        Ok(paths) => {
                            info!("Transfer complete: {} files", paths.len());
                        }
                        Err(e) => {
                            tracing::error!("Transfer failed: {}", e);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal received, stopping daemon");
                    self.quic_server.shutdown();
                    return Ok(());
                }
            }
        }
    }

    /// Run in ephemeral mode: accept one transfer, then exit.
    /// Prints the ready address to stdout for SSH bootstrapping.
    pub async fn run_ephemeral(&self) -> Result<Vec<PathBuf>> {
        println!("BYTEHAUL_READY {}", self.quic_server.local_addr());
        let result = self.accept_transfer(None).await;
        self.quic_server.shutdown();
        result
    }
}
