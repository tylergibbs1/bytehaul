use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::info;

use bytehaul_proto::engine::{EngineConfig, Receiver, TransferProgress};
use bytehaul_proto::transport::{QuicServer, TransportConfig};

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
    pub async fn accept_transfer(
        &self,
        progress_cb: Option<Box<dyn Fn(TransferProgress) + Send + Sync>>,
    ) -> Result<PathBuf> {
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
            ..Default::default()
        };

        let mut receiver =
            Receiver::new(engine_config).context("Failed to create receiver")?;
        if let Some(cb) = progress_cb {
            receiver.set_progress_callback(cb);
        }

        receiver
            .receive_file(&conn, &self.dest_dir)
            .await
            .map_err(|e| anyhow::anyhow!(e))
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
                        Ok(path) => {
                            info!("Transfer complete: {}", path.display());
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
    pub async fn run_ephemeral(&self) -> Result<PathBuf> {
        println!("BYTEHAUL_READY {}", self.quic_server.local_addr());
        self.accept_transfer(None).await
    }
}
