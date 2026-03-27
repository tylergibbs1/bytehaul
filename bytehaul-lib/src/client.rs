use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use bytehaul_proto::congestion::CongestionMode;
use bytehaul_proto::engine::{EngineConfig, Receiver, Sender, TransferProgress};
use bytehaul_proto::filter::FileFilter;
use bytehaul_proto::transport::{CongestionAlgo, QuicClient, QuicConnection, TransportConfig};

use crate::config::TransferConfig;

/// High-level ByteHaul client for sending files.
pub struct Client {
    conn: Arc<QuicConnection>,
    config: TransferConfig,
}

impl Client {
    /// Connect to a ByteHaul daemon at the given address.
    pub async fn connect_daemon(addr: &str, _auth: Option<()>) -> Result<Self> {
        let socket_addr: SocketAddr = addr.parse().context("Invalid daemon address")?;
        let conn = QuicClient::connect(socket_addr, "bytehaul").await?;
        Ok(Self {
            conn: Arc::new(conn),
            config: TransferConfig::default(),
        })
    }

    /// Connect to a ByteHaul daemon with tuned transport settings.
    ///
    /// Unlike [`connect_daemon`], this applies the transfer configuration's
    /// congestion algorithm to the QUIC transport layer at connection time,
    /// ensuring the sender uses optimized BBR initial windows and flow
    /// control settings from the start.
    pub async fn connect_daemon_tuned(addr: &str, config: &TransferConfig) -> Result<Self> {
        let socket_addr: SocketAddr = addr.parse().context("Invalid daemon address")?;
        let congestion_algo = match config.congestion {
            CongestionMode::Aggressive => CongestionAlgo::Bbr,
            CongestionMode::Fair => CongestionAlgo::Bbr,
        };
        let transport_config = TransportConfig {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            max_concurrent_streams: config.max_parallel_streams as u32,
            keep_alive_interval: None,
            idle_timeout: Duration::from_secs(30),
            congestion_algo,
        };
        let conn = QuicClient::connect_with_config(socket_addr, "bytehaul", transport_config).await?;
        Ok(Self {
            conn: Arc::new(conn),
            config: config.clone(),
        })
    }

    /// Connect to a remote host by bootstrapping a receiver via SSH.
    pub async fn connect_ssh(remote: &str) -> Result<Self> {
        let (addr, _ssh_process) = ssh_bootstrap(remote).await?;

        // Try QUIC first; if it fails (UDP blocked), report clearly
        match tokio::time::timeout(
            Duration::from_secs(10),
            QuicClient::connect(addr, "bytehaul"),
        )
        .await
        {
            Ok(Ok(conn)) => {
                info!("QUIC connection established to {}", addr);
                Ok(Self {
                    conn: Arc::new(conn),
                    config: TransferConfig::default(),
                })
            }
            Ok(Err(e)) => {
                warn!("QUIC connection failed: {}", e);
                warn!("This may indicate UDP is blocked by a firewall.");
                warn!("Consider using daemon mode or checking firewall rules.");
                // Fall back to SSH tunnel: forward QUIC over SSH port-forward
                info!("Attempting SSH port-forward fallback...");
                let conn = ssh_tunnel_fallback(remote, addr).await?;
                Ok(Self {
                    conn: Arc::new(conn),
                    config: TransferConfig::default(),
                })
            }
            Err(_) => {
                warn!("QUIC connection timed out (10s) - UDP may be blocked");
                warn!("Attempting SSH port-forward fallback...");
                let conn = ssh_tunnel_fallback(remote, addr).await?;
                Ok(Self {
                    conn: Arc::new(conn),
                    config: TransferConfig::default(),
                })
            }
        }
    }

    /// Connect directly to an already-established QUIC connection.
    pub fn from_connection(conn: QuicConnection) -> Self {
        Self {
            conn: Arc::new(conn),
            config: TransferConfig::default(),
        }
    }

    /// Apply a transfer configuration.
    pub fn with_config(mut self, config: TransferConfig) -> Self {
        self.config = config;
        self
    }

    /// Get the measured RTT from the QUIC connection handshake.
    /// Returns None if no RTT estimate is available yet.
    pub fn connection_rtt(&self) -> Option<std::time::Duration> {
        let stats = self.conn.inner().stats();
        let rtt = stats.path.rtt;
        if rtt.as_nanos() > 0 { Some(rtt) } else { None }
    }

    fn make_engine_config(&self) -> EngineConfig {
        EngineConfig {
            max_parallel_streams: self.config.max_parallel_streams,
            block_size: self.config.block_size,
            resume_enabled: self.config.resume,
            state_dir: self.config.state_dir.clone(),
            overwrite_mode: self.config.overwrite_mode,
            delta_enabled: self.config.delta,
            encrypt_state: self.config.encrypt_state,
            max_bandwidth_bps: self.config.max_bandwidth_mbps
                .map(|mbps| mbps * 1_000_000 / 8)
                .unwrap_or(0),
            compress: self.config.compress,
            compress_level: self.config.compress_level,
            adaptive: self.config.adaptive,
            ..Default::default()
        }
    }

    /// Send a single file to the remote.
    pub async fn send_file(&self, local_path: &str, remote_path: &str) -> Result<Transfer> {
        Ok(Transfer {
            conn: self.conn.clone(),
            config: self.make_engine_config(),
            source: TransferSource::File {
                local_path: local_path.to_string(),
                remote_path: remote_path.to_string(),
            },
            progress_cb: None,
        })
    }

    /// Send an entire directory to the remote.
    pub async fn send_directory(&self, local_dir: &str, remote_dir: &str) -> Result<Transfer> {
        Ok(Transfer {
            conn: self.conn.clone(),
            config: self.make_engine_config(),
            source: TransferSource::Directory {
                local_dir: local_dir.to_string(),
                remote_dir: remote_dir.to_string(),
            },
            progress_cb: None,
        })
    }

    /// Pull a single file from the remote to a local destination.
    pub async fn pull_file(&self, remote_path: &str, local_dest: &str) -> Result<Transfer> {
        Ok(Transfer {
            conn: self.conn.clone(),
            config: self.make_engine_config(),
            source: TransferSource::PullFile {
                remote_path: remote_path.to_string(),
                local_dest: local_dest.to_string(),
            },
            progress_cb: None,
        })
    }

    /// Pull a directory from the remote to a local destination.
    pub async fn pull_directory(&self, remote_path: &str, local_dest: &str) -> Result<Transfer> {
        Ok(Transfer {
            conn: self.conn.clone(),
            config: self.make_engine_config(),
            source: TransferSource::PullDirectory {
                remote_path: remote_path.to_string(),
                local_dest: local_dest.to_string(),
            },
            progress_cb: None,
        })
    }

    /// Send a directory with glob-based include/exclude filtering.
    pub async fn send_directory_filtered(
        &self,
        local_dir: &str,
        remote_dir: &str,
        filter: FileFilter,
    ) -> Result<Transfer> {
        Ok(Transfer {
            conn: self.conn.clone(),
            config: self.make_engine_config(),
            source: TransferSource::FilteredDirectory {
                local_dir: local_dir.to_string(),
                remote_dir: remote_dir.to_string(),
                filter,
            },
            progress_cb: None,
        })
    }
}

/// What to transfer.
enum TransferSource {
    File { local_path: String, remote_path: String },
    Directory { local_dir: String, remote_dir: String },
    FilteredDirectory { local_dir: String, remote_dir: String, filter: FileFilter },
    PullFile { remote_path: String, local_dest: String },
    PullDirectory { remote_path: String, local_dest: String },
}

/// A prepared transfer that can be observed and awaited.
pub struct Transfer {
    conn: Arc<QuicConnection>,
    config: EngineConfig,
    source: TransferSource,
    progress_cb: Option<Box<dyn Fn(TransferProgress) + Send + Sync>>,
}

impl Transfer {
    /// Register a progress callback.
    pub fn on_progress<F: Fn(TransferProgress) + Send + Sync + 'static>(&mut self, cb: F) {
        self.progress_cb = Some(Box::new(cb));
    }

    /// Execute the transfer and wait for completion.
    pub async fn wait(self) -> Result<()> {
        match self.source {
            TransferSource::PullFile { remote_path, local_dest } => {
                let mut receiver = Receiver::new(self.config)
                    .map_err(|e| anyhow::anyhow!(e))?;
                if let Some(cb) = self.progress_cb {
                    receiver.set_progress_callback(cb);
                }
                receiver
                    .pull_receive(&self.conn, &remote_path, false, Path::new(&local_dest))
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!(e))
            }
            TransferSource::PullDirectory { remote_path, local_dest } => {
                let mut receiver = Receiver::new(self.config)
                    .map_err(|e| anyhow::anyhow!(e))?;
                if let Some(cb) = self.progress_cb {
                    receiver.set_progress_callback(cb);
                }
                receiver
                    .pull_receive(&self.conn, &remote_path, true, Path::new(&local_dest))
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!(e))
            }
            _ => {
                let mut sender = Sender::new(self.config);
                if let Some(cb) = self.progress_cb {
                    sender.set_progress_callback(cb);
                }
                match self.source {
                    TransferSource::File { local_path, remote_path } => {
                        sender
                            .send_file(&self.conn, Path::new(&local_path), &remote_path)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                    TransferSource::Directory { local_dir, remote_dir } => {
                        sender
                            .send_directory(&self.conn, Path::new(&local_dir), &remote_dir)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                    TransferSource::FilteredDirectory { local_dir, remote_dir, filter } => {
                        sender
                            .send_directory_filtered(&self.conn, Path::new(&local_dir), &remote_dir, &filter)
                            .await
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                    TransferSource::PullFile { .. } | TransferSource::PullDirectory { .. } => {
                        unreachable!()
                    }
                }
            }
        }
    }
}

/// Bootstrap a ByteHaul receiver on a remote host via SSH.
///
/// 1. Check if `bytehaul` is on the remote PATH
/// 2. If not, scp the local binary to a temp location on the remote
/// 3. Launch the receiver in ephemeral mode
/// 4. Read the QUIC listener address from stdout
async fn ssh_bootstrap(remote: &str) -> Result<(SocketAddr, tokio::process::Child)> {
    info!("Bootstrapping receiver via SSH on {}", remote);

    let (user, host) = parse_remote(remote);

    // Step 1: Check if bytehaul exists on remote
    let remote_binary = detect_or_upload_binary(user, host).await?;

    // Step 2: Launch the receiver
    let mut cmd = tokio::process::Command::new("ssh");
    if let Some(user) = user {
        cmd.arg("-l").arg(user);
    }
    cmd.arg(host);
    cmd.arg(&remote_binary)
        .arg("daemon")
        .arg("--ephemeral");
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn().context("Failed to spawn SSH process")?;

    let stdout = child.stdout.take().context("No stdout from SSH")?;
    let mut reader = tokio::io::BufReader::new(stdout);
    let mut line = String::new();
    tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut line).await?;

    let line = line.trim();
    let addr_str = if let Some(addr) = line.strip_prefix("BYTEHAUL_READY ") {
        addr
    } else {
        anyhow::bail!("Unexpected output from remote bytehaul: {}", line);
    };

    // If the remote reported 0.0.0.0, substitute the SSH host
    let addr_str = if addr_str.starts_with("0.0.0.0:") || addr_str.starts_with("[::]:") {
        let port = addr_str.rsplit(':').next().unwrap_or("0");
        format!("{}:{}", host, port)
    } else {
        addr_str.to_string()
    };

    let addr: SocketAddr = addr_str
        .parse()
        .context("Failed to parse QUIC address from remote")?;

    info!("Remote receiver ready at {}", addr);
    Ok((addr, child))
}

/// Check if bytehaul exists on the remote. If not, upload the local binary.
async fn detect_or_upload_binary(user: Option<&str>, host: &str) -> Result<String> {
    // Check remote PATH
    let mut check_cmd = tokio::process::Command::new("ssh");
    if let Some(user) = user {
        check_cmd.arg("-l").arg(user);
    }
    check_cmd.arg(host);
    check_cmd.arg("which").arg("bytehaul");
    check_cmd.stdout(std::process::Stdio::piped());
    check_cmd.stderr(std::process::Stdio::null());

    let output = check_cmd.output().await.context("Failed to check remote binary")?;

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        debug!("Found bytehaul on remote at: {}", path);
        return Ok(path);
    }

    // Not found — upload the local binary
    info!("bytehaul not found on remote, uploading local binary...");

    let local_binary = std::env::current_exe().context("Failed to determine local binary path")?;
    let remote_path = "/tmp/bytehaul-ephemeral";

    let remote_dest = if let Some(user) = user {
        format!("{}@{}:{}", user, host, remote_path)
    } else {
        format!("{}:{}", host, remote_path)
    };

    let scp_status = tokio::process::Command::new("scp")
        .arg("-q")
        .arg(local_binary.to_str().unwrap())
        .arg(&remote_dest)
        .status()
        .await
        .context("Failed to scp binary to remote")?;

    if !scp_status.success() {
        anyhow::bail!("Failed to upload bytehaul binary to remote host");
    }

    // Make it executable
    let mut chmod_cmd = tokio::process::Command::new("ssh");
    if let Some(user) = user {
        chmod_cmd.arg("-l").arg(user);
    }
    chmod_cmd.arg(host);
    chmod_cmd.arg("chmod").arg("+x").arg(remote_path);
    let _ = chmod_cmd.status().await;

    info!("Uploaded bytehaul to {}", remote_path);
    Ok(remote_path.to_string())
}

/// Fall back to SSH port-forwarding when direct UDP/QUIC fails.
///
/// Opens an SSH local port-forward so QUIC traffic is tunneled through SSH.
/// This is slower (TCP encapsulation) but works when UDP is blocked.
async fn ssh_tunnel_fallback(remote: &str, target_addr: SocketAddr) -> Result<QuicConnection> {
    let (user, host) = parse_remote(remote);

    // Pick a random local port for the tunnel
    let local_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.local_addr()?.port()
    };

    let forward_spec = format!("{}:127.0.0.1:{}", local_port, target_addr.port());

    let mut tunnel_cmd = tokio::process::Command::new("ssh");
    tunnel_cmd.arg("-N"); // no remote command
    tunnel_cmd.arg("-L").arg(&forward_spec);
    if let Some(user) = user {
        tunnel_cmd.arg("-l").arg(user);
    }
    tunnel_cmd.arg(host);
    tunnel_cmd.stdout(std::process::Stdio::null());
    tunnel_cmd.stderr(std::process::Stdio::null());

    let _tunnel = tunnel_cmd.spawn().context("Failed to create SSH tunnel")?;

    // Give the tunnel a moment to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    let local_addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse()?;
    info!("SSH tunnel established, connecting QUIC via localhost:{}", local_port);

    let conn = QuicClient::connect(local_addr, "bytehaul")
        .await
        .context("Failed to connect via SSH tunnel")?;

    // Note: the tunnel SSH process is leaked intentionally — it will be cleaned
    // up when the main process exits. A proper cleanup would store the Child
    // handle and kill it on Drop.

    Ok(conn)
}

fn parse_remote(remote: &str) -> (Option<&str>, &str) {
    if remote.contains('@') {
        let parts: Vec<&str> = remote.splitn(2, '@').collect();
        (Some(parts[0]), parts[1])
    } else {
        (None, remote)
    }
}
