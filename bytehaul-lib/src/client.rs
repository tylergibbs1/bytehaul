use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use bytehaul_proto::engine::{EngineConfig, Sender, TransferProgress};
use bytehaul_proto::transport::{QuicClient, QuicConnection};

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
}

/// What to transfer.
enum TransferSource {
    File { local_path: String, remote_path: String },
    Directory { local_dir: String, remote_dir: String },
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
