use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use console::style;

#[derive(Args)]
pub struct InitArgs {
    /// Directory for ByteHaul configuration (default: ~/.bytehaul)
    #[arg(long)]
    pub dir: Option<String>,
}

const DEFAULT_CONFIG: &str = r#"# ByteHaul configuration
# Uncomment and modify values to change defaults.
# CLI flags always override these settings.

[transfer]
# block_size_mb = 4          # Block size in MB (1-64)
# parallel_streams = 16      # Number of parallel QUIC streams
# congestion = "fair"        # "fair" or "aggressive"
# max_bandwidth_mbps = 0     # 0 = unlimited
# resume = true              # Enable transfer resumption
# delta = false              # Only send changed blocks
# encrypt_state = false      # Encrypt resume state files (ChaCha20-Poly1305)
# fec_group_size = 0         # FEC: 0 = disabled, 7 = ~12.5% overhead, 15 = ~6%
# compress = false           # zstd compression for compressible data
# adaptive = false           # Auto-tune based on network conditions
# overwrite = "fail"         # "fail", "rename", or "overwrite"

[daemon]
# port = 7700                # Default daemon port
# bind = "0.0.0.0"           # Bind address
# dest = "."                 # Default destination directory

[logging]
# verbose = false            # Enable debug logging
"#;

pub async fn run(args: InitArgs) -> Result<()> {
    let base = match args.dir {
        Some(d) => PathBuf::from(d),
        None => dirs_home()?.join(".bytehaul"),
    };

    let config_path = base.join("config.toml");
    let state_dir = base.join("state");

    // Create directories
    tokio::fs::create_dir_all(&state_dir).await?;

    // Write config (don't overwrite existing)
    if config_path.exists() {
        eprintln!(
            "  {} Config already exists: {}",
            style("!").yellow(),
            style(config_path.display()).dim()
        );
    } else {
        tokio::fs::write(&config_path, DEFAULT_CONFIG).await?;
        eprintln!(
            "  {} Created {}",
            style("+").green(),
            style(config_path.display()).bold()
        );
    }

    eprintln!(
        "  {} State directory: {}",
        style("+").green(),
        style(state_dir.display()).dim()
    );

    eprintln!(
        "\n  Edit {} to customize defaults.",
        style(config_path.display()).cyan()
    );

    Ok(())
}

fn dirs_home() -> Result<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        return Ok(PathBuf::from(home));
    }
    if let Ok(profile) = std::env::var("USERPROFILE") {
        return Ok(PathBuf::from(profile));
    }
    anyhow::bail!("Could not determine home directory. Use --dir to specify.")
}
