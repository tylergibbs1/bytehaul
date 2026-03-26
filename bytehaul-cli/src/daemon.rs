use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, ValueEnum};
use console::style;

use bytehaul_lib::config::TransferConfig;
use bytehaul_lib::server::Server;
use bytehaul_proto::engine::OverwriteMode;

#[derive(Args)]
pub struct DaemonArgs {
    /// Port to listen on
    #[arg(short, long, default_value = "7700")]
    pub port: u16,

    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    pub bind: String,

    /// Directory to write received files to
    #[arg(short, long, default_value = ".")]
    pub dest: String,

    /// What to do if destination file exists
    #[arg(long, value_enum, default_value = "fail")]
    pub overwrite: OverwriteArg,

    /// Ephemeral mode: accept one transfer, then exit (used by SSH bootstrap)
    #[arg(long, hide = true)]
    pub ephemeral: bool,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum OverwriteArg {
    /// Fail if file exists
    Fail,
    /// Rename new file (append .1, .2, etc.)
    Rename,
    /// Overwrite existing file
    Overwrite,
}

impl From<OverwriteArg> for OverwriteMode {
    fn from(arg: OverwriteArg) -> Self {
        match arg {
            OverwriteArg::Fail => OverwriteMode::Fail,
            OverwriteArg::Rename => OverwriteMode::Rename,
            OverwriteArg::Overwrite => OverwriteMode::Overwrite,
        }
    }
}

pub async fn run(args: DaemonArgs) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    let dest_dir = PathBuf::from(&args.dest);

    if !dest_dir.exists() {
        tokio::fs::create_dir_all(&dest_dir).await?;
    }

    let config = TransferConfig {
        overwrite_mode: args.overwrite.into(),
        ..Default::default()
    };

    let server = Server::bind(addr, dest_dir)?.with_config(config);

    if args.ephemeral {
        server.run_ephemeral().await?;
    } else {
        eprintln!(
            "  {} ByteHaul daemon listening on {}",
            style("●").green(),
            style(server.local_addr()).cyan()
        );
        server.run().await?;
    }

    Ok(())
}
