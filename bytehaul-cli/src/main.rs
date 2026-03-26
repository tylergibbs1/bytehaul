mod send;
mod daemon;
mod bench;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "bytehaul",
    about = "Fast file transfer over QUIC",
    version,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a file or directory to a remote host
    Send(send::SendArgs),

    /// Start a receiver daemon
    Daemon(daemon::DaemonArgs),

    /// Run transfer benchmarks
    Bench(bench::BenchArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    let filter = if cli.verbose {
        EnvFilter::new("bytehaul=debug,bytehaul_proto=debug,bytehaul_lib=debug")
    } else {
        EnvFilter::new("bytehaul=info,bytehaul_proto=info,bytehaul_lib=info")
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    match cli.command {
        Commands::Send(args) => send::run(args).await,
        Commands::Daemon(args) => daemon::run(args).await,
        Commands::Bench(args) => bench::run(args).await,
    }
}
