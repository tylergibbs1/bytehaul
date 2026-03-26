mod bench;
mod clean;
mod completions;
mod daemon;
mod init;
pub mod output;
mod pull;
mod send;
mod status;

use clap::{CommandFactory, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "bytehaul",
    about = "Fast file transfer over QUIC",
    long_about = "ByteHaul transfers files over QUIC with parallel streams, BLAKE3 verification,\n\
                  and resumable transfers. Designed for high-bandwidth, high-latency links\n\
                  where TCP underperforms.\n\n\
                  Examples:\n  \
                    bytehaul send ./data.bin user@remote:/path/\n  \
                    bytehaul send -r ./dataset/ user@remote:/data/\n  \
                    bytehaul send --daemon 10.0.0.5:7700 ./file.bin /dest\n  \
                    bytehaul pull user@remote:/data/file.bin ./local/\n  \
                    bytehaul pull -r user@remote:/dataset/ ./local/\n  \
                    bytehaul daemon --port 7700 --dest /data/incoming\n  \
                    bytehaul status\n  \
                    bytehaul clean --max-age 3d",
    version,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Emit machine-readable JSON events to stdout instead of human output
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a file or directory to a remote host
    Send(send::SendArgs),

    /// Pull a file or directory from a remote host
    Pull(pull::PullArgs),

    /// Start a receiver daemon
    Daemon(daemon::DaemonArgs),

    /// Run transfer benchmarks
    Bench(bench::BenchArgs),

    /// Initialize ByteHaul configuration
    Init(init::InitArgs),

    /// Show active and recent transfers
    Status(status::StatusArgs),

    /// Clean up stale transfer state files
    Clean(clean::CleanArgs),

    /// Generate shell completions
    Completions(completions::CompletionsArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Completions don't need tracing
    if let Commands::Completions(args) = cli.command {
        let mut cmd = Cli::command();
        return completions::run(args, &mut cmd);
    }

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
        Commands::Send(args) => send::run(args, cli.json).await,
        Commands::Pull(args) => pull::run(args).await,
        Commands::Daemon(args) => daemon::run(args).await,
        Commands::Bench(args) => bench::run(args).await,
        Commands::Init(args) => init::run(args).await,
        Commands::Status(args) => status::run(args).await,
        Commands::Clean(args) => clean::run(args).await,
        Commands::Completions(_) => unreachable!(),
    }
}
