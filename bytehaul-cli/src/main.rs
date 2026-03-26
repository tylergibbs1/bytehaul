mod bench;
mod clean;
mod completions;
mod daemon;
mod gather;
mod init;
pub mod output;
mod pull;
mod send;
mod smart;
mod status;
mod sync_cmd;
mod watch;

use clap::{CommandFactory, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "bytehaul",
    about = "Fast file transfer over QUIC",
    long_about = "ByteHaul transfers files over QUIC with parallel streams, BLAKE3 verification,\n\
                  and resumable transfers. Designed for high-bandwidth, high-latency links\n\
                  where TCP underperforms.\n\n\
                  Quick start (zero config):\n  \
                    bytehaul ./data.bin user@remote:/path/\n  \
                    bytehaul ./dataset/ gpu-node:/data/\n\n\
                  Commands:\n  \
                    bytehaul send    Send with advanced options\n  \
                    bytehaul pull    Pull from remote\n  \
                    bytehaul sync    Bidirectional sync\n  \
                    bytehaul watch   Auto-send on changes\n  \
                    bytehaul gather  Collect from multiple hosts",
    version,
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Emit machine-readable JSON events to stdout
    #[arg(long, global = true)]
    json: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Send with advanced options (flags, filters, fan-out)
    Send(send::SendArgs),

    /// Pull files from a remote host
    Pull(pull::PullArgs),

    /// Sync a directory with a remote host
    Sync(sync_cmd::SyncArgs),

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

    /// Watch a directory and auto-send on changes
    Watch(watch::WatchArgs),

    /// Gather files from multiple remote hosts
    Gather(gather::GatherArgs),

    /// Generate shell completions
    Completions(completions::CompletionsArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Try to parse as a subcommand first.
    // If that fails and we have two positional args, use smart mode.
    let args: Vec<String> = std::env::args().collect();

    // Smart mode detection: if first non-flag arg is a path (contains / or .)
    // and isn't a known subcommand, use smart mode.
    let known_subcommands = [
        "send", "pull", "sync", "daemon", "bench", "init",
        "status", "clean", "watch", "gather", "completions", "help",
    ];

    let first_positional = args.iter().skip(1)
        .find(|a| !a.starts_with('-'))
        .map(|s| s.as_str());

    let is_smart_mode = match first_positional {
        Some(arg) if !known_subcommands.contains(&arg) => {
            // Looks like a path — check if there's a second positional (destination)
            let positionals: Vec<&str> = args.iter().skip(1)
                .filter(|a| !a.starts_with('-'))
                .map(|s| s.as_str())
                .collect();
            positionals.len() >= 2
        }
        _ => false,
    };

    if is_smart_mode {
        // Extract source and destination from args
        let positionals: Vec<String> = args.iter().skip(1)
            .filter(|a| !a.starts_with('-'))
            .cloned()
            .collect();
        let verbose = args.iter().any(|a| a == "-v" || a == "--verbose");

        if !verbose {
            // Suppress tracing in smart mode for clean output
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::new("error"))
                .with_target(false)
                .init();
        } else {
            tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::new("bytehaul=debug"))
                .with_target(false)
                .init();
        }

        return smart::run(&positionals[0], &positionals[1], None).await;
    }

    // Normal subcommand parsing
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            // If parsing fails with no subcommand, show friendly help
            if args.len() <= 1 {
                print_welcome();
                return Ok(());
            }
            e.exit();
        }
    };

    // Completions don't need tracing
    if let Some(Commands::Completions(args)) = cli.command {
        let mut cmd = Cli::command();
        return completions::run(args, &mut cmd);
    }

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
        Some(Commands::Send(args)) => send::run(args, cli.json).await,
        Some(Commands::Pull(args)) => pull::run(args).await,
        Some(Commands::Sync(args)) => sync_cmd::run(args).await,
        Some(Commands::Daemon(args)) => daemon::run(args).await,
        Some(Commands::Bench(args)) => bench::run(args).await,
        Some(Commands::Init(args)) => init::run(args).await,
        Some(Commands::Status(args)) => status::run(args).await,
        Some(Commands::Clean(args)) => clean::run(args).await,
        Some(Commands::Watch(args)) => watch::run(args).await,
        Some(Commands::Gather(args)) => gather::run(args).await,
        Some(Commands::Completions(_)) => unreachable!(),
        None => {
            print_welcome();
            Ok(())
        }
    }
}

fn print_welcome() {
    use console::style;
    println!();
    println!(
        "  {} {}",
        style("▲").cyan().bold(),
        style("ByteHaul").bold()
    );
    println!(
        "  {} Fast file transfer over QUIC",
        style("│").dim()
    );
    println!("  {}", style("│").dim());
    println!(
        "  {} {}",
        style("│").dim(),
        style("Quick start:").white().bold()
    );
    println!(
        "  {}   {} ./file.bin user@remote:/path/",
        style("│").dim(),
        style("bytehaul").cyan()
    );
    println!(
        "  {}   {} ./dataset/ gpu-node:/data/",
        style("│").dim(),
        style("bytehaul").cyan()
    );
    println!("  {}", style("│").dim());
    println!(
        "  {} {}",
        style("│").dim(),
        style("Commands:").white().bold()
    );
    println!(
        "  {}   {} {}    Send with advanced options",
        style("│").dim(),
        style("bytehaul").cyan(),
        style("send").white()
    );
    println!(
        "  {}   {} {}    Pull from remote",
        style("│").dim(),
        style("bytehaul").cyan(),
        style("pull").white()
    );
    println!(
        "  {}   {} {}    Bidirectional sync",
        style("│").dim(),
        style("bytehaul").cyan(),
        style("sync").white()
    );
    println!(
        "  {}   {} {}   Auto-send on changes",
        style("│").dim(),
        style("bytehaul").cyan(),
        style("watch").white()
    );
    println!(
        "  {}   {} {}  Collect from multiple hosts",
        style("│").dim(),
        style("bytehaul").cyan(),
        style("gather").white()
    );
    println!("  {}", style("│").dim());
    println!(
        "  {} Run {} for all options",
        style("│").dim(),
        style("bytehaul --help").cyan()
    );
    println!();
}
