mod scenarios;
mod harness;
mod report;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "bytehaul-bench",
    about = "ByteHaul benchmark infrastructure",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List available benchmark scenarios
    List,

    /// Run a specific benchmark scenario
    Run(RunArgs),

    /// Run all benchmark scenarios
    RunAll(RunAllArgs),

    /// Generate a benchmark report
    Report(ReportArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    /// Scenario name
    pub scenario: String,

    /// Remote destination
    pub destination: String,

    /// Iterations per tool
    #[arg(long, default_value = "3")]
    pub iterations: u32,

    /// Tools to compare against
    #[arg(long, value_delimiter = ',')]
    pub compare: Vec<String>,

    /// Output results to JSON file
    #[arg(long)]
    pub output: Option<String>,

    /// Network interface for tc/netem simulation (requires sudo)
    #[arg(long)]
    pub interface: Option<String>,

    /// Print netem commands without executing (dry run)
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(clap::Args)]
struct RunAllArgs {
    /// Remote destination
    pub destination: String,

    /// Iterations per tool
    #[arg(long, default_value = "3")]
    pub iterations: u32,

    /// Tools to compare against
    #[arg(long, value_delimiter = ',')]
    pub compare: Vec<String>,

    /// Output directory for results
    #[arg(long, default_value = "./bench-results")]
    pub output_dir: String,

    /// Network interface for tc/netem simulation (requires sudo)
    #[arg(long)]
    pub interface: Option<String>,

    /// Print netem commands without executing (dry run)
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(clap::Args)]
struct ReportArgs {
    /// Directory containing benchmark results
    pub results_dir: String,

    /// Output format (text, json, markdown)
    #[arg(long, default_value = "text")]
    pub format: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::List => {
            scenarios::list_scenarios();
            Ok(())
        }
        Commands::Run(args) => {
            let scenario = scenarios::get_scenario(&args.scenario)?;
            harness::run_scenario(
                &scenario,
                &args.destination,
                args.iterations,
                &args.compare,
                args.output.as_deref(),
                args.interface.as_deref(),
                args.dry_run,
            ).await
        }
        Commands::RunAll(args) => {
            harness::run_all_scenarios(
                &args.destination,
                args.iterations,
                &args.compare,
                &args.output_dir,
                args.interface.as_deref(),
                args.dry_run,
            ).await
        }
        Commands::Report(args) => {
            report::generate_report(&args.results_dir, &args.format)
        }
    }
}
