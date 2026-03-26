use std::path::PathBuf;

use anyhow::Result;
use chrono::Utc;
use clap::Args;
use console::style;

use bytehaul_proto::resume::StateManager;

#[derive(Args)]
pub struct StatusArgs {
    /// State directory (default: ~/.bytehaul/state)
    #[arg(long)]
    pub state_dir: Option<String>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: StatusArgs) -> Result<()> {
    let state_dir = args.state_dir.map(PathBuf::from);
    let mgr = StateManager::new(state_dir.clone())?;

    // Read all state files
    let dir = state_dir.unwrap_or_else(|| {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home).join(".bytehaul").join("state")
    });

    if !dir.exists() {
        eprintln!("  No state directory found. Run {} to initialize.", style("bytehaul init").cyan());
        return Ok(());
    }

    let mut entries = Vec::new();
    let rd = std::fs::read_dir(&dir)?;
    for entry in rd {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let data = std::fs::read_to_string(&path)?;
        if let Ok(state) = serde_json::from_str::<bytehaul_proto::resume::TransferState>(&data) {
            entries.push(state);
        }
    }

    if entries.is_empty() {
        eprintln!("  No active or recent transfers.");
        return Ok(());
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
        return Ok(());
    }

    entries.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));

    eprintln!(
        "  {:<14} {:<30} {:>10} {:>12} {:>10}  {}",
        "Transfer ID", "File", "Progress", "Size", "Age", "Status"
    );
    eprintln!("  {}", "─".repeat(90));

    let now = Utc::now();
    for state in &entries {
        let id_short = if state.transfer_id.len() > 12 {
            &state.transfer_id[..12]
        } else {
            &state.transfer_id
        };

        let received = state.received_count();
        let total = state.total_blocks;
        let progress = if total > 0 {
            format!("{}/{}", received, total)
        } else {
            "0/0".to_string()
        };
        let pct = if total > 0 {
            received as f64 / total as f64 * 100.0
        } else {
            0.0
        };

        let size = humansize::format_size(state.total_size, humansize::BINARY);

        let age = now.signed_duration_since(state.last_activity);
        let age_str = if age.num_days() > 0 {
            format!("{}d ago", age.num_days())
        } else if age.num_hours() > 0 {
            format!("{}h ago", age.num_hours())
        } else if age.num_minutes() > 0 {
            format!("{}m ago", age.num_minutes())
        } else {
            "just now".to_string()
        };

        let status = if received >= total {
            style("complete").green().to_string()
        } else if age.num_minutes() < 5 {
            style("active").cyan().to_string()
        } else {
            style("stale").yellow().to_string()
        };

        let file_display = if state.file_path.len() > 28 {
            format!("...{}", &state.file_path[state.file_path.len()-25..])
        } else {
            state.file_path.clone()
        };

        eprintln!(
            "  {:<14} {:<30} {:>10} {:>12} {:>10}  {}",
            id_short, file_display, format!("{:.0}%", pct), size, age_str, status
        );
    }

    eprintln!(
        "\n  {} transfer(s). Run {} to remove stale entries.",
        entries.len(),
        style("bytehaul clean").cyan()
    );

    Ok(())
}
