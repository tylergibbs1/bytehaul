use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

use bytehaul_proto::engine::TransferProgress;
use console::style;
use serde::Serialize;

/// Controls whether output is human-readable (stderr + indicatif),
/// machine-readable (JSON on stdout), or suppressed (quiet).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
    Quiet,
}

/// Structured events emitted in JSON mode. Each variant serializes with an
/// `"event"` tag so consumers can dispatch on it.
#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum JsonEvent {
    TransferStart {
        transfer_id: String,
        files: u64,
        total_bytes: u64,
    },
    Progress {
        transferred_bytes: u64,
        total_bytes: u64,
        speed_mbps: f64,
        elapsed_secs: f64,
    },
    TransferComplete {
        elapsed_secs: f64,
        total_bytes: u64,
        speed_mbps: f64,
        verified: bool,
    },
    DryRun {
        files: u64,
        total_bytes: u64,
        block_size: u32,
        blocks: u64,
    },
    Error {
        message: String,
    },
}

/// Write a single JSON event as one line to stdout. Flushes immediately so
/// each event is available to piped consumers without delay.
pub fn emit_json(event: &JsonEvent) {
    let mut stdout = io::stdout().lock();
    let _ = serde_json::to_writer(&mut stdout, event);
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}

/// Unified output abstraction used by all subcommands.
///
/// * **Human** mode: `info` prints to stderr, progress via indicatif.
/// * **Json** mode: `info` suppressed, events emitted as JSON lines.
/// * **Quiet** mode: all non-error output suppressed.
pub struct Reporter {
    mode: OutputMode,
}

impl Reporter {
    pub fn new(mode: OutputMode) -> Self {
        Self { mode }
    }

    /// Build from the global CLI flags.
    pub fn from_flags(json: bool, quiet: bool) -> Self {
        let mode = if json {
            OutputMode::Json
        } else if quiet {
            OutputMode::Quiet
        } else {
            OutputMode::Human
        };
        Self::new(mode)
    }

    /// Convenience constructor from a boolean flag (true = JSON).
    pub fn from_flag(json: bool) -> Self {
        Self::from_flags(json, false)
    }

    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    pub fn is_quiet(&self) -> bool {
        self.mode == OutputMode::Quiet
    }

    pub fn is_human(&self) -> bool {
        self.mode == OutputMode::Human
    }

    /// Print an informational message to stderr. Suppressed in JSON and Quiet mode.
    pub fn info(&self, msg: &str) {
        if self.mode == OutputMode::Human {
            eprintln!("{msg}");
        }
    }

    /// Print a success message with green checkmark.
    pub fn success(&self, msg: &str) {
        if self.mode == OutputMode::Human {
            eprintln!("  {} {msg}", style("\u{2713}").green().bold());
        }
    }

    /// Print a warning message.
    pub fn warn(&self, msg: &str) {
        if self.mode == OutputMode::Human {
            eprintln!("  {} {msg}", style("!").yellow().bold());
        }
    }

    /// Print an error message. Shown in all modes (stderr in Human/Quiet, JSON in Json).
    pub fn error(&self, msg: &str) {
        match self.mode {
            OutputMode::Json => emit_json(&JsonEvent::Error {
                message: msg.to_string(),
            }),
            _ => eprintln!("  {} {msg}", style("error:").red().bold()),
        }
    }

    /// Emit a structured JSON event. No-op in Human/Quiet mode.
    pub fn emit(&self, event: &JsonEvent) {
        if self.mode == OutputMode::Json {
            emit_json(event);
        }
    }

    /// Print a rich transfer summary. Shows in Human mode, emits JSON in Json mode.
    pub fn transfer_summary(
        &self,
        files: u64,
        total_bytes: u64,
        elapsed_secs: f64,
        verified: bool,
    ) {
        let speed_bytes = if elapsed_secs > 0.0 {
            total_bytes as f64 / elapsed_secs
        } else {
            0.0
        };
        let speed_mbps = (speed_bytes * 8.0) / 1_000_000.0;

        match self.mode {
            OutputMode::Human => {
                let size = format_bytes(total_bytes);
                let speed = format_bytes(speed_bytes as u64);
                eprintln!();
                eprintln!(
                    "  {} Transfer complete",
                    style("\u{2713}").green().bold()
                );
                if files > 1 {
                    eprintln!(
                        "    {} {files} files, {size}",
                        style("Transferred:").dim()
                    );
                } else {
                    eprintln!(
                        "    {} {size}",
                        style("Transferred:").dim()
                    );
                }
                eprintln!(
                    "    {}       {speed}/s ({speed_mbps:.0} Mbps)",
                    style("Speed:").dim()
                );
                eprintln!(
                    "    {}     {elapsed_secs:.1}s",
                    style("Elapsed:").dim()
                );
                if verified {
                    eprintln!(
                        "    {}    BLAKE3 {}",
                        style("Verified:").dim(),
                        style("\u{2713}").green()
                    );
                }
            }
            OutputMode::Json => {
                self.emit(&JsonEvent::TransferComplete {
                    elapsed_secs,
                    total_bytes,
                    speed_mbps,
                    verified,
                });
            }
            OutputMode::Quiet => {}
        }
    }

    /// Return a progress callback suitable for [`Transfer::on_progress`].
    ///
    /// * **Json** mode: returns `Some(callback)` that emits
    ///   [`JsonEvent::Progress`] for every progress tick.
    /// * **Human** mode: returns `None` so the caller can wire up an indicatif
    ///   progress bar instead.
    /// * **Quiet** mode: returns `None` (no progress).
    pub fn progress_callback(
        &self,
    ) -> Option<Box<dyn Fn(TransferProgress) + Send + Sync>> {
        match self.mode {
            OutputMode::Human | OutputMode::Quiet => None,
            OutputMode::Json => {
                let start = Instant::now();
                let start = Arc::new(start);
                Some(Box::new(move |p: TransferProgress| {
                    let elapsed = start.elapsed().as_secs_f64();
                    let speed_mbps = if elapsed > 0.0 {
                        (p.speed_bytes_per_sec * 8.0) / 1_000_000.0
                    } else {
                        0.0
                    };
                    emit_json(&JsonEvent::Progress {
                        transferred_bytes: p.transferred_bytes,
                        total_bytes: p.total_bytes,
                        speed_mbps,
                        elapsed_secs: elapsed,
                    });
                }))
            }
        }
    }
}

/// Standard progress bar template used across all commands.
pub const PROGRESS_TEMPLATE: &str =
    "  {bar:40.cyan/blue} {bytes}/{total_bytes} | {bytes_per_sec} | ETA {eta}";

/// Standard progress bar characters.
pub const PROGRESS_CHARS: &str = "\u{2588}\u{2589}\u{258a}\u{258b}\u{258c}\u{258d}\u{258e}\u{258f} ";

/// Standard spinner template for unknown-size phases.
pub const SPINNER_TEMPLATE: &str =
    "  {spinner:.cyan} {bytes} received | {bytes_per_sec} | {elapsed_precise}";

/// Format bytes into human-readable string (e.g., "1.2 GB").
pub fn format_bytes(bytes: u64) -> String {
    humansize::format_size(bytes, humansize::BINARY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reporter_from_flags() {
        let human = Reporter::from_flags(false, false);
        assert!(human.is_human());

        let json = Reporter::from_flags(true, false);
        assert!(json.is_json());

        let quiet = Reporter::from_flags(false, true);
        assert!(quiet.is_quiet());

        // json takes precedence over quiet
        let both = Reporter::from_flags(true, true);
        assert!(both.is_json());
    }

    #[test]
    fn reporter_from_flag() {
        let human = Reporter::from_flag(false);
        assert!(!human.is_json());
        assert_eq!(human.mode(), OutputMode::Human);

        let json = Reporter::from_flag(true);
        assert!(json.is_json());
        assert_eq!(json.mode(), OutputMode::Json);
    }

    #[test]
    fn json_event_serialization() {
        let event = JsonEvent::TransferStart {
            transfer_id: "abc123".to_string(),
            files: 5,
            total_bytes: 1024,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event\":\"transfer_start\""));
        assert!(json.contains("\"files\":5"));
        assert!(json.contains("\"total_bytes\":1024"));
    }

    #[test]
    fn json_event_progress_serialization() {
        let event = JsonEvent::Progress {
            transferred_bytes: 500,
            total_bytes: 1000,
            speed_mbps: 42.5,
            elapsed_secs: 1.5,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event\":\"progress\""));
        assert!(json.contains("\"speed_mbps\":42.5"));
    }

    #[test]
    fn json_event_complete_serialization() {
        let event = JsonEvent::TransferComplete {
            elapsed_secs: 3.14,
            total_bytes: 10485760,
            speed_mbps: 100.0,
            verified: true,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event\":\"transfer_complete\""));
        assert!(json.contains("\"verified\":true"));
    }

    #[test]
    fn json_event_dry_run_serialization() {
        let event = JsonEvent::DryRun {
            files: 10,
            total_bytes: 5242880,
            block_size: 4194304,
            blocks: 2,
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event\":\"dry_run\""));
        assert!(json.contains("\"block_size\":4194304"));
    }

    #[test]
    fn json_event_error_serialization() {
        let event = JsonEvent::Error {
            message: "something broke".to_string(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event\":\"error\""));
        assert!(json.contains("\"something broke\""));
    }

    #[test]
    fn human_reporter_progress_callback_is_none() {
        let reporter = Reporter::from_flag(false);
        assert!(reporter.progress_callback().is_none());
    }

    #[test]
    fn json_reporter_progress_callback_is_some() {
        let reporter = Reporter::from_flag(true);
        assert!(reporter.progress_callback().is_some());
    }

    #[test]
    fn format_bytes_works() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1024), "1 KiB");
    }
}
