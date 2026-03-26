use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

use bytehaul_proto::engine::TransferProgress;
use serde::Serialize;

/// Controls whether output is human-readable (stderr + indicatif) or
/// machine-readable (one JSON object per line on stdout).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
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
    // serde_json::to_writer already omits trailing newlines, so we add one.
    let _ = serde_json::to_writer(&mut stdout, event);
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}

/// A thin wrapper around [`OutputMode`] that subcommands use for all output.
///
/// * In **Human** mode, `info` prints to stderr (so it doesn't pollute piped
///   stdout) and progress is driven by indicatif.
/// * In **Json** mode, `info` is suppressed and progress is emitted as
///   structured [`JsonEvent::Progress`] lines.
pub struct Reporter {
    mode: OutputMode,
}

impl Reporter {
    pub fn new(mode: OutputMode) -> Self {
        Self { mode }
    }

    /// Convenience constructor from a boolean flag (true = JSON).
    pub fn from_flag(json: bool) -> Self {
        Self::new(if json { OutputMode::Json } else { OutputMode::Human })
    }

    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }

    /// Print an informational message to stderr. Suppressed in JSON mode.
    pub fn info(&self, msg: &str) {
        if self.mode == OutputMode::Human {
            eprintln!("{msg}");
        }
    }

    /// Emit a structured JSON event. No-op in Human mode.
    pub fn emit(&self, event: &JsonEvent) {
        if self.mode == OutputMode::Json {
            emit_json(event);
        }
    }

    /// Return a progress callback suitable for [`Transfer::on_progress`].
    ///
    /// * **Json** mode: returns `Some(callback)` that emits
    ///   [`JsonEvent::Progress`] for every progress tick.
    /// * **Human** mode: returns `None` so the caller can wire up an indicatif
    ///   progress bar instead.
    pub fn progress_callback(
        &self,
    ) -> Option<Box<dyn Fn(TransferProgress) + Send + Sync>> {
        match self.mode {
            OutputMode::Human => None,
            OutputMode::Json => {
                let start = Instant::now();
                // Wrap start in Arc so the closure is Fn (not FnOnce).
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
