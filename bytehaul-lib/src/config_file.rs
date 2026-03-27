//! Configuration file loader for ByteHaul.
//!
//! Loads defaults from `~/.bytehaul/config.toml`.  Every field is optional so
//! that a partial config file only overrides the keys it specifies; compiled
//! defaults are used for anything omitted.

use std::path::{Path, PathBuf};

use bytehaul_proto::congestion::CongestionMode;
use bytehaul_proto::engine::OverwriteMode;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur while loading a configuration file.
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    /// The file could not be read.
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    /// The file contents are not valid TOML or do not match the schema.
    #[error("failed to parse config file: {0}")]
    Parse(#[from] toml::de::Error),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, ConfigFileError>;

// ---------------------------------------------------------------------------
// Section structs
// ---------------------------------------------------------------------------

/// `[transfer]` section of the config file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransferSection {
    /// Block (chunk) size in mebibytes.
    pub block_size_mb: Option<u32>,
    /// Maximum number of parallel QUIC streams.
    pub parallel_streams: Option<usize>,
    /// Congestion mode: `"fair"` or `"aggressive"`.
    pub congestion: Option<String>,
    /// Maximum bandwidth in Mbps (0 = unlimited).
    pub max_bandwidth_mbps: Option<u64>,
    /// Enable transfer resumption.
    pub resume: Option<bool>,
    /// Enable delta (rsync-style) transfers.
    pub delta: Option<bool>,
    /// Encrypt resume-state files at rest.
    pub encrypt_state: Option<bool>,
    /// FEC group size (0 = disabled). Generates 1 parity chunk per N data chunks.
    pub fec_group_size: Option<usize>,
    /// Overwrite behaviour: `"fail"`, `"rename"`, or `"overwrite"`.
    pub overwrite: Option<String>,
}

/// `[daemon]` section of the config file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DaemonSection {
    /// TCP/QUIC port the daemon listens on.
    pub port: Option<u16>,
    /// Bind address.
    pub bind: Option<String>,
    /// Default destination directory for incoming files.
    pub dest: Option<String>,
}

/// `[logging]` section of the config file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoggingSection {
    /// Enable verbose (debug-level) logging by default.
    pub verbose: Option<bool>,
}

// ---------------------------------------------------------------------------
// ConfigFile
// ---------------------------------------------------------------------------

/// Top-level representation of `~/.bytehaul/config.toml`.
///
/// All sections and fields are optional.  Use the convenience accessors to
/// retrieve values with type conversions already applied.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigFile {
    /// `[transfer]` section.
    pub transfer: Option<TransferSection>,
    /// `[daemon]` section.
    pub daemon: Option<DaemonSection>,
    /// `[logging]` section.
    pub logging: Option<LoggingSection>,
}

impl ConfigFile {
    // -- Loading ------------------------------------------------------------

    /// Try to load `~/.bytehaul/config.toml`.
    ///
    /// Returns `Ok(None)` when the file does not exist.  Returns an error only
    /// if the file exists but cannot be read or parsed.
    pub fn load() -> Result<Option<ConfigFile>> {
        let path = match Self::default_path() {
            Some(p) => p,
            None => return Ok(None),
        };
        if !path.exists() {
            return Ok(None);
        }
        Self::load_from(&path).map(Some)
    }

    /// Load a config file from an explicit path.
    pub fn load_from(path: &Path) -> Result<ConfigFile> {
        let contents = std::fs::read_to_string(path)?;
        let config: ConfigFile = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Return the default config path (`~/.bytehaul/config.toml`).
    pub fn default_path() -> Option<PathBuf> {
        #[cfg(not(target_os = "windows"))]
        let home = std::env::var("HOME").ok();
        #[cfg(target_os = "windows")]
        let home = std::env::var("USERPROFILE").ok();

        home.map(|h| PathBuf::from(h).join(".bytehaul").join("config.toml"))
    }

    // -- Transfer accessors -------------------------------------------------

    /// Block size in bytes (`block_size_mb * 1024 * 1024`).
    pub fn transfer_block_size(&self) -> Option<u32> {
        self.transfer
            .as_ref()
            .and_then(|t| t.block_size_mb)
            .map(|mb| mb.saturating_mul(1024 * 1024))
    }

    /// Number of parallel streams.
    pub fn transfer_parallel(&self) -> Option<usize> {
        self.transfer.as_ref().and_then(|t| t.parallel_streams)
    }

    /// Congestion mode parsed from the string value.
    pub fn transfer_congestion(&self) -> Option<CongestionMode> {
        self.transfer
            .as_ref()
            .and_then(|t| t.congestion.as_deref())
            .and_then(|s| match s {
                "fair" => Some(CongestionMode::Fair),
                "aggressive" => Some(CongestionMode::Aggressive),
                _ => None,
            })
    }

    /// Maximum bandwidth in Mbps (`0` means unlimited / `None`).
    pub fn transfer_max_bandwidth_mbps(&self) -> Option<u64> {
        self.transfer
            .as_ref()
            .and_then(|t| t.max_bandwidth_mbps)
            .and_then(|v| if v == 0 { None } else { Some(v) })
    }

    /// Whether transfer resumption is enabled.
    pub fn transfer_resume(&self) -> Option<bool> {
        self.transfer.as_ref().and_then(|t| t.resume)
    }

    /// Whether delta transfers are enabled.
    pub fn transfer_delta(&self) -> Option<bool> {
        self.transfer.as_ref().and_then(|t| t.delta)
    }

    /// Whether resume-state encryption is enabled.
    pub fn transfer_encrypt_state(&self) -> Option<bool> {
        self.transfer.as_ref().and_then(|t| t.encrypt_state)
    }

    pub fn transfer_fec_group_size(&self) -> Option<usize> {
        self.transfer.as_ref().and_then(|t| t.fec_group_size)
    }

    /// Overwrite mode parsed from the string value.
    pub fn transfer_overwrite(&self) -> Option<OverwriteMode> {
        self.transfer
            .as_ref()
            .and_then(|t| t.overwrite.as_deref())
            .and_then(|s| match s {
                "fail" => Some(OverwriteMode::Fail),
                "rename" => Some(OverwriteMode::Rename),
                "overwrite" => Some(OverwriteMode::Overwrite),
                _ => None,
            })
    }

    // -- Daemon accessors ---------------------------------------------------

    /// Daemon listen port.
    pub fn daemon_port(&self) -> Option<u16> {
        self.daemon.as_ref().and_then(|d| d.port)
    }

    /// Daemon bind address.
    pub fn daemon_bind(&self) -> Option<&str> {
        self.daemon
            .as_ref()
            .and_then(|d| d.bind.as_deref())
    }

    /// Daemon default destination directory.
    pub fn daemon_dest(&self) -> Option<&str> {
        self.daemon
            .as_ref()
            .and_then(|d| d.dest.as_deref())
    }

    // -- Logging accessors --------------------------------------------------

    /// Whether verbose logging is enabled.
    pub fn logging_verbose(&self) -> Option<bool> {
        self.logging.as_ref().and_then(|l| l.verbose)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Full config round-trip.
    #[test]
    fn test_load_full_config() {
        let toml = r#"
[transfer]
block_size_mb = 4
parallel_streams = 16
congestion = "fair"
max_bandwidth_mbps = 0
resume = true
delta = false
encrypt_state = false
overwrite = "fail"

[daemon]
port = 7700
bind = "0.0.0.0"
dest = "."

[logging]
verbose = false
"#;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(toml.as_bytes()).unwrap();
        }

        let cfg = ConfigFile::load_from(&path).unwrap();

        assert_eq!(cfg.transfer_block_size(), Some(4 * 1024 * 1024));
        assert_eq!(cfg.transfer_parallel(), Some(16));
        assert_eq!(cfg.transfer_congestion(), Some(CongestionMode::Fair));
        // max_bandwidth_mbps = 0 maps to None (unlimited).
        assert_eq!(cfg.transfer_max_bandwidth_mbps(), None);
        assert_eq!(cfg.transfer_resume(), Some(true));
        assert_eq!(cfg.transfer_delta(), Some(false));
        assert_eq!(cfg.transfer_encrypt_state(), Some(false));
        assert_eq!(cfg.transfer_overwrite(), Some(OverwriteMode::Fail));

        assert_eq!(cfg.daemon_port(), Some(7700));
        assert_eq!(cfg.daemon_bind(), Some("0.0.0.0"));
        assert_eq!(cfg.daemon_dest(), Some("."));

        assert_eq!(cfg.logging_verbose(), Some(false));
    }

    /// A partial config only has the keys that are set.
    #[test]
    fn test_partial_config() {
        let toml = r#"
[transfer]
block_size_mb = 8
congestion = "aggressive"
"#;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, toml).unwrap();

        let cfg = ConfigFile::load_from(&path).unwrap();

        assert_eq!(cfg.transfer_block_size(), Some(8 * 1024 * 1024));
        assert_eq!(cfg.transfer_congestion(), Some(CongestionMode::Aggressive));
        // Unspecified fields are None.
        assert_eq!(cfg.transfer_parallel(), None);
        assert_eq!(cfg.transfer_resume(), None);
        assert_eq!(cfg.daemon_port(), None);
        assert_eq!(cfg.logging_verbose(), None);
    }

    /// An empty file produces an empty (all-None) config.
    #[test]
    fn test_empty_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "").unwrap();

        let cfg = ConfigFile::load_from(&path).unwrap();

        assert!(cfg.transfer.is_none());
        assert!(cfg.daemon.is_none());
        assert!(cfg.logging.is_none());
    }

    /// A missing file returns `Ok(None)` from `load()`.
    #[test]
    fn test_missing_file_returns_none() {
        // Temporarily override HOME to a temp dir with no config file.
        let dir = tempfile::tempdir().unwrap();
        let prev = std::env::var("HOME").ok();
        std::env::set_var("HOME", dir.path());

        let result = ConfigFile::load().unwrap();
        assert!(result.is_none());

        // Restore HOME.
        match prev {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
    }

    /// `load_from` with a nonexistent path returns an I/O error.
    #[test]
    fn test_load_from_nonexistent() {
        let result = ConfigFile::load_from(Path::new("/tmp/nonexistent-bytehaul-cfg.toml"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigFileError::Io(_)));
    }

    /// Invalid TOML returns a parse error.
    #[test]
    fn test_invalid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "this is [not valid toml").unwrap();

        let result = ConfigFile::load_from(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigFileError::Parse(_)));
    }

    /// Unknown congestion / overwrite strings map to None.
    #[test]
    fn test_unknown_enum_values() {
        let toml = r#"
[transfer]
congestion = "turbo"
overwrite = "yolo"
"#;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, toml).unwrap();

        let cfg = ConfigFile::load_from(&path).unwrap();
        assert_eq!(cfg.transfer_congestion(), None);
        assert_eq!(cfg.transfer_overwrite(), None);
    }

    /// Overwrite modes parse correctly.
    #[test]
    fn test_overwrite_modes() {
        for (input, expected) in [
            ("rename", OverwriteMode::Rename),
            ("overwrite", OverwriteMode::Overwrite),
            ("fail", OverwriteMode::Fail),
        ] {
            let toml = format!("[transfer]\noverwrite = \"{input}\"");
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("config.toml");
            std::fs::write(&path, toml).unwrap();

            let cfg = ConfigFile::load_from(&path).unwrap();
            assert_eq!(cfg.transfer_overwrite(), Some(expected));
        }
    }

    /// Non-zero max_bandwidth_mbps returns Some.
    #[test]
    fn test_nonzero_bandwidth() {
        let toml = "[transfer]\nmax_bandwidth_mbps = 500";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, toml).unwrap();

        let cfg = ConfigFile::load_from(&path).unwrap();
        assert_eq!(cfg.transfer_max_bandwidth_mbps(), Some(500));
    }
}
