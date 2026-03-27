use clap::ValueEnum;

use bytehaul_lib::config::TransferConfigBuilder;
use bytehaul_proto::congestion::CongestionMode;

pub const CLI_DEFAULT_BLOCK_SIZE_MB: u32 = 16;
pub const CLI_DEFAULT_PARALLEL_STREAMS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TransferProfile {
    /// Balanced defaults for everyday development and CI.
    Dev,
    /// High-throughput defaults for long-haul links.
    Wan,
    /// Max-throughput defaults for large bulk transfers.
    Bulk,
    /// Conservative defaults for low-risk or constrained environments.
    Safe,
}

#[derive(Debug, Clone, Copy)]
struct ProfileDefaults {
    congestion: CongestionMode,
    block_size_mb: u32,
    parallel_streams: usize,
}

#[derive(Debug, Clone)]
pub struct EffectiveTransferTuning {
    pub profile: TransferProfile,
    pub congestion: CongestionMode,
    pub block_size_mb: u32,
    pub parallel_streams: usize,
    pub overrides: Vec<&'static str>,
}

impl TransferProfile {
    fn defaults(self) -> ProfileDefaults {
        match self {
            Self::Dev => ProfileDefaults {
                congestion: CongestionMode::Fair,
                block_size_mb: 8,
                parallel_streams: 8,
            },
            Self::Wan => ProfileDefaults {
                congestion: CongestionMode::Aggressive,
                block_size_mb: 8,
                parallel_streams: 32,
            },
            Self::Bulk => ProfileDefaults {
                congestion: CongestionMode::Aggressive,
                block_size_mb: 32,
                parallel_streams: 64,
            },
            Self::Safe => ProfileDefaults {
                congestion: CongestionMode::Fair,
                block_size_mb: 4,
                parallel_streams: 4,
            },
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Wan => "wan",
            Self::Bulk => "bulk",
            Self::Safe => "safe",
        }
    }
}

impl EffectiveTransferTuning {
    pub fn summary_line(&self) -> String {
        let mut summary = format!(
            "  Profile: {} | congestion={} | block={} MB | streams={}",
            self.profile.label(),
            match self.congestion {
                CongestionMode::Fair => "fair",
                CongestionMode::Aggressive => "aggressive",
            },
            self.block_size_mb,
            self.parallel_streams,
        );

        if !self.overrides.is_empty() {
            summary.push_str(" | overrides: ");
            summary.push_str(&self.overrides.join(", "));
        }

        summary
    }
}

pub fn apply_transfer_profile(
    builder: TransferConfigBuilder,
    profile: TransferProfile,
    block_size_mb: u32,
    parallel_streams: usize,
    aggressive: bool,
) -> (TransferConfigBuilder, EffectiveTransferTuning) {
    let defaults = profile.defaults();
    let mut overrides = Vec::new();

    let block_size_mb = if block_size_mb == CLI_DEFAULT_BLOCK_SIZE_MB {
        defaults.block_size_mb
    } else {
        overrides.push("block-size");
        block_size_mb
    };

    let parallel_streams = if parallel_streams == CLI_DEFAULT_PARALLEL_STREAMS {
        defaults.parallel_streams
    } else {
        overrides.push("parallel");
        parallel_streams
    };

    let congestion = if aggressive {
        overrides.push("aggressive");
        CongestionMode::Aggressive
    } else {
        defaults.congestion
    };

    let tuning = EffectiveTransferTuning {
        profile,
        congestion,
        block_size_mb,
        parallel_streams,
        overrides,
    };

    let builder = builder
        .block_size_mb(tuning.block_size_mb)
        .max_parallel_streams(tuning.parallel_streams)
        .congestion(tuning.congestion)
        .adaptive(true);

    (builder, tuning)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_profile_changes_default_tuning() {
        let (_, tuning) = apply_transfer_profile(
            TransferConfigBuilder::default(),
            TransferProfile::Dev,
            CLI_DEFAULT_BLOCK_SIZE_MB,
            CLI_DEFAULT_PARALLEL_STREAMS,
            false,
        );
        assert_eq!(tuning.block_size_mb, 8);
        assert_eq!(tuning.parallel_streams, 8);
        assert_eq!(tuning.congestion, CongestionMode::Fair);
    }

    #[test]
    fn explicit_overrides_win() {
        let (_, tuning) =
            apply_transfer_profile(TransferConfigBuilder::default(), TransferProfile::Safe, 64, 24, true);
        assert_eq!(tuning.block_size_mb, 64);
        assert_eq!(tuning.parallel_streams, 24);
        assert_eq!(tuning.congestion, CongestionMode::Aggressive);
        assert_eq!(tuning.overrides, vec!["block-size", "parallel", "aggressive"]);
    }
}
