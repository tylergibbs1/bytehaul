use std::path::PathBuf;

use bytehaul_proto::congestion::CongestionMode;
use bytehaul_proto::engine::OverwriteMode;
use bytehaul_proto::manifest::DEFAULT_BLOCK_SIZE;

/// Configuration for a ByteHaul transfer.
#[derive(Debug, Clone)]
pub struct TransferConfig {
    pub resume: bool,
    pub congestion: CongestionMode,
    pub max_bandwidth_mbps: Option<u64>,
    pub block_size: u32,
    pub max_parallel_streams: usize,
    pub state_dir: Option<PathBuf>,
    pub overwrite_mode: OverwriteMode,
    pub delta: bool,
    pub encrypt_state: bool,
    pub compress: bool,
    pub compress_level: i32,
    pub adaptive: bool,
    pub fec_group_size: Option<usize>,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            resume: true,
            congestion: CongestionMode::Fair,
            max_bandwidth_mbps: None,
            block_size: DEFAULT_BLOCK_SIZE,
            max_parallel_streams: 16,
            state_dir: None,
            overwrite_mode: OverwriteMode::default(),
            delta: false,
            encrypt_state: false,
            compress: false,
            compress_level: bytehaul_proto::compress::DEFAULT_COMPRESSION_LEVEL,
            adaptive: false,
            fec_group_size: None,
        }
    }
}

impl TransferConfig {
    pub fn builder() -> TransferConfigBuilder {
        TransferConfigBuilder::default()
    }
}

/// Builder for TransferConfig.
#[derive(Debug, Clone, Default)]
pub struct TransferConfigBuilder {
    config: TransferConfig,
}

impl TransferConfigBuilder {
    pub fn resume(mut self, resume: bool) -> Self {
        self.config.resume = resume;
        self
    }

    pub fn congestion(mut self, mode: CongestionMode) -> Self {
        self.config.congestion = mode;
        self
    }

    pub fn max_bandwidth_mbps(mut self, mbps: u64) -> Self {
        self.config.max_bandwidth_mbps = Some(mbps);
        self
    }

    pub fn block_size(mut self, size: u32) -> Self {
        self.config.block_size = size;
        self
    }

    pub fn block_size_mb(mut self, mb: u32) -> Self {
        self.config.block_size = mb * 1024 * 1024;
        self
    }

    pub fn max_parallel_streams(mut self, n: usize) -> Self {
        self.config.max_parallel_streams = n;
        self
    }

    pub fn state_dir(mut self, dir: PathBuf) -> Self {
        self.config.state_dir = Some(dir);
        self
    }

    pub fn overwrite_mode(mut self, mode: OverwriteMode) -> Self {
        self.config.overwrite_mode = mode;
        self
    }

    pub fn delta(mut self, enabled: bool) -> Self {
        self.config.delta = enabled;
        self
    }

    pub fn encrypt_state(mut self, enabled: bool) -> Self {
        self.config.encrypt_state = enabled;
        self
    }

    pub fn compress(mut self, enabled: bool) -> Self {
        self.config.compress = enabled;
        self
    }

    pub fn compress_level(mut self, level: i32) -> Self {
        self.config.compress_level = level;
        self
    }

    pub fn adaptive(mut self, enabled: bool) -> Self {
        self.config.adaptive = enabled;
        self
    }

    pub fn fec_group_size(mut self, size: usize) -> Self {
        self.config.fec_group_size = Some(size);
        self
    }

    pub fn build(self) -> TransferConfig {
        self.config
    }
}
