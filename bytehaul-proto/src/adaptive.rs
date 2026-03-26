//! Adaptive transfer optimization based on observed network conditions.
//!
//! Implements research-backed heuristics:
//! - Auto-detect loss rate in first 2 seconds, switch BBR→Cubic if >0.1%
//!   (arxiv:2510.22461, arxiv:2603.12660)
//! - Recommend FEC group size based on measured loss
//!   (arxiv:1904.11326 QUIC-FEC)
//! - Enable GSO when available for reduced per-packet CPU overhead
//!   (arxiv:2310.09423 "QUIC is not Quick Enough")

use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::fec;
use crate::transport::CongestionAlgo;

/// Observed network conditions after a probe period.
#[derive(Debug, Clone)]
pub struct NetworkProfile {
    /// Estimated round-trip time.
    pub rtt: Duration,
    /// Estimated packet loss rate (0.0 to 1.0).
    pub loss_rate: f64,
    /// Estimated available bandwidth in bytes/sec.
    pub bandwidth_bps: u64,
}

/// Recommended transfer settings based on network profile.
#[derive(Debug, Clone)]
pub struct AdaptiveSettings {
    /// Which congestion algorithm to use.
    pub congestion_algo: CongestionAlgo,
    /// FEC group size (0 = disabled).
    pub fec_group_size: usize,
    /// Whether to enable GSO (Generic Segmentation Offload).
    pub enable_gso: bool,
    /// Recommended parallel stream count.
    pub parallel_streams: usize,
    /// Recommended block size in bytes.
    pub block_size: u32,
    /// Explanation of choices (for logging/display).
    pub rationale: Vec<String>,
}

/// Compute optimal transfer settings from observed network conditions.
///
/// This function encodes the findings from recent arxiv research:
///
/// 1. BBR vs Cubic: BBR is faster on clean links but collapses under loss.
///    Switch to Cubic at >0.1% loss (arxiv:2510.22461).
///
/// 2. FEC: Add forward erasure correction on lossy links to avoid
///    retransmission latency (arxiv:1904.11326).
///
/// 3. GSO: Enable Generic Segmentation Offload to reduce per-packet CPU
///    overhead, critical at high bandwidth (arxiv:2310.09423).
///
/// 4. Stream count: More streams help on high-BDP links but add overhead.
///    Scale with BDP.
///
/// 5. Block size: Larger blocks reduce per-chunk overhead. Scale with
///    bandwidth.
pub fn compute_adaptive_settings(profile: &NetworkProfile) -> AdaptiveSettings {
    let mut rationale = Vec::new();

    // ── Congestion algorithm ──
    let congestion_algo = if profile.loss_rate > 0.001 {
        rationale.push(format!(
            "Cubic selected: {:.1}% loss detected (BBR degrades above 0.1%)",
            profile.loss_rate * 100.0
        ));
        CongestionAlgo::Cubic
    } else {
        rationale.push("BBR selected: clean link (<0.1% loss)".into());
        CongestionAlgo::Bbr
    };

    // ── FEC ──
    let fec_group_size = fec::recommended_group_size(profile.loss_rate);
    if fec_group_size > 0 {
        let overhead = 100.0 / (fec_group_size as f64 + 1.0);
        rationale.push(format!(
            "FEC enabled: group_size={} ({:.1}% overhead) for {:.1}% loss",
            fec_group_size, overhead, profile.loss_rate * 100.0
        ));
    }

    // ── GSO ──
    // Enable GSO when bandwidth > 1 Gbps to reduce per-packet CPU cost.
    let enable_gso = profile.bandwidth_bps > 125_000_000; // > 1 Gbps
    if enable_gso {
        rationale.push("GSO enabled: high bandwidth link (>1 Gbps)".into());
    }

    // ── Parallel streams ──
    // BDP = bandwidth * RTT. Scale streams to fill the pipe.
    let bdp_bytes = (profile.bandwidth_bps as f64 * profile.rtt.as_secs_f64()) as u64;
    let parallel_streams = if bdp_bytes > 64 * 1024 * 1024 {
        64 // Very high BDP
    } else if bdp_bytes > 8 * 1024 * 1024 {
        32
    } else if bdp_bytes > 1024 * 1024 {
        16
    } else {
        8
    };
    rationale.push(format!(
        "Streams: {} (BDP ~{})",
        parallel_streams,
        humanize_bytes(bdp_bytes)
    ));

    // ── Block size ──
    // Larger blocks for higher bandwidth to reduce per-chunk overhead.
    let block_size = if profile.bandwidth_bps > 125_000_000 {
        64 * 1024 * 1024 // 64 MB for >1 Gbps
    } else if profile.bandwidth_bps > 12_500_000 {
        16 * 1024 * 1024 // 16 MB for >100 Mbps
    } else {
        4 * 1024 * 1024 // 4 MB for slower links
    };
    rationale.push(format!("Block size: {} MB", block_size / (1024 * 1024)));

    AdaptiveSettings {
        congestion_algo,
        fec_group_size,
        enable_gso,
        parallel_streams,
        block_size,
        rationale,
    }
}

/// Estimate network conditions from a Quinn connection's stats.
///
/// Call this after the first few seconds of a transfer to get a profile.
pub fn profile_from_quinn_stats(
    rtt: Duration,
    lost_packets: u64,
    sent_packets: u64,
    bytes_sent: u64,
    elapsed: Duration,
) -> NetworkProfile {
    let loss_rate = if sent_packets > 0 {
        lost_packets as f64 / sent_packets as f64
    } else {
        0.0
    };

    let bandwidth_bps = if elapsed.as_secs_f64() > 0.0 {
        (bytes_sent as f64 / elapsed.as_secs_f64()) as u64
    } else {
        0
    };

    let profile = NetworkProfile {
        rtt,
        loss_rate,
        bandwidth_bps,
    };

    info!(
        "Network profile: RTT={:?}, loss={:.2}%, bandwidth={}/s",
        rtt,
        loss_rate * 100.0,
        humanize_bytes(bandwidth_bps)
    );

    profile
}

fn humanize_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_link_uses_bbr() {
        let profile = NetworkProfile {
            rtt: Duration::from_millis(50),
            loss_rate: 0.0,
            bandwidth_bps: 125_000_000, // 1 Gbps
        };
        let settings = compute_adaptive_settings(&profile);
        assert_eq!(settings.congestion_algo, CongestionAlgo::Bbr);
        assert_eq!(settings.fec_group_size, 0);
    }

    #[test]
    fn lossy_link_uses_cubic_with_fec() {
        let profile = NetworkProfile {
            rtt: Duration::from_millis(100),
            loss_rate: 0.02, // 2% loss
            bandwidth_bps: 12_500_000, // 100 Mbps
        };
        let settings = compute_adaptive_settings(&profile);
        assert_eq!(settings.congestion_algo, CongestionAlgo::Cubic);
        assert_eq!(settings.fec_group_size, 7); // ~12.5% overhead for 2% loss
    }

    #[test]
    fn high_loss_uses_aggressive_fec() {
        let profile = NetworkProfile {
            rtt: Duration::from_millis(200),
            loss_rate: 0.1, // 10% loss
            bandwidth_bps: 1_250_000, // 10 Mbps
        };
        let settings = compute_adaptive_settings(&profile);
        assert_eq!(settings.congestion_algo, CongestionAlgo::Cubic);
        assert_eq!(settings.fec_group_size, 3); // 25% overhead for 10% loss
    }

    #[test]
    fn high_bandwidth_enables_gso() {
        let profile = NetworkProfile {
            rtt: Duration::from_millis(1),
            loss_rate: 0.0,
            bandwidth_bps: 1_250_000_000, // 10 Gbps
        };
        let settings = compute_adaptive_settings(&profile);
        assert!(settings.enable_gso);
        assert_eq!(settings.block_size, 64 * 1024 * 1024); // 64 MB blocks
    }

    #[test]
    fn low_bandwidth_conservative_settings() {
        let profile = NetworkProfile {
            rtt: Duration::from_millis(300),
            loss_rate: 0.005, // 0.5%
            bandwidth_bps: 625_000, // 5 Mbps
        };
        let settings = compute_adaptive_settings(&profile);
        assert_eq!(settings.congestion_algo, CongestionAlgo::Cubic);
        assert_eq!(settings.fec_group_size, 15);
        assert!(!settings.enable_gso);
        assert_eq!(settings.block_size, 4 * 1024 * 1024); // 4 MB
        assert_eq!(settings.parallel_streams, 8);
    }

    #[test]
    fn profile_from_stats() {
        let profile = profile_from_quinn_stats(
            Duration::from_millis(85),
            10,
            1000,
            50_000_000,
            Duration::from_secs(2),
        );
        assert_eq!(profile.rtt, Duration::from_millis(85));
        assert!((profile.loss_rate - 0.01).abs() < 0.001);
        assert_eq!(profile.bandwidth_bps, 25_000_000);
    }
}
