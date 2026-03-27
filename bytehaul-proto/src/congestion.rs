//! Congestion control and bandwidth management for ByteHaul.
//!
//! Rather than implementing Quinn's `Controller` trait directly (which is
//! version-sensitive and complex), we take a **wrapper approach**:
//!
//! - [`CongestionConfig`] tunes Quinn's built-in transport parameters for two
//!   operating modes: [`CongestionMode::Fair`] and [`CongestionMode::Aggressive`].
//! - [`BandwidthLimiter`] provides an application-level token-bucket rate
//!   limiter for `--max-rate` support.
//! - [`ByteHaulCongestion`] tracks our own view of the path for diagnostics.
//! - [`LinkStats`] exposes observability counters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::VarInt;
use tokio::time::sleep;
use tracing::{debug, info, instrument, trace, warn};

// ---------------------------------------------------------------------------
// CongestionMode
// ---------------------------------------------------------------------------

/// Operating mode for ByteHaul's congestion strategy.
///
/// * **Fair** -- plays nicely with competing traffic; uses Quinn defaults.
/// * **Aggressive** -- saturates available bandwidth.  Only appropriate on
///   dedicated links where ByteHaul is the sole consumer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CongestionMode {
    /// Use Quinn's default congestion parameters (BBR/Cubic).
    #[default]
    Fair,
    /// Enlarge windows and stream limits to saturate the link.
    /// **Only use on dedicated bandwidth.**
    Aggressive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LossClass {
    Benign,
    Random,
    RecoverableBurst,
    Congestion,
}

impl std::fmt::Display for CongestionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CongestionMode::Fair => write!(f, "fair"),
            CongestionMode::Aggressive => write!(f, "aggressive"),
        }
    }
}

// ---------------------------------------------------------------------------
// CongestionConfig -- configures Quinn TransportConfig
// ---------------------------------------------------------------------------

/// Knobs applied to a [`quinn::TransportConfig`] depending on the chosen
/// [`CongestionMode`].
#[derive(Debug, Clone)]
pub struct CongestionConfig {
    // -- Aggressive-mode overrides (Fair uses Quinn defaults for all of these)
    /// Initial congestion window hint expressed as number of MTU-sized packets.
    /// Quinn's default is 10; aggressive raises this significantly.
    pub aggressive_initial_window_packets: u32,

    /// Send window in bytes for aggressive mode (connection-level).
    pub aggressive_send_window: u64,

    /// Receive window in bytes for aggressive mode (connection-level).
    pub aggressive_receive_window: u64,

    /// Per-stream receive window for aggressive mode.
    pub aggressive_stream_receive_window: u64,

    /// Maximum concurrent bidirectional streams for aggressive mode.
    pub aggressive_max_bidi_streams: u64,

    /// Maximum concurrent unidirectional streams for aggressive mode.
    pub aggressive_max_uni_streams: u64,

    /// Initial path MTU to try (both modes).
    pub initial_mtu: u16,

    /// Minimum MTU that must be supported (both modes).
    pub min_mtu: u16,
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            aggressive_initial_window_packets: 100,
            // 64 MiB send / receive windows -- plenty for high-BDP paths.
            aggressive_send_window: 64 * 1024 * 1024,
            aggressive_receive_window: 64 * 1024 * 1024,
            aggressive_stream_receive_window: 32 * 1024 * 1024,
            aggressive_max_bidi_streams: 256,
            aggressive_max_uni_streams: 256,
            // Default to standard Ethernet jumbo-capable MTU probing.
            initial_mtu: 1200,
            min_mtu: 1200,
        }
    }
}

impl CongestionConfig {
    /// Create a new configuration with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply settings to a [`quinn::TransportConfig`] according to `mode`.
    ///
    /// For [`CongestionMode::Fair`] we leave Quinn's defaults alone (aside from
    /// MTU hints). For [`CongestionMode::Aggressive`] we open the floodgates.
    #[instrument(skip(self, transport), fields(%mode))]
    pub fn apply_to_transport(
        &self,
        transport: &mut quinn::TransportConfig,
        mode: CongestionMode,
    ) {
        // MTU settings apply regardless of mode.
        transport.initial_mtu(self.initial_mtu);
        transport.min_mtu(self.min_mtu);

        match mode {
            CongestionMode::Fair => {
                info!("congestion mode: fair -- using Quinn defaults");
            }
            CongestionMode::Aggressive => {
                info!(
                    send_window = self.aggressive_send_window,
                    receive_window = self.aggressive_receive_window,
                    stream_receive_window = self.aggressive_stream_receive_window,
                    max_bidi = self.aggressive_max_bidi_streams,
                    max_uni = self.aggressive_max_uni_streams,
                    "congestion mode: aggressive -- enlarging windows"
                );

                transport.send_window(self.aggressive_send_window);

                // VarInt is capped at 2^62 - 1; saturate if somehow exceeded.
                transport.receive_window(
                    VarInt::from_u64(self.aggressive_receive_window)
                        .unwrap_or(VarInt::MAX),
                );
                transport.stream_receive_window(
                    VarInt::from_u64(self.aggressive_stream_receive_window)
                        .unwrap_or(VarInt::MAX),
                );

                transport.max_concurrent_bidi_streams(
                    VarInt::from_u64(self.aggressive_max_bidi_streams)
                        .unwrap_or(VarInt::MAX),
                );
                transport.max_concurrent_uni_streams(
                    VarInt::from_u64(self.aggressive_max_uni_streams)
                        .unwrap_or(VarInt::MAX),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ByteHaulCongestion -- our own path state tracker
// ---------------------------------------------------------------------------

/// Internal bookkeeping for ByteHaul's view of the network path.
///
/// This is **not** a Quinn `Controller` implementation.  It mirrors state that
/// we derive from Quinn's connection stats plus our own observations and is
/// used for logging, heuristics, and mode-switch decisions.
#[derive(Debug)]
pub struct ByteHaulCongestion {
    mode: CongestionMode,
    /// Current congestion window (bytes), mirrored from Quinn stats.
    current_window: u64,
    /// Maximum window we allow (e.g. based on `--max-rate` or link cap).
    max_window: u64,
    /// Minimum observed RTT on this path.
    min_rtt: Duration,
    /// EWMA-smoothed RTT.
    smoothed_rtt: Duration,
    /// Bytes currently in flight (application view).
    bytes_in_flight: u64,
    /// Slow-start threshold.
    ssthresh: u64,
    /// Most recent loss classification.
    last_loss_class: LossClass,
}

impl ByteHaulCongestion {
    /// Create a new tracker for the given mode.
    pub fn new(mode: CongestionMode) -> Self {
        let max_window = match mode {
            CongestionMode::Fair => 16 * 1024 * 1024,       // 16 MiB
            CongestionMode::Aggressive => 256 * 1024 * 1024, // 256 MiB (match QUIC flow control)
        };
        Self {
            mode,
            current_window: 64 * 1024, // 64 KiB initial
            max_window,
            min_rtt: Duration::from_secs(u64::MAX),
            smoothed_rtt: Duration::ZERO,
            bytes_in_flight: 0,
            ssthresh: u64::MAX,
            last_loss_class: LossClass::Benign,
        }
    }

    // -- Accessors -----------------------------------------------------------

    pub fn mode(&self) -> CongestionMode {
        self.mode
    }

    pub fn current_window(&self) -> u64 {
        self.current_window
    }

    pub fn max_window(&self) -> u64 {
        self.max_window
    }

    pub fn min_rtt(&self) -> Duration {
        self.min_rtt
    }

    pub fn smoothed_rtt(&self) -> Duration {
        self.smoothed_rtt
    }

    pub fn bytes_in_flight(&self) -> u64 {
        self.bytes_in_flight
    }

    pub fn ssthresh(&self) -> u64 {
        self.ssthresh
    }

    pub fn last_loss_class(&self) -> LossClass {
        self.last_loss_class
    }

    // -- Mutations ------------------------------------------------------------

    /// Switch the operating mode at runtime.
    pub fn set_mode(&mut self, mode: CongestionMode) {
        if self.mode != mode {
            info!(
                old = %self.mode,
                new = %mode,
                "switching congestion mode"
            );
            self.mode = mode;
            // Adjust the ceiling when switching modes.
            self.max_window = match mode {
                CongestionMode::Fair => 16 * 1024 * 1024,
                CongestionMode::Aggressive => 256 * 1024 * 1024,
            };
        }
    }

    /// Override the maximum window (e.g. derived from `--max-rate`).
    pub fn set_max_window(&mut self, max: u64) {
        self.max_window = max;
    }

    /// Record that `bytes` were sent.
    pub fn on_sent(&mut self, bytes: u64) {
        self.bytes_in_flight = self.bytes_in_flight.saturating_add(bytes);
        trace!(bytes, in_flight = self.bytes_in_flight, "sent");
    }

    /// Record that `bytes` were acknowledged, and update RTT estimates.
    pub fn on_ack(&mut self, bytes: u64, rtt: Duration) {
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(bytes);

        // Update min RTT.
        if rtt < self.min_rtt {
            self.min_rtt = rtt;
        }

        // EWMA smoothed RTT (weight = 7/8 old, 1/8 new).
        if self.smoothed_rtt == Duration::ZERO {
            self.smoothed_rtt = rtt;
        } else {
            self.smoothed_rtt = self.smoothed_rtt.mul_f64(0.875) + rtt.mul_f64(0.125);
        }

        // Simple AIMD-ish window growth (this is for our bookkeeping only;
        // Quinn's real controller does the heavy lifting).
        if self.current_window < self.ssthresh {
            // Slow start: double per RTT.
            self.current_window = (self.current_window + bytes).min(self.max_window);
        } else {
            // Congestion avoidance: additive increase.
            let increment = (1460u64 * 1460) / self.current_window.max(1);
            self.current_window =
                (self.current_window + increment).min(self.max_window);
        }

        trace!(
            bytes,
            in_flight = self.bytes_in_flight,
            window = self.current_window,
            ?rtt,
            "ack"
        );
    }

    pub fn classify_loss(
        &self,
        loss_rate: f64,
        burst_losses: u64,
        current_rtt: Duration,
    ) -> LossClass {
        if loss_rate < 0.0005 && burst_losses == 0 {
            return LossClass::Benign;
        }

        let baseline = if self.min_rtt == Duration::from_secs(u64::MAX) || self.min_rtt.is_zero() {
            current_rtt
        } else {
            self.min_rtt
        };
        let inflation = if baseline.is_zero() {
            1.0
        } else {
            current_rtt.as_secs_f64() / baseline.as_secs_f64()
        };

        if inflation < 1.1 && loss_rate < 0.01 {
            return LossClass::Random;
        }

        if inflation < 1.25 && burst_losses <= 3 {
            return LossClass::RecoverableBurst;
        }

        LossClass::Congestion
    }

    /// Record a loss event and adapt the bookkeeping window based on the
    /// inferred loss class.
    pub fn on_loss_event(&mut self, lost_bytes: u64, class: LossClass) {
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(lost_bytes);
        self.last_loss_class = class;

        if matches!(class, LossClass::Benign) {
            trace!(lost_bytes, "benign loss event");
            return;
        }

        let factor = match class {
            LossClass::Benign => 1.0,
            LossClass::Random => match self.mode {
                CongestionMode::Fair => 0.9,
                CongestionMode::Aggressive => 0.94,
            },
            LossClass::RecoverableBurst => match self.mode {
                CongestionMode::Fair => 0.8,
                CongestionMode::Aggressive => 0.88,
            },
            LossClass::Congestion => match self.mode {
                CongestionMode::Fair => 0.5,
                CongestionMode::Aggressive => 0.7,
            },
        };

        self.ssthresh = ((self.current_window as f64) * factor) as u64;
        self.ssthresh = self.ssthresh.max(2 * 1460); // floor at 2 segments
        self.current_window = self.ssthresh;

        debug!(
            lost_bytes,
            ?class,
            window = self.current_window,
            ssthresh = self.ssthresh,
            "loss event"
        );
    }

    /// Backwards-compatible helper for callers that only know a loss occurred.
    pub fn on_congestion_event(&mut self, lost_bytes: u64) {
        self.on_loss_event(lost_bytes, LossClass::Congestion);
    }

    /// Update the congestion window to mirror Quinn's reported value.
    pub fn sync_window(&mut self, quinn_window: u64) {
        self.current_window = quinn_window.min(self.max_window);
    }
}

// ---------------------------------------------------------------------------
// BandwidthLimiter -- application-level token bucket for --max-rate
// ---------------------------------------------------------------------------

/// A token-bucket rate limiter that caps outgoing throughput.
///
/// Intended for use with the `--max-rate` CLI flag.  Call
/// [`wait_if_needed`](Self::wait_if_needed) before each send to throttle.
#[derive(Debug)]
pub struct BandwidthLimiter {
    /// Maximum bytes per second (0 = unlimited).
    max_bytes_per_sec: Arc<AtomicU64>,
    /// Tokens currently available (bytes we are allowed to send right now).
    tokens: f64,
    /// Timestamp of the last token refill.
    last_refill: Instant,
}

impl BandwidthLimiter {
    /// Create a new limiter.  Pass `0` to disable rate limiting.
    pub fn new(max_bytes_per_sec: u64) -> Self {
        Self {
            max_bytes_per_sec: Arc::new(AtomicU64::new(max_bytes_per_sec)),
            tokens: max_bytes_per_sec as f64,
            last_refill: Instant::now(),
        }
    }

    /// Dynamically update the maximum rate.
    pub fn update_rate(&self, new_max: u64) {
        info!(new_max, "updating bandwidth limit");
        self.max_bytes_per_sec.store(new_max, Ordering::Relaxed);
    }

    /// Current configured rate (bytes/sec).  Returns 0 when unlimited.
    pub fn rate(&self) -> u64 {
        self.max_bytes_per_sec.load(Ordering::Relaxed)
    }

    /// Wait (asynchronously) until we are allowed to send `bytes_to_send`.
    ///
    /// If the limiter is disabled (rate == 0) this returns immediately.
    pub async fn wait_if_needed(&mut self, bytes_to_send: u64) {
        let rate = self.max_bytes_per_sec.load(Ordering::Relaxed);
        if rate == 0 {
            return;
        }

        // Refill tokens based on elapsed time.
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        self.tokens += elapsed.as_secs_f64() * rate as f64;
        // Cap tokens at 1 second's worth (burst bucket).
        self.tokens = self.tokens.min(rate as f64);
        self.last_refill = now;

        let needed = bytes_to_send as f64;
        if self.tokens >= needed {
            self.tokens -= needed;
            return;
        }

        // Not enough tokens -- sleep until they accumulate.
        let deficit = needed - self.tokens;
        let wait_secs = deficit / rate as f64;
        trace!(
            deficit,
            wait_ms = wait_secs * 1000.0,
            "bandwidth limiter: throttling"
        );
        sleep(Duration::from_secs_f64(wait_secs)).await;

        // After sleeping, consume all needed tokens (they have accrued).
        self.tokens = 0.0;
        self.last_refill = Instant::now();
    }
}

// ---------------------------------------------------------------------------
// LinkStats -- observability
// ---------------------------------------------------------------------------

/// Snapshot of link-level statistics for observability and diagnostics.
#[derive(Debug, Clone)]
pub struct LinkStats {
    /// Estimated available bandwidth in bytes/sec.
    pub estimated_bandwidth: u64,
    /// Current round-trip time.
    pub rtt: Duration,
    /// Packet loss rate as a fraction (0.0 .. 1.0).
    pub loss_rate: f64,
    /// Total bytes sent on this path.
    pub bytes_sent: u64,
    /// Total bytes acknowledged on this path.
    pub bytes_acked: u64,
}

impl LinkStats {
    /// Create a zeroed stats snapshot.
    pub fn new() -> Self {
        Self {
            estimated_bandwidth: 0,
            rtt: Duration::ZERO,
            loss_rate: 0.0,
            bytes_sent: 0,
            bytes_acked: 0,
        }
    }

    /// Compute loss rate from sent/acked counters.  Returns 0.0 when no data
    /// has been sent.
    pub fn compute_loss_rate(&self) -> f64 {
        if self.bytes_sent == 0 {
            return 0.0;
        }
        let lost = self.bytes_sent.saturating_sub(self.bytes_acked);
        lost as f64 / self.bytes_sent as f64
    }

    /// Update the snapshot from a Quinn [`quinn::ConnectionStats`] and our own
    /// congestion tracker.
    pub fn update_from(
        &mut self,
        quinn_stats: &quinn::ConnectionStats,
        congestion: &ByteHaulCongestion,
    ) {
        // Quinn exposes path-level stats via `ConnectionStats::path`.
        let path = &quinn_stats.path;
        self.rtt = path.rtt;

        // PathStats doesn't track sent_bytes directly; use UDP-level stats.
        self.bytes_sent = quinn_stats.udp_tx.bytes;

        // Derive acknowledged bytes as sent minus lost.
        self.bytes_acked = quinn_stats.udp_tx.bytes.saturating_sub(path.lost_bytes);
        self.loss_rate = self.compute_loss_rate();

        // BDP-based bandwidth estimate: window / RTT.
        if congestion.smoothed_rtt() > Duration::ZERO {
            self.estimated_bandwidth = (congestion.current_window() as f64
                / congestion.smoothed_rtt().as_secs_f64())
                as u64;
        }
    }
}

impl Default for LinkStats {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mode_display() {
        assert_eq!(CongestionMode::Fair.to_string(), "fair");
        assert_eq!(CongestionMode::Aggressive.to_string(), "aggressive");
    }

    #[test]
    fn test_congestion_tracker_basics() {
        let mut cc = ByteHaulCongestion::new(CongestionMode::Fair);
        assert_eq!(cc.mode(), CongestionMode::Fair);

        cc.on_sent(1000);
        assert_eq!(cc.bytes_in_flight(), 1000);

        cc.on_ack(500, Duration::from_millis(20));
        assert_eq!(cc.bytes_in_flight(), 500);
        assert_eq!(cc.min_rtt(), Duration::from_millis(20));
    }

    #[test]
    fn test_mode_switch() {
        let mut cc = ByteHaulCongestion::new(CongestionMode::Fair);
        let fair_max = cc.max_window();

        cc.set_mode(CongestionMode::Aggressive);
        assert_eq!(cc.mode(), CongestionMode::Aggressive);
        assert!(cc.max_window() > fair_max);

        cc.set_mode(CongestionMode::Fair);
        assert_eq!(cc.max_window(), fair_max);
    }

    #[test]
    fn test_congestion_event_reduces_window() {
        let mut cc = ByteHaulCongestion::new(CongestionMode::Fair);
        // Grow the window a bit.
        for _ in 0..100 {
            cc.on_ack(1460, Duration::from_millis(10));
        }
        let before = cc.current_window();

        cc.on_congestion_event(5000);
        assert!(cc.current_window() < before);
    }

    #[test]
    fn test_loss_classifier_prefers_recoverable_burst_without_rtt_inflation() {
        let mut cc = ByteHaulCongestion::new(CongestionMode::Fair);
        cc.on_ack(1500, Duration::from_millis(40));
        let class = cc.classify_loss(0.01, 2, Duration::from_millis(44));
        assert_eq!(class, LossClass::RecoverableBurst);
    }

    #[test]
    fn test_loss_classifier_marks_inflated_rtt_as_congestion() {
        let mut cc = ByteHaulCongestion::new(CongestionMode::Fair);
        cc.on_ack(1500, Duration::from_millis(40));
        let class = cc.classify_loss(0.02, 6, Duration::from_millis(90));
        assert_eq!(class, LossClass::Congestion);
    }

    #[test]
    fn test_random_loss_backs_off_less_than_congestion() {
        let mut random = ByteHaulCongestion::new(CongestionMode::Fair);
        let mut congested = ByteHaulCongestion::new(CongestionMode::Fair);
        random.sync_window(1_000_000);
        congested.sync_window(1_000_000);

        random.on_loss_event(1000, LossClass::Random);
        congested.on_loss_event(1000, LossClass::Congestion);

        assert!(random.current_window() > congested.current_window());
    }

    #[test]
    fn test_aggressive_backs_off_less() {
        let mut fair = ByteHaulCongestion::new(CongestionMode::Fair);
        let mut aggr = ByteHaulCongestion::new(CongestionMode::Aggressive);

        // Give both the same starting window.
        let start_window = 1_000_000u64;
        fair.sync_window(start_window);
        aggr.sync_window(start_window);
        // Also set ssthresh high so the event logic uses current_window.
        fair.ssthresh = u64::MAX;
        aggr.ssthresh = u64::MAX;

        fair.on_congestion_event(1000);
        aggr.on_congestion_event(1000);

        // Aggressive should retain a larger window after the event.
        assert!(aggr.current_window() > fair.current_window());
    }

    #[tokio::test]
    async fn test_bandwidth_limiter_unlimited() {
        let mut limiter = BandwidthLimiter::new(0);
        // Should return instantly -- no throttling.
        limiter.wait_if_needed(1_000_000).await;
    }

    #[test]
    fn test_bandwidth_limiter_update_rate() {
        let limiter = BandwidthLimiter::new(1000);
        assert_eq!(limiter.rate(), 1000);
        limiter.update_rate(5000);
        assert_eq!(limiter.rate(), 5000);
    }

    #[test]
    fn test_link_stats_loss_rate() {
        let mut stats = LinkStats::new();
        stats.bytes_sent = 1000;
        stats.bytes_acked = 900;
        let loss = stats.compute_loss_rate();
        assert!((loss - 0.1).abs() < 1e-9);
    }

    #[test]
    fn test_link_stats_no_division_by_zero() {
        let stats = LinkStats::new();
        assert_eq!(stats.compute_loss_rate(), 0.0);
    }

    #[test]
    fn test_congestion_config_default() {
        let cfg = CongestionConfig::default();
        assert_eq!(cfg.aggressive_send_window, 64 * 1024 * 1024);
        assert_eq!(cfg.initial_mtu, 1200);
    }
}
