//! Forward Erasure Correction (FEC) for ByteHaul data streams.
//!
//! Based on the QUIC-FEC research (arxiv:1904.11326), this module adds
//! lightweight XOR-based redundancy to chunk transmissions so the receiver
//! can reconstruct lost packets without waiting for retransmission.
//!
//! This is especially effective on lossy links (satellite, cellular, WiFi)
//! where QUIC's retransmission-based recovery adds RTT-scale delays.
//!
//! # How it works
//!
//! For every N data chunks, the sender generates 1 parity chunk using XOR
//! across all N chunks. If any single chunk in the group is lost, the
//! receiver can reconstruct it from the parity + remaining chunks.
//!
//! This provides (N+1)/N coding rate — e.g., for N=7, sending 8 chunks
//! for every 7 data chunks gives ~12.5% overhead but recovers any single
//! loss in the group without retransmission.

use tracing::debug;

/// FEC configuration.
#[derive(Debug, Clone, Copy)]
pub struct FecConfig {
    /// Number of data chunks per FEC group. A parity chunk is generated
    /// for every `group_size` data chunks.
    /// Set to 0 to disable FEC.
    pub group_size: usize,
}

impl Default for FecConfig {
    fn default() -> Self {
        Self { group_size: 0 } // Disabled by default
    }
}

impl FecConfig {
    /// Create a config that generates 1 parity per N data chunks.
    /// Recommended: 7 for ~12.5% overhead, 15 for ~6.25% overhead.
    pub fn with_group_size(n: usize) -> Self {
        Self { group_size: n }
    }

    pub fn is_enabled(&self) -> bool {
        self.group_size > 0
    }

    /// Overhead ratio (e.g., 0.125 for group_size=8).
    pub fn overhead_ratio(&self) -> f64 {
        if self.group_size == 0 {
            0.0
        } else {
            1.0 / (self.group_size as f64 + 1.0)
        }
    }
}

/// XOR-based parity encoder for a group of chunks.
#[derive(Debug)]
pub struct FecEncoder {
    config: FecConfig,
    /// Accumulated XOR parity for the current group.
    parity: Vec<u8>,
    /// Number of chunks processed in the current group.
    count: usize,
    /// Maximum chunk size seen in the current group.
    max_size: usize,
}

impl FecEncoder {
    pub fn new(config: FecConfig) -> Self {
        Self {
            config,
            parity: Vec::new(),
            count: 0,
            max_size: 0,
        }
    }

    /// Feed a data chunk into the encoder. Returns `Some(parity)` when
    /// a complete group has been processed and a parity chunk is ready.
    pub fn feed(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        if !self.config.is_enabled() {
            return None;
        }

        // Grow parity buffer if needed
        if data.len() > self.parity.len() {
            self.parity.resize(data.len(), 0);
        }
        if data.len() > self.max_size {
            self.max_size = data.len();
        }

        // XOR data into parity
        for (i, &byte) in data.iter().enumerate() {
            self.parity[i] ^= byte;
        }

        self.count += 1;

        if self.count >= self.config.group_size {
            let parity = self.parity[..self.max_size].to_vec();
            self.reset();
            debug!(
                "FEC: generated parity for group of {} chunks ({} bytes)",
                self.config.group_size,
                parity.len()
            );
            Some(parity)
        } else {
            None
        }
    }

    /// Flush any partial group (returns parity even if group is incomplete).
    pub fn flush(&mut self) -> Option<Vec<u8>> {
        if self.count > 0 && self.config.is_enabled() {
            let parity = self.parity[..self.max_size].to_vec();
            self.reset();
            Some(parity)
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.parity.fill(0);
        self.count = 0;
        self.max_size = 0;
    }
}

/// XOR-based parity decoder that can recover a single lost chunk.
#[derive(Debug)]
pub struct FecDecoder {
    /// Chunks received in the current group (None = lost).
    chunks: Vec<Option<Vec<u8>>>,
    /// Parity chunk for the current group.
    parity: Option<Vec<u8>>,
    /// Group size.
    group_size: usize,
}

impl FecDecoder {
    pub fn new(group_size: usize) -> Self {
        Self {
            chunks: vec![None; group_size],
            parity: None,
            group_size,
        }
    }

    /// Record a received chunk at the given position within the group.
    pub fn receive_chunk(&mut self, position: usize, data: Vec<u8>) {
        if position < self.group_size {
            self.chunks[position] = Some(data);
        }
    }

    /// Record the parity chunk.
    pub fn receive_parity(&mut self, data: Vec<u8>) {
        self.parity = Some(data);
    }

    /// Try to recover a missing chunk. Returns `Some((position, data))`
    /// if exactly one chunk is missing and can be recovered.
    pub fn try_recover(&self) -> Option<(usize, Vec<u8>)> {
        let missing: Vec<usize> = self
            .chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_none())
            .map(|(i, _)| i)
            .collect();

        // Can only recover exactly 1 missing chunk
        if missing.len() != 1 {
            return None;
        }

        let parity = self.parity.as_ref()?;
        let missing_idx = missing[0];

        // Recovered = parity XOR all other chunks
        let mut recovered = parity.clone();
        for (i, chunk) in self.chunks.iter().enumerate() {
            if i == missing_idx {
                continue;
            }
            if let Some(data) = chunk {
                for (j, &byte) in data.iter().enumerate() {
                    if j < recovered.len() {
                        recovered[j] ^= byte;
                    }
                }
            }
        }

        debug!("FEC: recovered chunk {} from parity", missing_idx);
        Some((missing_idx, recovered))
    }

    /// Reset for the next group.
    pub fn reset(&mut self) {
        for c in &mut self.chunks {
            *c = None;
        }
        self.parity = None;
    }
}

/// Estimate recommended FEC group size based on observed loss rate.
///
/// - 0% loss: FEC disabled (group_size = 0)
/// - 0.1-1% loss: group_size = 15 (~6% overhead)
/// - 1-5% loss: group_size = 7 (~12.5% overhead)
/// - >5% loss: group_size = 3 (~25% overhead)
pub fn recommended_group_size(loss_rate: f64) -> usize {
    if loss_rate < 0.001 {
        0 // No FEC needed
    } else if loss_rate < 0.01 {
        15
    } else if loss_rate < 0.05 {
        7
    } else {
        3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fec_disabled_by_default() {
        let config = FecConfig::default();
        assert!(!config.is_enabled());
        assert_eq!(config.overhead_ratio(), 0.0);
    }

    #[test]
    fn encoder_produces_parity_at_group_boundary() {
        let mut enc = FecEncoder::new(FecConfig::with_group_size(3));

        assert!(enc.feed(&[0xAA, 0xBB]).is_none());
        assert!(enc.feed(&[0xCC, 0xDD]).is_none());
        let parity = enc.feed(&[0xEE, 0xFF]).expect("should produce parity");

        // XOR: 0xAA ^ 0xCC ^ 0xEE = 0x88, 0xBB ^ 0xDD ^ 0xFF = 0x99 (actually need to compute)
        assert_eq!(parity.len(), 2);
        assert_eq!(parity[0], 0xAA ^ 0xCC ^ 0xEE);
        assert_eq!(parity[1], 0xBB ^ 0xDD ^ 0xFF);
    }

    #[test]
    fn decoder_recovers_single_lost_chunk() {
        let chunks = vec![
            vec![0xAA, 0xBB],
            vec![0xCC, 0xDD],
            vec![0xEE, 0xFF],
        ];

        // Generate parity
        let mut enc = FecEncoder::new(FecConfig::with_group_size(3));
        enc.feed(&chunks[0]);
        enc.feed(&chunks[1]);
        let parity = enc.feed(&chunks[2]).unwrap();

        // Simulate losing chunk 1
        let mut dec = FecDecoder::new(3);
        dec.receive_chunk(0, chunks[0].clone());
        // chunk 1 is lost
        dec.receive_chunk(2, chunks[2].clone());
        dec.receive_parity(parity);

        let (idx, recovered) = dec.try_recover().expect("should recover");
        assert_eq!(idx, 1);
        assert_eq!(recovered, chunks[1]);
    }

    #[test]
    fn decoder_cannot_recover_two_losses() {
        let mut enc = FecEncoder::new(FecConfig::with_group_size(3));
        enc.feed(&[1, 2]);
        enc.feed(&[3, 4]);
        let parity = enc.feed(&[5, 6]).unwrap();

        let mut dec = FecDecoder::new(3);
        dec.receive_chunk(0, vec![1, 2]);
        // chunks 1 and 2 lost
        dec.receive_parity(parity);

        assert!(dec.try_recover().is_none());
    }

    #[test]
    fn flush_partial_group() {
        let mut enc = FecEncoder::new(FecConfig::with_group_size(5));
        enc.feed(&[0xAA]);
        enc.feed(&[0xBB]);
        let parity = enc.flush().expect("should flush partial");
        assert_eq!(parity[0], 0xAA ^ 0xBB);
    }

    #[test]
    fn recommended_sizes() {
        assert_eq!(recommended_group_size(0.0), 0);
        assert_eq!(recommended_group_size(0.005), 15);
        assert_eq!(recommended_group_size(0.02), 7);
        assert_eq!(recommended_group_size(0.1), 3);
    }

    #[test]
    fn encoder_handles_variable_chunk_sizes() {
        let mut enc = FecEncoder::new(FecConfig::with_group_size(2));
        enc.feed(&[0xAA, 0xBB, 0xCC]); // 3 bytes
        let parity = enc.feed(&[0xDD, 0xEE]).unwrap(); // 2 bytes (shorter)

        // Parity should be 3 bytes (max size)
        assert_eq!(parity.len(), 3);
        assert_eq!(parity[0], 0xAA ^ 0xDD);
        assert_eq!(parity[1], 0xBB ^ 0xEE);
        assert_eq!(parity[2], 0xCC ^ 0x00); // XOR with implicit 0 padding
    }
}
