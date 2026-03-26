use std::path::Path;

use bitvec::prelude::*;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

#[derive(Error, Debug)]
pub enum ChunkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid chunk index {index}, total chunks: {total}")]
    InvalidIndex { index: u64, total: u64 },
    #[error("Invalid block size: {0}")]
    InvalidBlockSize(u32),
}

/// Metadata about a single chunk.
#[derive(Debug, Clone, Copy)]
pub struct ChunkMeta {
    pub index: u64,
    pub offset: u64,
    pub size: u32,
}

/// Describes how a file is divided into fixed-size chunks.
#[derive(Debug, Clone, Copy)]
pub struct ChunkPlan {
    pub file_size: u64,
    pub block_size: u32,
    total_chunks: u64,
}

impl ChunkPlan {
    pub fn new(file_size: u64, block_size: u32) -> Self {
        let total_chunks = if file_size == 0 {
            0
        } else {
            file_size.div_ceil(block_size as u64)
        };
        Self {
            file_size,
            block_size,
            total_chunks,
        }
    }

    pub fn total_chunks(&self) -> u64 {
        self.total_chunks
    }

    /// Get metadata for a specific chunk index.
    pub fn chunk_meta(&self, index: u64) -> Option<ChunkMeta> {
        if index >= self.total_chunks {
            return None;
        }
        let offset = index * self.block_size as u64;
        let remaining = self.file_size - offset;
        let size = remaining.min(self.block_size as u64) as u32;
        Some(ChunkMeta {
            index,
            offset,
            size,
        })
    }

    /// Iterate over all chunk metadata.
    pub fn iter_chunks(&self) -> ChunkIter {
        ChunkIter {
            plan: *self,
            current: 0,
        }
    }
}

pub struct ChunkIter {
    plan: ChunkPlan,
    current: u64,
}

impl Iterator for ChunkIter {
    type Item = ChunkMeta;

    fn next(&mut self) -> Option<Self::Item> {
        let meta = self.plan.chunk_meta(self.current)?;
        self.current += 1;
        Some(meta)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.plan.total_chunks - self.current) as usize;
        (remaining, Some(remaining))
    }
}

/// Manages which chunks still need to be sent/received.
#[derive(Debug, Clone)]
pub struct ChunkScheduler {
    plan: ChunkPlan,
    received: BitVec,
    cursor: u64,
}

impl ChunkScheduler {
    pub fn new(plan: ChunkPlan, received: BitVec) -> Self {
        let total = plan.total_chunks();
        let mut bv = received;
        // Ensure the bitvec is the right length
        bv.resize(total as usize, false);
        Self {
            plan,
            received: bv,
            cursor: 0,
        }
    }

    /// Get all chunk indices not yet received.
    pub fn remaining(&self) -> Vec<u64> {
        self.received
            .iter()
            .enumerate()
            .filter(|(_, b)| !**b)
            .map(|(i, _)| i as u64)
            .collect()
    }

    /// Get the next batch of chunk indices to send in parallel.
    pub fn next_batch(&mut self, max_parallel: usize) -> Vec<u64> {
        let mut batch = Vec::with_capacity(max_parallel);
        let total = self.plan.total_chunks();

        while batch.len() < max_parallel && self.cursor < total {
            if !self.received[self.cursor as usize] {
                batch.push(self.cursor);
            }
            self.cursor += 1;
        }

        // If we've exhausted the cursor but still have remaining, wrap around
        if batch.is_empty() && !self.is_complete() {
            self.cursor = 0;
            while batch.len() < max_parallel && self.cursor < total {
                if !self.received[self.cursor as usize] {
                    batch.push(self.cursor);
                }
                self.cursor += 1;
            }
        }

        batch
    }

    /// Mark a chunk as received.
    pub fn mark_received(&mut self, index: u64) {
        if (index as usize) < self.received.len() {
            self.received.set(index as usize, true);
        }
    }

    /// Check if all chunks have been received.
    pub fn is_complete(&self) -> bool {
        self.received.all()
    }

    /// Get progress as (received, total).
    pub fn progress(&self) -> (u64, u64) {
        let received = self.received.count_ones() as u64;
        (received, self.plan.total_chunks())
    }

    /// Get the underlying bitfield.
    pub fn bitfield(&self) -> &BitVec {
        &self.received
    }
}

/// Read a specific chunk from a file.
pub struct ChunkReader;

impl ChunkReader {
    pub async fn read_chunk(path: &Path, offset: u64, size: u32) -> Result<Vec<u8>, ChunkError> {
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; size as usize];
        file.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

/// Write a chunk to a file at the correct offset.
pub struct ChunkWriter;

impl ChunkWriter {
    pub async fn write_chunk(path: &Path, offset: u64, data: &[u8]) -> Result<(), ChunkError> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        file.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_plan_basic() {
        let plan = ChunkPlan::new(10_000_000, 4_000_000);
        assert_eq!(plan.total_chunks(), 3);

        let m0 = plan.chunk_meta(0).unwrap();
        assert_eq!(m0.offset, 0);
        assert_eq!(m0.size, 4_000_000);

        let m1 = plan.chunk_meta(1).unwrap();
        assert_eq!(m1.offset, 4_000_000);
        assert_eq!(m1.size, 4_000_000);

        let m2 = plan.chunk_meta(2).unwrap();
        assert_eq!(m2.offset, 8_000_000);
        assert_eq!(m2.size, 2_000_000); // last chunk is smaller

        assert!(plan.chunk_meta(3).is_none());
    }

    #[test]
    fn test_chunk_plan_exact_multiple() {
        let plan = ChunkPlan::new(8_000_000, 4_000_000);
        assert_eq!(plan.total_chunks(), 2);
    }

    #[test]
    fn test_chunk_plan_empty() {
        let plan = ChunkPlan::new(0, 4_000_000);
        assert_eq!(plan.total_chunks(), 0);
    }

    #[test]
    fn test_scheduler() {
        let plan = ChunkPlan::new(10_000_000, 4_000_000);
        let bv = bitvec![0; 3];
        let mut sched = ChunkScheduler::new(plan, bv);

        assert_eq!(sched.progress(), (0, 3));
        assert!(!sched.is_complete());

        let batch = sched.next_batch(2);
        assert_eq!(batch, vec![0, 1]);

        sched.mark_received(0);
        sched.mark_received(1);
        assert_eq!(sched.progress(), (2, 3));

        let batch = sched.next_batch(2);
        assert_eq!(batch, vec![2]);

        sched.mark_received(2);
        assert!(sched.is_complete());
    }

    #[test]
    fn test_scheduler_with_resume() {
        let plan = ChunkPlan::new(10_000_000, 4_000_000);
        let mut bv = bitvec![0; 3];
        bv.set(0, true); // chunk 0 already received
        bv.set(2, true); // chunk 2 already received
        let sched = ChunkScheduler::new(plan, bv);

        assert_eq!(sched.progress(), (2, 3));
        let remaining = sched.remaining();
        assert_eq!(remaining, vec![1]);
    }
}
