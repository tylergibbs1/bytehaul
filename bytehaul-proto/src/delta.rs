//! Delta transfer support for ByteHaul.
//!
//! When a file already exists at the destination, block-level BLAKE3 signatures
//! are compared to determine which blocks have changed. Only changed blocks are
//! transferred, dramatically reducing bandwidth for files that have been
//! partially modified.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tracing::debug;

use crate::verify;

/// BLAKE3 signature of a single block in an existing file.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockSignature {
    /// Block index within the file.
    pub index: u64,
    /// BLAKE3 hash of the block contents.
    pub blake3: [u8; 32],
}

/// Result of comparing source blocks against destination signatures.
#[derive(Debug)]
pub struct DeltaPlan {
    /// Global block indices that already match the destination (skip these).
    pub matching_blocks: Vec<u64>,
    /// Global block indices that need to be transferred.
    pub changed_blocks: Vec<u64>,
    /// Total blocks in the file.
    pub total_blocks: u64,
}

impl DeltaPlan {
    /// Fraction of blocks that can be skipped (0.0 to 1.0).
    pub fn skip_ratio(&self) -> f64 {
        if self.total_blocks == 0 {
            return 0.0;
        }
        self.matching_blocks.len() as f64 / self.total_blocks as f64
    }

    /// Number of bytes saved (approximate, assumes uniform block size).
    pub fn bytes_saved(&self, block_size: u32) -> u64 {
        self.matching_blocks.len() as u64 * block_size as u64
    }
}

/// Compute block-level BLAKE3 signatures for an existing file.
///
/// Returns one `BlockSignature` per block. The caller can send these to the
/// sender so it can determine which blocks need to be transferred.
pub async fn compute_signatures(
    path: &Path,
    block_size: u32,
) -> Result<Vec<BlockSignature>, std::io::Error> {
    let meta = tokio::fs::metadata(path).await?;
    let file_size = meta.len();

    if file_size == 0 {
        return Ok(Vec::new());
    }

    let total_blocks = (file_size + block_size as u64 - 1) / block_size as u64;
    let mut signatures = Vec::with_capacity(total_blocks as usize);

    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; block_size as usize];

    for index in 0..total_blocks {
        let remaining = file_size - index * block_size as u64;
        let read_size = remaining.min(block_size as u64) as usize;
        file.read_exact(&mut buf[..read_size]).await?;
        let blake3 = verify::hash_bytes(&buf[..read_size]);
        signatures.push(BlockSignature { index, blake3 });
    }

    Ok(signatures)
}

/// Compare source file blocks against destination signatures to build a delta plan.
///
/// `global_block_offset` is the starting global block index for this file in a
/// multi-file manifest (0 for single-file transfers).
pub async fn compute_delta(
    source_path: &Path,
    dest_signatures: &[BlockSignature],
    block_size: u32,
    global_block_offset: u64,
) -> Result<DeltaPlan, std::io::Error> {
    let meta = tokio::fs::metadata(source_path).await?;
    let file_size = meta.len();

    if file_size == 0 {
        return Ok(DeltaPlan {
            matching_blocks: Vec::new(),
            changed_blocks: Vec::new(),
            total_blocks: 0,
        });
    }

    let total_blocks = (file_size + block_size as u64 - 1) / block_size as u64;

    // Build lookup from block index to BLAKE3 hash
    let dest_map: HashMap<u64, &[u8; 32]> = dest_signatures
        .iter()
        .map(|sig| (sig.index, &sig.blake3))
        .collect();

    let mut matching = Vec::new();
    let mut changed = Vec::new();

    let mut file = tokio::fs::File::open(source_path).await?;
    let mut buf = vec![0u8; block_size as usize];

    for local_index in 0..total_blocks {
        let remaining = file_size - local_index * block_size as u64;
        let read_size = remaining.min(block_size as u64) as usize;
        file.read_exact(&mut buf[..read_size]).await?;

        let source_hash = verify::hash_bytes(&buf[..read_size]);
        let global_index = global_block_offset + local_index;

        if let Some(dest_hash) = dest_map.get(&local_index) {
            if source_hash == **dest_hash {
                matching.push(global_index);
                continue;
            }
        }

        changed.push(global_index);
    }

    debug!(
        "Delta: {}/{} blocks match ({:.1}% savings)",
        matching.len(),
        total_blocks,
        if total_blocks > 0 {
            matching.len() as f64 / total_blocks as f64 * 100.0
        } else {
            0.0
        }
    );

    Ok(DeltaPlan {
        matching_blocks: matching,
        changed_blocks: changed,
        total_blocks,
    })
}

/// Copy matching blocks from the old destination file to the new destination file.
///
/// This is used when delta transfer identifies blocks that already exist at the
/// destination. The receiver copies these blocks from the existing file into
/// the new (pre-allocated) destination file.
pub async fn copy_matching_blocks(
    old_path: &Path,
    new_path: &Path,
    matching_local_indices: &[u64],
    block_size: u32,
    file_size: u64,
) -> Result<(), std::io::Error> {
    if matching_local_indices.is_empty() {
        return Ok(());
    }

    let mut old_file = tokio::fs::File::open(old_path).await?;
    let mut new_file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(new_path)
        .await?;

    let mut buf = vec![0u8; block_size as usize];

    for &local_index in matching_local_indices {
        let offset = local_index * block_size as u64;
        let remaining = file_size - offset;
        let read_size = remaining.min(block_size as u64) as usize;

        old_file.seek(SeekFrom::Start(offset)).await?;
        old_file.read_exact(&mut buf[..read_size]).await?;

        new_file.seek(SeekFrom::Start(offset)).await?;
        tokio::io::AsyncWriteExt::write_all(&mut new_file, &buf[..read_size]).await?;
    }

    tokio::io::AsyncWriteExt::flush(&mut new_file).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_compute_signatures() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.bin");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&vec![0xAA; 1024 * 1024]).unwrap(); // 1MB of 0xAA
        }

        let sigs = compute_signatures(&path, 256 * 1024).await.unwrap();
        assert_eq!(sigs.len(), 4); // 1MB / 256KB = 4 blocks

        // All blocks have same content, so same hash
        assert_eq!(sigs[0].blake3, sigs[1].blake3);
    }

    #[tokio::test]
    async fn test_identical_files_delta() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");

        let data = vec![0x42; 512 * 1024]; // 512KB
        std::fs::write(&src, &data).unwrap();
        std::fs::write(&dst, &data).unwrap();

        let sigs = compute_signatures(&dst, 256 * 1024).await.unwrap();
        let plan = compute_delta(&src, &sigs, 256 * 1024, 0).await.unwrap();

        assert_eq!(plan.matching_blocks.len(), 2);
        assert!(plan.changed_blocks.is_empty());
        assert!((plan.skip_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_partially_changed_delta() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");

        let block_size = 256 * 1024u32;

        // Create dst: 4 blocks of zeros
        std::fs::write(&dst, &vec![0u8; 4 * block_size as usize]).unwrap();

        // Create src: blocks 0,1 same (zeros), blocks 2,3 different (0xFF)
        let mut src_data = vec![0u8; 4 * block_size as usize];
        for i in (2 * block_size as usize)..(4 * block_size as usize) {
            src_data[i] = 0xFF;
        }
        std::fs::write(&src, &src_data).unwrap();

        let sigs = compute_signatures(&dst, block_size).await.unwrap();
        let plan = compute_delta(&src, &sigs, block_size, 0).await.unwrap();

        assert_eq!(plan.matching_blocks, vec![0, 1]);
        assert_eq!(plan.changed_blocks, vec![2, 3]);
        assert!((plan.skip_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_no_existing_file_delta() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.bin");
        std::fs::write(&src, &vec![0x42; 256 * 1024]).unwrap();

        // Empty signatures = no existing file
        let plan = compute_delta(&src, &[], 256 * 1024, 0).await.unwrap();
        assert!(plan.matching_blocks.is_empty());
        assert_eq!(plan.changed_blocks, vec![0]);
    }

    #[tokio::test]
    async fn test_copy_matching_blocks() {
        let dir = TempDir::new().unwrap();
        let old = dir.path().join("old.bin");
        let new = dir.path().join("new.bin");

        let block_size = 1024u32;
        let file_size = 4096u64;

        // Old file: 4 blocks with distinct content
        let mut old_data = vec![0u8; file_size as usize];
        for i in 0..4 {
            for j in 0..block_size as usize {
                old_data[i * block_size as usize + j] = i as u8;
            }
        }
        std::fs::write(&old, &old_data).unwrap();

        // New file: pre-allocated with zeros
        std::fs::write(&new, &vec![0u8; file_size as usize]).unwrap();

        // Copy blocks 0 and 2 from old to new
        copy_matching_blocks(&old, &new, &[0, 2], block_size, file_size)
            .await
            .unwrap();

        let result = std::fs::read(&new).unwrap();
        // Block 0 should be all 0x00 (from old)
        assert!(result[..1024].iter().all(|&b| b == 0));
        // Block 1 should still be zeros (not copied)
        assert!(result[1024..2048].iter().all(|&b| b == 0));
        // Block 2 should be all 0x02 (from old)
        assert!(result[2048..3072].iter().all(|&b| b == 2));
        // Block 3 should still be zeros (not copied)
        assert!(result[3072..4096].iter().all(|&b| b == 0));
    }
}
