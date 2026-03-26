//! Bidirectional sync support for ByteHaul.
//!
//! Compares manifests from both sides and produces a [`SyncPlan`] describing
//! which files to push, which to pull, and any conflicts.

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::manifest::{FileEntry, TransferManifest};

/// How to resolve conflicts when both sides have different versions of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictMode {
    /// Keep the newer file (by modification time). Falls back to source-wins
    /// if mtime is unavailable.
    #[default]
    Newer,
    /// Source (local) always wins.
    SourceWins,
    /// Destination (remote) always wins.
    DestWins,
    /// Skip conflicting files.
    Skip,
}

/// A file that exists on both sides with different content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConflict {
    /// Relative path of the conflicting file.
    pub path: PathBuf,
    /// BLAKE3 hash on the source side.
    pub source_hash: [u8; 32],
    /// BLAKE3 hash on the destination side.
    pub dest_hash: [u8; 32],
    /// How it was resolved.
    pub resolution: ConflictResolution,
}

/// How a conflict was resolved.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Send the source version to the destination.
    PushSource,
    /// Pull the destination version to the source.
    PullDest,
    /// Skip this file entirely.
    Skipped,
}

/// The result of comparing two manifests for bidirectional sync.
#[derive(Debug, Clone)]
pub struct SyncPlan {
    /// Files that exist only on the source — push to destination.
    pub to_push: Vec<usize>,
    /// Files that exist only on the destination — pull to source.
    pub to_pull: Vec<usize>,
    /// Files that differ on both sides.
    pub conflicts: Vec<SyncConflict>,
    /// Files that are identical on both sides — no action needed.
    pub unchanged: usize,
}

impl SyncPlan {
    /// Total number of files that need to be transferred.
    pub fn transfer_count(&self) -> usize {
        self.to_push.len()
            + self.to_pull.len()
            + self
                .conflicts
                .iter()
                .filter(|c| c.resolution != ConflictResolution::Skipped)
                .count()
    }

    /// True if there is nothing to do.
    pub fn is_empty(&self) -> bool {
        self.to_push.is_empty() && self.to_pull.is_empty() && self.conflicts.is_empty()
    }

    /// Summary string for display.
    pub fn summary(&self) -> String {
        format!(
            "{} to push, {} to pull, {} conflicts, {} unchanged",
            self.to_push.len(),
            self.to_pull.len(),
            self.conflicts.len(),
            self.unchanged
        )
    }
}

/// Compare a local and remote manifest to produce a sync plan.
///
/// `local_manifest` is the manifest built from the local directory.
/// `remote_manifest` is the manifest received from the remote.
/// Both must have `relative_path` set on their file entries.
pub fn compute_sync_plan(
    local_manifest: &TransferManifest,
    remote_manifest: &TransferManifest,
    conflict_mode: ConflictMode,
) -> SyncPlan {
    // Index remote files by relative path
    let remote_map: HashMap<PathBuf, (usize, &FileEntry)> = remote_manifest
        .files
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            entry
                .relative_path
                .as_ref()
                .map(|rp| (rp.clone(), (i, entry)))
        })
        .collect();

    // Index local files by relative path
    let local_map: HashMap<PathBuf, (usize, &FileEntry)> = local_manifest
        .files
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            entry
                .relative_path
                .as_ref()
                .map(|rp| (rp.clone(), (i, entry)))
        })
        .collect();

    let mut to_push = Vec::new();
    let mut to_pull = Vec::new();
    let mut conflicts = Vec::new();
    let mut unchanged = 0;

    // Check local files against remote
    for (rel_path, (local_idx, local_entry)) in &local_map {
        match remote_map.get(rel_path) {
            None => {
                // File only exists locally — push it
                to_push.push(*local_idx);
            }
            Some((_remote_idx, remote_entry)) => {
                if local_entry.blake3_hash == remote_entry.blake3_hash {
                    // Identical — no action
                    unchanged += 1;
                } else {
                    // Different content — conflict
                    let resolution = resolve_conflict(conflict_mode);
                    conflicts.push(SyncConflict {
                        path: rel_path.clone(),
                        source_hash: local_entry.blake3_hash,
                        dest_hash: remote_entry.blake3_hash,
                        resolution,
                    });
                }
            }
        }
    }

    // Files only on remote — pull them
    for (rel_path, (remote_idx, _)) in &remote_map {
        if !local_map.contains_key(rel_path) {
            to_pull.push(*remote_idx);
        }
    }

    SyncPlan {
        to_push,
        to_pull,
        conflicts,
        unchanged,
    }
}

fn resolve_conflict(mode: ConflictMode) -> ConflictResolution {
    match mode {
        ConflictMode::Newer => {
            // Without mtime data, fall back to source-wins
            ConflictResolution::PushSource
        }
        ConflictMode::SourceWins => ConflictResolution::PushSource,
        ConflictMode::DestWins => ConflictResolution::PullDest,
        ConflictMode::Skip => ConflictResolution::Skipped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{FileEntry, TransferManifest, DEFAULT_BLOCK_SIZE};

    fn entry(name: &str, hash_byte: u8) -> FileEntry {
        FileEntry {
            source_path: PathBuf::from(format!("/src/{name}")),
            dest_path: PathBuf::from(format!("/dst/{name}")),
            size: 1024,
            blake3_hash: [hash_byte; 32],
            relative_path: Some(PathBuf::from(name)),
        }
    }

    fn manifest(entries: Vec<FileEntry>) -> TransferManifest {
        TransferManifest::new(entries, DEFAULT_BLOCK_SIZE).unwrap()
    }

    #[test]
    fn identical_manifests() {
        let local = manifest(vec![entry("a.txt", 0xAA), entry("b.txt", 0xBB)]);
        let remote = manifest(vec![entry("a.txt", 0xAA), entry("b.txt", 0xBB)]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::Newer);
        assert!(plan.is_empty());
        assert_eq!(plan.unchanged, 2);
    }

    #[test]
    fn local_only_files_push() {
        let local = manifest(vec![entry("a.txt", 0xAA), entry("new.txt", 0xCC)]);
        let remote = manifest(vec![entry("a.txt", 0xAA)]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::Newer);
        assert_eq!(plan.to_push.len(), 1);
        assert_eq!(plan.to_pull.len(), 0);
        assert_eq!(plan.unchanged, 1);
    }

    #[test]
    fn remote_only_files_pull() {
        let local = manifest(vec![entry("a.txt", 0xAA)]);
        let remote = manifest(vec![entry("a.txt", 0xAA), entry("remote.txt", 0xDD)]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::Newer);
        assert_eq!(plan.to_push.len(), 0);
        assert_eq!(plan.to_pull.len(), 1);
    }

    #[test]
    fn conflict_source_wins() {
        let local = manifest(vec![entry("file.txt", 0xAA)]);
        let remote = manifest(vec![entry("file.txt", 0xBB)]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::SourceWins);
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(
            plan.conflicts[0].resolution,
            ConflictResolution::PushSource
        );
    }

    #[test]
    fn conflict_dest_wins() {
        let local = manifest(vec![entry("file.txt", 0xAA)]);
        let remote = manifest(vec![entry("file.txt", 0xBB)]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::DestWins);
        assert_eq!(plan.conflicts[0].resolution, ConflictResolution::PullDest);
    }

    #[test]
    fn conflict_skip() {
        let local = manifest(vec![entry("file.txt", 0xAA)]);
        let remote = manifest(vec![entry("file.txt", 0xBB)]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::Skip);
        assert_eq!(plan.conflicts[0].resolution, ConflictResolution::Skipped);
        assert_eq!(plan.transfer_count(), 0);
    }

    #[test]
    fn mixed_scenario() {
        let local = manifest(vec![
            entry("shared.txt", 0xAA),
            entry("local_only.txt", 0xBB),
            entry("conflict.txt", 0xCC),
        ]);
        let remote = manifest(vec![
            entry("shared.txt", 0xAA),
            entry("remote_only.txt", 0xDD),
            entry("conflict.txt", 0xEE),
        ]);

        let plan = compute_sync_plan(&local, &remote, ConflictMode::Newer);
        assert_eq!(plan.unchanged, 1);
        assert_eq!(plan.to_push.len(), 1);
        assert_eq!(plan.to_pull.len(), 1);
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.summary(), "1 to push, 1 to pull, 1 conflicts, 1 unchanged");
    }
}
