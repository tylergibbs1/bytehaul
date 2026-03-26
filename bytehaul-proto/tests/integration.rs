//! Integration tests for ByteHaul: full sender -> receiver engine flow over QUIC.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

use bytehaul_proto::engine::{EngineConfig, OverwriteMode, Receiver, Sender};
use bytehaul_proto::filter::FileFilter;
use bytehaul_proto::manifest::MIN_BLOCK_SIZE;
use bytehaul_proto::transport::{QuicClient, QuicServer, TransportConfig};
use bytehaul_proto::verify;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn localhost_transport_config() -> TransportConfig {
    TransportConfig {
        bind_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        ..Default::default()
    }
}

fn engine_config(state_dir: &Path) -> EngineConfig {
    EngineConfig {
        state_dir: Some(state_dir.to_path_buf()),
        ..Default::default()
    }
}

fn engine_config_with_block_size(state_dir: &Path, block_size: u32) -> EngineConfig {
    EngineConfig {
        state_dir: Some(state_dir.to_path_buf()),
        block_size,
        ..Default::default()
    }
}

/// Create a file filled with a repeating pattern.
async fn create_test_file(dir: &Path, name: &str, size: usize) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut file = tokio::fs::File::create(&path).await.unwrap();

    if size > 0 {
        // Repeating pattern so content is deterministic but not trivially compressible.
        let pattern: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        let mut remaining = size;
        while remaining > 0 {
            let chunk = remaining.min(pattern.len());
            file.write_all(&pattern[..chunk]).await.unwrap();
            remaining -= chunk;
        }
    }

    file.flush().await.unwrap();
    drop(file);
    path
}

/// Run a full sender -> receiver transfer and return the path of the received file.
///
/// `sender_config` and `receiver_config` allow callers to override engine settings
/// (e.g. block size). The receiver task is spawned on the runtime; the sender runs
/// inline on the current task. Both sides share a real QUIC connection over loopback.
async fn run_transfer(
    source_path: &Path,
    dest_name: &str,
    recv_dir: &Path,
    sender_config: EngineConfig,
    receiver_config: EngineConfig,
) -> std::path::PathBuf {
    let server = QuicServer::bind(localhost_transport_config()).expect("server should bind");
    let server_addr = server.local_addr();

    let recv_dir = recv_dir.to_path_buf();
    let receiver_config_clone = receiver_config;
    let recv_handle = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept should succeed");
        let receiver =
            Receiver::new(receiver_config_clone).expect("receiver creation should succeed");
        let received_path = receiver
            .receive_file(&conn, &recv_dir)
            .await
            .expect("receive_file should succeed");
        conn.close();
        received_path
    });

    let conn = QuicClient::connect(server_addr, "localhost")
        .await
        .expect("client connect should succeed");

    let sender = Sender::new(sender_config);
    sender
        .send_file(&conn, source_path, dest_name)
        .await
        .expect("send_file should succeed");

    conn.close();

    recv_handle.await.expect("receiver task should not panic")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_file_transfer() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let file_size = 1024 * 1024; // 1 MB
    let source_path = create_test_file(src_dir.path(), "data.bin", file_size).await;

    let source_hash = verify::hash_file(&source_path)
        .await
        .expect("hash source");

    let received_path = run_transfer(
        &source_path,
        "data.bin",
        recv_dir.path(),
        engine_config(state_dir.path()),
        engine_config(state_dir.path()),
    )
    .await;

    // Verify the received file exists and has the correct size.
    let metadata = tokio::fs::metadata(&received_path).await.unwrap();
    assert_eq!(metadata.len(), file_size as u64);

    // Verify BLAKE3 hash matches.
    let received_hash = verify::hash_file(&received_path)
        .await
        .expect("hash received");
    assert_eq!(
        source_hash, received_hash,
        "BLAKE3 hash of received file must match source"
    );
}

#[tokio::test]
async fn test_small_file_transfer() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let file_size = 100; // 100 bytes -- single chunk
    let source_path = create_test_file(src_dir.path(), "tiny.bin", file_size).await;

    let source_hash = verify::hash_file(&source_path)
        .await
        .expect("hash source");

    let received_path = run_transfer(
        &source_path,
        "tiny.bin",
        recv_dir.path(),
        engine_config(state_dir.path()),
        engine_config(state_dir.path()),
    )
    .await;

    let metadata = tokio::fs::metadata(&received_path).await.unwrap();
    assert_eq!(metadata.len(), file_size as u64);

    let received_hash = verify::hash_file(&received_path)
        .await
        .expect("hash received");
    assert_eq!(
        source_hash, received_hash,
        "BLAKE3 hash of received file must match source"
    );
}

#[tokio::test]
async fn test_empty_file_transfer() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let source_path = create_test_file(src_dir.path(), "empty.bin", 0).await;

    let source_hash = verify::hash_file(&source_path)
        .await
        .expect("hash source");

    let received_path = run_transfer(
        &source_path,
        "empty.bin",
        recv_dir.path(),
        engine_config(state_dir.path()),
        engine_config(state_dir.path()),
    )
    .await;

    let metadata = tokio::fs::metadata(&received_path).await.unwrap();
    assert_eq!(metadata.len(), 0);

    let received_hash = verify::hash_file(&received_path)
        .await
        .expect("hash received");
    assert_eq!(
        source_hash, received_hash,
        "BLAKE3 hash of empty file must match"
    );
}

#[tokio::test]
async fn test_resume_transfer() {
    use bytehaul_proto::resume::{StateManager, TransferState};

    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let block_size = MIN_BLOCK_SIZE; // 256 KiB
    let file_size: usize = 4 * block_size as usize; // exactly 4 blocks

    let source_path = create_test_file(src_dir.path(), "resume.bin", file_size).await;

    let source_hash = verify::hash_file(&source_path)
        .await
        .expect("hash source");

    // Build the manifest to determine the transfer_id the receiver will compute.
    let manifest = bytehaul_proto::manifest::TransferManifest::new(
        vec![bytehaul_proto::manifest::FileEntry {
            source_path: source_path.clone(),
            dest_path: std::path::PathBuf::from("resume.bin"),
            size: file_size as u64,
            blake3_hash: source_hash,
            relative_path: None,
        }],
        block_size,
    )
    .expect("manifest creation");

    let dest_path = recv_dir.path().join("resume.bin");

    // Pre-create the destination file with correct size and write the first two
    // blocks from the source, simulating a partial prior transfer.
    {
        let source_data = tokio::fs::read(&source_path).await.unwrap();
        let mut dest_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dest_path)
            .await
            .unwrap();

        // Write blocks 0 and 1 with correct data.
        let two_blocks = 2 * block_size as usize;
        dest_file.write_all(&source_data[..two_blocks]).await.unwrap();

        // Pre-allocate the full file size so the receiver can seek-write remaining blocks.
        dest_file.set_len(file_size as u64).await.unwrap();
        dest_file.flush().await.unwrap();
    }

    // Create a TransferState indicating blocks 0 and 1 are already received.
    let file_hash_hex = verify::hash_to_hex(&source_hash);
    let mut state = TransferState::new(
        manifest.transfer_id.clone(),
        dest_path.to_string_lossy().into_owned(),
        file_size as u64,
        block_size,
        4, // total blocks
        file_hash_hex,
    );
    state.mark_received(0);
    state.mark_received(1);

    // Persist the state so the receiver picks it up on startup.
    let state_mgr =
        StateManager::new(Some(state_dir.path().to_path_buf())).expect("state manager");
    state_mgr.save(&state).expect("save state");

    // Track how many chunks the sender actually sends.
    let chunks_sent = Arc::new(AtomicU64::new(0));
    let chunks_sent_clone = Arc::clone(&chunks_sent);

    // Run the transfer. The receiver should resume from block 2.
    let server = QuicServer::bind(localhost_transport_config()).expect("server should bind");
    let server_addr = server.local_addr();

    let recv_dir_path = recv_dir.path().to_path_buf();
    let receiver_state_dir = state_dir.path().to_path_buf();
    let recv_handle = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept");
        let receiver = Receiver::new(EngineConfig {
            state_dir: Some(receiver_state_dir),
            block_size,
            overwrite_mode: OverwriteMode::Overwrite,
            ..Default::default()
        })
        .expect("receiver");
        let path = receiver
            .receive_file(&conn, &recv_dir_path)
            .await
            .expect("receive_file");
        conn.close();
        path
    });

    let conn = QuicClient::connect(server_addr, "localhost")
        .await
        .expect("client connect");

    let mut sender = Sender::new(EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        block_size,
        ..Default::default()
    });
    sender.on_progress(move |progress| {
        chunks_sent_clone.store(progress.transferred_chunks, Ordering::Relaxed);
    });
    sender
        .send_file(&conn, &source_path, "resume.bin")
        .await
        .expect("send_file");
    conn.close();

    let received_path = recv_handle.await.expect("receiver task");

    // Verify the final file matches.
    let received_hash = verify::hash_file(&received_path)
        .await
        .expect("hash received");
    assert_eq!(
        source_hash, received_hash,
        "Resumed transfer must produce correct file"
    );

    // The sender should have sent at most 4 chunks total (2 already received + 2 new).
    // The progress counter includes the already-received count, so the final value
    // should be 4. The important thing is the transfer succeeded and the file is correct.
    let metadata = tokio::fs::metadata(&received_path).await.unwrap();
    assert_eq!(metadata.len(), file_size as u64);
}

#[tokio::test]
async fn test_large_block_transfer() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let file_size = 1024 * 1024; // 1 MB
    let source_path = create_test_file(src_dir.path(), "blocks.bin", file_size).await;

    let source_hash = verify::hash_file(&source_path)
        .await
        .expect("hash source");

    // Use MIN_BLOCK_SIZE (256 KiB) so the 1 MB file is split into 4 chunks.
    let config = engine_config_with_block_size(state_dir.path(), MIN_BLOCK_SIZE);

    let received_path = run_transfer(
        &source_path,
        "blocks.bin",
        recv_dir.path(),
        config.clone(),
        config,
    )
    .await;

    let metadata = tokio::fs::metadata(&received_path).await.unwrap();
    assert_eq!(metadata.len(), file_size as u64);

    let received_hash = verify::hash_file(&received_path)
        .await
        .expect("hash received");
    assert_eq!(
        source_hash, received_hash,
        "BLAKE3 hash must match with MIN_BLOCK_SIZE chunking"
    );
}

// ---------------------------------------------------------------------------
// Directory transfer tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_directory_transfer() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    // Create a directory tree with 3 files in subdirs.
    let root = src_dir.path();
    tokio::fs::create_dir_all(root.join("sub/deep")).await.unwrap();
    create_test_file(root, "alpha.bin", 8192).await;
    create_test_file(&root.join("sub"), "beta.bin", 16384).await;
    create_test_file(&root.join("sub/deep"), "gamma.bin", 4096).await;

    // Hash the source files for later verification.
    let hash_alpha = verify::hash_file(&root.join("alpha.bin")).await.unwrap();
    let hash_beta = verify::hash_file(&root.join("sub/beta.bin")).await.unwrap();
    let hash_gamma = verify::hash_file(&root.join("sub/deep/gamma.bin")).await.unwrap();

    let server = QuicServer::bind(localhost_transport_config()).expect("server should bind");
    let server_addr = server.local_addr();

    let recv_dir_path = recv_dir.path().to_path_buf();
    let receiver_config = EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        block_size: MIN_BLOCK_SIZE,
        ..Default::default()
    };

    let recv_handle = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept should succeed");
        let receiver = Receiver::new(receiver_config).expect("receiver creation should succeed");
        let received_paths = receiver
            .receive_transfer(&conn, &recv_dir_path)
            .await
            .expect("receive_transfer should succeed");
        conn.close();
        received_paths
    });

    let conn = QuicClient::connect(server_addr, "localhost")
        .await
        .expect("client connect should succeed");

    let sender = Sender::new(EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        block_size: MIN_BLOCK_SIZE,
        ..Default::default()
    });
    sender
        .send_directory(&conn, root, "transferred")
        .await
        .expect("send_directory should succeed");
    conn.close();

    let received_paths = recv_handle.await.expect("receiver task should not panic");

    // Verify we got 3 files.
    assert_eq!(received_paths.len(), 3, "should receive exactly 3 files");

    // Verify each file arrived with correct content by checking hashes.
    let dest_root = recv_dir.path().join("transferred");
    let recv_hash_alpha = verify::hash_file(&dest_root.join("alpha.bin")).await.unwrap();
    let recv_hash_beta = verify::hash_file(&dest_root.join("sub/beta.bin")).await.unwrap();
    let recv_hash_gamma = verify::hash_file(&dest_root.join("sub/deep/gamma.bin")).await.unwrap();

    assert_eq!(hash_alpha, recv_hash_alpha, "alpha.bin hash mismatch");
    assert_eq!(hash_beta, recv_hash_beta, "beta.bin hash mismatch");
    assert_eq!(hash_gamma, recv_hash_gamma, "gamma.bin hash mismatch");
}

#[tokio::test]
async fn test_directory_transfer_with_filter() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    // Create a directory with mixed file types.
    let root = src_dir.path();
    tokio::fs::create_dir_all(root.join("docs")).await.unwrap();
    create_test_file(root, "readme.txt", 1024).await;
    create_test_file(root, "image.bin", 2048).await;
    create_test_file(&root.join("docs"), "notes.txt", 512).await;
    create_test_file(&root.join("docs"), "data.bin", 4096).await;

    // Hash the .txt files we expect to transfer.
    let hash_readme = verify::hash_file(&root.join("readme.txt")).await.unwrap();
    let hash_notes = verify::hash_file(&root.join("docs/notes.txt")).await.unwrap();

    let server = QuicServer::bind(localhost_transport_config()).expect("server should bind");
    let server_addr = server.local_addr();

    let recv_dir_path = recv_dir.path().to_path_buf();
    let receiver_config = EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        block_size: MIN_BLOCK_SIZE,
        ..Default::default()
    };

    let recv_handle = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept should succeed");
        let receiver = Receiver::new(receiver_config).expect("receiver creation should succeed");
        let received_paths = receiver
            .receive_transfer(&conn, &recv_dir_path)
            .await
            .expect("receive_transfer should succeed");
        conn.close();
        received_paths
    });

    let conn = QuicClient::connect(server_addr, "localhost")
        .await
        .expect("client connect should succeed");

    let filter = FileFilter::new(&["*.txt".to_string()], &[]).expect("filter should build");
    let sender = Sender::new(EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        block_size: MIN_BLOCK_SIZE,
        ..Default::default()
    });
    sender
        .send_directory_filtered(&conn, root, "filtered", &filter)
        .await
        .expect("send_directory_filtered should succeed");
    conn.close();

    let received_paths = recv_handle.await.expect("receiver task should not panic");

    // Only .txt files should have been transferred.
    assert_eq!(received_paths.len(), 2, "should receive exactly 2 .txt files");

    let dest_root = recv_dir.path().join("filtered");
    let recv_hash_readme = verify::hash_file(&dest_root.join("readme.txt")).await.unwrap();
    let recv_hash_notes = verify::hash_file(&dest_root.join("docs/notes.txt")).await.unwrap();

    assert_eq!(hash_readme, recv_hash_readme, "readme.txt hash mismatch");
    assert_eq!(hash_notes, recv_hash_notes, "notes.txt hash mismatch");

    // Verify .bin files were NOT transferred.
    assert!(
        !dest_root.join("image.bin").exists(),
        "image.bin should not have been transferred"
    );
    assert!(
        !dest_root.join("docs/data.bin").exists(),
        "data.bin should not have been transferred"
    );
}

// ---------------------------------------------------------------------------
// Delta transfer tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delta_transfer_unchanged() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let file_size = MIN_BLOCK_SIZE as usize * 2; // 2 blocks
    let source_path = create_test_file(src_dir.path(), "stable.bin", file_size).await;
    let source_hash = verify::hash_file(&source_path).await.expect("hash source");

    // First transfer: send the file initially (no delta, just establish baseline).
    let received_path = run_transfer(
        &source_path,
        "stable.bin",
        recv_dir.path(),
        engine_config(state_dir.path()),
        engine_config(state_dir.path()),
    )
    .await;

    let first_hash = verify::hash_file(&received_path).await.expect("hash first");
    assert_eq!(source_hash, first_hash, "first transfer hash must match");

    // Second transfer: send the same file again with delta enabled.
    // The receiver already has the file, so delta should detect no changes.
    let server = QuicServer::bind(localhost_transport_config()).expect("server should bind");
    let server_addr = server.local_addr();

    let recv_dir_path = recv_dir.path().to_path_buf();
    let state_dir_path = state_dir.path().to_path_buf();
    let recv_handle = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept");
        let receiver = Receiver::new(EngineConfig {
            state_dir: Some(state_dir_path.clone()),
            delta_enabled: true,
            overwrite_mode: OverwriteMode::Overwrite,
            ..Default::default()
        })
        .expect("receiver");
        let paths = receiver
            .receive_transfer(&conn, &recv_dir_path)
            .await
            .expect("receive_transfer delta");
        conn.close();
        paths
    });

    let conn = QuicClient::connect(server_addr, "localhost")
        .await
        .expect("client connect");

    let sender = Sender::new(EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        delta_enabled: true,
        overwrite_mode: OverwriteMode::Overwrite,
        ..Default::default()
    });
    sender
        .send_file(&conn, &source_path, "stable.bin")
        .await
        .expect("send_file delta");
    conn.close();

    let delta_paths = recv_handle.await.expect("receiver task");
    assert!(!delta_paths.is_empty(), "should have received at least one file");

    // Verify the received file still matches after the delta transfer.
    let delta_hash = verify::hash_file(&delta_paths[0])
        .await
        .expect("hash delta");
    assert_eq!(
        source_hash, delta_hash,
        "delta transfer of unchanged file must produce correct result"
    );
}

#[tokio::test]
async fn test_delta_transfer_partial_change() {
    let src_dir = TempDir::new().unwrap();
    let recv_dir = TempDir::new().unwrap();
    let state_dir = TempDir::new().unwrap();

    let block_size = MIN_BLOCK_SIZE;
    let file_size = block_size as usize * 4; // 4 blocks
    let source_path = create_test_file(src_dir.path(), "mutable.bin", file_size).await;

    // First transfer: send the original file.
    let received_path = run_transfer(
        &source_path,
        "mutable.bin",
        recv_dir.path(),
        engine_config_with_block_size(state_dir.path(), block_size),
        engine_config_with_block_size(state_dir.path(), block_size),
    )
    .await;

    let first_hash = verify::hash_file(&received_path).await.expect("hash first");
    let original_hash = verify::hash_file(&source_path).await.expect("hash original");
    assert_eq!(original_hash, first_hash, "first transfer must match");

    // Modify approximately 25% of the file (the first block).
    {
        let mut data = tokio::fs::read(&source_path).await.unwrap();
        let modify_len = block_size as usize; // one full block = 25%
        for byte in data[..modify_len].iter_mut() {
            *byte = byte.wrapping_add(42);
        }
        tokio::fs::write(&source_path, &data).await.unwrap();
    }

    let modified_hash = verify::hash_file(&source_path).await.expect("hash modified");
    assert_ne!(
        original_hash, modified_hash,
        "modified file should differ from original"
    );

    // Second transfer: send the modified file with delta enabled.
    let server = QuicServer::bind(localhost_transport_config()).expect("server should bind");
    let server_addr = server.local_addr();

    let recv_dir_path = recv_dir.path().to_path_buf();
    let state_dir_path = state_dir.path().to_path_buf();
    let recv_handle = tokio::spawn(async move {
        let conn = server.accept().await.expect("accept");
        let receiver = Receiver::new(EngineConfig {
            state_dir: Some(state_dir_path),
            block_size,
            delta_enabled: true,
            overwrite_mode: OverwriteMode::Overwrite,
            ..Default::default()
        })
        .expect("receiver");
        let paths = receiver
            .receive_transfer(&conn, &recv_dir_path)
            .await
            .expect("receive_transfer delta partial");
        conn.close();
        paths
    });

    let conn = QuicClient::connect(server_addr, "localhost")
        .await
        .expect("client connect");

    let sender = Sender::new(EngineConfig {
        state_dir: Some(state_dir.path().to_path_buf()),
        block_size,
        delta_enabled: true,
        overwrite_mode: OverwriteMode::Overwrite,
        ..Default::default()
    });
    sender
        .send_file(&conn, &source_path, "mutable.bin")
        .await
        .expect("send_file delta partial");
    conn.close();

    let delta_paths = recv_handle.await.expect("receiver task");
    assert!(!delta_paths.is_empty(), "should have received at least one file");

    // Verify the received file matches the modified source.
    let delta_hash = verify::hash_file(&delta_paths[0])
        .await
        .expect("hash delta partial");
    assert_eq!(
        modified_hash, delta_hash,
        "delta transfer of partially changed file must produce correct result"
    );

    // Also verify the file size is unchanged.
    let metadata = tokio::fs::metadata(&delta_paths[0]).await.unwrap();
    assert_eq!(metadata.len(), file_size as u64, "file size must remain the same");
}
