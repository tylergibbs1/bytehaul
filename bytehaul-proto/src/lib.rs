//! Core protocol implementation for ByteHaul file transfer.
//!
//! This crate provides the wire protocol, QUIC transport layer, chunking
//! strategy, transfer manifest, resume state management, integrity
//! verification, congestion control, and the sender/receiver transfer engines.
pub mod chunking;
pub mod compress;
pub mod filter;
pub mod congestion;
pub mod crypto;
pub mod delta;
pub mod engine;
pub mod manifest;
pub mod resume;
pub mod storage;
pub mod sync;
pub mod transport;
pub mod verify;
pub mod wire;
