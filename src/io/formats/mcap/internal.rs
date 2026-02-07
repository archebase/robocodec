// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Internal types for MCAP format.
//!
//! These types are not part of the public API.

use std::collections::BTreeMap;

/// Message index entry for MCAP `MessageIndex` records.
///
/// Each entry records the `log_time` and offset of a message within the
/// uncompressed chunk data, enabling time-based random access.
#[derive(Debug, Clone)]
pub struct MessageIndexEntry {
    /// Message log time (nanoseconds)
    pub log_time: u64,
    /// Offset within the uncompressed chunk data
    pub offset: u64,
}

/// A compressed chunk ready for writing.
///
/// This represents a chunk that has been compressed and is ready
/// to be serialized to the MCAP file.
#[derive(Debug, Clone)]
pub struct CompressedChunk {
    /// Chunk sequence number
    pub sequence: u64,
    /// Compressed data (includes MCAP chunk header + message records)
    pub compressed_data: Vec<u8>,
    /// Uncompressed size
    pub uncompressed_size: usize,
    /// Message start time (earliest `log_time`)
    pub message_start_time: u64,
    /// Message end time (latest `log_time`)
    pub message_end_time: u64,
    /// Number of messages in this chunk
    pub message_count: usize,
    /// Compression ratio (compressed / uncompressed)
    pub compression_ratio: f64,
    /// Message indexes by channel ID for MCAP `MessageIndex` records.
    /// Maps `channel_id` -> list of (`log_time`, offset) entries.
    pub message_indexes: BTreeMap<u16, Vec<MessageIndexEntry>>,
}

impl CompressedChunk {
    /// Calculate the compression ratio.
    pub fn calculate_compression_ratio(&self) -> f64 {
        if self.uncompressed_size > 0 {
            self.compressed_data.len() as f64 / self.uncompressed_size as f64
        } else {
            1.0
        }
    }

    /// Get the compressed size in bytes.
    pub fn compressed_size(&self) -> usize {
        self.compressed_data.len()
    }
}
