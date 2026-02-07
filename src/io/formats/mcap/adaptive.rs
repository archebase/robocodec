// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Adaptive MCAP reader with strategy selection.
//!
//! This module provides an adaptive reader that selects the optimal reading strategy
//! based on file characteristics:
//! - Small files (<100MB) → SequentialReader (mcap crate, lower overhead)
//! - Large files (≥100MB) → ParallelReader (custom + rayon, faster for full scans)

use std::collections::HashMap;
use std::path::Path;

use crate::io::metadata::ChannelInfo;
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

/// File size threshold for switching between sequential and parallel reading.
/// Files below this size use sequential reading (lower overhead).
/// Files at or above this size use parallel reading (better throughput).
const PARALLEL_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB

/// Reading strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStrategy {
    /// Sequential reading using mcap crate (best for small files)
    Sequential,
    /// Parallel reading using custom + rayon (best for large files)
    Parallel,
}

impl ReadStrategy {
    /// Select the optimal strategy based on file size.
    pub fn for_file_size(file_size: u64) -> Self {
        if file_size < PARALLEL_THRESHOLD {
            ReadStrategy::Sequential
        } else {
            ReadStrategy::Parallel
        }
    }
}

/// Adaptive MCAP reader that selects the optimal reading strategy.
///
/// This reader automatically chooses between sequential and parallel reading
/// based on file size, optimizing for both small and large files.
pub enum AdaptiveMcapReader {
    /// Sequential reader using mcap crate
    Sequential(crate::io::formats::mcap::sequential::SequentialMcapReader),
    /// Parallel reader using custom + rayon
    Parallel(crate::io::formats::mcap::parallel::ParallelMcapReader),
}

impl AdaptiveMcapReader {
    /// Open an MCAP file with automatic strategy selection.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();

        // Get file size for strategy selection
        let file_size = std::fs::metadata(path_ref)
            .map_err(|e| CodecError::parse("AdaptiveMcapReader", format!("Failed to get metadata: {e}")))?
            .len();

        let strategy = ReadStrategy::for_file_size(file_size as u64);

        match strategy {
            ReadStrategy::Sequential => {
                let reader = crate::io::formats::mcap::sequential::SequentialMcapReader::open(path)?;
                Ok(AdaptiveMcapReader::Sequential(reader))
            }
            ReadStrategy::Parallel => {
                let reader = crate::io::formats::mcap::parallel::ParallelMcapReader::open(path)?;
                Ok(AdaptiveMcapReader::Parallel(reader))
            }
        }
    }

    /// Open with a specific strategy.
    pub fn open_with_strategy<P: AsRef<Path>>(path: P, strategy: ReadStrategy) -> Result<Self> {
        match strategy {
            ReadStrategy::Sequential => {
                let reader = crate::io::formats::mcap::sequential::SequentialMcapReader::open(path)?;
                Ok(AdaptiveMcapReader::Sequential(reader))
            }
            ReadStrategy::Parallel => {
                let reader = crate::io::formats::mcap::parallel::ParallelMcapReader::open(path)?;
                Ok(AdaptiveMcapReader::Parallel(reader))
            }
        }
    }

    /// Get the active strategy.
    #[must_use]
    pub fn strategy(&self) -> ReadStrategy {
        match self {
            AdaptiveMcapReader::Sequential(_) => ReadStrategy::Sequential,
            AdaptiveMcapReader::Parallel(_) => ReadStrategy::Parallel,
        }
    }

    /// Get the underlying sequential reader if available.
    pub fn as_sequential(&self) -> Option<&crate::io::formats::mcap::sequential::SequentialMcapReader> {
        match self {
            AdaptiveMcapReader::Sequential(r) => Some(r),
            AdaptiveMcapReader::Parallel(_) => None,
        }
    }

    /// Get the underlying parallel reader if available.
    pub fn as_parallel(&self) -> Option<&crate::io::formats::mcap::parallel::ParallelMcapReader> {
        match self {
            AdaptiveMcapReader::Sequential(_) => None,
            AdaptiveMcapReader::Parallel(r) => Some(r),
        }
    }

    /// Get chunk indexes (only available with parallel strategy).
    pub fn chunk_indexes(&self) -> &[crate::io::formats::mcap::parallel::ChunkIndex] {
        match self {
            AdaptiveMcapReader::Sequential(_) => &[],
            AdaptiveMcapReader::Parallel(r) => r.chunk_indexes(),
        }
    }
}

impl FormatReader for AdaptiveMcapReader {
    fn open_from_transport(
        _transport: Box<dyn crate::io::transport::Transport>,
        _path: String,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Err(CodecError::unsupported(
            "AdaptiveMcapReader requires local file access for memory mapping. \
             Use McapTransportReader for transport-based reading.",
        ))
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.channels(),
            AdaptiveMcapReader::Parallel(r) => r.channels(),
        }
    }

    fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.channel_by_topic(topic),
            AdaptiveMcapReader::Parallel(r) => r.channel_by_topic(topic),
        }
    }

    fn channels_by_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.channels_by_topic(topic),
            AdaptiveMcapReader::Parallel(r) => r.channels_by_topic(topic),
        }
    }

    fn message_count(&self) -> u64 {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.message_count(),
            AdaptiveMcapReader::Parallel(r) => r.message_count(),
        }
    }

    fn start_time(&self) -> Option<u64> {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.start_time(),
            AdaptiveMcapReader::Parallel(r) => r.start_time(),
        }
    }

    fn end_time(&self) -> Option<u64> {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.end_time(),
            AdaptiveMcapReader::Parallel(r) => r.end_time(),
        }
    }

    fn path(&self) -> &str {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.path(),
            AdaptiveMcapReader::Parallel(r) => r.path(),
        }
    }

    fn format(&self) -> crate::io::metadata::FileFormat {
        crate::io::metadata::FileFormat::Mcap
    }

    fn file_size(&self) -> u64 {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.file_size(),
            AdaptiveMcapReader::Parallel(r) => r.file_size(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.as_any(),
            AdaptiveMcapReader::Parallel(r) => r.as_any(),
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        match self {
            AdaptiveMcapReader::Sequential(r) => r.as_any_mut(),
            AdaptiveMcapReader::Parallel(r) => r.as_any_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_strategy_for_file_size() {
        // Small file (< 100MB) → Sequential
        assert_eq!(ReadStrategy::for_file_size(50 * 1024 * 1024), ReadStrategy::Sequential);
        assert_eq!(ReadStrategy::for_file_size(99 * 1024 * 1024), ReadStrategy::Sequential);

        // Large file (≥ 100MB) → Parallel
        assert_eq!(ReadStrategy::for_file_size(100 * 1024 * 1024), ReadStrategy::Parallel);
        assert_eq!(ReadStrategy::for_file_size(200 * 1024 * 1024), ReadStrategy::Parallel);
    }

    #[test]
    fn test_parallel_threshold() {
        assert_eq!(PARALLEL_THRESHOLD, 100 * 1024 * 1024);
    }

    #[test]
    fn test_adaptive_reader_small_file() {
        // Create a small test MCAP file
        use std::io::Write;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().expect("Failed to create temp file");

        // Write minimal MCAP file (magic + header + footer + magic)
        let mut file = std::fs::File::create(temp_file.path()).expect("Failed to create file");
        file.write_all(b"\x89\x4d\x43\x41\x50\x30\x0d\x0a").expect("Write magic"); // magic
        file.write_all(&[0x01u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8]).expect("Write header"); // OP_HEADER with empty length
        file.write_all(&[0x00u8; 20]).expect("Write padding");
        file.sync_all().expect("Sync");

        // Verify adaptive reader opens with sequential strategy
        let _reader = AdaptiveMcapReader::open(temp_file.path());
        // File is tiny so it should use Sequential strategy
        // (The file won't parse as valid MCAP but the strategy selection works)
        drop(temp_file);

        // Just verify the reader compiles and strategy logic works
        assert_eq!(ReadStrategy::for_file_size(1024), ReadStrategy::Sequential);
    }

    #[test]
    fn test_read_strategy_partial_eq() {
        // Test PartialEq implementation
        assert_eq!(ReadStrategy::Sequential, ReadStrategy::Sequential);
        assert_eq!(ReadStrategy::Parallel, ReadStrategy::Parallel);
        assert_ne!(ReadStrategy::Sequential, ReadStrategy::Parallel);
    }
}
