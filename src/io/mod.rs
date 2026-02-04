// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! I/O layer for robotics data formats.
//!
//! This module provides the foundational types and traits for reading
//! and writing robotics data files.

pub mod arena;
pub mod detection;
pub mod formats;
pub mod metadata;

// S3 streaming support (requires `s3` feature)
#[cfg(feature = "s3")]
pub mod s3;

// Re-exports
pub use arena::{MmapArena, MmapArenaRef};
pub use detection::{detect_format, is_bag_file, is_mcap_file, FormatDetector};
pub use metadata::{
    ChannelInfo, FileFormat, FileInfo, MessageMetadata, RawMessage, TimestampedDecodedMessage,
};

// Re-export S3 types when `s3` feature is enabled
#[cfg(feature = "s3")]
pub use s3::{S3Client, S3Location, S3Reader, S3ReaderConfig, S3ReaderState};

// Channel iterator (tightly coupled with pipeline - keep in roboflow)
// pub mod channel_iterator;

// Traits for format readers and writers
pub mod traits;
pub use traits::{FormatReader, FormatWriter};

// Re-export parallel reader types
pub use traits::{MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats};

// Filter for topic filtering
pub mod filter;
pub use filter::{ChannelFilter, TopicFilter};

// Unified reader/writer with auto-detection
pub mod reader;
pub mod writer;

// Reader exports
pub use reader::{DecodedMessageIter, ReaderConfig, ReaderConfigBuilder, RoboReader};

// Writer exports
pub use writer::{RoboWriter, WriteStrategy, WriterBuilder, WriterConfig, WriterConfigBuilder};
