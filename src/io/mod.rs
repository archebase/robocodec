// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! I/O layer for robotics data formats.
//!
//! This module provides the foundational types and traits for reading
//! and writing robotics data files.

pub mod arena;
pub(crate) mod detection;
// Format modules are accessible but hidden from docs
#[doc(hidden)]
pub mod formats;
pub mod metadata;

// Transport layer for different data sources
pub mod transport;

// S3 streaming support (requires `s3` feature)
// Hidden from docs but accessible for advanced use and testing
#[cfg(feature = "s3")]
#[doc(hidden)]
pub mod s3;

// Re-exports
pub use arena::{MmapArena, MmapArenaRef};
// Format detection is internal - users use auto-detection via RoboReader
pub(crate) use detection::{FormatDetector, detect_format, is_bag_file, is_mcap_file, is_rrd_file};
pub use metadata::{
    ChannelInfo, FileFormat, FileInfo, MessageMetadata, RawMessage, TimestampedDecodedMessage,
};

// Channel iterator (tightly coupled with pipeline - keep in roboflow)
// pub mod channel_iterator;

// Traits for format readers and writers
// Hidden from docs but accessible for advanced use
pub mod traits;
#[doc(hidden)]
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
