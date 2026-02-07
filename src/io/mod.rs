// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! I/O layer for robotics data formats.
//!
//! This module provides the foundational types and traits for reading
//! and writing robotics data files.

pub(crate) mod detection;
// Format modules are accessible but hidden from docs
#[doc(hidden)]
pub mod formats;
pub mod metadata;

// Streaming parser interface (unified across formats)
// Only available with remote feature since it uses FatalError from s3 module
#[cfg(feature = "remote")]
#[doc(hidden)]
pub mod streaming;

// Transport layer for different data sources
pub mod transport;

// Remote storage support (requires `remote` feature)
// Hidden from docs but accessible for advanced use and testing
#[cfg(feature = "remote")]
#[doc(hidden)]
pub mod s3;

// Re-exports
pub use metadata::{
    ChannelInfo, FileFormat, FileInfo, MessageMetadata, RawMessage, TimestampedDecodedMessage,
};

// Channel iterator (tightly coupled with pipeline - keep in roboflow)

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
