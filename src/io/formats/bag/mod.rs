// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! BAG format implementation.
//!
//! This module provides a complete ROS1 bag reader/writer implementation with:
//! - Parallel chunk-based reading for optimal performance
//! - Sequential reading
//! - Custom writer with manual chunk control for parallel compression

// Parallel reader implementation
pub mod parallel;

// Parser utilities
pub mod parser;

// Sequential reader implementation
pub mod sequential;

// Streaming parser (transport-agnostic)
#[cfg(feature = "remote")]
pub mod stream;

// Transport-based reader (S3, HTTP support)
#[cfg(feature = "remote")]
pub mod transport_reader;

// Writer implementation
pub mod writer;

// Re-exports
pub use parallel::{
    BagDecodedMessageIter, BagDecodedMessageStream, BagDecodedMessageWithTimestampIter,
    BagDecodedMessageWithTimestampStream, BagFormat, BagRawIter, ParallelBagReader,
};
pub use sequential::{BagSequentialFormat, SequentialBagRawIter, SequentialBagReader};
#[cfg(feature = "remote")]
pub use stream::{
    BAG_MAGIC_PREFIX, BagMessageRecord, BagRecord, BagRecordFields, BagRecordHeader,
    StreamingBagParser,
};
#[cfg(feature = "remote")]
pub use transport_reader::BagTransportReader;
pub use writer::{BagMessage, BagWriter};
