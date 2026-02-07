// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP format implementation.
//!
//! This module provides a complete MCAP reader/writer implementation with:
//! - Parallel chunk-based reading for optimal performance
//! - Sequential reading using the mcap crate
//! - S3 streaming using the mcap crate's LinearReader
//! - Automatic encoding detection and decoding
//! - Custom writer with manual chunk control for parallel compression
//!
//! **Note:** The parallel reader uses custom Rayon-based decompression for 6-8x speedup.
//! The sequential and S3 readers use the mcap crate for reliable parsing.

// Re-export constants at module level for convenience
pub use constants::{
    MCAP_MAGIC, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX, OP_DATA_END, OP_FOOTER, OP_HEADER,
    OP_MESSAGE, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};

// Constants module (pub for format/writer/mcap.rs access)
pub mod constants;

// Internal types (private to mcap format)
pub(crate) mod internal;

// Parallel reader implementation
pub mod parallel;

// Sequential reader implementation
pub mod sequential;

// Two-pass reader for files without summary
pub mod two_pass;

// Unified streaming parser (implements StreamingParser trait)
pub mod streaming;

// Transport-based reader
pub mod transport_reader;

// S3 adapter using mcap crate's LinearReader
// Private to this crate - used internally by S3Reader
pub(crate) mod s3_adapter;

// High-level API (auto-decoding reader + custom writer)
pub mod reader;
pub mod writer;

// Re-exports
pub use parallel::{ChunkIndex, ParallelMcapReader};
pub use reader::{McapFormat, McapReader, RawMessage};
pub use sequential::{SequentialMcapReader, SequentialRawIter};
pub use streaming::{
    ChannelRecordInfo, McapS3Adapter, McapStreamingParser, MessageRecord, SchemaInfo,
    StreamingMcapParser,
};
pub use transport_reader::McapTransportReader;
pub use two_pass::TwoPassMcapReader;
pub use writer::ParallelMcapWriter;

// Re-export DecodedMessage from core
pub use crate::core::DecodedMessage;
