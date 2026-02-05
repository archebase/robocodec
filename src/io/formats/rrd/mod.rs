// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Rerun RRD (Rerun Data) file format implementation.
//!
//! This module provides a complete RRD reader/writer implementation with:
//! - Sequential reading using the Rerun SDK
//! - Custom writer with chunk-based storage
//! - Automatic encoding detection and decoding
//!
//! # RRD File Format
//!
//! The RRD format is Rerun's native file format for storing time-series data.
//! It is based on LZ4-compressed chunks containing protobuf-encoded messages.
//!
//! ## File Structure
//!
//! ```text
//! [Header: Magic + Version]
//! [Schema: Message definitions]
//! [Chunk 1: LZ4 compressed messages]
//! [Chunk 2: LZ4 compressed messages]
//! ...
//! [Index: Chunk offsets for fast seeking]
//! [Footer: Magic + Metadata]
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::formats::rrd::RrdFormat;
//!
//! // Open an RRD file
//! let reader = RrdFormat::open("data.rrd")?;
//!
//! // Iterate over decoded messages with timestamps
//! let decoded_iter = reader.decode_messages_with_timestamp()?;
//! let mut stream = decoded_iter.stream()?;
//!
//! while let Some(result) = stream.next() {
//!     let (message, channel) = result?;
//!     println!("Topic: {}, Log Time: {:?}", channel.topic, message.log_time);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

//! Constants for RRD file format.
pub mod constants;

/// ArrowMsg protobuf encoding/decoding with LZ4 compression.
pub mod arrow_msg;

/// Parallel reader implementation.
pub mod parallel;

/// Reader implementation.
pub mod reader;

/// Streaming parser (transport-agnostic).
pub mod stream;

/// Writer implementation.
pub mod writer;

// Re-exports
pub use arrow_msg::{ArrowCompression, ArrowMsg};
pub use parallel::{MessageIndex, ParallelRrdReader};
pub use reader::{DecodedMessageWithTimestampStream, RrdFormat, RrdReader};
pub use stream::{
    Compression, MessageKind, RRD_STREAM_MAGIC, RrdMessageRecord, RrdStreamHeader,
    StreamingRrdParser,
};
pub use writer::{RrdCompression as WriterCompression, RrdWriter};
