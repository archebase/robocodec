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
//! // Iterate over decoded messages
//! for result in reader.decode_messages()? {
//!     let (message, channel) = result?;
//!     println!("Topic: {}, Data: {:?}", channel.topic, message);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

//! Constants for RRD file format.
pub mod constants;

/// Reader implementation.
pub mod reader;

/// Writer implementation.
pub mod writer;

// Re-exports
pub use reader::{DecodedMessageWithTimestampStream, RrdFormat, RrdReader};
pub use writer::RrdWriter;
