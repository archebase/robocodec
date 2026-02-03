// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Robocodec
//!
//! Robotics data format library for MCAP and ROS bag files.
//!
//! This library provides a unified interface for reading and writing robotics data files:
//! - **[`RoboReader`]** - Auto-detects format and provides unified message iteration
//! - **[`RoboWriter`]** - Auto-detects format from extension
//! - **[`RoboRewriter`]** - Unified rewriter with format auto-detection
//! - **[`Transform`]** - Topic/type renaming and transformations
//!
//! ## Example: Reading with Auto-Detection
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::RoboReader;
//! use robocodec::io::FormatReader;
//!
//! // Format auto-detected
//! let reader = RoboReader::open("file.mcap")?;
//! println!("Channels: {}", reader.channels().len());
//!
//! // Iterate over decoded messages
//! for result in reader.decoded()? {
//!     let decoded = result?;
//!     println!("Topic: {}", decoded.topic());
//!     println!("Data: {:?}", decoded.message);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: Writing with Auto-Detection
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::io::FormatWriter;
//! use robocodec::RoboWriter;
//!
//! // Format detected from extension (.mcap or .bag)
//! let mut writer = RoboWriter::create("output.mcap")?;
//! let channel_id = writer.add_channel("/topic", "type", "cdr", None)?;
//! writer.finish()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: Rewriting with Transformations
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::RoboRewriter;
//!
//! let mut rewriter = RoboRewriter::open("input.mcap")?;
//! rewriter.rewrite("output.mcap")?;
//! # Ok(())
//! # }
//! ```

// Core types
pub mod core;

// Re-export core types for convenience
pub use core::{CodecError, CodecValue, DecodedMessage, Encoding, PrimitiveType, Result};

// Encoding/decoding
pub mod encoding;

// Schema parsing
pub mod schema;

// Message transformations
pub mod transform;

// Pipeline types (arena, chunk, buffer pool)
pub mod types;

// I/O types (arena, metadata, traits, reader/writer strategies, etc.)
pub mod io;

// Re-export key I/O types
pub use io::metadata::{ChannelInfo, DecodedMessageResult, FileFormat, FileInfo, MessageMetadata};
pub use io::reader::{
    DecodedMessageIter, DecodedMessageWithTimestampIter, ReaderConfig, ReaderConfigBuilder,
};
pub use io::traits::{FormatReader, FormatWriter};
pub use io::{MmapArena, MmapArenaRef, RoboReader, RoboWriter};

// Rewriter support (shared types and traits)
pub mod rewriter;

pub use rewriter::{FormatRewriter, RewriteOptions, RewriteStats, RoboRewriter};

pub use transform::{
    MultiTransform, TopicRenameTransform, TransformBuilder, TransformError, TransformedChannel,
    TypeNormalization, TypeRenameTransform,
};

// Format-specific modules (available but not re-exported at top level)
// Use RoboReader/RoboWriter for a unified interface
pub use io::formats::bag;
pub use io::formats::mcap;

/// Decoder trait for generic decoding operations.
pub trait Decoder: Send + Sync {
    /// Decode data into a DecodedMessage.
    fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage>;
}

// Python bindings (optional feature)
#[cfg(feature = "python")]
pub mod python;
