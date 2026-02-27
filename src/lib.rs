// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Robocodec
//!
//! **Robocodec** is a high-performance robotics data codec library for reading, writing,
//! and converting robotics data files. It provides a unified, format-agnostic API with
//! automatic format detection and support for multiple encodings and schema types.
//!
//! ## Features
//!
//! - **Unified API** - Single interface for MCAP, ROS1 bag, and RRF2 formats
//! - **Auto-Detection** - Format detected from file extension or URL scheme
//! - **Remote Support** - First-class S3 support with streaming
//! - **Fast** - Parallel processing with rayon, zero-copy memory-mapped files
//! - **Transformations** - Topic/type renaming and format conversion built-in
//! - **Schema Support** - ROS `.msg`, ROS2 IDL, and OMG IDL
//! - **Encodings** - CDR, Protobuf, and JSON
//!
//! ## Supported Formats
//!
//! | Format | Read | Write | Notes |
//! |:--------|:-----|:-------|:------|
//! | MCAP | ✅ | ✅ | Robotics message format |
//! | ROS1 Bag | ✅ | ✅ | ROS1 legacy format |
//! | RRF2 | ✅ | ✅ | Rerun format (0.27+) |
//!
//! ## Quick Start
//!
//! ### Reading Messages
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::{RoboReader, FormatReader};
//!
//! // Format auto-detected from extension
//! let reader = RoboReader::open("file.mcap")?;
//!
//! // Inspect file metadata
//! println!("Channels: {}", reader.channels().len());
//! println!("Messages: {}", reader.message_count());
//!
//! // Iterate over decoded messages with timestamps
//! for result in reader.decoded()? {
//!     let msg = result?;
//!     println!("{}: {}", msg.channel.topic, msg.message.len());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### Writing Messages
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::{RoboWriter, FormatWriter};
//!
//! // Format detected from extension (.mcap, .bag, or .rrd)
//! let mut writer = RoboWriter::create("output.mcap")?;
//!
//! // Add a channel and write messages
//! let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;
//! // ... write messages ...
//! writer.finish()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Reading from S3
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::{RoboReader, FormatReader};
//!
//! // Read directly from S3 (requires `remote` feature, enabled by default)
//! let reader = RoboReader::open("s3://my-bucket/path/to/data.mcap")?;
//! println!("Found {} channels", reader.channels().len());
//!
//! // Custom endpoint (MinIO, Alibaba OSS, etc.)
//! let reader = RoboReader::open(
//!     "s3://bucket/data.mcap?endpoint=https://oss-cn-hangzhou.aliyuncs.com"
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Rewriting with Transformations
//!
//! Rewrite a file while applying topic and type transformations:
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::RoboRewriter;
//!
//! // Format detected from input extension
//! let mut rewriter = RoboRewriter::open("input.mcap")?;
//! let stats = rewriter.rewrite("output.mcap")?;
//! println!("Processed {} messages", stats.message_count);
//! # Ok(())
//! # }
//! ```
//!
//! ### Topic and Type Transformations
//!
//! ```rust,no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::{RoboRewriter, TransformBuilder, RewriteOptions};
//!
//! // Rename topics and types during conversion
//! let transform = TransformBuilder::new()
//!     .with_topic_rename("/old/topic", "/new/topic")
//!     .with_type_rename("OldType", "NewType")
//!     .build();
//!
//! let options = RewriteOptions::default().with_transforms(transform);
//! let mut rewriter = RoboRewriter::with_options("input.mcap", options)?;
//! rewriter.rewrite("output.mcap")?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature Flags
//!
//! | Feature | Default | Description |
//! |:--------|:--------|:------------|
//! | `remote` | ✅ Yes | S3 and HTTP/HTTPS transport support |
//! | `python` | ❌ No | Python bindings via PyO3 |
//! | `jemalloc` | ❌ No | Use jemalloc allocator (Linux only) |
//!
//! Disable default features:
//!
//! ```toml
//! [dependencies]
//! robocodec = { version = "0.1", default-features = false }
//! ```
//!
//! Enable specific features:
//!
//! ```toml
//! [dependencies]
//! robocodec = { version = "0.1", features = ["python", "jemalloc"] }
//! ```
//!
//! ## Public API
//!
//! The library exports these key types:
//!
//! - **[`RoboReader`]** - Unified reader with format auto-detection
//! - **[`RoboWriter`]** - Unified writer with format auto-detection
//! - **[`RoboRewriter`]** - Unified rewriter for format conversion
//! - **[`TransformBuilder`]** - Builder for topic/type transformations
//! - **[`DecodedMessageResult`]** - Message data with metadata and timestamps
//! - **[`ChannelInfo`]** - Channel/topic metadata
//! - **[`ReaderConfig`]** - Configuration for readers (parallel processing, chunk merging)
//! - **[`WriterConfig`]** - Configuration for writers
//! - **[`DecodedMessage`]** - Decoded message field name to value mapping
//! - **[`CodecValue`]** - Value type for decoded message fields
//!
//! ## S3 Authentication
//!
//! For S3-compatible storage, set credentials via environment variables:
//!
//! ```bash
//! export AWS_ACCESS_KEY_ID="your-access-key"
//! export AWS_SECRET_ACCESS_KEY="your-secret-key"
//! export AWS_REGION="us-east-1"  // optional, defaults to us-east-1
//! ```
//!
//! Works with AWS S3, Alibaba Cloud OSS, MinIO, and other S3-compatible services.

// Core types
//
// Clippy lint allowances for robotics data codec library:
//
// Performance and API design:
#![allow(clippy::cast_precision_loss)] // Timestamp conversions u64/i64/f64
#![allow(clippy::cast_possible_truncation)] // u64 to usize/u32 for indexing
#![allow(clippy::cast_sign_loss)] // u64/i64 timestamp conversions
#![allow(clippy::trivially_copy_pass_by_ref)] // Small types, API consistency
#![allow(clippy::clone_on_copy)] // intentional for API clarity
#![allow(clippy::assigning_clones)] // intentional for API clarity
#![allow(clippy::must_use_candidate)] // Public API has #[must_use] docs
#![allow(clippy::unused_async)] // Trait compatibility
//
// Code structure patterns:
#![allow(clippy::too_many_lines)] // Complex parsers need space
#![allow(clippy::match_same_arms)] // Identical arms for different variants
#![allow(clippy::items_after_statements)] // Test helpers defined after use
#![allow(clippy::ref_option)] // &Option<T> for performance
#![allow(clippy::struct_field_names)] // Consistent field prefixes
//
// Documentation and testing:
#![allow(clippy::missing_panics_doc)] // Panics rare, documented in code
#![allow(clippy::missing_errors_doc)] // Errors documented in type
#![allow(clippy::wildcard_imports)] // Test modules only

pub mod core;

// Re-export core error type for public API
pub use core::{CodecError, Result};

// Re-export core value types (decoded message representation)
pub use core::value::{CodecValue, DecodedMessage};

// Encoding/decoding (hidden from docs but available for advanced use)
#[doc(hidden)]
pub mod encoding;

// Schema parsing (hidden from docs but available for advanced use)
#[doc(hidden)]
pub mod schema;

// Message transformations
pub mod transform;

// I/O types (private implementation, but accessible for testing/advanced use)
#[doc(hidden)]
pub mod io;

// Re-export key public API types at top level
pub use io::RoboReader;
pub use io::metadata::{ChannelInfo, DecodedMessageResult};
pub use io::reader::ReaderConfig;
pub use io::writer::{RoboWriter, WriterConfig};

// Streaming API (requires `remote` feature)
#[cfg(feature = "remote")]
pub use io::streaming::{
    AlignedFrame, FrameAlignmentConfig, ImageData, ProgressEvent, ProgressTracker, StreamConfig,
    StreamEvent, StreamMode, StreamingRoboReader, TimestampedMessage,
};

// Format traits are available but hidden from documentation
// Users don't need to import these - methods work directly on RoboReader/RoboWriter
#[doc(hidden)]
pub use io::traits::FormatReader;
#[doc(hidden)]
pub use io::traits::FormatWriter;

// Rewriter support
pub mod rewriter;

// Public rewriter API - keep only what users need
pub use rewriter::{RewriteOptions, RewriteStats, RoboRewriter};

// Transformation builder and error type only
pub use transform::{TransformBuilder, TransformError};

// Format-specific modules are private implementation details
// Use RoboReader/RoboWriter for a unified interface

/// Decoder trait for generic decoding operations.
///
/// This trait provides a unified interface for decoding binary message data
/// into structured `DecodedMessage` objects.
///
/// # Example
///
/// ```no_run
/// # use robocodec::{Decoder, CodecError, DecodedMessage};
/// # struct MyDecoder;
/// # impl Decoder for MyDecoder {
/// #     fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage, CodecError> {
/// #         Ok(DecodedMessage::new())
/// #     }
/// # }
/// # fn test(decoder: &MyDecoder, data: &[u8]) -> Result<(), CodecError> {
/// let schema = "string data";
/// let message = decoder.decode(data, schema, Some("std_msgs/String"))?;
/// # Ok(())
/// # }
/// ```
pub trait Decoder: Send + Sync {
    /// Decode data into a `DecodedMessage`.
    ///
    /// # Arguments
    ///
    /// * `data` - Binary encoded message data
    /// * `schema` - Schema definition for the message type
    /// * `type_name` - Optional name of the message type
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data cannot be decoded according to the schema
    /// - The schema is invalid or malformed
    /// - The type name is not recognized
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::{Decoder, CodecError, DecodedMessage};
    /// # fn test(decoder: &dyn Decoder, data: &[u8]) -> Result<(), CodecError> {
    /// let schema = "int32 value\nstring name";
    /// let message = decoder.decode(data, schema, Some("test/Type"))?;
    /// # Ok(())
    /// # }
    /// ```
    fn decode(
        &self,
        data: &[u8],
        schema: &str,
        type_name: Option<&str>,
    ) -> Result<core::DecodedMessage>;
}

// Python bindings (optional feature)
#[cfg(feature = "python")]
pub mod python;
