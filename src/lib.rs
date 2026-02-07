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
//! - **[`TransformBuilder`]** - Topic/type renaming and transformations
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

// Re-export core types for convenience
pub use core::{CodecError, CodecValue, DecodedMessage, Encoding, PrimitiveType, Result};

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
pub use io::reader::{DecodedMessageIter, ReaderConfig};
pub use io::writer::{RoboWriter, WriterConfig};

// Format traits are available but hidden from documentation
// Users don't need to import these - methods work directly on RoboReader/RoboWriter
#[doc(hidden)]
pub use io::traits::FormatReader;
#[doc(hidden)]
pub use io::traits::FormatWriter;

// Rewriter support (shared types and traits)
pub mod rewriter;

pub use rewriter::{FormatRewriter, RewriteOptions, RewriteStats, RoboRewriter};

pub use transform::{
    MultiTransform, TopicRenameTransform, TransformBuilder, TransformError, TransformedChannel,
    TypeNormalization, TypeRenameTransform,
};

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
/// # use robocodec::{Decoder, DecodedMessage, CodecError};
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
    /// # use robocodec::{Decoder, CodecError};
    /// # fn test(decoder: &dyn Decoder, data: &[u8]) -> Result<(), CodecError> {
    /// let schema = "int32 value\nstring name";
    /// let message = decoder.decode(data, schema, Some("test/Type"))?;
    /// # Ok(())
    /// # }
    /// ```
    fn decode(&self, data: &[u8], schema: &str, type_name: Option<&str>) -> Result<DecodedMessage>;
}

// Python bindings (optional feature)
#[cfg(feature = "python")]
pub mod python;
