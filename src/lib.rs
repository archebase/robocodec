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
// Allow certain pedantic lints that are unavoidable in robotics code:
// - Cast precision loss: Converting timestamps between u64/i64/f64 is common
// - Size truncation: u64 to usize/u32 casts are necessary for indexing and serialization
// - Function lines: Some functions are complex by nature (e.g., parsers)
// - HashMap hasher: Using default hasher is appropriate for this use case
// - Unused self: Some trait methods require self even when not used
// - Self recursion: Helper functions often use self recursively
// - Let...else: The suggested pattern is less readable in many cases
// - Identical match arms: Some arms have identical bodies for different variants
// - Ref options: Using &Option<T> is intentional for performance in some cases
// - Items after statements: Test helpers are often defined after use
// - Unnecessary Result: Some functions return Result for API consistency
// - Wildcard matches: Some enums only have one variant currently
// - Unused return: Some functions return values that may be used by callers
// - Inefficient clone: Performance trade-offs are intentional for clarity
// - Must use: Public API methods are already documented with #[must_use]
// - Unused async: Required for trait compatibility
// - Pass by ref: Small types passed by ref for API consistency
// - Case-sensitive ext: File extension checks are intentional
// - String append: format! append is intentional for clarity
// - Field prefix: Struct fields use consistent prefixes
// - Argument not consumed: Arguments may be kept for API consistency
// - Wildcard enum matches: Match arms are complete for current variants
// - Underscore binding: Intentional use of underscore-prefixed names
// - Missing panic docs: Panics are rare and documented in code
// - Missing debug fields: Some Debug impls exclude internal fields
// - Long literals: Constants with specific values
// - Redundant continue: Explicit continue improves readability
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::implicit_hasher)]
#![allow(clippy::unused_self)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::ref_option)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::unnecessary_wraps)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::clone_on_copy)]
#![allow(clippy::assigning_clones)]
#![allow(clippy::unused_async)]
#![allow(clippy::trivially_copy_pass_by_ref)]
#![allow(clippy::case_sensitive_file_extension_comparisons)]
#![allow(clippy::format_push_string)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::needless_continue)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::single_match)]
#![allow(clippy::single_match_else)]
#![allow(clippy::manual_assert)]

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
pub use io::writer::{HttpAuthConfig, RoboWriter, WriterConfig};

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
