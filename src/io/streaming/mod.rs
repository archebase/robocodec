// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified streaming parser interface for robotics data formats.
//!
//! This module provides the [`StreamingParser`] trait, which abstracts
//! streaming parsing for different robotics data formats (MCAP, BAG, RRD).
//!
//! # Architecture
//!
//! The streaming parser interface allows format-specific parsers to work
//! with chunk-based data sources (like S3) where the entire file isn't
//! available at once.
//!
//! ## Example
//!
//! ```rust,no_run
//! use robocodec::io::streaming::StreamingParser;
//! use robocodec::io::formats::mcap::streaming::McapStreamingParser;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut parser = McapStreamingParser::new();
//!
//! // Feed chunks as they arrive from S3
//! let chunk = b"some MCAP data";
//! for message in parser.parse_chunk(chunk)? {
//!     // Process message
//!     println!("Got message from channel {}", message.channel_id);
//! }
//! # Ok(())
//! # }
//! ```

pub mod parser;

// Re-export the core trait
pub use parser::{AsStreamingParser, StreamingParser};
