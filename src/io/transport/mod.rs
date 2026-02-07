// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transport layer for robotics data formats.
//!
//! This module provides a generic abstraction over different data sources
//! (local files, S3, HTTP, etc.) that can be used by format-specific parsers.
//!
//! # Architecture
//!
//! - **[`Transport`]** - Async trait for unified byte I/O
//! - **[`TransportExt`]** - Convenience extension trait
//! - **[`local`]** - Local file transport implementation
//! - **[`s3`]** - S3 transport implementation (requires `remote` feature)
//! - **[`http`]** - HTTP transport implementation (requires `remote` feature)
//! - **[`memory`]** - In-memory transport implementation for testing

pub mod core;
pub mod local;

// Remote transport modules (require remote feature)
#[cfg(feature = "remote")]
pub mod http;
#[cfg(feature = "remote")]
pub mod s3;

// Memory transport for testing (requires remote feature for bytes dependency)
#[cfg(feature = "remote")]
pub mod memory;

// Re-export core transport types
pub use core::{Transport, TransportExt};
// Re-export transport implementations
#[cfg(feature = "remote")]
pub use http::HttpTransport;
#[cfg(feature = "remote")]
pub use memory::MemoryTransport;

/// Generic byte stream trait for reading data from various transports.
///
/// This trait abstracts over different data sources (local files, S3, HTTP, etc.)
/// allowing format-specific parsers to work with any transport.
///
/// # Example
///
/// The async `Transport` trait is the primary API:
///
/// ```rust,no_run
/// use robocodec::io::transport::{Transport, TransportExt, local::LocalTransport};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Local file transport
/// let mut stream = LocalTransport::open("data.mcap")?;
/// let mut buffer = vec![0u8; 1024];
/// let n = stream.read(&mut buffer).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(test)]
mod tests {
    // Tests have been removed along with ByteStream, ByteStreamExt, and ChunkIterator
}
