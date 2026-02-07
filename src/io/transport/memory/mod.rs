// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! In-memory transport implementation for testing.
//!
//! This module provides [`MemoryTransport`], which implements the [`Transport`]
//! trait for in-memory byte data. This is primarily useful for testing format
//! readers without needing actual files or network access.
//!
//! # Features
//!
//! - **Zero-copy**: Data is stored entirely in memory
//! - **Instant operations**: All operations complete immediately (no async overhead)
//! - **Seekable**: Full seek support within the data
//! - **Known length**: Length is always known
//!
//! # Example
//!
//! ```rust
//! use robocodec::io::transport::{memory::MemoryTransport, Transport, TransportExt};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create from owned bytes
//! let data = b"hello world".to_vec();
//! let mut transport = MemoryTransport::new(data);
//!
//! // Create from slice
//! let mut transport = MemoryTransport::from_slice(b"test data");
//!
//! // Read from memory
//! let mut buf = vec![0u8; 5];
//! let n = transport.read(&mut buf).await?;
//! assert_eq!(&buf, b"hello");
//! # Ok(())
//! # }
//! ```

mod transport;

// Re-export the memory transport
pub use transport::MemoryTransport;
