// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP transport implementation using the Transport trait.
//!
//! This module provides [`HttpTransport`], which implements the [`Transport`](crate::io::transport::Transport)
//! trait for HTTP/HTTPS URLs. Supports range requests for seeking and buffers
//! data for efficient reading.
//!
//! It also provides [`HttpWriter`] for writing robotics data files to HTTP/HTTPS
//! URLs using the [`FormatWriter`](crate::io::traits::FormatWriter) trait.

mod transport;
mod upload_strategy;
mod writer;

pub use transport::{HttpAuth, HttpTransport};
pub use upload_strategy::HttpUploadStrategy;
pub use writer::{HttpWriteError, HttpWriter};
