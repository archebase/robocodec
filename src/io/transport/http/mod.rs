// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP transport implementation using the Transport trait.
//!
//! This module provides [`HttpTransport`], which implements the [`Transport`]
//! trait for HTTP/HTTPS URLs. Supports range requests for seeking and buffers
//! data for efficient reading.

mod transport;

pub use transport::HttpTransport;
