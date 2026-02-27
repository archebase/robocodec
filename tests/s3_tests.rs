// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader and writer tests.
//!
//! This file is the entry point for S3 tests. The tests are organized into modules:
//! - `streaming` - Streaming parser tests (chunk boundary handling)
//! - `wiremock` - Wiremock mock server tests
//! - `integration` - S3 integration tests with MinIO
//! - `roboreader` - RoboReader S3 tests (BAG, MCAP, RRD)
//! - `streaming_reader` - StreamingRoboReader S3 tests via public API

mod s3;
