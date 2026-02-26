// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 test utilities and common imports.

pub mod integration;
pub mod roboreader;
pub mod streaming;
pub mod wiremock;

use std::path::PathBuf;

/// Get the path to a test fixture file.
pub fn fixture_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures");
    path.push(name);
    path
}
