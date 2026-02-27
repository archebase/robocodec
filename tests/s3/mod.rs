// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 test utilities and common imports.

pub mod integration;
pub mod parity;
pub mod performance;
pub mod roboreader;
pub mod streaming;
pub mod streaming_reader;
pub mod wiremock;

use std::path::PathBuf;

/// Return whether strict S3 tests are required in this run.
pub fn require_live_s3() -> bool {
    std::env::var("ROBOCODEC_REQUIRE_S3")
        .ok()
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true"
        })
        .unwrap_or(false)
}

/// Get the path to a test fixture file.
pub fn fixture_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures");
    path.push(name);
    path
}
