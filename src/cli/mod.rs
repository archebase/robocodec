// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CLI support utilities.
//!
//! This module is only available when the `cli` feature is enabled.
//! It provides shared utilities for the robocodec CLI that are not
//! part of the public library API.

#[cfg(feature = "cli")]
mod output;
#[cfg(feature = "cli")]
mod progress;
#[cfg(feature = "cli")]
mod time;

#[cfg(feature = "cli")]
pub use output::output_json_or;
#[cfg(feature = "cli")]
pub use progress::Progress;

/// Open a file with automatic format detection.
///
/// Convenience wrapper around `RoboReader::open` that provides better
/// error messages for invalid paths.
#[cfg(feature = "cli")]
pub fn open_reader(path: &std::path::Path) -> Result<crate::RoboReader> {
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 path: {:?}", path))?;
    Ok(crate::RoboReader::open(path_str)?)
}
#[cfg(feature = "cli")]
pub use time::{format_duration, format_timestamp, parse_time_range, parse_timestamp};

#[cfg(feature = "cli")]
pub use anyhow::Result as CliResult;

#[cfg(feature = "cli")]
pub type Result<T = ()> = CliResult<T>;
