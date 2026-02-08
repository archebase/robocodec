// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CLI utilities for the robocodec command-line interface.

use robocodec::RoboReader;

pub mod output;
pub mod progress;
pub mod time;

pub use anyhow::Result;

pub use output::output_json_or;
pub use progress::Progress;
pub use time::{format_duration, format_timestamp, parse_time_range};

/// Open a file with automatic format detection.
///
/// Convenience wrapper around `RoboReader::open` that provides better
/// error messages for invalid paths.
pub fn open_reader(path: &std::path::Path) -> Result<RoboReader> {
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 path: {:?}", path))?;
    Ok(RoboReader::open(path_str)?)
}
