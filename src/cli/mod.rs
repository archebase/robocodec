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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "cli")]
    #[test]
    fn test_open_reader_nonexistent_file() {
        let path = std::path::Path::new("/nonexistent/file/path.mcap");
        let result = open_reader(path);
        assert!(result.is_err());
    }

    #[cfg(feature = "cli")]
    #[test]
    fn test_open_reader_empty_path() {
        let path = std::path::Path::new("");
        let result = open_reader(path);
        assert!(result.is_err());
    }

    #[cfg(feature = "cli")]
    #[test]
    fn test_open_reader_invalid_utf8() {
        // Create a path with invalid UTF-8 (this is tricky on some systems)
        // On most systems, we can't actually create an invalid UTF-8 path
        // But we can test with a valid path that doesn't exist
        let path = std::path::Path::new("test\0.mcap"); // Null byte makes it invalid
        let result = open_reader(path);
        // Should either error on invalid UTF-8 or file not found
        assert!(result.is_err());
    }

    #[cfg(feature = "cli")]
    #[test]
    fn test_result_type_alias() {
        // Test that Result type alias works
        let _result: Result<()> = Ok(());
        let _result2: Result<i32> = Ok(42);
    }

    #[cfg(feature = "cli")]
    #[test]
    fn test_open_reader_relative_path() {
        let path = std::path::Path::new("nonexistent.bag");
        let result = open_reader(path);
        assert!(result.is_err());
    }
}
