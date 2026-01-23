// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Common utilities for CLI commands.

use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};

use robocodec::io::metadata::FileFormat;
use robocodec::RoboReader;

pub use anyhow::Result as CliResult;
pub type Result<T = ()> = CliResult<T>;

/// Result type with String error for validation functions.
pub type ParseResult<T> = std::result::Result<T, String>;

/// Format output for human-readable display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Json,
}

/// Command context shared across subcommands.
pub struct CommandContext {
    /// Output format for results
    pub output_format: OutputFormat,
    /// Whether to show progress bars
    pub progress: bool,
    /// Quiet mode (minimal output)
    pub quiet: bool,
}

impl Default for CommandContext {
    fn default() -> Self {
        Self {
            output_format: OutputFormat::Human,
            progress: true,
            quiet: false,
        }
    }
}

impl CommandContext {
    /// Create a new context with specified settings.
    pub fn new(output_format: OutputFormat, progress: bool, quiet: bool) -> Self {
        Self {
            output_format,
            progress,
            quiet,
        }
    }

    /// Create a context from CLI flags.
    pub fn from_flags(json: bool, no_progress: bool, quiet: bool) -> Self {
        Self {
            output_format: if json {
                OutputFormat::Json
            } else {
                OutputFormat::Human
            },
            progress: !no_progress && !quiet && !json,
            quiet,
        }
    }

    /// Check if progress bars should be shown.
    pub fn should_show_progress(&self) -> bool {
        self.progress && std::io::stderr().is_terminal()
    }
}

/// Detect file format from extension or content.
pub fn detect_format(path: &Path) -> FileFormat {
    // First try extension
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        match ext.to_lowercase().as_str() {
            "mcap" => return FileFormat::Mcap,
            "bag" => return FileFormat::Bag,
            _ => {}
        }
    }

    // Try to open as MCAP first, then BAG
    if RoboReader::open(path).is_ok() {
        return FileFormat::Mcap;
    }

    FileFormat::Unknown
}

/// Format a duration in nanoseconds to human-readable string.
pub fn format_duration(nanos: u64) -> String {
    let secs = nanos / 1_000_000_000;
    let millis = (nanos % 1_000_000_000) / 1_000_000;

    if secs >= 3600 {
        let hours = secs / 3600;
        let minutes = (secs % 3600) / 60;
        format!("{}h {}m", hours, minutes)
    } else if secs >= 60 {
        let minutes = secs / 60;
        let remaining_secs = secs % 60;
        format!("{}m {}s", minutes, remaining_secs)
    } else if secs > 0 {
        format!("{}.{:03}s", secs, millis)
    } else {
        format!("{}ms", millis)
    }
}

/// Format a byte count to human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a timestamp in nanoseconds to human-readable string.
pub fn format_timestamp(nanos: u64) -> String {
    let secs = nanos / 1_000_000_000;
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0);

    match datetime {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("{} ns", nanos),
    }
}

/// Parse a timestamp string to nanoseconds.
///
/// Accepts:
/// - Unix timestamp in seconds: "1234567890"
/// - Unix timestamp in nanoseconds: "1234567890000000000"
/// - ISO 8601: "2023-01-01T00:00:00Z"
pub fn parse_timestamp(s: &str) -> CliResult<u64> {
    // Try as nanoseconds first
    if let Ok(n) = s.parse::<u64>() {
        // If it's reasonably small (< year 3000), treat as seconds
        return Ok(if n < 32503680000 {
            n * 1_000_000_000
        } else {
            n
        });
    }

    // Try ISO 8601
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_nanos_opt().unwrap_or(0) as u64);
    }

    Err(anyhow::anyhow!("Invalid timestamp: {s}"))
}

/// Parse a time range string.
///
/// Formats: "start,end" or "start:duration" or "start-end"
pub fn parse_time_range(s: &str) -> CliResult<(u64, u64)> {
    let (start, end) = if s.contains(',') {
        let parts: Vec<&str> = s.splitn(2, ',').collect();
        (parts[0], parts[1])
    } else if s.contains(':') {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        (parts[0], parts[1])
    } else if s.contains('-') {
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        (parts[0], parts[1])
    } else {
        return Err(anyhow::anyhow!(
            "Time range must be in format: start,end or start:duration"
        ));
    };

    let start_ns = parse_timestamp(start)?;
    let end_ns = parse_timestamp(end)?;

    if end_ns <= start_ns {
        return Err(anyhow::anyhow!("End time must be after start time"));
    }

    Ok((start_ns, end_ns))
}

/// Progress bar wrapper for consistent progress reporting.
pub struct ProgressBar {
    inner: Option<indicatif::ProgressBar>,
    prefix: String,
}

impl ProgressBar {
    /// Create a new progress bar.
    pub fn new(total: u64, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        let inner = if std::io::stderr().is_terminal() {
            let pb = indicatif::ProgressBar::new(total);
            pb.set_style(indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("=>-"));
            pb.set_prefix(prefix.clone());
            Some(pb)
        } else {
            None
        };

        Self { inner, prefix }
    }

    /// Increment progress by one.
    pub fn inc(&self, delta: u64) {
        if let Some(pb) = &self.inner {
            pb.inc(delta);
        }
    }

    /// Set the current position.
    pub fn set_position(&self, pos: u64) {
        if let Some(pb) = &self.inner {
            pb.set_position(pos);
        }
    }

    /// Set a message.
    pub fn set_message(&self, msg: String) {
        if let Some(pb) = &self.inner {
            pb.set_message(msg);
        }
    }

    /// Finish the progress bar.
    pub fn finish_with_message(&self, msg: String) {
        if let Some(pb) = &self.inner {
            pb.finish_with_message(msg);
        }
    }

    /// Clear the progress bar (for temporary messages).
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Some(pb) = &self.inner {
            pb.suspend(f)
        } else {
            f()
        }
    }

    /// Check if the progress bar is visible.
    pub fn is_visible(&self) -> bool {
        self.inner.is_some()
    }
}

/// Open a file with automatic format detection.
pub fn open_reader(path: &Path) -> Result<RoboReader> {
    Ok(RoboReader::open(path)?)
}

/// Resolve input path, handling compressed files.
pub fn resolve_input_path(path: &Path) -> PathBuf {
    // Check for common compressed extensions
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        if ext == "zst" || ext == "lz4" || ext == "bz2" {
            if let Some(stem) = path.file_stem() {
                return PathBuf::from(stem);
            }
        }
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(500_000_000), "500ms");
        assert_eq!(format_duration(1_500_000_000), "1.500s");
        assert_eq!(format_duration(90_000_000_000), "1m 30s");
        assert_eq!(format_duration(3_600_000_000_000), "1h 0m");
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(5_000), "4.88 KB");
        assert_eq!(format_bytes(5_000_000), "4.77 MB");
        assert_eq!(format_bytes(5_000_000_000), "4.66 GB");
    }

    #[test]
    fn test_parse_timestamp() {
        assert_eq!(parse_timestamp("0").unwrap(), 0);
        assert_eq!(
            parse_timestamp("1234567890").unwrap(),
            1_234_567_890_000_000_000
        );
    }

    #[test]
    fn test_parse_time_range() {
        let (start, end) = parse_time_range("0,1000").unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1_000_000_000);

        let (start, end) = parse_time_range("1234567890:1234567900").unwrap();
        assert_eq!(start, 1_234_567_890_000_000_000);
        assert_eq!(end, 1_234_567_900_000_000_000);
    }
}
