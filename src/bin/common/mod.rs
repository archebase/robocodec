// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Common utilities for CLI commands.

use std::io::IsTerminal as _;
use std::path::Path;

use robocodec::RoboReader;

pub use anyhow::Result as CliResult;
pub type Result<T = ()> = CliResult<T>;

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
            pb.set_prefix(prefix);
            Some(pb)
        } else {
            None
        };

        Self { inner }
    }

    /// Finish the progress bar with a message.
    pub fn finish_with_message(&self, msg: String) {
        if let Some(pb) = &self.inner {
            pb.finish_with_message(msg);
        }
    }
}

/// Open a file with automatic format detection.
pub fn open_reader(path: &Path) -> Result<RoboReader> {
    Ok(RoboReader::open(path)?)
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
    fn test_parse_timestamp() {
        assert_eq!(parse_timestamp("0").unwrap(), 0);
        assert_eq!(
            parse_timestamp("1234567890").unwrap(),
            1_234_567_890_000_000_000
        );
    }

    #[test]
    fn test_parse_time_range() {
        let (start, end) = parse_time_range("0,1").unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1_000_000_000);

        let (start, end) = parse_time_range("1234567890:1234567900").unwrap();
        assert_eq!(start, 1_234_567_890_000_000_000);
        assert_eq!(end, 1_234_567_900_000_000_000);
    }
}
