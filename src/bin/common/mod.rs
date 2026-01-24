// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Common utilities for CLI commands.

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
///
/// # Notes
///
/// - Numeric timestamps smaller than ~year 3000 are treated as seconds
/// - Numeric timestamps larger than ~year 3000 are treated as nanoseconds
/// - ISO 8601 timestamps outside chrono's range (year > 262000000+) will error
pub fn parse_timestamp(s: &str) -> CliResult<u64> {
    // Approximate seconds from epoch to year 3000
    const SECONDS_THRESHOLD: u64 = 32_503_680_000;

    // Try as nanoseconds first
    if let Ok(n) = s.parse::<u64>() {
        // If it's reasonably small (< year 3000), treat as seconds
        return Ok(if n < SECONDS_THRESHOLD {
            n * 1_000_000_000
        } else {
            n
        });
    }

    // Try ISO 8601
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        let nanos = dt
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("Timestamp out of range (year > ~262000000): {s}"))?;
        return Ok(nanos as u64);
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
