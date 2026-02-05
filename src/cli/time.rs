// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Time formatting and parsing utilities for CLI.

use crate::cli::CliResult;

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

    #[test]
    fn test_format_duration_milliseconds() {
        assert_eq!(format_duration(1_000_000), "1ms");
        assert_eq!(format_duration(999_999_999), "999ms");
        assert_eq!(format_duration(500_000_000), "500ms");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(1_000_000_000), "1.000s");
        assert_eq!(format_duration(5_500_000_000), "5.500s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(60_000_000_000), "1m 0s");
        assert_eq!(format_duration(125_000_000_000), "2m 5s");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(3_600_000_000_000), "1h 0m");
        assert_eq!(format_duration(7_200_000_000_000), "2h 0m");
        assert_eq!(format_duration(3_600_000_000_000 + 60_000_000_000), "1h 1m");
    }

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0ms");
    }

    #[test]
    fn test_format_timestamp_valid() {
        let result = format_timestamp(1_700_000_000_000_000_000);
        // Just verify it doesn't panic and returns something
        assert!(!result.is_empty());
    }

    #[test]
    fn test_format_timestamp_zero() {
        let result = format_timestamp(0);
        assert!(result.contains("1970"));
    }

    #[test]
    fn test_parse_timestamp_zero() {
        assert_eq!(parse_timestamp("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_timestamp_as_seconds() {
        assert_eq!(
            parse_timestamp("1609459200").unwrap(),
            1_609_459_200_000_000_000
        ); // 2021-01-01 00:00:00 UTC
    }

    #[test]
    fn test_parse_timestamp_as_nanos() {
        // Large number should be treated as nanoseconds (just above threshold)
        // Max u64 is ~18.4e19, threshold is ~32.5e9 seconds
        // So we need a value > 32_503_680_000 * 1_000_000_000 = 32_503_680_000_000_000_000
        // But that overflows! Let's use a value within u64 range
        // 18_000_000_000_000_000_000 is valid and > threshold
        assert_eq!(
            parse_timestamp("18000000000000000000").unwrap(),
            18_000_000_000_000_000_000
        );
    }

    #[test]
    fn test_parse_timestamp_iso8601() {
        let result = parse_timestamp("2023-01-01T00:00:00Z");
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_parse_timestamp_iso8601_with_timezone() {
        let result = parse_timestamp("2023-01-01T00:00:00+00:00");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_timestamp_invalid_string() {
        assert!(parse_timestamp("invalid").is_err());
        assert!(parse_timestamp("").is_err());
        assert!(parse_timestamp("abc123").is_err());
    }

    #[test]
    fn test_parse_time_range_with_dash() {
        let (start, end) = parse_time_range("0-1").unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1_000_000_000);
    }

    #[test]
    fn test_parse_time_range_invalid_format() {
        assert!(parse_time_range("0").is_err());
        assert!(parse_time_range("").is_err());
    }

    #[test]
    fn test_parse_time_range_end_before_start() {
        assert!(parse_time_range("10,0").is_err());
        assert!(parse_time_range("100,50").is_err());
    }

    #[test]
    fn test_parse_time_range_equal_times() {
        // Equal times should error
        assert!(parse_time_range("100,100").is_err());
    }

    #[test]
    fn test_parse_timestamp_negative_rejected() {
        assert!(parse_timestamp("-1").is_err());
    }

    #[test]
    fn test_format_duration_boundary_values() {
        // Test exact boundary: 59.999 seconds
        assert_eq!(format_duration(59_999_000_000), "59.999s");

        // Test exact boundary: 60 seconds
        assert_eq!(format_duration(60_000_000_000), "1m 0s");

        // Test exact boundary: 3599.999 seconds
        assert_eq!(format_duration(3_599_999_000_000), "59m 59s");

        // Test exact boundary: 3600 seconds
        assert_eq!(format_duration(3_600_000_000_000), "1h 0m");
    }
}
