// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Extract command - extract subsets of data from files.

use std::path::PathBuf;

use clap::Subcommand;

use crate::common::{open_reader, parse_time_range, Progress, Result};
use robocodec::{FormatReader, RoboRewriter};

/// Extract subsets of data from files.
#[derive(Subcommand, Clone, Debug)]
pub enum ExtractCmd {
    /// Extract first N messages
    Messages {
        /// Input file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Number of messages to extract (default: all)
        #[arg(short, long)]
        count: Option<usize>,

        /// Show progress bar
        #[arg(long, default_value = "true")]
        progress: bool,
    },

    /// Extract specific topics
    Topics {
        /// Input file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Topics to extract (comma-separated)
        #[arg(value_name = "TOPICS")]
        topics: String,

        /// Show progress bar
        #[arg(long, default_value = "true")]
        progress: bool,
    },

    /// Extract N messages per topic
    PerTopic {
        /// Input file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Number of messages per topic
        #[arg(short, long, default_value = "1")]
        count: usize,

        /// Show progress bar
        #[arg(long, default_value = "true")]
        progress: bool,
    },

    /// Extract messages within time range
    TimeRange {
        /// Input file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Time range: start,end (nanoseconds or ISO 8601)
        #[arg(value_name = "RANGE")]
        range: String,

        /// Show progress bar
        #[arg(long, default_value = "true")]
        progress: bool,
    },

    /// Create a minimal fixture file with one message per topic
    Fixture {
        /// Input file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output directory (default: tests/fixtures)
        #[arg(short, long)]
        output_dir: Option<PathBuf>,

        /// Name for the fixture files
        #[arg(short, long)]
        name: Option<String>,
    },
}

impl ExtractCmd {
    pub fn run(self) -> Result<()> {
        match self {
            ExtractCmd::Messages {
                input,
                output,
                count,
                progress,
            } => cmd_extract_messages(input, output, count, progress),
            ExtractCmd::Topics {
                input,
                output,
                topics,
                progress,
            } => cmd_extract_topics(input, output, topics, progress),
            ExtractCmd::PerTopic {
                input,
                output,
                count,
                progress,
            } => cmd_extract_per_topic(input, output, count, progress),
            ExtractCmd::TimeRange {
                input,
                output,
                range,
                progress,
            } => cmd_extract_time_range(input, output, range, progress),
            ExtractCmd::Fixture {
                input,
                output_dir,
                name,
            } => cmd_create_fixture(input, output_dir, name),
        }
    }
}

/// Extract first N messages.
fn cmd_extract_messages(
    input: PathBuf,
    output: PathBuf,
    count: Option<usize>,
    show_progress: bool,
) -> Result<()> {
    println!("Extracting messages:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let reader = open_reader(&input)?;
    let total = reader.message_count();
    let channel_count = reader.channels().len() as u64;

    let limit = count.unwrap_or(total as usize);
    println!("  Limit: {} messages", limit);

    // Use rewriter for full file copy with limit support
    // For partial extraction, we need format-specific iteration which is not yet exposed
    if limit < total as usize {
        return Err(anyhow::anyhow!(
            "Partial message extraction (count < total) requires format-specific iteration. \
             Use the convert command for full file copying."
        ));
    }

    // Full file copy using rewriter
    let mut progress = if show_progress {
        Some(Progress::new(channel_count, "Copying channels"))
    } else {
        None
    };

    let mut rewriter = RoboRewriter::open(&input)?;

    // Simulate channel progress during rewrite
    if let Some(ref mut pb) = progress {
        for i in 0..channel_count {
            pb.set(i + 1);
        }
    }

    let stats = rewriter.rewrite(&output)?;

    if let Some(pb) = progress {
        pb.finish(format!("{} messages", stats.message_count));
    } else {
        println!("  Written: {} messages", stats.message_count);
    }

    Ok(())
}

/// Extract specific topics.
fn cmd_extract_topics(
    input: PathBuf,
    output: PathBuf,
    topics: String,
    show_progress: bool,
) -> Result<()> {
    let topics_list: Vec<String> = topics.split(',').map(|s| s.trim().to_string()).collect();

    println!("Extracting topics:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Topics: {:?}", topics_list);

    let reader = open_reader(&input)?;

    // Find matching channels and count messages
    let mut matching_channels: Vec<u16> = Vec::new();

    for (ch_id, channel) in reader.channels() {
        for topic in &topics_list {
            if channel.topic == *topic || channel.topic.contains(topic) {
                matching_channels.push(*ch_id);
                break;
            }
        }
    }

    if matching_channels.is_empty() {
        return Err(anyhow::anyhow!(
            "No matching topics found for: {:?}. Verify topic names exist in the input file.",
            topics_list
        ));
    }

    let mut progress = if show_progress {
        Some(Progress::new(
            matching_channels.len() as u64,
            "Processing channels",
        ))
    } else {
        None
    };

    // Simulate processing each channel
    for (i, &ch_id) in matching_channels.iter().enumerate() {
        if let Some(ref mut pb) = progress {
            pb.set((i + 1) as u64);
        }
        // In a full implementation, this would iterate through messages
        let _ = ch_id; // Channel would be processed here
    }

    if let Some(pb) = progress {
        pb.finish(format!("{} channels", matching_channels.len()));
    }

    // Topic extraction requires format-specific iteration which is not yet exposed
    Err(anyhow::anyhow!(
        "Topic-specific extraction requires format-specific message iteration. \
         This feature is not yet implemented. Use the convert command for full file copying."
    ))
}

/// Extract N messages per topic.
fn cmd_extract_per_topic(
    input: PathBuf,
    output: PathBuf,
    count: usize,
    show_progress: bool,
) -> Result<()> {
    println!("Extracting per topic:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Messages per topic: {}", count);

    if count != 1 {
        return Err(anyhow::anyhow!(
            "Per-topic extraction with count > 1 requires format-specific iteration. \
             This feature is not yet implemented."
        ));
    }

    let reader = open_reader(&input)?;
    let channel_count = reader.channels().len() as u64;

    let mut progress = if show_progress {
        Some(Progress::new(channel_count, "Scanning channels"))
    } else {
        None
    };

    // Simulate scanning each channel
    for (i, channel) in reader.channels().values().enumerate() {
        if let Some(ref mut pb) = progress {
            pb.set((i + 1) as u64);
        }
        let _ = channel.topic; // Topic would be processed here
    }

    if let Some(pb) = progress {
        pb.finish(format!("{} channels scanned", channel_count));
    }

    // Per-topic extraction requires format-specific iteration
    Err(anyhow::anyhow!(
        "Per-topic extraction requires format-specific message iteration. \
         This feature is not yet implemented. Use the convert command for full file copying."
    ))
}

/// Extract messages within time range.
fn cmd_extract_time_range(
    input: PathBuf,
    output: PathBuf,
    range: String,
    show_progress: bool,
) -> Result<()> {
    let (start_ns, end_ns) = parse_time_range(&range)?;

    println!("Extracting by time range:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Start: {}", start_ns);
    println!("  End:   {}", end_ns);

    // Check if the full file is within range (full file copy)
    if start_ns == 0 && end_ns == u64::MAX {
        let reader = open_reader(&input)?;
        let channel_count = reader.channels().len() as u64;

        let mut progress = if show_progress {
            Some(Progress::new(channel_count, "Copying channels"))
        } else {
            None
        };

        let mut rewriter = RoboRewriter::open(&input)?;

        // Simulate channel progress during rewrite
        if let Some(ref mut pb) = progress {
            for i in 0..channel_count {
                pb.set(i + 1);
            }
        }

        let stats = rewriter.rewrite(&output)?;

        if let Some(pb) = progress {
            pb.finish(format!("{} messages", stats.message_count));
        } else {
            println!("  Written: {} messages", stats.message_count);
        }

        return Ok(());
    }

    // Time range filtering requires format-specific iteration
    Err(anyhow::anyhow!(
        "Time range filtering requires format-specific message iteration. \
         This feature is not yet implemented. Use the convert command for full file copying."
    ))
}

/// Create minimal fixture files.
fn cmd_create_fixture(
    input: PathBuf,
    output_dir: Option<PathBuf>,
    name: Option<String>,
) -> Result<()> {
    println!("Creating fixtures:");
    println!("  Input:  {}", input.display());

    let reader = open_reader(&input)?;

    let fixture_dir = output_dir.unwrap_or_else(|| PathBuf::from("tests/fixtures"));

    std::fs::create_dir_all(&fixture_dir)?;

    let _fixture_name = name.unwrap_or_else(|| "fixture".to_string());

    println!("  Available topics:");
    for channel in reader.channels().values() {
        println!("    - {} ({})", channel.topic, channel.message_type);
    }

    // Fixture creation requires format-specific iteration to extract one message per topic
    Err(anyhow::anyhow!(
        "Fixture creation requires format-specific message iteration. \
         This feature is not yet implemented. Use the convert command for full file copying."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to get fixture path
    fn fixture_path(name: &str) -> PathBuf {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(manifest_dir)
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    /// Helper to get a temporary output path
    fn temp_output() -> PathBuf {
        std::env::temp_dir().join(format!("robocodec_test_{}.mcap", std::process::id()))
    }

    // ========================================================================
    // ExtractCmd::run() Tests
    // ========================================================================

    #[test]
    fn test_extract_cmd_messages_nonexistent_file() {
        let cmd = ExtractCmd::Messages {
            input: PathBuf::from("/nonexistent/file.mcap"),
            output: temp_output(),
            count: None,
            progress: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent input file");
        // Error message may vary, just check it fails
        let _ = result.unwrap_err();
    }

    #[test]
    fn test_extract_cmd_topics_nonexistent_file() {
        let cmd = ExtractCmd::Topics {
            input: PathBuf::from("/nonexistent/file.mcap"),
            output: temp_output(),
            topics: "tf".to_string(),
            progress: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent input file");
    }

    #[test]
    fn test_extract_cmd_per_topic_nonexistent_file() {
        let cmd = ExtractCmd::PerTopic {
            input: PathBuf::from("/nonexistent/file.mcap"),
            output: temp_output(),
            count: 1,
            progress: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent input file");
    }

    #[test]
    fn test_extract_cmd_time_range_nonexistent_file() {
        let cmd = ExtractCmd::TimeRange {
            input: PathBuf::from("/nonexistent/file.mcap"),
            output: temp_output(),
            range: "0,MAX".to_string(),
            progress: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent input file");
    }

    #[test]
    fn test_extract_cmd_fixture_nonexistent_file() {
        let cmd = ExtractCmd::Fixture {
            input: PathBuf::from("/nonexistent/file.mcap"),
            output_dir: None,
            name: None,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent input file");
    }

    // ========================================================================
    // Messages Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_extract_messages_partial_extraction_error() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return; // Skip if fixture not available
        }

        // Partial extraction (count < total) should error
        let result = cmd_extract_messages(path.clone(), temp_output(), Some(1), false);
        assert!(result.is_err(), "partial extraction should fail");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partial message extraction"));
    }

    #[test]
    fn test_cmd_extract_messages_invalid_range() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Can't test full extraction without a valid output
        // but we can verify the function attempts to open the file
        let result = cmd_extract_messages(
            path,
            PathBuf::from("/nonexistent/output/dir/file.mcap"),
            None,
            false,
        );
        assert!(result.is_err(), "should fail for invalid output path");
    }

    // ========================================================================
    // Topics Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_extract_topics_no_matching_topics() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Use a topic pattern that won't match
        let result = cmd_extract_topics(
            path,
            temp_output(),
            "definitely_nonexistent_topic_xyz".to_string(),
            false,
        );
        assert!(result.is_err(), "should fail when no topics match");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No matching topics"));
    }

    #[test]
    fn test_cmd_extract_topics_not_implemented() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Even with matching topics, should return not implemented error
        // First we need to find a real topic name
        let Ok(reader) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| open_reader(&path)))
        else {
            return;
        };

        let Ok(reader) = reader else { return };

        let Some(topic) = reader.channels().values().next().map(|ch| ch.topic.clone()) else {
            return;
        };

        let result = cmd_extract_topics(path, temp_output(), topic, false);
        assert!(result.is_err(), "topic extraction not yet implemented");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[test]
    fn test_cmd_extract_topics_multiple_topics() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Test with comma-separated topics
        let result = cmd_extract_topics(
            path,
            temp_output(),
            "topic1,topic2,topic3".to_string(),
            false,
        );
        // Should fail because these topics don't exist
        assert!(result.is_err());
    }

    #[test]
    fn test_cmd_extract_topics_whitespace_handling() {
        // Test that topics string is trimmed properly
        let topics_str = " topic1 , topic2 , topic3 ";
        let parsed: Vec<String> = topics_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        assert_eq!(parsed, vec!["topic1", "topic2", "topic3"]);
    }

    // ========================================================================
    // PerTopic Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_extract_per_topic_count_not_one() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // count != 1 should fail
        let result = cmd_extract_per_topic(path, temp_output(), 2, false);
        assert!(result.is_err(), "count > 1 should fail");
        assert!(result.unwrap_err().to_string().contains("count > 1"));
    }

    #[test]
    fn test_cmd_extract_per_topic_not_implemented() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Even with count=1, should return not implemented
        let result = cmd_extract_per_topic(path, temp_output(), 1, false);
        assert!(result.is_err(), "per-topic extraction not yet implemented");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    // ========================================================================
    // TimeRange Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_extract_time_range_invalid_range() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Invalid range format
        let result = cmd_extract_time_range(
            path,
            temp_output(),
            "invalid-range-format".to_string(),
            false,
        );
        assert!(result.is_err(), "invalid range format should fail");
    }

    #[test]
    fn test_cmd_extract_time_range_not_implemented() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Valid range that's not "0,MAX" should fail with not implemented
        let result = cmd_extract_time_range(path, temp_output(), "1000,2000".to_string(), false);
        assert!(result.is_err(), "time range filtering not yet implemented");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[test]
    fn test_cmd_extract_time_range_invalid_output() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Even with 0,MAX range, invalid output should fail
        let result = cmd_extract_time_range(
            path,
            PathBuf::from("/nonexistent/output/dir/file.mcap"),
            "0,MAX".to_string(),
            false,
        );
        assert!(result.is_err(), "invalid output path should fail");
    }

    // ========================================================================
    // Fixture Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_create_fixture_not_implemented() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_create_fixture(path, None, None);
        assert!(result.is_err(), "fixture creation not yet implemented");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[test]
    fn test_cmd_create_fixture_with_custom_dir() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let temp_dir = std::env::temp_dir().join("robocodec_fixture_test");
        let result = cmd_create_fixture(path, Some(temp_dir.clone()), Some("test".to_string()));

        // Clean up temp dir
        let _ = std::fs::remove_dir_all(temp_dir);

        assert!(result.is_err(), "fixture creation not yet implemented");
    }

    // ========================================================================
    // ExtractCmd Enum Tests
    // ========================================================================

    #[test]
    fn test_extract_cmd_clone() {
        let cmd = ExtractCmd::Messages {
            input: PathBuf::from("test.mcap"),
            output: PathBuf::from("out.mcap"),
            count: Some(10),
            progress: true,
        };
        let cloned = cmd.clone();
        match (cmd, cloned) {
            (ExtractCmd::Messages { input: i1, .. }, ExtractCmd::Messages { input: i2, .. }) => {
                assert_eq!(i1, i2);
            }
            _ => panic!("cloned commands should match"),
        }
    }

    #[test]
    fn test_extract_cmd_debug() {
        let cmd = ExtractCmd::Topics {
            input: PathBuf::from("test.mcap"),
            output: PathBuf::from("out.mcap"),
            topics: "tf".to_string(),
            progress: false,
        };
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("Topics"));
    }

    // ========================================================================
    // Progress Bar Tests
    // ========================================================================

    #[test]
    fn test_extract_with_progress_disabled() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Test with progress=false
        let result = cmd_extract_per_topic(path, temp_output(), 1, false);
        assert!(result.is_err()); // Not implemented, but should get past progress creation
    }

    #[test]
    fn test_extract_with_progress_enabled() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Test with progress=true
        let result = cmd_extract_per_topic(path, temp_output(), 1, true);
        assert!(result.is_err()); // Not implemented, but should get past progress creation
    }
}
