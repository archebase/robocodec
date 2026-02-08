// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Extract command - extract subsets of data from files.

use std::path::PathBuf;

use clap::Subcommand;

use crate::cli::{Progress, Result, open_reader, parse_time_range};
use robocodec::io::RawMessage;
use robocodec::{FormatReader, FormatWriter, RoboReader, RoboWriter};

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

    let limit = count.unwrap_or(total as usize);
    println!("  Limit: {} messages", limit);

    let output_str = output
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in output path"))?;
    let mut writer = RoboWriter::create(output_str)?;

    // Add all channels to writer
    let channel_map = add_channels_to_writer(&reader, &mut writer)?;

    let mut progress = if show_progress {
        Some(Progress::new(limit as u64, "Extracting messages"))
    } else {
        None
    };

    // Iterate raw messages and write up to limit
    let raw_iter = reader.iter_raw()?;
    let mut written = 0u64;

    for result in raw_iter {
        if written >= limit as u64 {
            break;
        }

        let (raw_msg, _channel_info) = result?;

        // Remap channel_id to writer's channel_id
        if let Some(&new_ch_id) = channel_map.get(&raw_msg.channel_id) {
            let write_msg = RawMessage {
                channel_id: new_ch_id,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                data: raw_msg.data,
                sequence: raw_msg.sequence,
            };
            writer.write(&write_msg)?;
            written += 1;
        }

        if let Some(ref mut pb) = progress {
            pb.set(written);
        }
    }

    writer.finish()?;

    if let Some(pb) = progress {
        pb.finish(format!("{written} messages"));
    } else {
        println!("  Written: {written} messages");
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

    // Find matching channels
    let mut matching_channels = std::collections::HashSet::new();

    for (ch_id, channel) in reader.channels() {
        for topic in &topics_list {
            if channel.topic == *topic || channel.topic.contains(topic) {
                matching_channels.insert(*ch_id);
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

    println!("  Matched {} channels", matching_channels.len());

    let output_str = output
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in output path"))?;
    let mut writer = RoboWriter::create(output_str)?;

    // Only add matching channels to writer
    let channel_map = add_matching_channels_to_writer(&reader, &mut writer, &matching_channels)?;

    let mut progress = if show_progress {
        Some(Progress::new(0, "Extracting topics"))
    } else {
        None
    };

    let raw_iter = reader.iter_raw()?;
    let mut written = 0u64;

    for result in raw_iter {
        let (raw_msg, _channel_info) = result?;

        // Only write messages from matching channels
        if let Some(&new_ch_id) = channel_map.get(&raw_msg.channel_id) {
            let write_msg = RawMessage {
                channel_id: new_ch_id,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                data: raw_msg.data,
                sequence: raw_msg.sequence,
            };
            writer.write(&write_msg)?;
            written += 1;
        }

        if let Some(ref mut pb) = progress {
            pb.set(written);
        }
    }

    writer.finish()?;

    if let Some(pb) = progress {
        pb.finish(format!(
            "{written} messages from {} topics",
            matching_channels.len()
        ));
    } else {
        println!(
            "  Written: {written} messages from {} topics",
            matching_channels.len()
        );
    }

    Ok(())
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

    let reader = open_reader(&input)?;
    let channel_count = reader.channels().len();

    let output_str = output
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in output path"))?;
    let mut writer = RoboWriter::create(output_str)?;

    // Add all channels to writer
    let channel_map = add_channels_to_writer(&reader, &mut writer)?;

    let mut progress = if show_progress {
        Some(Progress::new(
            (channel_count * count) as u64,
            "Extracting per topic",
        ))
    } else {
        None
    };

    // Track how many messages we've written per channel
    let mut per_channel_count: std::collections::HashMap<u16, usize> =
        std::collections::HashMap::new();

    let raw_iter = reader.iter_raw()?;
    let mut written = 0u64;
    let mut all_done = false;

    for result in raw_iter {
        if all_done {
            break;
        }

        let (raw_msg, _channel_info) = result?;

        let ch_count = per_channel_count.entry(raw_msg.channel_id).or_insert(0);

        if *ch_count < count
            && let Some(&new_ch_id) = channel_map.get(&raw_msg.channel_id)
        {
            let write_msg = RawMessage {
                channel_id: new_ch_id,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                data: raw_msg.data,
                sequence: raw_msg.sequence,
            };
            writer.write(&write_msg)?;
            written += 1;
            *ch_count += 1;
        }

        if let Some(ref mut pb) = progress {
            pb.set(written);
        }

        // Check if all channels have enough messages
        if per_channel_count.len() == channel_count
            && per_channel_count.values().all(|&c| c >= count)
        {
            all_done = true;
        }
    }

    writer.finish()?;

    if let Some(pb) = progress {
        pb.finish(format!("{written} messages from {channel_count} topics"));
    } else {
        println!("  Written: {written} messages from {channel_count} topics");
    }

    Ok(())
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

    let reader = open_reader(&input)?;

    let output_str = output
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in output path"))?;
    let mut writer = RoboWriter::create(output_str)?;

    // Add all channels to writer
    let channel_map = add_channels_to_writer(&reader, &mut writer)?;

    let mut progress = if show_progress {
        Some(Progress::new(0, "Extracting by time range"))
    } else {
        None
    };

    let raw_iter = reader.iter_raw()?;
    let mut written = 0u64;

    for result in raw_iter {
        let (raw_msg, _channel_info) = result?;

        // Filter by time range (use log_time)
        if raw_msg.log_time >= start_ns
            && raw_msg.log_time <= end_ns
            && let Some(&new_ch_id) = channel_map.get(&raw_msg.channel_id)
        {
            let write_msg = RawMessage {
                channel_id: new_ch_id,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                data: raw_msg.data,
                sequence: raw_msg.sequence,
            };
            writer.write(&write_msg)?;
            written += 1;
        }

        if let Some(ref mut pb) = progress {
            pb.set(written);
        }
    }

    writer.finish()?;

    if let Some(pb) = progress {
        pb.finish(format!("{written} messages"));
    } else {
        println!("  Written: {written} messages");
    }

    Ok(())
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

    let fixture_name = name.unwrap_or_else(|| "fixture".to_string());

    // Determine output extension from input
    let ext = input.extension().and_then(|e| e.to_str()).unwrap_or("bag");
    let output_path = fixture_dir.join(format!("{fixture_name}.{ext}"));

    println!("  Output: {}", output_path.display());
    println!("  Available topics:");
    for channel in reader.channels().values() {
        println!("    - {} ({})", channel.topic, channel.message_type);
    }

    let output_str = output_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in output path"))?;
    let mut writer = RoboWriter::create(output_str)?;

    // Add all channels to writer
    let channel_count = reader.channels().len();
    let channel_map = add_channels_to_writer(&reader, &mut writer)?;

    // Extract one message per topic (same as per-topic with count=1)
    let mut per_channel_count: std::collections::HashMap<u16, usize> =
        std::collections::HashMap::new();

    let raw_iter = reader.iter_raw()?;
    let mut written = 0u64;

    for result in raw_iter {
        let (raw_msg, _channel_info) = result?;

        let ch_count = per_channel_count.entry(raw_msg.channel_id).or_insert(0);

        if *ch_count < 1
            && let Some(&new_ch_id) = channel_map.get(&raw_msg.channel_id)
        {
            let write_msg = RawMessage {
                channel_id: new_ch_id,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
                data: raw_msg.data,
                sequence: raw_msg.sequence,
            };
            writer.write(&write_msg)?;
            written += 1;
            *ch_count += 1;
        }

        // Check if all channels have a message
        if per_channel_count.len() == channel_count && per_channel_count.values().all(|&c| c >= 1) {
            break;
        }
    }

    writer.finish()?;

    println!(
        "  Created fixture: {} ({written} messages from {channel_count} topics)",
        output_path.display()
    );

    Ok(())
}

/// Add all channels from reader to writer, returning a map from old channel_id to new channel_id.
fn add_channels_to_writer(
    reader: &RoboReader,
    writer: &mut RoboWriter,
) -> Result<std::collections::HashMap<u16, u16>> {
    let mut channel_map = std::collections::HashMap::new();

    for (&old_id, channel) in reader.channels() {
        let new_id = writer.add_channel(
            &channel.topic,
            &channel.message_type,
            &channel.encoding,
            channel.schema.as_deref(),
        )?;
        channel_map.insert(old_id, new_id);
    }

    Ok(channel_map)
}

/// Add only matching channels from reader to writer.
fn add_matching_channels_to_writer(
    reader: &RoboReader,
    writer: &mut RoboWriter,
    matching_channels: &std::collections::HashSet<u16>,
) -> Result<std::collections::HashMap<u16, u16>> {
    let mut channel_map = std::collections::HashMap::new();

    for (&old_id, channel) in reader.channels() {
        if matching_channels.contains(&old_id) {
            let new_id = writer.add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )?;
            channel_map.insert(old_id, new_id);
        }
    }

    Ok(channel_map)
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
    fn test_cmd_extract_messages_partial() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return; // Skip if fixture not available
        }

        let output = temp_output();
        let result = cmd_extract_messages(path, output.clone(), Some(1), false);
        // Should succeed - partial extraction now works
        assert!(
            result.is_ok(),
            "partial extraction should succeed: {:?}",
            result.err()
        );
        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn test_cmd_extract_messages_all() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let output = temp_output();
        let result = cmd_extract_messages(path, output.clone(), None, false);
        assert!(
            result.is_ok(),
            "full extraction should succeed: {:?}",
            result.err()
        );
        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn test_cmd_extract_messages_invalid_output() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

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
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No matching topics")
        );
    }

    #[test]
    fn test_cmd_extract_topics_matching() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Find a real topic name
        let Ok(reader) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| open_reader(&path)))
        else {
            return;
        };

        let Ok(reader) = reader else { return };

        let Some(topic) = reader.channels().values().next().map(|ch| ch.topic.clone()) else {
            return;
        };

        let output = temp_output();
        let result = cmd_extract_topics(path, output.clone(), topic, false);
        assert!(
            result.is_ok(),
            "topic extraction should succeed: {:?}",
            result.err()
        );
        let _ = std::fs::remove_file(&output);
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
    fn test_cmd_extract_per_topic_count_one() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let output = temp_output();
        let result = cmd_extract_per_topic(path, output.clone(), 1, false);
        assert!(
            result.is_ok(),
            "per-topic extraction with count=1 should succeed: {:?}",
            result.err()
        );
        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn test_cmd_extract_per_topic_count_multiple() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let output = temp_output();
        let result = cmd_extract_per_topic(path, output.clone(), 3, false);
        assert!(
            result.is_ok(),
            "per-topic extraction with count>1 should succeed: {:?}",
            result.err()
        );
        let _ = std::fs::remove_file(&output);
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
    fn test_cmd_extract_time_range_specific_range() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let output = temp_output();
        let result = cmd_extract_time_range(path, output.clone(), "0,MAX".to_string(), false);
        assert!(
            result.is_ok(),
            "time range extraction should succeed: {:?}",
            result.err()
        );
        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn test_cmd_extract_time_range_invalid_output() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

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
    fn test_cmd_create_fixture() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let temp_dir =
            std::env::temp_dir().join(format!("robocodec_fixture_{}", std::process::id()));
        let result = cmd_create_fixture(path, Some(temp_dir.clone()), Some("test".to_string()));
        assert!(
            result.is_ok(),
            "fixture creation should succeed: {:?}",
            result.err()
        );

        // Clean up
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_cmd_create_fixture_with_custom_dir() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let temp_dir =
            std::env::temp_dir().join(format!("robocodec_fixture_custom_{}", std::process::id()));
        let result =
            cmd_create_fixture(path, Some(temp_dir.clone()), Some("my_fixture".to_string()));
        assert!(
            result.is_ok(),
            "fixture creation should succeed: {:?}",
            result.err()
        );

        // Verify output file exists
        let output_file = temp_dir.join("my_fixture.mcap");
        assert!(
            output_file.exists(),
            "fixture file should exist at {:?}",
            output_file
        );

        // Clean up
        let _ = std::fs::remove_dir_all(temp_dir);
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
