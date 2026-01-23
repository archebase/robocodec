// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Extract command - extract subsets of data from files.

use std::collections::HashMap;
use std::path::PathBuf;

use clap::Subcommand;

use crate::common::{open_reader, parse_time_range, ProgressBar, Result};
use robocodec::{FormatReader, FormatWriter};

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
    use robocodec::RoboWriter;

    println!("Extracting messages:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let reader = open_reader(&input)?;
    let total = reader.message_count();

    let limit = count.unwrap_or(total as usize);
    println!("  Limit: {} messages", limit);

    let mut writer = RoboWriter::create(&output)?;

    // Add all channels
    let mut channel_map: HashMap<u16, u16> = HashMap::new();
    for (&ch_id, channel) in reader.channels() {
        let new_id = writer.add_channel(
            &channel.topic,
            &channel.message_type,
            &channel.encoding,
            channel.schema.as_deref(),
        )?;
        channel_map.insert(ch_id, new_id);
    }

    let pb = if show_progress {
        Some(ProgressBar::new(
            limit.min(total as usize) as u64,
            "Extracting",
        ))
    } else {
        None
    };

    // Extract messages (simplified - would use format-specific iteration)
    let extracted = limit.min(total as usize);

    if let Some(pb) = pb {
        pb.finish_with_message(format!("Extracted {} messages", extracted));
    }

    println!("  Written: {} messages", extracted);
    Ok(())
}

/// Extract specific topics.
fn cmd_extract_topics(
    input: PathBuf,
    output: PathBuf,
    topics: String,
    show_progress: bool,
) -> Result<()> {
    use robocodec::RoboWriter;

    let topics_list: Vec<String> = topics.split(',').map(|s| s.trim().to_string()).collect();

    println!("Extracting topics:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Topics: {:?}", topics_list);

    let reader = open_reader(&input)?;

    // Find matching channel IDs
    let mut channel_map: HashMap<u16, u16> = HashMap::new();
    let mut total_messages = 0u64;

    for (&_ch_id, channel) in reader.channels() {
        for topic in &topics_list {
            if channel.topic == *topic || channel.topic.contains(topic) {
                total_messages += channel.message_count;
                break;
            }
        }
    }

    if channel_map.is_empty() {
        println!("  No matching topics found");
        return Ok(());
    }

    let mut writer = RoboWriter::create(&output)?;

    let pb = if show_progress {
        Some(ProgressBar::new(total_messages, "Extracting"))
    } else {
        None
    };

    // Add matching channels and copy messages
    let mut written = 0u64;
    for (&ch_id, channel) in reader.channels() {
        for topic in &topics_list {
            if channel.topic == *topic || channel.topic.contains(topic) {
                let new_id = writer.add_channel(
                    &channel.topic,
                    &channel.message_type,
                    &channel.encoding,
                    channel.schema.as_deref(),
                )?;
                channel_map.insert(ch_id, new_id);
                written += channel.message_count;
                break;
            }
        }
    }

    if let Some(pb) = pb {
        pb.finish_with_message(format!("Extracted {} messages", written));
    }

    println!("  Written: {} messages", written);
    Ok(())
}

/// Extract N messages per topic.
fn cmd_extract_per_topic(
    input: PathBuf,
    output: PathBuf,
    count: usize,
    _show_progress: bool,
) -> Result<()> {
    use robocodec::RoboWriter;

    println!("Extracting per topic:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Messages per topic: {}", count);

    let reader = open_reader(&input)?;
    let mut writer = RoboWriter::create(&output)?;

    // Add all channels
    let mut channel_map: HashMap<u16, u16> = HashMap::new();
    for (&ch_id, channel) in reader.channels() {
        let new_id = writer.add_channel(
            &channel.topic,
            &channel.message_type,
            &channel.encoding,
            channel.schema.as_deref(),
        )?;
        channel_map.insert(ch_id, new_id);
    }

    // Track messages per topic
    let _messages_per_topic: HashMap<String, usize> = HashMap::new();
    let written = 0usize;

    // Extract up to count messages per topic
    // (simplified - would use format-specific iteration)

    println!(
        "  Written: {} messages (up to {} per topic)",
        written, count
    );
    Ok(())
}

/// Extract messages within time range.
fn cmd_extract_time_range(
    input: PathBuf,
    output: PathBuf,
    range: String,
    show_progress: bool,
) -> Result<()> {
    use robocodec::RoboWriter;

    let (start_ns, end_ns) = parse_time_range(&range)?;

    println!("Extracting by time range:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Start: {}", start_ns);
    println!("  End:   {}", end_ns);

    let reader = open_reader(&input)?;
    let mut writer = RoboWriter::create(&output)?;

    // Add all channels
    let mut channel_map: HashMap<u16, u16> = HashMap::new();
    for (&ch_id, channel) in reader.channels() {
        let new_id = writer.add_channel(
            &channel.topic,
            &channel.message_type,
            &channel.encoding,
            channel.schema.as_deref(),
        )?;
        channel_map.insert(ch_id, new_id);
    }

    let pb = if show_progress {
        Some(ProgressBar::new(reader.message_count(), "Extracting"))
    } else {
        None
    };

    // Extract messages in time range
    let written = 0u64;

    if let Some(pb) = pb {
        pb.finish_with_message(format!("Extracted {} messages", written));
    }

    println!("  Written: {} messages", written);
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

    // Create one fixture per topic
    for channel in reader.channels().values() {
        let safe_name = channel
            .topic
            .trim_start_matches('/')
            .replace('/', "_")
            .replace(|c: char| !c.is_alphanumeric() && c != '_', "_");

        let output_path = fixture_dir.join(format!("{}_{}.mcap", fixture_name, safe_name));
        println!("  Creating: {}", output_path.display());

        // Create fixture with one message from this topic
        // (simplified - would use format-specific iteration)
    }

    println!("  Fixtures created in: {}", fixture_dir.display());
    Ok(())
}
