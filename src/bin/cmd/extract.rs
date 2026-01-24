// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Extract command - extract subsets of data from files.

use std::path::PathBuf;

use clap::Subcommand;

use crate::common::{open_reader, parse_time_range, Result};
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
    _show_progress: bool,
) -> Result<()> {
    println!("Extracting messages:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let reader = open_reader(&input)?;
    let total = reader.message_count();

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
    let mut rewriter = RoboRewriter::open(&input)?;
    let stats = rewriter.rewrite(&output)?;

    println!("  Written: {} messages", stats.message_count);
    Ok(())
}

/// Extract specific topics.
fn cmd_extract_topics(
    input: PathBuf,
    output: PathBuf,
    topics: String,
    _show_progress: bool,
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
    _show_progress: bool,
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
    _show_progress: bool,
) -> Result<()> {
    let (start_ns, end_ns) = parse_time_range(&range)?;

    println!("Extracting by time range:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());
    println!("  Start: {}", start_ns);
    println!("  End:   {}", end_ns);

    // Check if the full file is within range
    if start_ns == 0 && end_ns == u64::MAX {
        let _ = (start_ns, end_ns); // Full file copy
                                    // Full file copy
        let mut rewriter = RoboRewriter::open(&input)?;
        let stats = rewriter.rewrite(&output)?;
        println!("  Written: {} messages", stats.message_count);
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
