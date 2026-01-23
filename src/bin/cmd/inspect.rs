// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Inspect command - show file information, topics, schemas, messages.

use std::path::PathBuf;

use clap::Subcommand;

use crate::common::{format_duration, format_timestamp, Result};
use robocodec::{FormatReader, RoboReader};

/// Inspect file contents.
#[derive(Subcommand, Clone, Debug)]
pub enum InspectCmd {
    /// Show basic file information and summary
    Info {
        /// Input file (MCAP or BAG)
        #[arg(value_name = "FILE")]
        input: PathBuf,
    },

    /// List all topics in the file
    Topics {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Filter topics by pattern
        #[arg(short, long)]
        filter: Option<String>,

        /// Show message counts
        #[arg(long)]
        counts: bool,
    },

    /// Show schema definition
    Schema {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Topic or message type to show (shows all if not specified)
        #[arg(value_name = "TOPIC|TYPE")]
        topic_or_type: Option<String>,
    },

    /// Show file statistics
    Stats {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,
    },
}

impl InspectCmd {
    pub fn run(self) -> Result<()> {
        match self {
            InspectCmd::Info { input } => cmd_info(input),
            InspectCmd::Topics {
                input,
                filter,
                counts,
            } => cmd_topics(input, filter, counts),
            InspectCmd::Schema {
                input,
                topic_or_type,
            } => cmd_schema(input, topic_or_type),
            InspectCmd::Stats { input } => cmd_stats(input),
        }
    }
}

/// Cmd: Show file info
fn cmd_info(input: PathBuf) -> Result<()> {
    let reader = RoboReader::open(&input)?;

    println!("=== {} ===", input.display());
    println!("Format: {:?}", reader.format());
    println!("Channels: {}", reader.channels().len());
    println!("Messages: {}", reader.message_count());

    if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
        println!("Start: {}", format_timestamp(start));
        println!("End: {}", format_timestamp(end));
        println!("Duration: {}", format_duration(end - start));
    }

    println!();
    println!("Channels:");
    for (&id, ch) in reader.channels() {
        println!(
            "  [{}] {} | {} | {} messages",
            id, ch.topic, ch.message_type, ch.message_count
        );
    }

    Ok(())
}

/// Cmd: List topics
fn cmd_topics(input: PathBuf, filter: Option<String>, show_counts: bool) -> Result<()> {
    let reader = RoboReader::open(&input)?;

    println!("=== Topics in {} ===", input.display());
    println!();

    for channel in reader.channels().values() {
        if let Some(ref pattern) = filter {
            let lower = pattern.to_lowercase();
            if !channel.topic.to_lowercase().contains(&lower)
                && !channel.message_type.to_lowercase().contains(&lower)
            {
                continue;
            }
        }

        println!("Topic: {}", channel.topic);
        println!("  Type: {}", channel.message_type);
        if show_counts {
            println!("  Messages: {}", channel.message_count);
        }
        println!();
    }

    Ok(())
}

/// Cmd: Show schema
fn cmd_schema(input: PathBuf, topic_or_type: Option<String>) -> Result<()> {
    let reader = RoboReader::open(&input)?;

    let mut found = false;

    for channel in reader.channels().values() {
        if let Some(ref filter) = topic_or_type {
            if !channel.topic.contains(filter) && !channel.message_type.contains(filter) {
                continue;
            }
        }

        found = true;
        println!("=== {} @ {} ===", channel.message_type, channel.topic);
        println!();

        if let Some(schema) = &channel.schema {
            for line in schema.lines().take(50) {
                println!("{}", line);
            }
            if schema.lines().count() > 50 {
                println!("... ({} lines total)", schema.lines().count());
            }
        } else {
            println!("(no schema available)");
        }
        println!();
    }

    if !found {
        if let Some(filter) = topic_or_type {
            println!("No matching topic or type found: {}", filter);
        }
    }

    Ok(())
}

/// Cmd: Show statistics
fn cmd_stats(input: PathBuf) -> Result<()> {
    let reader = RoboReader::open(&input)?;

    println!("=== Statistics for {} ===", input.display());
    println!("Total messages: {}", reader.message_count());
    println!("Channels: {}", reader.channels().len());

    let mut topic_count = 0;
    let mut seen = std::collections::HashSet::new();

    for channel in reader.channels().values() {
        if seen.insert(&channel.topic) {
            topic_count += 1;
        }
    }

    println!("Unique topics: {}", topic_count);

    if let (Some(start), Some(end)) = (reader.start_time(), reader.end_time()) {
        println!("Duration: {}", format_duration(end - start));
    }

    println!();
    println!("=== Per-Channel Breakdown ===");
    println!();

    let mut channels_vec: Vec<_> = reader.channels().values().collect();
    channels_vec.sort_by(|a, b| b.message_count.cmp(&a.message_count));

    for channel in channels_vec {
        let percentage = if reader.message_count() > 0 {
            (channel.message_count as f64 / reader.message_count() as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{}: {} ({:.1}%)",
            channel.topic, channel.message_count, percentage
        );
        println!("  Type: {}", channel.message_type);
    }

    Ok(())
}
