// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Search command - search within files for patterns.

use std::path::PathBuf;

use clap::Subcommand;

use crate::common::{open_reader, Result};
use robocodec::FormatReader;

/// Search within files.
#[derive(Subcommand, Clone, Debug)]
pub enum SearchCmd {
    /// Search for hex byte pattern in file
    Bytes {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Hex pattern (e.g., "1a ff 00")
        #[arg(value_name = "PATTERN")]
        pattern: String,

        /// Limit number of results
        #[arg(short, long, default_value = "10")]
        limit: usize,

        /// Show context around matches
        #[arg(long)]
        context: bool,
    },

    /// Search for UTF-8 string in file
    String {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Search text
        #[arg(value_name = "TEXT")]
        text: String,

        /// Limit number of results
        #[arg(short, long, default_value = "10")]
        limit: usize,

        /// Show context around matches
        #[arg(long)]
        context: bool,
    },

    /// Search for topics by name pattern
    Topics {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Pattern to search
        #[arg(value_name = "PATTERN")]
        pattern: String,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Search for message types
    Types {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Pattern to search
        #[arg(value_name = "PATTERN")]
        pattern: String,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Show fields for a topic
    Fields {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Topic name
        #[arg(value_name = "TOPIC")]
        topic: String,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Search field values across messages
    Values {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Topic name
        #[arg(value_name = "TOPIC")]
        topic: String,

        /// Field name pattern
        #[arg(value_name = "FIELD")]
        field: String,

        /// Limit number of results
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
}

impl SearchCmd {
    pub fn run(self) -> Result<()> {
        match self {
            SearchCmd::Bytes {
                input,
                pattern,
                limit,
                context,
            } => cmd_search_bytes(input, pattern, limit, context),
            SearchCmd::String {
                input,
                text,
                limit,
                context,
            } => cmd_search_string(input, text, limit, context),
            SearchCmd::Topics {
                input,
                pattern,
                json,
            } => cmd_search_topics(input, pattern, json),
            SearchCmd::Types {
                input,
                pattern,
                json,
            } => cmd_search_types(input, pattern, json),
            SearchCmd::Fields { input, topic, json } => cmd_show_fields(input, topic, json),
            SearchCmd::Values {
                input,
                topic,
                field,
                limit,
            } => cmd_search_values(input, topic, field, limit),
        }
    }
}

/// Search for hex byte pattern.
fn cmd_search_bytes(
    input: PathBuf,
    pattern: String,
    limit: usize,
    show_context: bool,
) -> Result<()> {
    let data = std::fs::read(&input)?;

    // Parse hex pattern
    let pattern_bytes: std::result::Result<Vec<u8>, _> = pattern
        .split_whitespace()
        .map(|s| u8::from_str_radix(s, 16))
        .collect();
    let pattern_bytes = pattern_bytes.map_err(|_| anyhow::anyhow!("Invalid hex pattern"))?;

    println!("Searching for byte pattern: {:02x?}", pattern_bytes);
    println!("File size: {} bytes", data.len());
    println!();

    let mut found_count = 0;
    let mut search_pos = 0;

    while search_pos + pattern_bytes.len() <= data.len() && found_count < limit {
        if let Some(pos) = data[search_pos..]
            .windows(pattern_bytes.len())
            .position(|w| w == pattern_bytes)
        {
            let actual_pos = search_pos + pos;
            found_count += 1;

            println!(
                "Found at offset: 0x{:08x} ({} bytes)",
                actual_pos, actual_pos
            );

            if show_context {
                let start = actual_pos.saturating_sub(16);
                let end = (actual_pos + 16 + pattern_bytes.len()).min(data.len());

                println!("  Context:");
                for (i, chunk) in data[start..end].chunks(16).enumerate() {
                    let offset = start + i * 16;
                    print!("    {:08x}: ", offset);
                    for (j, b) in chunk.iter().enumerate() {
                        if offset + j >= actual_pos && offset + j < actual_pos + pattern_bytes.len()
                        {
                            print!("*{:02x}* ", b);
                        } else {
                            print!("{:02x} ", b);
                        }
                        if (j + 1) % 8 == 0 {
                            print!(" ");
                        }
                    }
                    println!();
                }
                println!();
            }

            search_pos = actual_pos + pattern_bytes.len();
        } else {
            break;
        }
    }

    if found_count == 0 {
        println!("Pattern not found");
    } else if found_count == limit {
        println!("(... showing first {} occurrences)", limit);
    } else {
        println!("Total occurrences: {}", found_count);
    }

    Ok(())
}

/// Search for UTF-8 string.
fn cmd_search_string(input: PathBuf, text: String, limit: usize, show_context: bool) -> Result<()> {
    let data = std::fs::read(&input)?;

    println!("Searching for string: {:?}", text);
    println!("File size: {} bytes", data.len());
    println!();

    let pattern = text.as_bytes();
    let mut found_count = 0;
    let mut search_pos = 0;

    while search_pos + pattern.len() <= data.len() && found_count < limit {
        if let Some(pos) = data[search_pos..]
            .windows(pattern.len())
            .position(|w| w == pattern)
        {
            let actual_pos = search_pos + pos;
            found_count += 1;

            println!(
                "Found at offset: 0x{:08x} ({} bytes)",
                actual_pos, actual_pos
            );

            if show_context {
                let start = actual_pos.saturating_sub(32);
                let end = (actual_pos + 32 + pattern.len()).min(data.len());

                print!("  Context: \"");
                for (i, &b) in data[start..end].iter().enumerate() {
                    let abs_pos = start + i;
                    if abs_pos >= actual_pos && abs_pos < actual_pos + pattern.len() {
                        print!(">>>{}<<<", b as char);
                    } else if (32..=126).contains(&b) {
                        print!("{}", b as char);
                    } else if b == b'\n' {
                        print!("\\n");
                    } else if b == b'\r' {
                        print!("\\r");
                    } else if b == b'\t' {
                        print!("\\t");
                    } else {
                        print!("\\x{:02x}", b);
                    }
                }
                println!("\"");
                println!();
            }

            search_pos = actual_pos + pattern.len();
        } else {
            break;
        }
    }

    if found_count == 0 {
        println!("String not found");
    } else if found_count == limit {
        println!("(... showing first {} occurrences)", limit);
    } else {
        println!("Total occurrences: {}", found_count);
    }

    Ok(())
}

/// Search for topics by pattern.
fn cmd_search_topics(input: PathBuf, pattern: String, json: bool) -> Result<()> {
    let reader = open_reader(&input)?;
    let lower_pattern = pattern.to_lowercase();

    let mut found = false;

    for channel in reader.channels().values() {
        if channel.topic.to_lowercase().contains(&lower_pattern) {
            found = true;
            if json {
                println!(
                    "{{\"topic\": \"{}\", \"type\": \"{}\", \"messages\": {}}}",
                    channel.topic, channel.message_type, channel.message_count
                );
            } else {
                println!("Topic: {}", channel.topic);
                println!("  Type: {}", channel.message_type);
                println!("  Messages: {}", channel.message_count);
                println!();
            }
        }
    }

    if !found {
        println!("No topics matching '{:?}' found", pattern);
    }

    Ok(())
}

/// Search for message types by pattern.
fn cmd_search_types(input: PathBuf, pattern: String, json: bool) -> Result<()> {
    let reader = open_reader(&input)?;
    let lower_pattern = pattern.to_lowercase();

    let mut found = false;

    for channel in reader.channels().values() {
        if channel.message_type.to_lowercase().contains(&lower_pattern) {
            found = true;
            if json {
                println!(
                    "{{\"topic\": \"{}\", \"type\": \"{}\", \"messages\": {}}}",
                    channel.topic, channel.message_type, channel.message_count
                );
            } else {
                println!("Type: {}", channel.message_type);
                println!("  Topic: {}", channel.topic);
                println!("  Messages: {}", channel.message_count);
                println!();
            }
        }
    }

    if !found {
        println!("No message types matching '{:?}' found", pattern);
    }

    Ok(())
}

/// Show fields for a topic.
fn cmd_show_fields(input: PathBuf, topic: String, json: bool) -> Result<()> {
    if json {
        return Err(anyhow::anyhow!(
            "JSON output is not yet implemented for the fields command. \
             Use the default text output instead."
        ));
    }

    let reader = open_reader(&input)?;

    // Find the channel
    let channel = reader
        .channels()
        .values()
        .find(|ch| ch.topic == topic || ch.topic.contains(&topic))
        .or_else(|| {
            println!("Topic '{}' not found", topic);
            println!();
            println!("Available topics:");
            for ch in reader.channels().values() {
                println!("  {}", ch.topic);
            }
            None
        });

    let Some(channel) = channel else {
        return Ok(());
    };

    println!("Fields for topic: {}", channel.topic);
    println!("Message type: {}", channel.message_type);
    println!();

    if let Some(schema) = &channel.schema {
        // Parse and display fields
        for line in schema.lines() {
            let line = line.trim();

            // Skip comments, empty lines, and Header
            if line.is_empty() || line.starts_with('#') || line.starts_with("Header header") {
                continue;
            }

            // Extract field name and type
            // Format: "type name" or "type name=default" or "type name[length]"
            if let Some(space_pos) = line.find(char::is_whitespace) {
                let rest = line[space_pos..].trim_start();
                if let Some(name_end) =
                    rest.find(|c: char| c == '=' || c == '[' || c.is_whitespace())
                {
                    let field_name = &rest[..name_end];
                    let field_type = &line[..space_pos].trim();
                    println!("  {} : {}", field_name, field_type);
                }
            }
        }
    } else {
        println!("(no schema available)");
    }

    Ok(())
}

/// Search for field values.
fn cmd_search_values(input: PathBuf, topic: String, field: String, limit: usize) -> Result<()> {
    let reader = open_reader(&input)?;

    println!("Searching for field '{}' in topic '{}'", field, topic);
    println!();

    // Find the target channel
    let target_channel = reader
        .channels()
        .values()
        .find(|ch| ch.topic == topic || ch.topic.contains(&topic));

    let Some(target_channel) = target_channel else {
        println!("Topic '{}' not found", topic);
        return Ok(());
    };

    // Note: Actual implementation would decode messages and search for field values
    // For now, this is a placeholder showing the structure

    if limit != 10 {
        println!("Note: Custom limit not yet supported, using default behavior");
    }
    println!("Note: Message decoding requires format-specific reader");
    println!(
        "Channel: {} ({})",
        target_channel.topic, target_channel.message_type
    );

    Ok(())
}
