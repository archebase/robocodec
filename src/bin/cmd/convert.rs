// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Convert command - convert between formats and apply transformations.

use std::path::PathBuf;

use clap::Subcommand;

use crate::common::Result;
use robocodec::{FormatReader, FormatWriter, RoboReader, RoboRewriter, RoboWriter};

/// Convert between formats or apply transformations.
#[derive(Subcommand, Clone, Debug)]
pub enum ConvertCmd {
    /// Convert BAG to MCAP format
    ToMcap {
        /// Input BAG file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output MCAP file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,
    },

    /// Convert MCAP to BAG format
    ToBag {
        /// Input MCAP file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output BAG file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,
    },

    /// Rewrite file with transformations (topic/type renaming, filtering)
    Normalize {
        /// Input file
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Rename topics (format: old=new)
        #[arg(short, long)]
        rename_topic: Option<Vec<String>>,
    },
}

impl ConvertCmd {
    pub fn run(self) -> Result<()> {
        match self {
            ConvertCmd::ToMcap { input, output } => cmd_bag_to_mcap(input, output),
            ConvertCmd::ToBag { input, output } => cmd_mcap_to_bag(input, output),
            ConvertCmd::Normalize {
                input,
                output,
                rename_topic,
            } => cmd_normalize(input, output, rename_topic),
        }
    }
}

/// Convert BAG to MCAP.
fn cmd_bag_to_mcap(input: PathBuf, output: PathBuf) -> Result<()> {
    println!("Converting BAG to MCAP:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let reader = RoboReader::open(&input)?;
    println!("  Channels: {}", reader.channels().len());
    println!("  Messages: {}", reader.message_count());

    let mut writer = RoboWriter::create(&output)?;

    // Add all channels
    for (&_ch_id, channel) in reader.channels() {
        writer.add_channel(
            &channel.topic,
            &channel.message_type,
            &channel.encoding,
            channel.schema.as_deref(),
        )?;
        println!(
            "  Added channel: {} ({})",
            channel.topic, channel.message_type
        );
    }

    // Note: Full message copying would require format-specific iteration
    println!("  (Full message conversion not yet implemented)");
    writer.finish()?;

    println!("  Conversion complete!");
    Ok(())
}

/// Convert MCAP to BAG.
fn cmd_mcap_to_bag(input: PathBuf, output: PathBuf) -> Result<()> {
    println!("Converting MCAP to BAG:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let reader = RoboReader::open(&input)?;
    println!("  Channels: {}", reader.channels().len());
    println!("  Messages: {}", reader.message_count());

    let mut writer = RoboWriter::create(&output)?;

    // Add all channels
    for (_ch_id, channel) in reader.channels() {
        writer.add_channel(
            &channel.topic,
            &channel.message_type,
            &channel.encoding,
            channel.schema.as_deref(),
        )?;
        println!(
            "  Added channel: {} ({})",
            channel.topic, channel.message_type
        );
    }

    println!("  (Full message conversion not yet implemented)");
    writer.finish()?;

    println!("  Conversion complete!");
    Ok(())
}

/// Normalize with transformations.
fn cmd_normalize(input: PathBuf, output: PathBuf, renames: Option<Vec<String>>) -> Result<()> {
    println!("Normalizing:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let mut rewriter = RoboRewriter::open(&input)?;

    // Apply topic renames if provided
    if let Some(renames) = renames {
        println!("  Applying topic renames:");
        for rename in renames {
            let parts: Vec<&str> = rename.splitn(2, '=').collect();
            if parts.len() == 2 {
                println!("    {} -> {}", parts[0], parts[1]);
                // TODO: Apply the rename transformation
            }
        }
    }

    let stats = rewriter.rewrite(&output)?;

    println!("  Messages rewritten: {}", stats.message_count);
    println!("  Channels: {}", stats.channel_count);

    Ok(())
}
