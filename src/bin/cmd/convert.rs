// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Convert command - convert between formats and apply transformations.

use std::path::PathBuf;

use clap::Subcommand;

use crate::common::Result;
use robocodec::RoboRewriter;

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
            ConvertCmd::ToMcap { input, output } => cmd_convert(input, output, "BAG", "MCAP"),
            ConvertCmd::ToBag { input, output } => cmd_convert(input, output, "MCAP", "BAG"),
            ConvertCmd::Normalize {
                input,
                output,
                rename_topic,
            } => cmd_normalize(input, output, rename_topic),
        }
    }
}

/// Convert between formats (unified implementation).
fn cmd_convert(input: PathBuf, output: PathBuf, src_format: &str, dst_format: &str) -> Result<()> {
    println!("Converting {} to {}:", src_format, dst_format);
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    let mut rewriter = RoboRewriter::open(&input)?;
    let stats = rewriter.rewrite(&output)?;

    println!("  Messages written: {}", stats.message_count);
    println!("  Channels: {}", stats.channel_count);
    println!("  Conversion complete!");
    Ok(())
}

/// Normalize with transformations.
fn cmd_normalize(input: PathBuf, output: PathBuf, renames: Option<Vec<String>>) -> Result<()> {
    println!("Normalizing:");
    println!("  Input:  {}", input.display());
    println!("  Output: {}", output.display());

    // Apply topic renames if provided
    if let Some(renames) = renames {
        println!("  Applying topic renames:");
        for rename in &renames {
            let parts: Vec<&str> = rename.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid rename format '{}'. Expected: oldname=newname",
                    rename
                ));
            }
            let old_name = parts[0];
            let new_name = parts[1];
            if old_name.is_empty() || new_name.is_empty() {
                return Err(anyhow::anyhow!(
                    "Invalid rename format '{}': both old and new names must be non-empty",
                    rename
                ));
            }
            println!("    {} -> {}", old_name, new_name);
        }

        // Topic renaming is not yet implemented in the CLI
        return Err(anyhow::anyhow!(
            "Topic renaming transformations are not yet implemented in the CLI. \
             You can use the library API directly:\n\
             \n\
             use robocodec::{{RoboRewriter, RewriteOptions, transform::TransformBuilder}};\n\
             let pipeline = TransformBuilder::new()\n\
                 .with_topic_rename(\"/old\", \"/new\")\n\
                 .build();\n\
             let options = RewriteOptions::default().with_transforms(pipeline);\n\
             let mut rewriter = RoboRewriter::with_options(&input, options)?;\n\
             rewriter.rewrite(&output)?;\n"
        ));
    }

    let mut rewriter = RoboRewriter::open(&input)?;
    let stats = rewriter.rewrite(&output)?;

    println!("  Messages rewritten: {}", stats.message_count);
    println!("  Channels: {}", stats.channel_count);

    Ok(())
}
