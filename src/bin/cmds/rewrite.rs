// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Rewrite command - rewrite a file in the same format.

use std::path::PathBuf;

use clap::Args;

use robocodec::RoboRewriter;
use robocodec::cli::Result;

/// Rewrite a robotics data file (same format only).
///
/// Creates a new file with the same format as the input, re-encoding
/// all messages. This can be used to normalize file structure or
/// apply transformations in the future.
#[derive(Args, Clone, Debug)]
pub struct RewriteCmd {
    /// Input file path
    #[arg(value_name = "INPUT")]
    pub input: PathBuf,

    /// Output file path
    #[arg(value_name = "OUTPUT")]
    pub output: PathBuf,
}

impl RewriteCmd {
    pub fn run(self) -> Result<()> {
        // Check if this is a cross-format attempt
        let input_ext = self
            .input
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let output_ext = self
            .output
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        // Check if formats differ
        let is_cross_format = matches!(
            (input_ext.as_str(), output_ext.as_str()),
            ("bag", "mcap") | ("mcap", "bag")
        );

        if is_cross_format {
            return Err(anyhow::anyhow!(
                "Cross-format rewrite ({input_ext} → {output_ext}) is not supported.\n\n\
                robocodec can read and write both BAG and MCAP formats, but cannot convert between them.\n\
                The serialization formats are fundamentally different (ROS1 vs ROS2).\n\n\
                For format conversion, consider using:\n\
                - Foxglove's mcap CLI: https://github.com/foxglove/mcap\n\
                - rosbag tool for ROS1 bags\n\
                - ros2 bag tool for ROS2"
            ));
        }

        let format_name = match input_ext.as_str() {
            "bag" => "BAG",
            "mcap" => "MCAP",
            _ => "file",
        };

        println!("Rewriting {} file:", format_name);
        println!("  Input:  {}", self.input.display());
        println!("  Output: {}", self.output.display());

        let mut rewriter = RoboRewriter::open(&self.input)?;
        let stats = rewriter.rewrite(&self.output)?;

        println!("  Messages written: {}", stats.message_count);
        println!("  Channels: {}", stats.channel_count);
        println!("  Complete!");
        Ok(())
    }
}
