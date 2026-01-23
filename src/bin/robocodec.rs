// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! # Robocodec CLI
//!
//! Unified command-line tool for robotics data file operations.
//!
//! ## Usage
//!
//! ```sh
//! # Show file information
//! robocodec inspect info file.mcap
//!
//! # List topics
//! robocodec inspect topics file.bag
//!
//! # Convert formats
//! robocodec convert to-mcap input.bag output.mcap
//!
//! # Extract data
//! robocodec extract topics input.mcap output.mcap /camera,/lidar
//!
//! # Search for patterns
//! robocodec search topics input.mcap sensor
//! ```

mod cmd;
mod common;

use std::process;

use clap::{Parser, Subcommand};
use cmd::{ConvertCmd, ExtractCmd, InspectCmd, SchemaCmd, SearchCmd};
use common::Result;

/// Robocodec - Robotics data format toolkit
///
/// Work with MCAP and ROS bag files through a unified interface.
/// Format auto-detection means you rarely need to specify file types.
#[derive(Parser, Clone)]
#[command(name = "robocodec")]
#[command(about = "Robotics data format toolkit for MCAP and ROS bag files", long_about = None)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "ArcheBase")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Available commands
#[derive(Subcommand, Clone)]
enum Commands {
    /// Inspect file contents (info, topics, schemas, messages)
    #[command(subcommand)]
    Inspect(InspectCmd),

    /// Convert between formats (bag-to-mcap, mcap-to-bag, normalize)
    #[command(subcommand)]
    Convert(ConvertCmd),

    /// Extract subsets of data (by topic, time, count)
    #[command(subcommand)]
    Extract(ExtractCmd),

    /// Search within files (bytes, strings, topics, fields)
    #[command(subcommand)]
    Search(SearchCmd),

    /// Schema operations (list, show, validate, diff)
    #[command(subcommand)]
    Schema(SchemaCmd),
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Inspect(cmd) => cmd.run(),
        Commands::Convert(cmd) => cmd.run(),
        Commands::Extract(cmd) => cmd.run(),
        Commands::Search(cmd) => cmd.run(),
        Commands::Schema(cmd) => cmd.run(),
    }
}

fn main() {
    let result = run();

    if let Err(e) = result {
        eprintln!("Error: {e}");
        process::exit(1);
    }
}
