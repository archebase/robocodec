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
//! # Rewrite a file (same format)
//! robocodec rewrite input.bag output.bag
//!
//! # Extract data
//! robocodec extract topics input.mcap output.mcap /camera,/lidar
//!
//! # Search for patterns
//! robocodec search topics input.mcap sensor
//! ```

mod cmds;

use std::process;

use clap::{Parser, Subcommand};
use cmds::{ExtractCmd, InspectCmd, RewriteCmd, SchemaCmd, SearchCmd};
use robocodec::cli::Result;

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

    /// Rewrite a file (same format only)
    Rewrite(RewriteCmd),

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
        Commands::Rewrite(cmd) => cmd.run(),
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
