// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Basic file inspection example.
//!
//! Demonstrates opening robotics data files (MCAP, ROS1 bag) and inspecting
//! their metadata, channels, and message counts.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example read_file -- path/to/file.mcap
//! ```

use robocodec::{FormatReader, RoboReader};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: cargo run --example read_file -- <path-to-file>");
        eprintln!(
            "Example: cargo run --example read_file -- tests/fixtures/robocodec_test_14.mcap"
        );
        std::process::exit(1);
    });

    // Open file with automatic format detection
    let reader = RoboReader::open(&path)?;

    println!("📁 File: {}", path);
    println!("📊 Format: {:?}", reader.format());
    println!("📝 Channels: {}", reader.channels().len());
    println!("💬 Total messages: {}", reader.message_count());

    println!("\n─── Channels ───");
    for (_id, channel) in reader.channels() {
        println!(
            "  • {} ({}) - {} messages",
            channel.topic, channel.message_type, channel.message_count
        );
    }

    Ok(())
}
