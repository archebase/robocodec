// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Format conversion example.
//!
//! Demonstrates converting between robotics data formats (MCAP ↔ ROS1 bag)
//! using the unified RoboRewriter API.
//!
//! # Usage
//!
//! ```bash
//! # Convert MCAP to ROS1 bag
//! cargo run --example convert_format -- input.mcap output.bag
//!
//! # Convert ROS1 bag to MCAP
//! cargo run --example convert_format -- input.bag output.mcap
//! ```

use robocodec::RoboRewriter;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: cargo run --example convert_format -- <input-file> <output-file>");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  cargo run --example convert_format -- input.mcap output.bag");
        eprintln!("  cargo run --example convert_format -- input.bag output.mcap");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    println!("🔄 Converting {} → {}", input_path, output_path);

    // Create rewriter (format auto-detected from input)
    let mut rewriter = RoboRewriter::open(input_path)?;
    println!("   Input format: {:?}", input_path.rsplit('.').next());
    println!("   Input: {}", rewriter.input_path().display());

    // Detect output format from extension
    let output_format = if output_path.ends_with(".mcap") {
        "MCAP"
    } else if output_path.ends_with(".bag") {
        "ROS1 Bag"
    } else {
        "Unknown"
    };
    println!("   Output format: {}", output_format);

    // Convert
    let stats = rewriter.rewrite(output_path)?;

    println!();
    println!("✅ Conversion complete!");
    println!("   Messages processed: {}", stats.message_count);
    println!("   Channels processed: {}", stats.channel_count);
    if stats.decode_failures > 0 {
        println!("   ⚠️  Decode failures: {}", stats.decode_failures);
    }
    if stats.encode_failures > 0 {
        println!("   ⚠️  Encode failures: {}", stats.encode_failures);
    }

    Ok(())
}
