// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! File rewriting example.
//!
//! Demonstrates rewriting a robotics data file with the same format.
//! The rewriter can apply topic and type transformations during the process.
//!
//! # Usage
//!
//! ```bash
//! # Rewrite MCAP file (same format, can apply transformations)
//! cargo run --example convert_format -- input.mcap output.mcap
//!
//! # Rewrite ROS1 bag file (same format, can apply transformations)
//! cargo run --example convert_format -- input.bag output.bag
//! ```

use robocodec::RoboRewriter;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: cargo run --example convert_format -- <input-file> <output-file>");
        eprintln!();
        eprintln!("Rewrites a file in the same format. Output format must match input format.");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  cargo run --example convert_format -- input.mcap output.mcap");
        eprintln!("  cargo run --example convert_format -- input.bag output.bag");
        eprintln!();
        eprintln!("Note: See transform.rs for examples of applying topic/type transformations.");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    // Validate that input and output have the same extension
    let input_ext = input_path.rsplit('.').next().unwrap_or("");
    let output_ext = output_path.rsplit('.').next().unwrap_or("");

    if input_ext != output_ext {
        eprintln!("Error: Input and output formats must match.");
        eprintln!("  Input format: {}", input_ext);
        eprintln!("  Output format: {}", output_ext);
        eprintln!();
        eprintln!("Note: Cross-format conversion is not currently supported.");
        eprintln!("      The rewriter preserves the same format as the input file.");
        std::process::exit(1);
    }

    println!("🔄 Rewriting {} → {}", input_path, output_path);

    // Create rewriter (format auto-detected from input)
    let mut rewriter = RoboRewriter::open(input_path)?;
    println!("   Format: {}", input_ext.to_uppercase());
    println!("   Input: {}", rewriter.input_path().display());

    // Rewrite (same format)
    let stats = rewriter.rewrite(output_path)?;

    println!();
    println!("✅ Rewrite complete!");
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
