// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Topic and type transformation example.
//!
//! Demonstrates renaming topics and message types during file rewriting.
//! The rewriter preserves the same format but can transform schema and metadata.
//!
//! # Usage
//!
//! ```bash
//! # Transform topics while rewriting (same format)
//! cargo run --example transform -- input.mcap output.mcap
//! ```

use robocodec::{RewriteOptions, RoboRewriter, TransformBuilder};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: cargo run --example transform -- <input-file> <output-file>");
        eprintln!();
        eprintln!(
            "Rewrites a file with topic/type transformations. Output format must match input format."
        );
        eprintln!();
        eprintln!("Example:");
        eprintln!("  cargo run --example transform -- input.mcap output.mcap");
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    println!("🔄 Transforming: {} → {}", input_path, output_path);

    // Build transformations: rename topics and types
    let transform = TransformBuilder::new()
        // Rename a specific topic
        .with_topic_rename("/old_camera/image_raw", "/camera/image")
        // Rename another topic (wildcard prefix not supported, use explicit renames)
        .with_topic_rename("/lidar/points_old", "/lidar/points")
        // Rename message types
        .with_type_rename("sensor_msgs/PointCloud2", "custom_msgs/PointCloud")
        .build();

    println!();
    println!("─── Transformations ───");
    println!("  Topic renames:");
    println!("    /old_camera/image_raw → /camera/image");
    println!("    /lidar/points_old → /lidar/points");
    println!("  Type renames:");
    println!("    sensor_msgs/PointCloud2 → custom_msgs/PointCloud");

    // Create rewriter with transformations
    let options = RewriteOptions::default().with_transforms(transform);
    let mut rewriter = RoboRewriter::with_options(input_path, options)?;

    println!();
    println!("   Input format: {:?}", input_path.rsplit('.').next());
    println!("   Input: {}", rewriter.input_path().display());

    // Apply transformations and convert
    let stats = rewriter.rewrite(output_path)?;

    println!();
    println!("✅ Transformation complete!");
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
