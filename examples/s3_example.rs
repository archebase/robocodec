// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 remote file access example.
//!
//! Demonstrates reading robotics data files from S3-compatible storage
//! including AWS S3, MinIO, Alibaba OSS, and other S3-compatible services.
//!
//! # Usage
//!
//! ```bash
//! # S3 (requires credentials via environment variables)
//! export AWS_ACCESS_KEY_ID="your-access-key"
//! export AWS_SECRET_ACCESS_KEY="your-secret-key"
//! cargo run --example s3_example -- s3://my-bucket/path/to/data.mcap
//!
//! # S3 with custom endpoint (MinIO, Alibaba OSS, etc.)
//! cargo run --example s3_example -- "s3://bucket/data.mcap?endpoint=http://localhost:9000"
//! ```

use robocodec::{FormatReader, RoboReader};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: cargo run --example s3_example -- <s3-url>");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  cargo run --example s3_example -- s3://my-bucket/path/to/data.mcap");
        eprintln!(
            "  cargo run --example s3_example -- s3://bucket/data.mcap?endpoint=http://localhost:9000"
        );
        eprintln!();
        eprintln!("S3 credentials (via environment variables):");
        eprintln!("  AWS_ACCESS_KEY_ID");
        eprintln!("  AWS_SECRET_ACCESS_KEY");
        eprintln!("  AWS_REGION (optional, defaults to us-east-1)");
        std::process::exit(1);
    }

    let url = &args[1];

    if !url.starts_with("s3://") {
        eprintln!("Error: Only s3:// URLs are supported");
        eprintln!("Got: {}", url);
        std::process::exit(1);
    }

    println!("🌐 Opening: {}", url);

    // Format and transport auto-detected from URL scheme
    let reader = RoboReader::open(url)?;

    println!("📊 Format: {:?}", reader.format());
    println!("📝 Channels: {}", reader.channels().len());
    println!("💬 Total messages: {}", reader.message_count());
    println!();

    println!("─── Channels ───");
    for channel in reader.channels().values() {
        println!(
            "  • {} ({}) - {} messages",
            channel.topic, channel.message_type, channel.message_count
        );
    }

    println!();
    println!("✅ Successfully accessed S3 file!");

    Ok(())
}
