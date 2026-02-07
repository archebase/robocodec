// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Example of decoding a small number of messages from a ROS bag file.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example test_bag_decode_small -- path/to/file.bag
//! ```
//!
//! Or via environment variable:
//!
//! ```bash
//! BAG_PATH=path/to/file.bag cargo run --example test_bag_decode_small
//! ```

use robocodec::FormatReader;
use robocodec::io::formats::bag::BagFormat;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get path from command-line argument or environment variable
    let path = env::args()
        .nth(1)
        .or_else(|| env::var("BAG_PATH").ok())
        .unwrap_or_else(|| {
            eprintln!("Error: No bag file path provided");
            eprintln!();
            eprintln!("Usage:");
            eprintln!("  cargo run --example test_bag_decode_small -- <path-to-bag>");
            eprintln!();
            eprintln!("Or set BAG_PATH environment variable:");
            eprintln!("  BAG_PATH=<path-to-bag> cargo run --example test_bag_decode_small");
            eprintln!();
            std::process::exit(1);
        });

    let reader = BagFormat::open(&path)?;

    println!("Opened bag file");
    println!("Channels: {}", reader.channels().len());
    println!("Total messages: {}", reader.message_count());

    // Try to decode messages
    let decoded_iter = reader.decode_messages()?;
    let mut stream = decoded_iter.stream()?;

    let mut count = 0;
    let mut errors = 0;
    let mut metadata_count = 0;

    for result in &mut stream {
        match result {
            Ok((msg, channel)) => {
                count += 1;
                if channel.topic.contains("metadata") {
                    metadata_count += 1;
                    if metadata_count <= 3 {
                        println!(
                            "Metadata message {}: topic={}, fields={}",
                            metadata_count,
                            channel.topic,
                            msg.len()
                        );
                    }
                }
                if count <= 5 {
                    println!(
                        "Message {}: topic={}, fields={}",
                        count,
                        channel.topic,
                        msg.len()
                    );
                }
            }
            Err(e) => {
                errors += 1;
                if errors <= 5 {
                    eprintln!("Error {}: {}", errors, e);
                }
            }
        }
        if count >= 100 || errors >= 100 {
            break;
        }
    }

    println!("\nSuccessfully decoded {} messages", count);
    println!("Metadata messages decoded: {}", metadata_count);
    println!("Total errors: {}", errors);

    Ok(())
}
