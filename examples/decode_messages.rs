// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Message decoding example.
//!
//! Demonstrates iterating through decoded messages with timestamps.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example decode_messages -- path/to/file.mcap
//! ```

use robocodec::{FormatReader, RoboReader};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: cargo run --example decode_messages -- <path-to-file>");
        eprintln!(
            "Example: cargo run --example decode_messages -- tests/fixtures/robocodec_test_14.mcap"
        );
        std::process::exit(1);
    });

    let reader = RoboReader::open(&path)?;

    println!("📁 File: {}", path);
    println!("📊 Format: {:?}", reader.format());
    println!("💬 Total messages: {}", reader.message_count());
    println!();

    // Iterate through decoded messages
    let decoded = reader.decoded()?;
    let mut count = 0;
    let max_messages = 10;

    for result in decoded {
        match result {
            Ok(msg_result) => {
                count += 1;
                if count <= max_messages {
                    println!("Message #{}:", count);
                    println!(
                        "  Topic: {} ({})",
                        msg_result.channel.topic, msg_result.channel.message_type
                    );
                    println!("  Log time: {:?}", msg_result.log_time);
                    println!("  Publish time: {:?}", msg_result.publish_time);
                    println!("  Fields: {}", msg_result.message.len());
                    if !msg_result.message.is_empty() {
                        println!("  Sample fields:");
                        for (name, value) in msg_result.message.iter().take(3) {
                            println!("    - {}: {:?}", name, value);
                        }
                    }
                    println!();
                }
            }
            Err(e) => {
                eprintln!("Error decoding message: {}", e);
            }
        }

        if count >= max_messages {
            println!("(Showing first {} messages)", max_messages);
            break;
        }
    }

    Ok(())
}
