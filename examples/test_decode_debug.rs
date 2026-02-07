// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Debug example for decoding ROS bag messages.
//!
//! This example demonstrates how to decode messages from a ROS bag file.
//! It's primarily used for debugging and development purposes.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example test_decode_debug -- path/to/file.bag
//! ```
//!
//! Or via environment variable:
//!
//! ```bash
//! BAG_PATH=path/to/file.bag cargo run --example test_decode_debug
//! ```

use robocodec::encoding::CdrDecoder;
use robocodec::io::formats::bag::BagFormat;
use robocodec::schema::parse_schema;
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
            eprintln!("  cargo run --example test_decode_debug -- <path-to-bag>");
            eprintln!();
            eprintln!("Or set BAG_PATH environment variable:");
            eprintln!("  BAG_PATH=<path-to-bag> cargo run --example test_decode_debug");
            eprintln!();
            std::process::exit(1);
        });

    println!("Opening bag file: {}", path);
    let reader = BagFormat::open(&path)?;

    let mut iter = reader.iter_raw()?;

    // Find a simple message to debug
    while let Some(Ok((msg, channel))) = iter.next() {
        // Try the metadata message which has a simple structure
        if channel.topic.contains("metadata") {
            println!("Topic: {}", channel.topic);
            println!("Type: {}", channel.message_type);
            println!("Data length: {}", msg.data.len());
            println!(
                "First 64 bytes: {:02x?}",
                &msg.data[..msg.data.len().min(64)]
            );

            // Parse the schema
            if let Some(schema_str) = &channel.schema {
                println!("\nSchema:\n{}", schema_str);

                // Try to parse and decode
                match parse_schema(&channel.message_type, schema_str) {
                    Ok(schema) => {
                        println!("\nParsed schema successfully");
                        println!(
                            "Schema types: {:?}",
                            schema.types.keys().collect::<Vec<_>>()
                        );

                        // Try decoding
                        let decoder = CdrDecoder::new();
                        match decoder.decode_headerless_ros1(
                            &schema,
                            &msg.data,
                            Some(&channel.message_type),
                        ) {
                            Ok(decoded) => {
                                println!("\nDecoded successfully!");
                                for (k, v) in decoded.iter() {
                                    println!("  {}: {:?}", k, v);
                                }
                            }
                            Err(e) => {
                                println!("\nDecode error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("\nSchema parse error: {}", e);
                    }
                }
            }
            break;
        }
    }

    Ok(())
}
