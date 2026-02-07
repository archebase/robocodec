// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Example of dumping raw messages from a ROS bag file.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example test_bag_dump -- path/to/file.bag
//! ```
//!
//! Or via environment variable:
//!
//! ```bash
//! BAG_PATH=path/to/file.bag cargo run --example test_bag_dump
//! ```

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
            eprintln!("  cargo run --example test_bag_dump -- <path-to-bag>");
            eprintln!();
            eprintln!("Or set BAG_PATH environment variable:");
            eprintln!("  BAG_PATH=<path-to-bag> cargo run --example test_bag_dump");
            eprintln!();
            std::process::exit(1);
        });

    let reader = BagFormat::open(&path)?;

    let mut iter = reader.iter_raw()?;

    // Look at first few messages
    for i in 0..5 {
        if let Some(Ok((msg, channel))) = iter.next() {
            println!(
                "Message {}: topic={}, data_len={}",
                i,
                channel.topic,
                msg.data.len()
            );
            println!(
                "  First 32 bytes: {:02x?}",
                &msg.data[..msg.data.len().min(32)]
            );
        }
    }

    Ok(())
}
