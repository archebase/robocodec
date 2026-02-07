// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Example showing detailed decoding trace for ROS bag messages.
//!
//! This example demonstrates manual CDR decoding with detailed offset tracing.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example test_decode_trace -- path/to/file.bag
//! ```
//!
//! Or via environment variable:
//!
//! ```bash
//! BAG_PATH=path/to/file.bag cargo run --example test_decode_trace
//! ```

use robocodec::encoding::cdr::cursor::CdrCursor;
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
            eprintln!("  cargo run --example test_decode_trace -- <path-to-bag>");
            eprintln!();
            eprintln!("Or set BAG_PATH environment variable:");
            eprintln!("  BAG_PATH=<path-to-bag> cargo run --example test_decode_trace");
            eprintln!();
            std::process::exit(1);
        });

    let reader = BagFormat::open(&path)?;

    let mut iter = reader.iter_raw()?;

    // Find a metadata message
    for result in &mut iter {
        let Ok((msg, channel)) = result else { continue };
        if !channel.topic.contains("metadata") {
            continue;
        }

        println!("Topic: {}", channel.topic);
        println!("Data length: {}", msg.data.len());

        // Create a ROS1 cursor and manually decode
        let mut cursor = CdrCursor::new_headerless_ros1(&msg.data, true);

        println!("\nManual decoding:");
        println!("is_ros1: {}", cursor.is_ros1());

        // Read Header.seq (uint32)
        let seq = cursor.read_u32()?;
        println!("Header.seq = {} (offset now: {})", seq, cursor.position());

        // Read Header.stamp.sec (int32)
        let stamp_sec = cursor.read_i32()?;
        println!(
            "Header.stamp.sec = {} (offset now: {})",
            stamp_sec,
            cursor.position()
        );

        // Read Header.stamp.nsec (uint32)
        let stamp_nsec = cursor.read_u32()?;
        println!(
            "Header.stamp.nsec = {} (offset now: {})",
            stamp_nsec,
            cursor.position()
        );

        // Read Header.frame_id length (uint32)
        let frame_id_len = cursor.read_u32()?;
        println!(
            "Header.frame_id length = {} (offset now: {})",
            frame_id_len,
            cursor.position()
        );

        // Read Header.frame_id string
        let frame_id_bytes = cursor.read_bytes(frame_id_len as usize)?;
        let frame_id = String::from_utf8_lossy(frame_id_bytes);
        println!(
            "Header.frame_id = '{}' (offset now: {})",
            frame_id,
            cursor.position()
        );

        // Read json_data length (uint32)
        let json_data_len = cursor.read_u32()?;
        println!(
            "json_data length = {} (offset now: {})",
            json_data_len,
            cursor.position()
        );

        // Read json_data string (partial)
        let json_data_bytes = cursor.read_bytes(json_data_len.min(50) as usize)?;
        let json_data = String::from_utf8_lossy(json_data_bytes);
        println!("json_data (partial) = '{}'", json_data);

        break;
    }

    Ok(())
}
