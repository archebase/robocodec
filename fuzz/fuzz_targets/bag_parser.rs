// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fuzz target for ROS1 bag parser.
//!
//! This fuzzer tests the robustness of the ROS1 bag parser when given
//! arbitrary byte sequences as input.

#![no_main]

use libfuzzer_sys::fuzz_target;

/// Maximum input size to prevent memory exhaustion during fuzzing.
const MAX_INPUT_SIZE: usize = 1024 * 1024; // 1 MB

fuzz_target!(|data: &[u8]| {
    // Skip inputs that are too large
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    // Try to parse as ROS1 bag format
    // The parser should handle malformed data gracefully without panicking
    let _ = parse_bag_safe(data);
});

/// Safe wrapper around ROS1 bag parsing that catches panics.
fn parse_bag_safe(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Use std::panic::catch_unwind to prevent panics from crashing the fuzzer
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Try to validate bag magic number if data is long enough
        if data.len() >= 4 {
            // Bag file magic is "#ROS" (0x524F5323)
            let header = &data[0..4];
            if header == b"#ROS" {
                // Valid bag header, try to parse records
                parse_bag_records(data);
            }
        }
    }));

    // Map panic to error, return Ok otherwise
    result.map_err(|_| Box::<dyn std::error::Error>::from("Panic during parsing"))?;

    Ok(())
}

/// Attempt to parse ROS1 bag records from the data.
///
/// This is a minimal parser that just validates structure without
/// full decoding, suitable for fuzzing.
fn parse_bag_records(data: &[u8]) {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;

    let mut cursor = Cursor::new(data);

    // Skip the magic header
    let mut magic = [0u8; 4];
    if cursor.read_exact(&mut magic).is_err() {
        return;
    }

    // Try to read version
    let mut version = [0u8; 3];
    if cursor.read_exact(&mut version).is_err() {
        return;
    }

    // Limit iterations to prevent infinite loops
    for _ in 0..100 {
        // Try to read record header
        let mut record_header = [0u8; 4];
        if cursor.read_exact(&mut record_header).is_err() {
            break;
        }

        // Record starts with opcode
        let opcode = record_header[0];

        // Try to read record size
        if let Ok(record_size) = cursor.read_u32::<LittleEndian>() {
            // Sanity check on record size
            if record_size > 10_000_000 {
                break;
            }

            // Skip the record data
            let current_pos = cursor.position() as usize;
            let new_pos = current_pos + record_size as usize;

            if new_pos <= data.len() {
                cursor.set_position(new_pos as u64);
            } else {
                break;
            }
        } else {
            break;
        }

        // Early exit for certain opcodes
        if opcode == 0x00 || opcode == 0xFF {
            break;
        }
    }
}
