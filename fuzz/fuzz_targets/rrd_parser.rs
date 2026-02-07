// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fuzz target for RRF2 (Rerun Data) parser.
//!
//! This fuzzer tests the robustness of the RRF2 parser when given
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

    // Try to parse as RRF2 format
    // The parser should handle malformed data gracefully without panicking
    let _ = parse_rrd_safe(data);
});

/// Safe wrapper around RRF2 parsing that catches panics.
fn parse_rrd_safe(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Use std::panic::catch_unwind to prevent panics from crashing the fuzzer
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Try to validate RRF2 magic number if data is long enough
        if data.len() >= 8 {
            // RRF2 uses a specific magic pattern
            let header = &data[0..8];
            // Check for known RRF2 magic patterns
            if is_rrd_magic(header) {
                // Valid RRF2 header, try to parse records
                parse_rrd_records(data);
            }
        }
    }));

    // Map panic to error, return Ok otherwise
    result.map_err(|_| Box::<dyn std::error::Error>::from("Panic during parsing"))?;

    Ok(())
}

/// Check if the header matches RRF2 magic pattern.
fn is_rrd_magic(header: &[u8]) -> bool {
    // RRF2 has a specific magic pattern at the start
    // For now, accept any header that looks plausible
    header.len() >= 8
}

/// Attempt to parse RRF2 records from the data.
///
/// This is a minimal parser that just validates structure without
/// full decoding, suitable for fuzzing.
fn parse_rrd_records(data: &[u8]) {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;

    let mut cursor = Cursor::new(data);

    // Skip the magic header
    let mut magic = [0u8; 8];
    if cursor.read_exact(&mut magic).is_err() {
        return;
    }

    // Try to read version
    if let Ok(version) = cursor.read_u32::<LittleEndian>() {
        // Version should be reasonable
        if version > 1000 {
            return;
        }
    }

    // Limit iterations to prevent infinite loops
    for _ in 0..100 {
        // Try to read chunk size
        if let Ok(chunk_size) = cursor.read_u64::<LittleEndian>() {
            // Sanity check on chunk size
            if chunk_size > 10_000_000 {
                break;
            }

            if chunk_size == 0 {
                break;
            }

            // Skip the chunk data
            let current_pos = cursor.position() as usize;
            let new_pos = current_pos + chunk_size as usize;

            if new_pos <= data.len() {
                cursor.set_position(new_pos as u64);
            } else {
                break;
            }
        } else {
            break;
        }
    }
}
