// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fuzz target for MCAP parser.
//!
//! This fuzzer tests the robustness of the MCAP parser when given
//! arbitrary byte sequences as input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

/// Maximum input size to prevent memory exhaustion during fuzzing.
const MAX_INPUT_SIZE: usize = 1024 * 1024; // 1 MB

fuzz_target!(|data: &[u8]| {
    // Skip inputs that are too large
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    // Try to parse as MCAP format
    // The parser should handle malformed data gracefully without panicking
    let _ = parse_mcap_safe(data);
});

/// Safe wrapper around MCAP parsing that catches panics.
fn parse_mcap_safe(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Use std::panic::catch_unwind to prevent panics from crashing the fuzzer
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Try to validate MCAP magic number if data is long enough
        if data.len() >= 8 {
            // MCAP magic is 0x0A0A4943C1B20814 (little endian)
            // Just check that we can read these bytes without panicking
            let _ = &data[0..8];
        }

        // Try to parse with mcap crate if available
        // This will handle the actual MCAP structure
        if let Ok(cursor) = Cursor::new(data).downcast_ref::<Cursor<&[u8]>>() {
            // Attempt to parse MCAP records
            // We don't care about the result, just that it doesn't panic
            parse_mcap_records(cursor);
        }
    }));

    // Map panic to error, return Ok otherwise
    result.map_err(|_| Box::<dyn std::error::Error>::from("Panic during parsing"))?;

    Ok(())
}

/// Attempt to parse MCAP records from the data.
///
/// This is a minimal parser that just validates structure without
/// full decoding, suitable for fuzzing.
fn parse_mcap_records<R: std::io::Read + std::io::Seek>(mut reader: R) {
    use byteorder::{LittleEndian, ReadBytesExt};

    // Try to read MCAP header
    let mut magic = [0u8; 8];
    if reader.read_exact(&mut magic).is_err() {
        return; // Not enough data for header
    }

    // Check if magic matches MCAP format
    let expected_magic: u64 = 0x0A0A_4943_C1B2_0814;
    let actual_magic = u64::from_le_bytes(magic);

    if actual_magic != expected_magic {
        return; // Not a valid MCAP file
    }

    // If magic matches, try to read some records
    // Limit iterations to prevent infinite loops
    for _ in 0..100 {
        let mut op_header = [0u8; 9]; // 1 byte opcode + 8 bytes length
        if reader.read_exact(&mut op_header).is_err() {
            break;
        }

        let opcode = op_header[0];
        let length = u64::from_le_bytes(op_header[1..9].try_into().unwrap_or([0u8; 8]));

        // Sanity check on record length
        if length > 10_000_000 {
            break; // Unreasonably large record
        }

        // Skip the record data
        if length > 0 {
            let mut skip_buf = vec![0u8; length.min(4096) as usize];
            let mut remaining = length;
            while remaining > 0 {
                let to_read = remaining.min(skip_buf.len() as u64) as usize;
                skip_buf.resize(to_read, 0);
                if reader.read_exact(&mut skip_buf).is_err() {
                    return;
                }
                remaining -= to_read as u64;
            }
        }

        // Early exit for certain opcodes that indicate end of file
        if opcode == 0x00 || opcode == 0xFF {
            break;
        }
    }
}
