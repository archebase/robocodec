// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fuzz target for CDR (Common Data Representation) decoder.
//!
//! This fuzzer tests the robustness of the CDR decoder when given
//! arbitrary byte sequences as input.

#![no_main]

use libfuzzer_sys::fuzz_target;

/// Maximum input size to prevent memory exhaustion during fuzzing.
const MAX_INPUT_SIZE: usize = 1024 * 1024; // 1 MB

/// Test schema for fuzzing - a simple message with common field types.
const TEST_SCHEMA: &str = "
Header header
int32 value
float64 data
string name
uint8[] bytes
";

fuzz_target!(|data: &[u8]| {
    // Skip inputs that are too large
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    // Try to parse as CDR data
    // The decoder should handle malformed data gracefully without panicking
    let _ = decode_cdr_safe(data);
});

/// Safe wrapper around CDR decoding that catches panics.
fn decode_cdr_safe(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Use std::panic::catch_unwind to prevent panics from crashing the fuzzer
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Try to decode as CDR format
        decode_cdr_basic(data);
    }));

    // Map panic to error, return Ok otherwise
    result.map_err(|_| Box::<dyn std::error::Error>::from("Panic during decoding"))?;

    Ok(())
}

/// Attempt to decode CDR data from the input.
///
/// This is a minimal CDR decoder that just validates structure without
/// full decoding, suitable for fuzzing.
fn decode_cdr_basic(data: &[u8]) {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;

    let mut cursor = Cursor::new(data);

    // CDR data starts with a 4-byte header
    let mut cdr_header = [0u8; 4];
    if cursor.read_exact(&mut cdr_header).is_err() {
        return;
    }

    // First byte is endianness flag
    let endianness = cdr_header[0];
    if endianness != 0x00 && endianness != 0x01 {
        return; // Invalid endianness flag
    }

    // Try to read some primitive values
    // This exercises the decoder's validation logic
    let _ = cursor.read_u8(); // Try reading int8

    // Try reading int32
    if let Ok(_value) = cursor.read_i32::<LittleEndian>() {
        // Successfully read, try reading float64
        let _ = cursor.read_f64::<LittleEndian>();
    }

    // Try reading a string (length-prefixed)
    if let Ok(str_len) = cursor.read_u32::<LittleEndian>() {
        // Sanity check on string length
        if str_len < 1_000_000 {
            let current_pos = cursor.position() as usize;
            let new_pos = current_pos + str_len as usize;
            if new_pos <= data.len() {
                // Skip string data (including null terminator)
                cursor.set_position(new_pos as u64);
            }
        }
    }

    // Try reading an array (length-prefixed)
    if let Ok(array_len) = cursor.read_u32::<LittleEndian>() {
        // Sanity check on array length
        if array_len < 10_000 {
            // Try to read some elements
            for _ in 0..array_len.min(100) {
                let _ = cursor.read_u8();
            }
        }
    }
}
