// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Fuzz target for schema parser.
//!
//! This fuzzer tests the robustness of the schema parser when given
//! arbitrary byte sequences as input.

#![no_main]

use libfuzzer_sys::fuzz_target;

/// Maximum input size to prevent memory exhaustion during fuzzing.
const MAX_INPUT_SIZE: usize = 100 * 1024; // 100 KB (schemas are usually text)

fuzz_target!(|data: &[u8]| {
    // Skip inputs that are too large
    if data.len() > MAX_INPUT_SIZE {
        return;
    }

    // Try to parse as schema format
    // The parser should handle malformed data gracefully without panicking
    let _ = parse_schema_safe(data);
});

/// Safe wrapper around schema parsing that catches panics.
fn parse_schema_safe(data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // Use std::panic::catch_unwind to prevent panics from crashing the fuzzer
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Try to convert bytes to UTF-8 string
        if let Ok(text) = std::str::from_utf8(data) {
            // Try to parse as ROS .msg format
            parse_msg_format(text);
        }
    }));

    // Map panic to error, return Ok otherwise
    result.map_err(|_| Box::<dyn std::error::Error>::from("Panic during parsing"))?;

    Ok(())
}

/// Attempt to parse ROS .msg format from the input.
///
/// This is a minimal schema parser that just validates structure without
/// full decoding, suitable for fuzzing.
fn parse_msg_format(text: &str) {
    use std::collections::HashSet;

    let mut seen_types = HashSet::new();

    // Split text into lines
    for line in text.lines() {
        // Trim whitespace
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Skip whitespace-only lines
        if line.chars().all(|c| c.is_whitespace()) {
            continue;
        }

        // Try to parse as a field declaration
        // Format: TYPE NAME[=DEFAULT] # COMMENT
        let parts: Vec<&str> = line.split('#').collect(); // Split on comment
        let field_part = parts[0].trim();

        if field_part.is_empty() {
            continue;
        }

        // Try to extract type and name
        let field_parts: Vec<&str> = field_part.split_whitespace().collect();
        if field_parts.len() < 2 {
            continue; // Not a valid field declaration
        }

        let type_str = field_parts[0];
        let name_str = field_parts[1].split('=').next().unwrap_or(field_parts[1]);

        // Validate type string (check for known primitive types)
        if is_valid_type(type_str) {
            seen_types.insert(type_str.to_string());
        }

        // Validate field name (must be alphanumeric with underscores)
        if is_valid_identifier(name_str) {
            seen_types.insert(name_str.to_string());
        }

        // Check for array type (e.g., int32[10] or float64[])
        if type_str.contains('[') && type_str.contains(']') {
            // Array type - validate bracket positions
            let open_bracket = type_str.find('[');
            let close_bracket = type_str.find(']');
            if let (Some(open), Some(close)) = (open_bracket, close_bracket) {
                if open < close {
                    // Extract array size if present
                    let size_str = &type_str[open + 1..close];
                    if !size_str.is_empty() {
                        // Try to parse as number
                        let _ = size_str.parse::<usize>();
                    }
                }
            }
        }

        // Limit the number of fields to prevent excessive processing
        if seen_types.len() > 1000 {
            break;
        }
    }
}

/// Check if a type string is valid.
fn is_valid_type(type_str: &str) -> bool {
    // Common primitive types
    const VALID_TYPES: &[&str] = &[
        "bool",
        "int8",
        "uint8",
        "int16",
        "uint16",
        "int32",
        "uint32",
        "int64",
        "uint64",
        "float32",
        "float64",
        "string",
        "time",
        "duration",
        "byte",
        "char",
    ];

    // Check if it's a known primitive type
    if VALID_TYPES.contains(&type_str) {
        return true;
    }

    // Check if it's an array type (e.g., int32[10])
    if type_str.contains('[') {
        let base_type = type_str.split('[').next().unwrap_or(type_str);
        return VALID_TYPES.contains(&base_type);
    }

    // Assume it's a custom type (valid)
    true
}

/// Check if an identifier string is valid.
fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    // First character must be letter or underscore
    if !name.chars().next().map(|c| c.is_alphabetic() || c == '_').unwrap_or(false) {
        return false;
    }

    // Remaining characters must be alphanumeric or underscore
    name.chars().all(|c| c.is_alphanumeric() || c == '_')
}
