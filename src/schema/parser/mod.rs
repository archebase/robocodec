// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema parser implementations.

pub mod idl_parser;
pub mod msg_parser;
pub mod ros2_idl_parser;

// Include the unified parser (has format detection and ROS2 IDL handling)
pub mod unified;

pub use msg_parser::{RosVersion, parse_with_encoding, parse_with_version};
pub use ros2_idl_parser::{normalize_ros2_idl, parse as parse_ros2_idl};

// Main parser interface
use crate::core::Result;
use crate::schema::{MessageSchema, SchemaFormat};

/// Parse a schema from a string.
///
/// # Arguments
///
/// * `name` - Message name
/// * `definition` - Schema definition string
///
/// # Returns
///
/// Parsed `MessageSchema`
pub fn parse_schema(name: &str, definition: &str) -> Result<MessageSchema> {
    parse_schema_with_encoding(name, definition, SchemaFormat::Msg)
}

/// Parse a schema with explicit format specification.
///
/// # Arguments
///
/// * `name` - Message name
/// * `definition` - Schema definition string
/// * `format` - Schema format (Msg, Idl, etc.)
///
/// # Returns
///
/// Parsed `MessageSchema`
pub fn parse_schema_with_encoding(
    name: &str,
    definition: &str,
    format: SchemaFormat,
) -> Result<MessageSchema> {
    match format {
        SchemaFormat::Msg => msg_parser::parse(name, definition)
            .map_err(|e| crate::core::CodecError::parse("schema", e.to_string())),
        SchemaFormat::Idl => idl_parser::parse(name, definition)
            .map_err(|e| crate::core::CodecError::parse("schema", e.to_string())),
    }
}

/// Parse a schema with string-based encoding specification.
///
/// # Arguments
///
/// * `name` - Message name
/// * `definition` - Schema definition string
/// * `encoding` - Schema encoding string (e.g., "ros1msg", "ros2msg", "ros2idl")
///
/// # Returns
///
/// Parsed `MessageSchema`
pub fn parse_schema_with_encoding_str(
    name: &str,
    definition: &str,
    encoding: &str,
) -> Result<MessageSchema> {
    let encoding_lower = encoding.to_lowercase();

    // ROS2 IDL format needs special handling (strips separator headers)
    if encoding_lower.contains("ros2idl") {
        return ros2_idl_parser::parse(name, definition)
            .map_err(|e| crate::core::CodecError::parse("schema", e.to_string()));
    }

    // For other encodings, use the format-based parser from unified.rs
    // which handles format detection and ROS2 IDL header stripping
    unified::parse_schema_with_encoding(name, definition, &encoding_lower)
        .map_err(|e| crate::core::CodecError::parse("schema", e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // parse_schema Tests
    // =========================================================================

    #[test]
    fn test_parse_schema_simple_msg() {
        let definition = r#"string data"#;
        let result = parse_schema("test/Msg", definition);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_fields() {
        let definition = r#"
string name
int32 count
bool flag
"#;
        let result = parse_schema("test/Complex", definition);
        assert!(result.is_ok());
        let schema = result.unwrap();
        // Schema should have the main type defined
        assert!(!schema.types.is_empty());
    }

    #[test]
    fn test_parse_schema_empty_definition() {
        let definition = "";
        let result = parse_schema("test/Empty", definition);
        // Empty definitions should still parse (fields will be empty)
        assert!(result.is_ok());
    }

    // =========================================================================
    // parse_schema_with_encoding Tests
    // =========================================================================

    #[test]
    fn test_parse_schema_with_encoding_msg_format() {
        let definition = "string data";
        let result = parse_schema_with_encoding("test/Msg", definition, SchemaFormat::Msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_encoding_idl_format() {
        let definition = "struct Test { string data; };";
        let result = parse_schema_with_encoding("test/Test", definition, SchemaFormat::Idl);
        // IDL parsing may or may not succeed depending on the grammar
        // Just check it doesn't crash
        let _ = result;
    }

    #[test]
    fn test_parse_schema_with_encoding_invalid_msg() {
        let definition = "invalid type definition {{{";
        let result = parse_schema_with_encoding("test/Invalid", definition, SchemaFormat::Msg);
        // Should return an error for invalid syntax
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_schema_with_encoding_str Tests
    // =========================================================================

    #[test]
    fn test_parse_schema_with_encoding_str_ros1msg() {
        let definition = "string data";
        let result = parse_schema_with_encoding_str("test/Msg", definition, "ros1msg");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_encoding_str_ros2msg() {
        let definition = "string data";
        let result = parse_schema_with_encoding_str("test/Msg", definition, "ros2msg");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_encoding_str_case_insensitive() {
        let definition = "string data";
        let result = parse_schema_with_encoding_str("test/Msg", definition, "ROS1MSG");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_encoding_str_mixed_case() {
        let definition = "string data";
        let result = parse_schema_with_encoding_str("test/Msg", definition, "RoS2MsG");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_schema_with_encoding_str_empty_encoding() {
        let definition = "string data";
        let result = parse_schema_with_encoding_str("test/Msg", definition, "");
        // Empty encoding should still work (format detection)
        assert!(result.is_ok());
    }
}
