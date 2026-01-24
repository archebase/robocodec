// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Test for schema resolution with nested type references.
//!
//! This tests the fix for the issue where schemas with === separators
//! were being incorrectly preprocessed, causing the root message to
//! have 0 fields instead of the expected fields.

use robocodec::encoding::cdr::CdrDecoder;
use robocodec::schema::parse_schema;
use robocodec::schema::parser::msg_parser::parse;

#[test]
fn test_msg_parser_with_separator() {
    // Test the msg parser specifically with === separator
    // Use /msg/ in type name to force ROS2 detection (avoid header field removal)
    let schema_str = r#"std_msgs/Header header
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id"#;

    let schema = parse("test/msg/NestedMessage", schema_str).unwrap();

    let root_msg = schema
        .get_type_variants("test/NestedMessage")
        .expect("Root message should exist");
    assert_eq!(root_msg.fields.len(), 1, "Root message should have 1 field");
    assert_eq!(root_msg.fields[0].name, "header");
}

#[test]
fn test_msg_parser_with_indented_format() {
    // Test the indented format which gets preprocessed into === MSG blocks
    // Use /msg/ in type name to force ROS2 detection
    let schema_str = r#"std_msgs/Header header
  builtin_interfaces/Time stamp
  string frame_id"#;

    let schema = parse("test/msg/NestedMessage", schema_str).unwrap();

    // With indented format preprocessing, "stamp" and "frame_id" become fields of Header
    // and the root message only has "header"
    let root_msg = schema
        .get_type_variants("test/NestedMessage")
        .expect("Root message should exist");
    assert_eq!(
        root_msg.fields.len(),
        1,
        "Root message should have 1 field (header)"
    );

    // Check that Header has the expected fields from preprocessing
    let header = schema
        .get_type_variants("std_msgs/Header")
        .expect("Header should exist");
    assert_eq!(
        header.fields.len(),
        2,
        "Header should have stamp and frame_id"
    );
}

#[test]
fn test_decode_nested_header_with_time() {
    // Test schema with nested type references (std_msgs/Header -> builtin_interfaces/Time)
    // Use /msg/ in type name to force ROS2 detection
    let schema_str = r#"
std_msgs/Header header
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    let schema = parse_schema("test/msg/NestedMessage", schema_str).expect("parse schema");

    // Create test data
    let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header

    // Header: stamp.sec (int32)
    data.extend_from_slice(&12345i32.to_le_bytes());
    // Header: stamp.nanosec (uint32)
    data.extend_from_slice(&67890u32.to_le_bytes());
    // Header: frame_id (string) - length + data + null
    data.extend_from_slice(&10u32.to_le_bytes()); // length
    data.extend_from_slice(b"test_frame");
    data.push(0); // null terminator

    // Try to decode using the decoder
    let decoder = CdrDecoder::new();
    let result = decoder
        .decode(&schema, &data, None)
        .expect("decode should succeed");

    // Verify the decoded data
    assert!(result.contains_key("header"), "Should have header field");
}
