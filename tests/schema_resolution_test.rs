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

/// Test sensor_msgs/JointState decoding with nested Header.
///
/// This test reproduces the issue where decoding JointState with nested
/// std_msgs/Header fails with array length errors. The schema parses
/// correctly but CDR decoding has issues with field offset calculation
/// for nested structs.
#[test]
fn test_decode_joint_state_with_header() {
    // sensor_msgs/JointState schema with nested std_msgs/Header
    let schema_str = r#"
std_msgs/Header header
string[] name
float64[] position
float64[] velocity
float64[] effort
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    let schema = parse_schema("sensor_msgs/msg/JointState", schema_str).expect("parse schema");

    // Create test data matching the schema structure
    let mut data = vec![0x00, 0x01, 0x00, 0x00]; // CDR header (little-endian)

    // Header: stamp.sec (int32)
    data.extend_from_slice(&12345i32.to_le_bytes());
    // Header: stamp.nanosec (uint32)
    data.extend_from_slice(&67890u32.to_le_bytes());

    // Header: frame_id (string)
    // CDR string format: 4-byte length (includes null) + content + null terminator
    // Current position before frame_id: 12
    // "base_link" = 9 chars + null = 10 bytes total
    data.extend_from_slice(&10u32.to_le_bytes()); // length = 10 (includes null terminator)
    data.extend_from_slice(b"base_link\0"); // 10 bytes (9 chars + null)
                                            // Position after frame_id: 12 + 4 + 10 = 26
                                            // Need to align to 4 bytes for the array length, so pad to 28
    data.extend_from_slice(&[0, 0]); // 2 padding bytes

    // names array (string[]) - sequence length first
    // Current position: 28 (4-byte aligned)
    data.extend_from_slice(&2u32.to_le_bytes()); // 2 joint names

    // First string: "joint1" (6 chars + null = 7)
    data.extend_from_slice(&7u32.to_le_bytes()); // length = 7
    data.extend_from_slice(b"joint1\0"); // 7 bytes
                                         // Position: 28 + 4 + 4 + 7 = 43, need to align to 4 for next string length
    data.push(0); // 1 padding byte to reach 44

    // Second string: "joint2" (6 chars + null = 7)
    data.extend_from_slice(&7u32.to_le_bytes()); // length = 7
    data.extend_from_slice(b"joint2\0"); // 7 bytes
                                         // Position: 44 + 4 + 7 = 55

    // position array (float64[]) - needs 4-byte alignment for length
    // Position 55, need to align to 4 -> 56
    data.push(0); // 1 padding byte
    data.extend_from_slice(&2u32.to_le_bytes()); // 2 positions
                                                 // Position: 56 + 4 = 60, need 8-byte alignment for float64 -> already aligned
    data.extend_from_slice(&1.0f64.to_le_bytes());
    data.extend_from_slice(&2.0f64.to_le_bytes());

    // velocity array (float64[]) - position 60 + 16 = 76, aligned to 4
    data.extend_from_slice(&2u32.to_le_bytes()); // 2 velocities
                                                 // Position: 76 + 4 = 80, (80 - 4) % 8 = 4 - need 4 bytes padding for 8-byte alignment
    data.extend_from_slice(&[0, 0, 0, 0]); // padding to align float64 data to 8 bytes
                                           // Position: 80 + 4 = 84, (84 - 4) % 8 = 0 - now 8-byte aligned
    data.extend_from_slice(&0.1f64.to_le_bytes()); // velocity[0] at 84-91
    data.extend_from_slice(&0.2f64.to_le_bytes()); // velocity[1] at 92-99 (contiguous)
                                                   // After velocity array: pos = 100

    // effort array (float64[]) - position 100, aligned to 4
    // After velocity: pos = 100, (100 - 4) % 4 = 0 ✓
    data.extend_from_slice(&2u32.to_le_bytes()); // 2 effort values
                                                 // Position: 100 + 4 = 104, (104 - 4) % 8 = 4 - need 4 bytes padding for 8-byte alignment
    data.extend_from_slice(&[0, 0, 0, 0]); // padding to align float64 data to 8 bytes
                                           // Position: 104 + 4 = 108, (108 - 4) % 8 = 0 - now 8-byte aligned
    data.extend_from_slice(&0.0f64.to_le_bytes()); // effort[0] at 108-115
    data.extend_from_slice(&0.0f64.to_le_bytes()); // effort[1] at 116-123

    // Decode
    let decoder = CdrDecoder::new();
    let result = decoder
        .decode(&schema, &data, None)
        .expect("decoding should succeed");

    // Verify all fields are present with correct values
    assert!(result.contains_key("header"), "Should have header field");
    assert!(result.contains_key("name"), "Should have name field");
    assert!(
        result.contains_key("position"),
        "Should have position field"
    );
    assert!(
        result.contains_key("velocity"),
        "Should have velocity field"
    );
    assert!(result.contains_key("effort"), "Should have effort field");

    // Verify the decoded float64 values are correct
    if let Some(robocodec::CodecValue::Array(positions)) = result.get("position") {
        assert_eq!(positions.len(), 2, "Should have 2 positions");
        if let robocodec::CodecValue::Float64(v) = &positions[0] {
            assert_eq!(*v, 1.0, "position[0] should be 1.0");
        }
        if let robocodec::CodecValue::Float64(v) = &positions[1] {
            assert_eq!(*v, 2.0, "position[1] should be 2.0");
        }
    }

    if let Some(robocodec::CodecValue::Array(velocities)) = result.get("velocity") {
        assert_eq!(velocities.len(), 2, "Should have 2 velocities");
        if let robocodec::CodecValue::Float64(v) = &velocities[0] {
            assert!((*v - 0.1).abs() < f64::EPSILON, "velocity[0] should be 0.1");
        }
        if let robocodec::CodecValue::Float64(v) = &velocities[1] {
            assert!((*v - 0.2).abs() < f64::EPSILON, "velocity[1] should be 0.2");
        }
    }

    if let Some(robocodec::CodecValue::Array(efforts)) = result.get("effort") {
        assert_eq!(efforts.len(), 2, "Should have 2 effort values");
        for (i, e) in efforts.iter().enumerate() {
            if let robocodec::CodecValue::Float64(v) = e {
                assert_eq!(*v, 0.0, "effort[{}] should be 0.0", i);
            }
        }
    }
}
