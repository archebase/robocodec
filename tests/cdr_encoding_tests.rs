// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR (Common Data Representation) encoding and decoding tests.

use robocodec::encoding::cdr::{CdrCursor, CdrEncoder};
use robocodec::schema::parse_schema;

// ============================================================================
// CDR Cursor Reading Tests
// ============================================================================

#[test]
fn test_cdr_cursor_read_u32() {
    let data: Vec<u8> = vec![
        0x00, 0x01, 0x00, 0x00, // CDR header
        0x34, 0x12, 0x00, 0x00, // uint32 = 4660
    ];

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let value = cursor.read_u32().expect("read u32");
    assert_eq!(value, 4660);
}

#[test]
fn test_cdr_cursor_read_i32_round_trip() {
    // Test encoding and then decoding i32
    let original: i32 = -424;

    let mut encoder = CdrEncoder::new();
    let _ = encoder.int32(original);

    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let value = cursor.read_i32().expect("read i32");
    assert_eq!(value, original);
}

#[test]
fn test_cdr_cursor_read_f64() {
    let data: Vec<u8> = vec![
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F,
    ];

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    cursor.align(8).expect("align to 8 bytes");
    let value = cursor.read_f64().expect("read f64");
    assert!((value - 1.5).abs() < f64::EPSILON);
}

#[test]
fn test_cdr_cursor_read_u8() {
    let data: Vec<u8> = vec![0x00, 0x01, 0x00, 0x00, 0x42];

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let value = cursor.read_u8().expect("read u8");
    assert_eq!(value, 0x42);
}

// ============================================================================
// CDR Encoder Writing Tests
// ============================================================================

#[test]
fn test_cdr_encoder_write_u32() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.uint32(4660);

    let data = encoder.finish();

    // Verify CDR header
    assert_eq!(data[0], 0x00);
    assert_eq!(data[1], 0x01);

    // Verify encoded value
    let value = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    assert_eq!(value, 4660);
}

#[test]
fn test_cdr_encoder_write_i32() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.int32(-12345);

    let data = encoder.finish();

    let value = i32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    assert_eq!(value, -12345);
}

#[test]
fn test_cdr_encoder_write_f64() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.float64(3.14159);

    let data = encoder.finish();

    // Cursor starts after 4-byte CDR header
    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    // Data starts right after header, at position 4
    // f64 is 8-byte aligned, so it's at positions 4-11
    cursor.align(8).expect("align");

    let value = cursor.read_f64().expect("read f64");
    assert!((value - 3.14159).abs() < 0.00001);
}

#[test]
fn test_cdr_encoder_write_u8() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.uint8(0xFF);

    let data = encoder.finish();

    assert_eq!(data[4], 0xFF);
}

#[test]
fn test_cdr_encoder_write_string() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.string("Hello");

    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let _ = cursor.read_u32().expect("read length");

    let mut bytes = [0u8; 5];
    for i in 0..5 {
        bytes[i] = cursor.read_u8().expect("read byte");
    }
    let _null = cursor.read_u8().expect("read null");

    let s = std::str::from_utf8(&bytes).expect("valid UTF-8");
    assert_eq!(s, "Hello");
}

// ============================================================================
// Vector3 Encoding/Decoding Round-Trip
// ============================================================================

#[test]
fn test_cdr_vector3_round_trip() {
    let original = (1.5_f64, 2.5_f64, 3.5_f64);

    // Encode
    let mut encoder = CdrEncoder::new();
    let _ = encoder.float64(original.0);
    let _ = encoder.float64(original.1);
    let _ = encoder.float64(original.2);
    let data = encoder.finish();

    // Decode
    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    cursor.align(8).expect("align");

    let x = cursor.read_f64().expect("decode x");
    let y = cursor.read_f64().expect("decode y");
    let z = cursor.read_f64().expect("decode z");

    assert!((x - original.0).abs() < f64::EPSILON);
    assert!((y - original.1).abs() < f64::EPSILON);
    assert!((z - original.2).abs() < f64::EPSILON);
}

#[test]
fn test_cdr_quaternion_round_trip() {
    // Unit quaternion: w=1, x=y=z=0
    let mut encoder = CdrEncoder::new();
    let _ = encoder.float64(0.0); // x
    let _ = encoder.float64(0.0); // y
    let _ = encoder.float64(0.0); // z
    let _ = encoder.float64(1.0); // w
    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    cursor.align(8).expect("align");

    let x = cursor.read_f64().expect("x");
    let y = cursor.read_f64().expect("y");
    let z = cursor.read_f64().expect("z");
    let w = cursor.read_f64().expect("w");

    assert_eq!(x, 0.0);
    assert_eq!(y, 0.0);
    assert_eq!(z, 0.0);
    assert_eq!(w, 1.0);
}

// ============================================================================
// Sequence Tests
// ============================================================================

#[test]
fn test_cdr_encode_sequence() {
    let values: Vec<u32> = vec![42, 84, 126];

    let mut encoder = CdrEncoder::new();
    let _ = encoder.uint32(values.len() as u32);
    for v in &values {
        let _ = encoder.uint32(*v);
    }

    let data = encoder.finish();

    // Verify
    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    // Cursor starts at position 4 (after CDR header)

    let len = cursor.read_u32().expect("read length") as usize;
    assert_eq!(len, 3);

    let v0 = cursor.read_u32().expect("read [0]");
    let v1 = cursor.read_u32().expect("read [1]");
    let v2 = cursor.read_u32().expect("read [2]");

    assert_eq!(v0, 42);
    assert_eq!(v1, 84);
    assert_eq!(v2, 126);
}

// ============================================================================
// Alignment Tests
// ============================================================================

#[test]
fn test_cdr_alignment_u8_then_f64() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.uint8(1); // bool-like
                              // Padding to 8-byte alignment happens automatically in float64()
    let _ = encoder.float64(2.71828);
    let data = encoder.finish();

    // Verify size: 4 (header) + 1 (u8) + 7 (padding) + 8 (f64) = 20
    assert_eq!(data.len(), 20);

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    // Cursor starts at position 4 (after CDR header)

    let b = cursor.read_u8().expect("read u8");
    assert_eq!(b, 1);

    cursor.align(8).expect("align to 8");
    let f = cursor.read_f64().expect("read f64");
    assert!((f - 2.71828).abs() < 0.0001);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_cdr_empty_string() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.string("");

    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let len = cursor.read_u32().expect("read length") as usize;
    assert_eq!(len, 1); // Just null terminator

    let _null = cursor.read_u8().expect("read null");
}

#[test]
fn test_cdr_zero_values() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.int32(0);
    let _ = encoder.float64(0.0);
    let _ = encoder.uint8(0);
    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    // Cursor starts at position 4

    let i32_val = cursor.read_i32().expect("read i32");
    assert_eq!(i32_val, 0);

    cursor.align(8).expect("align");
    let f64_val = cursor.read_f64().expect("read f64");
    assert_eq!(f64_val, 0.0);

    let u8_val = cursor.read_u8().expect("read u8");
    assert_eq!(u8_val, 0);
}

// ============================================================================
// ROS1 Headerless Mode Tests
// ============================================================================

#[test]
fn test_cdr_ros1_headerless() {
    let data: Vec<u8> = vec![
        0x2A, 0x00, 0x00, 0x00, // uint32 = 42 (no CDR header)
    ];

    let mut cursor = CdrCursor::new_headerless(&data, true);
    let value = cursor.read_u32().expect("read u32");
    assert_eq!(value, 42);
}

// ============================================================================
// Schema Parsing Tests
// ============================================================================

#[test]
fn test_schema_parse_simple_message() {
    let schema_str = r#"
string data
"#;

    let schema =
        parse_schema("test/SimpleMessage", schema_str).expect("parse schema should succeed");

    assert_eq!(schema.name, "test/SimpleMessage");

    let msg_type = schema
        .get_type("test/SimpleMessage")
        .expect("type should exist");
    assert_eq!(msg_type.fields.len(), 1);
    assert_eq!(msg_type.fields[0].name, "data");
}

#[test]
fn test_schema_parse_nested_message() {
    let schema_str = r#"
std_msgs/Header header
===
MSG: std_msgs/Header
builtin_interfaces/Time stamp
===
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"#;

    let schema = parse_schema("test/NestedMessage", schema_str).expect("parse nested schema");

    // Top-level type should be registered
    assert!(schema.get_type("test/NestedMessage").is_some());
    assert!(schema.get_type("std_msgs/Header").is_some());
    assert!(schema.get_type("builtin_interfaces/Time").is_some());
}

#[test]
fn test_schema_type_resolution_ros2_convention() {
    // ROS2 uses /msg/ in type names
    let schema_str = r#"
std_msgs/msg/Header header
===
MSG: std_msgs/msg/Header
builtin_interfaces/msg/Time stamp
string frame_id
===
MSG: builtin_interfaces/msg/Time
int32 sec
uint32 nanosec
"#;

    let schema = parse_schema("test/Container", schema_str).expect("parse ROS2-style schema");

    // Verify types are accessible
    assert!(
        schema.get_type("std_msgs/msg/Header").is_some()
            || schema.get_type("std_msgs/Header").is_some()
    );
}

// ============================================================================
// Position Tracking Tests
// ============================================================================

#[test]
fn test_cdr_cursor_position_tracking() {
    let data: Vec<u8> = vec![
        0x00, 0x01, 0x00, 0x00, // CDR header
        0x01, 0x00, 0x00, 0x00, // uint32 = 1
        0x02, 0x00, 0x00, 0x00, // uint32 = 2
    ];

    let mut cursor = CdrCursor::new(&data).expect("create cursor");

    // After creation, position should be at start of data (after header)
    assert_eq!(cursor.position(), 4); // At CDR_HEADER_SIZE

    let _ = cursor.read_u32().expect("read first");
    assert_eq!(cursor.position(), 8);

    let _ = cursor.read_u32().expect("read second");
    assert_eq!(cursor.position(), 12);
}

#[test]
fn test_cdr_cursor_remaining() {
    let data: Vec<u8> = vec![
        0x00, 0x01, 0x00, 0x00, // CDR header
        0x01, 0x02, 0x03, 0x04, // 4 bytes of data
    ];

    let cursor = CdrCursor::new(&data).expect("create cursor");

    // Remaining should be total - position
    assert_eq!(cursor.remaining(), 4);

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let _ = cursor.read_u32();
    assert_eq!(cursor.remaining(), 0);
    assert!(cursor.is_at_end());
}

#[test]
fn test_cdr_cursor_read_bytes() {
    let data: Vec<u8> = vec![
        0x00, 0x01, 0x00, 0x00, // CDR header
        0x01, 0x02, 0x03, 0x04, // 4 bytes of data
    ];

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    let bytes = cursor.read_bytes(4).expect("read bytes");

    assert_eq!(bytes, &[0x01, 0x02, 0x03, 0x04]);
}

#[test]
fn test_cdr_encoder_multiple_values() {
    let mut encoder = CdrEncoder::new();
    let _ = encoder.uint32(1234567890);
    let _ = encoder.uint32(987654321);
    let _ = encoder.uint8(0xFF);

    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    // Cursor starts at position 4 (after CDR header)

    let first = cursor.read_u32().expect("read first");
    assert_eq!(first, 1234567890);

    let second = cursor.read_u32().expect("read second");
    assert_eq!(second, 987654321);

    let third = cursor.read_u8().expect("read u8");
    assert_eq!(third, 0xFF);
}
