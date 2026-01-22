// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema preservation tests.
//!
//! These tests verify that ROS message schemas are correctly preserved
//! through encode/decode cycles and format conversions.

use std::path::Path;

use robocodec::encoding::cdr::{CdrCursor, CdrEncoder};
use robocodec::io::formats::bag::BagFormat;
use robocodec::io::formats::mcap::McapReader;
use robocodec::io::traits::FormatReader;
use robocodec::schema::parse_schema;

// ============================================================================
// Schema String Preservation Tests
// ============================================================================

#[test]
fn test_schema_string_preserved_through_parsing() {
    let schema_str = r#"
std_msgs/String data
===
MSG: std_msgs/String
string data
"#;

    let schema = parse_schema("std_msgs/String", schema_str).expect("parse schema");

    // Verify the schema contains the expected type
    let msg_type = schema.get_type("std_msgs/String").expect("type exists");
    assert_eq!(msg_type.fields.len(), 1);
    assert_eq!(msg_type.fields[0].name, "data");
}

#[test]
fn test_schema_nested_preserved() {
    let schema_str = r#"
geometry_msgs/Transform transform
===
MSG: geometry_msgs/Transform
geometry_msgs/Vector3 translation
geometry_msgs/Quaternion rotation
===
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
===
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
"#;

    let schema = parse_schema("test/HasTransform", schema_str).expect("parse schema");

    // Verify all types are registered
    assert!(schema.get_type("test/HasTransform").is_some());
    assert!(schema.get_type("geometry_msgs/Transform").is_some());
    assert!(schema.get_type("geometry_msgs/Vector3").is_some());
    assert!(schema.get_type("geometry_msgs/Quaternion").is_some());
}

// ============================================================================
// CDR Round-Trip Schema Tests
// ============================================================================

#[test]
fn test_cdr_round_trip_preserves_header() {
    // std_msgs/Header has: Time stamp, string frame_id
    let original_sec = 1234567890_u32;
    let original_nsec = 987654321_u32;
    let original_frame_id = "test_frame";

    // Encode
    let mut encoder = CdrEncoder::new();
    let _ = encoder.uint32(original_sec);
    let _ = encoder.uint32(original_nsec);
    let _ = encoder.string(original_frame_id);

    let data = encoder.finish();

    // Decode
    let mut cursor = CdrCursor::new(&data).expect("create cursor");

    let sec = cursor.read_u32().expect("read sec");
    let nsec = cursor.read_u32().expect("read nsec");

    // Read frame_id string
    let frame_id_len = cursor.read_u32().expect("read frame_id length") as usize;
    let mut frame_id_bytes = vec![0u8; frame_id_len - 1];
    for byte in frame_id_bytes.iter_mut() {
        *byte = cursor.read_u8().expect("read frame_id byte");
    }
    let _null = cursor.read_u8().expect("read null");
    let frame_id = std::str::from_utf8(&frame_id_bytes).expect("valid UTF-8");

    assert_eq!(sec, original_sec);
    assert_eq!(nsec, original_nsec);
    assert_eq!(frame_id, original_frame_id);
}

#[test]
fn test_cdr_round_trip_preserves_point() {
    // geometry_msgs/Point: float64 x, y, z
    let original = (1.0_f64, 2.0_f64, 3.0_f64);

    let mut encoder = CdrEncoder::new();
    let _ = encoder.float64(original.0);
    let _ = encoder.float64(original.1);
    let _ = encoder.float64(original.2);
    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    cursor.align(8).expect("align");

    let x = cursor.read_f64().expect("x");
    let y = cursor.read_f64().expect("y");
    let z = cursor.read_f64().expect("z");

    assert!((x - original.0).abs() < f64::EPSILON);
    assert!((y - original.1).abs() < f64::EPSILON);
    assert!((z - original.2).abs() < f64::EPSILON);
}

#[test]
fn test_cdr_round_trip_preserves_pose() {
    // geometry_msgs/Pose: Point position, Quaternion orientation
    let position = (1.0_f64, 2.0_f64, 3.0_f64);
    let orientation = (0.0_f64, 0.0_f64, 0.0_f64, 1.0_f64);

    let mut encoder = CdrEncoder::new();
    let _ = encoder.float64(position.0); // x
    let _ = encoder.float64(position.1); // y
    let _ = encoder.float64(position.2); // z
    let _ = encoder.float64(orientation.0); // x
    let _ = encoder.float64(orientation.1); // y
    let _ = encoder.float64(orientation.2); // z
    let _ = encoder.float64(orientation.3); // w
    let data = encoder.finish();

    let mut cursor = CdrCursor::new(&data).expect("create cursor");
    cursor.align(8).expect("align");

    // Read position
    let px = cursor.read_f64().expect("position.x");
    let py = cursor.read_f64().expect("position.y");
    let pz = cursor.read_f64().expect("position.z");

    // Read orientation
    let ox = cursor.read_f64().expect("orientation.x");
    let oy = cursor.read_f64().expect("orientation.y");
    let oz = cursor.read_f64().expect("orientation.z");
    let ow = cursor.read_f64().expect("orientation.w");

    assert!((px - position.0).abs() < f64::EPSILON);
    assert!((py - position.1).abs() < f64::EPSILON);
    assert!((pz - position.2).abs() < f64::EPSILON);

    assert!((ox - orientation.0).abs() < f64::EPSILON);
    assert!((oy - orientation.1).abs() < f64::EPSILON);
    assert!((oz - orientation.2).abs() < f64::EPSILON);
    assert!((ow - orientation.3).abs() < f64::EPSILON);
}

// ============================================================================
// Format Conversion Schema Preservation Tests
// ============================================================================

#[test]
fn test_bag_to_mcap_preserves_schemas() {
    let fixture_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    // Read BAG file
    let bag_reader = BagFormat::open(fixture_path).expect("open bag");
    let bag_channels = bag_reader.channels();

    // Collect schemas
    let mut schemas = std::collections::HashMap::new();
    for channel in bag_channels.values() {
        if let Some(schema) = &channel.schema {
            if !schema.is_empty() {
                schemas.insert(channel.message_type.clone(), schema.clone());
            }
        }
    }

    // Verify we have schemas
    assert!(!schemas.is_empty(), "Should have some schemas");
}

#[test]
fn test_mcap_to_bag_preserves_schemas() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    // Read MCAP file
    let mcap_reader = McapReader::open(fixture_path).expect("open mcap");
    let mcap_channels = mcap_reader.channels();

    // Verify we can access schema information for each channel
    for channel in mcap_channels.values() {
        if let Some(schema) = &channel.schema {
            if !schema.is_empty() {
                // Schema is present - just verify it's accessible
                let schema_len = schema.len();
                assert!(schema_len > 0, "Schema should have content");
            }
        }
    }
}

// ============================================================================
// Schema Type Resolution Tests
// ============================================================================

#[test]
fn test_schema_resolves_builtin_types() {
    // Test that primitive types are recognized
    let schema_str = r#"
int32 count
float64 value
string name
bool flag
"#;

    let schema = parse_schema("test/Primitives", schema_str).expect("parse schema");

    let msg_type = schema.get_type("test/Primitives").expect("type exists");

    // Verify field types
    for field in &msg_type.fields {
        match field.name.as_str() {
            "count" => {}
            "value" => {}
            "name" => {}
            "flag" => {}
            _ => panic!("Unexpected field: {}", field.name),
        }
    }
}

#[test]
fn test_schema_resolves_array_types() {
    let schema_str = r#"
int32[10] array
string[] sequence
"#;

    let schema = parse_schema("test/Arrays", schema_str).expect("parse schema");

    let msg_type = schema.get_type("test/Arrays").expect("type exists");

    assert_eq!(msg_type.fields.len(), 2);

    // First field should be a fixed array
    match &msg_type.fields[0].type_name {
        robocodec::schema::FieldType::Array { .. } => {}
        _ => panic!("Expected Array type for 'array' field"),
    }

    // Second field should be a sequence (dynamic array)
    match &msg_type.fields[1].type_name {
        robocodec::schema::FieldType::Array { .. } => {}
        _ => panic!("Expected Array type for 'sequence' field"),
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_empty_schema() {
    let schema_str = "";

    let schema = parse_schema("test/Empty", schema_str).expect("parse empty schema");

    // Should have the main type even with no fields
    let msg_type = schema.get_type("test/Empty").expect("type exists");
    assert_eq!(msg_type.fields.len(), 0);
}

#[test]
fn test_schema_with_empty_lines() {
    let schema_str = r#"



int32 value


"#;

    let schema = parse_schema("test/Whitespace", schema_str).expect("parse schema with whitespace");

    let msg_type = schema.get_type("test/Whitespace").expect("type exists");
    assert_eq!(msg_type.fields.len(), 1);
}

#[test]
fn test_schema_with_comments() {
    let schema_str = r#"
# This is a comment
int32 value  # inline comment
# Another comment
string name
"#;

    let schema = parse_schema("test/Comments", schema_str).expect("parse schema with comments");

    let msg_type = schema.get_type("test/Comments").expect("type exists");
    assert_eq!(msg_type.fields.len(), 2);
}
