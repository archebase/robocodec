// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Round-trip property tests.
//!
//! These tests verify that encoding and then decoding preserves the original data.

use proptest::prelude::*;
use robocodec::core::CodecValue;
use robocodec::encoding::cdr::{CdrCursor, CdrEncoder};

/// Strategy for generating simple (non-nested) CodecValue instances
fn simple_codec_value() -> impl Strategy<Value = CodecValue> {
    prop_oneof![
        any::<i8>().prop_map(CodecValue::Int8),
        any::<i16>().prop_map(CodecValue::Int16),
        any::<i32>().prop_map(CodecValue::Int32),
        any::<i64>().prop_map(CodecValue::Int64),
        any::<u8>().prop_map(CodecValue::UInt8),
        any::<u16>().prop_map(CodecValue::UInt16),
        any::<u32>().prop_map(CodecValue::UInt32),
        any::<u64>().prop_map(CodecValue::UInt64),
        // Use smaller range for f32/f64 to avoid JSON precision issues
        prop::num::f32::NORMAL.prop_map(CodecValue::Float32),
        prop::num::f64::NORMAL.prop_map(CodecValue::Float64),
        Just(CodecValue::Bool(true)),
        Just(CodecValue::Bool(false)),
    ]
}

// ============================================================================
// CDR Round-trip Tests for Primitive Types
// ============================================================================

proptest! {
    /// Property: Encoding and decoding i8 values preserves the original value
    #[test]
    fn prop_round_trip_i8(original in any::<i8>()) {
        let mut encoder = CdrEncoder::new();
        encoder.int8(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_i8().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding i16 values preserves the original value
    #[test]
    fn prop_round_trip_i16(original in any::<i16>()) {
        let mut encoder = CdrEncoder::new();
        encoder.int16(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_i16().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding i32 values preserves the original value
    #[test]
    fn prop_round_trip_i32(original in any::<i32>()) {
        let mut encoder = CdrEncoder::new();
        encoder.int32(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_i32().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding i64 values preserves the original value
    #[test]
    fn prop_round_trip_i64(original in any::<i64>()) {
        let mut encoder = CdrEncoder::new();
        encoder.int64(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_i64().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding u8 values preserves the original value
    #[test]
    fn prop_round_trip_u8(original in any::<u8>()) {
        let mut encoder = CdrEncoder::new();
        encoder.uint8(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_u8().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding u16 values preserves the original value
    #[test]
    fn prop_round_trip_u16(original in any::<u16>()) {
        let mut encoder = CdrEncoder::new();
        encoder.uint16(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_u16().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding u32 values preserves the original value
    #[test]
    fn prop_round_trip_u32(original in any::<u32>()) {
        let mut encoder = CdrEncoder::new();
        encoder.uint32(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_u32().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding u64 values preserves the original value
    #[test]
    fn prop_round_trip_u64(original in any::<u64>()) {
        let mut encoder = CdrEncoder::new();
        encoder.uint64(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_u64().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding f32 values preserves the original value
    /// Note: NaN values are excluded as NaN != NaN
    #[test]
    fn prop_round_trip_f32(original in prop::num::f32::NORMAL) {
        let mut encoder = CdrEncoder::new();
        encoder.float32(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_f32().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding f64 values preserves the original value
    /// Note: NaN values are excluded as NaN != NaN
    #[test]
    fn prop_round_trip_f64(original in prop::num::f64::NORMAL) {
        let mut encoder = CdrEncoder::new();
        encoder.float64(original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_f64().unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding byte arrays preserves the original data
    #[test]
    fn prop_round_trip_bytes(original in prop::collection::vec(any::<u8>(), 0..100)) {
        let mut encoder = CdrEncoder::new();
        encoder.bytes(&original).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let decoded = cursor.read_bytes(original.len()).unwrap();
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding i32 arrays preserves the original data
    #[test]
    fn prop_round_trip_i32_array(original in prop::collection::vec(any::<i32>(), 0..20)) {
        let mut encoder = CdrEncoder::new();
        encoder.sequence_length(original.len()).unwrap();
        for val in &original {
            encoder.int32(*val).unwrap();
        }
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let _len = cursor.read_u32().unwrap();
        let mut decoded = Vec::new();
        for _ in 0..original.len() {
            decoded.push(cursor.read_i32().unwrap());
        }
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding u8 arrays preserves the original data
    #[test]
    fn prop_round_trip_u8_array(original in prop::collection::vec(any::<u8>(), 0..50)) {
        let mut encoder = CdrEncoder::new();
        encoder.uint8_array(&original, true).unwrap();
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let _len = cursor.read_u32().unwrap();
        let mut decoded = Vec::new();
        for _ in 0..original.len() {
            decoded.push(cursor.read_u8().unwrap());
        }
        prop_assert_eq!(original, decoded);
    }

    /// Property: Encoding and decoding f64 arrays preserves the original data
    #[test]
    fn prop_round_trip_f64_array(original in prop::collection::vec(prop::num::f64::NORMAL, 0..20)) {
        let mut encoder = CdrEncoder::new();
        encoder.sequence_length(original.len()).unwrap();
        for val in &original {
            encoder.float64(*val).unwrap();
        }
        let data = encoder.finish();

        let mut cursor = CdrCursor::new(&data).unwrap();
        let _len = cursor.read_u32().unwrap();
        let mut decoded = Vec::new();
        for _ in 0..original.len() {
            decoded.push(cursor.read_f64().unwrap());
        }
        prop_assert_eq!(original, decoded);
    }
}

// ============================================================================
// CodecValue Serialization Round-trip Tests
// ============================================================================

proptest! {
    /// Property: Serializing and deserializing CodecValue preserves the data
    /// Note: Floating point values may have small precision differences due to JSON serialization
    #[test]
    fn prop_codec_value_json_round_trip(value in simple_codec_value()) {
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();

        // For floating point values, use approximate comparison
        match (&value, &decoded) {
            (CodecValue::Float32(a), CodecValue::Float32(b)) => {
                let rel_diff = (a - b).abs() / (a.abs() + b.abs() + f32::MIN_POSITIVE);
                prop_assert!(rel_diff < 1e-6 || a == b, "Float32 values should be approximately equal");
            }
            (CodecValue::Float64(a), CodecValue::Float64(b)) => {
                let rel_diff = (a - b).abs() / (a.abs() + b.abs() + f64::MIN_POSITIVE);
                prop_assert!(rel_diff < 1e-10 || a == b, "Float64 values should be approximately equal");
            }
            _ => prop_assert_eq!(value, decoded),
        }
    }

    /// Property: JSON round-trip preserves integer values
    #[test]
    fn prop_codec_value_json_int_round_trip(original in any::<i64>()) {
        let value = CodecValue::Int64(original);
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// Property: JSON round-trip preserves string values
    #[test]
    fn prop_codec_value_json_string_round_trip(original in "[a-zA-Z0-9 ]{0,100}") {
        let value = CodecValue::String(original.clone());
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// Property: JSON round-trip preserves array values
    #[test]
    fn prop_codec_value_json_array_round_trip(original in prop::collection::vec(any::<i32>(), 0..20)) {
        let value = CodecValue::Array(
            original.iter().map(|&i| CodecValue::Int32(i)).collect()
        );
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// Property: JSON round-trip preserves nested structures
    /// Note: Floating point values may have small precision differences due to JSON serialization
    #[test]
    fn prop_codec_value_json_struct_round_trip(field1 in any::<i32>(), field2 in prop::num::f64::NORMAL) {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert("a".to_string(), CodecValue::Int32(field1));
        map.insert("b".to_string(), CodecValue::Float64(field2));
        let value = CodecValue::Struct(map);

        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();

        match (&value, &decoded) {
            (CodecValue::Struct(orig), CodecValue::Struct(dec)) => {
                prop_assert_eq!(orig.len(), dec.len());
                for (key, orig_val) in orig {
                    if let Some(dec_val) = dec.get(key) {
                        match (orig_val, dec_val) {
                            (CodecValue::Float64(a), CodecValue::Float64(b)) => {
                                // Use relative tolerance for large numbers
                                let rel_diff = (a - b).abs() / (a.abs() + b.abs() + 1.0);
                                prop_assert!(rel_diff < 1e-10 || a == b, "Float values should be approximately equal");
                            }
                            _ => prop_assert_eq!(orig_val, dec_val),
                        }
                    }
                }
            }
            _ => prop_assert!(false, "Both should be structs"),
        }
    }
}

// ============================================================================
// Timestamp Round-trip Tests
// ============================================================================

proptest! {
    /// Property: Timestamp construction from secs/nanos is reversible
    #[test]
    fn prop_timestamp_secs_nanos_reversible(secs in any::<u32>(), nanos in any::<u32>()) {
        let ts = CodecValue::timestamp_from_secs_nanos(secs, nanos);
        let recovered_nanos = ts.as_timestamp_nanos().unwrap();
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(recovered_nanos, expected);
    }

    /// Property: ROS1 timestamp construction is reversible
    #[test]
    fn prop_ros1_timestamp_reversible(secs in any::<u32>(), nanos in any::<u32>()) {
        let ts = CodecValue::from_ros1_time(secs, nanos);
        let recovered_nanos = ts.as_timestamp_nanos().unwrap();
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(recovered_nanos, expected);
    }

    /// Property: ROS2 timestamp construction is reversible
    #[test]
    fn prop_ros2_timestamp_reversible(secs in any::<i32>(), nanos in any::<u32>()) {
        let ts = CodecValue::from_ros2_time(secs, nanos);
        let recovered_nanos = ts.as_timestamp_nanos().unwrap();
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(recovered_nanos, expected);
    }

    /// Property: Duration construction is reversible
    #[test]
    fn prop_duration_secs_nanos_reversible(secs in any::<i32>(), nanos in any::<i32>()) {
        let dur = CodecValue::duration_from_secs_nanos(secs, nanos);
        let recovered_nanos = dur.as_duration_nanos().unwrap();
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(recovered_nanos, expected);
    }

    /// Property: ROS1 duration construction is reversible
    #[test]
    fn prop_ros1_duration_reversible(secs in any::<i32>(), nanos in any::<i32>()) {
        let dur = CodecValue::from_ros1_duration(secs, nanos);
        let recovered_nanos = dur.as_duration_nanos().unwrap();
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(recovered_nanos, expected);
    }

    /// Property: ROS2 duration construction is reversible
    #[test]
    fn prop_ros2_duration_reversible(secs in any::<i32>(), nanos in any::<u32>()) {
        let dur = CodecValue::from_ros2_duration(secs, nanos);
        let recovered_nanos = dur.as_duration_nanos().unwrap();
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(recovered_nanos, expected);
    }
}
