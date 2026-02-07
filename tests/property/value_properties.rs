// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CodecValue property tests.
//!
//! These tests verify properties specific to CodecValue behavior.

use proptest::prelude::*;
use robocodec::core::CodecValue;
use std::collections::HashMap;

// ============================================================================
// Arithmetic Properties
// ============================================================================

proptest! {
    /// Property: as_f64 on numeric values preserves the magnitude
    #[test]
    fn prop_as_f64_preserves_value_for_integers(n in any::<i64>()) {
        let value = CodecValue::Int64(n);
        let as_float = value.as_f64().unwrap();

        prop_assert!((as_float - (n as f64)).abs() < f64::EPSILON);
    }

    /// Property: as_u64 on positive integers preserves the value
    #[test]
    fn prop_as_u64_preserves_positive_integers(n in 0i64..i64::MAX) {
        let value = CodecValue::Int64(n);
        let as_unsigned = value.as_u64();

        prop_assert_eq!(as_unsigned, Some(n as u64));
    }

    /// Property: as_u64 returns None for negative integers
    #[test]
    fn prop_as_u64_returns_none_for_negative(n in -1000i64..0) {
        let value = CodecValue::Int64(n);
        let as_unsigned = value.as_u64();

        prop_assert_eq!(as_unsigned, None);
    }

    /// Property: as_i64 works for unsigned values that fit
    #[test]
    fn prop_as_i64_preserves_fitting_unsigned(n in 0u64..(i64::MAX as u64)) {
        let value = CodecValue::UInt64(n);
        let as_signed = value.as_i64();

        prop_assert_eq!(as_signed, Some(n as i64));
    }

    /// Property: as_i64 returns None for unsigned values that overflow i64
    #[test]
    fn prop_as_i64_none_for_overflow_unsigned(n in (i64::MAX as u64 + 1)..u64::MAX) {
        let value = CodecValue::UInt64(n);
        let as_signed = value.as_i64();

        prop_assert_eq!(as_signed, None);
    }
}

// ============================================================================
// Type Properties
// ============================================================================

proptest! {
    /// Property: is_container implies Array or Struct
    #[test]
    fn prop_is_container_consistent(arr in prop::collection::vec(any::<i32>(), 0..10)) {
        let array_val = CodecValue::Array(arr.iter().map(|&i| CodecValue::Int32(i)).collect());
        prop_assert!(array_val.is_container());

        let mut map = HashMap::new();
        map.insert("a".to_string(), CodecValue::Int32(42));
        let struct_val = CodecValue::Struct(map);
        prop_assert!(struct_val.is_container());
    }

    /// Property: Non-containers are not containers
    #[test]
    fn prop_non_containers_are_not_containers(n in any::<i64>()) {
        let int_val = CodecValue::Int64(n);
        prop_assert!(!int_val.is_container());

        let str_val = CodecValue::String("test".to_string());
        prop_assert!(!str_val.is_container());
    }

    /// Property: is_temporal only for Timestamp and Duration
    #[test]
    fn prop_temporal_values_only(n in any::<i64>()) {
        let ts = CodecValue::Timestamp(n);
        prop_assert!(ts.is_temporal());

        let dur = CodecValue::Duration(n);
        prop_assert!(dur.is_temporal());

        let int = CodecValue::Int64(n);
        prop_assert!(!int.is_temporal());
    }
}

// Tests without parameters go outside proptest! macro
#[test]
fn prop_only_null_is_null() {
    let null_val = CodecValue::Null;
    assert!(null_val.is_null());

    let int_val = CodecValue::Int32(0);
    assert!(!int_val.is_null());

    let str_val = CodecValue::String("".to_string());
    assert!(!str_val.is_null());
}

#[test]
fn prop_size_hint_fixed_size_exact() {
    assert_eq!(CodecValue::Bool(true).size_hint(), 1);
    assert_eq!(CodecValue::Int8(0).size_hint(), 1);
    assert_eq!(CodecValue::Int16(0).size_hint(), 2);
    assert_eq!(CodecValue::Int32(0).size_hint(), 4);
    assert_eq!(CodecValue::Int64(0).size_hint(), 8);
    assert_eq!(CodecValue::UInt64(0).size_hint(), 8);
    assert_eq!(CodecValue::Float64(0.0).size_hint(), 8);
}

#[test]
fn prop_null_size_hint_zero() {
    assert_eq!(CodecValue::Null.size_hint(), 0);
}

// ============================================================================
// Size Properties
// ============================================================================

proptest! {
    /// Property: size_hint for String is the string length
    #[test]
    fn prop_string_size_hint_matches_length(s in "[a-zA-Z0-9]{0,100}") {
        let val = CodecValue::String(s.clone());
        prop_assert_eq!(val.size_hint(), s.len());
    }

    /// Property: size_hint for Bytes is the data length
    #[test]
    fn prop_bytes_size_hint_matches_length(data in prop::collection::vec(any::<u8>(), 0..100)) {
        let val = CodecValue::Bytes(data.clone());
        prop_assert_eq!(val.size_hint(), data.len());
    }

    /// Property: size_hint for Array is monotonic with length
    #[test]
    fn prop_array_size_hint_monotonic(arr1 in prop::collection::vec(any::<i32>(), 0..10),
                                        arr2 in prop::collection::vec(any::<i32>(), 0..10)) {
        let val1 = CodecValue::Array(arr1.iter().map(|&i| CodecValue::Int32(i)).collect());
        let val2 = CodecValue::Array(arr2.iter().map(|&i| CodecValue::Int32(i)).collect());

        if arr1.len() < arr2.len() {
            prop_assert!(val1.size_hint() < val2.size_hint() || val1.size_hint() == 0);
        }
    }
}

// ============================================================================
// Conversion Properties
// ============================================================================

proptest! {
    /// Property: Timestamp from secs/nanos is within valid range
    #[test]
    fn prop_timestamp_valid_range(secs in any::<u32>(), nanos in any::<u32>()) {
        let ts = CodecValue::timestamp_from_secs_nanos(secs, nanos);
        let total_nanos = ts.as_timestamp_nanos().unwrap();

        // Should be positive
        prop_assert!(total_nanos >= 0);
    }

    /// Property: Duration can be negative
    #[test]
    fn prop_duration_can_be_negative(secs in -1000i32..0, nanos in any::<i32>()) {
        let dur = CodecValue::duration_from_secs_nanos(secs, nanos);
        let total_nanos = dur.as_duration_nanos().unwrap();

        prop_assert!(total_nanos <= 0);
    }

    /// Property: ROS1 time produces valid timestamps
    #[test]
    fn prop_ros1_time_valid(secs in any::<u32>(), nanos in any::<u32>()) {
        let ts = CodecValue::from_ros1_time(secs, nanos);
        let total_nanos = ts.as_timestamp_nanos().unwrap();

        prop_assert!(total_nanos >= 0);
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(total_nanos, expected);
    }

    /// Property: ROS2 time can be negative
    #[test]
    fn prop_ros2_time_can_be_negative(secs in -1000i32..0, nanos in 0u32..999_999_999u32) {
        // Constrain nanos to keep total negative when secs is negative
        let nanos = nanos % 1_000_000_000;
        let ts = CodecValue::from_ros2_time(secs, nanos);
        let total_nanos = ts.as_timestamp_nanos().unwrap();

        // Total should be negative since secs < 0 and nanos < 1 second
        let expected = (secs as i64) * 1_000_000_000 + (nanos as i64);
        prop_assert_eq!(total_nanos, expected);
        prop_assert!(total_nanos < 0);
    }
}

// ============================================================================
// Equality Properties
// ============================================================================

proptest! {
    /// Property: CodecValue equality is reflexive
    #[test]
    fn prop_codec_value_reflexive(value in simple_value()) {
        prop_assert_eq!(value.clone(), value);
    }

    /// Property: CodecValue equality is symmetric
    #[test]
    fn prop_codec_value_symmetric(a in simple_value(), b in simple_value()) {
        if a == b {
            prop_assert_eq!(b, a);
        }
    }

    /// Property: CodecValue equality is transitive
    #[test]
    fn prop_codec_value_transitive(a in simple_value(), b in simple_value(), c in simple_value()) {
        if a == b && b == c {
            prop_assert_eq!(a, c);
        }
    }

    /// Property: Cloned CodecValue equals original
    #[test]
    fn prop_codec_value_clone_equals(value in simple_value()) {
        prop_assert_eq!(value.clone(), value.clone());
    }
}

/// Strategy for generating simple CodecValue instances
fn simple_value() -> impl Strategy<Value = CodecValue> {
    prop_oneof![
        any::<i32>().prop_map(CodecValue::Int32),
        any::<i64>().prop_map(CodecValue::Int64),
        any::<u32>().prop_map(CodecValue::UInt32),
        any::<u64>().prop_map(CodecValue::UInt64),
        any::<f64>()
            .prop_map(|f| if f.is_finite() { f } else { 0.0 })
            .prop_map(CodecValue::Float64),
        any::<bool>().prop_map(CodecValue::Bool),
        "[a-zA-Z0-9]{0,50}".prop_map(CodecValue::String),
        prop::collection::vec(any::<u8>(), 0..50).prop_map(CodecValue::Bytes),
        any::<i64>().prop_map(CodecValue::Timestamp),
        any::<i64>().prop_map(CodecValue::Duration),
        prop::collection::vec(any::<i32>(), 0..5)
            .prop_map(|v| v.into_iter().map(CodecValue::Int32).collect())
            .prop_map(CodecValue::Array),
    ]
}
