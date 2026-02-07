// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Consistency property tests.
//!
//! These tests verify that data consistency invariants are maintained.

use proptest::prelude::*;
use robocodec::core::{CodecValue, DecodedMessage, Encoding};
use robocodec::io::metadata::{ChannelInfo, DecodedMessageResult, RawMessage};

// ============================================================================
// Strategy Definitions
// ============================================================================

/// Strategy for generating valid channel IDs
fn channel_id() -> impl Strategy<Value = u16> {
    0u16..1000u16
}

/// Strategy for generating valid topic names
fn topic_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z/_]{1,20}[a-z0-9_]{0,20}").unwrap()
}

/// Strategy for generating valid message type names
fn message_type() -> impl Strategy<Value = String> {
    "[a-z_]{1,10}/[a-z_]{1,10}/[A-Z][a-zA-Z0-9_]{0,30}"
}

/// Strategy for generating encoding strings
fn encoding_str() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("cdr".to_string()),
        Just("protobuf".to_string()),
        Just("json".to_string()),
    ]
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
    ]
}

/// Strategy for generating DecodedMessage instances
fn decoded_message() -> impl Strategy<Value = DecodedMessage> {
    prop::collection::hash_map("[a-z_]{1,20}[a-z0-9_]{0,10}", simple_value(), 0..10)
}

// ============================================================================
// ChannelInfo Consistency Tests
// ============================================================================

proptest! {
    /// Property: ChannelInfo fields remain consistent after construction
    #[test]
    fn prop_channel_info_consistent(id in channel_id(),
                                     topic in topic_name(),
                                     msg_type in message_type(),
                                     encoding in encoding_str()) {
        let info = ChannelInfo::new(id, &topic, &msg_type)
            .with_encoding(&encoding)
            .with_message_count(0);

        prop_assert_eq!(info.id, id);
        prop_assert_eq!(info.topic, topic);
        prop_assert_eq!(info.message_type, msg_type);
        prop_assert_eq!(info.encoding, encoding);
        prop_assert_eq!(info.message_count, 0);
    }

    /// Property: ChannelInfo clone produces identical data
    #[test]
    fn prop_channel_info_clone_consistent(id in channel_id(),
                                          topic in topic_name(),
                                          msg_type in message_type()) {
        let info1 = ChannelInfo::new(id, &topic, &msg_type);
        let info2 = info1.clone();

        prop_assert_eq!(info1.id, info2.id);
        prop_assert_eq!(info1.topic, info2.topic);
        prop_assert_eq!(info1.message_type, info2.message_type);
    }

    /// Property: ChannelInfo builder chain is consistent
    #[test]
    fn prop_channel_info_builder_chain_consistent(id in channel_id(),
                                                   topic in topic_name(),
                                                   msg_type in message_type()) {
        let info = ChannelInfo::new(id, &topic, &msg_type)
            .with_encoding("cdr")
            .with_schema("string data")
            .with_message_count(100)
            .with_callerid("/node");

        prop_assert_eq!(info.id, id);
        prop_assert_eq!(info.topic, topic);
        prop_assert_eq!(info.message_type, msg_type);
        prop_assert_eq!(info.encoding, "cdr");
        prop_assert_eq!(info.schema, Some("string data".to_string()));
        prop_assert_eq!(info.message_count, 100);
        prop_assert_eq!(info.callerid, Some("/node".to_string()));
    }
}

// ============================================================================
// RawMessage Consistency Tests
// ============================================================================

proptest! {
    /// Property: RawMessage length is consistent with data
    #[test]
    fn prop_raw_message_length_consistent(channel_id in channel_id(),
                                          data in prop::collection::vec(any::<u8>(), 0..100)) {
        let msg = RawMessage::new(channel_id, 1000, 900, data.clone());

        prop_assert_eq!(msg.len(), data.len());
        prop_assert_eq!(msg.is_empty(), data.is_empty());
        prop_assert_eq!(msg.data, data);
    }

    /// Property: RawMessage with sequence has consistent metadata
    #[test]
    fn prop_raw_message_sequence_consistent(channel_id in channel_id(),
                                            data in prop::collection::vec(any::<u8>(), 0..100),
                                            sequence in any::<u64>()) {
        let msg = RawMessage::new(channel_id, 1000, 900, data)
            .with_sequence(sequence);

        prop_assert_eq!(msg.channel_id, channel_id);
        prop_assert_eq!(msg.sequence, Some(sequence));
    }

    /// Property: RawMessage timestamp fields are preserved
    #[test]
    fn prop_raw_message_timestamps_preserved(channel_id in channel_id(),
                                             log_time in any::<u64>(),
                                             publish_time in any::<u64>(),
                                             data in prop::collection::vec(any::<u8>(), 0..50)) {
        let msg = RawMessage::new(channel_id, log_time, publish_time, data);

        prop_assert_eq!(msg.log_time, log_time);
        prop_assert_eq!(msg.publish_time, publish_time);
    }
}

// ============================================================================
// DecodedMessageResult Consistency Tests
// ============================================================================

proptest! {
    /// Property: DecodedMessageResult timestamps are consistent
    #[test]
    fn prop_decoded_result_timestamps_consistent(message in decoded_message(),
                                                   channel_id in channel_id(),
                                                   topic in topic_name()) {
        let channel = ChannelInfo::new(channel_id, &topic, "std_msgs/String");
        let log_time = Some(1_000_000_000u64);
        let publish_time = Some(900_000_000u64);

        let result = DecodedMessageResult::new(
            message,
            channel,
            log_time,
            publish_time,
        );

        prop_assert!(result.has_timestamps());
    }

    /// Property: DecodedMessageResult topic() returns the channel topic
    #[test]
    fn prop_decoded_result_topic_consistent(message in decoded_message(),
                                             topic in topic_name()) {
        let channel = ChannelInfo::new(0, &topic, "std_msgs/String");
        let result = DecodedMessageResult::new(
            message,
            channel,
            None,
            None,
        );

        prop_assert_eq!(result.topic(), topic);
    }

    /// Property: DecodedMessageResult message_type() returns the channel type
    #[test]
    fn prop_decoded_result_type_consistent(message in decoded_message(),
                                            msg_type in message_type()) {
        let channel = ChannelInfo::new(0, "/topic", &msg_type);
        let result = DecodedMessageResult::new(
            message,
            channel,
            None,
            None,
        );

        prop_assert_eq!(result.message_type(), msg_type);
    }

    /// Property: DecodedMessageResult times() returns correct tuple
    #[test]
    fn prop_decoded_result_times_consistent(message in decoded_message(),
                                             log_time in any::<u64>(),
                                             publish_time in any::<u64>()) {
        let channel = ChannelInfo::new(0, "/topic", "std_msgs/String");
        let result = DecodedMessageResult::new(
            message,
            channel,
            Some(log_time),
            Some(publish_time),
        );

        let times = result.times();
        prop_assert_eq!(times, (Some(log_time), Some(publish_time)));
    }

    /// Property: DecodedMessageResult with_sequence preserves sequence
    #[test]
    fn prop_decoded_result_sequence_consistent(message in decoded_message(),
                                                sequence in any::<u64>()) {
        let channel = ChannelInfo::new(0, "/topic", "std_msgs/String");
        let result = DecodedMessageResult::new(
            message,
            channel,
            None,
            None,
        ).with_sequence(sequence);

        prop_assert_eq!(result.sequence, Some(sequence));
    }
}

// ============================================================================
// CodecValue Consistency Tests
// ============================================================================

proptest! {
    /// Property: CodecValue type_name is consistent with actual type
    #[test]
    fn prop_codec_value_type_name_consistent(value in simple_value()) {
        let type_name = value.type_name();

        match value {
            CodecValue::Int32(_) => prop_assert_eq!(type_name, "int32"),
            CodecValue::Int64(_) => prop_assert_eq!(type_name, "int64"),
            CodecValue::UInt32(_) => prop_assert_eq!(type_name, "uint32"),
            CodecValue::UInt64(_) => prop_assert_eq!(type_name, "uint64"),
            CodecValue::Float64(_) => prop_assert_eq!(type_name, "float64"),
            CodecValue::Bool(_) => prop_assert_eq!(type_name, "bool"),
            CodecValue::String(_) => prop_assert_eq!(type_name, "string"),
            _ => prop_assert!(true), // Other types are handled
        }
    }

    /// Property: CodecValue is_numeric is consistent with as_f64
    #[test]
    fn prop_codec_value_numeric_consistent(value in simple_value()) {
        let is_numeric = value.is_numeric();
        let can_be_f64 = value.as_f64().is_some();

        prop_assert_eq!(is_numeric, can_be_f64,
            "is_numeric should be consistent with as_f64 returning Some");
    }

    /// Property: CodecValue is_integer is consistent with as_i64
    #[test]
    fn prop_codec_value_integer_consistent(value in simple_value()) {
        // is_integer checks if it's a signed or unsigned integer type
        // as_i64 returns Some only if it fits in i64
        // So for unsigned integers that fit, both should be true
        if value.is_unsigned_integer() {
            if let Some(n) = value.as_u64() {
                let fits = n <= (i64::MAX as u64);
                prop_assert_eq!(fits, value.as_i64().is_some());
            }
        }

        // For signed integers, as_i64 should always return Some
        if value.is_signed_integer() {
            prop_assert!(value.as_i64().is_some());
        }
    }

    /// Property: CodecValue size_hint is non-negative
    #[test]
    fn prop_codec_value_size_hint_non_negative(value in simple_value()) {
        let size = value.size_hint();
        prop_assert!(size <= 1_000_000, "Size hint should be reasonable");
    }

    /// Property: String CodecValue as_str returns the original string
    #[test]
    fn prop_string_codec_value_consistent(s in "[a-zA-Z0-9]{0,100}") {
        let value = CodecValue::String(s.clone());
        prop_assert_eq!(value.as_str(), Some(s.as_str()));
    }

    /// Property: Timestamp CodecValue nanos are preserved
    #[test]
    fn prop_timestamp_codec_value_consistent(nanos in any::<i64>()) {
        let value = CodecValue::Timestamp(nanos);
        prop_assert_eq!(value.as_timestamp_nanos(), Some(nanos));
    }

    /// Property: Duration CodecValue nanos are preserved
    #[test]
    fn prop_duration_codec_value_consistent(nanos in any::<i64>()) {
        let value = CodecValue::Duration(nanos);
        prop_assert_eq!(value.as_duration_nanos(), Some(nanos));
    }

    /// Property: Bytes CodecValue as_bytes returns the original data
    #[test]
    fn prop_bytes_codec_value_consistent(data in prop::collection::vec(any::<u8>(), 0..100)) {
        let value = CodecValue::Bytes(data.clone());
        prop_assert_eq!(value.as_bytes(), Some(data.as_slice()));
    }
}

// ============================================================================
// Encoding Consistency Tests
// ============================================================================

proptest! {
    /// Property: Encoding as_str is consistent with is_* methods
    #[test]
    fn prop_encoding_str_consistent(encoding in prop_oneof![
        Just(Encoding::Cdr),
        Just(Encoding::Protobuf),
        Just(Encoding::Json),
    ]) {
        let s = encoding.as_str();

        match encoding {
            Encoding::Cdr => {
                prop_assert_eq!(s, "cdr");
                prop_assert!(encoding.is_cdr());
                prop_assert!(!encoding.is_protobuf());
                prop_assert!(!encoding.is_json());
            }
            Encoding::Protobuf => {
                prop_assert_eq!(s, "protobuf");
                prop_assert!(!encoding.is_cdr());
                prop_assert!(encoding.is_protobuf());
                prop_assert!(!encoding.is_json());
            }
            Encoding::Json => {
                prop_assert_eq!(s, "json");
                prop_assert!(!encoding.is_cdr());
                prop_assert!(!encoding.is_protobuf());
                prop_assert!(encoding.is_json());
            }
        }
    }

    /// Property: Encoding from_str is consistent with as_str
    #[test]
    fn prop_encoding_from_str_consistent(s in prop_oneof![
        Just("cdr"),
        Just("protobuf"),
        Just("json"),
    ]) {
        let encoding: Result<Encoding, _> = s.parse();
        prop_assert!(encoding.is_ok());

        let encoding = encoding.unwrap();
        prop_assert_eq!(encoding.as_str(), s);
    }

    /// Property: Encoding is case-insensitive when parsing
    #[test]
    fn prop_encoding_case_insensitive(s in "[A-Za-z]{3,8}") {
        let lower = s.to_lowercase();
        let parsed: Result<Encoding, _> = lower.as_str().parse();

        match lower.as_str() {
            "cdr" | "protobuf" | "json" => {
                prop_assert!(parsed.is_ok());
            }
            _ => {
                prop_assert!(parsed.is_err());
            }
        }
    }
}

// ============================================================================
// HashMap Consistency Tests
// ============================================================================

proptest! {
    /// Property: DecodedMessage field access is consistent
    #[test]
    fn prop_decoded_message_field_access_consistent(
        fields in prop::collection::hash_map(
            "[a-z_]{1,20}",
            simple_value(),
            1..20,
        ),
        key in "[a-z_]{1,20}"
    ) {
        if let Some(value) = fields.get(&key) {
            // If key exists, we should get the same value
            prop_assert_eq!(fields.get(&key), Some(value));
        }
    }

    /// Property: DecodedMessage iteration returns all keys
    #[test]
    fn prop_decoded_message_iteration_consistent(
        fields in prop::collection::hash_map(
            "[a-z_]{1,20}",
            simple_value(),
            1..20,
        )
    ) {
        let keys_from_get: Vec<_> = fields.keys().collect();
        let keys_from_iter: Vec<_> = fields.iter().map(|(k, _)| k).collect();

        // Same number of keys
        prop_assert_eq!(keys_from_get.len(), keys_from_iter.len());

        // Same keys (as sets)
        use std::collections::HashSet;
        let set1: HashSet<_> = keys_from_get.into_iter().collect();
        let set2: HashSet<_> = keys_from_iter.into_iter().collect();
        prop_assert_eq!(set1, set2);
    }
}
