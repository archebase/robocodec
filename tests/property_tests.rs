// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Property-based tests for robocodec.
//!
//! This test module runs all property-based tests using the proptest framework.
//! Run with:
//!
//! ```bash
//! cargo test --test property_tests
//! ```
//!
//! For more detailed failure analysis, run with:
//!
//! ```bash
//! PROPTEST_FUZZ=100000 cargo test --test property_tests
//! ```

mod property;

// ============================================================================
// Additional Integration Property Tests
// ============================================================================

use proptest::prelude::*;
use robocodec::io::metadata::ChannelInfo;
use std::collections::HashMap;

// ============================================================================
// Format-Agnostic Property Tests
// ============================================================================

proptest! {
    /// Property: Empty channel info is valid
    #[test]
    fn prop_empty_channel_info_valid(id in 0u16..1000u16) {
        let info = ChannelInfo::new(id, "/empty/topic", "std_msgs/Empty");
        prop_assert_eq!(info.id, id);
        prop_assert_eq!(info.topic, "/empty/topic");
        prop_assert_eq!(info.message_type, "std_msgs/Empty");
        prop_assert_eq!(info.message_count, 0);
    }

    /// Property: Channel info with all fields set preserves values
    #[test]
    fn prop_channel_info_all_fields(id in 0u16..1000u16,
                                     topic in "/[a-z_]{1,20}",
                                     msg_type in "[a-z_]{1,10}/[A-Z][a-zA-Z]{1,20}") {
        let topic_cloned = topic.to_string();
        let msg_type_cloned = msg_type.to_string();
        let encoding = "cdr";
        let schema = "string data";
        let message_count = id as u64;
        let callerid = "/node123";

        let info = ChannelInfo::new(id, &topic_cloned, &msg_type_cloned)
            .with_encoding(encoding)
            .with_schema(schema)
            .with_message_count(message_count)
            .with_callerid(callerid);

        prop_assert_eq!(info.id, id);
        prop_assert_eq!(info.topic, topic_cloned);
        prop_assert_eq!(info.message_type, msg_type_cloned);
        prop_assert_eq!(info.encoding, encoding);
        prop_assert_eq!(info.schema, Some(schema.to_string()));
        prop_assert_eq!(info.message_count, message_count);
        prop_assert_eq!(info.callerid, Some(callerid.to_string()));
    }
}

// ============================================================================
// JSON Serialization Property Tests
// ============================================================================

proptest! {
    /// Property: JSON serialization of integers preserves values
    #[test]
    fn prop_json_int_preserved(n in any::<i64>()) {
        use robocodec::core::CodecValue;
        let value = CodecValue::Int64(n);
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// Property: JSON serialization of strings preserves content
    #[test]
    fn prop_json_string_preserved(s in "[a-zA-Z0-9 ]{0,100}") {
        use robocodec::core::CodecValue;
        let value = CodecValue::String(s.clone());
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(value, decoded);
    }

    /// Property: JSON serialization of arrays preserves length
    #[test]
    fn prop_json_array_length_preserved(arr in prop::collection::vec(any::<i32>(), 0..20)) {
        use robocodec::core::CodecValue;
        let value = CodecValue::Array(
            arr.iter().map(|&i| CodecValue::Int32(i)).collect()
        );
        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();

        match (&value, &decoded) {
            (CodecValue::Array(orig), CodecValue::Array(dec)) => {
                prop_assert_eq!(orig.len(), dec.len());
            }
            _ => prop_assert!(false, "Both should be arrays"),
        }
    }

    /// Property: JSON serialization of structs preserves keys
    #[test]
    fn prop_json_struct_keys_preserved(keys in prop::collection::vec("[a-z]{1,10}", 1..10)) {
        use robocodec::core::CodecValue;
        use std::collections::HashSet;

        let mut map = HashMap::new();
        for key in &keys {
            map.insert(key.clone(), CodecValue::Int32(42));
        }
        let value = CodecValue::Struct(map);

        let json = serde_json::to_string(&value).unwrap();
        let decoded: CodecValue = serde_json::from_str(&json).unwrap();

        match (&value, &decoded) {
            (CodecValue::Struct(orig), CodecValue::Struct(dec)) => {
                // Compare as sets since JSON might reorder keys
                let orig_keys: HashSet<_> = orig.keys().collect();
                let dec_keys: HashSet<_> = dec.keys().collect();
                prop_assert_eq!(orig_keys, dec_keys);
            }
            _ => prop_assert!(false, "Both should be structs"),
        }
    }
}

// ============================================================================
// Error Property Tests
// ============================================================================

proptest! {
    /// Property: CodecError can be cloned
    #[test]
    fn prop_error_cloneable(context in "[a-z]{1,20}", message in "[a-z]{1,50}") {
        use robocodec::core::CodecError;

        let err1 = CodecError::parse(&context, &message);
        let err2 = err1.clone();

        prop_assert_eq!(err1.to_string(), err2.to_string());
    }

    /// Property: Error log fields are non-empty for populated errors
    #[test]
    fn prop_error_log_fields_exist(context in "[a-z]{1,20}", message in "[a-z]{1,50}") {
        use robocodec::core::CodecError;

        let err = CodecError::parse(&context, &message);
        let fields = err.log_fields();

        prop_assert!(!fields.is_empty());
        prop_assert!(fields.len() >= 2);
    }
}
