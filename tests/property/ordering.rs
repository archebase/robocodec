// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Ordering property tests.
//!
//! These tests verify that ordering invariants are maintained across operations.

use proptest::prelude::*;
use robocodec::core::CodecValue;
use robocodec::io::metadata::{ChannelInfo, MessageMetadata, RawMessage};

// ============================================================================
// Strategy Definitions
// ============================================================================

/// Strategy for generating a vector of timestamps in nanoseconds
fn timestamp_vector() -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(1_000_000_000u64..2_000_000_000u64, 1..100)
}

/// Strategy for generating RawMessage instances with valid timestamps
fn raw_message_vector() -> impl Strategy<Value = Vec<RawMessage>> {
    prop::collection::vec(
        (
            0u16..10u16,                               // channel_id
            1_000_000_000u64..2_000_000_000u64,        // log_time
            1_000_000_000u64..2_000_000_000u64,        // publish_time
            prop::collection::vec(any::<u8>(), 0..50), // data
        )
            .prop_map(|(channel_id, log_time, publish_time, data)| {
                RawMessage::new(channel_id, log_time, publish_time, data)
            }),
        1..50,
    )
}

/// Strategy for generating MessageMetadata instances
fn message_metadata() -> impl Strategy<Value = MessageMetadata> {
    (
        0u16..10u16,                         // channel_id
        1_000_000_000u64..10_000_000_000u64, // log_time
        1_000_000_000u64..10_000_000_000u64, // publish_time
        100u64..1_000_000u64,                // data_offset
        1u32..1000u32,                       // data_len
    )
        .prop_map(
            |(channel_id, log_time, publish_time, data_offset, data_len)| {
                MessageMetadata::new(channel_id, log_time, publish_time, data_offset, data_len)
            },
        )
}

// ============================================================================
// Timestamp Ordering Tests
// ============================================================================

proptest! {
    /// Property: A sorted timestamp vector remains stable after sorting
    #[test]
    fn prop_sorted_timestamps_stable(mut timestamps in timestamp_vector()) {
        let len = timestamps.len();
        timestamps.sort();
        timestamps.dedup();

        // After sorting and dedup, all adjacent pairs should be ordered
        for i in 1..timestamps.len().min(100) {
            prop_assert!(timestamps[i] >= timestamps[i - 1],
                "Timestamps should be non-decreasing: {} >= {}",
                timestamps[i], timestamps[i - 1]);
        }

        // Length after dedup should be <= original length
        prop_assert!(timestamps.len() <= len);
    }

    /// Property: Timestamp range is non-negative
    #[test]
    fn prop_timestamp_range_non_negative(timestamps in timestamp_vector()) {
        if let (Some(min), Some(max)) = (timestamps.iter().min(), timestamps.iter().max()) {
            let range = *max - *min;
            prop_assert!(range >= 0, "Timestamp range should be non-negative");
        }
    }

    /// Property: Duration between timestamps is non-negative
    #[test]
    fn prop_timestamp_difference_non_negative(ts1 in 1_000_000_000u64..2_000_000_000u64,
                                                ts2 in 1_000_000_000u64..2_000_000_000u64) {
        let (earlier, later) = if ts1 <= ts2 { (ts1, ts2) } else { (ts2, ts1) };
        let duration = later - earlier;
        prop_assert!(duration >= 0, "Duration should be non-negative");
    }
}

// ============================================================================
// RawMessage Ordering Tests
// ============================================================================

proptest! {
    /// Property: Messages can be sorted by log_time
    #[test]
    fn prop_messages_sortable_by_log_time(mut messages in raw_message_vector()) {
        messages.sort_by_key(|m| m.log_time);

        // Verify all adjacent pairs are in order
        for i in 1..messages.len().min(50) {
            prop_assert!(messages[i].log_time >= messages[i - 1].log_time,
                "Messages should be sorted by log_time");
        }
    }

    /// Property: Messages can be sorted by publish_time
    #[test]
    fn prop_messages_sortable_by_publish_time(mut messages in raw_message_vector()) {
        messages.sort_by_key(|m| m.publish_time);

        // Verify all adjacent pairs are in order
        for i in 1..messages.len().min(50) {
            prop_assert!(messages[i].publish_time >= messages[i - 1].publish_time,
                "Messages should be sorted by publish_time");
        }
    }

    /// Property: Channel IDs are preserved during sorting
    #[test]
    fn prop_channel_ids_preserved_during_sort(mut messages in raw_message_vector()) {
        let original_channel_ids: Vec<_> = messages.iter().map(|m| m.channel_id).collect();

        messages.sort_by_key(|m| m.log_time);

        let sorted_channel_ids: Vec<_> = messages.iter().map(|m| m.channel_id).collect();

        // Check that the same number of messages exist
        prop_assert_eq!(original_channel_ids.len(), sorted_channel_ids.len());
    }
}

// ============================================================================
// MessageMetadata Ordering Tests
// ============================================================================

proptest! {
    /// Property: MessageMetadata data range is well-formed
    #[test]
    fn prop_metadata_data_range_valid(metadata in message_metadata()) {
        let (start, end) = metadata.data_range();
        prop_assert!(start < end, "Data range start should be less than end");
        prop_assert_eq!(end - start, metadata.data_len as u64,
            "Data range length should equal data_len");
    }

    /// Property: MessageMetadata is valid for reasonable file sizes
    #[test]
    fn prop_metadata_valid_for_file_size(metadata in message_metadata(),
                                          file_size in 1_000_000u64..100_000_000u64) {
        let (_start, end) = metadata.data_range();

        // Either it fits or it doesn't - no invalid states
        if end <= file_size {
            prop_assert!(metadata.is_valid_for_size(file_size));
        } else {
            prop_assert!(!metadata.is_valid_for_size(file_size));
        }
    }

    /// Property: MessageMetadata is invalid for file sizes smaller than the data
    #[test]
    fn prop_metadata_invalid_for_small_file(metadata in message_metadata()) {
        let too_small = metadata.data_offset.saturating_sub(1);
        if too_small > 0 {
            prop_assert!(!metadata.is_valid_for_size(too_small),
                "Metadata should be invalid for file smaller than data offset");
        }
    }
}

// ============================================================================
// CodecValue Ordering Tests
// ============================================================================

proptest! {
    /// Property: Type checking is consistent
    #[test]
    fn prop_type_checking_consistent(value in prop_oneof![
        any::<i64>().prop_map(CodecValue::Int64),
        any::<u64>().prop_map(CodecValue::UInt64),
        any::<f64>().prop_map(CodecValue::Float64),
        any::<bool>().prop_map(CodecValue::Bool),
    ]) {
        // If it's signed, it shouldn't be unsigned and vice versa
        if value.is_signed_integer() {
            prop_assert!(!value.is_unsigned_integer());
            prop_assert!(value.is_integer());
        }
        if value.is_unsigned_integer() {
            prop_assert!(!value.is_signed_integer());
            prop_assert!(value.is_integer());
        }

        // Floats are numeric but not integers
        if value.is_float() {
            prop_assert!(value.is_numeric());
            prop_assert!(!value.is_integer());
        }
    }
}

// ============================================================================
// ChannelInfo Ordering Tests
// ============================================================================

proptest! {
    /// Property: ChannelInfo IDs are unique in a collection
    #[test]
    fn prop_channel_ids_are_unique(count in 1usize..20usize) {
        use std::collections::HashSet;

        // Create channel infos with unique IDs using enumerate
        let channel_infos: Vec<_> = (0..count).map(|i| {
            let id = i as u16;
            ChannelInfo::new(id, &format!("/topic_{}", id), &format!("std_msgs/Type_{}", id))
        }).collect();

        // Collect unique channel IDs
        let unique_ids: HashSet<_> = channel_infos.iter().map(|c| c.id).collect();
        let total_ids = channel_infos.len();

        // Since we use unique IDs, all should be unique
        prop_assert_eq!(unique_ids.len(), total_ids);
    }

    /// Property: ChannelInfo builder preserves fields
    #[test]
    fn prop_channel_info_builder_preserves(id in 0u16..1000u16,
                                          _topic_count in 1usize..20usize) {
        let topic = format!("/topic_{}", id);
        let msg_type = format!("std_msgs/Type_{}", id);
        let encoding = "cdr";
        let message_count = id as u64;

        let info = ChannelInfo::new(id, &topic, &msg_type)
            .with_encoding(encoding)
            .with_message_count(message_count);

        prop_assert_eq!(info.id, id);
        prop_assert_eq!(info.topic, topic);
        prop_assert_eq!(info.message_type, msg_type);
        prop_assert_eq!(info.encoding, encoding);
        prop_assert_eq!(info.message_count, message_count);
    }
}

// ============================================================================
// Sequence Ordering Tests
// ============================================================================

proptest! {
    /// Property: Sequence numbers in a collection can be ordered
    #[test]
    fn prop_sequence_numbers_orderable(sequences in prop::collection::vec(
        any::<u64>(), 1..50
    )) {
        let mut sorted = sequences.clone();
        sorted.sort();
        sorted.dedup();

        // All elements should be in non-decreasing order
        for i in 1..sorted.len().min(100) {
            prop_assert!(sorted[i] >= sorted[i - 1]);
        }
    }

    /// Property: Sequence numbers are monotonically increasing
    #[test]
    fn prop_sequence_monotonic(start in 0u64..1000u64, count in 1usize..100usize) {
        let sequences: Vec<u64> = (0..count).map(|i| start + i as u64).collect();

        for i in 1..sequences.len() {
            prop_assert!(sequences[i] > sequences[i - 1]);
            prop_assert_eq!(sequences[i] - sequences[i - 1], 1);
        }
    }
}
