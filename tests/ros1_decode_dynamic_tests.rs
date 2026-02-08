// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Tests for the ROS1 `decode_dynamic` path in `CdrCodec`.
//!
//! The S3 / streaming decode path uses `CdrCodec::decode_dynamic()` via
//! the `CodecFactory`, which is a different code path from the local-file
//! `decode_headerless_ros1()` path. These tests exercise that branch with
//! a real ROS1 bag fixture (`robocodec_test_24_leju_claw.bag`) containing
//! 300 messages from a Leju claw robot, including topics like
//! `/leju_claw_state` (kuavo_msgs/lejuClawState) that have
//! `std_msgs/Header` (with the ROS1 `seq` field).
//!
//! The fixture was extracted from a production bag to reproduce a bug where
//! `decode_dynamic()` always called `CdrDecoder::decode()` (which expects
//! a 4-byte CDR header) instead of `decode_headerless_ros1()` for ROS1
//! data, causing the cursor to be 4 bytes off and reading string content
//! as length prefixes.

use std::collections::HashMap;
use std::path::PathBuf;

use robocodec::ChannelInfo;
use robocodec::core::Encoding;
use robocodec::encoding::codec::{CodecFactory, SchemaMetadata};
use robocodec::io::formats::bag::stream::StreamingBagParser;

fn fixture_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures");
    path.push(name);
    path
}

/// Build a `SchemaMetadata` cache from channel info, exactly mirroring
/// the production path in `roboflow-sources/src/decode.rs`.
fn build_schema_cache(
    channels: &HashMap<u16, ChannelInfo>,
    factory: &CodecFactory,
) -> HashMap<u16, SchemaMetadata> {
    let mut cache = HashMap::new();
    for (&id, ch) in channels {
        let encoding = factory.detect_encoding(&ch.encoding, ch.schema_encoding.as_deref());
        let schema = match encoding {
            Encoding::Cdr => SchemaMetadata::cdr_with_encoding(
                ch.message_type.clone(),
                ch.schema.clone().unwrap_or_default(),
                ch.schema_encoding.clone(),
            ),
            Encoding::Protobuf => SchemaMetadata::protobuf(
                ch.message_type.clone(),
                ch.schema_data.clone().unwrap_or_default(),
            ),
            Encoding::Json => SchemaMetadata::json(
                ch.message_type.clone(),
                ch.schema.clone().unwrap_or_default(),
            ),
        };
        cache.insert(id, schema);
    }
    cache
}

// ============================================================================
// Core regression test: streaming decode of ROS1 bag via decode_dynamic
// ============================================================================

/// Simulate the S3 streaming decode path for a ROS1 bag file.
///
/// This reads the fixture file in 64 KB streaming chunks through
/// `StreamingBagParser`, then decodes each raw message via
/// `CodecFactory` / `CdrCodec::decode_dynamic()` — the exact code
/// path that was broken before the fix.
#[test]
fn test_streaming_decode_dynamic_ros1_bag() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        eprintln!("Skipping: fixture not found at {}", path.display());
        return;
    }

    let data = std::fs::read(&path).expect("Failed to read fixture bag");

    // Phase 1: parse the bag via the streaming parser (same as S3 path)
    let mut parser = StreamingBagParser::new();
    let mut all_records = Vec::new();

    for chunk in data.chunks(64 * 1024) {
        let records = parser
            .parse_chunk(chunk)
            .expect("StreamingBagParser::parse_chunk failed");
        all_records.extend(records);
    }

    let channels = parser.channels();
    assert!(!channels.is_empty(), "Should discover at least one channel");
    assert!(
        !all_records.is_empty(),
        "Should parse at least one message record"
    );

    // Phase 2: build schema cache & codec factory (mirrors production path)
    let factory = CodecFactory::new();
    let schema_cache = build_schema_cache(&channels, &factory);

    // Phase 3: decode every message through decode_dynamic
    let mut decoded_count = 0usize;
    let mut error_count = 0usize;
    let mut topic_counts: HashMap<String, usize> = HashMap::new();

    for record in &all_records {
        let channel_id = record.conn_id as u16;
        let Some(channel_info) = channels.get(&channel_id) else {
            continue;
        };
        let Some(schema) = schema_cache.get(&channel_id) else {
            continue;
        };

        let encoding = schema.encoding();
        let codec = factory.get_codec(encoding).expect("Should have CDR codec");

        match codec.decode_dynamic(&record.data, schema) {
            Ok(decoded) => {
                decoded_count += 1;
                *topic_counts.entry(channel_info.topic.clone()).or_default() += 1;

                // Sanity: decoded message should have at least one field
                assert!(
                    !decoded.is_empty(),
                    "Decoded message for topic {} should have fields",
                    channel_info.topic
                );
            }
            Err(e) => {
                error_count += 1;
                // Print but don't fail — some topics (e.g. compressed images)
                // may have schemas that the CDR decoder can't fully handle,
                // but the claw/joint/tf topics must succeed.
                eprintln!(
                    "  decode error on topic={} type={}: {}",
                    channel_info.topic, channel_info.message_type, e
                );
            }
        }
    }

    eprintln!(
        "Decoded {decoded_count}/{} messages ({error_count} errors)",
        all_records.len()
    );
    for (topic, count) in &topic_counts {
        eprintln!("  {topic}: {count} messages");
    }

    // At least 50% of messages must decode (images may fail, that's OK)
    assert!(
        decoded_count > all_records.len() / 2,
        "Expected >50% decode success, got {decoded_count}/{}",
        all_records.len()
    );
}

// ============================================================================
// Targeted: /leju_claw_state must decode (the original failing topic)
// ============================================================================

/// Ensure that `/leju_claw_state` (kuavo_msgs/lejuClawState) messages
/// decode successfully. This was the exact topic that triggered the
/// "String length 1600351329 exceeds maximum" error before the fix.
#[test]
fn test_decode_dynamic_leju_claw_state_topic() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        eprintln!("Skipping: fixture not found at {}", path.display());
        return;
    }

    let data = std::fs::read(&path).expect("Failed to read fixture bag");

    let mut parser = StreamingBagParser::new();
    let mut all_records = Vec::new();

    for chunk in data.chunks(64 * 1024) {
        let records = parser.parse_chunk(chunk).expect("parse_chunk failed");
        all_records.extend(records);
    }

    let channels = parser.channels();
    let factory = CodecFactory::new();
    let schema_cache = build_schema_cache(&channels, &factory);

    let mut claw_decoded = 0usize;
    let mut claw_errors = Vec::new();

    for record in &all_records {
        let channel_id = record.conn_id as u16;
        let Some(channel_info) = channels.get(&channel_id) else {
            continue;
        };

        if channel_info.topic != "/leju_claw_state" {
            continue;
        }

        let schema = schema_cache
            .get(&channel_id)
            .expect("Should have schema for /leju_claw_state");

        let codec = factory
            .get_codec(schema.encoding())
            .expect("Should have CDR codec");

        match codec.decode_dynamic(&record.data, schema) {
            Ok(decoded) => {
                claw_decoded += 1;
                // The lejuClawState message should have a header field with seq
                // (since this is ROS1 and std_msgs/Header includes seq)
                assert!(
                    !decoded.is_empty(),
                    "Decoded lejuClawState should have fields"
                );
            }
            Err(e) => {
                claw_errors.push(format!("{e}"));
            }
        }
    }

    assert!(
        claw_decoded > 0,
        "Expected at least one /leju_claw_state message to decode, but got 0 (errors: {:?})",
        claw_errors
    );
    assert!(
        claw_errors.is_empty(),
        "All /leju_claw_state messages should decode, but {} failed: {:?}",
        claw_errors.len(),
        claw_errors
    );
}

// ============================================================================
// Targeted: /sensors_data_raw must also decode (flat ROS1 type with Header)
// ============================================================================

/// Ensure non-trivial ROS1 topics with `std_msgs/Header` decode correctly.
///
/// `/sensors_data_raw` (kuavo_msgs/sensorsData) is a flat struct with a
/// Header, exercising the same seq-field path as `/leju_claw_state`.
#[test]
fn test_decode_dynamic_sensors_data_raw_topic() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        eprintln!("Skipping: fixture not found at {}", path.display());
        return;
    }

    let data = std::fs::read(&path).expect("Failed to read fixture bag");

    let mut parser = StreamingBagParser::new();
    let mut all_records = Vec::new();

    for chunk in data.chunks(64 * 1024) {
        let records = parser.parse_chunk(chunk).expect("parse_chunk failed");
        all_records.extend(records);
    }

    let channels = parser.channels();
    let factory = CodecFactory::new();
    let schema_cache = build_schema_cache(&channels, &factory);

    let mut decoded_count = 0usize;
    let mut errors = Vec::new();

    for record in &all_records {
        let channel_id = record.conn_id as u16;
        let Some(channel_info) = channels.get(&channel_id) else {
            continue;
        };

        if channel_info.topic != "/sensors_data_raw" {
            continue;
        }

        let schema = schema_cache
            .get(&channel_id)
            .expect("Should have schema for /sensors_data_raw");

        let codec = factory
            .get_codec(schema.encoding())
            .expect("Should have CDR codec");

        match codec.decode_dynamic(&record.data, schema) {
            Ok(decoded) => {
                decoded_count += 1;
                assert!(
                    !decoded.is_empty(),
                    "Decoded sensorsData should have fields"
                );
            }
            Err(e) => {
                errors.push(format!("{e}"));
            }
        }
    }

    assert!(
        decoded_count > 0,
        "Expected at least one /sensors_data_raw message to decode (errors: {:?})",
        errors
    );
    assert!(
        errors.is_empty(),
        "All /sensors_data_raw messages should decode, but {} failed: {:?}",
        errors.len(),
        errors
    );
}

// ============================================================================
// Targeted: /tf must decode (tf2_msgs/TFMessage with nested Header)
// ============================================================================

/// Ensure `/tf` (tf2_msgs/TFMessage) messages decode successfully.
///
/// TFMessage contains a dynamic array of geometry_msgs/TransformStamped,
/// each with std_msgs/Header (seq, stamp, frame_id), child_frame_id string,
/// and Transform. This exercises nested Header resolution and ROS1 string
/// reading (no null terminator). Uses fixture `robocodec_test_24_leju_claw.bag`.
#[test]
fn test_decode_dynamic_tf_topic() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        eprintln!("Skipping: fixture not found at {}", path.display());
        return;
    }

    let data = std::fs::read(&path).expect("Failed to read fixture bag");

    let mut parser = StreamingBagParser::new();
    let mut all_records = Vec::new();

    for chunk in data.chunks(64 * 1024) {
        let records = parser.parse_chunk(chunk).expect("parse_chunk failed");
        all_records.extend(records);
    }

    let channels = parser.channels();
    let factory = CodecFactory::new();
    let schema_cache = build_schema_cache(&channels, &factory);

    let mut tf_decoded = 0usize;
    let mut tf_errors = Vec::new();

    for record in &all_records {
        let channel_id = record.conn_id as u16;
        let Some(channel_info) = channels.get(&channel_id) else {
            continue;
        };

        if channel_info.topic != "/tf" {
            continue;
        }

        let schema = schema_cache
            .get(&channel_id)
            .expect("Should have schema for /tf");

        let codec = factory
            .get_codec(schema.encoding())
            .expect("Should have CDR codec");

        match codec.decode_dynamic(&record.data, schema) {
            Ok(decoded) => {
                tf_decoded += 1;
                // TFMessage has "transforms" array
                assert!(!decoded.is_empty(), "Decoded TFMessage should have fields");
                assert!(
                    decoded.contains_key("transforms"),
                    "Decoded TFMessage should have 'transforms' field"
                );
            }
            Err(e) => {
                tf_errors.push(format!("{e}"));
            }
        }
    }

    assert!(
        tf_decoded > 0,
        "Expected at least one /tf message to decode, but got 0 (errors: {:?})",
        tf_errors
    );
    assert!(
        tf_errors.is_empty(),
        "All /tf messages should decode, but {} failed: {:?}",
        tf_errors.len(),
        tf_errors
    );
}

// ============================================================================
// Verify ROS1 schema encoding is detected for CDR channels
// ============================================================================

#[test]
fn test_ros1_bag_schema_encoding_detected() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        eprintln!("Skipping: fixture not found at {}", path.display());
        return;
    }

    let data = std::fs::read(&path).expect("Failed to read fixture bag");

    let mut parser = StreamingBagParser::new();
    for chunk in data.chunks(64 * 1024) {
        parser.parse_chunk(chunk).expect("parse_chunk failed");
    }

    let channels = parser.channels();
    let factory = CodecFactory::new();
    let schema_cache = build_schema_cache(&channels, &factory);

    // All CDR channels in a ROS1 bag should have ros1msg in schema_encoding
    // This is what triggers the ROS1 decode path in CdrCodec::decode_dynamic()
    let mut ros1_channel_count = 0usize;
    for (&id, schema) in &schema_cache {
        if let SchemaMetadata::Cdr {
            schema_encoding,
            type_name,
            ..
        } = schema
        {
            let ch = channels.get(&id).expect("channel should exist");
            // Verify schema_encoding contains "ros1" (case-insensitive check)
            let has_ros1_encoding = ch
                .schema_encoding
                .as_deref()
                .is_some_and(|e| e.to_lowercase().contains("ros1"));

            assert!(
                has_ros1_encoding,
                "Channel {} ({}) should have ros1* schema_encoding, got {:?}",
                ch.topic, type_name, ch.schema_encoding
            );
            assert!(
                schema_encoding
                    .as_deref()
                    .is_some_and(|e| e.to_lowercase().contains("ros1")),
                "SchemaMetadata for {} should carry ros1* encoding, got {:?}",
                type_name,
                schema_encoding
            );
            ros1_channel_count += 1;
        }
    }

    assert!(
        ros1_channel_count > 0,
        "Expected at least one ROS1 CDR channel in the fixture"
    );
}
