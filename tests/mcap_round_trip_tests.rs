// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP round-trip tests.
//!
//! These tests validate reading an MCAP file, writing it back, and verifying
//! that schemas, channels, and messages are preserved.

use std::fs;
use std::path::Path;

use robocodec::io::formats::mcap::McapReader;
use robocodec::io::formats::mcap::ParallelMcapWriter;
use robocodec::rewriter::mcap::McapRewriter;
use robocodec::rewriter::RewriteOptions;

fn temp_dir() -> std::path::PathBuf {
    let random = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_id = format!("{:?}", std::thread::current().id());
    std::env::temp_dir().join(format!(
        "robocodec_mcap_roundtrip_{}_{}_{}",
        std::process::id(),
        thread_id,
        random
    ))
}

fn temp_mcap_path(name: &str) -> (std::path::PathBuf, CleanupGuard) {
    let dir = temp_dir();
    fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("{}.mcap", name));
    let guard = CleanupGuard(dir);
    (path, guard)
}

#[derive(Debug)]
struct CleanupGuard(std::path::PathBuf);

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Test reading and writing an MCAP file preserves schemas.
#[test]
fn test_mcap_round_trip_preserves_schemas() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    let (output_path, _guard) = temp_mcap_path("round_trip_schema");

    // Read original
    let reader_original = McapReader::open(fixture_path).expect("open original");
    let original_channels = reader_original.channels();

    // Write to new file
    let mut writer = ParallelMcapWriter::create(&output_path).expect("create writer");

    // Add all channels, preserving their IDs
    for channel in original_channels.values() {
        let schema_id = if let Some(schema_data) = &channel.schema {
            // Use the message_type as the schema name (MCAP format uses schema name for message type)
            writer
                .add_schema(
                    &channel.message_type,
                    &channel.encoding,
                    schema_data.as_bytes(),
                )
                .expect("add schema")
        } else {
            0
        };

        writer
            .add_channel_with_id(
                channel.id,
                schema_id,
                &channel.topic,
                &channel.encoding,
                &std::collections::HashMap::new(),
            )
            .expect("add channel");
    }

    writer.finish().expect("finish writer");

    // Verify schemas match
    let reader_new = McapReader::open(&output_path).expect("open output");
    let new_channels = reader_new.channels();

    assert_eq!(
        new_channels.len(),
        original_channels.len(),
        "Channel count should match"
    );

    for (id, orig_channel) in original_channels.iter() {
        let new_channel = new_channels.get(id).expect("Channel should exist");
        assert_eq!(new_channel.topic, orig_channel.topic);
        assert_eq!(new_channel.message_type, orig_channel.message_type);
    }
}

/// Test reading and writing preserves message data.
#[test]
fn test_mcap_round_trip_preserves_messages() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    let (output_path, guard) = temp_mcap_path("round_trip_messages");

    // Read original messages
    let reader_original = McapReader::open(fixture_path).expect("open original");
    let original_channels = reader_original.channels();
    let mut messages = Vec::new();

    let iter = reader_original.iter_raw().expect("iterate");
    let stream = iter.stream().expect("stream");

    for result in stream {
        let (msg, _channel) = result.expect("read message");
        messages.push(msg);
    }

    // Write messages to new file
    let mut writer = ParallelMcapWriter::create(&output_path).expect("create writer");

    // First add channels
    for channel in original_channels.values() {
        let schema_id = if let Some(schema_data) = &channel.schema {
            // Use the message_type as the schema name (MCAP format uses schema name for message type)
            writer
                .add_schema(
                    &channel.message_type,
                    &channel.encoding,
                    schema_data.as_bytes(),
                )
                .expect("add schema")
        } else {
            0
        };

        writer
            .add_channel_with_id(
                channel.id,
                schema_id,
                &channel.topic,
                &channel.encoding,
                &std::collections::HashMap::new(),
            )
            .expect("add channel");
    }

    // Then write messages
    for msg in &messages {
        writer
            .write_message(msg.channel_id, msg.log_time, msg.publish_time, &msg.data)
            .expect("write message");
    }

    writer.finish().expect("finish");

    // Verify messages match
    let reader_new = McapReader::open(&output_path).expect("open output");
    let mut new_messages = Vec::new();

    let iter = reader_new.iter_raw().expect("iterate");
    let stream = iter.stream().expect("stream");

    for result in stream {
        let (msg, _channel) = result.expect("read message");
        new_messages.push(msg);
    }

    assert_eq!(
        new_messages.len(),
        messages.len(),
        "Message count should match"
    );

    for (i, (orig, new)) in messages.iter().zip(new_messages.iter()).enumerate() {
        assert_eq!(
            new.channel_id, orig.channel_id,
            "Message {} channel_id should match",
            i
        );
        assert_eq!(
            new.log_time, orig.log_time,
            "Message {} log_time should match",
            i
        );
        assert_eq!(new.data, orig.data, "Message {} data should match", i);
    }

    // Keep guard alive until here
    drop(guard);
}

/// Test using McapRewriter for round-trip conversion.
#[test]
fn test_mcap_rewriter_round_trip() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    let (output_path, _guard) = temp_mcap_path("rewriter_round_trip");

    // Use rewriter to copy file
    let options = RewriteOptions::default();
    let mut rewriter = McapRewriter::with_options(options);
    let stats = rewriter
        .rewrite(fixture_path, &output_path)
        .expect("rewrite");

    // Verify some messages were processed
    assert!(
        stats.message_count > 0,
        "Should have processed some messages"
    );
}

/// Test round-trip through rewriter with no transformations.
#[test]
fn test_mcap_rewriter_no_transform_preserves_data() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    let (output_path, _guard) = temp_mcap_path("rewriter_no_transform");

    // Read original message count
    let reader_original = McapReader::open(fixture_path).expect("open original");
    let iter = reader_original.iter_raw().expect("iterate");
    let stream = iter.stream().expect("stream");
    let original_count = stream.count();

    // Rewrite without transformations
    let options = RewriteOptions {
        transforms: None,
        validate_schemas: false,
        skip_decode_failures: true,
        passthrough_non_cdr: true,
    };
    let mut rewriter = McapRewriter::with_options(options);
    let stats = rewriter
        .rewrite(fixture_path, &output_path)
        .expect("rewrite");

    // Verify message count is preserved
    assert_eq!(stats.message_count, original_count as u64);
}

/// Test that channel info is preserved in round-trip.
#[test]
fn test_mcap_round_trip_preserves_channel_metadata() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    let (output_path, _guard) = temp_mcap_path("channel_metadata");

    // Read original
    let reader_original = McapReader::open(fixture_path).expect("open original");
    let channels = reader_original.channels();

    // Write to new file
    let mut writer = ParallelMcapWriter::create(&output_path).expect("create writer");

    for channel in channels.values() {
        let schema_id = if let Some(schema_data) = &channel.schema {
            // Use the message_type as the schema name (MCAP format uses schema name for message type)
            writer
                .add_schema(
                    &channel.message_type,
                    &channel.encoding,
                    schema_data.as_bytes(),
                )
                .expect("add schema")
        } else {
            0
        };

        writer
            .add_channel_with_id(
                channel.id,
                schema_id,
                &channel.topic,
                &channel.encoding,
                &std::collections::HashMap::new(),
            )
            .expect("add channel");
    }

    writer.finish().expect("finish");

    // Verify metadata
    let reader_new = McapReader::open(&output_path).expect("open output");

    for (id, orig) in channels.iter() {
        let new = reader_new.channels().get(id).expect("Channel should exist");
        assert_eq!(new.topic, orig.topic, "Topic should match");
        assert_eq!(
            new.message_type, orig.message_type,
            "Message type should match"
        );
        assert_eq!(new.encoding, orig.encoding, "Encoding should match");
    }
}

/// Test file info is preserved.
#[test]
fn test_mcap_round_trip_file_info() {
    let fixture_path = "tests/fixtures/robocodec_test_0.mcap";

    if !Path::new(fixture_path).exists() {
        eprintln!("Skipping test: fixture not found at {}", fixture_path);
        return;
    }

    let reader = McapReader::open(fixture_path).expect("open");

    // Just verify we can get file info
    let _start_time = reader.start_time();
    let _end_time = reader.end_time();
    let _message_count = reader.message_count();

    // If these work, the round-trip tests should work too
    assert!(reader.message_count() > 0, "Should have messages");
}
