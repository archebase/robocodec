// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Round-trip tests for RRD format using real Rerun files.
//!
//! These tests verify that we can read actual Rerun RRD files correctly
//! and write RRD files that can be read back.

use std::fs;
use std::path::Path;

use robocodec::io::formats::rrd::stream::{MessageKind, RRD_STREAM_MAGIC, StreamingRrdParser};
use robocodec::io::formats::rrd::{RrdReader, RrdWriter};
use robocodec::io::s3::StreamingParser;
use robocodec::io::{FormatWriter, RawMessage};

/// Helper function to load a test fixture file.
fn load_fixture(name: &str) -> Vec<u8> {
    let path = Path::new("tests/fixtures/rrd").join(name);
    fs::read(&path).unwrap_or_else(|_| panic!("Failed to read fixture: {}", name))
}

/// Test that we can read a Rerun RRD file using RrdReader.
#[test]
fn test_read_rerun_rrd_with_rrd_reader() {
    let path = "tests/fixtures/rrd/file1.rrd";
    assert!(Path::new(path).exists(), "Fixture file1.rrd should exist");

    // Open the file with RrdReader
    let reader = RrdReader::open(path).expect("Failed to open RRD file");
    println!("Opened: {}", reader.path());
    println!("Channels: {}", reader.channels().len());
    assert!(
        !reader.channels().is_empty(),
        "Should have at least one channel"
    );

    // Get decoded iterator
    let iter = reader
        .decode_messages()
        .expect("Failed to get decoded iterator");
    let mut message_count = 0;
    for result in iter {
        let (decoded, channel) = result.expect("Failed to read message");
        message_count += 1;
        let data_len = decoded
            .get("data")
            .and_then(|v| v.as_bytes())
            .map(|b| b.len())
            .unwrap_or(0);
        println!(
            "Message {}: channel={}, topic={}, data_len={}",
            message_count, channel.id, channel.topic, data_len
        );
    }

    println!("Total messages read: {}", message_count);
    assert!(message_count > 0, "Should have read at least one message");
}

/// Test reading all Rerun RRD files.
#[test]
fn test_read_all_rerun_rrd_files() {
    let rerun_files = [
        "file1.rrd",
        "file2.rrd",
        "file3.rrd",
        "file4.rrd",
        "file5.rrd",
        "file6.rrd",
        "file7.rrd",
        "file8.rrd",
        "file9.rrd",
        "file10.rrd",
        "file11.rrd",
        "file12.rrd",
        "file13.rrd",
        "file14.rrd",
        "file15.rrd",
        "file16.rrd",
        "file17.rrd",
        "file18.rrd",
        "file19.rrd",
        "file20.rrd",
    ];

    for filename in rerun_files {
        let path = format!("tests/fixtures/rrd/{}", filename);
        if !Path::new(&path).exists() {
            println!("Skipping {} (not found)", filename);
            continue;
        }

        let reader =
            RrdReader::open(&path).unwrap_or_else(|_| panic!("Failed to open {}", filename));
        let iter = reader
            .decode_messages()
            .unwrap_or_else(|_| panic!("Failed to get decoded iterator for {}", filename));

        let mut count = 0;
        for result in iter {
            let _msg =
                result.unwrap_or_else(|_| panic!("Failed to read message from {}", filename));
            count += 1;
        }

        println!("{} -> {} messages", filename, count);
        assert!(count > 0, "{} should have at least one message", filename);
    }
}

/// Test that we can write a valid RRD file.
#[test]
fn test_write_rrd_file() {
    use tempfile::NamedTempFile;

    let temp = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp.path().to_str().unwrap().to_string();

    // Create a writer
    let mut writer = RrdWriter::create(&path).expect("Failed to create writer");

    // Add a channel
    let channel_id = writer
        .add_channel("/test", "rerun.ArrowMsg", "protobuf", None)
        .expect("Failed to add channel");

    // Write some messages
    for i in 0u64..5 {
        let data = format!("test message {}", i).into_bytes();
        let message = RawMessage {
            channel_id,
            log_time: i * 1000,
            publish_time: i * 1000,
            data,
            sequence: Some(i),
        };
        writer.write(&message).expect("Failed to write message");
    }

    writer.finish().expect("Failed to finish");

    // Verify the file was written and has valid magic
    let written = fs::read(&path).expect("Failed to read written file");
    assert_eq!(&written[0..4], RRD_STREAM_MAGIC);

    // Verify we can read it back with RrdReader
    let reader = RrdReader::open(&path).expect("Failed to open written file");
    let iter = reader
        .decode_messages()
        .expect("Failed to get decoded iterator");
    let mut count = 0;
    for result in iter {
        let _msg = result.expect("Failed to read message back");
        count += 1;
    }
    assert_eq!(count, 5, "Should have read back 5 messages");
}

/// Test round-trip: read Rerun file -> write -> read again.
#[test]
fn test_round_trip_rerun_file() {
    use tempfile::NamedTempFile;

    let original_path = "tests/fixtures/rrd/file1.rrd";
    assert!(Path::new(original_path).exists(), "file1.rrd should exist");

    // Read original file using RrdReader
    let original_reader = RrdReader::open(original_path).expect("Failed to open original file");
    let original_iter = original_reader
        .decode_messages()
        .expect("Failed to get decoded iterator");

    // Collect messages
    let mut messages = Vec::new();
    for result in original_iter {
        let (decoded, channel) = result.expect("Failed to read message");
        messages.push((decoded, channel));
    }

    println!("Original: {} messages", messages.len());

    // Write to a new file
    let temp = NamedTempFile::new().expect("Failed to create temp file");
    let output_path = temp.path().to_str().unwrap().to_string();

    let mut writer = RrdWriter::create(&output_path).expect("Failed to create writer");

    // Add channel (RRD uses single channel with id 0)
    let channel_id = writer
        .add_channel("/", "rerun.ArrowMsg", "protobuf", None)
        .expect("Failed to add channel");

    // Write messages
    for (decoded, _channel) in &messages {
        if let Some(data_value) = decoded.get("data")
            && let Some(bytes) = data_value.as_bytes()
        {
            let data = bytes.to_vec();
            let raw_msg = RawMessage {
                channel_id,
                log_time: 0,
                publish_time: 0,
                data,
                sequence: None,
            };
            writer.write(&raw_msg).expect("Failed to write message");
        }
    }

    writer.finish().expect("Failed to finish");

    // Read back the written file
    let new_reader = RrdReader::open(&output_path).expect("Failed to open written file");
    let new_iter = new_reader
        .decode_messages()
        .expect("Failed to get decoded iterator");

    let mut new_count = 0;
    for result in new_iter {
        let _msg = result.expect("Failed to read message");
        new_count += 1;
    }

    println!("Round-trip: {} messages", new_count);

    // Verify message count matches
    assert_eq!(new_count, messages.len(), "Message count should match");
}

/// Test streaming parser with real Rerun file verifies message kinds.
#[test]
fn test_rerun_file_message_kinds() {
    let data = load_fixture("file1.rrd");
    let mut parser = StreamingRrdParser::new();
    let messages = parser.parse_chunk(&data).expect("Failed to parse");

    println!("Parsed {} messages", messages.len());

    // Count message kinds
    let mut arrow_msg_count = 0;
    let mut set_store_info_count = 0;
    let mut blueprint_count = 0;

    for msg in &messages {
        match msg.kind {
            MessageKind::ArrowMsg => arrow_msg_count += 1,
            MessageKind::SetStoreInfo => set_store_info_count += 1,
            MessageKind::BlueprintActivationCommand => blueprint_count += 1,
            MessageKind::End => {}
        }
    }

    println!(
        "Message kinds: ArrowMsg={}, SetStoreInfo={}, Blueprint={}",
        arrow_msg_count, set_store_info_count, blueprint_count
    );

    // We expect at least one ArrowMsg and possibly SetStoreInfo
    assert!(
        arrow_msg_count > 0 || set_store_info_count > 0,
        "Should have at least ArrowMsg or SetStoreInfo messages"
    );
}

/// Test that written RRD file has correct structure.
#[test]
fn test_written_rrd_structure() {
    let temp = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let path = temp.path().to_str().unwrap().to_string();

    // Write a simple RRD file
    {
        let mut writer = RrdWriter::create(&path).expect("Failed to create writer");
        let channel_id = writer
            .add_channel("/test", "rerun.ArrowMsg", "protobuf", None)
            .expect("Failed to add channel");

        let message = RawMessage {
            channel_id,
            log_time: 1000,
            publish_time: 1000,
            data: b"hello world".to_vec(),
            sequence: Some(0),
        };
        writer.write(&message).expect("Failed to write");
        writer.finish().expect("Failed to finish");
    }

    // Read and verify structure
    let data = fs::read(&path).expect("Failed to read file");

    // Check magic
    assert_eq!(&data[0..4], RRD_STREAM_MAGIC);

    // Parse with streaming parser
    let mut parser = StreamingRrdParser::new();
    let messages = parser.parse_chunk(&data).expect("Failed to parse");

    assert_eq!(messages.len(), 1, "Should have 1 message");
    assert_eq!(messages[0].kind, MessageKind::ArrowMsg);
    assert_eq!(messages[0].topic, "/"); // Default topic for ArrowMsg
    assert!(parser.is_initialized());
}
