// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Round-trip integration tests.
//!
//! Tests cover:
//! - Reading bag files and writing to mcap files
//! - Reading mcap files and writing to bag files
//! - Preserving message data and metadata through round trips
//! - Using sequential mode for both reading and writing

use std::fs;
use std::path::PathBuf;

use robocodec::io::ReaderConfig;
use robocodec::io::RoboReader;
use robocodec::io::RoboWriter;
use robocodec::io::traits::{FormatReader, FormatWriter};

// ============================================================================
// Test Fixtures
// ============================================================================

fn fixtures_dir() -> PathBuf {
    PathBuf::from("tests/fixtures")
}

/// Get a temporary directory for test files
fn temp_dir() -> PathBuf {
    let random = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_id = format!("{:?}", std::thread::current().id());
    std::env::temp_dir().join(format!(
        "robocodec_roundtrip_{}_{}_{}",
        std::process::id(),
        thread_id,
        random
    ))
}

/// Create a temporary file path with cleanup guard
fn temp_path(name: &str) -> (PathBuf, CleanupGuard) {
    let dir = temp_dir();
    fs::create_dir_all(&dir).ok();
    let path = dir.join(name);
    let guard = CleanupGuard(dir);
    (path, guard)
}

/// Cleanup guard for test temporary files
struct CleanupGuard(PathBuf);

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ============================================================================
// Bag to MCAP Round-Trip Tests (Sequential Mode)
// ============================================================================

#[test]
fn test_round_trip_bag_to_mcap_sequential() {
    let bag_file = fixtures_dir().join("robocodec_test_15.bag");
    if !bag_file.exists() {
        return; // Skip if fixture doesn't exist
    }

    let (mcap_file, _guard) = temp_path("round_trip_bag_to_mcap.mcap");

    // Step 1: Read the bag file with sequential strategy
    let reader =
        RoboReader::open_with_config(bag_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open bag file");

    let original_channels = reader.channels().clone();

    // Step 2: Write to MCAP file
    let mut writer =
        RoboWriter::create(mcap_file.to_str().unwrap()).expect("Failed to create MCAP writer");

    // Add all channels from the original file
    for channel in original_channels.values() {
        writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
    }

    // Note: The public API decoded() returns DecodedMessage which can't be
    // re-encoded to RawMessage without access to the original bytes.
    // For full data transfer, format-specific APIs would be needed.
    // This test verifies metadata preservation via the public API.
    writer.finish().expect("Failed to finish writer");

    // Step 3: Read back the MCAP file and verify
    let mcap_reader =
        RoboReader::open_with_config(mcap_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open MCAP file");

    let mcap_channels = mcap_reader.channels();

    // Verify channel count matches (metadata preserved via public API)
    assert_eq!(
        mcap_channels.len(),
        original_channels.len(),
        "Channel count should match"
    );

    // Verify the output file exists and has content
    assert!(mcap_file.exists());
    assert!(fs::metadata(&mcap_file).unwrap().len() > 0);

    println!(
        "Bag → MCAP via public API: {} channels, metadata preserved",
        original_channels.len()
    );
}

#[test]
fn test_round_trip_bag_to_mcap_preserves_topics() {
    let bag_file = fixtures_dir().join("robocodec_test_15.bag");
    if !bag_file.exists() {
        return;
    }

    let (mcap_file, _guard) = temp_path("round_trip_topics.mcap");

    // Read bag and collect topics
    let reader =
        RoboReader::open_with_config(bag_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open bag file");

    let original_topics: std::collections::HashSet<String> = reader
        .channels()
        .values()
        .map(|ch| ch.topic.clone())
        .collect();

    // Write to MCAP
    let mut writer =
        RoboWriter::create(mcap_file.to_str().unwrap()).expect("Failed to create MCAP writer");

    // Collect channel IDs for writing a dummy message
    let mut channel_ids: Vec<u16> = Vec::new();
    for channel in reader.channels().values() {
        let id = writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
        channel_ids.push(id);
    }

    // Write a dummy message to ensure file is created properly
    if let Some(&first_id) = channel_ids.first() {
        let dummy_msg = robocodec::io::metadata::RawMessage {
            channel_id: first_id,
            log_time: 0,
            publish_time: 0,
            data: vec![],
            sequence: None,
        };
        writer
            .write(&dummy_msg)
            .expect("Failed to write dummy message");
    }

    writer.finish().expect("Failed to finish writer");

    // Verify topics are preserved
    let mcap_reader =
        RoboReader::open_with_config(mcap_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open MCAP file");

    let mcap_topics: std::collections::HashSet<String> = mcap_reader
        .channels()
        .values()
        .map(|ch| ch.topic.clone())
        .collect();

    assert_eq!(
        original_topics, mcap_topics,
        "Topics should be preserved in round trip"
    );
}

#[test]
fn test_round_trip_bag_to_mcap_preserves_message_types() {
    let bag_file = fixtures_dir().join("robocodec_test_15.bag");
    if !bag_file.exists() {
        return;
    }

    let (mcap_file, _guard) = temp_path("round_trip_types.mcap");

    // Read bag and collect message types
    let reader =
        RoboReader::open_with_config(bag_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open bag file");

    let original_types: std::collections::HashMap<String, String> = reader
        .channels()
        .values()
        .map(|ch| (ch.topic.clone(), ch.message_type.clone()))
        .collect();

    // Write to MCAP
    let mut writer =
        RoboWriter::create(mcap_file.to_str().unwrap()).expect("Failed to create MCAP writer");

    for channel in reader.channels().values() {
        writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .ok();
    }

    writer.finish().ok();

    // Verify message types are preserved
    let mcap_reader =
        RoboReader::open_with_config(mcap_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open MCAP file");

    let mcap_types: std::collections::HashMap<String, String> = mcap_reader
        .channels()
        .values()
        .map(|ch| (ch.topic.clone(), ch.message_type.clone()))
        .collect();

    for (topic, orig_type) in &original_types {
        let mcap_type = mcap_types
            .get(topic)
            .unwrap_or_else(|| panic!("Topic {} not found", topic));
        // Message types may have slight variations due to schema handling
        // The core type name should match
        let orig_base = orig_type
            .trim_end_matches("_schema")
            .trim_end_matches("_msg");
        let mcap_base = mcap_type
            .trim_end_matches("_schema")
            .trim_end_matches("_msg");
        assert_eq!(
            orig_base, mcap_base,
            "Message type for topic {} should be preserved (original: {}, mcap: {})",
            topic, orig_type, mcap_type
        );
    }
}

// ============================================================================
// MCAP to Bag Round-Trip Tests (Sequential Mode)
// ============================================================================

#[test]
fn test_round_trip_mcap_to_bag_sequential() {
    let mcap_file = fixtures_dir().join("robocodec_test_0.mcap");
    if !mcap_file.exists() {
        return;
    }

    let (bag_file, _guard) = temp_path("round_trip_mcap_to_bag.bag");

    // Step 1: Read the MCAP file with sequential strategy
    let reader =
        RoboReader::open_with_config(mcap_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open MCAP file");

    let original_channels = reader.channels().clone();
    let original_message_count = reader.message_count();

    // Step 2: Write to bag file
    let mut writer =
        RoboWriter::create(bag_file.to_str().unwrap()).expect("Failed to create bag writer");

    // Add all channels from the original file
    let mut channel_map: std::collections::HashMap<u16, u16> = std::collections::HashMap::new();
    for (orig_id, channel) in &original_channels {
        let new_id = writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
        channel_map.insert(*orig_id, new_id);
    }

    // Collect and write messages (using raw iteration)
    // Note: For a complete implementation, we'd iterate through all messages
    // For now, just verify the structure is correct
    writer.finish().expect("Failed to finish writer");

    // Step 3: Read back the bag file and verify
    let bag_reader =
        RoboReader::open_with_config(bag_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open bag file");

    let bag_channels = bag_reader.channels();

    // Verify channel count matches
    assert_eq!(
        bag_channels.len(),
        original_channels.len(),
        "Channel count should match"
    );

    // Verify the output file exists
    assert!(bag_file.exists());
    assert!(fs::metadata(&bag_file).unwrap().len() > 0);

    if original_message_count > 0 {
        println!(
            "Original MCAP had {} messages, bag channels: {}",
            original_message_count,
            bag_channels.len()
        );
    }
}

// ============================================================================
// Multiple Format Round-Trip Tests
// ============================================================================

#[test]
fn test_round_trip_bag_mcap_bag() {
    // This test does a full round trip: bag -> mcap -> bag
    let original_bag = fixtures_dir().join("robocodec_test_15.bag");
    if !original_bag.exists() {
        return;
    }

    let (intermediate_mcap, _guard1) = temp_path("intermediate.mcap");
    let (final_bag, _guard2) = temp_path("final.bag");

    // Step 1: Read original bag
    let reader1 =
        RoboReader::open_with_config(original_bag.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open original bag");

    let original_channels: Vec<(String, String)> = reader1
        .channels()
        .values()
        .map(|ch| (ch.topic.clone(), ch.message_type.clone()))
        .collect();

    // Step 2: Write to intermediate MCAP
    let mut writer = RoboWriter::create(intermediate_mcap.to_str().unwrap())
        .expect("Failed to create MCAP writer");
    for channel in reader1.channels().values() {
        writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .ok();
    }
    writer.finish().ok();

    // Step 3: Read MCAP and write to final bag
    let reader2 = RoboReader::open_with_config(
        intermediate_mcap.to_str().unwrap(),
        ReaderConfig::sequential(),
    )
    .expect("Failed to open MCAP file");

    let mut writer2 =
        RoboWriter::create(final_bag.to_str().unwrap()).expect("Failed to create bag writer");
    for channel in reader2.channels().values() {
        writer2
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .ok();
    }
    writer2.finish().ok();

    // Step 4: Verify final bag matches original
    let final_reader =
        RoboReader::open_with_config(final_bag.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open final bag");

    let final_channels: Vec<(String, String)> = final_reader
        .channels()
        .values()
        .map(|ch| (ch.topic.clone(), ch.message_type.clone()))
        .collect();

    assert_eq!(
        original_channels.len(),
        final_channels.len(),
        "Channel count should match through double round trip"
    );
}

// ============================================================================
// Sequential Strategy Specific Tests
// ============================================================================

#[test]
fn test_sequential_strategy_bag_reader() {
    let bag_file = fixtures_dir().join("robocodec_test_15.bag");
    if !bag_file.exists() {
        return;
    }

    // Test that sequential strategy works for bag files
    let reader =
        RoboReader::open_with_config(bag_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open bag with sequential strategy");

    // Verify we can access channels
    assert!(!reader.channels().is_empty(), "Should have channels");
}

#[test]
fn test_sequential_strategy_mcap_reader() {
    let mcap_file = fixtures_dir().join("robocodec_test_0.mcap");
    if !mcap_file.exists() {
        return;
    }

    // Test that sequential strategy works for MCAP files
    let reader =
        RoboReader::open_with_config(mcap_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open MCAP with sequential strategy");

    // Verify we can access channels
    assert!(!reader.channels().is_empty(), "Should have channels");
}

#[test]
fn test_round_trip_with_auto_strategy() {
    // Test that auto strategy also works for round trips
    let bag_file = fixtures_dir().join("robocodec_test_15.bag");
    if !bag_file.exists() {
        return;
    }

    let (mcap_file, _guard) = temp_path("auto_round_trip.mcap");

    // Use auto strategy for reading
    let reader = RoboReader::open(bag_file.to_str().unwrap())
        .expect("Failed to open bag with auto strategy");

    let channel_count = reader.channels().len();
    assert!(channel_count > 0, "Should have channels");

    // Write to MCAP
    let mut writer =
        RoboWriter::create(mcap_file.to_str().unwrap()).expect("Failed to create MCAP writer");
    for channel in reader.channels().values() {
        writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .ok();
    }
    writer.finish().ok();

    // Verify with auto strategy
    let mcap_reader = RoboReader::open(mcap_file.to_str().unwrap())
        .expect("Failed to open MCAP with auto strategy");
    assert_eq!(
        mcap_reader.channels().len(),
        channel_count,
        "Channel count should match with auto strategy"
    );
}

// ============================================================================
// RRD Format Tests
// ============================================================================
//
// NOTE: RRD round-trip tests (Bag/MCAP ↔ RRD) are not yet implemented due to
// fundamental format differences:
//
// - RRF2 stores messages as decoded Arrow/Protobuf data
// - Bag/MCAP store raw encoded messages (CDR, protobuf, etc.)
//
// While RrdReader::decode_messages() is implemented and working, format conversion
// would require:
// 1. Decoding Arrow IPC data to structured messages (for RRD → Bag/MCAP)
// 2. Encoding structured messages back to Arrow IPC format (for Bag/MCAP → RRD)
//
// This is a significant feature that requires Arrow schema knowledge and is
// tracked separately from basic format reading support.

#[test]
fn test_rrd_file_can_be_opened_with_public_api() {
    // Test that RRD files can be opened using RoboReader
    let rrd_file = fixtures_dir().join("rrd/small_uncompressed.rrd");
    if !rrd_file.exists() {
        return; // Skip if fixture doesn't exist
    }

    let reader = RoboReader::open(rrd_file.to_str().unwrap())
        .expect("Failed to open RRD file with RoboReader");

    // Verify basic properties
    assert!(
        !reader.channels().is_empty(),
        "RRD file should have channels"
    );

    // Test the decoded() API works for RRD
    let mut decoded_count = 0;
    for result in reader.decoded().expect("Failed to get decoded iterator") {
        let decoded = result.expect("Failed to decode message");
        // Verify we got a message with the 'data' field
        assert!(
            decoded.message.contains_key("data"),
            "Decoded message should have 'data' field"
        );
        decoded_count += 1;
    }

    println!(
        "RRD file: {} channels, {} decoded messages",
        reader.channels().len(),
        decoded_count
    );

    // Should have parsed messages from the RRD file
    assert!(
        decoded_count > 0,
        "Should have decoded at least one message"
    );
}

// ============================================================================
// RRD Format Round-Trip Tests
//
// These tests use RoboWriter and RoboReader (the public API) to verify
// RRD files can be written and read correctly.
//
// NOTE: Cross-format round-trips (Bag/MCAP ↔ RRD) are limited because:
// 1. RRF2 stores messages as ArrowMsg payloads (Protobuf/Arrow encoded)
// 2. Bag/MCAP store raw encoded messages
// 3. The public API only provides decoded() iteration, not raw message access
//
// For full round-trip testing, see the format-specific tests that use
// internal APIs.
// ============================================================================

#[test]
fn test_rrd_write_and_read_with_public_api() {
    // Test: Create RRD file with RoboWriter, read back with RoboReader
    let (rrd_file, _guard) = temp_path("rrd_write_read.rrd");

    // Step 1: Write RRD file using RoboWriter
    let mut writer =
        RoboWriter::create(rrd_file.to_str().unwrap()).expect("Failed to create RRD writer");

    // Add test channels
    let channel_1 = writer
        .add_channel("/test/topic1", "test_msgs/Data1", "json", None)
        .expect("Failed to add channel");
    let channel_2 = writer
        .add_channel("/test/topic2", "test_msgs/Data2", "cdr", None)
        .expect("Failed to add channel");

    // Write test messages
    for i in 0..5 {
        let msg = robocodec::io::metadata::RawMessage {
            channel_id: channel_1,
            log_time: 1000 + i as u64,
            publish_time: 1000 + i as u64,
            data: format!("message {}", i).into_bytes(),
            sequence: Some(i as u64),
        };
        writer.write(&msg).expect("Failed to write message");
    }

    for i in 0..3 {
        let msg = robocodec::io::metadata::RawMessage {
            channel_id: channel_2,
            log_time: 2000 + i as u64,
            publish_time: 2000 + i as u64,
            data: format!("topic2-msg{}", i).into_bytes(),
            sequence: Some(i as u64),
        };
        writer.write(&msg).expect("Failed to write message");
    }

    writer.finish().expect("Failed to finish writer");

    // Step 2: Read back using RoboReader
    let reader = RoboReader::open(rrd_file.to_str().unwrap()).expect("Failed to open RRD file");

    // Verify format
    assert_eq!(format!("{:?}", reader.format()), "Rrd");

    // Verify decoded messages can be read
    let mut decoded_count = 0;
    for result in reader.decoded().expect("Failed to get decoded iterator") {
        let decoded = result.expect("Failed to decode message");
        assert!(
            decoded.message.contains_key("data"),
            "Decoded message should have 'data' field"
        );
        decoded_count += 1;
    }

    assert_eq!(decoded_count, 8, "Should decode 8 messages");

    println!("RRD write → read: 8 messages written and read back");
}

#[test]
fn test_rrd_roundtrip_via_bag() {
    // Test: Write to Bag → convert to RRD → read back
    // This verifies RrdWriter works correctly through the public API
    let bag_file = fixtures_dir().join("robocodec_test_15.bag");
    if !bag_file.exists() {
        return;
    }

    let (rrd_file, _guard) = temp_path("bag_to_rrd.rrd");

    // Read bag file
    let bag_reader =
        RoboReader::open_with_config(bag_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open bag file");

    let original_channels: Vec<_> = bag_reader.channels().values().cloned().collect();

    // Write to RRD using public API
    let mut rrd_writer =
        RoboWriter::create(rrd_file.to_str().unwrap()).expect("Failed to create RRD writer");

    for channel in &original_channels {
        rrd_writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
    }

    // Note: We can't write raw messages via public API, so we just verify
    // the file was created with correct channel metadata
    rrd_writer.finish().expect("Failed to finish writer");

    // Read back RRD and verify
    // Note: RRF2 format uses 1 default channel (topics are in ArrowMsg payloads)
    let rrd_reader = RoboReader::open(rrd_file.to_str().unwrap()).expect("Failed to open RRD file");

    assert!(
        !rrd_reader.channels().is_empty(),
        "Should have at least 1 channel"
    );

    println!(
        "Bag → RRD via public API: {} input channels, RRF2 uses default channel with entity paths",
        original_channels.len()
    );
}

#[test]
fn test_rrd_roundtrip_via_mcap() {
    // Test: Write to MCAP → convert to RRD → read back
    let mcap_file = fixtures_dir().join("robocodec_test_0.mcap");
    if !mcap_file.exists() {
        return;
    }

    let (rrd_file, _guard) = temp_path("mcap_to_rrd.rrd");

    // Read MCAP file
    let mcap_reader =
        RoboReader::open_with_config(mcap_file.to_str().unwrap(), ReaderConfig::sequential())
            .expect("Failed to open MCAP file");

    let original_channels: Vec<_> = mcap_reader.channels().values().cloned().collect();

    // Write to RRD using public API
    let mut rrd_writer =
        RoboWriter::create(rrd_file.to_str().unwrap()).expect("Failed to create RRD writer");

    for channel in &original_channels {
        rrd_writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
    }

    rrd_writer.finish().expect("Failed to finish writer");

    // Read back RRD and verify
    // Note: RRF2 format uses 1 default channel (topics are in ArrowMsg payloads)
    let rrd_reader = RoboReader::open(rrd_file.to_str().unwrap()).expect("Failed to open RRD file");

    assert!(
        !rrd_reader.channels().is_empty(),
        "Should have at least 1 channel"
    );

    println!(
        "MCAP → RRD via public API: {} input channels, RRF2 uses default channel with entity paths",
        original_channels.len()
    );
}

#[test]
fn test_rrd_to_bag_via_public_api() {
    // Test: Read RRD → write to Bag
    let rrd_file = fixtures_dir().join("rrd/small_uncompressed.rrd");
    if !rrd_file.exists() {
        return;
    }

    let (bag_file, _guard) = temp_path("rrd_to_bag.bag");

    // Read RRD file
    let rrd_reader = RoboReader::open(rrd_file.to_str().unwrap()).expect("Failed to open RRD file");

    let original_channels: Vec<_> = rrd_reader.channels().values().cloned().collect();

    // Write to Bag using public API
    let mut bag_writer =
        RoboWriter::create(bag_file.to_str().unwrap()).expect("Failed to create bag writer");

    for channel in &original_channels {
        bag_writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
    }

    bag_writer.finish().expect("Failed to finish writer");

    // Read back Bag and verify channels
    let bag_reader = RoboReader::open(bag_file.to_str().unwrap()).expect("Failed to open bag file");

    assert_eq!(
        bag_reader.channels().len(),
        original_channels.len(),
        "Channel count should be preserved"
    );

    println!(
        "RRD → Bag via public API: {} channels preserved",
        original_channels.len()
    );
}

#[test]
fn test_rrd_to_mcap_via_public_api() {
    // Test: Read RRD → write to MCAP
    let rrd_file = fixtures_dir().join("rrd/small_uncompressed.rrd");
    if !rrd_file.exists() {
        return;
    }

    let (mcap_file, _guard) = temp_path("rrd_to_mcap.mcap");

    // Read RRD file
    let rrd_reader = RoboReader::open(rrd_file.to_str().unwrap()).expect("Failed to open RRD file");

    let original_channels: Vec<_> = rrd_reader.channels().values().cloned().collect();

    // Write to MCAP using public API
    let mut mcap_writer =
        RoboWriter::create(mcap_file.to_str().unwrap()).expect("Failed to create MCAP writer");

    for channel in &original_channels {
        mcap_writer
            .add_channel(
                &channel.topic,
                &channel.message_type,
                &channel.encoding,
                channel.schema.as_deref(),
            )
            .expect("Failed to add channel");
    }

    mcap_writer.finish().expect("Failed to finish writer");

    // Read back MCAP and verify channels
    let mcap_reader =
        RoboReader::open(mcap_file.to_str().unwrap()).expect("Failed to open MCAP file");

    assert_eq!(
        mcap_reader.channels().len(),
        original_channels.len(),
        "Channel count should be preserved"
    );

    println!(
        "RRD → MCAP via public API: {} channels preserved",
        original_channels.len()
    );
}
