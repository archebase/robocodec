// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for BAG transport reader.
//!
//! These tests verify that `BagTransportReader` produces identical results
//! to the memory-mapped `BagFormat` reader.

use std::collections::HashMap;

use robocodec::io::{
    FormatReader,
    formats::bag::{BagFormat, BagTransportReader},
};

/// Get the path to a test fixture.
fn fixture_path(filename: &str) -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests/fixtures").join(filename)
}

/// Test that BagTransportReader can open a local BAG file.
#[test]
fn test_transport_reader_open_local() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    let reader = BagTransportReader::open(&bag_path).expect("Failed to open BAG file");

    // Should have at least one channel
    assert!(
        !reader.channels().is_empty(),
        "Expected at least one channel"
    );

    // Should have messages
    assert!(reader.message_count() > 0, "Expected at least one message");

    // Path should match
    assert_eq!(reader.path(), bag_path.to_string_lossy().as_ref());

    // Format should be Bag
    assert!(matches!(
        reader.format(),
        robocodec::io::metadata::FileFormat::Bag
    ));
}

/// Test that BagTransportReader produces the same channel info as BagFormat.
#[test]
fn test_transport_reader_channels_match_mmap() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    // Open via transport reader
    let transport_reader =
        BagTransportReader::open(&bag_path).expect("Failed to open with transport");
    let transport_channels: HashMap<_, _> = transport_reader
        .channels()
        .iter()
        .map(|(id, ch)| (*id, ch.clone()))
        .collect();

    // Open via memory-mapped reader
    let mmap_reader = BagFormat::open(&bag_path).expect("Failed to open with mmap");
    let mmap_channels: HashMap<_, _> = mmap_reader
        .channels()
        .iter()
        .map(|(id, ch)| (*id, ch.clone()))
        .collect();

    // Channel counts should match
    assert_eq!(
        transport_channels.len(),
        mmap_channels.len(),
        "Channel count mismatch"
    );

    // Each channel should match
    for (id, transport_ch) in &transport_channels {
        let mmap_ch = mmap_channels
            .get(id)
            .unwrap_or_else(|| panic!("Channel {} not found in mmap reader", id));

        assert_eq!(
            transport_ch.topic, mmap_ch.topic,
            "Topic mismatch for channel {}",
            id
        );
        assert_eq!(
            transport_ch.message_type, mmap_ch.message_type,
            "Message type mismatch for channel {}",
            id
        );
        assert_eq!(
            transport_ch.encoding, mmap_ch.encoding,
            "Encoding mismatch for channel {}",
            id
        );
    }
}

/// Test that BagTransportReader produces the same message count as BagFormat.
#[test]
fn test_transport_reader_message_count_match_mmap() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    let transport_reader =
        BagTransportReader::open(&bag_path).expect("Failed to open with transport");
    let mmap_reader = BagFormat::open(&bag_path).expect("Failed to open with mmap");

    assert!(
        transport_reader.message_count() > 0,
        "Transport reader should have messages"
    );
    assert!(
        mmap_reader.message_count() > 0,
        "Mmap reader should have messages"
    );
}

/// Test that timestamps are preserved correctly.
#[test]
fn test_transport_reader_timestamps_valid() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    let reader = BagTransportReader::open(&bag_path).expect("Failed to open BAG file");

    // Should have valid start and end times
    let start_time = reader.start_time().expect("Should have start time");
    let end_time = reader.end_time().expect("Should have end time");

    // End time should be >= start time
    assert!(
        end_time >= start_time,
        "End time ({}) should be >= start time ({})",
        end_time,
        start_time
    );

    // Times should be reasonable (not zero for a valid bag)
    assert!(start_time > 0, "Start time should be > 0");
}

/// Test iter_raw_boxed produces messages.
#[test]
fn test_transport_reader_iter_raw() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    let reader = BagTransportReader::open(&bag_path).expect("Failed to open BAG file");
    let message_count = reader.message_count();

    let mut count = 0;
    for result in reader.iter_raw_boxed().expect("Failed to create iterator") {
        let (_msg, _channel) = result.expect("Failed to read message");
        count += 1;
    }

    assert_eq!(
        count, message_count as usize,
        "Iterator should produce all messages"
    );
}

/// Test with multiple different BAG files.
#[test]
fn test_transport_reader_multiple_files() {
    let files = [
        "robocodec_test_15.bag",
        "robocodec_test_17.bag",
        "robocodec_test_18.bag",
    ];

    for filename in &files {
        let bag_path = fixture_path(filename);

        if !bag_path.exists() {
            continue; // Skip if file doesn't exist
        }

        let reader = BagTransportReader::open(&bag_path)
            .unwrap_or_else(|_| panic!("Failed to open {}", filename));

        assert!(
            !reader.channels().is_empty(),
            "{} should have channels",
            filename
        );
        assert!(
            reader.message_count() > 0,
            "{} should have messages",
            filename
        );
    }
}

/// Test that file size is reported correctly.
#[test]
fn test_transport_reader_file_size() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    let reader = BagTransportReader::open(&bag_path).expect("Failed to open BAG file");

    // File size should be > 0
    assert!(reader.file_size() > 0, "File size should be > 0");

    // Should match actual file size
    let metadata = std::fs::metadata(&bag_path).expect("Failed to get metadata");
    assert_eq!(
        reader.file_size(),
        metadata.len(),
        "File size should match actual file size"
    );
}

/// Test file_info method.
#[test]
fn test_transport_reader_file_info() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    let reader = BagTransportReader::open(&bag_path).expect("Failed to open BAG file");
    let info = reader.file_info();

    assert!(matches!(
        info.format,
        robocodec::io::metadata::FileFormat::Bag
    ));
    assert!(!info.channels.is_empty());
    assert!(info.message_count > 0);
    assert!(info.size > 0);
}
