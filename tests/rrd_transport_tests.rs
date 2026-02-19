// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for RRD transport reader.
//!
//! These tests verify that `RrdTransportReader` produces correct results
//! compared to the parallel reader.

use std::collections::HashMap;

use robocodec::io::{
    FormatReader,
    formats::rrd::{RrdFormat, RrdTransportReader},
};

/// Get the path to a test fixture.
fn fixture_path(filename: &str) -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests/fixtures/rrd").join(filename)
}

/// Test that RrdTransportReader can open a local RRD file.
#[test]
fn test_transport_reader_open_local() {
    let rrd_path = fixture_path("file1.rrd");

    let reader = RrdTransportReader::open(&rrd_path).expect("Failed to open RRD file");

    // Should have at least one channel
    assert!(
        !reader.channels().is_empty(),
        "Expected at least one channel"
    );

    // Should have messages
    assert!(reader.message_count() > 0, "Expected at least one message");

    // Path should match
    assert_eq!(reader.path(), rrd_path.to_string_lossy().as_ref());

    // Format should be Rrd
    assert!(matches!(
        reader.format(),
        robocodec::io::metadata::FileFormat::Rrd
    ));
}

/// Test that RrdTransportReader produces the same channel info as RrdFormat.
#[test]
fn test_transport_reader_channels_match_parallel() {
    let rrd_path = fixture_path("file1.rrd");

    // Open via transport reader
    let transport_reader =
        RrdTransportReader::open(&rrd_path).expect("Failed to open with transport");
    let transport_channels: HashMap<_, _> = transport_reader
        .channels()
        .iter()
        .map(|(id, ch)| (*id, ch.clone()))
        .collect();

    // Open via parallel reader
    let parallel_reader = RrdFormat::open(&rrd_path).expect("Failed to open with parallel");
    let parallel_channels: HashMap<_, _> = parallel_reader
        .channels()
        .iter()
        .map(|(id, ch)| (*id, ch.clone()))
        .collect();

    // Channel counts should match
    assert_eq!(
        transport_channels.len(),
        parallel_channels.len(),
        "Channel count mismatch"
    );

    // Each channel should match
    for (id, transport_ch) in &transport_channels {
        let parallel_ch = parallel_channels
            .get(id)
            .unwrap_or_else(|| panic!("Channel {} not found in parallel reader", id));

        assert_eq!(
            transport_ch.topic, parallel_ch.topic,
            "Topic mismatch for channel {}",
            id
        );
    }
}

/// Test that RrdTransportReader produces valid message counts.
#[test]
fn test_transport_reader_message_count_valid() {
    let rrd_path = fixture_path("file1.rrd");

    let transport_reader =
        RrdTransportReader::open(&rrd_path).expect("Failed to open with transport");

    assert!(
        transport_reader.message_count() > 0,
        "Transport reader should have messages"
    );
}

/// Test that timestamps are valid.
#[test]
fn test_transport_reader_timestamps_valid() {
    let rrd_path = fixture_path("file1.rrd");

    let reader = RrdTransportReader::open(&rrd_path).expect("Failed to open RRD file");

    // Should have valid start and end indices
    let start_idx = reader.start_time();
    let end_idx = reader.end_time();

    // Both should be present
    assert!(start_idx.is_some(), "Should have start index");
    assert!(end_idx.is_some(), "Should have end index");

    // End index should be >= start index
    assert!(
        end_idx.unwrap() >= start_idx.unwrap(),
        "End index should be >= start index"
    );
}

/// Test iter_raw_boxed produces messages.
#[test]
fn test_transport_reader_iter_raw() {
    let rrd_path = fixture_path("file1.rrd");

    let reader = RrdTransportReader::open(&rrd_path).expect("Failed to open RRD file");
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

/// Test with multiple different RRD files.
#[test]
fn test_transport_reader_multiple_files() {
    let files = ["file1.rrd", "file2.rrd", "file3.rrd"];

    for filename in &files {
        let rrd_path = fixture_path(filename);

        if !rrd_path.exists() {
            continue; // Skip if file doesn't exist
        }

        let reader = RrdTransportReader::open(&rrd_path)
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
    let rrd_path = fixture_path("file1.rrd");

    let reader = RrdTransportReader::open(&rrd_path).expect("Failed to open RRD file");

    // File size should be > 0
    assert!(reader.file_size() > 0, "File size should be > 0");

    // Should match actual file size
    let metadata = std::fs::metadata(&rrd_path).expect("Failed to get metadata");
    assert_eq!(
        reader.file_size(),
        metadata.len(),
        "File size should match actual file size"
    );
}

/// Test file_info method.
#[test]
fn test_transport_reader_file_info() {
    let rrd_path = fixture_path("file1.rrd");

    let reader = RrdTransportReader::open(&rrd_path).expect("Failed to open RRD file");
    let info = reader.file_info();

    assert!(matches!(
        info.format,
        robocodec::io::metadata::FileFormat::Rrd
    ));
    assert!(!info.channels.is_empty());
    assert!(info.message_count > 0);
    assert!(info.size > 0);
}
