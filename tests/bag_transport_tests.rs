// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for BAG transport-based opening.

use std::collections::HashMap;

use robocodec::io::{FormatReader, RoboReader, formats::bag::BagFormat};

/// Get the path to a test fixture.
fn fixture_path(filename: &str) -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests/fixtures").join(filename)
}

#[cfg(feature = "remote")]
fn bag_transport_from_fixture(filename: &str) -> Box<dyn robocodec::io::transport::Transport> {
    use robocodec::io::transport::memory::MemoryTransport;

    let bag_path = fixture_path(filename);
    let data = std::fs::read(&bag_path).unwrap_or_else(|_| panic!("Failed to read {:?}", bag_path));
    Box::new(MemoryTransport::new(data))
}

/// Test that BagFormat can open from a generic transport source.
#[test]
#[cfg(feature = "remote")]
fn test_bag_format_open_from_transport() {
    let transport = bag_transport_from_fixture("robocodec_test_15.bag");
    let reader = tokio_test::block_on(BagFormat::open_from_transport(
        transport,
        "memory://test.bag".to_string(),
    ))
    .expect("Failed to open BAG via transport");

    // Should have at least one channel
    assert!(
        !reader.channels().is_empty(),
        "Expected at least one channel"
    );

    // Should have messages
    assert!(reader.message_count() > 0, "Expected at least one message");

    // Should report provided logical path
    assert_eq!(reader.path(), "memory://test.bag");

    // Format should be Bag
    assert!(matches!(
        reader.format(),
        robocodec::io::metadata::FileFormat::Bag
    ));
}

/// Test that transport and local open produce equivalent channel metadata.
#[test]
#[cfg(feature = "remote")]
fn test_bag_format_transport_channels_match_local() {
    let bag_path = fixture_path("robocodec_test_15.bag");

    // Open via transport-based reader
    let transport_reader = tokio_test::block_on(BagFormat::open_from_transport(
        bag_transport_from_fixture("robocodec_test_15.bag"),
        "memory://test.bag".to_string(),
    ))
    .expect("Failed to open with transport");
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

/// Test that RoboReader routes BAG transport opening to supported readers.
#[test]
#[cfg(feature = "remote")]
fn test_robo_reader_open_from_transport_bag() {
    let reader = tokio_test::block_on(RoboReader::open_from_transport(
        bag_transport_from_fixture("robocodec_test_15.bag"),
        "memory://test.bag".to_string(),
    ))
    .expect("Failed to open RoboReader from transport");

    assert!(matches!(
        reader.format(),
        robocodec::io::metadata::FileFormat::Bag
    ));
    assert!(!reader.channels().is_empty());
    assert!(reader.message_count() > 0);
}
