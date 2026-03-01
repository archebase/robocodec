// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for RRD transport-based opening.

use std::collections::HashMap;

use robocodec::io::{FormatReader, RoboReader, formats::rrd::RrdFormat};

/// Get the path to a test fixture.
fn fixture_path(filename: &str) -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.join("tests/fixtures/rrd").join(filename)
}

#[cfg(feature = "remote")]
fn rrd_transport_from_fixture(filename: &str) -> Box<dyn robocodec::io::transport::Transport> {
    use robocodec::io::transport::memory::MemoryTransport;

    let rrd_path = fixture_path(filename);
    let data = std::fs::read(&rrd_path).unwrap_or_else(|_| panic!("Failed to read {:?}", rrd_path));
    Box::new(MemoryTransport::new(data))
}

/// Test that RrdFormat can open from a generic transport source.
#[test]
#[cfg(feature = "remote")]
fn test_rrd_format_open_from_transport() {
    let reader = tokio_test::block_on(RrdFormat::open_from_transport(
        rrd_transport_from_fixture("file1.rrd"),
        "memory://test.rrd".to_string(),
    ))
    .expect("Failed to open RRD via transport");

    // Should have at least one channel
    assert!(
        !reader.channels().is_empty(),
        "Expected at least one channel"
    );

    // Should have messages
    assert!(reader.message_count() > 0, "Expected at least one message");

    // Should report provided logical path
    assert_eq!(reader.path(), "memory://test.rrd");

    // Format should be Rrd
    assert!(matches!(
        reader.format(),
        robocodec::io::metadata::FileFormat::Rrd
    ));
}

/// Test that transport and local open produce equivalent channel metadata.
#[test]
#[cfg(feature = "remote")]
fn test_rrd_format_transport_channels_match_local() {
    let rrd_path = fixture_path("file1.rrd");

    // Open via transport-based reader
    let transport_reader = tokio_test::block_on(RrdFormat::open_from_transport(
        rrd_transport_from_fixture("file1.rrd"),
        "memory://test.rrd".to_string(),
    ))
    .expect("Failed to open with transport");
    let transport_channels: HashMap<_, _> = transport_reader
        .channels()
        .iter()
        .map(|(id, ch)| (*id, ch.clone()))
        .collect();

    // Open via local reader
    let local_reader = RrdFormat::open(&rrd_path).expect("Failed to open local RRD");
    let local_channels: HashMap<_, _> = local_reader
        .channels()
        .iter()
        .map(|(id, ch)| (*id, ch.clone()))
        .collect();

    // Channel counts should match
    assert_eq!(
        transport_channels.len(),
        local_channels.len(),
        "Channel count mismatch"
    );

    // Each channel should match
    for (id, transport_ch) in &transport_channels {
        let local_ch = local_channels
            .get(id)
            .unwrap_or_else(|| panic!("Channel {} not found in local reader", id));

        assert_eq!(
            transport_ch.topic, local_ch.topic,
            "Topic mismatch for channel {}",
            id
        );
    }
}

/// Test that RoboReader routes RRD transport opening to supported readers.
#[test]
#[cfg(feature = "remote")]
fn test_robo_reader_open_from_transport_rrd() {
    let reader = tokio_test::block_on(RoboReader::open_from_transport(
        rrd_transport_from_fixture("file1.rrd"),
        "memory://test.rrd".to_string(),
    ))
    .expect("Failed to open RoboReader from transport");

    assert!(matches!(
        reader.format(),
        robocodec::io::metadata::FileFormat::Rrd
    ));
    assert!(!reader.channels().is_empty());
    assert!(reader.message_count() > 0);
}
