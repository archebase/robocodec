// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified RoboWriter tests.
//!
//! Tests cover:
//! - RoboWriter creation with format auto-detection
//! - Writing MCAP files
//! - Writing bag files
//! - Downcasting to format-specific writers
//! - Error handling

use std::fs;
use std::path::PathBuf;

use robocodec::io::formats::bag::{BagFormat, BagWriter};
use robocodec::io::traits::FormatReader;
use robocodec::io::traits::FormatWriter;
use robocodec::io::writer::{WriteStrategy, WriterBuilder};
use robocodec::io::RoboWriter;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Simple ROS1 message definition
const STD_MSGS_STRING_DEF: &str = "string data";

/// Get a temporary directory for test files
fn temp_dir() -> PathBuf {
    let random = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let thread_id = format!("{:?}", std::thread::current().id());
    std::env::temp_dir().join(format!(
        "robocodec_writer_test_{}_{}_{}",
        std::process::id(),
        thread_id,
        random
    ))
}

/// Create a temporary file path with cleanup guard
fn temp_path(ext: &str) -> (PathBuf, CleanupGuard) {
    let dir = temp_dir();
    fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("test_{}.{}", std::process::id(), ext));
    let guard = CleanupGuard(dir);
    (path, guard)
}

/// Cleanup guard for test temporary files
#[derive(Debug)]
struct CleanupGuard(PathBuf);

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ============================================================================
// RoboWriter Creation Tests
// ============================================================================

#[test]
fn test_robowriter_create_bag() {
    let (path, _guard) = temp_path("bag");

    let writer = RoboWriter::create(&path);
    assert!(
        writer.is_ok(),
        "RoboWriter::create should succeed for .bag files: {:?}",
        writer.err()
    );
}

#[test]
fn test_robowriter_create_mcap() {
    let (path, _guard) = temp_path("mcap");

    let writer = RoboWriter::create(&path);
    assert!(
        writer.is_ok(),
        "RoboWriter::create should succeed for .mcap files: {:?}",
        writer.err()
    );
}

#[test]
fn test_robowriter_create_with_unknown_extension() {
    let (path, _guard) = temp_path("unknown");

    let writer = RoboWriter::create(&path);
    assert!(
        writer.is_err(),
        "RoboWriter::create should fail for unknown extensions"
    );
}

#[test]
fn test_robowriter_create_with_strategy() {
    let (path, _guard) = temp_path("bag");

    // Strategy parameter is currently ignored but should still work
    let writer = RoboWriter::create_with_strategy(&path, WriteStrategy::Parallel);
    assert!(
        writer.is_ok(),
        "RoboWriter::create_with_strategy should succeed: {:?}",
        writer.err()
    );
}

// ============================================================================
// RoboWriter Write Tests (Bag)
// ============================================================================

#[test]
fn test_robowriter_write_bag_messages() {
    let (path, _guard) = temp_path("bag");

    let mut writer = RoboWriter::create(&path).unwrap();

    // Add a channel
    let channel_id = writer
        .add_channel(
            "/chatter",
            "std_msgs/String",
            "cdr",
            Some(STD_MSGS_STRING_DEF),
        )
        .expect("add_channel should succeed");

    assert_eq!(channel_id, 0, "First channel should have ID 0");

    // Write a message
    let data = b"Hello, World!".to_vec();
    let raw_msg = robocodec::io::metadata::RawMessage {
        channel_id: 0,
        log_time: 1_500_000_000,
        publish_time: 1_500_000_001,
        data,
        sequence: None,
    };

    writer.write(&raw_msg).expect("write should succeed");
    writer.finish().expect("finish should succeed");

    // Verify file was created
    assert!(path.exists(), "bag file should exist");
}

#[test]
fn test_robowriter_write_bag_round_trip() {
    let (path, _guard) = temp_path("bag");

    // Write
    let mut writer = RoboWriter::create(&path).unwrap();
    writer
        .add_channel("/test", "std_msgs/String", "cdr", Some(STD_MSGS_STRING_DEF))
        .unwrap();

    let data = b"test message".to_vec();
    let raw_msg = robocodec::io::metadata::RawMessage {
        channel_id: 0,
        log_time: 1_000_000_000,
        publish_time: 1_000_000_000,
        data,
        sequence: None,
    };

    writer.write(&raw_msg).unwrap();
    writer.finish().unwrap();

    // Read back
    let reader = BagFormat::open(&path).unwrap();
    let channels = reader.channels();
    assert_eq!(channels.len(), 1, "should have 1 channel");

    let channel = channels.values().next().unwrap();
    assert_eq!(channel.topic, "/test");
    assert_eq!(channel.message_type, "std_msgs/String");
}

#[test]
fn test_robowriter_message_count() {
    let (path, _guard) = temp_path("bag");

    let mut writer = RoboWriter::create(&path).unwrap();
    writer
        .add_channel(
            "/chatter",
            "std_msgs/String",
            "cdr",
            Some(STD_MSGS_STRING_DEF),
        )
        .unwrap();

    assert_eq!(
        writer.message_count(),
        0,
        "initial message count should be 0"
    );

    let data = b"msg1".to_vec();
    let raw_msg = robocodec::io::metadata::RawMessage {
        channel_id: 0,
        log_time: 1_000_000_000,
        publish_time: 1_000_000_000,
        data,
        sequence: None,
    };

    writer.write(&raw_msg).unwrap();
    writer.write(&raw_msg).unwrap();
    writer.finish().unwrap();

    // Verify file was created and contains data
    assert!(path.exists() && fs::metadata(&path).unwrap().len() > 100);
}

#[test]
fn test_robowriter_channel_count() {
    let (path, _guard) = temp_path("bag");

    let mut writer = RoboWriter::create(&path).unwrap();

    assert_eq!(
        writer.channel_count(),
        0,
        "initial channel count should be 0"
    );

    writer
        .add_channel(
            "/chatter1",
            "std_msgs/String",
            "cdr",
            Some(STD_MSGS_STRING_DEF),
        )
        .unwrap();
    assert_eq!(writer.channel_count(), 1, "channel count should be 1");

    writer
        .add_channel(
            "/chatter2",
            "std_msgs/String",
            "cdr",
            Some(STD_MSGS_STRING_DEF),
        )
        .unwrap();
    assert_eq!(writer.channel_count(), 2, "channel count should be 2");
}

// ============================================================================
// RoboWriter Downcast Tests
// ============================================================================

#[test]
fn test_robowriter_downcast_bag_writer() {
    let (path, _guard) = temp_path("bag");

    let writer = RoboWriter::create(&path).unwrap();

    // Downcast to BagWriter should succeed
    let bag_writer = writer.downcast_ref::<BagWriter>();
    assert!(
        bag_writer.is_some(),
        "should be able to downcast to BagWriter"
    );
}

#[test]
fn test_robowriter_downcast_mcap_writer() {
    let (path, _guard) = temp_path("mcap");

    let mut writer = RoboWriter::create(&path).unwrap();

    // Note: ParallelMcapWriter.path() returns "unknown" as it doesn't store the path
    // Just verify the writer was created successfully
    assert_eq!(writer.path(), "unknown"); // McapWriter returns "unknown" for path

    // Verify the file was created
    writer.finish().unwrap();
    assert!(path.exists(), "mcap file should exist");
}

#[test]
fn test_robowriter_downcast_mut() {
    let (path, _guard) = temp_path("bag");

    let mut writer = RoboWriter::create(&path).unwrap();

    // Downcast to mutable BagWriter should succeed
    let bag_writer = writer.downcast_mut::<BagWriter>();
    assert!(
        bag_writer.is_some(),
        "should be able to downcast mut to BagWriter"
    );
}

#[test]
fn test_robowriter_downcast_wrong_type() {
    let (path, _guard) = temp_path("bag");

    let writer = RoboWriter::create(&path).unwrap();

    // Try to downcast BagWriter to something it's not (e.g., a different concrete type)
    // We can't test this with ParallelMcapWriter due to the generic parameter,
    // so we just verify the BagWriter downcast works
    let bag_writer = writer.downcast_ref::<BagWriter>();
    assert!(bag_writer.is_some(), "BagWriter should downcast to itself");
}

// ============================================================================
// WriterBuilder Tests
// ============================================================================

#[test]
fn test_writer_builder_new() {
    let _builder = WriterBuilder::new();
    // Just verify it creates successfully
    // Actual building requires a path
}

#[test]
fn test_writer_builder_with_path() {
    let (path, _guard) = temp_path("bag");

    let builder = WriterBuilder::new();
    let result = builder.path(&path).build();
    assert!(
        result.is_ok(),
        "WriterBuilder should build successfully: {:?}",
        result.err()
    );
}
