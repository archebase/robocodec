// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag writer tests.
//!
//! This file contains unit and integration tests for the bag_writer module.
//! Tests cover:
//! - BagMessage creation
//! - BagWriter file creation
//! - Adding connections
//! - Writing messages
//! - Chunking behavior
//! - Round-trip verification (write and read back)
//! - Error handling

use std::fs;
use std::path::PathBuf;

use robocodec::io::formats::bag::{BagFormat, BagMessage, BagWriter};
use robocodec::io::traits::FormatReader;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Simple ROS1 message definition for std_msgs/String
const STD_MSGS_STRING_DEF: &str = "string data";

/// Simple ROS1 message definition for std_msgs/Int32
const STD_MSGS_INT32_DEF: &str = "int32 data";

/// Simple ROS1 message definition for sensor_msgs/Image
const SENSOR_MSGS_IMAGE_DEF: &str = r#"
std_msgs/Header header
  uint32 seq
  time stamp
  string frame_id
uint32 height
uint32 width
string encoding
uint8 is_bigendian
uint32 step
uint8[] data
"#;

/// Get a temporary directory for test files
fn temp_dir() -> PathBuf {
    // Use a combination of process ID and a random element to avoid collisions
    // when tests run in parallel
    let random = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    std::env::temp_dir().join(format!(
        "robocodec_bag_writer_test_{}_{}",
        std::process::id(),
        random
    ))
}

/// Create a temporary bag file path and a cleanup guard for the directory.
/// The guard ensures the temporary directory is removed when the test completes.
fn temp_bag_path(name: &str) -> (PathBuf, CleanupGuard) {
    let dir = temp_dir();
    fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("{}.bag", name));
    let guard = CleanupGuard(dir);
    (path, guard)
}

/// Cleanup guard for test temporary files.
/// Stores the actual path to ensure cleanup targets the correct directory.
#[derive(Debug)]
struct CleanupGuard(PathBuf);

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ============================================================================
// BagMessage Unit Tests
// ============================================================================

#[test]
fn test_bag_message_new() {
    let conn_id = 1;
    let time_ns = 1_234_567_890;
    let data = vec![1, 2, 3, 4];

    let msg = BagMessage::new(conn_id, time_ns, data.clone());

    assert_eq!(msg.conn_id, conn_id, "connection ID should match");
    assert_eq!(msg.time_ns, time_ns, "timestamp should match");
    assert_eq!(msg.data, data, "data should match");
}

#[test]
fn test_bag_message_from_raw() {
    let conn_id = 5;
    let time_ns = 9_876_543_210;
    let data = vec![10, 20, 30, 40, 50];

    let msg = BagMessage::from_raw(conn_id, time_ns, data.clone());

    assert_eq!(msg.conn_id, conn_id);
    assert_eq!(msg.time_ns, time_ns);
    assert_eq!(msg.data, data);
}

#[test]
fn test_bag_message_clone() {
    let msg = BagMessage::new(1, 1000, vec![1, 2, 3]);
    let cloned = msg.clone();

    assert_eq!(msg.conn_id, cloned.conn_id);
    assert_eq!(msg.time_ns, cloned.time_ns);
    assert_eq!(msg.data, cloned.data);
}

// ============================================================================
// BagWriter Creation Tests
// ============================================================================

#[test]
fn test_writer_creates_file() {
    let (path, _guard) = temp_bag_path("test_creates_file");

    let result = BagWriter::create(&path);

    assert!(
        result.is_ok(),
        "BagWriter::create should succeed: {:?}",
        result.err()
    );

    let writer = result.unwrap();
    writer.finish().ok();

    assert!(path.exists(), "bag file should be created at {:?}", path);
}

#[test]
fn test_writer_creates_valid_version_header() {
    let (path, _guard) = temp_bag_path("test_version_header");

    let writer = BagWriter::create(&path).unwrap();
    writer.finish().unwrap();

    let contents = fs::read(&path).unwrap();

    // File should start with ROSBAG version line
    let version_line = "#ROSBAG V2.0\n";
    assert!(
        contents.starts_with(version_line.as_bytes()),
        "bag file should start with ROSBAG version line"
    );
}

#[test]
fn test_writer_file_header_is_4096_bytes() {
    let (path, _guard) = temp_bag_path("test_header_size");

    let writer = BagWriter::create(&path).unwrap();
    writer.finish().unwrap();

    let contents = fs::read(&path).unwrap();

    assert_eq!(contents.len(), 4096, "empty bag file should be 4096 bytes");
}

// ============================================================================
// Connection Tests
// ============================================================================

#[test]
fn test_add_single_connection() {
    let (path, _guard) = temp_bag_path("test_add_connection");

    let mut writer = BagWriter::create(&path).unwrap();
    let result = writer.add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF);

    assert!(
        result.is_ok(),
        "add_connection should succeed: {:?}",
        result.err()
    );

    writer.finish().unwrap();
}

#[test]
fn test_add_multiple_connections() {
    let (path, _guard) = temp_bag_path("test_multiple_connections");

    let mut writer = BagWriter::create(&path).unwrap();

    assert!(writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .is_ok());
    assert!(writer
        .add_connection(1, "/numbers", "std_msgs/Int32", STD_MSGS_INT32_DEF)
        .is_ok());
    assert!(writer
        .add_connection(2, "/camera", "sensor_msgs/Image", SENSOR_MSGS_IMAGE_DEF)
        .is_ok());

    writer.finish().unwrap();
}

// ============================================================================
// Message Writing Tests
// ============================================================================

#[test]
fn test_write_single_message() {
    let (path, _guard) = temp_bag_path("test_write_single");

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    let msg = BagMessage::new(0, 1_000_000_000, vec![1, 2, 3, 4]);
    let result = writer.write_message(&msg);

    assert!(
        result.is_ok(),
        "write_message should succeed: {:?}",
        result.err()
    );

    writer.finish().unwrap();
}

#[test]
fn test_write_multiple_messages_same_connection() {
    let (path, _guard) = temp_bag_path("test_multiple_messages");

    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    for i in 0..10 {
        let msg = BagMessage::new(0, i * 1_000_000_000, vec![i as u8; 4]);
        assert!(writer.write_message(&msg).is_ok());
    }

    writer.finish().unwrap();

    // Verify messages were written
    let reader = BagFormat::open(&path).unwrap();
    assert_eq!(reader.channels().len(), 1);
}

// ============================================================================
// Round-Trip Integration Tests
// ============================================================================

#[test]
fn test_round_trip_single_message() {
    let (path, _guard) = temp_bag_path("test_round_trip_single");

    // Write a message
    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
    let msg = BagMessage::new(0, 1_500_000_000, data);
    writer.write_message(&msg).unwrap();
    writer.finish().unwrap();

    // Read it back
    let reader = BagFormat::open(&path).unwrap();
    let channels = reader.channels();

    assert_eq!(channels.len(), 1, "should have 1 channel");

    let channel = channels.values().next().unwrap();
    assert_eq!(channel.topic, "/chatter");
    assert_eq!(channel.message_type, "std_msgs/String");
}

#[test]
fn test_round_trip_message_data_preserved() {
    let (path, _guard) = temp_bag_path("test_round_trip_data");

    // Create test data with known byte patterns
    let test_data_1 = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
    let test_data_2 = vec![0xAA, 0xBB, 0xCC, 0xDD];
    let test_data_3 = vec![0xFF, 0xFE, 0xFD, 0xFC, 0xFB];

    // Write messages with known data
    let mut writer = BagWriter::create(&path).unwrap();
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    writer
        .write_message(&BagMessage::new(0, 1_000_000_000, test_data_1.clone()))
        .unwrap();
    writer
        .write_message(&BagMessage::new(0, 2_000_000_000, test_data_2.clone()))
        .unwrap();
    writer
        .write_message(&BagMessage::new(0, 3_000_000_000, test_data_3.clone()))
        .unwrap();

    writer.finish().unwrap();

    // Verify file was created
    assert!(path.exists(), "bag file should exist");

    // Read back and verify message data is preserved
    let reader = BagFormat::open(&path).unwrap();
    let raw_iter = reader.iter_raw().unwrap();

    // Collect all messages
    let mut messages: Vec<(u64, Vec<u8>)> = Vec::new();
    for result in raw_iter {
        match result {
            Ok((raw_msg, _channel)) => {
                // raw_msg.data contains the message payload directly
                messages.push((raw_msg.log_time, raw_msg.data.clone()));
            }
            Err(e) => {
                panic!("Error reading message: {}", e);
            }
        }
    }

    // Verify we got 3 messages
    assert_eq!(messages.len(), 3, "should have 3 messages");

    // Verify timestamps match (in nanoseconds)
    assert_eq!(messages[0].0, 1_000_000_000);
    assert_eq!(messages[1].0, 2_000_000_000);
    assert_eq!(messages[2].0, 3_000_000_000);

    // Verify message data matches
    assert_eq!(
        messages[0].1, test_data_1,
        "first message data should match"
    );
    assert_eq!(
        messages[1].1, test_data_2,
        "second message data should match"
    );
    assert_eq!(
        messages[2].1, test_data_3,
        "third message data should match"
    );

    // Clean up
    let _ = fs::remove_file(&path);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_write_message_with_invalid_connection_id() {
    let (path, _guard) = temp_bag_path("test_invalid_conn_id");

    let mut writer = BagWriter::create(&path).unwrap();
    // Only add connection 0
    writer
        .add_connection(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF)
        .unwrap();

    // Try to write with connection ID 5 (doesn't exist)
    let msg = BagMessage::new(5, 1_000_000_000, vec![1, 2, 3]);
    let result = writer.write_message(&msg);

    assert!(
        result.is_err(),
        "writing with invalid connection ID should fail"
    );

    // Finish should still work (the failed write didn't corrupt state)
    writer.finish().ok();
}
