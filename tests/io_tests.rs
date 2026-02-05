// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Tests for the unified I/O layer.
//!
//! These tests verify the public API only (RoboReader, RoboWriter, config types).

use robocodec::io::metadata::{ChannelInfo, RawMessage};
use robocodec::io::{ReaderConfig, RoboReader};

#[test]
fn test_reader_config_default() {
    let config = ReaderConfig::default();
    assert!(config.prefer_parallel);
    assert!(config.chunk_merge_enabled);
    assert!(config.num_threads.is_none());
}

#[test]
fn test_reader_config_sequential() {
    let config = ReaderConfig::sequential();
    assert!(!config.prefer_parallel);
}

#[test]
fn test_reader_config_parallel() {
    let config = ReaderConfig::parallel();
    assert!(config.prefer_parallel);
}

#[test]
fn test_reader_config_builder() {
    let config = ReaderConfig::builder()
        .prefer_parallel(false)
        .num_threads(4)
        .chunk_merge_enabled(false)
        .build();

    assert!(!config.prefer_parallel);
    assert_eq!(config.num_threads, Some(4));
    assert!(!config.chunk_merge_enabled);
}

#[test]
fn test_channel_info_builder() {
    let info = ChannelInfo::new(1, "/test", "std_msgs/String")
        .with_encoding("json")
        .with_schema("string data")
        .with_message_count(100);

    assert_eq!(info.id, 1);
    assert_eq!(info.topic, "/test");
    assert_eq!(info.message_type, "std_msgs/String");
    assert_eq!(info.encoding, "json");
    assert_eq!(info.schema, Some("string data".to_string()));
    assert_eq!(info.message_count, 100);
}

#[test]
fn test_raw_message() {
    let msg = RawMessage::new(1, 1000, 900, b"test data".to_vec()).with_sequence(5);

    assert_eq!(msg.channel_id, 1);
    assert_eq!(msg.log_time, 1000);
    assert_eq!(msg.publish_time, 900);
    assert_eq!(msg.data, b"test data");
    assert_eq!(msg.sequence, Some(5));
    assert_eq!(msg.len(), 9);
}

#[test]
fn test_robo_reader_auto_config() {
    let result = RoboReader::open_with_config(
        "/tmp/claude/nonexistent_file_xYz123.mcap",
        ReaderConfig::default(),
    );
    assert!(result.is_err());
}
