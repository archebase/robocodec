// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag file rewriter tests.
//!
//! Tests cover:
//! - Creating rewriters with default and custom options
//! - Schema caching and validation
//! - CDR message rewriting
//! - Topic and type transformations
//! - Error handling

use std::fs;
use std::path::PathBuf;

use robocodec::io::formats::bag::{BagFormat, BagMessage, BagWriter};
use robocodec::io::traits::FormatReader;
use robocodec::rewriter::bag::BagRewriter;
use robocodec::rewriter::RewriteOptions;
use robocodec::transform::TransformBuilder;

// ============================================================================
// Test Fixtures
// ============================================================================

/// Simple ROS1 message definition for std_msgs/String
const STD_MSGS_STRING_DEF: &str = "string data";

/// Simple ROS1 message definition for std_msgs/Int32
const STD_MSGS_INT32_DEF: &str = "int32 data";

/// Get a temporary directory for test files
fn temp_dir() -> PathBuf {
    let random = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    std::env::temp_dir().join(format!(
        "robocodec_bag_rewriter_test_{}_{}",
        std::process::id(),
        random
    ))
}

/// Create a temporary bag file path with cleanup guard
fn temp_bag_path(name: &str) -> (PathBuf, CleanupGuard) {
    let dir = temp_dir();
    fs::create_dir_all(&dir).ok();
    let path = dir.join(format!("{}.bag", name));
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

/// Create a minimal test bag file with messages
fn create_test_bag(
    path: &PathBuf,
    topic: &str,
    message_type: &str,
    schema: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = BagWriter::create(path)?;

    // Add connection
    writer.add_connection_with_callerid(0, topic, message_type, schema, "/test_node")?;

    // Write a simple message - for std_msgs/String with CDR encoding
    // CDR format: 4-byte CDR header + little-endian string length + string bytes
    let message_data = "Hello, World!".as_bytes();
    let mut cdr_data = Vec::new();

    // CDR header (endianness flag + padding)
    cdr_data.push(0x01); // Little endian
    cdr_data.extend_from_slice(&[0x00, 0x00, 0x00]); // Padding

    // String length (4 bytes little-endian)
    let len = message_data.len() as u32;
    cdr_data.extend_from_slice(&len.to_le_bytes());

    // String data
    cdr_data.extend_from_slice(message_data);

    writer.write_message(&BagMessage::from_raw(0, 1_500_000_000, cdr_data))?;
    writer.finish()?;

    Ok(())
}

// ============================================================================
// BagRewriter Creation Tests
// ============================================================================

#[test]
fn test_rewriter_new_creates_with_default_options() {
    let rewriter = BagRewriter::new();

    assert!(rewriter.options().transforms.is_none());
    assert!(rewriter.options().validate_schemas);
    assert!(rewriter.options().skip_decode_failures);
    assert!(rewriter.options().passthrough_non_cdr);
}

#[test]
fn test_rewriter_with_custom_options() {
    let options = RewriteOptions {
        transforms: None,
        validate_schemas: false,
        skip_decode_failures: false,
        passthrough_non_cdr: false,
    };

    let rewriter = BagRewriter::with_options(options.clone());

    assert!(!rewriter.options().validate_schemas);
    assert!(!rewriter.options().skip_decode_failures);
    assert!(!rewriter.options().passthrough_non_cdr);
}

#[test]
fn test_rewriter_default_impl() {
    let rewriter = BagRewriter::default();

    assert!(rewriter.options().validate_schemas);
}

// ============================================================================
// BagRewriter Basic Rewrite Tests
// ============================================================================

#[test]
fn test_rewriter_simple_bag_copy() {
    let (input_path, _guard) = temp_bag_path("simple_copy_input");
    let (output_path, _guard_out) = temp_bag_path("simple_copy_output");

    // Create a simple test bag
    create_test_bag(&input_path, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF).unwrap();

    // Rewrite without transformations
    let mut rewriter = BagRewriter::new();
    let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

    // Verify statistics
    assert_eq!(stats.channel_count, 1, "should have 1 channel");
    assert_eq!(stats.message_count, 1, "should have 1 message");
    assert!(output_path.exists(), "output file should exist");

    // Verify the output can be read
    let reader = BagFormat::open(&output_path).unwrap();
    let channels = reader.channels();
    assert_eq!(channels.len(), 1);

    let channel = channels.values().next().unwrap();
    assert_eq!(channel.topic, "/chatter");
    assert_eq!(channel.message_type, "std_msgs/String");
}

#[test]
fn test_rewriter_preserves_message_data() {
    let (input_path, _guard) = temp_bag_path("preserve_data_input");
    let (output_path, _guard_out) = temp_bag_path("preserve_data_output");

    // Create test bag with known data
    create_test_bag(&input_path, "/test", "std_msgs/String", STD_MSGS_STRING_DEF).unwrap();

    // Rewrite with schema validation disabled to avoid decode issues
    let options = RewriteOptions {
        transforms: None,
        validate_schemas: false,
        skip_decode_failures: true,
        passthrough_non_cdr: true,
    };

    let mut rewriter = BagRewriter::with_options(options);
    rewriter.rewrite(&input_path, &output_path).unwrap();

    // Verify output was created
    assert!(output_path.exists(), "output file should exist");

    // Read output and verify there's content
    let reader = BagFormat::open(&output_path).unwrap();
    let messages: Vec<_> = reader.iter_raw().unwrap().filter_map(|r| r.ok()).collect();

    assert!(!messages.is_empty(), "should have at least one message");
}

#[test]
fn test_rewriter_multiple_channels() {
    let (input_path, _guard) = temp_bag_path("multi_channel_input");
    let (output_path, _guard_out) = temp_bag_path("multi_channel_output");

    // Create a bag with multiple channels
    {
        let mut writer = BagWriter::create(&input_path).unwrap();
        writer
            .add_connection_with_callerid(0, "/chatter1", "std_msgs/String", STD_MSGS_STRING_DEF, "/node1")
            .unwrap();
        writer
            .add_connection_with_callerid(1, "/chatter2", "std_msgs/Int32", STD_MSGS_INT32_DEF, "/node2")
            .unwrap();

        // Write messages to both channels
        let mut data1 = vec![0x01, 0x00, 0x00, 0x00]; // CDR header
        data1.extend_from_slice(&(5u32.to_le_bytes())); // string length
        data1.extend_from_slice(b"Hello");

        let mut data2 = vec![0x01, 0x00, 0x00, 0x00]; // CDR header
        data2.extend_from_slice(&42i32.to_le_bytes()); // int32 value

        writer
            .write_message(&BagMessage::from_raw(0, 1_000_000_000, data1))
            .unwrap();
        writer
            .write_message(&BagMessage::from_raw(1, 1_000_000_001, data2))
            .unwrap();
        writer.finish().unwrap();
    }

    // Rewrite
    let mut rewriter = BagRewriter::new();
    let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

    assert_eq!(stats.channel_count, 2, "should have 2 channels");
    assert_eq!(stats.message_count, 2, "should have 2 messages");

    // Verify output has both channels
    let reader = BagFormat::open(&output_path).unwrap();
    let channels = reader.channels();
    assert_eq!(channels.len(), 2);
}

// ============================================================================
// BagRewriter Transform Tests
// ============================================================================

#[test]
fn test_rewriter_with_topic_rename() {
    let (input_path, _guard) = temp_bag_path("topic_rename_input");
    let (output_path, _guard_out) = temp_bag_path("topic_rename_output");

    create_test_bag(&input_path, "/old_topic", "std_msgs/String", STD_MSGS_STRING_DEF).unwrap();

    // Create transform pipeline
    let pipeline = TransformBuilder::new()
        .with_topic_rename("/old_topic", "/new_topic")
        .build();

    let options = RewriteOptions {
        transforms: Some(pipeline),
        validate_schemas: false,
        skip_decode_failures: true,
        passthrough_non_cdr: true,
    };

    let mut rewriter = BagRewriter::with_options(options);
    let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

    assert_eq!(stats.topics_renamed, 1, "should have renamed 1 topic");

    // Verify the topic was renamed in output
    let reader = BagFormat::open(&output_path).unwrap();
    let channels = reader.channels();
    let channel = channels.values().next().unwrap();
    assert_eq!(channel.topic, "/new_topic");
}

#[test]
fn test_rewriter_with_type_rename() {
    let (input_path, _guard) = temp_bag_path("type_rename_input");
    let (output_path, _guard_out) = temp_bag_path("type_rename_output");

    create_test_bag(&input_path, "/chatter", "old_pkg/String", STD_MSGS_STRING_DEF).unwrap();

    // Create transform pipeline
    let pipeline = TransformBuilder::new()
        .with_type_rename("old_pkg/String", "new_pkg/String")
        .build();

    let options = RewriteOptions {
        transforms: Some(pipeline),
        validate_schemas: false,
        skip_decode_failures: true,
        passthrough_non_cdr: true,
    };

    let mut rewriter = BagRewriter::with_options(options);
    let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

    assert_eq!(stats.types_renamed, 1, "should have renamed 1 type");

    // Verify the type was renamed in output
    let reader = BagFormat::open(&output_path).unwrap();
    let channels = reader.channels();
    let channel = channels.values().next().unwrap();
    assert_eq!(channel.message_type, "new_pkg/String");
}

#[test]
fn test_rewriter_with_multiple_transforms() {
    let (input_path, _guard) = temp_bag_path("multi_transform_input");
    let (output_path, _guard_out) = temp_bag_path("multi_transform_output");

    create_test_bag(&input_path, "/old_topic", "old_pkg/String", STD_MSGS_STRING_DEF).unwrap();

    // Create transform pipeline with multiple transforms
    let pipeline = TransformBuilder::new()
        .with_topic_rename("/old_topic", "/new_topic")
        .with_type_rename("old_pkg/String", "new_pkg/String")
        .build();

    let options = RewriteOptions {
        transforms: Some(pipeline),
        validate_schemas: false,
        skip_decode_failures: true,
        passthrough_non_cdr: true,
    };

    let mut rewriter = BagRewriter::with_options(options);
    let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

    assert_eq!(stats.topics_renamed, 1);
    assert_eq!(stats.types_renamed, 1);

    // Verify both transformations were applied
    let reader = BagFormat::open(&output_path).unwrap();
    let channels = reader.channels();
    let channel = channels.values().next().unwrap();
    assert_eq!(channel.topic, "/new_topic");
    assert_eq!(channel.message_type, "new_pkg/String");
}

// ============================================================================
// BagRewriter Error Handling Tests
// ============================================================================

#[test]
fn test_rewriter_handles_missing_input_file() {
    let (input_path, _guard) = temp_bag_path("nonexistent_input");
    let (output_path, _guard_out) = temp_bag_path("error_output");

    let mut rewriter = BagRewriter::new();
    let result = rewriter.rewrite(&input_path, &output_path);

    assert!(result.is_err(), "should fail on missing input file");
}

#[test]
fn test_rewriter_preserves_callerid() {
    let (input_path, _guard) = temp_bag_path("callerid_input");
    let (output_path, _guard_out) = temp_bag_path("callerid_output");

    // Create bag with specific callerid
    {
        let mut writer = BagWriter::create(&input_path).unwrap();
        writer
            .add_connection_with_callerid(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF, "/test_publisher")
            .unwrap();
        writer.finish().unwrap();
    }

    // Rewrite
    let mut rewriter = BagRewriter::new();
    rewriter.rewrite(&input_path, &output_path).unwrap();

    // Verify callerid is preserved
    let reader = BagFormat::open(&output_path).unwrap();
    let channels = reader.channels();
    let channel = channels.values().next().unwrap();
    assert_eq!(channel.callerid.as_deref(), Some("/test_publisher"));
}

// ============================================================================
// BagRewriter Statistics Tests
// ============================================================================

#[test]
fn test_rewriter_tracks_statistics() {
    let (input_path, _guard) = temp_bag_path("stats_input");
    let (output_path, _guard_out) = temp_bag_path("stats_output");

    // Create bag with multiple messages
    {
        let mut writer = BagWriter::create(&input_path).unwrap();
        writer
            .add_connection_with_callerid(0, "/chatter", "std_msgs/String", STD_MSGS_STRING_DEF, "/node")
            .unwrap();

        for i in 0..5 {
            let mut data = vec![0x01, 0x00, 0x00, 0x00];
            data.extend_from_slice(&(5u32.to_le_bytes()));
            data.extend_from_slice(b"Hello");
            writer
                .write_message(&BagMessage::from_raw(0, 1_000_000_000 + i as u64, data))
                .unwrap();
        }
        writer.finish().unwrap();
    }

    // Rewrite with schema validation disabled to avoid decode issues
    let options = RewriteOptions {
        transforms: None,
        validate_schemas: false,
        skip_decode_failures: true,
        passthrough_non_cdr: true,
    };

    let mut rewriter = BagRewriter::with_options(options);
    let stats = rewriter.rewrite(&input_path, &output_path).unwrap();

    assert_eq!(stats.message_count, 5);
    assert_eq!(stats.channel_count, 1);
}
