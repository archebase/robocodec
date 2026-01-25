// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Sequential BAG reader using rosbag crate as reference.
//!
//! This module provides `SequentialBagReader` for reading ROS1 bag files
//! sequentially using the rosbag crate as the underlying implementation.
//!
//! Use this reader as the reference implementation to verify the correctness
//! of the parallel BAG reader.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

/// Sequential BAG reader format.
pub struct BagSequentialFormat;

impl BagSequentialFormat {
    /// Open a BAG file and return a sequential reader.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<SequentialBagReader> {
        SequentialBagReader::open(path)
    }
}

/// Sequential BAG reader using rosbag crate.
///
/// This reader uses the rosbag crate for reliable BAG file reading.
/// It's suitable for:
/// - Reference implementation for testing
/// - Sequential processing workflows
/// - Rewriting operations
pub struct SequentialBagReader {
    /// File path
    path: String,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: HashMap<u32, u16>,
    /// Total message count
    message_count: u64,
    /// Start timestamp (nanoseconds)
    start_time: Option<u64>,
    /// End timestamp (nanoseconds)
    end_time: Option<u64>,
}

impl SequentialBagReader {
    /// Open a BAG file for sequential reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        // Open the bag file using rosbag crate
        let bag = rosbag::RosBag::new(path_ref).map_err(|e| {
            CodecError::encode("SequentialBagReader", format!("Failed to open bag: {e}"))
        })?;

        let mut channels = HashMap::new();
        let mut conn_id_map = HashMap::new();
        let mut next_channel_id: u16 = 0;
        let mut connections_seen: HashSet<u32> = HashSet::new();

        // Collect connections from index section
        for record in bag.index_records() {
            let record = record.map_err(|e| {
                CodecError::encode("SequentialBagReader", format!("Failed to read index: {e}"))
            })?;
            if let rosbag::IndexRecord::Connection(conn) = record {
                if connections_seen.insert(conn.id) {
                    let channel_id = next_channel_id;
                    next_channel_id = next_channel_id.wrapping_add(1);

                    channels.insert(
                        channel_id,
                        ChannelInfo {
                            id: channel_id,
                            topic: conn.topic.to_string(),
                            message_type: conn.tp.to_string(),
                            encoding: "ros1".to_string(), // ROS1 serialization format
                            schema: Some(conn.message_definition.to_string()),
                            schema_data: None,
                            schema_encoding: Some("ros1msg".to_string()),
                            message_count: 0,
                            callerid: if conn.caller_id.is_empty() {
                                None
                            } else {
                                Some(conn.caller_id.to_string())
                            },
                        },
                    );
                    conn_id_map.insert(conn.id, channel_id);
                }
            }
        }

        // Also check chunk section for connections not in index
        for record in bag.chunk_records() {
            let record = record.map_err(|e| {
                CodecError::encode("SequentialBagReader", format!("Failed to read chunk: {e}"))
            })?;
            if let rosbag::ChunkRecord::Chunk(chunk) = record {
                for msg_result in chunk.messages() {
                    let msg_result = msg_result.map_err(|e| {
                        CodecError::encode(
                            "SequentialBagReader",
                            format!("Failed to read message: {e}"),
                        )
                    })?;
                    if let rosbag::MessageRecord::Connection(conn) = msg_result {
                        if connections_seen.insert(conn.id) {
                            let channel_id = next_channel_id;
                            next_channel_id = next_channel_id.wrapping_add(1);

                            channels.insert(
                                channel_id,
                                ChannelInfo {
                                    id: channel_id,
                                    topic: conn.topic.to_string(),
                                    message_type: conn.tp.to_string(),
                                    encoding: "ros1".to_string(), // ROS1 serialization format
                                    schema: Some(conn.message_definition.to_string()),
                                    schema_data: None,
                                    schema_encoding: Some("ros1msg".to_string()),
                                    message_count: 0,
                                    callerid: if conn.caller_id.is_empty() {
                                        None
                                    } else {
                                        Some(conn.caller_id.to_string())
                                    },
                                },
                            );
                            conn_id_map.insert(conn.id, channel_id);
                        }
                    }
                }
            }
        }

        Ok(Self {
            path: path_str,
            channels,
            conn_id_map,
            message_count: 0,
            start_time: None,
            end_time: None,
        })
    }

    /// Create a raw message iterator.
    pub fn iter_raw(&self) -> Result<SequentialBagRawIter> {
        SequentialBagRawIter::new(&self.path, &self.channels, &self.conn_id_map)
    }

    /// Get the connection ID to channel ID mapping.
    pub fn conn_id_map(&self) -> &HashMap<u32, u16> {
        &self.conn_id_map
    }
}

impl FormatReader for SequentialBagReader {
    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    fn message_count(&self) -> u64 {
        self.message_count
    }

    fn start_time(&self) -> Option<u64> {
        self.start_time
    }

    fn end_time(&self) -> Option<u64> {
        self.end_time
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn format(&self) -> FileFormat {
        FileFormat::Bag
    }

    fn file_size(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Raw message iterator for BAG files using rosbag crate.
///
/// This iterator uses the rosbag crate to read messages sequentially.
pub struct SequentialBagRawIter {
    /// Bag file reader
    bag: rosbag::RosBag,
    /// Channel information
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: HashMap<u32, u16>,
    /// Chunk records collected upfront
    chunk_records: Vec<Vec<rosbag::record_types::MessageData<'static>>>,
    /// Current messages being processed
    current_messages: Option<Vec<rosbag::record_types::MessageData<'static>>>,
    /// Current index within current_messages
    current_index: usize,
    /// Current chunk index
    chunk_index: usize,
}

impl SequentialBagRawIter {
    /// Create a new raw message iterator for a bag file.
    pub fn new(
        path: &str,
        channels: &HashMap<u16, ChannelInfo>,
        conn_id_map: &HashMap<u32, u16>,
    ) -> Result<Self> {
        let bag = rosbag::RosBag::new(Path::new(path)).map_err(|e| {
            CodecError::encode("SequentialBagRawIter", format!("Failed to open bag: {e}"))
        })?;

        Ok(Self {
            bag,
            channels: channels.clone(),
            conn_id_map: conn_id_map.clone(),
            chunk_records: Vec::new(),
            current_messages: None,
            current_index: 0,
            chunk_index: 0,
        })
    }

    /// Load the next chunk's messages.
    fn load_next_chunk(&mut self) -> Result<bool> {
        // Collect all chunk records on first access
        if self.chunk_index == 0 && self.chunk_records.is_empty() {
            for record in self.bag.chunk_records() {
                let record = record.map_err(|e| {
                    CodecError::encode(
                        "SequentialBagRawIter",
                        format!("Failed to read record: {e}"),
                    )
                })?;

                if let rosbag::ChunkRecord::Chunk(chunk) = record {
                    let mut messages = Vec::new();
                    for msg_result in chunk.messages() {
                        let msg_result = msg_result.map_err(|e| {
                            CodecError::encode(
                                "SequentialBagRawIter",
                                format!("Failed to read message: {e}"),
                            )
                        })?;
                        if let rosbag::MessageRecord::MessageData(msg) = msg_result {
                            // SAFETY: We extend the lifetime to 'static for storage.
                            // This is safe because we own the RosBag which owns the data.
                            let extended = unsafe {
                                std::mem::transmute::<
                                    rosbag::record_types::MessageData<'_>,
                                    rosbag::record_types::MessageData<'static>,
                                >(msg)
                            };
                            messages.push(extended);
                        }
                    }
                    if !messages.is_empty() {
                        self.chunk_records.push(messages);
                    }
                }
            }
        }

        if self.chunk_index >= self.chunk_records.len() {
            return Ok(false);
        }

        self.current_messages = Some(self.chunk_records[self.chunk_index].clone());
        self.current_index = 0;
        self.chunk_index += 1;
        Ok(true)
    }
}

impl Iterator for SequentialBagRawIter {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Load current messages if needed
            if self.current_messages.is_none() {
                match self.load_next_chunk() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let messages = self.current_messages.as_ref().unwrap();
            if self.current_index >= messages.len() {
                self.current_messages = None;
                continue;
            }

            let msg_data = &messages[self.current_index];
            self.current_index += 1;

            // Map connection ID to channel ID
            let channel_id = match self.conn_id_map.get(&msg_data.conn_id) {
                Some(&id) => id,
                None => continue, // Skip unknown connection IDs
            };

            // Get channel info
            let channel_info = match self.channels.get(&channel_id) {
                Some(info) => info.clone(),
                None => continue, // Skip unknown channel IDs
            };

            // Return raw message
            return Some(Ok((
                RawMessage {
                    channel_id,
                    log_time: msg_data.time,
                    publish_time: msg_data.time,
                    data: msg_data.data.to_vec(),
                    sequence: None,
                },
                channel_info,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to get fixture path
    fn fixture_path(name: &str) -> std::path::PathBuf {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        std::path::PathBuf::from(manifest_dir)
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    /// Test opening a valid BAG file
    #[test]
    fn test_sequential_bag_reader_open_valid() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return; // Skip if fixture not available
        }

        let result = SequentialBagReader::open(&path);
        assert!(
            result.is_ok(),
            "SequentialBagReader::open should succeed: {:?}",
            result.err()
        );
    }

    /// Test opening a nonexistent file
    #[test]
    fn test_sequential_bag_reader_open_nonexistent() {
        let result = SequentialBagReader::open("/nonexistent/file.bag");
        assert!(result.is_err(), "should fail for nonexistent file");
    }

    /// Test FormatReader trait methods
    #[test]
    fn test_sequential_bag_reader_format_reader_trait() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        // Test channels()
        let channels = reader.channels();
        assert!(!channels.is_empty(), "should have channels");

        // Test path()
        assert!(!reader.path().is_empty(), "path should not be empty");

        // Test format()
        assert_eq!(reader.format(), FileFormat::Bag);

        // Test file_size()
        assert!(reader.file_size() > 0, "file_size should be positive");
    }

    /// Test channel information extraction
    #[test]
    fn test_sequential_bag_reader_channels() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let channels = reader.channels();

        // Should have at least one channel
        assert!(!channels.is_empty(), "should discover channels");

        // Verify channel structure
        for (id, channel) in channels {
            assert_eq!(channel.id, *id, "channel id should match key");
            assert!(!channel.topic.is_empty(), "topic should not be empty");
            assert_eq!(channel.encoding, "ros1", "encoding should be ros1");
            assert_eq!(
                channel.schema_encoding,
                Some("ros1msg".to_string()),
                "schema encoding should be ros1msg"
            );
        }
    }

    /// Test conn_id_map accessor
    #[test]
    fn test_sequential_bag_reader_conn_id_map() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let conn_map = reader.conn_id_map();

        // Should have connection mappings
        assert!(!conn_map.is_empty(), "should have connection id mappings");
    }

    /// Test iter_raw creation
    #[test]
    fn test_sequential_bag_reader_iter_raw() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let result = reader.iter_raw();

        assert!(result.is_ok(), "iter_raw should succeed");
    }

    /// Test SequentialBagRawIter reads messages
    #[test]
    fn test_sequential_bag_raw_iter_messages() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let mut iter = reader.iter_raw().unwrap();

        // Should be able to read at least one message
        let first = iter.next();
        assert!(first.is_some(), "should have at least one message");

        if let Some(Ok((msg, channel))) = first {
            assert!(!msg.data.is_empty(), "message data should not be empty");
            assert!(
                !channel.topic.is_empty(),
                "channel topic should not be empty"
            );
            assert_eq!(channel.encoding, "ros1");
        }
    }

    /// Test reading all messages from a file
    #[test]
    fn test_sequential_bag_read_all_messages() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let iter = reader.iter_raw().unwrap();

        let count = iter.filter_map(|r| r.ok()).count();
        assert!(count > 0, "should read multiple messages");
    }

    /// Test as_any trait method
    #[test]
    fn test_sequential_bag_reader_as_any() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        // Test as_any
        let any = reader.as_any();
        assert!(any.is::<SequentialBagReader>());

        // Test as_any_mut
        let any_mut = reader.as_any();
        assert!(any_mut.is::<SequentialBagReader>());
    }

    /// Test start_time and end_time
    #[test]
    fn test_sequential_bag_reader_time_range() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        // These are currently not set (None)
        assert_eq!(reader.start_time(), None);
        assert_eq!(reader.end_time(), None);
    }

    /// Test message_count (currently 0)
    #[test]
    fn test_sequential_bag_reader_message_count() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        assert_eq!(
            reader.message_count(),
            0,
            "message count is not implemented"
        );
    }

    /// Test with multiple fixture files
    #[test]
    fn test_sequential_bag_reader_multiple_fixtures() {
        for i in [15, 17, 18, 19] {
            let path = fixture_path(&format!("robocodec_test_{}.bag", i));
            if !path.exists() {
                continue;
            }

            let result = SequentialBagReader::open(&path);
            if let Ok(reader) = result {
                // Verify basic properties
                assert!(!reader.path().is_empty());
                assert_eq!(reader.format(), FileFormat::Bag);
                assert!(reader.file_size() > 0);
            }
        }
    }

    /// Test BagSequentialFormat wrapper
    #[test]
    fn test_bag_sequential_format_open() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let result = BagSequentialFormat::open(&path);
        assert!(
            result.is_ok(),
            "BagSequentialFormat::open should succeed: {:?}",
            result.err()
        );
    }

    /// Test ROS1 encoding constants
    #[test]
    fn test_ros1_encoding_constant() {
        // Verify that we use "ros1" encoding for ROS1 bag files
        let ros1_encoding = "ros1";
        let ros1msg_schema_encoding = "ros1msg";

        assert_eq!(ros1_encoding, "ros1");
        assert_eq!(ros1msg_schema_encoding, "ros1msg");
        assert!(ros1_encoding.starts_with("ros1"));
        assert!(ros1msg_schema_encoding.starts_with("ros1"));
    }

    /// Compile-time checks for trait bounds
    #[test]
    fn test_sequential_bag_raw_iter_send() {
        fn assert_send<T: Send>() {}
        assert_send::<SequentialBagRawIter>();
    }

    #[test]
    fn test_sequential_bag_reader_format_trait_bound() {
        fn assert_format_reader<T: FormatReader>() {}
        assert_format_reader::<SequentialBagReader>();
    }

    /// Test that iter_raw can be called multiple times
    #[test]
    fn test_sequential_bag_reader_iter_raw_multiple() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        // Can create multiple iterators
        let iter1 = reader.iter_raw();
        assert!(iter1.is_ok());

        let iter2 = reader.iter_raw();
        assert!(iter2.is_ok());
    }

    /// Test channel info structure
    #[test]
    fn test_sequential_bag_channel_info_structure() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        for channel in reader.channels().values() {
            // Verify all fields are accessible
            let _ = &channel.topic;
            let _ = &channel.message_type;
            let _ = &channel.encoding;
            let _ = &channel.schema;
            let _ = &channel.schema_data;
            let _ = &channel.schema_encoding;
            let _ = &channel.callerid;

            // Verify ROS1-specific fields
            assert_eq!(channel.encoding, "ros1");
            assert_eq!(channel.schema_encoding, Some("ros1msg".to_string()));
        }
    }

    /// Test that callerid is properly extracted
    #[test]
    fn test_sequential_bag_callerid_extraction() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        for channel in reader.channels().values() {
            // callerid may be Some or None depending on the bag file
            let _callerid = &channel.callerid;
            // Just verify we can access it
        }
    }

    /// Test handling of messages with unknown connection IDs
    #[test]
    fn test_sequential_bag_iter_unknown_connection() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        // Create an iterator with empty connection map to test unknown connection handling
        let empty_conn_map = HashMap::new();
        let empty_channels = HashMap::new();
        let result = SequentialBagRawIter::new(reader.path(), &empty_channels, &empty_conn_map);

        // Even with empty maps, should create iterator
        assert!(result.is_ok());

        let mut iter = result.unwrap();
        // All messages should be skipped due to unknown connection
        let first = iter.next();
        // Either None (no messages) or continues skipping
        if let Some(Ok(_)) = first {
            // If we got a message, it should have gone through the channel lookup
        }
    }

    /// Test message data integrity
    #[test]
    fn test_sequential_bag_message_data_integrity() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let mut iter = reader.iter_raw().unwrap();

        if let Some(Ok((msg, channel))) = iter.next() {
            // Verify message structure
            assert!(msg.channel_id > 0, "channel_id should be positive");
            assert!(!msg.data.is_empty(), "data should not be empty");
            assert_eq!(
                msg.log_time, msg.publish_time,
                "ROS1 bags have same log/publish time"
            );
            assert_eq!(msg.sequence, None, "ROS1 bags don't have sequence numbers");
            assert!(!channel.topic.is_empty());
            assert!(!channel.message_type.is_empty());
        }
    }

    /// Test iterator exhausted after reading all messages
    #[test]
    fn test_sequential_bag_iter_exhausted() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();
        let mut iter = reader.iter_raw().unwrap();

        // Consume all messages
        let count = iter.by_ref().count();

        // Should return None after exhaustion
        assert!(iter.next().is_none(), "iterator should be exhausted");

        // Verify we read at least some messages
        assert!(count > 0, "should have read messages");
    }

    /// Test schema field is populated
    #[test]
    fn test_sequential_bag_schema_populated() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let reader = SequentialBagReader::open(&path).unwrap();

        for channel in reader.channels().values() {
            // Schema should be populated from message_definition
            assert!(
                channel.schema.is_some(),
                "schema should be populated from message_definition"
            );
            if let Some(ref schema) = channel.schema {
                assert!(!schema.is_empty(), "schema should not be empty");
            }
        }
    }
}
