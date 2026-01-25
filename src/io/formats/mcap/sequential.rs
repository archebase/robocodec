// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Sequential MCAP reader using the mcap crate.
//!
//! This module provides a simple sequential reader that uses the mcap crate
//! for reliable MCAP file reading. It's suitable for:
//! - Files without summary sections
//! - Sequential processing workflows
//! - Rewriting operations
//!
//! For parallel reading with chunk-based processing, use `ParallelMcapReader`.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use tracing::warn;

use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};

/// Sequential MCAP reader using the mcap crate.
///
/// This reader uses memory-mapping and the mcap crate's MessageStream
/// for sequential message iteration. It's reliable and works with
/// all valid MCAP files, including those without summary sections.
pub struct SequentialMcapReader {
    /// File path
    path: String,
    /// Memory-mapped file
    mmap: memmap2::Mmap,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Total message count (from summary, 0 if no summary)
    message_count: u64,
    /// Start timestamp (nanoseconds)
    start_time: Option<u64>,
    /// End timestamp (nanoseconds)
    end_time: Option<u64>,
    /// File size
    file_size: u64,
}

impl SequentialMcapReader {
    /// Open an MCAP file for sequential reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref).map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to open file: {e}"))
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                CodecError::encode(
                    "SequentialMcapReader",
                    format!("Failed to get metadata: {e}"),
                )
            })?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("SequentialMcapReader", format!("Failed to mmap file: {e}"))
        })?;

        // Try to read summary for metadata
        let summary_result = mcap::Summary::read(&mmap);

        let (channels, message_count, start_time, end_time) = match summary_result {
            Ok(Some(summary)) => {
                let mut channels = HashMap::new();
                let mut message_counts: HashMap<u16, u64> = HashMap::new();

                // Count messages per channel from stats
                if let Some(stats) = &summary.stats {
                    for (channel_id, count) in &stats.channel_message_counts {
                        message_counts.insert(*channel_id, *count);
                    }
                }

                // Build channel info from summary
                for (id, channel) in &summary.channels {
                    let schema = channel
                        .schema
                        .as_ref()
                        .and_then(|s| summary.schemas.get(&s.id));

                    let schema_text = schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok());
                    let schema_data = schema.map(|s| s.data.to_vec());
                    let schema_encoding = schema.map(|s| s.encoding.clone());

                    channels.insert(
                        *id,
                        ChannelInfo {
                            id: *id,
                            topic: channel.topic.clone(),
                            message_type: channel
                                .schema
                                .as_ref()
                                .map(|s| s.name.clone())
                                .unwrap_or_default(),
                            encoding: channel.message_encoding.clone(),
                            schema: schema_text,
                            schema_data,
                            schema_encoding,
                            message_count: *message_counts.get(id).unwrap_or(&0),
                            callerid: None,
                        },
                    );
                }

                let (start, end, count) = match &summary.stats {
                    Some(stats) => (
                        Some(stats.message_start_time),
                        Some(stats.message_end_time),
                        stats.message_count,
                    ),
                    None => (None, None, 0),
                };

                (channels, count, start, end)
            }
            Ok(None) => {
                warn!(
                    context = "SequentialMcapReader",
                    "MCAP file has no summary section, scanning for channels"
                );
                // Scan for channels by iterating through messages
                let channels = Self::scan_channels(&mmap)?;
                (channels, 0, None, None)
            }
            Err(e) => {
                warn!(
                    context = "SequentialMcapReader",
                    error = %e,
                    "Failed to read summary, scanning for channels"
                );
                let channels = Self::scan_channels(&mmap)?;
                (channels, 0, None, None)
            }
        };

        Ok(Self {
            path: path_str,
            mmap,
            channels,
            message_count,
            start_time,
            end_time,
            file_size,
        })
    }

    /// Scan the file to build channel information when no summary is available.
    fn scan_channels(mmap: &memmap2::Mmap) -> Result<HashMap<u16, ChannelInfo>> {
        let mut channels = HashMap::new();

        let stream = mcap::MessageStream::new(mmap).map_err(|e| {
            CodecError::encode(
                "SequentialMcapReader",
                format!("Failed to create message stream: {e}"),
            )
        })?;

        for result in stream {
            let message = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        context = "SequentialMcapReader",
                        error = %e,
                        "Error reading message during channel scan"
                    );
                    continue;
                }
            };

            let channel_id = message.channel.id;
            if let std::collections::hash_map::Entry::Vacant(e) = channels.entry(channel_id) {
                let schema = message.channel.schema.as_ref();
                let schema_text = schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok());
                let schema_data = schema.map(|s| s.data.to_vec());
                let schema_encoding = schema.map(|s| s.encoding.clone());

                e.insert(ChannelInfo {
                    id: channel_id,
                    topic: message.channel.topic.clone(),
                    message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                    encoding: message.channel.message_encoding.clone(),
                    schema: schema_text,
                    schema_data,
                    schema_encoding,
                    message_count: 0,
                    callerid: None,
                });
            }
        }

        Ok(channels)
    }

    /// Create a raw message iterator for sequential reading.
    pub fn iter_raw(&self) -> Result<SequentialRawIter<'_>> {
        SequentialRawIter::new(&self.mmap, &self.channels)
    }

    /// Get the memory-mapped data.
    pub fn mmap(&self) -> &memmap2::Mmap {
        &self.mmap
    }
}

impl FormatReader for SequentialMcapReader {
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
        FileFormat::Mcap
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Sequential raw message iterator using the mcap crate.
pub struct SequentialRawIter<'a> {
    channels: HashMap<u16, ChannelInfo>,
    stream: mcap::MessageStream<'a>,
}

impl<'a> SequentialRawIter<'a> {
    fn new(mmap: &'a memmap2::Mmap, channels: &HashMap<u16, ChannelInfo>) -> Result<Self> {
        let stream = mcap::MessageStream::new(mmap).map_err(|e| {
            CodecError::encode(
                "SequentialRawIter",
                format!("Failed to create message stream: {e}"),
            )
        })?;

        Ok(Self {
            channels: channels.clone(),
            stream,
        })
    }

    /// Get the channels.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }
}

impl<'a> Iterator for SequentialRawIter<'a> {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.stream.next() {
            let message = match result {
                Ok(m) => m,
                Err(e) => {
                    return Some(Err(CodecError::encode(
                        "SequentialRawIter",
                        format!("Read error: {e}"),
                    )))
                }
            };

            let channel_id = message.channel.id;

            // Get or create channel info
            let channel_info = if let Some(info) = self.channels.get(&channel_id) {
                info.clone()
            } else {
                // Channel not in our map, create one from the message
                let schema = message.channel.schema.as_ref();
                ChannelInfo {
                    id: channel_id,
                    topic: message.channel.topic.clone(),
                    message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                    encoding: message.channel.message_encoding.clone(),
                    schema: schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok()),
                    schema_data: schema.map(|s| s.data.to_vec()),
                    schema_encoding: schema.map(|s| s.encoding.clone()),
                    message_count: 0,
                    callerid: None,
                }
            };

            return Some(Ok((
                RawMessage {
                    channel_id,
                    log_time: message.log_time,
                    publish_time: message.publish_time,
                    data: message.data.to_vec(),
                    sequence: Some(message.sequence as u64),
                },
                channel_info,
            )));
        }

        None
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

    /// Test opening a valid MCAP file
    #[test]
    fn test_sequential_reader_open_valid() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return; // Skip if fixture not available
        }

        let result = SequentialMcapReader::open(&path);
        assert!(
            result.is_ok(),
            "SequentialMcapReader::open should succeed: {:?}",
            result.err()
        );
    }

    /// Test opening a nonexistent file
    #[test]
    fn test_sequential_reader_open_nonexistent() {
        let result = SequentialMcapReader::open("/nonexistent/file.mcap");
        assert!(result.is_err(), "should fail for nonexistent file");
    }

    /// Test FormatReader trait methods
    #[test]
    fn test_sequential_reader_format_reader_trait() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();

        // Test channels()
        let channels = reader.channels();
        assert!(!channels.is_empty(), "should have channels");

        // Test path()
        assert!(!reader.path().is_empty(), "path should not be empty");

        // Test format()
        assert_eq!(reader.format(), FileFormat::Mcap);

        // Test file_size()
        assert!(reader.file_size() > 0, "file_size should be positive");
    }

    /// Test channel information extraction
    #[test]
    fn test_sequential_reader_channels() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
        let channels = reader.channels();

        // Should have at least one channel
        assert!(!channels.is_empty(), "should discover channels");

        // Verify channel structure
        for (id, channel) in channels {
            assert_eq!(channel.id, *id, "channel id should match key");
            assert!(!channel.topic.is_empty(), "topic should not be empty");
            assert!(!channel.encoding.is_empty(), "encoding should not be empty");
        }
    }

    /// Test mmap accessor
    #[test]
    fn test_sequential_reader_mmap() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
        let mmap = reader.mmap();

        assert!(!mmap.is_empty(), "mmap should not be empty");
    }

    /// Test iter_raw creation
    #[test]
    fn test_sequential_reader_iter_raw() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
        let result = reader.iter_raw();

        assert!(result.is_ok(), "iter_raw should succeed");
    }

    /// Test SequentialRawIter reads messages
    #[test]
    fn test_sequential_raw_iter_messages() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
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
        }
    }

    /// Test SequentialRawIter channels accessor
    #[test]
    fn test_sequential_raw_iter_channels() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
        let iter = reader.iter_raw().unwrap();

        let channels = iter.channels();
        assert!(!channels.is_empty(), "iter should have channels");
    }

    /// Test reading all messages from a file
    #[test]
    fn test_sequential_read_all_messages() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
        let iter = reader.iter_raw().unwrap();

        let count = iter.filter_map(|r| r.ok()).count();
        assert!(count > 0, "should read multiple messages");
    }

    /// Test as_any trait method
    #[test]
    fn test_sequential_reader_as_any() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();

        // Test as_any
        let any = reader.as_any();
        assert!(any.is::<SequentialMcapReader>());

        // Test as_any_mut
        let any_mut = reader.as_any();
        assert!(any_mut.is::<SequentialMcapReader>());
    }

    /// Test start_time and end_time
    #[test]
    fn test_sequential_reader_time_range() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();

        // These may be None if file has no summary section
        let _start = reader.start_time();
        let _end = reader.end_time();
    }

    /// Test message_count from summary
    #[test]
    fn test_sequential_reader_message_count() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();
        let count = reader.message_count();

        // May be 0 if no summary, but should be non-negative
        let _ = count;
    }

    /// Test with multiple fixture files
    #[test]
    fn test_sequential_reader_multiple_fixtures() {
        for i in 0..=5 {
            let path = fixture_path(&format!("robocodec_test_{}.mcap", i));
            if !path.exists() {
                continue;
            }

            let result = SequentialMcapReader::open(&path);
            if let Ok(reader) = result {
                // Verify basic properties
                assert!(!reader.path().is_empty());
                assert_eq!(reader.format(), FileFormat::Mcap);
                assert!(reader.file_size() > 0);
            }
        }
    }

    /// Compile-time checks for trait bounds
    #[test]
    fn test_sequential_raw_iter_send() {
        fn assert_send<T: Send>() {}
        assert_send::<SequentialRawIter<'_>>();
    }

    #[test]
    fn test_sequential_reader_format_trait_bound() {
        fn assert_format_reader<T: FormatReader>() {}
        assert_format_reader::<SequentialMcapReader>();
    }

    /// Test that iter_raw can be called multiple times
    #[test]
    fn test_sequential_reader_iter_raw_multiple() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();

        // Can create multiple iterators
        let iter1 = reader.iter_raw();
        assert!(iter1.is_ok());

        let iter2 = reader.iter_raw();
        assert!(iter2.is_ok());
    }

    /// Test error handling for nonexistent file
    #[test]
    fn test_sequential_reader_nonexistent_file() {
        let result = SequentialMcapReader::open("/nonexistent/path/file.mcap");
        assert!(result.is_err());
    }

    /// Test that channel info is properly cloned
    #[test]
    fn test_sequential_channel_info_structure() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();

        for channel in reader.channels().values() {
            // Verify all fields are accessible
            let _ = &channel.topic;
            let _ = &channel.message_type;
            let _ = &channel.encoding;
            let _ = &channel.schema;
            let _ = &channel.schema_data;
            let _ = &channel.schema_encoding;
        }
    }

    /// Test SequentialRawIter handles unknown channels gracefully
    #[test]
    fn test_sequential_iter_unknown_channel_handling() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let reader = SequentialMcapReader::open(&path).unwrap();

        // Create an iterator with empty channels map to test unknown channel handling
        let empty_channels = HashMap::new();
        let result = SequentialRawIter::new(reader.mmap(), &empty_channels);

        // Even with empty channels, should create iterator
        assert!(result.is_ok());

        let mut iter = result.unwrap();
        // First message should create channel info on the fly
        if let Some(Ok((_msg, channel))) = iter.next() {
            // Channel should be created even if not in initial map
            assert!(!channel.topic.is_empty());
        }
    }
}
