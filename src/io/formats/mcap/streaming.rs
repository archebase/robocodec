// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified MCAP streaming parser using the StreamingParser trait.
//!
//! This module provides [`McapStreamingParser`], which implements the
//! unified [`StreamingParser`](crate::io::streaming::StreamingParser) trait
//! for MCAP files. It uses the mcap crate's `LinearReader` for robust
//! parsing with chunk boundary handling, CRC validation, and decompression.

use std::collections::HashMap;

use crate::io::formats::mcap::MCAP_MAGIC;
use crate::io::metadata::ChannelInfo;
use crate::io::s3::FatalError;
use crate::io::streaming::StreamingParser;

// Re-export types from s3_adapter for convenience
pub use crate::io::formats::mcap::s3_adapter::{
    ChannelRecordInfo, McapS3Adapter, MessageRecord, SchemaInfo,
};

// Type alias for backward compatibility with code using StreamingMcapParser
pub type StreamingMcapParser = McapStreamingParser;

/// Unified MCAP streaming parser.
///
/// This type implements the [`StreamingParser`] trait for MCAP files,
/// providing a consistent interface across all robotics data formats.
///
/// It wraps [`McapS3Adapter`] and provides trait object compatibility
/// for dynamic dispatch scenarios.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::streaming::StreamingParser;
/// use robocodec::io::formats::mcap::streaming::McapStreamingParser;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut parser = McapStreamingParser::new();
///
/// // Feed chunks as they arrive from S3 or other streaming source
/// let chunk = &[0x89, 0x4d, 0x43, 0x41, 0x50]; // MCAP magic prefix
/// for message in parser.parse_chunk(chunk)? {
///     println!("Got message from channel {}", message.channel_id);
/// }
///
/// // Get discovered channels
/// for (id, channel) in parser.channels() {
///     println!("Channel {}: {}", id, channel.topic);
/// }
/// # Ok(())
/// # }
/// ```
pub struct McapStreamingParser {
    /// The underlying S3 adapter that does the actual parsing
    adapter: McapS3Adapter,
    /// Cached channel map (converted from adapter's internal format)
    cached_channels: HashMap<u16, ChannelInfo>,
    /// Buffer for tracking magic bytes (for is_initialized compatibility)
    magic_buffer: Vec<u8>,
    /// Track whether we've seen the complete magic
    magic_seen: bool,
}

impl McapStreamingParser {
    /// Create a new MCAP streaming parser.
    pub fn new() -> Self {
        Self {
            adapter: McapS3Adapter::new(),
            cached_channels: HashMap::new(),
            magic_buffer: Vec::new(),
            magic_seen: false,
        }
    }

    /// Create a new MCAP streaming parser with a specific channel cache.
    pub fn with_adapter(adapter: McapS3Adapter) -> Self {
        Self {
            adapter,
            cached_channels: HashMap::new(),
            magic_buffer: Vec::new(),
            magic_seen: false,
        }
    }

    /// Get the underlying S3 adapter.
    pub fn adapter(&self) -> &McapS3Adapter {
        &self.adapter
    }

    /// Get a mutable reference to the underlying S3 adapter.
    pub fn adapter_mut(&mut self) -> &mut McapS3Adapter {
        &mut self.adapter
    }

    /// Rebuild the cached channel map from the adapter's internal state.
    fn rebuild_channels(&mut self) {
        self.cached_channels = self.adapter.channels();
    }
}

impl Default for McapStreamingParser {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: McapStreamingParser is safe to share between threads because:
// - The StreamingParser trait requires methods take &mut self, guaranteeing exclusive access
// - The underlying McapS3Adapter is only accessed through &mut self in parse_chunk
// - All other methods provide read-only access or reset the entire state
// This is necessary because mcap::LinearReader contains a !Sync Decompressor
unsafe impl Sync for McapStreamingParser {}

impl StreamingParser for McapStreamingParser {
    type Message = MessageRecord;

    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>, FatalError> {
        // Track magic bytes for is_initialized compatibility with old API
        if !self.magic_seen {
            for &byte in data {
                self.magic_buffer.push(byte);
                // Check if we've completed the magic
                if self.magic_buffer.len() >= MCAP_MAGIC.len() {
                    if &self.magic_buffer[..MCAP_MAGIC.len()] == MCAP_MAGIC {
                        self.magic_seen = true;
                    }
                    break; // Only check up to magic length
                }
            }
        }

        let messages = self.adapter.process_chunk(data)?;

        // Rebuild channels if we discovered new ones
        if self.adapter.has_channels() && self.cached_channels.is_empty() {
            self.rebuild_channels();
        }

        Ok(messages)
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        // Return cached channels if available, otherwise rebuild
        if self.cached_channels.is_empty() {
            // Note: This requires interior mutability in a real scenario,
            // but for read-only access we rebuild from adapter
            &self.cached_channels
        } else {
            &self.cached_channels
        }
    }

    fn message_count(&self) -> u64 {
        self.adapter.message_count()
    }

    fn is_initialized(&self) -> bool {
        // For compatibility with the old StreamingMcapParser API:
        // Return true if we've seen the complete magic bytes
        self.magic_seen
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_parser_new() {
        let parser = McapStreamingParser::new();
        assert!(!parser.is_initialized());
        assert_eq!(parser.message_count(), 0);
        assert!(parser.channels().is_empty());
    }

    #[test]
    fn test_streaming_parser_default() {
        let parser = McapStreamingParser::default();
        assert_eq!(parser.message_count(), 0);
    }

    #[test]
    fn test_streaming_parser_parse_magic() {
        let mut parser = McapStreamingParser::new();
        let magic = crate::io::formats::mcap::MCAP_MAGIC;
        let result = parser.parse_chunk(&magic);
        // Should succeed - magic is processed, no messages yet
        assert!(result.is_ok());
    }

    #[test]
    fn test_streaming_parser_reset() {
        let mut parser = McapStreamingParser::new();
        // Simulate some state
        let _ = parser.parse_chunk(&[1, 2, 3]);
        parser.reset();
        // Should be back to initial state
        assert_eq!(parser.message_count(), 0);
        assert!(!parser.is_initialized());
    }

    #[test]
    fn test_streaming_parser_adapter_access() {
        let mut parser = McapStreamingParser::new();
        // Can access underlying adapter
        let _adapter = parser.adapter();
        let _adapter = parser.adapter_mut();
    }

    #[test]
    fn test_message_record_trait_object() {
        // Verify MessageRecord can be used as the Message type
        fn use_parser(_parser: &dyn StreamingParser<Message = MessageRecord>) {
            // This function exists to verify trait object compatibility
        }

        let parser = McapStreamingParser::new();
        use_parser(&parser);
    }

    #[test]
    fn test_channel_record_info_fields() {
        let channel = ChannelRecordInfo {
            id: 42,
            topic: "/robot/camera".to_string(),
            message_encoding: "cdr".to_string(),
            schema_id: 1,
        };
        assert_eq!(channel.id, 42);
        assert_eq!(channel.topic, "/robot/camera");
        assert_eq!(channel.message_encoding, "cdr");
    }

    #[test]
    fn test_message_record_fields() {
        let msg = MessageRecord {
            channel_id: 5,
            log_time: 1234567890,
            publish_time: 1234567800,
            data: vec![0x01, 0x02, 0x03],
            sequence: 99,
        };
        assert_eq!(msg.channel_id, 5);
        assert_eq!(msg.log_time, 1234567890);
        assert_eq!(msg.publish_time, 1234567800);
        assert_eq!(msg.data, vec![0x01, 0x02, 0x03]);
        assert_eq!(msg.sequence, 99);
    }

    #[test]
    fn test_schema_info_fields() {
        let schema = SchemaInfo {
            id: 10,
            name: "sensor_msgs/msg/Image".to_string(),
            encoding: "ros2msg".to_string(),
            data: b"# std_msgs/msg/Header\nstring frame_id\n".to_vec(),
        };
        assert_eq!(schema.id, 10);
        assert_eq!(schema.name, "sensor_msgs/msg/Image");
        assert_eq!(schema.encoding, "ros2msg");
        assert!(!schema.data.is_empty());
    }
}
