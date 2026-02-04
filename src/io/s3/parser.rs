// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Shared streaming parser trait for S3 file format parsing.
//!
//! This module defines a unified interface for streaming parsers that handle
//! different robotics data formats (MCAP, BAG, RRD) from S3.

use std::collections::HashMap;

use crate::io::metadata::ChannelInfo;
use crate::io::s3::error::FatalError;

/// Shared trait for streaming parsers of robotics data formats.
///
/// This trait abstracts the common functionality needed to parse
/// different file formats (MCAP, BAG, RRD) from byte chunks
/// as they arrive from S3.
pub trait StreamingParser: Send + Sync {
    /// Message type yielded by this parser.
    type Message;

    /// Parse a chunk of data and extract any complete messages.
    ///
    /// This method should be called repeatedly as chunks arrive from S3.
    /// It maintains internal state to handle partial records that
    /// span chunk boundaries.
    ///
    /// # Arguments
    ///
    /// * `data` - A chunk of bytes from the S3 file
    ///
    /// # Returns
    ///
    /// A vector of complete messages found in this chunk (may be empty)
    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>, FatalError>;

    /// Get the discovered channels from this parser.
    ///
    /// Channels are discovered during initialization or while parsing.
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;

    /// Get the total number of messages parsed so far.
    fn message_count(&self) -> u64;

    /// Check if the parser has discovered any channels.
    ///
    /// This is used to determine if metadata initialization is complete.
    fn has_channels(&self) -> bool {
        !self.channels().is_empty()
    }

    /// Check if the parser is initialized and ready to yield messages.
    ///
    /// Different parsers may have different initialization requirements.
    /// For example, MCAP needs to parse at least the header before yielding.
    fn is_initialized(&self) -> bool;

    /// Reset the parser state for a new file.
    ///
    /// This is useful when reusing a parser instance.
    fn reset(&mut self);
}

/// Downcast helper for working with trait objects.
pub trait AsStreamingParser {
    /// Message type for this parser
    type Message;

    /// Get a reference as a StreamingParser trait object.
    fn as_streaming_parser(&self) -> &dyn StreamingParser<Message = Self::Message>;

    /// Get a mutable reference as a StreamingParser trait object.
    fn as_streaming_parser_mut(&mut self) -> &mut dyn StreamingParser<Message = Self::Message>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock parser for testing
    struct MockParser {
        channels: HashMap<u16, ChannelInfo>,
        message_count: u64,
        initialized: bool,
    }

    #[derive(Debug, Clone)]
    struct MockMessage {}

    impl StreamingParser for MockParser {
        type Message = MockMessage;

        fn parse_chunk(&mut self, _data: &[u8]) -> Result<Vec<Self::Message>, FatalError> {
            Ok(vec![])
        }

        fn channels(&self) -> &HashMap<u16, ChannelInfo> {
            &self.channels
        }

        fn message_count(&self) -> u64 {
            self.message_count
        }

        fn is_initialized(&self) -> bool {
            self.initialized
        }

        fn reset(&mut self) {
            self.channels.clear();
            self.message_count = 0;
            self.initialized = false;
        }
    }

    #[test]
    fn test_streaming_parser_has_channels() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };
        assert!(!parser.has_channels());
    }

    #[test]
    fn test_streaming_parser_reset() {
        let mut parser = MockParser {
            channels: {
                let mut map = HashMap::new();
                map.insert(
                    0,
                    ChannelInfo {
                        id: 0,
                        topic: "/test".to_string(),
                        message_type: "test_msgs/Test".to_string(),
                        encoding: "json".to_string(),
                        schema: None,
                        schema_data: None,
                        schema_encoding: None,
                        message_count: 0,
                        callerid: None,
                    },
                );
                map
            },
            message_count: 5,
            initialized: true,
        };

        parser.reset();
        assert!(!parser.has_channels());
        assert_eq!(parser.message_count(), 0);
        assert!(!parser.is_initialized());
    }
}
