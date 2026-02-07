// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified streaming parser trait for robotics data formats.
//!
//! This module defines the [`StreamingParser`] trait, which provides
//! a common interface for streaming parsers of different robotics
//! data formats (MCAP, ROS1 bag, RRD).

use std::collections::HashMap;

use crate::io::metadata::ChannelInfo;
use crate::io::s3::FatalError;

/// Unified trait for streaming parsers of robotics data formats.
///
/// This trait abstracts the common functionality needed to parse
/// different file formats (MCAP, BAG, RRD) from byte chunks
/// as they arrive from S3 or other streaming sources.
///
/// # Design
///
/// The trait is designed for chunk-based processing where:
/// 1. Data arrives in chunks (not all at once)
/// 2. Records may span chunk boundaries
/// 3. Metadata (channels) is discovered during parsing
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::streaming::StreamingParser;
/// use robocodec::io::formats::mcap::streaming::McapStreamingParser;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut parser: McapStreamingParser = McapStreamingParser::new();
///
/// // Feed chunks as they arrive from S3
/// let chunk = b"some MCAP data";
/// for message in parser.parse_chunk(chunk)? {
///     // Process message
///     println!("Message from channel: {}", message.channel_id);
/// }
/// # Ok(())
/// # }
/// ```
pub trait StreamingParser: Send + Sync {
    /// Message type yielded by this parser.
    ///
    /// Each format defines its own message type (e.g., `MessageRecord`,
    /// `BagMessageRecord`, etc.).
    type Message: Clone + Send;

    /// Parse a chunk of data and extract any complete messages.
    ///
    /// This method should be called repeatedly as chunks arrive from S3.
    /// It maintains internal state to handle partial records that
    /// span chunk boundaries.
    ///
    /// # Arguments
    ///
    /// * `data` - A chunk of bytes from the data source
    ///
    /// # Returns
    ///
    /// A vector of complete messages found in this chunk (may be empty
    /// if no complete records are in the chunk)
    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>, FatalError>;

    /// Get the discovered channels from this parser.
    ///
    /// Channels are discovered during initialization or while parsing.
    /// Returns a map from channel ID to channel information.
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;

    /// Get the total number of messages parsed so far.
    fn message_count(&self) -> u64;

    /// Check if the parser has discovered any channels.
    ///
    /// This is used to determine if metadata initialization is complete.
    /// Default implementation checks if channels map is non-empty.
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
///
/// This trait allows concrete parser types to expose themselves as
/// `StreamingParser` trait objects, enabling dynamic dispatch.
pub trait AsStreamingParser {
    /// Message type for this parser
    type Message;

    /// Get a reference as a `StreamingParser` trait object.
    fn as_streaming_parser(&self) -> &dyn StreamingParser<Message = Self::Message>;

    /// Get a mutable reference as a `StreamingParser` trait object.
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

    #[derive(Debug, Clone, PartialEq)]
    struct MockMessage {
        data: Vec<u8>,
    }

    impl StreamingParser for MockParser {
        type Message = MockMessage;

        fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>, FatalError> {
            // Simple mock: return messages based on data length
            if data.is_empty() {
                return Ok(vec![]);
            }
            // Return a message with the data
            Ok(vec![MockMessage {
                data: data.to_vec(),
            }])
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

    // =========================================================================
    // StreamingParser::has_channels tests
    // =========================================================================

    #[test]
    fn test_streaming_parser_has_channels_empty() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };
        assert!(!parser.has_channels());
    }

    #[test]
    fn test_streaming_parser_has_channels_with_channels() {
        let mut channels = HashMap::new();
        channels.insert(
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

        let parser = MockParser {
            channels,
            message_count: 0,
            initialized: false,
        };
        assert!(parser.has_channels());
    }

    // =========================================================================
    // StreamingParser::reset tests
    // =========================================================================

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

    #[test]
    fn test_streaming_parser_reset_when_empty() {
        let mut parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        parser.reset();
        assert!(!parser.has_channels());
        assert_eq!(parser.message_count(), 0);
        assert!(!parser.is_initialized());
    }

    // =========================================================================
    // StreamingParser::parse_chunk tests
    // =========================================================================

    #[test]
    fn test_streaming_parser_parse_chunk_empty() {
        let mut parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        let result = parser.parse_chunk(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_streaming_parser_parse_chunk_with_data() {
        let mut parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        let result = parser.parse_chunk(&[1, 2, 3, 4]);
        assert!(result.is_ok());
        let messages = result.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, vec![1, 2, 3, 4]);
    }

    // =========================================================================
    // StreamingParser::channels tests
    // =========================================================================

    #[test]
    fn test_streaming_parser_channels_empty() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        assert!(parser.channels().is_empty());
        assert_eq!(parser.channels().len(), 0);
    }

    #[test]
    fn test_streaming_parser_channels_multiple() {
        let mut channels = HashMap::new();
        channels.insert(
            0,
            ChannelInfo {
                id: 0,
                topic: "/topic1".to_string(),
                message_type: "test/Msg1".to_string(),
                encoding: "cdr".to_string(),
                schema: None,
                schema_data: None,
                schema_encoding: None,
                message_count: 0,
                callerid: None,
            },
        );
        channels.insert(
            1,
            ChannelInfo {
                id: 1,
                topic: "/topic2".to_string(),
                message_type: "test/Msg2".to_string(),
                encoding: "cdr".to_string(),
                schema: None,
                schema_data: None,
                schema_encoding: None,
                message_count: 0,
                callerid: None,
            },
        );

        let parser = MockParser {
            channels,
            message_count: 0,
            initialized: false,
        };

        assert_eq!(parser.channels().len(), 2);
        assert!(parser.channels().contains_key(&0));
        assert!(parser.channels().contains_key(&1));
    }

    // =========================================================================
    // StreamingParser::message_count tests
    // =========================================================================

    #[test]
    fn test_streaming_parser_message_count_zero() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        assert_eq!(parser.message_count(), 0);
    }

    #[test]
    fn test_streaming_parser_message_count_nonzero() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 42,
            initialized: false,
        };

        assert_eq!(parser.message_count(), 42);
    }

    #[test]
    fn test_streaming_parser_message_count_large() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 999_999,
            initialized: false,
        };

        assert_eq!(parser.message_count(), 999_999);
    }

    // =========================================================================
    // StreamingParser::is_initialized tests
    // =========================================================================

    #[test]
    fn test_streaming_parser_is_initialized_false() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        assert!(!parser.is_initialized());
    }

    #[test]
    fn test_streaming_parser_is_initialized_true() {
        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: true,
        };

        assert!(parser.is_initialized());
    }

    // =========================================================================
    // AsStreamingParser trait tests (trait object compatibility)
    // =========================================================================

    #[test]
    fn test_streaming_parser_dyn_compatible() {
        // Verify StreamingParser can be used as a trait object
        fn use_parser(_parser: &dyn StreamingParser<Message = MockMessage>) {
            // This function exists to verify trait object compatibility
        }

        let parser = MockParser {
            channels: HashMap::new(),
            message_count: 0,
            initialized: false,
        };

        use_parser(&parser);
    }

    // =========================================================================
    // MockMessage tests
    // =========================================================================

    #[test]
    fn test_mock_message_clone() {
        let msg = MockMessage {
            data: vec![1, 2, 3],
        };
        let cloned = msg.clone();
        assert_eq!(msg, cloned);
    }

    #[test]
    fn test_mock_message_partial_eq() {
        let msg1 = MockMessage {
            data: vec![1, 2, 3],
        };
        let msg2 = MockMessage {
            data: vec![1, 2, 3],
        };
        let msg3 = MockMessage {
            data: vec![4, 5, 6],
        };
        assert_eq!(msg1, msg2);
        assert_ne!(msg1, msg3);
    }
}
