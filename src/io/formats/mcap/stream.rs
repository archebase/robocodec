// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming MCAP parser.
//!
//! This module provides a zero-copy streaming parser that can parse MCAP records
//! from byte chunks as they arrive from any transport (S3, HTTP, etc.).

use std::collections::HashMap;

use super::constants::{
    MCAP_MAGIC, OP_ATTACHMENT, OP_ATTACHMENT_INDEX, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX,
    OP_DATA_END, OP_FOOTER, OP_HEADER, OP_MESSAGE, OP_MESSAGE_INDEX, OP_METADATA,
    OP_METADATA_INDEX, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};
use crate::io::metadata::ChannelInfo;
use crate::io::s3::FatalError;

/// MCAP record header as parsed from the stream.
///
/// **DEPRECATED**: This type is part of the old streaming API.
/// Use [`McapStreamingParser`] instead.
///
/// [`McapStreamingParser`]: crate::io::formats::mcap::streaming::McapStreamingParser
#[deprecated(since = "0.1.0", note = "Use McapStreamingParser instead")]
#[derive(Debug, Clone, PartialEq)]
pub struct McapRecordHeader {
    /// Record opcode
    pub opcode: u8,
    /// Record body length
    pub length: u64,
}

/// Parsed MCAP record with header and body.
///
/// **DEPRECATED**: This type is part of the old streaming API.
/// Use [`McapStreamingParser`] instead.
///
/// [`McapStreamingParser`]: crate::io::formats::mcap::streaming::McapStreamingParser
#[deprecated(since = "0.1.0", note = "Use McapStreamingParser instead")]
#[derive(Debug, Clone)]
pub struct McapRecord {
    /// Record header
    #[allow(deprecated)]
    pub header: McapRecordHeader,
    /// Record body data
    pub body: Vec<u8>,
}

/// Schema information from MCAP Schema record.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// Schema ID
    pub id: u16,
    /// Schema name (e.g., "sensor_msgs/msg/Image")
    pub name: String,
    /// Schema encoding (e.g., "ros2msg", "protobuf")
    pub encoding: String,
    /// Schema data
    pub data: Vec<u8>,
}

/// Channel information from MCAP Channel record.
#[derive(Debug, Clone)]
pub struct ChannelRecordInfo {
    /// Channel ID
    pub id: u16,
    /// Topic name
    pub topic: String,
    /// Message encoding (e.g., "cdr", "protobuf", "json")
    pub message_encoding: String,
    /// Schema ID (0 if none)
    pub schema_id: u16,
}

/// Message data from MCAP Message record.
#[derive(Debug, Clone)]
pub struct MessageRecord {
    /// Channel ID
    pub channel_id: u16,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds)
    pub publish_time: u64,
    /// Message data
    pub data: Vec<u8>,
    /// Sequence number
    pub sequence: u64,
}

/// Streaming MCAP parser.
///
/// **DEPRECATED**: Use [`McapStreamingParser`] or [`McapTransportReader`] instead,
/// which provide better compatibility with the unified transport layer and
/// the `mcap` crate's `LinearReader` for more robust parsing.
///
/// This parser maintains state across chunks and can parse MCAP records
/// incrementally as data arrives from any byte stream.
///
/// [`McapStreamingParser`]: crate::io::formats::mcap::streaming::McapStreamingParser
/// [`McapTransportReader`]: crate::io::formats::mcap::transport_reader::McapTransportReader
#[deprecated(
    since = "0.1.0",
    note = "Use McapStreamingParser or McapTransportReader for better compatibility with the transport layer"
)]
pub struct StreamingMcapParser {
    /// Discovered schemas indexed by schema ID
    schemas: HashMap<u16, SchemaInfo>,
    /// Discovered channels indexed by channel ID
    channels: HashMap<u16, ChannelRecordInfo>,
    /// Buffered partial record data from previous chunk
    buffer: Vec<u8>,
    /// Current parse state
    state: ParserState,
    /// Expected bytes remaining for current record
    remaining: u64,
    /// Current record opcode being parsed
    current_opcode: u8,
    /// Total messages parsed
    message_count: u64,
    /// Position within the buffer
    buffer_pos: usize,
}

#[allow(deprecated)]
impl StreamingMcapParser {
    /// Create a new streaming MCAP parser.
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            channels: HashMap::new(),
            buffer: Vec::new(),
            state: ParserState::NeedMagic,
            remaining: 0,
            current_opcode: 0,
            message_count: 0,
            buffer_pos: 0,
        }
    }

    /// Parse MCAP data from a chunk of bytes.
    ///
    /// Returns any complete records found in this chunk.
    ///
    /// # Arguments
    ///
    /// * `data` - A chunk of bytes from the MCAP file
    ///
    /// # Returns
    ///
    /// A vector of parsed message records. Schema and Channel records
    /// are stored internally and accessible via `channels()`.
    pub fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<MessageRecord>, FatalError> {
        // Append new data to buffer
        self.buffer.extend_from_slice(data);
        let mut messages = Vec::new();

        // Process all complete records from the buffer
        loop {
            let processed = self.process_one_record(&mut messages)?;
            if !processed {
                break;
            }
        }

        // Compact buffer if we've consumed a lot of data
        if self.buffer_pos > 1024 * 1024 {
            let remaining = self.buffer.len() - self.buffer_pos;
            self.buffer.copy_within(self.buffer_pos.., 0);
            self.buffer.truncate(remaining);
            self.buffer_pos = 0;
        }

        self.message_count += messages.len() as u64;
        Ok(messages)
    }

    /// Process one record from the buffer.
    /// Returns true if a record was processed, false if we need more data.
    fn process_one_record(
        &mut self,
        messages: &mut Vec<MessageRecord>,
    ) -> Result<bool, FatalError> {
        let available = self.buffer.len() - self.buffer_pos;

        match self.state {
            ParserState::NeedMagic => {
                if available < MCAP_MAGIC.len() {
                    return Ok(false);
                }

                // Verify magic
                let magic_slice = &self.buffer[self.buffer_pos..self.buffer_pos + MCAP_MAGIC.len()];
                if magic_slice != MCAP_MAGIC {
                    return Err(FatalError::invalid_format(
                        "MCAP magic",
                        magic_slice.to_vec(),
                    ));
                }

                self.buffer_pos += MCAP_MAGIC.len();
                self.state = ParserState::NeedRecordHeader;
                Ok(true)
            }
            ParserState::NeedRecordHeader => {
                // MCAP record header: opcode (1 byte) + length (8 bytes LE) = 9 bytes
                let header_bytes = 9;
                if available < header_bytes {
                    return Ok(false);
                }

                let slice = &self.buffer[self.buffer_pos..];

                // Read opcode
                self.current_opcode = slice[0];

                // Read length (little-endian u64 at offset 1)
                let length_bytes: [u8; 8] = slice[1..9]
                    .try_into()
                    .expect("slice has exactly 9 bytes after checking available >= 9");
                self.remaining = u64::from_le_bytes(length_bytes);

                // Validate record length
                if self.remaining > 100 * 1024 * 1024 {
                    return Err(FatalError::invalid_format(
                        "MCAP record length > 100MB",
                        vec![],
                    ));
                }

                self.buffer_pos += header_bytes;
                self.state = ParserState::NeedRecordBody;
                Ok(true)
            }
            ParserState::NeedRecordBody => {
                let available = (self.buffer.len() - self.buffer_pos) as u64;

                if available < self.remaining {
                    return Ok(false); // Need more data
                }

                // We have the full record body
                let start = self.buffer_pos;
                let end = start + self.remaining as usize;
                let body = self.buffer[start..end].to_vec();
                self.buffer_pos = end;

                // Process the record
                self.process_record(self.current_opcode, &body, messages)?;

                // Reset for next record
                self.state = ParserState::NeedRecordHeader;
                self.remaining = 0;
                Ok(true)
            }
        }
    }

    /// Process a complete MCAP record.
    fn process_record(
        &mut self,
        opcode: u8,
        body: &[u8],
        messages: &mut Vec<MessageRecord>,
    ) -> Result<(), FatalError> {
        match opcode {
            OP_HEADER => {
                // Header record - just verify it's valid
                if body.len() < 4 {
                    return Err(FatalError::invalid_format("MCAP Header record", vec![]));
                }
                // No metadata to extract from Header
            }
            OP_SCHEMA => {
                // Schema record - extract schema info
                let schema = self.parse_schema(body)?;
                self.schemas.insert(schema.id, schema);
            }
            OP_CHANNEL => {
                // Channel record - extract channel info
                let channel = self.parse_channel(body)?;
                self.channels.insert(channel.id, channel);
            }
            OP_MESSAGE => {
                // Message record - extract message
                let msg = self.parse_message(body)?;
                messages.push(msg);
            }
            OP_FOOTER | OP_DATA_END | OP_CHUNK | OP_CHUNK_INDEX | OP_MESSAGE_INDEX
            | OP_ATTACHMENT | OP_ATTACHMENT_INDEX | OP_STATISTICS | OP_METADATA
            | OP_METADATA_INDEX | OP_SUMMARY_OFFSET => {
                // Ignore these records for streaming
            }
            _ => {
                // Unknown opcode - this might indicate file corruption or version mismatch
                return Err(FatalError::io_error(format!(
                    "Unknown MCAP opcode: 0x{:02x}",
                    opcode
                )));
            }
        }
        Ok(())
    }

    /// Parse a Schema record.
    fn parse_schema(&self, body: &[u8]) -> Result<SchemaInfo, FatalError> {
        if body.len() < 6 {
            return Err(FatalError::invalid_format(
                "MCAP Schema record (need at least 6 bytes)",
                body[..body.len().min(10)].to_vec(),
            ));
        }

        let id = u16::from_le_bytes(
            body[0..2]
                .try_into()
                .expect("slice is exactly 2 bytes after len >= 6 check"),
        );
        let name_len = u16::from_le_bytes(
            body[2..4]
                .try_into()
                .expect("slice is exactly 2 bytes after len >= 6 check"),
        ) as usize;

        if body.len() < 4 + name_len {
            return Err(FatalError::invalid_format(
                "MCAP Schema name (incomplete)",
                vec![],
            ));
        }

        let name = String::from_utf8(body[4..4 + name_len].to_vec())
            .map_err(|_| FatalError::invalid_format("MCAP Schema name (invalid UTF-8)", vec![]))?;

        let offset = 4 + name_len;
        if body.len() < offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Schema encoding length",
                vec![],
            ));
        }

        let encoding_len = u16::from_le_bytes(
            body[offset..offset + 2]
                .try_into()
                .expect("slice is exactly 2 bytes after len check"),
        ) as usize;
        if body.len() < offset + 2 + encoding_len {
            return Err(FatalError::invalid_format(
                "MCAP Schema encoding (incomplete)",
                vec![],
            ));
        }

        let encoding = String::from_utf8(body[offset + 2..offset + 2 + encoding_len].to_vec())
            .map_err(|_| {
                FatalError::invalid_format("MCAP Schema encoding (invalid UTF-8)", vec![])
            })?;

        let data_start = offset + 2 + encoding_len;
        let data = body[data_start..].to_vec();

        Ok(SchemaInfo {
            id,
            name,
            encoding,
            data,
        })
    }

    /// Parse a Channel record.
    fn parse_channel(&self, body: &[u8]) -> Result<ChannelRecordInfo, FatalError> {
        if body.len() < 6 {
            return Err(FatalError::invalid_format(
                "MCAP Channel record (need at least 6 bytes)",
                body[..body.len().min(10)].to_vec(),
            ));
        }

        let id = u16::from_le_bytes(
            body[0..2]
                .try_into()
                .expect("slice is exactly 2 bytes after len >= 6 check"),
        );
        let topic_len = u16::from_le_bytes(
            body[2..4]
                .try_into()
                .expect("slice is exactly 2 bytes after len >= 6 check"),
        ) as usize;

        if body.len() < 4 + topic_len {
            return Err(FatalError::invalid_format(
                "MCAP Channel topic (incomplete)",
                vec![],
            ));
        }

        let topic = String::from_utf8(body[4..4 + topic_len].to_vec()).map_err(|_| {
            FatalError::invalid_format("MCAP Channel topic (invalid UTF-8)", vec![])
        })?;

        let offset = 4 + topic_len;
        if body.len() < offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Channel encoding length",
                vec![],
            ));
        }

        let encoding_len = u16::from_le_bytes(
            body[offset..offset + 2]
                .try_into()
                .expect("slice is exactly 2 bytes after len check"),
        ) as usize;
        if body.len() < offset + 2 + encoding_len {
            return Err(FatalError::invalid_format(
                "MCAP Channel message encoding (incomplete)",
                vec![],
            ));
        }

        let message_encoding = String::from_utf8(
            body[offset + 2..offset + 2 + encoding_len].to_vec(),
        )
        .map_err(|_| FatalError::invalid_format("MCAP Channel encoding (invalid UTF-8)", vec![]))?;

        let schema_offset = offset + 2 + encoding_len;
        if body.len() < schema_offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Channel schema id (incomplete)",
                vec![],
            ));
        }

        let schema_id = u16::from_le_bytes(
            body[schema_offset..schema_offset + 2]
                .try_into()
                .expect("slice is exactly 2 bytes after len check"),
        );

        Ok(ChannelRecordInfo {
            id,
            topic,
            message_encoding,
            schema_id,
        })
    }

    /// Parse a Message record.
    fn parse_message(&self, body: &[u8]) -> Result<MessageRecord, FatalError> {
        if body.len() < 20 {
            return Err(FatalError::invalid_format(
                "MCAP Message record (need at least 20 bytes)",
                body[..body.len().min(10)].to_vec(),
            ));
        }

        let channel_id = u16::from_le_bytes(
            body[0..2]
                .try_into()
                .expect("slice is exactly 2 bytes after len >= 20 check"),
        );
        let sequence = u64::from_le_bytes(
            body[2..10]
                .try_into()
                .expect("slice is exactly 8 bytes after len >= 20 check"),
        );
        let log_time = u64::from_le_bytes(
            body[10..18]
                .try_into()
                .expect("slice is exactly 8 bytes after len >= 20 check"),
        );
        let publish_time = u64::from_le_bytes(
            body[18..26]
                .try_into()
                .expect("slice is exactly 8 bytes after len >= 20 check"),
        );

        let data = body[20..].to_vec();

        Ok(MessageRecord {
            channel_id,
            log_time,
            publish_time,
            data,
            sequence,
        })
    }

    /// Get all discovered channels as ChannelInfo.
    pub fn channels(&self) -> HashMap<u16, ChannelInfo> {
        self.channels
            .iter()
            .map(|(id, ch)| {
                let schema = self.schemas.get(&ch.schema_id);
                let schema_text = schema.and_then(|s| String::from_utf8(s.data.clone()).ok());
                let schema_data = schema.map(|s| s.data.clone());
                let schema_encoding = schema.map(|s| s.encoding.clone());

                let message_type = schema.map(|s| s.name.clone()).unwrap_or_default();

                (
                    *id,
                    ChannelInfo {
                        id: *id,
                        topic: ch.topic.clone(),
                        message_type,
                        encoding: ch.message_encoding.clone(),
                        schema: schema_text,
                        schema_data,
                        schema_encoding,
                        message_count: 0, // Will be updated during iteration
                        callerid: None,
                    },
                )
            })
            .collect()
    }

    /// Get the total message count.
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Check if the parser has seen all channels.
    pub fn has_channels(&self) -> bool {
        !self.channels.is_empty()
    }

    /// Check if we've seen the magic bytes.
    pub fn is_initialized(&self) -> bool {
        !matches!(self.state, ParserState::NeedMagic)
    }
}

#[allow(deprecated)]
impl Default for StreamingMcapParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parser state for streaming MCAP parsing.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum ParserState {
    /// Waiting for magic bytes
    NeedMagic,
    /// Waiting for record header (opcode + length)
    NeedRecordHeader,
    /// Waiting for record body
    NeedRecordBody,
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_new() {
        let parser = StreamingMcapParser::new();
        assert!(!parser.is_initialized());
        assert!(!parser.has_channels());
        assert_eq!(parser.message_count(), 0);
    }

    #[test]
    fn test_parser_default() {
        let parser = StreamingMcapParser::default();
        assert_eq!(parser.message_count(), 0);
    }

    #[test]
    fn test_record_header() {
        let header = McapRecordHeader {
            opcode: OP_MESSAGE,
            length: 100,
        };
        assert_eq!(header.opcode, OP_MESSAGE);
        assert_eq!(header.length, 100);
    }

    #[test]
    fn test_schema_info() {
        let schema = SchemaInfo {
            id: 1,
            name: "test_msgs/Msg".to_string(),
            encoding: "ros2msg".to_string(),
            data: b"# definition".to_vec(),
        };
        assert_eq!(schema.id, 1);
        assert_eq!(schema.name, "test_msgs/Msg");
        assert_eq!(schema.encoding, "ros2msg");
    }

    #[test]
    fn test_channel_record_info() {
        let channel = ChannelRecordInfo {
            id: 1,
            topic: "/test".to_string(),
            message_encoding: "cdr".to_string(),
            schema_id: 0,
        };
        assert_eq!(channel.id, 1);
        assert_eq!(channel.topic, "/test");
        assert_eq!(channel.message_encoding, "cdr");
    }

    #[test]
    fn test_message_record() {
        let msg = MessageRecord {
            channel_id: 1,
            log_time: 1000,
            publish_time: 900,
            data: vec![1, 2, 3],
            sequence: 5,
        };
        assert_eq!(msg.channel_id, 1);
        assert_eq!(msg.log_time, 1000);
        assert_eq!(msg.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_parser_state() {
        assert_eq!(ParserState::NeedMagic, ParserState::NeedMagic);
        assert_eq!(ParserState::NeedRecordHeader, ParserState::NeedRecordHeader);
        assert_eq!(ParserState::NeedRecordBody, ParserState::NeedRecordBody);
    }

    #[test]
    fn test_parse_magic() {
        let mut parser = StreamingMcapParser::new();

        // Too short - should not error, just not advance
        let result = parser.parse_chunk(&MCAP_MAGIC[..4]);
        assert!(result.is_ok());
        assert!(!parser.is_initialized());

        // Full magic
        let result = parser.parse_chunk(&MCAP_MAGIC[4..]);
        assert!(result.is_ok());
        assert!(parser.is_initialized());
    }

    #[test]
    fn test_parse_schema_simple() {
        // Create a minimal Schema record:
        // id=1, name="TestMsg" (7 bytes), encoding="ros2msg" (7 bytes), data=b"# test"
        // id: 2 bytes = 0x01 0x00
        // name_len: 2 bytes = 0x07 0x00
        // name: 7 bytes = "TestMsg"
        // encoding_len: 2 bytes = 0x07 0x00
        // encoding: 7 bytes = "ros2msg"
        // data: 6 bytes = "# test"
        let schema_bytes = [
            0x01, 0x00, // id
            0x07, 0x00, // name_len
            b'T', b'e', b's', b't', b'M', b's', b'g', // name
            0x07, 0x00, // encoding_len
            b'r', b'o', b's', b'2', b'm', b's', b'g', // encoding
            b'#', b' ', b't', b'e', b's', b't', // data
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 22, 0, 0, 0, 0, 0, 0, 0])
            .unwrap(); // header

        let result = parser.parse_chunk(&schema_bytes);
        assert!(result.is_ok(), "Schema parse should succeed: {:?}", result);
        assert_eq!(parser.channels().len(), 0, "No channels yet");
    }

    #[test]
    fn test_parse_schema_with_zero_length_encoding() {
        // Test a schema where the encoding field itself is 0 length
        // This might be the issue - some schemas have empty encoding strings
        let schema_bytes = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name = "Foo"
            0x00, 0x00, // encoding_len = 0
            // No encoding bytes
            b'#', b' ', b't', b'e', b's', b't', // data
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 15, 0, 0, 0, 0, 0, 0, 0])
            .unwrap(); // header (body length = 15)

        let result = parser.parse_chunk(&schema_bytes);
        assert!(
            result.is_ok(),
            "Schema with 0-length encoding should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_parse_schema_with_large_name_len() {
        // Test what happens if name_len is larger than the body
        // This could happen if the record length is wrong
        let schema_bytes = [
            0x01, 0x00, // id = 1
            0xFF, 0xFF, // name_len = 65535 (way too large)
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 4, 0, 0, 0, 0, 0, 0, 0])
            .unwrap(); // header (body length = 4)

        let result = parser.parse_chunk(&schema_bytes);
        assert!(
            result.is_err(),
            "Should fail when name_len exceeds body length"
        );
    }

    #[test]
    fn test_channel_record_body() {
        // Test parsing a complete Channel record body
        let channel_body = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // topic_len = 3
            b'/', b'c', b'h', // topic = "/ch"
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding = "cdr"
            0x00, 0x00, // schema_id = 0
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_ok());
        assert!(parser.has_channels());
    }

    #[test]
    fn test_message_record_body() {
        // Test parsing a complete Message record body
        let message_body = [
            0x01, 0x00, // channel_id = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence = 0
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_time = 16
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // publish_time = 17
            b'd', b'a', b't', b'a', // data
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Add channel first
        let channel_body = [
            0x01, 0x00, 0x03, 0x00, b'/', b'c', b'h', 0x03, 0x00, b'c', b'd', b'r', 0x00, 0x00,
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel_body).unwrap();

        // Add message
        parser
            .parse_chunk(&[OP_MESSAGE, 30, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&message_body);
        assert!(result.is_ok());
        assert_eq!(parser.message_count(), 1);
    }

    #[test]
    fn test_schema_too_short() {
        // Test schema record with < 6 bytes
        let schema_body = [0x01, 0x00, 0x03, 0x00]; // only 4 bytes
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 4, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_incomplete_name() {
        // Test schema where name_len says 5 but only 3 bytes available
        let schema_body = [
            0x01, 0x00, // id
            0x05, 0x00, // name_len = 5
            b'F', b'o', b'o', // only 3 bytes of name
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 7, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_invalid_utf8_name() {
        // Test schema with invalid UTF-8 in name
        let schema_body = [
            0x01, 0x00, // id
            0x03, 0x00, // name_len = 3
            0xFF, 0xFE, 0xFD, // invalid UTF-8
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            b'#', b'd', // data (2 bytes to make body 14 bytes total)
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_err(), "Should fail for invalid UTF-8 in name");
    }

    #[test]
    fn test_schema_incomplete_encoding() {
        // Test schema where encoding_len says 5 but only 2 bytes available
        let schema_body = [
            0x01, 0x00, // id
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name
            0x05, 0x00, // encoding_len = 5
            b'c', b'd', // only 2 bytes
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 10, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_err(), "Should fail for incomplete encoding");
    }

    #[test]
    fn test_schema_invalid_utf8_encoding() {
        // Test schema with invalid UTF-8 in encoding
        let schema_body = [
            0x01, 0x00, // id
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name
            0x03, 0x00, // encoding_len = 3
            0xFF, 0xFE, 0xFD, // invalid UTF-8
            b'#', // data (1 byte to make body 13 bytes total)
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 13, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_err(), "Should fail for invalid UTF-8 in encoding");
    }

    #[test]
    fn test_channel_too_short() {
        // Test channel record with < 6 bytes
        let channel_body = [0x01, 0x00, 0x03]; // only 3 bytes
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 3, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_incomplete_topic() {
        // Test channel where topic_len says 5 but only 2 bytes available
        let channel_body = [
            0x01, 0x00, // id
            0x05, 0x00, // topic_len = 5
            b'/', b'c', // only 2 bytes of topic
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 6, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_invalid_utf8_topic() {
        // Test channel with invalid UTF-8 in topic
        let channel_body = [
            0x01, 0x00, // id
            0x03, 0x00, // topic_len = 3
            0xFF, 0xFE, 0xFD, // invalid UTF-8
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x00, 0x00, // schema_id
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_incomplete_encoding() {
        // Test channel where encoding_len says 5 but only 2 bytes available
        let channel_body = [
            0x01, 0x00, // id
            0x03, 0x00, // topic_len = 3
            b'/', b'c', b'h', // topic
            0x05, 0x00, // encoding_len = 5
            b'c', b'd', // only 2 bytes of encoding
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 10, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_err(), "Should fail for incomplete encoding");
    }

    #[test]
    fn test_channel_invalid_utf8_encoding() {
        // Test channel with invalid UTF-8 in encoding
        let channel_body = [
            0x01, 0x00, // id
            0x03, 0x00, // topic_len = 3
            b'/', b'c', b'h', // topic
            0x03, 0x00, // encoding_len = 3
            0xFF, 0xFE, 0xFD, // invalid UTF-8
            0x00, 0x00, // schema_id
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_channel_incomplete_schema_id() {
        // Test channel where schema_id is incomplete (only 1 byte available)
        let channel_body = [
            0x01, 0x00, // id
            0x03, 0x00, // topic_len = 3
            b'/', b'c', b'h', // topic
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x00, // only 1 byte of schema_id
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 13, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_message_too_short() {
        // Test message record with < 20 bytes
        let message_body = [0x01, 0x00, 0x00]; // only 3 bytes
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_MESSAGE, 3, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&message_body);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_magic() {
        // Test with invalid magic bytes
        let invalid_magic = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut parser = StreamingMcapParser::new();
        let result = parser.parse_chunk(&invalid_magic);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_opcode() {
        // Test with unknown opcode (0xFF)
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser.parse_chunk(&[0xFF, 2, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        let result = parser.parse_chunk(&[0, 0]);
        assert!(result.is_err(), "Should fail for unknown opcode");
    }

    #[test]
    fn test_header_too_short() {
        // Test Header record with < 4 bytes
        let header_body = [0x01, 0x02]; // only 2 bytes
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_HEADER, 2, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&header_body);
        assert!(result.is_err(), "Should fail for short header");
    }

    #[test]
    fn test_chunk_compaction() {
        // Test buffer compaction when buffer_pos > 1MB
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Add enough data to trigger compaction (need >1MB after magic + headers)
        // After magic (8 bytes) + header (9 bytes) = 17 bytes consumed
        // Need to add >1MB more data to trigger compaction
        let large_data = vec![0u8; 2 * 1024 * 1024];
        let len_bytes = (large_data.len() as u64).to_le_bytes();
        let mut header = [OP_HEADER; 9];
        header[1..].copy_from_slice(&len_bytes);
        parser.parse_chunk(&header).unwrap();
        let result = parser.parse_chunk(&large_data);
        assert!(result.is_ok(), "Should handle large data chunks");
    }

    #[test]
    fn test_all_opcodes_accepted() {
        // Test that various opcodes are accepted without error
        let opcodes = [
            OP_FOOTER,
            OP_DATA_END,
            OP_CHUNK,
            OP_CHUNK_INDEX,
            OP_MESSAGE_INDEX,
            OP_ATTACHMENT,
            OP_ATTACHMENT_INDEX,
            OP_STATISTICS,
            OP_METADATA,
            OP_METADATA_INDEX,
            OP_SUMMARY_OFFSET,
        ];

        for opcode in opcodes {
            let mut parser = StreamingMcapParser::new();
            parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
            parser
                .parse_chunk(&[opcode, 4, 0, 0, 0, 0, 0, 0, 0])
                .unwrap();
            let result = parser.parse_chunk(&[0, 0, 0, 0]);
            assert!(result.is_ok(), "Opcode 0x{:02x} should be accepted", opcode);
        }
    }

    #[test]
    fn test_parser_message_count() {
        // Test message_count() method
        let mut parser = StreamingMcapParser::new();
        assert_eq!(parser.message_count(), 0);

        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        assert_eq!(parser.message_count(), 0);
    }

    #[test]
    fn test_partial_data_waiting() {
        // Test that partial data is buffered correctly
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Send only 4 bytes of the 9-byte header
        let partial_header = &[OP_CHANNEL, 13, 0, 0];
        let result = parser.parse_chunk(partial_header);
        assert!(result.is_ok()); // Should succeed but return no messages

        // Send rest of header
        let rest_header = &[0, 0, 0, 0, 0];
        let result = parser.parse_chunk(rest_header);
        assert!(result.is_ok());
    }

    #[test]
    fn test_channels_empty_initially() {
        // Test that channels() is empty initially
        let parser = StreamingMcapParser::new();
        assert!(parser.channels().is_empty());
    }

    #[test]
    fn test_empty_schema_encoding() {
        // Test schema with 0-length encoding (covered in existing test but let's be explicit)
        let schema_body = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name
            0x00, 0x00, // encoding_len = 0
            // No encoding bytes
            b'#', b't', b'e', b's', b't', // data
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 13, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_record_too_large() {
        // Test record length > 100MB validation
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Send header with length > 100MB (101 * 1024 * 1024)
        let large_len = (101 * 1024 * 1024u64).to_le_bytes();
        let mut header = [OP_HEADER; 9];
        header[1..].copy_from_slice(&large_len);
        let result = parser.parse_chunk(&header);
        assert!(result.is_err(), "Should reject record > 100MB");
    }

    #[test]
    fn test_channel_schema_info_methods() {
        // Test ChannelRecordInfo fields
        let channel = ChannelRecordInfo {
            id: 42,
            topic: "/test/topic".to_string(),
            message_encoding: "cdr".to_string(),
            schema_id: 1,
        };
        assert_eq!(channel.id, 42);
        assert_eq!(channel.topic, "/test/topic");
        assert_eq!(channel.message_encoding, "cdr");
        assert_eq!(channel.schema_id, 1);
    }

    #[test]
    fn test_schema_info_methods() {
        // Test SchemaInfo fields
        let schema = SchemaInfo {
            id: 10,
            name: "TestMsg".to_string(),
            encoding: "ros2msg".to_string(),
            data: vec![1, 2, 3],
        };
        assert_eq!(schema.id, 10);
        assert_eq!(schema.name, "TestMsg");
        assert_eq!(schema.encoding, "ros2msg");
        assert_eq!(schema.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_message_record_methods() {
        // Test MessageRecord fields
        let msg = MessageRecord {
            channel_id: 5,
            log_time: 1000,
            publish_time: 900,
            data: vec![b'x', b'y', b'z'],
            sequence: 123,
        };
        assert_eq!(msg.channel_id, 5);
        assert_eq!(msg.log_time, 1000);
        assert_eq!(msg.publish_time, 900);
        assert_eq!(msg.data, vec![b'x', b'y', b'z']);
        assert_eq!(msg.sequence, 123);
    }

    #[test]
    fn test_mcap_record_header() {
        // Test McapRecordHeader fields
        let header = McapRecordHeader {
            opcode: 0x05,
            length: 42,
        };
        assert_eq!(header.opcode, 0x05);
        assert_eq!(header.length, 42);
    }

    #[test]
    fn test_empty_channel_topic() {
        // Test channel with 0-length topic
        let channel_body = [
            0x01, 0x00, // id
            0x00, 0x00, // topic_len = 0
            // No topic bytes
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x00, 0x00, // schema_id
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 11, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_ok());
        assert!(parser.has_channels());
    }

    #[test]
    fn test_empty_channel_encoding() {
        // Test channel with 0-length encoding
        let channel_body = [
            0x01, 0x00, // id
            0x03, 0x00, // topic_len = 3
            b'/', b't', b't', // topic
            0x00, 0x00, // encoding_len = 0
            // No encoding bytes
            0x00, 0x00, // schema_id
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 11, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_message_with_empty_data() {
        // Test message with 0-length data
        let message_body = [
            0x01, 0x00, // channel_id = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence = 0
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_time = 16
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, // publish_time = 17
                  // No data bytes
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Add channel first
        let channel_body = [
            0x01, 0x00, 0x03, 0x00, b'/', b'c', b'h', 0x03, 0x00, b'c', b'd', b'r', 0x00, 0x00,
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel_body).unwrap();

        // Add message with empty data (26 bytes = no data after timestamps)
        parser
            .parse_chunk(&[OP_MESSAGE, 26, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&message_body);
        assert!(result.is_ok());
        assert_eq!(parser.message_count(), 1);
    }

    #[test]
    fn test_empty_schema_name() {
        // Test schema with 0-length name
        let schema_body = [
            0x01, 0x00, // id = 1
            0x00, 0x00, // name_len = 0
            // No name bytes
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
                  // No data bytes
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_SCHEMA, 10, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&schema_body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_partial_header_then_more_data() {
        // Test sending header byte by byte
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Send header one byte at a time (total 9 bytes: 1 opcode + 8 length)
        parser.parse_chunk(&[OP_CHANNEL]).unwrap();
        parser.parse_chunk(&[14, 0]).unwrap(); // body_len = 14
        parser.parse_chunk(&[0, 0, 0, 0, 0]).unwrap();
        parser.parse_chunk(&[0]).unwrap();

        // Now send the body (14 bytes)
        let channel_body = [
            0x01, 0x00, 0x03, 0x00, b'/', b'c', b'h', 0x03, 0x00, b'c', b'd', b'r', 0x00, 0x00,
        ];
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_need_record_body_waiting_state() {
        // Test parser waiting for more data in NeedRecordBody state
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Send header
        parser
            .parse_chunk(&[OP_CHANNEL, 20, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();

        // Send only partial body (less than 20 bytes)
        let partial_body = [0x01, 0x00, 0x03, 0x00, b'/']; // 5 bytes
        let result = parser.parse_chunk(&partial_body);
        assert!(result.is_ok()); // Should succeed but return no messages

        // Send rest of body
        let rest_body = [
            b'c', b'h', 0x03, 0x00, b'c', b'd', b'r', 0x00, 0x00, // 9 bytes
            b'x', b'x', b'x', b'x', b'x', b'x',
        ]; // 6 extra bytes for total 20
        let result = parser.parse_chunk(&rest_body);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_schemas_and_channels() {
        // Test multiple schemas and channels
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Add schema 1 (id=1, name="Sch1"(4), encoding="cdr"(3), data="#"(1))
        // Total: 2 + 2 + 4 + 2 + 3 + 1 = 14 bytes
        let schema1_body = [
            0x01, 0x00, // id = 1
            0x04, 0x00, // name_len = 4
            b'S', b'c', b'h', b'1', // name
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            b'#', // data
        ];
        parser
            .parse_chunk(&[OP_SCHEMA, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&schema1_body).unwrap();

        // Add schema 2 (id=2, name="Sch2"(4), encoding="cdr"(3), data="#"(1))
        let schema2_body = [
            0x02, 0x00, // id = 2
            0x04, 0x00, // name_len = 4
            b'S', b'c', b'h', b'2', // name
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            b'#', // data
        ];
        parser
            .parse_chunk(&[OP_SCHEMA, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&schema2_body).unwrap();

        // Add channel 1 (id=1, topic="/ch1"(4), encoding="cdr"(3), schema_id=1)
        let channel1_body = [
            0x01, 0x00, // id = 1
            0x04, 0x00, // topic_len = 4
            b'/', b'c', b'h', b'1', // topic
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x01, 0x00, // schema_id = 1
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 15, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel1_body).unwrap();

        // Add channel 2 (id=2, topic="/ch2"(4), encoding="cdr"(3), schema_id=2)
        let channel2_body = [
            0x02, 0x00, // id = 2
            0x04, 0x00, // topic_len = 4
            b'/', b'c', b'h', b'2', // topic
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x02, 0x00, // schema_id = 2
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 15, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel2_body).unwrap();

        assert_eq!(parser.channels().len(), 2);
        assert!(parser.channels().contains_key(&1));
        assert!(parser.channels().contains_key(&2));
    }

    #[test]
    fn test_state_transitions() {
        // Test state transitions through the parser lifecycle
        let parser = StreamingMcapParser::new();
        assert!(!parser.is_initialized());

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        assert!(parser.is_initialized());

        // After magic, parser should be in NeedRecordHeader state
        // (we can't directly check state but we can verify behavior)
    }

    #[test]
    fn test_channel_info_conversion() {
        // Test that channels() properly converts to ChannelInfo
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Add schema with valid UTF-8 data
        // Total: 2 + 2 + 11 + 2 + 3 + 6 = 26 bytes
        let schema_body = [
            0x01, 0x00, // id = 1
            0x0B, 0x00, // name_len = 11
            b'T', b'e', b's', b't', b'M', b's', b'g', b'T', b'y', b'p', b'e', // name
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            b'#', b' ', b't', b'e', b's', b't', // data (valid UTF-8)
        ];
        parser
            .parse_chunk(&[OP_SCHEMA, 26, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&schema_body).unwrap();

        // Add channel referencing the schema
        // Total: 2 + 2 + 5 + 2 + 3 + 2 = 16 bytes
        let channel_body = [
            0x01, 0x00, // id = 1
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x01, 0x00, // schema_id = 1
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 16, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel_body).unwrap();

        // Check channels() output
        let channels = parser.channels();
        assert_eq!(channels.len(), 1);
        let channel = channels.get(&1).unwrap();
        assert_eq!(channel.id, 1);
        assert_eq!(channel.topic, "/test");
        assert_eq!(channel.message_type, "TestMsgType");
        assert_eq!(channel.encoding, "cdr");
        assert_eq!(channel.schema, Some("# test".to_string()));
        assert_eq!(channel.schema_encoding, Some("cdr".to_string()));
    }

    #[test]
    fn test_channel_without_schema() {
        // Test channel with schema_id=0 (no schema)
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        let channel_body = [
            0x01, 0x00, // id = 1
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x00, 0x00, // schema_id = 0 (no schema)
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 16, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel_body).unwrap();

        let channels = parser.channels();
        assert_eq!(channels.len(), 1);
        let channel = channels.get(&1).unwrap();
        assert_eq!(channel.message_type, ""); // Default when no schema
        assert_eq!(channel.schema, None);
        assert_eq!(channel.schema_encoding, None);
    }

    #[test]
    fn test_schema_with_non_utf8_data() {
        // Test schema where data is not valid UTF-8
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Total: 2 + 2 + 3 + 2 + 3 + 3 = 15 bytes
        let schema_body = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0xFF, 0xFE, 0xFD, // data (invalid UTF-8)
        ];
        parser
            .parse_chunk(&[OP_SCHEMA, 15, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&schema_body).unwrap();

        let channel_body = [
            0x01, 0x00, 0x03, 0x00, b'/', b'c', b'h', 0x03, 0x00, b'c', b'd', b'r', 0x01, 0x00,
        ];
        parser
            .parse_chunk(&[OP_CHANNEL, 14, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel_body).unwrap();

        let channels = parser.channels();
        // Channel should exist since we added it
        assert!(channels.contains_key(&1));
        let channel = channels.get(&1).unwrap();
        assert_eq!(channel.schema, None); // Non-UTF8 data returns None
        assert!(channel.schema_data.is_some()); // But raw data is still available
    }

    #[test]
    fn test_channel_record_info_display() {
        // Test ChannelRecordInfo can be displayed/printed
        let info = ChannelRecordInfo {
            id: 1,
            topic: "/test".to_string(),
            message_encoding: "cdr".to_string(),
            schema_id: 0,
        };
        // Just verify the struct works - the Debug trait should work
        assert!(format!("{:?}", info).contains("ChannelRecordInfo"));
    }

    #[test]
    fn test_mcap_record_display() {
        // Test McapRecord can be displayed/printed
        let record = McapRecord {
            header: McapRecordHeader {
                opcode: 0x05,
                length: 42,
            },
            body: vec![1, 2, 3, 4],
        };
        assert!(format!("{:?}", record).contains("McapRecord"));
    }

    #[test]
    fn test_message_record_creation() {
        // Test MessageRecord creation and field access
        let msg = MessageRecord {
            channel_id: 100,
            log_time: 999999,
            publish_time: 888888,
            data: vec![0xAB, 0xCD],
            sequence: 42,
        };
        assert_eq!(msg.channel_id, 100);
        assert_eq!(msg.log_time, 999999);
        assert_eq!(msg.publish_time, 888888);
        assert_eq!(msg.data, vec![0xAB, 0xCD]);
        assert_eq!(msg.sequence, 42);
    }

    #[test]
    fn test_schema_info_creation() {
        // Test SchemaInfo creation
        let schema = SchemaInfo {
            id: 5,
            name: "TestType".to_string(),
            encoding: "protobuf".to_string(),
            data: vec![0x10, 0x20, 0x30],
        };
        assert_eq!(schema.id, 5);
        assert_eq!(schema.name, "TestType");
        assert_eq!(schema.encoding, "protobuf");
        assert_eq!(schema.data, vec![0x10, 0x20, 0x30]);
    }

    #[test]
    fn test_channel_with_max_schema_id() {
        // Test channel with maximum schema_id (u16::MAX = 65535)
        let channel_body = [
            0x01, 0x00, // id = 1
            0x01, 0x00, // topic_len = 1
            b'/', // topic
            0x01, 0x00, // encoding_len = 1
            b'x', // encoding
            0xFF, 0xFF, // schema_id = 65535 (max u16)
        ];
        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();
        parser
            .parse_chunk(&[OP_CHANNEL, 10, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&channel_body);
        assert!(result.is_ok());
        let channels = parser.channels();
        let ch = channels.get(&1).unwrap();
        // Schema won't exist, so message_type will be empty
        assert_eq!(ch.message_type, "");
    }

    #[test]
    fn test_message_with_max_channel_id() {
        // Test message with maximum channel_id
        let message_body = [
            0xFF, 0xFF, // channel_id = 65535 (max u16)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence = 0
            0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_time = 16
            0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // publish_time = 17
        ];

        let mut parser = StreamingMcapParser::new();
        parser.parse_chunk(&MCAP_MAGIC[..]).unwrap();

        // Add channel first
        let channel_body = [0xFF, 0xFF, 0x01, 0x00, b'/', 0x01, 0x00, b'x', 0x00, 0x00];
        parser
            .parse_chunk(&[OP_CHANNEL, 10, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        parser.parse_chunk(&channel_body).unwrap();

        // Add message
        parser
            .parse_chunk(&[OP_MESSAGE, 26, 0, 0, 0, 0, 0, 0, 0])
            .unwrap();
        let result = parser.parse_chunk(&message_body);
        assert!(result.is_ok());
    }
}
