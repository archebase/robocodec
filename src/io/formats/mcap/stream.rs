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
#[derive(Debug, Clone, PartialEq)]
pub struct McapRecordHeader {
    /// Record opcode
    pub opcode: u8,
    /// Record body length
    pub length: u64,
}

/// Parsed MCAP record with header and body.
#[derive(Debug, Clone)]
pub struct McapRecord {
    /// Record header
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
/// This parser maintains state across chunks and can parse MCAP records
/// incrementally as data arrives from any byte stream.
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
}
