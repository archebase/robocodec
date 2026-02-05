// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming RRD parser (RRF2 format).
//!
//! This module provides a streaming parser for Rerun RRD (RRF2) files that can parse
//! RRD data from byte chunks as they arrive, without requiring the
//! entire file to be available locally.
//!
//! The RRF2 format structure:
//! - StreamHeader (12 bytes): magic(4) + version(4) + options(4)
//! - Messages: MessageHeader(16) + payload(var)
//! - StreamFooter (32 bytes): entries(20) + magic(4) + identifier(4) + count(4)

use std::collections::HashMap;

use crate::io::formats::rrd::arrow_msg::ArrowMsg;
use crate::io::formats::rrd::constants::{
    COMPRESSION_LZ4, COMPRESSION_OFF, DEFAULT_TOPIC, MESSAGE_HEADER_SIZE, MSG_KIND_ARROW_MSG,
    MSG_KIND_BLUEPRINT_ACTIVATION_COMMAND, MSG_KIND_END, MSG_KIND_SET_STORE_INFO, OLD_RRD_MAGIC,
    RRD_FOOTER_ID, RRD_MAGIC, SERIALIZER_MSGPACK, SERIALIZER_PROTOBUF, STREAM_FOOTER_SIZE,
    STREAM_HEADER_SIZE,
};
use crate::io::metadata::ChannelInfo;
use crate::io::s3::{FatalError, StreamingParser};

/// RRD magic for streaming (RRF2).
pub const RRD_STREAM_MAGIC: &[u8; 4] = RRD_MAGIC;

/// Message kind in RRF2 format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    End = 0,
    SetStoreInfo = 1,
    ArrowMsg = 2,
    BlueprintActivationCommand = 3,
}

impl MessageKind {
    /// Create from u64 value.
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            MSG_KIND_END => Some(Self::End),
            MSG_KIND_SET_STORE_INFO => Some(Self::SetStoreInfo),
            MSG_KIND_ARROW_MSG => Some(Self::ArrowMsg),
            MSG_KIND_BLUEPRINT_ACTIVATION_COMMAND => Some(Self::BlueprintActivationCommand),
            _ => None,
        }
    }

    /// Convert to u64.
    pub fn as_u64(self) -> u64 {
        self as u64
    }
}

/// Compression type in RRF2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Off,
    Lz4,
}

impl Compression {
    /// Create from u8 value.
    pub fn from_u8(value: u8) -> Result<Self, FatalError> {
        match value {
            COMPRESSION_OFF => Ok(Self::Off),
            COMPRESSION_LZ4 => Ok(Self::Lz4),
            _ => Err(FatalError::ConfigError {
                message: format!("Unknown compression type: {value}"),
            }),
        }
    }

    /// Convert to u8.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Off => COMPRESSION_OFF,
            Self::Lz4 => COMPRESSION_LZ4,
        }
    }
}

/// RRD stream header (12 bytes).
#[derive(Debug, Clone)]
pub struct RrdStreamHeader {
    /// Magic number (RRF2)
    pub magic: [u8; 4],
    /// Version (4 bytes)
    pub version: [u8; 4],
    /// Compression type
    pub compression: Compression,
    /// Serializer (always Protobuf = 2 for RRF2)
    pub serializer: u8,
}

impl RrdStreamHeader {
    /// Size of the stream header in bytes.
    pub const SIZE: usize = STREAM_HEADER_SIZE;
}

/// Message data from RRD.
#[derive(Debug, Clone)]
pub struct RrdMessageRecord {
    /// Message kind
    pub kind: MessageKind,
    /// Entity path (topic) - extracted from message or default
    pub topic: String,
    /// Message data (Protobuf encoded)
    pub data: Vec<u8>,
    /// Raw message index
    pub index: u64,
}

/// Parser state for RRD streaming.
#[derive(Debug, Clone, PartialEq)]
enum ParserState {
    /// Need to read magic number
    NeedMagic,
    /// Need to read stream header
    NeedHeader,
    /// Need to read message header
    NeedMessageHeader,
    /// Need to read message payload
    NeedMessagePayload { kind: MessageKind, len: usize },
    /// Need to read stream footer
    NeedFooter,
    /// End of file reached
    Eof,
}

/// Streaming RRD parser for RRF2 format.
///
/// This parser maintains state across chunks and can parse RRD data
/// incrementally as data arrives.
pub struct StreamingRrdParser {
    /// Parser state
    state: ParserState,
    /// Buffered partial data from previous chunk
    buffer: Vec<u8>,
    /// Position within the buffer
    buffer_pos: usize,

    /// RRD stream header (parsed during initialization)
    header: Option<RrdStreamHeader>,
    /// Discovered channels (entity paths)
    channels: HashMap<u16, ChannelInfo>,
    /// Total messages parsed
    message_count: u64,
    /// Current message index
    message_index: u64,

    /// Track if initialized (header parsed)
    initialized: bool,
}

impl StreamingRrdParser {
    /// Create a new streaming RRD parser.
    pub fn new() -> Self {
        Self {
            state: ParserState::NeedMagic,
            buffer: Vec::new(),
            buffer_pos: 0,
            header: None,
            channels: HashMap::new(),
            message_count: 0,
            message_index: 0,
            initialized: false,
        }
    }

    /// Parse the RRD stream header from bytes.
    fn parse_header(data: &[u8]) -> Result<RrdStreamHeader, FatalError> {
        if data.len() < STREAM_HEADER_SIZE {
            return Err(FatalError::invalid_format(
                "RRD header too short",
                data[..data.len().min(20)].to_vec(),
            ));
        }

        let magic = [data[0], data[1], data[2], data[3]];

        // Check for old RRF0/RRF1 formats
        if OLD_RRD_MAGIC.contains(&magic) {
            return Err(FatalError::ConfigError {
                message: format!(
                    "Old RRD version detected: {:?}. Please upgrade the file using rerun tools.",
                    std::str::from_utf8(&magic).unwrap_or("???")
                ),
            });
        }

        if magic != *RRD_MAGIC {
            return Err(FatalError::invalid_format(
                "RRD magic (expected RRF2)",
                magic.to_vec(),
            ));
        }

        let version = [data[4], data[5], data[6], data[7]];

        // Parse encoding options
        let compression = Compression::from_u8(data[8])?;
        let serializer = data[9];

        // Check reserved bytes are zero
        if data[10] != 0 || data[11] != 0 {
            return Err(FatalError::invalid_format(
                "RRD header reserved bytes",
                data[8..12].to_vec(),
            ));
        }

        // Validate serializer (must be Protobuf for RRF2)
        if serializer == SERIALIZER_MSGPACK {
            return Err(FatalError::ConfigError {
                message: "MsgPack serializer is no longer supported".to_string(),
            });
        }
        if serializer != SERIALIZER_PROTOBUF {
            return Err(FatalError::ConfigError {
                message: format!("Unknown serializer: {serializer}"),
            });
        }

        Ok(RrdStreamHeader {
            magic,
            version,
            compression,
            serializer,
        })
    }

    /// Initialize the default channel after parsing header.
    fn initialize_channels(&mut self) {
        let channel = ChannelInfo {
            id: 0,
            topic: DEFAULT_TOPIC.to_string(),
            message_type: "rerun.ArrowMsg".to_string(),
            encoding: "protobuf".to_string(),
            schema: None,
            schema_data: None,
            schema_encoding: Some("protobuf".to_string()),
            message_count: 0,
            callerid: None,
        };

        self.channels.insert(0, channel);
        self.initialized = true;
    }

    /// Decompress message payload if needed.
    ///
    /// In RRF2, ArrowMsg payloads can be LZ4 compressed at the message level.
    /// The payload is an ArrowMsg protobuf which contains:
    /// - compression field (i32)
    /// - uncompressed_size field (u64)
    /// - payload field (bytes) - the actual Arrow IPC data, potentially LZ4 compressed
    fn decompress_payload(&self, payload: &[u8]) -> Result<Vec<u8>, FatalError> {
        // Try to parse as ArrowMsg protobuf
        match ArrowMsg::from_bytes(payload) {
            Ok(arrow_msg) => {
                // Decompress the ArrowMsg payload if needed
                arrow_msg
                    .decompress_payload()
                    .map_err(|e| FatalError::ConfigError {
                        message: format!("Failed to decompress ArrowMsg payload: {e}"),
                    })
            }
            Err(_) => {
                // Not a valid ArrowMsg protobuf - return as-is for backward compatibility
                // This handles old-format RRD files without ArrowMsg wrapper
                Ok(payload.to_vec())
            }
        }
    }

    /// Get the RRD stream header if parsed.
    pub fn header(&self) -> Option<&RrdStreamHeader> {
        self.header.as_ref()
    }
}

impl Default for StreamingRrdParser {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingParser for StreamingRrdParser {
    type Message = RrdMessageRecord;

    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>, FatalError> {
        let mut all_messages = Vec::new();

        // Append new data to buffer
        self.buffer.extend_from_slice(data);

        while self.buffer_pos < self.buffer.len() {
            match self.state {
                ParserState::NeedMagic => {
                    if self.buffer.len() - self.buffer_pos < 4 {
                        break; // Need more data
                    }

                    let magic = &self.buffer[self.buffer_pos..self.buffer_pos + 4];

                    // Check for old formats
                    if OLD_RRD_MAGIC.contains(&magic.try_into().unwrap()) {
                        return Err(FatalError::ConfigError {
                            message: format!(
                                "Old RRD version detected: {:?}. Please upgrade the file using rerun tools.",
                                std::str::from_utf8(magic).unwrap_or("???")
                            ),
                        });
                    }

                    if magic != RRD_MAGIC {
                        return Err(FatalError::invalid_format(
                            "RRD magic (expected RRF2)",
                            magic.to_vec(),
                        ));
                    }

                    self.buffer_pos += 4;
                    self.state = ParserState::NeedHeader;
                }
                ParserState::NeedHeader => {
                    let available = self.buffer.len() - self.buffer_pos;
                    let needed = STREAM_HEADER_SIZE - 4; // Already read magic

                    if available < needed {
                        // Compact buffer and wait for more
                        self.buffer = self.buffer.split_off(self.buffer_pos);
                        self.buffer_pos = 0;
                        break;
                    }

                    // Reconstruct full header for parsing
                    let mut header_data = Vec::with_capacity(STREAM_HEADER_SIZE);
                    header_data.extend_from_slice(RRD_MAGIC);
                    header_data
                        .extend_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + needed]);

                    let header = Self::parse_header(&header_data)?;
                    self.header = Some(header.clone());
                    self.initialize_channels();

                    self.buffer_pos += needed;
                    self.state = ParserState::NeedMessageHeader;
                }
                ParserState::NeedMessageHeader => {
                    if self.buffer.len() - self.buffer_pos < MESSAGE_HEADER_SIZE {
                        self.buffer = self.buffer.split_off(self.buffer_pos);
                        self.buffer_pos = 0;
                        break;
                    }

                    // Parse MessageHeader: kind(u64) + len(u64)
                    let kind = u64::from_le_bytes(
                        self.buffer[self.buffer_pos..self.buffer_pos + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let len = u64::from_le_bytes(
                        self.buffer[self.buffer_pos + 8..self.buffer_pos + 16]
                            .try_into()
                            .unwrap(),
                    ) as usize;

                    let kind = MessageKind::from_u64(kind).ok_or_else(|| {
                        FatalError::invalid_format(
                            "Unknown message kind",
                            kind.to_le_bytes().to_vec(),
                        )
                    })?;

                    self.buffer_pos += MESSAGE_HEADER_SIZE;

                    if kind == MessageKind::End {
                        // End marker - expect footer next
                        self.state = ParserState::NeedFooter;
                    } else {
                        self.state = ParserState::NeedMessagePayload { kind, len };
                    }
                }
                ParserState::NeedMessagePayload { kind, len } => {
                    if self.buffer.len() - self.buffer_pos < len {
                        self.buffer = self.buffer.split_off(self.buffer_pos);
                        self.buffer_pos = 0;
                        break;
                    }

                    let payload = &self.buffer[self.buffer_pos..self.buffer_pos + len];

                    // Decompress if needed
                    let data = match self.decompress_payload(payload) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!("Failed to process RRD payload: {}", e);
                            // Skip this message
                            self.buffer_pos += len;
                            self.state = ParserState::NeedMessageHeader;
                            self.message_index += 1;
                            continue;
                        }
                    };

                    // Determine topic based on message kind
                    let topic = match kind {
                        MessageKind::ArrowMsg => DEFAULT_TOPIC.to_string(),
                        MessageKind::SetStoreInfo => "/store/info".to_string(),
                        MessageKind::BlueprintActivationCommand => "/blueprint".to_string(),
                        MessageKind::End => unreachable!(), // handled above
                    };

                    all_messages.push(RrdMessageRecord {
                        kind,
                        topic,
                        data,
                        index: self.message_index,
                    });

                    self.buffer_pos += len;
                    self.state = ParserState::NeedMessageHeader;
                    self.message_index += 1;
                }
                ParserState::NeedFooter => {
                    let remaining = self.buffer.len() - self.buffer_pos;

                    // For complete RRD files with full 32-byte footer, validate it
                    if remaining >= STREAM_FOOTER_SIZE {
                        // Footer structure: entries(20) + magic(4) + identifier(4) + num_entries(4)
                        let fourcc_offset = self.buffer_pos + 20;
                        let identifier_offset = self.buffer_pos + 24;

                        let fourcc = &self.buffer[fourcc_offset..fourcc_offset + 4];
                        let identifier = &self.buffer[identifier_offset..identifier_offset + 4];

                        if fourcc == RRD_MAGIC && identifier == RRD_FOOTER_ID {
                            // Valid footer found
                            self.state = ParserState::Eof;
                            break;
                        }
                        // Footer doesn't match expected format - fall through to check for FOOT marker
                    }

                    // Check for FOOT marker at the end of buffer (handles truncated/incomplete files)
                    let buffer_end = &self.buffer[self.buffer_pos..];
                    if buffer_end.ends_with(RRD_FOOTER_ID) {
                        // Found FOOT marker - accept as EOF even if footer is incomplete
                        self.state = ParserState::Eof;
                        break;
                    }

                    // Not enough data for footer and no FOOT marker - wait for more
                    self.buffer = self.buffer.split_off(self.buffer_pos);
                    self.buffer_pos = 0;
                    break;
                }
                ParserState::Eof => {
                    break;
                }
            }
        }

        // Compact buffer if we've consumed significant data
        if self.buffer_pos > 4096 {
            self.buffer.drain(..self.buffer_pos);
            self.buffer_pos = 0;
        }

        self.message_count += all_messages.len() as u64;
        Ok(all_messages)
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
        *self = Self::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rrd_magic() {
        assert_eq!(RRD_STREAM_MAGIC, b"RRF2");
    }

    #[test]
    fn test_parser_new() {
        let parser = StreamingRrdParser::new();
        assert!(!parser.is_initialized());
        assert_eq!(parser.message_count(), 0);
        assert!(parser.channels().is_empty());
    }

    #[test]
    fn test_parser_default() {
        let parser = StreamingRrdParser::default();
        assert!(!parser.is_initialized());
    }

    #[test]
    fn test_parse_header_valid() {
        let mut header_data = vec![0u8; STREAM_HEADER_SIZE];
        header_data[0..4].copy_from_slice(RRD_MAGIC);
        header_data[8] = COMPRESSION_OFF; // compression
        header_data[9] = SERIALIZER_PROTOBUF; // serializer

        let header = StreamingRrdParser::parse_header(&header_data).unwrap();
        assert_eq!(header.magic, *RRD_MAGIC);
        assert_eq!(header.compression, Compression::Off);
        assert_eq!(header.serializer, SERIALIZER_PROTOBUF);
    }

    #[test]
    fn test_parse_header_invalid_magic() {
        let header_data = vec![0xFFu8; STREAM_HEADER_SIZE];
        let result = StreamingRrdParser::parse_header(&header_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_header_old_magic() {
        let mut header_data = vec![0u8; STREAM_HEADER_SIZE];
        header_data[0..4].copy_from_slice(b"RRF0");

        let result = StreamingRrdParser::parse_header(&header_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Old RRD version"));
    }

    #[test]
    fn test_parse_header_too_short() {
        let header_data = vec![0u8; 8];
        let result = StreamingRrdParser::parse_header(&header_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_message_kind_from_u64() {
        assert_eq!(MessageKind::from_u64(0), Some(MessageKind::End));
        assert_eq!(MessageKind::from_u64(1), Some(MessageKind::SetStoreInfo));
        assert_eq!(MessageKind::from_u64(2), Some(MessageKind::ArrowMsg));
        assert_eq!(
            MessageKind::from_u64(3),
            Some(MessageKind::BlueprintActivationCommand)
        );
        assert_eq!(MessageKind::from_u64(999), None);
    }

    #[test]
    fn test_compression_from_u8() {
        assert_eq!(
            Compression::from_u8(COMPRESSION_OFF).unwrap(),
            Compression::Off
        );
        assert_eq!(
            Compression::from_u8(COMPRESSION_LZ4).unwrap(),
            Compression::Lz4
        );
        assert!(Compression::from_u8(99).is_err());
    }

    #[test]
    fn test_parser_magic() {
        let mut parser = StreamingRrdParser::new();
        let result = parser.parse_chunk(b"RRF2");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
        assert!(matches!(parser.state, ParserState::NeedHeader));
    }

    #[test]
    fn test_parser_invalid_magic() {
        let mut parser = StreamingRrdParser::new();
        let result = parser.parse_chunk(b"INVALID");
        assert!(result.is_err());
    }

    #[test]
    fn test_parser_full_header() {
        let mut parser = StreamingRrdParser::new();

        // Create a valid header
        let mut header_data = Vec::new();
        header_data.extend_from_slice(RRD_MAGIC);
        header_data.extend_from_slice(&[0u8; 4]); // version
        header_data.push(COMPRESSION_OFF);
        header_data.push(SERIALIZER_PROTOBUF);
        header_data.extend_from_slice(&[0u8; 2]); // reserved

        let result = parser.parse_chunk(&header_data);
        assert!(result.is_ok());
        assert!(parser.is_initialized());
        assert!(!parser.channels().is_empty());
    }

    #[test]
    fn test_parser_reset() {
        let mut parser = StreamingRrdParser::new();
        parser.parse_chunk(b"RRF2").unwrap();

        parser.reset();
        assert!(!parser.is_initialized());
        assert_eq!(parser.message_count(), 0);
        assert!(parser.channels().is_empty());
    }

    #[test]
    fn test_no_compression_decompress() {
        let mut parser = StreamingRrdParser::new();
        parser.header = Some(RrdStreamHeader {
            magic: *RRD_MAGIC,
            version: [0, 0, 0, 1],
            compression: Compression::Off,
            serializer: SERIALIZER_PROTOBUF,
        });

        let data = b"test data";
        let result = parser.decompress_payload(data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_decompress_passthrough() {
        // RRF2 stores messages as plain Protobuf regardless of stream compression flag
        // The decompress_payload function should pass through data unchanged
        let mut parser = StreamingRrdParser::new();
        parser.header = Some(RrdStreamHeader {
            magic: *RRD_MAGIC,
            version: [0, 0, 0, 1],
            compression: Compression::Lz4,
            serializer: SERIALIZER_PROTOBUF,
        });

        let data = b"test protobuf data";
        let result = parser.decompress_payload(data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_parse_simple_message() {
        let mut parser = StreamingRrdParser::new();

        // Create header
        let mut data = Vec::new();
        data.extend_from_slice(RRD_MAGIC);
        data.extend_from_slice(&[0u8; 4]); // version
        data.push(COMPRESSION_OFF);
        data.push(SERIALIZER_PROTOBUF);
        data.extend_from_slice(&[0u8; 2]); // reserved

        // Add message header (ArrowMsg)
        data.extend_from_slice(&2u64.to_le_bytes()); // kind = ArrowMsg
        data.extend_from_slice(&5u64.to_le_bytes()); // len = 5

        // Add payload
        data.extend_from_slice(b"hello");

        // Add end marker
        data.extend_from_slice(&0u64.to_le_bytes()); // kind = End
        data.extend_from_slice(&0u64.to_le_bytes()); // len = 0

        // Add footer
        data.extend_from_slice(&[0u8; 20]); // entries
        data.extend_from_slice(RRD_MAGIC);
        data.extend_from_slice(RRD_FOOTER_ID);
        data.extend_from_slice(&1u32.to_le_bytes()); // num_entries

        let result = parser.parse_chunk(&data);
        assert!(result.is_ok());

        let messages = result.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].kind, MessageKind::ArrowMsg);
        assert_eq!(messages[0].data, b"hello");
    }

    #[test]
    fn test_parse_end_message() {
        let mut parser = StreamingRrdParser::new();

        // Create header first
        let mut header = Vec::new();
        header.extend_from_slice(RRD_MAGIC);
        header.extend_from_slice(&[0u8; 4]);
        header.push(COMPRESSION_OFF);
        header.push(SERIALIZER_PROTOBUF);
        header.extend_from_slice(&[0u8; 2]);
        parser.parse_chunk(&header).unwrap();

        // Parse end message
        let mut data = Vec::new();
        data.extend_from_slice(&0u64.to_le_bytes()); // kind = End
        data.extend_from_slice(&0u64.to_le_bytes()); // len = 0

        let result = parser.parse_chunk(&data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }
}
