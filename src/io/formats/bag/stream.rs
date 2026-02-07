// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming BAG parser.
//!
//! This module provides a zero-copy streaming parser that can parse ROS1 bag records
//! from byte chunks as they arrive, without requiring the entire file
//! to be available locally.

use std::collections::HashMap;

use crate::io::formats::bag::parser::BagConnection;
use crate::io::metadata::ChannelInfo;
use crate::io::s3::FatalError;
use crate::io::streaming::StreamingParser;

/// BAG magic string prefix.
pub const BAG_MAGIC_PREFIX: &[u8] = b"#ROSBAG V";

/// BAG record op codes.
const OP_MSG_DATA: u8 = 0x02;
const OP_BAG_HEADER: u8 = 0x03;
const OP_INDEX_DATA: u8 = 0x04;
const OP_CHUNK: u8 = 0x05;
const OP_CHUNK_INFO: u8 = 0x06;
const OP_CONNECTION: u8 = 0x07;

/// BAG record header as parsed from the stream.
#[derive(Debug, Clone, PartialEq)]
pub struct BagRecordHeader {
    /// Record op code
    pub op: u8,
    /// Record header length
    pub header_len: u32,
    /// Record data length
    pub data_len: u32,
}

/// Parsed BAG record with header and data.
#[derive(Debug, Clone)]
pub struct BagRecord {
    /// Record header
    pub header: BagRecordHeader,
    /// Record header fields
    pub fields: BagRecordFields,
    /// Record data bytes
    pub data: Vec<u8>,
}

/// Parsed fields from a BAG record header.
#[derive(Debug, Clone, Default)]
pub struct BagRecordFields {
    /// Op code
    pub op: Option<u8>,
    /// Connection ID
    pub conn: Option<u32>,
    /// Timestamp (nanoseconds)
    pub time: Option<u64>,
    /// Topic name
    pub topic: Option<String>,
    /// Message type
    pub message_type: Option<String>,
    /// MD5 sum of message definition
    pub md5sum: Option<String>,
    /// Message definition (IDL-like text)
    pub message_definition: Option<String>,
    /// Caller ID (publishing node)
    pub callerid: Option<String>,
    /// Index position in file
    pub index_pos: Option<u64>,
    /// Connection count
    pub conn_count: Option<u32>,
    /// Chunk count
    pub chunk_count: Option<u32>,
    /// Chunk position in file
    pub chunk_pos: Option<u64>,
    /// Compression format ("none", "bz2", "lz4")
    pub compression: Option<String>,
    /// Uncompressed size
    pub size: Option<u32>,
    /// Start time
    pub start_time: Option<u64>,
    /// End time
    pub end_time: Option<u64>,
}

/// Message data from BAG Message Data record.
#[derive(Debug, Clone)]
pub struct BagMessageRecord {
    /// Connection ID
    pub conn_id: u32,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Message data
    pub data: Vec<u8>,
}

/// Streaming BAG parser.
///
/// This parser maintains state across chunks and can parse BAG records
/// incrementally as data arrives.
pub struct StreamingBagParser {
    /// Discovered connections indexed by connection ID
    connections: HashMap<u32, BagConnection>,
    /// Buffered partial record data from previous chunk
    buffer: Vec<u8>,
    /// Current parse state
    state: ParserState,
    /// Expected bytes remaining for current record
    remaining: usize,
    /// Total messages parsed
    message_count: u64,
    /// Position within the buffer
    buffer_pos: usize,
    /// Version string parsed from magic
    version: Option<String>,
    /// Cached channel map (converted from connections)
    cached_channels: HashMap<u16, ChannelInfo>,
}

impl StreamingBagParser {
    /// Create a new streaming BAG parser.
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            buffer: Vec::new(),
            state: ParserState::NeedMagic,
            remaining: 0,
            message_count: 0,
            buffer_pos: 0,
            version: None,
            cached_channels: HashMap::new(),
        }
    }

    /// Parse BAG data from a chunk of bytes.
    ///
    /// Returns any complete message records found in this chunk.
    ///
    /// # Arguments
    ///
    /// * `data` - A chunk of bytes from the BAG file
    ///
    /// # Returns
    ///
    /// A vector of parsed message records. Connection records
    /// are stored internally and accessible via `connections()`.
    pub fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<BagMessageRecord>, FatalError> {
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
        messages: &mut Vec<BagMessageRecord>,
    ) -> Result<bool, FatalError> {
        let available = self.buffer.len() - self.buffer_pos;

        match self.state {
            ParserState::NeedMagic => {
                // Need at least "#ROSBAG V" (9 bytes) + version (e.g., "2.0\n" = 4 bytes)
                if available < 13 {
                    return Ok(false);
                }

                // Verify magic
                let magic_slice = &self.buffer[self.buffer_pos..self.buffer_pos + 9];
                if magic_slice != BAG_MAGIC_PREFIX {
                    return Err(FatalError::invalid_format(
                        "BAG magic (#ROSBAG V)",
                        magic_slice.to_vec(),
                    ));
                }

                // Read version (e.g., "2.0\n")
                let version_bytes = &self.buffer[self.buffer_pos + 9..self.buffer_pos + 13];
                self.version = Some(String::from_utf8_lossy(version_bytes).trim().to_string());

                self.buffer_pos += 13;
                self.state = ParserState::NeedRecordHeader;
                Ok(true)
            }
            ParserState::NeedRecordHeader => {
                // BAG record starts with header_len: u32
                if available < 4 {
                    return Ok(false);
                }

                let header_len = u32::from_le_bytes(
                    self.buffer[self.buffer_pos..self.buffer_pos + 4]
                        .try_into()
                        .expect("slice is exactly 4 bytes after available >= 4 check"),
                ) as usize;

                // Validate header length (sanity check)
                if header_len > 1024 * 1024 {
                    return Err(FatalError::invalid_format(
                        "BAG record header length > 1MB",
                        vec![],
                    ));
                }

                // Check if we have the full header + data_len (4 bytes) + data
                let needed = 4 + header_len + 4;
                if available < needed {
                    self.remaining = header_len;
                    self.state = ParserState::NeedRecordHeaderBytes;
                    return Ok(false);
                }

                // We have enough data, parse the full record
                self.parse_full_record(available, messages)
            }
            ParserState::NeedRecordHeaderBytes => {
                // Need remaining header bytes + data_len (4 bytes) + data
                let needed = self.remaining + 4;
                if available < needed {
                    return Ok(false);
                }

                // Now we have the full header, proceed to read data_len
                let header_start = self.buffer_pos + 4;
                let header_end = header_start + self.remaining;
                let data_len_start = header_end;

                if data_len_start + 4 > self.buffer.len() {
                    return Ok(false);
                }

                let data_len = u32::from_le_bytes(
                    self.buffer[data_len_start..data_len_start + 4]
                        .try_into()
                        .expect("slice is exactly 4 bytes after len check"),
                ) as usize;

                // Validate data length
                if data_len > 100 * 1024 * 1024 {
                    return Err(FatalError::invalid_format(
                        "BAG record data length > 100MB",
                        vec![],
                    ));
                }

                // Check if we have the full data
                let total_needed = 4 + self.remaining + 4 + data_len;
                if available < total_needed {
                    self.remaining = total_needed;
                    self.state = ParserState::NeedRecordData;
                    return Ok(false);
                }

                // We have the full record
                self.parse_full_record(available, messages)
            }
            ParserState::NeedRecordData => {
                // Wait for the full record data
                if available < self.remaining {
                    return Ok(false);
                }

                self.parse_full_record(available, messages)
            }
        }
    }

    /// Parse a full record from the buffer.
    fn parse_full_record(
        &mut self,
        available: usize,
        messages: &mut Vec<BagMessageRecord>,
    ) -> Result<bool, FatalError> {
        let start = self.buffer_pos;

        // Read header_len
        if available < 4 {
            return Ok(false);
        }
        let header_len = u32::from_le_bytes(
            self.buffer[start..start + 4]
                .try_into()
                .expect("slice is exactly 4 bytes after available >= 4 check"),
        ) as usize;

        // Read header bytes
        if available < 4 + header_len {
            return Ok(false);
        }
        let header_bytes = &self.buffer[start + 4..start + 4 + header_len];

        // Parse header fields
        let fields = Self::parse_record_header(header_bytes)?;

        // Read data_len
        if available < 4 + header_len + 4 {
            return Ok(false);
        }
        let data_len = u32::from_le_bytes(
            self.buffer[start + 4 + header_len..start + 4 + header_len + 4]
                .try_into()
                .expect("slice is exactly 4 bytes after available check"),
        ) as usize;

        // Check if we have the full data
        let total_record_len = 4 + header_len + 4 + data_len;
        if available < total_record_len {
            self.state = ParserState::NeedRecordData;
            self.remaining = total_record_len;
            return Ok(false);
        }

        // Read data bytes
        let data_start = start + 4 + header_len + 4;
        let data_end = data_start + data_len;
        let data = self.buffer[data_start..data_end].to_vec();

        // Advance buffer position
        self.buffer_pos = data_end;

        // Process the record based on op code
        if let Some(op) = fields.op {
            match op {
                OP_MSG_DATA => {
                    // Message data record
                    if let Some(conn_id) = fields.conn
                        && let Some(time) = fields.time
                    {
                        messages.push(BagMessageRecord {
                            conn_id,
                            log_time: time,
                            data,
                        });
                    }
                }
                OP_CONNECTION => {
                    // Connection record - extract connection info
                    let data_fields = Self::parse_record_header(&data).unwrap_or_default();
                    if let Some(conn) = Self::connection_from_fields(&fields, &data_fields) {
                        self.connections.insert(conn.conn_id, conn);
                    }
                }
                OP_BAG_HEADER | OP_INDEX_DATA | OP_CHUNK | OP_CHUNK_INFO => {
                    // Metadata records - ignore for streaming
                }
                _ => {
                    // Unknown op code - this might indicate file corruption or version mismatch
                    return Err(FatalError::io_error(format!(
                        "Unknown BAG op code: 0x{op:02x}"
                    )));
                }
            }
        }

        // Reset for next record
        self.state = ParserState::NeedRecordHeader;
        self.remaining = 0;
        Ok(true)
    }

    /// Parse header bytes into named fields.
    pub fn parse_record_header(header_bytes: &[u8]) -> Result<BagRecordFields, FatalError> {
        let mut fields = BagRecordFields::default();
        let mut pos = 0;

        while pos + 4 <= header_bytes.len() {
            // Read field length
            let field_len = u32::from_le_bytes(
                header_bytes[pos..pos + 4]
                    .try_into()
                    .expect("slice is exactly 4 bytes after pos + 4 <= len check"),
            ) as usize;
            pos += 4;

            if field_len == 0 || pos + field_len > header_bytes.len() {
                break;
            }

            // Read field bytes
            let field_bytes = &header_bytes[pos..pos + field_len];
            pos += field_len;

            // Find the '=' separator
            if let Some(eq_pos) = field_bytes.iter().position(|&b| b == b'=') {
                let name = &field_bytes[..eq_pos];
                let value = &field_bytes[eq_pos + 1..];

                Self::parse_field(&mut fields, name, value);
            }
        }

        Ok(fields)
    }

    /// Parse a single field from name and value bytes.
    pub fn parse_field(fields: &mut BagRecordFields, name: &[u8], value: &[u8]) {
        match name {
            b"op" if value.len() == 1 => {
                fields.op = Some(value[0]);
            }
            b"conn" if value.len() >= 4 => {
                fields.conn = Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"time" if value.len() >= 8 => {
                // ROS time: sec (4 bytes) + nsec (4 bytes)
                let sec = u64::from(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
                let nsec = u64::from(u32::from_le_bytes([value[4], value[5], value[6], value[7]]));
                fields.time = Some(sec * 1_000_000_000 + nsec);
            }
            b"topic" => {
                fields.topic = Some(String::from_utf8_lossy(value).to_string());
            }
            b"md5sum" => {
                fields.md5sum = Some(String::from_utf8_lossy(value).to_string());
            }
            b"type" => {
                fields.message_type = Some(String::from_utf8_lossy(value).to_string());
            }
            b"message_definition" => {
                fields.message_definition = Some(String::from_utf8_lossy(value).to_string());
            }
            b"callerid" => {
                fields.callerid = Some(String::from_utf8_lossy(value).to_string());
            }
            b"index_pos" if value.len() >= 8 => {
                fields.index_pos = Some(u64::from_le_bytes([
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                ]));
            }
            b"conn_count" if value.len() >= 4 => {
                fields.conn_count =
                    Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"chunk_count" if value.len() >= 4 => {
                fields.chunk_count =
                    Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"chunk_pos" if value.len() >= 8 => {
                fields.chunk_pos = Some(u64::from_le_bytes([
                    value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
                ]));
            }
            b"compression" => {
                fields.compression = Some(String::from_utf8_lossy(value).to_string());
            }
            b"size" if value.len() >= 4 => {
                fields.size = Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
            }
            b"start_time" if value.len() >= 8 => {
                let sec = u64::from(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
                let nsec = u64::from(u32::from_le_bytes([value[4], value[5], value[6], value[7]]));
                fields.start_time = Some(sec * 1_000_000_000 + nsec);
            }
            b"end_time" if value.len() >= 8 => {
                let sec = u64::from(u32::from_le_bytes([value[0], value[1], value[2], value[3]]));
                let nsec = u64::from(u32::from_le_bytes([value[4], value[5], value[6], value[7]]));
                fields.end_time = Some(sec * 1_000_000_000 + nsec);
            }
            _ => {
                // Ignore unknown fields
            }
        }
    }

    /// Create a `BagConnection` from parsed header and data fields.
    fn connection_from_fields(
        header_fields: &BagRecordFields,
        data_fields: &BagRecordFields,
    ) -> Option<BagConnection> {
        Some(BagConnection {
            conn_id: header_fields.conn?,
            topic: header_fields.topic.clone()?,
            message_type: data_fields.message_type.clone().unwrap_or_default(),
            md5sum: data_fields.md5sum.clone().unwrap_or_default(),
            message_definition: data_fields.message_definition.clone().unwrap_or_default(),
            caller_id: data_fields.callerid.clone().unwrap_or_default(),
        })
    }

    /// Get all discovered connections as `ChannelInfo`.
    ///
    /// Uses the original BAG connection ID as the channel ID to ensure
    /// messages can be correctly associated with their channels.
    #[must_use]
    pub fn channels(&self) -> HashMap<u16, ChannelInfo> {
        self.connections
            .iter()
            .filter_map(|(conn_id, conn)| {
                // Only include conn_ids that fit in u16
                let channel_id = *conn_id as u16;
                if *conn_id != u32::from(channel_id) {
                    tracing::warn!(
                        context = "StreamingBagParser",
                        conn_id,
                        "Connection ID does not fit in u16, skipping channel"
                    );
                    return None;
                }
                let channel = ChannelInfo {
                    id: channel_id,
                    topic: conn.topic.clone(),
                    message_type: conn.message_type.clone(),
                    encoding: "ros1".to_string(),
                    schema: Some(conn.message_definition.clone()),
                    schema_data: None,
                    schema_encoding: Some("ros1msg".to_string()),
                    message_count: 0,
                    callerid: if conn.caller_id.is_empty() {
                        None
                    } else {
                        Some(conn.caller_id.clone())
                    },
                };
                Some((channel_id, channel))
            })
            .collect()
    }

    /// Get the connection ID to channel ID mapping.
    #[must_use]
    pub fn conn_id_map(&self) -> HashMap<u32, u16> {
        self.connections
            .iter()
            .enumerate()
            .map(|(i, (conn_id, _))| (*conn_id, i as u16))
            .collect()
    }

    /// Get the total message count.
    #[must_use]
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Check if the parser has seen all connections.
    #[must_use]
    pub fn has_connections(&self) -> bool {
        !self.connections.is_empty()
    }

    /// Check if we've seen the magic bytes.
    #[must_use]
    pub fn is_initialized(&self) -> bool {
        !matches!(self.state, ParserState::NeedMagic)
    }

    /// Get the parsed version string.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Reset the parser state for a new file.
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Rebuild the cached channel map from connections.
    fn rebuild_channels(&mut self) {
        self.cached_channels = self.channels();
    }
}

// SAFETY: StreamingBagParser is safe to send between threads because:
// - All fields (HashMap, Vec, enum) are Send
// - The parser maintains no thread-local state or handles
unsafe impl Send for StreamingBagParser {}

// SAFETY: StreamingBagParser is safe to share between threads because:
// - The StreamingParser trait requires methods take &mut self, guaranteeing exclusive access
// - All fields are either Send + Sync (HashMap, Vec, enum)
// - No interior mutability or shared state
unsafe impl Sync for StreamingBagParser {}

impl StreamingParser for StreamingBagParser {
    type Message = BagMessageRecord;

    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>, FatalError> {
        // Call the inherent parse_chunk method
        // Use fully qualified syntax to avoid recursion
        let messages = StreamingBagParser::parse_chunk(self, data)?;

        // Rebuild channels if we discovered new connections
        if self.has_connections() && self.cached_channels.is_empty() {
            self.rebuild_channels();
        }

        Ok(messages)
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.cached_channels
    }

    fn message_count(&self) -> u64 {
        StreamingBagParser::message_count(self)
    }

    fn has_channels(&self) -> bool {
        StreamingBagParser::has_connections(self)
    }

    fn is_initialized(&self) -> bool {
        StreamingBagParser::is_initialized(self)
    }

    fn reset(&mut self) {
        StreamingBagParser::reset(self);
    }
}

impl Default for StreamingBagParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parser state for streaming BAG parsing.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum ParserState {
    /// Waiting for magic bytes
    NeedMagic,
    /// Waiting for record header
    NeedRecordHeader,
    /// Waiting for remaining header bytes
    NeedRecordHeaderBytes,
    /// Waiting for record data
    NeedRecordData,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_new() {
        let parser = StreamingBagParser::new();
        assert!(!parser.is_initialized());
        assert!(!parser.has_connections());
        assert_eq!(parser.message_count(), 0);
        assert!(parser.version().is_none());
    }

    #[test]
    fn test_parser_default() {
        let parser = StreamingBagParser::default();
        assert_eq!(parser.message_count(), 0);
    }

    #[test]
    fn test_record_header() {
        let header = BagRecordHeader {
            op: OP_MSG_DATA,
            header_len: 10,
            data_len: 100,
        };
        assert_eq!(header.op, OP_MSG_DATA);
        assert_eq!(header.header_len, 10);
        assert_eq!(header.data_len, 100);
    }

    #[test]
    fn test_message_record() {
        let msg = BagMessageRecord {
            conn_id: 1,
            log_time: 1000,
            data: vec![1, 2, 3],
        };
        assert_eq!(msg.conn_id, 1);
        assert_eq!(msg.log_time, 1000);
        assert_eq!(msg.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_parser_state() {
        assert_eq!(ParserState::NeedMagic, ParserState::NeedMagic);
        assert_eq!(ParserState::NeedRecordHeader, ParserState::NeedRecordHeader);
    }

    #[test]
    fn test_parse_magic() {
        let mut parser = StreamingBagParser::new();

        // Too short - should not error, just not advance
        let result = parser.parse_chunk(b"#ROS");
        assert!(result.is_ok());
        assert!(!parser.is_initialized());

        // Full magic
        let result = parser.parse_chunk(b"BAG V2.0\n");
        assert!(result.is_ok());
        assert!(parser.is_initialized());
        assert_eq!(parser.version(), Some("2.0"));
    }

    #[test]
    fn test_parse_record_header_fields() {
        // Build a simple header with op=0x02
        let mut header_bytes = Vec::new();
        // Field 1: op=\x02 (field_len = 4)
        header_bytes.extend(&4u32.to_le_bytes());
        header_bytes.extend(b"op=\x02");

        let fields = StreamingBagParser::parse_record_header(&header_bytes).unwrap();
        assert_eq!(fields.op, Some(0x02));
    }

    #[test]
    fn test_parse_field_conn() {
        let mut fields = BagRecordFields::default();
        let conn_bytes = [1u8, 0, 0, 0];
        StreamingBagParser::parse_field(&mut fields, b"conn", &conn_bytes);
        assert_eq!(fields.conn, Some(1));
    }

    #[test]
    fn test_parse_field_time() {
        let mut fields = BagRecordFields::default();
        // time = 1234567890 sec + 123456789 nsec
        let mut time_bytes = Vec::new();
        time_bytes.extend(&1234567890u32.to_le_bytes());
        time_bytes.extend(&123456789u32.to_le_bytes());
        StreamingBagParser::parse_field(&mut fields, b"time", &time_bytes);

        let expected_time = 1234567890u64 * 1_000_000_000 + 123456789u64;
        assert_eq!(fields.time, Some(expected_time));
    }

    #[test]
    fn test_channels_empty() {
        let parser = StreamingBagParser::new();
        assert!(parser.channels().is_empty());
        assert!(parser.conn_id_map().is_empty());
    }
}
