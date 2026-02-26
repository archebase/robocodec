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
    /// Message counts per connection ID
    connection_message_counts: HashMap<u32, u64>,
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
            connection_message_counts: HashMap::new(),
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
        for msg in &messages {
            *self
                .connection_message_counts
                .entry(msg.conn_id)
                .or_insert(0) += 1;
        }
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
                OP_CHUNK => {
                    // Chunk records contain compressed message data.
                    // Decompress and recursively parse the inner records.
                    let compression = fields.compression.as_deref().unwrap_or("none");
                    let decompressed = Self::decompress_chunk(compression, &data)?;
                    self.parse_inner_records(&decompressed, messages)?;
                }
                OP_BAG_HEADER | OP_INDEX_DATA | OP_CHUNK_INFO => {
                    // Metadata records - skip for streaming
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

    /// Decompress chunk data based on the compression format.
    fn decompress_chunk(compression: &str, data: &[u8]) -> Result<Vec<u8>, FatalError> {
        match compression {
            "none" => Ok(data.to_vec()),
            "bz2" => {
                use bzip2::read::BzDecoder;
                use std::io::Read as _;
                let mut decoder = BzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| FatalError::io_error(format!("BZ2 decompression failed: {e}")))?;
                Ok(decompressed)
            }
            "lz4" => {
                use lz4_flex::decompress_size_prepended;
                decompress_size_prepended(data)
                    .map_err(|e| FatalError::io_error(format!("LZ4 decompression failed: {e}")))
            }
            _ => Err(FatalError::io_error(format!(
                "Unsupported BAG chunk compression: {compression}"
            ))),
        }
    }

    /// Parse inner records from decompressed chunk data.
    ///
    /// Decompressed chunks contain `OP_MSG_DATA` and `OP_CONNECTION` records.
    fn parse_inner_records(
        &mut self,
        data: &[u8],
        messages: &mut Vec<BagMessageRecord>,
    ) -> Result<(), FatalError> {
        let mut pos = 0;
        while pos + 4 <= data.len() {
            // Read header_len
            let header_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .expect("slice is exactly 4 bytes"),
            ) as usize;
            pos += 4;

            if pos + header_len + 4 > data.len() {
                break; // Incomplete record at end of chunk
            }

            // Parse header fields
            let header_bytes = &data[pos..pos + header_len];
            let fields = Self::parse_record_header(header_bytes)?;
            pos += header_len;

            // Read data_len
            let data_len = u32::from_le_bytes(
                data[pos..pos + 4]
                    .try_into()
                    .expect("slice is exactly 4 bytes"),
            ) as usize;
            pos += 4;

            if pos + data_len > data.len() {
                break; // Incomplete record at end of chunk
            }

            let record_data = &data[pos..pos + data_len];
            pos += data_len;

            match fields.op {
                Some(OP_MSG_DATA) => {
                    if let Some(conn_id) = fields.conn {
                        let time = fields.time.unwrap_or(0);
                        messages.push(BagMessageRecord {
                            conn_id,
                            log_time: time,
                            data: record_data.to_vec(),
                        });
                    }
                }
                Some(OP_CONNECTION) => {
                    let data_fields = Self::parse_record_header(record_data).unwrap_or_default();
                    if let Some(conn) = Self::connection_from_fields(&fields, &data_fields) {
                        self.connections.insert(conn.conn_id, conn);
                    }
                }
                _ => {
                    // Skip other record types inside chunks (e.g. index data)
                }
            }
        }
        Ok(())
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
                    message_count: self
                        .connection_message_counts
                        .get(conn_id)
                        .copied()
                        .unwrap_or(0),
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
        let prev_conn_count = self.connections.len();
        let messages = StreamingBagParser::parse_chunk(self, data)?;

        // Rebuild channels if we discovered new connections
        if self.connections.len() != prev_conn_count {
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

    // =========================================================================
    // Test helpers: build raw BAG binary structures
    // =========================================================================

    /// Build a BAG header field: `field_len(u32) | name=value`.
    fn build_field(name: &[u8], value: &[u8]) -> Vec<u8> {
        let field_len = (name.len() + 1 + value.len()) as u32; // +1 for '='
        let mut out = Vec::new();
        out.extend(&field_len.to_le_bytes());
        out.extend(name);
        out.push(b'=');
        out.extend(value);
        out
    }

    /// Build a complete BAG record: `header_len(u32) | header_bytes | data_len(u32) | data`.
    fn build_record(header_fields: &[u8], data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(&(header_fields.len() as u32).to_le_bytes());
        out.extend(header_fields);
        out.extend(&(data.len() as u32).to_le_bytes());
        out.extend(data);
        out
    }

    /// Build op field bytes.
    fn op_field(op: u8) -> Vec<u8> {
        build_field(b"op", &[op])
    }

    /// Build conn field bytes.
    fn conn_field(conn_id: u32) -> Vec<u8> {
        build_field(b"conn", &conn_id.to_le_bytes())
    }

    /// Build time field bytes (sec + nsec).
    fn time_field(sec: u32, nsec: u32) -> Vec<u8> {
        let mut value = Vec::new();
        value.extend(&sec.to_le_bytes());
        value.extend(&nsec.to_le_bytes());
        build_field(b"time", &value)
    }

    /// Build topic field bytes.
    fn topic_field(topic: &str) -> Vec<u8> {
        build_field(b"topic", topic.as_bytes())
    }

    /// Build compression field bytes.
    fn compression_field(compression: &str) -> Vec<u8> {
        build_field(b"compression", compression.as_bytes())
    }

    /// Build size field bytes (uncompressed size).
    fn size_field(size: u32) -> Vec<u8> {
        build_field(b"size", &size.to_le_bytes())
    }

    /// Build a BAG OP_MSG_DATA record.
    fn build_msg_data_record(conn_id: u32, sec: u32, nsec: u32, payload: &[u8]) -> Vec<u8> {
        let mut header = Vec::new();
        header.extend(op_field(OP_MSG_DATA));
        header.extend(conn_field(conn_id));
        header.extend(time_field(sec, nsec));
        build_record(&header, payload)
    }

    /// Build a BAG OP_CONNECTION record.
    fn build_connection_record(conn_id: u32, topic: &str) -> Vec<u8> {
        let mut header = Vec::new();
        header.extend(op_field(OP_CONNECTION));
        header.extend(conn_field(conn_id));
        header.extend(topic_field(topic));

        // Data section contains additional fields (type, md5sum, etc.)
        let mut data_fields = Vec::new();
        data_fields.extend(build_field(b"type", b"std_msgs/String"));
        data_fields.extend(build_field(b"md5sum", b"992ce8a1687cec8c8bd883ec73ca41d1"));
        data_fields.extend(build_field(b"message_definition", b"string data"));

        build_record(&header, &data_fields)
    }

    /// Build a BAG OP_CHUNK record with uncompressed inner data.
    fn build_chunk_record_none(inner_records: &[u8]) -> Vec<u8> {
        let mut header = Vec::new();
        header.extend(op_field(OP_CHUNK));
        header.extend(compression_field("none"));
        header.extend(size_field(inner_records.len() as u32));
        build_record(&header, inner_records)
    }

    /// Build a BAG OP_CHUNK record with LZ4-compressed inner data.
    fn build_chunk_record_lz4(inner_records: &[u8]) -> Vec<u8> {
        use lz4_flex::compress_prepend_size;
        let compressed = compress_prepend_size(inner_records);

        let mut header = Vec::new();
        header.extend(op_field(OP_CHUNK));
        header.extend(compression_field("lz4"));
        header.extend(size_field(inner_records.len() as u32));
        build_record(&header, &compressed)
    }

    /// Build a BAG OP_CHUNK record with BZ2-compressed inner data.
    fn build_chunk_record_bz2(inner_records: &[u8]) -> Vec<u8> {
        use bzip2::Compression;
        use bzip2::write::BzEncoder;
        use std::io::Write;

        let mut encoder = BzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(inner_records).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut header = Vec::new();
        header.extend(op_field(OP_CHUNK));
        header.extend(compression_field("bz2"));
        header.extend(size_field(inner_records.len() as u32));
        build_record(&header, &compressed)
    }

    /// Build a BAG header record (op=0x03).
    fn build_bag_header_record() -> Vec<u8> {
        let mut header = Vec::new();
        header.extend(op_field(OP_BAG_HEADER));
        // index_pos and conn_count/chunk_count are typically in the header
        header.extend(build_field(b"index_pos", &0u64.to_le_bytes()));
        header.extend(build_field(b"conn_count", &0u32.to_le_bytes()));
        header.extend(build_field(b"chunk_count", &0u32.to_le_bytes()));
        // Padding data (BAG header records often have padding)
        build_record(&header, &[0u8; 4])
    }

    /// Build a complete minimal BAG file with magic + header + records.
    fn build_bag_file(records: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(b"#ROSBAG V2.0\n");
        out.extend(build_bag_header_record());
        for record in records {
            out.extend(record);
        }
        out
    }

    // =========================================================================
    // Basic parser tests
    // =========================================================================

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

    // =========================================================================
    // Decompress chunk tests
    // =========================================================================

    #[test]
    fn test_decompress_chunk_none() {
        let data = b"hello world";
        let result = StreamingBagParser::decompress_chunk("none", data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decompress_chunk_lz4() {
        use lz4_flex::compress_prepend_size;
        let original = b"hello world this is a test of lz4 compression";
        let compressed = compress_prepend_size(original);
        let result = StreamingBagParser::decompress_chunk("lz4", &compressed).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_chunk_bz2() {
        use bzip2::Compression;
        use bzip2::write::BzEncoder;
        use std::io::Write;

        let original = b"hello world this is a test of bz2 compression";
        let mut encoder = BzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let result = StreamingBagParser::decompress_chunk("bz2", &compressed).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_chunk_unsupported() {
        let result = StreamingBagParser::decompress_chunk("zstd", b"data");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported BAG chunk compression")
        );
    }

    #[test]
    fn test_decompress_chunk_lz4_invalid_data() {
        let result = StreamingBagParser::decompress_chunk("lz4", b"\x00\x00\x00\x00garbage");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("LZ4 decompression failed"));
    }

    #[test]
    fn test_decompress_chunk_bz2_invalid_data() {
        let result = StreamingBagParser::decompress_chunk("bz2", b"not-bz2-data");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("BZ2 decompression failed"));
    }

    // =========================================================================
    // Inner record parsing tests
    // =========================================================================

    #[test]
    fn test_parse_inner_records_msg_data() {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();

        // Build inner records: a single message data record
        let inner = build_msg_data_record(0, 100, 500, b"payload-data");
        parser.parse_inner_records(&inner, &mut messages).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].conn_id, 0);
        assert_eq!(messages[0].log_time, 100 * 1_000_000_000 + 500);
        assert_eq!(messages[0].data, b"payload-data");
    }

    #[test]
    fn test_parse_inner_records_multiple_messages() {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();

        let mut inner = Vec::new();
        inner.extend(build_msg_data_record(0, 100, 0, b"msg1"));
        inner.extend(build_msg_data_record(1, 200, 0, b"msg2"));
        inner.extend(build_msg_data_record(0, 300, 0, b"msg3"));

        parser.parse_inner_records(&inner, &mut messages).unwrap();

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].data, b"msg1");
        assert_eq!(messages[1].data, b"msg2");
        assert_eq!(messages[2].data, b"msg3");
        assert_eq!(messages[0].conn_id, 0);
        assert_eq!(messages[1].conn_id, 1);
        assert_eq!(messages[2].conn_id, 0);
    }

    #[test]
    fn test_parse_inner_records_connection() {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();

        let inner = build_connection_record(0, "/camera/image");
        parser.parse_inner_records(&inner, &mut messages).unwrap();

        assert_eq!(messages.len(), 0);
        assert_eq!(parser.connections.len(), 1);
        let conn = parser.connections.get(&0).unwrap();
        assert_eq!(conn.topic, "/camera/image");
        assert_eq!(conn.message_type, "std_msgs/String");
    }

    #[test]
    fn test_parse_inner_records_connection_and_messages() {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/topic_a"));
        inner.extend(build_msg_data_record(0, 100, 0, b"data-a"));
        inner.extend(build_connection_record(1, "/topic_b"));
        inner.extend(build_msg_data_record(1, 200, 0, b"data-b"));

        parser.parse_inner_records(&inner, &mut messages).unwrap();

        assert_eq!(parser.connections.len(), 2);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].data, b"data-a");
        assert_eq!(messages[1].data, b"data-b");
    }

    #[test]
    fn test_parse_inner_records_empty() {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();
        parser.parse_inner_records(&[], &mut messages).unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_parse_inner_records_truncated() {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();

        // Build a valid record followed by a truncated one
        let mut inner = build_msg_data_record(0, 100, 0, b"valid");
        // Append a truncated header (just header_len, no actual data)
        inner.extend(&100u32.to_le_bytes());

        parser.parse_inner_records(&inner, &mut messages).unwrap();
        // Should parse the valid record and stop at the truncated one
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, b"valid");
    }

    // =========================================================================
    // End-to-end chunk processing tests
    // =========================================================================

    #[test]
    fn test_chunk_uncompressed_end_to_end() {
        let mut parser = StreamingBagParser::new();

        // Build inner records
        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/sensor/data"));
        inner.extend(build_msg_data_record(0, 1000, 0, b"sensor-reading"));

        // Build the uncompressed chunk record
        let chunk = build_chunk_record_none(&inner);

        // Build a complete BAG file
        let bag = build_bag_file(&[chunk]);

        // Parse it all in one go
        let messages = parser.parse_chunk(&bag).unwrap();

        assert!(parser.is_initialized());
        assert_eq!(parser.connections.len(), 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, b"sensor-reading");
        assert_eq!(messages[0].conn_id, 0);

        let channels = parser.channels();
        assert_eq!(channels.len(), 1);
        let ch = channels.values().next().unwrap();
        assert_eq!(ch.topic, "/sensor/data");
    }

    #[test]
    fn test_chunk_lz4_end_to_end() {
        let mut parser = StreamingBagParser::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/lidar/points"));
        inner.extend(build_msg_data_record(0, 500, 0, b"point-cloud-data"));
        inner.extend(build_msg_data_record(0, 600, 0, b"point-cloud-data-2"));

        let chunk = build_chunk_record_lz4(&inner);
        let bag = build_bag_file(&[chunk]);

        let messages = parser.parse_chunk(&bag).unwrap();

        assert_eq!(parser.connections.len(), 1);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].data, b"point-cloud-data");
        assert_eq!(messages[1].data, b"point-cloud-data-2");
    }

    #[test]
    fn test_chunk_bz2_end_to_end() {
        let mut parser = StreamingBagParser::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/imu/data"));
        inner.extend(build_msg_data_record(0, 42, 123, b"imu-reading"));

        let chunk = build_chunk_record_bz2(&inner);
        let bag = build_bag_file(&[chunk]);

        let messages = parser.parse_chunk(&bag).unwrap();

        assert_eq!(parser.connections.len(), 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, b"imu-reading");
        let expected_time = 42u64 * 1_000_000_000 + 123u64;
        assert_eq!(messages[0].log_time, expected_time);
    }

    #[test]
    fn test_multiple_chunks() {
        let mut parser = StreamingBagParser::new();

        // Chunk 1: connection + message
        let mut inner1 = Vec::new();
        inner1.extend(build_connection_record(0, "/cam/image"));
        inner1.extend(build_msg_data_record(0, 100, 0, b"frame-1"));
        let chunk1 = build_chunk_record_none(&inner1);

        // Chunk 2: another connection + messages
        let mut inner2 = Vec::new();
        inner2.extend(build_connection_record(1, "/joint/state"));
        inner2.extend(build_msg_data_record(0, 200, 0, b"frame-2"));
        inner2.extend(build_msg_data_record(1, 200, 0, b"joint-1"));
        let chunk2 = build_chunk_record_lz4(&inner2);

        let bag = build_bag_file(&[chunk1, chunk2]);
        let messages = parser.parse_chunk(&bag).unwrap();

        assert_eq!(parser.connections.len(), 2);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].data, b"frame-1");
        assert_eq!(messages[1].data, b"frame-2");
        assert_eq!(messages[2].data, b"joint-1");
    }

    #[test]
    fn test_chunk_with_streaming_parser_trait() {
        use crate::io::streaming::StreamingParser as _;

        let mut parser = StreamingBagParser::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/topic_a"));
        inner.extend(build_msg_data_record(0, 100, 0, b"data-a"));
        let chunk = build_chunk_record_none(&inner);
        let bag = build_bag_file(&[chunk]);

        // Use the StreamingParser trait method
        let messages = StreamingParser::parse_chunk(&mut parser, &bag).unwrap();

        assert_eq!(messages.len(), 1);
        assert!(parser.has_channels());
        let channels = StreamingParser::channels(&parser);
        assert_eq!(channels.len(), 1);
        assert!(channels.values().any(|c| c.topic == "/topic_a"));
    }

    #[test]
    fn test_incremental_streaming_across_chunks() {
        let mut parser = StreamingBagParser::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/test"));
        inner.extend(build_msg_data_record(0, 1, 0, b"msg"));
        let chunk = build_chunk_record_none(&inner);
        let bag = build_bag_file(&[chunk]);

        // Feed the bag data in small pieces to simulate streaming
        let piece_size = 32;
        let mut all_messages = Vec::new();
        for piece in bag.chunks(piece_size) {
            let msgs = parser.parse_chunk(piece).unwrap();
            all_messages.extend(msgs);
        }

        assert!(parser.is_initialized());
        assert_eq!(parser.connections.len(), 1);
        assert_eq!(all_messages.len(), 1);
        assert_eq!(all_messages[0].data, b"msg");
    }

    #[test]
    fn test_top_level_connection_before_chunk() {
        let mut parser = StreamingBagParser::new();

        // In some BAG files, connections appear as top-level records
        // (before chunks), then chunks contain only message data.
        let conn_record = build_connection_record(0, "/joint_cmd");

        let mut inner = Vec::new();
        inner.extend(build_msg_data_record(0, 100, 0, b"cmd-data"));
        let chunk = build_chunk_record_none(&inner);

        let bag = build_bag_file(&[conn_record, chunk]);
        let messages = parser.parse_chunk(&bag).unwrap();

        assert_eq!(parser.connections.len(), 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, b"cmd-data");
    }

    #[test]
    fn test_large_payload_in_chunk() {
        let mut parser = StreamingBagParser::new();

        // Simulate a large image payload
        let payload = vec![0xABu8; 1024 * 100]; // 100KB
        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/cam/image_raw"));
        inner.extend(build_msg_data_record(0, 100, 0, &payload));

        let chunk = build_chunk_record_lz4(&inner);
        let bag = build_bag_file(&[chunk]);

        let messages = parser.parse_chunk(&bag).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data.len(), 1024 * 100);
        assert!(messages[0].data.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn test_message_count_with_chunks() {
        let mut parser = StreamingBagParser::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/data"));
        for i in 0..10u32 {
            inner.extend(build_msg_data_record(0, i, 0, &[i as u8]));
        }
        let chunk = build_chunk_record_none(&inner);
        let bag = build_bag_file(&[chunk]);

        let messages = parser.parse_chunk(&bag).unwrap();

        assert_eq!(messages.len(), 10);
        assert_eq!(parser.message_count(), 10);
    }

    #[test]
    fn test_reset_clears_chunk_state() {
        let mut parser = StreamingBagParser::new();

        let mut inner = Vec::new();
        inner.extend(build_connection_record(0, "/data"));
        inner.extend(build_msg_data_record(0, 1, 0, b"msg"));
        let chunk = build_chunk_record_none(&inner);
        let bag = build_bag_file(&[chunk]);

        let messages = parser.parse_chunk(&bag).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(parser.connections.len(), 1);

        parser.reset();
        assert_eq!(parser.message_count(), 0);
        assert!(!parser.is_initialized());
        assert!(!parser.has_connections());
    }
}
