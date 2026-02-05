// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! RRD file reader with automatic encoding detection.
//!
//! This module provides `RrdReader` for reading Rerun RRD files with support for
//! various encodings used by Rerun.
//!
//! For parallel reading support, see `ParallelRrdReader` in the `parallel` module.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use tracing::warn;

use crate::core::{CodecError, DecodedMessage, Result};
use crate::encoding::{CdrDecoder, JsonDecoder, ProtobufDecoder};
use crate::io::traits::FormatReader;
use crate::io::writer::WriterConfig;
use crate::io::{ChannelInfo, FormatWriter, TimestampedDecodedMessage};

use super::arrow_msg::ArrowMsg;
use super::constants::*;
use super::parallel::ParallelRrdReader;

/// RRD format type.
///
/// This type provides factory methods for creating RRD readers and writers.
pub struct RrdFormat;

impl RrdFormat {
    /// Create an RRD reader with parallel reading support.
    ///
    /// The reader uses memory-mapping and processes messages in parallel
    /// using the Rayon thread pool.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<ParallelRrdReader> {
        ParallelRrdReader::open(path)
    }

    /// Create an RRD writer with the given configuration.
    ///
    /// Returns a boxed FormatWriter trait object for unified writer API.
    pub fn create_writer<P: AsRef<Path>>(
        path: P,
        _config: &WriterConfig,
    ) -> Result<Box<dyn FormatWriter>> {
        let writer = super::writer::RrdWriter::create(path)?;
        Ok(Box::new(writer))
    }
}

/// RRD file header (RRF2 stream header format).
///
/// The RRF2 stream header is 12 bytes:
/// - fourcc (4 bytes): "RRF2"
/// - version (4 bytes): [0, 0, 0, 1]
/// - options (4 bytes): compression(1) + serializer(1) + reserved(2)
#[derive(Debug, Clone)]
pub struct RrdHeader {
    /// Magic number ("RRF2")
    pub magic: [u8; 4],
    /// Format version (4 bytes, e.g., [0, 0, 0, 1])
    pub version: [u8; 4],
    /// Compression type (0=off, 1=lz4)
    pub compression: u8,
    /// Serializer type (2=msgpack, 3=protobuf)
    pub serializer: u8,
}

impl RrdHeader {
    /// Read the RRF2 stream header from a file.
    ///
    /// The RRF2 stream header is 12 bytes:
    /// - magic (4): "RRF2"
    /// - version (4): [0, 0, 0, 1]
    /// - options (4): compression(1) + serializer(1) + reserved(2)
    fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; 4];
        reader
            .read_exact(&mut magic)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read magic: {}", e)))?;

        if magic != *RRD_MAGIC {
            return Err(CodecError::parse(
                "RRD",
                format!(
                    "Invalid magic number: expected {:?}, got {:?}",
                    RRD_MAGIC, magic
                ),
            ));
        }

        let mut version = [0u8; 4];
        reader
            .read_exact(&mut version)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read version: {}", e)))?;

        // Validate version - reject clearly incompatible versions
        // Version [0, 0, 0, 0] indicates an unversioned/incompatible file
        if version == [0, 0, 0, 0] {
            return Err(CodecError::parse(
                "RRD",
                format!(
                    "Incompatible RRD version: {:?}. This file appears to be from an old or incompatible Rerun version. \
                    Please regenerate the file with a newer version of Rerun, or use Rerun's tools to convert the data.",
                    version
                ),
            ));
        }

        // Warn about versions significantly different from current
        if version < RRD_MIN_VERSION || version > [0, 0, 1, 0] {
            warn!(
                context = "rrd_reader",
                "Unusual RRD version: {:?}. Expected {:?}. The file may not parse correctly.",
                version,
                RRD_VERSION
            );
        }

        // Read encoding options (4 bytes)
        let mut options = [0u8; 4];
        reader
            .read_exact(&mut options)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read options: {}", e)))?;

        let compression = options[0];
        let serializer = options[1];

        // Check reserved bytes are zero
        if options[2] != 0 || options[3] != 0 {
            warn!(
                context = "rrd_reader",
                "Non-zero reserved bytes in RRF2 header"
            );
        }

        Ok(Self {
            magic,
            version,
            compression,
            serializer,
        })
    }

    /// Get the compression name for this header.
    fn compression_name(&self) -> &'static str {
        match self.compression {
            COMPRESSION_OFF => "off",
            COMPRESSION_LZ4 => "lz4",
            _ => "unknown",
        }
    }

    /// Get the serializer name for this header.
    fn serializer_name(&self) -> &'static str {
        match self.serializer {
            SERIALIZER_MSGPACK => "msgpack",
            SERIALIZER_PROTOBUF => "protobuf",
            _ => "unknown",
        }
    }
}

/// Robotics data reader - handles RRD files with automatic encoding detection.
pub struct RrdReader {
    /// Path to the RRD file
    path: String,
    /// File header
    header: RrdHeader,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
    /// Message count
    message_count: u64,
    /// Start timestamp
    start_time: Option<u64>,
    /// End timestamp
    end_time: Option<u64>,
    /// File size
    file_size: u64,
    /// Decoders for different encodings
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
}

impl RrdReader {
    /// Open an RRD file and read its metadata.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_obj = path.as_ref();
        let path_str = path_obj.to_string_lossy().to_string();

        // Check if file exists
        if !path_obj.exists() {
            return Err(CodecError::parse(
                "RRD",
                format!("File not found: {}", path_str),
            ));
        }

        // Get file size
        let file_size = std::fs::metadata(path_obj)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to get metadata: {}", e)))?
            .len();

        // Open file and read header
        let file = std::fs::File::open(path_obj)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to open file: {}", e)))?;

        let mut reader = BufReader::new(file);
        let header = RrdHeader::read(&mut reader)?;

        // Validate version (RRF2 version is [0, 0, 0, 1])
        // For now, we accept any version that starts with 0.0.0.x
        if header.version[0] != 0 || header.version[1] != 0 || header.version[2] != 0 {
            return Err(CodecError::parse(
                "RRD",
                format!("Unsupported version: {:?}", header.version),
            ));
        }

        // Read channel/schema information (for now, create a default channel)
        let mut channels = HashMap::new();
        let default_channel = ChannelInfo {
            id: 0,
            topic: DEFAULT_TOPIC.to_string(),
            message_type: "rerun.ArrowMsg".to_string(),
            encoding: MESSAGE_ENCODING_PROTOBUF.to_string(),
            schema: None,
            schema_data: None,
            schema_encoding: Some(header.serializer_name().to_string()),
            message_count: 0,
            callerid: None,
        };
        channels.insert(0, default_channel);

        // Note: RRF2 doesn't use chunk-based indexing like legacy RRD
        // Message count and timestamps are not available without parsing all messages
        let message_count = 0;
        let start_time = None;
        let end_time = None;

        if channels.is_empty() {
            warn!(context = "rrd_reader", "No channels found in RRD file");
        }

        Ok(Self {
            path: path_str,
            header,
            channels,
            message_count,
            start_time,
            end_time,
            file_size,
            cdr_decoder: Arc::new(CdrDecoder::new()),
            proto_decoder: Arc::new(ProtobufDecoder::new()),
            json_decoder: Arc::new(JsonDecoder::new()),
        })
    }

    /// Get all channel information.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Get channel info by topic name.
    pub fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.channels.values().find(|c| c.topic == topic)
    }

    /// Get total message count.
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Get start timestamp in nanoseconds.
    pub fn start_time(&self) -> Option<u64> {
        self.start_time
    }

    /// Get end timestamp in nanoseconds.
    pub fn end_time(&self) -> Option<u64> {
        self.end_time
    }

    /// Get the file path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Iterate over decoded messages.
    ///
    /// This is the primary API for consuming RRD data. Encoding detection
    /// and decoding happen automatically based on channel metadata.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(DecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages(&self) -> Result<DecodedMessageIter<'_>> {
        DecodedMessageIter::new(self)
    }

    /// Iterate over decoded messages with timestamps.
    ///
    /// Similar to `decode_messages()` but includes the original message timestamps.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(TimestampedDecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages_with_timestamp(&self) -> Result<DecodedMessageWithTimestampIter<'_>> {
        DecodedMessageWithTimestampIter::new(self)
    }

    /// Get the chunk count (always 0 for RRF2).
    ///
    /// RRF2 doesn't use chunk-based indexing like legacy RRD formats.
    /// This method returns 0 to indicate no chunk information is available.
    pub fn chunk_count(&self) -> usize {
        0
    }

    /// Get the RRD header.
    pub fn header(&self) -> &RrdHeader {
        &self.header
    }
}

impl FormatReader for RrdReader {
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

    fn format(&self) -> crate::io::metadata::FileFormat {
        crate::io::metadata::FileFormat::Rrd
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

/// Iterator over decoded messages from RRD file.
pub struct DecodedMessageIter<'a> {
    /// Reference to the reader
    reader: &'a RrdReader,
    /// Parsed messages from the file
    messages: Vec<(Vec<u8>, String)>,
    /// Current position in messages
    current_index: usize,
}

impl<'a> DecodedMessageIter<'a> {
    /// Create a new iterator from the reader.
    fn new(reader: &'a RrdReader) -> Result<Self> {
        let messages = Self::parse_messages(reader)?;
        Ok(Self {
            reader,
            messages,
            current_index: 0,
        })
    }

    /// Parse all messages from the RRD file.
    fn parse_messages(reader: &RrdReader) -> Result<Vec<(Vec<u8>, String)>> {
        use std::io::Read;

        use super::constants::{
            MESSAGE_HEADER_SIZE, MSG_KIND_ARROW_MSG, MSG_KIND_END, MSG_KIND_SET_STORE_INFO,
            RRD_MAGIC, STREAM_FOOTER_SIZE, STREAM_HEADER_SIZE,
        };

        let mut file = std::fs::File::open(&reader.path)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to open file: {}", e)))?;

        // Skip stream header (STREAM_HEADER_SIZE bytes)
        let mut header_buf = vec![0u8; STREAM_HEADER_SIZE];
        file.read_exact(&mut header_buf)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read header: {}", e)))?;

        // Verify magic
        if &header_buf[0..4] != RRD_MAGIC {
            return Err(CodecError::parse(
                "RRD",
                format!("Invalid magic: {:?}", &header_buf[0..4]),
            ));
        }

        let mut messages = Vec::new();
        let mut data_buf = Vec::new();

        // Read remaining file data
        file.read_to_end(&mut data_buf)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read data: {}", e)))?;

        let mut pos = 0;

        // Parse messages until we reach the footer
        while pos + MESSAGE_HEADER_SIZE <= data_buf.len() {
            // Check if we're at the footer (last 32 bytes)
            if pos + STREAM_FOOTER_SIZE <= data_buf.len() {
                let footer_start = data_buf.len() - STREAM_FOOTER_SIZE;
                if pos >= footer_start {
                    break;
                }
            }

            // Read message header: kind(u64 le) + len(u64 le)
            let kind = u64::from_le_bytes(data_buf[pos..pos + 8].try_into().unwrap_or([0u8; 8]));
            let len = u64::from_le_bytes(data_buf[pos + 8..pos + 16].try_into().unwrap_or([0u8; 8]))
                as usize;

            pos += MESSAGE_HEADER_SIZE;

            // Check for end marker
            if kind == MSG_KIND_END {
                break;
            }

            // Extract topic based on message kind
            let topic = match kind {
                MSG_KIND_ARROW_MSG => "/".to_string(),
                MSG_KIND_SET_STORE_INFO => "/store/info".to_string(),
                _ => "/".to_string(),
            };

            // Read payload if we have data
            if pos + len <= data_buf.len() {
                let payload = data_buf[pos..pos + len].to_vec();

                // Parse ArrowMsg protobuf and decompress
                let arrow_msg = ArrowMsg::from_bytes(&payload).map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to parse ArrowMsg: {e}"))
                })?;

                let decompressed = arrow_msg.decompress_payload().map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to decompress ArrowMsg: {e}"))
                })?;

                messages.push((decompressed, topic));
                pos += len;
            } else {
                break;
            }
        }

        Ok(messages)
    }
}

impl<'a> Iterator for DecodedMessageIter<'a> {
    type Item = Result<(DecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::core::CodecValue;

        if self.current_index >= self.messages.len() {
            return None;
        }

        let (data, topic) = &self.messages[self.current_index];
        self.current_index += 1;

        // Get channel info
        let channel = self
            .reader
            .channels()
            .get(&0)
            .cloned()
            .unwrap_or_else(|| ChannelInfo {
                id: 0,
                topic: topic.clone(),
                message_type: "rerun.ArrowMsg".to_string(),
                encoding: "protobuf".to_string(),
                schema: None,
                schema_data: None,
                schema_encoding: Some("protobuf".to_string()),
                message_count: 0,
                callerid: None,
            });

        // Create decoded message with raw data as bytes field
        // RRF2 messages are Protobuf-encoded; we store the raw payload
        let mut decoded = DecodedMessage::new();
        decoded.insert("data".to_string(), CodecValue::Bytes(data.clone()));

        Some(Ok((decoded, channel)))
    }
}

/// Iterator over decoded messages with timestamps from RRD file.
pub struct DecodedMessageWithTimestampIter<'a> {
    /// Reference to the reader
    reader: &'a RrdReader,
    /// Parsed messages from the file
    messages: Vec<(Vec<u8>, String)>,
    /// Current position in messages
    current_index: usize,
    /// Current message timestamp (nanoseconds, RRF2 doesn't have per-message timestamps)
    current_timestamp: u64,
}

impl<'a> DecodedMessageWithTimestampIter<'a> {
    /// Create a new iterator from the reader.
    fn new(reader: &'a RrdReader) -> Result<Self> {
        let messages = Self::parse_messages(reader)?;
        // RRF2 doesn't have timestamps at message level, use a reasonable default
        let start_timestamp = reader.start_time.unwrap_or(0);
        Ok(Self {
            reader,
            messages,
            current_index: 0,
            current_timestamp: start_timestamp,
        })
    }

    /// Parse all messages from the RRD file.
    fn parse_messages(reader: &RrdReader) -> Result<Vec<(Vec<u8>, String)>> {
        use std::io::Read;

        use super::constants::{
            MESSAGE_HEADER_SIZE, MSG_KIND_ARROW_MSG, MSG_KIND_END, MSG_KIND_SET_STORE_INFO,
            RRD_MAGIC, STREAM_FOOTER_SIZE, STREAM_HEADER_SIZE,
        };

        let mut file = std::fs::File::open(&reader.path)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to open file: {}", e)))?;

        // Skip stream header (STREAM_HEADER_SIZE bytes)
        let mut header_buf = vec![0u8; STREAM_HEADER_SIZE];
        file.read_exact(&mut header_buf)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read header: {}", e)))?;

        // Verify magic
        if &header_buf[0..4] != RRD_MAGIC {
            return Err(CodecError::parse(
                "RRD",
                format!("Invalid magic: {:?}", &header_buf[0..4]),
            ));
        }

        let mut messages = Vec::new();
        let mut data_buf = Vec::new();

        // Read remaining file data
        file.read_to_end(&mut data_buf)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read data: {}", e)))?;

        let mut pos = 0;

        // Parse messages until we reach the footer
        while pos + MESSAGE_HEADER_SIZE <= data_buf.len() {
            // Check if we're at the footer (last 32 bytes)
            if pos + STREAM_FOOTER_SIZE <= data_buf.len() {
                let footer_start = data_buf.len() - STREAM_FOOTER_SIZE;
                if pos >= footer_start {
                    break;
                }
            }

            // Read message header: kind(u64 le) + len(u64 le)
            let kind = u64::from_le_bytes(data_buf[pos..pos + 8].try_into().unwrap_or([0u8; 8]));
            let len = u64::from_le_bytes(data_buf[pos + 8..pos + 16].try_into().unwrap_or([0u8; 8]))
                as usize;

            pos += MESSAGE_HEADER_SIZE;

            // Check for end marker
            if kind == MSG_KIND_END {
                break;
            }

            // Extract topic based on message kind
            let topic = match kind {
                MSG_KIND_ARROW_MSG => "/".to_string(),
                MSG_KIND_SET_STORE_INFO => "/store/info".to_string(),
                _ => "/".to_string(),
            };

            // Read payload if we have data
            if pos + len <= data_buf.len() {
                let payload = data_buf[pos..pos + len].to_vec();

                // Parse ArrowMsg protobuf and decompress
                let arrow_msg = ArrowMsg::from_bytes(&payload).map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to parse ArrowMsg: {e}"))
                })?;

                let decompressed = arrow_msg.decompress_payload().map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to decompress ArrowMsg: {e}"))
                })?;

                messages.push((decompressed, topic));
                pos += len;
            } else {
                break;
            }
        }

        Ok(messages)
    }

    /// Create a stream for use with the unified API.
    pub fn stream(
        self,
    ) -> Result<std::vec::IntoIter<Result<(TimestampedDecodedMessage, ChannelInfo)>>> {
        use crate::core::CodecValue;

        let mut results = Vec::new();
        let start_timestamp = self.current_timestamp;

        for (index, (data, topic)) in self.messages.into_iter().enumerate() {
            let channel = self
                .reader
                .channels()
                .get(&0)
                .cloned()
                .unwrap_or_else(|| ChannelInfo {
                    id: 0,
                    topic: topic.clone(),
                    message_type: "rerun.ArrowMsg".to_string(),
                    encoding: "protobuf".to_string(),
                    schema: None,
                    schema_data: None,
                    schema_encoding: Some("protobuf".to_string()),
                    message_count: 0,
                    callerid: None,
                });

            // Create decoded message with raw data as bytes field
            let mut message = DecodedMessage::new();
            message.insert("data".to_string(), CodecValue::Bytes(data));

            let timestamped = TimestampedDecodedMessage {
                message,
                log_time: start_timestamp + index as u64,
                publish_time: start_timestamp + index as u64,
            };

            results.push(Ok((timestamped, channel)));
        }

        Ok(results.into_iter())
    }
}

impl<'a> Iterator for DecodedMessageWithTimestampIter<'a> {
    type Item = Result<(TimestampedDecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::core::CodecValue;

        if self.current_index >= self.messages.len() {
            return None;
        }

        let (data, topic) = &self.messages[self.current_index];
        self.current_index += 1;

        // Get channel info
        let channel = self
            .reader
            .channels()
            .get(&0)
            .cloned()
            .unwrap_or_else(|| ChannelInfo {
                id: 0,
                topic: topic.clone(),
                message_type: "rerun.ArrowMsg".to_string(),
                encoding: "protobuf".to_string(),
                schema: None,
                schema_data: None,
                schema_encoding: Some("protobuf".to_string()),
                message_count: 0,
                callerid: None,
            });

        // Create decoded message with raw data as bytes field
        let mut message = DecodedMessage::new();
        message.insert("data".to_string(), CodecValue::Bytes(data.clone()));

        // Create timestamped decoded message
        let timestamped = TimestampedDecodedMessage {
            message,
            log_time: self.current_timestamp,
            publish_time: self.current_timestamp,
        };

        self.current_timestamp += 1;

        Some(Ok((timestamped, channel)))
    }
}

/// Stream type for decoded messages with timestamps.
///
/// This type alias is used for compatibility with the unified API.
pub type DecodedMessageWithTimestampStream<'a> =
    std::vec::IntoIter<Result<(TimestampedDecodedMessage, ChannelInfo)>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_rrd_magic() {
        // RRF2 is the current Rerun RRD format
        assert_eq!(RRD_MAGIC, b"RRF2");
        assert_eq!(RRD_FOOTER_MAGIC, b"FOOT");
    }

    #[test]
    fn test_constants() {
        // RRF2 uses 4-byte version
        assert_eq!(RRD_VERSION, [0, 0, 0, 1]);
        assert_eq!(COMPRESSION_OFF, 0);
        assert_eq!(COMPRESSION_LZ4, 1);
        assert_eq!(SERIALIZER_MSGPACK, 1);
        assert_eq!(SERIALIZER_PROTOBUF, 2);
    }

    #[test]
    fn test_compression_names() {
        let header = RrdHeader {
            magic: *RRD_MAGIC,
            version: RRD_VERSION,
            compression: COMPRESSION_OFF,
            serializer: SERIALIZER_PROTOBUF,
        };
        assert_eq!(header.compression_name(), "off");
        assert_eq!(header.serializer_name(), "protobuf");
    }

    #[test]
    fn test_rrd_format_type() {
        // Test that RrdFormat can be used as a factory
        assert_eq!(std::mem::size_of::<RrdFormat>(), 0);
    }

    fn create_test_rrd_file(path: &str) -> std::io::Result<()> {
        let mut file = std::fs::File::create(path)?;

        // Write RRF2 stream header (12 bytes): fourcc(4) + version(4) + options(4)
        file.write_all(RRD_MAGIC)?; // fourcc: "RRF2"
        file.write_all(&RRD_VERSION)?; // version: [0, 0, 0, 1]

        // Write options: compression(1) + serializer(1) + reserved(2)
        file.write_all(&[COMPRESSION_OFF])?; // compression
        file.write_all(&[SERIALIZER_PROTOBUF])?; // serializer
        file.write_all(&[0u8, 0u8])?; // reserved

        // Write RRF2 stream footer (32 bytes)
        // For an empty file, just write zeros with the footer magic at the end
        let footer_data = vec![0u8; STREAM_FOOTER_SIZE - RRD_FOOTER_MAGIC.len()];
        file.write_all(&footer_data)?;
        file.write_all(RRD_FOOTER_MAGIC)?;

        Ok(())
    }

    #[test]
    fn test_open_nonexistent_file() {
        let result = RrdReader::open("/nonexistent/path/to/file.rrd");
        assert!(result.is_err());
    }

    #[test]
    fn test_open_invalid_magic() {
        let temp_path = std::env::temp_dir().join("test_invalid_magic.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(b"INVALID").unwrap();
        }

        let result = RrdReader::open(&temp_path);
        assert!(result.is_err());

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_message_count_zero() {
        let temp_path = std::env::temp_dir().join("test_empty.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.message_count(), 0);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_format() {
        let temp_path = std::env::temp_dir().join("test_format.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.format(), crate::io::metadata::FileFormat::Rrd);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_file_size() {
        let temp_path = std::env::temp_dir().join("test_size.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert!(reader.file_size() > 0);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_path() {
        let temp_path = std::env::temp_dir().join("test_path.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert!(reader.path().contains("test_path.rrd"));

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_chunk_count() {
        let temp_path = std::env::temp_dir().join("test_chunk_count.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.chunk_count(), 0);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_channels() {
        let temp_path = std::env::temp_dir().join("test_channels.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert!(!reader.channels().is_empty());
        assert!(reader.channel_by_topic(DEFAULT_TOPIC).is_some());

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_time_bounds() {
        let temp_path = std::env::temp_dir().join("test_time.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        // Empty file should have no time bounds
        assert_eq!(reader.start_time(), None);
        assert_eq!(reader.end_time(), None);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_decode_messages() {
        let temp_path = std::env::temp_dir().join("test_decode.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages().unwrap();
        // Iterator should be created but return no messages for empty file
        assert_eq!(iter.count(), 0);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_decode_messages_with_timestamp() {
        let temp_path = std::env::temp_dir().join("test_timestamp.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages_with_timestamp().unwrap();
        // Iterator should be created but return no messages for empty file
        assert_eq!(iter.count(), 0);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_reader_header() {
        let temp_path = std::env::temp_dir().join("test_header.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let header = reader.header();
        assert_eq!(header.version, RRD_VERSION);
        assert_eq!(header.compression, COMPRESSION_OFF);
        assert_eq!(header.serializer, SERIALIZER_PROTOBUF);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_all_compression_names() {
        let header = RrdHeader {
            magic: *RRD_MAGIC,
            version: RRD_VERSION,
            compression: COMPRESSION_LZ4,
            serializer: SERIALIZER_PROTOBUF,
        };
        assert_eq!(header.compression_name(), "lz4");

        let header_off = RrdHeader {
            compression: COMPRESSION_OFF,
            ..header
        };
        assert_eq!(header_off.compression_name(), "off");

        let header_unknown = RrdHeader {
            compression: 255,
            ..header
        };
        assert_eq!(header_unknown.compression_name(), "unknown");
    }

    #[test]
    fn test_all_serializer_names() {
        let header = RrdHeader {
            magic: *RRD_MAGIC,
            version: RRD_VERSION,
            compression: COMPRESSION_OFF,
            serializer: SERIALIZER_PROTOBUF,
        };
        assert_eq!(header.serializer_name(), "protobuf");

        let header_msgpack = RrdHeader {
            serializer: SERIALIZER_MSGPACK,
            ..header
        };
        assert_eq!(header_msgpack.serializer_name(), "msgpack");

        let header_unknown = RrdHeader {
            serializer: 255,
            ..header
        };
        assert_eq!(header_unknown.serializer_name(), "unknown");
    }

    #[test]
    fn test_unsupported_version() {
        let temp_path = std::env::temp_dir().join("test_version.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            // Write RRF2 header with unsupported version
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&[1, 0, 0, 0]).unwrap(); // unsupported version [1, 0, 0, 0]
            file.write_all(&0u32.to_le_bytes()).unwrap(); // options
            // Write minimal footer
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let result = RrdReader::open(&temp_path);
        assert!(result.is_err());

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_channel_by_topic_not_found() {
        let temp_path = std::env::temp_dir().join("test_not_found.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert!(reader.channel_by_topic("/nonexistent/topic").is_none());

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_as_any() {
        let temp_path = std::env::temp_dir().join("test_any.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        // Test as_any
        let _any: &dyn std::any::Any = reader.as_any();
        // Test as_any_mut
        let mut reader_mut = RrdReader::open(&temp_path).unwrap();
        let _any_mut: &mut dyn std::any::Any = reader_mut.as_any_mut();

        std::fs::remove_file(&temp_path).ok();
    }
}
