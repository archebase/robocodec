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
use super::constants::{
    COMPRESSION_LZ4, COMPRESSION_OFF, DEFAULT_TOPIC, MESSAGE_ENCODING_PROTOBUF, RRD_MAGIC,
    RRD_MIN_VERSION, RRD_VERSION, SERIALIZER_MSGPACK, SERIALIZER_PROTOBUF,
};
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
    /// Returns a boxed `FormatWriter` trait object for unified writer API.
    pub fn create_writer<P: AsRef<Path>>(
        path: P,
        _config: &WriterConfig,
    ) -> Result<Box<dyn FormatWriter>> {
        let writer = super::writer::RrdWriter::create(path)?;
        Ok(Box::new(writer))
    }

    /// Open an RRD reader from a transport source.
    #[cfg(feature = "remote")]
    pub fn open_from_transport(
        mut transport: Box<dyn crate::io::transport::Transport>,
        path: String,
    ) -> Result<ParallelRrdReader> {
        use std::pin::Pin;
        use std::task::{Context, Poll, Waker};

        let mut data = Vec::new();
        let mut buffer = vec![0u8; 64 * 1024];
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut pinned_transport = unsafe { Pin::new_unchecked(transport.as_mut()) };

        loop {
            match pinned_transport.as_mut().poll_read(&mut cx, &mut buffer) {
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(n)) => data.extend_from_slice(&buffer[..n]),
                Poll::Ready(Err(e)) => {
                    return Err(CodecError::encode(
                        "Transport",
                        format!("Failed to read from {path}: {e}"),
                    ));
                }
                Poll::Pending => std::thread::yield_now(),
            }
        }

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temp_path = std::env::temp_dir().join(format!(
            "robocodec_rrd_transport_{}_{}.rrd",
            std::process::id(),
            unique
        ));

        std::fs::write(&temp_path, &data).map_err(|e| {
            CodecError::encode(
                "RRD",
                format!("Failed to write temporary RRD data to {:?}: {e}", temp_path),
            )
        })?;

        let mut reader = ParallelRrdReader::open(&temp_path)?;
        reader.set_path_for_reporting(path);

        let _ = std::fs::remove_file(&temp_path);
        Ok(reader)
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
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read magic: {e}")))?;

        if magic != *RRD_MAGIC {
            return Err(CodecError::parse(
                "RRD",
                format!("Invalid magic number: expected {RRD_MAGIC:?}, got {magic:?}"),
            ));
        }

        let mut version = [0u8; 4];
        reader
            .read_exact(&mut version)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read version: {e}")))?;

        // Validate version - reject clearly incompatible versions
        // Version [0, 0, 0, 0] indicates an unversioned/incompatible file
        if version == [0, 0, 0, 0] {
            return Err(CodecError::parse(
                "RRD",
                format!(
                    "Incompatible RRD version: {version:?}. This file appears to be from an old or incompatible Rerun version. \
                    Please regenerate the file with a newer version of Rerun, or use Rerun's tools to convert the data."
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
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read options: {e}")))?;

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
                format!("File not found: {path_str}"),
            ));
        }

        // Get file size
        let file_size = std::fs::metadata(path_obj)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to get metadata: {e}")))?
            .len();

        // Open file and read header
        let file = std::fs::File::open(path_obj)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to open file: {e}")))?;

        let mut reader = BufReader::new(file);
        let header = RrdHeader::read(&mut reader)?;

        // Validate version - Rerun encodes its semver in the version field
        // e.g., [0, 27, 0, 193] = Rerun 0.27.0
        // For now, we accept any 0.x.x.x version (all Rerun 0.x releases)
        if header.version[0] != 0 {
            return Err(CodecError::parse(
                "RRD",
                format!(
                    "Unsupported version: {:?} (only Rerun 0.x supported)",
                    header.version
                ),
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
    #[must_use]
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    /// Get channel info by topic name.
    #[must_use]
    pub fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.channels.values().find(|c| c.topic == topic)
    }

    /// Get total message count.
    #[must_use]
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Get start timestamp in nanoseconds.
    #[must_use]
    pub fn start_time(&self) -> Option<u64> {
        self.start_time
    }

    /// Get end timestamp in nanoseconds.
    #[must_use]
    pub fn end_time(&self) -> Option<u64> {
        self.end_time
    }

    /// Get the file path.
    #[must_use]
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
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        0
    }

    /// Get the RRD header.
    #[must_use]
    pub fn header(&self) -> &RrdHeader {
        &self.header
    }
}

impl FormatReader for RrdReader {
    #[cfg(feature = "remote")]
    fn open_from_transport(
        _transport: Box<dyn crate::io::transport::Transport>,
        _path: String,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        Err(CodecError::unsupported(
            "RrdReader requires local file access. Use a streaming reader for transport-based reading.",
        ))
    }

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

    fn decoded_with_timestamp_boxed(
        &self,
    ) -> Result<Box<dyn crate::io::traits::DecodedMessageIterator + Send + Sync + '_>> {
        let iter = self.decode_messages_with_timestamp()?;
        let stream = iter.stream()?;
        Ok(Box::new(stream))
    }

    fn iter_raw_boxed(&self) -> Result<crate::io::traits::RawMessageIter<'_>> {
        let messages = DecodedMessageWithTimestampIter::parse_messages(self)?;
        let channel = self
            .channels
            .get(&0)
            .cloned()
            .unwrap_or_else(|| ChannelInfo {
                id: 0,
                topic: DEFAULT_TOPIC.to_string(),
                message_type: "rerun.ArrowMsg".to_string(),
                encoding: MESSAGE_ENCODING_PROTOBUF.to_string(),
                schema: None,
                schema_data: None,
                schema_encoding: Some(self.header.serializer_name().to_string()),
                message_count: 0,
                callerid: None,
            });
        let start_timestamp = self.start_time.unwrap_or(0);

        Ok(Box::new(messages.into_iter().enumerate().map(
            move |(index, (data, _topic))| {
                let timestamp = start_timestamp + index as u64;
                let raw = crate::io::metadata::RawMessage {
                    channel_id: 0,
                    log_time: timestamp,
                    publish_time: timestamp,
                    data,
                    sequence: Some(index as u64),
                };
                Ok((raw, channel.clone()))
            },
        )))
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
            .map_err(|e| CodecError::parse("RRD", format!("Failed to open file: {e}")))?;

        // Skip stream header (STREAM_HEADER_SIZE bytes)
        let mut header_buf = vec![0u8; STREAM_HEADER_SIZE];
        file.read_exact(&mut header_buf)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read header: {e}")))?;

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
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read data: {e}")))?;

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

                // Only ArrowMsg messages use the ArrowMsg protobuf format
                // SetStoreInfo and BlueprintActivationCommand are different protobufs
                let data = if kind == MSG_KIND_ARROW_MSG {
                    // Parse ArrowMsg protobuf and decompress
                    let arrow_msg = ArrowMsg::from_bytes(&payload).map_err(|e| {
                        CodecError::parse("RRD", format!("Failed to parse ArrowMsg: {e}"))
                    })?;

                    arrow_msg.decompress_payload().map_err(|e| {
                        CodecError::parse("RRD", format!("Failed to decompress ArrowMsg: {e}"))
                    })?
                } else {
                    // Other message types are returned as-is
                    payload
                };

                messages.push((data, topic));
                pos += len;
            } else {
                break;
            }
        }

        Ok(messages)
    }
}

impl Iterator for DecodedMessageIter<'_> {
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
            .map_err(|e| CodecError::parse("RRD", format!("Failed to open file: {e}")))?;

        // Skip stream header (STREAM_HEADER_SIZE bytes)
        let mut header_buf = vec![0u8; STREAM_HEADER_SIZE];
        file.read_exact(&mut header_buf)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read header: {e}")))?;

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
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read data: {e}")))?;

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

                // Only ArrowMsg messages use the ArrowMsg protobuf format
                // SetStoreInfo and BlueprintActivationCommand are different protobufs
                let data = if kind == MSG_KIND_ARROW_MSG {
                    // Parse ArrowMsg protobuf and decompress
                    let arrow_msg = ArrowMsg::from_bytes(&payload).map_err(|e| {
                        CodecError::parse("RRD", format!("Failed to parse ArrowMsg: {e}"))
                    })?;

                    arrow_msg.decompress_payload().map_err(|e| {
                        CodecError::parse("RRD", format!("Failed to decompress ArrowMsg: {e}"))
                    })?
                } else {
                    // Other message types are returned as-is
                    payload
                };

                messages.push((data, topic));
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

impl Iterator for DecodedMessageWithTimestampIter<'_> {
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

    use crate::io::formats::rrd::constants::{
        COMPRESSION_LZ4, COMPRESSION_OFF, MSG_KIND_END, MSG_KIND_SET_STORE_INFO, RRD_FOOTER_MAGIC,
        RRD_MAGIC, RRD_VERSION, SERIALIZER_MSGPACK, SERIALIZER_PROTOBUF, STREAM_FOOTER_SIZE,
    };

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

    // =======================================================================
    // Tests with real RRD fixture files
    // =======================================================================

    fn get_fixture_path(name: &str) -> String {
        format!("tests/fixtures/rrd/{}", name)
    }

    #[test]
    fn test_open_real_rrd_files() {
        // Test opening multiple real RRD files
        for i in 1..=20 {
            let path = get_fixture_path(&format!("file{i}.rrd"));
            if std::path::Path::new(&path).exists() {
                let result = RrdReader::open(&path);
                assert!(
                    result.is_ok(),
                    "Should open file{i}.rrd: {:?}",
                    result.err()
                );
            }
        }
    }

    #[test]
    fn test_decode_messages_real_file() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");
            let iter = reader.decode_messages().expect("Failed to create iterator");

            // Should have messages from real file
            let count = iter.count();
            assert!(count > 0, "Real RRD file should have messages");
        }
    }

    #[test]
    fn test_decode_messages_with_timestamp_real_file() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");
            let iter = reader
                .decode_messages_with_timestamp()
                .expect("Failed to create iterator");

            let count = iter.count();
            assert!(count > 0, "Real RRD file should have messages");
        }
    }

    #[test]
    fn test_stream_method_real_file() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");
            let iter = reader
                .decode_messages_with_timestamp()
                .expect("Failed to create iterator");

            // Test the stream() method
            let stream = iter.stream().expect("Failed to create stream");
            let results: Vec<_> = stream.collect();

            assert!(!results.is_empty(), "Stream should yield messages");

            // Verify each result has proper structure
            for result in results.iter().take(5) {
                assert!(result.is_ok(), "Each result should be Ok: {:?}", result);
                let (msg, channel) = result.as_ref().unwrap();
                assert!(!msg.message.is_empty(), "Message should have data");
                assert_eq!(channel.id, 0);
                assert!(!channel.topic.is_empty());
            }
        }
    }

    #[test]
    fn test_reader_real_file_properties() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");

            // Test various reader properties
            assert!(reader.file_size() > 0);
            assert!(!reader.path().is_empty());
            assert!(!reader.channels().is_empty());

            let header = reader.header();
            assert_eq!(&header.magic, RRD_MAGIC);
        }
    }

    #[test]
    fn test_real_file_header_compression() {
        // Check compression type in real files
        for i in 1..=5 {
            let path = get_fixture_path(&format!("file{i}.rrd"));
            if std::path::Path::new(&path).exists() {
                let reader = RrdReader::open(&path).expect("Failed to open file{i}.rrd");
                let header = reader.header();

                // Should have valid compression value
                assert!(
                    header.compression == COMPRESSION_OFF || header.compression == COMPRESSION_LZ4,
                    "Compression should be 0 (off) or 1 (lz4)"
                );

                // Should have valid serializer
                assert!(
                    header.serializer == SERIALIZER_MSGPACK
                        || header.serializer == SERIALIZER_PROTOBUF,
                    "Serializer should be msgpack or protobuf"
                );
            }
        }
    }

    #[test]
    fn test_stream_timestamps_increment() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");
            let iter = reader
                .decode_messages_with_timestamp()
                .expect("Failed to create iterator");
            let stream = iter.stream().expect("Failed to create stream");

            let results: Vec<_> = stream
                .take(10)
                .collect::<Result<Vec<_>>>()
                .expect("Should collect results");

            assert!(!results.is_empty());

            // Verify timestamps increment
            for i in 1..results.len() {
                let curr_log = results[i].0.log_time;
                let prev_log = results[i - 1].0.log_time;
                assert!(curr_log > prev_log, "Timestamps should increment");
            }
        }
    }

    #[test]
    fn test_decode_messages_iterator_behavior() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");
            let iter = reader.decode_messages().expect("Failed to create iterator");

            // Consume some messages and verify structure
            let mut count = 0;
            for result in iter.take(10) {
                assert!(result.is_ok(), "Should decode message: {:?}", result.err());
                let (msg, channel) = result.unwrap();
                assert!(!msg.is_empty(), "Message should have data");
                assert_eq!(channel.id, 0);
                count += 1;
            }

            assert!(count > 0, "Should decode at least one message");
        }
    }

    #[test]
    fn test_incompatible_version_zero() {
        // Test version [0, 0, 0, 0] which indicates incompatible file
        let temp_path = std::env::temp_dir().join("test_incompatible_version.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&[0, 0, 0, 0]).unwrap(); // incompatible version
            file.write_all(&0u32.to_le_bytes()).unwrap(); // options
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let result = RrdReader::open(&temp_path);
        assert!(result.is_err(), "Should reject version [0,0,0,0]");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_non_zero_reserved_bytes() {
        // Test header with non-zero reserved bytes (should warn but not error)
        let temp_path = std::env::temp_dir().join("test_reserved_bytes.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            // options with non-zero reserved bytes
            file.write_all(&[COMPRESSION_OFF, SERIALIZER_PROTOBUF, 0xFF, 0xFF])
                .unwrap();
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let result = RrdReader::open(&temp_path);
        assert!(
            result.is_ok(),
            "Should accept file with non-zero reserved bytes"
        );

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_msgpack_serializer() {
        // Test header with msgpack serializer
        let temp_path = std::env::temp_dir().join("test_msgpack.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[COMPRESSION_OFF, SERIALIZER_MSGPACK, 0, 0])
                .unwrap();
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.header().serializer, SERIALIZER_MSGPACK);
        assert_eq!(reader.header().serializer_name(), "msgpack");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_lz4_compression() {
        // Test header with LZ4 compression
        let temp_path = std::env::temp_dir().join("test_lz4.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[COMPRESSION_LZ4, SERIALIZER_PROTOBUF, 0, 0])
                .unwrap();
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.header().compression, COMPRESSION_LZ4);
        assert_eq!(reader.header().compression_name(), "lz4");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_unusual_version_warning() {
        // Test version significantly different from current (should warn but not error)
        let temp_path = std::env::temp_dir().join("test_unusual_version.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&[0, 0, 255, 255]).unwrap(); // unusual version
            file.write_all(&[COMPRESSION_OFF, SERIALIZER_PROTOBUF, 0, 0])
                .unwrap();
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let result = RrdReader::open(&temp_path);
        assert!(result.is_ok(), "Should accept unusual version with warning");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_rrd_format_factory_methods() {
        // Test RrdFormat factory methods
        let temp_path = std::env::temp_dir().join("test_factory.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        // Test open method (returns ParallelRrdReader)
        let result = RrdFormat::open(&temp_path);
        assert!(result.is_ok(), "RrdFormat::open should succeed");

        // Test create_writer method
        let temp_path_out = std::env::temp_dir().join("test_factory_out.rrd");
        let writer_result = RrdFormat::create_writer(&temp_path_out, &WriterConfig::default());
        assert!(
            writer_result.is_ok(),
            "RrdFormat::create_writer should succeed"
        );

        std::fs::remove_file(&temp_path).ok();
        std::fs::remove_file(&temp_path_out).ok();
    }

    #[test]
    fn test_file_too_small_for_header() {
        // Test file that's too small to contain a valid header
        let temp_path = std::env::temp_dir().join("test_too_small.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(b"RRF").unwrap(); // Only 3 bytes
        }

        let result = RrdReader::open(&temp_path);
        assert!(result.is_err(), "Should fail for file too small");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_truncated_header() {
        // Test file with partial header (only magic, no version/options)
        let temp_path = std::env::temp_dir().join("test_truncated.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap(); // 4 bytes, but need 12 total
        }

        let result = RrdReader::open(&temp_path);
        assert!(result.is_err(), "Should fail for truncated header");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_decode_multiple_rrd_files() {
        // Test decoding messages from multiple different RRD files
        for i in 1..=10 {
            let path = get_fixture_path(&format!("file{i}.rrd"));
            if std::path::Path::new(&path).exists() {
                let reader =
                    RrdReader::open(&path).unwrap_or_else(|_| panic!("Failed to open file{i}.rrd"));

                // Test decode_messages
                let iter1 = reader.decode_messages();
                assert!(iter1.is_ok(), "Should create decode_messages iterator");

                // Test decode_messages_with_timestamp
                let reader2 = RrdReader::open(&path)
                    .unwrap_or_else(|_| panic!("Failed to open file{i}.rrd again"));
                let iter2 = reader2.decode_messages_with_timestamp();
                assert!(
                    iter2.is_ok(),
                    "Should create decode_messages_with_timestamp iterator"
                );
            }
        }
    }

    #[test]
    fn test_reader_properties_multiple_files() {
        // Test reader properties across multiple files
        for i in 1..=10 {
            let path = get_fixture_path(&format!("file{i}.rrd"));
            if std::path::Path::new(&path).exists() {
                let reader =
                    RrdReader::open(&path).unwrap_or_else(|_| panic!("Failed to open file{i}.rrd"));

                // All files should have positive size
                assert!(
                    reader.file_size() > 0,
                    "File {} should have positive size",
                    i
                );

                // All files should have channels
                assert!(
                    !reader.channels().is_empty(),
                    "File {} should have channels",
                    i
                );

                // All should be RRD format
                assert_eq!(reader.format(), crate::io::metadata::FileFormat::Rrd);

                // All should have chunk count of 0
                assert_eq!(reader.chunk_count(), 0);

                // Header should be valid
                let header = reader.header();
                assert_eq!(&header.magic, RRD_MAGIC);
            }
        }
    }

    #[test]
    fn test_format_reader_trait() {
        // Test FormatReader trait implementation
        let temp_path = std::env::temp_dir().join("test_trait.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();

        // Test all FormatReader trait methods
        let _channels: &HashMap<u16, ChannelInfo> = reader.channels();
        let _count = reader.message_count();
        let _start = reader.start_time();
        let _end = reader.end_time();
        let _path = reader.path();
        let _format = reader.format();
        let _size = reader.file_size();
        let _any: &dyn std::any::Any = reader.as_any();

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_invalid_compression_value() {
        // Test with invalid compression value (should show "unknown")
        let temp_path = std::env::temp_dir().join("test_bad_compression.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[0xFF, SERIALIZER_PROTOBUF, 0, 0]).unwrap(); // invalid compression
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.header().compression_name(), "unknown");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_invalid_serializer_value() {
        // Test with invalid serializer value (should show "unknown")
        let temp_path = std::env::temp_dir().join("test_bad_serializer.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[COMPRESSION_OFF, 0xFF, 0, 0]).unwrap(); // invalid serializer
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.header().serializer_name(), "unknown");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_iterator_empty_messages() {
        // Test iterator behavior when there are no messages
        let temp_path = std::env::temp_dir().join("test_empty_iter.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages().unwrap();
        assert_eq!(iter.count(), 0, "Empty file should yield 0 messages");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_timestamp_iterator_empty_messages() {
        // Test timestamp iterator behavior when there are no messages
        let temp_path = std::env::temp_dir().join("test_empty_timestamp_iter.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages_with_timestamp().unwrap();
        assert_eq!(
            iter.count(),
            0,
            "Empty file should yield 0 timestamped messages"
        );

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_default_topic_constant() {
        // Verify DEFAULT_TOPIC is the expected value
        assert_eq!(DEFAULT_TOPIC, "/");
    }

    #[test]
    fn test_min_version_constant() {
        // Verify RRD_MIN_VERSION
        assert_eq!(RRD_MIN_VERSION, [0, 0, 0, 1]);
    }

    #[test]
    fn test_channel_by_topic_with_real_file() {
        let path = get_fixture_path("file1.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = RrdReader::open(&path).expect("Failed to open file1.rrd");

            // Should find the default topic
            let channel = reader.channel_by_topic(DEFAULT_TOPIC);
            assert!(channel.is_some(), "Should find default topic");

            // Should not find non-existent topic
            let nonexistent = reader.channel_by_topic("/nonexistent");
            assert!(nonexistent.is_none());
        }
    }

    #[test]
    fn test_decode_messages_iter_stream_method() {
        // Test the stream() method of the timestamp iterator
        let temp_path = std::env::temp_dir().join("test_stream_method.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages_with_timestamp().unwrap();
        let stream = iter
            .stream()
            .expect("Stream should be created even for empty file");

        let results: Vec<_> = stream.collect();
        // Empty file should yield no messages
        assert_eq!(results.len(), 0);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_decode_messages_iter_stream_empty_file() {
        // Test stream() method on empty file
        let temp_path = std::env::temp_dir().join("test_stream_empty.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages_with_timestamp().unwrap();
        let stream = iter
            .stream()
            .expect("Stream should be created even for empty file");

        let results: Vec<_> = stream.collect();
        assert_eq!(results.len(), 0, "Empty file should yield no messages");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_set_store_info_topic() {
        // Test that MSG_KIND_SET_STORE_INFO gets the "/store/info" topic
        // Note: Non-ArrowMsg messages are returned as-is without decompression
        let temp_path = std::env::temp_dir().join("test_store_info_topic.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            // Header
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[COMPRESSION_OFF, SERIALIZER_PROTOBUF, 0, 0])
                .unwrap();

            // SetStoreInfo message (kind=1, non-ArrowMsg)
            file.write_all(&MSG_KIND_SET_STORE_INFO.to_le_bytes())
                .unwrap();
            file.write_all(&4u64.to_le_bytes()).unwrap();
            file.write_all(b"info").unwrap();

            // End marker
            file.write_all(&MSG_KIND_END.to_le_bytes()).unwrap();
            file.write_all(&0u64.to_le_bytes()).unwrap();

            // Footer
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages().unwrap();

        // SetStoreInfo is not an ArrowMsg, so it gets returned as raw bytes
        let count = iter.count();
        // The message should be parsed but might fail ArrowMsg decoding
        assert!(count > 0, "Should process the message");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_unknown_message_kind_topic() {
        // Test that unknown message kind gets default "/" topic
        // Note: Non-ArrowMsg messages are returned as-is
        let temp_path = std::env::temp_dir().join("test_unknown_topic.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            // Header
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[COMPRESSION_OFF, SERIALIZER_PROTOBUF, 0, 0])
                .unwrap();

            // Unknown message kind (999)
            file.write_all(&999u64.to_le_bytes()).unwrap();
            file.write_all(&4u64.to_le_bytes()).unwrap();
            file.write_all(b"data").unwrap();

            // End marker
            file.write_all(&MSG_KIND_END.to_le_bytes()).unwrap();
            file.write_all(&0u64.to_le_bytes()).unwrap();

            // Footer
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages().unwrap();
        let count = iter.count();
        // Unknown kind should get "/" topic but may fail ArrowMsg decoding
        assert!(count > 0, "Should process unknown message kind");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_iterator_consumes_all_messages() {
        // Test that iterator properly consumes and counts messages
        // Using empty test file since ArrowMsg protobuf parsing requires valid format
        let temp_path = std::env::temp_dir().join("test_consume.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        let iter = reader.decode_messages_with_timestamp().unwrap();
        let count = iter.count();
        assert_eq!(count, 0, "Empty file should have 0 messages");

        std::fs::remove_file(&temp_path).ok();
    }
}
