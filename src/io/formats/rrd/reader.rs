// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! RRD file reader with automatic encoding detection.
//!
//! This module provides `RrdReader` for reading Rerun RRD files with support for
//! various encodings used by Rerun.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use tracing::warn;

use crate::core::{CodecError, DecodedMessage, Result};
use crate::encoding::{CdrDecoder, JsonDecoder, ProtobufDecoder};
use crate::io::traits::FormatReader;
use crate::io::writer::WriterConfig;
use crate::io::{ChannelInfo, FormatWriter, TimestampedDecodedMessage};

use super::constants::*;

/// RRD format type.
///
/// This type provides factory methods for creating RRD readers and writers.
pub struct RrdFormat;

impl RrdFormat {
    /// Create an RRD reader with decoding support.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<RrdReader> {
        RrdReader::open(path)
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

/// RRD file header.
#[derive(Debug, Clone)]
pub struct RrdHeader {
    /// Magic number ("RRD\0")
    pub magic: [u8; 4],
    /// Format version
    pub version: u16,
    /// Flags (reserved for future use)
    pub flags: u32,
    /// Compression type (0=none, 1=lz4, 2=zstd)
    pub compression: u8,
    /// Schema encoding (1=protobuf, 2=flatbuffers, 3=json)
    pub schema_encoding: u8,
    /// Chunk size
    pub chunk_size: u32,
    /// Number of chunks
    pub chunk_count: u64,
}

impl RrdHeader {
    /// Read the RRD header from a file.
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

        let version = reader
            .read_u16::<LittleEndian>()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read version: {}", e)))?;

        let flags = reader
            .read_u32::<LittleEndian>()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read flags: {}", e)))?;

        let compression = reader
            .read_u8()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read compression: {}", e)))?;

        let schema_encoding = reader.read_u8().map_err(|e| {
            CodecError::parse("RRD", format!("Failed to read schema encoding: {}", e))
        })?;

        // Skip 2 bytes (reserved)
        let mut reserved = [0u8; 2];
        reader
            .read_exact(&mut reserved)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read reserved: {}", e)))?;

        let chunk_size = reader
            .read_u32::<LittleEndian>()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read chunk size: {}", e)))?;

        // Skip 4 bytes (reserved)
        let mut reserved2 = [0u8; 4];
        reader
            .read_exact(&mut reserved2)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read reserved2: {}", e)))?;

        let chunk_count = reader
            .read_u64::<LittleEndian>()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to read chunk count: {}", e)))?;

        Ok(Self {
            magic,
            version,
            flags,
            compression,
            schema_encoding,
            chunk_size,
            chunk_count,
        })
    }

    /// Get the compression name for this header.
    fn compression_name(&self) -> &'static str {
        match self.compression {
            COMPRESSION_NONE => "none",
            COMPRESSION_LZ4 => "lz4",
            COMPRESSION_ZSTD => "zstd",
            _ => "unknown",
        }
    }

    /// Get the schema encoding name for this header.
    fn schema_encoding_name(&self) -> &'static str {
        match self.schema_encoding {
            SCHEMA_ENCODING_PROTOBUF => "protobuf",
            SCHEMA_ENCODING_FLATBUFFERS => "flatbuffers",
            SCHEMA_ENCODING_JSON => "json",
            _ => "unknown",
        }
    }
}

/// Chunk index entry for fast seeking.
#[derive(Debug, Clone)]
struct ChunkIndex {
    /// Chunk offset in file
    pub offset: u64,
    /// Chunk size in bytes (compressed)
    pub size: u32,
    /// Uncompressed size
    pub uncompressed_size: u32,
    /// Start timestamp (nanoseconds)
    pub time_start: u64,
    /// End timestamp (nanoseconds)
    pub time_end: u64,
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
    /// Chunk index for fast seeking
    chunk_index: Vec<ChunkIndex>,
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

        // Validate version
        if header.version > RRD_VERSION {
            return Err(CodecError::parse(
                "RRD",
                format!(
                    "Unsupported version: {} (supported: up to {})",
                    header.version, RRD_VERSION
                ),
            ));
        }

        // Read channel/schema information (for now, create a default channel)
        let mut channels = HashMap::new();
        let default_channel = ChannelInfo {
            id: 0,
            topic: DEFAULT_TOPIC.to_string(),
            message_type: "rerun.DataCell".to_string(),
            encoding: MESSAGE_ENCODING_PROTOBUF.to_string(),
            schema: None,
            schema_data: None,
            schema_encoding: Some(header.schema_encoding_name().to_string()),
            message_count: 0,
            callerid: None,
        };
        channels.insert(0, default_channel);

        // Read chunk index from end of file if available
        let chunk_index = Self::read_chunk_index(&path_str, &header, file_size)?;

        // Calculate message count from chunks
        let message_count = chunk_index.iter().map(|_| 1u64).sum(); // Placeholder

        // Get time range from chunks
        let (start_time, end_time) = if !chunk_index.is_empty() {
            let start = chunk_index.first().map(|c| c.time_start).unwrap_or(0);
            let end = chunk_index.last().map(|c| c.time_end).unwrap_or(0);
            (Some(start), Some(end))
        } else {
            (None, None)
        };

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
            chunk_index,
            cdr_decoder: Arc::new(CdrDecoder::new()),
            proto_decoder: Arc::new(ProtobufDecoder::new()),
            json_decoder: Arc::new(JsonDecoder::new()),
        })
    }

    /// Read the chunk index from the end of the file.
    fn read_chunk_index(path: &str, header: &RrdHeader, file_size: u64) -> Result<Vec<ChunkIndex>> {
        let _file = std::fs::File::open(path).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to open file for index: {}", e))
        })?;

        // For now, return empty index (full implementation would read index from end)
        let index = Vec::new();

        // If the file has at least header + footer, try to read the index
        if file_size > (HEADER_SIZE + FOOTER_SIZE) as u64 {
            // Skip to potential index position
            let index_offset = file_size - FOOTER_SIZE as u64 - (header.chunk_count * 24);
            if index_offset > HEADER_SIZE as u64 {
                // TODO: Implement actual index reading
            }
        }

        Ok(index)
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
        Ok(DecodedMessageIter {
            reader: self,
            current_index: 0,
        })
    }

    /// Iterate over decoded messages with timestamps.
    ///
    /// Similar to `decode_messages()` but includes the original message timestamps.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(TimestampedDecodedMessage, ChannelInfo)` tuples.
    pub fn decode_messages_with_timestamp(&self) -> Result<DecodedMessageWithTimestampIter<'_>> {
        Ok(DecodedMessageWithTimestampIter {
            reader: self,
            current_index: 0,
        })
    }

    /// Get the chunk count.
    pub fn chunk_count(&self) -> usize {
        self.chunk_index.len()
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
    /// Current chunk index
    current_index: usize,
}

impl<'a> Iterator for DecodedMessageIter<'a> {
    type Item = Result<(DecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        // For now, return None (placeholder implementation)
        // Full implementation would:
        // 1. Read next chunk from file
        // 2. Decompress (LZ4 or Zstd)
        // 3. Parse messages
        // 4. Decode based on channel encoding
        None
    }
}

/// Iterator over decoded messages with timestamps from RRD file.
pub struct DecodedMessageWithTimestampIter<'a> {
    /// Reference to the reader
    reader: &'a RrdReader,
    /// Current chunk index
    current_index: usize,
}

impl<'a> Iterator for DecodedMessageWithTimestampIter<'a> {
    type Item = Result<(TimestampedDecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        // For now, return None (placeholder implementation)
        None
    }
}

/// Stream type for decoded messages with timestamps.
///
/// This type alias is used for compatibility with the unified API.
pub type DecodedMessageWithTimestampStream<'a> =
    std::iter::Empty<Result<(TimestampedDecodedMessage, ChannelInfo)>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_rrd_magic() {
        assert_eq!(RRD_MAGIC, b"RRD\0");
        assert_eq!(RRD_FOOTER_MAGIC, b"RRD\0");
    }

    #[test]
    fn test_constants() {
        assert_eq!(RRD_VERSION, 1);
        assert_eq!(DEFAULT_CHUNK_SIZE, 256 * 1024);
        assert_eq!(MAX_CHUNK_SIZE, 16 * 1024 * 1024);
        assert_eq!(COMPRESSION_LZ4, 1);
        assert_eq!(COMPRESSION_ZSTD, 2);
        assert_eq!(COMPRESSION_NONE, 0);
        assert_eq!(SCHEMA_ENCODING_PROTOBUF, 1);
        assert_eq!(SCHEMA_ENCODING_FLATBUFFERS, 2);
        assert_eq!(SCHEMA_ENCODING_JSON, 3);
    }

    #[test]
    fn test_compression_names() {
        let header = RrdHeader {
            magic: *RRD_MAGIC,
            version: 1,
            flags: 0,
            compression: COMPRESSION_NONE,
            schema_encoding: SCHEMA_ENCODING_PROTOBUF,
            chunk_size: DEFAULT_CHUNK_SIZE as u32,
            chunk_count: 0,
        };
        assert_eq!(header.compression_name(), "none");
        assert_eq!(header.schema_encoding_name(), "protobuf");
    }

    #[test]
    fn test_rrd_format_type() {
        // Test that RrdFormat can be used as a factory
        assert_eq!(std::mem::size_of::<RrdFormat>(), 0);
    }

    fn create_test_rrd_file(path: &str) -> std::io::Result<()> {
        let mut file = std::fs::File::create(path)?;

        // Write header
        file.write_all(RRD_MAGIC)?;
        file.write_all(&1u16.to_le_bytes())?; // version
        file.write_all(&0u32.to_le_bytes())?; // flags
        file.write_all(&[COMPRESSION_LZ4])?; // compression
        file.write_all(&[SCHEMA_ENCODING_PROTOBUF])?; // schema encoding
        file.write_all(&[0u8, 0u8])?; // reserved
        file.write_all(&(DEFAULT_CHUNK_SIZE as u32).to_le_bytes())?; // chunk size
        file.write_all(&[0u8; 4])?; // reserved
        file.write_all(&0u64.to_le_bytes())?; // chunk count

        // Write footer (minimal)
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
    fn test_chunk_index() {
        let index = ChunkIndex {
            offset: 100,
            size: 1024,
            uncompressed_size: 2048,
            time_start: 1000,
            time_end: 2000,
        };
        assert_eq!(index.offset, 100);
        assert_eq!(index.size, 1024);
        assert_eq!(index.time_start, 1000);
        assert_eq!(index.time_end, 2000);
    }

    #[test]
    fn test_message_count_zero() {
        let temp_path = std::env::temp_dir().join("test_empty.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = RrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.message_count(), 0);

        std::fs::remove_file(&temp_path).ok();
    }
}
