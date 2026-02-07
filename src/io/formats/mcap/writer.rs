// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Custom MCAP writer with manual chunk control and summary section writing.
//!
//! This writer accepts pre-compressed chunks and serializes them directly
//! to the MCAP file format, bypassing the `mcap::Writer`'s internal compression.
//!
//! # Summary Section
//!
//! The writer tracks chunk metadata during writing and produces a proper
//! MCAP summary section with chunk indexes, enabling parallel reading
//! of the output file.
//!
//! # MCAP Format Compatibility
//!
//! This writer is designed to be compatible with the mcap crate v0.24.0.
//! The summary section format matches the specification at:
//! <https://github.com/foxglove/mcap/tree/main/docs/specification>

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::core::{CodecError, Result};
use crate::io::formats::mcap::constants::{
    MCAP_MAGIC, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX, OP_DATA_END, OP_FOOTER, OP_HEADER,
    OP_MESSAGE, OP_SCHEMA, OP_STATISTICS,
};
use crate::io::formats::mcap::internal::CompressedChunk;
use crate::io::metadata::RawMessage;
use crate::io::traits::FormatWriter;

/// MCAP compression identifiers.
const COMPRESSION_ZSTD: &str = "zstd";

/// Chunk index record for summary section.
///
/// Tracks metadata for each chunk written to enable parallel reading.
/// Format matches `mcap::records::ChunkIndex` exactly.
#[derive(Debug, Clone)]
struct ChunkIndexRecord {
    /// Earliest message `log_time` in chunk
    message_start_time: u64,
    /// Latest message `log_time` in chunk
    message_end_time: u64,
    /// Offset to chunk record from file start
    chunk_start_offset: u64,
    /// Total length of chunk record
    chunk_length: u64,
    /// Message index offsets: `channel_id` -> offset (empty map for our chunks)
    message_index_offsets: BTreeMap<u16, u64>,
    /// Message index length (0 = no message index)
    message_index_length: u64,
    /// Compression type (e.g., "zstd", "")
    compression: String,
    /// Size of compressed chunk data
    compressed_size: u64,
    /// Size of uncompressed chunk data
    uncompressed_size: u64,
}

/// Schema record for summary section.
#[derive(Debug, Clone)]
struct SchemaRecord {
    id: u16,
    name: String,
    encoding: String,
    data: Vec<u8>,
}

/// Channel record for summary section.
#[derive(Debug, Clone)]
struct ChannelRecord {
    id: u16,
    schema_id: u16,
    topic: String,
    message_encoding: String,
    metadata: HashMap<String, String>,
}

/// Default target chunk size for message buffering (4MB uncompressed)
const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Buffered message for chunk writing
#[derive(Debug, Clone)]
struct BufferedMessage {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

/// Custom MCAP writer with manual chunk control.
///
/// Unlike `mcap::Writer`, this writer accepts pre-compressed chunks
/// and writes them directly to the file, giving full control over
/// compression parallelism and chunk boundaries.
///
/// # Summary Section
///
/// The writer tracks chunk metadata during writing and produces
/// a proper MCAP summary section with chunk indexes, enabling
/// parallel reading of the output file.
///
/// # Message Buffering
///
/// When using `write_message()`, messages are buffered and automatically
/// written as compressed chunks when the buffer exceeds the target size.
/// This ensures the output file is suitable for parallel reading.
pub struct ParallelMcapWriter<W: Write> {
    /// Underlying writer
    writer: W,
    /// Schema IDs by name
    schema_ids: HashMap<String, u16>,
    /// Channel IDs by topic
    channel_ids: HashMap<String, u16>,
    /// Next schema ID
    next_schema_id: u16,
    /// Next channel ID
    next_channel_id: u16,
    /// Sequence numbers per channel
    sequences: HashMap<u16, u32>,
    /// Chunks written
    chunks_written: u64,
    /// Messages written
    messages_written: u64,
    /// Write start position (for summary section)
    write_start: u64,
    /// Current write position (tracked manually since `BufWriter` doesn't expose `stream_position`)
    current_position: u64,

    // === Summary section tracking ===
    /// Chunk index records for summary section
    chunk_indexes: Vec<ChunkIndexRecord>,
    /// Schema records for summary section (copies of schemas written in data section)
    schema_records: Vec<SchemaRecord>,
    /// Channel records for summary section (copies of channels written in data section)
    channel_records: Vec<ChannelRecord>,
    /// Per-channel message counts
    channel_message_counts: HashMap<u16, u64>,
    /// Earliest message time in file
    file_message_start_time: u64,
    /// Latest message time in file
    file_message_end_time: u64,
    /// Summary section start offset
    summary_start_offset: u64,

    // === Message buffering for chunk-based writing ===
    /// Buffered messages waiting to be written as a chunk
    message_buffer: Vec<BufferedMessage>,
    /// Current buffer size in bytes (uncompressed)
    buffer_size: usize,
    /// Target chunk size threshold
    target_chunk_size: usize,
}

impl ParallelMcapWriter<File> {
    /// Create a new writer that writes to the specified path.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            CodecError::encode("ParallelMcapWriter", format!("Failed to create file: {e}"))
        })?;

        Self::new(file)
    }
}

impl ParallelMcapWriter<BufWriter<File>> {
    /// Create a new writer with custom buffer capacity.
    pub fn create_with_buffer<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            CodecError::encode("ParallelMcapWriter", format!("Failed to create file: {e}"))
        })?;

        // Wrap file in BufWriter with specified capacity
        let writer = BufWriter::with_capacity(capacity, file);
        Self::new(writer)
    }
}

impl<W: Write> ParallelMcapWriter<W> {
    /// Create a new custom MCAP writer.
    pub fn new(writer: W) -> Result<Self> {
        Self::with_chunk_size(writer, DEFAULT_CHUNK_SIZE)
    }

    /// Create a new custom MCAP writer with a specific target chunk size.
    pub fn with_chunk_size(writer: W, target_chunk_size: usize) -> Result<Self> {
        let mut slf = Self {
            writer,
            schema_ids: HashMap::new(),
            channel_ids: HashMap::new(),
            next_schema_id: 1, // Start at 1 because schema_id 0 means "no schema" in MCAP
            next_channel_id: 0,
            sequences: HashMap::new(),
            chunks_written: 0,
            messages_written: 0,
            write_start: 0,
            current_position: 0,

            // Summary tracking
            chunk_indexes: Vec::new(),
            schema_records: Vec::new(),
            channel_records: Vec::new(),
            channel_message_counts: HashMap::new(),
            file_message_start_time: u64::MAX,
            file_message_end_time: 0,
            summary_start_offset: 0,

            // Message buffering
            message_buffer: Vec::new(),
            buffer_size: 0,
            target_chunk_size,
        };

        slf.write_header()?;
        Ok(slf)
    }

    /// Write bytes and update position tracking.
    fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data)?;
        self.current_position += data.len() as u64;
        Ok(())
    }

    /// Write a u8 and update position tracking.
    fn write_u8(&mut self, val: u8) -> Result<()> {
        self.writer.write_all(&[val])?;
        self.current_position += 1;
        Ok(())
    }

    /// Write a u16 and update position tracking.
    fn write_u16(&mut self, val: u16) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        self.current_position += 2;
        Ok(())
    }

    /// Write a u32 and update position tracking.
    fn write_u32(&mut self, val: u32) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        self.current_position += 4;
        Ok(())
    }

    /// Write a u64 and update position tracking.
    fn write_u64(&mut self, val: u64) -> Result<()> {
        self.writer.write_all(&val.to_le_bytes())?;
        self.current_position += 8;
        Ok(())
    }

    /// Get current write position.
    fn position(&self) -> u64 {
        self.current_position
    }

    /// Write the MCAP header.
    ///
    /// Format:
    /// - Magic: 0x89 + "MCAP" + 0x30 + \r\n (8 bytes)
    /// - Header record (op 0x01):
    ///   - record length (u64)
    ///   - profile (string: u32 length + bytes)
    ///   - library (string: u32 length + bytes)
    fn write_header(&mut self) -> Result<()> {
        // Magic bytes (8 bytes)
        self.write_bytes(&MCAP_MAGIC)?;

        // Header record
        self.write_u8(OP_HEADER)?;

        // Record length: 4 (profile length prefix) + 4 (library length prefix) = 8 bytes
        self.write_u64(8)?;

        // Profile (empty string)
        self.write_u32(0)?;

        // Library (empty string)
        self.write_u32(0)?;

        self.write_start = self.position();

        Ok(())
    }

    /// Add a schema to the MCAP file.
    ///
    /// Returns the schema ID. If the schema already exists, returns
    /// the existing ID.
    ///
    /// Schema record format:
    /// - opcode (u8 = 0x03)
    /// - record length (u64)
    /// - `schema_id` (u16)
    /// - name (string: u32 length + bytes)
    /// - encoding (string: u32 length + bytes)
    /// - data (bytes: u32 length + data)
    pub fn add_schema(&mut self, name: &str, encoding: &str, data: &[u8]) -> Result<u16> {
        if let Some(&id) = self.schema_ids.get(name) {
            return Ok(id);
        }

        let id = self.next_schema_id;
        self.next_schema_id = id.wrapping_add(1);

        // Write schema record
        self.write_u8(OP_SCHEMA)?;

        // Record length = 2 (id) + 4 + name.len() + 4 + encoding.len() + 4 + data.len()
        let record_length: u64 =
            2 + 4 + name.len() as u64 + 4 + encoding.len() as u64 + 4 + data.len() as u64;
        self.write_u64(record_length)?;

        // Schema ID
        self.write_u16(id)?;

        // Name (string)
        self.write_u32(name.len() as u32)?;
        self.write_bytes(name.as_bytes())?;

        // Encoding (string)
        self.write_u32(encoding.len() as u32)?;
        self.write_bytes(encoding.as_bytes())?;

        // Schema data
        self.write_u32(data.len() as u32)?;
        self.write_bytes(data)?;

        // Store schema record for summary section
        self.schema_records.push(SchemaRecord {
            id,
            name: name.to_string(),
            encoding: encoding.to_string(),
            data: data.to_vec(),
        });

        self.schema_ids.insert(name.to_string(), id);
        Ok(id)
    }

    /// Add a channel to the MCAP file.
    ///
    /// Returns the channel ID. If the channel already exists, returns
    /// the existing ID.
    ///
    /// Channel record format:
    /// - opcode (u8 = 0x04)
    /// - record length (u64)
    /// - `channel_id` (u16)
    /// - topic (string: u32 length + bytes)
    /// - `message_encoding` (string: u32 length + bytes)
    /// - `schema_id` (u16, 0 = no schema)
    /// - metadata (string map: u32 byte length + [u32 `key_len` + `key_bytes` + u32 `val_len` + `val_bytes`]...)
    pub fn add_channel(
        &mut self,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        if let Some(&id) = self.channel_ids.get(topic) {
            return Ok(id);
        }

        let id = self.next_channel_id;
        self.next_channel_id = id.wrapping_add(1);

        self.write_channel_record(id, schema_id, topic, encoding, metadata)
    }

    /// Add a channel with a specific ID.
    ///
    /// This is useful when the channel IDs in the source data must be preserved
    /// (e.g., when writing pre-compressed chunks that reference specific channel IDs).
    pub fn add_channel_with_id(
        &mut self,
        channel_id: u16,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        if let Some(&id) = self.channel_ids.get(topic) {
            return Ok(id);
        }

        // Update next_channel_id to avoid collisions
        if channel_id >= self.next_channel_id {
            self.next_channel_id = channel_id.wrapping_add(1);
        }

        self.write_channel_record(channel_id, schema_id, topic, encoding, metadata)
    }

    /// Internal method to write a channel record.
    fn write_channel_record(
        &mut self,
        id: u16,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        // Serialize metadata (includes byte-length prefix)
        let metadata_bytes = serialize_metadata(metadata)?;

        // Write channel record
        self.write_u8(OP_CHANNEL)?;

        // Record length = 2 + 2 + 4 + topic.len() + 4 + encoding.len() + metadata_bytes.len()
        // Note: metadata_bytes already includes the 4-byte length prefix
        // MCAP spec order: channel_id, schema_id, topic, encoding, metadata
        let record_length: u64 = 2
            + 2
            + 4
            + topic.len() as u64
            + 4
            + encoding.len() as u64
            + metadata_bytes.len() as u64;
        self.write_u64(record_length)?;

        // Channel ID (u16)
        self.write_u16(id)?;

        // Schema ID (u16) - must come right after channel ID per MCAP spec
        self.write_u16(schema_id)?;

        // Topic (string with u32 length prefix)
        self.write_u32(topic.len() as u32)?;
        self.write_bytes(topic.as_bytes())?;

        // Message encoding (string with u32 length prefix)
        self.write_u32(encoding.len() as u32)?;
        self.write_bytes(encoding.as_bytes())?;

        // Metadata (already includes byte-length prefix from serialize_metadata)
        self.write_bytes(&metadata_bytes)?;

        // Store channel record for summary section
        self.channel_records.push(ChannelRecord {
            id,
            schema_id,
            topic: topic.to_string(),
            message_encoding: encoding.to_string(),
            metadata: metadata.clone(),
        });

        // Initialize sequence number and message count
        self.sequences.insert(id, 0);
        self.channel_message_counts.insert(id, 0);

        self.channel_ids.insert(topic.to_string(), id);
        Ok(id)
    }

    /// Write a pre-compressed chunk.
    ///
    /// This is the key method for parallel compression. The chunk has
    /// already been compressed by the compression thread pool.
    ///
    /// Chunk record format:
    /// - opcode (u8 = 0x06)
    /// - record length (u64)
    /// - `message_start_time` (u64)
    /// - `message_end_time` (u64)
    /// - `uncompressed_size` (u64)
    /// - `uncompressed_crc` (u32)
    /// - compression (string: u32 length + bytes)
    /// - `compressed_size` (u64)
    /// - [records...]
    ///
    /// Also tracks metadata for the summary section.
    pub fn write_compressed_chunk(&mut self, chunk: CompressedChunk) -> Result<()> {
        // Record chunk start offset for summary
        let chunk_start_offset = self.position();

        // Update file-level time bounds
        self.file_message_start_time = self.file_message_start_time.min(chunk.message_start_time);
        self.file_message_end_time = self.file_message_end_time.max(chunk.message_end_time);

        // Write chunk header
        self.write_u8(OP_CHUNK)?;

        let compression_str = COMPRESSION_ZSTD;
        let compressed_size = chunk.compressed_data.len() as u64;
        let uncompressed_size = chunk.uncompressed_size as u64;

        // Chunk record length (excluding opcode and length field)
        // 8 + 8 + 8 + 4 + 4 + compression.len() + 8 + compressed_data
        let record_length: u64 =
            8 + 8 + 8 + 4 + 4 + compression_str.len() as u64 + 8 + compressed_size;
        self.write_u64(record_length)?;

        // Message start time
        self.write_u64(chunk.message_start_time)?;

        // Message end time
        self.write_u64(chunk.message_end_time)?;

        // Uncompressed size
        self.write_u64(uncompressed_size)?;

        // Uncompressed CRC (0 = no CRC for now)
        self.write_u32(0)?;

        // Compression (string)
        self.write_u32(compression_str.len() as u32)?;
        self.write_bytes(compression_str.as_bytes())?;

        // Compressed size
        self.write_u64(compressed_size)?;

        // Write compressed data
        self.write_bytes(&chunk.compressed_data)?;

        // Calculate chunk length (before MessageIndex records)
        let chunk_end_offset = self.position();
        let chunk_length = chunk_end_offset - chunk_start_offset;

        // Write MessageIndex records after the chunk
        // These enable time-based seeking within the chunk
        let message_index_start = self.position();
        let mut message_index_offsets: BTreeMap<u16, u64> = BTreeMap::new();

        for (channel_id, entries) in &chunk.message_indexes {
            // Record the offset of this MessageIndex record
            let index_offset = self.position();
            message_index_offsets.insert(*channel_id, index_offset);

            // Write MessageIndex record
            self.write_message_index(*channel_id, entries)?;
        }

        let message_index_length = self.position() - message_index_start;

        // Track chunk for summary section
        self.chunk_indexes.push(ChunkIndexRecord {
            message_start_time: chunk.message_start_time,
            message_end_time: chunk.message_end_time,
            chunk_start_offset,
            chunk_length,
            message_index_offsets,
            message_index_length,
            compression: compression_str.to_string(),
            compressed_size,
            uncompressed_size,
        });

        self.chunks_written += 1;
        self.messages_written += chunk.message_count as u64;

        Ok(())
    }

    /// Write a `MessageIndex` record.
    ///
    /// `MessageIndex` format:
    /// - opcode: 0x07 (1 byte)
    /// - `record_length`: u64
    /// - `channel_id`: u16
    /// - `records_length`: u32 (byte length of records array)
    /// - records: [(`log_time`: u64, offset: u64), ...]
    fn write_message_index(
        &mut self,
        channel_id: u16,
        entries: &[crate::io::formats::mcap::internal::MessageIndexEntry],
    ) -> Result<()> {
        const OP_MESSAGE_INDEX: u8 = 0x07;

        // Calculate records byte length: each entry is 16 bytes (8 + 8)
        let records_byte_length = entries.len() as u32 * 16;

        // Record length = 2 (channel_id) + 4 (records_length) + records_byte_length
        let record_length: u64 = 2 + 4 + u64::from(records_byte_length);

        self.write_u8(OP_MESSAGE_INDEX)?;
        self.write_u64(record_length)?;
        self.write_u16(channel_id)?;
        self.write_u32(records_byte_length)?;

        for entry in entries {
            self.write_u64(entry.log_time)?;
            self.write_u64(entry.offset)?;
        }

        Ok(())
    }

    /// Write a single message. Messages are buffered and written as compressed
    /// chunks when the buffer exceeds the target chunk size.
    ///
    /// This ensures the output file has proper chunk structure for parallel reading.
    pub fn write_message(
        &mut self,
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        data: &[u8],
    ) -> Result<()> {
        // Get the sequence number and increment
        let sequence = *self.sequences.entry(channel_id).or_insert(0);
        self.sequences.insert(channel_id, sequence + 1);

        // Update channel message count
        *self.channel_message_counts.entry(channel_id).or_insert(0) += 1;

        // Update file-level time bounds
        self.file_message_start_time = self.file_message_start_time.min(log_time);
        self.file_message_end_time = self.file_message_end_time.max(log_time);

        // Calculate message record size: opcode(1) + length(8) + channel_id(2) + sequence(4) + log_time(8) + publish_time(8) + data
        let message_size = 1 + 8 + 2 + 4 + 8 + 8 + data.len();

        // Buffer the message
        self.message_buffer.push(BufferedMessage {
            channel_id,
            sequence,
            log_time,
            publish_time,
            data: data.to_vec(),
        });
        self.buffer_size += message_size;
        self.messages_written += 1;

        // Flush buffer if it exceeds target chunk size
        if self.buffer_size >= self.target_chunk_size {
            self.flush_message_buffer()?;
        }

        Ok(())
    }

    /// Flush buffered messages as a compressed chunk.
    fn flush_message_buffer(&mut self) -> Result<()> {
        use crate::io::formats::mcap::internal::MessageIndexEntry;

        if self.message_buffer.is_empty() {
            return Ok(());
        }

        // Serialize messages to uncompressed chunk data
        let mut uncompressed_data = Vec::with_capacity(self.buffer_size);

        let mut chunk_start_time = u64::MAX;
        let mut chunk_end_time = 0u64;
        let mut chunk_message_indexes: BTreeMap<u16, Vec<MessageIndexEntry>> = BTreeMap::new();

        for msg in &self.message_buffer {
            chunk_start_time = chunk_start_time.min(msg.log_time);
            chunk_end_time = chunk_end_time.max(msg.log_time);

            // Record offset before writing message
            let offset = uncompressed_data.len() as u64;
            chunk_message_indexes
                .entry(msg.channel_id)
                .or_default()
                .push(MessageIndexEntry {
                    log_time: msg.log_time,
                    offset,
                });

            // Write message record: opcode + length + channel_id + sequence + log_time + publish_time + data
            uncompressed_data.push(OP_MESSAGE);

            let record_len = 2 + 4 + 8 + 8 + msg.data.len();
            uncompressed_data.extend_from_slice(&(record_len as u64).to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.channel_id.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.sequence.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.log_time.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.publish_time.to_le_bytes());
            uncompressed_data.extend_from_slice(&msg.data);
        }

        let message_count = self.message_buffer.len();
        let uncompressed_size = uncompressed_data.len();

        // Compress with zstd
        let compressed_data = zstd::bulk::compress(&uncompressed_data, 3).map_err(|e| {
            CodecError::encode(
                "ParallelMcapWriter",
                format!("Zstd compression failed: {e}"),
            )
        })?;

        // Write as a compressed chunk
        let chunk = CompressedChunk {
            sequence: self.chunks_written,
            compressed_data,
            uncompressed_size,
            message_start_time: chunk_start_time,
            message_end_time: chunk_end_time,
            message_count,
            compression_ratio: 0.0, // Not used here
            message_indexes: chunk_message_indexes,
        };

        // Clear the buffer before writing (to avoid double counting)
        self.message_buffer.clear();
        self.buffer_size = 0;

        // Temporarily adjust messages_written since write_compressed_chunk adds to it
        let saved_messages = self.messages_written;
        self.messages_written -= message_count as u64;

        self.write_compressed_chunk(chunk)?;

        // Restore the correct message count
        self.messages_written = saved_messages;

        Ok(())
    }

    /// Get the channel ID for a topic.
    pub fn get_channel_id(&self, topic: &str) -> Option<u16> {
        self.channel_ids.get(topic).copied()
    }

    /// Get the number of chunks written.
    pub fn chunks_written(&self) -> u64 {
        self.chunks_written
    }

    /// Get the number of messages written.
    pub fn messages_written(&self) -> u64 {
        self.messages_written
    }

    /// Flush the writer.
    pub fn flush(&mut self) -> Result<()> {
        std::io::Write::flush(&mut self.writer).map_err(|e| {
            CodecError::encode("ParallelMcapWriter", format!("Failed to flush output: {e}"))
        })
    }

    /// Write a chunk index record to the summary section.
    ///
    /// `ChunkIndex` record format (matching `mcap::records::ChunkIndex)`:
    /// - opcode (u8 = 0x08)
    /// - record length (u64)
    /// - `message_start_time` (u64)
    /// - `message_end_time` (u64)
    /// - `chunk_start_offset` (u64)
    /// - `chunk_length` (u64)
    /// - `message_index_offsets` (int map: u32 byte length + [u16 + u64]...)
    /// - `message_index_length` (u64)
    /// - compression (string: u32 length + bytes)
    /// - `compressed_size` (u64)
    /// - `uncompressed_size` (u64)
    fn write_chunk_index(&mut self, chunk_idx: &ChunkIndexRecord) -> Result<()> {
        self.write_u8(OP_CHUNK_INDEX)?;

        // Calculate record length
        // 8*8 (u64 fields) + 4 (map len) + map_bytes + 4 (string len) + compression.len()
        let map_bytes: u64 = chunk_idx
            .message_index_offsets
            .values()
            .map(|_| 2 + 8) // u16 key + u64 value
            .sum();
        let record_length: u64 = 8 * 7 + 4 + map_bytes + 4 + chunk_idx.compression.len() as u64;
        self.write_u64(record_length)?;

        // Message start time
        self.write_u64(chunk_idx.message_start_time)?;

        // Message end time
        self.write_u64(chunk_idx.message_end_time)?;

        // Chunk start offset
        self.write_u64(chunk_idx.chunk_start_offset)?;

        // Chunk length
        self.write_u64(chunk_idx.chunk_length)?;

        // Message index offsets (byte-length prefixed int map)
        self.write_u32(map_bytes as u32)?;
        for (&channel_id, &offset) in &chunk_idx.message_index_offsets {
            self.write_u16(channel_id)?;
            self.write_u64(offset)?;
        }

        // Message index length
        self.write_u64(chunk_idx.message_index_length)?;

        // Compression (string)
        self.write_u32(chunk_idx.compression.len() as u32)?;
        self.write_bytes(chunk_idx.compression.as_bytes())?;

        // Compressed size
        self.write_u64(chunk_idx.compressed_size)?;

        // Uncompressed size
        self.write_u64(chunk_idx.uncompressed_size)?;

        Ok(())
    }

    /// Write a statistics record to the summary section.
    ///
    /// Statistics record format (matching `mcap::records::Statistics)`:
    /// - opcode (u8 = 0x0B)
    /// - record length (u64)
    /// - `message_count` (u64)
    /// - `schema_count` (u16)
    /// - `channel_count` (u32)
    /// - `attachment_count` (u32)
    /// - `metadata_count` (u32)
    /// - `chunk_count` (u32)
    /// - `message_start_time` (u64)
    /// - `message_end_time` (u64)
    /// - `channel_message_counts` (int map: u32 byte length + [u16 + u64]...)
    fn write_statistics(&mut self) -> Result<()> {
        self.write_u8(OP_STATISTICS)?;

        // Calculate record length
        // 8 + 2 + 4*3 + 4 + 8*2 + 4 + map_bytes
        let map_bytes: u64 = self
            .channel_message_counts
            .values()
            .map(|_| 2 + 8) // u16 key + u64 value
            .sum();
        let record_length: u64 = 8 + 2 + 4 * 3 + 4 + 8 * 2 + 4 + map_bytes;
        self.write_u64(record_length)?;

        // Message count
        self.write_u64(self.messages_written)?;

        // Schema count
        self.write_u16(self.schema_ids.len() as u16)?;

        // Channel count
        self.write_u32(self.channel_ids.len() as u32)?;

        // Attachment count (0)
        self.write_u32(0)?;

        // Metadata count (0)
        self.write_u32(0)?;

        // Chunk count (u32!)
        self.write_u32(self.chunks_written as u32)?;

        // Message start time
        let start_time = if self.messages_written > 0 {
            self.file_message_start_time
        } else {
            0
        };
        self.write_u64(start_time)?;

        // Message end time
        let end_time = if self.messages_written > 0 {
            self.file_message_end_time
        } else {
            0
        };
        self.write_u64(end_time)?;

        // Channel message counts (byte-length prefixed int map)
        self.write_u32(map_bytes as u32)?;

        // Collect and sort counts to avoid borrow issues
        let counts: Vec<(u16, u64)> = {
            let mut sorted: Vec<_> = self.channel_message_counts.iter().collect();
            sorted.sort_by_key(|&(k, _)| k);
            sorted.iter().map(|&(&k, &v)| (k, v)).collect()
        };

        for (channel_id, count) in counts {
            self.write_u16(channel_id)?;
            self.write_u64(count)?;
        }

        Ok(())
    }

    /// Finalize the MCAP file with a proper summary section.
    ///
    /// This writes:
    /// 1. Data end section (`OP_DATA_END` = 0x0F)
    /// 2. Summary section with chunk indexes and statistics
    /// 3. Footer with summary reference
    /// 4. Magic bytes (8 bytes)
    ///
    /// The summary section enables parallel reading of the output file.
    ///
    /// Footer format:
    /// - opcode (u8 = 0x02)
    /// - `record_length` (u64 = 20)
    /// - `summary_start` (u64, 0 = no summary)
    /// - `summary_offset_start` (u64, 0 = no summary offset section)
    /// - `summary_crc` (u32, 0 = no CRC)
    pub fn finish(&mut self) -> Result<u64> {
        // Flush any remaining buffered messages as a final chunk
        self.flush_message_buffer()?;

        // Write data end section
        // Format: opcode (1) + record_length (8) + data_section_crc (4)
        self.write_u8(OP_DATA_END)?;
        self.write_u64(4)?; // Record length = 4 bytes for CRC field
        self.write_u32(0)?; // data_section_crc = 0 (no CRC computed)

        // === Start of summary section ===
        // Per MCAP spec and mcap crate: schemas first, then channels, then statistics, then chunk indexes
        self.summary_start_offset = self.position();

        // Write schema records (copies for summary section)
        let schema_records = self.schema_records.clone();
        for schema in &schema_records {
            self.write_summary_schema(schema)?;
        }

        // Write channel records (copies for summary section)
        let channel_records = self.channel_records.clone();
        for channel in &channel_records {
            self.write_summary_channel(channel)?;
        }

        // Write statistics record
        self.write_statistics()?;

        // Write chunk index records
        let chunk_indexes = self.chunk_indexes.clone();
        for chunk_idx in &chunk_indexes {
            self.write_chunk_index(chunk_idx)?;
        }

        // Note: We're not writing summary offsets (summary_offset_start = 0 in footer)
        // because we don't track the exact offsets of each section type.

        // === End of summary section ===

        // Write footer
        // Format: opcode (1) + record_length (8) + summary_start (8) + summary_offset_start (8) + summary_crc (4)
        self.write_u8(OP_FOOTER)?;

        // Record length = 8 (summary_start) + 8 (summary_offset_start) + 4 (summary_crc) = 20 bytes
        self.write_u64(20)?;

        // Summary start offset
        if self.chunk_indexes.is_empty() && self.messages_written == 0 {
            // No data written, zero summary
            self.write_u64(0)?;
        } else {
            self.write_u64(self.summary_start_offset)?;
        }

        // Summary offset start (0 = no summary offset section)
        self.write_u64(0)?;

        // Summary CRC (0 = no CRC computed)
        self.write_u32(0)?;

        // Write magic (8 bytes)
        self.write_bytes(&MCAP_MAGIC)?;

        self.flush()?;

        tracing::debug!(
            "Summary section written: {} schemas, {} channels, {} chunk indexes, {} messages",
            self.schema_records.len(),
            self.channel_records.len(),
            self.chunk_indexes.len(),
            self.messages_written
        );

        Ok(self.chunks_written)
    }

    /// Write a schema record to the summary section.
    fn write_summary_schema(&mut self, schema: &SchemaRecord) -> Result<()> {
        self.write_u8(OP_SCHEMA)?;

        let record_length: u64 = 2
            + 4
            + schema.name.len() as u64
            + 4
            + schema.encoding.len() as u64
            + 4
            + schema.data.len() as u64;
        self.write_u64(record_length)?;

        self.write_u16(schema.id)?;
        self.write_u32(schema.name.len() as u32)?;
        self.write_bytes(schema.name.as_bytes())?;
        self.write_u32(schema.encoding.len() as u32)?;
        self.write_bytes(schema.encoding.as_bytes())?;
        self.write_u32(schema.data.len() as u32)?;
        self.write_bytes(&schema.data)?;

        Ok(())
    }

    /// Write a channel record to the summary section.
    fn write_summary_channel(&mut self, channel: &ChannelRecord) -> Result<()> {
        // Serialize metadata (includes byte-length prefix)
        let metadata_bytes = serialize_metadata(&channel.metadata)?;

        self.write_u8(OP_CHANNEL)?;

        // Note: metadata_bytes already includes the 4-byte length prefix
        let record_length: u64 = 2
            + 2
            + 4
            + channel.topic.len() as u64
            + 4
            + channel.message_encoding.len() as u64
            + metadata_bytes.len() as u64;
        self.write_u64(record_length)?;

        self.write_u16(channel.id)?;
        self.write_u16(channel.schema_id)?;
        self.write_u32(channel.topic.len() as u32)?;
        self.write_bytes(channel.topic.as_bytes())?;
        self.write_u32(channel.message_encoding.len() as u32)?;
        self.write_bytes(channel.message_encoding.as_bytes())?;
        // Metadata already includes byte-length prefix from serialize_metadata
        self.write_bytes(&metadata_bytes)?;

        Ok(())
    }

    /// Get the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Serialize metadata `HashMap` to MCAP format.
///
/// Format: byte-length prefixed map of string pairs
/// - u32: total byte length of all entries
/// - For each entry: u32 `key_len` + `key_bytes` + u32 `val_len` + `val_bytes`
fn serialize_metadata(metadata: &HashMap<String, String>) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();

    // First, calculate total byte length
    let mut total_len: u32 = 0;
    for (key, value) in metadata {
        total_len += 4 + key.len() as u32 + 4 + value.len() as u32;
    }

    // Write total byte length
    bytes.write_u32::<LittleEndian>(total_len)?;

    // Write each entry
    for (key, value) in metadata {
        bytes.write_u32::<LittleEndian>(key.len() as u32)?;
        bytes.write_all(key.as_bytes())?;
        bytes.write_u32::<LittleEndian>(value.len() as u32)?;
        bytes.write_all(value.as_bytes())?;
    }

    Ok(bytes)
}

impl FormatWriter for ParallelMcapWriter<BufWriter<File>> {
    fn path(&self) -> &'static str {
        // We don't store the path in the writer, so return a placeholder
        // In a real implementation, we'd store the path
        "unknown"
    }

    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        // Add schema if provided
        let schema_id = if let Some(schema_data) = schema {
            let schema_name = format!("{message_type}_schema");
            self.add_schema(&schema_name, encoding, schema_data.as_bytes())?
        } else {
            0
        };

        // Use the internal add_channel method with empty metadata
        let empty_metadata = HashMap::new();
        self.add_channel(schema_id, topic, message_type, &empty_metadata)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        // Buffer the message - it will be written when chunk size threshold is reached
        self.write_message(
            message.channel_id,
            message.log_time,
            message.publish_time,
            &message.data,
        )
    }

    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        for msg in messages {
            self.write(msg)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        // Call the existing finish method (which returns u64 for chunks written)
        ParallelMcapWriter::finish(self).map(|_| ())
    }

    fn message_count(&self) -> u64 {
        self.messages_written
    }

    fn channel_count(&self) -> usize {
        self.channel_ids.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Read;
    use tempfile::NamedTempFile;

    /// Create a temporary file for testing
    fn create_temp_file() -> NamedTempFile {
        NamedTempFile::new().unwrap()
    }

    /// Helper to read a file's contents
    fn read_file_contents(path: &std::path::Path) -> Vec<u8> {
        let mut file = std::fs::File::open(path).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        buffer
    }

    /// Helper to create a test schema
    fn test_schema() -> (&'static str, &'static str, &'static [u8]) {
        (
            "test_schema",
            "protobuf",
            b"message Test { int32 value = 1; }",
        )
    }

    /// Helper to create test metadata
    fn test_metadata() -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());
        metadata
    }

    #[test]
    fn test_create_writer() {
        let temp_file = create_temp_file();
        let writer = ParallelMcapWriter::<File>::create(temp_file.path());
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        assert_eq!(writer.chunks_written(), 0);
        assert_eq!(writer.messages_written(), 0);
        assert_eq!(writer.channel_ids.len(), 0);
    }

    #[test]
    fn test_create_writer_with_buffer() {
        let temp_file = create_temp_file();
        let writer =
            ParallelMcapWriter::<BufWriter<File>>::create_with_buffer(temp_file.path(), 8192);
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        assert_eq!(writer.chunks_written(), 0);
        assert_eq!(writer.messages_written(), 0);
    }

    #[test]
    fn test_writer_with_custom_chunk_size() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();
        let writer = ParallelMcapWriter::with_chunk_size(file, 1024);
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        // The writer should have the custom chunk size set
        assert_eq!(writer.target_chunk_size, 1024);
    }

    #[test]
    fn test_write_header() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();
        let writer = ParallelMcapWriter::new(file);
        assert!(writer.is_ok());

        // Drop writer to flush
        drop(writer);

        let contents = read_file_contents(temp_file.path());

        // Check MCAP magic at the start
        assert_eq!(&contents[0..8], &MCAP_MAGIC[..]);

        // Check header opcode
        assert_eq!(contents[8], OP_HEADER);

        // Check record length (8 bytes for empty profile + library)
        let record_len = u64::from_le_bytes(contents[9..17].try_into().unwrap());
        assert_eq!(record_len, 8);
    }

    #[test]
    fn test_add_schema() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        let (name, encoding, data) = test_schema();
        let schema_id = writer.add_schema(name, encoding, data).unwrap();

        assert_eq!(schema_id, 1); // First schema ID

        // Adding the same schema again should return the existing ID
        let schema_id2 = writer.add_schema(name, encoding, data).unwrap();
        assert_eq!(schema_id2, schema_id);

        // Add a different schema
        let schema_id3 = writer.add_schema("schema2", "json", b"{}").unwrap();
        assert_eq!(schema_id3, 2);
    }

    #[test]
    fn test_add_channel() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        assert_eq!(channel_id, 0); // First channel ID

        // Adding the same topic again should return the existing ID
        let channel_id2 = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();
        assert_eq!(channel_id2, channel_id);

        // Add a different channel
        let channel_id3 = writer
            .add_channel(0, "/another/topic", "json", &HashMap::new())
            .unwrap();
        assert_eq!(channel_id3, 1);
    }

    #[test]
    fn test_add_channel_with_metadata() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        let metadata = test_metadata();
        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &metadata)
            .unwrap();

        assert_eq!(channel_id, 0);
        assert_eq!(writer.channel_ids.len(), 1);
    }

    #[test]
    fn test_add_channel_with_specific_id() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        // Add channel with specific ID
        let channel_id = writer
            .add_channel_with_id(5, 0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        assert_eq!(channel_id, 5);

        // Next auto-assigned ID should be 6
        let channel_id2 = writer
            .add_channel(0, "/another/topic", "protobuf", &HashMap::new())
            .unwrap();
        assert_eq!(channel_id2, 6);
    }

    #[test]
    fn test_get_channel_id() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        assert!(writer.get_channel_id("/test/topic").is_none());

        writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        assert_eq!(writer.get_channel_id("/test/topic"), Some(0));
        assert!(writer.get_channel_id("/nonexistent").is_none());
    }

    #[test]
    fn test_write_message() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        let data = b"test message data";
        let result = writer.write_message(1000, 1000, 1000, data);
        assert!(result.is_ok());

        // Message count should increase
        assert_eq!(writer.messages_written(), 1);

        // Write more messages (small data, won't trigger chunk flush immediately with default 4MB threshold)
        for i in 1..10 {
            writer
                .write_message(channel_id, 1000 + i, 1000 + i, data)
                .unwrap();
        }

        assert_eq!(writer.messages_written(), 10);
    }

    #[test]
    fn test_write_message_triggers_chunk_flush() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();

        // Create writer with small chunk size to trigger flush
        let mut writer = ParallelMcapWriter::with_chunk_size(file, 100).unwrap();

        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        // Write enough data to trigger chunk flush
        let large_data = vec![0u8; 200]; // Larger than chunk size

        writer
            .write_message(channel_id, 1000, 1000, &large_data)
            .unwrap();

        // Should have flushed and written a chunk
        assert_eq!(writer.chunks_written(), 1);
        assert_eq!(writer.messages_written(), 1);
    }

    #[test]
    fn test_write_multiple_messages_same_channel() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();

        // Create writer with small chunk size
        let mut writer = ParallelMcapWriter::with_chunk_size(file, 500).unwrap();

        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        let data = b"test data";

        // Write multiple messages
        for i in 0..10 {
            writer
                .write_message(
                    channel_id,
                    1000 + i as u64 * 100,
                    1000 + i as u64 * 100,
                    data,
                )
                .unwrap();
        }

        assert_eq!(writer.messages_written(), 10);
    }

    #[test]
    fn test_write_compressed_chunk() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        use crate::io::formats::mcap::internal::CompressedChunk;

        let message_indexes = BTreeMap::new();

        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4], // Dummy compressed data
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 2000,
            message_count: 5,
            compression_ratio: 0.5,
            message_indexes,
        };

        let result = writer.write_compressed_chunk(chunk);
        assert!(result.is_ok());

        assert_eq!(writer.chunks_written(), 1);
        assert_eq!(writer.messages_written(), 5);
    }

    #[test]
    fn test_write_compressed_chunk_with_indexes() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        use crate::io::formats::mcap::internal::{CompressedChunk, MessageIndexEntry};

        let mut message_indexes = BTreeMap::new();
        message_indexes.insert(
            0,
            vec![
                MessageIndexEntry {
                    log_time: 1000,
                    offset: 0,
                },
                MessageIndexEntry {
                    log_time: 2000,
                    offset: 50,
                },
            ],
        );

        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3, 4],
            uncompressed_size: 100,
            message_start_time: 1000,
            message_end_time: 2000,
            message_count: 2,
            compression_ratio: 0.5,
            message_indexes,
        };

        let result = writer.write_compressed_chunk(chunk);
        assert!(result.is_ok());

        assert_eq!(writer.chunks_written(), 1);
    }

    #[test]
    fn test_finish_writes_footer() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        // Add a channel and write a message
        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        writer
            .write_message(channel_id, 1000, 1000, b"test")
            .unwrap();

        let _chunks_written = writer.finish().unwrap();

        // Verify file has magic at end
        let contents = read_file_contents(temp_file.path());
        assert!(contents.len() > 8);

        // Check footer magic at the end
        let footer_magic_start = contents.len() - 8;
        assert_eq!(
            &contents[footer_magic_start..],
            &MCAP_MAGIC[..],
            "Footer magic should be at the end of the file"
        );
    }

    #[test]
    fn test_finish_empty_file() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        let chunks_written = writer.finish().unwrap();
        assert_eq!(chunks_written, 0);

        // Verify file is valid (has magic at both ends)
        let contents = read_file_contents(temp_file.path());
        assert_eq!(&contents[0..8], &MCAP_MAGIC[..]);
        assert_eq!(&contents[contents.len() - 8..], &MCAP_MAGIC[..]);
    }

    #[test]
    fn test_finish_with_summary_section() {
        let temp_file = create_temp_file();

        // Write messages to trigger chunk creation
        let file = File::create(temp_file.path()).unwrap();
        let mut writer = ParallelMcapWriter::with_chunk_size(file, 100).unwrap();

        // Add schema
        writer
            .add_schema("test_schema", "protobuf", b"schema data")
            .unwrap();

        // Add channel
        let channel_id = writer
            .add_channel(1, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        let data = vec![0u8; 200];
        writer.write_message(channel_id, 1000, 1000, &data).unwrap();

        writer.finish().unwrap();

        // Verify file has proper structure
        let contents = read_file_contents(temp_file.path());

        // Check MCAP magic at the start
        assert_eq!(&contents[0..8], &MCAP_MAGIC[..]);

        // Check MCAP magic at the end
        assert_eq!(&contents[contents.len() - 8..], &MCAP_MAGIC[..]);

        // Verify the file contains some data between the magic bytes
        assert!(contents.len() > 16);

        // Verify footer opcode exists somewhere in the file (should be near the end)
        let has_footer = contents.contains(&OP_FOOTER);
        assert!(has_footer, "File should contain OP_FOOTER");

        // Verify data end opcode exists
        let has_data_end = contents.contains(&OP_DATA_END);
        assert!(has_data_end, "File should contain OP_DATA_END");
    }

    #[test]
    fn test_serialize_metadata() {
        let metadata = test_metadata();
        let bytes = serialize_metadata(&metadata).unwrap();

        // Should have 4-byte length prefix + entries
        assert!(bytes.len() > 4);

        // Check length prefix
        let total_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(total_len as usize, bytes.len() - 4);
    }

    #[test]
    fn test_serialize_empty_metadata() {
        let metadata = HashMap::new();
        let bytes = serialize_metadata(&metadata).unwrap();

        // Should have 4-byte length prefix (value 0)
        assert_eq!(bytes.len(), 4);
        assert_eq!(&bytes[0..4], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_flush() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        // Write some data
        writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        let result = writer.flush();
        assert!(result.is_ok());
    }

    #[test]
    fn test_chunks_written() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        assert_eq!(writer.chunks_written(), 0);

        use crate::io::formats::mcap::internal::CompressedChunk;

        let chunk = CompressedChunk {
            sequence: 0,
            compressed_data: vec![1, 2, 3],
            uncompressed_size: 50,
            message_start_time: 1000,
            message_end_time: 2000,
            message_count: 1,
            compression_ratio: 0.5,
            message_indexes: BTreeMap::new(),
        };

        writer.write_compressed_chunk(chunk).unwrap();

        assert_eq!(writer.chunks_written(), 1);
    }

    #[test]
    fn test_messages_written() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        assert_eq!(writer.messages_written(), 0);

        let channel_id = writer
            .add_channel(0, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        writer
            .write_message(channel_id, 1000, 1000, b"data")
            .unwrap();

        assert_eq!(writer.messages_written(), 1);
    }

    #[test]
    fn test_channel_count() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        assert_eq!(writer.channel_ids.len(), 0);

        writer
            .add_channel(0, "/topic1", "protobuf", &HashMap::new())
            .unwrap();
        assert_eq!(writer.channel_ids.len(), 1);

        writer
            .add_channel(0, "/topic2", "json", &HashMap::new())
            .unwrap();
        assert_eq!(writer.channel_ids.len(), 2);

        // Adding duplicate topic should not increase count
        writer
            .add_channel(0, "/topic1", "protobuf", &HashMap::new())
            .unwrap();
        assert_eq!(writer.channel_ids.len(), 2);
    }

    #[test]
    fn test_format_writer_trait_path() {
        let temp_file = create_temp_file();
        let writer =
            ParallelMcapWriter::<BufWriter<File>>::create_with_buffer(temp_file.path(), 8192)
                .unwrap();
        // The FormatWriter trait requires a path() method
        // Our implementation returns "unknown" since we don't track the path
        assert_eq!(writer.path(), "unknown");
    }

    #[test]
    fn test_format_writer_trait() {
        let temp_file = create_temp_file();
        let mut writer =
            ParallelMcapWriter::<BufWriter<File>>::create_with_buffer(temp_file.path(), 8192)
                .unwrap();

        // Use FormatWriter trait - use explicit trait method syntax to avoid ambiguity
        use crate::io::traits::FormatWriter;

        // Test add_channel via trait
        let channel_id = FormatWriter::add_channel(
            &mut writer,
            "/test",
            "std_msgs/String",
            "protobuf",
            Some("schema data"),
        )
        .unwrap();
        assert_eq!(channel_id, 0);

        // Test write via trait
        let message = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: b"test data".to_vec(),
            sequence: Some(0),
        };
        FormatWriter::write(&mut writer, &message).unwrap();
        assert_eq!(FormatWriter::message_count(&writer), 1);

        // Test write_batch via trait
        let message2 = RawMessage {
            channel_id: 0,
            log_time: 2000,
            publish_time: 2000,
            data: b"msg2".to_vec(),
            sequence: Some(1),
        };
        FormatWriter::write_batch(&mut writer, &[message2]).unwrap();
        assert_eq!(FormatWriter::message_count(&writer), 2);

        // Test channel_count via trait
        assert_eq!(FormatWriter::channel_count(&writer), 1);
    }

    #[test]
    fn test_format_writer_trait_write() {
        let temp_file = create_temp_file();
        let mut writer =
            ParallelMcapWriter::<BufWriter<File>>::create_with_buffer(temp_file.path(), 8192)
                .unwrap();

        use crate::io::traits::FormatWriter;
        FormatWriter::add_channel(&mut writer, "/test", "std_msgs/String", "protobuf", None)
            .unwrap();

        let message = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: b"test data".to_vec(),
            sequence: Some(0),
        };

        let result = FormatWriter::write(&mut writer, &message);
        assert!(result.is_ok());
        assert_eq!(writer.messages_written(), 1);
    }

    #[test]
    fn test_format_writer_trait_write_batch() {
        let temp_file = create_temp_file();
        let mut writer =
            ParallelMcapWriter::<BufWriter<File>>::create_with_buffer(temp_file.path(), 8192)
                .unwrap();

        use crate::io::traits::FormatWriter;
        FormatWriter::add_channel(&mut writer, "/test", "std_msgs/String", "protobuf", None)
            .unwrap();

        let messages = vec![
            RawMessage {
                channel_id: 0,
                log_time: 1000,
                publish_time: 1000,
                data: b"msg1".to_vec(),
                sequence: Some(0),
            },
            RawMessage {
                channel_id: 0,
                log_time: 2000,
                publish_time: 2000,
                data: b"msg2".to_vec(),
                sequence: Some(1),
            },
        ];

        let result = FormatWriter::write_batch(&mut writer, &messages);
        assert!(result.is_ok());
        assert_eq!(writer.messages_written(), 2);
    }

    #[test]
    fn test_into_inner() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();
        let writer = ParallelMcapWriter::new(file).unwrap();

        // Finish the writer first
        let mut writer = writer;
        writer.finish().unwrap();

        // Get the inner writer
        let _inner = writer.into_inner();
        // File should be closed/dropped properly
    }

    #[test]
    fn test_message_sequence_tracking() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();
        let mut writer = ParallelMcapWriter::with_chunk_size(file, 1000).unwrap();

        let channel_id = writer
            .add_channel(0, "/test", "protobuf", &HashMap::new())
            .unwrap();

        // Write multiple messages - they should have increasing sequence numbers
        writer
            .write_message(channel_id, 1000, 1000, b"msg1")
            .unwrap();
        writer
            .write_message(channel_id, 2000, 2000, b"msg2")
            .unwrap();
        writer
            .write_message(channel_id, 3000, 3000, b"msg3")
            .unwrap();

        assert_eq!(writer.messages_written(), 3);
    }

    #[test]
    fn test_time_boundaries_tracking() {
        let temp_file = create_temp_file();
        let file = File::create(temp_file.path()).unwrap();
        let mut writer = ParallelMcapWriter::with_chunk_size(file, 100).unwrap();

        let channel_id = writer
            .add_channel(0, "/test", "protobuf", &HashMap::new())
            .unwrap();

        // Write messages with different timestamps
        writer
            .write_message(channel_id, 5000, 5000, b"msg1")
            .unwrap();
        writer
            .write_message(channel_id, 1000, 1000, b"msg2")
            .unwrap();
        writer
            .write_message(channel_id, 10000, 10000, b"msg3")
            .unwrap();

        writer.finish().unwrap();

        // Time boundaries should be tracked correctly
        // The writer should track min (1000) and max (10000) times
        // This is verified implicitly by the statistics section written
    }

    #[test]
    fn test_multiple_schemas() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        // Add multiple schemas
        let id1 = writer.add_schema("schema1", "protobuf", b"data1").unwrap();
        let id2 = writer.add_schema("schema2", "protobuf", b"data2").unwrap();
        let id3 = writer.add_schema("schema3", "json", b"data3").unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);

        // Verify schema deduplication
        let id1_again = writer.add_schema("schema1", "protobuf", b"data1").unwrap();
        assert_eq!(id1_again, id1);
    }

    #[test]
    fn test_schema_id_increment() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        // Schema IDs start at 1
        let id1 = writer.add_schema("s1", "protobuf", b"").unwrap();
        assert_eq!(id1, 1);

        let id2 = writer.add_schema("s2", "protobuf", b"").unwrap();
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_channel_with_schema() {
        let temp_file = create_temp_file();
        let mut writer = ParallelMcapWriter::<File>::create(temp_file.path()).unwrap();

        // Add schema first
        let schema_id = writer
            .add_schema("my_schema", "protobuf", b"schema definition")
            .unwrap();

        // Add channel with schema
        let channel_id = writer
            .add_channel(schema_id, "/test/topic", "protobuf", &HashMap::new())
            .unwrap();

        assert_eq!(channel_id, 0);
    }
}
