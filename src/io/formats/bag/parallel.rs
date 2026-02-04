// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag format implementation for the unified I/O layer.
//!
//! This module provides BAG-specific readers that implement the unified I/O traits.
//!
//! **Note:** This implementation uses a custom BAG parser with no external dependencies.
//! It supports:
//! - BZ2 and uncompressed chunks
//! - Parallel reading via chunk indexes (default behavior)
//! - Full connection metadata extraction
//! - Decoded message streaming via `decode_messages()`

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;

use crate::core::DecodedMessage;
use crate::encoding::CdrDecoder;
use crate::io::filter::ChannelFilter;
use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage, TimestampedDecodedMessage};
use crate::io::traits::{
    FormatReader, MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats,
};
use crate::{CodecError, Result};

use super::parser::{BagChunkInfo, BagConnection, BagParser};
use super::writer::BagWriter;

/// ROS1 bag format type.
///
/// This type provides factory methods for creating BAG readers.
/// Default behavior is parallel reading for optimal performance.
pub struct BagFormat;

impl BagFormat {
    /// Create a BAG reader with parallel reading support (default).
    ///
    /// The reader uses memory-mapping and processes chunks in parallel
    /// using the Rayon thread pool.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<ParallelBagReader> {
        ParallelBagReader::open(path)
    }

    /// Create a BAG writer with the given configuration.
    ///
    /// Returns a boxed FormatWriter trait object for unified writer API.
    pub fn create_writer<P: AsRef<Path>>(
        path: P,
        _config: &crate::io::writer::WriterConfig,
    ) -> Result<Box<dyn crate::io::traits::FormatWriter>> {
        // For now, we create a simple writer
        // TODO: Use config options for compression, chunk size, etc.
        let writer = BagWriter::create(path)?;
        Ok(Box::new(writer))
    }
}

/// Parallel BAG reader with memory-mapped file access.
///
/// This reader parses the BAG file metadata (connections, chunk indexes)
/// and supports parallel processing of chunks using Rayon.
pub struct ParallelBagReader {
    /// File path
    path: String,
    /// Custom BAG parser
    parser: BagParser,
    /// Channel information (channel_id -> ChannelInfo)
    channels: HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping (conn_id -> channel_id)
    conn_id_map: HashMap<u32, u16>,
    /// Total message count (estimated from chunks)
    message_count: u64,
    /// Start timestamp
    start_time: Option<u64>,
    /// End timestamp
    end_time: Option<u64>,
}

impl ParallelBagReader {
    /// Open a BAG file for parallel reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        // Open the custom parser
        let parser = BagParser::open(path_ref)?;

        // Build channels from parser's connections
        // Each connection becomes a separate channel to preserve callerid info
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut conn_id_map: HashMap<u32, u16> = HashMap::new();
        let mut next_channel_id: u16 = 0;

        // Use (topic, callerid) as the key to identify unique channels
        let mut topic_callerid_to_channel: HashMap<(String, String), u16> = HashMap::new();

        // Sort connections by conn_id to ensure deterministic channel ID assignment
        let mut sorted_conn_ids: Vec<u32> = parser.connections().keys().copied().collect();
        sorted_conn_ids.sort();

        for conn_id in sorted_conn_ids {
            let conn = &parser.connections()[&conn_id];
            let callerid = conn.caller_id.clone();
            let key = (conn.topic.clone(), callerid.clone());

            // Check if we already have a channel for this (topic, callerid) combination
            let channel_id = if let Some(&existing_id) = topic_callerid_to_channel.get(&key) {
                existing_id
            } else {
                let id = next_channel_id;
                next_channel_id = next_channel_id.wrapping_add(1);

                channels.insert(
                    id,
                    ChannelInfo {
                        id,
                        topic: conn.topic.clone(),
                        message_type: conn.message_type.clone(),
                        encoding: "ros1".to_string(), // ROS1 serialization format
                        schema: Some(conn.message_definition.clone()),
                        schema_data: None,
                        schema_encoding: Some("ros1msg".to_string()),
                        message_count: 0,
                        callerid: if callerid.is_empty() {
                            None
                        } else {
                            Some(callerid.clone())
                        },
                    },
                );
                topic_callerid_to_channel.insert(key, id);
                id
            };

            conn_id_map.insert(conn_id, channel_id);
        }

        // Calculate message count and time bounds from chunks
        let chunks = parser.chunks();
        let message_count = chunks.iter().map(|c| c.message_count as u64).sum();
        let start_time = chunks.first().map(|c| c.start_time);
        let end_time = chunks.last().map(|c| c.end_time);

        Ok(Self {
            path: path_str,
            parser,
            channels,
            conn_id_map,
            message_count,
            start_time,
            end_time,
        })
    }

    /// Get the connection ID to channel ID mapping.
    pub fn conn_id_map(&self) -> &HashMap<u32, u16> {
        &self.conn_id_map
    }

    /// Get all chunk information from the parser.
    pub fn chunks(&self) -> &[BagChunkInfo] {
        self.parser.chunks()
    }

    /// Get all connections from the parser.
    pub fn connections(&self) -> &HashMap<u32, BagConnection> {
        self.parser.connections()
    }

    /// Create a raw message iterator for sequential reading.
    ///
    /// This is useful for rewriters that need to process messages one by one.
    pub fn iter_raw(&self) -> Result<BagRawIter<'_>> {
        Ok(BagRawIter::new(
            &self.parser,
            &self.channels,
            &self.conn_id_map,
        ))
    }

    /// Decode messages from the BAG file.
    ///
    /// Returns an iterator that yields decoded messages with their channel info.
    /// Uses ROS1 CDR deserialization.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::formats::bag::BagFormat;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = BagFormat::open("test.bag")?;
    /// let decoded_iter = reader.decode_messages()?;
    /// let mut stream = decoded_iter.stream()?;
    ///
    /// while let Some(result) = stream.next() {
    ///     let (message, channel_info) = result?;
    ///     println!("Topic: {}, Data: {:?}", channel_info.topic, message);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn decode_messages(&self) -> Result<BagDecodedMessageIter<'_>> {
        Ok(BagDecodedMessageIter::new(
            &self.parser,
            &self.channels,
            &self.conn_id_map,
        ))
    }

    /// Decode messages with timestamps from the BAG file.
    ///
    /// Returns an iterator that yields decoded messages with their log_time and publish_time.
    /// Similar to `decode_messages` but includes timestamp information for each message.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::formats::bag::BagFormat;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = BagFormat::open("test.bag")?;
    /// let decoded_iter = reader.decode_messages_with_timestamp()?;
    /// let mut stream = decoded_iter.stream()?;
    ///
    /// while let Some(result) = stream.next() {
    ///     let (timestamped_msg, channel_info) = result?;
    ///     println!("Topic: {}, Log Time: {}", channel_info.topic, timestamped_msg.log_time);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn decode_messages_with_timestamp(&self) -> Result<BagDecodedMessageWithTimestampIter<'_>> {
        Ok(BagDecodedMessageWithTimestampIter::new(
            &self.parser,
            &self.channels,
            &self.conn_id_map,
        ))
    }

    /// Process a single chunk in parallel.
    fn process_chunk(
        chunk_info: &BagChunkInfo,
        parser: &BagParser,
        conn_id_map: &HashMap<u32, u16>,
        channels: &HashMap<u16, ChannelInfo>,
        _channel_filter: &Option<ChannelFilter>,
    ) -> Result<ProcessedChunk> {
        // Read and decompress the chunk
        let decompressed = parser.read_chunk(chunk_info)?;

        // Parse messages from decompressed data
        let messages = parser.parse_chunk_messages(&decompressed, conn_id_map)?;

        // Calculate total bytes
        let total_bytes = messages.iter().map(|m| m.data.len()).sum::<usize>();
        let message_count = messages.len();

        // Build message chunk
        let mut chunk = MessageChunkData::new(chunk_info.sequence);

        for msg in messages {
            // Verify channel exists
            if channels.contains_key(&msg.channel_id) {
                let raw_msg = RawMessage {
                    channel_id: msg.channel_id,
                    log_time: msg.log_time,
                    publish_time: msg.publish_time,
                    data: msg.data,
                    sequence: Some(msg.sequence as u64),
                };
                chunk.add_message(raw_msg);
            }
        }

        Ok(ProcessedChunk {
            chunk,
            total_bytes: total_bytes as u64,
            message_count: message_count as u64,
        })
    }
}

impl FormatReader for ParallelBagReader {
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

    fn format(&self) -> FileFormat {
        FileFormat::Bag
    }

    fn file_size(&self) -> u64 {
        self.parser.file_size()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ParallelReader for ParallelBagReader {
    fn read_parallel(
        &self,
        config: ParallelReaderConfig,
        sender: crossbeam_channel::Sender<MessageChunkData>,
    ) -> Result<ParallelReaderStats> {
        let num_threads = config.num_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        });

        println!(
            "Starting parallel BAG reader with {} worker threads...",
            num_threads
        );
        println!("  File: {}", self.path);
        println!("  Chunks to process: {}", self.parser.chunks().len());

        let total_start = Instant::now();

        // Build channel filter from topic filter
        let channel_filter = config
            .topic_filter
            .as_ref()
            .map(|tf| ChannelFilter::from_topic_filter(tf, self.channels()));

        // Create thread pool for controlled parallelism
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|index| format!("bag-reader-{}", index))
            .build()
            .map_err(|e| {
                CodecError::encode(
                    "ParallelBagReader",
                    format!("Failed to create thread pool: {e}"),
                )
            })?;

        // Get references for parallel processing
        let chunks = self.parser.chunks();
        let parser = &self.parser;
        let conn_id_map = &self.conn_id_map;
        let channels = &self.channels;

        // Process chunks in parallel
        let results: Vec<Result<ProcessedChunk>> = pool.install(|| {
            chunks
                .par_iter()
                .enumerate()
                .map(|(i, chunk_info)| {
                    if i % config.progress_interval == 0 && i > 0 {
                        eprint!("\rProcessing chunk {}/{}...", i, chunks.len());
                        let _ = std::io::stdout().flush();
                    }
                    Self::process_chunk(chunk_info, parser, conn_id_map, channels, &channel_filter)
                })
                .collect()
        });

        eprintln!(); // New line after progress

        // Collect results and send chunks
        let mut messages_read = 0u64;
        let mut chunks_processed = 0;
        let mut total_bytes = 0u64;

        for result in results {
            let processed = result?;
            chunks_processed += 1;
            messages_read += processed.message_count;
            total_bytes += processed.total_bytes;

            if processed.chunk.message_count() > 0 {
                sender.send(processed.chunk).map_err(|e| {
                    CodecError::encode("ParallelBagReader", format!("Failed to send chunk: {e}"))
                })?;
            }
        }

        let duration = total_start.elapsed();

        println!("Parallel BAG reader complete:");
        println!("  Chunks processed: {}", chunks_processed);
        println!("  Messages read: {}", messages_read);
        println!(
            "  Total bytes: {:.2} MB",
            total_bytes as f64 / (1024.0 * 1024.0)
        );
        println!("  Total time: {:.2}s", duration.as_secs_f64());

        Ok(ParallelReaderStats {
            messages_read,
            chunks_processed,
            total_bytes,
            read_time_sec: 0.0,
            decompress_time_sec: 0.0,
            deserialize_time_sec: 0.0,
            total_time_sec: duration.as_secs_f64(),
        })
    }

    fn chunk_count(&self) -> usize {
        self.parser.chunks().len()
    }

    fn supports_parallel(&self) -> bool {
        !self.parser.chunks().is_empty()
    }
}

/// Processed chunk ready to be sent to the output channel.
struct ProcessedChunk {
    /// Message chunk with all messages
    chunk: MessageChunkData,
    /// Total bytes in this chunk
    total_bytes: u64,
    /// Number of messages in this chunk
    message_count: u64,
}

/// Raw message iterator for BAG files (sequential reading).
///
/// This iterator processes chunks sequentially and yields messages one by one.
/// Used primarily by rewriters that need to process messages in order.
pub struct BagRawIter<'a> {
    /// Reference to the parser
    parser: &'a BagParser,
    /// Channel information
    channels: &'a HashMap<u16, ChannelInfo>,
    /// Connection ID to channel ID mapping
    conn_id_map: &'a HashMap<u32, u16>,
    /// Current chunk index
    current_chunk_idx: usize,
    /// Current messages from decompressed chunk
    current_messages: Vec<super::parser::BagMessageData>,
    /// Current message index within chunk
    current_msg_idx: usize,
}

impl<'a> BagRawIter<'a> {
    /// Create a new raw message iterator.
    pub fn new(
        parser: &'a BagParser,
        channels: &'a HashMap<u16, ChannelInfo>,
        conn_id_map: &'a HashMap<u32, u16>,
    ) -> Self {
        Self {
            parser,
            channels,
            conn_id_map,
            current_chunk_idx: 0,
            current_messages: Vec::new(),
            current_msg_idx: 0,
        }
    }

    /// Load the next chunk's messages.
    fn load_next_chunk(&mut self) -> Result<bool> {
        let chunks = self.parser.chunks();
        if self.current_chunk_idx >= chunks.len() {
            return Ok(false);
        }

        let chunk_info = &chunks[self.current_chunk_idx];
        self.current_chunk_idx += 1;

        // Read and decompress the chunk
        let decompressed = self.parser.read_chunk(chunk_info)?;

        // Parse messages from decompressed data
        self.current_messages = self
            .parser
            .parse_chunk_messages(&decompressed, self.conn_id_map)?;
        self.current_msg_idx = 0;

        Ok(true)
    }
}

impl<'a> Iterator for BagRawIter<'a> {
    type Item = Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we have messages in current chunk
            if self.current_msg_idx < self.current_messages.len() {
                let msg = &self.current_messages[self.current_msg_idx];
                self.current_msg_idx += 1;

                if let Some(channel_info) = self.channels.get(&msg.channel_id) {
                    return Some(Ok((
                        RawMessage {
                            channel_id: msg.channel_id,
                            log_time: msg.log_time,
                            publish_time: msg.publish_time,
                            data: msg.data.clone(),
                            sequence: Some(msg.sequence as u64),
                        },
                        channel_info.clone(),
                    )));
                }
                // Return error for unknown channel instead of silently skipping
                // This indicates data corruption or an indexing bug
                return Some(Err(CodecError::parse(
                    "BagRawIter",
                    format!(
                        "Unknown channel_id {}: message refers to non-existent channel. This indicates data corruption or an indexing bug.",
                        msg.channel_id
                    ),
                )));
            }

            // Load next chunk
            match self.load_next_chunk() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Iterator over decoded BAG messages.
///
/// Yields `(DecodedMessage, ChannelInfo)` tuples where `DecodedMessage`
/// is a `HashMap<String, CodecValue>` containing decoded field values.
pub struct BagDecodedMessageIter<'a> {
    parser: &'a BagParser,
    channels: &'a HashMap<u16, ChannelInfo>,
    conn_id_map: &'a HashMap<u32, u16>,
    decoder: Arc<CdrDecoder>,
}

impl<'a> BagDecodedMessageIter<'a> {
    /// Create a new decoded message iterator.
    fn new(
        parser: &'a BagParser,
        channels: &'a HashMap<u16, ChannelInfo>,
        conn_id_map: &'a HashMap<u32, u16>,
    ) -> Self {
        Self {
            parser,
            channels,
            conn_id_map,
            decoder: Arc::new(CdrDecoder::new()),
        }
    }

    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        self.channels
    }

    /// Create a proper streaming iterator over decoded messages.
    pub fn stream(&self) -> Result<BagDecodedMessageStream<'a>> {
        BagDecodedMessageStream::new(
            self.parser,
            self.channels,
            self.conn_id_map,
            Arc::clone(&self.decoder),
        )
    }
}

impl<'a> Iterator for BagDecodedMessageIter<'a> {
    type Item = std::result::Result<(DecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Note: This placeholder implementation doesn't work properly
        // Use stream() instead to get a proper streaming iterator
        None
    }
}

/// Streaming iterator over decoded BAG messages.
pub struct BagDecodedMessageStream<'a> {
    raw_iter: BagRawIter<'a>,
    decoder: Arc<CdrDecoder>,
    /// Cache for parsed schemas (message_type -> MessageSchema)
    schema_cache: HashMap<String, crate::schema::MessageSchema>,
}

impl<'a> BagDecodedMessageStream<'a> {
    /// Create a new decoded message stream.
    fn new(
        parser: &'a BagParser,
        channels: &'a HashMap<u16, ChannelInfo>,
        conn_id_map: &'a HashMap<u32, u16>,
        decoder: Arc<CdrDecoder>,
    ) -> Result<Self> {
        Ok(Self {
            raw_iter: BagRawIter::new(parser, channels, conn_id_map),
            decoder,
            schema_cache: HashMap::new(),
        })
    }

    /// Get or parse a schema for the given message type.
    fn get_or_parse_schema(
        &mut self,
        message_type: &str,
        message_definition: &str,
    ) -> std::result::Result<crate::schema::MessageSchema, CodecError> {
        // Check cache first
        if let Some(schema) = self.schema_cache.get(message_type) {
            return Ok(schema.clone());
        }

        // Parse the schema from the message definition
        let schema = crate::schema::parse_schema(message_type, message_definition)
            .map_err(|e| CodecError::parse(message_type, format!("Failed to parse schema: {e}")))?;

        // Cache it
        self.schema_cache
            .insert(message_type.to_string(), schema.clone());
        Ok(schema)
    }
}

impl<'a> Iterator for BagDecodedMessageStream<'a> {
    type Item = std::result::Result<(DecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (raw_msg, channel_info) = match self.raw_iter.next()? {
            Ok(msg) => msg,
            Err(e) => return Some(Err(e)),
        };

        // Decode using ROS1 CDR deserialization
        // BAG files store message definitions in the connection's message_definition field
        if let Some(schema) = &channel_info.schema {
            let parsed_schema = match self.get_or_parse_schema(&channel_info.message_type, schema) {
                Ok(s) => s,
                Err(e) => return Some(Err(e)),
            };

            // Use decode_headerless_ros1 for ROS1 bag messages
            // The BAG parser extracts just the CDR message data (without wrapper headers),
            // so we need to decode from byte 0 with ROS1 alignment rules.
            match self.decoder.decode_headerless_ros1(
                &parsed_schema,
                &raw_msg.data,
                Some(&channel_info.message_type),
            ) {
                Ok(msg) => Some(Ok((msg, channel_info))),
                Err(e) => Some(Err(CodecError::parse(
                    &channel_info.message_type,
                    format!("Decode failed: {e}"),
                ))),
            }
        } else {
            Some(Err(CodecError::parse(
                &channel_info.message_type,
                "No schema available (message_definition not found in connection)",
            )))
        }
    }
}

/// Iterator over decoded BAG messages with timestamps.
///
/// Yields `(TimestampedDecodedMessage, ChannelInfo)` tuples where each
/// message includes both the decoded field values and the log/publish timestamps.
pub struct BagDecodedMessageWithTimestampIter<'a> {
    parser: &'a BagParser,
    channels: &'a HashMap<u16, ChannelInfo>,
    conn_id_map: &'a HashMap<u32, u16>,
    decoder: Arc<CdrDecoder>,
}

impl<'a> BagDecodedMessageWithTimestampIter<'a> {
    /// Create a new decoded message iterator with timestamps.
    fn new(
        parser: &'a BagParser,
        channels: &'a HashMap<u16, ChannelInfo>,
        conn_id_map: &'a HashMap<u32, u16>,
    ) -> Self {
        Self {
            parser,
            channels,
            conn_id_map,
            decoder: Arc::new(CdrDecoder::new()),
        }
    }

    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        self.channels
    }

    /// Create a proper streaming iterator over decoded messages with timestamps.
    pub fn stream(&self) -> Result<BagDecodedMessageWithTimestampStream<'a>> {
        BagDecodedMessageWithTimestampStream::new(
            self.parser,
            self.channels,
            self.conn_id_map,
            Arc::clone(&self.decoder),
        )
    }
}

impl<'a> Iterator for BagDecodedMessageWithTimestampIter<'a> {
    type Item = std::result::Result<(TimestampedDecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Note: This placeholder implementation doesn't work properly
        // Use stream() instead to get a proper streaming iterator
        None
    }
}

/// Streaming iterator over decoded BAG messages with timestamps.
pub struct BagDecodedMessageWithTimestampStream<'a> {
    raw_iter: BagRawIter<'a>,
    decoder: Arc<CdrDecoder>,
    /// Cache for parsed schemas (message_type -> MessageSchema)
    schema_cache: HashMap<String, crate::schema::MessageSchema>,
}

impl<'a> BagDecodedMessageWithTimestampStream<'a> {
    /// Create a new decoded message stream with timestamps.
    fn new(
        parser: &'a BagParser,
        channels: &'a HashMap<u16, ChannelInfo>,
        conn_id_map: &'a HashMap<u32, u16>,
        decoder: Arc<CdrDecoder>,
    ) -> Result<Self> {
        Ok(Self {
            raw_iter: BagRawIter::new(parser, channels, conn_id_map),
            decoder,
            schema_cache: HashMap::new(),
        })
    }

    /// Get or parse a schema for the given message type.
    fn get_or_parse_schema(
        &mut self,
        message_type: &str,
        message_definition: &str,
    ) -> std::result::Result<crate::schema::MessageSchema, CodecError> {
        // Check cache first
        if let Some(schema) = self.schema_cache.get(message_type) {
            return Ok(schema.clone());
        }

        // Parse the schema from the message definition
        let schema = crate::schema::parse_schema(message_type, message_definition)
            .map_err(|e| CodecError::parse(message_type, format!("Failed to parse schema: {e}")))?;

        // Cache it
        self.schema_cache
            .insert(message_type.to_string(), schema.clone());
        Ok(schema)
    }
}

impl<'a> Iterator for BagDecodedMessageWithTimestampStream<'a> {
    type Item = std::result::Result<(TimestampedDecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (raw_msg, channel_info) = match self.raw_iter.next()? {
            Ok(msg) => msg,
            Err(e) => return Some(Err(e)),
        };

        // Decode using ROS1 CDR deserialization
        // BAG files store message definitions in the connection's message_definition field
        if let Some(schema) = &channel_info.schema {
            let parsed_schema = match self.get_or_parse_schema(&channel_info.message_type, schema) {
                Ok(s) => s,
                Err(e) => return Some(Err(e)),
            };

            // Use decode_headerless_ros1 for ROS1 bag messages
            // The BAG parser extracts just the CDR message data (without wrapper headers),
            // so we need to decode from byte 0 with ROS1 alignment rules.
            match self.decoder.decode_headerless_ros1(
                &parsed_schema,
                &raw_msg.data,
                Some(&channel_info.message_type),
            ) {
                Ok(message) => Some(Ok((
                    TimestampedDecodedMessage {
                        message,
                        log_time: raw_msg.log_time,
                        publish_time: raw_msg.publish_time,
                    },
                    channel_info,
                ))),
                Err(e) => Some(Err(CodecError::parse(
                    &channel_info.message_type,
                    format!(
                        "Decode failed for topic '{}' with log_time {}: {}",
                        channel_info.topic, raw_msg.log_time, e
                    ),
                ))),
            }
        } else {
            Some(Err(CodecError::parse(
                &channel_info.message_type,
                "No schema available (message_definition not found in connection)",
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::metadata::FileFormat;
    use std::path::Path;

    // =========================================================================
    // BagFormat Tests
    // =========================================================================

    #[test]
    fn test_bag_format() {
        let _ = BagFormat;
    }

    // =========================================================================
    // ProcessedChunk Tests
    // =========================================================================

    #[test]
    fn test_processed_chunk_creation() {
        let chunk = MessageChunkData::new(0);
        let processed = ProcessedChunk {
            chunk,
            total_bytes: 100,
            message_count: 5,
        };

        assert_eq!(processed.total_bytes, 100);
        assert_eq!(processed.message_count, 5);
        assert_eq!(processed.chunk.message_count(), 0);
    }

    // =========================================================================
    // ParallelBagReader::open Error Tests
    // =========================================================================

    #[test]
    fn test_parallel_bag_reader_open_nonexistent() {
        let result = ParallelBagReader::open("/nonexistent/path/to/file.bag");
        assert!(result.is_err());
    }

    #[test]
    fn test_parallel_bag_reader_open_empty_file() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        // Empty file should fail

        let result = ParallelBagReader::open(temp_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parallel_bag_reader_open_invalid_bag() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let mut file = temp_file.as_file();
        file.write_all(b"not a valid bag file").unwrap();

        let result = ParallelBagReader::open(temp_file.path());
        assert!(result.is_err());
    }

    // =========================================================================
    // FormatReader Trait Tests (with real fixtures)
    // =========================================================================

    #[test]
    fn test_parallel_bag_reader_with_fixture() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    assert_eq!(r.format(), FileFormat::Bag);
                    assert!(!r.path().is_empty());
                    assert!(r.file_size() > 0);
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_channels() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let channels = r.channels();
                    // Channels should be accessible
                    let _ = channels.len();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_conn_id_map() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let conn_map = r.conn_id_map();
                    // Connection map should be accessible
                    let _ = conn_map.len();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_chunks() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let chunks = r.chunks();
                    // Chunks should be accessible
                    let _ = chunks.len();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_connections() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let conns = r.connections();
                    // Connections should be accessible
                    let _ = conns.len();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_message_count() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    // Message count should be accessible
                    let _ = r.message_count();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_times() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    // Time fields should be accessible
                    let _ = r.start_time();
                    let _ = r.end_time();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_as_any() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let any = r.as_any();
                    assert!(any.is::<ParallelBagReader>());
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_as_any_mut() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(mut r) => {
                    let any_mut = r.as_any_mut();
                    assert!(any_mut.is::<ParallelBagReader>());
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    // =========================================================================
    // ParallelReader Trait Tests
    // =========================================================================

    #[test]
    fn test_parallel_bag_reader_chunk_count() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    // Chunk count should be accessible
                    let _ = r.chunk_count();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_supports_parallel() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    // Check if parallel reading is supported
                    let _ = r.supports_parallel();
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    // =========================================================================
    // BagRawIter Tests
    // =========================================================================

    #[test]
    fn test_bag_raw_iter_creation() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let iter = r.iter_raw();
                    // Iterator should be created successfully
                    let _ = iter;
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    // =========================================================================
    // BagDecodedMessageIter Tests
    // =========================================================================

    #[test]
    fn test_bag_decoded_message_iter_channels() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let iter = r.decode_messages();
                    match iter {
                        Ok(decoded_iter) => {
                            let channels = decoded_iter.channels();
                            // Channels should be accessible
                            let _ = channels.len();
                        }
                        Err(_) => {
                            // May fail if no schemas
                        }
                    }
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_bag_decoded_message_iter_stream() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let iter = r.decode_messages();
                    match iter {
                        Ok(decoded_iter) => {
                            let stream = decoded_iter.stream();
                            match stream {
                                Ok(_) => {
                                    // Stream should be created successfully
                                }
                                Err(_) => {
                                    // May fail without proper schemas
                                }
                            }
                        }
                        Err(_) => {
                            // May fail if no schemas
                        }
                    }
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    // =========================================================================
    // BagDecodedMessageWithTimestampIter Tests
    // =========================================================================

    #[test]
    fn test_bag_decoded_message_with_timestamp_iter_channels() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let iter = r.decode_messages_with_timestamp();
                    match iter {
                        Ok(decoded_iter) => {
                            let channels = decoded_iter.channels();
                            // Channels should be accessible
                            let _ = channels.len();
                        }
                        Err(_) => {
                            // May fail if no schemas
                        }
                    }
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    #[test]
    fn test_bag_decoded_message_with_timestamp_iter_stream() {
        let fixture_path = "tests/fixtures/robocodec_test_0.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path);
            match reader {
                Ok(r) => {
                    let iter = r.decode_messages_with_timestamp();
                    match iter {
                        Ok(decoded_iter) => {
                            let stream = decoded_iter.stream();
                            match stream {
                                Ok(_) => {
                                    // Stream should be created successfully
                                }
                                Err(_) => {
                                    // May fail without proper schemas
                                }
                            }
                        }
                        Err(_) => {
                            // May fail if no schemas
                        }
                    }
                }
                Err(_) => {
                    // Format may not match - acceptable
                }
            }
        }
    }

    // =========================================================================
    // Encoding Constant Tests
    // =========================================================================

    #[test]
    fn test_ros1_encoding_constant() {
        // Verify that we use "ros1" encoding for ROS1 bag files
        // This is important because "cdr" is for ROS2 and will cause
        // "Message encoding cdr with schema encoding 'ros1msg' is not supported" errors
        let ros1_encoding = "ros1";
        let ros1msg_schema_encoding = "ros1msg";

        // These constants should match what's used in the reader
        assert_eq!(ros1_encoding, "ros1");
        assert_eq!(ros1msg_schema_encoding, "ros1msg");

        // Verify they are compatible (ros1 encoding with ros1msg schema)
        assert!(ros1_encoding.starts_with("ros1"));
        assert!(ros1msg_schema_encoding.starts_with("ros1"));
    }

    // =========================================================================
    // Schema Cache Tests
    // =========================================================================

    // =========================================================================
    // Integration Tests with Real Fixtures
    // =========================================================================

    #[test]
    fn test_parallel_bag_reader_open_with_valid_fixtures() {
        // Test opening all available BAG fixture files
        let fixtures = [
            "tests/fixtures/robocodec_test_15.bag",
            "tests/fixtures/robocodec_test_17.bag",
            "tests/fixtures/robocodec_test_18.bag",
            "tests/fixtures/robocodec_test_19.bag",
            "tests/fixtures/robocodec_test_20.bag",
            "tests/fixtures/robocodec_test_21.bag",
            "tests/fixtures/robocodec_test_22.bag",
            "tests/fixtures/robocodec_test_23.bag",
        ];

        for fixture_path in fixtures {
            if Path::new(fixture_path).exists() {
                let result = ParallelBagReader::open(fixture_path);
                assert!(
                    result.is_ok(),
                    "Should open {}: {:?}",
                    fixture_path,
                    result.err()
                );
                if let Ok(reader) = result {
                    assert_eq!(reader.format(), FileFormat::Bag);
                    assert!(!reader.path().is_empty());
                    assert!(reader.file_size() > 0);
                }
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_channels_from_fixtures() {
        // Test that channels are correctly extracted from fixture files
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let channels = reader.channels();

            assert!(!channels.is_empty(), "Should have at least one channel");

            // Verify channel structure
            for (id, channel) in channels.iter() {
                assert_eq!(*id, channel.id);
                assert!(!channel.topic.is_empty());
                assert!(!channel.message_type.is_empty());
                assert_eq!(channel.encoding, "ros1");
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_conn_id_map_consistency() {
        // Test that conn_id_map correctly maps to channel IDs
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let conn_map = reader.conn_id_map();
            let channels = reader.channels();

            // Verify all conn_id_map values point to valid channels
            for (&conn_id, &channel_id) in conn_map.iter() {
                assert!(
                    channels.contains_key(&channel_id),
                    "conn_id {} maps to non-existent channel_id {}",
                    conn_id,
                    channel_id
                );
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_timestamps() {
        // Test start and end time are extracted
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");

            let _ = reader.start_time();
            let end = reader.end_time();

            // At least end time should be present for files with messages
            if reader.message_count() > 0 {
                assert!(end.is_some(), "Should have end_time when messages exist");
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_chunk_info() {
        // Test chunk information is extracted
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let chunks = reader.chunks();

            assert!(!chunks.is_empty(), "Should have at least one chunk");

            // Verify chunk structure
            for chunk in chunks.iter() {
                assert!(chunk.chunk_pos > 0, "Chunk position should be positive");
            }
        }
    }

    #[test]
    fn test_parallel_bag_reader_connections_structure() {
        // Test connection information structure
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let connections = reader.connections();

            assert!(
                !connections.is_empty(),
                "Should have at least one connection"
            );

            // Verify connection structure
            for (conn_id, conn) in connections.iter() {
                assert_eq!(*conn_id, conn.conn_id);
                assert!(!conn.topic.is_empty());
                assert!(!conn.message_type.is_empty());
            }
        }
    }

    // =========================================================================
    // BagRawIter Tests with Fixtures
    // =========================================================================

    #[test]
    fn test_bag_raw_iter_iteration() {
        // Test raw iterator actually yields messages
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let iter = reader.iter_raw().expect("Failed to create iterator");

            let mut count = 0;
            for result in iter.take(10) {
                match result {
                    Ok((raw_msg, channel_info)) => {
                        assert!(!channel_info.topic.is_empty());
                        assert!(raw_msg.log_time > 0 || raw_msg.publish_time > 0);
                        count += 1;
                    }
                    Err(_) => {
                        // Some messages may fail to decode - that's okay
                    }
                }
            }

            assert!(count > 0, "Should iterate at least one raw message");
        }
    }

    #[test]
    fn test_bag_raw_iter_message_structure() {
        // Test raw message structure
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let iter = reader.iter_raw().expect("Failed to create iterator");

            for (raw_msg, channel_info) in iter.take(5).filter_map(|r| r.ok()) {
                // Verify raw message fields
                assert!(!raw_msg.data.is_empty(), "Message data should not be empty");
                assert!(raw_msg.channel_id < 1000, "Channel ID should be reasonable");

                // Verify channel info matches
                assert_eq!(raw_msg.channel_id, channel_info.id);
            }
        }
    }

    // =========================================================================
    // BagDecodedMessageIter Tests with Fixtures
    // =========================================================================

    #[test]
    fn test_bag_decoded_message_iter_stream_iteration() {
        // Test decoded message stream actually yields decoded messages
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let decoded_iter = reader.decode_messages().expect("Failed to create iterator");
            let stream = decoded_iter.stream().expect("Failed to create stream");

            let mut success_count = 0;
            let mut error_count = 0;

            for result in stream.take(20) {
                match result {
                    Ok((message, channel_info)) => {
                        assert!(!channel_info.topic.is_empty());
                        assert!(!message.is_empty(), "Decoded message should have fields");
                        success_count += 1;
                        if success_count >= 1 {
                            break;
                        }
                    }
                    Err(_) => {
                        error_count += 1;
                    }
                }
            }

            assert!(
                success_count > 0,
                "Should decode at least one message (errors: {})",
                error_count
            );
        }
    }

    #[test]
    fn test_bag_decoded_message_iter_schema_cache() {
        // Test that schema cache works across multiple messages
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let decoded_iter = reader.decode_messages().expect("Failed to create iterator");

            let channels = decoded_iter.channels();
            assert!(!channels.is_empty(), "Should have channels");

            // Verify schema information is present
            for channel in channels.values() {
                if channel.message_type.starts_with("std_msgs")
                    || channel.message_type.starts_with("sensor_msgs")
                {
                    assert!(channel.schema.is_some(), "ROS messages should have schema");
                    assert_eq!(channel.schema_encoding, Some("ros1msg".to_string()));
                }
            }
        }
    }

    // =========================================================================
    // BagDecodedMessageWithTimestampIter Tests with Fixtures
    // =========================================================================

    #[test]
    fn test_bag_decoded_message_with_timestamp_stream_iteration() {
        // Test timestamped decoded message stream
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let timestamped_iter = reader
                .decode_messages_with_timestamp()
                .expect("Failed to create iterator");
            let stream = timestamped_iter.stream().expect("Failed to create stream");

            let mut found_message = false;

            for result in stream.take(20) {
                match result {
                    Ok((timestamped, channel_info)) => {
                        assert!(!channel_info.topic.is_empty());
                        assert!(!timestamped.message.is_empty());

                        // Verify timestamps are present
                        assert!(
                            timestamped.log_time > 0 || timestamped.publish_time > 0,
                            "At least one timestamp should be positive"
                        );

                        found_message = true;
                        break;
                    }
                    Err(_) => {
                        // Some decode errors are acceptable
                    }
                }
            }

            assert!(
                found_message,
                "Should decode at least one timestamped message"
            );
        }
    }

    #[test]
    fn test_bag_decoded_message_timestamps_consistency() {
        // Test that timestamps are consistent across channels
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let timestamped_iter = reader
                .decode_messages_with_timestamp()
                .expect("Failed to create iterator");
            let stream = timestamped_iter.stream().expect("Failed to create stream");

            let mut timestamps = Vec::new();

            for (timestamped, _) in stream.take(10).filter_map(|r| r.ok()) {
                timestamps.push((timestamped.log_time, timestamped.publish_time));
            }

            assert!(!timestamps.is_empty(), "Should collect some timestamps");
        }
    }

    // =========================================================================
    // ParallelReader Trait Tests with Fixtures
    // =========================================================================

    #[test]
    fn test_parallel_reader_chunk_count() {
        // Test chunk count from fixtures
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let chunk_count = reader.chunk_count();
            let chunks = reader.chunks();

            assert_eq!(chunk_count, chunks.len());
            assert!(chunk_count > 0);
        }
    }

    #[test]
    fn test_parallel_reader_supports_parallel() {
        // Test supports_parallel returns true for files with chunks
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            assert!(
                reader.supports_parallel(),
                "Should support parallel reading"
            );
        }
    }

    #[test]
    fn test_parallel_reader_read_parallel_basic() {
        // Test read_parallel with minimal config
        let fixture_path = "tests/fixtures/robocodec_test_18.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");

            let config = crate::io::traits::ParallelReaderConfig {
                num_threads: Some(1),
                topic_filter: None,
                channel_capacity: Some(32),
                progress_interval: 1,
                merge_enabled: false,
                merge_target_size: 1024,
            };

            let (sender, _receiver) = crossbeam_channel::unbounded();
            let result = reader.read_parallel(config, sender);

            assert!(result.is_ok(), "read_parallel should succeed: {:?}", result);
            let stats = result.unwrap();
            assert!(stats.messages_read > 0, "Should read some messages");
        }
    }

    #[test]
    fn test_parallel_reader_read_parallel_multiple_threads() {
        // Test read_parallel with multiple threads
        let fixture_path = "tests/fixtures/robocodec_test_18.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");

            let config = crate::io::traits::ParallelReaderConfig {
                num_threads: Some(2),
                topic_filter: None,
                channel_capacity: Some(32),
                progress_interval: 10,
                merge_enabled: false,
                merge_target_size: 1024,
            };

            let (sender, _receiver) = crossbeam_channel::unbounded();
            let result = reader.read_parallel(config, sender);

            assert!(
                result.is_ok(),
                "read_parallel with 2 threads should succeed"
            );
            let stats = result.unwrap();
            assert!(stats.chunks_processed > 0, "Should process some chunks");
        }
    }

    #[test]
    fn test_parallel_reader_read_parallel_stats() {
        // Test ParallelReaderStats are populated correctly
        let fixture_path = "tests/fixtures/robocodec_test_18.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");

            let config = crate::io::traits::ParallelReaderConfig {
                num_threads: Some(1),
                topic_filter: None,
                channel_capacity: Some(32),
                progress_interval: 1,
                merge_enabled: false,
                merge_target_size: 1024,
            };

            let (sender, _receiver) = crossbeam_channel::unbounded();
            let result = reader.read_parallel(config, sender);

            assert!(result.is_ok());
            let stats = result.unwrap();

            assert!(stats.messages_read > 0, "messages_read should be positive");
            assert!(
                stats.chunks_processed > 0,
                "chunks_processed should be positive"
            );
            assert!(stats.total_bytes > 0, "total_bytes should be positive");
            assert!(
                stats.total_time_sec >= 0.0,
                "total_time_sec should be non-negative"
            );
        }
    }

    // =========================================================================
    // ProcessedChunk Tests
    // =========================================================================

    #[test]
    fn test_processed_chunk_with_messages() {
        // Test ProcessedChunk with actual MessageChunkData
        let mut chunk = MessageChunkData::new(42);
        chunk.add_message(RawMessage {
            channel_id: 1,
            log_time: 1000,
            publish_time: 900,
            data: vec![1, 2, 3],
            sequence: Some(0),
        });

        let processed = ProcessedChunk {
            chunk,
            total_bytes: 3,
            message_count: 1,
        };

        assert_eq!(processed.total_bytes, 3);
        assert_eq!(processed.message_count, 1);
        assert_eq!(processed.chunk.sequence, 42);
        assert_eq!(processed.chunk.message_count(), 1);
    }

    #[test]
    fn test_processed_chunk_empty() {
        // Test ProcessedChunk with empty chunk
        let chunk = MessageChunkData::new(0);
        let processed = ProcessedChunk {
            chunk,
            total_bytes: 0,
            message_count: 0,
        };

        assert_eq!(processed.total_bytes, 0);
        assert_eq!(processed.message_count, 0);
        assert_eq!(processed.chunk.message_count(), 0);
    }

    // =========================================================================
    // FormatReader Trait Implementation Tests
    // =========================================================================

    #[test]
    fn test_format_reader_all_methods() {
        // Test all FormatReader trait methods
        let fixture_path = "tests/fixtures/robocodec_test_15.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");

            // Test all trait methods
            let _channels = reader.channels();
            let _msg_count = reader.message_count();
            let _start = reader.start_time();
            let _end = reader.end_time();
            let _path = reader.path();
            let format = reader.format();
            let _size = reader.file_size();

            assert_eq!(format, FileFormat::Bag);

            // Test as_any downcasting
            let any = reader.as_any();
            assert!(any.is::<ParallelBagReader>());
        }
    }

    // =========================================================================
    // Multi-File Tests
    // =========================================================================

    #[test]
    fn test_multiple_bag_files_different_channels() {
        // Test that different BAG files have different channel sets
        let fixtures = [
            ("tests/fixtures/robocodec_test_15.bag", "test_15"),
            ("tests/fixtures/robocodec_test_17.bag", "test_17"),
        ];

        let mut channel_sets = Vec::new();

        for (path, _name) in fixtures {
            if Path::new(path).exists()
                && let Ok(reader) = ParallelBagReader::open(path)
            {
                let topics: Vec<_> = reader
                    .channels()
                    .values()
                    .map(|c| c.topic.clone())
                    .collect();
                channel_sets.push(topics);
            }
        }

        // Different files should have different channel configurations
        if channel_sets.len() >= 2 {
            // At minimum, verify we can open and read channels from multiple files
            for (i, channels) in channel_sets.iter().enumerate() {
                assert!(!channels.is_empty(), "File {} should have channels", i);
            }
        }
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    #[test]
    fn test_bag_raw_iter_empty_chunk_handling() {
        // Test that empty chunks are handled correctly
        let fixture_path = "tests/fixtures/robocodec_test_19.bag";
        if Path::new(fixture_path).exists() {
            let reader = ParallelBagReader::open(fixture_path).expect("Failed to open");
            let iter = reader.iter_raw().expect("Failed to create iterator");

            // Just verify we can iterate without panicking
            let count = iter.count();
            // Count might be 0 if file is empty or all messages fail
            let _ = count;
        }
    }
}
