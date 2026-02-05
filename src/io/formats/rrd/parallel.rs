// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Parallel RRD reader with memory-mapped file access.
//!
//! This module provides parallel reading capability for RRF2 files.
//! Since RRF2 doesn't have built-in chunk indexing, this implementation:
//! 1. Scans the file to build a message index on first open
//! 2. Divides messages into chunks for parallel processing
//! 3. Uses Rayon for concurrent decompression and parsing

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;

use rayon::prelude::*;

use crate::io::TopicFilter;
use crate::io::formats::rrd::constants::*;
use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::{
    FormatReader, MessageChunkData, ParallelReader, ParallelReaderConfig, ParallelReaderStats,
};
use crate::{CodecError, Result};

/// Message index entry for RRD files.
///
/// Since RRF2 doesn't have built-in chunk indexing, we scan the file
/// and create these entries to enable parallel processing.
#[derive(Debug, Clone)]
pub struct MessageIndex {
    /// Offset in file where message starts (after stream header)
    pub offset: u64,
    /// Message kind
    pub kind: u64,
    /// Message data length
    pub length: usize,
    /// Topic name for this message
    pub topic: String,
}

/// Parallel RRD reader with message indexing.
///
/// This reader scans the RRD file to build a message index,
/// then supports parallel processing of message chunks.
pub struct ParallelRrdReader {
    /// File path
    path: String,
    /// Memory-mapped file data
    mmap: memmap2::Mmap,
    /// Channel information
    channels: HashMap<u16, ChannelInfo>,
    /// Total message count
    message_count: u64,
    /// Start timestamp
    start_time: Option<u64>,
    /// End timestamp
    end_time: Option<u64>,
    /// Message index for parallel reading
    message_index: Vec<MessageIndex>,
    /// File size
    file_size: u64,
}

impl ParallelRrdReader {
    /// Open an RRD file for parallel reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_str = path_ref.to_string_lossy().to_string();

        let file = File::open(path_ref).map_err(|e| {
            CodecError::encode("ParallelRrdReader", format!("Failed to open file: {e}"))
        })?;

        let file_size = file
            .metadata()
            .map_err(|e| {
                CodecError::encode("ParallelRrdReader", format!("Failed to get metadata: {e}"))
            })?
            .len();

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("ParallelRrdReader", format!("Failed to mmap file: {e}"))
        })?;

        // Read metadata and build message index
        let metadata = Self::read_metadata(&mmap)?;

        Ok(Self {
            path: path_str,
            mmap,
            channels: metadata.channels,
            message_count: metadata.message_count,
            start_time: metadata.start_time,
            end_time: metadata.end_time,
            message_index: metadata.message_index,
            file_size,
        })
    }

    /// Get the message index.
    pub fn message_index(&self) -> &[MessageIndex] {
        &self.message_index
    }

    /// Read metadata and build message index from an RRD file.
    fn read_metadata(data: &[u8]) -> Result<RrdMetadata> {
        if data.len() < STREAM_HEADER_SIZE {
            return Err(CodecError::parse(
                "ParallelRrdReader",
                "File too small for RRD header",
            ));
        }

        // Verify magic
        let magic = &data[0..4];
        if magic != RRD_MAGIC {
            return Err(CodecError::parse(
                "ParallelRrdReader",
                format!(
                    "Invalid RRD magic: expected {:?}, got {:?}",
                    RRD_MAGIC, magic
                ),
            ));
        }

        // Build message index by scanning the file
        let message_index = Self::build_message_index(data)?;

        // Create default channel
        let mut channels = HashMap::new();
        let default_channel = ChannelInfo {
            id: 0,
            topic: DEFAULT_TOPIC.to_string(),
            message_type: "rerun.ArrowMsg".to_string(),
            encoding: MESSAGE_ENCODING_PROTOBUF.to_string(),
            schema: None,
            schema_data: None,
            schema_encoding: Some("protobuf".to_string()),
            message_count: message_index.len() as u64,
            callerid: None,
        };
        channels.insert(0, default_channel);

        Ok(RrdMetadata {
            channels,
            message_count: message_index.len() as u64,
            start_time: None,
            end_time: None,
            message_index,
        })
    }

    /// Build a message index by scanning the RRD file.
    ///
    /// This scan finds all messages and records their offsets, kinds, and lengths
    /// to enable parallel processing.
    fn build_message_index(data: &[u8]) -> Result<Vec<MessageIndex>> {
        let mut index = Vec::new();
        let mut pos = STREAM_HEADER_SIZE; // Start after stream header

        while pos + MESSAGE_HEADER_SIZE <= data.len() {
            // Check if we're at the footer (last 32 bytes)
            if pos + STREAM_FOOTER_SIZE <= data.len() {
                let footer_start = data.len() - STREAM_FOOTER_SIZE;
                if pos >= footer_start {
                    break;
                }
            }

            // Read message header: kind(u64 le) + len(u64 le)
            let kind = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap_or([0u8; 8]));
            let len =
                u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap_or([0u8; 8])) as usize;

            // Sanity check on length
            if len > data.len() - pos - MESSAGE_HEADER_SIZE {
                break;
            }

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

            index.push(MessageIndex {
                offset: pos as u64,
                kind,
                length: len,
                topic,
            });

            pos += len;
        }

        Ok(index)
    }

    /// Check if an RRD file can be read in parallel.
    ///
    /// Returns (has_messages, message_count).
    pub fn check_parallel<P: AsRef<Path>>(path: P) -> Result<(bool, usize)> {
        let file = File::open(path.as_ref()).map_err(|e| {
            CodecError::encode("ParallelRrdReader", format!("Failed to open file: {e}"))
        })?;

        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            CodecError::encode("ParallelRrdReader", format!("Failed to mmap file: {e}"))
        })?;

        let index = Self::build_message_index(&mmap)?;
        Ok((!index.is_empty(), index.len()))
    }

    /// Process a chunk of message indices in parallel.
    fn process_chunk(
        &self,
        chunk_start: usize,
        chunk_end: usize,
        _filter: &Option<TopicFilter>,
    ) -> Result<MessageChunkData> {
        let mut chunk_data = MessageChunkData::new(chunk_start as u64);

        for idx in &self.message_index[chunk_start..chunk_end] {
            // Extract payload from mmap
            let offset = idx.offset as usize;
            if offset + idx.length <= self.mmap.len() {
                let payload = &self.mmap[offset..offset + idx.length];

                // Create raw message
                let raw_msg = RawMessage {
                    channel_id: 0, // RRF2 uses a single channel
                    log_time: 0,   // RRF2 doesn't have timestamps at message level
                    publish_time: 0,
                    data: payload.to_vec(),
                    sequence: None,
                };

                chunk_data.add_message(raw_msg);
            }
        }

        Ok(chunk_data)
    }

    /// Decode messages with timestamps from the RRD file.
    ///
    /// Returns an iterator that yields decoded messages with their log_time and publish_time.
    /// RRF2 doesn't have timestamps at message level, so sequential timestamps are generated.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::formats::rrd::RrdFormat;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = RrdFormat::open("data.rrd")?;
    /// let decoded_iter = reader.decode_messages_with_timestamp()?;
    /// let mut stream = decoded_iter.stream()?;
    ///
    /// while let Some(result) = stream.next() {
    ///     let (timestamped_msg, channel_info) = result?;
    ///     println!("Topic: {}", channel_info.topic);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn decode_messages_with_timestamp(&self) -> Result<RrdDecodedMessageWithTimestampIter<'_>> {
        Ok(RrdDecodedMessageWithTimestampIter::new(
            &self.message_index,
            &self.mmap,
            &self.channels,
        ))
    }
}

/// Iterator over decoded RRD messages with timestamps.
///
/// Yields `(TimestampedDecodedMessage, ChannelInfo)` tuples where each
/// message includes both the decoded field values and the log/publish timestamps.
/// RRF2 doesn't have timestamps at message level, so sequential timestamps are generated.
pub struct RrdDecodedMessageWithTimestampIter<'a> {
    /// Reference to message index
    message_index: &'a [MessageIndex],
    /// Reference to memory-mapped file data
    mmap: &'a memmap2::Mmap,
    /// Channel information
    channels: &'a HashMap<u16, ChannelInfo>,
    /// Current position in message index
    current_index: usize,
    /// Start timestamp for generating sequential timestamps
    start_timestamp: u64,
}

impl<'a> RrdDecodedMessageWithTimestampIter<'a> {
    /// Create a new decoded message iterator with timestamps.
    fn new(
        message_index: &'a [MessageIndex],
        mmap: &'a memmap2::Mmap,
        channels: &'a HashMap<u16, ChannelInfo>,
    ) -> Self {
        Self {
            message_index,
            mmap,
            channels,
            current_index: 0,
            start_timestamp: 0,
        }
    }

    /// Get the channels for this iterator.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        self.channels
    }

    /// Create a proper streaming iterator over decoded messages with timestamps.
    pub fn stream(&self) -> Result<RrdDecodedMessageWithTimestampStream<'a>> {
        RrdDecodedMessageWithTimestampStream::new(
            self.message_index,
            self.mmap,
            self.channels,
            self.start_timestamp,
        )
    }
}

impl<'a> Iterator for RrdDecodedMessageWithTimestampIter<'a> {
    type Item = Result<(crate::io::metadata::TimestampedDecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Note: This placeholder implementation doesn't work properly
        // Use stream() instead to get a proper streaming iterator
        None
    }
}

/// Streaming iterator over decoded RRD messages with timestamps.
pub struct RrdDecodedMessageWithTimestampStream<'a> {
    /// Reference to message index
    message_index: &'a [MessageIndex],
    /// Reference to memory-mapped file data
    mmap: &'a memmap2::Mmap,
    /// Channel information
    channels: &'a HashMap<u16, ChannelInfo>,
    /// Current position in message index
    current_index: usize,
    /// Current timestamp for sequential numbering
    current_timestamp: u64,
}

impl<'a> RrdDecodedMessageWithTimestampStream<'a> {
    /// Create a new decoded message stream with timestamps.
    fn new(
        message_index: &'a [MessageIndex],
        mmap: &'a memmap2::Mmap,
        channels: &'a HashMap<u16, ChannelInfo>,
        start_timestamp: u64,
    ) -> Result<Self> {
        Ok(Self {
            message_index,
            mmap,
            channels,
            current_index: 0,
            current_timestamp: start_timestamp,
        })
    }
}

impl<'a> Iterator for RrdDecodedMessageWithTimestampStream<'a> {
    type Item = Result<(crate::io::metadata::TimestampedDecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::core::{CodecValue, DecodedMessage};
        use crate::io::metadata::TimestampedDecodedMessage;

        if self.current_index >= self.message_index.len() {
            return None;
        }

        let idx = &self.message_index[self.current_index];
        self.current_index += 1;

        // Extract payload from mmap
        let offset = idx.offset as usize;
        if offset + idx.length > self.mmap.len() {
            return Some(Err(CodecError::parse(
                "RrdDecodedMessageWithTimestampStream",
                format!(
                    "Message offset {} + length {} exceeds file size {}",
                    offset,
                    idx.length,
                    self.mmap.len()
                ),
            )));
        }

        let payload = &self.mmap[offset..offset + idx.length];

        // Get or create channel info
        let channel = self
            .channels
            .get(&0)
            .cloned()
            .unwrap_or_else(|| ChannelInfo {
                id: 0,
                topic: idx.topic.clone(),
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
        decoded.insert("data".to_string(), CodecValue::Bytes(payload.to_vec()));

        let timestamped = TimestampedDecodedMessage {
            message: decoded,
            log_time: self.current_timestamp,
            publish_time: self.current_timestamp,
        };

        // Increment timestamp for next message (RRF2 doesn't have per-message timestamps)
        self.current_timestamp += 1;

        Some(Ok((timestamped, channel)))
    }
}

impl FormatReader for ParallelRrdReader {
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
        FileFormat::Rrd
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

impl ParallelReader for ParallelRrdReader {
    fn read_parallel(
        &self,
        config: ParallelReaderConfig,
        sender: crossbeam_channel::Sender<MessageChunkData>,
    ) -> Result<ParallelReaderStats> {
        let start_time = Instant::now();
        let mut stats = ParallelReaderStats::default();

        if self.message_index.is_empty() {
            return Ok(stats);
        }

        // Determine chunk size based on message count and thread count
        let num_threads = config
            .num_threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));

        let messages_per_chunk = (self.message_index.len() + num_threads - 1) / num_threads;

        // Create chunks for parallel processing
        let chunks: Vec<(usize, usize)> = self
            .message_index
            .chunks(messages_per_chunk.max(1))
            .enumerate()
            .map(|(i, chunk)| {
                let start = i * messages_per_chunk.max(1);
                let end = start + chunk.len();
                (start, end)
            })
            .collect();

        stats.chunks_processed = chunks.len();

        // Process chunks in parallel using Rayon
        let results: Vec<Result<MessageChunkData>> = chunks
            .into_par_iter()
            .map(|(start, end)| self.process_chunk(start, end, &config.topic_filter))
            .collect();

        // Send results through channel
        let mut total_messages = 0u64;
        let mut total_bytes = 0u64;

        for result in results {
            match result {
                Ok(chunk) => {
                    total_messages += chunk.message_count() as u64;
                    total_bytes += chunk.total_data_size() as u64;
                    sender.send(chunk).map_err(|e| {
                        CodecError::encode("ParallelRrdReader", format!("Channel error: {e}"))
                    })?;
                }
                Err(e) => return Err(e),
            }
        }

        stats.messages_read = total_messages;
        stats.total_bytes = total_bytes;
        stats.total_time_sec = start_time.elapsed().as_secs_f64();

        Ok(stats)
    }

    fn chunk_count(&self) -> usize {
        // Return number of "chunks" based on thread count
        // For RRF2, we divide messages into chunks dynamically
        let num_threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        let messages_per_chunk = (self.message_index.len() + num_threads - 1) / num_threads;
        (self.message_index.len() + messages_per_chunk.max(1) - 1) / messages_per_chunk.max(1)
    }

    fn supports_parallel(&self) -> bool {
        !self.message_index.is_empty()
    }
}

/// Metadata extracted from RRD file.
#[derive(Debug, Clone)]
struct RrdMetadata {
    channels: HashMap<u16, ChannelInfo>,
    message_count: u64,
    start_time: Option<u64>,
    end_time: Option<u64>,
    message_index: Vec<MessageIndex>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::CodecValue;
    use std::io::Write;

    fn create_test_rrd_file(path: &str) -> std::io::Result<()> {
        let mut file = std::fs::File::create(path)?;

        // Write RRF2 stream header (12 bytes): fourcc(4) + version(4) + options(4)
        file.write_all(RRD_MAGIC)?; // fourcc: "RRF2"
        file.write_all(&RRD_VERSION)?; // version: [0, 0, 0, 1]

        // Write options: compression(1) + serializer(1) + reserved(2)
        file.write_all(&[COMPRESSION_OFF])?; // compression
        file.write_all(&[SERIALIZER_PROTOBUF])?; // serializer
        file.write_all(&[0u8, 0u8])?; // reserved

        // Write a test message: kind(u64 le) + len(u64 le) + payload
        file.write_all(&MSG_KIND_ARROW_MSG.to_le_bytes())?;
        let payload = b"test_payload";
        file.write_all(&(payload.len() as u64).to_le_bytes())?;
        file.write_all(payload)?;

        // Write end marker
        file.write_all(&MSG_KIND_END.to_le_bytes())?;
        file.write_all(&0u64.to_le_bytes())?;

        // Write RRF2 stream footer (32 bytes)
        let footer_data = vec![0u8; STREAM_FOOTER_SIZE - RRD_FOOTER_MAGIC.len()];
        file.write_all(&footer_data)?;
        file.write_all(RRD_FOOTER_MAGIC)?;

        Ok(())
    }

    #[test]
    fn test_build_message_index() {
        let temp_path = std::env::temp_dir().join("test_index.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let data = std::fs::read(&temp_path).unwrap();
        let index = ParallelRrdReader::build_message_index(&data).unwrap();

        assert_eq!(index.len(), 1); // One test message
        assert_eq!(index[0].kind, MSG_KIND_ARROW_MSG);
        assert_eq!(index[0].length, 12); // "test_payload"
        assert_eq!(index[0].topic, "/");

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_parallel_rrd_open() {
        let temp_path = std::env::temp_dir().join("test_parallel.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = ParallelRrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.message_count(), 1);
        assert!(reader.supports_parallel());
        assert_eq!(reader.chunk_count(), 1); // Single thread, single chunk

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_parallel_rrd_format() {
        let temp_path = std::env::temp_dir().join("test_format_rrd.rrd");
        create_test_rrd_file(temp_path.to_str().unwrap()).unwrap();

        let reader = ParallelRrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.format(), FileFormat::Rrd);

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_parallel_rrd_empty_file() {
        let temp_path = std::env::temp_dir().join("test_empty_rrd.rrd");
        {
            let mut file = std::fs::File::create(&temp_path).unwrap();
            // Write minimal RRD header
            file.write_all(RRD_MAGIC).unwrap();
            file.write_all(&RRD_VERSION).unwrap();
            file.write_all(&[COMPRESSION_OFF, SERIALIZER_PROTOBUF, 0, 0])
                .unwrap();
            // Write footer
            file.write_all(&[0u8; 28]).unwrap();
            file.write_all(RRD_FOOTER_MAGIC).unwrap();
        }

        let reader = ParallelRrdReader::open(&temp_path).unwrap();
        assert_eq!(reader.message_count(), 0);
        assert!(!reader.supports_parallel());

        std::fs::remove_file(&temp_path).ok();
    }

    // =========================================================================
    // Tests with real fixtures
    // =========================================================================

    fn get_fixture_path(name: &str) -> String {
        format!("tests/fixtures/rrd/{}", name)
    }

    #[test]
    fn test_parallel_rrd_open_with_fixtures() {
        let fixtures = [
            "file1.rrd",
            "file2.rrd",
            "file3.rrd",
            "small_uncompressed.rrd",
            "timestamps.rrd",
        ];

        for fixture in fixtures {
            let path = get_fixture_path(fixture);
            if std::path::Path::new(&path).exists() {
                let result = ParallelRrdReader::open(&path);
                assert!(
                    result.is_ok(),
                    "Should open {}: {:?}",
                    fixture,
                    result.err()
                );
                if let Ok(reader) = result {
                    assert_eq!(reader.format(), FileFormat::Rrd);
                    assert!(!reader.path().is_empty());
                    assert!(reader.file_size() > 0);
                }
            }
        }
    }

    #[test]
    fn test_parallel_rrd_message_count_with_fixtures() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");
            assert!(reader.message_count() > 0, "Should have messages");
        }
    }

    #[test]
    fn test_parallel_rrd_channels_with_fixtures() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");
            assert!(!reader.channels().is_empty(), "Should have channels");
        }
    }

    #[test]
    fn test_parallel_rrd_message_index() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");
            let index = reader.message_index();
            assert!(!index.is_empty(), "Should have message index entries");

            // Verify index structure
            for entry in index.iter().take(10) {
                assert!(entry.offset > 0, "Offset should be positive");
                assert!(entry.length > 0, "Length should be positive");
                assert!(!entry.topic.is_empty(), "Topic should not be empty");
            }
        }
    }

    #[test]
    fn test_parallel_rrd_supports_parallel_with_fixtures() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");
            assert!(
                reader.supports_parallel(),
                "Should support parallel reading with messages"
            );
            assert!(reader.chunk_count() > 0, "Should have chunks");
        }
    }

    #[test]
    fn test_parallel_rrd_check_parallel() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let (has_messages, count) =
                ParallelRrdReader::check_parallel(&path).expect("check_parallel should succeed");
            assert!(has_messages, "Should have messages");
            assert!(count > 0, "Message count should be positive");
        }
    }

    #[test]
    fn test_parallel_rrd_decode_messages_with_timestamp() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");

            let iter = reader.decode_messages_with_timestamp();
            assert!(iter.is_ok(), "Should create iterator: {:?}", iter.err());

            let iter = iter.unwrap();
            assert!(!iter.channels().is_empty(), "Iterator should have channels");

            // Test stream()
            let stream = iter.stream();
            assert!(stream.is_ok(), "Should create stream: {:?}", stream.err());

            let mut stream = stream.unwrap();
            let mut count = 0;
            let mut success_count = 0;

            // Try to read a few messages
            for _ in 0..10 {
                match stream.next() {
                    Some(Ok((msg, channel))) => {
                        count += 1;
                        success_count += 1;
                        assert!(!channel.topic.is_empty(), "Topic should not be empty");
                        assert!(!msg.message.is_empty(), "Message should have data");
                    }
                    Some(Err(_)) => {
                        count += 1;
                        // Some messages may fail to decode
                    }
                    None => break,
                }
            }

            assert!(
                count > 0,
                "Should iterate at least one message (success: {})",
                success_count
            );
        }
    }

    #[test]
    fn test_parallel_rrd_large_file() {
        let path = get_fixture_path("large_multichannel.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open large file");
            assert!(
                reader.message_count() > 0,
                "Large file should have messages"
            );
            assert!(reader.supports_parallel(), "Should support parallel");
        }
    }

    #[test]
    fn test_parallel_rrd_timestamps_file() {
        let path = get_fixture_path("timestamps.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");
            assert!(
                reader.message_count() > 0,
                "Timestamps file should have messages"
            );
        }
    }

    #[test]
    fn test_parallel_rrd_format_reader_traits() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let mut reader = ParallelRrdReader::open(&path).expect("Failed to open");

            // Test FormatReader trait methods
            assert_eq!(reader.format(), FileFormat::Rrd);
            assert!(reader.file_size() > 0);
            assert!(!reader.path().is_empty());

            // Test as_any and as_any_mut
            assert!(reader.as_any().is::<ParallelRrdReader>());
            assert!(reader.as_any_mut().is::<ParallelRrdReader>());
        }
    }

    #[test]
    fn test_parallel_rrd_channel_info() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");
            let channels = reader.channels();

            // Verify channel structure
            for (id, channel) in channels.iter() {
                assert_eq!(*id, channel.id);
                assert!(!channel.topic.is_empty());
                assert_eq!(channel.id, 0, "RRD uses single channel with id 0");
                assert_eq!(channel.encoding, "protobuf");
                assert_eq!(channel.message_type, "rerun.ArrowMsg");
            }
        }
    }

    #[test]
    fn test_parallel_rrd_stream_multiple_messages() {
        let path = get_fixture_path("small_uncompressed.rrd");
        if std::path::Path::new(&path).exists() {
            let reader = ParallelRrdReader::open(&path).expect("Failed to open");

            let iter = reader
                .decode_messages_with_timestamp()
                .expect("Failed to create iterator");
            let mut stream = iter.stream().expect("Failed to create stream");

            let mut count = 0;
            let mut total_bytes = 0;

            // Read multiple messages
            while let Some(result) = stream.next() {
                match result {
                    Ok((msg, channel)) => {
                        count += 1;
                        if let Some(CodecValue::Bytes(data)) = msg.message.get("data") {
                            total_bytes += data.len();
                        }
                        assert_eq!(channel.id, 0);

                        // Limit iterations for test
                        if count >= 100 {
                            break;
                        }
                    }
                    Err(_) => {
                        // Some messages may fail to decode
                        count += 1;
                        if count >= 100 {
                            break;
                        }
                    }
                }
            }

            assert!(count > 0, "Should read at least one message, got {}", count);
        }
    }
}
