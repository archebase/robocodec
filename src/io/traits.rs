// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core traits for unified I/O operations.
//!
//! This module defines the foundational traits that all format-specific
//! readers and writers must implement. These traits provide a consistent
//! API across all supported formats (MCAP, ROS1 bag, etc.).

use std::any::Any;
use std::collections::HashMap;

use crate::{CodecError, Result};

use super::metadata::{ChannelInfo, FileInfo, RawMessage, TimestampedDecodedMessage};

// Re-export filter types
use super::filter::TopicFilter;

/// Trait for iterating over decoded messages with timestamps.
///
/// This trait abstracts over format-specific iterator implementations,
/// allowing unified iteration via trait objects.
pub trait DecodedMessageIterator:
    Iterator<Item = Result<(TimestampedDecodedMessage, ChannelInfo)>>
{
    /// Convert to a boxed trait object.
    fn into_boxed(self) -> Box<dyn DecodedMessageIterator + Send + Sync>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Box::new(self)
    }
}

// Implement for any type that matches the required bounds
impl<T> DecodedMessageIterator for T where
    T: Iterator<Item = Result<(TimestampedDecodedMessage, ChannelInfo)>> + Send + Sync
{
}

/// Trait for reading robotics data from different file formats.
///
/// This trait abstracts over format-specific readers to provide a unified API.
/// All readers must implement this trait to be compatible with the unified I/O layer.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::traits::FormatReader;
///
/// fn process_reader(reader: &dyn FormatReader) {
///     println!("Channels: {}", reader.channels().len());
///     println!("Messages: {}", reader.message_count());
/// }
/// ```
pub trait FormatReader: Send + Sync {
    /// Open a reader from any transport source.
    ///
    /// This method enables format readers to work with any data source
    /// (local files, S3, HTTP, etc.) through the unified Transport abstraction.
    ///
    /// Only available when the `remote` feature is enabled.
    ///
    /// # Arguments
    ///
    /// * `transport` - Boxed transport trait object for reading data
    /// * `path` - Path or URL string for reporting (used for metadata)
    ///
    /// # Returns
    ///
    /// A format-specific reader instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The transport cannot be read
    /// - The data is not a valid file for this format
    /// - Required metadata cannot be extracted
    #[cfg(feature = "remote")]
    fn open_from_transport(
        transport: Box<dyn crate::io::transport::Transport>,
        path: String,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Get all channel information.
    ///
    /// Returns a map of channel ID to channel info.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// let channels = reader.channels();
    /// for (id, channel) in channels {
    ///     println!("Channel {}: {} ({})", id, channel.topic, channel.message_type);
    /// }
    /// # }
    /// ```
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;

    /// Get channel info by topic name.
    ///
    /// Returns the first matching channel. In ROS1 bag files, multiple
    /// connections can have the same topic name with different callerids.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// if let Some(channel) = reader.channel_by_topic("/chatter") {
    ///     println!("Found topic: {}", channel.topic);
    ///     println!("Message type: {}", channel.message_type);
    /// }
    /// # }
    /// ```
    fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.channels().values().find(|c| c.topic == topic)
    }

    /// Get all channels with the given topic name.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// let channels = reader.channels_by_topic("/chatter");
    /// for channel in channels {
    ///     println!("Channel {}: {}", channel.id, channel.topic);
    /// }
    /// # }
    /// ```
    fn channels_by_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        self.channels()
            .values()
            .filter(|c| c.topic == topic)
            .collect()
    }

    /// Get the total message count.
    ///
    /// Returns 0 if the count is unknown (e.g., for files without summary).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// let count = reader.message_count();
    /// if count > 0 {
    ///     println!("File contains {} messages", count);
    /// } else {
    ///     println!("Message count unknown (no summary section)");
    /// }
    /// # }
    /// ```
    fn message_count(&self) -> u64;

    /// Get the start timestamp in nanoseconds.
    ///
    /// Returns `None` if no timestamp information is available.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// if let Some(start) = reader.start_time() {
    ///     println!("Start time: {} ns", start);
    /// }
    /// # }
    /// ```
    fn start_time(&self) -> Option<u64>;

    /// Get the end timestamp in nanoseconds.
    ///
    /// Returns `None` if no timestamp information is available.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// if let Some(end) = reader.end_time() {
    ///     println!("End time: {} ns", end);
    /// }
    /// # }
    /// ```
    fn end_time(&self) -> Option<u64>;

    /// Get the file path.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// println!("Reading from: {}", reader.path());
    /// # }
    /// ```
    fn path(&self) -> &str;

    /// Get file information metadata.
    ///
    /// Returns a `FileInfo` struct containing all file metadata in a single
    /// convenient structure.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// let info = reader.file_info();
    /// println!("Format: {:?}", info.format);
    /// println!("Size: {} bytes", info.size);
    /// println!("Channels: {}", info.channels.len());
    /// println!("Messages: {}", info.message_count);
    /// # }
    /// ```
    #[must_use]
    fn file_info(&self) -> FileInfo {
        FileInfo {
            path: self.path().to_string(),
            format: self.format(),
            size: self.file_size(),
            channels: self.channels().clone(),
            message_count: self.message_count(),
            start_time: self.start_time().unwrap_or(0),
            end_time: self.end_time().unwrap_or(0),
            duration: self.duration(),
        }
    }

    /// Get the file format.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # use robocodec::io::metadata::FileFormat;
    /// # fn test(reader: &dyn FormatReader) {
    /// match reader.format() {
    ///     FileFormat::Mcap => println!("MCAP format"),
    ///     FileFormat::Bag => println!("ROS1 Bag format"),
    ///     FileFormat::Rrd => println!("RRD format"),
    ///     FileFormat::Unknown => println!("Unknown format"),
    /// }
    /// # }
    /// ```
    fn format(&self) -> crate::io::metadata::FileFormat;

    /// Get the file size in bytes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// let size = reader.file_size();
    /// println!("File size: {} bytes", size);
    /// # }
    /// ```
    fn file_size(&self) -> u64;

    /// Get the duration in nanoseconds.
    ///
    /// Calculates the duration as `end_time - start_time`. Returns 0 if
    /// either timestamp is missing or if `end_time` is not greater than `start_time`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatReader;
    /// # fn test(reader: &dyn FormatReader) {
    /// let duration_ns = reader.duration();
    /// let duration_sec = duration_ns as f64 / 1_000_000_000.0;
    /// println!("Duration: {:.2} seconds", duration_sec);
    /// # }
    /// ```
    #[must_use]
    fn duration(&self) -> u64 {
        match (self.start_time(), self.end_time()) {
            (Some(s), Some(e)) if e > s => e - s,
            _ => 0,
        }
    }

    /// Create a boxed iterator over decoded messages with timestamps.
    ///
    /// This method provides a trait-based alternative to downcasting,
    /// allowing format readers to provide decoded messages with timestamps
    /// without exposing concrete types.
    ///
    /// The default implementation returns an error, indicating that the
    /// format reader does not support this operation. Format-specific
    /// readers should override this method to provide their implementation.
    ///
    /// # Returns
    ///
    /// A boxed iterator yielding `(TimestampedDecodedMessage, ChannelInfo)` tuples.
    ///
    /// # Errors
    ///
    /// Returns an error if the format reader does not support decoded iteration.
    #[allow(unused_variables)]
    fn decoded_with_timestamp_boxed(
        &self,
    ) -> Result<Box<dyn DecodedMessageIterator + Send + Sync + '_>> {
        Err(CodecError::unsupported(
            "decoded_with_timestamp_boxed() not supported for this format reader",
        ))
    }

    /// Downcast to `Any` for accessing format-specific functionality.
    fn as_any(&self) -> &dyn Any;

    /// Downcast mutably to `Any` for accessing format-specific functionality.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Trait for writing robotics data to different file formats.
///
/// This trait abstracts over format-specific writers to provide a unified API.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::traits::FormatWriter;
/// use robocodec::io::metadata::RawMessage;
///
/// fn write_messages<W: FormatWriter>(writer: &mut W, messages: &[RawMessage]) {
///     for msg in messages {
///         writer.write(msg).unwrap();
///     }
///     writer.finish().unwrap();
/// }
/// ```
pub trait FormatWriter: Send {
    /// Get the output file path.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # fn test(writer: &dyn FormatWriter) {
    /// println!("Writing to: {}", writer.path());
    /// # }
    /// ```
    fn path(&self) -> &str;

    /// Add a channel/topic to the file.
    ///
    /// Returns the assigned channel ID.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name (e.g., "/chatter", "/odom")
    /// * `message_type` - Message type name (e.g., "`std_msgs/String`")
    /// * `encoding` - Message encoding (e.g., "cdr", "protobuf")
    /// * `schema` - Optional schema definition
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # fn test(writer: &mut dyn FormatWriter) -> Result<(), Box<dyn std::error::Error>> {
    /// let channel_id = writer.add_channel(
    ///     "/chatter",
    ///     "std_msgs/String",
    ///     "cdr",
    ///     Some("string data")
    /// )?;
    /// println!("Added channel with ID: {}", channel_id);
    /// # Ok(())
    /// # }
    /// ```
    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16>;

    /// Write a raw message to the file.
    ///
    /// The message must reference a channel that was previously added
    /// via `add_channel`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # use robocodec::io::metadata::RawMessage;
    /// # fn test(writer: &mut dyn FormatWriter) -> Result<(), Box<dyn std::error::Error>> {
    /// let message = RawMessage {
    ///     channel_id: 0,
    ///     log_time: 1000,
    ///     publish_time: 1000,
    ///     data: vec![1, 2, 3, 4],
    ///     sequence: None,
    /// };
    /// writer.write(&message)?;
    /// # Ok(())
    /// # }
    /// ```
    fn write(&mut self, message: &RawMessage) -> Result<()>;

    /// Write multiple messages in batch.
    ///
    /// Default implementation calls `write` for each message.
    /// Format-specific implementations may override this for better performance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # use robocodec::io::metadata::RawMessage;
    /// # fn test(writer: &mut dyn FormatWriter, messages: &[RawMessage]) -> Result<(), Box<dyn std::error::Error>> {
    /// writer.write_batch(messages)?;
    /// println!("Wrote {} messages", messages.len());
    /// # Ok(())
    /// # }
    /// ```
    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        for msg in messages {
            self.write(msg)?;
        }
        Ok(())
    }

    /// Finalize and close the file.
    ///
    /// This must be called to ensure all data is flushed and the
    /// file is properly closed with necessary footer sections.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # fn test(writer: &mut dyn FormatWriter) -> Result<(), Box<dyn std::error::Error>> {
    /// // Write all messages...
    /// // Finalize the file
    /// writer.finish()?;
    /// println!("File written successfully");
    /// # Ok(())
    /// # }
    /// ```
    fn finish(&mut self) -> Result<()>;

    /// Get the number of messages written so far.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # fn test(writer: &dyn FormatWriter) {
    /// println!("Messages written: {}", writer.message_count());
    /// # }
    /// ```
    fn message_count(&self) -> u64;

    /// Get the number of channels added so far.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::FormatWriter;
    /// # fn test(writer: &dyn FormatWriter) {
    /// println!("Channels added: {}", writer.channel_count());
    /// # }
    /// ```
    fn channel_count(&self) -> usize;

    /// Downcast to `Any` for accessing format-specific functionality.
    fn as_any(&self) -> &dyn Any;

    /// Downcast mutably to `Any` for accessing format-specific functionality.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Configuration for parallel reading.
#[derive(Debug, Clone)]
pub struct ParallelReaderConfig {
    /// Number of worker threads (None = auto-detect CPU count)
    pub num_threads: Option<usize>,
    /// Topic/channel filter (None = read all topics)
    pub topic_filter: Option<TopicFilter>,
    /// Backpressure via bounded channel capacity
    pub channel_capacity: Option<usize>,
    /// Progress reporting interval (number of chunks between updates)
    pub progress_interval: usize,
    /// Enable merging of small chunks into larger ones.
    /// This reduces compression overhead and improves throughput,
    /// especially for files with many small chunks (e.g., BAG files).
    /// Default: true.
    pub merge_enabled: bool,
    /// Target size for merged chunks in bytes.
    /// Only used when `merge_enabled` is true.
    /// Default: 16MB.
    pub merge_target_size: usize,
}

impl Default for ParallelReaderConfig {
    fn default() -> Self {
        Self {
            num_threads: None,
            topic_filter: None,
            channel_capacity: Some(32),
            progress_interval: 10,
            merge_enabled: true,
            merge_target_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

impl ParallelReaderConfig {
    /// Set the number of worker threads.
    #[must_use]
    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }

    /// Set the topic filter.
    #[must_use]
    pub fn with_topic_filter(mut self, filter: TopicFilter) -> Self {
        self.topic_filter = Some(filter);
        self
    }

    /// Set the channel capacity for backpressure.
    #[must_use]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = Some(capacity);
        self
    }

    /// Set the progress reporting interval.
    #[must_use]
    pub fn with_progress_interval(mut self, interval: usize) -> Self {
        self.progress_interval = interval;
        self
    }

    /// Set whether chunk merging is enabled.
    ///
    /// When enabled, small chunks are merged into larger chunks to reduce
    /// compression overhead and improve throughput.
    #[must_use]
    pub fn with_merge_enabled(mut self, enabled: bool) -> Self {
        self.merge_enabled = enabled;
        self
    }

    /// Set the target size for merged chunks in bytes.
    ///
    /// Only used when `merge_enabled` is true. Chunks will be merged
    /// until they reach approximately this size.
    #[must_use]
    pub fn with_merge_target_size(mut self, size: usize) -> Self {
        self.merge_target_size = size;
        self
    }
}

/// Statistics from parallel reading.
#[derive(Debug, Clone)]
pub struct ParallelReaderStats {
    /// Total messages read
    pub messages_read: u64,
    /// Number of chunks processed
    pub chunks_processed: usize,
    /// Total data bytes processed
    pub total_bytes: u64,
    /// Time spent reading chunks (seconds)
    pub read_time_sec: f64,
    /// Time spent decompressing (seconds)
    pub decompress_time_sec: f64,
    /// Time spent deserializing messages (seconds)
    pub deserialize_time_sec: f64,
    /// Total time for parallel read (seconds)
    pub total_time_sec: f64,
}

impl Default for ParallelReaderStats {
    fn default() -> Self {
        Self {
            messages_read: 0,
            chunks_processed: 0,
            total_bytes: 0,
            read_time_sec: 0.0,
            decompress_time_sec: 0.0,
            deserialize_time_sec: 0.0,
            total_time_sec: 0.0,
        }
    }
}

/// A message chunk with raw message data.
///
/// This type is used to pass messages from parallel readers to the pipeline.
/// It contains all messages from a single file chunk, along with metadata.
#[derive(Debug)]
pub struct MessageChunkData {
    /// Chunk sequence number
    pub sequence: u64,
    /// Messages in this chunk
    pub messages: Vec<RawMessage>,
    /// Message start time (earliest `log_time` in chunk)
    pub message_start_time: u64,
    /// Message end time (latest `log_time` in chunk)
    pub message_end_time: u64,
}

impl MessageChunkData {
    /// Create a new empty message chunk.
    #[must_use]
    pub fn new(sequence: u64) -> Self {
        Self {
            sequence,
            messages: Vec::new(),
            message_start_time: u64::MAX,
            message_end_time: 0,
        }
    }

    /// Add a message to this chunk.
    pub fn add_message(&mut self, msg: RawMessage) {
        self.message_start_time = self.message_start_time.min(msg.log_time);
        self.message_end_time = self.message_end_time.max(msg.log_time);
        self.messages.push(msg);
    }

    /// Get the number of messages in this chunk.
    #[must_use]
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Check if this chunk is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Get the total size of all message data in this chunk.
    #[must_use]
    pub fn total_data_size(&self) -> usize {
        self.messages.iter().map(|m| m.data.len()).sum()
    }
}

/// Parallel reader capability for high-performance chunk-based reading.
///
/// This trait extends `FormatReader` with parallel reading capabilities for
/// formats that support chunk-based access (MCAP, ROS1 bag, etc.).
///
/// # Two-Phase Pattern
///
/// All parallel readers follow a two-phase pattern:
/// 1. **Discovery Phase** (Sequential): Read metadata to enable parallel access
///    - MCAP with summary: Read summary section at end of file
///    - MCAP without summary: Scan file to build chunk index (>1GB only)
///    - BAG: Read chunk info records from index section
/// 2. **Processing Phase** (Parallel): Process chunks concurrently
///    - Use Rayon thread pool to decompress and parse chunks
///    - Send results through crossbeam channel
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::io::traits::ParallelReader;
/// use crossbeam_channel::bounded;
///
/// // Assume you have a reader that implements ParallelReader
/// // let reader = ...;
/// // let (sender, receiver) = bounded(32);
/// //
/// // std::thread::spawn(move || {
/// //     let config = robocodec::io::traits::ParallelReaderConfig::default();
/// //     let stats = reader.read_parallel(config, sender).unwrap();
/// //     println!("Read {} messages", stats.messages_read);
/// // });
/// //
/// // for chunk in receiver {
/// //     // Process chunk...
/// // }
/// # Ok(())
/// # }
/// ```
pub trait ParallelReader: FormatReader {
    /// Read chunks in parallel and send to output channel.
    ///
    /// This method processes chunks concurrently using a Rayon thread pool
    /// and sends `MessageChunkData` objects through the provided channel. The channel
    /// provides backpressure to prevent memory overload.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for parallel reading (threads, filtering, etc.)
    /// * `sender` - Crossbeam channel for sending processed chunks
    ///
    /// # Returns
    ///
    /// Statistics about the parallel read operation (message count, timing, etc.)
    fn read_parallel(
        &self,
        config: ParallelReaderConfig,
        sender: crossbeam_channel::Sender<MessageChunkData>,
    ) -> Result<ParallelReaderStats>;

    /// Get the number of chunks in the file.
    ///
    /// Returns 0 if the file doesn't support chunk-based reading.
    fn chunk_count(&self) -> usize;

    /// Check if this file can be read in parallel.
    ///
    /// Returns true if the file has the necessary metadata for parallel access
    /// (summary section, chunk info records, etc.).
    fn supports_parallel(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_by_topic() {
        let mut channels = HashMap::new();
        channels.insert(1, ChannelInfo::new(1, "/ch1", "type1"));
        channels.insert(2, ChannelInfo::new(2, "/ch2", "type2"));
        channels.insert(3, ChannelInfo::new(3, "/ch1", "type3")); // Same topic

        struct TestReader {
            channels: HashMap<u16, ChannelInfo>,
        }

        impl FormatReader for TestReader {
            #[cfg(feature = "remote")]
            fn open_from_transport(
                _transport: Box<dyn crate::io::transport::Transport>,
                _path: String,
            ) -> Result<Self>
            where
                Self: Sized,
            {
                Ok(Self {
                    channels: HashMap::new(),
                })
            }

            fn channels(&self) -> &HashMap<u16, ChannelInfo> {
                &self.channels
            }

            fn message_count(&self) -> u64 {
                0
            }

            fn start_time(&self) -> Option<u64> {
                None
            }

            fn end_time(&self) -> Option<u64> {
                None
            }

            fn path(&self) -> &str {
                "test"
            }

            fn format(&self) -> crate::io::metadata::FileFormat {
                crate::io::metadata::FileFormat::Unknown
            }

            fn file_size(&self) -> u64 {
                0
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        let reader = TestReader { channels };

        assert!(reader.channel_by_topic("/ch1").is_some());
        assert!(reader.channel_by_topic("/ch2").is_some());
        assert!(reader.channel_by_topic("/ch3").is_none());

        let ch1_channels = reader.channels_by_topic("/ch1");
        assert_eq!(ch1_channels.len(), 2);
    }

    #[test]
    fn test_parallel_reader_config_default() {
        let config = ParallelReaderConfig::default();
        assert_eq!(config.num_threads, None);
        assert!(config.topic_filter.is_none());
        assert_eq!(config.channel_capacity, Some(32));
        assert_eq!(config.progress_interval, 10);
        assert!(config.merge_enabled);
        assert_eq!(config.merge_target_size, 16 * 1024 * 1024);
    }

    #[test]
    fn test_parallel_reader_config_builders() {
        let filter = TopicFilter::include(vec!["/test".to_string()]);

        let config = ParallelReaderConfig::default()
            .with_threads(4)
            .with_topic_filter(filter)
            .with_channel_capacity(64)
            .with_progress_interval(20)
            .with_merge_enabled(false)
            .with_merge_target_size(8 * 1024 * 1024);

        assert_eq!(config.num_threads, Some(4));
        assert_eq!(config.channel_capacity, Some(64));
        assert_eq!(config.progress_interval, 20);
        assert!(!config.merge_enabled);
        assert_eq!(config.merge_target_size, 8 * 1024 * 1024);
    }

    #[test]
    fn test_parallel_reader_stats_default() {
        let stats = ParallelReaderStats::default();
        assert_eq!(stats.messages_read, 0);
        assert_eq!(stats.chunks_processed, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.read_time_sec, 0.0);
        assert_eq!(stats.decompress_time_sec, 0.0);
        assert_eq!(stats.deserialize_time_sec, 0.0);
        assert_eq!(stats.total_time_sec, 0.0);
    }

    #[test]
    fn test_message_chunk_data_new() {
        let chunk = MessageChunkData::new(42);
        assert_eq!(chunk.sequence, 42);
        assert!(chunk.is_empty());
        assert_eq!(chunk.message_count(), 0);
        assert_eq!(chunk.total_data_size(), 0);
        assert_eq!(chunk.message_start_time, u64::MAX);
        assert_eq!(chunk.message_end_time, 0);
    }

    #[test]
    fn test_message_chunk_data_add_message() {
        let mut chunk = MessageChunkData::new(1);

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3, 4],
            sequence: None,
        };

        chunk.add_message(msg.clone());
        assert!(!chunk.is_empty());
        assert_eq!(chunk.message_count(), 1);
        assert_eq!(chunk.total_data_size(), 4);
        assert_eq!(chunk.message_start_time, 1000);
        assert_eq!(chunk.message_end_time, 1000);

        // Add another message with different timestamps
        let msg2 = RawMessage {
            channel_id: 0,
            log_time: 2000,
            publish_time: 2000,
            data: vec![5, 6],
            sequence: None,
        };
        chunk.add_message(msg2);
        assert_eq!(chunk.message_count(), 2);
        assert_eq!(chunk.total_data_size(), 6);
        assert_eq!(chunk.message_start_time, 1000);
        assert_eq!(chunk.message_end_time, 2000);
    }

    #[test]
    fn test_message_chunk_data_multiple_messages() {
        let mut chunk = MessageChunkData::new(1);

        for i in 0..5 {
            chunk.add_message(RawMessage {
                channel_id: 0,
                log_time: i * 1000,
                publish_time: i * 1000,
                data: vec![i as u8],
                sequence: None,
            });
        }

        assert_eq!(chunk.message_count(), 5);
        assert_eq!(chunk.total_data_size(), 5);
        assert_eq!(chunk.message_start_time, 0);
        assert_eq!(chunk.message_end_time, 4000);
    }

    #[test]
    fn test_file_info() {
        let mut channels = HashMap::new();
        channels.insert(1, ChannelInfo::new(1, "/test", "std_msgs/String"));

        struct TestReader {
            channels: HashMap<u16, ChannelInfo>,
        }

        impl FormatReader for TestReader {
            #[cfg(feature = "remote")]
            fn open_from_transport(
                _transport: Box<dyn crate::io::transport::Transport>,
                _path: String,
            ) -> Result<Self>
            where
                Self: Sized,
            {
                Ok(Self {
                    channels: HashMap::new(),
                })
            }

            fn channels(&self) -> &HashMap<u16, ChannelInfo> {
                &self.channels
            }

            fn message_count(&self) -> u64 {
                100
            }

            fn start_time(&self) -> Option<u64> {
                Some(1000)
            }

            fn end_time(&self) -> Option<u64> {
                Some(5000)
            }

            fn path(&self) -> &str {
                "test.mcap"
            }

            fn format(&self) -> crate::io::metadata::FileFormat {
                crate::io::metadata::FileFormat::Mcap
            }

            fn file_size(&self) -> u64 {
                10000
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        let reader = TestReader { channels };
        let info = reader.file_info();

        assert_eq!(info.path, "test.mcap");
        assert_eq!(info.message_count, 100);
        assert_eq!(info.start_time, 1000);
        assert_eq!(info.end_time, 5000);
        assert_eq!(info.duration, 4000);
        assert_eq!(info.size, 10000);
    }

    #[test]
    fn test_duration_no_times() {
        let empty_channels = HashMap::new();

        struct TestReader {
            _channels: HashMap<u16, ChannelInfo>,
        }

        impl FormatReader for TestReader {
            #[cfg(feature = "remote")]
            fn open_from_transport(
                _transport: Box<dyn crate::io::transport::Transport>,
                _path: String,
            ) -> Result<Self>
            where
                Self: Sized,
            {
                Ok(Self {
                    _channels: HashMap::new(),
                })
            }

            fn channels(&self) -> &HashMap<u16, ChannelInfo> {
                &self._channels
            }

            fn message_count(&self) -> u64 {
                0
            }

            fn start_time(&self) -> Option<u64> {
                None
            }

            fn end_time(&self) -> Option<u64> {
                None
            }

            fn path(&self) -> &str {
                "test"
            }

            fn format(&self) -> crate::io::metadata::FileFormat {
                crate::io::metadata::FileFormat::Unknown
            }

            fn file_size(&self) -> u64 {
                0
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        let reader = TestReader {
            _channels: empty_channels,
        };
        assert_eq!(reader.duration(), 0);
    }

    #[test]
    fn test_duration_equal_times() {
        let empty_channels = HashMap::new();

        struct TestReader {
            _channels: HashMap<u16, ChannelInfo>,
        }

        impl FormatReader for TestReader {
            #[cfg(feature = "remote")]
            fn open_from_transport(
                _transport: Box<dyn crate::io::transport::Transport>,
                _path: String,
            ) -> Result<Self>
            where
                Self: Sized,
            {
                Ok(Self {
                    _channels: HashMap::new(),
                })
            }

            fn channels(&self) -> &HashMap<u16, ChannelInfo> {
                &self._channels
            }

            fn message_count(&self) -> u64 {
                0
            }

            fn start_time(&self) -> Option<u64> {
                Some(1000)
            }

            fn end_time(&self) -> Option<u64> {
                Some(1000)
            }

            fn path(&self) -> &str {
                "test"
            }

            fn format(&self) -> crate::io::metadata::FileFormat {
                crate::io::metadata::FileFormat::Unknown
            }

            fn file_size(&self) -> u64 {
                0
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        let reader = TestReader {
            _channels: empty_channels,
        };
        assert_eq!(reader.duration(), 0);
    }

    #[test]
    fn test_format_writer_write_batch_default() {
        struct TestWriter {
            messages: Vec<RawMessage>,
        }

        impl FormatWriter for TestWriter {
            fn path(&self) -> &str {
                "test"
            }

            fn add_channel(
                &mut self,
                _topic: &str,
                _message_type: &str,
                _encoding: &str,
                _schema: Option<&str>,
            ) -> Result<u16> {
                Ok(0)
            }

            fn write(&mut self, message: &RawMessage) -> Result<()> {
                self.messages.push(message.clone());
                Ok(())
            }

            fn message_count(&self) -> u64 {
                self.messages.len() as u64
            }

            fn channel_count(&self) -> usize {
                0
            }

            fn finish(&mut self) -> Result<()> {
                Ok(())
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }
        }

        let mut writer = TestWriter {
            messages: Vec::new(),
        };

        let msgs = vec![
            RawMessage {
                channel_id: 0,
                log_time: 0,
                publish_time: 0,
                data: vec![1],
                sequence: None,
            },
            RawMessage {
                channel_id: 0,
                log_time: 1,
                publish_time: 1,
                data: vec![2],
                sequence: None,
            },
        ];

        writer.write_batch(&msgs).unwrap();
        assert_eq!(writer.message_count(), 2);
    }
}
