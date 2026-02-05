// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified reader with automatic format detection.
//!
//! This module provides a high-level reader that automatically detects
//! the file format (MCAP or ROS1 bag) and provides a unified API for
//! reading messages.
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::RoboReader;
//! use robocodec::io::FormatReader;
//!
//! // Open with automatic format detection
//! let reader = RoboReader::open("data.mcap")?;
//!
//! // Iterate over decoded messages with metadata
//! for result in reader.decoded()? {
//!     let decoded = result?;
//!     println!("Topic: {}", decoded.topic());
//!     println!("Data: {:?}", decoded.message);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod config;

pub use config::{ReaderConfig, ReaderConfigBuilder};

use crate::io::detection::detect_format;
use crate::io::formats::bag::BagFormat;
use crate::io::formats::mcap::McapFormat;
use crate::io::formats::mcap::reader::DecodedMessageWithTimestampStream as McapTimestampedStream;
use crate::io::formats::rrd::RrdFormat;
use crate::io::metadata::{ChannelInfo, DecodedMessageResult, FileFormat};
use crate::io::traits::{FormatReader, ParallelReader};
use crate::{CodecError, Result};

/// Get or create a shared Tokio runtime for blocking async operations.
///
/// This reuses a single runtime across all S3 operations, avoiding
/// the overhead of creating a new runtime for each open/write.
#[cfg(feature = "s3")]
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"))
}

enum DecodedMessageIterInner<'a> {
    Mcap(McapTimestampedStream<'a>),
    Bag(crate::io::formats::bag::BagDecodedMessageWithTimestampStream<'a>),
    Rrd(crate::io::formats::rrd::DecodedMessageWithTimestampStream<'a>),
    ParallelRrd(crate::io::formats::rrd::parallel::RrdDecodedMessageWithTimestampStream<'a>),
}

/// Unified decoded message iterator.
///
/// This iterator works across both MCAP and ROS1 bag formats,
/// providing a consistent interface for iterating over decoded messages.
/// Timestamps are populated when available from the underlying format.
///
/// # Example
///
/// ```rust,no_run
/// # use robocodec::io::RoboReader;
/// # fn test() -> Result<(), Box<dyn std::error::Error>> {
/// let reader = RoboReader::open("data.mcap")?;
/// for result in reader.decoded()? {
///     let decoded = result?;
///     println!("Topic: {}", decoded.topic());
///     if decoded.has_timestamps() {
///         println!("Log time: {:?}", decoded.log_time);
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct DecodedMessageIter<'a> {
    inner: DecodedMessageIterInner<'a>,
}

// Import alias for cleaner code
use DecodedMessageIterInner as Inner;

impl<'a> Iterator for DecodedMessageIter<'a> {
    type Item = Result<DecodedMessageResult>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            Inner::Mcap(stream) => stream.next().map(|result| {
                result.map(|(msg, ch)| {
                    let ch_info = ChannelInfo {
                        id: ch.id,
                        topic: ch.topic.clone(),
                        message_type: ch.message_type.clone(),
                        encoding: ch.encoding.clone(),
                        schema: ch.schema.clone(),
                        schema_data: ch.schema_data.clone(),
                        schema_encoding: ch.schema_encoding.clone(),
                        message_count: ch.message_count,
                        callerid: ch.callerid.clone(),
                    };
                    DecodedMessageResult {
                        message: msg.message,
                        channel: ch_info,
                        log_time: Some(msg.log_time),
                        publish_time: Some(msg.publish_time),
                        sequence: None,
                    }
                })
            }),
            Inner::Bag(stream) => stream.next().map(|result| {
                result.map(|(msg, ch)| {
                    let ch_info = ChannelInfo {
                        id: ch.id,
                        topic: ch.topic.clone(),
                        message_type: ch.message_type.clone(),
                        encoding: ch.encoding.clone(),
                        schema: ch.schema.clone(),
                        schema_data: ch.schema_data.clone(),
                        schema_encoding: ch.schema_encoding.clone(),
                        message_count: ch.message_count,
                        callerid: ch.callerid.clone(),
                    };
                    DecodedMessageResult {
                        message: msg.message,
                        channel: ch_info,
                        log_time: Some(msg.log_time),
                        publish_time: Some(msg.publish_time),
                        sequence: None,
                    }
                })
            }),
            Inner::Rrd(stream) => stream.next().map(|result| {
                result.map(|(msg, ch)| {
                    let ch_info = ChannelInfo {
                        id: ch.id,
                        topic: ch.topic.clone(),
                        message_type: ch.message_type.clone(),
                        encoding: ch.encoding.clone(),
                        schema: ch.schema.clone(),
                        schema_data: ch.schema_data.clone(),
                        schema_encoding: ch.schema_encoding.clone(),
                        message_count: ch.message_count,
                        callerid: ch.callerid.clone(),
                    };
                    DecodedMessageResult {
                        message: msg.message,
                        channel: ch_info,
                        log_time: Some(msg.log_time),
                        publish_time: Some(msg.publish_time),
                        sequence: None,
                    }
                })
            }),
            Inner::ParallelRrd(stream) => stream.next().map(|result| {
                result.map(|(msg, ch)| {
                    let ch_info = ChannelInfo {
                        id: ch.id,
                        topic: ch.topic.clone(),
                        message_type: ch.message_type.clone(),
                        encoding: ch.encoding.clone(),
                        schema: ch.schema.clone(),
                        schema_data: ch.schema_data.clone(),
                        schema_encoding: ch.schema_encoding.clone(),
                        message_count: ch.message_count,
                        callerid: ch.callerid.clone(),
                    };
                    DecodedMessageResult {
                        message: msg.message,
                        channel: ch_info,
                        log_time: Some(msg.log_time),
                        publish_time: Some(msg.publish_time),
                        sequence: None,
                    }
                })
            }),
        }
    }
}

/// Unified reader for robotics data files.
///
/// Automatically detects format (MCAP or ROS1 bag) and provides
/// a consistent API for reading messages and metadata.
pub struct RoboReader {
    /// The inner format-specific reader
    inner: Box<dyn FormatReader>,
}

impl RoboReader {
    /// Open a file with automatic format detection and default configuration.
    ///
    /// Supports both local file paths and S3 URLs (s3://bucket/key).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open, or S3 URL (s3://bucket/key)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::RoboReader;
    ///
    /// // Local file
    /// let reader = RoboReader::open("data.mcap")?;
    ///
    /// // S3 object (requires tokio runtime)
    /// let reader = RoboReader::open("s3://my-bucket/path/to/data.mcap")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open(path: &str) -> Result<Self> {
        Self::open_with_config(path, ReaderConfig::default())
    }

    /// Open a file with the specified configuration.
    ///
    /// Supports both local file paths and S3 URLs (s3://bucket/key).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open, or S3 URL (s3://bucket/key)
    /// * `config` - Reader configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{RoboReader, ReaderConfig};
    ///
    /// let reader = RoboReader::open_with_config(
    ///     "data.mcap",
    ///     ReaderConfig::default()
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open_with_config(path: &str, config: ReaderConfig) -> Result<Self> {
        let _ = config; // Config reserved for future use

        // Check if this is an S3 URL
        #[cfg(feature = "s3")]
        {
            if let Ok(location) = crate::io::s3::S3Location::from_s3_url(path) {
                // Use S3Reader for s3:// URLs
                let rt = shared_runtime();
                let reader =
                    rt.block_on(async { crate::io::s3::S3Reader::open(location).await })?;
                return Ok(Self {
                    inner: Box::new(reader),
                });
            }
        }

        // Fall back to local file path
        let path_obj = std::path::Path::new(path);

        if !path_obj.exists() {
            return Err(CodecError::parse(
                "RoboReader",
                format!("File not found: {}", path),
            ));
        }

        let format = detect_format(path_obj)?;

        let inner: Box<dyn FormatReader> = match format {
            FileFormat::Mcap => Box::new(McapFormat::open(path_obj)?),
            FileFormat::Bag => Box::new(BagFormat::open(path_obj)?),
            FileFormat::Rrd => Box::new(RrdFormat::open(path_obj)?),
            FileFormat::Unknown => {
                return Err(CodecError::parse(
                    "RoboReader",
                    format!("Unknown file format: {}", path),
                ));
            }
        };

        Ok(Self { inner })
    }

    /// Iterate over decoded messages with metadata and timestamps.
    ///
    /// Returns an iterator that yields `DecodedMessageResult` containing
    /// the decoded message, channel info, and timestamps (when available
    /// from the format).
    ///
    /// # Timestamps
    ///
    /// Timestamps are included when available from the underlying format.
    /// Both MCAP and BAG formats provide timestamps, so `log_time` and
    /// `publish_time` will typically be populated. Use `has_timestamps()`
    /// on the result to check if both timestamps are present.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use robocodec::io::RoboReader;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = RoboReader::open("data.mcap")?;
    /// for result in reader.decoded()? {
    ///     let decoded = result?;
    ///     println!("Topic: {}", decoded.topic());
    ///     if decoded.has_timestamps() {
    ///         println!("Log time: {:?}", decoded.log_time);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn decoded(&self) -> Result<DecodedMessageIter<'_>> {
        use crate::io::formats::bag::ParallelBagReader;
        use crate::io::formats::mcap::reader::McapReader;
        use crate::io::formats::rrd::RrdReader;

        // Try MCAP first - use timestamped stream to get timestamps
        if let Some(mcap) = self.inner.as_any().downcast_ref::<McapReader>() {
            let mcap_iter = mcap.decode_messages_with_timestamp()?;
            let mcap_stream = mcap_iter.stream()?;
            return Ok(DecodedMessageIter {
                inner: Inner::Mcap(mcap_stream),
            });
        }

        // Try BAG - use timestamped stream to get timestamps
        if let Some(bag) = self.inner.as_any().downcast_ref::<ParallelBagReader>() {
            let bag_iter = bag.decode_messages_with_timestamp()?;
            let bag_stream = bag_iter.stream()?;
            return Ok(DecodedMessageIter {
                inner: Inner::Bag(bag_stream),
            });
        }

        // Try RRD - use timestamped stream to get timestamps
        if let Some(rrd) = self.inner.as_any().downcast_ref::<RrdReader>() {
            let rrd_iter = rrd.decode_messages_with_timestamp()?;
            let rrd_stream = rrd_iter.stream()?;
            return Ok(DecodedMessageIter {
                inner: Inner::Rrd(rrd_stream),
            });
        }

        // Try Parallel RRD - use timestamped stream to get timestamps
        use crate::io::formats::rrd::parallel::ParallelRrdReader;
        if let Some(rrd) = self.inner.as_any().downcast_ref::<ParallelRrdReader>() {
            let rrd_iter = rrd.decode_messages_with_timestamp()?;
            let rrd_stream = rrd_iter.stream()?;
            return Ok(DecodedMessageIter {
                inner: Inner::ParallelRrd(rrd_stream),
            });
        }

        // Include format information in error for better debugging
        let format_name = match self.inner.format() {
            crate::io::metadata::FileFormat::Mcap => "MCAP",
            crate::io::metadata::FileFormat::Bag => "ROS1 Bag",
            crate::io::metadata::FileFormat::Rrd => "RRD",
            crate::io::metadata::FileFormat::Unknown => "Unknown",
        };
        Err(CodecError::parse(
            "RoboReader",
            format!(
                "decoded() not supported for this format (detected: {})",
                format_name
            ),
        ))
    }

    /// Get the file information as a unified struct.
    pub fn file_info(&self) -> crate::io::metadata::FileInfo {
        self.inner.file_info()
    }

    /// Get the detected file format.
    pub fn format(&self) -> FileFormat {
        self.inner.format()
    }

    /// Check if this reader supports parallel reading.
    pub fn supports_parallel(&self) -> bool {
        use crate::io::formats::bag::ParallelBagReader;
        use crate::io::formats::mcap::parallel::ParallelMcapReader;
        use crate::io::formats::rrd::parallel::ParallelRrdReader;

        self.inner
            .as_any()
            .downcast_ref::<ParallelMcapReader>()
            .map(ParallelReader::supports_parallel)
            .or_else(|| {
                self.inner
                    .as_any()
                    .downcast_ref::<ParallelBagReader>()
                    .map(ParallelReader::supports_parallel)
            })
            .or_else(|| {
                self.inner
                    .as_any()
                    .downcast_ref::<ParallelRrdReader>()
                    .map(ParallelReader::supports_parallel)
            })
            .unwrap_or(false)
    }

    /// Get the number of chunks (for progress tracking).
    pub fn chunk_count(&self) -> usize {
        use crate::io::formats::bag::ParallelBagReader;
        use crate::io::formats::mcap::parallel::ParallelMcapReader;
        use crate::io::formats::rrd::parallel::ParallelRrdReader;

        self.inner
            .as_any()
            .downcast_ref::<ParallelMcapReader>()
            .map(ParallelReader::chunk_count)
            .or_else(|| {
                self.inner
                    .as_any()
                    .downcast_ref::<ParallelBagReader>()
                    .map(ParallelReader::chunk_count)
            })
            .or_else(|| {
                self.inner
                    .as_any()
                    .downcast_ref::<ParallelRrdReader>()
                    .map(ParallelReader::chunk_count)
            })
            .unwrap_or(0)
    }
}

impl FormatReader for RoboReader {
    fn channels(&self) -> &std::collections::HashMap<u16, ChannelInfo> {
        self.inner.channels()
    }

    fn channel_by_topic(&self, topic: &str) -> Option<&ChannelInfo> {
        self.inner.channel_by_topic(topic)
    }

    fn channels_by_topic(&self, topic: &str) -> Vec<&ChannelInfo> {
        self.inner.channels_by_topic(topic)
    }

    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    fn start_time(&self) -> Option<u64> {
        self.inner.start_time()
    }

    fn end_time(&self) -> Option<u64> {
        self.inner.end_time()
    }

    fn path(&self) -> &str {
        self.inner.path()
    }

    fn format(&self) -> FileFormat {
        self.inner.format()
    }

    fn file_size(&self) -> u64 {
        self.inner.file_size()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.inner.as_any_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::metadata::{ChannelInfo, FileFormat};

    // Mock FormatReader for testing
    struct MockReader {
        path: String,
        channels: std::collections::HashMap<u16, ChannelInfo>,
        message_count: u64,
        start_time: Option<u64>,
        end_time: Option<u64>,
        file_size: u64,
    }

    impl MockReader {
        fn new(path: &str) -> Self {
            Self {
                path: path.to_string(),
                channels: std::collections::HashMap::new(),
                message_count: 0,
                start_time: None,
                end_time: None,
                file_size: 0,
            }
        }
    }

    impl FormatReader for MockReader {
        fn channels(&self) -> &std::collections::HashMap<u16, ChannelInfo> {
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
            FileFormat::Unknown
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

    #[test]
    fn test_robo_reader_delegates_to_inner() {
        let mut mock = MockReader::new("test.bag");
        mock.channels
            .insert(0, ChannelInfo::new(0, "/test", "std_msgs/String"));
        mock.message_count = 100;
        mock.start_time = Some(1000);
        mock.end_time = Some(5000);
        mock.file_size = 10000;

        let reader = RoboReader {
            inner: Box::new(mock),
        };

        // Test delegation
        assert_eq!(reader.path(), "test.bag");
        assert_eq!(reader.message_count(), 100);
        assert_eq!(reader.start_time(), Some(1000));
        assert_eq!(reader.end_time(), Some(5000));
        assert_eq!(reader.file_size(), 10000);
        assert_eq!(reader.format(), FileFormat::Unknown);
        assert_eq!(reader.channels().len(), 1);
    }

    #[test]
    fn test_robo_reader_channel_by_topic() {
        let mut mock = MockReader::new("test.mcap");
        mock.channels
            .insert(0, ChannelInfo::new(0, "/chatter", "std_msgs/String"));
        mock.channels
            .insert(1, ChannelInfo::new(1, "/odom", "nav_msgs/Odometry"));

        let reader = RoboReader {
            inner: Box::new(mock),
        };

        let chatter = reader.channel_by_topic("/chatter");
        assert!(chatter.is_some());
        assert_eq!(chatter.unwrap().topic, "/chatter");

        let unknown = reader.channel_by_topic("/unknown");
        assert!(unknown.is_none());
    }

    #[test]
    fn test_decoded_message_result() {
        use crate::core::DecodedMessage;

        let message = DecodedMessage::new();
        let channel = ChannelInfo::new(0, "/test", "test_msgs/Test");
        let result = DecodedMessageResult::new(message.clone(), channel, Some(1000), Some(900));

        assert_eq!(result.topic(), "/test");
        assert_eq!(result.message_type(), "test_msgs/Test");
        assert_eq!(result.log_time, Some(1000));
        assert_eq!(result.publish_time, Some(900));
        assert_eq!(result.times(), (Some(1000), Some(900)));
        assert!(result.has_timestamps());
    }

    #[test]
    fn test_decoded_message_result_without_timestamps() {
        use crate::core::DecodedMessage;

        let message = DecodedMessage::new();
        let channel = ChannelInfo::new(0, "/test", "test_msgs/Test");
        let result = DecodedMessageResult::new(message.clone(), channel, None, None);

        assert_eq!(result.topic(), "/test");
        assert_eq!(result.log_time, None);
        assert_eq!(result.publish_time, None);
        assert!(!result.has_timestamps());
    }

    #[test]
    fn test_decoded_message_result_with_sequence() {
        use crate::core::DecodedMessage;

        let message = DecodedMessage::new();
        let channel = ChannelInfo::new(0, "/test", "test_msgs/Test");
        let result =
            DecodedMessageResult::new(message, channel, Some(1000), Some(900)).with_sequence(42);

        assert_eq!(result.sequence, Some(42));
    }

    #[test]
    fn test_open_file_not_found() {
        let result = RoboReader::open("/nonexistent/path/to/file.mcap");
        assert!(result.is_err());
        match result {
            Err(err) => {
                let err_msg = format!("{}", err);
                assert!(err_msg.contains("File not found"));
                assert!(err_msg.contains("/nonexistent/path/to/file.mcap"));
            }
            Ok(_) => panic!("Expected error for non-existent file"),
        }
    }

    #[test]
    fn test_open_unknown_format() {
        // Create a temp file with unknown extension
        let temp_path = std::env::temp_dir().join("test_unknown.xyz123");
        std::fs::write(&temp_path, b"invalid content").unwrap();

        let result = RoboReader::open(temp_path.to_str().unwrap());
        assert!(result.is_err());
        match result {
            Err(err) => {
                let err_msg = format!("{}", err);
                assert!(err_msg.contains("Unknown file format"));
            }
            Ok(_) => panic!("Expected error for unknown format"),
        }

        // Cleanup
        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_open_with_config_delegates() {
        let temp_path = std::env::temp_dir().join("test_config.mcap");
        std::fs::write(&temp_path, b"").unwrap();

        // Just verify it accepts the config parameter
        let config = ReaderConfig::default();
        let result = RoboReader::open_with_config(temp_path.to_str().unwrap(), config);
        // Will fail to parse as valid MCAP but should accept the config param
        assert!(result.is_err());

        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_decoded_not_supported_for_unknown_format() {
        // MockReader has Unknown format, so decoded() should fail
        let mock = MockReader::new("test.unknown");
        let reader = RoboReader {
            inner: Box::new(mock),
        };

        let result = reader.decoded();
        assert!(result.is_err());
        match result {
            Err(err) => {
                let err_msg = format!("{}", err);
                assert!(err_msg.contains("decoded() not supported for this format"));
                assert!(err_msg.contains("Unknown"));
            }
            Ok(_) => panic!("Expected error for unsupported format"),
        }
    }

    #[test]
    fn test_channels_by_topic() {
        let mut mock = MockReader::new("test.mcap");
        mock.channels
            .insert(0, ChannelInfo::new(0, "/chatter", "std_msgs/String"));
        mock.channels
            .insert(1, ChannelInfo::new(1, "/chatter", "std_msgs/String")); // Same topic, different channel

        let reader = RoboReader {
            inner: Box::new(mock),
        };

        let channels = reader.channels_by_topic("/chatter");
        assert_eq!(channels.len(), 2);

        let empty = reader.channels_by_topic("/nonexistent");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_supports_parallel_false_for_mock() {
        let mock = MockReader::new("test.bag");
        let reader = RoboReader {
            inner: Box::new(mock),
        };

        // MockReader doesn't implement ParallelReader, so should return false
        assert!(!reader.supports_parallel());
    }

    #[test]
    fn test_chunk_count_zero_for_mock() {
        let mock = MockReader::new("test.bag");
        let reader = RoboReader {
            inner: Box::new(mock),
        };

        // MockReader doesn't implement ParallelReader, so should return 0
        assert_eq!(reader.chunk_count(), 0);
    }

    #[test]
    fn test_file_info_delegates_to_inner() {
        let mut mock = MockReader::new("test.mcap");
        mock.message_count = 42;
        mock.file_size = 12345;
        mock.start_time = Some(100);
        mock.end_time = Some(200);

        let reader = RoboReader {
            inner: Box::new(mock),
        };

        let info = reader.file_info();
        assert_eq!(info.message_count, 42);
        assert_eq!(info.size, 12345);
        assert_eq!(info.start_time, 100);
        assert_eq!(info.end_time, 200);
    }

    #[test]
    fn test_format_delegates_to_inner() {
        let mock = MockReader::new("test.bag");
        let reader = RoboReader {
            inner: Box::new(mock),
        };

        assert_eq!(reader.format(), FileFormat::Unknown);
    }
}
