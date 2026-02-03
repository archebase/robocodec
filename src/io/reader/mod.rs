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
use crate::io::formats::mcap::reader::DecodedMessageWithTimestampStream as McapTimestampedStream;
use crate::io::formats::mcap::McapFormat;
use crate::io::metadata::{ChannelInfo, DecodedMessageResult, FileFormat};
use crate::io::traits::{FormatReader, ParallelReader};
use crate::{CodecError, Result};
use std::path::Path;

enum DecodedMessageIterInner<'a> {
    Mcap(McapTimestampedStream<'a>),
    Bag(crate::io::formats::bag::BagDecodedMessageWithTimestampStream<'a>),
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
    /// # Arguments
    ///
    /// * `path` - Path to the file to open
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::RoboReader;
    ///
    /// let reader = RoboReader::open("data.mcap")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, ReaderConfig::default())
    }

    /// Open a file with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open
    /// * `config` - Reader configuration (currently reserved for future use)
    ///
    /// # Note
    ///
    /// The `config` parameter is currently accepted but not used.
    /// The reader automatically selects the optimal reading strategy based on file characteristics.
    /// This parameter is reserved for future use when explicit strategy control will be implemented.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{RoboReader, ReaderConfig};
    ///
    /// let reader = RoboReader::open_with_config(
    ///     "data.mcap",
    ///     ReaderConfig::builder().prefer_parallel(true).build()
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: ReaderConfig) -> Result<Self> {
        // Note: config is currently reserved for future use
        // The format readers auto-select optimal strategy based on file characteristics
        let _ = config; // Suppress unused warning while preserving API
        let path = path.as_ref();

        if !path.exists() {
            return Err(CodecError::parse(
                "RoboReader",
                format!("File not found: {}", path.display()),
            ));
        }

        let format = detect_format(path)?;

        let inner: Box<dyn FormatReader> = match format {
            FileFormat::Mcap => Box::new(McapFormat::open(path)?),
            FileFormat::Bag => Box::new(BagFormat::open(path)?),
            FileFormat::Unknown => {
                return Err(CodecError::parse(
                    "RoboReader",
                    format!("Unknown file format: {}", path.display()),
                ))
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

        // Include format information in error for better debugging
        let format_name = match self.inner.format() {
            crate::io::metadata::FileFormat::Mcap => "MCAP",
            crate::io::metadata::FileFormat::Bag => "ROS1 Bag",
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
            .unwrap_or(false)
    }

    /// Get the number of chunks (for progress tracking).
    pub fn chunk_count(&self) -> usize {
        use crate::io::formats::bag::ParallelBagReader;
        use crate::io::formats::mcap::parallel::ParallelMcapReader;

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
}
