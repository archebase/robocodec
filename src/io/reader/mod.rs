// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified reader with automatic strategy selection.
//!
//! This module provides a high-level reader that automatically selects
//! the optimal reading strategy (sequential vs parallel) based on file
//! capabilities and configuration.
//!
//! # Strategy Selection
//!
//! The reader supports three strategies:
//! - **Auto**: Automatically choose parallel for MCAP with summary, sequential otherwise
//! - **Sequential**: Always read sequentially (fallback)
//! - **Parallel**: Force parallel reading (requires MCAP with summary)
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::{ReaderBuilder, ReadStrategy};
//!
//! // Auto-detect strategy
//! let reader = ReaderBuilder::new()
//!     .path("data.mcap")
//!     .build()?;
//!
//! // Force specific strategy
//! let reader = ReaderBuilder::new()
//!     .path("data.mcap")
//!     .strategy(ReadStrategy::Parallel)
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod builder;
pub mod strategy;

pub use builder::{ReaderBuilder, ReaderConfig};
pub use strategy::{AutoStrategy, ParallelStrategy, ReadStrategy, SequentialStrategy};

use crate::core::DecodedMessage;
use crate::io::metadata::ChannelInfo;
use crate::io::traits::FormatReader;
use crate::{CodecError, Result};
use std::path::Path;

/// Unified decoded message iterator that works for both BAG and MCAP formats.
///
/// This enum wraps format-specific iterators to provide a consistent API.
pub enum DecodedMessageIter<'a> {
    /// MCAP format decoded message iterator
    Mcap(crate::io::formats::mcap::reader::DecodedMessageIter<'a>),
    /// BAG format decoded message iterator
    Bag(crate::io::formats::bag::BagDecodedMessageIter<'a>),
}

impl<'a> DecodedMessageIter<'a> {
    /// Get the channels for this iterator.
    pub fn channels(&self) -> &std::collections::HashMap<u16, ChannelInfo> {
        match self {
            Self::Mcap(iter) => {
                // Convert MCAP's ChannelInfo to our unified ChannelInfo
                static EMPTY_CHANNELS: std::sync::OnceLock<
                    std::collections::HashMap<u16, ChannelInfo>,
                > = std::sync::OnceLock::new();
                EMPTY_CHANNELS.get_or_init(|| {
                    let mcap_channels = iter.channels();
                    let mut channels = std::collections::HashMap::new();
                    for (&id, ch) in mcap_channels {
                        channels.insert(
                            id,
                            ChannelInfo {
                                id: ch.id,
                                topic: ch.topic.clone(),
                                message_type: ch.message_type.clone(),
                                encoding: ch.encoding.clone(),
                                schema: ch.schema.clone(),
                                schema_data: ch.schema_data.clone(),
                                schema_encoding: ch.schema_encoding.clone(),
                                message_count: ch.message_count,
                                callerid: ch.callerid.clone(),
                            },
                        );
                    }
                    channels
                })
            }
            Self::Bag(iter) => iter.channels(),
        }
    }

    /// Create a proper streaming iterator over decoded messages.
    pub fn stream(&self) -> Result<DecodedMessageStream<'a>> {
        match self {
            Self::Mcap(iter) => Ok(DecodedMessageStream::Mcap(iter.stream()?)),
            Self::Bag(iter) => Ok(DecodedMessageStream::Bag(iter.stream()?)),
        }
    }
}

/// Streaming iterator over decoded messages (unified for BAG and MCAP).
pub enum DecodedMessageStream<'a> {
    /// MCAP format decoded message stream
    Mcap(crate::io::formats::mcap::reader::DecodedMessageStream<'a>),
    /// BAG format decoded message stream
    Bag(crate::io::formats::bag::BagDecodedMessageStream<'a>),
}

impl<'a> Iterator for DecodedMessageStream<'a> {
    type Item = std::result::Result<(DecodedMessage, ChannelInfo), CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mcap(ref mut stream) => stream.next().map(|result| {
                result.map(|(msg, ch)| {
                    let ch = ChannelInfo {
                        id: ch.id,
                        topic: ch.topic,
                        message_type: ch.message_type,
                        encoding: ch.encoding,
                        schema: ch.schema,
                        schema_data: ch.schema_data,
                        schema_encoding: ch.schema_encoding,
                        message_count: ch.message_count,
                        callerid: ch.callerid,
                    };
                    (msg, ch)
                })
            }),
            Self::Bag(ref mut stream) => stream.next(),
        }
    }
}

/// Unified reader that delegates to the optimal strategy.
///
/// This type provides a consistent API regardless of the underlying
/// strategy (sequential or parallel). Supports auto-detection of
/// BAG and MCAP formats.
pub struct RoboReader {
    /// The inner format-specific reader
    inner: Box<dyn FormatReader>,
    /// The strategy being used
    strategy: ReadStrategy,
}

impl RoboReader {
    /// Open a file with automatic strategy detection.
    ///
    /// This is the simplest way to open a file - the reader will
    /// automatically detect the format and choose the optimal strategy.
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
        ReaderBuilder::new().path(path).build()
    }

    /// Open a file with a specific strategy.
    ///
    /// Use this when you want to force a particular reading strategy
    /// instead of relying on automatic detection.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open
    /// * `strategy` - The strategy to use for reading
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{RoboReader, ReadStrategy};
    ///
    /// let reader = RoboReader::open_with_strategy(
    ///     "data.mcap",
    ///     ReadStrategy::Sequential
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open_with_strategy<P: AsRef<Path>>(path: P, strategy: ReadStrategy) -> Result<Self> {
        ReaderBuilder::new().path(path).strategy(strategy).build()
    }

    /// Get the reading strategy being used.
    pub fn strategy(&self) -> &ReadStrategy {
        &self.strategy
    }

    /// Downcast to the inner reader for format-specific operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::RoboReader;
    /// # use robocodec::io::formats::mcap::McapFormat;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// # let reader = RoboReader::open("data.mcap")?;
    /// if let Some(mcap) = reader.downcast_ref::<McapFormat>() {
    ///     // Access MCAP-specific methods
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.as_any().downcast_ref::<T>()
    }

    /// Downcast mutably to the inner reader.
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.inner.as_any_mut().downcast_mut::<T>()
    }

    /// Decode messages from the reader.
    ///
    /// This method works with both MCAP and BAG formats, automatically
    /// detecting the format and returning the appropriate iterator.
    ///
    /// # Returns
    ///
    /// A unified iterator yielding `(DecodedMessage, ChannelInfo)` tuples.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use robocodec::io::RoboReader;
    /// # fn test() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = RoboReader::open("data.mcap")?;
    /// let decoded_iter = reader.decode_messages()?;
    /// let mut stream = decoded_iter.stream()?;
    ///
    /// while let Some(result) = stream.next() {
    ///     let (message, channel_info) = result?;
    ///     println!("Topic: {}", channel_info.topic);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn decode_messages(&self) -> Result<DecodedMessageIter<'_>> {
        use crate::io::formats::bag::ParallelBagReader;
        use crate::io::formats::mcap::reader::McapReader;

        // Try MCAP first
        if let Some(mcap) = self.downcast_ref::<McapReader>() {
            return Ok(DecodedMessageIter::Mcap(mcap.decode_messages()?));
        }

        // Try BAG
        if let Some(bag) = self.downcast_ref::<ParallelBagReader>() {
            return Ok(DecodedMessageIter::Bag(bag.decode_messages()?));
        }

        Err(crate::CodecError::parse(
            "RoboReader",
            "decode_messages not supported for this format",
        ))
    }

    /// Decode messages with timestamps from the reader.
    ///
    /// Similar to `decode_messages` but includes log_time and publish_time
    /// for each message.
    pub fn decode_messages_with_timestamp(
        &self,
    ) -> Result<crate::io::formats::mcap::reader::DecodedMessageWithTimestampIter<'_>> {
        use crate::io::formats::mcap::reader::McapReader;

        if let Some(mcap) = self.downcast_ref::<McapReader>() {
            return mcap.decode_messages_with_timestamp();
        }

        Err(crate::CodecError::parse(
            "RoboReader",
            "decode_messages_with_timestamp not supported for this format",
        ))
    }
}

impl FormatReader for RoboReader {
    fn channels(&self) -> &std::collections::HashMap<u16, crate::io::metadata::ChannelInfo> {
        self.inner.channels()
    }

    fn channel_by_topic(&self, topic: &str) -> Option<&crate::io::metadata::ChannelInfo> {
        self.inner.channel_by_topic(topic)
    }

    fn channels_by_topic(&self, topic: &str) -> Vec<&crate::io::metadata::ChannelInfo> {
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

    fn format(&self) -> crate::io::metadata::FileFormat {
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
    fn test_unified_reader_creation() {
        // This is a placeholder test - real tests would use actual files
        // The structure demonstrates the API usage
        let _builder = ReaderBuilder::new();
    }

    #[test]
    fn test_robo_reader_strategy() {
        let mock = MockReader::new("test.mcap");
        let reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Sequential,
        };

        assert_eq!(reader.strategy(), &ReadStrategy::Sequential);
    }

    #[test]
    fn test_robo_reader_downcast_ref() {
        let mock = MockReader::new("test.mcap");
        let reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Auto,
        };

        // Should be able to downcast to MockReader
        let mock_ref = reader.downcast_ref::<MockReader>();
        assert!(mock_ref.is_some());
        assert_eq!(mock_ref.unwrap().path(), "test.mcap");
    }

    #[test]
    fn test_robo_reader_downcast_mut() {
        let mock = MockReader::new("test.mcap");
        let mut reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Auto,
        };

        // Should be able to downcast mutably to MockReader
        let mock_mut = reader.downcast_mut::<MockReader>();
        assert!(mock_mut.is_some());
        assert_eq!(mock_mut.unwrap().path(), "test.mcap");
    }

    #[test]
    fn test_robo_reader_downcast_wrong_type() {
        let mock = MockReader::new("test.mcap");
        let mut reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Auto,
        };

        // Try to downcast to wrong type should fail
        let wrong_ref = reader.downcast_ref::<String>();
        assert!(wrong_ref.is_none());

        let wrong_mut = reader.downcast_mut::<String>();
        assert!(wrong_mut.is_none());
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
            strategy: ReadStrategy::Auto,
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
            strategy: ReadStrategy::Auto,
        };

        let chatter = reader.channel_by_topic("/chatter");
        assert!(chatter.is_some());
        assert_eq!(chatter.unwrap().topic, "/chatter");

        let unknown = reader.channel_by_topic("/unknown");
        assert!(unknown.is_none());
    }

    #[test]
    fn test_robo_reader_channels_by_topic() {
        let mut mock = MockReader::new("test.mcap");
        mock.channels
            .insert(0, ChannelInfo::new(0, "/chatter", "std_msgs/String"));
        mock.channels
            .insert(1, ChannelInfo::new(1, "/chatter", "std_msgs/String"));

        let reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Auto,
        };

        let channels = reader.channels_by_topic("/chatter");
        assert_eq!(channels.len(), 2);
    }

    #[test]
    fn test_decode_messages_not_supported() {
        let mock = MockReader::new("test.bag");
        let reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Auto,
        };

        // Should return error since MockReader doesn't implement McapReader
        let result = reader.decode_messages();
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_messages_with_timestamp_not_supported() {
        let mock = MockReader::new("test.bag");
        let reader = RoboReader {
            inner: Box::new(mock),
            strategy: ReadStrategy::Auto,
        };

        // Should return error since MockReader doesn't implement McapReader
        let result = reader.decode_messages_with_timestamp();
        assert!(result.is_err());
    }
}
