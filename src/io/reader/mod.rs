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
//!
//! # S3 URLs
//!
//! For reading from S3-compatible storage:
//!
//! ```rust,no_run
//! use robocodec::io::RoboReader;
//!
//! // S3 object
//! let reader = RoboReader::open("s3://my-bucket/path/to/data.mcap")?;
//!
//! // S3 with custom endpoint (e.g., MinIO)
//! let reader = RoboReader::open("s3://my-bucket/file.mcap?endpoint=http://localhost:9000")?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod config;

pub use config::{ReaderConfig, ReaderConfigBuilder};

use crate::io::detection::detect_format;
use crate::io::formats::bag::BagFormat;
use crate::io::formats::mcap::McapFormat;
use crate::io::formats::rrd::RrdFormat;
use crate::io::metadata::{ChannelInfo, DecodedMessageResult, FileFormat};
use crate::io::traits::{DecodedMessageIterator, FormatReader, ParallelReader};
use crate::{CodecError, Result};

/// Get or create a shared Tokio runtime for blocking async operations.
///
/// This reuses a single runtime across all S3 operations, avoiding
/// the overhead of creating a new runtime for each open/write.
#[cfg(feature = "remote")]
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"))
}

/// Helper function to convert a timestamped message and channel into a decoded message result.
fn to_decoded_message_result(
    msg: crate::io::metadata::TimestampedDecodedMessage,
    ch: ChannelInfo,
) -> DecodedMessageResult {
    DecodedMessageResult {
        message: msg.message,
        channel: ch,
        log_time: Some(msg.log_time),
        publish_time: Some(msg.publish_time),
        sequence: None,
    }
}

/// Unified decoded message iterator.
///
/// This iterator works across all supported formats (MCAP, ROS1 bag, RRF2),
/// providing a consistent interface for iterating over decoded messages.
/// Timestamps are populated when available from the underlying format.
///
/// This uses a trait-based approach internally, avoiding fragile downcasting.
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
    /// The inner boxed iterator over timestamped messages
    inner: Box<dyn DecodedMessageIterator + Send + Sync + 'a>,
}

impl<'a> DecodedMessageIter<'a> {
    /// Create a new decoded message iterator from a boxed iterator.
    fn new(inner: Box<dyn DecodedMessageIterator + Send + Sync + 'a>) -> Self {
        Self { inner }
    }
}

impl Iterator for DecodedMessageIter<'_> {
    type Item = Result<DecodedMessageResult>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.map(|(msg, ch)| to_decoded_message_result(msg, ch)))
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
    /// Parse a URL to create an appropriate Transport.
    ///
    /// This helper function detects the S3 URL scheme (s3://)
    /// and creates the corresponding Transport implementation.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(transport))` - Successfully created transport from URL
    /// - `Ok(None)` - Not a URL (local file path)
    /// - `Err` - Unsupported URL scheme or parse error
    #[cfg(feature = "remote")]
    fn parse_url_to_transport(
        url: &str,
    ) -> Result<Option<Box<dyn crate::io::transport::Transport>>> {
        use crate::io::transport::s3::S3Transport;

        // Check for s3:// scheme
        if let Ok(location) = crate::io::s3::S3Location::from_s3_url(url) {
            // Create S3Transport using the shared runtime
            let rt = shared_runtime();
            let transport = rt.block_on(async {
                let client = crate::io::s3::S3Client::default_client().map_err(|e| {
                    CodecError::encode("S3", format!("Failed to create S3 client: {e}"))
                })?;
                S3Transport::new(client, location).await.map_err(|e| {
                    CodecError::encode("S3", format!("Failed to create S3 transport: {e}"))
                })
            })?;
            return Ok(Some(Box::new(transport)));
        }

        // Not a URL - treat as local path
        Ok(None)
    }

    /// Open a file with automatic format detection and default configuration.
    ///
    /// Supports both local file paths and S3 URLs (<s3://bucket/key>).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open, or S3 URL (<s3://bucket/key>)
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
    /// Supports both local file paths and S3 URLs (<s3://bucket/key>).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to open, or S3 URL (<s3://bucket/key>)
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
    pub fn open_with_config(path: &str, _config: ReaderConfig) -> Result<Self> {
        // Try to parse as URL and create appropriate transport
        #[cfg(feature = "remote")]
        {
            if let Some(transport) = Self::parse_url_to_transport(path)? {
                // Use transport-based reading
                // Detect format from path extension
                let path_obj = std::path::Path::new(path);
                let format = detect_format(path_obj)?;

                // Only MCAP format supports transport-based reading
                match format {
                    FileFormat::Mcap => {
                        return Ok(Self {
                            inner: Box::new(
                                crate::io::formats::mcap::transport_reader::McapTransportReader::open_from_transport(
                                    transport,
                                    path.to_string(),
                                )?,
                            ),
                        });
                    }
                    FileFormat::Bag => {
                        return Err(CodecError::unsupported(
                            "BAG format does not support transport-based reading. Use local file access.",
                        ));
                    }
                    FileFormat::Rrd => {
                        return Err(CodecError::unsupported(
                            "RRD format does not support transport-based reading. Use local file access.",
                        ));
                    }
                    FileFormat::Unknown => {
                        return Err(CodecError::parse(
                            "RoboReader",
                            format!("Unknown file format from URL: {path}"),
                        ));
                    }
                }
            }
        }

        // Fall back to local file path
        let path_obj = std::path::Path::new(path);

        if !path_obj.exists() {
            return Err(CodecError::parse(
                "RoboReader",
                format!("File not found: {path}"),
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
                    format!("Unknown file format: {path}"),
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
    ///
    /// # Trait-based approach
    ///
    /// This method uses the `decoded_with_timestamp_boxed()` trait method
    /// instead of downcasting, making it more maintainable and allowing
    /// new formats to be added without modifying this code.
    pub fn decoded(&self) -> Result<DecodedMessageIter<'_>> {
        // Use the trait-based approach - this will work for any format
        // that implements decoded_with_timestamp_boxed()
        let boxed_iter = self.inner.decoded_with_timestamp_boxed().map_err(|_| {
            let format_name = match self.inner.format() {
                crate::io::metadata::FileFormat::Mcap => "MCAP",
                crate::io::metadata::FileFormat::Bag => "ROS1 Bag",
                crate::io::metadata::FileFormat::Rrd => "RRD",
                crate::io::metadata::FileFormat::Unknown => "Unknown",
            };
            CodecError::parse(
                "RoboReader",
                format!("decoded() not supported for this format (detected: {format_name})"),
            )
        })?;

        Ok(DecodedMessageIter::new(boxed_iter))
    }

    /// Get the file information as a unified struct.
    #[must_use]
    pub fn file_info(&self) -> crate::io::metadata::FileInfo {
        self.inner.file_info()
    }

    /// Get the detected file format.
    #[must_use]
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
    #[cfg(feature = "remote")]
    fn open_from_transport(
        transport: Box<dyn crate::io::transport::Transport>,
        path: String,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        // Detect format from path extension
        let path_obj = std::path::Path::new(&path);
        let format = detect_format(path_obj)?;

        // Delegate to the appropriate format-specific reader
        // Note: Most format readers don't support transport-based reading,
        // so this will only work for transport-compatible readers
        let inner: Box<dyn FormatReader> = match format {
            FileFormat::Mcap => {
                // McapTransportReader supports transport-based reading
                use crate::io::formats::mcap::transport_reader::McapTransportReader;
                Box::new(McapTransportReader::open_from_transport(transport, path)?)
            }
            FileFormat::Bag => {
                // BAG readers don't support transport-based reading
                return Err(CodecError::unsupported(
                    "BAG format does not support transport-based reading. \
                     Use local file access or S3Reader for S3 sources.",
                ));
            }
            FileFormat::Rrd => {
                // RRD readers don't support transport-based reading
                return Err(CodecError::unsupported(
                    "RRD format does not support transport-based reading. \
                     Use local file access.",
                ));
            }
            FileFormat::Unknown => {
                return Err(CodecError::parse(
                    "RoboReader",
                    format!("Unknown file format: {path}"),
                ));
            }
        };

        Ok(Self { inner })
    }

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
        #[cfg(feature = "remote")]
        fn open_from_transport(
            _transport: Box<dyn crate::io::transport::Transport>,
            path: String,
        ) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(Self::new(&path))
        }

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

    #[test]
    #[cfg(feature = "remote")]
    fn test_parse_url_to_transport_with_s3_url() {
        // Test valid S3 URL - this will attempt to create an S3Client
        // In a test environment without credentials, this may fail, but
        // the URL parsing itself should work
        let result = RoboReader::parse_url_to_transport("s3://my-bucket/path/to/file.mcap");

        // The result may be Ok or Err depending on whether S3 credentials are available
        // If it's Ok, we should get Some(transport)
        // If it's Err, it should be related to S3 client creation, not URL parsing
        match result {
            Ok(transport_option) => {
                // If successful, we should have a transport
                assert!(
                    transport_option.is_some(),
                    "Expected Some(transport) for valid S3 URL"
                );
            }
            Err(e) => {
                // If error, it should be related to S3 client creation, not URL parsing
                let err_msg = format!("{}", e);
                // Error should mention S3, not URL parsing
                assert!(
                    err_msg.contains("S3")
                        || err_msg.contains("client")
                        || err_msg.contains("transport"),
                    "Expected S3-related error, got: {}",
                    err_msg
                );
            }
        }

        // Test S3 URL with endpoint query parameter (localhost is allowed for testing)
        let result = RoboReader::parse_url_to_transport(
            "s3://my-bucket/file.mcap?endpoint=http://localhost:9000",
        );
        // Same as above - check for reasonable error or success
        match result {
            Ok(transport_option) => {
                assert!(
                    transport_option.is_some(),
                    "Expected Some(transport) for S3 URL with endpoint"
                );
            }
            Err(e) => {
                let err_msg = format!("{}", e);
                assert!(
                    err_msg.contains("S3")
                        || err_msg.contains("client")
                        || err_msg.contains("transport"),
                    "Expected S3-related error, got: {}",
                    err_msg
                );
            }
        }
    }

    #[test]
    #[cfg(feature = "remote")]
    fn test_parse_url_to_transport_with_local_path_returns_none() {
        // Test local file path (should return None)
        let result = RoboReader::parse_url_to_transport("/path/to/file.mcap");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test relative path
        let result = RoboReader::parse_url_to_transport("file.mcap");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    #[cfg(feature = "remote")]
    fn test_parse_url_to_transport_with_invalid_s3_url() {
        // Test invalid S3 URL (missing bucket)
        let result = RoboReader::parse_url_to_transport("s3://");
        assert!(result.is_ok()); // Invalid S3 URL is treated as local path
        assert!(result.unwrap().is_none());

        // Test malformed URL
        let result = RoboReader::parse_url_to_transport("s3:///key");
        assert!(result.is_ok()); // Invalid S3 URL is treated as local path
        assert!(result.unwrap().is_none());
    }
}
