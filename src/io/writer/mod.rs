// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified writer for robotics data formats.
//!
//! This module provides a high-level writer that automatically detects
//! the format from the file extension.

pub mod builder;

pub use builder::{WriteStrategy, WriterBuilder, WriterConfig, WriterConfigBuilder};

use crate::io::detection::detect_format;
use crate::io::formats::bag::BagFormat;
use crate::io::formats::mcap::McapFormat;
use crate::io::formats::rrd::RrdFormat;
use crate::io::metadata::{FileFormat, RawMessage};
use crate::io::traits::FormatWriter;
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

/// Unified writer that delegates to format-specific implementations.
pub struct RoboWriter {
    /// The inner format-specific writer
    inner: Box<dyn FormatWriter>,
}

impl RoboWriter {
    /// Create a new writer with automatic format detection based on file extension.
    ///
    /// Supports both local file paths and S3 URLs (<s3://bucket/key>).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file, or S3 URL (<s3://bucket/key>)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{FormatWriter, RoboWriter};
    ///
    /// // Local file
    /// let writer = RoboWriter::create("output.mcap")?;
    ///
    /// // S3 object (requires tokio runtime)
    /// let writer = RoboWriter::create("s3://my-bucket/path/to/output.mcap")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create(path: &str) -> Result<Self> {
        Self::create_with_config(path, WriterConfig::default())
    }

    /// Create a writer with the specified configuration.
    ///
    /// Supports both local file paths and S3 URLs (<s3://bucket/key>).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file, or S3 URL (<s3://bucket/key>)
    /// * `config` - Writer configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{RoboWriter, WriterConfig};
    ///
    /// let writer = RoboWriter::create_with_config(
    ///     "output.mcap",
    ///     WriterConfig::default()
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create_with_config(path: &str, config: WriterConfig) -> Result<Self> {
        // Check for S3 URLs (requires remote feature for tokio/reqwest)
        #[cfg(feature = "remote")]
        {
            // Check for S3 URLs first
            if let Ok(location) = crate::io::s3::S3Location::from_s3_url(path) {
                // Use S3Writer for s3:// URLs
                let rt = shared_runtime();
                let writer = rt.block_on(async {
                    let client = crate::io::s3::S3Client::default_client().map_err(|e| {
                        CodecError::EncodeError {
                            codec: "S3".to_string(),
                            message: e.to_string(),
                        }
                    })?;
                    crate::io::s3::S3Writer::new(location, client).map_err(|e| {
                        CodecError::EncodeError {
                            codec: "S3".to_string(),
                            message: e.to_string(),
                        }
                    })
                })?;
                return Ok(Self {
                    inner: Box::new(writer),
                });
            }
        }

        // Fall back to local file path
        let path_obj = std::path::Path::new(path);

        // Get parent directory and ensure it exists
        if let Some(parent) = path_obj.parent()
            && !parent.as_os_str().is_empty()
        {
            match parent.try_exists() {
                Ok(false) => {
                    return Err(CodecError::parse(
                        "RoboWriter",
                        format!("Parent directory does not exist: {}", parent.display()),
                    ));
                }
                Err(e) => {
                    return Err(CodecError::parse(
                        "RoboWriter",
                        format!("Cannot access parent directory {}: {}", parent.display(), e),
                    ));
                }
                Ok(true) => {}
            }
        }

        let format = detect_format(path_obj)?;

        let inner: Box<dyn FormatWriter> = match format {
            FileFormat::Mcap => McapFormat::create_writer(path_obj, &config)?,
            FileFormat::Bag => BagFormat::create_writer(path_obj, &config)?,
            FileFormat::Rrd => RrdFormat::create_writer(path_obj, &config)?,
            FileFormat::Unknown => {
                // Try to determine from extension
                let extension = path_obj.extension().and_then(|e| e.to_str()).unwrap_or("");

                match extension {
                    "mcap" => McapFormat::create_writer(path_obj, &config)?,
                    "bag" => BagFormat::create_writer(path_obj, &config)?,
                    "rrd" => RrdFormat::create_writer(path_obj, &config)?,
                    _ => {
                        return Err(CodecError::parse(
                            "RoboWriter",
                            format!(
                                "Unknown file format. Use .mcap, .bag, or .rrd extension: {path}"
                            ),
                        ));
                    }
                }
            }
        };

        Ok(Self { inner })
    }

    /// Get the file format being written.
    #[must_use]
    pub fn format(&self) -> FileFormat {
        // Determine from path extension
        match self.path().rsplit('.').next() {
            Some("mcap") => FileFormat::Mcap,
            Some("bag") => FileFormat::Bag,
            Some("rrd") => FileFormat::Rrd,
            _ => FileFormat::Unknown,
        }
    }

    /// Downcast to the inner writer for format-specific operations.
    #[must_use]
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.as_any().downcast_ref::<T>()
    }

    /// Downcast mutably to the inner writer.
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.inner.as_any_mut().downcast_mut::<T>()
    }
}

impl FormatWriter for RoboWriter {
    fn path(&self) -> &str {
        self.inner.path()
    }

    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        self.inner
            .add_channel(topic, message_type, encoding, schema)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        self.inner.write(message)
    }

    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        self.inner.write_batch(messages)
    }

    fn finish(&mut self) -> Result<()> {
        self.inner.finish()
    }

    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    fn channel_count(&self) -> usize {
        self.inner.channel_count()
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
    use crate::io::metadata::{ChannelInfo, RawMessage};

    // Mock FormatWriter for testing
    struct MockWriter {
        path: String,
        channels: Vec<ChannelInfo>,
        messages: Vec<RawMessage>,
    }

    impl MockWriter {
        fn new(path: &str) -> Self {
            Self {
                path: path.to_string(),
                channels: Vec::new(),
                messages: Vec::new(),
            }
        }
    }

    impl FormatWriter for MockWriter {
        fn path(&self) -> &str {
            &self.path
        }

        fn add_channel(
            &mut self,
            topic: &str,
            message_type: &str,
            _encoding: &str,
            _schema: Option<&str>,
        ) -> Result<u16> {
            let id = self.channels.len() as u16;
            self.channels.push(ChannelInfo {
                id,
                topic: topic.to_string(),
                message_type: message_type.to_string(),
                encoding: "mock".to_string(),
                schema: None,
                schema_data: None,
                schema_encoding: None,
                message_count: 0,
                callerid: None,
            });
            Ok(id)
        }

        fn write(&mut self, message: &RawMessage) -> Result<()> {
            self.messages.push(message.clone());
            Ok(())
        }

        fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
            self.messages.extend(messages.iter().cloned());
            Ok(())
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        fn message_count(&self) -> u64 {
            self.messages.len() as u64
        }

        fn channel_count(&self) -> usize {
            self.channels.len()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_robowriter_delegates_to_inner() {
        let mut mock = MockWriter::new("test.bag");
        let channel_id = mock
            .add_channel("/test", "test_msgs/Test", "cdr", None)
            .unwrap();

        let mut writer = RoboWriter {
            inner: Box::new(mock),
        };

        // Test delegation of path
        assert_eq!(writer.path(), "test.bag");

        // Test delegation of channel_count
        assert_eq!(writer.channel_count(), 1);

        // Test delegation of message_count
        assert_eq!(writer.message_count(), 0);

        // Test write delegation
        let msg = RawMessage {
            channel_id,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3],
            sequence: None,
        };
        writer.write(&msg).unwrap();
        assert_eq!(writer.message_count(), 1);

        // Test write_batch delegation
        writer.write_batch(&[msg.clone(), msg.clone()]).unwrap();
        assert_eq!(writer.message_count(), 3);

        // Test finish delegation
        writer.finish().unwrap();
    }

    // =========================================================================
    // RoboWriter::format Tests
    // =========================================================================

    #[test]
    fn test_robowriter_format_mcap() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.mcap")),
        };
        assert_eq!(writer.format(), FileFormat::Mcap);
    }

    #[test]
    fn test_robowriter_format_bag() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.bag")),
        };
        assert_eq!(writer.format(), FileFormat::Bag);
    }

    #[test]
    fn test_robowriter_format_rrd() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.rrd")),
        };
        assert_eq!(writer.format(), FileFormat::Rrd);
    }

    #[test]
    fn test_robowriter_format_unknown() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.unknown")),
        };
        assert_eq!(writer.format(), FileFormat::Unknown);
    }

    #[test]
    fn test_robowriter_format_no_extension() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("testfile")),
        };
        assert_eq!(writer.format(), FileFormat::Unknown);
    }

    // =========================================================================
    // RoboWriter::downcast Tests
    // =========================================================================

    #[test]
    fn test_robowriter_downcast_ref_success() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.bag")),
        };
        let mock_ref = writer.downcast_ref::<MockWriter>();
        assert!(mock_ref.is_some());
    }

    #[test]
    fn test_robowriter_downcast_ref_wrong_type() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.bag")),
        };
        let wrong_ref = writer.downcast_ref::<String>();
        assert!(wrong_ref.is_none());
    }

    #[test]
    fn test_robowriter_downcast_mut_success() {
        let mut writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.bag")),
        };
        let mock_mut = writer.downcast_mut::<MockWriter>();
        assert!(mock_mut.is_some());
    }

    #[test]
    fn test_robowriter_downcast_mut_wrong_type() {
        let mut writer = RoboWriter {
            inner: Box::new(MockWriter::new("test.bag")),
        };
        let wrong_mut = writer.downcast_mut::<String>();
        assert!(wrong_mut.is_none());
    }

    // =========================================================================
    // FormatWriter Trait Implementation Tests
    // =========================================================================

    #[test]
    fn test_format_writer_path() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        assert_eq!(writer.path(), "output.bag");
    }

    #[test]
    fn test_format_writer_add_channel() {
        let mut writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        let result = writer.add_channel("/test", "test/Msg", "cdr", None);
        assert!(result.is_ok());
        assert_eq!(writer.channel_count(), 1);
    }

    #[test]
    fn test_format_writer_message_count_initially_zero() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        assert_eq!(writer.message_count(), 0);
    }

    #[test]
    fn test_format_writer_channel_count_initially_zero() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        assert_eq!(writer.channel_count(), 0);
    }

    #[test]
    fn test_format_writer_finish_empty() {
        let mut writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        assert!(writer.finish().is_ok());
    }

    #[test]
    fn test_format_writer_as_any() {
        let writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        let any = writer.as_any();
        assert!(any.is::<MockWriter>());
    }

    #[test]
    fn test_format_writer_as_any_mut() {
        let mut writer = RoboWriter {
            inner: Box::new(MockWriter::new("output.bag")),
        };
        let any_mut = writer.as_any_mut();
        assert!(any_mut.is::<MockWriter>());
    }

    // =========================================================================
    // RoboWriter::create Error Paths
    // =========================================================================

    #[test]
    fn test_robowriter_create_unknown_extension_error() {
        // This would create a file, so we just verify the function exists
        // The actual error handling is tested by integration tests
        let path = "/tmp/nonexistent_test.xyz123";
        let result = RoboWriter::create(path);
        // Should fail because of unknown extension
        assert!(result.is_err());
    }

    #[test]
    fn test_robowriter_create_with_empty_extension_error() {
        let path = "/tmp/test_no_ext";
        let result = RoboWriter::create(path);
        // Should fail because of no extension
        assert!(result.is_err());
    }

    // =========================================================================
    // WriterConfig Tests
    // =========================================================================

    #[test]
    fn test_writer_config_default() {
        let config = WriterConfig::default();
        let _ = config; // Just verify it can be created
    }

    #[test]
    fn test_writer_config_builder() {
        let builder = WriterConfigBuilder::new();
        let config = builder.build();
        let _ = config; // Verify build works
    }

    // =========================================================================
    // MockWriter Helper Tests
    // =========================================================================

    #[test]
    fn test_mock_writer_new() {
        let mock = MockWriter::new("test.bag");
        assert_eq!(mock.path, "test.bag");
        assert!(mock.channels.is_empty());
        assert!(mock.messages.is_empty());
    }

    #[test]
    fn test_mock_writer_add_channel() {
        let mut mock = MockWriter::new("test.bag");
        let id = mock.add_channel("/test", "test/Msg", "cdr", None).unwrap();
        assert_eq!(id, 0);
        assert_eq!(mock.channels.len(), 1);
        assert_eq!(mock.channels[0].topic, "/test");
        assert_eq!(mock.channels[0].message_type, "test/Msg");
    }

    #[test]
    fn test_mock_writer_write() {
        let mut mock = MockWriter::new("test.bag");
        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3],
            sequence: None,
        };
        mock.write(&msg).unwrap();
        assert_eq!(mock.messages.len(), 1);
        assert_eq!(mock.messages[0].data, vec![1, 2, 3]);
    }

    #[test]
    fn test_mock_writer_write_batch() {
        let mut mock = MockWriter::new("test.bag");
        let msg1 = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1],
            sequence: None,
        };
        let msg2 = RawMessage {
            channel_id: 0,
            log_time: 2000,
            publish_time: 2000,
            data: vec![2],
            sequence: None,
        };
        mock.write_batch(&[msg1, msg2]).unwrap();
        assert_eq!(mock.messages.len(), 2);
    }

    #[test]
    fn test_mock_writer_finish() {
        let mut mock = MockWriter::new("test.bag");
        assert!(mock.finish().is_ok());
    }

    #[test]
    fn test_mock_writer_message_count() {
        let mock = MockWriter::new("test.bag");
        assert_eq!(mock.message_count(), 0);
    }

    #[test]
    fn test_mock_writer_channel_count() {
        let mock = MockWriter::new("test.bag");
        assert_eq!(mock.channel_count(), 0);
    }

    #[test]
    fn test_mock_writer_as_any() {
        let mock = MockWriter::new("test.bag");
        let any = mock.as_any();
        assert!(any.is::<MockWriter>());
    }

    #[test]
    fn test_mock_writer_as_any_mut() {
        let mut mock = MockWriter::new("test.bag");
        let any_mut = mock.as_any_mut();
        assert!(any_mut.is::<MockWriter>());
    }

    // =========================================================================
    // S3 URL Detection Tests
    // =========================================================================
}
