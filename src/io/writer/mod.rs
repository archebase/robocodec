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
use crate::io::metadata::{FileFormat, RawMessage};
use crate::io::traits::FormatWriter;
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

/// Unified writer that delegates to format-specific implementations.
pub struct RoboWriter {
    /// The inner format-specific writer
    inner: Box<dyn FormatWriter>,
}

impl RoboWriter {
    /// Create a new writer with automatic format detection based on file extension.
    ///
    /// Supports both local file paths and S3 URLs (s3://bucket/key).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file, or S3 URL (s3://bucket/key)
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
    /// Supports both local file paths and S3 URLs (s3://bucket/key).
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file, or S3 URL (s3://bucket/key)
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
        let _ = config; // Config reserved for future use

        // Check if this is an S3 URL
        #[cfg(feature = "s3")]
        {
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
        if let Some(parent) = path_obj.parent() {
            if !parent.as_os_str().is_empty() {
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
        }

        let format = detect_format(path_obj)?;

        let inner: Box<dyn FormatWriter> = match format {
            FileFormat::Mcap => McapFormat::create_writer(path_obj, &config)?,
            FileFormat::Bag => BagFormat::create_writer(path_obj, &config)?,
            FileFormat::Unknown => {
                // Try to determine from extension
                let extension = path_obj.extension().and_then(|e| e.to_str()).unwrap_or("");

                match extension {
                    "mcap" => McapFormat::create_writer(path_obj, &config)?,
                    "bag" => BagFormat::create_writer(path_obj, &config)?,
                    _ => {
                        return Err(CodecError::parse(
                            "RoboWriter",
                            format!("Unknown file format. Use .mcap or .bag extension: {}", path),
                        ));
                    }
                }
            }
        };

        Ok(Self { inner })
    }

    /// Get the file format being written.
    pub fn format(&self) -> FileFormat {
        // Determine from path extension
        match self.path().rsplit('.').next() {
            Some("mcap") => FileFormat::Mcap,
            Some("bag") => FileFormat::Bag,
            _ => FileFormat::Unknown,
        }
    }

    /// Downcast to the inner writer for format-specific operations.
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
}
