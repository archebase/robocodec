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
use std::path::Path;

/// Unified writer that delegates to format-specific implementations.
pub struct RoboWriter {
    /// The inner format-specific writer
    inner: Box<dyn FormatWriter>,
}

impl RoboWriter {
    /// Create a new writer with automatic format detection based on file extension.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{FormatWriter, RoboWriter};
    ///
    /// let mut writer = RoboWriter::create("output.mcap")?;
    /// let channel_id = writer.add_channel("/topic", "type", "cdr", None)?;
    /// writer.finish()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_config(path, WriterConfig::default())
    }

    /// Create a writer with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file
    /// * `config` - Writer configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::{RoboWriter, WriterConfig};
    ///
    /// let mut writer = RoboWriter::create_with_config(
    ///     "output.mcap",
    ///     WriterConfig::builder().chunk_size(1024 * 1024).build()
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create_with_config<P: AsRef<Path>>(path: P, config: WriterConfig) -> Result<Self> {
        let path = path.as_ref();

        // Get parent directory and ensure it exists
        if let Some(parent) = path.parent() {
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
                    Ok(true) => {} // Parent exists, continue
                }
            }
        }

        let format = detect_format(path)?;

        let inner: Box<dyn FormatWriter> = match format {
            FileFormat::Mcap => McapFormat::create_writer(path, &config)?,
            FileFormat::Bag => BagFormat::create_writer(path, &config)?,
            FileFormat::Unknown => {
                // Try to determine from extension
                let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");

                match extension {
                    "mcap" => McapFormat::create_writer(path, &config)?,
                    "bag" => BagFormat::create_writer(path, &config)?,
                    _ => {
                        return Err(CodecError::parse(
                            "RoboWriter",
                            format!(
                                "Unknown file format. Use .mcap or .bag extension: {}",
                                path.display()
                            ),
                        ))
                    }
                }
            }
        };

        Ok(Self { inner })
    }

    /// Get the file information as a unified struct.
    pub fn format(&self) -> FileFormat {
        // Determine from path extension
        match self.path().rsplit('.').next() {
            Some("mcap") => FileFormat::Mcap,
            Some("bag") => FileFormat::Bag,
            _ => FileFormat::Unknown,
        }
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
