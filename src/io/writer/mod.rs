// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified writer for robotics data formats.
//!
//! This module provides a high-level writer that supports different
//! formats and writing strategies.

pub mod builder;
pub mod write_strategy;

pub use builder::{WriterBuilder, WriterConfig};
pub use write_strategy::{AutoWrite, ParallelWrite, SequentialWrite, WriteStrategy};

use crate::io::metadata::RawMessage;
use crate::io::traits::FormatWriter;
use crate::Result;
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
    /// use robocodec::io::RoboWriter;
    ///
    /// let writer = RoboWriter::create("output.mcap")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        WriterBuilder::new().path(path).build()
    }

    /// Create a writer with a specific strategy.
    pub fn create_with_strategy<P: AsRef<Path>>(path: P, _strategy: WriteStrategy) -> Result<Self> {
        // For now, strategy doesn't affect writer creation
        Self::create(path)
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
    fn test_robowriter_downcast_ref() {
        let mock = MockWriter::new("test.mcap");
        let writer = RoboWriter {
            inner: Box::new(mock),
        };

        // Should be able to downcast to MockWriter
        let mock_ref = writer.downcast_ref::<MockWriter>();
        assert!(mock_ref.is_some());
        assert_eq!(mock_ref.unwrap().path(), "test.mcap");
    }

    #[test]
    fn test_robowriter_downcast_mut() {
        let mock = MockWriter::new("test.mcap");
        let mut writer = RoboWriter {
            inner: Box::new(mock),
        };

        // Should be able to downcast mutably to MockWriter
        let mock_mut = writer.downcast_mut::<MockWriter>();
        assert!(mock_mut.is_some());
        assert_eq!(mock_mut.unwrap().path(), "test.mcap");
    }

    #[test]
    fn test_robowriter_downcast_wrong_type() {
        let mock = MockWriter::new("test.mcap");
        let mut writer = RoboWriter {
            inner: Box::new(mock),
        };

        // Try to downcast to wrong type should fail
        let wrong_ref = writer.downcast_ref::<String>();
        assert!(wrong_ref.is_none());

        let wrong_mut = writer.downcast_mut::<String>();
        assert!(wrong_mut.is_none());
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
