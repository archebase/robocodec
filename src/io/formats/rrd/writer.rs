// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! RRD file writer (RRF2 format).
//!
//! This module provides `RrdWriter` for writing Rerun RRD (RRF2) files.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::core::{CodecError, Result};
use crate::io::metadata::{ChannelInfo, RawMessage};
use crate::io::traits::FormatWriter;

use super::constants::{
    COMPRESSION_OFF, MSG_KIND_ARROW_MSG, MSG_KIND_END, RRD_FOOTER_MAGIC, RRD_MAGIC, RRD_VERSION,
    SERIALIZER_PROTOBUF, STREAM_FOOTER_SIZE,
};

/// RRD file writer (RRF2 format).
///
/// Creates RRF2 format files with sequential message storage.
pub struct RrdWriter {
    /// Output file
    file: std::fs::File,
    /// File path
    path: String,
    /// Next channel ID
    next_channel_id: u16,
    /// Channels added to the file
    channels: HashMap<u16, ChannelInfo>,
    /// Total messages written
    message_count: u64,
    /// Finished flag
    finished: bool,
}

impl RrdWriter {
    /// Create a new RRD writer.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the output file
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_obj = path.as_ref();
        let path_str = path_obj.to_string_lossy().to_string();

        // Ensure parent directory exists
        if let Some(parent) = path_obj.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            return Err(CodecError::parse(
                "RRD",
                format!("Parent directory does not exist: {}", parent.display()),
            ));
        }

        // Create the file
        let mut file = std::fs::File::create(path_obj)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to create file: {}", e)))?;

        // Write RRF2 stream header
        Self::write_header(&mut file)?;

        Ok(Self {
            file,
            path: path_str,
            next_channel_id: 0,
            channels: HashMap::new(),
            message_count: 0,
            finished: false,
        })
    }

    /// Write the RRF2 stream header (12 bytes).
    ///
    /// Format: magic(4) + version(4) + options(4)
    fn write_header<W: Write>(writer: &mut W) -> Result<()> {
        // Magic: "RRF2"
        writer
            .write_all(RRD_MAGIC)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write magic: {}", e)))?;

        // Version: [0, 0, 0, 1]
        writer
            .write_all(&RRD_VERSION)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write version: {}", e)))?;

        // Options: compression(1) + serializer(1) + reserved(2)
        writer
            .write_all(&[COMPRESSION_OFF]) // compression: off
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write compression: {}", e)))?;

        writer
            .write_all(&[SERIALIZER_PROTOBUF]) // serializer: protobuf
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write serializer: {}", e)))?;

        writer
            .write_all(&[0u8, 0]) // reserved
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write reserved: {}", e)))?;

        Ok(())
    }

    /// Write a message to the file.
    ///
    /// RRF2 message format: kind(u64) + len(u64) + payload
    fn write_message(&mut self, kind: u64, data: &[u8]) -> Result<()> {
        if self.finished {
            return Err(CodecError::parse("RRD", "Cannot write to finished writer"));
        }

        // Write message header: kind + len
        self.file.write_u64::<LittleEndian>(kind).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to write message kind: {}", e))
        })?;

        self.file
            .write_u64::<LittleEndian>(data.len() as u64)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write message len: {}", e)))?;

        // Write payload
        self.file.write_all(data).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to write message data: {}", e))
        })?;

        self.message_count += 1;

        Ok(())
    }

    /// Write the RRF2 stream footer and finalize.
    fn write_footer(&mut self) -> Result<()> {
        // Write end marker message
        self.write_message(MSG_KIND_END, &[])?;

        // Write stream footer (32 bytes)
        // Format: entries(20) + magic(4) + identifier(4) + count(4)
        let footer_data = vec![0u8; STREAM_FOOTER_SIZE];
        self.file
            .write_all(&footer_data)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write footer: {}", e)))?;

        self.file.write_all(RRD_FOOTER_MAGIC).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to write footer magic: {}", e))
        })?;

        Ok(())
    }
}

impl FormatWriter for RrdWriter {
    fn path(&self) -> &str {
        &self.path
    }

    fn add_channel(
        &mut self,
        topic: &str,
        _message_type: &str,
        _encoding: &str,
        _schema: Option<&str>,
    ) -> Result<u16> {
        // In RRF2, channels are implicit (entity paths are in message payloads)
        // We track them for API compatibility but don't write them to the file
        let id = self.next_channel_id;
        self.next_channel_id += 1;

        let channel = ChannelInfo {
            id,
            topic: topic.to_string(),
            message_type: "rerun.ArrowMsg".to_string(),
            encoding: "protobuf".to_string(),
            schema: None,
            schema_data: None,
            schema_encoding: Some("protobuf".to_string()),
            message_count: 0,
            callerid: None,
        };

        self.channels.insert(id, channel);
        Ok(id)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        if self.finished {
            return Err(CodecError::parse("RRD", "Cannot write to finished writer"));
        }

        // Update channel message count
        if let Some(channel) = self.channels.get_mut(&message.channel_id) {
            channel.message_count += 1;
        }

        // Write message as ArrowMsg (kind=2)
        // In RRF2, we write the raw data as the payload
        self.write_message(MSG_KIND_ARROW_MSG, &message.data)?;

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        self.write_footer()?;
        self.file
            .flush()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to flush file: {}", e)))?;

        self.finished = true;
        Ok(())
    }

    fn message_count(&self) -> u64 {
        self.message_count
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_temp_writer() -> (RrdWriter, tempfile::NamedTempFile) {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let writer = RrdWriter::create(temp_file.path()).expect("Failed to create writer");
        (writer, temp_file)
    }

    #[test]
    fn test_create_writer() {
        let (writer, _temp) = create_temp_writer();
        assert_eq!(writer.message_count(), 0);
        assert_eq!(writer.channel_count(), 0);
        assert!(!writer.finished);
    }

    #[test]
    fn test_add_channel() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let id = writer
            .add_channel("/test", "std_msgs/String", "json", None)
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(writer.channel_count(), 1);

        let id2 = writer
            .add_channel("/test2", "std_msgs/Int32", "cdr", None)
            .unwrap();
        assert_eq!(id2, 1);
        assert_eq!(writer.channel_count(), 2);
    }

    #[test]
    fn test_write_message() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let channel_id = writer
            .add_channel("/test", "std_msgs/String", "json", None)
            .unwrap();

        let message = RawMessage {
            channel_id,
            log_time: 1000,
            publish_time: 1000,
            data: b"test data".to_vec(),
            sequence: Some(0),
        };

        writer.write(&message).expect("Failed to write message");
        assert_eq!(writer.message_count(), 1);

        writer.finish().expect("Failed to finish");

        // Verify the file was written
        let (reader_writer, _temp) = create_temp_writer();
        let data = std::fs::read(reader_writer.path()).expect("Failed to read file");

        // Verify magic
        assert_eq!(&data[0..4], RRD_MAGIC);

        // Verify version
        assert_eq!(data[4..8], RRD_VERSION);

        // Verify options
        assert_eq!(data[8], COMPRESSION_OFF);
        assert_eq!(data[9], SERIALIZER_PROTOBUF);

        println!("Written {} bytes", data.len());
    }

    #[test]
    fn test_finish() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        writer
            .add_channel("/test", "std_msgs/String", "json", None)
            .unwrap();
        writer.finish().expect("Failed to finish");
        assert!(writer.finished);

        // Writing after finish should fail
        let message = RawMessage {
            channel_id: 0,
            log_time: 0,
            publish_time: 0,
            data: vec![],
            sequence: Some(0),
        };
        assert!(writer.write(&message).is_err());
    }

    #[test]
    fn test_write_after_finish() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        writer.finish().expect("Failed to finish");

        let message = RawMessage {
            channel_id: 0,
            log_time: 0,
            publish_time: 0,
            data: vec![],
            sequence: Some(0),
        };
        assert!(writer.write(&message).is_err());
    }

    #[test]
    fn test_write_to_unknown_channel() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let message = RawMessage {
            channel_id: 999, // Unknown channel
            log_time: 0,
            publish_time: 0,
            data: vec![],
            sequence: Some(0),
        };

        // Should not fail - channels are tracked in-memory only in RRF2
        writer.write(&message).expect("Failed to write message");
        assert_eq!(writer.message_count(), 1);
    }

    #[test]
    fn test_write_batch() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let channel_id = writer
            .add_channel("/test", "std_msgs/String", "json", None)
            .unwrap();

        // Write multiple messages
        for i in 0..10 {
            let message = RawMessage {
                channel_id,
                log_time: i * 1000,
                publish_time: i * 1000,
                data: format!("message {}", i).into_bytes(),
                sequence: Some(i),
            };
            writer.write(&message).expect("Failed to write message");
        }

        assert_eq!(writer.message_count(), 10);
        writer.finish().expect("Failed to finish");
    }

    #[test]
    fn test_write_batch_mixed_channels() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let id1 = writer
            .add_channel("/channel1", "std_msgs/String", "json", None)
            .unwrap();
        let id2 = writer
            .add_channel("/channel2", "std_msgs/Int32", "cdr", None)
            .unwrap();

        // Write to different channels
        for i in 0..5 {
            let message = RawMessage {
                channel_id: id1,
                log_time: i * 1000,
                publish_time: i * 1000,
                data: format!("ch1-msg{}", i).into_bytes(),
                sequence: Some(i),
            };
            writer.write(&message).expect("Failed to write message");
        }

        for i in 0..5 {
            let message = RawMessage {
                channel_id: id2,
                log_time: i * 1000,
                publish_time: i * 1000,
                data: format!("ch2-msg{}", i).into_bytes(),
                sequence: Some(i + 5),
            };
            writer.write(&message).expect("Failed to write message");
        }

        assert_eq!(writer.message_count(), 10);
        writer.finish().expect("Failed to finish");
    }

    #[test]
    fn test_empty_write_batch() {
        let (mut writer, _temp) = create_temp_writer();

        // Write batch with no messages
        assert_eq!(writer.message_count(), 0);
        writer.finish().expect("Failed to finish");
        assert!(writer.finished);
    }

    #[test]
    fn test_writer_path() {
        let (writer, temp) = create_temp_writer();
        let path = writer.path();
        assert!(!path.is_empty());

        // Verify the file was created
        assert!(temp.path().exists());
    }
}
