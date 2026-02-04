// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! RRD file writer with chunk-based compression.
#![allow(dead_code)]
//!
//! This module provides `RrdWriter` for writing Rerun RRD files.

use std::collections::HashMap;
use std::io::{Seek, Write};
use std::path::Path;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::core::{CodecError, Result};
use crate::io::metadata::{ChannelInfo, RawMessage};
use crate::io::traits::FormatWriter;

use super::constants::*;

/// RRD file writer.
///
/// Creates RRD files with LZ4 compression and chunk-based storage.
pub struct RrdWriter {
    /// Output file
    file: std::fs::File,
    /// File path
    path: String,
    /// Next channel ID
    next_channel_id: u16,
    /// Channels added to the file
    channels: HashMap<u16, ChannelInfo>,
    /// Current chunk buffer
    chunk_buffer: Vec<u8>,
    /// Messages in current chunk
    chunk_messages: Vec<RawMessage>,
    /// Total messages written
    message_count: u64,
    /// Current chunk size target
    chunk_size: usize,
    /// Compression type
    compression: u8,
    /// Schema encoding
    schema_encoding: u8,
    /// Start timestamp
    start_time: Option<u64>,
    /// End timestamp
    end_time: Option<u64>,
    /// Chunk index entries
    chunk_index: Vec<ChunkIndexEntry>,
    /// Finished flag
    finished: bool,
}

/// Chunk index entry.
#[derive(Debug, Clone)]
struct ChunkIndexEntry {
    /// Offset in file
    offset: u64,
    /// Compressed size
    size: u32,
    /// Uncompressed size
    uncompressed_size: u32,
    /// Start time
    time_start: u64,
    /// End time
    time_end: u64,
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

        // Write header placeholder (we'll rewrite it at the end)
        Self::write_header(&mut file)?;

        Ok(Self {
            file,
            path: path_str,
            next_channel_id: 0,
            channels: HashMap::new(),
            chunk_buffer: Vec::new(),
            chunk_messages: Vec::new(),
            message_count: 0,
            chunk_size: DEFAULT_CHUNK_SIZE,
            compression: COMPRESSION_LZ4,
            schema_encoding: SCHEMA_ENCODING_PROTOBUF,
            start_time: None,
            end_time: None,
            chunk_index: Vec::new(),
            finished: false,
        })
    }

    /// Create a new RRD writer with custom chunk size.
    pub fn create_with_chunk_size<P: AsRef<Path>>(path: P, chunk_size: usize) -> Result<Self> {
        let mut writer = Self::create(path)?;
        writer.chunk_size = chunk_size.min(MAX_CHUNK_SIZE);
        Ok(writer)
    }

    /// Create a new RRD writer with custom compression.
    pub fn create_with_compression<P: AsRef<Path>>(path: P, compression: u8) -> Result<Self> {
        let mut writer = Self::create(path)?;
        writer.compression = compression;
        Ok(writer)
    }

    /// Write the RRD file header.
    fn write_header<W: Write>(writer: &mut W) -> Result<()> {
        writer
            .write_all(RRD_MAGIC)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write magic: {}", e)))?;

        writer
            .write_u16::<LittleEndian>(RRD_VERSION)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write version: {}", e)))?;

        writer
            .write_u32::<LittleEndian>(0)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write flags: {}", e)))?;

        writer
            .write_u8(COMPRESSION_LZ4)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write compression: {}", e)))?;

        writer.write_u8(SCHEMA_ENCODING_PROTOBUF).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to write schema encoding: {}", e))
        })?;

        // Reserved bytes
        writer
            .write_all(&[0u8; 2])
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write reserved: {}", e)))?;

        writer
            .write_u32::<LittleEndian>(DEFAULT_CHUNK_SIZE as u32)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write chunk size: {}", e)))?;

        // Reserved bytes
        writer
            .write_all(&[0u8; 4])
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write reserved: {}", e)))?;

        writer
            .write_u64::<LittleEndian>(0)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write chunk count: {}", e)))?;

        Ok(())
    }

    /// Write the current chunk to the file.
    fn flush_chunk(&mut self) -> Result<()> {
        if self.chunk_buffer.is_empty() {
            return Ok(());
        }

        let chunk_start = self
            .file
            .stream_position()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to get position: {}", e)))?;

        let time_start = self.chunk_messages.first().map(|m| m.log_time).unwrap_or(0);
        let time_end = self.chunk_messages.last().map(|m| m.log_time).unwrap_or(0);

        // Compress chunk (for now, just write uncompressed)
        let uncompressed_size = self.chunk_buffer.len() as u32;
        let data = self.compress_chunk(&self.chunk_buffer)?;
        let compressed_size = data.len() as u32;

        // Write chunk header
        self.file
            .write_u32::<LittleEndian>(compressed_size)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write chunk size: {}", e)))?;

        self.file
            .write_u32::<LittleEndian>(uncompressed_size)
            .map_err(|e| {
                CodecError::parse("RRD", format!("Failed to write uncompressed size: {}", e))
            })?;

        self.file
            .write_u64::<LittleEndian>(time_start)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write time start: {}", e)))?;

        self.file
            .write_u64::<LittleEndian>(time_end)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write time end: {}", e)))?;

        // Write chunk data
        self.file
            .write_all(&data)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write chunk data: {}", e)))?;

        // Add to index
        self.chunk_index.push(ChunkIndexEntry {
            offset: chunk_start,
            size: compressed_size,
            uncompressed_size,
            time_start,
            time_end,
        });

        // Update time range
        if self.start_time.is_none() {
            self.start_time = Some(time_start);
        }
        self.end_time = Some(time_end);

        // Clear buffers
        self.chunk_buffer.clear();
        self.chunk_messages.clear();

        Ok(())
    }

    /// Compress a chunk using the configured compression.
    fn compress_chunk(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.compression {
            COMPRESSION_NONE => Ok(data.to_vec()),
            COMPRESSION_LZ4 => {
                // For now, return uncompressed (placeholder for LZ4)
                // Full implementation would use lz4_flex
                Ok(data.to_vec())
            }
            COMPRESSION_ZSTD => {
                // For now, return uncompressed (placeholder for Zstd)
                // Full implementation would use zstd
                Ok(data.to_vec())
            }
            _ => Err(CodecError::parse(
                "RRD",
                format!("Unknown compression type: {}", self.compression),
            )),
        }
    }

    /// Write the RRD file footer and finalize.
    fn write_footer(&mut self) -> Result<()> {
        // Flush any remaining data
        if !self.chunk_buffer.is_empty() {
            self.flush_chunk()?;
        }

        // Write chunk index
        for entry in &self.chunk_index {
            self.file
                .write_u64::<LittleEndian>(entry.offset)
                .map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to write index offset: {}", e))
                })?;

            self.file
                .write_u32::<LittleEndian>(entry.size)
                .map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to write index size: {}", e))
                })?;

            self.file
                .write_u32::<LittleEndian>(entry.uncompressed_size)
                .map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to write index uncompressed: {}", e))
                })?;

            self.file
                .write_u64::<LittleEndian>(entry.time_start)
                .map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to write index time start: {}", e))
                })?;

            self.file
                .write_u64::<LittleEndian>(entry.time_end)
                .map_err(|e| {
                    CodecError::parse("RRD", format!("Failed to write index time end: {}", e))
                })?;
        }

        // Write footer magic
        self.file.write_all(RRD_FOOTER_MAGIC).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to write footer magic: {}", e))
        })?;

        // Update header with chunk count
        let current_pos = self
            .file
            .stream_position()
            .map_err(|e| CodecError::parse("RRD", format!("Failed to get position: {}", e)))?;

        self.file.seek(std::io::SeekFrom::Start(24)).map_err(|e| {
            CodecError::parse("RRD", format!("Failed to seek to chunk count: {}", e))
        })?;

        self.file
            .write_u64::<LittleEndian>(self.chunk_index.len() as u64)
            .map_err(|e| CodecError::parse("RRD", format!("Failed to write chunk count: {}", e)))?;

        self.file
            .seek(std::io::SeekFrom::Start(current_pos))
            .map_err(|e| CodecError::parse("RRD", format!("Failed to seek back: {}", e)))?;

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
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        let id = self.next_channel_id;
        self.next_channel_id += 1;

        let channel = ChannelInfo {
            id,
            topic: topic.to_string(),
            message_type: message_type.to_string(),
            encoding: encoding.to_string(),
            schema: schema.map(|s| s.to_string()),
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

        // Update time range
        if self.start_time.is_none() {
            self.start_time = Some(message.log_time);
        }
        self.end_time = Some(message.log_time);

        // Add to chunk buffer
        // For now, just store the raw data
        // Full implementation would serialize as protobuf
        self.chunk_buffer.extend_from_slice(&message.data);
        self.chunk_messages.push(message.clone());

        // Check if we should flush the chunk
        if self.chunk_buffer.len() >= self.chunk_size {
            self.flush_chunk()?;
        }

        self.message_count += 1;

        // Update channel message count
        if let Some(channel) = self.channels.get_mut(&message.channel_id) {
            channel.message_count += 1;
        }

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
    use tempfile::NamedTempFile;

    fn create_temp_writer() -> (RrdWriter, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
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
            sequence: None,
        };

        writer.write(&message).unwrap();
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

        let messages = vec![
            RawMessage {
                channel_id,
                log_time: 1000,
                publish_time: 1000,
                data: b"data1".to_vec(),
                sequence: None,
            },
            RawMessage {
                channel_id,
                log_time: 2000,
                publish_time: 2000,
                data: b"data2".to_vec(),
                sequence: None,
            },
        ];

        writer.write_batch(&messages).unwrap();
        assert_eq!(writer.message_count(), 2);
    }

    #[test]
    fn test_finish() {
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
            data: b"test".to_vec(),
            sequence: None,
        };

        writer.write(&message).unwrap();
        writer.finish().unwrap();
        assert!(writer.finished);

        // Finishing again should be idempotent
        writer.finish().unwrap();
    }

    #[test]
    fn test_chunk_size() {
        let temp_path = std::env::temp_dir().join("test_chunk_size.rrd");
        let writer = RrdWriter::create_with_chunk_size(&temp_path, 1024).unwrap();
        assert_eq!(writer.chunk_size, 1024);
        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_max_chunk_size() {
        let temp_path = std::env::temp_dir().join("test_max_chunk_size.rrd");
        let writer = RrdWriter::create_with_chunk_size(&temp_path, MAX_CHUNK_SIZE * 2).unwrap();
        assert_eq!(writer.chunk_size, MAX_CHUNK_SIZE);
        std::fs::remove_file(&temp_path).ok();
    }

    #[test]
    fn test_add_channel_with_schema() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let schema = Some("string data");
        let id = writer
            .add_channel("/test", "std_msgs/String", "json", schema)
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(writer.channel_count(), 1);

        // Verify schema was stored
        let channel = &writer.channels[&0];
        assert_eq!(channel.schema.as_deref(), Some("string data"));
    }

    #[test]
    fn test_write_to_unknown_channel() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let message = RawMessage {
            channel_id: 999, // unknown channel
            log_time: 1000,
            publish_time: 1000,
            data: b"test".to_vec(),
            sequence: None,
        };

        // Write succeeds even for unknown channel (message is stored)
        // but channel count won't be updated
        let result = writer.write(&message);
        assert!(result.is_ok());
        assert_eq!(writer.message_count(), 1);
    }

    #[test]
    fn test_write_batch_mixed_channels() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let id1 = writer
            .add_channel("/ch1", "std_msgs/String", "json", None)
            .unwrap();
        let id2 = writer
            .add_channel("/ch2", "std_msgs/Int32", "cdr", None)
            .unwrap();

        let messages = vec![
            RawMessage {
                channel_id: id1,
                log_time: 1000,
                publish_time: 1000,
                data: b"data1".to_vec(),
                sequence: None,
            },
            RawMessage {
                channel_id: id2,
                log_time: 2000,
                publish_time: 2000,
                data: b"data2".to_vec(),
                sequence: None,
            },
        ];

        writer.write_batch(&messages).unwrap();
        assert_eq!(writer.message_count(), 2);
    }

    #[test]
    fn test_writer_path() {
        let writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        // Path should contain "tmp" (tempfile path)
        assert!(writer.path().len() > 0);
    }

    #[test]
    fn test_as_any() {
        let writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        // Test as_any
        let _any: &dyn std::any::Any = writer.as_any();
    }

    #[test]
    fn test_as_any_mut() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        // Test as_any_mut
        let _any_mut: &mut dyn std::any::Any = writer.as_any_mut();
    }

    #[test]
    fn test_empty_write_batch() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let messages: Vec<RawMessage> = vec![];
        writer.write_batch(&messages).unwrap();
        assert_eq!(writer.message_count(), 0);
    }

    #[test]
    fn test_compression_chunk() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let id = writer
            .add_channel("/test", "std_msgs/String", "json", None)
            .unwrap();

        // Write enough data to trigger chunk flush
        let large_data = vec![b'x'; 10000];
        for i in 0..10 {
            let message = RawMessage {
                channel_id: id,
                log_time: i as u64 * 1000,
                publish_time: i as u64 * 1000,
                data: large_data.clone(),
                sequence: None,
            };
            writer.write(&message).unwrap();
        }

        assert_eq!(writer.message_count(), 10);
    }

    #[test]
    fn test_write_after_finish() {
        let mut writer = {
            let (w, _temp) = create_temp_writer();
            w
        };

        let id = writer
            .add_channel("/test", "std_msgs/String", "json", None)
            .unwrap();
        let message = RawMessage {
            channel_id: id,
            log_time: 1000,
            publish_time: 1000,
            data: b"test".to_vec(),
            sequence: None,
        };

        writer.write(&message).unwrap();
        writer.finish().unwrap();

        // Writing after finish should fail
        let result = writer.write(&message);
        assert!(result.is_err());
    }
}
