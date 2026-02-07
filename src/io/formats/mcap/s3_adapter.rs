// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming adapter using the mcap crate's LinearReader.
//!
//! This module provides an adapter that integrates `mcap::LinearReader` with S3
//! streaming. The LinearReader uses an event-driven API that is perfect for
//! streaming scenarios where data arrives in chunks.

use std::collections::HashMap;

use crate::io::formats::mcap::constants::{OP_CHANNEL, OP_MESSAGE, OP_SCHEMA};
use crate::io::metadata::ChannelInfo;
use crate::io::s3::FatalError;

/// S3 streaming adapter using mcap::LinearReader.
///
/// This adapter wraps the mcap crate's LinearReader and provides a simple
/// chunk-based API suitable for S3 streaming. It processes MCAP records
/// incrementally as data arrives from S3.
pub struct McapS3Adapter {
    /// The underlying mcap LinearReader
    reader: mcap::sans_io::linear_reader::LinearReader,
    /// Discovered schemas indexed by schema ID
    schemas: HashMap<u16, SchemaInfo>,
    /// Discovered channels indexed by channel ID
    channels: HashMap<u16, ChannelRecordInfo>,
    /// Total messages parsed
    message_count: u64,
}

/// Schema information extracted from MCAP Schema records.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// Schema ID
    pub id: u16,
    /// Schema name (e.g., "sensor_msgs/msg/Image")
    pub name: String,
    /// Schema encoding (e.g., "ros2msg", "protobuf")
    pub encoding: String,
    /// Schema data
    pub data: Vec<u8>,
}

/// Channel information extracted from MCAP Channel records.
#[derive(Debug, Clone)]
pub struct ChannelRecordInfo {
    /// Channel ID
    pub id: u16,
    /// Topic name
    pub topic: String,
    /// Message encoding (e.g., "cdr", "protobuf", "json")
    pub message_encoding: String,
    /// Schema ID (0 if none)
    pub schema_id: u16,
}

/// Message data from MCAP Message records.
#[derive(Debug, Clone)]
pub struct MessageRecord {
    /// Channel ID
    pub channel_id: u16,
    /// Log timestamp (nanoseconds)
    pub log_time: u64,
    /// Publish timestamp (nanoseconds)
    pub publish_time: u64,
    /// Message data
    pub data: Vec<u8>,
    /// Sequence number
    pub sequence: u64,
}

impl McapS3Adapter {
    /// Create a new S3 adapter.
    pub fn new() -> Self {
        Self {
            reader: mcap::sans_io::linear_reader::LinearReader::new(),
            schemas: HashMap::new(),
            channels: HashMap::new(),
            message_count: 0,
        }
    }

    /// Process a chunk of data from S3.
    ///
    /// Returns any complete message records found in this chunk.
    /// Schema and Channel records are stored internally and accessible via `channels()`.
    pub fn process_chunk(&mut self, data: &[u8]) -> Result<Vec<MessageRecord>, FatalError> {
        let mut messages = Vec::new();

        // Insert data into the reader
        let len = data.len();
        self.reader.insert(len).copy_from_slice(data);
        self.reader.notify_read(len);

        // Process all available events
        while let Some(event) = self.reader.next_event() {
            let event =
                event.map_err(|e| FatalError::io_error(format!("MCAP parse error: {}", e)))?;

            match event {
                mcap::sans_io::linear_reader::LinearReadEvent::ReadRequest(_) => break,
                mcap::sans_io::linear_reader::LinearReadEvent::Record { opcode, data } => {
                    // Clone the data to avoid borrow checker issues
                    let data = data.to_vec();
                    self.process_record(opcode, &data, &mut messages)?;
                }
            }
        }

        self.message_count += messages.len() as u64;
        Ok(messages)
    }

    /// Process a single MCAP record.
    fn process_record(
        &mut self,
        opcode: u8,
        body: &[u8],
        messages: &mut Vec<MessageRecord>,
    ) -> Result<(), FatalError> {
        match opcode {
            OP_SCHEMA => {
                let schema = self.parse_schema(body)?;
                self.schemas.insert(schema.id, schema);
            }
            OP_CHANNEL => {
                let channel = self.parse_channel(body)?;
                self.channels.insert(channel.id, channel);
            }
            OP_MESSAGE => {
                let msg = self.parse_message(body)?;
                messages.push(msg);
            }
            // Ignore other records for streaming
            _ => {}
        }
        Ok(())
    }

    /// Parse a Schema record.
    fn parse_schema(&self, body: &[u8]) -> Result<SchemaInfo, FatalError> {
        const MIN_SCHEMA_LEN: usize = 4;

        if body.len() < MIN_SCHEMA_LEN {
            return Err(FatalError::invalid_format(
                "MCAP Schema record",
                body[..body.len().min(10)].to_vec(),
            ));
        }

        let id = u16::from_le_bytes(body[0..2].try_into().unwrap());
        let name_len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;

        if body.len() < 4 + name_len {
            return Err(FatalError::invalid_format(
                "MCAP Schema name (incomplete)",
                vec![],
            ));
        }

        let name = String::from_utf8(body[4..4 + name_len].to_vec())
            .map_err(|_| FatalError::invalid_format("MCAP Schema name (invalid UTF-8)", vec![]))?;

        let offset = 4 + name_len;
        if body.len() < offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Schema encoding length",
                vec![],
            ));
        }

        let encoding_len = u16::from_le_bytes(
            body[offset..offset + 2]
                .try_into()
                .expect("slice is exactly 2 bytes after len check"),
        ) as usize;
        if body.len() < offset + 2 + encoding_len {
            return Err(FatalError::invalid_format(
                "MCAP Schema encoding (incomplete)",
                vec![],
            ));
        }

        let encoding = String::from_utf8(body[offset + 2..offset + 2 + encoding_len].to_vec())
            .map_err(|_| {
                FatalError::invalid_format("MCAP Schema encoding (invalid UTF-8)", vec![])
            })?;

        let data_start = offset + 2 + encoding_len;
        let data = body[data_start..].to_vec();

        Ok(SchemaInfo {
            id,
            name,
            encoding,
            data,
        })
    }

    /// Parse a Channel record.
    fn parse_channel(&self, body: &[u8]) -> Result<ChannelRecordInfo, FatalError> {
        const MIN_CHANNEL_LEN: usize = 4;

        if body.len() < MIN_CHANNEL_LEN {
            return Err(FatalError::invalid_format(
                "MCAP Channel record",
                body[..body.len().min(10)].to_vec(),
            ));
        }

        let id = u16::from_le_bytes(body[0..2].try_into().unwrap());
        let topic_len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;

        if body.len() < 4 + topic_len {
            return Err(FatalError::invalid_format(
                "MCAP Channel topic (incomplete)",
                vec![],
            ));
        }

        let topic = String::from_utf8(body[4..4 + topic_len].to_vec()).map_err(|_| {
            FatalError::invalid_format("MCAP Channel topic (invalid UTF-8)", vec![])
        })?;

        let offset = 4 + topic_len;
        if body.len() < offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Channel encoding length",
                vec![],
            ));
        }

        let encoding_len = u16::from_le_bytes(
            body[offset..offset + 2]
                .try_into()
                .expect("slice is exactly 2 bytes after len check"),
        ) as usize;
        if body.len() < offset + 2 + encoding_len {
            return Err(FatalError::invalid_format(
                "MCAP Channel message encoding (incomplete)",
                vec![],
            ));
        }

        let message_encoding = String::from_utf8(
            body[offset + 2..offset + 2 + encoding_len].to_vec(),
        )
        .map_err(|_| FatalError::invalid_format("MCAP Channel encoding (invalid UTF-8)", vec![]))?;

        let schema_offset = offset + 2 + encoding_len;
        if body.len() < schema_offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Channel schema id (incomplete)",
                vec![],
            ));
        }

        let schema_id = u16::from_le_bytes(
            body[schema_offset..schema_offset + 2]
                .try_into()
                .expect("slice is exactly 2 bytes after len check"),
        );

        Ok(ChannelRecordInfo {
            id,
            topic,
            message_encoding,
            schema_id,
        })
    }

    /// Parse a Message record.
    fn parse_message(&self, body: &[u8]) -> Result<MessageRecord, FatalError> {
        const MESSAGE_HEADER_LEN: usize = 20;

        if body.len() < MESSAGE_HEADER_LEN {
            return Err(FatalError::invalid_format(
                "MCAP Message record",
                body[..body.len().min(10)].to_vec(),
            ));
        }

        let channel_id = u16::from_le_bytes(body[0..2].try_into().unwrap());
        let sequence = u64::from_le_bytes(body[2..10].try_into().unwrap());
        let log_time = u64::from_le_bytes(body[10..18].try_into().unwrap());
        let publish_time = u64::from_le_bytes(body[18..26].try_into().unwrap());

        let data = body[20..].to_vec();

        Ok(MessageRecord {
            channel_id,
            log_time,
            publish_time,
            data,
            sequence,
        })
    }

    /// Get all discovered channels as ChannelInfo.
    pub fn channels(&self) -> HashMap<u16, ChannelInfo> {
        self.channels
            .iter()
            .map(|(&id, ch)| {
                let schema = self.schemas.get(&ch.schema_id);
                let schema_text = schema.and_then(|s| String::from_utf8(s.data.clone()).ok());
                let schema_data = schema.map(|s| s.data.clone());
                let schema_encoding = schema.map(|s| s.encoding.clone());
                let message_type = schema.map(|s| s.name.clone()).unwrap_or_default();

                (
                    id,
                    ChannelInfo {
                        id,
                        topic: ch.topic.clone(),
                        message_type,
                        encoding: ch.message_encoding.clone(),
                        schema: schema_text,
                        schema_data,
                        schema_encoding,
                        message_count: 0,
                        callerid: None,
                    },
                )
            })
            .collect()
    }

    /// Get the total message count.
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Check if the parser has seen all channels.
    pub fn has_channels(&self) -> bool {
        !self.channels.is_empty()
    }
}

impl Default for McapS3Adapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adapter_new() {
        let adapter = McapS3Adapter::new();
        assert!(!adapter.has_channels());
        assert_eq!(adapter.message_count(), 0);
    }

    #[test]
    fn test_adapter_default() {
        let adapter = McapS3Adapter::default();
        assert_eq!(adapter.message_count(), 0);
    }

    #[test]
    fn test_schema_info() {
        let schema = SchemaInfo {
            id: 1,
            name: "test_msgs/Msg".to_string(),
            encoding: "ros2msg".to_string(),
            data: b"# definition".to_vec(),
        };
        assert_eq!(schema.id, 1);
        assert_eq!(schema.name, "test_msgs/Msg");
        assert_eq!(schema.encoding, "ros2msg");
    }

    #[test]
    fn test_channel_record_info() {
        let channel = ChannelRecordInfo {
            id: 1,
            topic: "/test".to_string(),
            message_encoding: "cdr".to_string(),
            schema_id: 0,
        };
        assert_eq!(channel.id, 1);
        assert_eq!(channel.topic, "/test");
        assert_eq!(channel.message_encoding, "cdr");
    }

    #[test]
    fn test_message_record() {
        let msg = MessageRecord {
            channel_id: 1,
            log_time: 1000,
            publish_time: 900,
            data: vec![1, 2, 3],
            sequence: 5,
        };
        assert_eq!(msg.channel_id, 1);
        assert_eq!(msg.log_time, 1000);
        assert_eq!(msg.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_channels_empty_initially() {
        let adapter = McapS3Adapter::new();
        assert!(adapter.channels().is_empty());
    }

    #[test]
    fn test_process_chunk_with_mcap_magic() {
        // Test that the adapter can handle MCAP magic bytes
        let mut adapter = McapS3Adapter::new();
        let magic = crate::io::formats::mcap::MCAP_MAGIC;
        let result = adapter.process_chunk(&magic);
        // Should succeed even with just magic (no records yet)
        assert!(result.is_ok());
    }

    #[test]
    fn test_message_record_fields() {
        let msg = MessageRecord {
            channel_id: 5,
            log_time: 999999,
            publish_time: 888888,
            data: vec![0xAB, 0xCD],
            sequence: 42,
        };
        assert_eq!(msg.channel_id, 5);
        assert_eq!(msg.log_time, 999999);
        assert_eq!(msg.publish_time, 888888);
        assert_eq!(msg.data, vec![0xAB, 0xCD]);
        assert_eq!(msg.sequence, 42);
    }
}
