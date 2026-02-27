// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transport-based MCAP reader using mcap::MessageStream.
//!
//! This module provides [`McapTransportReader`], which implements the
//! [`FormatReader`](crate::io::traits::FormatReader) trait using the
//! unified transport layer for I/O and the official mcap crate's
//! `MessageStream` for proper MCAP parsing including CHUNK handling.

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::io::metadata::{ChannelInfo, FileFormat, RawMessage};
use crate::io::traits::FormatReader;
use crate::io::transport::local::LocalTransport;
use crate::{CodecError, Result};

/// Transport-based MCAP reader.
///
/// This reader buffers data from the transport and uses the official
/// mcap crate's `MessageStream` for proper parsing, including CHUNK
/// record decompression.
pub struct McapTransportReader {
    /// File path (for reporting)
    path: String,
    /// All parsed message timestamps (for start/end time)
    message_timestamps: Vec<u64>,
    /// Discovered channels
    channels: HashMap<u16, ChannelInfo>,
    /// Parsed raw messages
    raw_messages: Vec<RawMessage>,
    /// File size
    file_size: u64,
}

impl McapTransportReader {
    /// Open a MCAP file from the local filesystem.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let transport = LocalTransport::open(path_ref).map_err(|e| {
            CodecError::encode(
                "IO",
                format!("Failed to open {}: {}", path_ref.display(), e),
            )
        })?;
        Self::with_transport(transport, path_ref.to_string_lossy().to_string())
    }

    /// Create from a LocalTransport.
    fn with_transport(
        mut transport: impl crate::io::transport::Transport,
        path: String,
    ) -> Result<Self> {
        let file_size = transport.len().unwrap_or(0);

        // Read all data from transport into buffer
        let buffer = Self::read_all_from_transport(&mut transport, &path)?;

        // Use mcap::MessageStream to parse the buffered data
        Self::parse_from_buffer(buffer, path, file_size)
    }

    /// Read all data from a transport into a buffer.
    fn read_all_from_transport(
        transport: &mut dyn crate::io::transport::Transport,
        path: &str,
    ) -> Result<Vec<u8>> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        const CHUNK_SIZE: usize = 64 * 1024;
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut result = Vec::new();

        // SAFETY: Transport is Unpin, pinning is temporary
        let mut pinned = unsafe { Pin::new_unchecked(transport) };

        loop {
            match pinned.as_mut().poll_read(&mut cx, &mut buffer) {
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(n)) => {
                    result.extend_from_slice(&buffer[..n]);
                }
                Poll::Ready(Err(e)) => {
                    return Err(CodecError::encode(
                        "Transport",
                        format!("Failed to read from {path}: {e}"),
                    ));
                }
                Poll::Pending => {
                    std::thread::yield_now();
                    continue;
                }
            }
        }

        Ok(result)
    }

    /// Parse MCAP data from a buffer.
    fn parse_from_buffer(buffer: Vec<u8>, path: String, file_size: u64) -> Result<Self> {
        let mut channels = HashMap::new();
        let mut message_timestamps = Vec::new();
        let mut raw_messages = Vec::new();

        // Use mcap::MessageStream for proper parsing
        let stream = mcap::MessageStream::new(&buffer).map_err(|e| {
            CodecError::parse(
                "MCAP",
                format!("Failed to create message stream for {path}: {e}"),
            )
        })?;

        for result in stream {
            match result {
                Ok(message) => {
                    let channel_id = message.channel.id;

                    // Store channel if not already seen
                    if let std::collections::hash_map::Entry::Vacant(e) = channels.entry(channel_id)
                    {
                        let schema = message.channel.schema.as_ref();
                        let schema_text =
                            schema.and_then(|s| String::from_utf8(s.data.to_vec()).ok());
                        let schema_data = schema.map(|s| s.data.to_vec());
                        let schema_encoding = schema.map(|s| s.encoding.clone());

                        e.insert(ChannelInfo {
                            id: channel_id,
                            topic: message.channel.topic.clone(),
                            message_type: schema.map(|s| s.name.clone()).unwrap_or_default(),
                            encoding: message.channel.message_encoding.clone(),
                            schema: schema_text,
                            schema_data,
                            schema_encoding,
                            message_count: 0,
                            callerid: None,
                        });
                    }

                    // Store message timestamp
                    message_timestamps.push(message.log_time);

                    // Store raw message
                    raw_messages.push(RawMessage {
                        channel_id,
                        log_time: message.log_time,
                        publish_time: message.publish_time,
                        data: message.data.to_vec(),
                        sequence: Some(u64::from(message.sequence)),
                    });
                }
                Err(e) => {
                    return Err(CodecError::parse(
                        "MCAP",
                        format!("Failed to parse message from {path}: {e}"),
                    ));
                }
            }
        }

        Ok(Self {
            path,
            message_timestamps,
            channels,
            raw_messages,
            file_size,
        })
    }
}

impl FormatReader for McapTransportReader {
    #[cfg(feature = "remote")]
    fn open_from_transport(
        mut transport: Box<dyn crate::io::transport::Transport>,
        path: String,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let file_size = transport.len().unwrap_or(0);
        let buffer = Self::read_all_from_transport(transport.as_mut(), &path)?;
        Self::parse_from_buffer(buffer, path, file_size)
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
    }

    fn message_count(&self) -> u64 {
        self.message_timestamps.len() as u64
    }

    fn start_time(&self) -> Option<u64> {
        self.message_timestamps.first().copied()
    }

    fn end_time(&self) -> Option<u64> {
        self.message_timestamps.last().copied()
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn format(&self) -> FileFormat {
        FileFormat::Mcap
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn iter_raw_boxed(&self) -> Result<crate::io::traits::RawMessageIter<'_>> {
        Ok(Box::new(self.raw_messages.iter().map(|msg| {
            let channel = self.channels.get(&msg.channel_id).cloned().ok_or_else(|| {
                CodecError::parse(
                    "McapTransportReader",
                    format!("Channel {} not found", msg.channel_id),
                )
            })?;
            Ok((msg.clone(), channel))
        })))
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

    #[test]
    fn test_transport_reader_creation() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/robocodec_test_0.mcap");

        if !path.exists() {
            return;
        }

        let reader = McapTransportReader::open(&path).unwrap();
        assert_eq!(reader.format(), FileFormat::Mcap);
        assert!(reader.message_count() > 0);
        assert!(!reader.channels().is_empty());
    }
}
