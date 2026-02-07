// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transport-based MCAP reader.
//!
//! This module provides [`McapTransportReader`], which implements the
//! [`FormatReader`](crate::io::traits::FormatReader) trait using the
//! unified transport layer for I/O and the streaming parser for parsing.
//!
//! This provides a clean separation between I/O (transport) and parsing,
//! allowing the same reader to work with local files, S3, or any other
//! transport implementation.

use std::collections::HashMap;
use std::io::Read;

use crate::io::metadata::{ChannelInfo, FileFormat};
use crate::io::streaming::parser::StreamingParser;
use crate::io::traits::FormatReader;
use crate::io::transport::Transport;
use crate::io::transport::local::LocalTransport;
use crate::{CodecError, Result};

use super::s3_adapter::MessageRecord;
use super::streaming::McapStreamingParser;

/// Transport-based MCAP reader.
///
/// This reader uses the unified transport layer for I/O and the streaming
/// parser for MCAP parsing. It implements `FormatReader` for consistent
/// access across all robotics data formats.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::formats::mcap::McapTransportReader;
/// use robocodec::io::traits::FormatReader;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Open from local file using transport
/// let mut reader = McapTransportReader::open("data.mcap")?;
///
/// // Access channels
/// for (id, channel) in reader.channels() {
///     println!("Channel {}: {}", id, channel.topic);
/// }
/// # Ok(())
/// # }
/// ```
pub struct McapTransportReader {
    /// The streaming parser
    parser: McapStreamingParser,
    /// File path (for reporting)
    path: String,
    /// All parsed messages (for sequential iteration)
    messages: Vec<MessageRecord>,
    /// File size
    file_size: u64,
}

impl McapTransportReader {
    /// Open a MCAP file from the local filesystem.
    ///
    /// This is a convenience method that creates a `LocalTransport` and
    /// initializes the reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or is not a valid MCAP file.
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

    /// Create a new reader from a transport.
    ///
    /// This method reads the entire file through the transport to parse
    /// all messages. For large files, consider using streaming methods
    /// or the parallel reader instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be read or the data is
    /// not a valid MCAP file.
    pub fn with_transport(mut transport: LocalTransport, path: String) -> Result<Self> {
        let mut parser = McapStreamingParser::new();
        let mut messages = Vec::new();
        let file_size = transport.len().unwrap_or(0);

        let chunk_size = 64 * 1024; // 64KB chunks
        let mut buffer = vec![0u8; chunk_size];
        let mut total_read = 0;

        // Read and parse the entire file
        loop {
            let n = transport.file_mut().read(&mut buffer).map_err(|e| {
                CodecError::encode("Transport", format!("Failed to read from {}: {}", path, e))
            })?;

            if n == 0 {
                break;
            }
            total_read += n;

            match parser.parse_chunk(&buffer[..n]) {
                Ok(chunk_messages) => {
                    messages.extend(chunk_messages);
                }
                Err(_) if total_read == n && n < 8 => {
                    // Empty or very short file - might be valid but with no messages
                    break;
                }
                Err(e) => {
                    return Err(CodecError::parse(
                        "MCAP",
                        format!("Failed to parse MCAP data at {}: {}", path, e),
                    ));
                }
            }
        }

        Ok(Self {
            parser,
            path,
            messages,
            file_size,
        })
    }

    /// Get all parsed messages.
    pub fn messages(&self) -> &[MessageRecord] {
        &self.messages
    }

    /// Get the streaming parser.
    pub fn parser(&self) -> &McapStreamingParser {
        &self.parser
    }

    /// Get a mutable reference to the streaming parser.
    pub fn parser_mut(&mut self) -> &mut McapStreamingParser {
        &mut self.parser
    }
}

impl FormatReader for McapTransportReader {
    fn open_from_transport(
        mut transport: Box<dyn crate::io::transport::Transport>,
        path: String,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let mut parser = McapStreamingParser::new();
        let mut messages = Vec::new();
        let file_size = transport.len().unwrap_or(0);

        // Read all data from the transport using poll-based interface
        use std::pin::Pin;
        use std::task::{Context, Poll, Waker};

        // Create a no-op waker for polling
        let waker = Waker::noop();
        let mut cx = Context::from_waker(&waker);

        const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut total_read = 0;

        // SAFETY: The transport is pinned for the duration of this block.
        // We don't move it after creating the Pin, and we drop it at the end
        // of the function when we're done with it.
        let mut pinned_transport = unsafe { Pin::new_unchecked(transport.as_mut()) };

        // Read and parse the entire file
        loop {
            match pinned_transport.as_mut().poll_read(&mut cx, &mut buffer) {
                Poll::Ready(Ok(n)) if n == 0 => break,
                Poll::Ready(Ok(n)) => {
                    total_read += n;

                    match parser.parse_chunk(&buffer[..n]) {
                        Ok(chunk_messages) => {
                            messages.extend(chunk_messages);
                        }
                        Err(_) if total_read == n && n < 8 => {
                            // Empty or very short file - might be valid but with no messages
                            break;
                        }
                        Err(e) => {
                            return Err(CodecError::parse(
                                "MCAP",
                                format!("Failed to parse MCAP data at {}: {}", path, e),
                            ));
                        }
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Err(CodecError::encode(
                        "Transport",
                        format!("Failed to read from {}: {}", path, e),
                    ));
                }
                Poll::Pending => {
                    return Err(CodecError::encode(
                        "Transport",
                        "Unexpected pending from non-async transport".to_string(),
                    ));
                }
            }
        }

        Ok(Self {
            parser,
            path,
            messages,
            file_size,
        })
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        self.parser.channels()
    }

    fn message_count(&self) -> u64 {
        self.parser.message_count()
    }

    fn start_time(&self) -> Option<u64> {
        self.messages.first().map(|m| m.log_time)
    }

    fn end_time(&self) -> Option<u64> {
        self.messages.last().map(|m| m.log_time)
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
    fn test_message_record_fields() {
        let msg = MessageRecord {
            channel_id: 5,
            log_time: 1234567890,
            publish_time: 1234567800,
            data: vec![0x01, 0x02, 0x03],
            sequence: 99,
        };
        assert_eq!(msg.channel_id, 5);
        assert_eq!(msg.log_time, 1234567890);
        assert_eq!(msg.data, vec![0x01, 0x02, 0x03]);
    }
}
