// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transport-based BAG reader.
//!
//! This module provides [`BagTransportReader`], which implements the
//! [`FormatReader`](crate::io::traits::FormatReader) trait using the
//! unified transport layer for I/O and the streaming parser for parsing.
//!
//! This provides a clean separation between I/O (transport) and parsing,
//! allowing the same reader to work with local files, S3, or any other
//! transport implementation.
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::formats::bag::BagTransportReader;
//! use robocodec::io::traits::FormatReader;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open from local file using transport
//! let reader = BagTransportReader::open("data.bag")?;
//!
//! // Access channels
//! for (id, channel) in reader.channels() {
//!     println!("Channel {}: {}", id, channel.topic);
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::io::formats::bag::stream::{BagMessageRecord, StreamingBagParser};
use crate::io::metadata::{ChannelInfo, FileFormat};
use crate::io::traits::FormatReader;
use crate::io::transport::Transport;
use crate::io::transport::local::LocalTransport;
use crate::{CodecError, Result};

/// Transport-based BAG reader.
///
/// This reader uses the unified transport layer for I/O and the streaming
/// parser for BAG parsing. It implements `FormatReader` for consistent
/// access across all robotics data formats.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::formats::bag::BagTransportReader;
/// use robocodec::io::traits::FormatReader;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Open from local file using transport
/// let reader = BagTransportReader::open("data.bag")?;
///
/// // Access channels
/// for (id, channel) in reader.channels() {
///     println!("Channel {}: {}", id, channel.topic);
/// }
/// # Ok(())
/// # }
/// ```
pub struct BagTransportReader {
    /// The streaming parser
    parser: StreamingBagParser,
    /// File path (for reporting)
    path: String,
    /// All parsed messages
    messages: Vec<BagMessageRecord>,
    /// File size
    file_size: u64,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
}

impl BagTransportReader {
    /// Open a BAG file from the local filesystem.
    ///
    /// This is a convenience method that creates a `LocalTransport` and
    /// initializes the reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or is not a valid BAG file.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::formats::bag::BagTransportReader;
    /// use robocodec::io::traits::FormatReader;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = BagTransportReader::open("data.bag")?;
    /// println!("Opened BAG with {} channels", reader.channels().len());
    /// # Ok(())
    /// # }
    /// ```
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

    /// Create a new reader from a `LocalTransport`.
    ///
    /// This method reads the entire file through the transport to parse
    /// all messages. For large files, consider using the parallel reader
    /// with memory-mapped files instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be read or the data is
    /// not a valid BAG file.
    fn with_transport(mut transport: LocalTransport, path: String) -> Result<Self> {
        use std::io::Read;

        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();
        let file_size = transport.len().unwrap_or(0);

        let chunk_size = 64 * 1024; // 64KB chunks
        let mut buffer = vec![0u8; chunk_size];
        let mut total_read = 0;

        // Read and parse the entire file
        loop {
            let n = transport.file_mut().read(&mut buffer).map_err(|e| {
                CodecError::encode("Transport", format!("Failed to read from {path}: {e}"))
            })?;

            if n == 0 {
                break;
            }
            total_read += n;

            match parser.parse_chunk(&buffer[..n]) {
                Ok(chunk_messages) => {
                    messages.extend(chunk_messages);
                }
                Err(_) if total_read == n && n < 13 => {
                    // Empty or very short file - might be valid but with no messages
                    break;
                }
                Err(e) => {
                    return Err(CodecError::parse(
                        "BAG",
                        format!("Failed to parse BAG data at {path}: {e}"),
                    ));
                }
            }
        }

        // Build channels from parser connections
        let channels = parser.channels();

        Ok(Self {
            parser,
            path,
            messages,
            file_size,
            channels,
        })
    }

    /// Get all parsed messages.
    #[must_use]
    pub fn messages(&self) -> &[BagMessageRecord] {
        &self.messages
    }

    /// Get the streaming parser.
    #[must_use]
    pub fn parser(&self) -> &StreamingBagParser {
        &self.parser
    }

    /// Get a mutable reference to the streaming parser.
    pub fn parser_mut(&mut self) -> &mut StreamingBagParser {
        &mut self.parser
    }

    /// Convert a BAG message record to a raw message with channel info.
    ///
    /// This helper method creates a `RawMessage` from a `BagMessageRecord`,
    /// using the connection ID to look up the channel information.
    fn message_to_raw(
        &self,
        msg: &BagMessageRecord,
    ) -> Option<(crate::io::metadata::RawMessage, ChannelInfo)> {
        let channel = self.channels.get(&(msg.conn_id as u16))?;

        let raw_msg = crate::io::metadata::RawMessage {
            channel_id: msg.conn_id as u16,
            log_time: msg.log_time,
            publish_time: msg.log_time, // BAG doesn't have separate publish time
            data: msg.data.clone(),
            sequence: None, // BAG doesn't have sequence numbers
        };

        Some((raw_msg, channel.clone()))
    }
}

impl FormatReader for BagTransportReader {
    #[cfg(feature = "remote")]
    fn open_from_transport(mut transport: Box<dyn Transport>, path: String) -> Result<Self>
    where
        Self: Sized,
    {
        let mut parser = StreamingBagParser::new();
        let mut messages = Vec::new();
        let file_size = transport.len().unwrap_or(0);

        // Read all data from the transport using poll-based interface
        use std::pin::Pin;
        use std::task::{Context, Poll, Waker};

        // Create a no-op waker for polling
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut total_read = 0;

        // # Safety
        //
        // Using `Pin::new_unchecked` here is safe because:
        //
        // 1. **Unpin requirement**: The `Transport` trait requires `Unpin`, which means
        //    the transport can be safely moved. However, `poll_read` requires a `Pin`,
        //    so we need to create one.
        //
        // 2. **No movement**: The transport is a mutable reference (`transport.as_mut()`)
        //    that we pin in place. We never move the transport after pinning it.
        //
        // 3. **Local scope**: The pinned reference is only used within this function
        //    and never escapes. It's dropped when the function returns.
        //
        // 4. **No interior mutability**: The transport's implementation of `poll_read`
        //    doesn't rely on interior mutability that would be violated by moving.
        //
        // The `new_unchecked` is necessary because we have a mutable reference to
        //    a trait object that already satisfies `Unpin`, but there's no safe way
        //    to create a Pin from a mutable reference to a trait object.
        let mut pinned_transport = unsafe { Pin::new_unchecked(transport.as_mut()) };

        // Read and parse the entire file
        loop {
            match pinned_transport.as_mut().poll_read(&mut cx, &mut buffer) {
                Poll::Ready(Ok(0)) => break,
                Poll::Ready(Ok(n)) => {
                    total_read += n;

                    match parser.parse_chunk(&buffer[..n]) {
                        Ok(chunk_messages) => {
                            messages.extend(chunk_messages);
                        }
                        Err(_) if total_read == n && n < 13 => {
                            // Empty or very short file - might be valid but with no messages
                            break;
                        }
                        Err(e) => {
                            return Err(CodecError::parse(
                                "BAG",
                                format!("Failed to parse BAG data at {path}: {e}"),
                            ));
                        }
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Err(CodecError::encode(
                        "Transport",
                        format!("Failed to read from {path}: {e}"),
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

        // Build channels from parser connections
        let channels = parser.channels();

        Ok(Self {
            parser,
            path,
            messages,
            file_size,
            channels,
        })
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        &self.channels
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
        FileFormat::Bag
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

    fn iter_raw_boxed(
        &self,
    ) -> Result<
        Box<
            dyn Iterator<Item = Result<(crate::io::metadata::RawMessage, ChannelInfo)>> + Send + '_,
        >,
    > {
        let iter = BagTransportRawIter::new(self);
        Ok(Box::new(iter))
    }
}

/// Iterator over raw messages from a BagTransportReader.
struct BagTransportRawIter<'a> {
    reader: &'a BagTransportReader,
    index: usize,
}

impl<'a> BagTransportRawIter<'a> {
    fn new(reader: &'a BagTransportReader) -> Self {
        Self { reader, index: 0 }
    }
}

impl<'a> Iterator for BagTransportRawIter<'a> {
    type Item = Result<(crate::io::metadata::RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.reader.messages.len() {
            return None;
        }

        let msg = &self.reader.messages[self.index];
        self.index += 1;

        match self.reader.message_to_raw(msg) {
            Some((raw_msg, channel)) => Some(Ok((raw_msg, channel))),
            None => {
                // Channel not found - this shouldn't happen if parsing succeeded
                Some(Err(CodecError::parse(
                    "BagTransportReader",
                    format!("Channel not found for connection ID {}", msg.conn_id),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bag_message_record_fields() {
        let msg = BagMessageRecord {
            conn_id: 5,
            log_time: 1234567890,
            data: vec![0x01, 0x02, 0x03],
        };
        assert_eq!(msg.conn_id, 5);
        assert_eq!(msg.log_time, 1234567890);
        assert_eq!(msg.data, vec![0x01, 0x02, 0x03]);
    }
}
