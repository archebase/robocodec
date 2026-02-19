// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transport-based RRD reader.
//!
//! This module provides [`RrdTransportReader`], which implements the
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
//! use robocodec::io::formats::rrd::RrdTransportReader;
//! use robocodec::io::traits::FormatReader;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open from local file using transport
//! let reader = RrdTransportReader::open("data.rrd")?;
//!
//! // Access channels
//! for (id, channel) in reader.channels() {
//!     println!("Channel {}: {}", id, channel.topic);
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::io::formats::rrd::stream::{RrdMessageRecord, StreamingRrdParser};
use crate::io::metadata::{ChannelInfo, FileFormat};
use crate::io::streaming::StreamingParser;
use crate::io::traits::FormatReader;
use crate::io::transport::local::LocalTransport;
use crate::io::transport::Transport;
use crate::{CodecError, Result};

/// Transport-based RRD reader.
///
/// This reader uses the unified transport layer for I/O and the streaming
/// parser for RRD parsing. It implements `FormatReader` for consistent
/// access across all robotics data formats.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::formats::rrd::RrdTransportReader;
/// use robocodec::io::traits::FormatReader;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Open from local file using transport
/// let reader = RrdTransportReader::open("data.rrd")?;
///
/// // Access channels
/// for (id, channel) in reader.channels() {
///     println!("Channel {}: {}", id, channel.topic);
/// }
/// # Ok(())
/// # }
/// ```
pub struct RrdTransportReader {
    /// The streaming parser
    parser: StreamingRrdParser,
    /// File path (for reporting)
    path: String,
    /// All parsed messages
    messages: Vec<RrdMessageRecord>,
    /// File size
    file_size: u64,
    /// Channel information indexed by channel ID
    channels: HashMap<u16, ChannelInfo>,
}

impl RrdTransportReader {
    /// Open an RRD file from the local filesystem.
    ///
    /// This is a convenience method that creates a `LocalTransport` and
    /// initializes the reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or is not a valid RRD file.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::formats::rrd::RrdTransportReader;
    /// use robocodec::io::traits::FormatReader;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = RrdTransportReader::open("data.rrd")?;
    /// println!("Opened RRD with {} channels", reader.channels().len());
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
    /// all messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be read or the data is
    /// not a valid RRD file.
    fn with_transport(mut transport: LocalTransport, path: String) -> Result<Self> {
        use std::io::Read;

        let mut parser = StreamingRrdParser::new();
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
                Err(_) if total_read == n && n < 12 => {
                    // Empty or very short file - might be valid but with no messages
                    break;
                }
                Err(e) => {
                    return Err(CodecError::parse(
                        "RRD",
                        format!("Failed to parse RRD data at {path}: {e}"),
                    ));
                }
            }
        }

        // Build channels from parser
        let channels = parser.channels().clone();

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
    pub fn messages(&self) -> &[RrdMessageRecord] {
        &self.messages
    }

    /// Get the streaming parser.
    #[must_use]
    pub fn parser(&self) -> &StreamingRrdParser {
        &self.parser
    }

    /// Get a mutable reference to the streaming parser.
    pub fn parser_mut(&mut self) -> &mut StreamingRrdParser {
        &mut self.parser
    }

    /// Convert an RRD message record to a raw message with channel info.
    ///
    /// This helper method creates a `RawMessage` from an `RrdMessageRecord`,
    /// using the message index to look up the channel information.
    fn message_to_raw(
        &self,
        msg: &RrdMessageRecord,
    ) -> Option<(crate::io::metadata::RawMessage, ChannelInfo)> {
        // RRD uses channel_id 0 for all ArrowMsg messages
        let channel = self.channels.get(&0)?;

        let raw_msg = crate::io::metadata::RawMessage {
            channel_id: 0,
            log_time: msg.index, // Use message index as log_time (RRD doesn't have timestamps in the same way)
            publish_time: msg.index,
            data: msg.data.clone(),
            sequence: Some(msg.index),
        };

        Some((raw_msg, channel.clone()))
    }
}

impl FormatReader for RrdTransportReader {
    #[cfg(feature = "remote")]
    fn open_from_transport(mut transport: Box<dyn Transport>, path: String) -> Result<Self>
    where
        Self: Sized,
    {
        let mut parser = StreamingRrdParser::new();
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

        // SAFETY: Using `Pin::new_unchecked` here is safe because:
        // 1. The `Transport` trait requires `Unpin`
        // 2. The transport is a mutable reference that we pin in place
        // 3. The pinned reference is only used within this function
        // 4. No interior mutability is violated
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
                        Err(_) if total_read == n && n < 12 => {
                            // Empty or very short file - might be valid but with no messages
                            break;
                        }
                        Err(e) => {
                            return Err(CodecError::parse(
                                "RRD",
                                format!("Failed to parse RRD data at {path}: {e}"),
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

        // Build channels from parser
        let channels = parser.channels().clone();

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
        // RRD doesn't have explicit timestamps, use message index
        self.messages.first().map(|m| m.index)
    }

    fn end_time(&self) -> Option<u64> {
        // RRD doesn't have explicit timestamps, use message index
        self.messages.last().map(|m| m.index)
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn format(&self) -> FileFormat {
        FileFormat::Rrd
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
        let iter = RrdTransportRawIter::new(self);
        Ok(Box::new(iter))
    }
}

/// Iterator over raw messages from a RrdTransportReader.
struct RrdTransportRawIter<'a> {
    reader: &'a RrdTransportReader,
    index: usize,
}

impl<'a> RrdTransportRawIter<'a> {
    fn new(reader: &'a RrdTransportReader) -> Self {
        Self { reader, index: 0 }
    }
}

impl<'a> Iterator for RrdTransportRawIter<'a> {
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
                    "RrdTransportReader",
                    format!("Channel not found for message index {}", msg.index),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_rrd_message_record_fields() {
        let msg = RrdMessageRecord {
            kind: crate::io::formats::rrd::stream::MessageKind::ArrowMsg,
            topic: "/test".to_string(),
            data: vec![0x01, 0x02, 0x03],
            index: 5,
        };
        assert_eq!(msg.topic, "/test");
        assert_eq!(msg.index, 5);
        assert_eq!(msg.data, vec![0x01, 0x02, 0x03]);
    }

    /// Helper to create a minimal RRD file header
    fn create_test_rrd_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        // Write RRD magic (RRF2)
        file.write_all(b"RRF2").unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_rrd_transport_reader_open_nonexistent() {
        let result = RrdTransportReader::open("/nonexistent/path/file.rrd");
        assert!(result.is_err());
    }

    #[test]
    fn test_rrd_transport_reader_open_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let result = RrdTransportReader::open(file.path());
        // Empty file behavior - may succeed with no messages or fail depending on implementation
        match result {
            Ok(reader) => {
                // If it succeeds, should have no messages
                assert_eq!(reader.message_count(), 0);
            }
            Err(_) => {
                // Or it may fail to parse - both are acceptable
            }
        }
    }

    #[test]
    fn test_rrd_transport_reader_file_size() {
        // Get the manifest directory for fixtures
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        // Find first RRD file
        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let fixture_path = rrd_file.unwrap();
        let reader = RrdTransportReader::open(&fixture_path).unwrap();
        let metadata = std::fs::metadata(&fixture_path).unwrap();

        assert_eq!(reader.file_size(), metadata.len());
        assert_eq!(reader.path(), fixture_path.to_string_lossy().as_ref());
    }

    #[test]
    fn test_rrd_transport_reader_channels() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();

        // Should have at least one channel
        assert!(!reader.channels().is_empty(), "Should have channels");

        // Test channels() method returns correct data
        let channels = reader.channels();
        for (id, channel) in channels {
            assert!(
                !channel.topic.is_empty(),
                "Channel {} should have topic",
                id
            );
        }
    }

    #[test]
    fn test_rrd_transport_reader_message_count() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();

        // Should have messages
        assert!(reader.message_count() > 0, "Should have messages");

        // Test that message_count is consistent
        let count = reader.message_count();
        assert_eq!(
            reader.message_count(),
            count,
            "Message count should be consistent"
        );
    }

    #[test]
    fn test_rrd_transport_reader_timestamps() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();

        let start = reader.start_time();
        let end = reader.end_time();

        // RRD uses message indices as timestamps
        assert!(start.is_some(), "Should have start index");
        assert!(end.is_some(), "Should have end index");
        assert!(
            end.unwrap() >= start.unwrap(),
            "End index should be >= start index"
        );
    }

    #[test]
    fn test_rrd_transport_reader_iter_raw() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();
        let expected_count = reader.message_count();

        let iter = reader.iter_raw_boxed().unwrap();
        let count = iter.filter(|r| r.is_ok()).count() as u64;

        assert_eq!(count, expected_count, "Iterator should return all messages");
    }

    #[test]
    fn test_rrd_transport_reader_format() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();
        assert_eq!(reader.format(), FileFormat::Rrd);
    }

    #[test]
    fn test_rrd_transport_reader_as_any() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();

        // Test as_any
        let any_ref = reader.as_any();
        assert!(any_ref.downcast_ref::<RrdTransportReader>().is_some());
    }

    #[test]
    fn test_rrd_transport_reader_parser_accessors() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let mut reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();

        // Test parser() accessor
        let _parser = reader.parser();

        // Test parser_mut() accessor
        let _parser_mut = reader.parser_mut();

        // Test messages() accessor
        let _messages = reader.messages();
    }

    #[test]
    fn test_rrd_transport_reader_file_info() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();
        let info = reader.file_info();

        assert_eq!(info.format, FileFormat::Rrd);
        assert!(!info.channels.is_empty());
        assert!(info.message_count > 0);
        assert!(info.size > 0);
    }

    /// Test multiple RRD fixtures
    #[test]
    fn test_rrd_transport_reader_multiple_fixtures() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    if count >= 5 {
                        break;
                    }

                    let fixture_name = path.file_name().unwrap().to_string_lossy();
                    let reader = RrdTransportReader::open(&path)
                        .unwrap_or_else(|_| panic!("Failed to open {}", fixture_name));

                    assert!(
                        !reader.channels().is_empty(),
                        "{} should have channels",
                        fixture_name
                    );
                    assert!(
                        reader.message_count() > 0,
                        "{} should have messages",
                        fixture_name
                    );

                    count += 1;
                }
            }
        }

        assert!(count > 0, "Should have tested at least one RRD fixture");
    }

    #[test]
    fn test_rrd_transport_reader_as_any_mut() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let rrd_dir = manifest_dir.join("tests/fixtures/rrd");

        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        if rrd_file.is_none() {
            eprintln!("Skipping test: no RRD fixtures found");
            return;
        }

        let mut reader = RrdTransportReader::open(&rrd_file.unwrap()).unwrap();

        // Test as_any_mut
        let any_ref = reader.as_any_mut();
        assert!(any_ref.downcast_ref::<RrdTransportReader>().is_some());
    }
}
