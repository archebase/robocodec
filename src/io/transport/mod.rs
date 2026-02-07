// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Transport layer for robotics data formats.
//!
//! This module provides a generic abstraction over different data sources
//! (local files, S3, HTTP, etc.) that can be used by format-specific parsers.
//!
//! # Architecture
//!
//! - **[`Transport`]** - Async trait for unified byte I/O
//! - **[`TransportExt`]** - Convenience extension trait
//! - **[`local`]** - Local file transport implementation
//! - **[`s3`]** - S3 transport implementation
//! - **[`http`]** - HTTP transport implementation
//! - **[`memory`]** - In-memory transport implementation for testing
//! - **[`ByteStream`]** - Legacy sync trait (deprecated)

pub mod http;
pub mod local;
pub mod memory;
pub mod s3;
pub mod transport;

use std::io;

// Re-export core transport types
pub use transport::{Transport, TransportExt};
// Re-export transport implementations
pub use http::HttpTransport;
pub use memory::MemoryTransport;

/// Generic byte stream trait for reading data from various transports.
///
/// This trait abstracts over different data sources (local files, S3, HTTP, etc.)
/// allowing format-specific parsers to work with any transport.
///
/// # Example
///
/// The async `Transport` trait is the primary API:
///
/// ```rust,no_run
/// use robocodec::io::transport::{Transport, TransportExt, local::LocalTransport};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Local file transport
/// let mut stream = LocalTransport::open("data.mcap")?;
/// let mut buffer = vec![0u8; 1024];
/// let n = stream.read(&mut buffer).await?;
/// # Ok(())
/// # }
/// ```
pub trait ByteStream: Send + Sync {
    /// Read bytes into the given buffer.
    ///
    /// Returns the number of bytes read. May return 0 if no bytes are
    /// currently available but more may come later (for streaming).
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Seek to a specific offset in the stream.
    ///
    /// Returns the new position. Returns an error if seeking is not
    /// supported by this stream (e.g., for pure streaming sources).
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64>;

    /// Get the current position in the stream.
    fn position(&self) -> u64;

    /// Get the total length of the stream, if known.
    ///
    /// Returns `None` for streams of unknown length (e.g., HTTP chunked encoding).
    fn len(&self) -> Option<u64>;

    /// Check if the stream is empty.
    fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// Check if this stream supports seeking.
    fn can_seek(&self) -> bool {
        true
    }

    /// Read all remaining bytes into a vector.
    ///
    /// This is a convenience method that repeatedly calls `read` until
    /// the stream is exhausted.
    fn read_to_end(&mut self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];

        loop {
            let n = self.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }

        Ok(buf)
    }
}

/// Extension trait for turning byte streams into chunk iterators.
pub trait ByteStreamExt: ByteStream {
    /// Read data in chunks of a specific size.
    ///
    /// Returns an iterator that yields chunks of bytes. Each chunk will be
    /// at most `chunk_size` bytes, except possibly the last chunk which may
    /// be smaller.
    fn chunks(self, chunk_size: usize) -> ChunkIterator<Self>
    where
        Self: Sized,
    {
        ChunkIterator::new(self, chunk_size)
    }
}

impl<T: ByteStream> ByteStreamExt for T {}

/// Iterator that reads chunks from a byte stream.
pub struct ChunkIterator<S: ByteStream> {
    stream: Option<S>,
    chunk_size: usize,
}

impl<S: ByteStream> ChunkIterator<S> {
    /// Create a new chunk iterator.
    fn new(stream: S, chunk_size: usize) -> Self {
        Self {
            stream: Some(stream),
            chunk_size,
        }
    }
}

impl<S: ByteStream> Iterator for ChunkIterator<S> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let stream = self.stream.as_mut()?;
        let mut buf = vec![0u8; self.chunk_size];
        match stream.read(&mut buf) {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some(Ok(buf))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock stream for testing
    struct MockStream {
        data: Vec<u8>,
        pos: usize,
        can_seek: bool,
    }

    impl MockStream {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                pos: 0,
                can_seek: true,
            }
        }

        fn with_seeking(mut self, can_seek: bool) -> Self {
            self.can_seek = can_seek;
            self
        }
    }

    impl ByteStream for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let remaining = self.data.len() - self.pos;
            if remaining == 0 {
                return Ok(0);
            }
            let to_read = buf.len().min(remaining);
            buf[..to_read].copy_from_slice(&self.data[self.pos..self.pos + to_read]);
            self.pos += to_read;
            Ok(to_read)
        }

        fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            if !self.can_seek {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "seek not supported",
                ));
            }
            let new_pos = match pos {
                io::SeekFrom::Start(n) => n as usize,
                io::SeekFrom::End(n) => self.data.len().saturating_add_signed(n as isize),
                io::SeekFrom::Current(n) => self.pos.saturating_add_signed(n as isize),
            };
            self.pos = new_pos.min(self.data.len());
            Ok(self.pos as u64)
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn len(&self) -> Option<u64> {
            Some(self.data.len() as u64)
        }

        fn can_seek(&self) -> bool {
            self.can_seek
        }
    }

    #[test]
    fn test_byte_stream_read() {
        let mut stream = MockStream::new(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 3];
        assert_eq!(stream.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf, &[1, 2, 3]);
        assert_eq!(stream.position(), 3);
    }

    #[test]
    fn test_byte_stream_read_to_end() {
        let mut stream = MockStream::new(vec![1, 2, 3, 4, 5]);
        let data = stream.read_to_end().unwrap();
        assert_eq!(data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_byte_stream_seek() {
        let mut stream = MockStream::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(stream.seek(io::SeekFrom::Start(2)).unwrap(), 2);
        assert_eq!(stream.position(), 2);
        assert_eq!(stream.seek(io::SeekFrom::Current(1)).unwrap(), 3);
        assert_eq!(stream.seek(io::SeekFrom::End(-1)).unwrap(), 4);
    }

    #[test]
    fn test_byte_stream_len() {
        let stream = MockStream::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(stream.len(), Some(5));
        assert!(!stream.is_empty());
    }

    #[test]
    fn test_byte_stream_is_empty() {
        let stream = MockStream::new(vec![]);
        assert_eq!(stream.len(), Some(0));
        assert!(stream.is_empty());
    }

    #[test]
    fn test_byte_stream_can_seek() {
        let stream = MockStream::new(vec![1, 2, 3]).with_seeking(true);
        assert!(stream.can_seek());
        let stream = MockStream::new(vec![1, 2, 3]).with_seeking(false);
        assert!(!stream.can_seek());
    }

    #[test]
    fn test_chunk_iterator() {
        let stream = MockStream::new(vec![1, 2, 3, 4, 5, 6, 7]);
        let mut chunks = stream.chunks(3);
        assert_eq!(chunks.next().unwrap().unwrap(), vec![1, 2, 3]);
        assert_eq!(chunks.next().unwrap().unwrap(), vec![4, 5, 6]);
        assert_eq!(chunks.next().unwrap().unwrap(), vec![7]);
        assert!(chunks.next().is_none());
    }
}
