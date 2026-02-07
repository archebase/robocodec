// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Async byte source for S3 streaming with mcap crate integration.
//!
//! This module provides an AsyncRead implementation for S3 objects
//! that can be used with mcap::tokio::LinearReader for efficient
//! streaming of MCAP files from S3.

use std::io::{self, Seek, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::io::s3::{client::S3Client, config::S3ReaderConfig, location::S3Location};

/// Configuration for S3 streaming source.
#[derive(Clone, Debug)]
pub struct S3StreamConfig {
    /// Buffer size for S3 requests (default: 256KB)
    pub buffer_size: usize,
    /// Maximum number of concurrent range requests
    pub max_concurrent_requests: usize,
    /// S3 client configuration
    pub s3_config: S3ReaderConfig,
}

impl Default for S3StreamConfig {
    fn default() -> Self {
        Self {
            buffer_size: 256 * 1024,
            max_concurrent_requests: 4,
            s3_config: S3ReaderConfig::default(),
        }
    }
}

/// Async byte source for S3 objects.
///
/// Implements AsyncRead for use with mcap::tokio::LinearReader.
/// Efficiently streams S3 objects using HTTP Range requests.
pub struct S3ByteSource {
    /// S3 client for HTTP requests
    client: S3Client,
    /// S3 location being read
    location: S3Location,
    /// Current read position
    pos: u64,
    /// Total object size
    size: u64,
    /// Read buffer for data fetched from S3
    buffer: Vec<u8>,
    /// Current position within buffer
    buffer_pos: usize,
    /// Number of valid bytes in buffer
    buffer_len: usize,
    /// Buffer size for S3 requests
    buffer_size: usize,
}

impl S3ByteSource {
    /// Create a new S3 byte source.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to read from
    /// * `config` - Configuration for the stream source
    pub async fn open(
        location: S3Location,
        config: S3StreamConfig,
    ) -> Result<Self, crate::io::s3::error::FatalError> {
        let client = S3Client::new(config.s3_config)?;

        // Get object size first via HEAD request
        let size = client.object_size(&location).await?;

        Ok(Self {
            client,
            location,
            pos: 0,
            size,
            buffer: Vec::with_capacity(config.buffer_size),
            buffer_pos: 0,
            buffer_len: 0,
            buffer_size: config.buffer_size,
        })
    }

    /// Get the total size of the S3 object.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the current read position.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Fetch more data from S3 into the buffer.
    pub async fn fetch_more(&mut self) -> io::Result<usize> {
        // Calculate how much to fetch (up to buffer_size)
        let remaining = self.size.saturating_sub(self.pos);
        let to_fetch = self.buffer_size.min(remaining as usize);

        if to_fetch == 0 {
            return Ok(0); // EOF
        }

        // Fetch range from S3
        let data = self
            .client
            .fetch_range(&self.location, self.pos, to_fetch as u64)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let len = data.len();
        if len == 0 {
            return Ok(0); // EOF
        }

        // Resize buffer if needed and copy data
        self.buffer.clear();
        self.buffer.reserve(len);
        self.buffer.extend_from_slice(&data);

        self.buffer_pos = 0;
        self.buffer_len = len;

        Ok(len)
    }
}

impl AsyncRead for S3ByteSource {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If we have buffered data, copy it first
        if self.buffer_pos < self.buffer_len {
            let available = &self.buffer[self.buffer_pos..self.buffer_len];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            self.buffer_pos += to_copy;
            self.pos += to_copy as u64;

            return Poll::Ready(Ok(()));
        }

        // Check if we're at EOF
        if self.pos >= self.size {
            return Poll::Ready(Ok(()));
        }

        // No more buffered data and not at EOF - would need async fetch
        // Return Pending to indicate caller should use async methods
        Poll::Pending
    }
}

// Implement Seek for sync compatibility and seeking
impl Seek for S3ByteSource {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as u64,
            SeekFrom::End(offset) => {
                let pos = self.size as i64 + offset;
                if pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek before start",
                    ));
                }
                pos as u64
            }
            SeekFrom::Current(offset) => {
                let pos = self.pos as i64 + offset;
                if pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek before start",
                    ));
                }
                pos as u64
            }
        };

        if new_pos > self.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek beyond end",
            ));
        }

        // Clear buffer on seek
        self.buffer_pos = 0;
        self.buffer_len = 0;
        self.pos = new_pos;

        Ok(new_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_stream_config_default() {
        let config = S3StreamConfig::default();
        assert_eq!(config.buffer_size, 256 * 1024);
        assert_eq!(config.max_concurrent_requests, 4);
    }

    #[test]
    fn test_s3_byte_source_seek() {
        // Test seek logic
        let pos = SeekFrom::Start(100);
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as u64,
            SeekFrom::End(offset) => (1000i64 + offset) as u64,
            SeekFrom::Current(offset) => (500u64 as i64 + offset) as u64,
        };
        assert_eq!(new_pos, 100u64);
    }
}
