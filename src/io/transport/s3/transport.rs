// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 transport implementation using the Transport trait.
//!
//! This module provides [`S3Transport`], which implements the [`Transport`]
//! trait for S3 and S3-compatible storage services.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::FutureExt;

use crate::io::s3::{FatalError, S3Client, S3Location};
use crate::io::transport::Transport;

/// S3 transport implementation.
///
/// Wraps an `S3Client` and implements the async `Transport` trait for S3 objects.
/// Supports range-based reads and seeking.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::transport::{s3::S3Transport, Transport, TransportExt};
/// use robocodec::io::s3::{S3Client, S3Location};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = S3Client::default_client()?;
/// let location = S3Location::new("my-bucket", "data.mcap");
/// let mut transport = S3Transport::new(client, location).await?;
///
/// // Read from S3
/// let mut buf = vec![0u8; 4096];
/// let n = transport.read(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
pub struct S3Transport {
    /// The S3 client for making requests
    client: S3Client,
    /// The S3 object location
    location: S3Location,
    /// Current position in the object
    pos: u64,
    /// Total object length
    len: u64,
    /// Read buffer for data fetched from S3
    buffer: Vec<u8>,
    /// Current read offset within the buffer
    buffer_offset: usize,
    /// Pending fetch future (for `poll_read`)
    fetch_future: Option<FetchFuture>,
}

/// Future for fetching a range from S3.
type FetchFuture = futures::future::BoxFuture<'static, Result<Bytes, FatalError>>;

impl S3Transport {
    /// Create a new S3 transport.
    ///
    /// This will fetch the object metadata to determine the size.
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist or metadata cannot be fetched.
    pub async fn new(client: S3Client, location: S3Location) -> Result<Self, FatalError> {
        let len = client.object_size(&location).await?;
        Ok(Self {
            client,
            location,
            pos: 0,
            len,
            buffer: Vec::new(),
            buffer_offset: 0,
            fetch_future: None,
        })
    }

    /// Create a new S3 transport with a known size.
    ///
    /// This skips the initial metadata fetch when the size is already known.
    #[must_use]
    pub fn with_size(client: S3Client, location: S3Location, len: u64) -> Self {
        Self {
            client,
            location,
            pos: 0,
            len,
            buffer: Vec::new(),
            buffer_offset: 0,
            fetch_future: None,
        }
    }

    /// Fill the internal buffer by fetching from S3.
    ///
    /// Fetches up to `size` bytes starting at the current position.
    fn fetch_data(&mut self, size: usize) -> FetchFuture {
        let client = self.client.clone();
        let location = self.location.clone();
        let offset = self.pos;

        async move { client.fetch_range(&location, offset, size as u64).await }.boxed()
    }

    /// Get a reference to the S3 client.
    #[must_use]
    pub fn client(&self) -> &S3Client {
        &self.client
    }

    /// Get a reference to the S3 location.
    #[must_use]
    pub fn location(&self) -> &S3Location {
        &self.location
    }
}

// Implement Unpin for S3Transport (needed for Transport async methods)
impl Unpin for S3Transport {}

// SAFETY: S3Transport is safe to share between threads because:
// - The Transport trait requires poll_read/poll_seek to take Pin<&mut Self>, guaranteeing exclusive access
// - All fields are either Send + Sync (client, location, pos, len, buffer, buffer_offset)
// - The futures are only accessed through &mut self in poll_read/poll_seek
unsafe impl Sync for S3Transport {}

impl Transport for S3Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // If we have buffered data, copy it first
        if self.buffer_offset < self.buffer.len() {
            let available = self.buffer.len() - self.buffer_offset;
            let to_copy = buf.len().min(available);

            buf[..to_copy]
                .copy_from_slice(&self.buffer[self.buffer_offset..self.buffer_offset + to_copy]);
            self.buffer_offset += to_copy;
            self.pos += to_copy as u64;

            // Clear buffer if fully consumed
            if self.buffer_offset >= self.buffer.len() {
                self.buffer.clear();
                self.buffer_offset = 0;
            }

            return Poll::Ready(Ok(to_copy));
        }

        // Check if we're at EOF
        if self.pos >= self.len {
            return Poll::Ready(Ok(0));
        }

        // Start or continue a fetch
        if self.fetch_future.is_none() {
            // Fetch a chunk (64KB default, or remaining bytes if less)
            let chunk_size = 64 * 1024;
            let remaining = self.len - self.pos;
            let to_fetch = chunk_size.min(remaining as usize) as u64;

            self.fetch_future = Some(self.fetch_data(to_fetch as usize));
        }

        // Poll the fetch future
        let fetch_result = self
            .fetch_future
            .as_mut()
            .as_mut()
            .expect(
                "fetch_future set to Some() in is_none() check above or from previous iteration",
            )
            .poll_unpin(cx);

        match fetch_result {
            Poll::Ready(Ok(data)) => {
                self.fetch_future = None;

                // Store fetched data in buffer
                self.buffer = data.to_vec();
                self.buffer_offset = 0;

                // Copy to output buffer
                let to_copy = buf.len().min(self.buffer.len());
                buf[..to_copy].copy_from_slice(&self.buffer[..to_copy]);
                self.buffer_offset = to_copy;
                self.pos += to_copy as u64;

                Poll::Ready(Ok(to_copy))
            }
            Poll::Ready(Err(e)) => {
                self.fetch_future = None;
                Poll::Ready(Err(io::Error::other(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_seek(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: u64,
    ) -> Poll<io::Result<u64>> {
        // If seeking within the current buffer, just adjust offset
        let buffer_start = self.pos - self.buffer_offset as u64;
        let buffer_end = buffer_start + self.buffer.len() as u64;

        if pos >= buffer_start && pos <= buffer_end {
            // Seek within current buffer
            self.buffer_offset = (pos - buffer_start) as usize;
            self.pos = pos;
            return Poll::Ready(Ok(pos));
        }

        // For seeks outside the buffer, we can clear it and update position
        // S3 supports range requests, so we don't need to fetch
        self.buffer.clear();
        self.buffer_offset = 0;
        self.pos = pos.min(self.len);
        Poll::Ready(Ok(self.pos))
    }

    fn position(&self) -> u64 {
        self.pos
    }

    fn len(&self) -> Option<u64> {
        Some(self.len)
    }

    fn is_seekable(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_transport_with_size() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "key.mcap");
        let transport = S3Transport::with_size(client, location, 1024);

        assert_eq!(transport.len(), Some(1024));
        assert_eq!(transport.position(), 0);
        assert!(transport.is_seekable());
    }

    #[test]
    fn test_s3_transport_seek_within_bounds() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "key.mcap");
        let mut transport = S3Transport::with_size(client, location, 1024);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Seek to middle of file
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 512);
        assert!(matches!(poll, Poll::Ready(Ok(512))));
        assert_eq!(transport.position(), 512);
    }

    #[test]
    fn test_s3_transport_seek_past_end() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "key.mcap");
        let mut transport = S3Transport::with_size(client, location, 1024);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Seek past end of file
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 2048);
        assert!(matches!(poll, Poll::Ready(Ok(1024)))); // Clamped to file size
        assert_eq!(transport.position(), 1024);
    }

    #[test]
    fn test_s3_transport_client_and_location() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "key.mcap");
        let transport = S3Transport::with_size(client.clone(), location.clone(), 1024);

        assert_eq!(transport.location().bucket(), "bucket");
        assert_eq!(transport.location().key(), "key.mcap");
    }

    #[test]
    fn test_s3_transport_eof() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "key.mcap");
        let mut transport = S3Transport::with_size(client, location, 100);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Seek to end
        let _poll = Pin::new(&mut transport).poll_seek(&mut cx, 100);
        assert_eq!(transport.position(), 100);

        // Read at EOF returns 0
        let mut buf = [0u8; 10];
        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        // At EOF, poll_read returns Ready(Ok(0))
        assert!(matches!(poll, Poll::Ready(Ok(0))));
    }
}
