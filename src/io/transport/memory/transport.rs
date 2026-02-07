// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! In-memory transport implementation using the Transport trait.
//!
//! This module provides [`MemoryTransport`], which implements the [`Transport`]
//! trait for in-memory byte data. All operations complete immediately since
//! the data is already in memory.

use std::io::{self, IoSliceMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;

use crate::io::transport::Transport;

/// In-memory transport implementation.
///
/// Wraps byte data in memory and implements the async `Transport` trait.
/// All operations complete immediately since data is already in memory,
/// making this ideal for testing without I/O overhead.
///
/// # Seeking
///
/// Full seeking is supported within the bounds of the stored data.
/// Seeking past the end of data will clamp to the data length.
///
/// # Thread Safety
///
/// MemoryTransport is Send + Sync, allowing it to be used in multi-threaded
/// contexts. The Transport trait's poll methods ensure exclusive access
/// through Pin<&mut Self>.
pub struct MemoryTransport {
    /// The underlying data stored as Bytes for efficient cloning
    data: Bytes,
    /// Current position in the data
    pos: usize,
}

impl MemoryTransport {
    /// Create a new MemoryTransport from owned bytes.
    ///
    /// # Arguments
    ///
    /// * `data` - Vector of bytes to store in memory
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let data = b"hello world".to_vec();
    /// let transport = MemoryTransport::new(data);
    /// assert_eq!(transport.len(), Some(11));
    /// ```
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Bytes::from(data),
            pos: 0,
        }
    }

    /// Create a new MemoryTransport from a byte slice.
    ///
    /// This copies the slice into owned memory.
    ///
    /// # Arguments
    ///
    /// * `data` - Slice of bytes to store in memory
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let transport = MemoryTransport::from_slice(b"test data");
    /// assert_eq!(transport.len(), Some(9));
    /// ```
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            data: Bytes::copy_from_slice(data),
            pos: 0,
        }
    }

    /// Create a new MemoryTransport from Bytes.
    ///
    /// This is zero-cost since Bytes is already owned.
    ///
    /// # Arguments
    ///
    /// * `data` - Bytes to store in memory
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    /// use bytes::Bytes;
    ///
    /// let data = Bytes::from_static(b"static data");
    /// let transport = MemoryTransport::from_bytes(data);
    /// assert_eq!(transport.len(), Some(11));
    /// ```
    pub fn from_bytes(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    /// Get the underlying data.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let transport = MemoryTransport::from_slice(b"test");
    /// assert_eq!(transport.data(), b"test");
    /// ```
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get the current position.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let transport = MemoryTransport::from_slice(b"test");
    /// assert_eq!(transport.position(), 0);
    /// ```
    pub fn position(&self) -> u64 {
        self.pos as u64
    }

    /// Get the total length.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let transport = MemoryTransport::from_slice(b"test");
    /// assert_eq!(transport.len(), Some(4));
    /// ```
    pub fn len(&self) -> Option<u64> {
        Some(self.data.len() as u64)
    }

    /// Check if the data is empty.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let transport = MemoryTransport::from_slice(b"");
    /// assert!(transport.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Check if seeking is supported (always true).
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let transport = MemoryTransport::from_slice(b"test");
    /// assert!(transport.is_seekable());
    /// ```
    pub fn is_seekable(&self) -> bool {
        true
    }

    /// Seek to an absolute offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - Absolute offset to seek to
    ///
    /// # Returns
    ///
    /// Returns the new position after seeking.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let mut transport = MemoryTransport::from_slice(b"hello world");
    /// transport.seek_to(6).unwrap();
    /// assert_eq!(transport.position(), 6);
    /// ```
    pub fn seek_to(&mut self, offset: u64) -> io::Result<u64> {
        self.pos = offset as usize;
        Ok(self.pos as u64)
    }

    /// Rewind to the beginning.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let mut transport = MemoryTransport::from_slice(b"test");
    /// transport.seek_to(2).unwrap();
    /// transport.rewind();
    /// assert_eq!(transport.position(), 0);
    /// ```
    pub fn rewind(&mut self) {
        self.pos = 0;
    }

    /// Read a slice of data without advancing the position.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    ///
    /// Returns the number of bytes read.
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::io::transport::memory::MemoryTransport;
    ///
    /// let mut transport = MemoryTransport::from_slice(b"hello");
    /// let mut buf = [0u8; 3];
    /// let n = transport.peek(&mut buf).unwrap();
    /// assert_eq!(n, 3);
    /// assert_eq!(&buf, b"hel");
    /// assert_eq!(transport.position(), 0); // Position unchanged
    /// ```
    pub fn peek(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.data.len() - self.pos;
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = buf.len().min(remaining);
        buf[..to_read].copy_from_slice(&self.data[self.pos..self.pos + to_read]);
        Ok(to_read)
    }

    /// Read data into multiple buffers without advancing the position.
    ///
    /// # Arguments
    ///
    /// * `bufs` - Slice of IoSliceMut buffers to read into
    ///
    /// # Returns
    ///
    /// Returns the total number of bytes read.
    pub fn peek_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let mut total_read = 0;
        let mut offset = self.pos;

        for buf in bufs {
            let remaining = self.data.len() - offset;
            if remaining == 0 {
                break;
            }
            let to_read: usize = buf.len().min(remaining);
            buf[..to_read].copy_from_slice(&self.data[offset..offset + to_read]);
            offset += to_read;
            total_read += to_read;
        }

        Ok(total_read)
    }
}

// Implement Unpin for MemoryTransport (needed for Transport async methods)
impl Unpin for MemoryTransport {}

// SAFETY: MemoryTransport is safe to share between threads because:
// - The Transport trait requires poll_read/poll_seek to take Pin<&mut Self>, guaranteeing exclusive access
// - All fields are Send + Sync (Bytes is Send + Sync, pos is usize)
// - Bytes is immutable after creation, providing safe concurrent reads
// - The mutable position is only accessed through &mut self in poll methods
unsafe impl Sync for MemoryTransport {}

impl Transport for MemoryTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Memory operations complete immediately
        let this = self.get_mut();
        let remaining = this.data.len() - this.pos;
        if remaining == 0 {
            return Poll::Ready(Ok(0));
        }
        let to_read = buf.len().min(remaining);
        buf[..to_read].copy_from_slice(&this.data[this.pos..this.pos + to_read]);
        this.pos += to_read;
        Poll::Ready(Ok(to_read))
    }

    fn poll_seek(self: Pin<&mut Self>, _cx: &mut Context<'_>, pos: u64) -> Poll<io::Result<u64>> {
        // Seek operations complete immediately
        let this = self.get_mut();
        // Clamp to data length
        this.pos = pos.min(this.data.len() as u64) as usize;
        Poll::Ready(Ok(this.pos as u64))
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }

    fn len(&self) -> Option<u64> {
        Some(self.data.len() as u64)
    }

    fn is_seekable(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use crate::io::transport::TransportExt;

    use super::*;

    #[test]
    fn test_memory_transport_new() {
        let data = b"hello world".to_vec();
        let transport = MemoryTransport::new(data);
        assert_eq!(transport.len(), Some(11));
        assert_eq!(transport.position(), 0);
        assert!(transport.is_seekable());
        assert!(!transport.is_empty());
    }

    #[test]
    fn test_memory_transport_from_slice() {
        let transport = MemoryTransport::from_slice(b"test data");
        assert_eq!(transport.len(), Some(9));
        assert_eq!(transport.data(), b"test data");
    }

    #[test]
    fn test_memory_transport_from_bytes() {
        let data = Bytes::from_static(b"static data");
        let transport = MemoryTransport::from_bytes(data);
        assert_eq!(transport.len(), Some(11));
    }

    #[test]
    fn test_memory_transport_empty() {
        let transport = MemoryTransport::from_slice(b"");
        assert_eq!(transport.len(), Some(0));
        assert!(transport.is_empty());
    }

    #[test]
    fn test_memory_transport_position() {
        let transport = MemoryTransport::from_slice(b"test");
        assert_eq!(transport.position(), 0);
    }

    #[test]
    fn test_memory_transport_is_seekable() {
        let transport = MemoryTransport::from_slice(b"test");
        assert!(transport.is_seekable());
    }

    #[test]
    fn test_memory_transport_seek_to() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        transport.seek_to(6).unwrap();
        assert_eq!(transport.position(), 6);
    }

    #[test]
    fn test_memory_transport_rewind() {
        let mut transport = MemoryTransport::from_slice(b"test");
        transport.seek_to(2).unwrap();
        transport.rewind();
        assert_eq!(transport.position(), 0);
    }

    #[test]
    fn test_memory_transport_peek() {
        let mut transport = MemoryTransport::from_slice(b"hello");
        let mut buf = [0u8; 3];
        let n = transport.peek(&mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf, b"hel");
        assert_eq!(transport.position(), 0); // Position unchanged
    }

    #[test]
    fn test_memory_transport_peek_eof() {
        let mut transport = MemoryTransport::from_slice(b"hi");
        let mut buf = [0u8; 10];
        let n = transport.peek(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");
    }

    #[test]
    fn test_memory_transport_poll_read() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        let mut buf = [0u8; 5];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(5))));
        assert_eq!(&buf, b"hello");
        assert_eq!(transport.position(), 5);
    }

    #[test]
    fn test_memory_transport_poll_read_eof() {
        let mut transport = MemoryTransport::from_slice(b"hi");
        let mut buf = [0u8; 10];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // First read gets the data
        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(2))));

        // Second read returns EOF
        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(0))));
    }

    #[test]
    fn test_memory_transport_poll_read_partial() {
        let mut transport = MemoryTransport::from_slice(b"hello");
        let mut buf = [0u8; 10];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(5))));
        assert_eq!(&buf[..5], b"hello");
    }

    #[test]
    fn test_memory_transport_poll_seek() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 6);
        assert!(matches!(poll, Poll::Ready(Ok(6))));
        assert_eq!(transport.position(), 6);
    }

    #[test]
    fn test_memory_transport_poll_seek_past_end() {
        let mut transport = MemoryTransport::from_slice(b"hello");
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Seeking past end should clamp to length
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 100);
        assert!(matches!(poll, Poll::Ready(Ok(5)))); // Clamped to 5
        assert_eq!(transport.position(), 5);
    }

    #[test]
    fn test_memory_transport_poll_seek_to_beginning() {
        let mut transport = MemoryTransport::from_slice(b"hello");
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Read some data first
        let mut buf = [0u8; 3];
        let _ = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert_eq!(transport.position(), 3);

        // Seek back to beginning
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 0);
        assert!(matches!(poll, Poll::Ready(Ok(0))));
        assert_eq!(transport.position(), 0);

        // Read again to verify we get the same data
        let mut buf2 = [0u8; 3];
        let _ = Pin::new(&mut transport).poll_read(&mut cx, &mut buf2);
        assert_eq!(&buf2, b"hel");
    }

    #[tokio::test]
    async fn test_memory_transport_read() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        let mut buf = vec![0u8; 5];
        let n = transport.read(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[tokio::test]
    async fn test_memory_transport_seek() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        transport.seek(6).await.unwrap();
        assert_eq!(transport.position(), 6);

        let mut buf = vec![0u8; 5];
        let n = transport.read(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
    }

    #[tokio::test]
    async fn test_memory_transport_read_exact() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        let mut buf = [0u8; 11];
        transport.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello world");
    }

    #[tokio::test]
    async fn test_memory_transport_read_to_end() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        let data = transport.read_to_end().await.unwrap();
        assert_eq!(data, b"hello world".to_vec());
    }

    #[tokio::test]
    async fn test_memory_transport_read_exact_past_end() {
        let mut transport = MemoryTransport::from_slice(b"hi");
        let mut buf = [0u8; 10];
        let result = transport.read_exact(&mut buf).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_transport_zero_copy() {
        let data = Bytes::from_static(b"test data");
        let transport = MemoryTransport::from_bytes(data.clone());
        // The Bytes object should be shared (not copied)
        assert_eq!(transport.data(), data.as_ref());
    }

    #[test]
    fn test_memory_transport_peek_vectored() {
        let mut transport = MemoryTransport::from_slice(b"hello world");
        let mut buf1 = [0u8; 3];
        let mut buf2 = [0u8; 4];
        let mut bufs = [IoSliceMut::new(&mut buf1), IoSliceMut::new(&mut buf2)];

        let n = transport.peek_vectored(&mut bufs).unwrap();
        assert_eq!(n, 7);
        assert_eq!(&buf1, b"hel");
        assert_eq!(&buf2, b"lo w");
        assert_eq!(transport.position(), 0); // Position unchanged
    }

    #[test]
    fn test_memory_transport_peek_vectored_partial() {
        let mut transport = MemoryTransport::from_slice(b"hi");
        let mut buf1 = [0u8; 3];
        let mut buf2 = [0u8; 4];
        let mut bufs = [IoSliceMut::new(&mut buf1), IoSliceMut::new(&mut buf2)];

        let n = transport.peek_vectored(&mut bufs).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf1[..2], b"hi");
        assert_eq!(buf2, [0u8; 4]);
    }

    #[test]
    fn test_memory_transport_send_sync() {
        // Verify MemoryTransport is Send + Sync
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemoryTransport>();
    }

    #[tokio::test]
    async fn test_memory_transport_multiple_reads() {
        let mut transport = MemoryTransport::from_slice(b"hello world");

        // First read
        let mut buf1 = [0u8; 5];
        let n = transport.read(&mut buf1).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf1, b"hello");

        // Second read
        let mut buf2 = [0u8; 6];
        let n = transport.read(&mut buf2).await.unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf2, b" world");

        // Third read (EOF)
        let mut buf3 = [0u8; 10];
        let n = transport.read(&mut buf3).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_memory_transport_seek_and_read() {
        let mut transport = MemoryTransport::from_slice(b"0123456789");

        // Read first 3 bytes
        let mut buf = [0u8; 3];
        transport.read(&mut buf).await.unwrap();
        assert_eq!(&buf, b"012");

        // Seek to position 7
        transport.seek(7).await.unwrap();

        // Read from position 7
        let n = transport.read(&mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"789");
    }
}
