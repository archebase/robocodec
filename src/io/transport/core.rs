// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core transport trait for unified byte I/O.
//!
//! This module provides the [`Transport`] trait, which abstracts over
//! different data sources (local files, S3, HTTP, etc.) for async
//! byte-level I/O operations.
//!
//! # Architecture
//!
//! The transport layer is **internal only** - not exposed in the public API.
//! It provides a unified async interface that format-specific readers can use
//! to work with any data source.
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::transport::{Transport, TransportExt, local::LocalTransport};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // All transports implement the same interface
//! let mut transport = LocalTransport::open("data.mcap")?;
//!
//! // Async read
//! let mut buf = vec![0u8; 4096];
//! let n = transport.read(&mut buf).await?;
//! # Ok(())
//! # }
//! ```

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Extension trait providing async convenience methods for [`Transport`].
///
/// This trait is automatically implemented for all types implementing `Transport`.
pub trait TransportExt: Transport {
    /// Async read into the given buffer.
    ///
    /// This is a convenience method that wraps `poll_read` in a future.
    /// Returns the number of bytes read (0 at EOF).
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a, Self>
    where
        Self: Unpin,
    {
        ReadFuture {
            transport: self,
            buf,
        }
    }

    /// Async seek to a specific offset.
    ///
    /// This is a convenience method that wraps `poll_seek` in a future.
    /// Returns the new position after seeking.
    fn seek<'a>(&'a mut self, pos: u64) -> SeekFuture<'a, Self>
    where
        Self: Unpin,
    {
        SeekFuture {
            transport: self,
            pos,
        }
    }

    /// Async read exactly the given number of bytes.
    ///
    /// Returns an error if EOF is reached before filling the buffer.
    fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadExactFuture<'a, Self>
    where
        Self: Unpin,
    {
        ReadExactFuture {
            transport: self,
            buf,
        }
    }

    /// Async read all remaining bytes into a vector.
    ///
    /// Returns an empty vector if the length is unknown.
    fn read_to_end<'a>(&'a mut self) -> ReadToEndFuture<'a, Self>
    where
        Self: Unpin,
    {
        ReadToEndFuture { transport: self }
    }
}

impl<T: Transport + ?Sized> TransportExt for T {}

/// Future returned by [`TransportExt::read`].
pub struct ReadFuture<'a, T: ?Sized> {
    transport: &'a mut T,
    buf: &'a mut [u8],
}

impl<T: Transport + Unpin + ?Sized> std::future::Future for ReadFuture<'_, T> {
    type Output = io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY:
        // - We extract raw pointers to both fields before creating any mutable references
        // - The pointers are to non-overlapping fields within the same struct
        // - We use as_mut().get_unchecked_mut() to reborrow instead of moving
        // - The references won't escape this function
        unsafe {
            let this = self.as_mut().get_unchecked_mut();
            let buf_ptr = this.buf.as_mut_ptr();
            let transport_ptr = this.transport as *mut T;

            let buf = std::slice::from_raw_parts_mut(buf_ptr, this.buf.len());
            let transport = std::pin::Pin::new_unchecked(&mut *transport_ptr);
            transport.poll_read(cx, buf)
        }
    }
}

/// Future returned by [`TransportExt::seek`].
pub struct SeekFuture<'a, T: ?Sized> {
    transport: &'a mut T,
    pos: u64,
}

impl<T: Transport + Unpin + ?Sized> std::future::Future for SeekFuture<'_, T> {
    type Output = io::Result<u64>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let pos = self.pos;
        let transport = std::pin::Pin::new(&mut *self.transport);
        transport.poll_seek(cx, pos)
    }
}

/// Future returned by [`TransportExt::read_exact`].
pub struct ReadExactFuture<'a, T: ?Sized> {
    transport: &'a mut T,
    buf: &'a mut [u8],
}

impl<T: Transport + Unpin + ?Sized> std::future::Future for ReadExactFuture<'_, T> {
    type Output = io::Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            if self.buf.is_empty() {
                return std::task::Poll::Ready(Ok(()));
            }

            let n = unsafe {
                let this = self.as_mut().get_unchecked_mut();
                let buf_ptr = this.buf.as_mut_ptr();
                let buf_len = this.buf.len();
                let transport_ptr = this.transport as *mut T;

                let buf = std::slice::from_raw_parts_mut(buf_ptr, buf_len);
                let transport = std::pin::Pin::new_unchecked(&mut *transport_ptr);
                std::task::ready!(transport.poll_read(cx, buf)?)
            };

            if n == 0 {
                return std::task::Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read_exact: reached EOF before filling buffer",
                )));
            }

            // Advance the buffer slice using get_unchecked_mut to avoid borrow issues
            self.buf = unsafe {
                let this = self.as_mut().get_unchecked_mut();
                &mut std::mem::take(&mut this.buf)[n..]
            };
        }
    }
}

/// Future returned by [`TransportExt::read_to_end`].
pub struct ReadToEndFuture<'a, T: ?Sized> {
    transport: &'a mut T,
}

impl<T: Transport + Unpin + ?Sized> std::future::Future for ReadToEndFuture<'_, T> {
    type Output = io::Result<Vec<u8>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Some(total_len) = self.transport.len() else {
            return std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "read_to_end: unknown length",
            )));
        };

        let pos = self.transport.position();
        let remaining = total_len.saturating_sub(pos);

        let mut buf = vec![0u8; remaining as usize];
        let mut offset = 0;

        while offset < buf.len() {
            let transport = std::pin::Pin::new(&mut *self.transport);
            let n = std::task::ready!(transport.poll_read(cx, &mut buf[offset..]))?;

            if n == 0 {
                break;
            }

            offset += n;
        }

        buf.truncate(offset);
        std::task::Poll::Ready(Ok(buf))
    }
}

/// Unified async transport trait for reading bytes from various sources.
///
/// This trait is **internal only** - not exposed in the public API.
/// All data sources (local files, S3, HTTP) implement this trait.
///
/// # Design
///
/// The trait uses poll-based methods (`poll_read`, `poll_seek`) for async
/// compatibility. This allows both truly async sources (S3, HTTP) and
/// synchronous sources (local files) to work through the same interface.
///
/// # Thread Safety
///
/// All transports must be `Send + Sync` for use in multi-threaded contexts.
pub trait Transport: Send + Sync {
    /// Async read into the given buffer.
    ///
    /// Returns the number of bytes read. May return 0 if no bytes are
    /// currently available but more may come later (for streaming sources).
    ///
    /// # Arguments
    ///
    /// * `cx` - Task context for waking
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    ///
    /// - `Poll::Ready(Ok(n))` - Successfully read n bytes (n may be 0 for EOF)
    /// - `Poll::Ready(Err(e))` - I/O error occurred
    /// - `Poll::Pending` - Operation not ready, will wake via `cx`
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>;

    /// Async seek to a specific offset.
    ///
    /// Returns the new position after seeking. Returns an error if seeking
    /// is not supported by this transport (e.g., for pure streaming sources).
    ///
    /// # Arguments
    ///
    /// * `cx` - Task context for waking
    /// * `pos` - Absolute offset to seek to
    ///
    /// # Returns
    ///
    /// - `Poll::Ready(Ok(pos))` - Successfully seeked to pos
    /// - `Poll::Ready(Err(e))` - Seek error or not supported
    /// - `Poll::Pending` - Operation not ready, will wake via `cx`
    fn poll_seek(self: Pin<&mut Self>, cx: &mut Context<'_>, pos: u64) -> Poll<io::Result<u64>>;

    /// Get the current position in the stream.
    fn position(&self) -> u64;

    /// Get the total length if known.
    ///
    /// Returns `None` for streams of unknown length (e.g., HTTP chunked encoding).
    fn len(&self) -> Option<u64>;

    /// Check if this transport is empty.
    ///
    /// Returns `true` if the length is known and zero, `false` otherwise.
    fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// Check if this transport supports seeking.
    fn is_seekable(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock transport for testing
    struct MockTransport {
        data: Vec<u8>,
        pos: usize,
        can_seek: bool,
    }

    impl MockTransport {
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

    impl Unpin for MockTransport {}

    impl Transport for MockTransport {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
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

        fn poll_seek(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            pos: u64,
        ) -> Poll<io::Result<u64>> {
            let this = self.get_mut();
            if !this.can_seek {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "seek not supported",
                )));
            }
            this.pos = pos as usize;
            Poll::Ready(Ok(pos))
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn len(&self) -> Option<u64> {
            Some(self.data.len() as u64)
        }

        fn is_seekable(&self) -> bool {
            self.can_seek
        }
    }

    #[test]
    fn test_transport_position() {
        let transport = MockTransport::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(transport.position(), 0);
    }

    #[test]
    fn test_transport_len() {
        let transport = MockTransport::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(transport.len(), Some(5));
    }

    #[test]
    fn test_transport_is_seekable() {
        let transport = MockTransport::new(vec![1, 2, 3]).with_seeking(true);
        assert!(transport.is_seekable());

        let transport = MockTransport::new(vec![1, 2, 3]).with_seeking(false);
        assert!(!transport.is_seekable());
    }

    #[test]
    fn test_transport_poll_read() {
        let mut transport = MockTransport::new(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 3];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(3))));
        assert_eq!(&buf, &[1, 2, 3]);
        assert_eq!(transport.position(), 3);
    }

    #[test]
    fn test_transport_poll_read_eof() {
        let mut transport = MockTransport::new(vec![1, 2]);
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
    fn test_transport_poll_seek() {
        let mut transport = MockTransport::new(vec![1, 2, 3, 4, 5]);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 2);
        assert!(matches!(poll, Poll::Ready(Ok(2))));
        assert_eq!(transport.position(), 2);
    }

    #[test]
    fn test_transport_poll_seek_unsupported() {
        let mut transport = MockTransport::new(vec![1, 2, 3]).with_seeking(false);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 1);
        assert!(matches!(poll, Poll::Ready(Err(_))));
    }
}
