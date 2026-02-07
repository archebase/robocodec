// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Local file transport implementation.
//!
//! This module provides [`LocalTransport`], which implements the [`Transport`]
//! trait for local files using synchronous `std::fs::File` with an async interface.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::transport::Transport;

/// Local file transport implementation.
///
/// Wraps `std::fs::File` and implements the async `Transport` trait.
/// The async methods immediately complete since file I/O is synchronous.
pub struct LocalTransport {
    /// The underlying file
    file: File,
    /// Current position in the file
    pos: u64,
    /// File length
    len: u64,
}

impl LocalTransport {
    /// Open a local file for transport.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if metadata
    /// cannot be read.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path.as_ref())?;
        let len = file.metadata()?.len();
        Ok(Self { file, pos: 0, len })
    }

    /// Create a new LocalTransport from an existing File.
    pub fn from_file(file: File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        Ok(Self { file, pos: 0, len })
    }
}

// Implement Unpin for LocalTransport (needed for Transport async methods)
impl Unpin for LocalTransport {}

impl Transport for LocalTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Synchronous file I/O completes immediately
        let this = self.get_mut();
        let n = this.file.read(buf)?;
        this.pos += n as u64;
        Poll::Ready(Ok(n))
    }

    fn poll_seek(self: Pin<&mut Self>, _cx: &mut Context<'_>, pos: u64) -> Poll<io::Result<u64>> {
        // Synchronous seek completes immediately
        let this = self.get_mut();
        let new_pos = this.file.seek(SeekFrom::Start(pos))?;
        this.pos = new_pos;
        Poll::Ready(Ok(new_pos))
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

/// Additional convenience methods for LocalTransport.
impl LocalTransport {
    /// Seek to an absolute offset.
    pub fn seek_to(&mut self, offset: u64) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.pos = offset;
        Ok(())
    }

    /// Skip forward by N bytes.
    pub fn skip(&mut self, n: u64) -> io::Result<()> {
        let new_pos = self.file.seek(SeekFrom::Current(n as i64))?;
        self.pos = new_pos;
        Ok(())
    }

    /// Get a reference to the underlying file.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Get a mutable reference to the underlying file.
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::transport::TransportExt;
    use std::io::Write;

    #[test]
    fn test_local_transport_open() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let transport = LocalTransport::open(file.path()).unwrap();
        assert_eq!(transport.len(), Some(11));
        assert_eq!(transport.position(), 0);
        assert!(transport.is_seekable());
    }

    #[test]
    fn test_local_transport_poll_read() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        let mut buf = [0u8; 5];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(5))));
        assert_eq!(&buf, b"hello");
        assert_eq!(transport.position(), 5);
    }

    #[test]
    fn test_local_transport_poll_seek() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 6);
        assert!(matches!(poll, Poll::Ready(Ok(6))));
        assert_eq!(transport.position(), 6);

        let mut buf = [0u8; 5];
        let poll = Pin::new(&mut transport).poll_read(&mut cx, &mut buf);
        assert!(matches!(poll, Poll::Ready(Ok(5))));
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_local_transport_seek_to() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        transport.seek_to(6).unwrap();
        assert_eq!(transport.position(), 6);
    }

    #[test]
    fn test_local_transport_skip() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        transport.skip(6).unwrap();
        assert_eq!(transport.position(), 6);
    }

    #[tokio::test]
    async fn test_local_transport_read() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        let mut buf = [0u8; 5];
        let n = transport.read(&mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[tokio::test]
    async fn test_local_transport_seek() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        transport.seek(6).await.unwrap();
        assert_eq!(transport.position(), 6);
    }

    #[tokio::test]
    async fn test_local_transport_read_exact() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        let mut buf = [0u8; 11];
        transport.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello world");
    }

    #[tokio::test]
    async fn test_local_transport_read_to_end() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"hello world").unwrap();

        let mut transport = LocalTransport::open(file.path()).unwrap();
        let data = transport.read_to_end().await.unwrap();
        assert_eq!(data, b"hello world".to_vec());
    }

    #[test]
    fn test_local_transport_empty() {
        let file = tempfile::NamedTempFile::new().unwrap();

        let transport = LocalTransport::open(file.path()).unwrap();
        assert_eq!(transport.len(), Some(0));
        assert_eq!(transport.position(), 0);
        assert!(transport.is_seekable());
    }

    #[test]
    fn test_local_transport_from_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write_all(b"data").unwrap();

        let file_ref = file.as_file();
        let transport = LocalTransport::from_file(file_ref.try_clone().unwrap()).unwrap();
        assert_eq!(transport.len(), Some(4));
    }
}
