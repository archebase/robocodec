// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Local file transport implementation.

use crate::io::transport::ByteStream;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

/// Local file stream implementation.
///
/// Provides a [`ByteStream`] implementation for local files using
/// memory-mapped I/O for efficient random access.
pub struct FileStream {
    /// The underlying file
    file: File,
    /// Current position in the file
    pos: u64,
    /// File length
    file_len: u64,
}

impl FileStream {
    /// Open a file for streaming.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if metadata
    /// cannot be read.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path.as_ref())?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            file,
            pos: 0,
            file_len,
        })
    }

    /// Create a new FileStream from an existing File.
    pub fn from_file(file: File) -> io::Result<Self> {
        let file_len = file.metadata()?.len();
        Ok(Self {
            file,
            pos: 0,
            file_len,
        })
    }
}

impl ByteStream for FileStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = self.file.seek(pos)?;
        self.pos = new_pos;
        Ok(new_pos)
    }

    fn position(&self) -> u64 {
        self.pos
    }

    fn len(&self) -> Option<u64> {
        Some(self.file_len)
    }

    fn can_seek(&self) -> bool {
        true
    }
}

/// Seek to a specific offset in the file.
///
/// This is a convenience method that forwards to [`ByteStream::seek`].
impl FileStream {
    /// Seek to an absolute offset.
    pub fn seek_to(&mut self, offset: u64) -> io::Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    /// Skip forward by N bytes.
    pub fn skip(&mut self, n: u64) -> io::Result<()> {
        self.seek(SeekFrom::Current(n as i64))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_file_stream_open() {
        // Create a temporary file
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();

        let stream = FileStream::open(file.path()).unwrap();
        assert_eq!(stream.len(), Some(11));
        assert!(!stream.is_empty());
    }

    #[test]
    fn test_file_stream_read() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();

        let mut stream = FileStream::open(file.path()).unwrap();
        let mut buf = [0u8; 5];
        assert_eq!(stream.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"hello");
        assert_eq!(stream.position(), 5);
    }

    #[test]
    fn test_file_stream_seek() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();

        let mut stream = FileStream::open(file.path()).unwrap();
        stream.seek_to(6).unwrap();
        assert_eq!(stream.position(), 6);

        let mut buf = [0u8; 5];
        stream.read(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_file_stream_skip() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();

        let mut stream = FileStream::open(file.path()).unwrap();
        stream.skip(6).unwrap();
        assert_eq!(stream.position(), 6);
    }

    #[test]
    fn test_file_stream_read_to_end() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();

        let mut stream = FileStream::open(file.path()).unwrap();
        let data = stream.read_to_end().unwrap();
        assert_eq!(data, b"hello world".to_vec());
    }

    #[test]
    fn test_file_stream_empty() {
        let file = tempfile::NamedTempFile::new().unwrap();
        // Empty file

        let mut stream = FileStream::open(file.path()).unwrap();
        assert_eq!(stream.len(), Some(0));
        assert!(stream.is_empty());
        assert!(stream.read_to_end().unwrap().is_empty());
    }

    #[test]
    fn test_file_stream_can_seek() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let stream = FileStream::open(file.path()).unwrap();
        assert!(stream.can_seek());
    }
}
