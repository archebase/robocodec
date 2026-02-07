// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP upload strategy for `HttpWriter`.
//!
//! This module defines the upload strategies available for HTTP/HTTPS write operations.
//! Different strategies offer trade-offs between efficiency, compatibility, and memory usage.

/// HTTP upload strategy.
///
/// Defines how data is uploaded to the HTTP server. Each strategy has different
/// requirements for server support and resource usage.
///
/// # Variants
///
/// * **`SinglePut`** - Upload entire file in a single PUT request. Simple but requires
///   the entire file to be in memory. Suitable for small files (< 10MB).
///
/// * **`ChunkedPut`** - Upload file in chunks using multiple PUT requests with Content-Range
///   headers. Server must support HTTP Range requests. Most efficient for large files
///   while maintaining broad compatibility.
///
/// * **`ChunkedEncoding`** - Upload using Transfer-Encoding: chunked. Most memory-efficient
///   as data streams directly to the server without buffering. Server support varies
///   significantly across implementations.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::transport::http::HttpUploadStrategy;
///
/// // Default strategy (ChunkedPut)
/// let strategy = HttpUploadStrategy::default();
///
/// // Explicit strategy selection
/// let strategy = HttpUploadStrategy::SinglePut;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpUploadStrategy {
    /// Single PUT request for the entire file.
    ///
    /// Simple to implement but requires the entire file to be in memory.
    /// Suitable for small files (< 10MB).
    ///
    /// # Server Requirements
    ///
    /// - Server must accept PUT requests
    /// - No special headers required
    ///
    /// # Limitations
    ///
    /// - Entire file buffered in memory
    /// - No resume capability on failure
    /// - No progress tracking during upload
    SinglePut,

    /// Chunked upload using multiple PUT requests with Content-Range.
    ///
    /// File is split into chunks and uploaded sequentially. Each chunk is a
    /// separate PUT request with a Content-Range header indicating the byte range.
    ///
    /// # Server Requirements
    ///
    /// - Server must support HTTP Range requests (Accept-Ranges: bytes)
    /// - Server must accept PUT with Content-Range headers
    ///
    /// # Advantages
    ///
    /// - Memory efficient (only one chunk in memory at a time)
    /// - Resumable (can retry failed chunks)
    /// - Progress tracking possible
    ///
    /// # Default
    ///
    /// This is the default strategy as it balances efficiency with compatibility.
    #[default]
    ChunkedPut,

    /// Streaming upload using Transfer-Encoding: chunked.
    ///
    /// Data streams directly to the server using HTTP chunked transfer encoding.
    /// Most memory-efficient option but server support varies.
    ///
    /// # Server Requirements
    ///
    /// - Server must accept Transfer-Encoding: chunked
    /// - Server must handle chunked requests correctly
    ///
    /// # Advantages
    ///
    /// - Lowest memory usage (streaming)
    /// - Upload starts immediately
    ///
    /// # Limitations
    ///
    /// - Server support varies significantly
    /// - Difficult to resume on failure
    /// - Some intermediaries may buffer entire request
    ChunkedEncoding,
}

impl std::fmt::Display for HttpUploadStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SinglePut => write!(f, "SinglePut"),
            Self::ChunkedPut => write!(f, "ChunkedPut"),
            Self::ChunkedEncoding => write!(f, "ChunkedEncoding"),
        }
    }
}

impl HttpUploadStrategy {
    /// Check if this strategy requires server Range request support.
    ///
    /// Returns true for `ChunkedPut`, which needs the server to accept and
    /// process Content-Range headers.
    #[must_use]
    pub fn requires_range_support(&self) -> bool {
        matches!(self, Self::ChunkedPut)
    }

    /// Check if this strategy streams data (no full buffering).
    ///
    /// Returns true for `ChunkedEncoding`, which streams data without
    /// buffering the entire file in memory.
    #[must_use]
    pub fn is_streaming(&self) -> bool {
        matches!(self, Self::ChunkedEncoding)
    }

    /// Get the recommended chunk size for this strategy.
    ///
    /// Returns the recommended chunk size in bytes. For `SinglePut`,
    /// this returns the maximum recommended file size.
    #[must_use]
    pub fn recommended_chunk_size(&self) -> usize {
        match self {
            // SinglePut: Return maximum recommended file size (10MB)
            Self::SinglePut => 10 * 1024 * 1024,
            // ChunkedPut: Default to 5MB chunks (balance between overhead and efficiency)
            Self::ChunkedPut => 5 * 1024 * 1024,
            // ChunkedEncoding: Smaller chunks for streaming (64KB)
            Self::ChunkedEncoding => 64 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_strategy() {
        let strategy = HttpUploadStrategy::default();
        assert_eq!(strategy, HttpUploadStrategy::ChunkedPut);
    }

    #[test]
    fn test_display_strategy() {
        assert_eq!(format!("{}", HttpUploadStrategy::SinglePut), "SinglePut");
        assert_eq!(format!("{}", HttpUploadStrategy::ChunkedPut), "ChunkedPut");
        assert_eq!(
            format!("{}", HttpUploadStrategy::ChunkedEncoding),
            "ChunkedEncoding"
        );
    }

    #[test]
    fn test_requires_range_support() {
        assert!(!HttpUploadStrategy::SinglePut.requires_range_support());
        assert!(HttpUploadStrategy::ChunkedPut.requires_range_support());
        assert!(!HttpUploadStrategy::ChunkedEncoding.requires_range_support());
    }

    #[test]
    fn test_is_streaming() {
        assert!(!HttpUploadStrategy::SinglePut.is_streaming());
        assert!(!HttpUploadStrategy::ChunkedPut.is_streaming());
        assert!(HttpUploadStrategy::ChunkedEncoding.is_streaming());
    }

    #[test]
    fn test_recommended_chunk_size() {
        assert_eq!(
            HttpUploadStrategy::SinglePut.recommended_chunk_size(),
            10 * 1024 * 1024
        );
        assert_eq!(
            HttpUploadStrategy::ChunkedPut.recommended_chunk_size(),
            5 * 1024 * 1024
        );
        assert_eq!(
            HttpUploadStrategy::ChunkedEncoding.recommended_chunk_size(),
            64 * 1024
        );
    }

    #[test]
    fn test_strategy_copy() {
        let strategy = HttpUploadStrategy::ChunkedPut;
        let copy = strategy;
        assert_eq!(strategy, copy);
    }

    #[test]
    fn test_strategy_equality() {
        assert_eq!(HttpUploadStrategy::SinglePut, HttpUploadStrategy::SinglePut);
        assert_ne!(
            HttpUploadStrategy::SinglePut,
            HttpUploadStrategy::ChunkedPut
        );
        assert_ne!(
            HttpUploadStrategy::ChunkedPut,
            HttpUploadStrategy::ChunkedEncoding
        );
    }
}
