// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP writer for robotics data files.
//!
//! This module provides [`HttpWriter`], which implements the [`FormatWriter`] trait
//! for HTTP/HTTPS URLs. Data is buffered and uploaded when [`FormatWriter::finish()`]
//! is called.
//!
//! # Features
//!
//! - **Buffering**: Data is buffered in memory before upload
//! - **Chunked upload**: Supports large files via chunked upload strategies
//! - **Authentication**: Supports Bearer tokens and Basic auth
//! - **Retry logic**: Configurable retry attempts for failed uploads
//! - **Multiple strategies**: SinglePut, ChunkedPut, ChunkedEncoding
//!
//! # Limitations
//!
//! Due to the synchronous [`FormatWriter`] trait, all data is buffered in memory
//! and uploaded during [`finish()`][FormatWriter::finish]. For large files (>50MB),
//! consider using a local file writer and uploading separately.
//!
//! The maximum buffer size is 50MB (10x minimum chunk size) to prevent
//! unbounded memory growth.

use crate::io::metadata::{ChannelInfo, RawMessage};
use crate::io::traits::FormatWriter;
use crate::io::transport::http::HttpAuth;
use crate::io::transport::http::upload_strategy::HttpUploadStrategy;
use crate::{CodecError, Result};
use bytes::Bytes;
use std::collections::HashMap;

/// Default chunk size for HTTP chunked upload (5MB).
const DEFAULT_CHUNK_SIZE: usize = 5 * 1024 * 1024;

/// Maximum buffer size to prevent unbounded memory growth (50MB).
const MAX_BUFFER_SIZE: usize = 50 * 1024 * 1024;

/// Default number of retry attempts for failed uploads.
const DEFAULT_MAX_RETRIES: usize = 3;

/// Upload state machine for tracking upload progress.
#[derive(Debug, Clone, PartialEq, Eq)]
enum UploadState {
    /// No data written yet
    Initial,
    /// Accumulating data in buffer
    Buffering,
    /// Upload in progress
    Uploading,
    /// Upload finished successfully
    Completed,
    /// Upload failed, retry pending
    Failed { error: String, retries_left: usize },
}

/// HTTP-specific write errors.
#[derive(Debug, thiserror::Error)]
pub enum HttpWriteError {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    /// Server returned error status
    #[error("Server returned error status: {0}")]
    ServerError(u16),

    /// Upload failed after all retries
    #[error("Upload failed after {0} retries: {1}")]
    UploadFailed(usize, String),

    /// Server does not support Range requests
    #[error("Server does not support Range requests for chunked upload")]
    RangeNotSupported,

    /// Buffer size exceeded
    #[error("Buffer size exceeded: {0} bytes")]
    BufferSizeExceeded(usize),

    /// Upload already finished
    #[error("Upload already finished")]
    AlreadyFinished,

    /// Upload already in progress
    #[error("Upload already in progress")]
    AlreadyInProgress,

    /// Invalid URL
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    /// Chunk size too small
    #[error("Chunk size too small: {0} bytes (minimum: 1MB)")]
    ChunkSizeTooSmall(usize),
}

impl From<HttpWriteError> for crate::CodecError {
    fn from(err: HttpWriteError) -> Self {
        crate::CodecError::EncodeError {
            codec: "HTTP".to_string(),
            message: err.to_string(),
        }
    }
}

/// Writer for HTTP/HTTPS URLs.
///
/// This writer buffers data in memory and uploads to an HTTP server when
/// [`finish()`][FormatWriter::finish] is called. It implements the [`FormatWriter`]
/// trait, allowing it to be used transparently with the unified writer API.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::{FormatWriter, RoboWriter};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // HTTP write works through RoboWriter
/// let mut writer = RoboWriter::create("https://example.com/output.mcap")?;
///
/// let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;
/// // ... write messages ...
/// writer.finish()?;
/// # Ok(())
/// # }
/// ```
pub struct HttpWriter {
    /// Target URL
    url: String,
    /// HTTP client with authentication configured
    client: reqwest::Client,
    /// Authentication configuration
    auth: Option<HttpAuth>,
    /// Write buffer
    buffer: Vec<u8>,
    /// Upload strategy
    strategy: HttpUploadStrategy,
    /// Size of each chunk for chunked upload
    upload_chunk_size: usize,
    /// Maximum retry attempts for failed uploads
    max_retries: usize,
    /// Upload state machine
    upload_state: UploadState,
    /// Channel ID counter
    next_channel_id: u16,
    /// Registered channels
    channels: HashMap<u16, ChannelInfo>,
    /// Message count
    message_count: u64,
    /// Whether the writer has been finished
    finished: bool,
}

impl HttpWriter {
    /// Create a new HTTP writer with default configuration.
    ///
    /// # Arguments
    ///
    /// * `url` - HTTP/HTTPS URL to write to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid
    /// - The HTTP client cannot be created
    pub async fn new(url: &str) -> Result<Self> {
        Self::with_config(
            url,
            None,
            HttpUploadStrategy::default(),
            DEFAULT_CHUNK_SIZE,
            DEFAULT_MAX_RETRIES,
        )
        .await
    }

    /// Create a new HTTP writer with authentication.
    ///
    /// # Arguments
    ///
    /// * `url` - HTTP/HTTPS URL to write to
    /// * `auth` - Authentication configuration
    pub async fn with_auth(url: &str, auth: Option<HttpAuth>) -> Result<Self> {
        Self::with_config(
            url,
            auth,
            HttpUploadStrategy::default(),
            DEFAULT_CHUNK_SIZE,
            DEFAULT_MAX_RETRIES,
        )
        .await
    }

    /// Create a new HTTP writer with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `url` - HTTP/HTTPS URL to write to
    /// * `auth` - Authentication configuration
    /// * `strategy` - Upload strategy to use
    /// * `upload_chunk_size` - Size of each chunk for chunked upload
    /// * `max_retries` - Maximum retry attempts for failed uploads
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid
    /// - The chunk size is too small (< 1MB)
    /// - The HTTP client cannot be created
    pub async fn with_config(
        url: &str,
        auth: Option<HttpAuth>,
        strategy: HttpUploadStrategy,
        upload_chunk_size: usize,
        max_retries: usize,
    ) -> Result<Self> {
        // Validate URL
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(CodecError::parse(
                "HttpWriter",
                HttpWriteError::InvalidUrl(url.to_string()).to_string(),
            ));
        }

        // Validate chunk size (minimum 1MB for ChunkedPut)
        if strategy == HttpUploadStrategy::ChunkedPut && upload_chunk_size < 1024 * 1024 {
            return Err(CodecError::parse(
                "HttpWriter",
                HttpWriteError::ChunkSizeTooSmall(upload_chunk_size).to_string(),
            ));
        }

        // Build HTTP client with authentication
        let client = Self::build_client(&auth)?;

        Ok(Self {
            url: url.to_string(),
            client,
            auth,
            buffer: Vec::with_capacity(upload_chunk_size),
            strategy,
            upload_chunk_size,
            max_retries,
            upload_state: UploadState::Initial,
            next_channel_id: 0,
            channels: HashMap::new(),
            message_count: 0,
            finished: false,
        })
    }

    /// Build a reqwest client with authentication configured.
    fn build_client(auth: &Option<HttpAuth>) -> Result<reqwest::Client> {
        let mut builder =
            reqwest::Client::builder().redirect(reqwest::redirect::Policy::limited(10));

        // Configure bearer token via default headers
        if let Some(auth) = auth
            && let Some(token) = auth.bearer_token()
        {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(value) = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
            {
                headers.insert(reqwest::header::AUTHORIZATION, value);
                builder = builder.default_headers(headers);
            }
        }

        builder
            .build()
            .map_err(|e| CodecError::parse("HttpWriter", format!("Failed to build client: {}", e)))
    }

    /// Write raw bytes to the buffer.
    fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        if self.finished {
            return Err(CodecError::parse(
                "HttpWriter",
                HttpWriteError::AlreadyFinished.to_string(),
            ));
        }

        // Check buffer size limit
        if self.buffer.len() + data.len() > MAX_BUFFER_SIZE {
            return Err(CodecError::parse(
                "HttpWriter",
                HttpWriteError::BufferSizeExceeded(MAX_BUFFER_SIZE).to_string(),
            ));
        }

        self.buffer.extend_from_slice(data);
        self.upload_state = UploadState::Buffering;

        Ok(())
    }

    /// Perform HTTP PUT request for single upload.
    async fn http_put(&self, data: Bytes) -> core::result::Result<(), HttpWriteError> {
        let mut request = self.client.put(&self.url);

        // Add basic auth if configured
        if let Some(auth) = &self.auth
            && let (Some(username), Some(password)) = (auth.basic_username(), auth.basic_password())
        {
            request = request.basic_auth(username, Some(password));
        }

        let response = request.body(data).send().await?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(HttpWriteError::ServerError(status.as_u16()))
        }
    }

    /// Perform HTTP PUT request with Content-Range for chunked upload.
    async fn http_put_range(
        &self,
        data: Bytes,
        offset: usize,
        total: usize,
    ) -> core::result::Result<(), HttpWriteError> {
        let mut request = self.client.put(&self.url);

        // Add basic auth if configured
        if let Some(auth) = &self.auth
            && let (Some(username), Some(password)) = (auth.basic_username(), auth.basic_password())
        {
            request = request.basic_auth(username, Some(password));
        }

        // Add Content-Range header
        let end = offset + data.len() - 1;
        request = request.header(
            reqwest::header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", offset, end, total),
        );

        let response = request.body(data).send().await?;

        let status = response.status();
        if status.is_success() || status.as_u16() == 206 {
            // 200 OK or 206 Partial Content
            Ok(())
        } else if status.as_u16() == 404 || status.as_u16() == 403 {
            // Server might not support Range requests
            Err(HttpWriteError::RangeNotSupported)
        } else {
            Err(HttpWriteError::ServerError(status.as_u16()))
        }
    }

    /// Check if the server supports Range requests.
    async fn check_range_support(&self) -> core::result::Result<bool, HttpWriteError> {
        let mut request = self.client.head(&self.url);

        // Add basic auth if configured
        if let Some(auth) = &self.auth
            && let (Some(username), Some(password)) = (auth.basic_username(), auth.basic_password())
        {
            request = request.basic_auth(username, Some(password));
        }

        let response = request.send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(HttpWriteError::ServerError(status.as_u16()));
        }

        // Check Accept-Ranges header
        let accepts_ranges = response
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

        Ok(accepts_ranges)
    }

    /// Upload buffer using SinglePut strategy.
    async fn upload_single_put(&mut self) -> core::result::Result<(), HttpWriteError> {
        let data = Bytes::from(self.buffer.clone());
        self.http_put(data).await?;
        self.upload_state = UploadState::Completed;
        Ok(())
    }

    /// Upload buffer using ChunkedPut strategy.
    async fn upload_chunked_put(&mut self) -> core::result::Result<(), HttpWriteError> {
        let total_size = self.buffer.len();

        // Check if server supports Range requests
        let supports_range = self.check_range_support().await?;
        if !supports_range {
            return Err(HttpWriteError::RangeNotSupported);
        }

        let mut offset = 0;
        while offset < total_size {
            let chunk_end = (offset + self.upload_chunk_size).min(total_size);
            let chunk = Bytes::from(self.buffer[offset..chunk_end].to_vec());

            self.http_put_range(chunk.clone(), offset, total_size)
                .await?;
            offset = chunk_end;
            self.upload_state = UploadState::Uploading;
        }

        self.upload_state = UploadState::Completed;
        Ok(())
    }

    /// Upload buffer with retry logic.
    async fn upload_with_retry(&mut self) -> core::result::Result<(), HttpWriteError> {
        let mut retries_left = self.max_retries;

        loop {
            let result = match self.strategy {
                HttpUploadStrategy::SinglePut => self.upload_single_put().await,
                HttpUploadStrategy::ChunkedPut => self.upload_chunked_put().await,
                HttpUploadStrategy::ChunkedEncoding => {
                    // For now, ChunkedEncoding falls back to SinglePut
                    // TODO: Implement true streaming chunked encoding
                    self.upload_single_put().await
                }
            };

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if retries_left == 0 {
                        self.upload_state = UploadState::Failed {
                            error: e.to_string(),
                            retries_left: 0,
                        };
                        return Err(HttpWriteError::UploadFailed(
                            self.max_retries,
                            e.to_string(),
                        ));
                    }
                    retries_left -= 1;
                    self.upload_state = UploadState::Failed {
                        error: e.to_string(),
                        retries_left,
                    };
                    // TODO: Add exponential backoff
                    continue;
                }
            }
        }
    }

    /// Get the target URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the upload strategy.
    pub fn strategy(&self) -> HttpUploadStrategy {
        self.strategy
    }

    /// Get the current buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }
}

impl FormatWriter for HttpWriter {
    fn path(&self) -> &str {
        // Extract path from URL
        self.url
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("output.mcap")
    }

    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        let id = self.next_channel_id;
        self.next_channel_id = id
            .checked_add(1)
            .ok_or_else(|| CodecError::parse("HttpWriter", "Channel ID overflow"))?;

        let channel = ChannelInfo {
            id,
            topic: topic.to_string(),
            message_type: message_type.to_string(),
            encoding: encoding.to_string(),
            schema: schema.map(|s| s.to_string()),
            schema_data: None,
            schema_encoding: None,
            message_count: 0,
            callerid: None,
        };

        self.channels.insert(id, channel);
        Ok(id)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        self.write_bytes(&message.data)?;
        self.message_count = self.message_count.saturating_add(1);
        Ok(())
    }

    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        for msg in messages {
            self.write(msg)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        // Upload the buffer
        if !self.buffer.is_empty() {
            // Use shared runtime for async operations
            let rt = shared_runtime();

            rt.block_on(async { self.upload_with_retry().await })
                .map_err(|e: HttpWriteError| CodecError::EncodeError {
                    codec: "HTTP".to_string(),
                    message: e.to_string(),
                })?;
        }

        self.finished = true;
        Ok(())
    }

    fn message_count(&self) -> u64 {
        self.message_count
    }

    fn channel_count(&self) -> usize {
        self.channels.len()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Get or create a shared Tokio runtime for blocking async operations.
///
/// This reuses a single runtime across all HTTP write operations, avoiding
/// the overhead of creating a new runtime for each operation.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;

    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

    RT.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("Failed to create shared tokio runtime")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_CHUNK_SIZE, 5 * 1024 * 1024);
        assert_eq!(MAX_BUFFER_SIZE, 50 * 1024 * 1024);
        assert_eq!(DEFAULT_MAX_RETRIES, 3);
    }

    #[test]
    fn test_upload_state_display() {
        assert_eq!(format!("{:?}", UploadState::Initial), "Initial");
        assert_eq!(format!("{:?}", UploadState::Buffering), "Buffering");
        assert_eq!(format!("{:?}", UploadState::Uploading), "Uploading");
        assert_eq!(format!("{:?}", UploadState::Completed), "Completed");
        assert_eq!(
            format!(
                "{:?}",
                UploadState::Failed {
                    error: "test".to_string(),
                    retries_left: 2
                }
            ),
            "Failed { error: \"test\", retries_left: 2 }"
        );
    }

    #[test]
    fn test_upload_state_equality() {
        let state1 = UploadState::Initial;
        let state2 = UploadState::Initial;
        assert_eq!(state1, state2);

        let state3 = UploadState::Buffering;
        assert_ne!(state1, state3);
    }

    #[test]
    fn test_http_write_error_display() {
        let err = HttpWriteError::ServerError(500);
        assert_eq!(format!("{}", err), "Server returned error status: 500");

        let err = HttpWriteError::RangeNotSupported;
        assert_eq!(
            format!("{}", err),
            "Server does not support Range requests for chunked upload"
        );

        let err = HttpWriteError::AlreadyFinished;
        assert_eq!(format!("{}", err), "Upload already finished");

        let err = HttpWriteError::BufferSizeExceeded(1000);
        assert_eq!(format!("{}", err), "Buffer size exceeded: 1000 bytes");
    }

    #[test]
    fn test_upload_strategy_requires_range_support() {
        assert!(!HttpUploadStrategy::SinglePut.requires_range_support());
        assert!(HttpUploadStrategy::ChunkedPut.requires_range_support());
        assert!(!HttpUploadStrategy::ChunkedEncoding.requires_range_support());
    }

    #[test]
    fn test_upload_strategy_is_streaming() {
        assert!(!HttpUploadStrategy::SinglePut.is_streaming());
        assert!(!HttpUploadStrategy::ChunkedPut.is_streaming());
        assert!(HttpUploadStrategy::ChunkedEncoding.is_streaming());
    }

    #[tokio::test]
    async fn test_http_writer_new_invalid_url() {
        let result = HttpWriter::new("ftp://example.com/file.mcap").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_writer_new_valid_url() {
        let result = HttpWriter::new("https://example.com/file.mcap").await;
        assert!(result.is_ok());

        let writer = result.unwrap();
        assert_eq!(writer.url(), "https://example.com/file.mcap");
        assert_eq!(writer.strategy(), HttpUploadStrategy::default());
        assert_eq!(writer.buffer_size(), 0);
    }

    #[tokio::test]
    async fn test_http_writer_chunk_size_too_small() {
        let result = HttpWriter::with_config(
            "https://example.com/file.mcap",
            None,
            HttpUploadStrategy::ChunkedPut,
            512 * 1024, // 512KB, less than 1MB minimum
            3,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_writer_add_channel() {
        let mut writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        let id = writer
            .add_channel("/test", "std_msgs/String", "cdr", None)
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(writer.channel_count(), 1);

        let id2 = writer
            .add_channel("/test2", "std_msgs/Header", "cdr", None)
            .unwrap();
        assert_eq!(id2, 1);
        assert_eq!(writer.channel_count(), 2);
    }

    #[tokio::test]
    async fn test_http_writer_write() {
        let mut writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3, 4],
            sequence: None,
        };

        writer.write(&msg).unwrap();
        assert_eq!(writer.message_count(), 1);
        assert_eq!(writer.buffer_size(), 4);
    }

    #[tokio::test]
    async fn test_http_writer_write_batch() {
        let mut writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3, 4],
            sequence: None,
        };

        writer
            .write_batch(&[msg.clone(), msg.clone(), msg.clone()])
            .unwrap();
        assert_eq!(writer.message_count(), 3);
        assert_eq!(writer.buffer_size(), 12);
    }

    #[tokio::test]
    async fn test_http_writer_write_after_finish() {
        let mut writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        // Mark as finished
        writer.finished = true;

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3, 4],
            sequence: None,
        };

        let result = writer.write(&msg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already finished"));
    }

    #[tokio::test]
    async fn test_http_writer_buffer_size_limit() {
        let mut writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        // Fill buffer to near max limit
        writer.buffer.resize(MAX_BUFFER_SIZE - 100, 0);

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1; 200], // Exceeds limit
            sequence: None,
        };

        let result = writer.write(&msg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Buffer size"));
    }

    #[tokio::test]
    async fn test_http_writer_channel_id_overflow() {
        let mut writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        // Set next_channel_id to max value
        writer.next_channel_id = u16::MAX;

        let result = writer.add_channel("/test", "type", "cdr", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));
    }

    #[tokio::test]
    async fn test_http_writer_path() {
        let writer = HttpWriter::new("https://example.com/path/to/file.mcap")
            .await
            .unwrap();

        assert_eq!(writer.path(), "file.mcap");
    }

    #[tokio::test]
    async fn test_http_writer_path_no_extension() {
        let writer = HttpWriter::new("https://example.com/data").await.unwrap();

        assert_eq!(writer.path(), "data");
    }

    #[tokio::test]
    async fn test_http_writer_with_auth() {
        let auth = HttpAuth::bearer("test-token");
        let writer = HttpWriter::with_auth("https://example.com/file.mcap", Some(auth))
            .await
            .unwrap();

        assert_eq!(writer.url(), "https://example.com/file.mcap");
        assert!(writer.auth.is_some());
    }

    #[tokio::test]
    async fn test_http_writer_downcast() {
        let writer = HttpWriter::new("https://example.com/file.mcap")
            .await
            .unwrap();

        let as_any: &dyn std::any::Any = writer.as_any();
        assert!(as_any.is::<HttpWriter>());
    }
}
