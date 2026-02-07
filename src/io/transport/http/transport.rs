// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP transport implementation using the Transport trait.
//!
//! This module provides [`HttpTransport`], which implements the [`Transport`]
//! trait for HTTP/HTTPS URLs.
//!
//! # Features
//!
//! - **Range requests**: Supports HTTP range requests for seeking
//! - **HEAD requests**: Uses HEAD to determine content length
//! - **Buffering**: Buffers data for efficient reading
//! - **Redirect handling**: Follows HTTP redirects automatically
//! - **Authentication**: Supports Bearer tokens and Basic auth
//!
//! # Example
//!
//! ```rust,no_run
//! use robocodec::io::transport::{http::HttpTransport, Transport, TransportExt};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create HTTP transport
//! let mut transport = HttpTransport::new("https://example.com/data.mcap").await?;
//!
//! // Read from HTTP
//! let mut buf = vec![0u8; 4096];
//! let n = transport.read(&mut buf).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication
//!
//! For authenticated HTTP endpoints, you can configure authentication:
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use robocodec::io::transport::http::HttpTransport;
//!
//! // Bearer token (OAuth2/JWT)
//! let transport = HttpTransport::new("https://example.com/data.mcap")
//!     .await?
//!     .with_bearer_token("your-token");
//!
//! // Basic authentication
//! let transport = HttpTransport::new("https://example.com/data.mcap")
//!     .await?
//!     .with_basic_auth("username", "password");
//! # Ok(())
//! # }
//! ```

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::FutureExt;

use crate::io::transport::Transport;

/// Default buffer size for HTTP reads (64KB).
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Authentication configuration for HTTP requests.
#[derive(Debug, Clone, Default)]
pub struct HttpAuth {
    /// Bearer token (OAuth2/JWT)
    bearer_token: Option<String>,
    /// Basic auth username
    basic_username: Option<String>,
    /// Basic auth password
    basic_password: Option<String>,
}

impl HttpAuth {
    /// Create bearer token authentication.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            bearer_token: Some(token.into()),
            basic_username: None,
            basic_password: None,
        }
    }

    /// Create basic authentication.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            bearer_token: None,
            basic_username: Some(username.into()),
            basic_password: Some(password.into()),
        }
    }

    /// Get the bearer token if configured.
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    /// Get the basic auth username if configured.
    pub fn basic_username(&self) -> Option<&str> {
        self.basic_username.as_deref()
    }

    /// Get the basic auth password if configured.
    pub fn basic_password(&self) -> Option<&str> {
        self.basic_password.as_deref()
    }
}

/// HTTP transport implementation.
///
/// Wraps an HTTP URL and implements the async `Transport` trait.
/// Supports range-based reads and seeking when the server supports it.
///
/// # Seeking
///
/// Seeking is supported when the HTTP server supports range requests.
/// If the server doesn't support range requests, `is_seekable()` returns `false`
/// and seek operations will fail.
pub struct HttpTransport {
    /// The HTTP URL being accessed
    url: String,
    /// HTTP client for making requests
    client: reqwest::Client,
    /// Authentication configuration
    auth: Option<HttpAuth>,
    /// Current position in the resource
    pos: u64,
    /// Total resource length (None if unknown)
    len: Option<u64>,
    /// Whether the server supports range requests
    supports_range: bool,
    /// Read buffer for data fetched from HTTP
    buffer: Vec<u8>,
    /// Current read offset within the buffer
    buffer_offset: usize,
    /// Pending fetch future (for poll_read)
    fetch_future: Option<FetchFuture>,
    /// Whether to use basic auth (stored for per-request configuration)
    use_basic_auth: bool,
    /// Basic auth username (if configured)
    basic_username: Option<String>,
    /// Basic auth password (if configured)
    basic_password: Option<String>,
}

/// Future for fetching a range via HTTP.
type FetchFuture = futures::future::BoxFuture<'static, Result<Bytes, HttpError>>;

/// HTTP-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    /// Invalid HTTP response
    #[error("Invalid HTTP response: {0}")]
    InvalidResponse(String),

    /// Server returned error status
    #[error("Server returned error status: {0}")]
    ServerError(u16),

    /// Content length not available
    #[error("Content length not available")]
    NoContentLength,

    /// Range requests not supported
    #[error("Range requests not supported by server")]
    RangeNotSupported,
}

impl HttpTransport {
    /// Create a new HTTP transport.
    ///
    /// This will fetch the resource metadata via HEAD request to determine
    /// the size and whether range requests are supported.
    ///
    /// # Arguments
    ///
    /// * `url` - HTTP/HTTPS URL to access
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid
    /// - The HEAD request fails
    /// - The server returns an error status
    pub async fn new(url: impl AsRef<str>) -> Result<Self, HttpError> {
        Self::with_auth(url.as_ref(), None).await
    }

    /// Create a new HTTP transport with authentication.
    ///
    /// # Arguments
    ///
    /// * `url` - HTTP/HTTPS URL to access
    /// * `auth` - Authentication configuration
    pub async fn with_auth(url: &str, auth: Option<HttpAuth>) -> Result<Self, HttpError> {
        let url = url.to_string();
        let (client, use_basic_auth, basic_username, basic_password) = Self::build_client(&auth)?;
        let (len, supports_range) = Self::fetch_metadata(&client, &url).await?;

        Ok(Self {
            url,
            client,
            auth,
            pos: 0,
            len,
            supports_range,
            buffer: Vec::new(),
            buffer_offset: 0,
            fetch_future: None,
            use_basic_auth,
            basic_username,
            basic_password,
        })
    }

    /// Build a reqwest client with authentication configured.
    fn build_client(
        auth: &Option<HttpAuth>,
    ) -> Result<(reqwest::Client, bool, Option<String>, Option<String>), reqwest::Error> {
        let mut builder =
            reqwest::Client::builder().redirect(reqwest::redirect::Policy::limited(10));

        // Configure bearer token via default headers (basic auth is per-request)
        let use_basic_auth = auth.as_ref().is_some_and(|a| a.basic_username.is_some());
        let basic_username = auth.as_ref().and_then(|a| a.basic_username.clone());
        let basic_password = auth.as_ref().and_then(|a| a.basic_password.clone());

        if let Some(auth) = auth
            && let Some(token) = &auth.bearer_token
        {
            // Bearer token via default headers
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(value) = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
            {
                headers.insert(reqwest::header::AUTHORIZATION, value);
                builder = builder.default_headers(headers);
            }
        }

        let client = builder.build()?;
        Ok((client, use_basic_auth, basic_username, basic_password))
    }

    /// Create a new HTTP transport with a known size.
    ///
    /// This skips the initial HEAD request when the size is already known.
    /// Range request support will be detected on first read.
    ///
    /// # Arguments
    ///
    /// * `url` - HTTP/HTTPS URL to access
    /// * `len` - Known content length
    pub fn with_size(url: impl AsRef<str>, len: u64) -> Self {
        let url = url.as_ref().to_string();
        let (client, use_basic_auth, basic_username, basic_password) =
            Self::build_client(&None).expect("Failed to create HTTP client");

        Self {
            url,
            client,
            auth: None,
            pos: 0,
            len: Some(len),
            supports_range: true,
            buffer: Vec::new(),
            buffer_offset: 0,
            fetch_future: None,
            use_basic_auth,
            basic_username,
            basic_password,
        }
    }

    /// Set bearer token authentication.
    ///
    /// Returns a new transport with bearer token configured.
    /// This is useful for OAuth2/JWT authentication.
    ///
    /// # Arguments
    ///
    /// * `token` - Bearer token (e.g., JWT)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use robocodec::io::transport::http::HttpTransport;
    ///
    /// let transport = HttpTransport::new("https://example.com/data.mcap")
    ///     .await?
    ///     .with_bearer_token("your-token-here");
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_bearer_token(mut self, token: &str) -> Self {
        self.auth = Some(HttpAuth::bearer(token));
        let (client, use_basic_auth, basic_username, basic_password) =
            Self::build_client(&self.auth).expect("Failed to create HTTP client with bearer token");
        self.client = client;
        self.use_basic_auth = use_basic_auth;
        self.basic_username = basic_username;
        self.basic_password = basic_password;
        self
    }

    /// Set basic authentication.
    ///
    /// Returns a new transport with basic auth configured.
    ///
    /// # Arguments
    ///
    /// * `username` - HTTP username
    /// * `password` - HTTP password
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use robocodec::io::transport::http::HttpTransport;
    ///
    /// let transport = HttpTransport::new("https://example.com/data.mcap")
    ///     .await?
    ///     .with_basic_auth("user", "pass");
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_basic_auth(mut self, username: &str, password: &str) -> Self {
        self.auth = Some(HttpAuth::basic(username, password));
        let (client, use_basic_auth, basic_username, basic_password) =
            Self::build_client(&self.auth).expect("Failed to create HTTP client with basic auth");
        self.client = client;
        self.use_basic_auth = use_basic_auth;
        self.basic_username = basic_username;
        self.basic_password = basic_password;
        self
    }

    /// Fetch metadata via HEAD request.
    async fn fetch_metadata(
        client: &reqwest::Client,
        url: &str,
    ) -> Result<(Option<u64>, bool), HttpError> {
        let response = client.head(url).send().await?;

        let status = response.status();
        if !status.is_success() {
            return Err(HttpError::ServerError(status.as_u16()));
        }

        // Check Content-Length
        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        // Check Accept-Ranges for range request support
        let accepts_ranges = response
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

        Ok((content_length, accepts_ranges))
    }

    /// Fill the internal buffer by fetching from HTTP.
    ///
    /// Fetches up to `size` bytes starting at the current position.
    fn fetch_data(&self, size: usize) -> FetchFuture {
        let client = self.client.clone();
        let url = self.url.clone();
        let offset = self.pos;
        let use_basic_auth = self.use_basic_auth;
        let basic_username = self.basic_username.clone();
        let basic_password = self.basic_password.clone();

        async move {
            let mut request = client.get(&url);

            // Add basic auth if configured
            if use_basic_auth
                && let (Some(username), Some(password)) = (basic_username, basic_password)
            {
                request = request.basic_auth(username, Some(password));
            }

            // Add Range header for partial content
            let end = offset.saturating_add(size as u64).saturating_sub(1);
            request = request.header(reqwest::header::RANGE, format!("bytes={}-{}", offset, end));

            let response = request.send().await?;

            let status = response.status();
            if status.is_success() {
                // 200 OK - full content
                let bytes = response.bytes().await?;
                Ok(bytes)
            } else if status == 206 {
                // 206 Partial Content - range request successful
                let bytes = response.bytes().await?;
                Ok(bytes)
            } else if status == 416 {
                // Range Not Satisfiable - requested range beyond resource
                Ok(Bytes::new())
            } else {
                Err(HttpError::ServerError(status.as_u16()))
            }
        }
        .boxed()
    }

    /// Get the URL being accessed.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get a reference to the HTTP client.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

// Implement Unpin for HttpTransport (needed for Transport async methods)
impl Unpin for HttpTransport {}

// SAFETY: HttpTransport is safe to share between threads because:
// - The Transport trait requires poll_read/poll_seek to take Pin<&mut Self>, guaranteeing exclusive access
// - All fields are either Send + Sync (client is Send + Sync, url is String, pos/len are u64, etc.)
// - reqwest::Client is designed to be Send + Sync
// - The futures are only accessed through &mut self in poll_read/poll_seek
unsafe impl Sync for HttpTransport {}

impl Transport for HttpTransport {
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

        // Check if we're at EOF (only if we know the length)
        if let Some(len) = self.len
            && self.pos >= len
        {
            return Poll::Ready(Ok(0));
        }

        // Start or continue a fetch
        if self.fetch_future.is_none() {
            // Fetch a chunk (64KB default)
            let chunk_size = DEFAULT_BUFFER_SIZE;
            self.fetch_future = Some(self.fetch_data(chunk_size));
        }

        // Poll the fetch future
        let fetch_result = self.fetch_future.as_mut().as_mut().unwrap().poll_unpin(cx);

        match fetch_result {
            Poll::Ready(Ok(data)) => {
                self.fetch_future = None;

                // If we got empty data, we're at EOF
                if data.is_empty() {
                    return Poll::Ready(Ok(0));
                }

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
        if !self.supports_range {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "HTTP server does not support range requests",
            )));
        }

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
        // HTTP supports range requests, so we don't need to fetch
        self.buffer.clear();
        self.buffer_offset = 0;

        // Clamp to known length if available
        if let Some(len) = self.len {
            self.pos = pos.min(len);
        } else {
            self.pos = pos;
        }

        Poll::Ready(Ok(self.pos))
    }

    fn position(&self) -> u64 {
        self.pos
    }

    fn len(&self) -> Option<u64> {
        self.len
    }

    fn is_seekable(&self) -> bool {
        self.supports_range
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_auth_bearer() {
        let auth = HttpAuth::bearer("test-token");
        assert!(auth.bearer_token().is_some());
        assert!(auth.basic_username().is_none());
        assert_eq!(auth.bearer_token(), Some("test-token"));
    }

    #[test]
    fn test_http_auth_basic() {
        let auth = HttpAuth::basic("user", "pass");
        assert!(auth.bearer_token().is_none());
        assert_eq!(auth.basic_username(), Some("user"));
        assert_eq!(auth.basic_password(), Some("pass"));
    }

    #[test]
    fn test_http_transport_with_size() {
        let transport = HttpTransport::with_size("https://example.com/data.mcap", 1024);

        assert_eq!(transport.url(), "https://example.com/data.mcap");
        assert_eq!(transport.len(), Some(1024));
        assert_eq!(transport.position(), 0);
        assert!(transport.is_seekable());
    }

    #[test]
    fn test_http_transport_seek_within_bounds() {
        let mut transport = HttpTransport::with_size("https://example.com/data.mcap", 1024);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Seek to middle of file
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 512);
        assert!(matches!(poll, Poll::Ready(Ok(512))));
        assert_eq!(transport.position(), 512);
    }

    #[test]
    fn test_http_transport_seek_past_end() {
        let mut transport = HttpTransport::with_size("https://example.com/data.mcap", 1024);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Seek past end of file
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 2048);
        assert!(matches!(poll, Poll::Ready(Ok(1024)))); // Clamped to file size
        assert_eq!(transport.position(), 1024);
    }

    #[test]
    fn test_http_transport_eof() {
        let mut transport = HttpTransport::with_size("https://example.com/data.mcap", 100);

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

    #[test]
    fn test_http_transport_seek_within_buffer() {
        let mut transport = HttpTransport::with_size("https://example.com/data.mcap", 1024);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Simulate having data in the buffer
        transport.buffer = vec![1, 2, 3, 4, 5];
        transport.buffer_offset = 2;
        transport.pos = 2;

        // Seek within buffer (to position 3)
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 3);
        assert!(matches!(poll, Poll::Ready(Ok(3))));
        assert_eq!(transport.position(), 3);
        assert_eq!(transport.buffer_offset, 3);
    }

    #[test]
    fn test_http_transport_seek_clears_buffer() {
        let mut transport = HttpTransport::with_size("https://example.com/data.mcap", 1024);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Simulate having data in the buffer at position 0-4
        transport.buffer = vec![1, 2, 3, 4, 5];
        transport.buffer_offset = 2;
        transport.pos = 2;

        // Seek outside buffer (to position 100)
        let poll = Pin::new(&mut transport).poll_seek(&mut cx, 100);
        assert!(matches!(poll, Poll::Ready(Ok(100))));
        assert_eq!(transport.position(), 100);
        assert!(transport.buffer.is_empty());
        assert_eq!(transport.buffer_offset, 0);
    }

    #[test]
    fn test_http_transport_unknown_length_seekable() {
        // Create transport with unknown length but assuming range support
        let transport = HttpTransport::with_size("https://example.com/data.mcap", 0);
        let transport_with_unknown = HttpTransport {
            len: None,
            ..transport
        };

        // Should still be seekable if range requests are supported
        assert!(transport_with_unknown.is_seekable());
        assert_eq!(transport_with_unknown.len(), None);
    }

    #[test]
    fn test_http_error_display() {
        let err = HttpError::InvalidResponse("test error".to_string());
        assert_eq!(format!("{}", err), "Invalid HTTP response: test error");

        let err = HttpError::ServerError(404);
        assert_eq!(format!("{}", err), "Server returned error status: 404");

        let err = HttpError::RangeNotSupported;
        assert_eq!(format!("{}", err), "Range requests not supported by server");
    }
}
