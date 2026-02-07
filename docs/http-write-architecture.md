# HTTP/HTTPS Write Support Architecture

## Overview

This document outlines the architecture for supporting HTTP/HTTPS write operations in robocodec. The design follows the existing pattern used for S3 write support, leveraging the `FormatWriter` trait and HTTP PUT requests.

## Design Goals

1. **Consistent API**: HTTP write should work seamlessly with existing `RoboWriter` API
2. **Authentication Support**: Support Bearer tokens and Basic auth via `WriterConfig`
3. **Efficient Upload**: Support chunked/streaming upload to avoid buffering entire file in memory
4. **Error Recovery**: Handle HTTP errors gracefully with retry logic
5. **Minimal Dependencies**: Use existing `reqwest` crate without adding new dependencies

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Public API Layer                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ RoboWriter::create("https://example.com/output.mcap")?   │  │
│  │ RoboWriter::create_with_config(url, WriterConfig)?        │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      WriterConfig Layer                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ WriterConfig {                                           │  │
│  │   path, strategy, compression_level,                     │  │
│  │   chunk_size, num_threads,                                │  │
│  │   // NEW:                                                │  │
│  │   http_auth: HttpAuthConfig,                             │  │
│  │   upload_chunk_size: usize,  // HTTP upload chunk size    │  │
│  │   max_retries: usize,          // Retry failed uploads    │  │
│  │ }                                                          │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      URL Detection Layer                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ parse_url_to_writer(path, WriterConfig)                    │  │
│  │   │                                                         │  │
│  │   ├─ s3://  → S3Writer                                   │  │
│  │   ├─ http:// → NEW: HttpWriter                           │  │
│  │   ├─ https:// → NEW: HttpWriter                          │  │
│  │   └─ <local> → Local format writer (McapFormat, etc.)     │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HttpWriter Implementation                    │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ struct HttpWriter {                                      │  │
│  │   url: String,              // HTTP/HTTPS URL              │  │
│  │   client: reqwest::Client,  // HTTP client with auth       │  │
│  │   auth: Option<HttpAuth>,    // Authentication config       │  │
│  │   buffer: Vec<u8>,          // Write buffer                │  │
│  │   buffer_size: usize,       // Buffer size threshold       │  │
│  │   upload_chunk_size: usize, // HTTP chunk size             │  │
│  │   max_retries: usize,       // Max retry attempts         │  │
│  │   next_channel_id: u16,    // Channel ID counter          │  │
│  │   channels: HashMap<...>,  // Registered channels         │  │
│  │   message_count: u64,       // Message counter             │  │
│  │   finished: bool,          // Completion flag             │  │
│  │   upload_state: UploadState, // State machine              │  │
│  │ }                                                          │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ enum UploadState {                                        │  │
│  │   Initial,       // No data written yet                  │  │
│  │   Buffering,     // Accumulating data in buffer          │  │
│  │   Uploading,     // HTTP PUT in progress                 │  │
│  │   Completed,     // Upload finished                      │  │
│  │   Failed,        // Upload failed, retry pending          │  │
│  │ }                                                          │  │
│  └───────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Upload Strategies                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ enum HttpUploadStrategy {                                │  │
│  │   // Single PUT request (for small files)                 │  │
│  │   SinglePut,                                              │  │
│  │   // Chunked upload (multiple PUT requests with range)    │  │
│  │   ChunkedPut,                                             │  │
│  │   // Streaming upload (Transfer-Encoding: chunked)         │  │
│  │   ChunkedEncoding,                                        │  │
│  │ }                                                          │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Components

### 1. HttpWriter (NEW)

**File**: `src/io/transport/http/writer.rs`

```rust
/// Writer for HTTP/HTTPS URLs.
///
/// This writer buffers data and uploads to HTTP server using PUT requests.
/// Supports chunked upload for large files and authentication.
pub struct HttpWriter {
    /// Target URL
    url: String,
    /// HTTP client with authentication configured
    client: reqwest::Client,
    /// Authentication configuration
    auth: Option<HttpAuth>,
    /// Write buffer
    buffer: Vec<u8>,
    /// Buffer size threshold before triggering upload
    buffer_size: usize,
    /// Size of each chunk for chunked upload
    upload_chunk_size: usize,
    /// Maximum retry attempts for failed uploads
    max_retries: usize,
    /// Upload state machine
    upload_state: UploadState,
    /// Format-specific metadata
    format_writer: Box<dyn crate::io::traits::FormatWriter>,
    /// Channel ID counter
    next_channel_id: u16,
    /// Registered channels
    channels: HashMap<u16, ChannelInfo>,
    /// Message count
    message_count: u64,
    /// Whether the writer has been finished
    finished: bool,
}

enum UploadState {
    Initial,
    Buffering,
    Uploading,
    Completed,
    Failed { error: String, retries_left: usize },
}
```

**Key Methods**:

```rust
impl HttpWriter {
    /// Create a new HTTP writer.
    pub async fn new(url: &str, auth: Option<HttpAuth>) -> Result<Self>;

    /// Create with custom configuration.
    pub async fn with_config(
        url: &str,
        auth: Option<HttpAuth>,
        buffer_size: usize,
        upload_chunk_size: usize,
        max_retries: usize,
    ) -> Result<Self>;

    /// Flush buffer to HTTP server.
    async fn flush(&mut self) -> Result<()>;

    /// Retry a failed upload.
    async fn retry_upload(&mut self) -> Result<()>;

    /// Perform HTTP PUT request.
    async fn http_put(&self, data: &[u8], offset: usize, total: Option<usize>)
        -> Result<reqwest::Response>;
}

impl FormatWriter for HttpWriter {
    // Delegate to format_writer for format-specific operations
    // Upload on finish()
}
```

### 2. Updated WriterConfig

**File**: `src/io/writer/builder.rs`

```rust
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub path: PathBuf,
    pub strategy: WriteStrategy,
    pub compression_level: Option<i32>,
    pub chunk_size: Option<usize>,
    pub num_threads: Option<usize>,
    // NEW: HTTP authentication configuration
    pub http_auth: HttpAuthConfig,
    // NEW: Upload chunk size for HTTP (default: 5MB)
    pub http_upload_chunk_size: usize,
    // NEW: Max retries for HTTP upload (default: 3)
    pub http_max_retries: usize,
}

#[derive(Debug, Clone, Default)]
pub struct HttpAuthConfig {
    pub bearer_token: Option<String>,
    pub basic_username: Option<String>,
    pub basic_password: Option<String>,
}

impl HttpAuthConfig {
    pub fn bearer(token: impl Into<String>) -> Self;
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self;
}

impl WriterConfigBuilder {
    // NEW: Add HTTP auth methods
    pub fn http_bearer_token(mut self, token: impl Into<String>) -> Self;
    pub fn http_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self;
    pub fn http_upload_chunk_size(mut self, size: usize) -> Self;
    pub fn http_max_retries(mut self, retries: usize) -> Self;
}
```

### 3. URL Detection in RoboWriter

**File**: `src/io/writer/mod.rs`

```rust
impl RoboWriter {
    pub fn create_with_config(path: &str, config: WriterConfig) -> Result<Self> {
        // Detect URL scheme
        #[cfg(feature = "s3")]
        {
            // S3 URL detection (existing)
            if let Ok(location) = crate::io::s3::S3Location::from_s3_url(path) {
                // ... existing S3Writer creation
            }

            // NEW: HTTP/HTTPS URL detection
            if path.starts_with("http://") || path.starts_with("https://") {
                return Self::create_http_writer(path, config);
            }
        }

        // Local file handling (existing)
        // ...
    }

    #[cfg(feature = "s3")]
    fn create_http_writer(path: &str, config: WriterConfig) -> Result<Self> {
        use crate::io::transport::http::HttpWriter;

        // Parse auth from config or URL query parameters
        let auth = Self::resolve_http_auth(path, &config.http_auth);

        let rt = shared_runtime();
        let writer = rt.block_on(async {
            HttpWriter::with_config(
                path,
                auth,
                config.http_upload_chunk_size,
                config.http_max_retries,
            ).await
        })?;

        Ok(Self { inner: Box::new(writer) })
    }
}
```

### 4. Upload Strategies

**File**: `src/io/transport/http/upload_strategy.rs`

```rust
/// HTTP upload strategy.
#[derive(Debug, Clone, Copy)]
pub enum HttpUploadStrategy {
    /// Single PUT request for the entire file.
    /// Simple but requires entire file in memory.
    SinglePut,

    /// Chunked upload using multiple PUT requests with Content-Range.
    /// Server must support Range requests.
    ChunkedPut,

    /// Streaming upload using Transfer-Encoding: chunked.
    /// Most efficient but server support varies.
    ChunkedEncoding,
}

impl Default for HttpUploadStrategy {
    fn default() -> Self {
        // Default to ChunkedPut as balance between efficiency and compatibility
        Self::ChunkedPut
    }
}
```

### 5. Error Handling

```rust
/// HTTP-specific write errors.
#[derive(Debug, thiserror::Error)]
pub enum HttpWriteError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("Server returned error status: {0}")]
    ServerError(u16),

    #[error("Upload failed after {0} retries: {1}")]
    UploadFailed(usize, String),

    #[error("Server does not support Range requests for chunked upload")]
    RangeNotSupported,

    #[error("Buffer size exceeded: {0} bytes")]
    BufferSizeExceeded(usize),

    #[error("Upload already finished")]
    AlreadyFinished,

    #[error("Upload already in progress")]
    AlreadyInProgress,
}
```

## Usage Examples

### Basic HTTP Write

```rust
use robocodec::io::RoboWriter;

let mut writer = RoboWriter::create("https://example.com/output.mcap")?;
let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;

// Write messages
writer.write(&RawMessage { /* ... */ })?;

// Finish triggers upload
writer.finish()?;
```

### With Authentication

```rust
use robocodec::io::{RoboWriter, WriterConfig};

let config = WriterConfig::builder()
    .http_bearer_token("your-token")
    .http_upload_chunk_size(10 * 1024 * 1024)  // 10MB chunks
    .build();

let mut writer = RoboWriter::create_with_config(
    "https://example.com/output.mcap",
    config
)?;
// ... write messages ...
writer.finish()?;
```

### With URL Query Parameters

```rust
use robocodec::io::RoboWriter;

// Auth in URL
let mut writer = RoboWriter::create(
    "https://user:pass@example.com/output.mcap"
)?;
// ... write messages ...
writer.finish()?;
```

## Server Requirements

For HTTP write to work, the server must support **one** of:

1. **Range requests** (for `ChunkedPut` strategy) - Recommended
   - Server responds to `HEAD` with `Accept-Ranges: bytes`
   - Server responds to `PUT` with `Content-Range` header

2. **Single PUT** (for `SinglePut` strategy)
   - Server accepts entire file in one PUT request
   - Limited by available memory

3. **Transfer-Encoding: chunked** (for `ChunkedEncoding` strategy)
   - Server accepts chunked transfer encoding
   - Most modern HTTP servers support this

## Implementation Phases

### Phase 1: Basic SinglePut (MVP)
- Implement `HttpWriter` with single PUT upload
- Basic authentication support
- No chunking (entire file in memory)

### Phase 2: Chunked Upload
- Add `ChunkedPut` strategy
- Detect server Range support via HEAD
- Implement retry logic for failed chunks

### Phase 3: Advanced Features
- Add `ChunkedEncoding` strategy
- Progress callbacks for large uploads
- Pause/resume capability

### Phase 4: Optimization
- Parallel chunk upload (if server supports multiple ranges)
- Compression before upload
- Deduplication for incremental updates

## Testing Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_writer_creation() {
        // Test URL detection
        let url = "https://example.com/test.mcap";
        assert!(is_http_url(url));
    }

    #[test]
    fn test_auth_config() {
        let config = HttpAuthConfig::bearer("token");
        assert!(config.bearer_token.is_some());
    }

    // Integration tests with mock HTTP server
    #[tokio::test]
    async fn test_http_upload_bearer_token() {
        // Mock server expecting Bearer token
        // Verify auth header sent correctly
    }

    #[tokio::test]
    async fn test_http_upload_basic_auth() {
        // Mock server expecting Basic auth
        // Verify auth header sent correctly
    }

    #[tokio::test]
    async fn test_chunked_upload() {
        // Mock server supporting Range requests
        // Verify chunks uploaded correctly
    }

    #[tokio::test]
    async fn test_retry_on_failure() {
        // Mock server that fails first request
        // Verify retry logic works
    }
}
```

## Comparison with S3Writer

| Feature | S3Writer | HttpWriter (proposed) |
|---------|-----------|----------------------|
| Multipart upload | ✅ S3-specific | ❌ N/A (HTTP doesn't have standard multipart upload API) |
| Range-based chunking | ❌ N/A | ✅ If server supports Range |
| Authentication | AWS SigV4 | Bearer / Basic |
| Retry logic | Custom | Custom |
| Buffer strategy | Part-based | Chunk-based |
| Streaming | ✅ | ✅ (Transfer-Encoding) |

## Open Questions

1. **Should we support POST vs PUT?**
   - PUT is more RESTful for creating/replacing a resource
   - POST might be more compatible with some APIs
   - Decision: Default to PUT, allow POST via config?

2. **How to handle partial failure?**
   - If server supports Range, we can retry individual chunks
   - If server doesn't support Range, entire upload must be retried
   - Consider temporary file fallback for very large files?

3. **Progress reporting?**
   - Add callback trait for upload progress?
   - Return a progress handle from `create()`?
