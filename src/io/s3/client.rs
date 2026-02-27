// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP client for S3 streaming operations.

use crate::io::s3::signer::hex_sha256;
use crate::io::s3::{config::S3ReaderConfig, error::FatalError, location::S3Location, signer};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Uri};
use std::str::FromStr;
use tokio::time::sleep;

/// Default AWS region when not specified.
const DEFAULT_AWS_REGION: &str = "us-east-1";

/// HTTP client for S3 operations.
///
/// Wraps a `reqwest::Client` with S3-specific configuration for
/// streaming operations with HTTP Range requests.
#[derive(Clone)]
pub struct S3Client {
    /// The underlying HTTP client
    client: reqwest::Client,
    /// Configuration for the S3 operations
    config: S3ReaderConfig,
}

impl S3Client {
    /// Create a new S3 client with the given configuration.
    #[must_use = "client creation can fail if configuration is invalid"]
    pub fn new(config: S3ReaderConfig) -> Result<Self, FatalError> {
        config.validate().map_err(|e| FatalError::ConfigError {
            message: e.to_string(),
        })?;

        let client_builder = reqwest::Client::builder()
            .timeout(config.request_timeout())
            .pool_max_idle_per_host(config.pool_max_idle());

        let client = if config.validate_ssl() {
            client_builder
        } else {
            client_builder.danger_accept_invalid_certs(true)
        };

        let client = client.build().map_err(|e| FatalError::ConfigError {
            message: format!("Failed to create HTTP client: {e}"),
        })?;

        Ok(Self { client, config })
    }

    /// Create a new S3 client with default configuration.
    pub fn default_client() -> Result<Self, FatalError> {
        Self::new(S3ReaderConfig::default())
    }

    /// Fetch a byte range from the S3 object.
    ///
    /// Uses HTTP Range requests to efficiently fetch partial data.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to fetch from
    /// * `offset` - Starting byte offset
    /// * `length` - Number of bytes to fetch
    ///
    /// # Returns
    ///
    /// The requested bytes as a `Bytes` object.
    pub async fn fetch_range(
        &self,
        location: &S3Location,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, FatalError> {
        // Handle zero-length request
        if length == 0 {
            return Ok(Bytes::new());
        }

        let url = location.url();
        let range_header = format!("bytes={}-{}", offset, offset + length - 1);

        // Build and send the request
        let response = self
            .build_signed_get_request(&url, &Method::GET, location, |headers| {
                Self::insert_header(headers, http::header::RANGE, &range_header)
            })
            .await?;

        self.check_response(&response, location)?;
        let expected_length =
            self.validate_range_response_headers(&response, location, offset, length)?;

        let bytes = response.bytes().await.map_err(|e| FatalError::IoError {
            message: format!("Failed to read response body: {e}"),
        })?;

        if bytes.len() as u64 != expected_length {
            return Err(FatalError::IoError {
                message: format!(
                    "Range GET body length mismatch for s3://{}/{}: expected {} bytes, got {} (offset={}, length={})",
                    location.bucket(),
                    location.key(),
                    expected_length,
                    bytes.len(),
                    offset,
                    length
                ),
            });
        }

        Ok(bytes)
    }

    /// Fetch the first N bytes from the S3 object (for header scanning).
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to fetch from
    /// * `length` - Number of bytes to fetch
    ///
    /// # Returns
    ///
    /// The requested bytes as a `Bytes` object.
    pub async fn fetch_header(
        &self,
        location: &S3Location,
        length: usize,
    ) -> Result<Bytes, FatalError> {
        self.fetch_range(location, 0, length as u64).await
    }

    /// Fetch the last N bytes from the S3 object (for footer scanning).
    ///
    /// Uses HTTP Range requests to efficiently fetch the end of the file.
    /// This is used to find MCAP footers and summary offsets.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to fetch from
    /// * `length` - Number of bytes to fetch from the end
    /// * `file_size` - Total size of the file (for calculating offset)
    ///
    /// # Returns
    ///
    /// The requested bytes as a `Bytes` object.
    pub async fn fetch_tail(
        &self,
        location: &S3Location,
        length: u64,
        file_size: u64,
    ) -> Result<Bytes, FatalError> {
        let offset = file_size.saturating_sub(length);
        self.fetch_range(location, offset, length).await
    }

    /// Get the size of the S3 object.
    ///
    /// Uses a HEAD request to get the object metadata.
    pub async fn object_size(&self, location: &S3Location) -> Result<u64, FatalError> {
        let url = location.url();
        let response = self
            .build_signed_get_request(&url, &Method::HEAD, location, |_| Ok(()))
            .await?;

        self.check_response(&response, location)?;

        if !response.status().is_success() {
            return Err(FatalError::HttpError {
                status: Some(response.status().as_u16()),
                message: format!("HEAD request failed: HTTP {}", response.status().as_u16()),
            });
        }

        response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| FatalError::IoError {
                message: "Content-Length header not found".to_string(),
            })
    }

    /// Initialize a multipart upload to S3.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to upload to
    ///
    /// # Returns
    ///
    /// The upload ID that must be used for subsequent `upload_part` calls.
    pub async fn create_upload(&self, location: &S3Location) -> Result<String, FatalError> {
        let url = location.url();
        let response = self
            .build_signed_post_request(&url, location, |headers| {
                Self::insert_header(
                    headers,
                    http::header::HeaderName::from_static("x-amz-content-sha256"),
                    "UNSIGNED-PAYLOAD",
                )
            })
            .await?
            .query(&[("uploads", "")])
            .send()
            .await
            .map_err(|e| FatalError::HttpError {
                status: None,
                message: format!("Failed to create upload: {e}"),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!("Failed to create upload: HTTP {}", status.as_u16()),
            });
        }

        // Parse the UploadId from the XML response
        let body = response.text().await.map_err(|e| FatalError::IoError {
            message: format!("Failed to read response: {e}"),
        })?;

        // Extract UploadId from XML response
        // Format: <InitiateMultipartUploadResult...><UploadId>...</UploadId>...
        if let Some(start) = body.find("<UploadId>")
            && let Some(end) = body.find("</UploadId>")
        {
            return Ok(body[start + 10..end].to_string());
        }

        Err(FatalError::IoError {
            message: "Failed to parse UploadId from response".to_string(),
        })
    }

    /// Upload a part to a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to upload to
    /// * `upload_id` - The upload ID returned by `create_upload`
    /// * `part_number` - The part number (1-indexed)
    /// * `data` - The part data to upload
    ///
    /// # Returns
    ///
    /// The `ETag` of the uploaded part, needed for `complete_upload`.
    pub async fn upload_part(
        &self,
        location: &S3Location,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, FatalError> {
        let url = location.url();
        let content_hash = hex_sha256(&data);

        let response = self
            .build_signed_post_request(&url, location, |headers| {
                Self::insert_header(
                    headers,
                    http::header::HeaderName::from_static("x-amz-content-sha256"),
                    &content_hash,
                )
            })
            .await?
            .query(&[
                ("partNumber", part_number.to_string().as_str()),
                ("uploadId", upload_id),
            ])
            .body(data)
            .send()
            .await
            .map_err(|e| FatalError::HttpError {
                status: None,
                message: format!("Failed to upload part: {e}"),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!("Failed to upload part: HTTP {}", status.as_u16()),
            });
        }

        // Get ETag from response headers
        response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .ok_or_else(|| FatalError::IoError {
                message: "ETag header not found in upload response".to_string(),
            })
    }

    /// Complete a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location
    /// * `upload_id` - The upload ID returned by `create_upload`
    /// * `parts` - List of (`part_number`, etag) tuples for each uploaded part
    pub async fn complete_upload(
        &self,
        location: &S3Location,
        upload_id: &str,
        parts: Vec<(u32, String)>,
    ) -> Result<(), FatalError> {
        // Build the XML body for complete multipart upload
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (part_number, etag) in &parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{part_number}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");

        let body = Bytes::from(xml);
        let url = location.url();
        let content_hash = hex_sha256(&body);

        let response = self
            .build_signed_post_request(&url, location, |headers| {
                Self::insert_header(
                    headers,
                    http::header::HeaderName::from_static("x-amz-content-sha256"),
                    &content_hash,
                )
            })
            .await?
            .query(&[("uploadId", upload_id)])
            .body(body)
            .send()
            .await
            .map_err(|e| FatalError::HttpError {
                status: None,
                message: format!("Failed to complete upload: {e}"),
            })?;

        let status = response.status();
        if !status.is_success() {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!("Failed to complete upload: HTTP {}", status.as_u16()),
            });
        }

        Ok(())
    }

    /// Abort a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location
    /// * `upload_id` - The upload ID to abort
    pub async fn abort_upload(
        &self,
        location: &S3Location,
        upload_id: &str,
    ) -> Result<(), FatalError> {
        let url = location.url();
        let response = self
            .build_signed_delete_request(&url, location, |headers| {
                Self::insert_header(
                    headers,
                    http::header::HeaderName::from_static("x-amz-content-sha256"),
                    "UNSIGNED-PAYLOAD",
                )
            })
            .await?
            .query(&[("uploadId", upload_id)])
            .send()
            .await
            .map_err(|e| FatalError::HttpError {
                status: None,
                message: format!("Failed to abort upload: {e}"),
            })?;

        // Check for error status
        if !response.status().is_success() {
            return Err(FatalError::HttpError {
                status: Some(response.status().as_u16()),
                message: format!(
                    "Failed to abort upload: HTTP {}",
                    response.status().as_u16()
                ),
            });
        }

        Ok(())
    }

    /// Get a reference to the underlying HTTP client.
    #[must_use]
    pub fn http_client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Get the configuration.
    #[must_use]
    pub fn config(&self) -> &S3ReaderConfig {
        &self.config
    }

    // ========================================================================
    // Private Helper Methods
    // ========================================================================

    /// Build and send a signed GET/HEAD request to S3.
    async fn build_signed_get_request<F>(
        &self,
        url: &str,
        method: &'static Method,
        location: &S3Location,
        header_builder: F,
    ) -> Result<reqwest::Response, FatalError>
    where
        F: Fn(&mut HeaderMap) -> Result<(), FatalError>,
    {
        let uri = Uri::from_str(url).map_err(|e| FatalError::HttpError {
            status: None,
            message: format!("Invalid URL: {e}"),
        })?;

        let retry = self.config.retry().clone();
        let max_retries = retry.max_retries();

        for attempt in 0..=max_retries {
            let mut headers = HeaderMap::new();
            header_builder(&mut headers)?;

            // Sign the request if credentials are available
            if let Some(credentials) = self.config.credentials()
                && signer::should_sign(&credentials)
            {
                let region = location.region().unwrap_or(DEFAULT_AWS_REGION);
                signer::sign_request(&credentials, region, "s3", method, &uri, &mut headers)
                    .map_err(|e| FatalError::HttpError {
                        status: None,
                        message: format!("Failed to sign request: {e}"),
                    })?;
            }

            // Build the request with signed headers
            let request_builder = match *method {
                Method::GET => self.client.get(url),
                Method::HEAD => self.client.head(url),
                _ => {
                    return Err(FatalError::HttpError {
                        status: None,
                        message: format!("Unsupported HTTP method: {method:?}"),
                    });
                }
            };

            // Add headers (excluding 'host' which reqwest handles automatically)
            let mut request_builder = request_builder;
            for (name, value) in &headers {
                if let Ok(value_str) = value.to_str()
                    && name.as_str() != "host"
                {
                    request_builder = request_builder.header(name.as_str(), value_str);
                }
            }

            match request_builder.send().await {
                Ok(response) => {
                    if Self::is_retryable_status(response.status()) {
                        if attempt < max_retries {
                            sleep(retry.delay_for_attempt(attempt)).await;
                            continue;
                        }

                        let status = response.status().as_u16();
                        return Err(FatalError::HttpError {
                            status: Some(status),
                            message: format!(
                                "HTTP {} after {} attempts for {} {}",
                                status,
                                attempt + 1,
                                method,
                                url
                            ),
                        });
                    }

                    return Ok(response);
                }
                Err(err) => {
                    if Self::is_retryable_transport_error(&err) && attempt < max_retries {
                        sleep(retry.delay_for_attempt(attempt)).await;
                        continue;
                    }

                    return Err(Self::map_transport_error(err));
                }
            }
        }

        Err(FatalError::HttpError {
            status: None,
            message: format!("Failed to execute {} request for {}", method, url),
        })
    }

    /// Build a signed POST request (returns `RequestBuilder` for further customization).
    async fn build_signed_post_request<F>(
        &self,
        url: &str,
        location: &S3Location,
        header_builder: F,
    ) -> Result<reqwest::RequestBuilder, FatalError>
    where
        F: FnOnce(&mut HeaderMap) -> Result<(), FatalError>,
    {
        self.build_signed_request(url, &Method::POST, location, header_builder)
            .await
    }

    /// Build a signed DELETE request (returns `RequestBuilder` for further customization).
    async fn build_signed_delete_request<F>(
        &self,
        url: &str,
        location: &S3Location,
        header_builder: F,
    ) -> Result<reqwest::RequestBuilder, FatalError>
    where
        F: FnOnce(&mut HeaderMap) -> Result<(), FatalError>,
    {
        self.build_signed_request(url, &Method::DELETE, location, header_builder)
            .await
    }

    /// Build a signed request (returns `RequestBuilder` for further customization).
    async fn build_signed_request<F>(
        &self,
        url: &str,
        method: &'static Method,
        location: &S3Location,
        header_builder: F,
    ) -> Result<reqwest::RequestBuilder, FatalError>
    where
        F: FnOnce(&mut HeaderMap) -> Result<(), FatalError>,
    {
        let uri = Uri::from_str(url).map_err(|e| FatalError::HttpError {
            status: None,
            message: format!("Invalid URL: {e}"),
        })?;

        let mut headers = HeaderMap::new();
        header_builder(&mut headers)?;

        // Sign the request if credentials are available
        if let Some(credentials) = self.config.credentials()
            && signer::should_sign(&credentials)
        {
            let region = location.region().unwrap_or(DEFAULT_AWS_REGION);
            signer::sign_request(&credentials, region, "s3", method, &uri, &mut headers).map_err(
                |e| FatalError::HttpError {
                    status: None,
                    message: format!("Failed to sign request: {e}"),
                },
            )?;
        }

        // Build the request with signed headers
        let request_builder = match *method {
            Method::POST => self.client.post(url),
            Method::PUT => self.client.put(url),
            Method::DELETE => self.client.delete(url),
            _ => {
                return Err(FatalError::HttpError {
                    status: None,
                    message: format!("Unsupported HTTP method: {method:?}"),
                });
            }
        };

        // Add headers (excluding 'host' which reqwest handles automatically)
        let mut result_builder = request_builder;
        for (name, value) in &headers {
            if let Ok(value_str) = value.to_str()
                && name.as_str() != "host"
            {
                result_builder = result_builder.header(name.as_str(), value_str);
            }
        }

        Ok(result_builder)
    }

    /// Check response for S3-specific error codes.
    fn check_response(
        &self,
        response: &reqwest::Response,
        location: &S3Location,
    ) -> Result<(), FatalError> {
        let status = response.status();

        if status == 404 {
            return Err(FatalError::ObjectNotFound {
                bucket: location.bucket().to_string(),
                key: location.key().to_string(),
            });
        }

        if status == 403 {
            return Err(FatalError::AccessDenied {
                bucket: location.bucket().to_string(),
                key: location.key().to_string(),
                details: "Check credentials and bucket permissions".to_string(),
            });
        }

        Ok(())
    }

    /// Validate status and headers for a range GET response.
    fn validate_range_response_headers(
        &self,
        response: &reqwest::Response,
        location: &S3Location,
        offset: u64,
        length: u64,
    ) -> Result<u64, FatalError> {
        let status = response.status();
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Range GET must return 206 Partial Content for s3://{}/{} (offset={}, length={}), got HTTP {}",
                    location.bucket(),
                    location.key(),
                    offset,
                    length,
                    status.as_u16()
                ),
            });
        }

        let content_range = response
            .headers()
            .get(http::header::CONTENT_RANGE)
            .ok_or_else(|| FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Missing Content-Range header in 206 response for s3://{}/{} (offset={}, length={})",
                    location.bucket(),
                    location.key(),
                    offset,
                    length
                ),
            })?
            .to_str()
            .map_err(|e| FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Invalid Content-Range header for s3://{}/{}: {}",
                    location.bucket(),
                    location.key(),
                    e
                ),
            })?;

        let (range_start, range_end, total_size) = Self::parse_content_range(content_range)?;
        if range_start != offset {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Unexpected Content-Range start for s3://{}/{}: expected {}, got {}",
                    location.bucket(),
                    location.key(),
                    offset,
                    range_start
                ),
            });
        }

        let expected_length = range_end
            .checked_sub(range_start)
            .and_then(|v| v.checked_add(1))
            .ok_or_else(|| FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Invalid Content-Range span for s3://{}/{}: {}",
                    location.bucket(),
                    location.key(),
                    content_range
                ),
            })?;

        if expected_length != length {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Unexpected Content-Range length for s3://{}/{}: expected {}, got {} ({})",
                    location.bucket(),
                    location.key(),
                    length,
                    expected_length,
                    content_range
                ),
            });
        }

        if let Some(total_size) = total_size
            && range_end >= total_size
        {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!(
                    "Invalid Content-Range total for s3://{}/{}: {}",
                    location.bucket(),
                    location.key(),
                    content_range
                ),
            });
        }

        if let Some(content_length) = response.headers().get(http::header::CONTENT_LENGTH) {
            let content_length = content_length
                .to_str()
                .map_err(|e| FatalError::HttpError {
                    status: Some(status.as_u16()),
                    message: format!(
                        "Invalid Content-Length header for s3://{}/{}: {}",
                        location.bucket(),
                        location.key(),
                        e
                    ),
                })?
                .parse::<u64>()
                .map_err(|e| FatalError::HttpError {
                    status: Some(status.as_u16()),
                    message: format!(
                        "Non-numeric Content-Length header for s3://{}/{}: {}",
                        location.bucket(),
                        location.key(),
                        e
                    ),
                })?;

            if content_length != expected_length {
                return Err(FatalError::HttpError {
                    status: Some(status.as_u16()),
                    message: format!(
                        "Content-Length mismatch for s3://{}/{}: expected {}, got {}",
                        location.bucket(),
                        location.key(),
                        expected_length,
                        content_length
                    ),
                });
            }
        }

        Ok(expected_length)
    }

    fn parse_content_range(value: &str) -> Result<(u64, u64, Option<u64>), FatalError> {
        let value = value.trim();
        let bytes_prefix = "bytes ";
        let rest = value
            .strip_prefix(bytes_prefix)
            .ok_or_else(|| FatalError::HttpError {
                status: Some(206),
                message: format!("Invalid Content-Range format: {value}"),
            })?;

        let (range, total) = rest.split_once('/').ok_or_else(|| FatalError::HttpError {
            status: Some(206),
            message: format!("Invalid Content-Range format: {value}"),
        })?;

        let (start, end) = range.split_once('-').ok_or_else(|| FatalError::HttpError {
            status: Some(206),
            message: format!("Invalid Content-Range range: {value}"),
        })?;

        let start = start.parse::<u64>().map_err(|e| FatalError::HttpError {
            status: Some(206),
            message: format!("Invalid Content-Range start in '{value}': {e}"),
        })?;

        let end = end.parse::<u64>().map_err(|e| FatalError::HttpError {
            status: Some(206),
            message: format!("Invalid Content-Range end in '{value}': {e}"),
        })?;

        if end < start {
            return Err(FatalError::HttpError {
                status: Some(206),
                message: format!("Invalid Content-Range order: {value}"),
            });
        }

        let total = if total == "*" {
            None
        } else {
            Some(total.parse::<u64>().map_err(|e| FatalError::HttpError {
                status: Some(206),
                message: format!("Invalid Content-Range total in '{value}': {e}"),
            })?)
        };

        Ok((start, end, total))
    }

    fn is_retryable_status(status: reqwest::StatusCode) -> bool {
        matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
    }

    fn is_retryable_transport_error(err: &reqwest::Error) -> bool {
        err.is_connect() || err.is_timeout() || Self::is_transient_error_message(&err.to_string())
    }

    fn is_transient_error_message(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        [
            "connection reset",
            "connection closed",
            "broken pipe",
            "timed out",
            "timeout",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    fn map_transport_error(err: reqwest::Error) -> FatalError {
        if err.is_connect()
            || err.is_timeout()
            || Self::is_transient_error_message(&err.to_string())
        {
            FatalError::HttpError {
                status: None,
                message: format!("Transient transport failure: {err}"),
            }
        } else {
            FatalError::HttpError {
                status: None,
                message: err.to_string(),
            }
        }
    }

    /// Helper to insert a header into a `HeaderMap` with proper error handling.
    fn insert_header(
        headers: &mut HeaderMap,
        name: http::header::HeaderName,
        value: &str,
    ) -> Result<(), FatalError> {
        let header_value = HeaderValue::from_str(value).map_err(|e| FatalError::HttpError {
            status: None,
            message: format!("Invalid {name:?} header value: {e}"),
        })?;
        headers.insert(name, header_value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::s3::AwsCredentials;

    #[test]
    fn test_s3_client_new_default() {
        let client = S3Client::default_client();
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.config().buffer_size(), 64 * 1024);
    }

    #[test]
    fn test_s3_client_new_with_config() {
        let config = S3ReaderConfig::default().with_buffer_size(128 * 1024);
        let client = S3Client::new(config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.config().buffer_size(), 128 * 1024);
    }

    #[test]
    fn test_s3_client_new_invalid_config() {
        let config = S3ReaderConfig::default().with_buffer_size(0);
        let client = S3Client::new(config);
        assert!(client.is_err());
    }

    #[test]
    fn test_s3_client_getters() {
        let client = S3Client::default_client().unwrap();
        assert_eq!(client.config().buffer_size(), 64 * 1024);
        assert!(client.http_client() as *const _ as usize != 0);
    }

    #[test]
    fn test_insert_header_valid() {
        let mut headers = HeaderMap::new();
        assert!(S3Client::insert_header(&mut headers, http::header::RANGE, "bytes=0-100").is_ok());
        assert!(headers.get("Range").is_some());
    }

    #[test]
    fn test_insert_header_invalid_value() {
        let mut headers = HeaderMap::new();
        // Invalid header value (contains null byte)
        let result = S3Client::insert_header(&mut headers, http::header::RANGE, "bytes=\0-0");
        assert!(result.is_err());
    }

    #[test]
    fn test_s3_client_with_credentials() {
        let creds = AwsCredentials::new("test_key", "test_secret").unwrap();
        let config = S3ReaderConfig::default().with_credentials(Some(creds));
        let client = S3Client::new(config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert!(client.config().credentials().is_some());
    }

    #[test]
    fn test_s3_client_with_invalid_ssl_disabled() {
        let config = S3ReaderConfig::default().with_validate_ssl(false);
        let client = S3Client::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_s3_client_with_custom_timeout() {
        let config =
            S3ReaderConfig::default().with_request_timeout(std::time::Duration::from_secs(60));
        let client = S3Client::new(config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(
            client.config().request_timeout(),
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn test_s3_client_with_pool_config() {
        let config = S3ReaderConfig::default().with_pool_max_idle(10);
        let client = S3Client::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_s3_client_invalid_timeout() {
        let config =
            S3ReaderConfig::default().with_request_timeout(std::time::Duration::from_secs(0));
        assert!(S3Client::new(config).is_err());
    }

    #[test]
    fn test_parse_content_range_valid() {
        let (start, end, total) = S3Client::parse_content_range("bytes 100-199/1000").unwrap();
        assert_eq!(start, 100);
        assert_eq!(end, 199);
        assert_eq!(total, Some(1000));
    }

    #[test]
    fn test_parse_content_range_invalid_prefix() {
        let result = S3Client::parse_content_range("items 0-9/10");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_content_range_invalid_order() {
        let result = S3Client::parse_content_range("bytes 20-10/100");
        assert!(result.is_err());
    }

    #[test]
    fn test_retryable_status_classification() {
        assert!(S3Client::is_retryable_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(S3Client::is_retryable_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(!S3Client::is_retryable_status(
            reqwest::StatusCode::FORBIDDEN
        ));
        assert!(!S3Client::is_retryable_status(
            reqwest::StatusCode::NOT_FOUND
        ));
    }

    #[test]
    fn test_transient_error_message_classification() {
        assert!(S3Client::is_transient_error_message(
            "connection reset by peer while sending request"
        ));
        assert!(S3Client::is_transient_error_message("request timeout"));
        assert!(!S3Client::is_transient_error_message(
            "invalid header value"
        ));
    }

    // =========================================================================
    // insert_header edge case tests
    // =========================================================================

    #[test]
    fn test_insert_header_empty_value() {
        let mut headers = HeaderMap::new();
        // Empty header value should be valid
        assert!(S3Client::insert_header(&mut headers, http::header::RANGE, "").is_ok());
        assert!(headers.get("Range").is_some());
    }

    #[test]
    fn test_insert_header_unicode_value() {
        let mut headers = HeaderMap::new();
        // Unicode value should be valid
        assert!(S3Client::insert_header(&mut headers, http::header::USER_AGENT, "测试").is_ok());
    }

    #[test]
    fn test_insert_header_multiple_values() {
        let mut headers = HeaderMap::new();
        assert!(
            S3Client::insert_header(
                &mut headers,
                http::header::HeaderName::from_static("x-amz-meta-test"),
                "value1"
            )
            .is_ok()
        );
        // Second insert should overwrite
        assert!(
            S3Client::insert_header(
                &mut headers,
                http::header::HeaderName::from_static("x-amz-meta-test"),
                "value2"
            )
            .is_ok()
        );
        assert_eq!(
            headers.get("x-amz-meta-test").unwrap().to_str().unwrap(),
            "value2"
        );
    }

    // =========================================================================
    // S3Client creation edge cases
    // =========================================================================

    #[test]
    fn test_s3_client_with_credentials_and_retry() {
        let creds = AwsCredentials::new("key", "secret").unwrap();
        let retry = crate::io::s3::RetryConfig::default().with_max_retries(3);
        let config = S3ReaderConfig::default()
            .with_credentials(Some(creds))
            .with_retry(retry);
        let client = S3Client::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_s3_client_clone() {
        let client = S3Client::default_client().unwrap();
        let cloned = client.clone();
        assert_eq!(client.config().buffer_size(), cloned.config().buffer_size());
    }
}
