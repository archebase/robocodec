// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! HTTP client for S3 streaming operations.

use crate::io::s3::{config::S3ReaderConfig, error::FatalError, location::S3Location};
use bytes::Bytes;

// Note: We use a simplified credential approach that doesn't require full AWS SDK
// The proper AWS SigV4 signing will be added in Phase 4 with the `s3` feature

/// HTTP client for S3 operations.
///
/// Wraps a reqwest::Client with S3-specific configuration for
/// streaming operations with HTTP Range requests.
pub struct S3Client {
    /// The underlying HTTP client
    client: reqwest::Client,
    /// Configuration for the S3 operations
    config: S3ReaderConfig,
}

impl S3Client {
    /// Create a new S3 client with the given configuration.
    pub fn new(config: S3ReaderConfig) -> Result<Self, FatalError> {
        config.validate().map_err(|e| FatalError::ConfigError {
            message: e.to_string(),
        })?;

        let client_builder = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(config.pool_max_idle);

        let client = if !config.validate_ssl {
            client_builder.danger_accept_invalid_certs(true)
        } else {
            client_builder
        };

        let client = client.build().map_err(|e| FatalError::ConfigError {
            message: format!("Failed to create HTTP client: {}", e),
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
        let url = location.url();
        let range_header = format!("bytes={}-{}", offset, offset + length.saturating_sub(1));

        let request = self.client.get(&url).header("Range", range_header);

        // Add AWS credentials if configured
        // Note: For now we use a simplified approach. In Phase 4 with the `s3` feature,
        // we'll add proper AWS SigV4 signing using aws-config
        let _credentials = &self.config.credentials;
        // TODO: Add proper AWS SigV4 signing in Phase 4
        // For now, we rely on the default credential chain from the environment

        let response = request.send().await.map_err(|e| {
            if e.is_connect() || e.is_timeout() {
                FatalError::HttpError {
                    status: None,
                    message: format!("Connection failed: {}", e),
                }
            } else {
                FatalError::HttpError {
                    status: None,
                    message: e.to_string(),
                }
            }
        })?;

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

        if !status.is_success() && status.as_u16() != 206 {
            // 206 is Partial Content (successful range request)
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!("HTTP {}", status.as_u16()),
            });
        }

        response.bytes().await.map_err(|e| FatalError::IoError {
            message: format!("Failed to read response body: {}", e),
        })
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

    /// Get the size of the S3 object.
    ///
    /// Uses a HEAD request to get the object metadata.
    pub async fn object_size(&self, location: &S3Location) -> Result<u64, FatalError> {
        let url = location.url();

        let response = self.client.head(&url).send().await.map_err(|e| {
            if e.is_connect() || e.is_timeout() {
                FatalError::HttpError {
                    status: None,
                    message: format!("Connection failed: {}", e),
                }
            } else {
                FatalError::HttpError {
                    status: None,
                    message: e.to_string(),
                }
            }
        })?;

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

        if !status.is_success() {
            return Err(FatalError::HttpError {
                status: Some(status.as_u16()),
                message: format!("HEAD request failed: HTTP {}", status.as_u16()),
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

    /// Get a reference to the underlying HTTP client.
    pub fn http_client(&self) -> &reqwest::Client {
        &self.client
    }

    /// Get the configuration.
    pub fn config(&self) -> &S3ReaderConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_client_new_default() {
        let client = S3Client::default_client();
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.config.buffer_size, 64 * 1024);
    }

    #[test]
    fn test_s3_client_new_with_config() {
        let config = S3ReaderConfig::default().with_buffer_size(128 * 1024);
        let client = S3Client::new(config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.config.buffer_size, 128 * 1024);
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
        assert_eq!(client.config().buffer_size, 64 * 1024);
        assert!(client.http_client() as *const _ as usize != 0);
    }
}
