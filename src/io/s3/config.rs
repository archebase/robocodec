// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Configuration for S3 streaming operations.

use std::fmt;
use std::time::Duration;

/// AWS credentials for S3 access.
#[derive(Clone, PartialEq, Eq)]
pub struct AwsCredentials {
    /// AWS access key ID
    pub(crate) access_key_id: String,
    /// AWS secret access key
    pub(crate) secret_access_key: String,
    /// AWS session token (for temporary credentials)
    pub(crate) session_token: Option<String>,
}

// Custom Debug that redacts sensitive information
impl fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl AwsCredentials {
    /// Create new AWS credentials.
    ///
    /// Returns `None` if either `access_key_id` or `secret_access_key` is empty.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Option<Self> {
        let access_key_id = access_key_id.into();
        let secret_access_key = secret_access_key.into();

        if access_key_id.is_empty() || secret_access_key.is_empty() {
            return None;
        }

        Some(Self {
            access_key_id,
            secret_access_key,
            session_token: None,
        })
    }

    /// Get the access key ID.
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Get the secret access key.
    #[must_use]
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// Get the session token if present.
    #[must_use]
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// Set the session token for temporary credentials.
    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    /// Create credentials from environment variables.
    ///
    /// Reads from:
    /// - `AWS_ACCESS_KEY_ID` or `AWS_ACCESS_KEY`
    /// - `AWS_SECRET_ACCESS_KEY` or `AWS_SECRET_KEY`
    /// - `AWS_SESSION_TOKEN` (optional)
    ///
    /// Returns `None` if the required environment variables are not set.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("AWS_ACCESS_KEY"))
            .ok()?;

        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("AWS_SECRET_KEY"))
            .ok()?;

        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

        // Use new() to validate non-empty
        Self::new(access_key_id, secret_access_key).map(|mut creds| {
            creds.session_token = session_token;
            creds
        })
    }
}

/// Retry configuration for S3 requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub(crate) max_retries: usize,
    /// Initial delay before first retry
    pub(crate) initial_delay: Duration,
    /// Maximum delay between retries
    pub(crate) max_delay: Duration,
    /// Whether to use exponential backoff
    pub(crate) exponential_backoff: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            exponential_backoff: true,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the maximum number of retries.
    #[must_use]
    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    /// Get the initial delay.
    #[must_use]
    pub fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    /// Get the maximum delay.
    #[must_use]
    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Check if exponential backoff is enabled.
    #[must_use]
    pub fn exponential_backoff(&self) -> bool {
        self.exponential_backoff
    }

    /// Set the maximum number of retries.
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial delay.
    #[must_use]
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set the maximum delay.
    #[must_use]
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set whether to use exponential backoff.
    #[must_use]
    pub fn with_exponential_backoff(mut self, enabled: bool) -> Self {
        self.exponential_backoff = enabled;
        self
    }

    /// Calculate delay for a given retry attempt.
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        if !self.exponential_backoff {
            return self.initial_delay;
        }

        let delay_ms = self.initial_delay.as_millis() as u64 * 2_u64.pow(attempt as u32);
        let delay = Duration::from_millis(delay_ms);
        delay.min(self.max_delay)
    }
}

/// Configuration for S3 streaming reader.
#[derive(Debug, Clone)]
pub struct S3ReaderConfig {
    /// Buffer size for S3 reads (default: 64KB)
    pub(crate) buffer_size: usize,

    /// Maximum in-memory chunk size (default: 10MB)
    pub(crate) max_chunk_size: usize,

    /// Number of bytes to scan for header (default: 1MB)
    pub(crate) header_scan_limit: usize,

    /// AWS credentials (None = lazy load from env at request time)
    pub(crate) credentials: Option<AwsCredentials>,

    /// Whether to use lazy credential loading from env
    pub(crate) lazy_credentials: bool,

    /// Retry configuration
    pub(crate) retry: RetryConfig,

    /// Request timeout (default: 30 seconds)
    pub(crate) request_timeout: Duration,

    /// Connection pool size (default: 10)
    pub(crate) pool_max_idle: usize,

    /// Whether to validate SSL certificates (default: true)
    pub(crate) validate_ssl: bool,
}

impl Default for S3ReaderConfig {
    fn default() -> Self {
        Self {
            buffer_size: 64 * 1024,           // 64KB
            max_chunk_size: 10 * 1024 * 1024, // 10MB
            header_scan_limit: 1024 * 1024,   // 1MB
            credentials: None,
            lazy_credentials: true, // Lazy load from env by default
            retry: RetryConfig::default(),
            request_timeout: Duration::from_secs(30),
            pool_max_idle: 10,
            validate_ssl: true,
        }
    }
}

impl S3ReaderConfig {
    /// Create a new configuration with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the buffer size for S3 reads.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the maximum in-memory chunk size.
    #[must_use]
    pub fn max_chunk_size(&self) -> usize {
        self.max_chunk_size
    }

    /// Get the header scan limit.
    #[must_use]
    pub fn header_scan_limit(&self) -> usize {
        self.header_scan_limit
    }

    /// Get the AWS credentials.
    ///
    /// If lazy credential loading is enabled and no explicit credentials were set,
    /// this will attempt to load from environment variables at access time.
    #[must_use]
    pub fn credentials(&self) -> Option<AwsCredentials> {
        if let Some(ref creds) = self.credentials {
            return Some(creds.clone());
        }

        if self.lazy_credentials {
            AwsCredentials::from_env()
        } else {
            None
        }
    }

    /// Check if lazy credential loading is enabled.
    #[must_use]
    pub fn lazy_credentials(&self) -> bool {
        self.lazy_credentials
    }

    /// Get the retry configuration.
    #[must_use]
    pub fn retry(&self) -> &RetryConfig {
        &self.retry
    }

    /// Get the request timeout.
    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Get the connection pool max idle connections.
    #[must_use]
    pub fn pool_max_idle(&self) -> usize {
        self.pool_max_idle
    }

    /// Get whether to validate SSL certificates.
    #[must_use]
    pub fn validate_ssl(&self) -> bool {
        self.validate_ssl
    }

    /// Set the buffer size for S3 reads.
    ///
    /// Invalid values will be caught by [`validate()`](Self::validate).
    #[must_use]
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the maximum in-memory chunk size.
    ///
    /// Invalid values will be caught by [`validate()`](Self::validate).
    #[must_use]
    pub fn with_max_chunk_size(mut self, size: usize) -> Self {
        self.max_chunk_size = size;
        self
    }

    /// Set the header scan limit.
    ///
    /// Invalid values will be caught by [`validate()`](Self::validate).
    #[must_use]
    pub fn with_header_scan_limit(mut self, limit: usize) -> Self {
        self.header_scan_limit = limit;
        self
    }

    /// Set the AWS credentials.
    ///
    /// Accepts `None` to use lazy credential loading from environment variables.
    /// Invalid credentials (empty access key or secret) will be ignored.
    /// Calling this method disables lazy loading (even if credentials are filtered out).
    #[must_use]
    pub fn with_credentials(mut self, credentials: Option<AwsCredentials>) -> Self {
        self.credentials = credentials
            .filter(|c| !c.access_key_id().is_empty() && !c.secret_access_key().is_empty());
        // Disable lazy loading when explicit credentials are set (even if filtered out)
        self.lazy_credentials = false;
        self
    }

    /// Set whether to use lazy credential loading from environment variables.
    ///
    /// When enabled (default), credentials are read from environment variables
    /// at request time, allowing credentials to be set after the client is created.
    #[must_use]
    pub fn with_lazy_credentials(mut self, lazy: bool) -> Self {
        self.lazy_credentials = lazy;
        self
    }

    /// Set the retry configuration.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Set the request timeout.
    ///
    /// Invalid values will be caught by [`validate()`](Self::validate).
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the connection pool max idle connections.
    ///
    /// Invalid values will be caught by [`validate()`](Self::validate).
    #[must_use]
    pub fn with_pool_max_idle(mut self, max_idle: usize) -> Self {
        self.pool_max_idle = max_idle;
        self
    }

    /// Set whether to validate SSL certificates.
    #[must_use]
    pub fn with_validate_ssl(mut self, validate: bool) -> Self {
        self.validate_ssl = validate;
        self
    }

    /// Validate the configuration.
    ///
    /// Returns an error if any configuration values are invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.buffer_size == 0 {
            return Err(ConfigError::InvalidValue(
                "buffer_size must be greater than 0".to_string(),
            ));
        }

        if self.max_chunk_size < self.buffer_size {
            return Err(ConfigError::InvalidValue(format!(
                "max_chunk_size ({}) must be at least buffer_size ({})",
                self.max_chunk_size, self.buffer_size
            )));
        }

        if self.header_scan_limit < 1024 {
            return Err(ConfigError::InvalidValue(
                "header_scan_limit must be at least 1024 bytes".to_string(),
            ));
        }

        if self.request_timeout.as_secs() == 0 {
            return Err(ConfigError::InvalidValue(
                "request_timeout must be greater than 0".to_string(),
            ));
        }

        if self.pool_max_idle == 0 {
            return Err(ConfigError::InvalidValue(
                "pool_max_idle must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

/// Configuration validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Invalid configuration value
    InvalidValue(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidValue(msg) => write!(f, "Invalid configuration: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aws_credentials_new() {
        let creds = AwsCredentials::new("key_id", "secret_key").unwrap();
        assert_eq!(creds.access_key_id(), "key_id");
        assert_eq!(creds.secret_access_key(), "secret_key");
        assert!(creds.session_token().is_none());
    }

    #[test]
    fn test_aws_credentials_new_empty() {
        // Empty credentials should return None
        assert!(AwsCredentials::new("", "secret_key").is_none());
        assert!(AwsCredentials::new("key_id", "").is_none());
    }

    #[test]
    fn test_aws_credentials_with_session_token() {
        let creds = AwsCredentials::new("key_id", "secret_key")
            .unwrap()
            .with_session_token("session_token");
        assert_eq!(creds.session_token(), Some("session_token"));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.initial_delay(), Duration::from_millis(100));
        assert_eq!(config.max_delay(), Duration::from_secs(10));
        assert!(config.exponential_backoff());
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::default()
            .with_max_retries(5)
            .with_initial_delay(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(30))
            .with_exponential_backoff(false);

        assert_eq!(config.max_retries(), 5);
        assert_eq!(config.initial_delay(), Duration::from_millis(200));
        assert_eq!(config.max_delay(), Duration::from_secs(30));
        assert!(!config.exponential_backoff());
    }

    #[test]
    fn test_retry_delay_for_attempt() {
        let config = RetryConfig::default();

        // First retry
        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        // Second retry (2x)
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        // Third retry (4x)
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        // Fourth retry (8x)
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));
        // Should cap at max_delay
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(10));
    }

    #[test]
    fn test_retry_delay_linear() {
        let config = RetryConfig::default().with_exponential_backoff(false);

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(10), Duration::from_millis(100));
    }

    #[test]
    fn test_s3_reader_config_default() {
        let config = S3ReaderConfig::default();
        assert_eq!(config.buffer_size(), 64 * 1024);
        assert_eq!(config.max_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(config.header_scan_limit(), 1024 * 1024);
        assert_eq!(config.request_timeout(), Duration::from_secs(30));
        assert_eq!(config.pool_max_idle(), 10);
        assert!(config.validate_ssl());
    }

    #[test]
    fn test_s3_reader_config_builder() {
        let config = S3ReaderConfig::default()
            .with_buffer_size(128 * 1024)
            .with_max_chunk_size(20 * 1024 * 1024)
            .with_header_scan_limit(2 * 1024 * 1024)
            .with_request_timeout(Duration::from_secs(60))
            .with_pool_max_idle(20)
            .with_validate_ssl(false);

        assert_eq!(config.buffer_size(), 128 * 1024);
        assert_eq!(config.max_chunk_size(), 20 * 1024 * 1024);
        assert_eq!(config.header_scan_limit(), 2 * 1024 * 1024);
        assert_eq!(config.request_timeout(), Duration::from_secs(60));
        assert_eq!(config.pool_max_idle(), 20);
        assert!(!config.validate_ssl());
    }

    #[test]
    fn test_s3_reader_config_with_credentials() {
        let creds = AwsCredentials::new("key", "secret").unwrap();
        let config = S3ReaderConfig::default().with_credentials(Some(creds.clone()));
        assert!(config.credentials().is_some());
        assert_eq!(config.credentials().unwrap().access_key_id(), "key");
    }

    #[test]
    fn test_s3_reader_config_with_invalid_credentials() {
        // Invalid credentials (empty) should be filtered out
        let creds = AwsCredentials::new("", "").unwrap_or_else(|| AwsCredentials {
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
        });
        let config = S3ReaderConfig::default().with_credentials(Some(creds));
        assert!(config.credentials().is_none());
    }

    #[test]
    fn test_s3_reader_config_validate_success() {
        let config = S3ReaderConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_s3_reader_config_validate_buffer_size_zero() {
        let config = S3ReaderConfig::default().with_buffer_size(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_reader_config_validate_max_chunk_smaller_than_buffer() {
        let config = S3ReaderConfig::default()
            .with_buffer_size(1024 * 1024)
            .with_max_chunk_size(512 * 1024);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_reader_config_validate_header_scan_too_small() {
        let config = S3ReaderConfig::default().with_header_scan_limit(512);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_reader_config_validate_timeout_zero() {
        let config = S3ReaderConfig::default().with_request_timeout(Duration::ZERO);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_reader_config_validate_pool_max_idle_zero() {
        let config = S3ReaderConfig::default().with_pool_max_idle(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_reader_config_with_retry() {
        let retry = RetryConfig::default().with_max_retries(5);
        let config = S3ReaderConfig::default().with_retry(retry);
        assert_eq!(config.retry().max_retries(), 5);
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::InvalidValue("test error".to_string());
        assert_eq!(format!("{}", err), "Invalid configuration: test error");
    }
}
