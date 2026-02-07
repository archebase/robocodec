// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Builder pattern for creating unified writers.

use std::path::PathBuf;

use crate::{CodecError, Result};

/// HTTP authentication configuration for writer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpAuthConfig {
    /// Bearer token (OAuth2/JWT)
    pub bearer_token: Option<String>,
    /// Basic auth username
    pub basic_username: Option<String>,
    /// Basic auth password
    pub basic_password: Option<String>,
}

impl HttpAuthConfig {
    /// Create bearer token authentication.
    ///
    /// # Arguments
    ///
    /// * `token` - Bearer token (e.g., JWT or OAuth2 access token)
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::HttpAuthConfig;
    ///
    /// let config = HttpAuthConfig::bearer("your-token-here");
    /// assert!(config.bearer_token().is_some());
    /// ```
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            bearer_token: Some(token.into()),
            basic_username: None,
            basic_password: None,
        }
    }

    /// Create basic authentication.
    ///
    /// # Arguments
    ///
    /// * `username` - HTTP username
    /// * `password` - HTTP password
    ///
    /// # Example
    ///
    /// ```rust
    /// use robocodec::HttpAuthConfig;
    ///
    /// let config = HttpAuthConfig::basic("user", "pass");
    /// assert!(config.basic_username().is_some());
    /// assert_eq!(config.basic_username(), Some("user"));
    /// ```
    #[must_use]
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            bearer_token: None,
            basic_username: Some(username.into()),
            basic_password: Some(password.into()),
        }
    }

    /// Check if this configuration has any authentication set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bearer_token.is_none() && self.basic_username.is_none()
    }

    /// Get the bearer token if configured.
    #[must_use]
    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    /// Get the basic auth username if configured.
    #[must_use]
    pub fn basic_username(&self) -> Option<&str> {
        self.basic_username.as_deref()
    }

    /// Get the basic auth password if configured.
    #[must_use]
    pub fn basic_password(&self) -> Option<&str> {
        self.basic_password.as_deref()
    }
}

/// Writing strategy selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteStrategy {
    /// Auto-detect optimal strategy
    #[default]
    Auto,
    /// Sequential writing
    Sequential,
    /// Parallel writing
    Parallel,
}

impl WriteStrategy {
    fn resolve(self) -> Self {
        match self {
            WriteStrategy::Auto => WriteStrategy::Sequential,
            other => other,
        }
    }
}

/// Configuration for creating a writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Path to the output file
    pub path: PathBuf,
    /// Writing strategy to use
    pub strategy: WriteStrategy,
    /// Compression level (1-22 for ZSTD)
    pub compression_level: Option<i32>,
    /// Chunk size in bytes
    pub chunk_size: Option<usize>,
    /// Number of threads for parallel compression
    pub num_threads: Option<usize>,
    /// HTTP authentication configuration
    pub http_auth: HttpAuthConfig,
    /// HTTP upload chunk size in bytes (default: 5MB)
    pub http_upload_chunk_size: usize,
    /// HTTP max retries for failed uploads (default: 3)
    pub http_max_retries: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            strategy: WriteStrategy::Auto,
            compression_level: None,
            chunk_size: None,
            num_threads: None,
            http_auth: HttpAuthConfig::default(),
            http_upload_chunk_size: 5 * 1024 * 1024, // 5MB
            http_max_retries: 3,
        }
    }
}

impl WriterConfig {
    /// Create a new builder for WriterConfig.
    #[must_use]
    pub fn builder() -> WriterConfigBuilder {
        WriterConfigBuilder::new()
    }
}

/// Builder for `WriterConfig`.
///
/// Provides a fluent API for creating writer configurations.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::WriterConfig;
///
/// let config = WriterConfig::builder()
///     .compression_level(3)
///     .chunk_size(1024 * 1024)
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct WriterConfigBuilder {
    config: WriterConfig,
}

impl WriterConfigBuilder {
    /// Create a new builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the compression level.
    #[must_use]
    pub fn compression_level(mut self, level: i32) -> Self {
        self.config.compression_level = Some(level);
        self
    }

    /// Set the chunk size in bytes.
    #[must_use]
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.config.chunk_size = Some(size);
        self
    }

    /// Set the number of threads.
    #[must_use]
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Set HTTP bearer token authentication.
    ///
    /// # Arguments
    ///
    /// * `token` - Bearer token (e.g., JWT or OAuth2 access token)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use robocodec::io::WriterConfig;
    /// let config = WriterConfig::builder()
    ///     .http_bearer_token("your-token-here")
    ///     .build();
    /// ```
    #[must_use]
    pub fn http_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.config.http_auth = HttpAuthConfig::bearer(token);
        self
    }

    /// Set HTTP basic authentication.
    ///
    /// # Arguments
    ///
    /// * `username` - HTTP username
    /// * `password` - HTTP password
    ///
    /// # Example
    ///
    /// ```rust
    /// # use robocodec::io::WriterConfig;
    /// let config = WriterConfig::builder()
    ///     .http_basic_auth("user", "pass")
    ///     .build();
    /// ```
    #[must_use]
    pub fn http_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.http_auth = HttpAuthConfig::basic(username, password);
        self
    }

    /// Set HTTP upload chunk size in bytes.
    ///
    /// # Arguments
    ///
    /// * `size` - Chunk size for HTTP upload (minimum 1MB for ChunkedPut)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use robocodec::io::WriterConfig;
    /// let config = WriterConfig::builder()
    ///     .http_upload_chunk_size(10 * 1024 * 1024) // 10MB
    ///     .build();
    /// ```
    #[must_use]
    pub fn http_upload_chunk_size(mut self, size: usize) -> Self {
        self.config.http_upload_chunk_size = size;
        self
    }

    /// Set HTTP max retries for failed uploads.
    ///
    /// # Arguments
    ///
    /// * `retries` - Maximum number of retry attempts
    ///
    /// # Example
    ///
    /// ```rust
    /// # use robocodec::io::WriterConfig;
    /// let config = WriterConfig::builder()
    ///     .http_max_retries(5)
    ///     .build();
    /// ```
    #[must_use]
    pub fn http_max_retries(mut self, retries: usize) -> Self {
        self.config.http_max_retries = retries;
        self
    }

    /// Build the configuration.
    #[must_use]
    pub fn build(self) -> WriterConfig {
        self.config
    }
}

/// Builder for creating unified writers.
#[derive(Debug, Clone, Default)]
pub struct WriterBuilder {
    config: WriterConfig,
}

impl WriterBuilder {
    /// Create a new builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path to the output file.
    #[must_use]
    pub fn path<P: AsRef<std::path::Path>>(mut self, path: P) -> Self {
        self.config.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the writing strategy.
    #[must_use]
    pub fn strategy(mut self, strategy: WriteStrategy) -> Self {
        self.config.strategy = strategy;
        self
    }

    /// Set the compression level (1-22 for ZSTD).
    #[must_use]
    pub fn compression_level(mut self, level: i32) -> Self {
        self.config.compression_level = Some(level);
        self
    }

    /// Set the chunk size in bytes.
    #[must_use]
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.config.chunk_size = Some(size);
        self
    }

    /// Set the number of threads for parallel compression.
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Build the writer.
    pub fn build(self) -> Result<super::RoboWriter> {
        let path = self.config.path.clone();

        if path.as_os_str().is_empty() {
            return Err(CodecError::parse("WriterBuilder", "Path is not set"));
        }

        // Validate parent directory exists
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            match parent.try_exists() {
                Ok(false) => {
                    return Err(CodecError::parse(
                        "WriterBuilder",
                        format!("Parent directory does not exist: {}", parent.display()),
                    ));
                }
                Err(e) => {
                    return Err(CodecError::parse(
                        "WriterBuilder",
                        format!("Cannot access parent directory {}: {}", parent.display(), e),
                    ));
                }
                Ok(true) => {} // Parent exists, continue
            }
        }

        // Detect format from extension
        let format = crate::io::detection::detect_format(&path);

        // Resolve Auto strategy to concrete strategy
        let resolved_strategy = self.config.strategy.resolve();

        // For new files, we trust the extension
        let format = match format {
            Ok(crate::io::metadata::FileFormat::Unknown) => {
                // If unknown, try extension
                match path.extension().and_then(|e| e.to_str()) {
                    Some("mcap") => crate::io::metadata::FileFormat::Mcap,
                    Some("bag") => crate::io::metadata::FileFormat::Bag,
                    _ => {
                        return Err(CodecError::parse(
                            "WriterBuilder",
                            format!("Unknown file format from extension: {}", path.display()),
                        ));
                    }
                }
            }
            Ok(f) => f,
            Err(e) => return Err(e),
        };

        // Update config with resolved strategy
        let config = WriterConfig {
            strategy: resolved_strategy,
            ..self.config
        };

        // Create the appropriate writer
        let inner = match format {
            crate::io::metadata::FileFormat::Mcap => {
                crate::io::formats::mcap::McapFormat::create_writer(&path, &config)?
            }
            crate::io::metadata::FileFormat::Bag => {
                crate::io::formats::bag::BagFormat::create_writer(&path, &config)?
            }
            crate::io::metadata::FileFormat::Rrd => {
                crate::io::formats::rrd::RrdFormat::create_writer(&path, &config)?
            }
            crate::io::metadata::FileFormat::Unknown => {
                return Err(CodecError::parse("WriterBuilder", "Unknown file format"));
            }
        };

        Ok(super::RoboWriter { inner })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = WriterConfig::default();
        assert_eq!(config.strategy, WriteStrategy::Auto);
        assert_eq!(config.compression_level, None);
        assert_eq!(config.chunk_size, None);
    }

    #[test]
    fn test_config_builder() {
        let config = WriterConfig::builder()
            .compression_level(3)
            .chunk_size(1024 * 1024)
            .num_threads(4)
            .build();

        assert_eq!(config.compression_level, Some(3));
        assert_eq!(config.chunk_size, Some(1024 * 1024));
        assert_eq!(config.num_threads, Some(4));
    }

    #[test]
    fn test_write_strategy_resolve() {
        assert_eq!(WriteStrategy::Auto.resolve(), WriteStrategy::Sequential);
        assert_eq!(
            WriteStrategy::Sequential.resolve(),
            WriteStrategy::Sequential
        );
        assert_eq!(WriteStrategy::Parallel.resolve(), WriteStrategy::Parallel);
    }

    #[test]
    fn test_builder_default() {
        let builder = WriterBuilder::new();
        assert_eq!(builder.config.strategy, WriteStrategy::Auto);
        assert_eq!(builder.config.compression_level, None);
    }

    #[test]
    fn test_builder_fluent() {
        let builder = WriterBuilder::new()
            .path("output.mcap")
            .compression_level(3)
            .chunk_size(1024 * 1024);

        assert_eq!(builder.config.path, PathBuf::from("output.mcap"));
        assert_eq!(builder.config.compression_level, Some(3));
        assert_eq!(builder.config.chunk_size, Some(1024 * 1024));
    }

    #[test]
    fn test_builder_path_not_set() {
        let builder = WriterBuilder::new();
        let result = builder.build();
        assert!(result.is_err());
        match result {
            Err(err) => {
                let err_msg = format!("{}", err);
                assert!(err_msg.contains("Path is not set"));
            }
            Ok(_) => panic!("Expected error when path not set"),
        }
    }

    #[test]
    fn test_builder_parent_directory_not_exists() {
        // Use a non-existent parent directory
        let result = WriterBuilder::new()
            .path("/nonexistent_directory_12345/output.mcap")
            .build();

        assert!(result.is_err());
        match result {
            Err(err) => {
                let err_msg = format!("{}", err);
                assert!(err_msg.contains("Parent directory does not exist"));
                assert!(err_msg.contains("/nonexistent_directory_12345"));
            }
            Ok(_) => panic!("Expected error when parent directory doesn't exist"),
        }
    }

    #[test]
    fn test_builder_unknown_extension() {
        // Create a temp directory
        let temp_dir = std::env::temp_dir();
        let unknown_path = temp_dir.join("test.unknown_ext_xyz");

        let result = WriterBuilder::new().path(&unknown_path).build();

        assert!(result.is_err());
        match result {
            Err(err) => {
                let err_msg = format!("{}", err);
                assert!(err_msg.contains("Unknown file format from extension"));
            }
            Ok(_) => panic!("Expected error for unknown extension"),
        }
    }

    #[test]
    fn test_builder_strategy_methods() {
        let builder = WriterBuilder::new()
            .path("output.bag")
            .strategy(WriteStrategy::Parallel)
            .compression_level(5)
            .chunk_size(2048)
            .num_threads(8);

        assert_eq!(builder.config.strategy, WriteStrategy::Parallel);
        assert_eq!(builder.config.compression_level, Some(5));
        assert_eq!(builder.config.chunk_size, Some(2048));
        assert_eq!(builder.config.num_threads, Some(8));
    }

    #[test]
    fn test_write_strategy_variants() {
        // Test that all variants can be created
        let auto = WriteStrategy::Auto;
        let sequential = WriteStrategy::Sequential;
        let parallel = WriteStrategy::Parallel;

        assert_eq!(auto.resolve(), WriteStrategy::Sequential);
        assert_eq!(sequential.resolve(), WriteStrategy::Sequential);
        assert_eq!(parallel.resolve(), WriteStrategy::Parallel);
    }

    // =========================================================================
    // HttpAuthConfig Tests
    // =========================================================================

    #[test]
    fn test_http_auth_config_default() {
        let config = HttpAuthConfig::default();
        assert!(config.is_empty());
        assert!(config.bearer_token.is_none());
        assert!(config.basic_username.is_none());
        assert!(config.basic_password.is_none());
    }

    #[test]
    fn test_http_auth_config_bearer() {
        let config = HttpAuthConfig::bearer("test-token");
        assert!(!config.is_empty());
        assert_eq!(config.bearer_token(), Some("test-token"));
        assert!(config.basic_username().is_none());
        assert!(config.basic_password().is_none());
    }

    #[test]
    fn test_http_auth_config_basic() {
        let config = HttpAuthConfig::basic("user", "pass");
        assert!(!config.is_empty());
        assert!(config.bearer_token().is_none());
        assert_eq!(config.basic_username(), Some("user"));
        assert_eq!(config.basic_password(), Some("pass"));
    }

    #[test]
    fn test_http_auth_config_equality() {
        let config1 = HttpAuthConfig::bearer("token");
        let config2 = HttpAuthConfig::bearer("token");
        assert_eq!(config1, config2);

        let config3 = HttpAuthConfig::basic("user", "pass");
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_writer_config_http_defaults() {
        let config = WriterConfig::default();
        assert!(config.http_auth.is_empty());
        assert_eq!(config.http_upload_chunk_size, 5 * 1024 * 1024);
        assert_eq!(config.http_max_retries, 3);
    }

    #[test]
    fn test_writer_config_builder_http_bearer() {
        let config = WriterConfig::builder()
            .http_bearer_token("test-token")
            .build();

        assert_eq!(config.http_auth.bearer_token(), Some("test-token"));
        assert!(config.http_auth.basic_username().is_none());
    }

    #[test]
    fn test_writer_config_builder_http_basic() {
        let config = WriterConfig::builder()
            .http_basic_auth("user", "pass")
            .build();

        assert!(config.http_auth.bearer_token().is_none());
        assert_eq!(config.http_auth.basic_username(), Some("user"));
        assert_eq!(config.http_auth.basic_password(), Some("pass"));
    }

    #[test]
    fn test_writer_config_builder_http_upload_chunk_size() {
        let config = WriterConfig::builder()
            .http_upload_chunk_size(10 * 1024 * 1024)
            .build();

        assert_eq!(config.http_upload_chunk_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_writer_config_builder_http_max_retries() {
        let config = WriterConfig::builder().http_max_retries(5).build();

        assert_eq!(config.http_max_retries, 5);
    }

    #[test]
    fn test_writer_config_builder_http_all_options() {
        let config = WriterConfig::builder()
            .http_bearer_token("token")
            .http_upload_chunk_size(8 * 1024 * 1024)
            .http_max_retries(7)
            .build();

        assert_eq!(config.http_auth.bearer_token(), Some("token"));
        assert_eq!(config.http_upload_chunk_size, 8 * 1024 * 1024);
        assert_eq!(config.http_max_retries, 7);
    }
}
