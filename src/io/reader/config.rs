// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Reader configuration.

/// HTTP authentication configuration.
#[derive(Debug, Clone, Default)]
pub struct HttpAuthConfig {
    /// Bearer token for OAuth2/JWT authentication.
    pub bearer_token: Option<String>,
    /// Basic authentication username.
    pub basic_username: Option<String>,
    /// Basic authentication password.
    pub basic_password: Option<String>,
}

impl HttpAuthConfig {
    /// Create a new bearer token authentication config.
    #[must_use]
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            bearer_token: Some(token.into()),
            basic_username: None,
            basic_password: None,
        }
    }

    /// Create a new basic authentication config.
    #[must_use]
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            bearer_token: None,
            basic_username: Some(username.into()),
            basic_password: Some(password.into()),
        }
    }

    /// Check if any authentication is configured.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.bearer_token.is_some() || self.basic_username.is_some()
    }
}

/// Configuration for opening a `RoboReader`.
///
/// This config provides options for controlling reader behavior.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Prefer parallel reading when available (default: true).
    pub prefer_parallel: bool,
    /// Number of threads for parallel reading (None = auto-detect).
    pub num_threads: Option<usize>,
    /// Enable chunk merging for small chunks (default: true).
    pub chunk_merge_enabled: bool,
    /// Target merged chunk size in bytes (default: 16MB).
    pub chunk_merge_target_size: usize,
    /// HTTP authentication configuration.
    pub http_auth: HttpAuthConfig,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            prefer_parallel: true,
            num_threads: None,
            chunk_merge_enabled: true,
            chunk_merge_target_size: 16 * 1024 * 1024,
            http_auth: HttpAuthConfig::default(),
        }
    }
}

impl ReaderConfig {
    /// Create a new builder for ReaderConfig.
    #[must_use]
    pub fn builder() -> ReaderConfigBuilder {
        ReaderConfigBuilder::new()
    }

    /// Create a config that prefers parallel reading.
    #[must_use]
    pub fn parallel() -> Self {
        Self {
            prefer_parallel: true,
            ..Default::default()
        }
    }

    /// Create a config that prefers sequential reading.
    #[must_use]
    pub fn sequential() -> Self {
        Self {
            prefer_parallel: false,
            ..Default::default()
        }
    }

    /// Set HTTP bearer token authentication.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::ReaderConfig;
    ///
    /// let config = ReaderConfig::default()
    ///     .with_http_bearer_token("your-token-here");
    /// ```
    #[must_use]
    pub fn with_http_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.http_auth = HttpAuthConfig::bearer(token);
        self
    }

    /// Set HTTP basic authentication.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::ReaderConfig;
    ///
    /// let config = ReaderConfig::default()
    ///     .with_http_basic_auth("username", "password");
    /// ```
    #[must_use]
    pub fn with_http_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.http_auth = HttpAuthConfig::basic(username, password);
        self
    }
}

/// Builder for `ReaderConfig`.
///
/// Provides a fluent API for creating reader configurations.
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::ReaderConfig;
///
/// let config = ReaderConfig::builder()
///     .prefer_parallel(true)
///     .num_threads(4)
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReaderConfigBuilder {
    config: ReaderConfig,
}

impl ReaderConfigBuilder {
    /// Create a new builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to prefer parallel reading.
    #[must_use]
    pub fn prefer_parallel(mut self, value: bool) -> Self {
        self.config.prefer_parallel = value;
        self
    }

    /// Set the number of threads for parallel reading.
    #[must_use]
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Set whether chunk merging is enabled.
    #[must_use]
    pub fn chunk_merge_enabled(mut self, enabled: bool) -> Self {
        self.config.chunk_merge_enabled = enabled;
        self
    }

    /// Set the target merged chunk size in bytes.
    #[must_use]
    pub fn chunk_merge_target_size(mut self, size: usize) -> Self {
        self.config.chunk_merge_target_size = size;
        self
    }

    /// Set HTTP bearer token authentication.
    #[must_use]
    pub fn http_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.config.http_auth = HttpAuthConfig::bearer(token);
        self
    }

    /// Set HTTP basic authentication.
    #[must_use]
    pub fn http_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.http_auth = HttpAuthConfig::basic(username, password);
        self
    }

    /// Build the configuration.
    #[must_use]
    pub fn build(self) -> ReaderConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReaderConfig::default();
        assert!(config.prefer_parallel);
        assert!(config.chunk_merge_enabled);
        assert_eq!(config.chunk_merge_target_size, 16 * 1024 * 1024);
        assert!(config.num_threads.is_none());
    }

    #[test]
    fn test_parallel_config() {
        let config = ReaderConfig::parallel();
        assert!(config.prefer_parallel);
    }

    #[test]
    fn test_sequential_config() {
        let config = ReaderConfig::sequential();
        assert!(!config.prefer_parallel);
    }

    #[test]
    fn test_builder() {
        let config = ReaderConfig::builder()
            .prefer_parallel(false)
            .num_threads(4)
            .chunk_merge_enabled(false)
            .chunk_merge_target_size(8 * 1024 * 1024)
            .build();

        assert!(!config.prefer_parallel);
        assert_eq!(config.num_threads, Some(4));
        assert!(!config.chunk_merge_enabled);
        assert_eq!(config.chunk_merge_target_size, 8 * 1024 * 1024);
    }
}
