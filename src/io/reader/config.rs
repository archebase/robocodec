// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Reader configuration.

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
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            prefer_parallel: true,
            num_threads: None,
            chunk_merge_enabled: true,
            chunk_merge_target_size: 16 * 1024 * 1024,
        }
    }
}

impl ReaderConfig {
    /// Create a new builder for ReaderConfig.
    pub fn builder() -> ReaderConfigBuilder {
        ReaderConfigBuilder::new()
    }

    /// Create a config that prefers parallel reading.
    pub fn parallel() -> Self {
        Self {
            prefer_parallel: true,
            ..Default::default()
        }
    }

    /// Create a config that prefers sequential reading.
    pub fn sequential() -> Self {
        Self {
            prefer_parallel: false,
            ..Default::default()
        }
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to prefer parallel reading.
    pub fn prefer_parallel(mut self, value: bool) -> Self {
        self.config.prefer_parallel = value;
        self
    }

    /// Set the number of threads for parallel reading.
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Set whether chunk merging is enabled.
    pub fn chunk_merge_enabled(mut self, enabled: bool) -> Self {
        self.config.chunk_merge_enabled = enabled;
        self
    }

    /// Set the target merged chunk size in bytes.
    pub fn chunk_merge_target_size(mut self, size: usize) -> Self {
        self.config.chunk_merge_target_size = size;
        self
    }

    /// Build the configuration.
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
