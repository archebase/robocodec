// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Builder pattern for creating unified writers.

use std::path::PathBuf;

use crate::Result;

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
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            strategy: WriteStrategy::Auto,
            compression_level: None,
            chunk_size: None,
            num_threads: None,
        }
    }
}

impl WriterConfig {
    /// Create a new builder for `WriterConfig`.
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

    /// Build the configuration.
    #[must_use]
    pub fn build(self) -> WriterConfig {
        self.config
    }
}

/// Builder for creating unified writers.
///
/// Provides a fluent interface for creating `RoboWriter` instances
/// with custom configuration.
///
/// # Example
///
/// ```rust,no_run
/// # use robocodec::Result;
/// use robocodec::io::WriterBuilder;
///
/// # fn main() -> Result<()> {
/// let writer = WriterBuilder::new()
///     .compression_level(3)
///     .chunk_size(1024 * 1024)
///     .create("output.mcap")?;
/// # Ok(())
/// # }
/// ```
pub struct WriterBuilder {
    config: WriterConfig,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WriterBuilder {
    /// Create a new writer builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: WriterConfig::default(),
        }
    }

    /// Set the output file path.
    #[must_use]
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.path = path.into();
        self
    }

    /// Set the writing strategy.
    #[must_use]
    pub fn strategy(mut self, strategy: WriteStrategy) -> Self {
        self.config.strategy = strategy;
        self
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

    /// Build the writer configuration.
    #[must_use]
    pub fn build(self) -> WriterConfig {
        self.config
    }

    /// Create the writer with the configured settings.
    pub fn create(self, path: &str) -> Result<crate::io::RoboWriter> {
        crate::io::RoboWriter::create_with_config(path, self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_config_default() {
        let config = WriterConfig::default();
        assert!(config.path.as_os_str().is_empty());
        assert_eq!(config.strategy, WriteStrategy::Auto);
        assert!(config.compression_level.is_none());
        assert!(config.chunk_size.is_none());
        assert!(config.num_threads.is_none());
    }

    #[test]
    fn test_writer_config_builder() {
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
    fn test_write_strategy_default() {
        let strategy = WriteStrategy::default();
        assert_eq!(strategy, WriteStrategy::Auto);
    }

    #[test]
    fn test_writer_builder() {
        let config = WriterBuilder::new()
            .path("output.mcap")
            .compression_level(5)
            .build();

        assert_eq!(config.path, PathBuf::from("output.mcap"));
        assert_eq!(config.compression_level, Some(5));
    }
}
