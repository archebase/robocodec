// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Builder pattern for creating unified writers.

use std::path::PathBuf;

use crate::{CodecError, Result};

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
    /// Create a new builder for WriterConfig.
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the compression level.
    pub fn compression_level(mut self, level: i32) -> Self {
        self.config.compression_level = Some(level);
        self
    }

    /// Set the chunk size in bytes.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.config.chunk_size = Some(size);
        self
    }

    /// Set the number of threads.
    pub fn num_threads(mut self, count: usize) -> Self {
        self.config.num_threads = Some(count);
        self
    }

    /// Build the configuration.
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path to the output file.
    pub fn path<P: AsRef<std::path::Path>>(mut self, path: P) -> Self {
        self.config.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the writing strategy.
    pub fn strategy(mut self, strategy: WriteStrategy) -> Self {
        self.config.strategy = strategy;
        self
    }

    /// Set the compression level (1-22 for ZSTD).
    pub fn compression_level(mut self, level: i32) -> Self {
        self.config.compression_level = Some(level);
        self
    }

    /// Set the chunk size in bytes.
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
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(CodecError::parse(
                    "WriterBuilder",
                    format!("Parent directory does not exist: {}", parent.display()),
                ));
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
                        ))
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
            crate::io::metadata::FileFormat::Unknown => {
                return Err(CodecError::parse("WriterBuilder", "Unknown file format"))
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
}
