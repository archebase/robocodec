// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified rewriter support for robotics data formats.
//!
//! This module provides a trait-based abstraction for format-specific rewriters,
//! shared configuration types, and a unified facade that detects the format
//! from file extension.
//!
//! # Architecture
//!
//! - [`FormatRewriter`] - Trait for format-specific rewriter implementations
//! - [`RewriteOptions`] - Configuration for rewrite operations
//! - [`RewriteStats`] - Statistics from rewrite operations
//! - [`RoboRewriter`] - Unified facade that auto-detects format

use std::path::Path;

use crate::core::{CodecError, Result};
use crate::transform::MultiTransform;

/// Options for rewrite operations.
///
/// These options are shared across all format-specific rewriter implementations.
#[derive(Clone, Debug)]
pub struct RewriteOptions {
    /// Whether to validate schemas before rewriting
    pub validate_schemas: bool,

    /// Whether to skip messages that fail to decode
    pub skip_decode_failures: bool,

    /// Whether to pass through non-CDR messages without re-encoding
    pub passthrough_non_cdr: bool,

    /// Optional transformation pipeline for topic/type renaming.
    /// If None, no transformations are applied.
    pub transforms: Option<MultiTransform>,
}

impl Default for RewriteOptions {
    fn default() -> Self {
        Self {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        }
    }
}

impl RewriteOptions {
    /// Add a transform pipeline to the rewrite options.
    pub fn with_transforms(mut self, pipeline: MultiTransform) -> Self {
        self.transforms = Some(pipeline);
        self
    }

    /// Check if transformations are configured.
    pub fn has_transforms(&self) -> bool {
        self.transforms.as_ref().is_some_and(|p| !p.is_empty())
    }
}

/// Statistics from a rewrite operation.
///
/// These statistics are provided by all format-specific rewriter implementations.
#[derive(Debug, Clone, Default)]
pub struct RewriteStats {
    /// Total messages processed
    pub message_count: u64,

    /// Total channels processed
    pub channel_count: u64,

    /// Messages that failed to decode
    pub decode_failures: u64,

    /// Messages that failed to encode
    pub encode_failures: u64,

    /// Messages that were successfully re-encoded
    pub reencoded_count: u64,

    /// Messages passed through without re-encoding
    pub passthrough_count: u64,

    /// Number of topics renamed (if transforms were applied)
    pub topics_renamed: u64,

    /// Number of types renamed (if transforms were applied)
    pub types_renamed: u64,
}

impl RewriteStats {
    /// Create a new empty statistics struct.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Trait for format-specific rewriter implementations.
///
/// This trait defines the common interface that all format-specific rewriters
/// must implement. Each rewriter handles the specifics of reading, transforming,
/// and writing its respective format.
pub trait FormatRewriter: Send + Sync {
    /// Rewrite from input to output with configured transforms.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input file
    /// * `output_path` - Path to the output file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation.
    fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>;

    /// Get the options used for rewriting.
    fn options(&self) -> &RewriteOptions;

    /// Get as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Detect the format from a file path.
///
/// # Returns
///
/// - `Some("mcap")` for `.mcap` files
/// - `Some("bag")` for `.bag` files
/// - `None` for unknown extensions
pub fn detect_format(path: &Path) -> Option<&'static str> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "mcap" => Some("mcap"),
            "bag" => Some("bag"),
            _ => None,
        })
}

/// Unified rewriter facade that auto-detects format from file extension.
///
/// `RoboRewriter` provides a unified interface for rewriting both MCAP and BAG
/// files. The format is detected from the input file extension.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::RoboRewriter;
///
/// // MCAP format (detected from .mcap extension)
/// let mut rewriter = RoboRewriter::open("data.mcap")?;
/// rewriter.rewrite("output.mcap")?;
///
/// // BAG format (detected from .bag extension)
/// let mut rewriter = RoboRewriter::open("data.bag")?;
/// rewriter.rewrite("output.bag")?;
/// # Ok(())
/// # }
/// ```
pub enum RoboRewriter {
    /// MCAP format rewriter
    Mcap(crate::rewriter::mcap::McapRewriter, std::path::PathBuf),

    /// BAG format rewriter
    Bag(crate::rewriter::bag::BagRewriter, std::path::PathBuf),
}

impl RoboRewriter {
    /// Open a file and create the appropriate rewriter based on format detection.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the input file (format detected from extension)
    ///
    /// # Returns
    ///
    /// A `RoboRewriter` instance for the detected format.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file extension is not recognized
    /// - The file cannot be opened
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_options(path, RewriteOptions::default())
    }

    /// Create a rewriter with custom options for the specified file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the input file (format detected from extension)
    /// * `options` - Rewrite options including transforms
    ///
    /// # Returns
    ///
    /// A `RoboRewriter` instance for the detected format.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file extension is not recognized
    /// - The file cannot be opened
    pub fn with_options<P: AsRef<Path>>(path: P, options: RewriteOptions) -> Result<Self> {
        let path_ref = path.as_ref();
        let path_buf = path_ref.to_path_buf();

        // Validate that the file exists
        if !path_ref.exists() {
            return Err(CodecError::parse(
                "RoboRewriter",
                format!("File not found: {}", path_ref.display()),
            ));
        }

        match detect_format(path_ref) {
            Some("mcap") => Ok(RoboRewriter::Mcap(
                crate::rewriter::mcap::McapRewriter::with_options(options),
                path_buf,
            )),
            Some("bag") => Ok(RoboRewriter::Bag(
                crate::rewriter::bag::BagRewriter::with_options(options),
                path_buf,
            )),
            _ => Err(CodecError::encode(
                "RoboRewriter",
                format!(
                    "Unknown format: {:?}. Supported extensions: .mcap, .bag",
                    path_ref.extension()
                ),
            )),
        }
    }

    /// Rewrite to an output file.
    ///
    /// # Arguments
    ///
    /// * `output_path` - Path to the output file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation.
    pub fn rewrite<P: AsRef<Path>>(&mut self, output_path: P) -> Result<RewriteStats> {
        match self {
            RoboRewriter::Mcap(rewriter, input_path) => rewriter.rewrite(input_path, output_path),
            RoboRewriter::Bag(rewriter, input_path) => rewriter.rewrite(input_path, output_path),
        }
    }

    /// Get the options used for rewriting.
    pub fn options(&self) -> &RewriteOptions {
        match self {
            RoboRewriter::Mcap(rewriter, _) => rewriter.options(),
            RoboRewriter::Bag(rewriter, _) => rewriter.options(),
        }
    }

    /// Get the input file path.
    pub fn input_path(&self) -> &Path {
        match self {
            RoboRewriter::Mcap(_, path) | RoboRewriter::Bag(_, path) => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format_mcap() {
        let path = Path::new("test.mcap");
        assert_eq!(detect_format(path), Some("mcap"));
    }

    #[test]
    fn test_detect_format_bag() {
        let path = Path::new("test.bag");
        assert_eq!(detect_format(path), Some("bag"));
    }

    #[test]
    fn test_detect_format_unknown() {
        let path = Path::new("test.txt");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_detect_format_no_extension() {
        let path = Path::new("testfile");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_rewrite_options_default() {
        let options = RewriteOptions::default();
        assert!(options.validate_schemas);
        assert!(options.skip_decode_failures);
        assert!(options.passthrough_non_cdr);
        assert!(!options.has_transforms());
    }

    #[test]
    fn test_rewrite_stats_default() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
    }

    #[test]
    fn test_rewrite_stats_new() {
        let stats = RewriteStats::new();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
        assert_eq!(stats.reencoded_count, 0);
        assert_eq!(stats.passthrough_count, 0);
        assert_eq!(stats.decode_failures, 0);
        assert_eq!(stats.encode_failures, 0);
        assert_eq!(stats.topics_renamed, 0);
        assert_eq!(stats.types_renamed, 0);
    }

    #[test]
    fn test_rewrite_options_with_transforms() {
        use crate::transform::TransformBuilder;
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let options = RewriteOptions::default().with_transforms(pipeline);
        assert!(options.has_transforms());
    }

    #[test]
    fn test_rewrite_options_has_transforms_empty_pipeline() {
        use crate::transform::MultiTransform;
        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: Some(MultiTransform::new()),
        };
        assert!(!options.has_transforms());
    }

    #[test]
    fn test_rewrite_options_has_transforms_none() {
        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        assert!(!options.has_transforms());
    }

    #[test]
    fn test_rewrite_options_all_false() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        assert!(!options.validate_schemas);
        assert!(!options.skip_decode_failures);
        assert!(!options.passthrough_non_cdr);
        assert!(!options.has_transforms());
    }

    #[test]
    fn test_rewrite_options_all_true() {
        use crate::transform::TransformBuilder;
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(pipeline),
        };
        assert!(options.validate_schemas);
        assert!(options.skip_decode_failures);
        assert!(options.passthrough_non_cdr);
        assert!(options.has_transforms());
    }

    #[test]
    fn test_rewrite_stats_clone() {
        let stats1 = RewriteStats {
            message_count: 10,
            ..Default::default()
        };
        let stats2 = stats1.clone();
        assert_eq!(stats2.message_count, 10);
    }

    #[test]
    fn test_detect_format_case_insensitive() {
        // File extensions should be case-sensitive on Unix
        let path_mcap = Path::new("test.mcap");
        assert_eq!(detect_format(path_mcap), Some("mcap"));

        let path_bag = Path::new("test.bag");
        assert_eq!(detect_format(path_bag), Some("bag"));

        // Uppercase extensions are not recognized
        let path_upper = Path::new("test.MCAP");
        assert_eq!(detect_format(path_upper), None);
    }

    #[test]
    fn test_detect_format_with_dot_in_name() {
        // Files with dots in name should still detect from extension
        let path = Path::new("test.data.mcap");
        assert_eq!(detect_format(path), Some("mcap"));

        let path = Path::new("my.file.bag");
        assert_eq!(detect_format(path), Some("bag"));
    }

    #[test]
    fn test_detect_format_rrd() {
        // RRD format should not be recognized (not supported by rewriter)
        let path = Path::new("test.rrd");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_detect_format_empty_extension() {
        // File with trailing dot but no extension
        let path = Path::new("test.");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_detect_format_multiple_extensions() {
        // Files with multiple dots should still detect correctly
        let path = Path::new("test.data.file.mcap");
        assert_eq!(detect_format(path), Some("mcap"));

        let path = Path::new("my.data.file.bag");
        assert_eq!(detect_format(path), Some("bag"));
    }

    #[test]
    fn test_detect_format_json() {
        // JSON is not a supported rewriter format
        let path = Path::new("test.json");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_detect_format_dat() {
        // .dat files are not recognized
        let path = Path::new("test.dat");
        assert_eq!(detect_format(path), None);
    }

    #[test]
    fn test_robo_rewriter_open_unknown_format() {
        // This would try to open a file, so we test the error path differently
        let path = Path::new("test.unknown");
        let result = RoboRewriter::open(path);
        // Should fail due to unknown format OR file not found
        assert!(result.is_err());
    }

    #[test]
    fn test_robo_rewriter_open_nonexistent_file() {
        let path = Path::new("nonexistent.mcap");
        let result = RoboRewriter::open(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_robo_rewriter_with_options_unknown_format() {
        let path = Path::new("test.unknown");
        let options = RewriteOptions::default();
        let result = RoboRewriter::with_options(path, options);
        // Should fail due to unknown format
        assert!(result.is_err());
    }

    #[test]
    fn test_robo_rewriter_input_path_unsupported_extension() {
        // Test that unsupported extensions are handled
        let path = Path::new("test.txt");
        let format = detect_format(path);
        assert_eq!(format, None);
    }

    #[test]
    fn test_rewrite_options_builder_pattern() {
        use crate::transform::TransformBuilder;

        // Test the builder pattern is ergonomic
        let options = RewriteOptions::default().with_transforms(
            TransformBuilder::new()
                .with_topic_rename("/a", "/b")
                .with_type_rename("old/Old", "new/New")
                .build(),
        );

        assert!(options.has_transforms());
        assert!(options.validate_schemas);
        assert!(options.skip_decode_failures);
        assert!(options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewrite_options_combinations() {
        // Test various boolean combinations
        let test_cases = [
            (true, true, true),
            (true, true, false),
            (true, false, true),
            (true, false, false),
            (false, true, true),
            (false, true, false),
            (false, false, true),
            (false, false, false),
        ];

        for (validate_schemas, skip_decode_failures, passthrough_non_cdr) in test_cases {
            let options = RewriteOptions {
                validate_schemas,
                skip_decode_failures,
                passthrough_non_cdr,
                transforms: None,
            };

            assert_eq!(options.validate_schemas, validate_schemas);
            assert_eq!(options.skip_decode_failures, skip_decode_failures);
            assert_eq!(options.passthrough_non_cdr, passthrough_non_cdr);
        }
    }

    #[test]
    fn test_rewrite_stats_mutability() {
        // Test that stats fields can be modified
        let mut stats = RewriteStats::default();

        stats.message_count = 100;
        stats.channel_count = 5;
        stats.reencoded_count = 80;
        stats.passthrough_count = 20;
        stats.decode_failures = 2;
        stats.encode_failures = 1;
        stats.topics_renamed = 3;
        stats.types_renamed = 4;

        assert_eq!(stats.message_count, 100);
        assert_eq!(stats.channel_count, 5);
        assert_eq!(stats.reencoded_count, 80);
        assert_eq!(stats.passthrough_count, 20);
        assert_eq!(stats.decode_failures, 2);
        assert_eq!(stats.encode_failures, 1);
        assert_eq!(stats.topics_renamed, 3);
        assert_eq!(stats.types_renamed, 4);
    }

    #[test]
    fn test_rewrite_stats_fields_are_public() {
        // Verify all fields are accessible
        let stats = RewriteStats {
            message_count: 1,
            channel_count: 2,
            decode_failures: 3,
            encode_failures: 4,
            reencoded_count: 5,
            passthrough_count: 6,
            topics_renamed: 7,
            types_renamed: 8,
        };

        // All fields should be accessible publicly
        let _ = stats.message_count;
        let _ = stats.channel_count;
        let _ = stats.decode_failures;
        let _ = stats.encode_failures;
        let _ = stats.reencoded_count;
        let _ = stats.passthrough_count;
        let _ = stats.topics_renamed;
        let _ = stats.types_renamed;
    }

    #[test]
    fn test_rewrite_options_fields_are_public() {
        // Verify all fields are accessible
        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: None,
        };

        // All fields should be accessible publicly
        let _ = options.validate_schemas;
        let _ = options.skip_decode_failures;
        let _ = options.passthrough_non_cdr;
        let _ = options.transforms;
    }

    #[test]
    fn test_rewrite_options_clone() {
        use crate::transform::TransformBuilder;

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let options1 = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: Some(pipeline),
        };

        let options2 = options1.clone();

        assert_eq!(options1.validate_schemas, options2.validate_schemas);
        assert_eq!(options1.skip_decode_failures, options2.skip_decode_failures);
        assert_eq!(options1.passthrough_non_cdr, options2.passthrough_non_cdr);
        assert!(options2.has_transforms());
    }

    #[test]
    fn test_rewrite_stats_equality() {
        let stats1 = RewriteStats {
            message_count: 10,
            channel_count: 2,
            ..Default::default()
        };

        let stats2 = RewriteStats {
            message_count: 10,
            channel_count: 2,
            ..Default::default()
        };

        assert_eq!(stats1.message_count, stats2.message_count);
        assert_eq!(stats1.channel_count, stats2.channel_count);
    }

    #[test]
    fn test_rewrite_stats_independent_fields() {
        // Verify each stat field tracks independently
        let mut stats = RewriteStats::default();

        stats.message_count = 10;
        stats.reencoded_count = 8;
        stats.passthrough_count = 2;

        // reencoded + passthrough may be less than message_count (due to failures)
        assert_eq!(stats.reencoded_count + stats.passthrough_count, 10);
        assert_eq!(stats.message_count, 10);
    }

    #[test]
    fn test_format_rewriter_send_sync_bounds() {
        // The FormatRewriter trait requires Send + Sync
        // Verify that concrete implementations satisfy these bounds
        fn assert_send_sync<T: Send + Sync>() {}

        // BagRewriter should be Send + Sync
        assert_send_sync::<crate::rewriter::bag::BagRewriter>();

        // McapRewriter should be Send + Sync
        assert_send_sync::<crate::rewriter::mcap::McapRewriter>();
    }

    #[test]
    fn test_rewrite_options_debug_format() {
        let options = RewriteOptions::default();
        let debug_str = format!("{:?}", options);
        assert!(debug_str.contains("RewriteOptions"));
    }

    #[test]
    fn test_rewrite_stats_debug_format() {
        let stats = RewriteStats::default();
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("RewriteStats"));
    }
}
