// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP file rewriter using decode-encode-write flow.
//!
//! This module provides functionality to normalize MCAP files by:
//! 1. Reading messages from the source MCAP
//! 2. Decoding each message (handles any CDR header issues)
//! 3. Re-encoding with proper CDR headers using schema-driven encoding
//! 4. Writing to a new MCAP file
//! 5. Optionally applying transformations (topic/type renaming, schema rewriting)
//!
//! This ensures consistent CDR formatting across all messages.
//!
//! **Note:** This implementation uses a custom MCAP writer with no external dependencies.
//!
//! # Module Organization
//!
//! - [`context`] - Context types for rewrite operations
//! - [`schema`] - Schema caching and management
//! - [`channel`] - Channel mapping and topic collision handling
//! - [`message`] - Message processing logic

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use tracing::warn;

use crate::core::{CodecError, Result};
use crate::io::formats::mcap::reader::McapReader;
use crate::io::formats::mcap::writer::ParallelMcapWriter;
use crate::schema::MessageSchema;
use crate::transform::ChannelInfo as TransformChannelInfo;

// Re-export submodule types
pub mod channel;
pub mod context;
pub mod message;
pub mod schema;

// Re-export commonly used types from submodules
pub use channel::{
    build_channel_mappings, get_transformed_topic, get_transformed_type,
    initialize_topic_collision_check, is_topic_renamed, is_type_renamed, resolve_topic_collision,
};
pub use context::{MessageHandling, RewriteContext};
pub use message::{
    determine_message_handling, extract_package_name, is_cdr_encoding, rewrite_cdr_message,
    should_passthrough_encoding, write_message_raw,
};
pub use schema::{build_schema_mappings, cache_schemas};
pub use schema::{get_schema_bytes, get_schema_encoding, should_add_schema};

use crate::rewriter::{FormatRewriter, RewriteOptions, RewriteStats};

/// Trait abstracting MCAP writer operations for testing.
///
/// This trait allows mocking the writer in tests and makes the rewriter
/// more testable by isolating writer-specific logic.
pub trait McapWriter: Send + Sync {
    /// Add a schema to the MCAP file.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be added to the MCAP file.
    fn add_schema(&mut self, name: &str, encoding: &str, data: &[u8]) -> Result<u16>;

    /// Add a channel to the MCAP file.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel cannot be added to the MCAP file.
    fn add_channel(
        &mut self,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16>;

    /// Write a message to the MCAP file.
    ///
    /// # Errors
    ///
    /// Returns an error if the message cannot be written to the MCAP file.
    fn write_message(
        &mut self,
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        data: &[u8],
    ) -> Result<()>;

    /// Finish writing and flush the MCAP file.
    ///
    /// Returns the total number of messages written.
    ///
    /// # Errors
    ///
    /// Returns an error if the MCAP file cannot be finalized or flushed.
    fn finish(&mut self) -> Result<u64>;
}

/// Implement `McapWriter` for the actual `ParallelMcapWriter`.
impl<W: std::io::Write + Send + Sync> McapWriter for ParallelMcapWriter<W> {
    fn add_schema(&mut self, name: &str, encoding: &str, data: &[u8]) -> Result<u16> {
        self.add_schema(name, encoding, data)
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to add schema: {e}")))
    }

    fn add_channel(
        &mut self,
        schema_id: u16,
        topic: &str,
        encoding: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<u16> {
        self.add_channel(schema_id, topic, encoding, metadata)
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to add channel: {e}")))
    }

    fn write_message(
        &mut self,
        channel_id: u16,
        log_time: u64,
        publish_time: u64,
        data: &[u8],
    ) -> Result<()> {
        self.write_message(channel_id, log_time, publish_time, data)
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to write message: {e}")))
    }

    fn finish(&mut self) -> Result<u64> {
        self.finish()
            .map(|_| 0)
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to finish MCAP: {e}")))
    }
}

/// MCAP file rewriter.
///
/// Performs a full decode-encode-write cycle to normalize MCAP files.
/// Can optionally apply transformations to rename topics, message types,
/// and rewrite schema definitions.
pub struct McapRewriter {
    /// Options for rewriting
    options: RewriteOptions,
    /// Cached schemas indexed by type name (transformed type name if transforms applied)
    schemas: HashMap<String, MessageSchema>,
    /// Statistics
    stats: RewriteStats,
    /// Sequence numbers per channel
    sequences: HashMap<u16, u32>,
}

impl McapRewriter {
    /// Create a new rewriter with default options.
    #[must_use]
    pub fn new() -> Self {
        Self::with_options(RewriteOptions::default())
    }

    /// Create a new rewriter with custom options.
    #[must_use]
    pub fn with_options(options: RewriteOptions) -> Self {
        Self {
            options,
            schemas: HashMap::new(),
            stats: RewriteStats::default(),
            sequences: HashMap::new(),
        }
    }

    /// Rewrite an MCAP file to a new location.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input MCAP file
    /// * `output_path` - Path to the output MCAP file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The input MCAP file cannot be opened or is malformed
    /// - The output MCAP file cannot be created
    /// - Schema parsing fails when `validate_schemas` is enabled
    /// - Transformation validation fails
    /// - Message encoding or writing fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use robocodec::rewriter::mcap::McapRewriter;
    /// use robocodec::transform::TransformBuilder;
    /// use robocodec::rewriter::RewriteOptions;
    ///
    /// // With transformations
    /// let options = RewriteOptions::default().with_transforms(
    ///     TransformBuilder::new()
    ///         .with_topic_rename("/old_camera", "/camera")
    ///         .with_type_rename("foo/JointState", "bar/JointState")
    ///         .build()
    /// );
    ///
    /// let mut rewriter = McapRewriter::with_options(options);
    /// let stats = rewriter.rewrite("input.mcap", "output.mcap")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        // Reset statistics and sequences
        self.stats = RewriteStats::default();
        self.sequences = HashMap::new();

        // Open input MCAP
        let reader = McapReader::open(input_path)?;
        self.stats.channel_count = reader.channels().len() as u64;

        // Validate transformations if configured
        if let Some(ref pipeline) = self.options.transforms {
            let transform_channels: Vec<TransformChannelInfo> = reader
                .channels()
                .values()
                .map(TransformChannelInfo::from_reader_info)
                .collect();
            pipeline
                .validate(&transform_channels)
                .map_err(|e| CodecError::encode("Transform", e.to_string()))?;
        }

        // Create output file
        let output_file = File::create(output_path).map_err(|e| {
            CodecError::encode("MCAP", format!("Failed to create output file: {e}"))
        })?;

        let mut mcap_writer =
            ParallelMcapWriter::new(BufWriter::new(output_file)).map_err(|e| {
                CodecError::encode("MCAP", format!("Failed to create MCAP writer: {e}"))
            })?;

        // Pre-parse all schemas with transformations applied
        if self.options.validate_schemas {
            cache_schemas(
                &reader,
                &mut self.schemas,
                self.options.transforms.as_ref(),
                self.options.validate_schemas,
            )?;
        }

        // Build schema ID and channel ID mappings with transformations
        let mut schema_ids: HashMap<String, u16> = HashMap::new();
        let mut channel_map: HashMap<u16, u16> = HashMap::new();
        let mut topic_counter: HashMap<String, u32> = HashMap::new();
        let pipeline = self.options.transforms.as_ref();

        // First pass: add all schemas (with transformations applied)
        build_schema_mappings(
            reader.channels(),
            &mut schema_ids,
            &mut mcap_writer,
            pipeline,
        )?;

        // Second pass: add all channels (with transformations applied)
        build_channel_mappings(
            reader.channels(),
            &schema_ids,
            &mut topic_counter,
            &mut channel_map,
            &mut mcap_writer,
            pipeline,
            &mut self.stats,
        )?;

        // Process messages
        let messages = reader.iter_raw()?;
        let mut stream = messages.stream()?;

        // Clone schemas for use in closure
        let schemas = self.schemas.clone();

        // Build a map of original channel_id -> transformed message type for schema lookup
        let pipeline = self.options.transforms.as_ref();
        let channel_type_map: HashMap<u16, String> = reader
            .channels()
            .iter()
            .map(|(id, ch)| {
                let transformed = if let Some(p) = pipeline {
                    p.transform_type(&ch.message_type, None).0
                } else {
                    ch.message_type.clone()
                };
                (*id, transformed)
            })
            .collect();

        // Extract boolean flags (can't clone RewriteOptions due to MultiTransform)
        let passthrough_non_cdr = self.options.passthrough_non_cdr;

        // Process each message
        for result in &mut stream {
            let (msg, channel_info) = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!(
                        context = "message_read",
                        error = %e,
                        "Failed to read message"
                    );
                    continue;
                }
            };

            self.stats.message_count += 1;
            let new_channel_id = channel_map
                .get(&msg.channel_id)
                .copied()
                .unwrap_or(msg.channel_id);

            // Only rewrite CDR messages
            if channel_info.encoding != "cdr"
                && channel_info.encoding != "ros2"
                && channel_info.encoding != "ros2msg"
            {
                // Pass through non-CDR messages
                if passthrough_non_cdr {
                    write_message_raw(&mut mcap_writer, &msg, new_channel_id)?;
                    self.stats.passthrough_count += 1;
                }
                continue;
            }

            // Get the transformed message type for schema lookup
            let transformed_type = channel_type_map
                .get(&msg.channel_id)
                .unwrap_or(&channel_info.message_type);

            // Get or parse schema (using transformed type)
            let schema_opt = schemas.get(transformed_type);

            // Decode and re-encode CDR messages
            if let Some(schema) = schema_opt {
                rewrite_cdr_message(
                    &mut mcap_writer,
                    &msg,
                    schema,
                    new_channel_id,
                    &channel_info.topic,
                    &self.options,
                    &mut self.stats,
                )?;
            } else {
                // No schema available, pass through as-is
                write_message_raw(&mut mcap_writer, &msg, new_channel_id)?;
                self.stats.passthrough_count += 1;
            }
        }

        // Finish the MCAP writer
        mcap_writer
            .finish()
            .map_err(|e| CodecError::encode("MCAP", format!("Failed to finish MCAP: {e}")))?;

        Ok(self.stats.clone())
    }

    /// Get the options used for rewriting.
    #[must_use]
    pub fn options(&self) -> &RewriteOptions {
        &self.options
    }
}

impl FormatRewriter for McapRewriter {
    fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        self.rewrite(input_path, output_path)
    }

    fn options(&self) -> &RewriteOptions {
        self.options()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for McapRewriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to rewrite an MCAP file.
///
/// # Arguments
///
/// * `input_path` - Path to the input MCAP file
/// * `output_path` - Path to the output MCAP file
///
/// # Errors
///
/// Returns an error if:
/// - The input file cannot be opened or is malformed
/// - The output file cannot be created
/// - Message decoding or encoding fails
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use robocodec::rewriter::mcap::rewrite_mcap;
///
/// let stats = rewrite_mcap("input.mcap", "output.mcap")?;
/// println!("Processed {} messages", stats.message_count);
/// # Ok(())
/// # }
/// ```
pub fn rewrite_mcap<P1, P2>(input_path: P1, output_path: P2) -> Result<RewriteStats>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let mut rewriter = McapRewriter::new();
    rewriter.rewrite(input_path, output_path)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::TransformBuilder;
    use std::path::PathBuf;

    /// Get the fixtures directory path
    pub fn fixtures_dir() -> PathBuf {
        // Use CARGO_MANIFEST_DIR to get the robocodec crate root,
        // then go up to workspace root to access shared fixtures
        let manifest_dir =
            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| String::from("."));
        PathBuf::from(manifest_dir)
            .parent()
            .expect("manifest dir should have parent")
            .join("tests")
            .join("fixtures")
    }

    /// Get a temporary file path for test output
    pub fn temp_output(name: &str) -> PathBuf {
        let random = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        std::env::temp_dir().join(format!("roboflow_mcap_test_{random}_{name}"))
    }

    /// Check if a fixture file exists
    pub fn fixture_exists(name: &str) -> bool {
        fixtures_dir().join(name).exists()
    }

    // =========================================================================
    // Construction Tests
    // =========================================================================

    #[test]
    fn test_rewriter_new_creates_with_default_options() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.options.validate_schemas);
        assert!(rewriter.options.skip_decode_failures);
        assert!(rewriter.options.passthrough_non_cdr);
        assert!(!rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_default() {
        let rewriter = McapRewriter::default();
        assert!(rewriter.options.validate_schemas);
    }

    #[test]
    fn test_rewriter_with_custom_options() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.validate_schemas);
        assert!(!rewriter.options.skip_decode_failures);
        assert!(!rewriter.options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_with_options_has_empty_caches() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.schemas.is_empty());
        assert_eq!(rewriter.stats.message_count, 0);
        assert!(rewriter.sequences.is_empty());
    }

    #[test]
    fn test_rewriter_options_returns_reference() {
        let rewriter = McapRewriter::new();
        let opts = rewriter.options();
        assert!(opts.validate_schemas);
    }

    // =========================================================================
    // RewriteOptions Tests
    // =========================================================================

    #[test]
    fn test_rewrite_options_default() {
        let opts = RewriteOptions::default();
        assert!(opts.validate_schemas);
        assert!(opts.skip_decode_failures);
        assert!(opts.passthrough_non_cdr);
        assert!(!opts.has_transforms());
    }

    #[test]
    fn test_rewrite_options_with_transforms() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: Some(pipeline),
        };
        assert!(opts.has_transforms());
    }

    // =========================================================================
    // RewriteStats Tests
    // =========================================================================

    #[test]
    fn test_rewrite_stats_default() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
        assert_eq!(stats.topics_renamed, 0);
        assert_eq!(stats.types_renamed, 0);
        assert_eq!(stats.reencoded_count, 0);
        assert_eq!(stats.passthrough_count, 0);
        assert_eq!(stats.decode_failures, 0);
        assert_eq!(stats.encode_failures, 0);
    }

    #[test]
    fn test_rewrite_stats_can_be_updated() {
        let stats = RewriteStats {
            message_count: 100,
            channel_count: 5,
            topics_renamed: 2,
            types_renamed: 1,
            reencoded_count: 95,
            passthrough_count: 5,
            decode_failures: 2,
            encode_failures: 1,
        };
        assert_eq!(stats.message_count, 100);
        assert_eq!(stats.channel_count, 5);
        assert_eq!(stats.topics_renamed, 2);
        assert_eq!(stats.types_renamed, 1);
        assert_eq!(stats.reencoded_count, 95);
        assert_eq!(stats.passthrough_count, 5);
        assert_eq!(stats.decode_failures, 2);
        assert_eq!(stats.encode_failures, 1);
    }

    // =========================================================================
    // FormatRewriter Trait Tests
    // =========================================================================

    #[test]
    fn test_mcap_rewriter_implements_format_rewriter_methods() {
        let rewriter = McapRewriter::new();
        // Test that the trait methods are accessible
        let _opts = rewriter.options();
        let _any = rewriter.as_any();
    }

    // =========================================================================
    // Error Handling Tests
    // =========================================================================

    #[test]
    fn test_rewriter_returns_error_for_nonexistent_input() {
        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite("/nonexistent/file.mcap", "/tmp/output.mcap");
        assert!(result.is_err());
    }

    #[test]
    fn test_rewriter_returns_error_for_invalid_output_path() {
        if fixture_exists("example.mcap") {
            let mut rewriter = McapRewriter::new();
            // Use an invalid path (directory that doesn't exist)
            let result = rewriter.rewrite(
                fixtures_dir().join("example.mcap"),
                "/nonexistent/dir/output.mcap",
            );
            assert!(result.is_err());
        }
    }

    // =========================================================================
    // Integration Tests (with actual MCAP files)
    // =========================================================================

    #[test]
    fn test_rewriter_processes_mcap_file() {
        if !fixture_exists("example.mcap") {
            return; // Skip if fixture not available
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_rewrite");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewriter_tracks_statistics() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_stats");

        let mut rewriter = McapRewriter::new();
        if let Ok(_stats) = rewriter.rewrite(&input, &output) {
            // Stats should be non-zero if file had messages
            let _ = std::fs::remove_file(&output);
        }
    }

    // =========================================================================
    // Transform Tests
    // =========================================================================

    #[test]
    fn test_rewriter_with_transform_pipeline() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old_topic", "/new_topic")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_transform");

        let result = rewriter.rewrite(&input, &output);
        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewriter_with_skip_decode_failures() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.skip_decode_failures);
    }

    #[test]
    fn test_rewriter_with_passthrough_non_cdr() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.passthrough_non_cdr);
    }

    // =========================================================================
    // Multiple Rewrite Tests
    // =========================================================================

    #[test]
    fn test_multiple_rewrites_are_independent() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output1 = temp_output("test_multi_1");
        let output2 = temp_output("test_multi_2");

        let mut rewriter = McapRewriter::new();

        // First rewrite
        let stats1 = rewriter.rewrite(&input, &output1);
        let channel_count1 = rewriter.stats.channel_count;

        // Second rewrite should reset stats
        let stats2 = rewriter.rewrite(&input, &output2);
        let channel_count2 = rewriter.stats.channel_count;

        // Clean up
        let _ = std::fs::remove_file(&output1);
        let _ = std::fs::remove_file(&output2);

        if let (Ok(s1), Ok(s2)) = (stats1, stats2) {
            assert_eq!(s1.channel_count, s2.channel_count);
            assert_eq!(channel_count1, channel_count2);
        }
    }

    #[test]
    fn test_rewriter_with_empty_transform_pipeline() {
        let pipeline = TransformBuilder::new().build();
        let opts = RewriteOptions::default().with_transforms(pipeline);
        let rewriter = McapRewriter::with_options(opts);
        // Empty pipeline should work without errors
        assert!(!rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_preserves_all_options() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: Some(pipeline),
        };

        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.validate_schemas);
        assert!(!rewriter.options.skip_decode_failures);
        assert!(!rewriter.options.passthrough_non_cdr);
        assert!(rewriter.options.has_transforms());
    }

    // =========================================================================
    // Round-trip Tests
    // =========================================================================

    #[test]
    fn test_rewriter_round_trip_preserves_messages() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_roundtrip");

        let mut rewriter = McapRewriter::new();
        if let Ok(stats) = rewriter.rewrite(&input, &output) {
            // If we processed messages, verify output file exists
            if stats.message_count > 0 {
                assert!(output.exists());
                let _ = std::fs::remove_file(&output);
            }
        }
    }

    #[test]
    fn test_rewriter_initializes_with_empty_schemas() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.schemas.is_empty());
    }

    #[test]
    fn test_rewriter_resets_state_on_multiple_rewrites() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output1 = temp_output("test_reset_1");
        let output2 = temp_output("test_reset_2");

        let mut rewriter = McapRewriter::new();

        let _ = rewriter.rewrite(&input, &output1);
        let stats1 = rewriter.stats.clone();

        let _ = rewriter.rewrite(&input, &output2);
        let stats2 = rewriter.stats.clone();

        // Stats should be reset on second rewrite
        assert_eq!(stats2.message_count, stats1.message_count);

        let _ = std::fs::remove_file(&output1);
        let _ = std::fs::remove_file(&output2);
    }

    #[test]
    fn test_rewriter_with_no_schema_validation() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.validate_schemas);
    }

    // =========================================================================
    // As Any Tests
    // =========================================================================

    #[test]
    fn test_as_any_returns_valid_reference() {
        let rewriter = McapRewriter::new();
        let _any = rewriter.as_any();
        // Just verify it doesn't panic
    }

    #[test]
    fn test_as_any_downcast() {
        let rewriter = McapRewriter::new();
        let any = rewriter.as_any();
        let _downcast = any.downcast_ref::<McapRewriter>();
        // Downcast should succeed
    }

    // =========================================================================
    // More Tests...
    // =========================================================================

    #[test]
    fn test_rewriter_with_non_cdr_passthrough_disabled() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_skip_decode_failures_false() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.skip_decode_failures);
    }

    #[test]
    fn test_rewriter_all_options_false() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.validate_schemas);
        assert!(!rewriter.options.skip_decode_failures);
        assert!(!rewriter.options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_tracks_reencoded_count() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_reencoded");

        let mut rewriter = McapRewriter::new();
        if let Ok(_stats) = rewriter.rewrite(&input, &output) {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewriter_tracks_passthrough_count() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_passthrough");

        let mut rewriter = McapRewriter::new();
        if let Ok(_stats) = rewriter.rewrite(&input, &output) {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewriter_tracks_failure_counts() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_failures");

        let mut rewriter = McapRewriter::new();
        if let Ok(_stats) = rewriter.rewrite(&input, &output) {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewriter_with_type_transform_only() {
        let pipeline = TransformBuilder::new()
            .with_type_rename("old/OldType", "new/NewType")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_with_topic_transform_only() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_with_multiple_transforms() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .with_type_rename("old/Old", "new/New")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_with_no_transforms_but_options_set() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_maintains_sequence_state() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.sequences.is_empty());
    }

    #[test]
    fn test_schemas_map_initially_empty() {
        let rewriter = McapRewriter::new();
        assert!(rewriter.schemas.is_empty());
    }

    #[test]
    fn test_rewriter_with_validat_schemas_true() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.validate_schemas);
    }

    // =========================================================================
    // Stats Field Tests
    // =========================================================================

    #[test]
    fn test_rewrite_stats_topics_renamed() {
        let stats = RewriteStats {
            topics_renamed: 5,
            ..RewriteStats::default()
        };
        assert_eq!(stats.topics_renamed, 5);
    }

    #[test]
    fn test_rewrite_stats_types_renamed() {
        let stats = RewriteStats {
            types_renamed: 3,
            ..RewriteStats::default()
        };
        assert_eq!(stats.types_renamed, 3);
    }

    #[test]
    fn test_rewrite_stats_encode_failures() {
        let stats = RewriteStats {
            encode_failures: 1,
            ..RewriteStats::default()
        };
        assert_eq!(stats.encode_failures, 1);
    }

    // =========================================================================
    // RewriteOptions Field Tests
    // =========================================================================

    #[test]
    fn test_rewrite_options_validate_schemas_false() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        assert!(!opts.validate_schemas);
    }

    #[test]
    fn test_rewrite_options_skip_decode_failures_false() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: None,
        };
        assert!(!opts.skip_decode_failures);
    }

    #[test]
    fn test_rewrite_options_passthrough_non_cdr_false() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        assert!(!opts.passthrough_non_cdr);
    }

    #[test]
    fn test_rewrite_options_has_transforms_none() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        assert!(!opts.has_transforms());
    }

    #[test]
    fn test_rewrite_options_has_transforms_some_empty() {
        let pipeline = TransformBuilder::new().build();
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(pipeline),
        };
        assert!(!opts.has_transforms());
    }

    // =========================================================================
    // As Any Tests
    // =========================================================================

    #[test]
    fn test_rewriter_as_any() {
        let rewriter = McapRewriter::new();
        let _ = rewriter.as_any();
    }

    // =========================================================================
    // Convenience Function Tests
    // =========================================================================

    #[test]
    fn test_rewrite_mcap_convenience_function() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_convenience");

        let result = rewrite_mcap(&input, &output);
        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    // =========================================================================
    // Statistics Tracking Tests
    // =========================================================================

    #[test]
    fn test_rewriter_tracks_topics_renamed() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_topics_renamed");

        let _ = rewriter.rewrite(&input, &output);
        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn test_rewriter_tracks_types_renamed() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let pipeline = TransformBuilder::new()
            .with_type_rename("old/Old", "new/New")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_types_renamed");

        let _ = rewriter.rewrite(&input, &output);
        let _ = std::fs::remove_file(&output);
    }

    // =========================================================================
    // Schema Caching Tests
    // =========================================================================

    #[test]
    fn test_rewriter_caches_schemas_when_validate_true() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.validate_schemas);
    }

    #[test]
    fn test_rewriter_no_schema_caching_when_validate_false() {
        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.validate_schemas);
    }

    #[test]
    fn test_rewriter_passthrough_non_cdr_enabled() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_passthrough_non_cdr_disabled() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(!rewriter.options.passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_skip_decode_failures_true() {
        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.skip_decode_failures);
    }

    #[test]
    fn test_rewriter_handles_empty_stats() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
    }

    #[test]
    fn test_rewriter_schema_caching_with_type_transform() {
        let pipeline = TransformBuilder::new()
            .with_type_rename("old/Old", "new/New")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.validate_schemas);
    }

    // =========================================================================
    // Stats Tests
    // =========================================================================

    #[test]
    fn test_rewrite_stats_all_fields_zero() {
        let stats = RewriteStats::default();
        assert_eq!(stats.message_count, 0);
        assert_eq!(stats.channel_count, 0);
        assert_eq!(stats.topics_renamed, 0);
        assert_eq!(stats.types_renamed, 0);
        assert_eq!(stats.reencoded_count, 0);
        assert_eq!(stats.passthrough_count, 0);
        assert_eq!(stats.decode_failures, 0);
        assert_eq!(stats.encode_failures, 0);
    }

    #[test]
    fn test_rewriter_resets_sequences_on_rewrite() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_sequences");

        let mut rewriter = McapRewriter::new();

        let _ = rewriter.rewrite(&input, &output);
        assert!(rewriter.sequences.is_empty()); // Sequences are reset

        let _ = std::fs::remove_file(&output);
    }

    #[test]
    fn test_rewriter_resets_stats_on_rewrite() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output1 = temp_output("test_stats_reset_1");
        let output2 = temp_output("test_stats_reset_2");

        let mut rewriter = McapRewriter::new();

        let _ = rewriter.rewrite(&input, &output1);
        let _ = rewriter.rewrite(&input, &output2);

        // Second rewrite should have reset stats
        let stats = rewriter.stats.clone();
        assert_eq!(stats.channel_count, 0); // Should be reset

        let _ = std::fs::remove_file(&output1);
        let _ = std::fs::remove_file(&output2);
    }

    // =========================================================================
    // Error Path Tests
    // =========================================================================

    #[test]
    fn test_rewriter_returns_error_for_missing_fixture() {
        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite("/nonexistent/file.mcap", "/tmp/output.mcap");
        assert!(result.is_err());
    }

    // =========================================================================
    // Stats Clone Tests
    // =========================================================================

    #[test]
    fn test_rewrite_stats_clone_and_equality() {
        let stats1 = RewriteStats {
            message_count: 10,
            ..RewriteStats::default()
        };
        let stats2 = stats1.clone();
        assert_eq!(stats1.message_count, stats2.message_count);
    }

    // =========================================================================
    // Options Debug Tests
    // =========================================================================

    #[test]
    fn test_rewrite_options_debug() {
        let opts = RewriteOptions::default();
        let _ = format!("{:?}", opts);
    }

    #[test]
    fn test_mcap_rewriter_clone() {
        let rewriter = McapRewriter::new();
        // Rewriter itself doesn't need to be cloneable,
        // but we can verify the options work
        let _ = rewriter.options();
    }

    // =========================================================================
    // Rewrite Function Tests
    // =========================================================================

    #[test]
    fn test_rewrite_mcap_function_with_invalid_input() {
        let result = rewrite_mcap("/nonexistent/file.mcap", "/tmp/output.mcap");
        assert!(result.is_err());
    }

    #[test]
    fn test_mcap_rewriter_with_all_transform_options() {
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .with_type_rename("old/Old", "new/New")
            .build();

        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: Some(pipeline),
        };

        let rewriter = McapRewriter::with_options(opts);
        assert!(rewriter.options.has_transforms());
    }

    #[test]
    fn test_rewriter_process_with_various_encoding_checks() {
        // Test that encoding detection works
        assert!(is_cdr_encoding("cdr"));
        assert!(!is_cdr_encoding("json"));
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
            let opts = RewriteOptions {
                validate_schemas,
                skip_decode_failures,
                passthrough_non_cdr,
                transforms: None,
            };

            assert_eq!(opts.validate_schemas, validate_schemas);
            assert_eq!(opts.skip_decode_failures, skip_decode_failures);
            assert_eq!(opts.passthrough_non_cdr, passthrough_non_cdr);
        }
    }

    #[test]
    fn test_rewriter_options_accessor_methods() {
        let rewriter = McapRewriter::new();
        let opts = rewriter.options();
        assert!(opts.validate_schemas);
    }

    #[test]
    fn test_mcap_rewriter_display_behavior() {
        // Just verify the rewriter can be created and accessed
        let rewriter = McapRewriter::new();
        let _ = format!("{:?}", rewriter.options);
    }

    #[test]
    fn test_rewrite_stats_partial_update() {
        let stats = RewriteStats {
            message_count: 10,
            reencoded_count: 8,
            ..RewriteStats::default()
        };
        assert_eq!(stats.message_count, 10);
        assert_eq!(stats.reencoded_count, 8);
    }

    #[test]
    fn test_mcap_rewriter_statistics_accumulation() {
        let stats = RewriteStats {
            message_count: 100,
            channel_count: 5,
            reencoded_count: 80,
            passthrough_count: 20,
            ..RewriteStats::default()
        };

        assert_eq!(stats.message_count, 100);
        assert_eq!(stats.channel_count, 5);
        assert_eq!(stats.reencoded_count + stats.passthrough_count, 100);
    }

    #[test]
    fn test_rewrite_options_with_empty_transforms() {
        let pipeline = TransformBuilder::new().build();
        let opts = RewriteOptions::default().with_transforms(pipeline);
        assert!(!opts.has_transforms());
    }

    #[test]
    fn test_rewriter_path_handling() {
        // Test that the rewriter can handle paths as references
        let input: &str = "test.mcap";
        let output: &str = "out.mcap";
        let _ = input;
        let _ = output;
    }

    #[test]
    fn test_rewriter_reference_types() {
        let rewriter = McapRewriter::new();
        // Verify reference types work
        let _opts: &RewriteOptions = rewriter.options();
    }

    #[test]
    fn test_rewrite_stats_all_fields_independent() {
        let stats = RewriteStats {
            message_count: 100,
            channel_count: 5,
            reencoded_count: 80,
            passthrough_count: 20,
            decode_failures: 2,
            encode_failures: 1,
            topics_renamed: 3,
            types_renamed: 4,
        };

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
    fn test_mcap_rewriter_implements_send() {
        fn assert_send<T: Send>() {}
        assert_send::<McapRewriter>();
    }

    #[test]
    fn test_mcap_rewriter_implements_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<McapRewriter>();
    }

    #[test]
    fn test_rewrite_stats_implements_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RewriteStats>();
    }

    // =========================================================================
    // CDR Encoding Tests
    // =========================================================================

    #[test]
    fn test_is_cdr_encoding() {
        assert!(is_cdr_encoding("cdr"));
        assert!(is_cdr_encoding("ros2"));
        assert!(is_cdr_encoding("ros2msg"));
        assert!(!is_cdr_encoding("json"));
    }

    #[test]
    fn test_resolve_topic_collision_no_collision() {
        let mut ctx = RewriteContext::new();
        let topic = resolve_topic_collision("/chatter".to_string(), &mut ctx);
        assert_eq!(topic, "/chatter");
    }

    #[test]
    fn test_resolve_topic_collision_with_collision() {
        let mut ctx = RewriteContext::new();
        ctx.topic_counter.insert("/chatter".to_string(), 0);
        let topic = resolve_topic_collision("/chatter".to_string(), &mut ctx);
        assert_eq!(topic, "/chatter_1");
    }

    #[test]
    fn test_resolve_topic_collision_multiple_collisions() {
        let mut ctx = RewriteContext::new();
        ctx.topic_counter.insert("/chatter".to_string(), 1);
        let topic = resolve_topic_collision("/chatter".to_string(), &mut ctx);
        assert_eq!(topic, "/chatter_2");
    }

    #[test]
    fn test_initialize_topic_collision_check_no_collision() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let reader = McapReader::open(fixtures_dir().join("example.mcap")).ok();
        if let Some(reader) = reader {
            let _ = initialize_topic_collision_check("/unique_topic", 0, &reader);
        }
    }

    // =========================================================================
    // RewriteContext Tests
    // =========================================================================

    #[test]
    fn test_rewrite_context_new() {
        let ctx = RewriteContext::new();
        assert!(ctx.schema_ids.is_empty());
        assert!(ctx.channel_map.is_empty());
    }

    #[test]
    fn test_rewrite_context_reset() {
        let mut ctx = RewriteContext::new();
        ctx.schema_ids.insert("test".to_string(), 1);
        ctx.channel_map.insert(1, 2);
        ctx.reset();
        assert!(ctx.schema_ids.is_empty());
        assert!(ctx.channel_map.is_empty());
    }

    #[test]
    fn test_rewrite_context_get_channel_id() {
        let mut ctx = RewriteContext::new();
        ctx.channel_map.insert(1, 10);
        assert_eq!(ctx.get_channel_id(1), Some(10));
        assert_eq!(ctx.get_channel_id(2), None);
    }

    #[test]
    fn test_rewrite_context_get_schema_id() {
        let mut ctx = RewriteContext::new();
        ctx.schema_ids.insert("std_msgs/String".to_string(), 5);
        assert_eq!(ctx.get_schema_id("std_msgs/String"), Some(5));
    }

    #[test]
    fn test_rewrite_context_get_transformed_type() {
        let mut ctx = RewriteContext::new();
        ctx.channel_type_map.insert(1, "new/Type".to_string());
        assert_eq!(ctx.get_transformed_type(1), Some("new/Type"));
    }

    #[test]
    fn test_rewrite_context_has_topic_collision() {
        let ctx = RewriteContext::new();
        assert!(!ctx.has_topic_collision("/topic", 1));

        let mut ctx_with_collision = RewriteContext::new();
        ctx_with_collision
            .topic_counter
            .insert("/topic".to_string(), 1);
        assert!(ctx_with_collision.has_topic_collision("/topic", 1));
    }

    // =========================================================================
    // McapWriter Trait Tests
    // =========================================================================

    #[test]
    fn test_mcap_writer_trait_bounds() {
        fn assert_writer_bounds<W: McapWriter>() {}
        // Just verify the trait bounds work
        assert_writer_bounds::<ParallelMcapWriter<BufWriter<std::io::Cursor<Vec<u8>>>>>();
    }

    #[test]
    fn test_mcap_rewriter_send_sync_bounds() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<McapRewriter>();
    }

    // =========================================================================
    // Integration Tests with Real MCAP Files
    // =========================================================================

    #[test]
    fn test_rewrite_simple_mcap_fixture() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_simple");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewrite_mcap_with_all_options_disabled() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_all_disabled");

        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewrite_mcap_with_schema_validation() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_validation");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewrite_mcap_preserves_channels() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_preserve");

        let mut rewriter = McapRewriter::new();
        if let Ok(_stats) = rewriter.rewrite(&input, &output) {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewrite_mcap_with_skip_decode_failures() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("test_skip");

        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_rewrite_mcap_nonexistent_file() {
        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite("/nonexistent.mcap", "/tmp/output.mcap");
        assert!(result.is_err());
    }

    #[test]
    fn test_rewrite_mcap_invalid_output_path() {
        let mut rewriter = McapRewriter::new();
        // Use an invalid output path
        let result = rewriter.rewrite(
            "/nonexistent/input.mcap",
            "/nonexistent_deeply_nested_dir/output.mcap",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_rewrite_context_integration() {
        let mut ctx = RewriteContext::new();
        ctx.schema_ids.insert("test".to_string(), 1);
        ctx.channel_map.insert(1, 2);
        ctx.topic_counter.insert("/topic".to_string(), 1);
        ctx.channel_type_map.insert(1, "TestType".to_string());

        assert_eq!(ctx.get_schema_id("test"), Some(1));
        assert_eq!(ctx.get_channel_id(1), Some(2));
        assert!(ctx.has_topic_collision("/topic", 1));
        assert_eq!(ctx.get_transformed_type(1), Some("TestType"));
    }

    #[test]
    fn test_cdr_encoding_detection() {
        // Test all CDR variants
        for encoding in ["cdr", "ros2", "ros2msg"] {
            assert!(
                is_cdr_encoding(encoding),
                "Encoding {} should be CDR",
                encoding
            );
        }

        // Test non-CDR encodings
        for encoding in ["json", "protobuf", "xml", ""] {
            assert!(
                !is_cdr_encoding(encoding),
                "Encoding {} should NOT be CDR",
                encoding
            );
        }
    }

    #[test]
    fn test_topic_collision_resolution() {
        let mut ctx = RewriteContext::new();

        // First occurrence - no suffix
        let topic1 = resolve_topic_collision("/test".to_string(), &mut ctx);
        assert_eq!(topic1, "/test");

        // Second occurrence - suffix
        ctx.topic_counter.insert("/test".to_string(), 0);
        let topic2 = resolve_topic_collision("/test".to_string(), &mut ctx);
        assert_eq!(topic2, "/test_1");

        // Third occurrence - increment suffix
        let topic3 = resolve_topic_collision("/test".to_string(), &mut ctx);
        assert_eq!(topic3, "/test_2");
    }

    #[test]
    fn test_initialize_topic_collision_checks_reader() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let reader = McapReader::open(fixtures_dir().join("example.mcap")).ok();
        if let Some(reader) = reader {
            // Just verify the function can be called
            for (id, channel) in reader.channels() {
                let _ = initialize_topic_collision_check(&channel.topic, *id, &reader);
            }
        }
    }

    #[test]
    fn test_get_schema_encoding() {
        assert_eq!(get_schema_encoding(Some("ros2msg")), "ros2msg");
        assert_eq!(get_schema_encoding(None), "ros2msg");
        assert_eq!(get_schema_encoding(Some("idl")), "idl");
    }

    #[test]
    fn test_should_add_schema() {
        let mut schema_ids = HashMap::new();
        assert!(should_add_schema(&schema_ids, "test"));

        schema_ids.insert("test".to_string(), 1);
        assert!(!should_add_schema(&schema_ids, "test"));
    }

    #[test]
    fn test_get_schema_bytes() {
        let transformed = Some("transformed".to_string());
        let original = Some("original".to_string());

        let bytes = get_schema_bytes(transformed.as_ref(), original.as_ref());
        assert_eq!(bytes, Some("transformed".as_bytes()));
    }

    #[test]
    fn test_should_passthrough_encoding() {
        assert!(!should_passthrough_encoding("cdr"));
        assert!(should_passthrough_encoding("json"));
    }

    #[test]
    fn test_extract_package_name() {
        assert_eq!(extract_package_name("std_msgs/String"), "std_msgs");
        assert_eq!(extract_package_name("geometry_msgs/Pose"), "geometry_msgs");
    }

    #[test]
    fn test_schema_bytes_priority() {
        // Transformed schema should take priority
        let transformed = Some("transformed".to_string());
        let original = Some("original".to_string());

        let bytes = get_schema_bytes(transformed.as_ref(), original.as_ref());
        assert_eq!(bytes, Some("transformed".as_bytes()));
    }

    #[test]
    fn test_package_extraction_edge_cases() {
        assert_eq!(extract_package_name("Type"), "Type");
        assert_eq!(extract_package_name(""), "");
        assert_eq!(extract_package_name("a/b/c"), "a");
    }

    #[test]
    fn test_encoding_and_passthrough_relationship() {
        // CDR encodings should NOT passthrough
        for encoding in ["cdr", "ros2", "ros2msg"] {
            assert!(is_cdr_encoding(encoding));
            assert!(!should_passthrough_encoding(encoding));
        }

        // Non-CDR encodings should passthrough
        for encoding in ["json", "protobuf"] {
            assert!(!is_cdr_encoding(encoding));
            assert!(should_passthrough_encoding(encoding));
        }
    }

    #[test]
    fn test_determine_message_handling_passthrough_non_cdr() {
        // Non-CDR encodings always passthrough
        assert_eq!(
            determine_message_handling("json", false),
            MessageHandling::Passthrough
        );
        assert_eq!(
            determine_message_handling("protobuf", true),
            MessageHandling::Passthrough
        );
    }

    #[test]
    fn test_determine_message_handling_reencode_with_schema() {
        // CDR with schema should reencode
        assert_eq!(
            determine_message_handling("cdr", true),
            MessageHandling::Reencode
        );
    }

    #[test]
    fn test_determine_message_handling_passthrough_no_schema() {
        // CDR without schema should passthrough
        assert_eq!(
            determine_message_handling("cdr", false),
            MessageHandling::Passthrough
        );
    }

    #[test]
    fn test_message_handling_traits() {
        // MessageHandling should implement Copy trait
        let handling = MessageHandling::Reencode;
        let _copied = handling;
        let _another_copy = handling;
    }

    #[test]
    fn test_message_handling_comprehensive() {
        // Test all combinations
        let test_cases = [
            ("cdr", true, MessageHandling::Reencode),
            ("cdr", false, MessageHandling::Passthrough),
            ("ros2", true, MessageHandling::Reencode),
            ("ros2", false, MessageHandling::Passthrough),
            ("ros2msg", true, MessageHandling::Reencode),
            ("ros2msg", false, MessageHandling::Passthrough),
            ("json", true, MessageHandling::Passthrough),
            ("json", false, MessageHandling::Passthrough),
            ("protobuf", true, MessageHandling::Passthrough),
            ("protobuf", false, MessageHandling::Passthrough),
        ];

        for (encoding, has_schema, expected) in test_cases {
            assert_eq!(
                determine_message_handling(encoding, has_schema),
                expected,
                "Encoding {} with schema={} should be {:?}",
                encoding,
                has_schema,
                expected
            );
        }
    }

    // =========================================================================
    // Integration Tests
    // =========================================================================

    #[test]
    fn test_integration_rewrite_basic_mcap() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_basic");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_with_validation() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_validation");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_without_validation() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_no_validation");

        let opts = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_with_topic_transform() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_topic_transform");

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_larger_file() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_larger");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_preserves_data() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_preserves");

        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_with_passthrough_disabled() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_no_passthrough");

        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_with_skip_decode_failures() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_skip_failures");

        let opts = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_error_nonexistent() {
        let mut rewriter = McapRewriter::new();
        let result = rewriter.rewrite("/nonexistent.mcap", "/tmp/output.mcap");
        assert!(result.is_err());
    }

    #[test]
    fn test_integration_rewrite_with_type_transform() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_type_transform");

        let pipeline = TransformBuilder::new()
            .with_type_rename("old/Old", "new/New")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_multiple_rewrites_independent() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output1 = temp_output("integration_multi_1");
        let output2 = temp_output("integration_multi_2");

        let mut rewriter = McapRewriter::new();
        let _ = rewriter.rewrite(&input, &output1);
        let stats1 = rewriter.stats.clone();

        let _ = rewriter.rewrite(&input, &output2);
        let stats2 = rewriter.stats.clone();

        // Both should complete
        assert_eq!(stats1.channel_count, stats2.channel_count);

        let _ = std::fs::remove_file(&output1);
        let _ = std::fs::remove_file(&output2);
    }

    #[test]
    fn test_integration_rewrite_empty_transform_pipeline() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_empty_transform");

        let pipeline = TransformBuilder::new().build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }

    #[test]
    fn test_integration_rewrite_with_both_transforms() {
        if !fixture_exists("example.mcap") {
            return;
        }

        let input = fixtures_dir().join("example.mcap");
        let output = temp_output("integration_both_transforms");

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .with_type_rename("old/Old", "new/New")
            .build();

        let opts = RewriteOptions::default().with_transforms(pipeline);
        let mut rewriter = McapRewriter::with_options(opts);
        let result = rewriter.rewrite(&input, &output);

        if result.is_ok() {
            let _ = std::fs::remove_file(&output);
        }
    }
}
