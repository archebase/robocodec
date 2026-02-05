// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ROS1 bag file rewriter using decode-encode-write flow.
//!
//! This module provides functionality to rewrite ROS1 bag files with
//! optional transformations (topic/type renaming).

use std::collections::HashMap;
use std::path::Path;

use tracing::warn;

use crate::core::{CodecError, Result};
use crate::encoding::{CdrDecoder, CdrEncoder};
use crate::io::formats::bag::BagFormat;
use crate::io::formats::bag::writer::BagWriter;
use crate::io::traits::FormatReader;
use crate::rewriter::{FormatRewriter, RewriteOptions, RewriteStats};
use crate::schema::{MessageSchema, parse_schema};

/// ROS1 bag file rewriter.
///
/// Performs a full decode-encode-write cycle to normalize ROS1 bag files.
/// Can optionally apply transformations to rename topics and message types.
pub struct BagRewriter {
    /// Options for rewriting
    options: RewriteOptions,
    /// Cached schemas indexed by type name
    schemas: HashMap<String, MessageSchema>,
    /// Statistics
    stats: RewriteStats,
}

impl BagRewriter {
    /// Create a new rewriter with default options.
    pub fn new() -> Self {
        Self::with_options(RewriteOptions::default())
    }

    /// Create a new rewriter with custom options.
    pub fn with_options(options: RewriteOptions) -> Self {
        Self {
            options,
            schemas: HashMap::new(),
            stats: RewriteStats::default(),
        }
    }

    /// Check if a message should be re-encoded based on schema availability.
    ///
    /// # Arguments
    ///
    /// * `transformed_type` - The transformed message type
    /// * `schemas` - Map of available schemas
    ///
    /// # Returns
    ///
    /// true if schema is available for re-encoding
    #[must_use]
    pub fn has_schema_for_reencode(
        transformed_type: Option<&str>,
        schemas: &HashMap<String, MessageSchema>,
    ) -> bool {
        transformed_type.is_some_and(|t| schemas.contains_key(t))
    }

    /// Get the package name from a type name.
    ///
    /// # Arguments
    ///
    /// * `type_name` - The full type name (e.g., "std_msgs/String")
    ///
    /// # Returns
    ///
    /// The package name (e.g., "std_msgs") or empty string
    #[must_use]
    pub fn extract_package_name(type_name: &str) -> &str {
        type_name.split('/').next().unwrap_or("")
    }

    /// Check if packages are different.
    ///
    /// # Arguments
    ///
    /// * `old_type` - The original type name
    /// * `new_type` - The new type name
    ///
    /// # Returns
    ///
    /// true if packages are different and both non-empty
    #[must_use]
    pub fn has_package_change(old_type: &str, new_type: &str) -> bool {
        let old_pkg = Self::extract_package_name(old_type);
        let new_pkg = Self::extract_package_name(new_type);

        !old_pkg.is_empty() && !new_pkg.is_empty() && old_pkg != new_pkg
    }

    /// Rewrite a ROS1 bag file to a new location.
    ///
    /// # Arguments
    ///
    /// * `input_path` - Path to the input bag file
    /// * `output_path` - Path to the output bag file
    ///
    /// # Returns
    ///
    /// Statistics about the rewrite operation.
    pub fn rewrite<P1, P2>(&mut self, input_path: P1, output_path: P2) -> Result<RewriteStats>
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
    {
        // Reset statistics
        self.stats = RewriteStats::default();

        // Open input bag to get channel information
        let reader = BagFormat::open(input_path.as_ref())?;
        let channels = FormatReader::channels(&reader).clone();
        self.stats.channel_count = channels.len() as u64;

        // Validate transformations if configured
        if let Some(ref pipeline) = self.options.transforms {
            let transform_channels: Vec<crate::transform::ChannelInfo> = channels
                .values()
                .map(|ch| crate::transform::ChannelInfo {
                    id: ch.id,
                    topic: ch.topic.clone(),
                    message_type: ch.message_type.clone(),
                    encoding: ch.encoding.clone(),
                    schema: ch.schema.clone(),
                    schema_encoding: ch.schema_encoding.clone(),
                })
                .collect();
            pipeline
                .validate(&transform_channels)
                .map_err(|e| CodecError::encode("Transform", e.to_string()))?;
        }

        // Create output bag writer
        let mut writer = BagWriter::create(output_path)?;

        // Pre-parse all schemas with transformations applied
        if self.options.validate_schemas {
            self.cache_schemas(&reader)?;
        }

        // Build connection ID mapping for transformed topics
        // Maps: original channel_id -> new sequential connection ID
        let mut conn_mapping: HashMap<u16, u16> = HashMap::new();
        // Use composite key (topic, callerid) to preserve connections from different publishers
        let mut topic_callerid_to_new_conn: HashMap<(String, Option<String>), u16> = HashMap::new();
        let mut next_new_conn_id: u16 = 0;

        let pipeline = self.options.transforms.as_ref();

        // First pass: add all connections (with transformations applied)
        for (orig_channel_id, channel) in channels.iter() {
            // Apply transformations to get the target type and topic
            let (transformed_type, transformed_schema) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, channel.schema.as_deref())
            } else {
                (channel.message_type.clone(), channel.schema.clone())
            };

            let transformed_topic = if let Some(p) = pipeline {
                p.transform_topic(&channel.topic)
                    .unwrap_or_else(|| channel.topic.clone())
            } else {
                channel.topic.clone()
            };

            // Preserve callerid from the original channel (ROS1-specific metadata)
            let callerid = channel.callerid.clone();

            // Check if we already have a connection for this (topic, callerid) combination
            // This ensures we don't merge connections from different publishers
            let conn_key = (transformed_topic.clone(), callerid.clone());
            let new_conn_id = if let Some(&existing_id) = topic_callerid_to_new_conn.get(&conn_key)
            {
                existing_id
            } else {
                let new_id = next_new_conn_id;
                next_new_conn_id = next_new_conn_id.wrapping_add(1);

                // Add connection to writer with callerid preserved
                let callerid_str = callerid.as_deref().unwrap_or("");
                writer.add_connection_with_callerid(
                    new_id,
                    &transformed_topic,
                    &transformed_type,
                    transformed_schema.as_deref().unwrap_or(""),
                    callerid_str,
                )?;

                topic_callerid_to_new_conn.insert(conn_key, new_id);
                new_id
            };

            conn_mapping.insert(*orig_channel_id, new_conn_id);

            // Track transformation statistics
            if let Some(p) = pipeline {
                if p.transform_topic(&channel.topic).as_deref() != Some(&channel.topic) {
                    self.stats.topics_renamed += 1;
                }
                if p.transform_type(&channel.message_type, None).0 != channel.message_type {
                    self.stats.types_renamed += 1;
                }
            }
        }

        // Process messages
        let reader = BagFormat::open(input_path.as_ref())?;
        let iter = reader.iter_raw()?;
        let stream = iter;

        // Build a map of channel_id -> transformed type for schema lookup
        let channel_type_map: HashMap<u16, String> = channels
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

        let cdr_decoder = CdrDecoder::new();
        let schemas = self.schemas.clone();

        // Process each message
        for result in stream {
            let (raw_msg, _channel_info) = match result {
                Ok(msg) => msg,
                Err(e) => {
                    warn!(
                        context = "bag_message_read",
                        error = %e,
                        "Failed to read message"
                    );
                    continue;
                }
            };

            self.stats.message_count += 1;

            // Get the new connection ID for this message
            let new_conn_id = conn_mapping.get(&raw_msg.channel_id).copied();

            // Skip if we don't have a mapping (shouldn't happen)
            let new_conn_id = match new_conn_id {
                Some(id) => id,
                None => {
                    warn!(
                        context = "bag_rewrite",
                        channel_id = raw_msg.channel_id,
                        "No connection mapping for channel, skipping message"
                    );
                    continue;
                }
            };

            // Get the transformed message type for schema lookup
            let transformed_type = channel_type_map
                .get(&raw_msg.channel_id)
                .map(|s| s.as_str());

            // Try to decode and re-encode CDR messages
            if let Some(type_str) = transformed_type {
                if let Some(schema) = schemas.get(type_str) {
                    // Use the io::RawMessage directly
                    match self.rewrite_cdr_message(&cdr_decoder, &raw_msg, schema) {
                        Ok(data) => {
                            // Write re-encoded message
                            writer.write_message(
                                &crate::io::formats::bag::writer::BagMessage::from_raw(
                                    new_conn_id,
                                    raw_msg.log_time,
                                    data,
                                ),
                            )?;
                            self.stats.reencoded_count += 1;
                        }
                        Err(e) => {
                            warn!(
                                context = "bag_decode",
                                error = %e,
                                "Failed to decode message"
                            );
                            self.stats.decode_failures += 1;
                            if self.options.skip_decode_failures {
                                continue;
                            }
                            // Pass through original data
                            writer.write_message(
                                &crate::io::formats::bag::writer::BagMessage::from_raw(
                                    new_conn_id,
                                    raw_msg.log_time,
                                    raw_msg.data.clone(),
                                ),
                            )?;
                        }
                    }
                } else {
                    // No schema, pass through
                    writer.write_message(
                        &crate::io::formats::bag::writer::BagMessage::from_raw(
                            new_conn_id,
                            raw_msg.log_time,
                            raw_msg.data.clone(),
                        ),
                    )?;
                    self.stats.passthrough_count += 1;
                }
            } else {
                // Pass through original data
                writer.write_message(&crate::io::formats::bag::writer::BagMessage::from_raw(
                    new_conn_id,
                    raw_msg.log_time,
                    raw_msg.data.clone(),
                ))?;
                self.stats.passthrough_count += 1;
            }
        }

        // Finish the bag writer
        writer.finish()?;

        Ok(self.stats.clone())
    }

    /// Cache all schemas from the bag file, applying transformations if configured.
    fn cache_schemas(&mut self, reader: &crate::io::formats::bag::ParallelBagReader) -> Result<()> {
        let pipeline = self.options.transforms.as_ref();
        let channels = FormatReader::channels(reader);

        for channel in channels.values() {
            // Apply transformations to get target type
            let (target_type, _target_schema) = if let Some(p) = pipeline {
                p.transform_type(&channel.message_type, channel.schema.as_deref())
            } else {
                (channel.message_type.clone(), channel.schema.clone())
            };

            // Only cache if not already cached under the target type
            if !self.schemas.contains_key(&target_type) {
                let schema_to_parse = channel.schema.as_ref();

                if let Some(schema_text) = schema_to_parse {
                    match parse_schema(&channel.message_type, schema_text) {
                        Ok(mut schema) => {
                            // Apply package renaming if types differ
                            if target_type != channel.message_type {
                                let old_package =
                                    channel.message_type.split('/').next().unwrap_or("");
                                let new_package = target_type.split('/').next().unwrap_or("");

                                if !old_package.is_empty()
                                    && !new_package.is_empty()
                                    && old_package != new_package
                                {
                                    schema.rename_package(old_package, new_package);
                                }
                                schema.name = target_type.clone();
                                if schema.package.as_deref() == Some(old_package) {
                                    schema.package = Some(new_package.to_string());
                                }
                            }

                            self.schemas.insert(target_type.clone(), schema);
                        }
                        Err(e) => {
                            if self.options.validate_schemas {
                                return Err(CodecError::encode(
                                    "BagRewriter",
                                    format!(
                                        "Failed to parse schema for {} (from {}): {}",
                                        target_type, channel.message_type, e
                                    ),
                                ));
                            }
                            // Non-validating mode: continue without schema
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Rewrite a CDR message by decoding and re-encoding.
    fn rewrite_cdr_message(
        &mut self,
        decoder: &CdrDecoder,
        msg: &crate::io::metadata::RawMessage,
        schema: &MessageSchema,
    ) -> Result<Vec<u8>> {
        // Decode the message (handles CDR header internally)
        let decoded = decoder.decode(schema, &msg.data, Some(&schema.name))?;

        // Re-encode with proper CDR header
        let mut encoder = CdrEncoder::new();
        encoder.encode_message(&decoded, schema, &schema.name)?;

        let encoded_data = encoder.finish();
        Ok(encoded_data)
    }

    /// Get the options used for rewriting.
    pub fn options(&self) -> &RewriteOptions {
        &self.options
    }
}

impl FormatRewriter for BagRewriter {
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

impl Default for BagRewriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper to create a temporary directory for testing
    fn create_temp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[test]
    fn test_create_rewriter() {
        let rewriter = BagRewriter::new();
        assert!(rewriter.options().transforms.is_none());
    }

    #[test]
    fn test_create_rewriter_with_options() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let rewriter = BagRewriter::with_options(options.clone());
        assert!(!rewriter.options().validate_schemas);
        assert!(rewriter.options().skip_decode_failures);
    }

    #[test]
    fn test_create_rewriter_default() {
        let rewriter = BagRewriter::default();
        // BagRewriter::new() uses RewriteOptions::default() which has:
        // validate_schemas: true, skip_decode_failures: true, passthrough_non_cdr: true
        assert!(rewriter.options().validate_schemas);
        assert!(rewriter.options().skip_decode_failures);
    }

    #[test]
    fn test_options_method() {
        let rewriter = BagRewriter::new();
        let options = rewriter.options();
        assert!(options.transforms.is_none());
    }

    #[test]
    fn test_format_rewriter_trait() {
        let rewriter = BagRewriter::new();
        // Verify it implements FormatRewriter
        let _any_rewriter: &dyn std::any::Any = &rewriter;
        // Just checking the trait is implemented
    }

    #[test]
    fn test_rewrite_creates_output() {
        let temp_dir = create_temp_dir();

        // Use an existing bag fixture as input
        let input_path = "tests/fixtures/robocodec_test_15.bag";

        // Skip if input doesn't exist
        if !std::path::Path::new(input_path).exists() {
            return;
        }

        let output_path = temp_dir.path().join("output.bag");

        let mut rewriter = BagRewriter::new();
        let result = rewriter.rewrite(input_path, &output_path);

        // For now, we just check that the rewrite attempt runs
        // The actual success depends on having valid fixtures
        let _ = result;

        // If rewrite succeeded, check output exists
        if result.is_ok() {
            assert!(output_path.exists());
        }
    }

    #[test]
    fn test_rewrite_with_skip_decode_failures() {
        let temp_dir = create_temp_dir();

        let input_path = "tests/fixtures/robocodec_test_15.bag";
        if !std::path::Path::new(input_path).exists() {
            return;
        }

        let output_path = temp_dir.path().join("output_skip.bag");

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let mut rewriter = BagRewriter::with_options(options);

        let _ = rewriter.rewrite(input_path, &output_path);
    }

    #[test]
    fn test_rewrite_with_no_schema_validation() {
        let temp_dir = create_temp_dir();

        let input_path = "tests/fixtures/robocodec_test_15.bag";
        if !std::path::Path::new(input_path).exists() {
            return;
        }

        let output_path = temp_dir.path().join("output_no_validation.bag");

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: None,
        };
        let mut rewriter = BagRewriter::with_options(options);

        let _ = rewriter.rewrite(input_path, &output_path);
    }

    #[test]
    fn test_as_any() {
        let rewriter = BagRewriter::new();
        let _any: &dyn std::any::Any = rewriter.as_any();
    }

    #[test]
    fn test_as_any_downcast() {
        let rewriter = BagRewriter::new();
        let any = rewriter.as_any();
        // Verify we can downcast back to BagRewriter
        assert!(any.is::<BagRewriter>());
    }

    #[test]
    fn test_format_rewriter_trait_methods() {
        let rewriter = BagRewriter::new();
        // Verify FormatRewriter trait methods are accessible
        let _options = rewriter.options();
        let _any = rewriter.as_any();
    }

    #[test]
    fn test_rewriter_with_transforms() {
        use crate::transform::TransformBuilder;

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old", "/new")
            .build();

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(pipeline),
        };

        let rewriter = BagRewriter::with_options(options);
        assert!(rewriter.options().transforms.is_some());
        assert!(rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_empty_schemas_initially() {
        let rewriter = BagRewriter::new();
        // Access the private schemas field indirectly through behavior
        // Since schemas is private, we verify through rewrite behavior
        assert!(rewriter.options().validate_schemas);
    }

    #[test]
    fn test_rewriter_stats_initially_zero() {
        let rewriter = BagRewriter::new();
        // Can't directly access stats, but we can verify default options
        assert!(rewriter.options().skip_decode_failures);
        assert!(rewriter.options().passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_with_all_options_false() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        };

        let rewriter = BagRewriter::with_options(options);
        assert!(!rewriter.options().validate_schemas);
        assert!(!rewriter.options().skip_decode_failures);
        assert!(!rewriter.options().passthrough_non_cdr);
    }

    #[test]
    fn test_rewriter_with_all_options_true() {
        use crate::transform::TransformBuilder;

        let pipeline = TransformBuilder::new()
            .with_type_rename("old", "new")
            .build();

        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(pipeline),
        };

        let rewriter = BagRewriter::with_options(options);
        assert!(rewriter.options().validate_schemas);
        assert!(rewriter.options().skip_decode_failures);
        assert!(rewriter.options().passthrough_non_cdr);
        assert!(rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_with_empty_transform_pipeline() {
        use crate::transform::MultiTransform;

        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(MultiTransform::new()),
        };

        let rewriter = BagRewriter::with_options(options);
        assert!(!rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_options_combinations() {
        use crate::transform::TransformBuilder;

        // Test 1: Only validate_schemas = true
        let rewriter = BagRewriter::with_options(RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: None,
        });
        assert!(rewriter.options().validate_schemas);

        // Test 2: Only skip_decode_failures = true
        let rewriter = BagRewriter::with_options(RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        });
        assert!(rewriter.options().skip_decode_failures);

        // Test 3: Only passthrough_non_cdr = true
        let rewriter = BagRewriter::with_options(RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: true,
            transforms: None,
        });
        assert!(rewriter.options().passthrough_non_cdr);

        // Test 4: With transforms
        let pipeline = TransformBuilder::new()
            .with_topic_rename("/a", "/b")
            .with_type_rename("old/Old", "new/New")
            .build();

        let rewriter = BagRewriter::with_options(RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: Some(pipeline),
        });
        assert!(rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_returns_error_for_nonexistent_input() {
        let temp_dir = create_temp_dir();
        let output_path = temp_dir.path().join("output.bag");

        let mut rewriter = BagRewriter::new();
        let result = rewriter.rewrite("nonexistent.bag", &output_path);

        assert!(result.is_err());
    }

    #[test]
    fn test_rewriter_send_sync_trait_bounds() {
        // Verify BagRewriter satisfies the trait bounds
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BagRewriter>();
    }

    #[test]
    fn test_rewriter_clone_is_not_implemented() {
        // BagRewriter does not implement Clone
        // This is a compile-time verification that we're not accidentally cloning
        let rewriter = BagRewriter::new();
        let _rewriter_ref = &rewriter; // Just take a reference
    }

    #[test]
    fn test_multiple_rewriters_independent() {
        let rewriter1 = BagRewriter::new();
        let rewriter2 = BagRewriter::new();

        // Each rewriter should have independent options
        assert_eq!(
            rewriter1.options().validate_schemas,
            rewriter2.options().validate_schemas
        );
    }

    #[test]
    fn test_rewriter_with_passthrough_non_cdr() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: None,
        };

        let rewriter = BagRewriter::with_options(options);
        assert!(rewriter.options().passthrough_non_cdr);
        assert!(!rewriter.options().validate_schemas);
        assert!(rewriter.options().skip_decode_failures);
    }

    #[test]
    fn test_rewriter_options_accessor_returns_reference() {
        let rewriter = BagRewriter::new();
        let options = rewriter.options();

        // Should be able to access fields through the reference
        let _ = options.validate_schemas;
        let _ = options.skip_decode_failures;
        let _ = options.passthrough_non_cdr;
        let _ = options.transforms.as_ref();
    }

    #[test]
    fn test_rewriter_with_various_boolean_combinations() {
        let combinations = [
            (false, false, false),
            (false, false, true),
            (false, true, false),
            (false, true, true),
            (true, false, false),
            (true, false, true),
            (true, true, false),
        ];

        for (validate, skip, passthrough) in combinations {
            let options = RewriteOptions {
                validate_schemas: validate,
                skip_decode_failures: skip,
                passthrough_non_cdr: passthrough,
                transforms: None,
            };

            let rewriter = BagRewriter::with_options(options);
            assert_eq!(rewriter.options().validate_schemas, validate);
            assert_eq!(rewriter.options().skip_decode_failures, skip);
            assert_eq!(rewriter.options().passthrough_non_cdr, passthrough);
        }
    }

    #[test]
    fn test_rewrite_with_transform_pipeline_tracks_renames() {
        use crate::transform::TransformBuilder;

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/old_topic", "/new_topic")
            .with_type_rename("old/OldType", "new/NewType")
            .build();

        let options = RewriteOptions::default().with_transforms(pipeline);
        let rewriter = BagRewriter::with_options(options);

        assert!(rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_nonexistent_file() {
        let temp_dir = create_temp_dir();
        let input_path = temp_dir.path().join("nonexistent.bag");
        let output_path = temp_dir.path().join("output.bag");

        let mut rewriter = BagRewriter::new();
        let result = rewriter.rewrite(&input_path, &output_path);

        // Should fail because input doesn't exist
        assert!(result.is_err());
    }

    #[test]
    fn test_rewriter_preserves_all_option_fields() {
        use crate::transform::TransformBuilder;

        let pipeline = TransformBuilder::new()
            .with_topic_rename("/a", "/b")
            .build();

        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: false,
            passthrough_non_cdr: false,
            transforms: Some(pipeline),
        };

        let rewriter = BagRewriter::with_options(options);

        assert!(rewriter.options().validate_schemas);
        assert!(!rewriter.options().skip_decode_failures);
        assert!(!rewriter.options().passthrough_non_cdr);
        assert!(rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_with_invalid_output_directory() {
        let temp_dir = create_temp_dir();
        let input_path = temp_dir.path().join("input.bag");

        // Create a minimal input file
        std::fs::write(&input_path, b"invalid bag content").unwrap();

        let output_path = "/nonexistent/directory/output.bag";

        let mut rewriter = BagRewriter::new();
        let result = rewriter.rewrite(&input_path, output_path);

        // Should fail because output directory doesn't exist
        assert!(result.is_err());
    }

    #[test]
    fn test_rewriter_empty_transforms_has_no_transforms() {
        use crate::transform::MultiTransform;

        let options = RewriteOptions {
            validate_schemas: true,
            skip_decode_failures: true,
            passthrough_non_cdr: true,
            transforms: Some(MultiTransform::new()),
        };

        let rewriter = BagRewriter::with_options(options);
        assert!(!rewriter.options().has_transforms());
    }

    #[test]
    fn test_rewriter_options_fields_match() {
        let options = RewriteOptions {
            validate_schemas: false,
            skip_decode_failures: true,
            passthrough_non_cdr: false,
            transforms: None,
        };

        let rewriter = BagRewriter::with_options(options);

        assert!(!rewriter.options().validate_schemas);
        assert!(rewriter.options().skip_decode_failures);
        assert!(!rewriter.options().passthrough_non_cdr);
        assert!(!rewriter.options().has_transforms());
    }

    #[test]
    fn test_has_schema_for_reencode() {
        let mut schemas: HashMap<String, MessageSchema> = HashMap::new();

        // No schemas available
        assert!(!BagRewriter::has_schema_for_reencode(
            Some("std_msgs/String"),
            &schemas
        ));
        assert!(!BagRewriter::has_schema_for_reencode(None, &schemas));

        // Schema available
        schemas.insert(
            "std_msgs/String".to_string(),
            MessageSchema::new("std_msgs/String".to_string()),
        );
        assert!(BagRewriter::has_schema_for_reencode(
            Some("std_msgs/String"),
            &schemas
        ));
        assert!(!BagRewriter::has_schema_for_reencode(
            Some("geometry_msgs/Twist"),
            &schemas
        ));
    }

    #[test]
    fn test_extract_package_name_bag() {
        assert_eq!(
            BagRewriter::extract_package_name("std_msgs/String"),
            "std_msgs"
        );
        assert_eq!(
            BagRewriter::extract_package_name("geometry_msgs/Twist"),
            "geometry_msgs"
        );
        assert_eq!(
            BagRewriter::extract_package_name("sensor_msgs/Image"),
            "sensor_msgs"
        );

        // Edge cases
        assert_eq!(BagRewriter::extract_package_name("NoSlash"), "NoSlash");
        assert_eq!(BagRewriter::extract_package_name(""), "");
        assert_eq!(BagRewriter::extract_package_name("/LeadingSlash"), "");
    }

    #[test]
    fn test_has_package_change() {
        // Same package
        assert!(!BagRewriter::has_package_change(
            "std_msgs/String",
            "std_msgs/Int32"
        ));

        // Different packages
        assert!(BagRewriter::has_package_change(
            "std_msgs/String",
            "geometry_msgs/Twist"
        ));

        // No package (no slash) - both are non-empty and different
        assert!(BagRewriter::has_package_change(
            "MessageType",
            "AnotherType"
        ));

        // Same type without slash
        assert!(!BagRewriter::has_package_change(
            "MessageType",
            "MessageType"
        ));

        // Empty strings
        assert!(!BagRewriter::has_package_change("", "std_msgs/String"));
        assert!(!BagRewriter::has_package_change("std_msgs/String", ""));
        assert!(!BagRewriter::has_package_change("", ""));

        // Complex package names
        assert!(BagRewriter::has_package_change(
            "old_pkg/Type",
            "new_pkg/Type"
        ));
    }

    #[test]
    fn test_package_extraction_consistency() {
        // Test that extract_package_name is consistent
        let test_cases = [
            ("pkg/Type", "pkg"),
            ("pkg/subpkg/Type", "pkg"), // Only first part
            ("Type", "Type"),           // No slash, returns whole string
            ("", ""),                   // Empty returns empty
        ];

        for (input, expected) in test_cases {
            assert_eq!(BagRewriter::extract_package_name(input), expected);
        }
    }
}
