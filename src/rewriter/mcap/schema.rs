// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema caching and management for MCAP rewrite operations.

use crate::core::{CodecError, Result};
use crate::io::formats::mcap::reader::McapReader;
use crate::io::formats::mcap::writer::ParallelMcapWriter;
use crate::schema::{MessageSchema, parse_schema};
use std::collections::HashMap;

/// Build schema ID mappings for all channels in the MCAP file.
///
/// This function performs the first pass over channels to add all schemas
/// to the writer, applying type transformations if configured.
///
/// # Arguments
///
/// * `channels` - Map of channel ID to channel info
/// * `schema_ids` - Output map of type name to schema ID
/// * `writer` - MCAP writer to add schemas to
/// * `pipeline` - Optional transform pipeline
pub fn build_schema_mappings<W: std::io::Write + Send + Sync>(
    channels: &HashMap<u16, crate::io::metadata::ChannelInfo>,
    schema_ids: &mut HashMap<String, u16>,
    writer: &mut ParallelMcapWriter<W>,
    pipeline: Option<&crate::transform::MultiTransform>,
) -> Result<()> {
    for (_channel_id, channel) in channels.iter() {
        // Apply transformations to get the target type name and schema
        let (transformed_type, transformed_schema) = if let Some(p) = pipeline {
            p.transform_type(&channel.message_type, channel.schema.as_deref())
        } else {
            (channel.message_type.clone(), channel.schema.clone())
        };

        if !schema_ids.contains_key(&transformed_type) {
            let schema_bytes = transformed_schema
                .as_ref()
                .map(|s| s.as_bytes())
                .or_else(|| channel.schema.as_ref().map(|s| s.as_bytes()));

            if let Some(bytes) = schema_bytes {
                let schema_id = writer
                    .add_schema(
                        &transformed_type,
                        channel.schema_encoding.as_deref().unwrap_or("ros2msg"),
                        bytes,
                    )
                    .map_err(|e| {
                        CodecError::encode("MCAP", format!("Failed to add schema: {e}"))
                    })?;
                schema_ids.insert(transformed_type.clone(), schema_id);
            } else {
                schema_ids.insert(transformed_type.clone(), 0);
            }
        }
    }
    Ok(())
}

/// Cache all schemas from the MCAP file, applying transformations if configured.
///
/// # Arguments
///
/// * `reader` - The MCAP reader to read schemas from
/// * `schemas` - Output map to cache parsed schemas
/// * `pipeline` - Optional transform pipeline
/// * `validate_schemas` - Whether to validate schema parsing
pub fn cache_schemas(
    reader: &McapReader,
    schemas: &mut HashMap<String, MessageSchema>,
    pipeline: Option<&crate::transform::MultiTransform>,
    validate_schemas: bool,
) -> Result<()> {
    for channel in reader.channels().values() {
        // Apply transformations to get target type
        let (target_type, _target_schema) = if let Some(p) = pipeline {
            p.transform_type(&channel.message_type, channel.schema.as_deref())
        } else {
            (channel.message_type.clone(), channel.schema.clone())
        };

        // Only cache if not already cached under the target type
        if !schemas.contains_key(&target_type) {
            // Use original schema for parsing (before text transformation)
            let schema_to_parse = channel.schema.as_ref();

            if let Some(schema_text) = schema_to_parse {
                match parse_schema(&channel.message_type, schema_text) {
                    Ok(mut schema) => {
                        // Apply package renaming to the parsed schema's internal types
                        if target_type != channel.message_type {
                            // Extract package names from old and new type names
                            let old_package = channel.message_type.split('/').next().unwrap_or("");
                            let new_package = target_type.split('/').next().unwrap_or("");

                            // Only rename if packages differ
                            if !old_package.is_empty()
                                && !new_package.is_empty()
                                && old_package != new_package
                            {
                                schema.rename_package(old_package, new_package);
                            }

                            // Update the schema's main name
                            schema.name = target_type.clone();
                            if schema.package.as_deref() == Some(old_package) {
                                schema.package = Some(new_package.to_string());
                            }
                        }

                        schemas.insert(target_type.clone(), schema);
                    }
                    Err(e) => {
                        if validate_schemas {
                            return Err(CodecError::encode(
                                "MCAP",
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

/// Get the schema encoding with a default fallback.
///
/// # Arguments
///
/// * `encoding` - Optional schema encoding from channel
///
/// # Returns
///
/// The encoding string or "ros2msg" as default
#[must_use]
pub fn get_schema_encoding(encoding: Option<&str>) -> &str {
    encoding.unwrap_or("ros2msg")
}

/// Determine if a schema should be added (not already present).
///
/// # Arguments
///
/// * `schema_ids` - Map of already-added schema IDs
/// * `type_name` - The type name to check
///
/// # Returns
///
/// true if the schema should be added, false if already present
#[must_use]
pub fn should_add_schema(schema_ids: &HashMap<String, u16>, type_name: &str) -> bool {
    !schema_ids.contains_key(type_name)
}

/// Get schema bytes with fallback to original schema.
///
/// # Arguments
///
/// * `transformed_schema` - The transformed schema (may be None)
/// * `original_schema` - The original schema (may be None)
///
/// # Returns
///
/// Some bytes if either schema is present, None otherwise
#[must_use]
pub fn get_schema_bytes<'a>(
    transformed_schema: Option<&'a String>,
    original_schema: Option<&'a String>,
) -> Option<&'a [u8]> {
    transformed_schema
        .map(|s| s.as_bytes())
        .or_else(|| original_schema.map(|s| s.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_add_schema_new() {
        let schema_ids = HashMap::new();
        assert!(should_add_schema(&schema_ids, "std_msgs/String"));
    }

    #[test]
    fn test_should_add_schema_existing() {
        let mut schema_ids = HashMap::new();
        schema_ids.insert("std_msgs/String".to_string(), 1);
        assert!(!should_add_schema(&schema_ids, "std_msgs/String"));
    }

    #[test]
    fn test_should_add_schema_different_type() {
        let mut schema_ids = HashMap::new();
        schema_ids.insert("std_msgs/String".to_string(), 1);
        assert!(should_add_schema(&schema_ids, "geometry_msgs/Pose"));
    }

    #[test]
    fn test_get_schema_encoding_some() {
        assert_eq!(get_schema_encoding(Some("ros2msg")), "ros2msg");
        assert_eq!(get_schema_encoding(Some("cdr")), "cdr");
    }

    #[test]
    fn test_get_schema_encoding_none() {
        assert_eq!(get_schema_encoding(None), "ros2msg");
    }

    #[test]
    fn test_get_schema_bytes_transformed() {
        let transformed = Some("transformed_schema".to_string());
        let original = Some("original_schema".to_string());

        let bytes = get_schema_bytes(transformed.as_ref(), original.as_ref());
        assert_eq!(bytes, Some("transformed_schema".as_bytes()));
    }

    #[test]
    fn test_get_schema_bytes_original_fallback() {
        let transformed = None;
        let original = Some("original_schema".to_string());

        let bytes = get_schema_bytes(transformed, original.as_ref());
        assert_eq!(bytes, Some("original_schema".as_bytes()));
    }

    #[test]
    fn test_get_schema_bytes_none() {
        let bytes = get_schema_bytes(None, None);
        assert_eq!(bytes, None);
    }

    #[test]
    fn test_get_schema_bytes_both_none() {
        let transformed: Option<&String> = None;
        let original: Option<&String> = None;

        let bytes = get_schema_bytes(transformed, original);
        assert!(bytes.is_none());
    }

    #[test]
    fn test_get_schema_bytes_empty_strings() {
        let transformed = Some("".to_string());
        let original = Some("original".to_string());

        // Transformed exists (even if empty), so it should be used
        let bytes = get_schema_bytes(transformed.as_ref(), original.as_ref());
        assert_eq!(bytes, Some("".as_bytes()));
    }

    #[test]
    fn test_get_schema_encoding_variants() {
        // Common ROS2 schema encodings
        assert_eq!(get_schema_encoding(Some("ros2msg")), "ros2msg");
        assert_eq!(get_schema_encoding(Some("ros2")), "ros2");
        assert_eq!(get_schema_encoding(Some("idl")), "idl");
    }

    #[test]
    fn test_should_add_schema_case_sensitive() {
        let mut schema_ids = HashMap::new();
        schema_ids.insert("std_msgs/String".to_string(), 1);

        // Different case should be treated as different type
        assert!(should_add_schema(&schema_ids, "std_msgs/string"));
        assert!(should_add_schema(&schema_ids, "STD_MSGS/String"));
    }

    #[test]
    fn test_should_add_schema_empty_map() {
        let schema_ids = HashMap::new();
        assert!(should_add_schema(&schema_ids, ""));
        assert!(should_add_schema(&schema_ids, "AnyType"));
    }
}
