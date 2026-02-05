// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Message processing logic for MCAP rewrite operations.

use std::fs::File;
use std::io::BufWriter;

use tracing::warn;

use crate::core::{CodecError, Result};
use crate::encoding::{CdrDecoder, CdrEncoder};
use crate::io::formats::mcap::reader::RawMessage;
use crate::io::formats::mcap::writer::ParallelMcapWriter;
use crate::rewriter::RewriteOptions;
use crate::rewriter::RewriteStats;
use crate::schema::MessageSchema;

use super::context::MessageHandling;

/// Check if an encoding is CDR-based (requires CDR decoding/encoding).
///
/// # Arguments
///
/// * `encoding` - The encoding string from channel metadata
///
/// # Returns
///
/// true if the encoding is CDR-based, false otherwise
#[must_use]
pub fn is_cdr_encoding(encoding: &str) -> bool {
    matches!(encoding, "cdr" | "ros2" | "ros2msg")
}

/// Check if an encoding should be passed through without re-encoding.
///
/// # Arguments
///
/// * `encoding` - The message encoding
///
/// # Returns
///
/// true if the encoding is NOT a CDR-based encoding (should passthrough)
#[must_use]
pub fn should_passthrough_encoding(encoding: &str) -> bool {
    !is_cdr_encoding(encoding)
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

/// Determine how to handle a message based on encoding and schema availability.
///
/// # Arguments
///
/// * `encoding` - The message encoding
/// * `has_schema` - Whether a schema is available for re-encoding
///
/// # Returns
///
/// A [`MessageHandling`] indicating how the message should be processed
#[must_use]
pub fn determine_message_handling(encoding: &str, has_schema: bool) -> MessageHandling {
    if !is_cdr_encoding(encoding) {
        MessageHandling::Passthrough
    } else if has_schema {
        MessageHandling::Reencode
    } else {
        MessageHandling::Passthrough
    }
}

/// Rewrite a CDR message by decoding and re-encoding.
pub fn rewrite_cdr_message(
    mcap_writer: &mut ParallelMcapWriter<BufWriter<File>>,
    msg: &RawMessage,
    schema: &MessageSchema,
    channel_id: u16,
    topic: &str,
    options: &RewriteOptions,
    stats: &mut RewriteStats,
) -> Result<()> {
    // Decode the message (handles CDR header internally)
    let decoder = CdrDecoder::new();
    let decoded = match decoder.decode(schema, &msg.data, Some(&schema.name)) {
        Ok(d) => d,
        Err(e) => {
            warn!(
                context = "cdr_decode",
                error = %e,
                schema = %schema.name,
                topic = %topic,
                "Failed to decode CDR message"
            );
            stats.decode_failures += 1;
            if options.skip_decode_failures {
                // Skip this message entirely (message will be lost)
                return Ok(());
            }
            // Pass through original data on decode failure
            write_message_raw(mcap_writer, msg, channel_id)?;
            return Ok(());
        }
    };

    // Re-encode with proper CDR header
    let mut encoder = CdrEncoder::new();
    match encoder.encode_message(&decoded, schema, &schema.name) {
        Ok(()) => {}
        Err(e) => {
            warn!(
                context = "cdr_encode",
                error = %e,
                schema = %schema.name,
                topic = %topic,
                "Failed to encode CDR message (passing through original data)"
            );
            stats.encode_failures += 1;
            // Pass through original data on encode failure
            write_message_raw(mcap_writer, msg, channel_id)?;
            return Ok(());
        }
    }

    let encoded_data = encoder.finish();

    // Write the re-encoded message using custom writer
    mcap_writer
        .write_message(channel_id, msg.log_time, msg.publish_time, &encoded_data)
        .map_err(|e| CodecError::encode("MCAP", format!("Failed to write message: {e}")))?;

    stats.reencoded_count += 1;
    Ok(())
}

/// Write a raw message without re-encoding.
pub fn write_message_raw(
    mcap_writer: &mut ParallelMcapWriter<BufWriter<File>>,
    msg: &RawMessage,
    channel_id: u16,
) -> Result<()> {
    mcap_writer
        .write_message(channel_id, msg.log_time, msg.publish_time, &msg.data)
        .map_err(|e| CodecError::encode("MCAP", format!("Failed to write message: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_cdr_encoding() {
        assert!(is_cdr_encoding("cdr"));
        assert!(is_cdr_encoding("ros2"));
        assert!(is_cdr_encoding("ros2msg"));
        assert!(!is_cdr_encoding("json"));
        assert!(!is_cdr_encoding("protobuf"));
        assert!(!is_cdr_encoding(""));
    }

    #[test]
    fn test_should_passthrough_encoding() {
        assert!(!should_passthrough_encoding("cdr"));
        assert!(!should_passthrough_encoding("ros2"));
        assert!(!should_passthrough_encoding("ros2msg"));
        assert!(should_passthrough_encoding("json"));
        assert!(should_passthrough_encoding("protobuf"));
        assert!(should_passthrough_encoding(""));
    }

    #[test]
    fn test_extract_package_name() {
        assert_eq!(extract_package_name("std_msgs/String"), "std_msgs");
        assert_eq!(extract_package_name("geometry_msgs/Pose"), "geometry_msgs");
        assert_eq!(extract_package_name("MessageType"), "MessageType");
        assert_eq!(extract_package_name(""), "");
        assert_eq!(extract_package_name("a/b/c"), "a");
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
        assert_eq!(
            determine_message_handling("ros2", true),
            MessageHandling::Reencode
        );
        assert_eq!(
            determine_message_handling("ros2msg", true),
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
        assert_eq!(
            determine_message_handling("ros2", false),
            MessageHandling::Passthrough
        );
    }

    #[test]
    fn test_is_cdr_encoding_empty_string() {
        assert!(!is_cdr_encoding(""));
    }

    #[test]
    fn test_is_cdr_encoding_case_sensitive() {
        assert!(is_cdr_encoding("cdr"));
        assert!(!is_cdr_encoding("CDR"));
        assert!(!is_cdr_encoding("Cdr"));
    }

    #[test]
    fn test_extract_package_name_edge_cases() {
        assert_eq!(extract_package_name("a"), "a");
        assert_eq!(extract_package_name("/"), "");
        assert_eq!(extract_package_name("std_msgs/msg/String"), "std_msgs");
    }

    #[test]
    fn test_determine_message_handling_all_cdr_variants() {
        // All CDR variants with schema -> Reencode
        for encoding in ["cdr", "ros2", "ros2msg"] {
            assert_eq!(
                determine_message_handling(encoding, true),
                MessageHandling::Reencode,
                "Encoding {} with schema should reencode",
                encoding
            );
        }

        // All CDR variants without schema -> Passthrough
        for encoding in ["cdr", "ros2", "ros2msg"] {
            assert_eq!(
                determine_message_handling(encoding, false),
                MessageHandling::Passthrough,
                "Encoding {} without schema should passthrough",
                encoding
            );
        }
    }

    #[test]
    fn test_should_passthrough_encoding_all_variants() {
        // CDR variants should NOT passthrough
        for encoding in ["cdr", "ros2", "ros2msg"] {
            assert!(
                !should_passthrough_encoding(encoding),
                "Encoding {} should not passthrough",
                encoding
            );
        }

        // Non-CDR variants should passthrough
        for encoding in ["json", "protobuf", "xml", ""] {
            assert!(
                should_passthrough_encoding(encoding),
                "Encoding {} should passthrough",
                encoding
            );
        }
    }
}
