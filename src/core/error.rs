// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core error types for robocodec.
//!
//! Provides error types for format I/O operations:
//! - Schema and data parsing
//! - Buffer and decoding
//! - Encoding operations
//! - File format operations

use std::fmt;

/// Errors that can occur during format I/O operations.
#[derive(Debug, Clone)]
pub enum CodecError {
    /// Parse error in schema or data
    ParseError {
        /// What was being parsed
        context: String,
        /// Error message
        message: String,
    },

    /// Invalid schema format
    InvalidSchema {
        /// Schema name or identifier
        schema_name: String,
        /// Validation error message
        reason: String,
    },

    /// Type not found in registry
    TypeNotFound {
        /// Type name that was not found
        type_name: String,
    },

    /// Buffer too short for requested read
    BufferTooShort {
        /// Requested bytes
        requested: usize,
        /// Available bytes
        available: usize,
        /// Cursor position when error occurred
        cursor_pos: u64,
    },

    /// Invalid alignment
    AlignmentError {
        /// Expected alignment
        expected: u64,
        /// Actual position
        actual: u64,
    },

    /// Array or sequence length exceeded data bounds
    LengthExceeded {
        /// Length that was read
        length: usize,
        /// Position in buffer
        position: usize,
        /// Buffer length
        buffer_len: usize,
    },

    /// Field decode error with context
    FieldDecodeError {
        /// Field name
        field_name: String,
        /// Field type
        field_type: String,
        /// Cursor position when error occurred
        cursor_pos: u64,
        /// Underlying error
        cause: String,
    },

    /// Unsupported type or feature
    Unsupported {
        /// What is not supported
        feature: String,
    },

    /// Encoding/decoding error
    EncodeError {
        /// Codec context (e.g., "CDR", "Protobuf", "JSON")
        codec: String,
        /// Error message
        message: String,
    },

    /// Invariant violation (for unsafe block validation failures)
    InvariantViolation {
        /// Description of the invariant that was violated
        invariant: String,
    },

    /// Other error
    Other(String),
}

impl CodecError {
    /// Create a parse error.
    pub fn parse(context: impl Into<String>, message: impl Into<String>) -> Self {
        CodecError::ParseError {
            context: context.into(),
            message: message.into(),
        }
    }

    /// Create an invalid schema error.
    pub fn invalid_schema(schema_name: impl Into<String>, reason: impl Into<String>) -> Self {
        CodecError::InvalidSchema {
            schema_name: schema_name.into(),
            reason: reason.into(),
        }
    }

    /// Create a "type not found" error.
    pub fn type_not_found(type_name: impl Into<String>) -> Self {
        CodecError::TypeNotFound {
            type_name: type_name.into(),
        }
    }

    /// Create an encode/decode error.
    pub fn encode(codec: impl Into<String>, message: impl Into<String>) -> Self {
        CodecError::EncodeError {
            codec: codec.into(),
            message: message.into(),
        }
    }

    /// Create a buffer too short error.
    pub fn buffer_too_short(requested: usize, available: usize, cursor_pos: u64) -> Self {
        CodecError::BufferTooShort {
            requested,
            available,
            cursor_pos,
        }
    }

    /// Create an alignment error.
    pub fn alignment_error(expected: u64, actual: u64) -> Self {
        CodecError::AlignmentError { expected, actual }
    }

    /// Create a length exceeded error.
    pub fn length_exceeded(length: usize, position: usize, buffer_len: usize) -> Self {
        CodecError::LengthExceeded {
            length,
            position,
            buffer_len,
        }
    }

    /// Create an unsupported feature error.
    pub fn unsupported(feature: impl Into<String>) -> Self {
        CodecError::Unsupported {
            feature: feature.into(),
        }
    }

    /// Create an invariant violation error (for unsafe block validation).
    pub fn invariant_violation(invariant: impl Into<String>) -> Self {
        CodecError::InvariantViolation {
            invariant: invariant.into(),
        }
    }

    /// Create an "unknown codec" error.
    pub fn unknown_codec(encoding: impl Into<String>) -> Self {
        CodecError::Unsupported {
            feature: format!("unknown codec: {}", encoding.into()),
        }
    }

    /// Get structured fields for logging.
    pub fn log_fields(&self) -> Vec<(&'static str, String)> {
        match self {
            CodecError::ParseError { context, message } => {
                vec![("context", context.clone()), ("message", message.clone())]
            }
            CodecError::InvalidSchema {
                schema_name,
                reason,
            } => vec![("schema", schema_name.clone()), ("reason", reason.clone())],
            CodecError::TypeNotFound { type_name } => vec![("type", type_name.clone())],
            CodecError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => vec![
                ("requested", requested.to_string()),
                ("available", available.to_string()),
                ("cursor", cursor_pos.to_string()),
            ],
            CodecError::AlignmentError { expected, actual } => vec![
                ("expected", expected.to_string()),
                ("actual", actual.to_string()),
            ],
            CodecError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => vec![
                ("length", length.to_string()),
                ("position", position.to_string()),
                ("buffer_len", buffer_len.to_string()),
            ],
            CodecError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => vec![
                ("field", field_name.clone()),
                ("type", field_type.clone()),
                ("cursor", cursor_pos.to_string()),
                ("cause", cause.clone()),
            ],
            CodecError::Unsupported { feature } => vec![("feature", feature.clone())],
            CodecError::EncodeError { codec, message } => {
                vec![("codec", codec.clone()), ("message", message.clone())]
            }
            CodecError::InvariantViolation { invariant } => {
                vec![("invariant", invariant.clone())]
            }
            CodecError::Other(msg) => vec![("message", msg.clone())],
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::ParseError { context, message } => {
                write!(f, "Parse error in {context}: {message}")
            }
            CodecError::InvalidSchema {
                schema_name,
                reason,
            } => {
                write!(f, "Invalid schema '{schema_name}': {reason}")
            }
            CodecError::TypeNotFound { type_name } => {
                write!(f, "Type not found: '{type_name}'")
            }
            CodecError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => write!(
                f,
                "Buffer too short: requested {requested} bytes at position {cursor_pos}, but only {available} bytes available"
            ),
            CodecError::AlignmentError { expected, actual } => write!(
                f,
                "Alignment error: expected alignment of {expected}, but position is {actual}"
            ),
            CodecError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => write!(
                f,
                "Length {length} exceeds buffer at position {position} (buffer length: {buffer_len})"
            ),
            CodecError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => write!(
                f,
                "Failed to decode field '{field_name}' (type: '{field_type}', cursor_pos: {cursor_pos}): {cause}"
            ),
            CodecError::Unsupported { feature } => {
                write!(f, "Unsupported feature: '{feature}'")
            }
            CodecError::EncodeError { codec, message } => {
                write!(f, "{codec} encode error: {message}")
            }
            CodecError::InvariantViolation { invariant } => {
                write!(f, "Invariant violation: {invariant}")
            }
            CodecError::Other(msg) => write!(f, "Other error: {msg}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<std::io::Error> for CodecError {
    fn from(err: std::io::Error) -> Self {
        CodecError::EncodeError {
            codec: "IO".to_string(),
            message: err.to_string(),
        }
    }
}

/// Result type for robocodec operations.
pub type Result<T> = std::result::Result<T, CodecError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error() {
        let err = CodecError::parse("TestContext", "test error message");
        assert!(matches!(err, CodecError::ParseError { .. }));
        assert_eq!(
            err.to_string(),
            "Parse error in TestContext: test error message"
        );
    }

    #[test]
    fn test_invalid_schema_error() {
        let err = CodecError::invalid_schema("MySchema", "invalid field");
        assert!(matches!(err, CodecError::InvalidSchema { .. }));
        assert_eq!(err.to_string(), "Invalid schema 'MySchema': invalid field");
    }

    #[test]
    fn test_type_not_found_error() {
        let err = CodecError::type_not_found("UnknownType");
        assert!(matches!(err, CodecError::TypeNotFound { .. }));
        assert_eq!(err.to_string(), "Type not found: 'UnknownType'");
    }

    #[test]
    fn test_encode_error() {
        let err = CodecError::encode("CDR", "encoding failed");
        assert!(matches!(err, CodecError::EncodeError { .. }));
        assert_eq!(err.to_string(), "CDR encode error: encoding failed");
    }

    #[test]
    fn test_buffer_too_short_error() {
        let err = CodecError::buffer_too_short(100, 50, 10);
        assert!(matches!(err, CodecError::BufferTooShort { .. }));
        assert_eq!(
            err.to_string(),
            "Buffer too short: requested 100 bytes at position 10, but only 50 bytes available"
        );
    }

    #[test]
    fn test_alignment_error() {
        let err = CodecError::alignment_error(8, 5);
        assert!(matches!(err, CodecError::AlignmentError { .. }));
        assert_eq!(
            err.to_string(),
            "Alignment error: expected alignment of 8, but position is 5"
        );
    }

    #[test]
    fn test_length_exceeded_error() {
        let err = CodecError::length_exceeded(1000, 500, 800);
        assert!(matches!(err, CodecError::LengthExceeded { .. }));
        assert_eq!(
            err.to_string(),
            "Length 1000 exceeds buffer at position 500 (buffer length: 800)"
        );
    }

    #[test]
    fn test_unsupported_error() {
        let err = CodecError::unsupported("complex_feature");
        assert!(matches!(err, CodecError::Unsupported { .. }));
        assert_eq!(err.to_string(), "Unsupported feature: 'complex_feature'");
    }

    #[test]
    fn test_invariant_violation_error() {
        let err = CodecError::invariant_violation("buffer invariant");
        assert!(matches!(err, CodecError::InvariantViolation { .. }));
        assert_eq!(err.to_string(), "Invariant violation: buffer invariant");
    }

    #[test]
    fn test_unknown_codec_error() {
        let err = CodecError::unknown_codec("unknown_encoding");
        assert!(matches!(err, CodecError::Unsupported { .. }));
        assert_eq!(
            err.to_string(),
            "Unsupported feature: 'unknown codec: unknown_encoding'"
        );
    }

    #[test]
    fn test_other_error() {
        let err = CodecError::Other("something went wrong".to_string());
        assert!(matches!(err, CodecError::Other(_)));
        assert_eq!(err.to_string(), "Other error: something went wrong");
    }

    #[test]
    fn test_log_fields_parse_error() {
        let err = CodecError::parse("Context", "message");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "context");
        assert_eq!(fields[0].1, "Context");
        assert_eq!(fields[1].0, "message");
        assert_eq!(fields[1].1, "message");
    }

    #[test]
    fn test_log_fields_buffer_too_short() {
        let err = CodecError::buffer_too_short(100, 50, 10);
        let fields = err.log_fields();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].0, "requested");
        assert_eq!(fields[0].1, "100");
        assert_eq!(fields[1].0, "available");
        assert_eq!(fields[1].1, "50");
        assert_eq!(fields[2].0, "cursor");
        assert_eq!(fields[2].1, "10");
    }

    #[test]
    fn test_log_fields_alignment_error() {
        let err = CodecError::alignment_error(8, 5);
        let fields = err.log_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "expected");
        assert_eq!(fields[0].1, "8");
        assert_eq!(fields[1].0, "actual");
        assert_eq!(fields[1].1, "5");
    }

    #[test]
    fn test_log_fields_length_exceeded() {
        let err = CodecError::length_exceeded(1000, 500, 800);
        let fields = err.log_fields();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].0, "length");
        assert_eq!(fields[0].1, "1000");
        assert_eq!(fields[1].0, "position");
        assert_eq!(fields[1].1, "500");
        assert_eq!(fields[2].0, "buffer_len");
        assert_eq!(fields[2].1, "800");
    }

    #[test]
    fn test_log_fields_invalid_schema() {
        let err = CodecError::invalid_schema("MySchema", "reason");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "schema");
        assert_eq!(fields[0].1, "MySchema");
        assert_eq!(fields[1].0, "reason");
        assert_eq!(fields[1].1, "reason");
    }

    #[test]
    fn test_log_fields_type_not_found() {
        let err = CodecError::type_not_found("MyType");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "type");
        assert_eq!(fields[0].1, "MyType");
    }

    #[test]
    fn test_log_fields_unsupported() {
        let err = CodecError::unsupported("feature");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "feature");
        assert_eq!(fields[0].1, "feature");
    }

    #[test]
    fn test_log_fields_encode_error() {
        let err = CodecError::encode("Codec", "error");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "codec");
        assert_eq!(fields[0].1, "Codec");
        assert_eq!(fields[1].0, "message");
        assert_eq!(fields[1].1, "error");
    }

    #[test]
    fn test_log_fields_invariant_violation() {
        let err = CodecError::invariant_violation("test");
        let fields = err.log_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "invariant");
        assert_eq!(fields[0].1, "test");
    }

    #[test]
    fn test_log_fields_other() {
        let err = CodecError::Other("msg".to_string());
        let fields = err.log_fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "message");
        assert_eq!(fields[0].1, "msg");
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let codec_err: CodecError = io_err.into();
        assert!(matches!(codec_err, CodecError::EncodeError { .. }));
        assert_eq!(codec_err.to_string(), "IO encode error: file not found");
    }

    #[test]
    fn test_error_debug_format() {
        let err = CodecError::parse("Test", "message");
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("ParseError"));
    }

    #[test]
    fn test_field_decode_error() {
        let err = CodecError::FieldDecodeError {
            field_name: "my_field".to_string(),
            field_type: "int32".to_string(),
            cursor_pos: 42,
            cause: "invalid value".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to decode field 'my_field' (type: 'int32', cursor_pos: 42): invalid value"
        );
        let fields = err.log_fields();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].0, "field");
        assert_eq!(fields[0].1, "my_field");
        assert_eq!(fields[1].0, "type");
        assert_eq!(fields[1].1, "int32");
        assert_eq!(fields[2].0, "cursor");
        assert_eq!(fields[2].1, "42");
        assert_eq!(fields[3].0, "cause");
        assert_eq!(fields[3].1, "invalid value");
    }

    #[test]
    fn test_error_clone() {
        let err1 = CodecError::parse("Context", "message");
        let err2 = err1.clone();
        assert_eq!(err1.to_string(), err2.to_string());
    }
}
