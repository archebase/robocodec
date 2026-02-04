// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Error types for S3 streaming operations.

use std::fmt;

/// Errors that can occur during S3 streaming operations.
///
/// These are divided into recoverable errors (which allow the stream to continue)
/// and fatal errors (which abort the stream).
#[derive(Debug, Clone)]
pub enum S3Error {
    /// Recoverable errors that allow streaming to continue
    Recoverable(RecoverableError),
    /// Fatal errors that require aborting the stream
    Fatal(FatalError),
}

impl S3Error {
    /// Check if this error is recoverable.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, S3Error::Recoverable(_))
    }

    /// Check if this error is fatal.
    pub fn is_fatal(&self) -> bool {
        matches!(self, S3Error::Fatal(_))
    }

    /// Get a description of the error context.
    pub fn context(&self) -> &str {
        match self {
            S3Error::Recoverable(err) => err.context(),
            S3Error::Fatal(err) => err.context(),
        }
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::Recoverable(err) => write!(f, "{}", err),
            S3Error::Fatal(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for S3Error {}

/// Recoverable errors that allow streaming to continue.
///
/// These errors represent issues with individual messages or records
/// that can be skipped without affecting the rest of the stream.
#[derive(Debug, Clone)]
pub enum RecoverableError {
    /// Corrupted message - skip and continue
    MessageCorruption {
        /// Byte offset in the stream where corruption occurred
        offset: u64,
        /// Description of the corruption
        error: String,
    },

    /// Unknown channel ID - skip message
    UnknownChannel {
        /// The unknown channel ID
        channel_id: u16,
    },

    /// Parse error - skip record
    ParseError {
        /// Type of record being parsed
        record_type: String,
        /// Parse error message
        error: String,
    },
}

impl RecoverableError {
    /// Get the error context.
    pub fn context(&self) -> &str {
        match self {
            RecoverableError::MessageCorruption { .. } => "message corruption",
            RecoverableError::UnknownChannel { .. } => "unknown channel",
            RecoverableError::ParseError { .. } => "parse error",
        }
    }

    /// Create a message corruption error.
    pub fn message_corruption(offset: u64, error: impl Into<String>) -> Self {
        RecoverableError::MessageCorruption {
            offset,
            error: error.into(),
        }
    }

    /// Create an unknown channel error.
    pub fn unknown_channel(channel_id: u16) -> Self {
        RecoverableError::UnknownChannel { channel_id }
    }

    /// Create a parse error.
    pub fn parse_error(record_type: impl Into<String>, error: impl Into<String>) -> Self {
        RecoverableError::ParseError {
            record_type: record_type.into(),
            error: error.into(),
        }
    }
}

impl fmt::Display for RecoverableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoverableError::MessageCorruption { offset, error } => {
                write!(f, "Message corruption at offset {}: {}", offset, error)
            }
            RecoverableError::UnknownChannel { channel_id } => {
                write!(f, "Unknown channel: {}", channel_id)
            }
            RecoverableError::ParseError { record_type, error } => {
                write!(f, "Parse error in {} record: {}", record_type, error)
            }
        }
    }
}

/// Fatal errors that require aborting the stream.
///
/// These errors represent conditions that prevent further processing
/// of the stream.
#[derive(Debug, Clone)]
pub enum FatalError {
    /// S3 access denied
    AccessDenied {
        /// Bucket name
        bucket: String,
        /// Object key
        key: String,
        /// Additional error details
        details: String,
    },

    /// Object not found
    ObjectNotFound {
        /// Bucket name
        bucket: String,
        /// Object key
        key: String,
    },

    /// Invalid file format
    InvalidFormat {
        /// Expected magic bytes or header
        expected: &'static str,
        /// Actual bytes found
        found: Vec<u8>,
    },

    /// Memory limit exceeded
    MemoryLimitExceeded {
        /// Requested allocation size
        requested: usize,
        /// Configured limit
        limit: usize,
    },

    /// HTTP/network error
    HttpError {
        /// HTTP status code (if applicable)
        status: Option<u16>,
        /// Error message
        message: String,
    },

    /// IO error during streaming
    IoError {
        /// Error message
        message: String,
    },

    /// Configuration error
    ConfigError {
        /// Configuration issue description
        message: String,
    },

    /// AWS credentials error
    CredentialsError {
        /// Error details
        message: String,
    },
}

impl FatalError {
    /// Get the error context.
    pub fn context(&self) -> &str {
        match self {
            FatalError::AccessDenied { .. } => "access denied",
            FatalError::ObjectNotFound { .. } => "object not found",
            FatalError::InvalidFormat { .. } => "invalid format",
            FatalError::MemoryLimitExceeded { .. } => "memory limit exceeded",
            FatalError::HttpError { .. } => "HTTP error",
            FatalError::IoError { .. } => "IO error",
            FatalError::ConfigError { .. } => "configuration error",
            FatalError::CredentialsError { .. } => "credentials error",
        }
    }

    /// Create an access denied error.
    pub fn access_denied(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        FatalError::AccessDenied {
            bucket: bucket.into(),
            key: key.into(),
            details: String::new(),
        }
    }

    /// Create an object not found error.
    pub fn object_not_found(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        FatalError::ObjectNotFound {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// Create an invalid format error.
    pub fn invalid_format(expected: &'static str, found: Vec<u8>) -> Self {
        FatalError::InvalidFormat { expected, found }
    }

    /// Create a memory limit exceeded error.
    pub fn memory_limit_exceeded(requested: usize, limit: usize) -> Self {
        FatalError::MemoryLimitExceeded { requested, limit }
    }

    /// Create an HTTP error.
    pub fn http_error(status: Option<u16>, message: impl Into<String>) -> Self {
        FatalError::HttpError {
            status,
            message: message.into(),
        }
    }

    /// Create an IO error.
    pub fn io_error(message: impl Into<String>) -> Self {
        FatalError::IoError {
            message: message.into(),
        }
    }

    /// Create a configuration error.
    pub fn config_error(message: impl Into<String>) -> Self {
        FatalError::ConfigError {
            message: message.into(),
        }
    }

    /// Create a credentials error.
    pub fn credentials_error(message: impl Into<String>) -> Self {
        FatalError::CredentialsError {
            message: message.into(),
        }
    }
}

impl std::error::Error for FatalError {}

impl fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FatalError::AccessDenied {
                bucket,
                key,
                details,
            } => {
                if details.is_empty() {
                    write!(f, "Access denied to s3://{}/{}", bucket, key)
                } else {
                    write!(f, "Access denied to s3://{}/{}: {}", bucket, key, details)
                }
            }
            FatalError::ObjectNotFound { bucket, key } => {
                write!(f, "Object not found: s3://{}/{}", bucket, key)
            }
            FatalError::InvalidFormat { expected, found } => {
                let preview = if found.len() <= 8 {
                    format!("{:?}", found)
                } else {
                    format!("{:?}...", &found[..8])
                };
                write!(
                    f,
                    "Invalid format: expected {}, found {}",
                    expected, preview
                )
            }
            FatalError::MemoryLimitExceeded { requested, limit } => {
                write!(
                    f,
                    "Memory limit exceeded: requested {} bytes, limit is {} bytes",
                    requested, limit
                )
            }
            FatalError::HttpError { status, message } => {
                if let Some(code) = status {
                    write!(f, "HTTP error {}: {}", code, message)
                } else {
                    write!(f, "HTTP error: {}", message)
                }
            }
            FatalError::IoError { message } => {
                write!(f, "IO error: {}", message)
            }
            FatalError::ConfigError { message } => {
                write!(f, "Configuration error: {}", message)
            }
            FatalError::CredentialsError { message } => {
                write!(f, "AWS credentials error: {}", message)
            }
        }
    }
}

impl From<FatalError> for crate::CodecError {
    fn from(err: FatalError) -> Self {
        crate::CodecError::EncodeError {
            codec: "S3".to_string(),
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // S3Error tests
    // =========================================================================

    #[test]
    fn test_s3_error_recoverable() {
        let err = S3Error::Recoverable(RecoverableError::UnknownChannel { channel_id: 5 });
        assert!(err.is_recoverable());
        assert!(!err.is_fatal());
        assert_eq!(err.context(), "unknown channel");
    }

    #[test]
    fn test_s3_error_fatal() {
        let err = S3Error::Fatal(FatalError::object_not_found("bucket", "key"));
        assert!(err.is_fatal());
        assert!(!err.is_recoverable());
        assert_eq!(err.context(), "object not found");
    }

    #[test]
    fn test_s3_error_display_recoverable() {
        let err = S3Error::Recoverable(RecoverableError::UnknownChannel { channel_id: 42 });
        let display = format!("{}", err);
        assert!(display.contains("Unknown channel"));
        assert!(display.contains("42"));
    }

    #[test]
    fn test_s3_error_display_fatal() {
        let err = S3Error::Fatal(FatalError::object_not_found("my-bucket", "my-key"));
        let display = format!("{}", err);
        assert!(display.contains("Object not found"));
        assert!(display.contains("s3://my-bucket/my-key"));
    }

    #[test]
    fn test_s3_error_clone() {
        let err = S3Error::Fatal(FatalError::config_error("test"));
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    #[test]
    fn test_s3_error_as_error() {
        let err = S3Error::Fatal(FatalError::io_error("test"));
        let _err: &dyn std::error::Error = &err;
    }

    // =========================================================================
    // RecoverableError tests
    // =========================================================================

    #[test]
    fn test_recoverable_error_constructors() {
        let err = RecoverableError::message_corruption(1000, "data corrupted");
        assert!(matches!(err, RecoverableError::MessageCorruption { .. }));

        let err = RecoverableError::unknown_channel(42);
        assert!(matches!(err, RecoverableError::UnknownChannel { .. }));

        let err = RecoverableError::parse_error("Message", "invalid format");
        assert!(matches!(err, RecoverableError::ParseError { .. }));
    }

    #[test]
    fn test_recoverable_error_context_message_corruption() {
        let err = RecoverableError::message_corruption(500, "bad checksum");
        assert_eq!(err.context(), "message corruption");
    }

    #[test]
    fn test_recoverable_error_context_unknown_channel() {
        let err = RecoverableError::unknown_channel(99);
        assert_eq!(err.context(), "unknown channel");
    }

    #[test]
    fn test_recoverable_error_context_parse_error() {
        let err = RecoverableError::parse_error("Chunk", "invalid length");
        assert_eq!(err.context(), "parse error");
    }

    #[test]
    fn test_recoverable_error_display_message_corruption() {
        let err = RecoverableError::MessageCorruption {
            offset: 1024,
            error: "checksum failed".to_string(),
        };
        let display = format!("{}", err);
        assert!(display.contains("Message corruption"));
        assert!(display.contains("1024"));
        assert!(display.contains("checksum failed"));
    }

    #[test]
    fn test_recoverable_error_display_unknown_channel() {
        let err = RecoverableError::UnknownChannel { channel_id: 123 };
        assert_eq!(format!("{}", err), "Unknown channel: 123");
    }

    #[test]
    fn test_recoverable_error_display_parse_error() {
        let err = RecoverableError::ParseError {
            record_type: "Message".to_string(),
            error: "invalid header".to_string(),
        };
        let display = format!("{}", err);
        assert!(display.contains("Parse error"));
        assert!(display.contains("Message"));
        assert!(display.contains("invalid header"));
    }

    #[test]
    fn test_recoverable_error_clone() {
        let err = RecoverableError::parse_error("Test", "test error");
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    // =========================================================================
    // FatalError tests
    // =========================================================================

    #[test]
    fn test_fatal_error_constructors() {
        let err = FatalError::access_denied("bucket", "key");
        assert!(matches!(err, FatalError::AccessDenied { .. }));

        let err = FatalError::object_not_found("bucket", "key");
        assert!(matches!(err, FatalError::ObjectNotFound { .. }));

        let err = FatalError::invalid_format("MCAP", vec![0x89, 0x4D, 0x43, 0x41]);
        assert!(matches!(err, FatalError::InvalidFormat { .. }));

        let err = FatalError::memory_limit_exceeded(20_000_000, 10_000_000);
        assert!(matches!(err, FatalError::MemoryLimitExceeded { .. }));

        let err = FatalError::http_error(Some(404), "not found");
        assert!(matches!(err, FatalError::HttpError { .. }));

        let err = FatalError::io_error("read failed");
        assert!(matches!(err, FatalError::IoError { .. }));

        let err = FatalError::config_error("invalid buffer size");
        assert!(matches!(err, FatalError::ConfigError { .. }));

        let err = FatalError::credentials_error("no credentials found");
        assert!(matches!(err, FatalError::CredentialsError { .. }));
    }

    #[test]
    fn test_fatal_error_context() {
        assert_eq!(
            FatalError::access_denied("b", "k").context(),
            "access denied"
        );
        assert_eq!(
            FatalError::object_not_found("b", "k").context(),
            "object not found"
        );
        assert_eq!(
            FatalError::invalid_format("", vec![]).context(),
            "invalid format"
        );
        assert_eq!(
            FatalError::memory_limit_exceeded(100, 50).context(),
            "memory limit exceeded"
        );
        assert_eq!(FatalError::http_error(None, "test").context(), "HTTP error");
        assert_eq!(FatalError::io_error("test").context(), "IO error");
        assert_eq!(
            FatalError::config_error("test").context(),
            "configuration error"
        );
        assert_eq!(
            FatalError::credentials_error("test").context(),
            "credentials error"
        );
    }

    #[test]
    fn test_fatal_error_display() {
        let err = FatalError::AccessDenied {
            bucket: "my-bucket".to_string(),
            key: "file.mcap".to_string(),
            details: "invalid credentials".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "Access denied to s3://my-bucket/file.mcap: invalid credentials"
        );

        let err = FatalError::ObjectNotFound {
            bucket: "my-bucket".to_string(),
            key: "file.mcap".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "Object not found: s3://my-bucket/file.mcap"
        );

        let err = FatalError::InvalidFormat {
            expected: "MCAP",
            found: vec![0x00, 0x01, 0x02],
        };
        assert_eq!(
            format!("{}", err),
            "Invalid format: expected MCAP, found [0, 1, 2]"
        );

        let err = FatalError::MemoryLimitExceeded {
            requested: 20_000_000,
            limit: 10_000_000,
        };
        assert_eq!(
            format!("{}", err),
            "Memory limit exceeded: requested 20000000 bytes, limit is 10000000 bytes"
        );

        let err = FatalError::HttpError {
            status: Some(404),
            message: "not found".to_string(),
        };
        assert_eq!(format!("{}", err), "HTTP error 404: not found");

        let err = FatalError::HttpError {
            status: None,
            message: "connection failed".to_string(),
        };
        assert_eq!(format!("{}", err), "HTTP error: connection failed");
    }

    #[test]
    fn test_fatal_error_display_access_denied_no_details() {
        let err = FatalError::AccessDenied {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            details: String::new(),
        };
        assert_eq!(format!("{}", err), "Access denied to s3://bucket/key");
    }

    #[test]
    fn test_fatal_error_display_io_error() {
        let err = FatalError::IoError {
            message: "failed to read".to_string(),
        };
        assert_eq!(format!("{}", err), "IO error: failed to read");
    }

    #[test]
    fn test_fatal_error_display_config_error() {
        let err = FatalError::ConfigError {
            message: "invalid endpoint".to_string(),
        };
        assert_eq!(format!("{}", err), "Configuration error: invalid endpoint");
    }

    #[test]
    fn test_fatal_error_display_credentials_error() {
        let err = FatalError::CredentialsError {
            message: "no credentials".to_string(),
        };
        assert_eq!(format!("{}", err), "AWS credentials error: no credentials");
    }

    #[test]
    fn test_fatal_error_display_invalid_format_long() {
        let err = FatalError::InvalidFormat {
            expected: "MCAP0",
            found: vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09],
        };
        let display = format!("{}", err);
        // Should truncate to 8 elements with "..." suffix
        assert!(display.contains("..."));
        assert!(display.contains("Invalid format"));
    }

    #[test]
    fn test_fatal_error_clone() {
        let err = FatalError::io_error("test");
        let cloned = err.clone();
        assert_eq!(format!("{}", err), format!("{}", cloned));
    }

    #[test]
    fn test_fatal_error_as_error() {
        let err = FatalError::io_error("test");
        let _err: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_fatal_error_into_codec_error() {
        let err = FatalError::object_not_found("bucket", "key");
        let codec_err: crate::CodecError = err.into();
        assert!(codec_err.to_string().contains("Object not found"));
        assert!(codec_err.to_string().contains("S3"));
    }

    #[test]
    fn test_fatal_error_debug() {
        let err = FatalError::io_error("test");
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("IoError"));
    }

    #[test]
    fn test_s3_error_debug() {
        let err = S3Error::Fatal(FatalError::io_error("test"));
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Fatal"));
    }

    #[test]
    fn test_recoverable_error_debug() {
        let err = RecoverableError::unknown_channel(42);
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("UnknownChannel"));
    }
}
