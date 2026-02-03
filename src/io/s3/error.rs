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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_recoverable_error_constructors() {
        let err = RecoverableError::message_corruption(1000, "data corrupted");
        assert!(matches!(err, RecoverableError::MessageCorruption { .. }));

        let err = RecoverableError::unknown_channel(42);
        assert!(matches!(err, RecoverableError::UnknownChannel { .. }));

        let err = RecoverableError::parse_error("Message", "invalid format");
        assert!(matches!(err, RecoverableError::ParseError { .. }));
    }

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
}
