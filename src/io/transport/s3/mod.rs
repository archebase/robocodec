// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 transport implementation.
//!
//! This module provides S3-specific transport functionality using the AWS S3 protocol.
//! It supports S3-compatible services like AWS S3, `MinIO`, Cloudflare R2, etc.

mod transport;

// Re-export from the s3 module (public API)
pub use crate::io::s3::{
    AwsCredentials, FatalError, RecoverableError, RetryConfig, S3Client, S3Error, S3Location,
    S3ReaderConfig,
};

// Signer functions (re-exported from s3/)
pub use crate::io::s3::{should_sign, sign_request};

// Streaming parser trait (re-exported from unified streaming module)
pub use crate::io::streaming::StreamingParser;

// Re-export the S3 transport
pub use transport::S3Transport;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_location_new() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap");
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
        assert!(location.region().is_none());
        assert!(location.endpoint().is_none());
    }

    #[test]
    fn test_s3_location_builder() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap")
            .with_region("us-west-2")
            .with_endpoint("https://s3.amazonaws.com");

        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
        assert_eq!(location.region(), Some("us-west-2"));
        assert_eq!(location.endpoint(), Some("https://s3.amazonaws.com"));
    }

    #[test]
    fn test_s3_location_url() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap");
        assert_eq!(
            location.url(),
            "https://my-bucket.s3.amazonaws.com/path/to/file.mcap"
        );

        let location_with_region =
            S3Location::new("my-bucket", "path/to/file.mcap").with_region("eu-west-1");
        assert_eq!(
            location_with_region.url(),
            "https://my-bucket.s3.eu-west-1.amazonaws.com/path/to/file.mcap"
        );

        let location_custom = S3Location::new("my-bucket", "path/to/file.mcap")
            .with_endpoint("https://minio.example.com");
        assert_eq!(
            location_custom.url(),
            "https://minio.example.com/my-bucket/path/to/file.mcap"
        );
    }

    #[test]
    fn test_s3_location_s3_scheme() {
        let location = S3Location::from_s3_url("s3://my-bucket/path/to/file.mcap").unwrap();
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
    }

    #[test]
    fn test_s3_config_default() {
        let config = S3ReaderConfig::default();
        assert_eq!(config.buffer_size(), 64 * 1024);
        assert_eq!(config.max_chunk_size(), 10 * 1024 * 1024);
        assert_eq!(config.header_scan_limit(), 1024 * 1024);
    }

    #[test]
    fn test_s3_config_builder() {
        let config = S3ReaderConfig::default()
            .with_buffer_size(128 * 1024)
            .with_max_chunk_size(20 * 1024 * 1024)
            .with_header_scan_limit(2 * 1024 * 1024);

        assert_eq!(config.buffer_size(), 128 * 1024);
        assert_eq!(config.max_chunk_size(), 20 * 1024 * 1024);
        assert_eq!(config.header_scan_limit(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_s3_reader_state_display() {
        // Note: S3ReaderState is in the old s3/reader module
        // This test will be moved when reader is refactored
        // Placeholder test - currently does nothing
    }

    #[test]
    fn test_recoverable_error_display() {
        let err = RecoverableError::MessageCorruption {
            offset: 1000,
            error: "invalid data".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "Message corruption at offset 1000: invalid data"
        );

        let err = RecoverableError::UnknownChannel { channel_id: 42 };
        assert_eq!(format!("{}", err), "Unknown channel: 42");

        let err = RecoverableError::ParseError {
            record_type: "Message".to_string(),
            error: "invalid format".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "Parse error in Message record: invalid format"
        );
    }
}
