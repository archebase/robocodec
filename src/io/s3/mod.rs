// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader for robotics data files.
//!
//! This module provides a pure streaming reader for S3-hosted robotics data files
//! (MCAP, BAG) that streams data sequentially without random access, parsing metadata
//! on-the-fly to build an in-memory index.

mod bag_stream;
mod client;
mod config;
mod error;
mod location;
pub mod mcap_stream;
mod reader;
mod signer;
mod writer;

pub use bag_stream::{
    BagMessageRecord, BagRecord, BagRecordFields, BagRecordHeader, StreamingBagParser,
    BAG_MAGIC_PREFIX,
};
pub use client::S3Client;
pub use config::{AwsCredentials, RetryConfig, S3ReaderConfig};
pub use error::{FatalError, RecoverableError, S3Error};
pub use location::S3Location;
pub use mcap_stream::{
    ChannelRecordInfo, McapRecord, McapRecordHeader, MessageRecord, SchemaInfo, StreamingMcapParser,
};
// Re-export MCAP magic from formats module
pub use crate::io::formats::mcap::constants::MCAP_MAGIC;
pub use reader::{S3MessageStream, S3Reader, S3ReaderState};
pub use writer::S3Writer;

// Test-only exports - these are public but only intended for testing
pub use reader::{S3ReaderConstructor, SummarySchemaInfo};

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
        let location = S3Location::new("my-bucket", "path/to/file.bag")
            .with_region("us-west-2")
            .with_endpoint("https://s3.amazonaws.com");

        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.bag");
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
        assert_eq!(format!("{}", S3ReaderState::Initial), "Initial");
        assert_eq!(format!("{}", S3ReaderState::Eof), "End of file");
        assert_eq!(
            format!("{}", S3ReaderState::Error("test error".to_string())),
            "Error: test error"
        );
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
