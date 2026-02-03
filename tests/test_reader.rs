// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for S3 reader.

#[cfg(feature = "s3")]
use robocodec::io::s3::{S3Location, S3ReaderConfig, S3ReaderState};
#[cfg(feature = "s3")]
use robocodec::io::traits::FormatReader;

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_new() {
    let location = S3Location::new("my-bucket", "path/to/file.mcap");
    assert_eq!(location.bucket(), "my-bucket");
    assert_eq!(location.key(), "path/to/file.mcap");
    assert!(location.region().is_none());
    assert!(location.endpoint().is_none());
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_is_mcap() {
    let location = S3Location::new("bucket", "file.mcap");
    assert!(location.is_mcap());
    assert!(!location.is_bag());
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_is_bag() {
    let location = S3Location::new("bucket", "file.bag");
    assert!(location.is_bag());
    assert!(!location.is_mcap());
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_url() {
    let location = S3Location::new("my-bucket", "path/to/file.mcap");
    assert_eq!(
        location.url(),
        "https://my-bucket.s3.amazonaws.com/path/to/file.mcap"
    );
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_with_region() {
    let location = S3Location::new("my-bucket", "file.bag").with_region("us-west-2");
    assert_eq!(location.region(), Some("us-west-2"));
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_with_endpoint() {
    let location =
        S3Location::new("my-bucket", "file.mcap").with_endpoint("https://minio.example.com");
    assert_eq!(location.endpoint(), Some("https://minio.example.com"));
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_reader_config_default() {
    let config = S3ReaderConfig::default();
    assert_eq!(config.buffer_size, 64 * 1024);
    assert_eq!(config.max_chunk_size, 10 * 1024 * 1024);
    assert_eq!(config.header_scan_limit, 1024 * 1024);
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_reader_config_builder() {
    let config = S3ReaderConfig::default()
        .with_buffer_size(128 * 1024)
        .with_max_chunk_size(20 * 1024 * 1024)
        .with_header_scan_limit(2 * 1024 * 1024);

    assert_eq!(config.buffer_size, 128 * 1024);
    assert_eq!(config.max_chunk_size, 20 * 1024 * 1024);
    assert_eq!(config.header_scan_limit, 2 * 1024 * 1024);
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_reader_state_display() {
    assert_eq!(format!("{}", S3ReaderState::Initial), "Initial");
    assert_eq!(format!("{}", S3ReaderState::Eof), "End of file");
    assert_eq!(
        format!("{}", S3ReaderState::Error("test".to_string())),
        "Error: test"
    );
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_from_s3_url() {
    let location = S3Location::from_s3_url("s3://my-bucket/path/to/file.mcap").unwrap();
    assert_eq!(location.bucket(), "my-bucket");
    assert_eq!(location.key(), "path/to/file.mcap");
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_reader_format_reader_trait() {
    // Test that S3Reader implements FormatReader
    fn assert_format_reader<T: FormatReader>() {}
    assert_format_reader::<robocodec::io::s3::S3Reader>();
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_display() {
    let location = S3Location::new("my-bucket", "path/to/file.mcap");
    assert_eq!(format!("{}", location), "s3://my-bucket/path/to/file.mcap");
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_location_extension() {
    let location = S3Location::new("bucket", "path/to/file.mcap");
    assert_eq!(location.extension(), Some("mcap"));

    let location = S3Location::new("bucket", "path/to/file.bag");
    assert_eq!(location.extension(), Some("bag"));

    let location = S3Location::new("bucket", "path/to/no_extension");
    assert_eq!(location.extension(), None);
}
