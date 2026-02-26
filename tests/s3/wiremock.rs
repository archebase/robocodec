// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Wiremock-based tests for S3 functionality.

use robocodec::io::s3::{S3Client, S3Location, S3ReaderConfig, S3ReaderConstructor};
use robocodec::io::traits::FormatReader;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path as wiremock_path},
};

#[tokio::test]
async fn test_s3_client_fetch_range_success() {
    let mock_server = MockServer::start().await;

    let data = b"Hello, S3!";
    Mock::given(method("GET"))
        .and(wiremock_path("/test-bucket/test.mcap"))
        .and(header("Range", "bytes=0-10"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(data))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location = S3Location::new("test-bucket", "test.mcap").with_endpoint(mock_server.uri());

    let result = client.fetch_range(&location, 0, 11).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_s3_client_404() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(wiremock_path("/test-bucket/missing.mcap"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location = S3Location::new("test-bucket", "missing.mcap").with_endpoint(mock_server.uri());

    let result = client.fetch_range(&location, 0, 100).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_s3_client_object_size() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(wiremock_path("/test-bucket/test.mcap"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "12345"))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location = S3Location::new("test-bucket", "test.mcap").with_endpoint(mock_server.uri());

    let result = client.object_size(&location).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 12345);
}

#[tokio::test]
async fn test_s3_client_empty_response() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(wiremock_path("/test-bucket/empty.mcap"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(b""))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location = S3Location::new("test-bucket", "empty.mcap").with_endpoint(mock_server.uri());

    let result = client.fetch_range(&location, 0, 100).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[tokio::test]
async fn test_s3_client_403_access_denied() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(wiremock_path("/secure-bucket/restricted.mcap"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location =
        S3Location::new("secure-bucket", "restricted.mcap").with_endpoint(mock_server.uri());

    let result = client.fetch_range(&location, 0, 100).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_s3_client_500_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(wiremock_path("/test-bucket/error.mcap"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location = S3Location::new("test-bucket", "error.mcap").with_endpoint(mock_server.uri());

    let result = client.fetch_range(&location, 0, 100).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_s3_reader_state_queries() {
    let constructor = S3ReaderConstructor::new_mcap();
    let reader = constructor.build();

    assert!(reader.has_more());
    assert_eq!(reader.path(), "test.mcap");
    assert_eq!(reader.format(), robocodec::io::metadata::FileFormat::Mcap);
    assert_eq!(reader.file_size(), 0);
    assert_eq!(reader.message_count(), 0);
    assert!(reader.start_time().is_none());
    assert!(reader.end_time().is_none());
}

#[tokio::test]
async fn test_s3_reader_location() {
    let constructor = S3ReaderConstructor::new_mcap();
    let reader = constructor.build();

    assert_eq!(reader.location().bucket(), "test-bucket");
    assert_eq!(reader.location().key(), "test.mcap");
}

#[tokio::test]
async fn test_s3_client_head_missing_content_length() {
    let mock_server = MockServer::start().await;

    Mock::given(method("HEAD"))
        .and(wiremock_path("/test-bucket/no-length.mcap"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location =
        S3Location::new("test-bucket", "no-length.mcap").with_endpoint(mock_server.uri());

    let result = client.object_size(&location).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Content-Length"));
}

#[tokio::test]
async fn test_s3_client_invalid_uri() {
    let config = S3ReaderConfig::default();
    let client = S3Client::new(config).unwrap();

    let location = S3Location::new("test-bucket", "file with spaces.mcap");

    let result = client.fetch_range(&location, 0, 100).await;
    assert!(result.is_err());
}
