// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader and writer tests.
//!
//! This file contains all tests for S3 functionality, organized by module:
//! - Streaming parser tests (chunk boundary handling)
//! - Golden file comparison tests
//! - Wiremock mock server tests
//! - MinIO integration tests

use std::path::PathBuf;
use std::time::Duration;

use robocodec::io::s3::{
    S3Client, S3Location, S3Reader, S3ReaderConfig, StreamingBagParser, StreamingMcapParser,
    MCAP_MAGIC,
};
use robocodec::io::traits::FormatReader;

fn fixture_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures");
    path.push(name);
    path
}

// ============================================================================
// Streaming Parser Tests
// ============================================================================

mod streaming_tests {
    use super::*;

    #[test]
    fn test_mcap_stream_magic_detection() {
        let mut parser = StreamingMcapParser::new();

        for (i, &byte) in MCAP_MAGIC.iter().enumerate() {
            let result = parser.parse_chunk(&[byte]);
            assert!(result.is_ok());
            if i < MCAP_MAGIC.len() - 1 {
                assert!(!parser.is_initialized());
            }
        }
        assert!(parser.is_initialized());
    }

    #[test]
    fn test_mcap_stream_invalid_magic() {
        let mut parser = StreamingMcapParser::new();
        let result = parser.parse_chunk(b"INVALID_MAGIC");
        assert!(result.is_err());
    }

    #[test]
    fn test_mcap_stream_self_consistent() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let data = std::fs::read(&path).unwrap();

        let mut parser_4k = StreamingMcapParser::new();
        let mut parser_64k = StreamingMcapParser::new();

        let mut msgs_4k = 0u64;
        let mut msgs_64k = 0u64;

        for chunk in data.chunks(4096) {
            if let Ok(msgs) = parser_4k.parse_chunk(chunk) {
                msgs_4k += msgs.len() as u64;
            }
        }

        for chunk in data.chunks(65536) {
            if let Ok(msgs) = parser_64k.parse_chunk(chunk) {
                msgs_64k += msgs.len() as u64;
            }
        }

        assert_eq!(msgs_4k, msgs_64k, "Message count independent of chunk size");
        assert_eq!(
            parser_4k.channels().len(),
            parser_64k.channels().len(),
            "Channel discovery consistent"
        );
    }

    #[test]
    fn test_bag_stream_magic_detection() {
        let mut parser = StreamingBagParser::new();
        let magic_full = b"#ROSBAG V2.0\n";

        for (i, &byte) in magic_full.iter().enumerate() {
            let result = parser.parse_chunk(&[byte]);
            assert!(result.is_ok());
            if i < magic_full.len() - 1 {
                assert!(!parser.is_initialized());
            }
        }
        assert!(parser.is_initialized());
        assert_eq!(parser.version(), Some("2.0"));
    }

    #[test]
    fn test_bag_stream_self_consistent() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        let data = std::fs::read(&path).unwrap();

        let mut parser_4k = StreamingBagParser::new();
        let mut parser_64k = StreamingBagParser::new();

        let mut msgs_4k = 0u64;
        let mut msgs_64k = 0u64;

        for chunk in data.chunks(4096) {
            if let Ok(msgs) = parser_4k.parse_chunk(chunk) {
                msgs_4k += msgs.len() as u64;
            }
        }

        for chunk in data.chunks(65536) {
            if let Ok(msgs) = parser_64k.parse_chunk(chunk) {
                msgs_64k += msgs.len() as u64;
            }
        }

        assert_eq!(msgs_4k, msgs_64k);
        assert_eq!(parser_4k.channels().len(), parser_64k.channels().len());
    }

    #[test]
    fn test_diagnostic_simple_mcap() {
        // Test with a minimal manually constructed MCAP file
        // to verify the parser works correctly
        let mut mcap_data = Vec::new();

        // Magic
        mcap_data.extend_from_slice(b"\x89MCAP0\r\n");

        // Header record
        mcap_data.push(0x01); // OP_HEADER
        mcap_data.extend_from_slice(&4u64.to_le_bytes()); // length = 4
        mcap_data.extend_from_slice(&0u32.to_le_bytes()); // profile = 0

        // Schema record
        let schema = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name = "Foo"
            0x07, 0x00, // encoding_len = 7
            b'r', b'o', b's', b'2', b'm', b's', b'g', // encoding = "ros2msg"
            b'#', b' ', b't', b'e', b's', b't', // data
        ];
        mcap_data.push(0x03); // OP_SCHEMA
        mcap_data.extend_from_slice(&(schema.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&schema);

        // Channel record
        let channel = [
            0x00, 0x01, // channel_id = 256
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic = "/test"
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding = "cdr"
            0x01, 0x00, // schema_id = 1
        ];
        mcap_data.push(0x04); // OP_CHANNEL
        mcap_data.extend_from_slice(&(channel.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&channel);

        // Message record
        let msg = [
            0x00, 0x01, // channel_id = 256
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_time = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // publish_time = 0
            b'h', b'e', b'l', b'l', b'o', // data
        ];
        mcap_data.push(0x05); // OP_MESSAGE
        mcap_data.extend_from_slice(&(msg.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&msg);

        // Parse in small chunks
        let mut parser = StreamingMcapParser::new();
        for (i, chunk) in mcap_data.chunks(10).enumerate() {
            let result = parser.parse_chunk(chunk);
            assert!(result.is_ok(), "Chunk {} failed: {:?}", i, result);
        }

        // Should have found the channel
        assert_eq!(parser.channels().len(), 1, "Should have 1 channel");
        assert_eq!(parser.message_count(), 1, "Should have 1 message");
    }

    #[test]
    fn test_diagnostic_with_chunk() {
        // Test with a MCAP file that has a CHUNK record
        let mut mcap_data = Vec::new();

        // Magic
        mcap_data.extend_from_slice(b"\x89MCAP0\r\n");

        // Header record
        mcap_data.push(0x01); // OP_HEADER
        mcap_data.extend_from_slice(&4u64.to_le_bytes()); // length = 4
        mcap_data.extend_from_slice(&0u32.to_le_bytes()); // profile = 0

        // CHUNK record (large record with compressed data)
        let chunk_size = 1000; // Small chunk for testing
        mcap_data.push(0x06); // OP_CHUNK
        mcap_data.extend_from_slice(&(chunk_size as u64).to_le_bytes());
        // Add chunk body (could be compressed data)
        for i in 0..chunk_size {
            mcap_data.push((i % 256) as u8);
        }

        // Schema record (after the chunk)
        let schema = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name = "Foo"
            0x07, 0x00, // encoding_len = 7
            b'r', b'o', b's', b'2', b'm', b's', b'g', // encoding = "ros2msg"
            b'#', b' ', b't', b'e', b's', b't', // data
        ];
        mcap_data.push(0x03); // OP_SCHEMA
        mcap_data.extend_from_slice(&(schema.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&schema);

        // Channel record
        let channel = [
            0x00, 0x01, // channel_id = 256
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic = "/test"
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding = "cdr"
            0x01, 0x00, // schema_id = 1
        ];
        mcap_data.push(0x04); // OP_CHANNEL
        mcap_data.extend_from_slice(&(channel.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&channel);

        // Parse in small chunks to test chunk boundary handling
        let mut parser = StreamingMcapParser::new();
        for (i, chunk) in mcap_data.chunks(100).enumerate() {
            let result = parser.parse_chunk(chunk);
            if let Err(e) = &result {
                eprintln!("Error at chunk {}: {:?}", i, e);
                eprintln!(
                    "Parser state: initialized={}, channels={}",
                    parser.is_initialized(),
                    parser.channels().len()
                );
            }
            assert!(result.is_ok(), "Chunk {} failed: {:?}", i, result);
        }

        // Should have found the channel (after skipping the CHUNK)
        assert_eq!(
            parser.channels().len(),
            1,
            "Should have 1 channel after CHUNK"
        );
    }

    #[test]
    fn test_diagnostic_realistic_structure() {
        // Test with a MCAP file structure similar to the real file:
        // HEADER -> CHUNK -> MESSAGE_INDEX -> DATA_END -> SCHEMA -> CHANNEL -> MESSAGE
        let mut mcap_data = Vec::new();

        // Magic
        mcap_data.extend_from_slice(b"\x89MCAP0\r\n");

        // Header record
        mcap_data.push(0x01); // OP_HEADER
        mcap_data.extend_from_slice(&4u64.to_le_bytes()); // length = 4
        mcap_data.extend_from_slice(&0u32.to_le_bytes()); // profile = 0

        // CHUNK record (simulating compressed data)
        let chunk_size = 200; // Small chunk for testing
        mcap_data.push(0x06); // OP_CHUNK
        mcap_data.extend_from_slice(&(chunk_size as u64).to_le_bytes());
        // Add chunk body (simulated compressed data)
        for i in 0..chunk_size {
            mcap_data.push((i % 256) as u8);
        }

        // MESSAGE_INDEX records (before schemas in real files)
        for _i in 0..3 {
            mcap_data.push(0x07); // OP_MESSAGE_INDEX
            mcap_data.extend_from_slice(&22u64.to_le_bytes());
            // Add dummy index data
            for _ in 0..22 {
                mcap_data.push(0);
            }
        }

        // DATA_END record
        mcap_data.push(0x0F); // OP_DATA_END
        mcap_data.extend_from_slice(&4u64.to_le_bytes());
        mcap_data.extend_from_slice(&0u32.to_le_bytes());

        // Schema record (after DATA_END in real files)
        let schema = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'F', b'o', b'o', // name = "Foo"
            0x07, 0x00, // encoding_len = 7
            b'r', b'o', b's', b'2', b'm', b's', b'g', // encoding = "ros2msg"
            b'#', b' ', b't', b'e', b's', b't', // data
        ];
        mcap_data.push(0x03); // OP_SCHEMA
        mcap_data.extend_from_slice(&(schema.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&schema);

        // Channel record
        let channel = [
            0x00, 0x01, // channel_id = 256
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic = "/test"
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding = "cdr"
            0x01, 0x00, // schema_id = 1
        ];
        mcap_data.push(0x04); // OP_CHANNEL
        mcap_data.extend_from_slice(&(channel.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&channel);

        // Message record
        let msg = [
            0x00, 0x01, // channel_id = 256
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // sequence = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_time = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // publish_time = 0
            b'h', b'e', b'l', b'l', b'o', // data
        ];
        mcap_data.push(0x05); // OP_MESSAGE
        mcap_data.extend_from_slice(&(msg.len() as u64).to_le_bytes());
        mcap_data.extend_from_slice(&msg);

        // Parse in small chunks to test chunk boundary handling
        let mut parser = StreamingMcapParser::new();
        for (i, chunk) in mcap_data.chunks(50).enumerate() {
            let result = parser.parse_chunk(chunk);
            if let Err(e) = &result {
                eprintln!("Error at chunk {}: {:?}", i, e);
                eprintln!("Total bytes so far: {}", i * 50);
                eprintln!(
                    "Parser state: initialized={}, channels={}",
                    parser.is_initialized(),
                    parser.channels().len()
                );
            }
            assert!(result.is_ok(), "Chunk {} failed: {:?}", i, result);
        }

        // Should have found the channel
        assert_eq!(parser.channels().len(), 1, "Should have 1 channel");
        assert_eq!(parser.message_count(), 1, "Should have 1 message");
    }

    #[test]
    fn test_simple_mcap_file() {
        // Test with a simple MCAP file that has Schema -> Channel -> Message
        // This file was created to work with the streaming parser
        // (unlike the fixture files which have CHUNK records)
        let path = fixture_path("simple_streaming_test.mcap");
        if !path.exists() {
            return;
        }

        let data = std::fs::read(&path).unwrap();
        let mut parser = StreamingMcapParser::new();

        // Parse in small chunks to test chunk boundaries
        for (i, chunk) in data.chunks(10).enumerate() {
            let result = parser.parse_chunk(chunk);
            assert!(result.is_ok(), "Chunk {} failed: {:?}", i, result);
        }

        // Verify results
        assert_eq!(parser.channels().len(), 1, "Should have 1 channel");
        assert_eq!(parser.message_count(), 1, "Should have 1 message");

        // Check channel details
        let channels = parser.channels();
        assert!(channels.contains_key(&1), "Should have channel id 1");
        let channel = &channels[&1];
        assert_eq!(channel.topic, "/camera/image_raw");
        assert_eq!(channel.encoding, "cdr");
    }
}

// ============================================================================
// Golden File Comparison Tests
// ============================================================================

mod golden_tests {
    use super::*;

    /// Verify the regular McapReader can parse the test file correctly.
    /// This serves as a baseline to verify the test files are valid.
    #[test]
    fn test_regular_reader_works() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        use robocodec::io::formats::mcap::McapReader;
        let reader = McapReader::open(&path).unwrap();
        eprintln!("Regular reader: {} channels", reader.channels().len());
        eprintln!("Regular reader: {} messages", reader.message_count());

        assert!(reader.channels().len() > 0, "Should have channels");
        assert!(reader.message_count() > 0, "Should have messages");
    }

    /// Verify the BAG file is valid and can be parsed.
    #[test]
    fn test_regular_bag_reader_works() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        use robocodec::io::formats::bag::SequentialBagReader;
        let reader = SequentialBagReader::open(&path).unwrap();
        eprintln!("BAG reader: {} channels", reader.channels().len());
        eprintln!("BAG reader: {} messages", reader.message_count());

        assert!(reader.channels().len() > 0, "Should have channels");
        // Note: Some BAG files may have channels but no messages
    }
}

// ============================================================================
// Wiremock Mock Server Tests
// ============================================================================

mod wiremock_tests {
    use super::*;
    use wiremock::{
        matchers::{header, method, path as wiremock_path},
        Mock, MockServer, ResponseTemplate,
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

        let location =
            S3Location::new("test-bucket", "test.mcap").with_endpoint(&mock_server.uri());

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

        let location =
            S3Location::new("test-bucket", "missing.mcap").with_endpoint(&mock_server.uri());

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

        let location =
            S3Location::new("test-bucket", "test.mcap").with_endpoint(&mock_server.uri());

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

        let location =
            S3Location::new("test-bucket", "empty.mcap").with_endpoint(&mock_server.uri());

        let result = client.fetch_range(&location, 0, 100).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}

// ============================================================================
// MinIO Integration Tests
// ============================================================================

mod minio_tests {
    use super::*;

    #[derive(Clone)]
    struct MinIOConfig {
        pub endpoint: String,
        pub bucket: String,
        pub region: String,
    }

    impl Default for MinIOConfig {
        fn default() -> Self {
            Self {
                endpoint: std::env::var("MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:9000".to_string()),
                bucket: std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "test-bucket".to_string()),
                region: std::env::var("MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            }
        }
    }

    async fn minio_available() -> bool {
        let config = MinIOConfig::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .danger_accept_invalid_certs(true)
            .build();

        let Ok(client) = client else { return false };
        let url = format!("{}/", config.endpoint);
        client.head(&url).send().await.is_ok()
    }

    async fn upload_to_minio(
        config: &MinIOConfig,
        key: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(true)
            .build()?;

        let url = format!("{}/{}/{}", config.endpoint, config.bucket, key);
        let response = client
            .put(&url)
            .header("Content-Type", "application/octet-stream")
            .body(data.to_vec())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("Upload failed: HTTP {}", response.status()).into());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_minio_docker_instructions() {
        println!("\n==== MinIO Docker Setup Instructions ====");
        println!("docker run -d --name minio-test -p 9000:9000 -p 9001:9001 \\");
        println!("  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \\");
        println!("  quay.io/minio/minio server /data --console-address ':9001'");
        println!();
        println!("Set MINIO_ENDPOINT=http://localhost:9000 to run integration tests");
        println!("=========================================\n");
    }

    #[tokio::test]
    async fn test_minio_read_mcap() {
        if !minio_available().await {
            return;
        }

        let config = MinIOConfig::default();
        let fixture_path = fixture_path("robocodec_test_0.mcap");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_0.mcap";

        // Skip test if bucket doesn't exist (403 Forbidden)
        if upload_to_minio(&config, key, &data).await.is_err() {
            eprintln!(
                "Skipping MinIO test: bucket '{}' does not exist or is not accessible",
                config.bucket
            );
            eprintln!(
                "Create the bucket with: mc mb {}/{}",
                config.endpoint, config.bucket
            );
            return;
        }

        // Clean up
        let key_cleanup = key.to_string();
        let endpoint = config.endpoint.clone();
        let bucket = config.bucket.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
            let _ = client.delete(&url).send().await;
        });

        let location = S3Location::new(&config.bucket, key)
            .with_endpoint(&config.endpoint)
            .with_region(&config.region);

        let result = S3Reader::open(location).await;
        assert!(result.is_ok(), "Failed to open S3 reader");

        let reader = result.unwrap();
        assert_eq!(reader.format(), robocodec::io::metadata::FileFormat::Mcap);
        assert!(FormatReader::file_size(&reader) > 0);
    }
}
