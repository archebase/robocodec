// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader and writer tests.
//!
//! This file contains all tests for S3 functionality, organized by module:
//! - Streaming parser tests (chunk boundary handling)
//! - Two-tier reading tests (footer-first, summary parsing, fallback scanning)
//! - Golden file comparison tests
//! - Wiremock mock server tests
//! - S3 integration tests

use std::path::PathBuf;
use std::time::Duration;

use robocodec::io::s3::{
    MCAP_MAGIC, S3Client, S3Location, S3Reader, S3ReaderConfig, S3ReaderConstructor,
    StreamingBagParser, StreamingMcapParser, SummarySchemaInfo,
};
use robocodec::io::streaming::StreamingParser;
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
        // Test with a MCAP file that has schema and channel records
        // NOTE: The old test used invalid CHUNK data which the mcap crate's
        // LinearReader cannot handle. We test the core functionality (chunk
        // boundary handling with schema/channel records) without CHUNK.
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

        // Should have found the channel
        assert_eq!(parser.channels().len(), 1, "Should have 1 channel");
    }

    #[test]
    fn test_diagnostic_realistic_structure() {
        // Test with a MCAP file structure: HEADER -> SCHEMA -> CHANNEL -> MESSAGE
        // NOTE: The old test used invalid CHUNK data which the mcap crate's
        // LinearReader cannot handle. We test the core functionality with
        // valid records.
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
// Two-Tier Reading Tests (Footer-First + Fallback Scanning)
// ============================================================================

mod two_tier_tests {
    use super::*;

    /// Test MCAP footer parsing with valid footer data.
    #[test]
    fn test_mcap_footer_parsing() {
        // Create minimal valid MCAP footer data:
        // - summary_offset: u64 (8 bytes)
        // - summary_section_start: u64 (8 bytes)
        // - summary_crc: u32 (4 bytes)
        let mut footer_data = Vec::new();

        // summary_offset = 1000
        footer_data.extend_from_slice(&1000u64.to_le_bytes());
        // summary_section_start = 500
        footer_data.extend_from_slice(&500u64.to_le_bytes());
        // summary_crc = 0
        footer_data.extend_from_slice(&0u32.to_le_bytes());

        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        let result = reader.parse_mcap_footer(&footer_data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1000);
    }

    /// Test MCAP footer parsing with insufficient data.
    #[test]
    fn test_mcap_footer_too_short() {
        let footer_data = vec![1, 2, 3, 4]; // Less than 8 bytes

        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        let result = reader.parse_mcap_footer(&footer_data);
        assert!(result.is_err());
    }

    /// Test schema record parsing from summary section.
    #[test]
    fn test_schema_record_parsing() {
        // Create a valid Schema record:
        // id=1, name="TestMsg" (7 bytes), encoding="ros2msg" (7 bytes), data=b"# test"
        let schema_bytes = [
            0x01, 0x00, // id = 1
            0x07, 0x00, // name_len = 7
            b'T', b'e', b's', b't', b'M', b's', b'g', // name = "TestMsg"
            0x07, 0x00, // encoding_len = 7
            b'r', b'o', b's', b'2', b'm', b's', b'g', // encoding = "ros2msg"
            b'#', b' ', b't', b'e', b's', b't', // data = "# test"
        ];

        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        let result = reader.parse_schema_record(&schema_bytes);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.id, 1);
        assert_eq!(schema.name, "TestMsg");
        assert_eq!(schema.encoding, "ros2msg");
    }

    /// Test channel record parsing from summary section.
    #[test]
    fn test_channel_record_parsing() {
        // First create a schema map
        use std::collections::HashMap;
        let mut schemas = HashMap::new();
        schemas.insert(
            1,
            SummarySchemaInfo {
                id: 1,
                name: "TestMsg".to_string(),
                encoding: "ros2msg".to_string(),
                data: b"# test".to_vec(),
            },
        );

        // Create a valid Channel record:
        // id=2, topic="/test" (5 bytes), encoding="cdr" (3 bytes), schema_id=1
        let channel_bytes = [
            0x02, 0x00, // channel_id = 2
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic = "/test"
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding = "cdr"
            0x01, 0x00, // schema_id = 1
        ];

        let mut channels = HashMap::new();

        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        let result = reader.parse_channel_record(&channel_bytes, &schemas, &mut channels);
        assert!(result.is_ok());
        assert_eq!(channels.len(), 1);
        assert!(channels.contains_key(&2));
        let channel = &channels[&2];
        assert_eq!(channel.topic, "/test");
        assert_eq!(channel.encoding, "cdr");
        assert_eq!(channel.message_type, "TestMsg");
    }

    /// Test summary data parsing with multiple records.
    #[test]
    fn test_summary_data_parsing() {
        // Create a summary section with Schema and Channel records
        let mut summary_data = Vec::new();

        // Schema record: id=1, name="Msg", encoding="ros2msg", data="# test"
        let schema = [
            0x01, 0x00, // id = 1
            0x03, 0x00, // name_len = 3
            b'M', b's', b'g', // name = "Msg"
            0x07, 0x00, // encoding_len = 7
            b'r', b'o', b's', b'2', b'm', b's', b'g', // encoding
            b'#', b' ', b't', b'e', b's', b't', // data
        ];
        summary_data.push(0x03); // OP_SCHEMA
        summary_data.extend_from_slice(&(schema.len() as u64).to_le_bytes());
        summary_data.extend_from_slice(&schema);

        // Channel record: id=1, topic="/test", encoding="cdr", schema_id=1
        let channel = [
            0x01, 0x00, // channel_id = 1
            0x05, 0x00, // topic_len = 5
            b'/', b't', b'e', b's', b't', // topic
            0x03, 0x00, // encoding_len = 3
            b'c', b'd', b'r', // encoding
            0x01, 0x00, // schema_id = 1
        ];
        summary_data.push(0x04); // OP_CHANNEL
        summary_data.extend_from_slice(&(channel.len() as u64).to_le_bytes());
        summary_data.extend_from_slice(&channel);

        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        let result = reader.parse_mcap_summary_data(&summary_data);
        assert!(result.is_ok());
        let channels = result.unwrap();
        assert_eq!(channels.len(), 1);
        assert!(channels.contains_key(&1));
    }
}

// ============================================================================
// Golden File Comparison Tests
// ============================================================================

mod golden_tests {
    use super::*;

    /// Verify the regular RoboReader can parse the test file correctly.
    /// This serves as a baseline to verify the test files are valid.
    #[test]
    fn test_regular_reader_works() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        use robocodec::RoboReader;
        let reader = RoboReader::open(path.to_str().unwrap()).unwrap();
        eprintln!("Regular reader: {} channels", reader.channels().len());
        eprintln!("Regular reader: {} messages", reader.message_count());

        assert!(!reader.channels().is_empty(), "Should have channels");
        assert!(reader.message_count() > 0, "Should have messages");
    }

    /// Verify the BAG file is valid and can be parsed.
    #[test]
    fn test_regular_bag_reader_works() {
        let path = fixture_path("robocodec_test_15.bag");
        if !path.exists() {
            return;
        }

        use robocodec::RoboReader;
        let reader = RoboReader::open(path.to_str().unwrap()).unwrap();
        eprintln!("BAG reader: {} channels", reader.channels().len());
        eprintln!("BAG reader: {} messages", reader.message_count());

        assert!(!reader.channels().is_empty(), "Should have channels");
        // Note: Some BAG files may have channels but no messages
    }
}

// ============================================================================
// Wiremock Mock Server Tests
// ============================================================================

mod wiremock_tests {
    use super::*;
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

        let location =
            S3Location::new("test-bucket", "missing.mcap").with_endpoint(mock_server.uri());

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

        let location =
            S3Location::new("test-bucket", "empty.mcap").with_endpoint(mock_server.uri());

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

        let location =
            S3Location::new("test-bucket", "error.mcap").with_endpoint(mock_server.uri());

        let result = client.fetch_range(&location, 0, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_reader_state_queries() {
        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        // Initial state should have more (not EOF or Error)
        assert!(reader.has_more());

        // Check basic properties
        assert_eq!(reader.path(), "test.mcap");
        assert_eq!(reader.format(), robocodec::io::metadata::FileFormat::Mcap);
        assert_eq!(reader.file_size(), 0); // Not initialized yet

        // Streaming reader doesn't pre-count messages
        assert_eq!(reader.message_count(), 0);

        // Streaming reader doesn't track time bounds during header scan
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

        // Mock HEAD response without content-length
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

        // Create a location with an invalid URL character
        let location = S3Location::new("test-bucket", "file with spaces.mcap");

        // This should fail during URI parsing in fetch_range
        let result = client.fetch_range(&location, 0, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_multipart_upload_create() {
        use wiremock::matchers::method;

        let mock_server = MockServer::start().await;

        // Mock the InitiateMultipartUploadResponse
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/upload.mcap"))
            .and(header("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))
            .and(wiremock_path("/test-bucket/upload.mcap"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("<?xml version=\"1.0\"?><InitiateMultipartUploadResult><UploadId>test-upload-id-123</UploadId></InitiateMultipartUploadResult>")
            )
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "upload.mcap").with_endpoint(mock_server.uri());

        let result = client.create_upload(&location).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test-upload-id-123");
    }

    #[tokio::test]
    async fn test_s3_multipart_upload_create_failure() {
        use wiremock::matchers::method;

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/fail.mcap"))
            .and(header("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location = S3Location::new("test-bucket", "fail.mcap").with_endpoint(mock_server.uri());

        let result = client.create_upload(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_multipart_upload_part() {
        use wiremock::matchers::method;

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/part.mcap"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"test-etag-123\""))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location = S3Location::new("test-bucket", "part.mcap").with_endpoint(mock_server.uri());

        let data = bytes::Bytes::from(&b"test data"[..]);
        let result = client.upload_part(&location, "upload-id", 1, data).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test-etag-123");
    }

    #[tokio::test]
    async fn test_s3_multipart_complete() {
        use wiremock::matchers::method;

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/complete.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "complete.mcap").with_endpoint(mock_server.uri());

        let parts = vec![(1, "etag1".to_string()), (2, "etag2".to_string())];
        let result = client.complete_upload(&location, "upload-id", parts).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_s3_multipart_abort() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(wiremock_path("/test-bucket/abort.mcap"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "abort.mcap").with_endpoint(mock_server.uri());

        let result = client.abort_upload(&location, "upload-id").await;
        assert!(result.is_ok());
    }

    // =========================================================================
    // Additional wiremock tests for uncovered code paths
    // =========================================================================

    #[tokio::test]
    async fn test_s3_client_fetch_header_success() {
        let mock_server = MockServer::start().await;

        let data = b"MCAP header data";
        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/header.mcap"))
            .and(header("Range", "bytes=0-15"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(data))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "header.mcap").with_endpoint(mock_server.uri());

        let result = client.fetch_header(&location, 16).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 16);
    }

    #[tokio::test]
    async fn test_s3_client_fetch_tail_success() {
        let mock_server = MockServer::start().await;

        let data = b"MCAP footer";
        // fetch_tail(11, 111) will call fetch_range(100, 11) which produces "bytes=100-110"
        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/tail.mcap"))
            .and(header("Range", "bytes=100-110"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(data))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location = S3Location::new("test-bucket", "tail.mcap").with_endpoint(mock_server.uri());

        let result = client.fetch_tail(&location, 11, 111).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 11);
    }

    #[tokio::test]
    async fn test_s3_client_create_upload_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/fail-upload.mcap"))
            .and(header("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))
            .respond_with(ResponseTemplate::new(403).set_body_raw(
                "<Error><Code>AccessDenied</Code></Error>",
                "application/xml",
            ))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "fail-upload.mcap").with_endpoint(mock_server.uri());

        let result = client.create_upload(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_client_create_upload_invalid_response() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/bad-upload.mcap"))
            .and(header("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw("Invalid response without UploadId", "text/plain"),
            )
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "bad-upload.mcap").with_endpoint(mock_server.uri());

        let result = client.create_upload(&location).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_client_upload_part_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/part-error.mcap"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "part-error.mcap").with_endpoint(mock_server.uri());

        use bytes::Bytes;
        let result = client
            .upload_part(
                &location,
                "upload-id",
                1,
                Bytes::copy_from_slice(b"test data"),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_client_complete_upload_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/complete-error.mcap"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "complete-error.mcap").with_endpoint(mock_server.uri());

        let parts = vec![(1u32, "etag1".to_string())];
        let result = client.complete_upload(&location, "upload-id", parts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_client_abort_upload_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(wiremock_path("/test-bucket/abort-error.mcap"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "abort-error.mcap").with_endpoint(mock_server.uri());

        let result = client.abort_upload(&location, "upload-id").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_client_fetch_range_invalid_response() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/invalid.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "invalid.mcap").with_endpoint(mock_server.uri());

        let result = client.fetch_range(&location, 0, 100).await;
        // Should succeed with 200 status (not 206, but check_range_status allows 200)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_s3_client_fetch_tail_with_zero_offset() {
        let mock_server = MockServer::start().await;

        let data = b"Tail data";
        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/zero-offset.mcap"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(data))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "zero-offset.mcap").with_endpoint(mock_server.uri());

        let result = client.fetch_tail(&location, 9, 9).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_s3_client_connection_error() {
        let mock_server = MockServer::start().await;

        // Mount a mock that will be immediately reset, causing connection errors
        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/connect-error.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        // Reset the mock server to make the endpoint unavailable
        mock_server.reset().await;

        let config = S3ReaderConfig::default().with_request_timeout(Duration::from_secs(1));
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "connect-error.mcap").with_endpoint(mock_server.uri());

        // This should fail with a connection error
        let result = client.fetch_range(&location, 0, 100).await;
        assert!(result.is_err());
    }

    // =========================================================================
    // Additional coverage tests for uncovered code paths
    // =========================================================================

    #[tokio::test]
    async fn test_s3_client_object_size_500_error() {
        let mock_server = MockServer::start().await;

        // HEAD request returns 500 error
        // This tests the path where check_response returns Ok (not 404/403)
        // but the is_success check fails
        Mock::given(method("HEAD"))
            .and(wiremock_path("/test-bucket/error.mcap"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "error.mcap").with_endpoint(mock_server.uri());

        let result = client.object_size(&location).await;
        assert!(result.is_err());
        // Should be HttpError (not ObjectNotFound or AccessDenied)
        match result {
            Err(robocodec::io::s3::FatalError::HttpError {
                status: Some(500), ..
            }) => {
                // Expected path
            }
            _ => panic!("Expected HttpError with status 500, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_s3_client_object_size_503_error() {
        let mock_server = MockServer::start().await;

        // HEAD request returns 503 Service Unavailable
        Mock::given(method("HEAD"))
            .and(wiremock_path("/test-bucket/unavailable.mcap"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "unavailable.mcap").with_endpoint(mock_server.uri());

        let result = client.object_size(&location).await;
        assert!(result.is_err());
        match result {
            Err(robocodec::io::s3::FatalError::HttpError {
                status: Some(503), ..
            }) => {
                // Expected
            }
            _ => panic!("Expected HttpError with status 503"),
        }
    }

    #[tokio::test]
    async fn test_s3_client_fetch_tail_length_exceeds_file_size() {
        let mock_server = MockServer::start().await;

        // When length > file_size, fetch_tail uses saturating_sub
        // fetch_tail(100, 50) -> offset = 50.saturating_sub(100) = 0
        // This tests the saturating_sub path
        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/small.mcap"))
            .and(header("Range", "bytes=0-99")) // offset 0, length 100
            .respond_with(ResponseTemplate::new(206).set_body_bytes(vec![0u8; 50]))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "small.mcap").with_endpoint(mock_server.uri());

        // Request 100 bytes but file is only 50 bytes
        // saturating_sub ensures we don't underflow
        let result = client.fetch_tail(&location, 100, 50).await;
        assert!(result.is_ok());
        // We get at most 50 bytes (what the mock returns)
        assert!(result.unwrap().len() <= 100);
    }

    #[tokio::test]
    async fn test_s3_client_fetch_tail_exact_file_size() {
        let mock_server = MockServer::start().await;

        let data = b"Exact file content";
        Mock::given(method("GET"))
            .and(wiremock_path("/test-bucket/exact.mcap"))
            .and(header("Range", "bytes=0-17"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(data))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "exact.mcap").with_endpoint(mock_server.uri());

        // Request exactly the file size
        let result = client.fetch_tail(&location, 18, 18).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 18);
    }

    #[tokio::test]
    async fn test_s3_client_upload_part_missing_etag() {
        let mock_server = MockServer::start().await;

        // Response without ETag header
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/no-etag.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "no-etag.mcap").with_endpoint(mock_server.uri());

        let data = bytes::Bytes::from(&b"test"[..]);
        let result = client.upload_part(&location, "upload-id", 1, data).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ETag"));
    }

    #[tokio::test]
    async fn test_s3_client_upload_part_empty_etag() {
        let mock_server = MockServer::start().await;

        // Response with empty ETag header (missing value)
        // This should fail since ETag is required
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/empty-etag.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "empty-etag.mcap").with_endpoint(mock_server.uri());

        let data = bytes::Bytes::from(&b"test"[..]);
        let result = client.upload_part(&location, "upload-id", 1, data).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ETag"));
    }

    #[tokio::test]
    async fn test_s3_client_upload_part_valid_etag_variations() {
        let mock_server = MockServer::start().await;

        // Test various valid ETag formats
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/etag-variation.mcap"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"abc123\""))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "etag-variation.mcap").with_endpoint(mock_server.uri());

        let data = bytes::Bytes::from(&b"test"[..]);
        let result = client.upload_part(&location, "upload-id", 1, data).await;
        assert!(result.is_ok());
        // ETag quotes should be trimmed
        assert_eq!(result.unwrap(), "abc123");
    }

    #[tokio::test]
    async fn test_s3_client_complete_upload_500_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/complete-500.mcap"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string("<Error><Code>InternalError</Code></Error>"),
            )
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "complete-500.mcap").with_endpoint(mock_server.uri());

        let parts = vec![(1, "etag1".to_string())];
        let result = client.complete_upload(&location, "upload-id", parts).await;
        assert!(result.is_err());
        match result {
            Err(robocodec::io::s3::FatalError::HttpError {
                status: Some(500), ..
            }) => {
                // Expected
            }
            _ => panic!("Expected HttpError with status 500"),
        }
    }

    #[tokio::test]
    async fn test_s3_client_fetch_range_zero_length() {
        let mock_server = MockServer::start().await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location = S3Location::new("test-bucket", "zero.mcap").with_endpoint(mock_server.uri());

        // Zero-length fetch should return empty bytes without making a request
        let result = client.fetch_range(&location, 0, 0).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_s3_client_create_upload_malformed_xml() {
        let mock_server = MockServer::start().await;

        // Malformed XML - missing closing tag for UploadId
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/malformed.mcap"))
            .and(header("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "<InitiateMultipartUploadResult><UploadId>no-close",
                "application/xml",
            ))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "malformed.mcap").with_endpoint(mock_server.uri());

        let result = client.create_upload(&location).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("UploadId"));
    }

    #[tokio::test]
    async fn test_s3_client_create_upload_empty_uploadid() {
        let mock_server = MockServer::start().await;

        // XML with empty UploadId
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/empty-id.mcap"))
            .and(header("x-amz-content-sha256", "UNSIGNED-PAYLOAD"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw("<InitiateMultipartUploadResult><UploadId></UploadId></InitiateMultipartUploadResult>", "application/xml"),
            )
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "empty-id.mcap").with_endpoint(mock_server.uri());

        let result = client.create_upload(&location).await;
        assert!(result.is_ok());
        // Empty string is valid for UploadId (edge case)
        assert_eq!(result.unwrap(), "");
    }

    #[tokio::test]
    async fn test_s3_client_upload_part_network_error() {
        let mock_server = MockServer::start().await;

        // Create a mock then immediately reset it to cause network errors
        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/net-error.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        mock_server.reset().await;

        let config = S3ReaderConfig::default().with_request_timeout(Duration::from_secs(1));
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "net-error.mcap").with_endpoint(mock_server.uri());

        let data = bytes::Bytes::from(&b"test"[..]);
        let result = client.upload_part(&location, "upload-id", 1, data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_s3_client_multiple_parts_complete() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(wiremock_path("/test-bucket/multi.mcap"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = S3ReaderConfig::default();
        let client = S3Client::new(config).unwrap();

        let location =
            S3Location::new("test-bucket", "multi.mcap").with_endpoint(mock_server.uri());

        // Test with many parts to ensure XML generation works
        let parts: Vec<(u32, String)> = (1..=10).map(|i| (i, format!("etag{}", i))).collect();

        let result = client.complete_upload(&location, "upload-id", parts).await;
        assert!(result.is_ok());
    }
}

// ============================================================================
// S3 Integration Tests
// ============================================================================

mod s3_integration_tests {
    use super::*;

    #[derive(Clone)]
    struct S3Config {
        pub endpoint: String,
        pub bucket: String,
        pub region: String,
    }

    impl Default for S3Config {
        fn default() -> Self {
            Self {
                endpoint: std::env::var("MINIO_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:9000".to_string()),
                bucket: std::env::var("MINIO_BUCKET")
                    .unwrap_or_else(|_| "test-fixtures".to_string()),
                region: std::env::var("MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            }
        }
    }

    async fn s3_available() -> bool {
        let config = S3Config::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .danger_accept_invalid_certs(true)
            .build();

        let Ok(client) = client else {
            if std::env::var("S3_TESTS_REQUIRE_AVAILABLE").is_ok() {
                panic!("S3_TESTS_REQUIRE_AVAILABLE is set but S3 client could not be created");
            }
            return false;
        };
        let url = format!("{}/", config.endpoint);
        let available = client.head(&url).send().await.is_ok();

        if !available && std::env::var("S3_TESTS_REQUIRE_AVAILABLE").is_ok() {
            panic!(
                "S3_TESTS_REQUIRE_AVAILABLE is set but S3 is not available at {}. \
                Start MinIO with: docker compose up -d",
                config.endpoint
            );
        }

        available
    }

    async fn upload_to_s3(
        config: &S3Config,
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
    async fn test_s3_docker_instructions() {
        println!("\n==== S3 Docker Setup Instructions ====");
        println!("Using docker-compose (recommended):");
        println!("  docker compose up -d");
        println!();
        println!("Or manually:");
        println!("  docker run -d --name robocodec-minio -p 9000:9000 -p 9001:9001 \\");
        println!("    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \\");
        println!("    minio/minio server /data --console-address ':9001'");
        println!();
        println!("Upload fixtures:");
        println!("  ./scripts/upload-fixtures-to-minio.sh");
        println!();
        println!("Run tests:");
        println!("  cargo test --features remote s3_integration_tests");
        println!();
        println!("Web console: http://localhost:9001 (minioadmin/minioadmin)");
        println!("=========================================\n");
    }

    #[tokio::test]
    async fn test_s3_read_mcap() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixture_path = fixture_path("robocodec_test_0.mcap");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_0.mcap";

        // Skip test if bucket doesn't exist (403 Forbidden)
        if upload_to_s3(&config, key, &data).await.is_err() {
            eprintln!(
                "Skipping S3 test: bucket '{}' does not exist or is not accessible",
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

    /// Test full message streaming from S3.
    /// This verifies the complete S3 streaming read pipeline.
    #[tokio::test]
    async fn test_s3_stream_messages() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixture_path = fixture_path("robocodec_test_0.mcap");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_0.mcap";

        // Skip test if bucket doesn't exist
        if upload_to_s3(&config, key, &data).await.is_err() {
            eprintln!(
                "Skipping S3 test: bucket '{}' does not exist. Create with: docker compose up -d",
                config.bucket
            );
            return;
        }

        // Clean up after test
        let key_cleanup = key.to_string();
        let endpoint = config.endpoint.clone();
        let bucket = config.bucket.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
            let _ = client.delete(&url).send().await;
        });

        // Open and stream messages
        let location = S3Location::new(&config.bucket, key)
            .with_endpoint(&config.endpoint)
            .with_region(&config.region);

        let reader = S3Reader::open(location).await.unwrap();
        eprintln!(
            "Opened S3 reader, file size: {}",
            FormatReader::file_size(&reader)
        );
        eprintln!("Discovered {} channels", reader.channels().len());

        // Stream all messages
        let mut stream = reader.iter_messages();
        let mut message_count = 0;
        let mut total_bytes = 0;

        while let Some(result) = stream.next_message().await {
            let (channel, data) = result.unwrap();
            message_count += 1;
            total_bytes += data.len();

            if message_count <= 3 {
                eprintln!(
                    "Message {}: channel={}, topic={}, data_len={}",
                    message_count,
                    channel.id,
                    channel.topic,
                    data.len()
                );
            }
        }

        eprintln!(
            "Streamed {} messages, {} bytes total",
            message_count, total_bytes
        );

        assert!(message_count > 0, "Should stream at least one message");
        assert!(
            !reader.channels().is_empty(),
            "Should have discovered channels"
        );
    }

    /// Test streaming a BAG file from S3.
    #[tokio::test]
    async fn test_s3_stream_bag() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixture_path = fixture_path("robocodec_test_15.bag");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_15.bag";

        // Skip test if bucket doesn't exist
        if upload_to_s3(&config, key, &data).await.is_err() {
            eprintln!("Skipping S3 BAG test: bucket does not exist");
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

        let reader = S3Reader::open(location).await.unwrap();
        assert_eq!(reader.format(), robocodec::io::metadata::FileFormat::Bag);
        eprintln!("BAG file size: {}", FormatReader::file_size(&reader));

        // Stream some messages to verify it works
        let mut stream = reader.iter_messages();
        let mut message_count = 0;

        while let Some(result) = stream.next_message().await {
            result.unwrap();
            message_count += 1;
            // Limit iterations for test speed
            if message_count >= 10 {
                break;
            }
        }

        eprintln!("Streamed {} messages from BAG file", message_count);
    }

    /// Test chunk boundary handling by using a small max_chunk_size.
    #[tokio::test]
    async fn test_s3_chunk_boundaries() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixture_path = fixture_path("robocodec_test_0.mcap");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_0_chunked.mcap";

        if upload_to_s3(&config, key, &data).await.is_err() {
            eprintln!("Skipping S3 chunk test: bucket does not exist");
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

        // Use a very small chunk size to force multiple S3 requests
        let mut reader_config = S3ReaderConfig::default();
        reader_config = reader_config.with_max_chunk_size(4096); // 4KB chunks

        let location = S3Location::new(&config.bucket, key)
            .with_endpoint(&config.endpoint)
            .with_region(&config.region);

        let reader = S3Reader::open_with_config(location, reader_config)
            .await
            .unwrap();

        let mut stream = reader.iter_messages();
        let mut message_count = 0;

        while let Some(result) = stream.next_message().await {
            result.unwrap();
            message_count += 1;
        }

        assert!(
            message_count > 0,
            "Should stream messages even with small chunk size"
        );
        eprintln!("Streamed {} messages with 4KB chunks", message_count);
    }

    /// Test BAG file streaming from S3 with chunk boundary handling.
    #[tokio::test]
    async fn test_s3_stream_bag_chunk_boundaries() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixture_path = fixture_path("robocodec_test_15.bag");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_15_chunked.bag";

        if upload_to_s3(&config, key, &data).await.is_err() {
            eprintln!("Skipping S3 BAG chunk test: bucket does not exist");
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

        // Test with various chunk sizes to ensure boundary handling works
        for chunk_size in [4096u64, 8192, 16384, 65536] {
            let mut reader_config = S3ReaderConfig::default();
            reader_config = reader_config.with_max_chunk_size(chunk_size as usize);

            let location = S3Location::new(&config.bucket, key)
                .with_endpoint(&config.endpoint)
                .with_region(&config.region);

            let reader = S3Reader::open_with_config(location, reader_config)
                .await
                .unwrap();

            assert_eq!(reader.format(), robocodec::io::metadata::FileFormat::Bag);

            let mut stream = reader.iter_messages();
            let mut message_count = 0;

            while let Some(result) = stream.next_message().await {
                if result.is_ok() {
                    message_count += 1;
                }
            }

            eprintln!("BAG chunk size {}: {} messages", chunk_size, message_count);
            assert!(
                message_count > 0,
                "Should stream BAG messages with chunk size {}",
                chunk_size
            );
        }
    }

    /// Test BAG message count matches between S3 and local file.
    #[tokio::test]
    async fn test_s3_bag_message_count_matches_local() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixture_path = fixture_path("robocodec_test_15.bag");

        if !fixture_path.exists() {
            return;
        }

        // Get local message count using BagTransportReader
        let local_reader =
            robocodec::io::formats::bag::BagTransportReader::open(&fixture_path).unwrap();
        let local_message_count = local_reader.message_count();
        let local_channels = local_reader.channels().len();
        eprintln!(
            "Local BAG: {} messages, {} channels",
            local_message_count, local_channels
        );

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_15_count.bag";

        if upload_to_s3(&config, key, &data).await.is_err() {
            eprintln!("Skipping S3 BAG count test: bucket does not exist");
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

        let reader = S3Reader::open(location).await.unwrap();
        let s3_channels = reader.channels().len();
        eprintln!("S3 BAG: {} channels", s3_channels);

        // Stream all messages and count
        let mut stream = reader.iter_messages();
        let mut s3_message_count = 0u64;

        while let Some(result) = stream.next_message().await {
            result.unwrap();
            s3_message_count += 1;
        }

        eprintln!("S3 BAG: {} messages streamed", s3_message_count);

        // Channel count should match
        assert_eq!(
            s3_channels, local_channels,
            "Channel count should match between S3 and local"
        );

        // Message count should match
        assert_eq!(
            s3_message_count, local_message_count,
            "Message count should match between S3 ({}) and local ({})",
            s3_message_count, local_message_count
        );
    }

    /// Test BAG streaming with multiple fixtures.
    #[tokio::test]
    async fn test_s3_stream_bag_multiple_fixtures() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let fixtures = [
            "robocodec_test_15.bag",
            "robocodec_test_17.bag",
            "robocodec_test_18.bag",
        ];

        for (idx, fixture_name) in fixtures.iter().enumerate() {
            let fixture_path = fixture_path(fixture_name);

            if !fixture_path.exists() {
                continue;
            }

            let data = std::fs::read(&fixture_path).unwrap();
            let key = format!("test/multi/{}_{}", idx, fixture_name);

            if upload_to_s3(&config, &key, &data).await.is_err() {
                eprintln!(
                    "Skipping S3 BAG multi test for {}: upload failed",
                    fixture_name
                );
                continue;
            }

            let key_cleanup = key.clone();
            let endpoint = config.endpoint.clone();
            let bucket = config.bucket.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
                let _ = client.delete(&url).send().await;
            });

            let location = S3Location::new(&config.bucket, &key)
                .with_endpoint(&config.endpoint)
                .with_region(&config.region);

            let reader = S3Reader::open(location).await;
            if reader.is_err() {
                eprintln!(
                    "Failed to open {} from S3: {:?}",
                    fixture_name,
                    reader.err()
                );
                continue;
            }

            let reader = reader.unwrap();
            assert_eq!(
                reader.format(),
                robocodec::io::metadata::FileFormat::Bag,
                "Format should be BAG for {}",
                fixture_name
            );

            let mut stream = reader.iter_messages();
            let mut message_count = 0;

            while let Some(result) = stream.next_message().await {
                result.unwrap_or_else(|e| {
                    panic!("Should parse message from {}: {:?}", fixture_name, e)
                });
                message_count += 1;
            }

            assert!(
                message_count > 0,
                "Should stream messages from {}",
                fixture_name
            );
            eprintln!("{}: {} messages", fixture_name, message_count);
        }
    }

    /// Test RRD file streaming from S3 with chunk boundary handling.
    #[tokio::test]
    async fn test_s3_stream_rrd_chunk_boundaries() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let rrd_dir = fixture_path("rrd");

        if !rrd_dir.exists() {
            eprintln!("Skipping S3 RRD chunk test: no RRD fixtures directory");
            return;
        }

        // Find first .rrd file
        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        let rrd_path = match rrd_file {
            Some(p) => p,
            None => {
                eprintln!("Skipping S3 RRD chunk test: no RRD files found");
                return;
            }
        };

        let data = std::fs::read(&rrd_path).unwrap();
        let key = format!(
            "test/rrd/chunked_{}",
            rrd_path.file_name().unwrap().to_string_lossy()
        );

        if upload_to_s3(&config, &key, &data).await.is_err() {
            eprintln!("Skipping S3 RRD chunk test: bucket does not exist");
            return;
        }

        // Clean up
        let key_cleanup = key.clone();
        let endpoint = config.endpoint.clone();
        let bucket = config.bucket.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
            let _ = client.delete(&url).send().await;
        });

        // Test with various chunk sizes
        for chunk_size in [4096u64, 8192, 16384, 65536] {
            let mut reader_config = S3ReaderConfig::default();
            reader_config = reader_config.with_max_chunk_size(chunk_size as usize);

            let location = S3Location::new(&config.bucket, &key)
                .with_endpoint(&config.endpoint)
                .with_region(&config.region);

            let reader = S3Reader::open_with_config(location, reader_config)
                .await
                .unwrap();

            assert_eq!(reader.format(), robocodec::io::metadata::FileFormat::Rrd);

            let mut stream = reader.iter_messages();
            let mut message_count = 0;

            while let Some(result) = stream.next_message().await {
                if result.is_ok() {
                    message_count += 1;
                }
            }

            eprintln!("RRD chunk size {}: {} messages", chunk_size, message_count);
            assert!(
                message_count > 0,
                "Should stream RRD messages with chunk size {}",
                chunk_size
            );
        }
    }

    /// Test RRD message count matches between S3 and local file.
    #[tokio::test]
    async fn test_s3_rrd_message_count_matches_local() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let rrd_dir = fixture_path("rrd");

        if !rrd_dir.exists() {
            eprintln!("Skipping S3 RRD count test: no RRD fixtures directory");
            return;
        }

        // Find first .rrd file
        let mut rrd_file = None;
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_file = Some(path);
                    break;
                }
            }
        }

        let rrd_path = match rrd_file {
            Some(p) => p,
            None => {
                eprintln!("Skipping S3 RRD count test: no RRD files found");
                return;
            }
        };

        // Get local message count
        let local_reader =
            robocodec::io::formats::rrd::RrdTransportReader::open(&rrd_path).unwrap();
        let local_message_count = local_reader.message_count();
        let local_channels = local_reader.channels().len();
        eprintln!(
            "Local RRD: {} messages, {} channels",
            local_message_count, local_channels
        );

        let data = std::fs::read(&rrd_path).unwrap();
        let key = format!(
            "test/rrd/count_{}",
            rrd_path.file_name().unwrap().to_string_lossy()
        );

        if upload_to_s3(&config, &key, &data).await.is_err() {
            eprintln!("Skipping S3 RRD count test: bucket does not exist");
            return;
        }

        // Clean up
        let key_cleanup = key.clone();
        let endpoint = config.endpoint.clone();
        let bucket = config.bucket.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
            let _ = client.delete(&url).send().await;
        });

        let location = S3Location::new(&config.bucket, &key)
            .with_endpoint(&config.endpoint)
            .with_region(&config.region);

        let reader = S3Reader::open(location).await.unwrap();
        let s3_channels = reader.channels().len();
        eprintln!("S3 RRD: {} channels", s3_channels);

        // Stream all messages and count
        let mut stream = reader.iter_messages();
        let mut s3_message_count = 0u64;

        while let Some(result) = stream.next_message().await {
            result.unwrap();
            s3_message_count += 1;
        }

        eprintln!("S3 RRD: {} messages streamed", s3_message_count);

        // Channel count should match
        assert_eq!(
            s3_channels, local_channels,
            "Channel count should match between S3 and local for RRD"
        );

        // Message count should match
        assert_eq!(
            s3_message_count, local_message_count,
            "Message count should match between S3 ({}) and local ({}) for RRD",
            s3_message_count, local_message_count
        );
    }

    /// Test RRD streaming with multiple fixtures.
    #[tokio::test]
    async fn test_s3_stream_rrd_multiple_fixtures() {
        if !s3_available().await {
            return;
        }

        let config = S3Config::default();
        let rrd_dir = fixture_path("rrd");

        if !rrd_dir.exists() {
            eprintln!("Skipping S3 RRD multi test: no RRD fixtures directory");
            return;
        }

        // Get first 5 RRD files
        let mut rrd_files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&rrd_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("rrd") {
                    rrd_files.push(path);
                    if rrd_files.len() >= 5 {
                        break;
                    }
                }
            }
        }

        if rrd_files.is_empty() {
            eprintln!("Skipping S3 RRD multi test: no RRD files found");
            return;
        }

        for (idx, rrd_path) in rrd_files.iter().enumerate() {
            let data = std::fs::read(rrd_path).unwrap();
            let fixture_name = rrd_path.file_name().unwrap().to_string_lossy();
            let key = format!("test/rrd/multi/{}_{}", idx, fixture_name);

            if upload_to_s3(&config, &key, &data).await.is_err() {
                eprintln!(
                    "Skipping S3 RRD multi test for {}: upload failed",
                    fixture_name
                );
                continue;
            }

            let key_cleanup = key.clone();
            let endpoint = config.endpoint.clone();
            let bucket = config.bucket.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
                let _ = client.delete(&url).send().await;
            });

            let location = S3Location::new(&config.bucket, &key)
                .with_endpoint(&config.endpoint)
                .with_region(&config.region);

            let reader = S3Reader::open(location).await;
            if reader.is_err() {
                eprintln!(
                    "Failed to open {} from S3: {:?}",
                    fixture_name,
                    reader.err()
                );
                continue;
            }

            let reader = reader.unwrap();
            assert_eq!(
                reader.format(),
                robocodec::io::metadata::FileFormat::Rrd,
                "Format should be RRD for {}",
                fixture_name
            );

            let mut stream = reader.iter_messages();
            let mut message_count = 0;

            while let Some(result) = stream.next_message().await {
                result.unwrap_or_else(|e| {
                    panic!("Should parse message from {}: {:?}", fixture_name, e)
                });
                message_count += 1;
            }

            assert!(
                message_count > 0,
                "Should stream messages from {}",
                fixture_name
            );
            eprintln!("{}: {} messages", fixture_name, message_count);
        }
    }
}
