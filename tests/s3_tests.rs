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
//! - MinIO integration tests

use std::path::PathBuf;
use std::time::Duration;

use robocodec::io::s3::{
    MCAP_MAGIC, S3Client, S3Location, S3Reader, S3ReaderConfig, S3ReaderConstructor,
    StreamingBagParser, StreamingMcapParser, SummarySchemaInfo,
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
            mcap_data.extend_from_slice(&[0u8; 22]);
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

        use robocodec::io::formats::bag::SequentialBagReader;
        let reader = SequentialBagReader::open(&path).unwrap();
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
        println!("  cargo test --features s3 minio_tests");
        println!();
        println!("Web console: http://localhost:9001 (minioadmin/minioadmin)");
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

    /// Test full message streaming from MinIO.
    /// This verifies the complete S3 streaming read pipeline.
    #[tokio::test]
    async fn test_minio_stream_messages() {
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

        // Skip test if bucket doesn't exist
        if upload_to_minio(&config, key, &data).await.is_err() {
            eprintln!(
                "Skipping MinIO test: bucket '{}' does not exist. Create with: docker compose up -d",
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

    /// Test streaming a BAG file from MinIO.
    #[tokio::test]
    async fn test_minio_stream_bag() {
        if !minio_available().await {
            return;
        }

        let config = MinIOConfig::default();
        let fixture_path = fixture_path("robocodec_test_15.bag");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_15.bag";

        // Skip test if bucket doesn't exist
        if upload_to_minio(&config, key, &data).await.is_err() {
            eprintln!("Skipping MinIO BAG test: bucket does not exist");
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
    async fn test_minio_chunk_boundaries() {
        if !minio_available().await {
            return;
        }

        let config = MinIOConfig::default();
        let fixture_path = fixture_path("robocodec_test_0.mcap");

        if !fixture_path.exists() {
            return;
        }

        let data = std::fs::read(&fixture_path).unwrap();
        let key = "test/robocodec_test_0_chunked.mcap";

        if upload_to_minio(&config, key, &data).await.is_err() {
            eprintln!("Skipping MinIO chunk test: bucket does not exist");
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
}
