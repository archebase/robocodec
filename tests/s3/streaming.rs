// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming parser tests for S3 functionality.

use robocodec::io::s3::{MCAP_MAGIC, StreamingBagParser, StreamingMcapParser};
use robocodec::io::streaming::StreamingParser;

use super::fixture_path;

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
