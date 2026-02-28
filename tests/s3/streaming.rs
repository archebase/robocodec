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
    assert!(
        path.exists(),
        "Fixture required for streaming test is missing: {}",
        path.display()
    );

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
    assert!(
        path.exists(),
        "Fixture required for streaming test is missing: {}",
        path.display()
    );

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

    // Header record (profile + library)
    let profile = b"";
    let library = b"test";
    let header_len = 4 + profile.len() + 4 + library.len();
    mcap_data.push(0x01); // OP_HEADER
    mcap_data.extend_from_slice(&(header_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&(profile.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(profile);
    mcap_data.extend_from_slice(&(library.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(library);

    // Schema record (using correct MCAP format with u32 lengths)
    let schema_name = b"Foo";
    let schema_encoding = b"ros2msg";
    let schema_data = b"# test";
    let schema_len = 2 + 4 + schema_name.len() + 4 + schema_encoding.len() + 4 + schema_data.len();
    mcap_data.push(0x03); // OP_SCHEMA
    mcap_data.extend_from_slice(&(schema_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // id = 1
    mcap_data.extend_from_slice(&(schema_name.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_name);
    mcap_data.extend_from_slice(&(schema_encoding.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_encoding);
    mcap_data.extend_from_slice(&(schema_data.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_data);

    // Channel record (using correct MCAP format with u32 lengths)
    let topic = b"/test";
    let msg_encoding = b"cdr";
    let channel_len = 2 + 2 + 4 + topic.len() + 4 + msg_encoding.len() + 4;
    mcap_data.push(0x04); // OP_CHANNEL
    mcap_data.extend_from_slice(&(channel_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // channel_id = 1
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // schema_id = 1
    mcap_data.extend_from_slice(&(topic.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(topic);
    mcap_data.extend_from_slice(&(msg_encoding.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(msg_encoding);
    mcap_data.extend_from_slice(&0u32.to_le_bytes()); // metadata count = 0

    // Message record
    let msg_data = b"hello";
    let msg_len = 2 + 4 + 8 + 8 + msg_data.len();
    mcap_data.push(0x05); // OP_MESSAGE
    mcap_data.extend_from_slice(&(msg_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // channel_id = 1
    mcap_data.extend_from_slice(&1u32.to_le_bytes()); // sequence = 1
    mcap_data.extend_from_slice(&0u64.to_le_bytes()); // log_time = 0
    mcap_data.extend_from_slice(&0u64.to_le_bytes()); // publish_time = 0
    mcap_data.extend_from_slice(msg_data);

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

    // Header record (profile + library)
    let profile = b"";
    let library = b"test";
    let header_len = 4 + profile.len() + 4 + library.len();
    mcap_data.push(0x01); // OP_HEADER
    mcap_data.extend_from_slice(&(header_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&(profile.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(profile);
    mcap_data.extend_from_slice(&(library.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(library);

    // Schema record (using correct MCAP format with u32 lengths)
    let schema_name = b"Foo";
    let schema_encoding = b"ros2msg";
    let schema_data = b"# test";
    let schema_len = 2 + 4 + schema_name.len() + 4 + schema_encoding.len() + 4 + schema_data.len();
    mcap_data.push(0x03); // OP_SCHEMA
    mcap_data.extend_from_slice(&(schema_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // id = 1
    mcap_data.extend_from_slice(&(schema_name.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_name);
    mcap_data.extend_from_slice(&(schema_encoding.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_encoding);
    mcap_data.extend_from_slice(&(schema_data.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_data);

    // Channel record (using correct MCAP format with u32 lengths)
    let topic = b"/test";
    let msg_encoding = b"cdr";
    let channel_len = 2 + 2 + 4 + topic.len() + 4 + msg_encoding.len() + 4;
    mcap_data.push(0x04); // OP_CHANNEL
    mcap_data.extend_from_slice(&(channel_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // channel_id = 1
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // schema_id = 1
    mcap_data.extend_from_slice(&(topic.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(topic);
    mcap_data.extend_from_slice(&(msg_encoding.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(msg_encoding);
    mcap_data.extend_from_slice(&0u32.to_le_bytes()); // metadata count = 0

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

    // Header record (profile + library)
    let profile = b"";
    let library = b"test";
    let header_len = 4 + profile.len() + 4 + library.len();
    mcap_data.push(0x01); // OP_HEADER
    mcap_data.extend_from_slice(&(header_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&(profile.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(profile);
    mcap_data.extend_from_slice(&(library.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(library);

    // Schema record (using correct MCAP format with u32 lengths)
    let schema_name = b"Foo";
    let schema_encoding = b"ros2msg";
    let schema_data = b"# test";
    let schema_len = 2 + 4 + schema_name.len() + 4 + schema_encoding.len() + 4 + schema_data.len();
    mcap_data.push(0x03); // OP_SCHEMA
    mcap_data.extend_from_slice(&(schema_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // id = 1
    mcap_data.extend_from_slice(&(schema_name.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_name);
    mcap_data.extend_from_slice(&(schema_encoding.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_encoding);
    mcap_data.extend_from_slice(&(schema_data.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(schema_data);

    // Channel record (using correct MCAP format with u32 lengths)
    let topic = b"/test";
    let msg_encoding = b"cdr";
    let channel_len = 2 + 2 + 4 + topic.len() + 4 + msg_encoding.len() + 4;
    mcap_data.push(0x04); // OP_CHANNEL
    mcap_data.extend_from_slice(&(channel_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // channel_id = 1
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // schema_id = 1
    mcap_data.extend_from_slice(&(topic.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(topic);
    mcap_data.extend_from_slice(&(msg_encoding.len() as u32).to_le_bytes());
    mcap_data.extend_from_slice(msg_encoding);
    mcap_data.extend_from_slice(&0u32.to_le_bytes()); // metadata count = 0

    // Message record
    let msg_data = b"hello";
    let msg_len = 2 + 4 + 8 + 8 + msg_data.len();
    mcap_data.push(0x05); // OP_MESSAGE
    mcap_data.extend_from_slice(&(msg_len as u64).to_le_bytes());
    mcap_data.extend_from_slice(&1u16.to_le_bytes()); // channel_id = 1
    mcap_data.extend_from_slice(&1u32.to_le_bytes()); // sequence = 1
    mcap_data.extend_from_slice(&0u64.to_le_bytes()); // log_time = 0
    mcap_data.extend_from_slice(&0u64.to_le_bytes()); // publish_time = 0
    mcap_data.extend_from_slice(msg_data);

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
    let path = fixture_path("robocodec_test_0.mcap");
    assert!(
        path.exists(),
        "Fixture required for streaming test is missing: {}",
        path.display()
    );

    let data = std::fs::read(&path).unwrap();
    let mut parser = StreamingMcapParser::new();

    // Parse in small chunks to test chunk boundaries
    for (i, chunk) in data.chunks(10).enumerate() {
        let result = parser.parse_chunk(chunk);
        assert!(result.is_ok(), "Chunk {} failed: {:?}", i, result);
    }

    // Verify parser discovered channels/messages from real fixture data.
    assert!(
        parser.channels().len() > 0,
        "Expected at least one channel in fixture"
    );
    assert!(
        parser.message_count() > 0,
        "Expected at least one message in fixture"
    );
}
