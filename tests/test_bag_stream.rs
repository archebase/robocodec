// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for BAG streaming parser.

#[cfg(feature = "remote")]
use robocodec::io::formats::bag::StreamingBagParser;
#[cfg(feature = "remote")]
use robocodec::io::s3::{
    BAG_MAGIC_PREFIX, BagMessageRecord, BagRecordFields, BagRecordHeader, FatalError,
};
#[cfg(feature = "remote")]
use std::path::Path;

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parser_new() {
    let parser = StreamingBagParser::new();
    assert!(!parser.is_initialized());
    assert!(!parser.has_connections());
    assert_eq!(parser.message_count(), 0);
    assert!(parser.version().is_none());
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parser_default() {
    let parser = StreamingBagParser::default();
    assert_eq!(parser.message_count(), 0);
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parse_magic() {
    let mut parser = StreamingBagParser::new();

    // Too short - should not error, just not advance
    let result = parser.parse_chunk(b"#ROS");
    assert!(result.is_ok());
    assert!(!parser.is_initialized());

    // Full magic with version
    let result = parser.parse_chunk(b"BAG V2.0\n");
    assert!(result.is_ok());
    assert!(parser.is_initialized());
    assert_eq!(parser.version(), Some("2.0"));
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parse_invalid_magic() {
    let mut parser = StreamingBagParser::new();

    let result = parser.parse_chunk(b"INVALID_MAGIC");
    assert!(result.is_err());

    match result {
        Err(FatalError::InvalidFormat { expected, .. }) => {
            assert_eq!(expected, "BAG magic (#ROSBAG V)");
        }
        _ => {
            panic!("Expected InvalidFormat error");
        }
    }
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_constants() {
    assert_eq!(BAG_MAGIC_PREFIX.len(), 9);
    assert_eq!(BAG_MAGIC_PREFIX, b"#ROSBAG V");
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_channels_empty() {
    let parser = StreamingBagParser::new();
    assert!(parser.channels().is_empty());
    assert!(parser.conn_id_map().is_empty());
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parse_chunk_incomplete() {
    let mut parser = StreamingBagParser::new();

    // Send incomplete magic
    let result = parser.parse_chunk(b"#ROS");
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // No messages yet
    assert!(!parser.is_initialized());
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parse_record_header() {
    // Build a simple header with op=0x02 (MSG_DATA)
    let mut header_bytes = Vec::new();
    // Field 1: op=\x02 (field_len = 4)
    header_bytes.extend(&4u32.to_le_bytes());
    header_bytes.extend(b"op=\x02");

    let fields = StreamingBagParser::parse_record_header(&header_bytes);
    assert!(fields.is_ok());
    let fields = fields.unwrap();
    assert_eq!(fields.op, Some(0x02));
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parse_field_conn() {
    let mut fields = BagRecordFields::default();
    let conn_bytes = [1u8, 0, 0, 0];
    StreamingBagParser::parse_field(&mut fields, b"conn", &conn_bytes);
    assert_eq!(fields.conn, Some(1));
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_parse_field_time() {
    let mut fields = BagRecordFields::default();
    // time = 1234567890 sec + 123456789 nsec
    let mut time_bytes = Vec::new();
    time_bytes.extend(&1234567890u32.to_le_bytes());
    time_bytes.extend(&123456789u32.to_le_bytes());
    StreamingBagParser::parse_field(&mut fields, b"time", &time_bytes);

    let expected_time = 1234567890u64 * 1_000_000_000 + 123456789u64;
    assert_eq!(fields.time, Some(expected_time));
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_message_record() {
    let msg = BagMessageRecord {
        conn_id: 1,
        log_time: 1000,
        data: vec![1, 2, 3],
    };
    assert_eq!(msg.conn_id, 1);
    assert_eq!(msg.log_time, 1000);
    assert_eq!(msg.data, vec![1, 2, 3]);
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_record_header() {
    let header = BagRecordHeader {
        op: 0x02,
        header_len: 10,
        data_len: 100,
    };
    assert_eq!(header.op, 0x02);
    assert_eq!(header.header_len, 10);
    assert_eq!(header.data_len, 100);
}

#[cfg(feature = "remote")]
#[test]
fn test_bag_stream_record_fields_default() {
    let fields = BagRecordFields::default();
    assert!(fields.op.is_none());
    assert!(fields.conn.is_none());
    assert!(fields.time.is_none());
    assert!(fields.topic.is_none());
}

// =========================================================================
// Real fixture file tests - feed actual .bag files through StreamingBagParser
// =========================================================================

#[cfg(feature = "remote")]
/// Helper: read a bag fixture file and parse it through the streaming parser.
/// Returns (total_messages, num_connections, parser).
fn parse_fixture_bag(filename: &str) -> (Vec<BagMessageRecord>, usize, StreamingBagParser) {
    let path = format!("tests/fixtures/{filename}");
    assert!(Path::new(&path).exists(), "Fixture file not found: {path}");

    let data = std::fs::read(&path).unwrap();
    let mut parser = StreamingBagParser::new();

    // Feed the entire file in 256KB chunks to simulate streaming
    let chunk_size = 256 * 1024;
    let mut all_messages = Vec::new();

    for piece in data.chunks(chunk_size) {
        let msgs = parser
            .parse_chunk(piece)
            .unwrap_or_else(|e| panic!("Failed to parse {filename}: {e}"));
        all_messages.extend(msgs);
    }

    let num_connections = parser.channels().len();
    (all_messages, num_connections, parser)
}

#[cfg(feature = "remote")]
#[test]
fn test_fixture_bag_15_streaming() {
    let (messages, num_channels, parser) = parse_fixture_bag("robocodec_test_15.bag");

    assert!(parser.is_initialized());
    assert_eq!(parser.version(), Some("2.0"));
    assert!(num_channels > 0, "Expected at least 1 channel, got 0");
    assert!(
        !messages.is_empty(),
        "Expected messages from robocodec_test_15.bag, got 0"
    );
    assert_eq!(parser.message_count(), messages.len() as u64);

    // Verify all messages have valid conn_id that maps to a known connection
    let channels = parser.channels();
    for msg in &messages {
        assert!(
            channels.contains_key(&(msg.conn_id as u16)),
            "Message references unknown conn_id {}",
            msg.conn_id
        );
    }

    println!(
        "robocodec_test_15.bag: {} messages, {} channels",
        messages.len(),
        num_channels
    );
    for (id, ch) in &channels {
        println!(
            "  channel {id}: topic={}, type={}",
            ch.topic, ch.message_type
        );
    }
}

#[cfg(feature = "remote")]
#[test]
fn test_fixture_bag_18_streaming() {
    // Smaller file (887K), good for quick validation
    let (messages, num_channels, parser) = parse_fixture_bag("robocodec_test_18.bag");

    assert!(parser.is_initialized());
    assert!(num_channels > 0, "Expected at least 1 channel");
    assert!(
        !messages.is_empty(),
        "Expected messages from robocodec_test_18.bag, got 0"
    );

    println!(
        "robocodec_test_18.bag: {} messages, {} channels",
        messages.len(),
        num_channels
    );
}

#[cfg(feature = "remote")]
#[test]
fn test_fixture_bag_19_streaming() {
    let (messages, num_channels, parser) = parse_fixture_bag("robocodec_test_19.bag");

    assert!(parser.is_initialized());
    assert!(num_channels > 0);
    assert!(!messages.is_empty());

    println!(
        "robocodec_test_19.bag: {} messages, {} channels",
        messages.len(),
        num_channels
    );
}

#[cfg(feature = "remote")]
#[test]
fn test_fixture_bag_23_streaming() {
    let (messages, num_channels, parser) = parse_fixture_bag("robocodec_test_23.bag");

    assert!(parser.is_initialized());
    assert!(num_channels > 0);
    assert!(!messages.is_empty());

    println!(
        "robocodec_test_23.bag: {} messages, {} channels",
        messages.len(),
        num_channels
    );
}

#[cfg(feature = "remote")]
#[test]
fn test_fixture_bag_streaming_vs_nonstreaming_consistency() {
    // Compare: streaming parser should discover the same connections and
    // message count as the non-streaming BagParser.
    use robocodec::io::formats::bag::parser::BagParser;

    let bag_path = "tests/fixtures/robocodec_test_18.bag";
    if !Path::new(bag_path).exists() {
        println!("Skipping: fixture not found");
        return;
    }

    // --- Non-streaming parser ---
    let non_streaming = BagParser::open(bag_path).unwrap();
    let ns_conn_count = non_streaming.connections().len();

    // Build the conn_id_map the same way parallel reader does:
    // map each connection ID to a sequential channel index
    let conn_id_map: std::collections::HashMap<u32, u16> = non_streaming
        .connections()
        .keys()
        .enumerate()
        .map(|(i, &conn_id)| (conn_id, i as u16))
        .collect();

    let mut ns_message_count = 0usize;
    for chunk_info in non_streaming.chunks() {
        let decompressed = non_streaming.read_chunk(chunk_info).unwrap();
        let msgs = non_streaming
            .parse_chunk_messages(&decompressed, &conn_id_map)
            .unwrap();
        ns_message_count += msgs.len();
    }

    // --- Streaming parser ---
    let (stream_messages, stream_conn_count, _parser) = parse_fixture_bag("robocodec_test_18.bag");

    println!(
        "Non-streaming: {} connections, {} messages",
        ns_conn_count, ns_message_count
    );
    println!(
        "Streaming:     {} connections, {} messages",
        stream_conn_count,
        stream_messages.len()
    );

    // Connection counts should match
    assert_eq!(
        stream_conn_count, ns_conn_count,
        "Connection count mismatch: streaming={stream_conn_count}, non-streaming={ns_conn_count}"
    );

    // Message counts should match
    assert_eq!(
        stream_messages.len(),
        ns_message_count,
        "Message count mismatch: streaming={}, non-streaming={ns_message_count}",
        stream_messages.len()
    );
}

#[cfg(feature = "remote")]
#[test]
fn test_fixture_bag_small_chunk_streaming() {
    // Test streaming with very small read chunks (64 bytes) to stress
    // the cross-chunk boundary handling
    let path = "tests/fixtures/robocodec_test_19.bag";
    if !Path::new(path).exists() {
        println!("Skipping: fixture not found");
        return;
    }

    let data = std::fs::read(path).unwrap();
    let mut parser = StreamingBagParser::new();

    // Feed in tiny 64-byte chunks
    let mut all_messages = Vec::new();
    for piece in data.chunks(64) {
        let msgs = parser.parse_chunk(piece).unwrap();
        all_messages.extend(msgs);
    }

    assert!(parser.is_initialized());
    assert!(
        !all_messages.is_empty(),
        "Expected messages with 64-byte streaming chunks"
    );
    assert!(!parser.channels().is_empty());

    // Compare with the larger chunk parse
    let (large_chunk_msgs, _, _) = parse_fixture_bag("robocodec_test_19.bag");
    assert_eq!(
        all_messages.len(),
        large_chunk_msgs.len(),
        "64-byte chunks should yield same message count as 256KB chunks"
    );
}

#[cfg(feature = "remote")]
#[test]
fn test_all_fixture_bags_nonzero_messages() {
    // Ensure ALL fixture .bag files produce at least some messages
    let fixtures = [
        "robocodec_test_15.bag",
        "robocodec_test_17.bag",
        "robocodec_test_18.bag",
        "robocodec_test_19.bag",
        "robocodec_test_20.bag",
        "robocodec_test_21.bag",
        "robocodec_test_22.bag",
        "robocodec_test_23.bag",
    ];

    for fixture in &fixtures {
        let path = format!("tests/fixtures/{fixture}");
        if !Path::new(&path).exists() {
            println!("Skipping {fixture}: not found");
            continue;
        }

        let (messages, channels, parser) = parse_fixture_bag(fixture);
        assert!(parser.is_initialized(), "{fixture}: parser not initialized");
        assert!(channels > 0, "{fixture}: no channels discovered");
        assert!(
            !messages.is_empty(),
            "{fixture}: no messages extracted (likely chunk handling bug)"
        );

        println!(
            "{fixture}: {} messages, {} channels, version={:?}",
            messages.len(),
            channels,
            parser.version()
        );
    }
}
