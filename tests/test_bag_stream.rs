// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for BAG streaming parser.

#[cfg(feature = "s3")]
use robocodec::io::s3::{
    BagMessageRecord, BagRecordFields, BagRecordHeader, FatalError, StreamingBagParser,
    BAG_MAGIC_PREFIX,
};

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_parser_new() {
    let parser = StreamingBagParser::new();
    assert!(!parser.is_initialized());
    assert!(!parser.has_connections());
    assert_eq!(parser.message_count(), 0);
    assert!(parser.version().is_none());
}

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_parser_default() {
    let parser = StreamingBagParser::default();
    assert_eq!(parser.message_count(), 0);
}

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_constants() {
    assert_eq!(BAG_MAGIC_PREFIX.len(), 9);
    assert_eq!(BAG_MAGIC_PREFIX, b"#ROSBAG V");
}

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_channels_empty() {
    let parser = StreamingBagParser::new();
    assert!(parser.channels().is_empty());
    assert!(parser.conn_id_map().is_empty());
}

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_parse_chunk_incomplete() {
    let mut parser = StreamingBagParser::new();

    // Send incomplete magic
    let result = parser.parse_chunk(b"#ROS");
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // No messages yet
    assert!(!parser.is_initialized());
}

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_parse_field_conn() {
    let mut fields = BagRecordFields::default();
    let conn_bytes = [1u8, 0, 0, 0];
    StreamingBagParser::parse_field(&mut fields, b"conn", &conn_bytes);
    assert_eq!(fields.conn, Some(1));
}

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
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

#[cfg(feature = "s3")]
#[test]
fn test_bag_stream_record_fields_default() {
    let fields = BagRecordFields::default();
    assert!(fields.op.is_none());
    assert!(fields.conn.is_none());
    assert!(fields.time.is_none());
    assert!(fields.topic.is_none());
}
