// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for MCAP streaming parser.

#[cfg(feature = "s3")]
use robocodec::io::s3::{FatalError, MCAP_MAGIC, StreamingMcapParser};
#[cfg(feature = "s3")]
use robocodec::io::streaming::StreamingParser;

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_parser_new() {
    let parser = StreamingMcapParser::new();
    assert!(!parser.is_initialized());
    assert!(!parser.has_channels());
    assert_eq!(parser.message_count(), 0);
}

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_parser_default() {
    let parser = StreamingMcapParser::default();
    assert_eq!(parser.message_count(), 0);
}

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_parse_magic() {
    let mut parser = StreamingMcapParser::new();

    // Too short - should not error, just not advance
    let result = parser.parse_chunk(&MCAP_MAGIC[..4]);
    assert!(result.is_ok());
    assert!(!parser.is_initialized());

    // Full magic
    let result = parser.parse_chunk(&MCAP_MAGIC[4..]);
    assert!(result.is_ok());
    assert!(parser.is_initialized());
}

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_parse_invalid_magic() {
    let mut parser = StreamingMcapParser::new();

    let result = parser.parse_chunk(b"INVALID_MAGIC");
    assert!(result.is_err());

    // The mcap crate returns an IoError for bad magic, not InvalidFormat
    // We just check that an error is returned
    if let Err(FatalError::IoError { message }) = result {
        assert!(
            message.contains("Bad magic") || message.contains("magic"),
            "Expected error about bad magic, got: {}",
            message
        );
    } else {
        panic!("Expected IoError about bad magic, got: {:?}", result);
    }
}

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_constants() {
    assert_eq!(MCAP_MAGIC.len(), 8);
    assert_eq!(MCAP_MAGIC[0], 0x89);
    assert_eq!(MCAP_MAGIC[1], b'M');
    assert_eq!(MCAP_MAGIC[2], b'C');
    assert_eq!(MCAP_MAGIC[3], b'A');
    assert_eq!(MCAP_MAGIC[4], b'P');
    assert_eq!(MCAP_MAGIC[5], b'0');
    assert_eq!(MCAP_MAGIC[6], 0x0D);
    assert_eq!(MCAP_MAGIC[7], 0x0A);
}

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_channels_empty() {
    let parser = StreamingMcapParser::new();
    assert!(parser.channels().is_empty());
}

#[cfg(feature = "s3")]
#[test]
fn test_mcap_stream_parse_chunk_incomplete() {
    let mut parser = StreamingMcapParser::new();

    // Send incomplete magic
    let result = parser.parse_chunk(&MCAP_MAGIC[..4]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // No messages yet
    assert!(!parser.is_initialized());
}
