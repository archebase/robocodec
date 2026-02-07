// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for MCAP streaming parser.

#[cfg(feature = "remote")]
use robocodec::io::s3::{FatalError, MCAP_MAGIC, StreamingMcapParser};
#[cfg(feature = "remote")]
use robocodec::io::streaming::StreamingParser;

#[cfg(feature = "remote")]
#[test]
fn test_mcap_stream_parser_new() {
    let parser = StreamingMcapParser::new();
    assert!(!parser.is_initialized());
    assert!(!parser.has_channels());
    assert_eq!(parser.message_count(), 0);
}

#[cfg(feature = "remote")]
#[test]
fn test_mcap_stream_parser_default() {
    let parser = StreamingMcapParser::default();
    assert_eq!(parser.message_count(), 0);
}

#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
#[test]
fn test_mcap_stream_channels_empty() {
    let parser = StreamingMcapParser::new();
    assert!(parser.channels().is_empty());
}

#[cfg(feature = "remote")]
#[test]
fn test_mcap_stream_parse_chunk_incomplete() {
    let mut parser = StreamingMcapParser::new();

    // Send incomplete magic
    let result = parser.parse_chunk(&MCAP_MAGIC[..4]);
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // No messages yet
    assert!(!parser.is_initialized());
}

// ============================================================================
// Public API Tests
// ============================================================================

/// Test that MCAP files can be read using the public API (RoboReader).
/// This ensures the public API provides equivalent functionality to internal streaming parsers.
#[cfg(feature = "remote")]
#[test]
fn test_public_api_robo_reader_mcap() {
    use robocodec::{FormatReader, RoboReader};
    use std::path::Path;

    // Use a standard fixture file
    let fixture_path = Path::new("tests/fixtures/robocodec_test_0.mcap");
    if !fixture_path.exists() {
        return; // Skip test if fixture doesn't exist
    }

    // Verify RoboReader (public API) can read the MCAP file
    let reader =
        RoboReader::open(fixture_path.to_str().unwrap()).expect("RoboReader should open MCAP file");
    let channels = reader.channels();

    // Should have successfully read channels
    eprintln!("RoboReader found {} channels", channels.len());
    assert!(!channels.is_empty(), "Should have at least one channel");

    // Verify we can iterate over messages using public API
    let iter = reader.decoded().expect("Should get decoded iterator");
    let mut count = 0;
    for result in iter.take(10) {
        if result.is_ok() {
            count += 1;
        }
    }
    eprintln!("RoboReader read {} messages (sampled)", count);
}
