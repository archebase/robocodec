// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for RRD S3 streaming with real Rerun RRD files.
//!
//! These tests use actual RRD files from Rerun to verify that the S3 streaming
//! parser works correctly with real-world data in ArrowMsg protobuf format.

#![cfg(feature = "remote")]

use std::fs;
use std::path::Path;

use robocodec::io::formats::rrd::stream::{RRD_STREAM_MAGIC, StreamingRrdParser};
use robocodec::io::s3::StreamingParser;

/// Helper function to load a test fixture file.
fn load_fixture(name: &str) -> Vec<u8> {
    let path = Path::new("tests/fixtures/rrd").join(name);
    fs::read(&path).unwrap_or_else(|_| panic!("Failed to read fixture: {}", name))
}

// ----------------------------------------------------------------------------
// Tests with real Rerun RRD files (ArrowMsg protobuf format)
// ----------------------------------------------------------------------------

#[test]
fn test_rerun_file1_parses() {
    let data = load_fixture("file1.rrd");

    // Verify RRF2 magic
    assert_eq!(&data[0..4], RRD_STREAM_MAGIC);

    let mut parser = StreamingRrdParser::new();
    let result = parser.parse_chunk(&data);

    // Should parse successfully
    assert!(
        result.is_ok(),
        "Failed to parse rerun file1.rrd: {:?}",
        result.err()
    );

    let messages = result.unwrap();
    println!("Parsed {} messages from file1.rrd", messages.len());

    // Should have parsed some messages
    assert!(!messages.is_empty());
    assert!(parser.is_initialized());
}

#[test]
fn test_rerun_file2_parses() {
    let data = load_fixture("file2.rrd");

    // Verify RRF2 magic
    assert_eq!(&data[0..4], RRD_STREAM_MAGIC);

    let mut parser = StreamingRrdParser::new();
    let result = parser.parse_chunk(&data);

    assert!(
        result.is_ok(),
        "Failed to parse rerun file2.rrd: {:?}",
        result.err()
    );

    let messages = result.unwrap();
    println!("Parsed {} messages from file2.rrd", messages.len());
    assert!(!messages.is_empty());
}

#[test]
fn test_rerun_file3_parses() {
    let data = load_fixture("file3.rrd");

    // Verify RRF2 magic
    assert_eq!(&data[0..4], RRD_STREAM_MAGIC);

    let mut parser = StreamingRrdParser::new();
    let result = parser.parse_chunk(&data);

    assert!(
        result.is_ok(),
        "Failed to parse rerun file3.rrd: {:?}",
        result.err()
    );

    let messages = result.unwrap();
    println!("Parsed {} messages from file3.rrd", messages.len());
    assert!(!messages.is_empty());
}

#[test]
fn test_rerun_all_files_have_valid_magic() {
    let rerun_files = vec!["file1.rrd", "file2.rrd", "file3.rrd"];

    for filename in rerun_files {
        let data = load_fixture(filename);
        assert_eq!(
            &data[0..4],
            RRD_STREAM_MAGIC,
            "Rerun file {} has invalid magic",
            filename
        );
    }
}

// ----------------------------------------------------------------------------
// Comprehensive tests for all 20 rerun RRD files
// ----------------------------------------------------------------------------

#[test]
fn test_rerun_all_files_parse() {
    let rerun_files = vec![
        "file1.rrd",
        "file2.rrd",
        "file3.rrd",
        "file4.rrd",
        "file5.rrd",
        "file6.rrd",
        "file7.rrd",
        "file8.rrd",
        "file9.rrd",
        "file10.rrd",
        "file11.rrd",
        "file12.rrd",
        "file13.rrd",
        "file14.rrd",
        "file15.rrd",
        "file16.rrd",
        "file17.rrd",
        "file18.rrd",
        "file19.rrd",
        "file20.rrd",
    ];

    let mut total_messages = 0;
    let mut total_files = 0;

    for filename in rerun_files {
        let data = load_fixture(filename);

        // Verify RRF2 magic
        assert_eq!(
            &data[0..4],
            RRD_STREAM_MAGIC,
            "Rerun file {} has invalid magic",
            filename
        );

        let mut parser = StreamingRrdParser::new();
        match parser.parse_chunk(&data) {
            Ok(messages) => {
                let msg_count = messages.len();
                total_messages += msg_count;
                total_files += 1;
                println!("{}: {} messages", filename, msg_count);
                assert!(
                    msg_count > 0,
                    "{} should have at least one message",
                    filename
                );
            }
            Err(e) => {
                panic!("Failed to parse {}: {:?}", filename, e);
            }
        }
    }

    println!("Total: {} files, {} messages", total_files, total_messages);
    assert_eq!(total_files, 20, "All 20 rerun files should parse");
}
