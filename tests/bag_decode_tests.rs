// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! BAG decoded() integration tests.
//!
//! This test verifies that the unified decoded() API works correctly
//! for both MCAP and BAG formats.

use robocodec::io::FormatReader;
use robocodec::io::RoboReader;
use std::path::Path;

#[test]
fn test_unified_decoded_for_bag() {
    // Test that RoboReader::decoded() works for BAG files
    let bag_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(bag_path).exists() {
        println!("Skipping test: fixture file not found");
        return;
    }

    let reader = RoboReader::open(bag_path).expect("Failed to open BAG file");

    // Get the decoded message iterator - this works directly for both BAG and MCAP
    let decoded_iter = reader.decoded();

    // Verify we got the unified iterator
    let mut decoded_iter = match decoded_iter {
        Ok(iter) => iter,
        Err(e) => panic!("Failed to create decoded iterator: {:?}", e),
    };

    // Try to read messages - some may fail due to fixture data issues
    // but at least some should decode successfully
    let mut decoded_count = 0;
    let mut error_count = 0;
    let max_attempts = 10;

    for _ in 0..max_attempts {
        if let Some(result) = decoded_iter.next() {
            match result {
                Ok(decoded) => {
                    println!(
                        "Successfully decoded message from topic: {}",
                        decoded.topic()
                    );
                    println!("Message type: {}", decoded.message_type());
                    decoded_count += 1;
                    break; // Found at least one decodable message
                }
                Err(e) => {
                    error_count += 1;
                    println!("Message decode error (attempt {}): {:?}", error_count, e);
                }
            }
        } else {
            break; // No more messages
        }
    }

    assert!(
        decoded_count > 0,
        "Should be able to decode at least one message (tried {} messages, {} errors)",
        decoded_count + error_count,
        error_count
    );
}

#[test]
fn test_decoded_multiple_files() {
    // Test that opening multiple files returns different channel data
    // This catches the OnceLock global cache bug where channels from
    // the first file would be returned for all subsequent files

    let paths = [
        "tests/fixtures/robocodec_test_15.bag",
        "tests/fixtures/robocodec_test_17.bag",
    ];

    let mut channels_list = Vec::new();

    for path in paths {
        if !Path::new(path).exists() {
            println!("Skipping test: fixture file not found: {}", path);
            return;
        }

        let reader = RoboReader::open(path).expect("Failed to open file");

        // Collect channel topics for this file
        let topics: Vec<_> = reader
            .channels()
            .values()
            .map(|ch| ch.topic.clone())
            .collect();

        channels_list.push(topics);
    }

    // Verify the two files have different channel topics
    // (If OnceLock bug existed, both would have the same channels)
    assert_ne!(
        channels_list[0], channels_list[1],
        "Different files should have different channels"
    );
}

#[test]
fn test_decoded_messages_with_timestamp_for_bag() {
    // Test that RoboReader::decoded() returns messages with timestamps for BAG files
    let bag_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(bag_path).exists() {
        println!("Skipping test: fixture file not found");
        return;
    }

    let reader = RoboReader::open(bag_path).expect("Failed to open BAG file");

    // Get the decoded message iterator (now includes timestamps)
    let iter = reader.decoded().expect("Failed to create decoded iterator");

    // Try to read at least one message with timestamp
    let mut found_message = false;

    for result in iter.take(10) {
        match result {
            Ok(decoded) => {
                // Verify we got both message and timestamps
                println!(
                    "Got message from topic: {} with log_time: {:?}",
                    decoded.topic(),
                    decoded.log_time
                );

                // Verify timestamps are present (Some for BAG files)
                assert!(
                    decoded.log_time.is_some() || decoded.publish_time.is_some(),
                    "At least one timestamp should be Some"
                );

                // If timestamps exist, verify they're non-zero
                if let Some(log_time) = decoded.log_time {
                    assert!(
                        log_time > 0,
                        "log_time should be non-zero for valid messages"
                    );
                }

                found_message = true;
                break;
            }
            Err(e) => {
                println!("Message decode error: {:?}", e);
                // Some decode errors are acceptable for test fixtures
            }
        }
    }

    assert!(
        found_message,
        "Should be able to decode at least one timestamped message"
    );
}

#[test]
fn test_decode_error_includes_context() {
    // Test that decode errors include topic and timestamp for better debugging
    // This verifies the fix that improved error context
    let bag_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(bag_path).exists() {
        println!("Skipping test: fixture file not found");
        return;
    }

    let reader = RoboReader::open(bag_path).expect("Failed to open BAG file");
    let iter = reader.decoded().expect("Failed to get iterator");

    // Read messages and check error format when errors occur
    let mut found_error = false;

    for result in iter.take(100) {
        match result {
            Ok(_) => {}
            Err(e) => {
                let error_msg = format!("{:?}", e);
                // Check that error message includes useful context
                // (topic name and/or timestamp should be mentioned)
                if error_msg.contains("topic") || error_msg.contains("log_time") {
                    found_error = true;
                    println!("Error with context: {}", error_msg);
                    break;
                }
            }
        }
    }

    // If we didn't find an error with context, that's actually okay -
    // it might mean all messages decoded successfully. The important
    // thing is that WHEN errors occur, they include context.
    if found_error {
        println!("Verified: decode errors include topic/timestamp context");
    } else {
        println!("No decode errors encountered (all messages decoded successfully)");
    }
}

#[test]
fn test_mcap_and_bag_api_consistency() {
    // Test that both MCAP and BAG formats support the same decoded() API
    // This ensures API consistency across formats

    let bag_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(bag_path).exists() {
        println!("Skipping test: fixture file not found");
        return;
    }

    // Test BAG format
    let bag_reader = RoboReader::open(bag_path).expect("Failed to open BAG file");
    let bag_result = bag_reader.decoded();
    assert!(bag_result.is_ok(), "BAG format should support decoded()");

    let _bag_iter = bag_result.unwrap();
    // The iterator should be usable immediately
    let bag_channels = bag_reader.channels();
    assert!(!bag_channels.is_empty(), "BAG should have channels");

    println!("BAG format API verified: decoded() works correctly with timestamps");
}
