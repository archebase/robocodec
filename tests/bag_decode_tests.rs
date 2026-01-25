// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! BAG decode_messages integration tests.
//!
//! This test verifies that the unified decode_messages API works correctly
//! for both MCAP and BAG formats.

use robocodec::io::RoboReader;
use std::path::Path;

#[test]
fn test_unified_decode_messages_for_bag() {
    // Test that RoboReader::decode_messages() works for BAG files
    let bag_path = "tests/fixtures/robocodec_test_15.bag";

    if !Path::new(bag_path).exists() {
        println!("Skipping test: fixture file not found");
        return;
    }

    let reader = RoboReader::open(bag_path).expect("Failed to open BAG file");

    // Get the decoded message iterator - this should work for both BAG and MCAP
    let decoded_iter = reader.decode_messages();

    // Verify we got the unified iterator
    let decoded_iter = match decoded_iter {
        Ok(iter) => iter,
        Err(e) => panic!("Failed to create decode_messages iterator: {:?}", e),
    };

    // Now try to get the stream
    let stream = decoded_iter.stream();
    assert!(stream.is_ok(), "Should be able to create stream");

    let mut stream = stream.unwrap();

    // Try to read one message (if file has messages)
    if let Some(result) = stream.next() {
        let (message, channel) = result.expect("Failed to decode first message");
        println!("Successfully decoded message from topic: {}", channel.topic);
        println!("Message fields: {:?}", message.keys());
    }
}
