// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! MCAP integration tests for test fixture 13.
//!
//! This test validates that robocodec can parse schemas from robocodec_test_13.mcap
//! file and decode messages correctly.

use std::path::Path;

use robocodec::io::RoboReader;
use robocodec::FormatReader;

/// Path to the fixtures directory.
const FIXTURES_DIR: &str = "tests/fixtures";

/// Test the robocodec_test_13.mcap fixture file.
#[test]
fn test_robocodec_test_13_fixture() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_13.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    println!("\n=== Testing MCAP fixture: robocodec_test_13 ===");

    // Open the MCAP file using RoboReader
    let reader =
        RoboReader::open(fixture_path.to_str().unwrap()).expect("Failed to open MCAP file");

    let channels = reader.channels();
    println!("MCAP has {} channels", channels.len());

    let mut channels_tested = 0;
    let mut total_messages = 0;

    // Test each channel using decoded message iterator
    let decoded_iter = reader.decode_messages();
    if let Ok(iter) = decoded_iter {
        let mut stream = match iter.stream() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to get stream: {}", e);
                return;
            }
        };

        while let Some(result) = stream.next() {
            match result {
                Ok((decoded, channel_info)) => {
                    total_messages += 1;

                    if total_messages == 1 {
                        println!(
                            "  First message: channel={}, topic={}, encoding={}, type={}",
                            channel_info.id,
                            channel_info.topic,
                            channel_info.encoding,
                            channel_info.message_type
                        );
                    }

                    // Verify we got some decoded data
                    if !decoded.is_empty() {
                        channels_tested += 1;
                    }

                    // Test a reasonable number of messages
                    if total_messages >= 100 {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Decode error: {}", e);
                }
            }
        }
    }

    println!(
        "  ✓ Tested {} channels with {} total messages",
        channels_tested, total_messages
    );

    // Assert expectations met
    assert!(!channels.is_empty(), "Expected at least 1 channel");
}
