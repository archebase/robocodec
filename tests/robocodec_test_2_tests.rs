// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Tests for robocodec_test_2.mcap fixture.
//!
//! This fixture contains PeriStatus messages with ros2idl encoding.

use std::path::Path;

use robocodec::io::RoboReader;
use robocodec::FormatReader;

/// Path to the fixtures directory.
const FIXTURES_DIR: &str = "tests/fixtures";

#[test]
fn test_robocodec_test_2_open() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let result = RoboReader::open(fixture_path.to_str().unwrap());
    assert!(result.is_ok(), "Should open robocodec_test_2.mcap");

    let reader = result.unwrap();
    let channels = reader.channels();
    assert!(
        !channels.is_empty(),
        "File should have at least one channel"
    );
}

#[test]
fn test_robocodec_test_2_has_channels() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let reader = RoboReader::open(fixture_path.to_str().unwrap()).expect("Should open MCAP");
    let channels = reader.channels();

    let channel_count = channels.len();
    assert!(channel_count > 0, "Should have at least one channel");
}

#[test]
fn test_robocodec_test_2_iterate_messages() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let reader = RoboReader::open(fixture_path.to_str().unwrap()).expect("Should open MCAP");

    let mut count = 0;
    if let Ok(iter) = reader.decoded() {
        for result in iter.take(10) {
            match result {
                Ok(_) => count += 1,
                Err(e) => {
                    eprintln!("Decode error: {}", e);
                }
            }
        }
    }

    assert!(count > 0, "Should have at least one message");
}

#[test]
fn test_robocodec_test_2_peristatus_schema() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let reader = RoboReader::open(fixture_path.to_str().unwrap()).expect("Should open MCAP");

    // Find the PeriStatus channel
    let mut found_peristatus = false;
    let channels = reader.channels();
    for (_id, channel) in channels {
        if channel.message_type.contains("PeriStatus") {
            found_peristatus = true;
            // Verify schema is present
            assert!(channel.schema.is_some(), "PeriStatus should have a schema");

            if let Some(schema) = &channel.schema {
                assert!(!schema.is_empty(), "Schema should not be empty");
            }

            // Verify encoding is CDR (ros2idl)
            assert_eq!(channel.encoding, "cdr");
        }
    }

    assert!(found_peristatus, "Should find PeriStatus channel");
}

#[test]
fn test_robocodec_test_2_decode_messages() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let reader = RoboReader::open(fixture_path.to_str().unwrap()).expect("Should open MCAP");

    let mut decoded_count = 0;
    if let Ok(iter) = reader.decoded() {
        for result in iter.take(10) {
            match result {
                Ok(_) => decoded_count += 1,
                Err(_) => continue,
            }
        }
    }

    assert!(decoded_count > 0, "Should decode at least one message");
}

#[test]
fn test_robocodec_test_2_message_order() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let reader = RoboReader::open(fixture_path.to_str().unwrap()).expect("Should open MCAP");
    println!("Message count: {}", reader.message_count());

    // Use decoded() for message iteration
    let mut count = 0;

    if let Ok(iter) = reader.decoded() {
        for result in iter.take(10) {
            match result {
                Ok(decoded_result) => {
                    count += 1;
                    if count == 1 {
                        println!(
                            "First message from channel: {}",
                            decoded_result.channel.topic
                        );
                    }

                    if !decoded_result.message.is_empty() && count >= 10 {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Decode error: {}", e);
                }
            }
        }
    } else {
        eprintln!("Failed to decode messages");
    }

    assert!(count > 0, "Should have at least one message");
}

#[test]
fn test_robocodec_test_2_channel_topics() {
    let fixture_path = Path::new(FIXTURES_DIR).join("robocodec_test_2.mcap");

    if !fixture_path.exists() {
        eprintln!(
            "Skipping test: fixture file not found: {}",
            fixture_path.display()
        );
        return;
    }

    let reader = RoboReader::open(fixture_path.to_str().unwrap()).expect("Should open MCAP");

    let mut has_topic = false;
    let channels = reader.channels();
    for (_id, channel) in channels {
        if !channel.topic.is_empty() {
            has_topic = true;
            // Topic should start with /
            assert!(channel.topic.starts_with('/'), "Topic should start with /");
        }
    }

    assert!(has_topic, "Should have at least one channel with a topic");
}
