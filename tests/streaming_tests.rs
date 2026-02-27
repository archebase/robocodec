// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Integration tests for the streaming API.

use std::path::PathBuf;

use robocodec::io::streaming::{
    AlignedFrame, FrameAlignmentConfig, StreamConfig, StreamingRoboReader, TimestampedMessage,
};

/// Get the path to a test fixture file.
fn fixture_path(name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures");
    path.push(name);
    path
}

/// Test that StreamingRoboReader can open a local MCAP file.
#[tokio::test]
async fn test_streaming_reader_open_mcap() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        eprintln!("Skipping test: fixture not found at {:?}", path);
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open MCAP file");

    // Verify basic metadata
    assert!(reader.file_size() > 0, "File size should be greater than 0");
    assert!(
        reader.message_count() > 0,
        "Message count should be greater than 0"
    );
    assert!(
        !reader.channels().is_empty(),
        "Should have at least one channel"
    );
}

/// Test that StreamingRoboReader can open a local BAG file.
#[tokio::test]
async fn test_streaming_reader_open_bag() {
    let path = fixture_path("robocodec_test_15.bag");
    if !path.exists() {
        eprintln!("Skipping test: fixture not found at {:?}", path);
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open BAG file");

    assert!(reader.file_size() > 0);
    assert!(reader.message_count() > 0);
}

/// Test that StreamingRoboReader can open a local RRD file.
#[tokio::test]
async fn test_streaming_reader_open_rrd() {
    let path = fixture_path("rrd/file1.rrd");
    if !path.exists() {
        eprintln!("Skipping test: fixture not found at {:?}", path);
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open RRD file");

    assert!(reader.file_size() > 0, "File size should be greater than 0");
    assert!(
        reader.message_count() > 0,
        "Message count should be greater than 0"
    );
}

/// Test collecting all messages from a file.
#[tokio::test]
async fn test_streaming_reader_collect_messages() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    let expected_count = reader.message_count();
    let messages = reader
        .collect_messages()
        .expect("Failed to collect messages");

    assert!(!messages.is_empty(), "Should have collected messages");
    assert_eq!(
        messages.len() as u64,
        expected_count,
        "Collected message count should match reader metadata"
    );

    // Verify message structure
    for msg in &messages {
        assert!(!msg.topic.is_empty(), "Message should have a topic");
        // Verify timestamps are reasonable (non-zero for most messages)
        assert!(
            msg.log_time >= msg.publish_time,
            "Log time should be >= publish time"
        );
    }
}

/// Test processing messages with a callback.
#[tokio::test]
async fn test_streaming_reader_process_messages() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    let mut message_count = 0;
    let mut topics = std::collections::HashSet::new();

    reader
        .process_messages(|msg: TimestampedMessage| {
            message_count += 1;
            topics.insert(msg.topic.clone());
            Ok(())
        })
        .expect("Failed to process messages");

    assert!(message_count > 0, "Should have processed messages");
    assert!(!topics.is_empty(), "Should have found topics");
}

/// Test progress tracking during message processing.
#[tokio::test]
async fn test_streaming_reader_progress_tracking() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    let initial_progress = reader.progress().parsing_event();
    match initial_progress {
        robocodec::io::streaming::ProgressEvent::Parsing {
            messages_parsed, ..
        } => {
            assert_eq!(messages_parsed, 0, "Should start with 0 messages parsed");
        }
        _ => panic!("Expected Parsing event"),
    }

    // Process some messages
    reader
        .process_messages(|_| Ok(()))
        .expect("Failed to process messages");
}

/// Test frame alignment with closest-state matching.
#[tokio::test]
async fn test_frame_alignment_closest_state() {
    // Use the leju_claw bag file which has both images and state
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        eprintln!("Skipping test: fixture not found at {:?}", path);
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    // Debug: print available topics
    println!("Available channels:");
    for ch in reader.channels().values() {
        println!("  - {} ({})", ch.topic, ch.message_type);
    }

    let frame_config = FrameAlignmentConfig::new(30)
        .with_image_topic("/cam_l/color/image_raw/compressed") // Use the correct topic
        .with_state_topic("/kuavo_arm_traj")
        .with_max_latency(100_000_000); // 100ms tolerance

    println!("Image topics: {:?}", frame_config.image_topics);
    println!("State topics: {:?}", frame_config.state_topics);

    let mut frame_count = 0;
    let mut frames_with_state = 0;
    let mut message_count = 0;

    reader
        .process_messages(|msg: TimestampedMessage| {
            message_count += 1;
            if message_count <= 10 {
                println!(
                    "Message {}: {} @ {}",
                    message_count, msg.topic, msg.log_time
                );
            }
            Ok(())
        })
        .expect("Failed to process messages");

    println!("Total messages: {}", message_count);

    // Now process frames
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    reader
        .process_frames(frame_config, |frame: AlignedFrame| {
            frame_count += 1;
            if !frame.states.is_empty() {
                frames_with_state += 1;
            }

            // Verify frame structure
            assert!(frame.timestamp > 0, "Frame should have timestamp");
            assert!(
                !frame.images.is_empty() || !frame.states.is_empty(),
                "Frame should have either images or state"
            );

            Ok(())
        })
        .expect("Failed to process frames");

    println!(
        "Frames: {}, frames_with_state: {}",
        frame_count, frames_with_state
    );
    assert!(frame_count > 0, "Should have emitted frames");
    println!(
        "Frames: {}, Frames with state: {} ({}%)",
        frame_count,
        frames_with_state,
        if frame_count > 0 {
            (frames_with_state as f64 / frame_count as f64) * 100.0
        } else {
            0.0
        }
    );
}

/// Test collecting all frames.
#[tokio::test]
async fn test_frame_stream_collect_frames() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    let frame_config = FrameAlignmentConfig::new(30)
        .with_image_topic("/cam_l/color/image_raw/compressed")
        .with_image_topic("/cam_l/color/image_raw/compressed")
        .with_image_topic("/cam_r/color/image_raw/compressed")
        .with_state_topic("/kuavo_arm_traj")
        .with_state_topic("/leju_claw_state");

    let frames = reader
        .collect_frames(frame_config)
        .expect("Failed to collect frames");

    assert!(!frames.is_empty(), "Should have collected frames");

    // Verify frame ordering
    let mut last_timestamp = 0u64;
    for (i, frame) in frames.iter().enumerate() {
        assert_eq!(frame.frame_index, i, "Frame index should be sequential");
        assert!(
            frame.timestamp >= last_timestamp,
            "Frames should be in timestamp order"
        );
        last_timestamp = frame.timestamp;
    }
}

/// Test AlignedFrame helper methods.
#[test]
fn test_aligned_frame_helpers() {
    let mut frame = AlignedFrame::new(0, 1_000_000_000);

    // Add an image
    frame.add_image("camera_0", 640, 480, vec![0u8; 100], true);

    // Add state
    frame.add_state("joint_positions", vec![0.1, 0.2, 0.3, 0.4, 0.5]);

    // Test getters
    let img = frame.get_image("camera_0");
    assert!(img.is_some());
    let img = img.unwrap();
    assert_eq!(img.width, 640);
    assert_eq!(img.height, 480);

    let state = frame.get_state("joint_positions");
    assert!(state.is_some());
    assert_eq!(state.unwrap().len(), 5);

    // Test has_required_* methods
    assert!(frame.has_required_images(&["camera_0"]));
    assert!(!frame.has_required_images(&["camera_1"]));
    assert!(frame.has_required_state(&["joint_positions"]));
    assert!(!frame.has_required_state(&["missing_state"]));
}

/// Test TimestampedMessage structure.
#[test]
fn test_timestamped_message() {
    use robocodec::io::metadata::ChannelInfo;

    let channel = ChannelInfo {
        id: 1,
        topic: "/test/topic".to_string(),
        message_type: "std_msgs/String".to_string(),
        encoding: "cdr".to_string(),
        schema: None,
        schema_data: None,
        schema_encoding: None,
        message_count: 0,
        callerid: None,
    };

    let msg = TimestampedMessage {
        topic: "/test/topic".to_string(),
        log_time: 1_000_000_000,
        publish_time: 999_999_000,
        sequence: 42,
        data: robocodec::CodecValue::String("hello".to_string()),
        channel,
    };

    assert_eq!(msg.topic, "/test/topic");
    assert_eq!(msg.log_time, 1_000_000_000);
    assert_eq!(msg.sequence, 42);
}

/// Test frame alignment with exact matching (no closest-state).
#[tokio::test]
async fn test_frame_alignment_exact_matching() {
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open file");

    let frame_config = FrameAlignmentConfig::new(30)
        .with_image_topic("/cam_l/color/image_raw/compressed")
        .with_state_topic("/kuavo_arm_traj")
        .with_exact_matching(); // Use exact timestamp matching

    let mut frame_count = 0;

    reader
        .process_frames(frame_config, |_frame: AlignedFrame| {
            frame_count += 1;
            Ok(())
        })
        .expect("Failed to process frames");

    assert!(
        frame_count > 0,
        "Should have frames even with exact matching"
    );
}

/// Test error handling when file doesn't exist.
#[tokio::test]
async fn test_streaming_reader_file_not_found() {
    let config = StreamConfig::new();
    let result = StreamingRoboReader::open("/nonexistent/path/file.mcap", config).await;

    assert!(result.is_err(), "Should fail for non-existent file");
}

// ============================================================================
// Format-Specific Message Collection Tests
// ============================================================================

/// Test collecting messages from BAG file.
#[tokio::test]
async fn test_bag_format_collect_messages() {
    // Use a simpler BAG file that doesn't have parse errors
    let path = fixture_path("robocodec_test_24_leju_claw.bag");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open BAG file");

    let messages = reader
        .collect_messages()
        .expect("Failed to collect messages from BAG");

    assert!(!messages.is_empty(), "Should have messages from BAG file");

    // Verify all messages have valid topics and timestamps
    for msg in &messages {
        assert!(!msg.topic.is_empty(), "BAG message should have topic");
        assert!(msg.log_time > 0, "BAG message should have valid timestamp");
    }
}

/// Test collecting messages from RRD file.
#[tokio::test]
async fn test_rrd_format_collect_messages() {
    let path = fixture_path("rrd/file1.rrd");
    if !path.exists() {
        return;
    }

    let config = StreamConfig::new();
    let reader = StreamingRoboReader::open(path.to_str().unwrap(), config)
        .await
        .expect("Failed to open RRD file");

    let messages = reader
        .collect_messages()
        .expect("Failed to collect messages from RRD");

    assert!(!messages.is_empty(), "Should have messages from RRD file");

    // Verify RRD-specific message structure
    // Note: RRD messages may have log_time == 0, so we only check topic
    for msg in &messages {
        assert!(!msg.topic.is_empty(), "RRD message should have topic");
    }
}

/// Test that all three formats can be processed with process_messages.
#[tokio::test]
async fn test_all_formats_process_messages() {
    // Test MCAP
    let mcap_path = fixture_path("robocodec_test_0.mcap");
    if mcap_path.exists() {
        let config = StreamConfig::new();
        let reader = StreamingRoboReader::open(mcap_path.to_str().unwrap(), config)
            .await
            .expect("Failed to open MCAP");

        let mut count = 0;
        reader
            .process_messages(|_| {
                count += 1;
                Ok(())
            })
            .expect("Failed to process MCAP messages");
        assert!(count > 0, "Should process MCAP messages");
    }

    // Test BAG - use a simpler file that doesn't have parse errors
    let bag_path = fixture_path("robocodec_test_24_leju_claw.bag");
    if bag_path.exists() {
        let config = StreamConfig::new();
        let reader = StreamingRoboReader::open(bag_path.to_str().unwrap(), config)
            .await
            .expect("Failed to open BAG");

        let mut count = 0;
        reader
            .process_messages(|_| {
                count += 1;
                Ok(())
            })
            .expect("Failed to process BAG messages");
        assert!(count > 0, "Should process BAG messages");
    }

    // Test RRD
    let rrd_path = fixture_path("rrd/file1.rrd");
    if rrd_path.exists() {
        let config = StreamConfig::new();
        let reader = StreamingRoboReader::open(rrd_path.to_str().unwrap(), config)
            .await
            .expect("Failed to open RRD");

        let mut count = 0;
        reader
            .process_messages(|_| {
                count += 1;
                Ok(())
            })
            .expect("Failed to process RRD messages");
        assert!(count > 0, "Should process RRD messages");
    }
}
