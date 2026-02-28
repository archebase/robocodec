// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! StreamingRoboReader S3 integration tests.

use robocodec::io::streaming::{
    AlignedFrame, FrameAlignmentConfig, StreamConfig, StreamingRoboReader,
};

use super::fixture_path;
use super::integration::{S3Config, ensure_bucket_exists, s3_available, upload_to_s3};

/// Async cleanup helper - call AFTER test assertions to avoid race conditions.
async fn cleanup_s3_object(config: &S3Config, key: &str) {
    let client = reqwest::Client::new();
    let url = format!("{}/{}/{}", config.endpoint, config.bucket, key);
    let _ = client.delete(&url).send().await;
}

/// Helper that uploads fixture and returns config+key for cleanup after assertions.
async fn setup_streaming_reader_s3_case(fixture_name: &str, key: &str) -> (S3Config, String) {
    assert!(
        s3_available().await,
        "MinIO/S3 is unavailable; StreamingRoboReader S3 test requires MinIO"
    );

    let fixture = fixture_path(fixture_name);
    assert!(
        fixture.exists(),
        "Fixture required for StreamingRoboReader S3 test is missing: {}",
        fixture.display()
    );

    let config = S3Config::default();
    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    let data = std::fs::read(&fixture).expect("Failed to read fixture for S3 upload");
    upload_to_s3(&config, key, &data)
        .await
        .expect("Failed to upload fixture to S3/MinIO");

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, key, config.endpoint
    );

    (config, s3_url)
}

#[tokio::test]
async fn test_streaming_robo_reader_open_s3_rrd_collects_messages() {
    let key = "test/streaming_reader_file1.rrd";
    let (config, s3_url) = setup_streaming_reader_s3_case("rrd/file1.rrd", key).await;

    let reader = StreamingRoboReader::open(&s3_url, StreamConfig::new())
        .await
        .expect("StreamingRoboReader::open failed for rrd/file1.rrd");

    let messages = tokio::task::spawn_blocking(move || reader.collect_messages())
        .await
        .expect("collect_messages worker task panicked")
        .expect("collect_messages failed for rrd/file1.rrd");

    assert!(
        !messages.is_empty(),
        "Expected at least one streamed message for rrd/file1.rrd"
    );

    cleanup_s3_object(&config, key).await;
}

#[tokio::test]
async fn test_streaming_robo_reader_open_s3_mcap_collects_messages() {
    let key = "test/streaming_reader_robocodec_test_0.mcap";
    let (config, s3_url) = setup_streaming_reader_s3_case("robocodec_test_0.mcap", key).await;

    let reader = StreamingRoboReader::open(&s3_url, StreamConfig::new())
        .await
        .expect("StreamingRoboReader::open failed for robocodec_test_0.mcap");

    let messages = tokio::task::spawn_blocking(move || reader.collect_messages())
        .await
        .expect("collect_messages worker task panicked")
        .expect("collect_messages failed for robocodec_test_0.mcap");

    assert!(
        !messages.is_empty(),
        "Expected at least one streamed message for robocodec_test_0.mcap"
    );

    cleanup_s3_object(&config, key).await;
}

#[tokio::test]
async fn test_streaming_robo_reader_open_s3_bag_collects_messages() {
    let key = "test/streaming_reader_robocodec_test_24_leju_claw.bag";
    let (config, s3_url) =
        setup_streaming_reader_s3_case("robocodec_test_24_leju_claw.bag", key).await;

    let reader = StreamingRoboReader::open(&s3_url, StreamConfig::new())
        .await
        .expect("StreamingRoboReader::open failed for robocodec_test_24_leju_claw.bag");

    let messages = tokio::task::spawn_blocking(move || reader.collect_messages())
        .await
        .expect("collect_messages worker task panicked")
        .expect("collect_messages failed for robocodec_test_24_leju_claw.bag");

    assert!(
        !messages.is_empty(),
        "Expected at least one streamed message for robocodec_test_24_leju_claw.bag"
    );

    cleanup_s3_object(&config, key).await;
}

/// Helper for S3 frame alignment tests.
async fn setup_s3_frame_alignment_test(fixture_name: &str, key: &str) -> (S3Config, String) {
    assert!(
        s3_available().await,
        "MinIO/S3 is unavailable; S3 frame alignment test requires MinIO"
    );

    let fixture = fixture_path(fixture_name);
    assert!(
        fixture.exists(),
        "Fixture required for S3 frame alignment test is missing: {}",
        fixture.display()
    );

    let config = S3Config::default();
    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    let data = std::fs::read(&fixture).expect("Failed to read fixture for S3 upload");
    upload_to_s3(&config, key, &data)
        .await
        .expect("Failed to upload fixture to S3/MinIO");

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, key, config.endpoint
    );

    (config, s3_url)
}

#[tokio::test]
async fn test_streaming_robo_reader_open_s3_bag_collect_frames() {
    let key = "test/frame_align_collect_robocodec_test_24_leju_claw.bag";
    let (config, s3_url) =
        setup_s3_frame_alignment_test("robocodec_test_24_leju_claw.bag", key).await;

    let reader = StreamingRoboReader::open(&s3_url, StreamConfig::new())
        .await
        .expect("StreamingRoboReader::open failed for S3 frame alignment");

    let frame_config = FrameAlignmentConfig::new(30)
        .with_image_topic("/cam_l/color/image_raw/compressed")
        .with_state_topic("/kuavo_arm_traj");

    let frames = tokio::task::spawn_blocking(move || reader.collect_frames(frame_config))
        .await
        .expect("collect_frames worker task panicked")
        .expect("collect_frames failed for S3 frame alignment");

    assert!(!frames.is_empty(), "Expected at least one frame from S3");

    let mut last_timestamp = 0u64;
    for (i, frame) in frames.iter().enumerate() {
        assert_eq!(frame.frame_index, i, "Frame index should be sequential");
        assert!(
            frame.timestamp >= last_timestamp,
            "Frames should be in timestamp order"
        );
        last_timestamp = frame.timestamp;
    }

    cleanup_s3_object(&config, key).await;
}

#[tokio::test]
async fn test_streaming_robo_reader_open_s3_bag_process_frames() {
    let key = "test/frame_align_process_robocodec_test_24_leju_claw.bag";
    let (config, s3_url) =
        setup_s3_frame_alignment_test("robocodec_test_24_leju_claw.bag", key).await;

    let reader = StreamingRoboReader::open(&s3_url, StreamConfig::new())
        .await
        .expect("StreamingRoboReader::open failed for S3 frame alignment");

    let frame_config = FrameAlignmentConfig::new(30)
        .with_image_topic("/cam_l/color/image_raw/compressed")
        .with_state_topic("/kuavo_arm_traj");

    let frame_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let frame_count_clone = frame_count.clone();

    let result = tokio::task::spawn_blocking(move || {
        reader.process_frames(frame_config, move |frame: AlignedFrame| {
            frame_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            assert!(frame.timestamp > 0, "Frame should have timestamp");
            assert!(
                !frame.images.is_empty() || !frame.states.is_empty(),
                "Frame should have either images or states"
            );

            Ok(())
        })
    })
    .await
    .expect("process_frames worker task panicked");

    result.expect("process_frames failed for S3 frame alignment");

    let count = frame_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        count > 0,
        "Expected at least one frame from S3 via process_frames"
    );

    cleanup_s3_object(&config, key).await;
}
