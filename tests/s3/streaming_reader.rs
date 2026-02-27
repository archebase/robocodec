// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! StreamingRoboReader S3 integration tests.

use robocodec::io::streaming::{StreamConfig, StreamingRoboReader};

use super::fixture_path;
use super::integration::{S3Config, ensure_bucket_exists, s3_available, upload_to_s3};
use super::require_live_s3;

fn spawn_best_effort_cleanup(config: &S3Config, key: &str) {
    let endpoint = config.endpoint.clone();
    let bucket = config.bucket.clone();
    let key_cleanup = key.to_string();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
        let _ = client.delete(&url).send().await;
    });
}

async fn run_streaming_reader_s3_case(fixture_name: &str, key: &str) {
    if !require_live_s3() {
        return;
    }

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

    spawn_best_effort_cleanup(&config, key);

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, key, config.endpoint
    );

    let reader = StreamingRoboReader::open(&s3_url, StreamConfig::new())
        .await
        .unwrap_or_else(|e| panic!("StreamingRoboReader::open failed for {fixture_name}: {e}"));

    let messages = tokio::task::spawn_blocking(move || reader.collect_messages())
        .await
        .expect("collect_messages worker task panicked")
        .unwrap_or_else(|e| panic!("collect_messages failed for {fixture_name}: {e}"));

    assert!(
        !messages.is_empty(),
        "Expected at least one streamed message for {fixture_name}"
    );
}

#[tokio::test]
async fn test_streaming_robo_reader_open_s3_rrd_collects_messages() {
    run_streaming_reader_s3_case("rrd/file1.rrd", "test/streaming_reader_file1.rrd").await;
}
