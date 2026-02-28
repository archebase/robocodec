// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 performance guardrail tests (fail-fast, coarse thresholds).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use robocodec::io::RoboReader;

use super::fixture_path;
use super::integration::{S3Config, ensure_bucket_exists, s3_available, upload_to_s3};

// Conservative CI guardrail: protects against obvious regressions while tolerating
// noisy shared runners and cold-start effects.
const FIRST_MESSAGE_MAX: Duration = Duration::from_secs(12);
// Conservative CI guardrail for full raw iteration over small/medium fixtures.
const TOTAL_READ_MAX: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct PerfResult {
    raw_count: usize,
    time_to_first_message: Duration,
    total_read_duration: Duration,
}

#[derive(Debug)]
struct S3ObjectCleanupGuard {
    endpoint: String,
    bucket: String,
    key: String,
}

impl S3ObjectCleanupGuard {
    fn new(config: &S3Config, key: &str) -> Self {
        Self {
            endpoint: config.endpoint.clone(),
            bucket: config.bucket.clone(),
            key: key.to_string(),
        }
    }
}

impl Drop for S3ObjectCleanupGuard {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();

        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let url = format!("{}/{}/{}", endpoint, bucket, key);
            let _ = client.delete(&url).send().await;
        });
    }
}

fn unique_key(prefix: &str, extension: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after unix epoch")
        .as_nanos();
    format!(
        "test/{prefix}_{}_{}.{}",
        std::process::id(),
        nanos,
        extension
    )
}

async fn run_s3_perf_guardrail_case(fixture_name: &str, s3_key: String) {
    assert!(
        s3_available().await,
        "MinIO is unavailable; S3 performance tests require MinIO to be running"
    );

    let local_fixture_path = fixture_path(fixture_name);
    assert!(
        local_fixture_path.exists(),
        "Fixture required for S3 performance test is missing at {:?}",
        local_fixture_path
    );

    let config = S3Config::default();
    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    let data =
        std::fs::read(&local_fixture_path).expect("Failed to read local fixture bytes for upload");
    upload_to_s3(&config, &s3_key, &data)
        .await
        .expect("Failed to upload fixture to S3/MinIO");

    let _cleanup = S3ObjectCleanupGuard::new(&config, &s3_key);

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, s3_key, config.endpoint
    );

    let perf = tokio::task::spawn_blocking(move || {
        let start = Instant::now();

        let reader = RoboReader::open(&s3_url)
            .map_err(|e| format!("Failed to open S3 fixture via RoboReader: {e}"))?;
        let mut iter = reader
            .iter_raw()
            .map_err(|e| format!("Failed to create raw iterator for S3 fixture: {e}"))?;

        let first_item = iter
            .next()
            .ok_or_else(|| "S3 fixture produced zero raw messages".to_string())?;
        first_item.map_err(|e| format!("First raw item failed for S3 fixture: {e}"))?;

        let time_to_first_message = start.elapsed();
        let mut raw_count = 1usize;

        for item in iter {
            item.map_err(|e| format!("Raw iteration failed for S3 fixture: {e}"))?;
            raw_count += 1;
        }

        Ok::<PerfResult, String>(PerfResult {
            raw_count,
            time_to_first_message,
            total_read_duration: start.elapsed(),
        })
    })
    .await
    .expect("S3 performance worker task failed")
    .unwrap_or_else(|e| panic!("S3 performance case failed for {fixture_name}: {e}"));

    assert!(
        perf.raw_count > 0,
        "raw_count must be > 0 for fixture {fixture_name}; got {}",
        perf.raw_count
    );
    assert!(
        perf.time_to_first_message <= FIRST_MESSAGE_MAX,
        "time-to-first-message exceeded threshold for fixture {fixture_name}: {:?} > {:?}",
        perf.time_to_first_message,
        FIRST_MESSAGE_MAX
    );
    assert!(
        perf.total_read_duration <= TOTAL_READ_MAX,
        "total read duration exceeded threshold for fixture {fixture_name}: {:?} > {:?}",
        perf.total_read_duration,
        TOTAL_READ_MAX
    );
}

#[tokio::test]
async fn test_s3_perf_guardrail_bag() {
    run_s3_perf_guardrail_case(
        "robocodec_test_15.bag",
        unique_key("perf_guardrail_bag", "bag"),
    )
    .await;
}

#[tokio::test]
async fn test_s3_perf_guardrail_rrd() {
    run_s3_perf_guardrail_case("rrd/file1.rrd", unique_key("perf_guardrail_rrd", "rrd")).await;
}

#[tokio::test]
async fn test_s3_perf_guardrail_mcap() {
    run_s3_perf_guardrail_case(
        "robocodec_test_0.mcap",
        unique_key("perf_guardrail_mcap", "mcap"),
    )
    .await;
}
