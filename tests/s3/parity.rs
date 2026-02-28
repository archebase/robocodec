// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Local vs S3 parity correctness tests using only RoboReader public API.

use std::collections::HashSet;

use robocodec::io::RoboReader;
use robocodec::io::traits::FormatReader;

use super::fixture_path;
use super::integration::{S3Config, ensure_bucket_exists, s3_available, upload_to_s3};

#[derive(Debug)]
struct ParitySnapshot {
    format: robocodec::io::metadata::FileFormat,
    channel_count: usize,
    channel_set: HashSet<(String, String, String)>,
    raw_success_count: usize,
    decoded_outcome: DecodedOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodedOutcome {
    Success {
        count: usize,
        tuples: Vec<(String, u64, u64)>,
    },
    Failure {
        message: String,
    },
}

fn channel_signature_set(reader: &RoboReader) -> HashSet<(String, String, String)> {
    reader
        .channels()
        .values()
        .map(|channel| {
            (
                channel.topic.clone(),
                channel.message_type.clone(),
                channel.encoding.clone(),
            )
        })
        .collect()
}

fn successful_raw_count(reader: &RoboReader) -> robocodec::Result<usize> {
    let iter = reader.iter_raw()?;
    Ok(iter.filter(|item| item.is_ok()).count())
}

fn normalize_error_message(error: &str) -> String {
    let masked_digits = error
        .chars()
        .map(|c| if c.is_ascii_digit() { '#' } else { c })
        .collect::<String>();

    masked_digits
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn collect_decoded_outcome(reader: &RoboReader) -> DecodedOutcome {
    let iter = match reader.decoded() {
        Ok(iter) => iter,
        Err(e) => {
            return DecodedOutcome::Failure {
                message: normalize_error_message(&e.to_string()),
            };
        }
    };

    let mut count = 0usize;
    let mut tuples = Vec::new();

    for item in iter {
        let decoded = match item {
            Ok(decoded) => decoded,
            Err(e) => {
                return DecodedOutcome::Failure {
                    message: normalize_error_message(&e.to_string()),
                };
            }
        };
        count += 1;
        tuples.push((
            decoded.topic().to_string(),
            decoded.log_time.unwrap_or(0),
            decoded.publish_time.unwrap_or(0),
        ));
    }

    DecodedOutcome::Success { count, tuples }
}

fn snapshot_from_reader(reader: &RoboReader) -> robocodec::Result<ParitySnapshot> {
    Ok(ParitySnapshot {
        format: reader.format(),
        channel_count: reader.channels().len(),
        channel_set: channel_signature_set(reader),
        raw_success_count: successful_raw_count(reader)?,
        decoded_outcome: collect_decoded_outcome(reader),
    })
}

fn is_iter_raw_unsupported(error_text: &str) -> bool {
    let normalized = error_text.to_ascii_lowercase();
    normalized.contains("iter_raw") && normalized.contains("not supported")
}

fn spawn_best_effort_cleanup(config: &S3Config, key: &str) {
    let key_cleanup = key.to_string();
    let endpoint = config.endpoint.clone();
    let bucket = config.bucket.clone();

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
        let _ = client.delete(&url).send().await;
    });
}

async fn run_local_vs_s3_parity_case(fixture_name: &str, s3_key: &str) {
    assert!(
        s3_available().await,
        "MinIO is unavailable; local vs S3 parity tests require MinIO to be running"
    );

    let local_fixture_path = fixture_path(fixture_name);
    assert!(
        local_fixture_path.exists(),
        "Fixture required for S3 parity test is missing at {:?}",
        local_fixture_path
    );

    let local_path = local_fixture_path.to_string_lossy().into_owned();
    let local_reader = RoboReader::open(&local_path)
        .unwrap_or_else(|e| panic!("Failed to open local fixture {fixture_name}: {e}"));
    let local_snapshot = match snapshot_from_reader(&local_reader) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let error_text = e.to_string();
            if is_iter_raw_unsupported(&error_text) {
                panic!(
                    "iter_raw must be supported for local RoboReader parity test ({fixture_name}): {}",
                    error_text
                );
            }
            panic!("Failed to collect local parity snapshot for {fixture_name}: {e}");
        }
    };

    let config = S3Config::default();
    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    let data =
        std::fs::read(&local_fixture_path).expect("Failed to read local fixture bytes for upload");
    upload_to_s3(&config, s3_key, &data)
        .await
        .expect("Failed to upload fixture to S3/MinIO");

    spawn_best_effort_cleanup(&config, s3_key);

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, s3_key, config.endpoint
    );

    let s3_snapshot = match tokio::task::spawn_blocking(move || {
        let reader = RoboReader::open(&s3_url).map_err(|e| e.to_string())?;
        snapshot_from_reader(&reader).map_err(|e| e.to_string())
    })
    .await
    {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(error_text)) => {
            if is_iter_raw_unsupported(&error_text) {
                panic!(
                    "iter_raw must be supported for S3 RoboReader parity test ({fixture_name}): {}",
                    error_text
                );
            }
            panic!("Failed to collect S3 parity snapshot for {fixture_name}: {error_text}");
        }
        Err(join_error) => panic!("S3 parity worker task failed for {fixture_name}: {join_error}"),
    };

    assert_eq!(s3_snapshot.format, local_snapshot.format, "format mismatch");
    assert_eq!(
        s3_snapshot.channel_count, local_snapshot.channel_count,
        "channel count mismatch"
    );
    assert_eq!(
        s3_snapshot.channel_set, local_snapshot.channel_set,
        "channel topic/type/encoding set mismatch"
    );
    assert_eq!(
        s3_snapshot.raw_success_count, local_snapshot.raw_success_count,
        "successful raw iteration count mismatch"
    );

    match (
        &local_snapshot.decoded_outcome,
        &s3_snapshot.decoded_outcome,
    ) {
        (
            DecodedOutcome::Success {
                count: local_count,
                tuples: local_tuples,
            },
            DecodedOutcome::Success {
                count: s3_count,
                tuples: s3_tuples,
            },
        ) => {
            assert_eq!(
                s3_count, local_count,
                "successful decoded iteration count mismatch"
            );
            assert_eq!(
                s3_tuples, local_tuples,
                "decoded topic/timestamp sequence mismatch"
            );
        }
        (
            DecodedOutcome::Failure {
                message: local_message,
            },
            DecodedOutcome::Failure {
                message: s3_message,
            },
        ) => {
            assert!(
                local_message == s3_message
                    || local_message.starts_with(s3_message)
                    || s3_message.starts_with(local_message),
                "decoded failure mismatch: local={local_message:?}, s3={s3_message:?}"
            );
        }
        (local_outcome, s3_outcome) => {
            panic!("decoded parity mismatch: local={local_outcome:?}, s3={s3_outcome:?}");
        }
    }
}

#[tokio::test]
async fn test_local_vs_s3_parity_bag() {
    run_local_vs_s3_parity_case("robocodec_test_15.bag", "test/parity_robocodec_test_15.bag").await;
}

#[tokio::test]
async fn test_local_vs_s3_parity_rrd() {
    run_local_vs_s3_parity_case("rrd/file1.rrd", "test/parity_file1.rrd").await;
}

#[tokio::test]
async fn test_local_vs_s3_parity_mcap() {
    run_local_vs_s3_parity_case("robocodec_test_0.mcap", "test/parity_robocodec_test_0.mcap").await;
}
