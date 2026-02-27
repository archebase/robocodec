// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! RoboReader S3 tests - verifies all formats work via RoboReader::open("s3://...").

use super::fixture_path;
use super::integration::{S3Config, ensure_bucket_exists, s3_available, upload_to_s3};

/// Test RoboReader::open with BAG file via S3.
///
/// Regression test: Previously this panicked at std::ops::function.rs:250:5.
#[tokio::test]
async fn test_robo_reader_open_s3_bag_no_panic() {
    assert!(s3_available().await, "MinIO/S3 is required for this test");

    let config = S3Config::default();
    let fixture_path = fixture_path("robocodec_test_15.bag");

    assert!(
        fixture_path.exists(),
        "Fixture is required for this test: {}",
        fixture_path.display()
    );

    let data = std::fs::read(&fixture_path).unwrap();
    let key = "test/regression_robocodec_test_15.bag";

    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    upload_to_s3(&config, key, &data)
        .await
        .expect("Failed to upload BAG fixture to S3/MinIO");

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, key, config.endpoint
    );

    // This should NOT panic - previously panicked at std::ops::function.rs:250:5
    let result = tokio::task::spawn_blocking(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            robocodec::io::RoboReader::open(&s3_url)
        }))
    })
    .await;

    // Clean up after test completes
    let key_cleanup = key.to_string();
    let endpoint = config.endpoint.clone();
    let bucket = config.bucket.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
        let _ = client.delete(&url).send().await;
    });

    match result {
        Ok(Ok(Ok(reader))) => {
            assert_eq!(
                reader.format(),
                robocodec::io::metadata::FileFormat::Bag,
                "Format should be BAG"
            );
            let (count, channel_count) = std::thread::spawn(move || {
                let mut channels = std::collections::HashSet::new();
                let mut count = 0usize;

                for result in reader
                    .iter_raw()
                    .expect("raw iteration should be available")
                {
                    match result {
                        Ok((_, ch)) => {
                            channels.insert(ch.id);
                            count += 1;
                        }
                        Err(e) => panic!("Unexpected BAG raw iteration error: {}", e),
                    }
                }

                (count, channels.len())
            })
            .join()
            .expect("raw iteration thread should not panic");
            assert!(count > 0, "Should have messages via raw iteration");
            assert!(channel_count > 0, "Should have channels via raw iteration");
            eprintln!("RoboReader::open succeeded: {} messages", count);
        }
        Ok(Ok(Err(e))) => {
            panic!(
                "RoboReader::open('s3://...bag') returned error for valid uploaded BAG fixture: {}",
                e
            );
        }
        Ok(Err(panic_info)) => {
            let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            panic!(
                "RoboReader::open('s3://...bag') panicked: {}. \
                 This is the regression we are testing for!",
                panic_msg
            );
        }
        Err(e) => {
            panic!("Task join failed: {:?}", e);
        }
    }
}

/// Test RoboReader::open with MCAP file via S3.
#[tokio::test]
async fn test_robo_reader_open_s3_mcap() {
    assert!(s3_available().await, "MinIO/S3 is required for this test");

    let config = S3Config::default();
    let fixture_path = fixture_path("robocodec_test_0.mcap");

    assert!(
        fixture_path.exists(),
        "Fixture is required for this test: {}",
        fixture_path.display()
    );

    let data = std::fs::read(&fixture_path).unwrap();
    let key = "test/s3_mcap_test.mcap";

    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    upload_to_s3(&config, key, &data)
        .await
        .expect("Failed to upload MCAP fixture to S3/MinIO");

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, key, config.endpoint
    );

    let result =
        tokio::task::spawn_blocking(move || robocodec::io::RoboReader::open(&s3_url)).await;

    // Clean up after test completes
    let key_cleanup = key.to_string();
    let endpoint = config.endpoint.clone();
    let bucket = config.bucket.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
        let _ = client.delete(&url).send().await;
    });

    match result {
        Ok(Ok(reader)) => {
            assert_eq!(
                reader.format(),
                robocodec::io::metadata::FileFormat::Mcap,
                "Format should be MCAP"
            );
            let count = std::thread::spawn(move || {
                reader
                    .iter_raw()
                    .expect("raw iteration should be available")
                    .filter(|r| r.is_ok())
                    .count()
            })
            .join()
            .expect("raw iteration thread should not panic");
            assert!(count > 0, "Should have messages via raw iteration");
            eprintln!("RoboReader::open (MCAP) succeeded: {} messages", count);
        }
        Ok(Err(e)) => panic!("RoboReader::open (MCAP) failed: {}", e),
        Err(e) => panic!("Task join failed: {:?}", e),
    }
}

/// Test RoboReader::open with RRD file via S3.
#[tokio::test]
async fn test_robo_reader_open_s3_rrd() {
    assert!(s3_available().await, "MinIO/S3 is required for this test");

    let config = S3Config::default();
    let fixture_path = fixture_path("rrd/file1.rrd");

    assert!(
        fixture_path.exists(),
        "Fixture is required for this test: {}",
        fixture_path.display()
    );

    let data = std::fs::read(&fixture_path).unwrap();
    let key = "test/s3_rrd_test.rrd";

    ensure_bucket_exists(&config)
        .await
        .expect("S3/MinIO bucket check failed");

    upload_to_s3(&config, key, &data)
        .await
        .expect("Failed to upload RRD fixture to S3/MinIO");

    let s3_url = format!(
        "s3://{}/{}?endpoint={}",
        config.bucket, key, config.endpoint
    );

    let result =
        tokio::task::spawn_blocking(move || robocodec::io::RoboReader::open(&s3_url)).await;

    // Clean up after test completes
    let key_cleanup = key.to_string();
    let endpoint = config.endpoint.clone();
    let bucket = config.bucket.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("{}/{}/{}", endpoint, bucket, key_cleanup);
        let _ = client.delete(&url).send().await;
    });

    match result {
        Ok(Ok(reader)) => {
            assert_eq!(
                reader.format(),
                robocodec::io::metadata::FileFormat::Rrd,
                "Format should be RRD"
            );
            let count = std::thread::spawn(move || {
                reader
                    .iter_raw()
                    .expect("raw iteration should be available")
                    .filter(|r| r.is_ok())
                    .count()
            })
            .join()
            .expect("raw iteration thread should not panic");
            assert!(count > 0, "Should have messages via raw iteration");
            eprintln!("RoboReader::open (RRD) succeeded: {} messages", count);
        }
        Ok(Err(e)) => panic!("RoboReader::open (RRD) failed: {}", e),
        Err(e) => panic!("Task join failed: {:?}", e),
    }
}
