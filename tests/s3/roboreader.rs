// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! RoboReader S3 tests - verifies all formats work via RoboReader::open("s3://...").

use super::fixture_path;
use super::integration::{S3Config, ensure_bucket_exists, s3_available, upload_to_s3};
use super::require_live_s3;

async fn cleanup_s3_object(config: &S3Config, key: &str) {
    let client = reqwest::Client::new();
    let url = format!("{}/{}/{}", config.endpoint, config.bucket, key);
    let _ = client.delete(&url).send().await;
}

/// Test RoboReader::open with BAG file via S3.
///
/// Regression test: Previously this panicked at std::ops::function.rs:250:5.
#[tokio::test]
async fn test_robo_reader_open_s3_bag_no_panic() {
    if !require_live_s3() {
        return;
    }

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

    let outcome: Result<(), String> = match result {
        Ok(Ok(Ok(reader))) => {
            if reader.format() != robocodec::io::metadata::FileFormat::Bag {
                Err("Format should be BAG".to_string())
            } else {
                let raw_outcome =
                    match std::thread::spawn(move || -> Result<(usize, usize), String> {
                        let mut channels = std::collections::HashSet::new();
                        let mut count = 0usize;
                        let iter = reader
                            .iter_raw()
                            .map_err(|e| format!("raw iteration should be available: {}", e))?;

                        for result in iter {
                            match result {
                                Ok((_, ch)) => {
                                    channels.insert(ch.id);
                                    count += 1;
                                }
                                Err(e) => {
                                    return Err(format!(
                                        "Unexpected BAG raw iteration error: {}",
                                        e
                                    ));
                                }
                            }
                        }

                        Ok((count, channels.len()))
                    })
                    .join()
                    {
                        Ok(value) => value,
                        Err(_) => Err("raw iteration thread should not panic".to_string()),
                    };

                match raw_outcome {
                    Ok((count, channel_count)) => {
                        if count == 0 {
                            Err("Should have messages via raw iteration".to_string())
                        } else if channel_count == 0 {
                            Err("Should have channels via raw iteration".to_string())
                        } else {
                            eprintln!("RoboReader::open succeeded: {} messages", count);
                            Ok(())
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }
        Ok(Ok(Err(e))) => Err(format!(
            "RoboReader::open('s3://...bag') returned error for valid uploaded BAG fixture: {}",
            e
        )),
        Ok(Err(panic_info)) => {
            let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            Err(format!(
                "RoboReader::open('s3://...bag') panicked: {}. This is the regression we are testing for!",
                panic_msg
            ))
        }
        Err(e) => Err(format!("Task join failed: {:?}", e)),
    };

    cleanup_s3_object(&config, key).await;
    outcome.unwrap_or_else(|e| panic!("{}", e));
}

/// Test RoboReader::open with MCAP file via S3.
#[tokio::test]
async fn test_robo_reader_open_s3_mcap() {
    if !require_live_s3() {
        return;
    }

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

    let outcome: Result<(), String> = match result {
        Ok(Ok(reader)) => {
            if reader.format() != robocodec::io::metadata::FileFormat::Mcap {
                Err("Format should be MCAP".to_string())
            } else {
                let count_outcome = match std::thread::spawn(move || -> Result<usize, String> {
                    let iter = reader
                        .iter_raw()
                        .map_err(|e| format!("raw iteration should be available: {}", e))?;
                    Ok(iter.filter(|r| r.is_ok()).count())
                })
                .join()
                {
                    Ok(value) => value,
                    Err(_) => Err("raw iteration thread should not panic".to_string()),
                };

                match count_outcome {
                    Ok(count) => {
                        if count == 0 {
                            Err("Should have messages via raw iteration".to_string())
                        } else {
                            eprintln!("RoboReader::open (MCAP) succeeded: {} messages", count);
                            Ok(())
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }
        Ok(Err(e)) => Err(format!("RoboReader::open (MCAP) failed: {}", e)),
        Err(e) => Err(format!("Task join failed: {:?}", e)),
    };

    cleanup_s3_object(&config, key).await;
    outcome.unwrap_or_else(|e| panic!("{}", e));
}

/// Test RoboReader::open with RRD file via S3.
#[tokio::test]
async fn test_robo_reader_open_s3_rrd() {
    if !require_live_s3() {
        return;
    }

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

    let outcome: Result<(), String> = match result {
        Ok(Ok(reader)) => {
            if reader.format() != robocodec::io::metadata::FileFormat::Rrd {
                Err("Format should be RRD".to_string())
            } else {
                let count_outcome = match std::thread::spawn(move || -> Result<usize, String> {
                    let iter = reader
                        .iter_raw()
                        .map_err(|e| format!("raw iteration should be available: {}", e))?;
                    Ok(iter.filter(|r| r.is_ok()).count())
                })
                .join()
                {
                    Ok(value) => value,
                    Err(_) => Err("raw iteration thread should not panic".to_string()),
                };

                match count_outcome {
                    Ok(count) => {
                        if count == 0 {
                            Err("Should have messages via raw iteration".to_string())
                        } else {
                            eprintln!("RoboReader::open (RRD) succeeded: {} messages", count);
                            Ok(())
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }
        Ok(Err(e)) => Err(format!("RoboReader::open (RRD) failed: {}", e)),
        Err(e) => Err(format!("Task join failed: {:?}", e)),
    };

    cleanup_s3_object(&config, key).await;
    outcome.unwrap_or_else(|e| panic!("{}", e));
}
