// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Upload test fixtures to MinIO for S3 testing.

use std::path::Path;
use std::time::Duration;

/// Simple URL encoding for AWS credentials
fn encode_url(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '&' | '=' | '?' | '#' | ' ' | '/' | ':' => format!("%{:02X}", c as u8),
            _ => c.to_string(),
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let bucket = std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "test-fixtures".to_string());
    let access_key = std::env::var("MINIO_USER").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = std::env::var("MINIO_PASSWORD").unwrap_or_else(|_| "minioadmin".to_string());

    let fixtures_dir = Path::new("tests/fixtures");
    if !fixtures_dir.exists() {
        eprintln!(
            "Error: Fixtures directory not found at {}",
            fixtures_dir.display()
        );
        std::process::exit(1);
    }

    println!("Uploading fixtures to MinIO...");
    println!("  Endpoint: {}", endpoint);
    println!("  Bucket: {}", bucket);

    // Check if MinIO is running
    let health_url = format!("{}/minio/health/live", endpoint);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .danger_accept_invalid_certs(true)
        .build()?;

    match client.head(&health_url).send().await {
        Ok(resp) if resp.status().is_success() => {}
        _ => {
            eprintln!("Error: MinIO is not running at {}", endpoint);
            eprintln!("Start MinIO with: docker compose up -d");
            std::process::exit(1);
        }
    }

    // Use simple PUT with path-style URL (MinIO default)
    // For MinIO with default credentials, we can use virtual-hosted style
    let mut count = 0;
    let mut failed = 0;

    for entry in std::fs::read_dir(fixtures_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("mcap")
            || path.extension().and_then(|s| s.to_str()) == Some("bag")
        {
            let filename = path.file_name().unwrap().to_string_lossy().to_string();
            let data = std::fs::read(&path)?;

            // MinIO accepts path-style URLs with query param credentials
            let url = format!(
                "{}/{}/{}?AWSAccessKeyId={}&x-amz-security-token={}",
                endpoint,
                bucket,
                filename,
                encode_url(&access_key),
                encode_url(&secret_key)
            );

            let response = client
                .put(&url)
                .header("Content-Type", "application/octet-stream")
                .body(data)
                .send()
                .await?;

            if response.status().is_success() {
                println!("  Uploaded {}", filename);
                count += 1;
            } else {
                eprintln!(
                    "  Failed to upload {}: HTTP {}",
                    filename,
                    response.status()
                );
                failed += 1;
            }
        }
    }

    println!("\nUploaded {} fixture files", count);
    if failed > 0 {
        eprintln!(
            "Failed to upload {} files (may need proper S3 client)",
            failed
        );
        eprintln!("\nTry creating bucket manually:");
        eprintln!("  docker run --rm -it minio/mc \\");
        eprintln!("    mc alias set local http://localhost:9000 minioadmin minioadmin && \\");
        eprintln!("    mc mb local/test-fixtures && \\");
        eprintln!("    mc cp tests/fixtures/*.mcap local/test-fixtures/");
    }

    if count == 0 {
        std::process::exit(1);
    }

    println!("\nRun tests with: cargo test --features s3 minio_tests");

    Ok(())
}
