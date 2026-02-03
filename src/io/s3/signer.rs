// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! AWS SigV4 request signing for S3.

use crate::io::s3::config::AwsCredentials;
use http::{HeaderMap, HeaderValue, Method, Uri};
use std::time::{SystemTime, UNIX_EPOCH};

/// Sign an HTTP request with AWS SigV4.
///
/// This function adds the necessary AWS Signature Version 4 headers to authenticate
/// requests to AWS S3 or compatible services.
///
/// # Arguments
///
/// * `credentials` - AWS credentials (access key ID, secret access key, optional token)
/// * `region` - AWS region (e.g., "us-east-1")
/// * `service` - AWS service name (typically "s3")
/// * `method` - HTTP method
/// * `uri` - Request URI
/// * `headers` - Existing request headers (will be modified in-place)
///
/// # Returns
///
/// Ok(()) if signing succeeded, Err otherwise.
pub fn sign_request(
    credentials: &AwsCredentials,
    region: &str,
    service: &str,
    method: &Method,
    uri: &Uri,
    headers: &mut HeaderMap,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let access_key = credentials.access_key_id();
    let secret_key = credentials.secret_access_key();
    let session_token = credentials.session_token();

    if access_key.is_empty() || secret_key.is_empty() {
        return Err("Credentials are empty".into());
    }

    // Get current timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "System time is before UNIX epoch")?;
    let amz_date = format_amz_date(now.as_secs());
    let date_stamp = &amz_date[..8];

    // Extract host from URI
    let host = uri.host().ok_or("Invalid URI: missing host")?.to_string();

    // Build the path and query string
    let path = uri.path();
    let query = uri.query().map(|q| format!("?{}", q)).unwrap_or_default();
    let canonical_uri = &format!("{}{}", path, query);

    // Set required headers
    headers.insert("Host", HeaderValue::from_str(&host)?);
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_static("UNSIGNED-PAYLOAD"),
    );
    headers.insert("x-amz-date", HeaderValue::from_str(&amz_date)?);

    // Add session token if present
    if let Some(token) = session_token {
        if !token.is_empty() {
            headers.insert("x-amz-security-token", HeaderValue::from_str(token)?);
        }
    }

    // Create canonical query string (empty for our use case)
    let canonical_query_string = "";

    // Create canonical headers
    let canonical_headers = format_canonical_headers(headers);

    // Create signed headers list
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    // Add session token to signed headers if present
    let signed_headers = if session_token.map_or(false, |t| !t.is_empty()) {
        "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
    } else {
        signed_headers
    };

    // Create canonical request
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query_string,
        canonical_headers,
        signed_headers
    );

    // Create string to sign
    let credential_scope = format!("{}/{}/{}/aws4_request", date_stamp, region, service);
    let hashed_canonical_request = hex_sha256(canonical_request.as_bytes());
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, credential_scope, hashed_canonical_request
    );

    // Calculate signature
    let signature = calculate_signature(secret_key, date_stamp, region, service, &string_to_sign);

    // Add authorization header
    let authorization_header = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}/{}/{}/aws4_request, SignedHeaders={}, Signature={}",
        access_key, date_stamp, region, service, signed_headers, signature
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&authorization_header)?,
    );

    Ok(())
}

/// Format timestamp in AMZ date format.
fn format_amz_date(secs: u64) -> String {
    use chrono::DateTime;
    let dt = DateTime::from_timestamp(secs as i64, 0).unwrap();
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Format canonical headers string.
fn format_canonical_headers(headers: &HeaderMap) -> String {
    let mut canonical_headers = String::new();

    // Always include these headers in order
    let header_names = vec!["host", "x-amz-content-sha256", "x-amz-date"];
    let mut header_names = header_names.into_iter().collect::<Vec<_>>();

    // Add x-amz-security-token if present
    if headers.get("x-amz-security-token").is_some() {
        header_names.push("x-amz-security-token");
    }

    for name in header_names {
        if let Some(value) = headers.get(name) {
            if let Ok(value_str) = value.to_str() {
                canonical_headers.push_str(name);
                canonical_headers.push(':');
                canonical_headers.push_str(value_str.trim());
                canonical_headers.push_str("\n");
            }
        }
    }

    canonical_headers
}

/// Calculate SHA-256 hash and return as hex string.
fn hex_sha256(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(data);
    hex::encode(hash)
}

/// Derive signing key.
fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let k_date = {
        let mut mac = HmacSha256::new_from_slice(format!("AWS4{}", secret).as_bytes()).unwrap();
        mac.update(date.as_bytes());
        mac.finalize().into_bytes()
    };

    let k_region = {
        let mut mac = HmacSha256::new_from_slice(&k_date).unwrap();
        mac.update(region.as_bytes());
        mac.finalize().into_bytes()
    };

    let k_service = {
        let mut mac = HmacSha256::new_from_slice(&k_region).unwrap();
        mac.update(service.as_bytes());
        mac.finalize().into_bytes()
    };

    let k_signing = {
        let mut mac = HmacSha256::new_from_slice(&k_service).unwrap();
        mac.update(b"aws4_request");
        mac.finalize().into_bytes()
    };

    let mut result = [0u8; 32];
    result.copy_from_slice(&k_signing[..]);
    result
}

/// Calculate the signature for the string to sign.
fn calculate_signature(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let signing_key = derive_signing_key(secret_key, date_stamp, region, service);

    let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize();

    hex::encode(result.into_bytes())
}

/// Check if we have valid credentials that should be used for signing.
pub fn should_sign(credentials: &AwsCredentials) -> bool {
    !credentials.access_key_id().is_empty() && !credentials.secret_access_key().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_sign_none_credentials() {
        // AwsCredentials::new returns None for empty keys
        let result = AwsCredentials::new("", "");
        assert!(result.is_none());
    }

    #[test]
    fn test_should_sign_valid_credentials() {
        let creds = AwsCredentials::new("key", "secret").unwrap();
        assert!(should_sign(&creds));
    }

    #[test]
    fn test_format_amz_date() {
        let date = format_amz_date(1735689600); // 2025-01-01 00:00:00 UTC
        assert!(date.starts_with("20250101"));
        assert!(date.ends_with("Z"));
    }

    #[test]
    fn test_hex_sha256() {
        let hash = hex_sha256(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
