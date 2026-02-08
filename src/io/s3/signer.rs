// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! AWS `SigV4` request signing for S3.

use crate::io::s3::config::AwsCredentials;
use http::{HeaderMap, HeaderValue, Method, Uri};
use std::time::{SystemTime, UNIX_EPOCH};

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

    // Extract host from URI - include port for non-standard ports
    // This matches what reqwest sends: host:port for non-standard ports
    let host = if let Some(port) = uri.port_u16() {
        // Include port if explicitly specified (e.g., 127.0.0.1:9000)
        format!(
            "{}:{}",
            uri.host().ok_or("Invalid URI: missing host")?,
            port
        )
    } else {
        // For implicit ports (443 for https, 80 for http), use host only
        uri.host().ok_or("Invalid URI: missing host")?.to_string()
    };

    // Build the path and query string
    let path = uri.path();
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let canonical_uri = &format!("{path}{query}");

    // Set required headers
    headers.insert("Host", HeaderValue::from_str(&host)?);
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_static("UNSIGNED-PAYLOAD"),
    );
    headers.insert("x-amz-date", HeaderValue::from_str(&amz_date)?);

    // Add session token if present
    if let Some(token) = session_token
        && !token.is_empty()
    {
        headers.insert("x-amz-security-token", HeaderValue::from_str(token)?);
    }

    // Create canonical query string (empty for our use case)
    let canonical_query_string = "";

    // Create canonical headers
    let canonical_headers = format_canonical_headers(headers);

    // Create signed headers list
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    // Add session token to signed headers if present
    let signed_headers = if session_token.is_some_and(|t| !t.is_empty()) {
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
    let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let hashed_canonical_request = hex_sha256(canonical_request.as_bytes());
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashed_canonical_request}");

    // Calculate signature
    let signature = calculate_signature(secret_key, date_stamp, region, service, &string_to_sign);

    // Add authorization header
    let authorization_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{date_stamp}/{region}/{service}/aws4_request, SignedHeaders={signed_headers}, Signature={signature}"
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
    let dt = DateTime::from_timestamp(secs as i64, 0).expect("valid timestamp for AWS signature");
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
        if let Some(value) = headers.get(name)
            && let Ok(value_str) = value.to_str()
        {
            canonical_headers.push_str(name);
            canonical_headers.push(':');
            canonical_headers.push_str(value_str.trim());
            canonical_headers.push('\n');
        }
    }

    canonical_headers
}

/// Calculate SHA-256 hash and return as hex string.
pub(crate) fn hex_sha256(data: &[u8]) -> String {
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
        let mut mac = HmacSha256::new_from_slice(format!("AWS4{secret}").as_bytes())
            .expect("AWS4 prefix + secret key should be correct length for HMAC");
        mac.update(date.as_bytes());
        mac.finalize().into_bytes()
    };

    let k_region = {
        let mut mac =
            HmacSha256::new_from_slice(&k_date).expect("HMAC output is always correct size");
        mac.update(region.as_bytes());
        mac.finalize().into_bytes()
    };

    let k_service = {
        let mut mac =
            HmacSha256::new_from_slice(&k_region).expect("HMAC output is always correct size");
        mac.update(service.as_bytes());
        mac.finalize().into_bytes()
    };

    let k_signing = {
        let mut mac =
            HmacSha256::new_from_slice(&k_service).expect("HMAC output is always correct size");
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

    let mut mac = HmacSha256::new_from_slice(&signing_key)
        .expect("signing_key from derive_signing_key is always 32 bytes");
    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize();

    hex::encode(result.into_bytes())
}

/// Check if we have valid credentials that should be used for signing.
#[must_use]
pub fn should_sign(credentials: &AwsCredentials) -> bool {
    !credentials.access_key_id().is_empty() && !credentials.secret_access_key().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::s3::config::AwsCredentials;
    use std::str::FromStr;

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

    #[test]
    fn test_hex_sha256_non_empty() {
        let hash = hex_sha256(b"test");
        assert_eq!(
            hash,
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn test_should_sign_empty_access_key() {
        // Empty access key means new() returns None
        let creds = AwsCredentials::new("", "secret");
        assert!(creds.is_none());
    }

    #[test]
    fn test_should_sign_empty_secret_key() {
        // Empty secret key means new() returns None
        let creds = AwsCredentials::new("key", "");
        assert!(creds.is_none());
    }

    #[test]
    fn test_format_amz_date_epoch() {
        let date = format_amz_date(0); // 1970-01-01 00:00:00 UTC
        assert!(date.starts_with("19700101"));
        assert!(date.ends_with("Z"));
    }

    #[test]
    fn test_sign_request_valid_credentials() {
        let creds = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();

        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());

        // Check that required headers were added
        assert!(headers.contains_key("Authorization"));
        assert!(headers.contains_key("x-amz-date"));
        assert!(headers.contains_key("x-amz-content-sha256"));

        // Authorization header should contain our access key
        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn test_sign_request_empty_credentials() {
        // Empty credentials result in None from new()
        let creds = AwsCredentials::new("", "");
        assert!(creds.is_none());
    }

    #[test]
    fn test_sign_request_with_session_token() {
        let creds = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap()
        .with_session_token("session_token");

        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());

        // Check that session token header was added
        assert!(headers.contains_key("x-amz-security-token"));

        // Authorization header should include security-token in signed headers
        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains("x-amz-security-token"));
    }

    #[test]
    fn test_sign_request_with_query_string() {
        let creds = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();

        let uri =
            Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt?versionId=123").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sign_request_post_method() {
        let creds = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();

        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::POST, &uri, &mut headers);
        assert!(result.is_ok());

        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn test_format_canonical_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("Host", HeaderValue::from_static("example.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20250101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let canonical = format_canonical_headers(&headers);

        // Check headers are in correct order and format
        assert!(canonical.contains("host:example.com\n"));
        assert!(canonical.contains("x-amz-content-sha256:UNSIGNED-PAYLOAD\n"));
        assert!(canonical.contains("x-amz-date:20250101T000000Z\n"));
    }

    #[test]
    fn test_sign_request_with_non_standard_port() {
        let creds = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();

        // Test with explicit port 9000 (common for MinIO)
        let uri = Uri::from_str("https://127.0.0.1:9000/bucket/key").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());

        // Host header should include the port
        let host = headers.get("Host").unwrap().to_str().unwrap();
        assert_eq!(host, "127.0.0.1:9000");

        // The canonical headers should also include the port
        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn test_sign_request_with_standard_port() {
        let creds = AwsCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .unwrap();

        // Test with standard HTTPS port (no explicit port in URI)
        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());

        // Host header should NOT include port for implicit standard ports
        let host = headers.get("Host").unwrap().to_str().unwrap();
        assert_eq!(host, "examplebucket.s3.amazonaws.com");
    }

    #[test]
    fn test_format_canonical_headers_with_session_token() {
        let mut headers = HeaderMap::new();
        headers.insert("Host", HeaderValue::from_static("example.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20250101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );
        headers.insert("x-amz-security-token", HeaderValue::from_static("my-token"));

        let canonical = format_canonical_headers(&headers);

        // Session token should be included at the end
        assert!(canonical.contains("x-amz-security-token:my-token\n"));
    }
}
