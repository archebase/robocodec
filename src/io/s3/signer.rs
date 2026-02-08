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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "System time is before UNIX epoch")?;
    sign_request_at(
        credentials,
        region,
        service,
        method,
        uri,
        headers,
        now.as_secs(),
    )
}

/// Core signing logic with an explicit timestamp for testability.
fn sign_request_at(
    credentials: &AwsCredentials,
    region: &str,
    service: &str,
    method: &Method,
    uri: &Uri,
    headers: &mut HeaderMap,
    timestamp_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let access_key = credentials.access_key_id();
    let secret_key = credentials.secret_access_key();
    let session_token = credentials.session_token();

    if access_key.is_empty() || secret_key.is_empty() {
        return Err("Credentials are empty".into());
    }

    let amz_date = format_amz_date(timestamp_secs);
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

    // Canonical URI is just the path (URL-encoded), without query string
    let canonical_uri = uri.path();

    // Canonical query string: sorted key=value pairs without leading '?'
    let canonical_query_string = build_canonical_query_string(uri.query().unwrap_or(""));

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

    // Build the payload hash (we always use UNSIGNED-PAYLOAD for streaming)
    let payload_hash = "UNSIGNED-PAYLOAD";

    // Create canonical request per AWS SigV4 spec (6 components):
    //   HTTPMethod \n CanonicalURI \n CanonicalQueryString \n
    //   CanonicalHeaders \n SignedHeaders \n HashedPayload
    //
    // Note: canonical_headers already ends with '\n' (one per header line).
    // The format's '\n' between canonical_headers and signed_headers creates
    // the required blank line separator per the AWS spec.
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query_string,
        canonical_headers,
        signed_headers,
        payload_hash,
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

/// Build the canonical query string per AWS SigV4 spec.
///
/// Parameters are sorted by name, URI-encoded, and joined with '&'.
/// Empty query returns an empty string.
fn build_canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut params: Vec<(&str, &str)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            (key, value)
        })
        .collect();

    // Sort by parameter name, then by value
    params.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
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

/// Build the canonical request string (exposed for testing).
#[cfg(test)]
fn build_canonical_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    let canonical_uri = uri.path();
    let canonical_query_string = build_canonical_query_string(uri.query().unwrap_or(""));
    let canonical_headers = format_canonical_headers(headers);

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query_string,
        canonical_headers,
        signed_headers,
        payload_hash,
    )
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

    // ── Helper constants for deterministic tests ──────────────────────
    const TEST_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
    const TEST_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    // 2025-01-01T00:00:00Z
    const TEST_TIMESTAMP: u64 = 1735689600;

    fn test_creds() -> AwsCredentials {
        AwsCredentials::new(TEST_ACCESS_KEY, TEST_SECRET_KEY).unwrap()
    }

    // ── Credential validation tests ──────────────────────────────────

    #[test]
    fn test_should_sign_none_credentials() {
        let result = AwsCredentials::new("", "");
        assert!(result.is_none());
    }

    #[test]
    fn test_should_sign_valid_credentials() {
        let creds = AwsCredentials::new("key", "secret").unwrap();
        assert!(should_sign(&creds));
    }

    #[test]
    fn test_should_sign_empty_access_key() {
        let creds = AwsCredentials::new("", "secret");
        assert!(creds.is_none());
    }

    #[test]
    fn test_should_sign_empty_secret_key() {
        let creds = AwsCredentials::new("key", "");
        assert!(creds.is_none());
    }

    #[test]
    fn test_sign_request_empty_credentials() {
        let creds = AwsCredentials::new("", "");
        assert!(creds.is_none());
    }

    // ── Utility function tests ───────────────────────────────────────

    #[test]
    fn test_format_amz_date() {
        let date = format_amz_date(TEST_TIMESTAMP);
        assert_eq!(date, "20250101T000000Z");
    }

    #[test]
    fn test_format_amz_date_epoch() {
        let date = format_amz_date(0);
        assert_eq!(date, "19700101T000000Z");
    }

    #[test]
    fn test_hex_sha256_empty() {
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

    // ── Canonical query string tests ─────────────────────────────────

    #[test]
    fn test_build_canonical_query_string_empty() {
        assert_eq!(build_canonical_query_string(""), "");
    }

    #[test]
    fn test_build_canonical_query_string_single_param() {
        assert_eq!(
            build_canonical_query_string("versionId=123"),
            "versionId=123"
        );
    }

    #[test]
    fn test_build_canonical_query_string_sorted() {
        // Parameters must be sorted by key name
        let result = build_canonical_query_string("z=1&a=2&m=3");
        assert_eq!(result, "a=2&m=3&z=1");
    }

    #[test]
    fn test_build_canonical_query_string_same_key() {
        // Same key, sorted by value
        let result = build_canonical_query_string("key=b&key=a");
        assert_eq!(result, "key=a&key=b");
    }

    // ── Canonical headers tests ──────────────────────────────────────

    #[test]
    fn test_format_canonical_headers_order_and_format() {
        let mut headers = HeaderMap::new();
        headers.insert("Host", HeaderValue::from_static("example.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20250101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let canonical = format_canonical_headers(&headers);

        // Must be in alphabetical order: host, x-amz-content-sha256, x-amz-date
        let expected = "host:example.com\n\
                        x-amz-content-sha256:UNSIGNED-PAYLOAD\n\
                        x-amz-date:20250101T000000Z\n";
        assert_eq!(canonical, expected);
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
        assert!(canonical.contains("x-amz-security-token:my-token\n"));
    }

    // ── Canonical request structure tests ─────────────────────────────

    #[test]
    fn test_canonical_request_has_six_components() {
        let uri = Uri::from_str("https://bucket.s3.amazonaws.com/key").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Host", HeaderValue::from_static("bucket.s3.amazonaws.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20250101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let cr = build_canonical_request(
            &Method::GET,
            &uri,
            &headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        );

        // SigV4 canonical request must have exactly 6 lines
        // (canonical_headers contributes 3 lines ending with \n, plus 3 other lines)
        let lines: Vec<&str> = cr.split('\n').collect();
        // Structure:
        //   0: GET
        //   1: /key
        //   2: (empty - canonical query string)
        //   3: host:bucket.s3.amazonaws.com
        //   4: x-amz-content-sha256:UNSIGNED-PAYLOAD
        //   5: x-amz-date:20250101T000000Z
        //   6: (empty - trailing \n from last header)
        //   7: host;x-amz-content-sha256;x-amz-date
        //   8: UNSIGNED-PAYLOAD
        assert_eq!(lines.len(), 9, "canonical request line count: {lines:?}");
        assert_eq!(lines[0], "GET");
        assert_eq!(lines[1], "/key");
        assert_eq!(lines[2], ""); // empty canonical query string
        assert_eq!(lines[3], "host:bucket.s3.amazonaws.com");
        assert_eq!(lines[4], "x-amz-content-sha256:UNSIGNED-PAYLOAD");
        assert_eq!(lines[5], "x-amz-date:20250101T000000Z");
        assert_eq!(lines[6], ""); // blank line after canonical headers
        assert_eq!(lines[7], "host;x-amz-content-sha256;x-amz-date");
        assert_eq!(lines[8], "UNSIGNED-PAYLOAD"); // payload hash!
    }

    #[test]
    fn test_canonical_request_payload_hash_present() {
        let uri = Uri::from_str("https://bucket.s3.amazonaws.com/key").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Host", HeaderValue::from_static("bucket.s3.amazonaws.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20250101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let cr = build_canonical_request(
            &Method::GET,
            &uri,
            &headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        );

        // The canonical request MUST end with the payload hash
        assert!(
            cr.ends_with("UNSIGNED-PAYLOAD"),
            "canonical request must end with payload hash, got: ...{}",
            &cr[cr.len().saturating_sub(50)..]
        );
    }

    #[test]
    fn test_canonical_request_query_string_not_in_uri() {
        // URI with query string: query must appear in canonical query string field,
        // NOT appended to the canonical URI
        let uri = Uri::from_str("https://bucket.s3.amazonaws.com/key?versionId=123").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Host", HeaderValue::from_static("bucket.s3.amazonaws.com"));
        headers.insert("x-amz-date", HeaderValue::from_static("20250101T000000Z"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );

        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let cr = build_canonical_request(
            &Method::GET,
            &uri,
            &headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        );

        let lines: Vec<&str> = cr.split('\n').collect();
        // Line 1 = canonical URI (path only, no query)
        assert_eq!(
            lines[1], "/key",
            "canonical URI must not contain query string"
        );
        // Line 2 = canonical query string
        assert_eq!(
            lines[2], "versionId=123",
            "query string must be in canonical query string field"
        );
    }

    // ── Host header with port tests ──────────────────────────────────

    #[test]
    fn test_sign_request_host_includes_non_standard_port() {
        let creds = test_creds();
        let uri = Uri::from_str("http://127.0.0.1:9000/bucket/key").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers,
            TEST_TIMESTAMP,
        );
        assert!(result.is_ok());

        let host = headers.get("Host").unwrap().to_str().unwrap();
        assert_eq!(
            host, "127.0.0.1:9000",
            "Host must include non-standard port"
        );
    }

    #[test]
    fn test_sign_request_host_excludes_standard_port() {
        let creds = test_creds();
        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers,
            TEST_TIMESTAMP,
        );
        assert!(result.is_ok());

        let host = headers.get("Host").unwrap().to_str().unwrap();
        assert_eq!(host, "examplebucket.s3.amazonaws.com");
    }

    // ── Deterministic signature tests ────────────────────────────────

    #[test]
    fn test_sign_request_deterministic_no_query() {
        let creds = test_creds();
        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();

        // Sign the same request twice at the same timestamp
        let mut headers1 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers1,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let mut headers2 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers2,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let auth1 = headers1.get("Authorization").unwrap().to_str().unwrap();
        let auth2 = headers2.get("Authorization").unwrap().to_str().unwrap();
        assert_eq!(
            auth1, auth2,
            "same inputs at same timestamp must produce same signature"
        );
    }

    #[test]
    fn test_sign_request_deterministic_with_query() {
        let creds = test_creds();
        let uri =
            Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt?versionId=abc").unwrap();

        let mut headers1 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers1,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let mut headers2 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers2,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let auth1 = headers1.get("Authorization").unwrap().to_str().unwrap();
        let auth2 = headers2.get("Authorization").unwrap().to_str().unwrap();
        assert_eq!(auth1, auth2);
    }

    #[test]
    fn test_sign_request_different_timestamps_differ() {
        let creds = test_creds();
        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();

        let mut headers1 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers1,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let mut headers2 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers2,
            TEST_TIMESTAMP + 1,
        )
        .unwrap();

        let auth1 = headers1.get("Authorization").unwrap().to_str().unwrap();
        let auth2 = headers2.get("Authorization").unwrap().to_str().unwrap();
        assert_ne!(
            auth1, auth2,
            "different timestamps must produce different signatures"
        );
    }

    #[test]
    fn test_sign_request_query_string_affects_signature() {
        let creds = test_creds();

        let uri_no_q = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let uri_with_q =
            Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt?versionId=1").unwrap();

        let mut h1 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri_no_q,
            &mut h1,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let mut h2 = HeaderMap::new();
        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri_with_q,
            &mut h2,
            TEST_TIMESTAMP,
        )
        .unwrap();

        let auth1 = h1.get("Authorization").unwrap().to_str().unwrap();
        let auth2 = h2.get("Authorization").unwrap().to_str().unwrap();
        assert_ne!(auth1, auth2, "query string must affect the signature");
    }

    // ── AWS SigV4 reference test ─────────────────────────────────────
    // Validates the canonical request format against the AWS specification.
    // Reference: https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html

    #[test]
    fn test_sigv4_canonical_request_format_aws_spec() {
        // Use the well-known AWS example credentials and fixed timestamp
        let creds = test_creds();
        let uri =
            Uri::from_str("https://examplebucket.s3.amazonaws.com/photos/photo1.jpg").unwrap();
        let mut headers = HeaderMap::new();

        sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers,
            TEST_TIMESTAMP,
        )
        .unwrap();

        // Verify the Authorization header has the correct structure
        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential="));
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(auth.contains("Signature="));

        // Verify credential scope
        assert!(auth.contains("20250101/us-east-1/s3/aws4_request"));

        // Verify x-amz-content-sha256 is set to UNSIGNED-PAYLOAD
        let content_sha = headers
            .get("x-amz-content-sha256")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(content_sha, "UNSIGNED-PAYLOAD");
    }

    // ── sign_request integration tests ───────────────────────────────

    #[test]
    fn test_sign_request_valid_credentials() {
        let creds = test_creds();
        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());

        assert!(headers.contains_key("Authorization"));
        assert!(headers.contains_key("x-amz-date"));
        assert!(headers.contains_key("x-amz-content-sha256"));

        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains(TEST_ACCESS_KEY));
    }

    #[test]
    fn test_sign_request_with_session_token() {
        let creds = test_creds().with_session_token("session_token");

        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());

        assert!(headers.contains_key("x-amz-security-token"));

        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains("x-amz-security-token"));
    }

    #[test]
    fn test_sign_request_with_query_string() {
        let creds = test_creds();
        let uri =
            Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt?versionId=123").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::GET, &uri, &mut headers);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sign_request_post_method() {
        let creds = test_creds();
        let uri = Uri::from_str("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request(&creds, "us-east-1", "s3", &Method::POST, &uri, &mut headers);
        assert!(result.is_ok());

        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.contains(TEST_ACCESS_KEY));
    }

    // ── MinIO path-style URL test ────────────────────────────────────

    #[test]
    fn test_sign_request_minio_path_style() {
        let creds = AwsCredentials::new("minioadmin", "minioadmin").unwrap();
        let uri = Uri::from_str("http://127.0.0.1:9000/mybucket/mykey.bag").unwrap();
        let mut headers = HeaderMap::new();

        let result = sign_request_at(
            &creds,
            "us-east-1",
            "s3",
            &Method::GET,
            &uri,
            &mut headers,
            TEST_TIMESTAMP,
        );
        assert!(result.is_ok());

        // Host must include port for MinIO
        let host = headers.get("Host").unwrap().to_str().unwrap();
        assert_eq!(host, "127.0.0.1:9000");

        // Authorization header must be well-formed
        let auth = headers.get("Authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with(
            "AWS4-HMAC-SHA256 Credential=minioadmin/20250101/us-east-1/s3/aws4_request"
        ));
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(auth.contains("Signature="));

        // Verify signature is a 64-char hex string
        let sig_start = auth.find("Signature=").unwrap() + "Signature=".len();
        let signature = &auth[sig_start..];
        assert_eq!(signature.len(), 64, "signature must be 64 hex chars");
        assert!(
            signature.chars().all(|c| c.is_ascii_hexdigit()),
            "signature must be hex: {signature}"
        );
    }
}
