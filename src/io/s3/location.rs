// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 location descriptor.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use url::Url;

/// S3 location descriptor.
///
/// Represents the location of an object stored in S3 or an S3-compatible service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    /// Bucket name
    bucket: String,
    /// Object key
    key: String,
    /// Optional region (defaults to env/config)
    region: Option<String>,
    /// Optional S3 endpoint URL (for MinIO/R2 compatibility)
    endpoint: Option<String>,
}

/// Validate an S3 bucket name according to AWS naming rules.
///
/// Bucket names must:
/// - Be 3-63 characters long
/// - Contain only lowercase letters, numbers, dots, and hyphens
/// - Start and end with a letter or number
/// - Not contain two adjacent dots
/// - Not be formatted as an IP address (e.g., 192.168.1.1)
fn validate_bucket_name(bucket: &str) -> Result<(), S3UrlParseError> {
    let len = bucket.len();

    // Length check
    if !(3..=63).contains(&len) {
        return Err(S3UrlParseError::InvalidBucketName);
    }

    // Character set check
    if !bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
    {
        return Err(S3UrlParseError::InvalidBucketName);
    }

    // Must start and end with letter/number
    if bucket
        .bytes()
        .next()
        .map(|b| !b.is_ascii_alphanumeric())
        .unwrap_or(false)
        || bucket
            .bytes()
            .last()
            .map(|b| !b.is_ascii_alphanumeric())
            .unwrap_or(false)
    {
        return Err(S3UrlParseError::InvalidBucketName);
    }

    // No two adjacent dots
    if bucket.contains("..") {
        return Err(S3UrlParseError::InvalidBucketName);
    }

    // Not an IP address format
    if bucket.parse::<Ipv4Addr>().is_ok() || bucket.parse::<Ipv6Addr>().is_ok() {
        return Err(S3UrlParseError::InvalidBucketName);
    }

    Ok(())
}

/// Validate an endpoint URL to prevent SSRF attacks.
///
/// Ensures:
/// - URL is valid and uses HTTPS (or HTTP for localhost in tests)
/// - Host is not a private/internal IP address (except localhost for tests)
fn validate_endpoint(endpoint: &str) -> Result<(), S3UrlParseError> {
    let url = Url::parse(endpoint).map_err(|_| S3UrlParseError::InvalidEndpoint)?;

    // For testing, allow HTTP for localhost
    let is_localhost = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host.starts_with("127.0.0.1")
            || host.starts_with("[::1]")
            || host.ends_with(".localhost")
    });

    // Must be HTTPS, unless it's localhost (for testing)
    if url.scheme() != "https" && !is_localhost {
        return Err(S3UrlParseError::EndpointNotHttps);
    }

    // Check host against blocked patterns
    if let Some(host) = url.host_str() {
        // Allow localhost for HTTP (testing), but block for HTTPS
        let is_local_http = is_localhost && url.scheme() == "http";

        // Block localhost variants (unless using HTTP for testing)
        if !is_local_http && (host == "localhost" || host.ends_with(".localhost")) {
            return Err(S3UrlParseError::BlockedEndpoint);
        }

        // Block common internal/private IP patterns (unless localhost with HTTP for testing)
        if let Ok(addr) = host.parse::<IpAddr>() {
            let is_loopback = matches!(addr, IpAddr::V4(v4) if v4.is_loopback())
                || matches!(addr, IpAddr::V6(v6) if v6.is_loopback() || v6.is_unspecified());

            // Allow loopback for HTTP (testing), block otherwise
            let should_block = if is_loopback {
                !is_local_http
            } else {
                matches!(addr, IpAddr::V4(v4) if v4.is_private() || v4.is_link_local() || v4.is_unspecified())
                    || matches!(addr, IpAddr::V6(v6) if v6.is_unspecified())
            };

            if should_block {
                return Err(S3UrlParseError::BlockedEndpoint);
            }
        }

        // Block metadata service endpoints (AWS and cloud provider metadata services)
        if host.contains("169.254.169.254")
            || host.contains("100.100.100.200")
            || host.contains("metadata.google.internal")
        {
            return Err(S3UrlParseError::BlockedEndpoint);
        }
    }

    Ok(())
}

impl S3Location {
    /// Create a new S3 location.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The S3 bucket name
    /// * `key` - The object key within the bucket
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::new("my-bucket", "path/to/file.mcap");
    /// ```
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        let bucket = bucket.into();
        // Validate bucket name
        validate_bucket_name(&bucket).unwrap();
        Self {
            bucket,
            key: key.into(),
            region: None,
            endpoint: None,
        }
    }

    /// Set the AWS region.
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::new("my-bucket", "file.mcap")
    ///     .with_region("us-west-2");
    /// ```
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom S3 endpoint URL.
    ///
    /// This is useful for S3-compatible services like MinIO or Cloudflare R2.
    ///
    /// # Security
    ///
    /// The endpoint must use HTTPS and cannot point to:
    /// - Private IP addresses (192.168.x.x, 10.x.x.x, 172.16-31.x.x)
    /// - Loopback addresses (localhost, 127.x.x.x)
    /// - Cloud metadata services (169.254.169.254)
    ///
    /// # Panics
    ///
    /// Panics if the endpoint URL is invalid, uses HTTP instead of HTTPS,
    /// or points to a blocked/private IP address.
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::new("my-bucket", "file.mcap")
    ///     .with_endpoint("https://minio.example.com");
    /// ```
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        validate_endpoint(&endpoint).expect("Invalid endpoint URL");
        self.endpoint = Some(endpoint);
        self
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the object key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the region, if set.
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// Get the custom endpoint, if set.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Get the full S3 URL for this location.
    ///
    /// The URL format depends on whether a custom endpoint is set:
    /// - Default: `https://{bucket}.s3.{region}.amazonaws.com/{key}`
    /// - Custom endpoint: `{endpoint}/{bucket}/{key}`
    pub fn url(&self) -> String {
        if let Some(endpoint) = &self.endpoint {
            // Custom endpoint (MinIO, R2, etc.)
            format!(
                "{}/{}/{}",
                endpoint.trim_end_matches('/'),
                self.bucket,
                self.key
            )
        } else if let Some(region) = &self.region {
            // AWS S3 with region
            format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.bucket, region, self.key
            )
        } else {
            // AWS S3 default (us-east-1 style)
            format!("https://{}.s3.amazonaws.com/{}", self.bucket, self.key)
        }
    }

    /// Create an S3Location from an s3:// URL.
    ///
    /// Supports formats:
    /// - `s3://{bucket}/{key}`
    /// - `s3://{bucket}/{key}?endpoint={custom_endpoint}` (for MinIO, Alibaba OSS, etc.)
    /// - `s3://{bucket}/{key}?region={region}` (explicit region)
    ///
    /// The endpoint query parameter is useful for S3-compatible services:
    /// - MinIO: `s3://bucket/key?endpoint=http://localhost:9000`
    /// - Alibaba OSS: `s3://bucket/key?endpoint=https://oss-cn-hangzhou.aliyuncs.com`
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::from_s3_url("s3://my-bucket/path/to/file.mcap").unwrap();
    /// assert_eq!(location.bucket(), "my-bucket");
    /// assert_eq!(location.key(), "path/to/file.mcap");
    ///
    /// let location = S3Location::from_s3_url(
    ///     "s3://my-bucket/file.mcap?endpoint=http://localhost:9000"
    /// ).unwrap();
    /// assert_eq!(location.endpoint(), Some("http://localhost:9000"));
    /// ```
    pub fn from_s3_url(url: &str) -> Result<Self, S3UrlParseError> {
        let url = url.trim();

        if !url.starts_with("s3://") {
            return Err(S3UrlParseError::InvalidScheme);
        }

        let path = &url[5..]; // Skip "s3://"

        // Split query string if present
        let (path_without_query, query) = match path.find('?') {
            Some(q) => (&path[..q], Some(&path[q + 1..])),
            None => (path, None),
        };

        // For simplicity, we only support bucket/key format
        let slash_idx = path_without_query
            .find('/')
            .ok_or(S3UrlParseError::InvalidFormat)?;

        let bucket = &path_without_query[..slash_idx];
        let key = &path_without_query[slash_idx + 1..];

        if bucket.is_empty() {
            return Err(S3UrlParseError::InvalidFormat);
        }

        // Validate bucket name for security
        validate_bucket_name(bucket)?;

        // Parse query parameters with URL decoding
        let mut endpoint = None;
        let mut region = None;

        if let Some(query_str) = query {
            for pair in query_str.split('&') {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                // URL-decode the value (e.g., %3A -> ':')
                let decoded = percent_encoding::percent_decode_str(value)
                    .decode_utf8()
                    .ok()
                    .map(|v| v.into_owned());
                match (key, decoded) {
                    ("endpoint", Some(value)) if !value.is_empty() => endpoint = Some(value),
                    ("region", Some(value)) if !value.is_empty() => region = Some(value),
                    _ => {} // Ignore unknown parameters, decode failures, or empty values
                }
            }
        }

        // If no endpoint specified, check S3_ENDPOINT environment variable
        if endpoint.is_none() {
            endpoint = std::env::var("S3_ENDPOINT").ok();
        }

        // Validate endpoint for security (SSRF prevention)
        if let Some(ref ep) = endpoint {
            validate_endpoint(ep)?;
        }

        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            region,
            endpoint,
        })
    }

    /// Get the file extension of the object key.
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::new("bucket", "path/to/file.mcap");
    /// assert_eq!(location.extension(), Some("mcap"));
    /// ```
    pub fn extension(&self) -> Option<&str> {
        // Find the last dot in the key
        let dot_pos = self.key.rfind('.')?;
        // Ensure there's at least one character after the dot
        if dot_pos + 1 >= self.key.len() {
            return None;
        }
        // Ensure the dot is not the first character (hidden files like .gitignore)
        // and the character before the dot is not a path separator (e.g., path/to/.hidden)
        if dot_pos == 0 {
            return None;
        }
        // Check if the character before the dot is a path separator
        if self.key.as_bytes()[dot_pos - 1] == b'/' {
            return None;
        }
        // Check that there's no path separator after the dot
        if self.key[dot_pos..].contains('/') {
            return None;
        }
        Some(&self.key[dot_pos + 1..])
    }

    /// Check if this location points to an MCAP file.
    pub fn is_mcap(&self) -> bool {
        self.extension() == Some("mcap")
    }

    /// Check if this location points to a BAG file.
    pub fn is_bag(&self) -> bool {
        self.extension() == Some("bag")
    }
}

impl fmt::Display for S3Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

/// Error that can occur when parsing or validating an S3 URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3UrlParseError {
    /// URL does not start with "s3://"
    InvalidScheme,
    /// URL format is invalid
    InvalidFormat,
    /// Bucket name is invalid
    InvalidBucketName,
    /// Endpoint URL is invalid
    InvalidEndpoint,
    /// Endpoint must use HTTPS
    EndpointNotHttps,
    /// Endpoint points to a blocked/private IP
    BlockedEndpoint,
}

impl fmt::Display for S3UrlParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3UrlParseError::InvalidScheme => write!(f, "URL must start with 's3://'"),
            S3UrlParseError::InvalidFormat => write!(f, "Invalid S3 URL format"),
            S3UrlParseError::InvalidBucketName => {
                write!(
                    f,
                    "Bucket name must be 3-63 chars, lowercase alphanumeric, dot, or hyphen"
                )
            }
            S3UrlParseError::InvalidEndpoint => write!(f, "Invalid endpoint URL"),
            S3UrlParseError::EndpointNotHttps => write!(f, "Endpoint must use HTTPS scheme"),
            S3UrlParseError::BlockedEndpoint => {
                write!(f, "Endpoint points to a blocked or private IP address")
            }
        }
    }
}

impl std::error::Error for S3UrlParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_location_new() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap");
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
        assert!(location.region().is_none());
        assert!(location.endpoint().is_none());
    }

    #[test]
    fn test_s3_location_builder() {
        let location = S3Location::new("my-bucket", "path/to/file.bag")
            .with_region("us-west-2")
            .with_endpoint("https://s3.amazonaws.com");

        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.bag");
        assert_eq!(location.region(), Some("us-west-2"));
        assert_eq!(location.endpoint(), Some("https://s3.amazonaws.com"));
    }

    #[test]
    fn test_s3_location_url_default() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap");
        assert_eq!(
            location.url(),
            "https://my-bucket.s3.amazonaws.com/path/to/file.mcap"
        );
    }

    #[test]
    fn test_s3_location_url_with_region() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap").with_region("eu-west-1");
        assert_eq!(
            location.url(),
            "https://my-bucket.s3.eu-west-1.amazonaws.com/path/to/file.mcap"
        );
    }

    #[test]
    fn test_s3_location_url_with_endpoint() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap")
            .with_endpoint("https://minio.example.com");
        assert_eq!(
            location.url(),
            "https://minio.example.com/my-bucket/path/to/file.mcap"
        );

        let location = S3Location::new("my-bucket", "path/to/file.mcap")
            .with_endpoint("https://minio.example.com/");
        assert_eq!(
            location.url(),
            "https://minio.example.com/my-bucket/path/to/file.mcap"
        );
    }

    #[test]
    fn test_s3_location_from_url() {
        let location = S3Location::from_s3_url("s3://my-bucket/path/to/file.mcap").unwrap();
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
        assert!(location.region().is_none());
    }

    #[test]
    fn test_s3_location_from_url_invalid() {
        assert!(S3Location::from_s3_url("http://example.com/file").is_err());
        assert!(S3Location::from_s3_url("s3://").is_err());
        assert!(S3Location::from_s3_url("s3://bucket").is_err());
        assert!(S3Location::from_s3_url("s3:///key").is_err());
    }

    #[test]
    fn test_s3_location_extension() {
        let location = S3Location::new("bucket", "path/to/file.mcap");
        assert_eq!(location.extension(), Some("mcap"));

        let location = S3Location::new("bucket", "path/to/file.bag");
        assert_eq!(location.extension(), Some("bag"));

        let location = S3Location::new("bucket", "path/to/no_extension");
        assert_eq!(location.extension(), None);

        let location = S3Location::new("bucket", "path/to/.hidden");
        assert_eq!(location.extension(), None);

        let location = S3Location::new("bucket", "path/to/file.tar.gz");
        assert_eq!(location.extension(), Some("gz"));

        let location = S3Location::new("bucket", "");
        assert_eq!(location.extension(), None);
    }

    #[test]
    fn test_s3_location_is_mcap() {
        let location = S3Location::new("bucket", "file.mcap");
        assert!(location.is_mcap());
        assert!(!location.is_bag());
    }

    #[test]
    fn test_s3_location_is_bag() {
        let location = S3Location::new("bucket", "file.bag");
        assert!(location.is_bag());
        assert!(!location.is_mcap());
    }

    #[test]
    fn test_s3_location_display() {
        let location = S3Location::new("my-bucket", "path/to/file.mcap");
        assert_eq!(format!("{}", location), "s3://my-bucket/path/to/file.mcap");
    }

    #[test]
    fn test_s3_location_equality() {
        let loc1 = S3Location::new("bucket", "key");
        let loc2 = S3Location::new("bucket", "key");
        assert_eq!(loc1, loc2);

        let loc3 = S3Location::new("bucket", "key").with_region("us-west-2");
        assert_ne!(loc1, loc3);
    }

    #[test]
    fn test_from_url_with_nested_key() {
        let location = S3Location::from_s3_url("s3://bucket/path/to/nested/file.mcap").unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "path/to/nested/file.mcap");
    }

    #[test]
    fn test_from_url_key_with_slash() {
        let location = S3Location::from_s3_url("s3://bucket/path/to/file.mcap").unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "path/to/file.mcap");
    }

    #[test]
    fn test_from_url_with_endpoint_query() {
        let location =
            S3Location::from_s3_url("s3://bucket/file.mcap?endpoint=http://localhost:9000")
                .unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        assert_eq!(location.endpoint(), Some("http://localhost:9000"));
    }

    #[test]
    fn test_from_url_with_region_query() {
        let location = S3Location::from_s3_url("s3://bucket/file.mcap?region=eu-west-1").unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        assert_eq!(location.region(), Some("eu-west-1"));
    }

    #[test]
    fn test_from_url_with_endpoint_and_region_query() {
        let location = S3Location::from_s3_url(
            "s3://bucket/file.mcap?endpoint=https://oss-cn-hangzhou.aliyuncs.com&region=cn-hangzhou",
        )
        .unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        assert_eq!(
            location.endpoint(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
        assert_eq!(location.region(), Some("cn-hangzhou"));
    }

    #[test]
    fn test_from_url_with_endpoint_minio() {
        let location =
            S3Location::from_s3_url("s3://my-bucket/data.bag?endpoint=http://localhost:9000")
                .unwrap();
        assert_eq!(location.bucket(), "my-bucket");
        assert_eq!(location.key(), "data.bag");
        assert_eq!(location.endpoint(), Some("http://localhost:9000"));
    }

    #[test]
    fn test_from_url_empty_query_value() {
        // Empty endpoint query parameter should be ignored
        let location = S3Location::from_s3_url("s3://bucket/file.mcap?endpoint=").unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        // Empty endpoint values are ignored
        assert_eq!(location.endpoint(), None);
    }

    #[test]
    fn test_from_url_with_unknown_query_param() {
        // Unknown query parameters should be ignored
        let location = S3Location::from_s3_url("s3://bucket/file.mcap?unknown=value").unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        assert!(location.endpoint().is_none());
        assert!(location.region().is_none());
    }

    #[test]
    fn test_from_url_with_multiple_query_params() {
        let location = S3Location::from_s3_url(
            "s3://bucket/file.mcap?endpoint=http://localhost:9000&region=us-west-1&unknown=value",
        )
        .unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        assert_eq!(location.endpoint(), Some("http://localhost:9000"));
        assert_eq!(location.region(), Some("us-west-1"));
    }

    #[test]
    fn test_from_url_with_encoded_endpoint() {
        // URL-encoded endpoint should be decoded
        let location =
            S3Location::from_s3_url("s3://bucket/file.mcap?endpoint=http%3A%2F%2Flocalhost%3A9000")
                .unwrap();
        assert_eq!(location.bucket(), "bucket");
        assert_eq!(location.key(), "file.mcap");
        assert_eq!(location.endpoint(), Some("http://localhost:9000"));
    }
}
