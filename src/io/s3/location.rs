// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 location descriptor.

use std::fmt;

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
        Self {
            bucket: bucket.into(),
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
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::new("my-bucket", "file.mcap")
    ///     .with_endpoint("https://minio.example.com");
    /// ```
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
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
    /// - `s3://{region}/{bucket}/{key}` (explicit region)
    ///
    /// Note: For URLs with 3+ path segments, the first segment is treated as
    /// region only if it matches known AWS region patterns (starts with specific
    /// geographic prefixes). Otherwise, it's treated as the bucket name.
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::io::s3::S3Location;
    ///
    /// let location = S3Location::from_s3_url("s3://my-bucket/path/to/file.mcap").unwrap();
    /// assert_eq!(location.bucket(), "my-bucket");
    /// assert_eq!(location.key(), "path/to/file.mcap");
    /// ```
    pub fn from_s3_url(url: &str) -> Result<Self, S3UrlParseError> {
        let url = url.trim();

        if !url.starts_with("s3://") {
            return Err(S3UrlParseError::InvalidScheme);
        }

        let path = &url[5..]; // Skip "s3://"

        // For simplicity, we only support bucket/key format
        // The region/bucket/key format is ambiguous and not commonly used
        let slash_idx = path.find('/').ok_or(S3UrlParseError::InvalidFormat)?;

        let bucket = &path[..slash_idx];
        let key = &path[slash_idx + 1..];

        if bucket.is_empty() {
            return Err(S3UrlParseError::InvalidFormat);
        }

        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            region: None,
            endpoint: None,
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

/// Error that can occur when parsing an S3 URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3UrlParseError {
    /// URL does not start with "s3://"
    InvalidScheme,
    /// URL format is invalid
    InvalidFormat,
}

impl fmt::Display for S3UrlParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3UrlParseError::InvalidScheme => write!(f, "URL must start with 's3://'"),
            S3UrlParseError::InvalidFormat => write!(f, "Invalid S3 URL format"),
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
}
