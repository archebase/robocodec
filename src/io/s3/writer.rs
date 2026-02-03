// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 writer for robotics data files.
//!
//! This module provides a writer that uploads robotics data files (MCAP, BAG)
//! to S3 using multipart upload for efficient handling of large files.

use crate::io::metadata::{ChannelInfo, RawMessage};
use crate::io::s3::{client::S3Client, error::FatalError, location::S3Location};

/// Writer for S3-hosted robotics data files.
///
/// This writer buffers data in memory and uploads to S3 using multipart upload
/// when the buffer size exceeds the part size threshold.
///
/// # Limitations
///
/// Due to the synchronous `FormatWriter` trait, all data is buffered in memory
/// and uploaded during `finish()`. For large files (>50MB), consider:
/// - Using the local file writer and then uploading separately
/// - Implementing an async writer API
///
/// The maximum buffer size is 50MB (10x minimum part size) to prevent
/// unbounded memory growth.
///
/// # Multipart Upload
///
/// S3 multipart upload is used for efficient handling of large files:
/// - Default part size: 5MB (S3 minimum)
/// - Parts are uploaded sequentially during `finish()`
/// - Maximum 10,000 parts per upload (50GB with default part size)
/// Get or create a shared Tokio runtime for blocking async operations.
///
/// This reuses a single runtime across all S3 operations, avoiding
/// the overhead of creating a new runtime for each operation.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;

    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

    RT.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("Failed to create shared tokio runtime")
    })
}

use crate::io::traits::FormatWriter;
use crate::{CodecError, Result};
use bytes::Bytes;
use std::collections::HashMap;

/// Default part size for S3 multipart upload (5MB).
const DEFAULT_PART_SIZE: usize = 5 * 1024 * 1024;

/// Minimum part size for S3 multipart upload (5MB).
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Writer for S3-hosted robotics data files.
///
/// This writer buffers data in memory and uploads to S3 using multipart upload
/// when the buffer size exceeds the part size threshold.
pub struct S3Writer {
    /// S3 client for upload operations
    client: S3Client,
    /// S3 location for the output file
    location: S3Location,
    /// Write buffer
    buffer: Vec<u8>,
    /// Part size for multipart upload
    part_size: usize,
    /// Upload ID for multipart upload (None until first part is uploaded)
    upload_id: Option<String>,
    /// List of uploaded parts (part_number, etag)
    parts: Vec<(u32, String)>,
    /// Next part number to upload
    next_part_number: u32,
    /// Whether the writer has been finished
    finished: bool,
    /// Channel ID counter
    next_channel_id: u16,
    /// Registered channels
    channels: HashMap<u16, ChannelInfo>,
    /// Message count
    message_count: u64,
}

impl S3Writer {
    /// Create a new S3 writer.
    ///
    /// # Arguments
    ///
    /// * `location` - S3 location to write to
    /// * `client` - S3 client for upload operations
    pub fn new(location: S3Location, client: S3Client) -> Result<Self> {
        Ok(Self {
            client,
            location,
            buffer: Vec::with_capacity(DEFAULT_PART_SIZE),
            part_size: DEFAULT_PART_SIZE,
            upload_id: None,
            parts: Vec::new(),
            next_part_number: 1,
            finished: false,
            next_channel_id: 0,
            channels: HashMap::new(),
            message_count: 0,
        })
    }

    /// Create a new S3 writer with custom part size.
    ///
    /// # Arguments
    ///
    /// * `location` - S3 location to write to
    /// * `client` - S3 client for upload operations
    /// * `part_size` - Part size for multipart upload (must be >= 5MB)
    pub fn with_part_size(
        location: S3Location,
        client: S3Client,
        part_size: usize,
    ) -> Result<Self> {
        if part_size < MIN_PART_SIZE {
            return Err(CodecError::parse(
                "S3Writer",
                format!("Part size must be at least {} bytes", MIN_PART_SIZE),
            ));
        }
        Ok(Self {
            client,
            location,
            buffer: Vec::with_capacity(part_size),
            part_size,
            upload_id: None,
            parts: Vec::new(),
            next_part_number: 1,
            finished: false,
            next_channel_id: 0,
            channels: HashMap::new(),
            message_count: 0,
        })
    }

    /// Upload the current buffer as a part.
    async fn upload_buffer(&mut self) -> core::result::Result<(), FatalError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Initialize multipart upload if not already done
        if self.upload_id.is_none() {
            let upload_id = self.client.create_upload(&self.location).await?;
            self.upload_id = Some(upload_id);
        }

        // Upload the buffer as a part
        let data = Bytes::from(self.buffer.clone());
        let etag = self
            .client
            .upload_part(
                &self.location,
                self.upload_id.as_ref().unwrap(),
                self.next_part_number,
                data,
            )
            .await?;

        self.parts.push((self.next_part_number, etag));
        self.next_part_number += 1;
        self.buffer.clear();

        Ok(())
    }

    /// Maximum buffer size before forcing a flush (10x part size).
    /// This prevents unbounded memory growth while allowing
    /// for some batching overhead.
    const MAX_BUFFER_SIZE: usize = 10 * MIN_PART_SIZE;

    /// Write raw bytes to the buffer.
    fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        if self.finished {
            return Err(CodecError::parse("S3Writer", "Writer already finished"));
        }

        // Check buffer size before adding new data
        if self.buffer.len() + data.len() > Self::MAX_BUFFER_SIZE {
            return Err(CodecError::parse(
                "S3Writer",
                format!(
                    "Buffer size limit exceeded: {} bytes",
                    Self::MAX_BUFFER_SIZE
                ),
            ));
        }

        self.buffer.extend_from_slice(data);

        // Check if buffer exceeds part size
        while self.buffer.len() >= self.part_size {
            // For async compatibility, we drain ready parts but don't upload yet
            // The actual upload happens in finish() with the tokio runtime
            // This is a limitation of the sync FormatWriter trait
            break;
        }

        Ok(())
    }

    /// Get the S3 location.
    pub fn location(&self) -> &S3Location {
        &self.location
    }
}

impl FormatWriter for S3Writer {
    fn path(&self) -> &str {
        self.location.key()
    }

    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> Result<u16> {
        let id = self.next_channel_id;
        self.next_channel_id = id
            .checked_add(1)
            .ok_or_else(|| CodecError::parse("S3Writer", "Channel ID overflow"))?;

        let channel = ChannelInfo {
            id,
            topic: topic.to_string(),
            message_type: message_type.to_string(),
            encoding: encoding.to_string(),
            schema: schema.map(|s| s.to_string()),
            schema_data: None,
            schema_encoding: None,
            message_count: 0,
            callerid: None,
        };

        self.channels.insert(id, channel);
        Ok(id)
    }

    fn write(&mut self, message: &RawMessage) -> Result<()> {
        // Buffer the message data with size limit check
        self.write_bytes(&message.data)?;
        self.message_count = self.message_count.saturating_add(1);
        Ok(())
    }

    fn write_batch(&mut self, messages: &[RawMessage]) -> Result<()> {
        for msg in messages {
            self.write(msg)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        // Upload remaining buffer
        if !self.buffer.is_empty() {
            // Use shared runtime for async operations
            let rt = shared_runtime();

            rt.block_on(async {
                if let Err(e) = self.upload_buffer().await {
                    // Abort the upload on error
                    if let Some(upload_id) = &self.upload_id {
                        let _ = self.client.abort_upload(&self.location, upload_id).await;
                    }
                    Err(e)
                } else {
                    Ok(())
                }
            })
            .map_err(|e: FatalError| CodecError::EncodeError {
                codec: "S3".to_string(),
                message: e.to_string(),
            })?;
        }

        // Complete the multipart upload
        if let Some(upload_id) = &self.upload_id {
            let parts = self.parts.clone();
            let location = self.location.clone();
            let client = self.client.clone();

            // Use shared runtime for async operations
            let rt = shared_runtime();
            rt.block_on(async move { client.complete_upload(&location, upload_id, parts).await })
                .map_err(|e| CodecError::EncodeError {
                    codec: "S3".to_string(),
                    message: e.to_string(),
                })?;
        } else if !self.buffer.is_empty() && self.parts.is_empty() {
            // Single part upload (small file)
            // Use a simple PUT request
            let data = Bytes::from(self.buffer.clone());
            let location = self.location.clone();
            let client = self.client.clone();

            // Use shared runtime for async operations
            let rt = shared_runtime();
            rt.block_on(async move {
                // For small files, we can use upload_part with part number 1
                // and then complete with just that part
                let upload_id = client.create_upload(&location).await?;
                let etag = client.upload_part(&location, &upload_id, 1, data).await?;
                client
                    .complete_upload(&location, &upload_id, vec![(1, etag)])
                    .await
            })
            .map_err(|e| CodecError::EncodeError {
                codec: "S3".to_string(),
                message: e.to_string(),
            })?;
        }

        self.finished = true;
        Ok(())
    }

    fn message_count(&self) -> u64 {
        self.message_count
    }

    fn channel_count(&self) -> usize {
        self.channels.len()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_writer_new() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let writer = S3Writer::new(location, client);
        assert!(writer.is_ok());
    }

    #[test]
    fn test_s3_writer_with_part_size() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let writer = S3Writer::with_part_size(location, client, 10 * 1024 * 1024);
        assert!(writer.is_ok());
    }

    #[test]
    fn test_s3_writer_invalid_part_size() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let writer = S3Writer::with_part_size(location, client, 1024);
        assert!(writer.is_err());
    }

    #[test]
    fn test_s3_writer_add_channel() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let mut writer = S3Writer::new(location, client).unwrap();

        let id = writer
            .add_channel("/test", "std_msgs/String", "cdr", None)
            .unwrap();
        assert_eq!(id, 0);
        assert_eq!(writer.channel_count(), 1);

        let id2 = writer
            .add_channel("/test2", "std_msgs/Header", "cdr", None)
            .unwrap();
        assert_eq!(id2, 1);
        assert_eq!(writer.channel_count(), 2);
    }

    #[test]
    fn test_s3_writer_path() {
        let location = S3Location::new("bucket", "path/to/test.mcap");
        let client = S3Client::default_client().unwrap();
        let writer = S3Writer::new(location, client).unwrap();
        assert_eq!(writer.path(), "path/to/test.mcap");
    }

    #[test]
    fn test_s3_writer_location() {
        let location = S3Location::new("bucket", "path/to/test.mcap");
        let client = S3Client::default_client().unwrap();
        let writer = S3Writer::new(location, client).unwrap();
        assert_eq!(writer.location().bucket(), "bucket");
        assert_eq!(writer.location().key(), "path/to/test.mcap");
    }

    #[test]
    fn test_s3_writer_write() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let mut writer = S3Writer::new(location, client).unwrap();

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3, 4],
            sequence: None,
        };

        writer.write(&msg).unwrap();
        assert_eq!(writer.message_count(), 1);
        assert!(!writer.buffer.is_empty());
    }

    #[test]
    fn test_s3_writer_write_batch() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let mut writer = S3Writer::new(location, client).unwrap();

        let msg = RawMessage {
            channel_id: 0,
            log_time: 1000,
            publish_time: 1000,
            data: vec![1, 2, 3, 4],
            sequence: None,
        };

        writer
            .write_batch(&[msg.clone(), msg.clone(), msg.clone()])
            .unwrap();
        assert_eq!(writer.message_count(), 3);
    }

    #[test]
    fn test_s3_writer_downcast() {
        let location = S3Location::new("bucket", "test.mcap");
        let client = S3Client::default_client().unwrap();
        let writer = S3Writer::new(location, client).unwrap();

        let as_any: &dyn std::any::Any = writer.as_any();
        assert!(as_any.is::<S3Writer>());
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_PART_SIZE, 5 * 1024 * 1024);
        assert_eq!(MIN_PART_SIZE, 5 * 1024 * 1024);
    }
}
