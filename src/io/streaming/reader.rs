// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming reader for high-performance message processing.

use crate::io::detection::detect_format;
use crate::io::metadata::{ChannelInfo, FileFormat};
use crate::io::reader::RoboReader;
use crate::io::reader::config::ReaderConfig;
use crate::io::streaming::config::{FrameAlignmentConfig, StreamConfig};
use crate::io::streaming::progress::ProgressTracker;
use crate::io::streaming::stream::{AlignedFrame, TimestampedMessage};
use crate::io::traits::FormatReader;
use crate::{CodecError, CodecValue, Result};

/// A streaming reader for robotics data files.
///
/// Provides high-performance streaming with support for:
/// - Streaming download from S3/cloud storage
/// - Parallel message processing
/// - Frame-aligned output (for roboflow integration)
/// - Progress tracking
///
/// # Example
///
/// ```rust,no_run
/// use robocodec::io::streaming::{StreamingRoboReader, StreamConfig, StreamMode};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = StreamConfig::new()
///     .with_mode(StreamMode::Parallel)
///     .with_prefetch_chunks(4);
///
/// let reader = StreamingRoboReader::open(
///     "s3://my-bucket/data.mcap",
///     config
/// ).await?;
///
/// for result in reader.message_stream() {
///     let msg = result?;
///     println!("{} @ {}: {:?}", msg.topic, msg.log_time, msg.data);
/// }
/// # Ok(())
/// # }
/// ```
pub struct StreamingRoboReader {
    inner: Box<dyn FormatReader>,
    #[allow(dead_code)]
    config: StreamConfig,
    progress: ProgressTracker,
}

impl StreamingRoboReader {
    /// Open a file with streaming configuration.
    ///
    /// Supports both local file paths and S3 URLs.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file or S3 URL
    /// * `config` - Streaming configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::streaming::{StreamingRoboReader, StreamConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let reader = StreamingRoboReader::open(
    ///     "data.mcap",
    ///     StreamConfig::new()
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(path: &str, config: StreamConfig) -> Result<Self> {
        // Try to parse as URL and create appropriate transport
        #[cfg(feature = "remote")]
        {
            if let Some(transport) = Self::parse_url_to_transport(path).await? {
                let path_for_detection = path.split('?').next().unwrap_or(path);
                let path_obj = std::path::Path::new(path_for_detection);
                let format = detect_format(path_obj)?;

                let inner: Box<dyn FormatReader> = match format {
                    FileFormat::Mcap => Box::new(
                        crate::io::formats::mcap::transport_reader::McapTransportReader::open_from_transport(
                            transport,
                            path.to_string(),
                        )?,
                    ),
                    FileFormat::Bag => Box::new(
                        crate::io::formats::bag::BagTransportReader::open_from_transport(
                            transport,
                            path.to_string(),
                        )?,
                    ),
                    FileFormat::Rrd => Box::new(
                        crate::io::formats::rrd::RrdTransportReader::open_from_transport(
                            transport,
                            path.to_string(),
                        )?,
                    ),
                    FileFormat::Unknown => {
                        return Err(CodecError::parse(
                            "StreamingRoboReader",
                            format!("Unknown file format for path: {path}"),
                        ));
                    }
                };

                let progress = ProgressTracker::with_totals(
                    Some(inner.file_size()),
                    Some(inner.message_count()),
                    None,
                );

                return Ok(Self {
                    inner,
                    config,
                    progress,
                });
            }
        }

        // Local file - use standard RoboReader
        let reader = RoboReader::open_with_config(path, ReaderConfig::default())?;
        let file_size = reader.file_size();
        let message_count = reader.message_count();

        // Convert to StreamingRoboReader by extracting inner
        let inner = reader.into_inner();

        let progress = ProgressTracker::with_totals(Some(file_size), Some(message_count), None);

        Ok(Self {
            inner,
            config,
            progress,
        })
    }

    /// Process all messages with a callback function.
    ///
    /// This method consumes the reader and calls the provided function
    /// for each decoded message.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use robocodec::io::streaming::{StreamingRoboReader, StreamConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let reader = StreamingRoboReader::open("data.mcap", StreamConfig::new()).await?;
    /// reader.process_messages(|msg| {
    ///     println!("Topic: {}", msg.topic);
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn process_messages<F>(self, mut callback: F) -> Result<()>
    where
        F: FnMut(TimestampedMessage) -> Result<()>,
    {
        let decoded_iter = self
            .inner
            .decoded_with_timestamp_boxed()
            .expect("Failed to create decoded iterator");

        for result in decoded_iter {
            let (msg, ch) = result?;
            let timestamped_msg = TimestampedMessage {
                topic: ch.topic.clone(),
                log_time: msg.log_time,
                publish_time: msg.publish_time,
                sequence: 0,
                data: CodecValue::Struct(msg.message),
                channel: ch,
            };
            self.progress.increment_messages();
            callback(timestamped_msg)?;
        }

        Ok(())
    }

    /// Get a message stream for iterating over decoded messages.
    ///
    /// This method consumes the reader and returns a vector of all messages.
    /// For large files, consider using `process_messages()` instead.
    pub fn collect_messages(self) -> Result<Vec<TimestampedMessage>> {
        let mut messages = Vec::new();
        self.process_messages(|msg| {
            messages.push(msg);
            Ok(())
        })?;
        Ok(messages)
    }

    /// Process frames with a callback function.
    ///
    /// This method consumes the reader and calls the provided function
    /// for each aligned frame. Uses closest-state matching for performance.
    ///
    /// # Arguments
    ///
    /// * `config` - Frame alignment configuration
    /// * `callback` - Function to call for each frame
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use robocodec::io::streaming::{FrameAlignmentConfig, StreamingRoboReader, StreamConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let reader = StreamingRoboReader::open("data.mcap", StreamConfig::new()).await?;
    /// let frame_config = FrameAlignmentConfig::new(30)
    ///     .with_image_topic("/camera/image")
    ///     .with_state_topic("/joint_states");
    ///
    /// reader.process_frames(frame_config, |frame| {
    ///     println!("Frame {} @ {}ns", frame.frame_index, frame.timestamp);
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn process_frames<F>(self, config: FrameAlignmentConfig, mut callback: F) -> Result<()>
    where
        F: FnMut(AlignedFrame) -> Result<()>,
    {
        let mut frame_stream =
            crate::io::streaming::stream::FrameStream::with_progress(config, self.progress.clone());

        self.process_messages(|msg| {
            let frames = frame_stream.process_message(msg);
            for frame in frames {
                callback(frame)?;
            }
            Ok(())
        })?;

        // Process any remaining frames
        let remaining = frame_stream.drain_remaining();
        for frame in remaining {
            callback(frame)?;
        }

        Ok(())
    }

    /// Collect all aligned frames.
    ///
    /// This method consumes the reader and returns a vector of all frames.
    /// For large files, consider using `process_frames()` instead.
    pub fn collect_frames(self, config: FrameAlignmentConfig) -> Result<Vec<AlignedFrame>> {
        let mut frames = Vec::new();
        self.process_frames(config, |frame| {
            frames.push(frame);
            Ok(())
        })?;
        // Sort frames by timestamp to ensure chronological order
        // (necessary when multiple image topics are configured)
        frames.sort_by_key(|f| f.timestamp);
        // Reassign frame indices after sorting
        for (i, frame) in frames.iter_mut().enumerate() {
            frame.frame_index = i;
        }
        Ok(frames)
    }

    /// Get the progress tracker.
    pub fn progress(&self) -> &ProgressTracker {
        &self.progress
    }

    /// Get file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.inner.file_size()
    }

    /// Get total message count.
    pub fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    /// Get channels information.
    pub fn channels(&self) -> &std::collections::HashMap<u16, ChannelInfo> {
        self.inner.channels()
    }

    #[cfg(feature = "remote")]
    async fn parse_url_to_transport(
        url: &str,
    ) -> Result<Option<Box<dyn crate::io::transport::Transport>>> {
        use crate::io::transport::s3::S3Transport;

        // Check for s3:// scheme
        if let Ok(location) = crate::io::s3::S3Location::from_s3_url(url) {
            // Create S3Transport
            let client = crate::io::s3::S3Client::default_client().map_err(|e| {
                CodecError::encode("S3", format!("Failed to create S3 client: {e}"))
            })?;
            let transport = S3Transport::new(client, location).await.map_err(|e| {
                CodecError::encode("S3", format!("Failed to create S3 transport: {e}"))
            })?;
            return Ok(Some(Box::new(transport)));
        }

        // Not a URL - treat as local path
        Ok(None)
    }
}
