// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming types for message and frame processing.

use crate::io::metadata::ChannelInfo;
use crate::io::streaming::config::FrameAlignmentConfig;
use crate::io::streaming::progress::{ProgressEvent, ProgressTracker};
use crate::{CodecValue, Result};

/// A message with timestamp information.
#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    /// Topic name
    pub topic: String,
    /// Log time in nanoseconds
    pub log_time: u64,
    /// Publish time in nanoseconds
    pub publish_time: u64,
    /// Message sequence number
    pub sequence: u64,
    /// Decoded message data
    pub data: CodecValue,
    /// Channel information
    pub channel: ChannelInfo,
}

/// A frame containing aligned image and state data.
#[derive(Debug, Clone)]
pub struct AlignedFrame {
    /// Frame index
    pub frame_index: usize,
    /// Frame timestamp in nanoseconds
    pub timestamp: u64,
    /// Images by feature name
    pub images: std::collections::HashMap<String, ImageData>,
    /// State data by feature name
    pub states: std::collections::HashMap<String, Vec<f32>>,
    /// Raw messages that contributed to this frame
    pub messages: Vec<TimestampedMessage>,
}

/// Image data for frames.
#[derive(Debug, Clone)]
pub struct ImageData {
    /// Image width
    pub width: u32,
    /// Image height
    pub height: u32,
    /// Image data (encoded or raw)
    pub data: Vec<u8>,
    /// Whether the data is encoded (JPEG/PNG) or raw RGB
    pub is_encoded: bool,
    /// Original timestamp
    pub original_timestamp: u64,
}

impl AlignedFrame {
    /// Create a new empty frame.
    pub fn new(frame_index: usize, timestamp: u64) -> Self {
        Self {
            frame_index,
            timestamp,
            images: std::collections::HashMap::new(),
            states: std::collections::HashMap::new(),
            messages: Vec::new(),
        }
    }

    /// Add an image to the frame.
    pub fn add_image(
        &mut self,
        name: impl Into<String>,
        width: u32,
        height: u32,
        data: Vec<u8>,
        is_encoded: bool,
    ) {
        self.images.insert(
            name.into(),
            ImageData {
                width,
                height,
                data,
                is_encoded,
                original_timestamp: self.timestamp,
            },
        );
    }

    /// Add state data to the frame.
    pub fn add_state(&mut self, name: impl Into<String>, values: Vec<f32>) {
        self.states.insert(name.into(), values);
    }

    /// Get an image by name.
    pub fn get_image(&self, name: &str) -> Option<&ImageData> {
        self.images.get(name)
    }

    /// Get state data by name.
    pub fn get_state(&self, name: &str) -> Option<&Vec<f32>> {
        self.states.get(name)
    }

    /// Check if the frame has all required images.
    pub fn has_required_images(&self, required: &[impl AsRef<str>]) -> bool {
        required
            .iter()
            .all(|r| self.images.contains_key(r.as_ref()))
    }

    /// Check if the frame has all required state.
    pub fn has_required_state(&self, required: &[impl AsRef<str>]) -> bool {
        required
            .iter()
            .all(|r| self.states.contains_key(r.as_ref()))
    }
}

/// Stream event for message and frame streams.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A decoded message is available
    Message(TimestampedMessage),
    /// An aligned frame is ready (frame-aligned mode only)
    Frame(AlignedFrame),
    /// Progress update
    Progress(ProgressEvent),
    /// Stream complete
    Complete,
    /// Error occurred
    Error(String),
}

/// Iterator-based message stream for synchronous usage.
pub struct MessageStream {
    inner: Box<dyn Iterator<Item = Result<TimestampedMessage>> + Send>,
    progress: ProgressTracker,
}

impl MessageStream {
    /// Create a new message stream from an iterator.
    pub fn new(
        inner: Box<dyn Iterator<Item = Result<TimestampedMessage>> + Send>,
        progress: ProgressTracker,
    ) -> Self {
        Self { inner, progress }
    }

    /// Get the progress tracker.
    pub fn progress(&self) -> &ProgressTracker {
        &self.progress
    }

    /// Collect all messages into a vector.
    pub fn collect_all(self) -> Result<Vec<TimestampedMessage>> {
        self.inner.collect()
    }
}

impl Iterator for MessageStream {
    type Item = Result<TimestampedMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.next();
        if result.is_some() {
            self.progress.increment_messages();
        }
        result
    }
}

/// Frame-aligned stream for roboflow integration.
pub struct FrameStream {
    config: FrameAlignmentConfig,
    progress: ProgressTracker,
    message_buffer: Vec<TimestampedMessage>,
    state_buffer: std::collections::HashMap<String, Vec<(u64, Vec<f32>)>>,
    next_frame_time: Option<u64>,
    frame_index: usize,
}

impl FrameStream {
    /// Create a new frame stream with the given configuration.
    pub fn new(config: FrameAlignmentConfig) -> Self {
        let progress = ProgressTracker::new();
        Self {
            config,
            progress,
            message_buffer: Vec::new(),
            state_buffer: std::collections::HashMap::new(),
            next_frame_time: None,
            frame_index: 0,
        }
    }

    /// Create a new frame stream with custom progress tracker.
    pub fn with_progress(config: FrameAlignmentConfig, progress: ProgressTracker) -> Self {
        Self {
            config,
            progress,
            message_buffer: Vec::new(),
            state_buffer: std::collections::HashMap::new(),
            next_frame_time: None,
            frame_index: 0,
        }
    }

    /// Process a message and return any completed frames.
    pub fn process_message(&mut self, msg: TimestampedMessage) -> Vec<AlignedFrame> {
        let log_time = msg.log_time;

        // Extract state data if this is a state topic
        if self.config.state_topics.contains(&msg.topic)
            && let Some(state) = Self::extract_state(&msg.data)
        {
            let entries = self.state_buffer.entry(msg.topic.clone()).or_default();
            entries.push((msg.log_time, state));
        }

        // Only buffer image-topic messages (which are searched by find_image_at_time).
        // Non-image/non-state messages are never used, so skip them to avoid
        // cloning megabytes of image data for irrelevant topics.
        if self.config.image_topics.contains(&msg.topic) {
            self.message_buffer.push(msg);
            self.progress
                .set_messages_buffered(self.message_buffer.len());
        }

        // Check if we should emit frames
        self.try_emit_frames(log_time)
    }

    /// Drain any remaining frames from the buffer.
    ///
    /// This method can be called multiple times and doesn't consume the stream.
    pub fn drain_remaining(&mut self) -> Vec<AlignedFrame> {
        // Collect image messages first to avoid borrow issues
        let mut image_messages: Vec<TimestampedMessage> = self
            .message_buffer
            .iter()
            .filter(|msg| self.config.image_topics.contains(&msg.topic))
            .cloned()
            .collect();

        // Sort by timestamp to ensure frames are in chronological order
        image_messages.sort_by_key(|msg| msg.log_time);

        // Emit all remaining frames from buffered messages
        let mut frames = Vec::new();
        for msg in image_messages {
            if let Some(frame) = self.create_frame_for_message(&msg, self.frame_index) {
                frames.push(frame);
                self.frame_index += 1;
            }
        }
        // Clear the buffer after processing
        self.message_buffer.clear();
        self.progress.set_messages_buffered(0);
        frames
    }

    /// Finish processing and emit any remaining frames.
    ///
    /// This consumes the stream. Use `drain_remaining()` if you need to
    /// keep the stream alive.
    pub fn finish(mut self) -> Vec<AlignedFrame> {
        self.drain_remaining()
    }

    /// Get the progress tracker.
    pub fn progress(&self) -> &ProgressTracker {
        &self.progress
    }

    fn try_emit_frames(&mut self, current_time: u64) -> Vec<AlignedFrame> {
        let mut frames = Vec::new();
        let frame_interval_ns = 1_000_000_000u64 / self.config.fps as u64;

        // Initialize next frame time if needed
        if self.next_frame_time.is_none() {
            self.next_frame_time = Some(current_time);
        }

        // Emit frames up to current time
        while let Some(frame_time) = self.next_frame_time {
            if frame_time > current_time {
                break;
            }

            // Find image messages at this frame time
            let image_msg = self.find_image_at_time(frame_time).cloned();
            if let Some(msg) = image_msg
                && let Some(mut frame) = self.create_frame(&msg, frame_time, self.frame_index)
            {
                // Find matching state using closest-state matching
                self.match_state_to_frame(&mut frame, frame_time);
                self.progress.increment_frames();
                frames.push(frame);
                self.frame_index += 1;
            }

            self.next_frame_time = Some(frame_time + frame_interval_ns);
        }

        // Evict old messages that can no longer match any future frame.
        // Without this, the buffer grows unboundedly (hundreds of thousands of
        // messages, including MB-sized images), causing O(n) scans to stall.
        if let Some(next_frame) = self.next_frame_time {
            let image_tolerance = 16_666_667u64; // ~16ms, same as find_image_at_time
            let msg_cutoff = next_frame.saturating_sub(image_tolerance);
            self.message_buffer.retain(|msg| msg.log_time >= msg_cutoff);
            self.progress
                .set_messages_buffered(self.message_buffer.len());

            let state_cutoff = next_frame.saturating_sub(self.config.max_state_latency_ns);
            for entries in self.state_buffer.values_mut() {
                entries.retain(|(time, _)| *time >= state_cutoff);
            }
        }

        frames
    }

    fn find_image_at_time(&self, target_time: u64) -> Option<&TimestampedMessage> {
        self.message_buffer.iter().find(|msg| {
            self.config.image_topics.contains(&msg.topic)
                && Self::is_within_tolerance(msg.log_time, target_time, 16_666_667)
            // ~16ms tolerance
        })
    }

    fn create_frame(
        &self,
        msg: &TimestampedMessage,
        frame_time: u64,
        frame_index: usize,
    ) -> Option<AlignedFrame> {
        let mut frame = AlignedFrame::new(frame_index, frame_time);

        // Extract image data
        if let Some(image_data) = Self::extract_image(&msg.data) {
            frame.add_image(
                &msg.topic,
                image_data.width,
                image_data.height,
                image_data.data,
                image_data.is_encoded,
            );
            frame.messages.push(msg.clone());
            Some(frame)
        } else {
            None
        }
    }

    fn create_frame_for_message(
        &self,
        msg: &TimestampedMessage,
        frame_index: usize,
    ) -> Option<AlignedFrame> {
        let mut frame = AlignedFrame::new(frame_index, msg.log_time);

        if let Some(image_data) = Self::extract_image(&msg.data) {
            frame.add_image(
                &msg.topic,
                image_data.width,
                image_data.height,
                image_data.data,
                image_data.is_encoded,
            );
            frame.messages.push(msg.clone());
            self.match_state_to_frame(&mut frame, msg.log_time);
            Some(frame)
        } else {
            None
        }
    }

    fn match_state_to_frame(&self, frame: &mut AlignedFrame, frame_time: u64) {
        for state_topic in &self.config.state_topics {
            if let Some(states) = self.state_buffer.get(state_topic)
                && let Some((_, state_data)) =
                    Self::find_closest_state(states, frame_time, self.config.max_state_latency_ns)
            {
                frame.add_state(state_topic, state_data);
            }
        }
    }

    fn find_closest_state(
        states: &[(u64, Vec<f32>)],
        target_time: u64,
        max_latency: u64,
    ) -> Option<(u64, Vec<f32>)> {
        states
            .iter()
            .min_by_key(|(time, _)| {
                if target_time > *time {
                    target_time - time
                } else {
                    time - target_time
                }
            })
            .filter(|(time, _)| {
                let diff = if target_time > *time {
                    target_time - time
                } else {
                    time - target_time
                };
                diff <= max_latency
            })
            .cloned()
    }

    fn extract_state(data: &CodecValue) -> Option<Vec<f32>> {
        match data {
            CodecValue::Array(arr) => {
                let state: Vec<f32> = arr
                    .iter()
                    .filter_map(|v| match v {
                        CodecValue::Float32(n) => Some(*n),
                        CodecValue::Float64(n) => Some(*n as f32),
                        CodecValue::Int32(n) => Some(*n as f32),
                        CodecValue::Int64(n) => Some(*n as f32),
                        _ => None,
                    })
                    .collect();
                if state.is_empty() { None } else { Some(state) }
            }
            CodecValue::Struct(map) => {
                // Try to extract from "position" field (ROS JointState)
                if let Some(CodecValue::Array(positions)) = map.get("position") {
                    let state: Vec<f32> = positions
                        .iter()
                        .filter_map(|v| match v {
                            CodecValue::Float32(n) => Some(*n),
                            CodecValue::Float64(n) => Some(*n as f32),
                            _ => None,
                        })
                        .collect();
                    if state.is_empty() { None } else { Some(state) }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn extract_image(data: &CodecValue) -> Option<ImageData> {
        match data {
            CodecValue::Struct(map) => {
                // Check for CompressedImage format
                if let Some(format) = map.get("format").and_then(|v| {
                    if let CodecValue::String(s) = v {
                        Some(s.as_str())
                    } else {
                        None
                    }
                }) {
                    // Try to extract data as either Bytes or Array of UInt8
                    if let Some(data) = Self::extract_byte_data(map.get("data")) {
                        // Extract dimensions if available
                        let width = map
                            .get("width")
                            .and_then(|v| match v {
                                CodecValue::UInt32(w) => Some(*w),
                                _ => None,
                            })
                            .unwrap_or(0);
                        let height = map
                            .get("height")
                            .and_then(|v| match v {
                                CodecValue::UInt32(h) => Some(*h),
                                _ => None,
                            })
                            .unwrap_or(0);

                        let is_encoded = format != "rgb8";
                        return Some(ImageData {
                            width,
                            height,
                            data,
                            is_encoded,
                            original_timestamp: 0,
                        });
                    }
                }

                // Check for raw image
                if let (
                    Some(CodecValue::UInt32(width)),
                    Some(CodecValue::UInt32(height)),
                    Some(data),
                ) = (
                    map.get("width"),
                    map.get("height"),
                    Self::extract_byte_data(map.get("data")),
                ) {
                    let expected_rgb_size = (*width as usize) * (*height as usize) * 3;
                    let is_encoded = data.len() < expected_rgb_size;

                    return Some(ImageData {
                        width: *width,
                        height: *height,
                        data,
                        is_encoded,
                        original_timestamp: 0,
                    });
                }

                None
            }
            _ => None,
        }
    }

    /// Extract byte data from either Bytes or Array(UInt8) CodecValue.
    fn extract_byte_data(value: Option<&CodecValue>) -> Option<Vec<u8>> {
        match value {
            Some(CodecValue::Bytes(bytes)) => Some(bytes.clone()),
            Some(CodecValue::Array(arr)) => {
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|v| match v {
                        CodecValue::UInt8(n) => Some(*n),
                        CodecValue::Int8(n) => Some(*n as u8),
                        _ => None,
                    })
                    .collect();
                if bytes.is_empty() && !arr.is_empty() {
                    None
                } else {
                    Some(bytes)
                }
            }
            _ => None,
        }
    }

    fn is_within_tolerance(time: u64, target: u64, tolerance: u64) -> bool {
        if time > target {
            time - target <= tolerance
        } else {
            target - time <= tolerance
        }
    }
}
