// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming configuration and types.

/// Streaming mode for reading messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamMode {
    /// Sequential single-threaded processing (low memory, slower)
    Sequential,
    /// Parallel multi-threaded processing (higher memory, faster)
    Parallel,
    /// Adaptive mode: automatically switches based on file size and network conditions
    #[default]
    Adaptive,
}

/// Configuration for streaming operations.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Streaming mode
    pub mode: StreamMode,
    /// Number of chunks to prefetch (for S3/cloud storage)
    pub prefetch_chunks: usize,
    /// Buffer size per chunk in bytes
    pub buffer_size: usize,
    /// Maximum concurrent downloads for S3
    pub max_concurrent_downloads: usize,
    /// Enable progress tracking
    pub enable_progress: bool,
    /// Enable frame-aligned mode (for roboflow integration)
    pub frame_aligned: bool,
    /// Target FPS for frame alignment (only used when frame_aligned is true)
    pub target_fps: u32,
    /// Maximum latency tolerance for state matching in milliseconds
    /// (if None, uses exact timestamp matching which is slower)
    pub max_state_latency_ms: Option<u64>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            mode: StreamMode::Adaptive,
            prefetch_chunks: 4,
            buffer_size: 64 * 1024 * 1024, // 64MB
            max_concurrent_downloads: 8,
            enable_progress: true,
            frame_aligned: false,
            target_fps: 30,
            max_state_latency_ms: Some(50), // 50ms tolerance for closest-state matching
        }
    }
}

impl StreamConfig {
    /// Create a new streaming config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set streaming mode.
    pub fn with_mode(mut self, mode: StreamMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set prefetch chunks.
    pub fn with_prefetch_chunks(mut self, chunks: usize) -> Self {
        self.prefetch_chunks = chunks;
        self
    }

    /// Set buffer size in bytes.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set max concurrent downloads.
    pub fn with_max_concurrent_downloads(mut self, max: usize) -> Self {
        self.max_concurrent_downloads = max;
        self
    }

    /// Enable or disable progress tracking.
    pub fn with_progress(mut self, enable: bool) -> Self {
        self.enable_progress = enable;
        self
    }

    /// Enable frame-aligned mode.
    pub fn with_frame_alignment(mut self, fps: u32) -> Self {
        self.frame_aligned = true;
        self.target_fps = fps;
        self
    }

    /// Set maximum state latency tolerance.
    pub fn with_state_latency_tolerance(mut self, latency_ms: u64) -> Self {
        self.max_state_latency_ms = Some(latency_ms);
        self
    }
}

/// Frame alignment configuration for state matching.
#[derive(Debug, Clone)]
pub struct FrameAlignmentConfig {
    /// Target frames per second
    pub fps: u32,
    /// Topics that provide state data (e.g., joint positions)
    pub state_topics: Vec<String>,
    /// Topics that provide image data
    pub image_topics: Vec<String>,
    /// Maximum latency tolerance for state matching in nanoseconds
    pub max_state_latency_ns: u64,
    /// Whether to use closest-state matching (true) or exact timestamp matching (false)
    pub use_closest_matching: bool,
}

impl FrameAlignmentConfig {
    /// Create a new frame alignment config.
    pub fn new(fps: u32) -> Self {
        Self {
            fps,
            state_topics: Vec::new(),
            image_topics: Vec::new(),
            max_state_latency_ns: 50_000_000, // 50ms default
            use_closest_matching: true,
        }
    }

    /// Add a state topic.
    pub fn with_state_topic(mut self, topic: impl Into<String>) -> Self {
        self.state_topics.push(topic.into());
        self
    }

    /// Add an image topic.
    pub fn with_image_topic(mut self, topic: impl Into<String>) -> Self {
        self.image_topics.push(topic.into());
        self
    }

    /// Set max state latency tolerance.
    pub fn with_max_latency(mut self, latency_ns: u64) -> Self {
        self.max_state_latency_ns = latency_ns;
        self
    }

    /// Use exact timestamp matching (disables closest-state matching).
    pub fn with_exact_matching(mut self) -> Self {
        self.use_closest_matching = false;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_config_default() {
        let config = StreamConfig::default();
        assert_eq!(config.mode, StreamMode::Adaptive);
        assert_eq!(config.prefetch_chunks, 4);
        assert_eq!(config.buffer_size, 64 * 1024 * 1024); // 64MB
        assert_eq!(config.max_concurrent_downloads, 8);
        assert!(config.enable_progress);
        assert!(!config.frame_aligned);
        assert_eq!(config.target_fps, 30);
        assert_eq!(config.max_state_latency_ms, Some(50));
    }

    #[test]
    fn test_stream_config_builder() {
        let config = StreamConfig::new()
            .with_mode(StreamMode::Parallel)
            .with_prefetch_chunks(8)
            .with_buffer_size(128 * 1024 * 1024)
            .with_max_concurrent_downloads(16)
            .with_progress(false)
            .with_frame_alignment(60)
            .with_state_latency_tolerance(100);

        assert_eq!(config.mode, StreamMode::Parallel);
        assert_eq!(config.prefetch_chunks, 8);
        assert_eq!(config.buffer_size, 128 * 1024 * 1024);
        assert_eq!(config.max_concurrent_downloads, 16);
        assert!(!config.enable_progress);
        assert!(config.frame_aligned);
        assert_eq!(config.target_fps, 60);
        assert_eq!(config.max_state_latency_ms, Some(100));
    }

    #[test]
    fn test_stream_mode_equality() {
        assert_eq!(StreamMode::Sequential, StreamMode::Sequential);
        assert_eq!(StreamMode::Parallel, StreamMode::Parallel);
        assert_eq!(StreamMode::Adaptive, StreamMode::Adaptive);
        assert_ne!(StreamMode::Sequential, StreamMode::Parallel);
    }

    #[test]
    fn test_frame_alignment_config_default() {
        let config = FrameAlignmentConfig::new(30);
        assert_eq!(config.fps, 30);
        assert!(config.state_topics.is_empty());
        assert!(config.image_topics.is_empty());
        assert_eq!(config.max_state_latency_ns, 50_000_000); // 50ms
        assert!(config.use_closest_matching);
    }

    #[test]
    fn test_frame_alignment_config_builder() {
        let config = FrameAlignmentConfig::new(60)
            .with_state_topic("/joint_states")
            .with_state_topic("/gripper_state")
            .with_image_topic("/camera/image")
            .with_image_topic("/camera/depth")
            .with_max_latency(100_000_000)
            .with_exact_matching();

        assert_eq!(config.fps, 60);
        assert_eq!(config.state_topics.len(), 2);
        assert!(config.state_topics.contains(&"/joint_states".to_string()));
        assert!(config.state_topics.contains(&"/gripper_state".to_string()));
        assert_eq!(config.image_topics.len(), 2);
        assert!(config.image_topics.contains(&"/camera/image".to_string()));
        assert!(config.image_topics.contains(&"/camera/depth".to_string()));
        assert_eq!(config.max_state_latency_ns, 100_000_000);
        assert!(!config.use_closest_matching);
    }

    #[test]
    fn test_frame_alignment_config_chaining() {
        let config = FrameAlignmentConfig::new(30)
            .with_state_topic("/state1")
            .with_state_topic("/state2")
            .with_state_topic("/state3");

        assert_eq!(config.state_topics.len(), 3);
    }
}
