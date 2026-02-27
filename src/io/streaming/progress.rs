// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Progress tracking for streaming operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const NONE_U64: u64 = u64::MAX;
const NONE_USIZE: u64 = u64::MAX;

/// Progress event for streaming operations.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Download progress (for S3/cloud storage)
    Download {
        /// Bytes downloaded so far
        bytes_downloaded: u64,
        /// Total bytes to download (if known)
        total_bytes: Option<u64>,
        /// Download percentage (0-100)
        percentage: f32,
    },
    /// Parsing progress
    Parsing {
        /// Messages parsed so far
        messages_parsed: u64,
        /// Total messages (if known)
        total_messages: Option<u64>,
        /// Current chunk being parsed
        current_chunk: usize,
        /// Total chunks (if known)
        total_chunks: Option<usize>,
    },
    /// Frame alignment progress (for roboflow integration)
    FrameAlignment {
        /// Frames emitted so far
        frames_emitted: u64,
        /// Messages buffered waiting for alignment
        messages_buffered: usize,
    },
    /// Processing complete
    Complete,
    /// Error occurred
    Error {
        /// Error message
        message: String,
    },
}

/// Progress tracker for streaming operations.
#[derive(Debug, Clone)]
pub struct ProgressTracker {
    inner: Arc<ProgressTrackerInner>,
}

#[derive(Debug)]
struct ProgressTrackerInner {
    bytes_downloaded: AtomicU64,
    total_bytes: AtomicU64,
    messages_parsed: AtomicU64,
    total_messages: AtomicU64,
    current_chunk: AtomicU64,
    total_chunks: AtomicU64,
    frames_emitted: AtomicU64,
    messages_buffered: AtomicU64,
}

impl ProgressTracker {
    /// Create a new progress tracker.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ProgressTrackerInner {
                bytes_downloaded: AtomicU64::new(0),
                total_bytes: AtomicU64::new(NONE_U64),
                messages_parsed: AtomicU64::new(0),
                total_messages: AtomicU64::new(NONE_U64),
                current_chunk: AtomicU64::new(0),
                total_chunks: AtomicU64::new(NONE_USIZE),
                frames_emitted: AtomicU64::new(0),
                messages_buffered: AtomicU64::new(0),
            }),
        }
    }

    /// Create a progress tracker with known totals.
    pub fn with_totals(
        total_bytes: Option<u64>,
        total_messages: Option<u64>,
        total_chunks: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(ProgressTrackerInner {
                bytes_downloaded: AtomicU64::new(0),
                total_bytes: AtomicU64::new(total_bytes.unwrap_or(NONE_U64)),
                messages_parsed: AtomicU64::new(0),
                total_messages: AtomicU64::new(total_messages.unwrap_or(NONE_U64)),
                current_chunk: AtomicU64::new(0),
                total_chunks: AtomicU64::new(total_chunks.map(|c| c as u64).unwrap_or(NONE_USIZE)),
                frames_emitted: AtomicU64::new(0),
                messages_buffered: AtomicU64::new(0),
            }),
        }
    }

    /// Update bytes downloaded.
    pub fn update_bytes_downloaded(&self, bytes: u64) {
        self.inner
            .bytes_downloaded
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Set total bytes.
    pub fn set_total_bytes(&self, bytes: u64) {
        self.inner.total_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Increment messages parsed.
    pub fn increment_messages(&self) {
        self.inner.messages_parsed.fetch_add(1, Ordering::Relaxed);
    }

    /// Set total messages.
    pub fn set_total_messages(&self, messages: u64) {
        self.inner.total_messages.store(messages, Ordering::Relaxed);
    }

    /// Set current chunk.
    pub fn set_current_chunk(&self, chunk: usize) {
        self.inner
            .current_chunk
            .store(chunk as u64, Ordering::Relaxed);
    }

    /// Set total chunks.
    pub fn set_total_chunks(&self, chunks: usize) {
        self.inner
            .total_chunks
            .store(chunks as u64, Ordering::Relaxed);
    }

    /// Increment frames emitted.
    pub fn increment_frames(&self) {
        self.inner.frames_emitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Set messages buffered.
    pub fn set_messages_buffered(&self, buffered: usize) {
        self.inner
            .messages_buffered
            .store(buffered as u64, Ordering::Relaxed);
    }

    /// Get current download progress event.
    pub fn download_event(&self) -> ProgressEvent {
        let bytes_downloaded = self.inner.bytes_downloaded.load(Ordering::Relaxed);
        let total_bytes_val = self.inner.total_bytes.load(Ordering::Relaxed);
        let total_bytes = if total_bytes_val == NONE_U64 {
            None
        } else {
            Some(total_bytes_val)
        };
        let percentage = total_bytes
            .map(|t| (bytes_downloaded as f32 / t as f32) * 100.0)
            .unwrap_or(0.0)
            .min(100.0);

        ProgressEvent::Download {
            bytes_downloaded,
            total_bytes,
            percentage,
        }
    }

    /// Get current parsing progress event.
    pub fn parsing_event(&self) -> ProgressEvent {
        let messages_parsed = self.inner.messages_parsed.load(Ordering::Relaxed);
        let total_messages_val = self.inner.total_messages.load(Ordering::Relaxed);
        let total_messages = if total_messages_val == NONE_U64 {
            None
        } else {
            Some(total_messages_val)
        };
        let current_chunk = self.inner.current_chunk.load(Ordering::Relaxed) as usize;
        let total_chunks_val = self.inner.total_chunks.load(Ordering::Relaxed);
        let total_chunks = if total_chunks_val == NONE_USIZE {
            None
        } else {
            Some(total_chunks_val as usize)
        };

        ProgressEvent::Parsing {
            messages_parsed,
            total_messages,
            current_chunk,
            total_chunks,
        }
    }

    /// Get current frame alignment event.
    pub fn frame_alignment_event(&self) -> ProgressEvent {
        let frames_emitted = self.inner.frames_emitted.load(Ordering::Relaxed);
        let messages_buffered = self.inner.messages_buffered.load(Ordering::Relaxed) as usize;

        ProgressEvent::FrameAlignment {
            frames_emitted,
            messages_buffered,
        }
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker_new() {
        let tracker = ProgressTracker::new();
        let event = tracker.download_event();
        match event {
            ProgressEvent::Download {
                bytes_downloaded,
                total_bytes,
                percentage,
            } => {
                assert_eq!(bytes_downloaded, 0);
                assert_eq!(total_bytes, None);
                assert_eq!(percentage, 0.0);
            }
            _ => panic!("Expected Download event"),
        }
    }

    #[test]
    fn test_progress_tracker_with_totals() {
        let tracker = ProgressTracker::with_totals(Some(1000), Some(500), Some(10));

        let event = tracker.parsing_event();
        match event {
            ProgressEvent::Parsing {
                total_messages,
                total_chunks,
                ..
            } => {
                assert_eq!(total_messages, Some(500));
                assert_eq!(total_chunks, Some(10));
            }
            _ => panic!("Expected Parsing event"),
        }
    }

    #[test]
    fn test_update_bytes_downloaded() {
        let tracker = ProgressTracker::with_totals(Some(1000), None, None);
        tracker.update_bytes_downloaded(500);

        let event = tracker.download_event();
        match event {
            ProgressEvent::Download {
                bytes_downloaded,
                percentage,
                ..
            } => {
                assert_eq!(bytes_downloaded, 500);
                assert_eq!(percentage, 50.0);
            }
            _ => panic!("Expected Download event"),
        }
    }

    #[test]
    fn test_increment_messages() {
        let tracker = ProgressTracker::new();
        tracker.increment_messages();
        tracker.increment_messages();
        tracker.increment_messages();

        let event = tracker.parsing_event();
        match event {
            ProgressEvent::Parsing {
                messages_parsed, ..
            } => {
                assert_eq!(messages_parsed, 3);
            }
            _ => panic!("Expected Parsing event"),
        }
    }

    #[test]
    fn test_set_total_bytes() {
        let tracker = ProgressTracker::new();
        tracker.set_total_bytes(2048);

        let event = tracker.download_event();
        match event {
            ProgressEvent::Download { total_bytes, .. } => {
                assert_eq!(total_bytes, Some(2048));
            }
            _ => panic!("Expected Download event"),
        }
    }

    #[test]
    fn test_percentage_calculation() {
        let tracker = ProgressTracker::with_totals(Some(100), None, None);
        tracker.update_bytes_downloaded(25);

        let event = tracker.download_event();
        match event {
            ProgressEvent::Download { percentage, .. } => {
                assert_eq!(percentage, 25.0);
            }
            _ => panic!("Expected Download event"),
        }

        // Test percentage capped at 100
        tracker.update_bytes_downloaded(200);
        let event = tracker.download_event();
        match event {
            ProgressEvent::Download { percentage, .. } => {
                assert_eq!(percentage, 100.0);
            }
            _ => panic!("Expected Download event"),
        }
    }

    #[test]
    fn test_frame_alignment_event() {
        let tracker = ProgressTracker::new();
        tracker.increment_frames();
        tracker.increment_frames();
        tracker.set_messages_buffered(10);

        let event = tracker.frame_alignment_event();
        match event {
            ProgressEvent::FrameAlignment {
                frames_emitted,
                messages_buffered,
            } => {
                assert_eq!(frames_emitted, 2);
                assert_eq!(messages_buffered, 10);
            }
            _ => panic!("Expected FrameAlignment event"),
        }
    }

    #[test]
    fn test_progress_event_clone() {
        let event = ProgressEvent::Download {
            bytes_downloaded: 100,
            total_bytes: Some(1000),
            percentage: 10.0,
        };
        let cloned = event.clone();

        match cloned {
            ProgressEvent::Download {
                bytes_downloaded,
                total_bytes,
                percentage,
            } => {
                assert_eq!(bytes_downloaded, 100);
                assert_eq!(total_bytes, Some(1000));
                assert_eq!(percentage, 10.0);
            }
            _ => panic!("Expected Download event"),
        }
    }
}
