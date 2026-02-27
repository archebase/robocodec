// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Streaming API for high-performance message processing.
//!
//! This module provides:
//! - The [`StreamingParser`] trait for low-level chunk-based parsing
//! - High-level streaming readers with [`StreamingRoboReader`]
//! - Frame-aligned streaming for roboflow integration
//! - Progress tracking
//!
//! # Example: Basic Streaming
//!
//! ```rust,no_run
//! use robocodec::io::streaming::{StreamingRoboReader, StreamConfig, StreamMode};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = StreamConfig::new()
//!     .with_mode(StreamMode::Parallel)
//!     .with_prefetch_chunks(4);
//!
//! let reader = StreamingRoboReader::open(
//!     "s3://my-bucket/data.mcap",
//!     config
//! ).await?;
//!
//! for result in reader.message_stream() {
//!     let msg = result?;
//!     println!("{} @ {}: {:?}", msg.topic, msg.log_time, msg.data);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Frame-Aligned Streaming
//!
//! ```rust,no_run
//! use robocodec::io::streaming::{
//!     StreamingRoboReader, StreamConfig,
//!     FrameAlignmentConfig
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let reader = StreamingRoboReader::open(
//!     "data.mcap",
//!     StreamConfig::new()
//! ).await?;
//!
//! let frame_config = FrameAlignmentConfig::new(30)
//!     .with_image_topic("/camera/image")
//!     .with_state_topic("/joint_states");
//!
//! for result in reader.frame_stream(frame_config) {
//!     let frame = result?;
//!     println!("Frame {}: {} images, {} states",
//!         frame.frame_index,
//!         frame.images.len(),
//!         frame.states.len()
//!     );
//! }
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod parser;
pub mod progress;
pub mod reader;
pub mod stream;

// Re-export the core trait
pub use parser::{AsStreamingParser, StreamingParser};

// Re-export new streaming API types
pub use config::{FrameAlignmentConfig, StreamConfig, StreamMode};
pub use progress::{ProgressEvent, ProgressTracker};
pub use reader::StreamingRoboReader;
pub use stream::{AlignedFrame, ImageData, StreamEvent, TimestampedMessage};
