// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings via PyO3.
//!
//! This module provides Python 3.11+ bindings for robocodec using PyO3.
//!
//! # Python API
//!
//! ```python
//! import robocodec
//!
//! # Reading
//! reader = robocodec.RoboReader("data.mcap")
//! print(f"Messages: {reader.message_count}")
//!
//! # Writing
//! writer = robocodec.RoboWriter("output.mcap")
//! channel_id = writer.add_channel("/topic", "std_msgs/String", "cdr", None)
//! writer.finish()
//!
//! # Rewriting with transforms
//! builder = robocodec.TransformBuilder()
//!     .with_topic_rename("/old", "/new")
//!     .with_type_rename("OldMsg", "NewMsg")
//!
//! rewriter = robocodec.RoboRewriter("input.mcap", transform_builder=builder)
//! stats = rewriter.rewrite("output.mcap")
//! print(f"Processed {stats.message_count} messages")
//! ```

#[cfg(feature = "python")]
use pyo3::prelude::*;

// Public submodule exports
pub mod convert;
pub mod error;
pub mod metadata;
pub mod reader;
pub mod rewriter;
pub mod transform;
pub mod writer;

// Re-export key types for convenience
pub use error::RobocodecError;
pub use metadata::PyChannelInfo;
pub use metadata::PyRewriteStats;
pub use reader::PyRoboReader;
pub use rewriter::PyRoboRewriter;
pub use transform::PyTransformBuilder;
pub use writer::PyRoboWriter;

/// Python module for robocodec robotics data library.
#[pymodule]
fn _robocodec(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add exception class (created by create_exception! macro)
    m.add("RobocodecError", m.py().get_type::<RobocodecError>())?;

    // Add remaining classes
    m.add_class::<PyRoboReader>()?;
    m.add_class::<PyRoboWriter>()?;
    m.add_class::<PyRoboRewriter>()?;
    m.add_class::<PyTransformBuilder>()?;
    m.add_class::<PyChannelInfo>()?;
    m.add_class::<PyRewriteStats>()?;

    Ok(())
}
