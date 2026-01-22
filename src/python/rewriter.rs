// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for RoboRewriter.

use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::rewriter::{RewriteOptions, RoboRewriter as IoRoboRewriter};

use super::error::to_py_result;
use super::metadata::PyRewriteStats;
use super::transform::PyTransformBuilder;

/// Unified rewriter with format auto-detection and transform support.
///
/// `RoboRewriter` reads a robotics data file, applies optional transformations
/// (topic/type renaming), and writes to an output file.
///
/// # Example
///
/// ```python
/// import robocodec
///
/// # Simple rewrite
/// rewriter = robocodec.RoboRewriter("input.mcap")
/// stats = rewriter.rewrite("output.mcap")
///
/// # Note: For transformations, use the standalone rewrite_* functions
/// # or create a rewriter via the TransformBuilder.build_rewriter() method
/// ```
#[pyclass(name = "RoboRewriter")]
pub struct PyRoboRewriter {
    inner: IoRoboRewriter,
}

#[pymethods]
impl PyRoboRewriter {
    /// Open a file for rewriting (format auto-detected).
    ///
    /// Parameters
    /// ----------
    /// input_path : str
    ///     Path to the input file (.mcap or .bag)
    /// validate_schemas : bool, default True
    ///     Whether to validate message schemas
    /// skip_decode_failures : bool, default True
    ///     Whether to skip messages that fail to decode
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If the file cannot be opened or format is not recognized
    ///
    /// # Example
    ///
    /// ```python
    /// rewriter = robocodec.RoboRewriter("input.mcap")
    /// ```
    #[new]
    #[pyo3(signature = (input_path, validate_schemas=true, skip_decode_failures=true))]
    fn new(input_path: &str, validate_schemas: bool, skip_decode_failures: bool) -> PyResult<Self> {
        let options = RewriteOptions {
            validate_schemas,
            skip_decode_failures,
            passthrough_non_cdr: true,
            transforms: None,
        };

        let inner = to_py_result(IoRoboRewriter::with_options(input_path, options))?;
        Ok(Self { inner })
    }

    /// Open a file for rewriting with transformations.
    ///
    /// This is a class method that creates a rewriter with the specified
    /// transformation pipeline.
    ///
    /// Parameters
    /// ----------
    /// input_path : str
    ///     Path to the input file (.mcap or .bag)
    /// transform_builder : TransformBuilder
    ///     Transformation builder for topic/type renaming
    /// validate_schemas : bool, default True
    ///     Whether to validate message schemas
    /// skip_decode_failures : bool, default True
    ///     Whether to skip messages that fail to decode
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If the file cannot be opened or format is not recognized
    ///
    /// # Example
    ///
    /// ```python
    /// builder = robocodec.TransformBuilder()
    ///     .with_topic_rename("/old", "/new")
    /// rewriter = robocodec.RoboRewriter.with_transforms("input.mcap", builder)
    /// ```
    #[classmethod]
    #[pyo3(signature = (input_path, transform_builder, validate_schemas=true, skip_decode_failures=true))]
    fn with_transforms(
        _cls: &Bound<'_, PyType>,
        input_path: &str,
        transform_builder: PyTransformBuilder,
        validate_schemas: bool,
        skip_decode_failures: bool,
    ) -> PyResult<Self> {
        let transforms = Some(transform_builder.build_inner());
        let options = RewriteOptions {
            validate_schemas,
            skip_decode_failures,
            passthrough_non_cdr: true,
            transforms,
        };

        let inner = to_py_result(IoRoboRewriter::with_options(input_path, options))?;
        Ok(Self { inner })
    }

    /// Rewrite to an output file.
    ///
    /// Parameters
    /// ----------
    /// output_path : str
    ///     Path to the output file
    ///
    /// Returns
    /// -------
    /// RewriteStats
    ///     Statistics about the rewrite operation
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If the rewrite operation fails
    ///
    /// # Example
    ///
    /// ```python
    /// stats = rewriter.rewrite("output.mcap")
    /// print(f"Processed {stats.message_count} messages")
    /// print(f"Renamed {stats.topics_renamed} topics")
    /// ```
    fn rewrite(&mut self, output_path: &str) -> PyResult<PyRewriteStats> {
        let stats = to_py_result(self.inner.rewrite(output_path))?;
        Ok(PyRewriteStats::from(&stats))
    }

    /// Get the input file path.
    ///
    /// Returns
    /// -------
    /// str
    ///     Path to the input file
    #[getter]
    fn input_path(&self) -> String {
        self.inner.input_path().display().to_string()
    }

    /// Get the rewrite options being used.
    ///
    /// Returns
    /// -------
    /// bool
    ///     Whether schemas are being validated
    #[getter]
    fn validate_schemas(&self) -> bool {
        self.inner.options().validate_schemas
    }

    /// Get whether decode failures are being skipped.
    ///
    /// Returns
    /// -------
    /// bool
    ///     Whether decode failures are being skipped
    #[getter]
    fn skip_decode_failures(&self) -> bool {
        self.inner.options().skip_decode_failures
    }

    /// Get whether transformations are configured.
    ///
    /// Returns
    /// -------
    /// bool
    ///     Whether transformations are enabled
    #[getter]
    fn has_transforms(&self) -> bool {
        self.inner.options().has_transforms()
    }

    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "RoboRewriter(input='{}', transforms={})",
            self.inner.input_path().display(),
            self.has_transforms()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_py_rewrite_stats_from_rust() {
        let rust_stats = crate::rewriter::RewriteStats {
            message_count: 500,
            channel_count: 5,
            decode_failures: 1,
            encode_failures: 0,
            reencoded_count: 400,
            passthrough_count: 99,
            topics_renamed: 2,
            types_renamed: 1,
        };

        let py_stats = PyRewriteStats::from(&rust_stats);

        assert_eq!(py_stats.message_count, 500);
        assert_eq!(py_stats.channel_count, 5);
        assert_eq!(py_stats.decode_failures, 1);
        assert_eq!(py_stats.encode_failures, 0);
        assert_eq!(py_stats.reencoded_count, 400);
        assert_eq!(py_stats.passthrough_count, 99);
        assert_eq!(py_stats.topics_renamed, 2);
        assert_eq!(py_stats.types_renamed, 1);
    }

    #[test]
    fn test_py_rewrite_stats_field_values() {
        let stats = PyRewriteStats {
            message_count: 100,
            channel_count: 2,
            decode_failures: 0,
            encode_failures: 0,
            reencoded_count: 80,
            passthrough_count: 20,
            topics_renamed: 1,
            types_renamed: 0,
        };

        // Just verify the struct can be created and fields are accessible
        assert_eq!(stats.message_count, 100);
        assert_eq!(stats.channel_count, 2);
        assert_eq!(stats.decode_failures, 0);
    }
}
