// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for TransformBuilder.

use pyo3::prelude::*;
use std::collections::HashMap;

use crate::transform::TransformBuilder;

/// Builder for creating topic/type transformation pipelines.
///
/// `TransformBuilder` provides a fluent API for creating transformations
/// that can be applied during rewrite operations.
///
/// # Example
///
/// ```python
/// import robocodec
///
/// builder = robocodec.TransformBuilder()
///     .with_topic_rename("/old", "/new")
///     .with_type_rename("OldMsg", "NewMsg")
///     .with_topic_type_rename("/specific", "nmx.msg.Old", "nmx.msg.New")
///
/// # Note: Pass the builder directly to RoboRewriter
/// rewriter = robocodec.RoboRewriter.with_transforms("input.mcap", builder)
/// stats = rewriter.rewrite("output.mcap")
/// ```
#[pyclass(name = "TransformBuilder")]
#[derive(Clone)]
pub struct PyTransformBuilder {
    topic_mappings: HashMap<String, String>,
    topic_wildcards: Vec<(String, String)>,
    type_mappings: HashMap<String, String>,
    type_wildcards: Vec<(String, String)>,
    topic_type_mappings: HashMap<(String, String), String>,
}

#[pymethods]
impl PyTransformBuilder {
    /// Create a new transform builder.
    ///
    /// # Example
    ///
    /// ```python
    /// builder = robocodec.TransformBuilder()
    /// ```
    #[new]
    fn new() -> Self {
        Self {
            topic_mappings: HashMap::new(),
            topic_wildcards: Vec::new(),
            type_mappings: HashMap::new(),
            type_wildcards: Vec::new(),
            topic_type_mappings: HashMap::new(),
        }
    }

    /// Add a topic rename mapping.
    ///
    /// Parameters
    /// ----------
    /// from : str
    ///     Source topic name
    /// to : str
    ///     Target topic name
    ///
    /// Returns
    /// -------
    /// TransformBuilder
    ///     Self for method chaining
    ///
    /// # Example
    ///
    /// ```python
    /// builder = builder.with_topic_rename("/old_topic", "/new_topic")
    /// ```
    fn with_topic_rename<'a>(
        mut slf: PyRefMut<'a, Self>,
        from: String,
        to: String,
    ) -> PyRefMut<'a, Self> {
        slf.topic_mappings.insert(from, to);
        slf
    }

    /// Add a wildcard topic rename mapping.
    ///
    /// The wildcard `*` matches any topic suffix.
    ///
    /// Parameters
    /// ----------
    /// pattern : str
    ///     Wildcard pattern like "/foo/*"
    /// target : str
    ///     Target pattern like "/bar/*"
    ///
    /// Returns
    /// -------
    /// TransformBuilder
    ///     Self for method chaining
    ///
    /// # Example
    ///
    /// ```python
    /// # Rename all topics starting with /foo/ to /bar/
    /// builder = builder.with_topic_rename_wildcard("/foo/*", "/bar/*")
    /// ```
    fn with_topic_rename_wildcard<'a>(
        mut slf: PyRefMut<'a, Self>,
        pattern: String,
        target: String,
    ) -> PyRefMut<'a, Self> {
        slf.topic_wildcards.push((pattern, target));
        slf
    }

    /// Add a type rename mapping.
    ///
    /// Parameters
    /// ----------
    /// from : str
    ///     Source type name
    /// to : str
    ///     Target type name
    ///
    /// Returns
    /// -------
    /// TransformBuilder
    ///     Self for method chaining
    ///
    /// # Example
    ///
    /// ```python
    /// builder = builder.with_type_rename("old_pkg/Msg", "new_pkg/Msg")
    /// ```
    fn with_type_rename<'a>(
        mut slf: PyRefMut<'a, Self>,
        from: String,
        to: String,
    ) -> PyRefMut<'a, Self> {
        slf.type_mappings.insert(from, to);
        slf
    }

    /// Add a wildcard type rename mapping.
    ///
    /// The wildcard `*` matches any type name suffix.
    ///
    /// Parameters
    /// ----------
    /// pattern : str
    ///     Wildcard pattern like "foo/*"
    /// target : str
    ///     Target pattern like "bar/*"
    ///
    /// Returns
    /// -------
    /// TransformBuilder
    ///     Self for method chaining
    ///
    /// # Example
    ///
    /// ```python
    /// # Rename all types starting with "foo/" to "bar/"
    /// builder = builder.with_type_rename_wildcard("foo/*", "bar/*")
    /// ```
    fn with_type_rename_wildcard<'a>(
        mut slf: PyRefMut<'a, Self>,
        pattern: String,
        target: String,
    ) -> PyRefMut<'a, Self> {
        slf.type_wildcards.push((pattern, target));
        slf
    }

    /// Add a topic-specific type rename mapping.
    ///
    /// This allows the same source type to map to different target types
    /// based on the topic.
    ///
    /// Parameters
    /// ----------
    /// topic : str
    ///     The topic name (exact match)
    /// source_type : str
    ///     Original type name (e.g., "nmx.msg.LowdimData")
    /// target_type : str
    ///     New type name (e.g., "nmx.msg.JointStates")
    ///
    /// Returns
    /// -------
    /// TransformBuilder
    ///     Self for method chaining
    ///
    /// # Example
    ///
    /// ```python
    /// builder = builder.with_topic_type_rename(
    ///     "/lowdim/joint",
    ///     "nmx.msg.LowdimData",
    ///     "nmx.msg.JointStates"
    /// )
    /// ```
    fn with_topic_type_rename<'a>(
        mut slf: PyRefMut<'a, Self>,
        topic: String,
        source_type: String,
        target_type: String,
    ) -> PyRefMut<'a, Self> {
        slf.topic_type_mappings
            .insert((topic, source_type), target_type);
        slf
    }

    /// String representation.
    fn __repr__(&self) -> String {
        let total = self.topic_mappings.len()
            + self.topic_wildcards.len()
            + self.type_mappings.len()
            + self.type_wildcards.len()
            + self.topic_type_mappings.len();
        format!("TransformBuilder(rules={})", total)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl PyTransformBuilder {
    /// Build the transformation pipeline (internal use).
    /// This constructs the MultiTransform from the stored rules.
    pub(crate) fn build_inner(self) -> crate::transform::MultiTransform {
        // Reconstruct the builder from stored rules
        let mut builder = TransformBuilder::new();

        // Note: We can't directly set private fields, so we need to use
        // the public API. But the builder's methods consume self,
        // so we chain them together.
        for (from, to) in self.topic_mappings {
            builder = builder.with_topic_rename(from, to);
        }
        for (pattern, target) in self.topic_wildcards {
            builder = builder.with_topic_rename_wildcard(pattern, target);
        }
        for (from, to) in self.type_mappings {
            builder = builder.with_type_rename(from, to);
        }
        for (pattern, target) in self.type_wildcards {
            builder = builder.with_type_rename_wildcard(pattern, target);
        }
        for ((topic, source_type), target_type) in self.topic_type_mappings {
            builder = builder.with_topic_type_rename(topic, source_type, target_type);
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_builder_new() {
        let builder = PyTransformBuilder::new();
        assert_eq!(builder.topic_mappings.len(), 0);
        assert_eq!(builder.topic_wildcards.len(), 0);
        assert_eq!(builder.type_mappings.len(), 0);
        assert_eq!(builder.type_wildcards.len(), 0);
        assert_eq!(builder.topic_type_mappings.len(), 0);
    }

    #[test]
    fn test_transform_builder_rule_count() {
        let mut builder = PyTransformBuilder::new();
        assert_eq!(builder.topic_mappings.len(), 0);

        builder
            .topic_mappings
            .insert("/old".to_string(), "/new".to_string());
        builder
            .type_mappings
            .insert("OldType".to_string(), "NewType".to_string());

        assert_eq!(builder.topic_mappings.len(), 1);
        assert_eq!(builder.type_mappings.len(), 1);
        assert_eq!(builder.topic_wildcards.len(), 0);
        assert_eq!(builder.type_wildcards.len(), 0);
        assert_eq!(builder.topic_type_mappings.len(), 0);
    }

    #[test]
    fn test_transform_builder_build_inner() {
        let mut builder = PyTransformBuilder::new();
        builder
            .topic_mappings
            .insert("/old".to_string(), "/new".to_string());

        // Should not panic when building
        let _transform = builder.build_inner();
    }
}
