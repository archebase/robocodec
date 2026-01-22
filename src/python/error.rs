// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Error conversion between Rust and Python.

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::fmt;

use crate::CodecError;

// Create the custom exception type.
// Note: With abi3 feature, we cannot directly extend PyException.
// Instead, we create a standalone exception that inherits from Exception.
pyo3::create_exception!(robocodec, RobocodecError, PyException);

/// Python exception for robocodec errors with structured data.
///
/// This struct provides structured error information that can be accessed
/// from Python. The actual exception type is `RobocodecError` (created above).
pub struct PyRobocodecError {
    /// Error kind/category (e.g., "ParseError", "InvalidSchema")
    pub kind: String,

    /// Context information (e.g., schema name, codec name)
    pub context: Option<String>,

    /// Human-readable error message
    pub message: String,
}

impl fmt::Display for PyRobocodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ctx) = &self.context {
            write!(f, "{}: {}", ctx, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl From<CodecError> for PyRobocodecError {
    fn from(err: CodecError) -> Self {
        let (kind, context, message) = match &err {
            CodecError::ParseError { context, message } => (
                "ParseError".to_string(),
                Some(context.clone()),
                message.clone(),
            ),
            CodecError::InvalidSchema {
                schema_name,
                reason,
            } => (
                "InvalidSchema".to_string(),
                Some(schema_name.clone()),
                reason.clone(),
            ),
            CodecError::TypeNotFound { type_name } => (
                "TypeNotFound".to_string(),
                Some(type_name.clone()),
                format!("Type '{}' not found", type_name),
            ),
            CodecError::BufferTooShort {
                requested,
                available,
                cursor_pos,
            } => (
                "BufferTooShort".to_string(),
                Some(format!("cursor={}", cursor_pos)),
                format!(
                    "Requested {} bytes but only {} bytes available",
                    requested, available
                ),
            ),
            CodecError::AlignmentError { expected, actual } => (
                "AlignmentError".to_string(),
                None,
                format!(
                    "Expected alignment of {}, but position is {}",
                    expected, actual
                ),
            ),
            CodecError::LengthExceeded {
                length,
                position,
                buffer_len,
            } => (
                "LengthExceeded".to_string(),
                Some(format!("position={}", position)),
                format!(
                    "Length {} exceeds buffer at position {} (buffer length: {})",
                    length, position, buffer_len
                ),
            ),
            CodecError::FieldDecodeError {
                field_name,
                field_type,
                cursor_pos,
                cause,
            } => (
                "FieldDecodeError".to_string(),
                Some(format!("{} @ {}", field_name, cursor_pos)),
                format!(
                    "Failed to decode field '{}' (type: '{}', cursor_pos: {}): {}",
                    field_name, field_type, cursor_pos, cause
                ),
            ),
            CodecError::Unsupported { feature } => (
                "Unsupported".to_string(),
                Some(feature.clone()),
                format!("Unsupported feature: '{}'", feature),
            ),
            CodecError::EncodeError { codec, message } => (
                "EncodeError".to_string(),
                Some(codec.clone()),
                message.clone(),
            ),
            CodecError::InvariantViolation { invariant } => (
                "InvariantViolation".to_string(),
                None,
                format!("Invariant violation: {}", invariant),
            ),
            CodecError::Other(msg) => ("Error".to_string(), None, msg.clone()),
        };

        PyRobocodecError {
            kind,
            context,
            message,
        }
    }
}

/// Convert a CodecError directly to a PyErr.
impl From<CodecError> for PyErr {
    fn from(err: CodecError) -> Self {
        let py_err = PyRobocodecError::from(err);
        PyErr::new::<RobocodecError, _>(py_err.to_string())
    }
}

/// Convert a Rust `Result` to a Python `PyResult`.
pub fn to_py_result<T>(result: crate::Result<T>) -> PyResult<T> {
    result.map_err(PyErr::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversion_parse_error() {
        let err = CodecError::parse("TestContext", "test error message");
        let py_err = PyRobocodecError::from(err);

        assert_eq!(py_err.kind, "ParseError");
        assert_eq!(py_err.context, Some("TestContext".to_string()));
        assert_eq!(py_err.message, "test error message");
    }

    #[test]
    fn test_error_conversion_invalid_schema() {
        let err = CodecError::invalid_schema("MySchema", "invalid field");
        let py_err = PyRobocodecError::from(err);

        assert_eq!(py_err.kind, "InvalidSchema");
        assert_eq!(py_err.context, Some("MySchema".to_string()));
        assert_eq!(py_err.message, "invalid field");
    }

    #[test]
    fn test_error_conversion_type_not_found() {
        let err = CodecError::type_not_found("UnknownType");
        let py_err = PyRobocodecError::from(err);

        assert_eq!(py_err.kind, "TypeNotFound");
        assert_eq!(py_err.context, Some("UnknownType".to_string()));
    }

    #[test]
    fn test_error_str_with_context() {
        let err = CodecError::parse("Context", "message");
        let py_err = PyRobocodecError::from(err);

        assert_eq!(py_err.to_string(), "Context: message");
    }

    #[test]
    fn test_error_str_without_context() {
        let err = CodecError::Other("something went wrong".to_string());
        let py_err = PyRobocodecError::from(err);

        assert_eq!(py_err.to_string(), "something went wrong");
    }
}
