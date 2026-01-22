// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Error conversion between Rust and Python.

use pyo3::prelude::*;

use crate::CodecError;

// Create the base RobocodecError exception type.
// This uses PyO3's create_exception! macro which creates a proper
// Python exception class that inherits from Exception.
pyo3::create_exception!(_robocodec, RobocodecError, pyo3::exceptions::PyException);

/// Convert a CodecError to structured (kind, context, message) tuple.
fn codec_error_to_tuple(err: &CodecError) -> (String, Option<String>, String) {
    match err {
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
    }
}

/// Convert a CodecError directly to a PyErr.
///
/// The error data is passed as a tuple (kind, context, message) which
/// becomes available in Python via the exception's args attribute.
impl From<CodecError> for PyErr {
    fn from(err: CodecError) -> Self {
        let (kind, context, message) = codec_error_to_tuple(&err);
        PyErr::new::<RobocodecError, _>((kind, context, message))
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
        let (kind, context, message) = codec_error_to_tuple(&err);

        assert_eq!(kind, "ParseError");
        assert_eq!(context, Some("TestContext".to_string()));
        assert_eq!(message, "test error message");
    }

    #[test]
    fn test_error_conversion_invalid_schema() {
        let err = CodecError::invalid_schema("MySchema", "invalid field");
        let (kind, context, message) = codec_error_to_tuple(&err);

        assert_eq!(kind, "InvalidSchema");
        assert_eq!(context, Some("MySchema".to_string()));
        assert_eq!(message, "invalid field");
    }

    #[test]
    fn test_error_conversion_type_not_found() {
        let err = CodecError::type_not_found("UnknownType");
        let (kind, context, _message) = codec_error_to_tuple(&err);

        assert_eq!(kind, "TypeNotFound");
        assert_eq!(context, Some("UnknownType".to_string()));
    }

    #[test]
    fn test_error_to_pyerr() {
        use pyo3::Python;

        let err = CodecError::parse("Context", "message");
        let py_err: PyErr = err.into();

        // Verify it creates a PyErr without panicking
        Python::with_gil(|py| {
            assert!(py_err.is_instance_of::<RobocodecError>(py));
        });
    }
}
