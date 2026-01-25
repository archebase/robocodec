// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Type conversion between Rust and Python.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::IntoPyObjectExt;

use crate::core::{CodecValue, DecodedMessage};

/// Macro to reduce repetition in numeric value conversion.
/// Converts a numeric type to Python int/float.
macro_rules! convert_numeric {
    ($py:expr, $value:expr) => {
        Ok($value.into_py_any($py)?.into_bound($py))
    };
}

/// Convert a `CodecValue` to a native Python type.
///
/// # Mapping
///
/// - `Bool` → `bool`
/// - `Int8/16/32/64`, `UInt8/16/32/64` → `int`
/// - `Float32/64` → `float`
/// - `String` → `str`
/// - `Bytes` → `bytes`
/// - `Timestamp` → `int` (nanoseconds since Unix epoch)
/// - `Duration` → `int` (nanoseconds, may be negative)
/// - `Array` → `list`
/// - `Struct` → `dict`
/// - `Null` → `None`
pub fn codec_value_to_py<'py>(py: Python<'py>, value: &CodecValue) -> PyResult<Bound<'py, PyAny>> {
    match value {
        // Boolean
        CodecValue::Bool(v) => convert_numeric!(py, *v),

        // Signed integers
        CodecValue::Int8(v) => convert_numeric!(py, *v),
        CodecValue::Int16(v) => convert_numeric!(py, *v),
        CodecValue::Int32(v) => convert_numeric!(py, *v),
        CodecValue::Int64(v) => convert_numeric!(py, *v),

        // Unsigned integers
        CodecValue::UInt8(v) => convert_numeric!(py, *v),
        CodecValue::UInt16(v) => convert_numeric!(py, *v),
        CodecValue::UInt32(v) => convert_numeric!(py, *v),
        CodecValue::UInt64(v) => convert_numeric!(py, *v),

        // Floating point
        CodecValue::Float32(v) => convert_numeric!(py, *v as f64),
        CodecValue::Float64(v) => convert_numeric!(py, *v),

        // String
        CodecValue::String(v) => convert_numeric!(py, v.clone()),

        // Bytes
        CodecValue::Bytes(v) => Ok(PyBytes::new(py, v.as_slice()).into_any()),

        // Temporal types (as nanoseconds)
        CodecValue::Timestamp(v) => convert_numeric!(py, *v),
        CodecValue::Duration(v) => convert_numeric!(py, *v),

        // Array (recursive conversion)
        CodecValue::Array(items) => {
            let py_list = PyList::empty(py);
            for item in items {
                py_list.append(codec_value_to_py(py, item)?)?;
            }
            Ok(py_list.into_any())
        }

        // Struct (convert to dict)
        CodecValue::Struct(fields) => decoded_message_to_py(py, fields).map(|x| x.into_any()),

        // Null
        CodecValue::Null => Ok(py.None().into_bound(py).into_any()),
    }
}

/// Convert a `DecodedMessage` (HashMap) to a Python dict.
///
/// This recursively converts all `CodecValue` instances in the message
/// to native Python types.
pub fn decoded_message_to_py<'py>(
    py: Python<'py>,
    msg: &DecodedMessage,
) -> PyResult<Bound<'py, PyDict>> {
    let py_dict = PyDict::new(py);
    for (key, value) in msg {
        py_dict.set_item(key, codec_value_to_py(py, value)?)?;
    }
    Ok(py_dict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::CodecValue;
    use pyo3::types::PyList;
    use std::collections::HashMap;

    #[test]
    fn test_convert_bool() {
        Python::with_gil(|py| {
            let value = CodecValue::Bool(true);
            let result = codec_value_to_py(py, &value).unwrap();
            assert!(result.extract::<bool>().unwrap());
        });
    }

    #[test]
    fn test_convert_integers() {
        Python::with_gil(|py| {
            let tests = vec![
                CodecValue::Int8(-42),
                CodecValue::Int16(-4200),
                CodecValue::Int32(-420000),
                CodecValue::Int64(-4200000000),
                CodecValue::UInt8(42),
                CodecValue::UInt16(4200),
                CodecValue::UInt32(420000),
                CodecValue::UInt64(4200000000),
            ];

            for value in tests {
                let result = codec_value_to_py(py, &value).unwrap();
                assert!(result.extract::<i64>().is_ok());
            }
        });
    }

    #[test]
    fn test_convert_floats() {
        Python::with_gil(|py| {
            let result = codec_value_to_py(py, &CodecValue::Float32(1.5)).unwrap();
            assert!((result.extract::<f64>().unwrap() - 1.5).abs() < 0.001);

            let result = codec_value_to_py(py, &CodecValue::Float64(2.5)).unwrap();
            assert_eq!(result.extract::<f64>().unwrap(), 2.5);
        });
    }

    #[test]
    fn test_convert_string() {
        Python::with_gil(|py| {
            let value = CodecValue::String("hello".to_string());
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<String>().unwrap(), "hello");
        });
    }

    #[test]
    fn test_convert_bytes() {
        Python::with_gil(|py| {
            let data = vec![1u8, 2, 3, 4];
            let value = CodecValue::Bytes(data.clone());
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<Vec<u8>>().unwrap(), data);
        });
    }

    #[test]
    fn test_convert_null() {
        Python::with_gil(|py| {
            let value = CodecValue::Null;
            let result = codec_value_to_py(py, &value).unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn test_convert_array() {
        Python::with_gil(|py| {
            let value = CodecValue::Array(vec![
                CodecValue::Int32(1),
                CodecValue::Int32(2),
                CodecValue::String("test".to_string()),
            ]);
            let result = codec_value_to_py(py, &value).unwrap();
            assert!(result.downcast::<PyList>().is_ok());

            let list: Vec<PyObject> = result.extract().unwrap();
            assert_eq!(list.len(), 3);
        });
    }

    #[test]
    fn test_convert_struct() {
        Python::with_gil(|py| {
            let mut fields = HashMap::new();
            fields.insert("name".to_string(), CodecValue::String("test".to_string()));
            fields.insert("value".to_string(), CodecValue::Int32(42));

            let value = CodecValue::Struct(fields);
            let result = codec_value_to_py(py, &value).unwrap();
            assert!(result.downcast::<PyDict>().is_ok());

            let dict: HashMap<String, PyObject> = result.extract().unwrap();
            assert_eq!(dict.len(), 2);
            assert!(dict.contains_key("name"));
            assert!(dict.contains_key("value"));
        });
    }

    #[test]
    fn test_convert_decoded_message() {
        Python::with_gil(|py| {
            let mut msg = HashMap::new();
            msg.insert("field1".to_string(), CodecValue::Int32(123));
            msg.insert("field2".to_string(), CodecValue::String("test".to_string()));

            let result = decoded_message_to_py(py, &msg).unwrap();
            assert_eq!(result.len(), 2);

            // Test accessing dict items via iteration
            for (key, val) in result.iter() {
                let key_str: String = key.extract().unwrap();
                if key_str == "field1" {
                    assert_eq!(val.extract::<i32>().unwrap(), 123);
                } else if key_str == "field2" {
                    assert_eq!(val.extract::<String>().unwrap(), "test");
                }
            }
        });
    }

    // ========================================================================
    // Timestamp and Duration Tests
    // ========================================================================

    #[test]
    fn test_convert_timestamp() {
        Python::with_gil(|py| {
            let value = CodecValue::Timestamp(1234567890);
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<u64>().unwrap(), 1234567890);
        });
    }

    #[test]
    fn test_convert_timestamp_zero() {
        Python::with_gil(|py| {
            let value = CodecValue::Timestamp(0);
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<u64>().unwrap(), 0);
        });
    }

    #[test]
    fn test_convert_duration_positive() {
        Python::with_gil(|py| {
            let value = CodecValue::Duration(1000000); // 1ms in nanoseconds
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<i64>().unwrap(), 1000000);
        });
    }

    #[test]
    fn test_convert_duration_negative() {
        Python::with_gil(|py| {
            let value = CodecValue::Duration(-1000000); // -1ms in nanoseconds
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<i64>().unwrap(), -1000000);
        });
    }

    #[test]
    fn test_convert_duration_zero() {
        Python::with_gil(|py| {
            let value = CodecValue::Duration(0);
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<i64>().unwrap(), 0);
        });
    }

    // ========================================================================
    // Empty Collection Tests
    // ========================================================================

    #[test]
    fn test_convert_empty_array() {
        Python::with_gil(|py| {
            let value = CodecValue::Array(vec![]);
            let result = codec_value_to_py(py, &value).unwrap();
            let list = result.downcast::<PyList>().unwrap();
            assert_eq!(list.len(), 0);
        });
    }

    #[test]
    fn test_convert_empty_struct() {
        Python::with_gil(|py| {
            let fields = HashMap::new();
            let value = CodecValue::Struct(fields);
            let result = codec_value_to_py(py, &value).unwrap();
            let dict = result.downcast::<PyDict>().unwrap();
            assert_eq!(dict.len(), 0);
        });
    }

    #[test]
    fn test_convert_empty_decoded_message() {
        Python::with_gil(|py| {
            let msg = HashMap::new();
            let result = decoded_message_to_py(py, &msg).unwrap();
            assert_eq!(result.len(), 0);
        });
    }

    #[test]
    fn test_convert_empty_string() {
        Python::with_gil(|py| {
            let value = CodecValue::String("".to_string());
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<String>().unwrap(), "");
        });
    }

    #[test]
    fn test_convert_empty_bytes() {
        Python::with_gil(|py| {
            let data: Vec<u8> = vec![];
            let value = CodecValue::Bytes(data);
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<Vec<u8>>().unwrap(), Vec::<u8>::new());
        });
    }

    // ========================================================================
    // Nested Structure Tests
    // ========================================================================

    #[test]
    fn test_convert_nested_array() {
        Python::with_gil(|py| {
            let value = CodecValue::Array(vec![
                CodecValue::Array(vec![CodecValue::Int32(1), CodecValue::Int32(2)]),
                CodecValue::Array(vec![CodecValue::Int32(3), CodecValue::Int32(4)]),
            ]);
            let result = codec_value_to_py(py, &value).unwrap();
            let list = result.downcast::<PyList>().unwrap();
            assert_eq!(list.len(), 2);
        });
    }

    #[test]
    fn test_convert_nested_struct() {
        Python::with_gil(|py| {
            let mut inner_fields = HashMap::new();
            inner_fields.insert("x".to_string(), CodecValue::Float64(1.0));
            inner_fields.insert("y".to_string(), CodecValue::Float64(2.0));

            let mut outer_fields = HashMap::new();
            outer_fields.insert("point".to_string(), CodecValue::Struct(inner_fields));
            outer_fields.insert("label".to_string(), CodecValue::String("test".to_string()));

            let value = CodecValue::Struct(outer_fields);
            let result = codec_value_to_py(py, &value).unwrap();
            let dict = result.downcast::<PyDict>().unwrap();
            assert_eq!(dict.len(), 2);
        });
    }

    #[test]
    fn test_convert_array_of_structs() {
        Python::with_gil(|py| {
            let mut fields1 = HashMap::new();
            fields1.insert("id".to_string(), CodecValue::Int32(1));

            let mut fields2 = HashMap::new();
            fields2.insert("id".to_string(), CodecValue::Int32(2));

            let value = CodecValue::Array(vec![
                CodecValue::Struct(fields1),
                CodecValue::Struct(fields2),
            ]);
            let result = codec_value_to_py(py, &value).unwrap();
            let list = result.downcast::<PyList>().unwrap();
            assert_eq!(list.len(), 2);
        });
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_convert_bool_false() {
        Python::with_gil(|py| {
            let value = CodecValue::Bool(false);
            let result = codec_value_to_py(py, &value).unwrap();
            assert!(!result.extract::<bool>().unwrap());
        });
    }

    #[test]
    fn test_convert_integer_boundary_values() {
        Python::with_gil(|py| {
            // Test i8 boundaries
            let result = codec_value_to_py(py, &CodecValue::Int8(i8::MIN)).unwrap();
            assert_eq!(result.extract::<i64>().unwrap(), i8::MIN as i64);

            let result = codec_value_to_py(py, &CodecValue::Int8(i8::MAX)).unwrap();
            assert_eq!(result.extract::<i64>().unwrap(), i8::MAX as i64);

            // Test i64 boundaries
            let result = codec_value_to_py(py, &CodecValue::Int64(i64::MAX)).unwrap();
            assert!(result.extract::<i64>().is_ok());
        });
    }

    #[test]
    fn test_convert_unsigned_integer_max() {
        Python::with_gil(|py| {
            let result = codec_value_to_py(py, &CodecValue::UInt64(u64::MAX)).unwrap();
            assert!(result.extract::<u64>().is_ok());
        });
    }

    #[test]
    fn test_convert_float_special_values() {
        Python::with_gil(|py| {
            // Test infinity
            let result = codec_value_to_py(py, &CodecValue::Float64(f64::INFINITY)).unwrap();
            assert!(result.extract::<f64>().unwrap().is_infinite());

            // Test negative infinity
            let result = codec_value_to_py(py, &CodecValue::Float64(f64::NEG_INFINITY)).unwrap();
            assert!(result.extract::<f64>().unwrap().is_infinite());

            // Test NaN
            let result = codec_value_to_py(py, &CodecValue::Float64(f64::NAN)).unwrap();
            assert!(result.extract::<f64>().unwrap().is_nan());
        });
    }

    #[test]
    fn test_convert_string_with_unicode() {
        Python::with_gil(|py| {
            let value = CodecValue::String("Hello 世界 🌍".to_string());
            let result = codec_value_to_py(py, &value).unwrap();
            assert_eq!(result.extract::<String>().unwrap(), "Hello 世界 🌍");
        });
    }

    #[test]
    fn test_convert_string_with_special_chars() {
        Python::with_gil(|py| {
            let value = CodecValue::String("line1\nline2\ttab\r\0null".to_string());
            let result = codec_value_to_py(py, &value).unwrap();
            assert!(result.extract::<String>().is_ok());
        });
    }

    // ========================================================================
    // DecodedMessage Edge Cases
    // ========================================================================

    #[test]
    fn test_convert_decoded_message_with_null_value() {
        Python::with_gil(|py| {
            let mut msg = HashMap::new();
            msg.insert("field1".to_string(), CodecValue::Int32(123));
            msg.insert("field2".to_string(), CodecValue::Null);

            let result = decoded_message_to_py(py, &msg).unwrap();
            assert_eq!(result.len(), 2);

            // Check that null field is None
            for (key, val) in result.iter() {
                let key_str: String = key.extract().unwrap();
                if key_str == "field2" {
                    assert!(val.is_none());
                }
            }
        });
    }

    #[test]
    fn test_convert_decoded_message_with_array() {
        Python::with_gil(|py| {
            let mut msg = HashMap::new();
            msg.insert(
                "items".to_string(),
                CodecValue::Array(vec![CodecValue::Int32(1), CodecValue::Int32(2)]),
            );

            let result = decoded_message_to_py(py, &msg).unwrap();
            assert_eq!(result.len(), 1);

            for (_key, val) in result.iter() {
                let list = val.downcast::<PyList>();
                assert!(list.is_ok());
                if let Ok(lst) = list {
                    assert_eq!(lst.len(), 2);
                }
            }
        });
    }

    #[test]
    fn test_convert_decoded_message_with_nested_message() {
        Python::with_gil(|py| {
            let mut inner = HashMap::new();
            inner.insert("x".to_string(), CodecValue::Float64(1.0));

            let mut outer = HashMap::new();
            outer.insert("inner".to_string(), CodecValue::Struct(inner));

            let result = decoded_message_to_py(py, &outer).unwrap();
            assert_eq!(result.len(), 1);
        });
    }

    // ========================================================================
    // Type Trait Tests
    // ========================================================================

    #[test]
    fn test_codec_value_display() {
        // Test that CodecValue has a reasonable Debug representation
        let value = CodecValue::Int32(42);
        let _ = format!("{:?}", value);
    }

    #[test]
    fn test_python_any_conversion() {
        Python::with_gil(|py| {
            // Test various conversions to PyAny
            let tests: Vec<CodecValue> = vec![
                CodecValue::Bool(true),
                CodecValue::Int32(42),
                CodecValue::Float64(2.5),
                CodecValue::String("test".to_string()),
                CodecValue::Null,
            ];

            for value in tests {
                let result = codec_value_to_py(py, &value);
                assert!(result.is_ok(), "conversion should succeed for {:?}", value);
            }
        });
    }
}
