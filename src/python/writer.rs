// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for RoboWriter.

use pyo3::prelude::*;

use crate::io::metadata::FileFormat;
use crate::io::traits::FormatWriter;
use crate::io::RoboWriter as IoRoboWriter;

use super::error::to_py_result;

/// Unified robotics data writer with auto-detection.
///
/// `RoboWriter` automatically detects the output format from the file
/// extension (.mcap or .bag) and provides a consistent API for writing.
///
/// # Example
///
/// ```python
/// import robocodec
///
/// writer = robocodec.RoboWriter("output.mcap")
/// channel_id = writer.add_channel("/chatter", "std_msgs/String", "cdr", None)
/// writer.finish()
/// ```
#[pyclass(name = "RoboWriter", unsendable)]
pub struct PyRoboWriter {
    inner: IoRoboWriter,
    format: FileFormat,
}

#[pymethods]
impl PyRoboWriter {
    /// Create a new robotics data file (format from extension).
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Path to the output file (.mcap or .bag)
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If the file cannot be created or format is not recognized
    ///
    /// # Example
    ///
    /// ```python
    /// writer = robocodec.RoboWriter("output.mcap")
    /// ```
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        // Detect format from file extension
        let format = if path.ends_with(".mcap") {
            FileFormat::Mcap
        } else if path.ends_with(".bag") {
            FileFormat::Bag
        } else {
            FileFormat::Unknown
        };
        let inner = to_py_result(IoRoboWriter::create(path))?;
        Ok(Self { inner, format })
    }

    /// Add a channel to the file.
    ///
    /// Parameters
    /// ----------
    /// topic : str
    ///     Topic name (e.g., "/chatter")
    /// message_type : str
    ///     Message type name (e.g., "std_msgs/String")
    /// encoding : str
    ///     Encoding format (e.g., "cdr", "protobuf", "json")
    /// schema : str or None
    ///     Optional schema definition
    ///
    /// Returns
    /// -------
    /// int
    ///     The assigned channel ID
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If the channel cannot be added
    ///
    /// # Example
    ///
    /// ```python
    /// channel_id = writer.add_channel(
    ///     "/chatter",
    ///     "std_msgs/String",
    ///     "cdr",
    ///     "string data"
    /// )
    /// ```
    fn add_channel(
        &mut self,
        topic: &str,
        message_type: &str,
        encoding: &str,
        schema: Option<&str>,
    ) -> PyResult<u16> {
        to_py_result(
            self.inner
                .add_channel(topic, message_type, encoding, schema),
        )
    }

    /// Finish writing and close the file.
    ///
    /// This must be called to ensure all data is flushed to disk.
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If finishing the write fails
    ///
    /// # Example
    ///
    /// ```python
    /// writer.finish()
    /// ```
    fn finish(&mut self) -> PyResult<()> {
        to_py_result(self.inner.finish())
    }

    /// Get message count.
    ///
    /// Returns
    /// -------
    /// int
    ///     Number of messages written so far
    #[getter]
    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    /// Get channel count.
    ///
    /// Returns
    /// -------
    /// int
    ///     Number of channels added
    #[getter]
    fn channel_count(&self) -> usize {
        self.inner.channel_count()
    }

    /// Get file path.
    ///
    /// Returns
    /// -------
    /// str
    ///     Path to the output file
    #[getter]
    fn path(&self) -> String {
        self.inner.path().to_string()
    }

    /// Get detected file format.
    ///
    /// Returns
    /// -------
    /// str
    ///     File format: "MCAP", "BAG", or "Unknown"
    #[getter]
    fn format(&self) -> String {
        match self.format {
            FileFormat::Mcap => "MCAP".to_string(),
            FileFormat::Bag => "BAG".to_string(),
            FileFormat::Unknown => "Unknown".to_string(),
        }
    }

    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "RoboWriter(path='{}', format={}, messages={}, channels={})",
            self.inner.path(),
            self.format(),
            self.inner.message_count(),
            self.inner.channel_count()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to detect format from file extension.
    /// This mirrors the logic used in PyRoboWriter::new.
    fn detect_format(path: &str) -> FileFormat {
        if path.ends_with(".mcap") {
            FileFormat::Mcap
        } else if path.ends_with(".bag") {
            FileFormat::Bag
        } else {
            FileFormat::Unknown
        }
    }

    /// Helper function to convert FileFormat to display string.
    /// This mirrors the logic used in PyRoboWriter::format.
    fn format_to_string(format: FileFormat) -> &'static str {
        match format {
            FileFormat::Mcap => "MCAP",
            FileFormat::Bag => "BAG",
            FileFormat::Unknown => "Unknown",
        }
    }

    #[test]
    fn test_format_detection_mcap() {
        assert_eq!(detect_format("test.mcap"), FileFormat::Mcap);
        assert_eq!(detect_format("/path/to/file.mcap"), FileFormat::Mcap);
        assert_eq!(detect_format("MCAP.mcap"), FileFormat::Mcap);
    }

    #[test]
    fn test_format_detection_bag() {
        assert_eq!(detect_format("test.bag"), FileFormat::Bag);
        assert_eq!(detect_format("/path/to/file.bag"), FileFormat::Bag);
        assert_eq!(detect_format("data.bag"), FileFormat::Bag);
    }

    #[test]
    fn test_format_detection_unknown() {
        assert_eq!(detect_format("test.txt"), FileFormat::Unknown);
        assert_eq!(detect_format("data.json"), FileFormat::Unknown);
        assert_eq!(detect_format("no_extension"), FileFormat::Unknown);
    }

    #[test]
    fn test_format_to_string_conversion() {
        assert_eq!(format_to_string(FileFormat::Mcap), "MCAP");
        assert_eq!(format_to_string(FileFormat::Bag), "BAG");
        assert_eq!(format_to_string(FileFormat::Unknown), "Unknown");
    }

    #[test]
    fn test_format_detection_roundtrip() {
        let test_cases = [
            ("file.mcap", FileFormat::Mcap, "MCAP"),
            ("file.bag", FileFormat::Bag, "BAG"),
            ("file.txt", FileFormat::Unknown, "Unknown"),
        ];

        for (path, expected_format, expected_str) in test_cases {
            let format = detect_format(path);
            assert_eq!(format, expected_format);
            assert_eq!(format_to_string(format), expected_str);
        }
    }

    #[test]
    fn test_format_detection_edge_cases() {
        // Case sensitivity - extensions are case-sensitive
        assert_eq!(detect_format("file.MCAP"), FileFormat::Unknown);
        assert_eq!(detect_format("file.BAG"), FileFormat::Unknown);

        // Multiple dots in filename
        assert_eq!(detect_format("file.name.mcap"), FileFormat::Mcap);
        assert_eq!(detect_format("file.name.bag"), FileFormat::Bag);

        // Empty string
        assert_eq!(detect_format(""), FileFormat::Unknown);
    }
}
