// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Python bindings for RoboReader.

use pyo3::prelude::*;

use crate::io::metadata::FileFormat;
use crate::io::traits::FormatReader;
use crate::io::RoboReader as IoRoboReader;

use super::error::to_py_result;
use super::metadata::PyChannelInfo;

/// Unified robotics data reader with auto-detection.
///
/// `RoboReader` automatically detects the file format (MCAP or ROS1 bag)
/// from the file extension and provides a consistent API for reading.
///
/// # Example
///
/// ```python
/// import robocodec
///
/// reader = robocodec.RoboReader("data.mcap")
/// print(f"Messages: {reader.message_count}")
///
/// for channel in reader.channels():
///     print(f"  {channel.topic}: {channel.message_type}")
/// ```
#[pyclass(name = "RoboReader")]
pub struct PyRoboReader {
    inner: IoRoboReader,
}

#[pymethods]
impl PyRoboReader {
    /// Open a robotics data file (auto-detects format).
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Path to the file to open (.mcap or .bag)
    ///
    /// Raises
    /// ------
    /// RobocodecError
    ///     If the file cannot be opened or format is not recognized
    ///
    /// # Example
    ///
    /// ```python
    /// reader = robocodec.RoboReader("data.mcap")
    /// ```
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let inner = to_py_result(IoRoboReader::open(path))?;
        Ok(Self { inner })
    }

    /// Get all channels in the file.
    ///
    /// Returns
    /// -------
    /// list[ChannelInfo]
    ///     List of channel information objects
    ///
    /// # Example
    ///
    /// ```python
    /// for channel in reader.channels():
    ///     print(f"{channel.topic}: {channel.message_type}")
    /// ```
    fn channels(&self) -> PyResult<Vec<PyChannelInfo>> {
        Ok(self
            .inner
            .channels()
            .values()
            .map(PyChannelInfo::from)
            .collect())
    }

    /// Get a channel by topic name.
    ///
    /// Parameters
    /// ----------
    /// topic : str
    ///     Topic name to search for
    ///
    /// Returns
    /// -------
    /// ChannelInfo or None
    ///     Channel information if found, None otherwise
    ///
    /// # Example
    ///
    /// ```python
    /// channel = reader.channel_by_topic("/chatter")
    /// if channel:
    ///     print(f"Found: {channel.message_type}")
    /// ```
    fn channel_by_topic(&self, topic: &str) -> PyResult<Option<PyChannelInfo>> {
        Ok(self.inner.channel_by_topic(topic).map(PyChannelInfo::from))
    }

    /// Get all channels for a specific topic.
    ///
    /// Parameters
    /// ----------
    /// topic : str
    ///     Topic name to search for
    ///
    /// Returns
    /// -------
    /// list[ChannelInfo]
    ///     List of channels matching the topic
    ///
    /// # Example
    ///
    /// ```python
    /// channels = reader.channels_by_topic("/chatter")
    /// print(f"Found {len(channels)} channels for /chatter")
    /// ```
    fn channels_by_topic(&self, topic: &str) -> PyResult<Vec<PyChannelInfo>> {
        Ok(self
            .inner
            .channels_by_topic(topic)
            .into_iter()
            .map(PyChannelInfo::from)
            .collect())
    }

    /// Get total message count.
    ///
    /// Returns
    /// -------
    /// int
    ///     Total number of messages in the file
    #[getter]
    fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    /// Get start time (nanoseconds since Unix epoch).
    ///
    /// Returns
    /// -------
    /// int or None
    ///     Start timestamp if available, None otherwise
    #[getter]
    fn start_time(&self) -> Option<u64> {
        self.inner.start_time()
    }

    /// Get end time (nanoseconds since Unix epoch).
    ///
    /// Returns
    /// -------
    /// int or None
    ///     End timestamp if available, None otherwise
    #[getter]
    fn end_time(&self) -> Option<u64> {
        self.inner.end_time()
    }

    /// Get file path.
    ///
    /// Returns
    /// -------
    /// str
    ///     Path to the file being read
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
        match self.inner.format() {
            FileFormat::Mcap => "MCAP".to_string(),
            FileFormat::Bag => "BAG".to_string(),
            FileFormat::Unknown => "Unknown".to_string(),
        }
    }

    /// Get file size in bytes.
    ///
    /// Returns
    /// -------
    /// int
    ///     Size of the file in bytes
    #[getter]
    fn file_size(&self) -> u64 {
        self.inner.file_size()
    }

    /// String representation.
    fn __repr__(&self) -> String {
        format!(
            "RoboReader(path='{}', format={}, messages={}, channels={})",
            self.inner.path(),
            self.format(),
            self.inner.message_count(),
            self.inner.channels().len()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::metadata::ChannelInfo;

    #[test]
    fn test_py_channel_info_from_rust() {
        // Test conversion via actual use
        let rust_info = ChannelInfo::new(1, "/test", "std_msgs/String");
        let py_info = PyChannelInfo::from(&rust_info);

        assert_eq!(py_info.id, 1);
        assert_eq!(py_info.topic, "/test");
        assert_eq!(py_info.message_type, "std_msgs/String");
    }

    #[test]
    fn test_py_channel_info_with_full_details() {
        let mut rust_info = ChannelInfo::new(42, "/chatter", "std_msgs/String");
        rust_info.encoding = "cdr".to_string();
        rust_info.message_count = 100;

        let py_info = PyChannelInfo::from(&rust_info);

        assert_eq!(py_info.id, 42);
        assert_eq!(py_info.topic, "/chatter");
        assert_eq!(py_info.message_type, "std_msgs/String");
        assert_eq!(py_info.encoding, "cdr");
        assert_eq!(py_info.message_count, 100);
    }
}
