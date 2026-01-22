// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Metadata type wrappers for Python.

use pyo3::prelude::*;

use crate::io::metadata::ChannelInfo;
use crate::rewriter::RewriteStats;

/// Channel information exposed to Python.
///
/// Represents a channel (topic) in a robotics data file with its
/// associated metadata.
#[pyclass(name = "ChannelInfo")]
#[derive(Clone)]
pub struct PyChannelInfo {
    /// Unique channel ID within the file
    #[pyo3(get)]
    pub id: u16,

    /// Topic name (e.g., "/joint_states", "/tf")
    #[pyo3(get)]
    pub topic: String,

    /// Message type name (e.g., "sensor_msgs/msg/JointState")
    #[pyo3(get)]
    pub message_type: String,

    /// Encoding format (e.g., "cdr", "protobuf", "json")
    #[pyo3(get)]
    pub encoding: String,

    /// Schema definition (message definition text)
    #[pyo3(get)]
    pub schema: Option<String>,

    /// Schema encoding (e.g., "ros2msg", "protobuf")
    #[pyo3(get)]
    pub schema_encoding: Option<String>,

    /// Number of messages in this channel
    #[pyo3(get)]
    pub message_count: u64,

    /// Caller ID (ROS1 specific, identifies the publishing node)
    #[pyo3(get)]
    pub callerid: Option<String>,
}

impl From<&ChannelInfo> for PyChannelInfo {
    fn from(info: &ChannelInfo) -> Self {
        Self {
            id: info.id,
            topic: info.topic.clone(),
            message_type: info.message_type.clone(),
            encoding: info.encoding.clone(),
            schema: info.schema.clone(),
            schema_encoding: info.schema_encoding.clone(),
            message_count: info.message_count,
            callerid: info.callerid.clone(),
        }
    }
}

#[pymethods]
impl PyChannelInfo {
    fn __repr__(&self) -> String {
        format!(
            "ChannelInfo(id={}, topic='{}', type='{}', encoding='{}', messages={})",
            self.id, self.topic, self.message_type, self.encoding, self.message_count
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Statistics from a rewrite operation.
///
/// Provides detailed statistics about a rewrite operation, including
/// message counts, failures, and transformation results.
#[pyclass(name = "RewriteStats")]
#[derive(Clone)]
pub struct PyRewriteStats {
    /// Total messages processed
    #[pyo3(get)]
    pub message_count: u64,

    /// Total channels processed
    #[pyo3(get)]
    pub channel_count: u64,

    /// Messages that failed to decode
    #[pyo3(get)]
    pub decode_failures: u64,

    /// Messages that failed to encode
    #[pyo3(get)]
    pub encode_failures: u64,

    /// Messages that were successfully re-encoded
    #[pyo3(get)]
    pub reencoded_count: u64,

    /// Messages passed through without re-encoding
    #[pyo3(get)]
    pub passthrough_count: u64,

    /// Number of topics renamed (if transforms were applied)
    #[pyo3(get)]
    pub topics_renamed: u64,

    /// Number of types renamed (if transforms were applied)
    #[pyo3(get)]
    pub types_renamed: u64,
}

impl From<&RewriteStats> for PyRewriteStats {
    fn from(stats: &RewriteStats) -> Self {
        Self {
            message_count: stats.message_count,
            channel_count: stats.channel_count,
            decode_failures: stats.decode_failures,
            encode_failures: stats.encode_failures,
            reencoded_count: stats.reencoded_count,
            passthrough_count: stats.passthrough_count,
            topics_renamed: stats.topics_renamed,
            types_renamed: stats.types_renamed,
        }
    }
}

#[pymethods]
impl PyRewriteStats {
    fn __repr__(&self) -> String {
        format!(
            "RewriteStats(messages={}, channels={}, decode_failures={}, encode_failures={}, reencoded={}, passthrough={}, topics_renamed={}, types_renamed={})",
            self.message_count,
            self.channel_count,
            self.decode_failures,
            self.encode_failures,
            self.reencoded_count,
            self.passthrough_count,
            self.topics_renamed,
            self.types_renamed
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
    fn test_channel_info_from_rust() {
        let rust_info = ChannelInfo::new(1, "/test/topic", "std_msgs/String")
            .with_encoding("cdr")
            .with_message_count(100)
            .with_callerid("/node");

        let py_info = PyChannelInfo::from(&rust_info);

        assert_eq!(py_info.id, 1);
        assert_eq!(py_info.topic, "/test/topic");
        assert_eq!(py_info.message_type, "std_msgs/String");
        assert_eq!(py_info.encoding, "cdr");
        assert_eq!(py_info.message_count, 100);
        assert_eq!(py_info.callerid, Some("/node".to_string()));
    }

    #[test]
    fn test_channel_info_repr() {
        let info = PyChannelInfo {
            id: 1,
            topic: "/chatter".to_string(),
            message_type: "std_msgs/String".to_string(),
            encoding: "cdr".to_string(),
            schema: None,
            schema_encoding: None,
            message_count: 100,
            callerid: None,
        };

        let repr = info.__repr__();
        assert!(repr.contains("id=1"));
        assert!(repr.contains("topic='/chatter'"));
        assert!(repr.contains("messages=100"));
    }

    #[test]
    fn test_rewrite_stats_from_rust() {
        let rust_stats = RewriteStats {
            message_count: 1000,
            channel_count: 10,
            decode_failures: 5,
            encode_failures: 2,
            reencoded_count: 800,
            passthrough_count: 195,
            topics_renamed: 3,
            types_renamed: 1,
        };

        let py_stats = PyRewriteStats::from(&rust_stats);

        assert_eq!(py_stats.message_count, 1000);
        assert_eq!(py_stats.channel_count, 10);
        assert_eq!(py_stats.decode_failures, 5);
        assert_eq!(py_stats.encode_failures, 2);
    }
}
