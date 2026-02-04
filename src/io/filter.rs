// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Topic and connection filtering for parallel readers.
//!
//! This module provides filtering capabilities for parallel readers,
//! allowing efficient selection of specific topics/channels during
//! concurrent chunk processing.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use crate::io::metadata::ChannelInfo;

/// Filter for selecting topics/connections during parallel reading.
#[derive(Clone, Default)]
pub enum TopicFilter {
    /// Read all topics (no filtering)
    #[default]
    All,
    /// Read only specific topics
    Include(Vec<String>),
    /// Exclude specific topics
    Exclude(Vec<String>),
    /// Include topics matching regex pattern
    RegexInclude(Arc<regex::Regex>),
    /// Exclude topics matching regex pattern
    RegexExclude(Arc<regex::Regex>),
    /// Custom filter function
    Custom(Arc<dyn Fn(&str) -> bool + Send + Sync>),
}

impl fmt::Debug for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => f.debug_tuple("All").finish(),
            Self::Include(v) => f.debug_tuple("Include").field(v).finish(),
            Self::Exclude(v) => f.debug_tuple("Exclude").field(v).finish(),
            Self::RegexInclude(_) => f.debug_tuple("RegexInclude").field(&"<regex>").finish(),
            Self::RegexExclude(_) => f.debug_tuple("RegexExclude").field(&"<regex>").finish(),
            Self::Custom(_) => f.debug_tuple("Custom").field(&"<fn>").finish(),
        }
    }
}

impl TopicFilter {
    /// Check if a topic should be included.
    pub fn should_include(&self, topic: &str) -> bool {
        match self {
            TopicFilter::All => true,
            TopicFilter::Include(topics) => topics.contains(&topic.to_string()),
            TopicFilter::Exclude(topics) => !topics.contains(&topic.to_string()),
            TopicFilter::RegexInclude(re) => re.is_match(topic),
            TopicFilter::RegexExclude(re) => !re.is_match(topic),
            TopicFilter::Custom(f) => f(topic),
        }
    }

    /// Create an include filter from topic names.
    pub fn include(topics: Vec<String>) -> Self {
        Self::Include(topics)
    }

    /// Create an exclude filter from topic names.
    pub fn exclude(topics: Vec<String>) -> Self {
        Self::Exclude(topics)
    }

    /// Create a regex include filter.
    pub fn regex_include(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(|re| Self::RegexInclude(Arc::new(re)))
    }

    /// Create a regex exclude filter.
    pub fn regex_exclude(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(|re| Self::RegexExclude(Arc::new(re)))
    }

    /// Create a custom filter from a function.
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self::Custom(Arc::new(f))
    }
}

/// Channel filter mapping topic names to channel IDs.
#[derive(Debug, Clone)]
pub struct ChannelFilter {
    /// Allowed channel IDs
    pub allowed_channels: HashSet<u16>,
    /// Topic to channel ID mapping
    pub topic_to_channels: HashMap<String, Vec<u16>>,
}

impl ChannelFilter {
    /// Create a channel filter from topic filter and channel info.
    pub fn from_topic_filter(filter: &TopicFilter, channels: &HashMap<u16, ChannelInfo>) -> Self {
        let mut allowed_channels = HashSet::new();
        let mut topic_to_channels: HashMap<String, Vec<u16>> = HashMap::new();

        for (&id, channel) in channels {
            if filter.should_include(&channel.topic) {
                allowed_channels.insert(id);
                topic_to_channels
                    .entry(channel.topic.clone())
                    .or_default()
                    .push(id);
            }
        }

        Self {
            allowed_channels,
            topic_to_channels,
        }
    }

    /// Create a filter that includes all channels.
    pub fn all(channels: &HashMap<u16, ChannelInfo>) -> Self {
        let mut allowed_channels = HashSet::new();
        let mut topic_to_channels: HashMap<String, Vec<u16>> = HashMap::new();

        for (&id, channel) in channels {
            allowed_channels.insert(id);
            topic_to_channels
                .entry(channel.topic.clone())
                .or_default()
                .push(id);
        }

        Self {
            allowed_channels,
            topic_to_channels,
        }
    }

    /// Check if a channel ID is allowed.
    pub fn allows_channel(&self, channel_id: u16) -> bool {
        self.allowed_channels.contains(&channel_id)
    }

    /// Get the number of allowed channels.
    pub fn channel_count(&self) -> usize {
        self.allowed_channels.len()
    }

    /// Get all channel IDs for a topic.
    pub fn channels_for_topic(&self, topic: &str) -> &[u16] {
        self.topic_to_channels
            .get(topic)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_filter_all() {
        let filter = TopicFilter::All;
        assert!(filter.should_include("/any_topic"));
        assert!(filter.should_include("/another_topic"));
    }

    #[test]
    fn test_topic_filter_include() {
        let filter = TopicFilter::include(vec!["/camera/image_raw".into(), "/lidar/points".into()]);
        assert!(filter.should_include("/camera/image_raw"));
        assert!(filter.should_include("/lidar/points"));
        assert!(!filter.should_include("/imu/data"));
    }

    #[test]
    fn test_topic_filter_exclude() {
        let filter = TopicFilter::exclude(vec!["/tf".into()]);
        assert!(!filter.should_include("/tf"));
        assert!(filter.should_include("/camera"));
    }

    #[test]
    fn test_topic_filter_regex() {
        let filter = TopicFilter::regex_include("/camera/.*").unwrap();
        assert!(filter.should_include("/camera/image_raw"));
        assert!(filter.should_include("/camera/info"));
        assert!(!filter.should_include("/lidar/points"));
    }

    #[test]
    fn test_channel_filter_from_topic_filter() {
        let mut channels = HashMap::new();
        channels.insert(0, ChannelInfo::new(0, "/camera", "sensor_msgs/Image"));
        channels.insert(1, ChannelInfo::new(1, "/lidar", "sensor_msgs/PointCloud2"));
        channels.insert(2, ChannelInfo::new(2, "/imu", "sensor_msgs/Imu"));

        let filter = TopicFilter::include(vec!["/camera".into()]);
        let channel_filter = ChannelFilter::from_topic_filter(&filter, &channels);

        assert!(channel_filter.allows_channel(0));
        assert!(!channel_filter.allows_channel(1));
        assert!(!channel_filter.allows_channel(2));
        assert_eq!(channel_filter.channel_count(), 1);
    }

    #[test]
    fn test_channel_filter_all() {
        let mut channels = HashMap::new();
        channels.insert(0, ChannelInfo::new(0, "/camera", "sensor_msgs/Image"));
        channels.insert(1, ChannelInfo::new(1, "/lidar", "sensor_msgs/PointCloud2"));

        let filter = ChannelFilter::all(&channels);
        assert!(filter.allows_channel(0));
        assert!(filter.allows_channel(1));
        assert_eq!(filter.channel_count(), 2);
    }
}

// =========================================================================
// TopicFilter::Debug tests
// =========================================================================

#[test]
fn test_topic_filter_debug_all() {
    let filter = TopicFilter::All;
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("All"));
}

#[test]
fn test_topic_filter_debug_include() {
    let filter = TopicFilter::Include(vec!["/test".to_string()]);
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("Include"));
}

#[test]
fn test_topic_filter_debug_exclude() {
    let filter = TopicFilter::Exclude(vec!["/test".to_string()]);
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("Exclude"));
}

#[test]
fn test_topic_filter_debug_regex_include() {
    let filter = TopicFilter::regex_include(".*").unwrap();
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("RegexInclude"));
}

#[test]
fn test_topic_filter_debug_regex_exclude() {
    let filter = TopicFilter::regex_exclude(".*").unwrap();
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("RegexExclude"));
}

#[test]
fn test_topic_filter_debug_custom() {
    let filter = TopicFilter::custom(|_| true);
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("Custom"));
}

// =========================================================================
// TopicFilter::Default tests
// =========================================================================

#[test]
fn test_topic_filter_default() {
    let filter = TopicFilter::default();
    assert!(matches!(filter, TopicFilter::All));
    assert!(filter.should_include("/any_topic"));
}

// =========================================================================
// TopicFilter::Clone tests
// =========================================================================

#[test]
fn test_topic_filter_clone_all() {
    let filter = TopicFilter::All;
    let cloned = filter.clone();
    assert!(cloned.should_include("/test"));
}

#[test]
fn test_topic_filter_clone_include() {
    let filter = TopicFilter::Include(vec!["/test".to_string()]);
    let cloned = filter.clone();
    assert!(cloned.should_include("/test"));
    assert!(!cloned.should_include("/other"));
}

#[test]
fn test_topic_filter_clone_regex() {
    let filter = TopicFilter::regex_include("/test.*").unwrap();
    let cloned = filter.clone();
    assert!(cloned.should_include("/test123"));
}

// =========================================================================
// TopicFilter::regex_include tests
// =========================================================================

#[test]
fn test_topic_filter_regex_include_valid() {
    let filter = TopicFilter::regex_include("^/camera/.*$").unwrap();
    assert!(filter.should_include("/camera/image_raw"));
    assert!(!filter.should_include("/lidar/points"));
}

#[test]
fn test_topic_filter_regex_include_invalid() {
    let result = TopicFilter::regex_include("[invalid(");
    assert!(result.is_err());
}

#[test]
fn test_topic_filter_regex_include_empty_pattern() {
    // Empty regex pattern matches everything (including empty string)
    let filter = TopicFilter::regex_include("").unwrap();
    assert!(filter.should_include(""));
    assert!(filter.should_include("anything"));
}

// =========================================================================
// TopicFilter::regex_exclude tests
// =========================================================================

#[test]
fn test_topic_filter_regex_exclude_valid() {
    let filter = TopicFilter::regex_exclude("^/tf/.*$").unwrap();
    assert!(!filter.should_include("/tf/0"));
    assert!(filter.should_include("/camera/image_raw"));
}

#[test]
fn test_topic_filter_regex_exclude_invalid() {
    let result = TopicFilter::regex_exclude("[invalid(");
    assert!(result.is_err());
}

#[test]
fn test_topic_filter_regex_exclude_wildcard() {
    let filter = TopicFilter::regex_exclude(".*").unwrap();
    assert!(!filter.should_include("/anything"));
    assert!(!filter.should_include(""));
}

// =========================================================================
// TopicFilter::custom tests
// =========================================================================

#[test]
fn test_topic_filter_custom_true() {
    let filter = TopicFilter::custom(|topic| topic.starts_with("/camera"));
    assert!(filter.should_include("/camera/image_raw"));
    assert!(!filter.should_include("/lidar/points"));
}

#[test]
fn test_topic_filter_custom_false() {
    let filter = TopicFilter::custom(|_| false);
    assert!(!filter.should_include("/anything"));
}

#[test]
fn test_topic_filter_custom_complex() {
    let filter = TopicFilter::custom(|topic| topic.len() > 5 && topic.contains('/'));
    assert!(filter.should_include("/camera/image_raw"));
    assert!(!filter.should_include("/tf"));
}

// =========================================================================
// TopicFilter::include and exclude constructors
// =========================================================================

#[test]
fn test_topic_filter_include_empty() {
    let filter = TopicFilter::include(vec![]);
    assert!(!filter.should_include("/anything"));
}

#[test]
fn test_topic_filter_exclude_empty() {
    let filter = TopicFilter::exclude(vec![]);
    assert!(filter.should_include("/anything"));
}

#[test]
fn test_topic_filter_exclude_multiple() {
    let filter = TopicFilter::exclude(vec!["/tf".to_string(), "/tf_static".to_string()]);
    assert!(!filter.should_include("/tf"));
    assert!(!filter.should_include("/tf_static"));
    assert!(filter.should_include("/camera"));
}

// =========================================================================
// ChannelFilter::Debug tests
// =========================================================================

#[test]
fn test_channel_filter_debug() {
    let mut channels = HashMap::new();
    channels.insert(1, ChannelInfo::new(1, "/test", "test/Msg"));

    let filter = ChannelFilter::all(&channels);
    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("ChannelFilter"));
}

// =========================================================================
// ChannelFilter::channels_for_topic tests
// =========================================================================

#[test]
fn test_channel_filter_channels_for_topic_existing() {
    let mut channels = HashMap::new();
    channels.insert(1, ChannelInfo::new(1, "/camera", "sensor_msgs/Image"));
    channels.insert(
        2,
        ChannelInfo::new(2, "/camera", "sensor_msgs/CompressedImage"),
    );
    channels.insert(3, ChannelInfo::new(3, "/lidar", "sensor_msgs/PointCloud2"));

    let filter = ChannelFilter::all(&channels);
    let camera_channels = filter.channels_for_topic("/camera");
    assert_eq!(camera_channels.len(), 2);
    assert!(camera_channels.contains(&1));
    assert!(camera_channels.contains(&2));
}

#[test]
fn test_channel_filter_channels_for_topic_not_found() {
    let mut channels = HashMap::new();
    channels.insert(1, ChannelInfo::new(1, "/camera", "sensor_msgs/Image"));

    let filter = ChannelFilter::all(&channels);
    let lidar_channels = filter.channels_for_topic("/lidar");
    assert_eq!(lidar_channels.len(), 0);
    assert!(lidar_channels.is_empty());
}

#[test]
fn test_channel_filter_channels_for_topic_with_filter() {
    let mut channels = HashMap::new();
    channels.insert(1, ChannelInfo::new(1, "/camera", "sensor_msgs/Image"));
    channels.insert(2, ChannelInfo::new(2, "/lidar", "sensor_msgs/PointCloud2"));

    let topic_filter = TopicFilter::include(vec!["/camera".to_string()]);
    let filter = ChannelFilter::from_topic_filter(&topic_filter, &channels);

    let camera_channels = filter.channels_for_topic("/camera");
    assert_eq!(camera_channels.len(), 1);

    let lidar_channels = filter.channels_for_topic("/lidar");
    assert_eq!(lidar_channels.len(), 0);
}

// =========================================================================
// ChannelFilter with multiple channels per topic
// =========================================================================

#[test]
fn test_channel_filter_multiple_channels_same_topic() {
    let mut channels = HashMap::new();
    channels.insert(1, ChannelInfo::new(1, "/camera", "sensor_msgs/Image"));
    channels.insert(
        2,
        ChannelInfo::new(2, "/camera", "sensor_msgs/CompressedImage"),
    );
    channels.insert(3, ChannelInfo::new(3, "/camera", "sensor_msgs/CameraInfo"));

    let filter = ChannelFilter::all(&channels);
    assert_eq!(filter.channel_count(), 3);
    assert_eq!(filter.channels_for_topic("/camera").len(), 3);
}

// =========================================================================
// ChannelFilter::allows_channel edge cases
// =========================================================================

#[test]
fn test_channel_filter_allows_no_channels() {
    let channels: HashMap<u16, ChannelInfo> = HashMap::new();
    let filter = ChannelFilter::all(&channels);
    assert!(!filter.allows_channel(0));
    assert!(!filter.allows_channel(1));
    assert_eq!(filter.channel_count(), 0);
}

#[test]
fn test_channel_filter_allows_specific_ids() {
    let mut channels = HashMap::new();
    channels.insert(0, ChannelInfo::new(0, "/camera", "test/Msg"));
    channels.insert(100, ChannelInfo::new(100, "/lidar", "test/Msg"));
    channels.insert(65535, ChannelInfo::new(65535, "/imu", "test/Msg"));

    let filter = ChannelFilter::all(&channels);
    assert!(filter.allows_channel(0));
    assert!(filter.allows_channel(100));
    assert!(filter.allows_channel(65535));
    assert!(!filter.allows_channel(1));
}
