// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Context types for MCAP rewrite operations.

use std::collections::HashMap;

/// Context for rewrite operations, holding all intermediate state.
///
/// This struct makes the rewrite process more testable by providing
/// access to intermediate state that would otherwise be hidden
/// in local variables within the rewrite function.
#[derive(Debug, Default)]
pub struct RewriteContext {
    /// Maps transformed type names to schema IDs
    pub schema_ids: HashMap<String, u16>,
    /// Maps original channel IDs to new channel IDs
    pub channel_map: HashMap<u16, u16>,
    /// Tracks topic name collisions for deduplication
    pub topic_counter: HashMap<String, u32>,
    /// Maps channel IDs to transformed message types
    pub channel_type_map: HashMap<u16, String>,
}

impl RewriteContext {
    /// Create a new empty rewrite context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the new channel ID for an original channel ID.
    pub fn get_channel_id(&self, old_id: u16) -> Option<u16> {
        self.channel_map.get(&old_id).copied()
    }

    /// Get the schema ID for a transformed type name.
    pub fn get_schema_id(&self, type_name: &str) -> Option<u16> {
        self.schema_ids.get(type_name).copied()
    }

    /// Get the transformed message type for a channel.
    pub fn get_transformed_type(&self, channel_id: u16) -> Option<&str> {
        self.channel_type_map.get(&channel_id).map(|s| s.as_str())
    }

    /// Check if a topic name has a collision and needs a suffix.
    pub fn has_topic_collision(&self, topic: &str, _current_channel_id: u16) -> bool {
        // Check if we've already seen this topic (collision detection)
        if let Some(&count) = self.topic_counter.get(topic) {
            return count > 0;
        }
        false
    }

    /// Reset all mappings.
    pub fn reset(&mut self) {
        self.schema_ids.clear();
        self.channel_map.clear();
        self.topic_counter.clear();
        self.channel_type_map.clear();
    }
}

/// How a message should be handled during rewriting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageHandling {
    /// Pass through the message without modification
    Passthrough,
    /// Decode and re-encode the message with proper CDR headers
    Reencode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_context_new() {
        let ctx = RewriteContext::new();
        assert!(ctx.schema_ids.is_empty());
        assert!(ctx.channel_map.is_empty());
        assert!(ctx.topic_counter.is_empty());
        assert!(ctx.channel_type_map.is_empty());
    }

    #[test]
    fn test_rewrite_context_default() {
        let ctx = RewriteContext::default();
        assert!(ctx.schema_ids.is_empty());
        assert!(ctx.channel_map.is_empty());
    }

    #[test]
    fn test_rewrite_context_reset() {
        let mut ctx = RewriteContext::new();
        ctx.schema_ids.insert("test".to_string(), 1);
        ctx.channel_map.insert(1, 2);
        ctx.topic_counter.insert("/topic".to_string(), 1);
        ctx.channel_type_map.insert(1, "TestType".to_string());

        ctx.reset();

        assert!(ctx.schema_ids.is_empty());
        assert!(ctx.channel_map.is_empty());
        assert!(ctx.topic_counter.is_empty());
        assert!(ctx.channel_type_map.is_empty());
    }

    #[test]
    fn test_rewrite_context_get_channel_id() {
        let mut ctx = RewriteContext::new();
        ctx.channel_map.insert(1, 10);
        ctx.channel_map.insert(2, 20);

        assert_eq!(ctx.get_channel_id(1), Some(10));
        assert_eq!(ctx.get_channel_id(2), Some(20));
        assert_eq!(ctx.get_channel_id(3), None);
    }

    #[test]
    fn test_rewrite_context_get_schema_id() {
        let mut ctx = RewriteContext::new();
        ctx.schema_ids.insert("std_msgs/String".to_string(), 5);

        assert_eq!(ctx.get_schema_id("std_msgs/String"), Some(5));
        assert_eq!(ctx.get_schema_id("other/Type"), None);
    }

    #[test]
    fn test_rewrite_context_get_transformed_type() {
        let mut ctx = RewriteContext::new();
        ctx.channel_type_map.insert(1, "new/Type".to_string());

        assert_eq!(ctx.get_transformed_type(1), Some("new/Type"));
        assert_eq!(ctx.get_transformed_type(2), None);
    }

    #[test]
    fn test_rewrite_context_has_topic_collision() {
        let mut ctx = RewriteContext::new();
        assert!(!ctx.has_topic_collision("/topic", 1));

        ctx.topic_counter.insert("/topic".to_string(), 0);
        assert!(!ctx.has_topic_collision("/topic", 1));

        ctx.topic_counter.insert("/topic".to_string(), 1);
        assert!(ctx.has_topic_collision("/topic", 1));
    }

    #[test]
    fn test_message_handling_equality() {
        assert_eq!(MessageHandling::Passthrough, MessageHandling::Passthrough);
        assert_eq!(MessageHandling::Reencode, MessageHandling::Reencode);
        assert_ne!(MessageHandling::Passthrough, MessageHandling::Reencode);
    }

    #[test]
    fn test_message_handling_copy() {
        let handling = MessageHandling::Reencode;
        let _ = handling; // Can use after move
        let copied = handling;
        assert_eq!(copied, MessageHandling::Reencode);
    }
}
