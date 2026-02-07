# Streaming Parser Unification

**Related**: [Transport and Streaming Unification](./transport-streaming-unification.md)
**Date**: 2026-02-07

## Overview

This document details the unification of streaming parsers across all formats (MCAP, BAG, RRD).

## Current State

| Format | Parser | parse_chunk() | Implements StreamingParser? |
|--------|--------|--------------|----------------------------|
| MCAP | `StreamingMcapParser` (deprecated) | ✅ | ❌ |
| MCAP | `McapS3Adapter` | ❌ (`process_chunk`) | ❌ |
| BAG | `StreamingBagParser` | ✅ | ❌ |
| RRD | `StreamingRrdParser` | ✅ | ✅ **Only one!** |

**Problem**: Inconsistent interfaces, trait defined but not used.

## Target State

All formats implement `StreamingParser` with consistent method signatures:

```rust
pub trait StreamingParser: Send + Sync {
    type Message: Clone + Send;

    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>>;
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;
    fn message_count(&self) -> u64;
    fn has_channels(&self) -> bool;
    fn is_initialized(&self) -> bool;
    fn reset(&mut self);
}
```

---

## Implementation Details

### MCAP Streaming Parser

**New file**: `src/io/formats/mcap/streaming.rs`

```rust
/// Unified MCAP streaming parser.
///
/// Wraps the mcap crate's LinearReader for robust parsing
/// while implementing the StreamingParser trait.
pub struct McapStreamingParser {
    reader: mcap::sans_io::linear_reader::LinearReader,
    schemas: HashMap<u16, SchemaInfo>,
    channels: HashMap<u16, ChannelRecordInfo>,
    message_count: u64,
}

impl StreamingParser for McapStreamingParser {
    type Message = MessageRecord;

    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>> {
        // Use mcap::LinearReader
        let mut messages = Vec::new();
        self.reader.insert(data.len()).copy_from_slice(data);
        self.reader.notify_read(data.len());

        while let Some(event) = self.reader.next_event() {
            match event? {
                LinearReadEvent::ReadRequest(_) => break,
                LinearReadEvent::Record { opcode, data } => {
                    self.process_record(opcode, data, &mut messages)?;
                }
            }
        }
        Ok(messages)
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        static CHANNELS: OnceLock<HashMap<u16, ChannelInfo>> = OnceLock::new();
        // ... convert internal channels to ChannelInfo
    }

    fn message_count(&self) -> u64 { self.message_count }
    fn has_channels(&self) -> bool { !self.channels.is_empty() }
    fn is_initialized(&self) -> bool { self.has_channels() }
    fn reset(&mut self) { *self = Self::new(); }
}
```

### BAG Streaming Parser

**Update**: `src/io/formats/bag/stream.rs`

```rust
// Add trait implementation
impl StreamingParser for StreamingBagParser {
    type Message = BagMessageRecord;

    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>> {
        // Already exists, just delegate
        self.parse_chunk(data)
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        // Convert internal channels to ChannelInfo
        static CHANNELS: OnceLock<HashMap<u16, ChannelInfo>> = OnceLock::new();
        // ...
    }

    fn message_count(&self) -> u64 { self.message_count }
    fn has_channels(&self) -> bool { !self.connections.is_empty() }
    fn is_initialized(&self) -> bool { true }
    fn reset(&mut self) { self.connections.clear(); }
}
```

### RRD Streaming Parser

**Update**: `src/io/formats/rrd/stream.rs`

Already implements `StreamingParser` - just needs signature verification.

---

## Deprecation Plan

### Phase 1: Mark Old MCAP Parser as Deprecated

```rust
// src/io/formats/mcap/stream.rs (old file)

/// Streaming MCAP parser.
///
/// **DEPRECATED**: Use `McapStreamingParser` instead, which provides
/// a unified interface and better compatibility with the mcap crate.
#[deprecated(since = "0.2.0", note = "Use McapStreamingParser instead")]
pub struct StreamingMcapParser {
    // ...
}
```

### Phase 2: Update References

Search and replace:
- `crate::io::formats::mcap::stream::StreamingMcapParser`
- `crate::io::formats::mcap::stream::MessageRecord`
- `crate::io::formats::mcap::stream::SchemaInfo`
- `crate::io::formats::mcap::stream::ChannelRecordInfo`

Replace with new locations in `streaming.rs`.

### Phase 3: Remove (Future Release)

After deprecation period, remove old `stream.rs` entirely.

---

## File Changes

```
src/io/formats/mcap/
├── streaming.rs          # NEW - McapStreamingParser
├── s3_adapter.rs        # DEPRECATE - functionality moved to streaming.rs
├── stream.rs             # DEPRECATE - old parser, remove in future
└── mod.rs               # Add streaming.rs to exports

src/io/formats/bag/
└── stream.rs             # UPDATE - implement StreamingParser

src/io/formats/rrd/
└── stream.rs             # UPDATE - verify StreamingParser impl

src/io/
├── streaming/
│   ├── mod.rs            # NEW module
│   └── parser.rs         # NEW - StreamingParser trait (consolidated)
└── s3/
    └── parser.rs          # REMOVE - functionality moved to streaming/parser.rs
```

---

## Testing

### Unified Streaming Parser Tests

```rust
// src/io/streaming/tests.rs

#[test]
fn test_all_parsers_implement_trait() {
    // Verify all parsers implement StreamingParser
    fn assert_parser<T: StreamingParser>(_parser: &T) {}

    assert_parser(&McapStreamingParser::new());
    assert_parser(&StreamingBagParser::new());
    assert_parser(&StreamingRrdParser::new());
}

#[test]
fn test_chunk_boundary_handling() {
    // Test that parsers correctly handle records split across chunks
    let test_data = generate_split_record();

    let mut parser = McapStreamingParser::new();

    // First chunk (partial record)
    let result1 = parser.parse_chunk(&test_data[0..100]);
    assert!(result1.unwrap().is_empty()); // No complete message yet

    // Second chunk (completes record)
    let result2 = parser.parse_chunk(&test_data[100..]);
    assert!(result2.unwrap().len() == 1); // One complete message
}
```

---

## Migration Checklist

- [ ] Create `src/io/streaming/parser.rs` with `StreamingParser` trait
- [ ] Create `src/io/formats/mcap/streaming.rs` with `McapStreamingParser`
- [ ] Implement `StreamingParser` for `StreamingBagParser`
- [ ] Verify `StreamingRrdParser` implementation
- [ ] Update `src/io/formats/mcap/mod.rs` exports
- [ ] Update `src/io/s3/reader.rs` to use trait objects
- [ ] Deprecate old streaming parsers
- [ ] Add comprehensive tests
- [ ] Update documentation

---

**Document Version**: 1.0
**Last Updated**: 2026-02-07
