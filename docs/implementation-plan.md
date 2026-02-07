# Implementation Plan: Transport + Streaming Unification

**Related**:
- [Transport and Streaming Unification](./transport-streaming-unification.md)
- [Streaming Parser Unification](./streaming-parser-unification.md)

**Status**: Ready for Implementation

---

## Overview

This document provides the step-by-step implementation plan for unifying the transport and streaming layers in robocodec.

---

## Implementation Steps

### Step 1: Create Transport Infrastructure

**Files to create**:
- `src/io/transport/transport.rs` - Core `Transport` trait and `TransportExt`
- `src/io/transport/local.rs` - `LocalTransport` implementation

**Files to modify**:
- `src/io/transport/mod.rs` - Update exports

**Implementation**:
```rust
// transport.rs
pub trait Transport: Send + Sync {
    fn poll_read(...) -> Poll<io::Result<usize>>;
    fn poll_seek(...) -> Poll<io::Result<u64>>;
    fn position(&self) -> u64;
    fn len(&self) -> Option<u64>;
    fn is_seekable(&self) -> bool;
}

// local.rs
pub struct LocalTransport {
    file: std::fs::File,
    pos: u64,
    len: u64,
}

impl Transport for LocalTransport {
    // Wraps std::fs::File with async interface
}
```

**Tests**:
- LocalTransport can read files
- LocalTransport can seek within files
- Position tracking works correctly

---

### Step 2: Create S3Transport

**Files to create**:
- `src/io/transport/s3.rs` - `S3Transport` implementation

**Files to delete**:
- `src/io/transport/s3/mod.rs` - Unused re-export module

**Implementation**:
```rust
pub struct S3Transport {
    client: S3Client,
    location: S3Location,
    pos: u64,
    len: u64,
    buffer: Vec<u8>, // For async read buffering
}

impl Transport for S3Transport {
    // Uses S3Client::fetch_range() internally
}
```

**Tests**:
- S3Transport can read from S3
- S3Transport can seek (using range requests)
- Proper error handling

---

### Step 3: Consolidate StreamingParser Trait

**Files to create**:
- `src/io/streaming/mod.rs` - New module
- `src/io/streaming/parser.rs` - Consolidated `StreamingParser` trait

**Files to move**:
- `src/io/s3/parser.rs` → `src/io/streaming/parser.rs`

**Files to modify**:
- `src/io/mod.rs` - Add `streaming` module

**Implementation**:
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

### Step 4: Create Unified MCAP Streaming Parser

**Files to create**:
- `src/io/formats/mcap/streaming.rs` - `McapStreamingParser`

**Files to deprecate**:
- `src/io/formats/mcap/stream.rs` - Mark as deprecated
- `src/io/formats/mcap/s3_adapter.rs` - Mark as deprecated

**Implementation**:
```rust
pub struct McapStreamingParser {
    reader: mcap::sans_io::linear_reader::LinearReader,
    // ...
}

impl StreamingParser for McapStreamingParser {
    type Message = MessageRecord;
    // ...
}
```

---

### Step 5: Implement StreamingParser for BAG

**Files to modify**:
- `src/io/formats/bag/stream.rs`

**Implementation**:
```rust
impl StreamingParser for StreamingBagParser {
    type Message = BagMessageRecord;
    // Delegate to existing methods
}
```

---

### Step 6: Update FormatReader Trait

**Files to modify**:
- `src/io/traits.rs`

**Changes**:
```rust
pub trait FormatReader: Send + Sync {
    // ... existing methods ...

    /// Open from any transport source (internal method).
    #[doc(hidden)]
    fn open_from_transport(
        transport: Box<dyn Transport>,
        config: &ReaderConfig,
    ) -> Result<Self>
    where
        Self: Sized;
}
```

---

### Step 7: Implement open_from_transport for MCAP

**Files to modify**:
- `src/io/formats/mcap/mod.rs` or `src/io/formats/mcap/reader.rs`

**Implementation**:
```rust
impl McapFormat {
    pub fn open_from_transport(
        transport: Box<dyn Transport>,
        config: &ReaderConfig,
    ) -> Result<Self> {
        // Use transport to determine reader strategy
        // (Parallel vs Sequential vs TwoPass)
    }
}
```

---

### Step 8: Implement open_from_transport for BAG and RRD

**Files to modify**:
- `src/io/formats/bag/mod.rs`
- `src/io/formats/rrd/mod.rs`

---

### Step 9: Update RoboReader

**Files to modify**:
- `src/io/reader/mod.rs`

**Changes**:
```rust
impl RoboReader {
    pub fn open(location: &str) -> Result<Self> {
        // Parse location (s3://, http://, or local path)
        let parsed = Location::parse(location)?;

        // Create appropriate transport
        let transport: Box<dyn Transport> = create_transport(&parsed)?;

        // Detect format from transport
        let format = detect_format_from_transport(&transport)?;

        // Open format reader from transport
        let inner = format.open_from_transport(transport, &ReaderConfig::default())?;

        Ok(Self { inner })
    }
}
```

---

### Step 10: Create Location Parser

**Files to create**:
- `src/io/location.rs` - Location parsing and URL handling

**Implementation**:
```rust
pub enum LocationKind {
    Local,
    S3,
    Http,
}

pub struct ParsedLocation {
    pub kind: LocationKind,
    pub path: Option<String>,
    pub url: Option<String>,
}

pub fn parse_location(input: &str) -> Result<ParsedLocation> {
    if input.starts_with("s3://") {
        parse_s3_location(input)
    } else if input.starts_with("http://") || input.starts_with("https://") {
        parse_http_location(input)
    } else {
        parse_local_location(input)
    }
}
```

---

### Step 11: Update S3Reader

**Files to modify**:
- `src/io/s3/reader.rs`

**Changes**:
- Use `S3Transport` instead of direct `S3Client` calls
- Use `StreamingParser` trait object instead of enum branching
- Simplify `S3MessageStream`

---

### Step 12: Cleanup

**Files to delete**:
- `src/io/transport/s3/mod.rs` - Unused re-export
- `src/io/s3/parser.rs` - Moved to streaming/parser.rs

**Files to deprecate**:
- `src/io/formats/mcap/stream.rs` - Old streaming parser
- `src/io/formats/mcap/s3_adapter.rs` - Functionality moved to streaming.rs

**Files to update**:
- `src/io/mod.rs` - Update module structure
- `CLAUDE.md` - Update architecture documentation

---

## Order of Implementation

**Recommended sequence** (minimizes breakage, allows testing at each step):

1. Transport infrastructure (Steps 1-3)
2. Streaming parser unification (Steps 4-6)
3. Format integration (Steps 7-8)
4. RoboReader integration (Steps 9-11)
5. Cleanup and documentation (Step 12)

**Each step should**:
- Be compilable
- Pass all tests
- Be commit-able

---

## Testing Strategy

### After Each Step

1. Run `cargo test` - ensure no regressions
2. Run `cargo clippy` - ensure no warnings
3. Run `cargo fmt` - ensure formatting

### Final Integration Tests

```rust
// Test local file reading
#[test]
fn test_local_mcap_via_transport() {
    let reader = RoboReader::open("tests/fixtures/example.mcap").unwrap();
    let count = reader.decoded().count();
    assert!(count > 0);
}

// Test S3 reading (if available)
#[test]
#[cfg(feature = "s3")]
fn test_s3_mcap_via_transport() {
    let reader = RoboReader::open("s3://test-bucket/example.mcap").unwrap();
    let count = reader.decoded().count();
    assert!(count > 0);
}
```

---

## Rollback Plan

If implementation fails:
1. Each step is in its own commit - revert specific commit
2. Keep design documents for future reference
3. Document what failed and why

---

**Document Version**: 1.0
**Last Updated**: 2026-02-07
