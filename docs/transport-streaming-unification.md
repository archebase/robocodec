# Transport and Streaming Unification Design

**Author**: Design Team
**Date**: 2026-02-07
**Status**: Approved for Implementation

## Executive Summary

This document describes the unification of the transport and streaming layers in robocodec. The goal is to create a consistent, unified architecture that works across all data sources (local files, S3, HTTP) and all formats (MCAP, BAG, RRD).

**Key Decisions**:
1. Local file readers will use the `Transport` trait (unified path)
2. `Transport` trait is internal-only (not part of public API)
3. Full unification in one implementation phase
4. No backward compatibility - direct integration with `RoboReader`/`RoboWriter`

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Transport Layer](#transport-layer)
- [Streaming Parser Layer](#streaming-parser-layer)
- [Format Layer Integration](#format-layer-integration)
- [Migration Plan](#migration-plan)
- [API Changes](#api-changes)
- [Testing Strategy](#testing-strategy)

---

## Architecture Overview

### Current State

```
RoboReader
    │
    ├── local file path → Sequential/Parallel Reader → std::fs::File (direct)
    │
    └── s3:// URL → S3Reader → S3Client → HTTP (async)
```

**Problems**:
- Duplicated code paths for local vs S3
- No abstraction for new data sources (HTTP, Azure, GCS)
- Inconsistent streaming parser interfaces
- Adding new format requires touching multiple modules

### Target State

```
RoboReader
    │
    ├── any path/URL → LocationParser
    │                        │
    │                        ▼
    │                 ┌────────────────┐
    │                 │ Transport Layer │
    │                 │  (internal)     │
    │                 └────────┬────────┘
    │                          │
    │         ┌──────────────────┼──────────────┐
    │         ▼                  ▼              ▼
    │    ┌─────────┐       ┌─────────┐    ┌─────────┐
    │    │ Local   │       │    S3    │    │  HTTP   │
    │    │Transport│       │Transport │    │Transport│
    │    └────┬────┘       └────┬────┘    └────┬────┘
    │         │                  │               │
    │         └──────────┬───────┴───────────────┘
    │                    │
    │                    ▼
    │         ┌─────────────────────┐
    │         │  Format Reader       │
    │         │  (uses Transport)    │
    │         │  ┌─────────────────┐ │
    │         │  │ StreamingParser │ │
    │         │  └─────────────────┘ │
    │         └─────────────────────┘
    │                    │
    ▼                    ▼
   Decoded Messages    Raw Bytes
```

---

## Transport Layer

### Core Trait

```rust
// src/io/transport/transport.rs

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Unified transport trait for reading bytes from various sources.
///
/// This trait is **internal only** - not exposed in public API.
/// All data sources (local files, S3, HTTP) implement this trait.
pub trait Transport: Send + Sync {
    /// Async read into the given buffer.
    ///
    /// Returns the number of bytes read (may be 0 if no data available
    /// but more may come later for streaming sources).
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>;

    /// Async seek to a specific offset.
    ///
    /// Returns an error if seeking is not supported by this transport.
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: u64,
    ) -> Poll<io::Result<u64>>;

    /// Get the current position.
    fn position(&self) -> u64;

    /// Get the total length if known.
    fn len(&self) -> Option<u64>;

    /// Check if this transport supports seeking.
    fn is_seekable(&self) -> bool;
}
```

### Convenience Extension

```rust
/// Convenience methods for Transport.
pub trait TransportExt: Transport {
    /// Read data asynchronously.
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Seek asynchronously.
    async fn seek(&mut self, pos: u64) -> io::Result<u64>;

    /// Read exactly N bytes.
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()>;

    /// Read all remaining bytes.
    async fn read_to_end(&mut self) -> io::Result<Vec<u8>>;
}
```

### Implementations

| Transport | File | S3 | HTTP | Memory |
|-----------|------|-----|------|--------|
| `LocalTransport` | ✅ | ❌ | ❌ | ❌ |
| `S3Transport` | ❌ | ✅ | ❌ | ❌ |
| `HttpTransport` | ❌ | ❌ | ✅ | ❌ |
| `MemoryTransport` | ❌ | ❌ | ❌ | ✅ |

---

## Streaming Parser Layer

### Core Trait

```rust
// src/io/streaming/parser.rs

/// Streaming parser trait for incremental format parsing.
///
/// All format-specific streaming parsers implement this trait.
pub trait StreamingParser: Send + Sync {
    /// Message type yielded by this parser.
    type Message: Clone + Send;

    /// Parse a chunk of data and extract any complete messages.
    fn parse_chunk(&mut self, data: &[u8]) -> Result<Vec<Self::Message>>;

    /// Get the discovered channels.
    fn channels(&self) -> &HashMap<u16, ChannelInfo>;

    /// Get the total message count.
    fn message_count(&self) -> u64;

    /// Check if channels have been discovered.
    fn has_channels(&self) -> bool;

    /// Check if parser is ready to yield messages.
    fn is_initialized(&self) -> bool;

    /// Reset parser state for a new file.
    fn reset(&mut self);
}
```

### Implementations

| Format | Parser | Status | Notes |
|--------|--------|--------|-------|
| MCAP | `McapStreamingParser` | NEW | Unified interface |
| BAG | `BagStreamingParser` | UPDATE | Implement trait |
| RRD | `RrdStreamingParser` | UPDATE | Already implements |

---

## Format Layer Integration

### Updated FormatReader Trait

```rust
// src/io/traits.rs

pub trait FormatReader: Send + Sync {
    // ... existing methods unchanged ...

    /// Open from any transport source.
    ///
    /// This is the primary method for all format readers.
    fn open_from_transport(
        transport: Box<dyn Transport>,
        config: &ReaderConfig,
    ) -> Result<Self>
    where
        Self: Sized;
}
```

### Format Implementation Pattern

```rust
impl FormatReader for McapFormat {
    fn open_from_transport(
        transport: Box<dyn Transport>,
        config: &ReaderConfig,
    ) -> Result<Self> {
        // 1. Detect if file has summary (via transport)
        // 2. Choose reader strategy:
        //    - With summary: ParallelMcapReader
        //    - Without summary: TwoPassMcapReader
        // 3. Return appropriate reader wrapper
    }
}
```

---

## Migration Plan

### Phase 0: Preparation (Design Complete ✅)
- [x] Design documentation
- [ ] Implementation plan

### Phase 1: Infrastructure (Core Traits)
1. Create `src/io/transport/transport.rs` - `Transport` trait
2. Create `src/io/transport/transport/local.rs` - `LocalTransport` impl
3. Create `src/io/transport/transport/s3.rs` - `S3Transport` impl
4. Create `src/io/streaming/parser.rs` - Consolidate `StreamingParser`
5. Update `src/io/transport/mod.rs` with exports

### Phase 2: Transport Implementations
1. Implement `LocalTransport` using async wrapper around `std::fs::File`
2. Implement `S3Transport` using existing `S3Client`
3. Add `TransportExt` convenience methods
4. Unit tests for all transports

### Phase 3: Streaming Parser Unification
1. Create `McapStreamingParser` (unified interface)
2. Update `BagStreamingParser` to implement `StreamingParser`
3. Update `RrdStreamingParser` to match new trait signature
4. Deprecate old streaming parsers

### Phase 4: Format Integration
1. Update `FormatReader::open_from_transport()` for all formats
2. Update `McapFormat` to use transport
3. Update `BagFormat` to use transport
4. Update `RrdFormat` to to use transport

### Phase 5: RoboReader Integration
1. Update `RoboReader::open()` to use transport layer
2. Update `RoboReader::open_with_config()` to use transport
3. Update location detection logic
4. Integration tests

### Phase 6: Cleanup
1. Remove deprecated code
2. Remove unused modules (`transport/s3/` re-export)
3. Update documentation
4. Final integration tests

---

## API Changes

### Public API (RoboReader)

```rust
// BEFORE (still works)
let reader = RoboReader::open("data.mcap")?;
let reader = RoboReader::open_with_config("data.mcap", config)?;

// AFTER (new capabilities)
let reader = RoboReader::open("s3://bucket/data.mcap")?;
let reader = RoboReader::open("https://example.com/data.mcap")?;
let reader = RoboReader::open_with_config("s3://bucket/data.mcap", config)?;
```

### Internal API (Transport)

```rust
// NOT exposed in public API
use crate::io::transport::{Transport, TransportExt};

// Usage inside format readers
let transport: Box<dyn Transport> = match location {
    Location::Local(path) => Box::new(LocalTransport::open(path)?),
    Location::S3(url) => Box::new(S3Transport::open(url).await?),
    Location::Http(url) => Box::new(HttpTransport::open(url).await?),
};

// Use transport
let data = transport.read_to_end().await?;
```

---

## File Structure Changes

```
src/io/
├── transport/
│   ├── mod.rs              # Module exports
│   ├── transport.rs        # Transport trait + TransportExt (NEW)
│   ├── local.rs            # LocalTransport (moved from transport/local.rs)
│   ├── s3.rs               # S3Transport (NEW)
│   └── memory.rs           # MemoryTransport (NEW, for testing)
├── streaming/
│   ├── mod.rs              # Module exports
│   └── parser.rs           # StreamingParser trait (consolidated)
├── formats/
│   ├── mcap/
│   │   ├── mod.rs           # Add open_from_transport()
│   │   ├── streaming.rs     # McapStreamingParser (NEW)
│   │   ├── s3_adapter.rs    # Keep or deprecate
│   │   └── ...
│   ├── bag/
│   │   ├── mod.rs           # Add open_from_transport()
│   │   ├── streaming.rs     # Update to implement StreamingParser
│   │   └── ...
│   └── rrd/
│       ├── mod.rs           # Add open_from_transport()
│       ├── streaming.rs     # Update to implement StreamingParser
│       └── ...
├── s3/
│   ├── mod.rs              # Simplified - re-export transport
│   ├── reader.rs           # Major refactor to use Transport
│   └── client.rs           # Unchanged
├── traits.rs               # Add open_from_transport()
└── reader/
    └── mod.rs              # Update to use Location → Transport mapping

DELETED:
├── transport/s3/mod.rs     # Unused re-export module
└── s3/parser.rs            # Moved to streaming/parser.rs
```

---

## Testing Strategy

### Unit Tests

1. **Transport Tests**
   - `LocalTransport` with various file types
   - `S3Transport` with mock HTTP responses
   - `MemoryTransport` for algorithmic testing

2. **StreamingParser Tests**
   - All parsers implement `StreamingParser` trait
   - Chunk boundary handling
   - State management

3. **Integration Tests**
   - End-to-end: `RoboReader::open()` for all sources
   - Format detection from various sources
   - Error handling for unreachable sources

### Performance Tests

1. Ensure local file performance is not degraded
2. Verify S3 streaming maintains throughput
3. Measure memory usage for large files

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Async overhead for local files | Performance | Keep fast path optimized |
| Breaking existing internal APIs | Stability | Update all call sites |
| Complex trait object usage | Maintainability | Clear documentation, type aliases |

---

## Success Criteria

- [ ] All formats can read from local files via `Transport`
- [ ] All formats can read from S3 via `Transport`
- [ ] `RoboReader::open()` works with s3:// URLs
- [ ] `StreamingParser` implemented for all formats
- [ ] All tests pass (1800+ tests)
- [ ] No performance regression for local files
- [ ] Documentation updated in CLAUDE.md

---

## Appendix: Code Examples

### Example 1: Adding a New Transport

```rust
// src/io/transport/azure.rs

pub struct AzureTransport {
    client: AzureClient,
    container: String,
    blob: String,
}

impl Transport for AzureTransport {
    fn poll_read(...) -> Poll<io::Result<usize>> {
        // Azure blob read implementation
    }

    fn poll_seek(...) -> Poll<io::Result<u64>> {
        // Azure supports range requests
    }

    fn position(&self) -> u64 { /* ... */ }
    fn len(&self) -> Option<u64> { /* ... */ }
    fn is_seekable(&self) -> bool { true }
}
```

### Example 2: Using Transport in Format Reader

```rust
impl FormatReader for McapFormat {
    fn open_from_transport(
        transport: Box<dyn Transport>,
        config: &ReaderConfig,
    ) -> Result<Self> {
        // Use transport to detect file characteristics
        let len = transport.len().ok_or_else(|| {
            CodecError::config("Cannot determine file size")
        })?;

        // Try to read summary section
        let has_summary = if transport.is_seekable() {
            transport.seek(len - 1024).await?;
            // Read and parse summary...
        };

        if has_summary {
            Ok(McapFormat::Parallel(ParallelMcapReader::open_from_transport(transport)?))
        } else {
            Ok(McapFormat::TwoPass(TwoPassMcapReader::open_from_transport(transport)?))
        }
    }
}
```

### Example 3: RoboReader Integration

```rust
impl RoboReader {
    pub fn open(location: &str) -> Result<Self> {
        let parsed = Location::parse(location)?;
        let transport: Box<dyn Transport> = match parsed.kind {
            LocationKind::Local => Box::new(LocalTransport::open(&parsed.path)?),
            LocationKind::S3 => Box::new(S3Transport::open(&parsed.url).await?),
            LocationKind::Http => Box::new(HttpTransport::open(&parsed.url).await?),
        };

        let format = detect_format_from_transport(&transport).await?;

        Ok(Self {
            inner: format.open_from_transport(transport, &ReaderConfig::default())?,
        })
    }
}
```

---

**Document Version**: 1.0
**Last Updated**: 2026-02-07
