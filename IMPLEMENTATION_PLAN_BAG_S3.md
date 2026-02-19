# BAG S3 Streaming Read Implementation Plan

## Overview

Enable `RoboReader` to stream ROS1 BAG files from S3 and other remote transports, matching the existing MCAP transport support.

## Current State

| Component | Status |
|-----------|--------|
| `StreamingBagParser` | ✅ Complete in `src/io/formats/bag/stream.rs` |
| `McapTransportReader` | ✅ Reference implementation available |
| `RoboReader::open()` | ❌ Returns error for BAG URLs |
| `BagTransportReader` | ❌ Does not exist |

## Goals

1. **API Consistency**: `RoboReader::open("s3://bucket/data.bag")` works seamlessly
2. **Feature Parity**: Transport-based BAG reading matches MCAP capabilities
3. **Clean Architecture**: Follow existing patterns (transport abstraction, streaming parser)
4. **Test Coverage**: Unit tests + integration tests with fixtures

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RoboReader                               │
│                     (already exists)                            │
└──────────────────────┬──────────────────────────────────────────┘
                       │ open_with_config()
                       │ detects s3:// scheme
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              BagTransportReader (NEW)                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Transport (Box<dyn Transport>)                         │   │
│  │  - S3Transport                                          │   │
│  │  - LocalTransport                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼ poll_read() chunks                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  StreamingBagParser (exists)                            │   │
│  │  - Incremental parsing                                  │   │
│  │  - Chunk buffering                                      │   │
│  │  - Decompression (none, bz2, lz4)                       │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Core Implementation (Day 1)

**Files to Create:**

1. **`src/io/formats/bag/transport_reader.rs`**
   - `BagTransportReader` struct
   - Implements `FormatReader` trait
   - `open_from_transport()` for S3/remote
   - `open()` convenience for local files

```rust
pub struct BagTransportReader {
    parser: StreamingBagParser,
    path: String,
    messages: Vec<BagMessageRecord>,
    file_size: u64,
    // Channel mapping from conn_id to ChannelInfo
    channels: HashMap<u16, ChannelInfo>,
}
```

**Key Methods:**
- `open_from_transport()` - Read all data via `poll_read()`, parse incrementally
- `channels()` - Convert parser's `connections()` to `ChannelInfo` map
- `message_count()` - From parser
- `start_time()/end_time()` - From first/last message timestamps

2. **Update `src/io/formats/bag/mod.rs`**
   - Add `pub mod transport_reader;`
   - Re-export `BagTransportReader`

### Phase 2: RoboReader Integration (Day 1-2)

**File: `src/io/reader/mod.rs`**

Update `open_with_config()` to enable BAG transport reading:

```rust
// Current (line ~238-246):
FileFormat::Bag => {
    return Err(CodecError::unsupported(
        "BAG format does not support transport-based reading...",
    ));
}

// New:
FileFormat::Bag => {
    return Ok(Self {
        inner: Box::new(
            crate::io::formats::bag::transport_reader::BagTransportReader::open_from_transport(
                transport,
                path.to_string(),
            )?,
        ),
    });
}
```

### Phase 3: Testing (Day 2-3)

**Unit Tests** (`src/io/formats/bag/transport_reader.rs`):
- Open local BAG via `BagTransportReader::open()`
- Verify channels, message counts, timestamps
- Test with compressed chunks (bz2, lz4)

**Integration Tests** (`tests/bag_transport_tests.rs`):
- `RoboReader::open()` with local BAG file (uses transport path)
- Compare output between `BagFormat` (mmap) and `BagTransportReader` (streaming)
- Verify message content, timestamps, channel info match exactly

**S3 Tests** (existing `tests/s3_tests.rs`):
- Add `test_s3_stream_bag()` test case (already stubbed at line 1672)

### Phase 4: Documentation & Polish (Day 3)

- Update `README.md` to document BAG S3 support
- Add doc examples to `BagTransportReader`
- Update `CLAUDE.md` architecture diagram
- Run full test suite: `make test`

## File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `src/io/formats/bag/transport_reader.rs` | Create | New transport-based BAG reader |
| `src/io/formats/bag/mod.rs` | Modify | Add module export |
| `src/io/reader/mod.rs` | Modify | Enable BAG in transport dispatch |
| `tests/bag_transport_tests.rs` | Create | Integration tests |
| `tests/s3_tests.rs` | Modify | Enable S3 BAG test |
| `README.md` | Modify | Document S3 BAG support |

## Key Design Decisions

### 1. Single-Pass Reading

Unlike MCAP which can seek to summary section, BAG files must be read sequentially for transport access. The entire file is streamed and parsed in one pass.

**Trade-off:** Simpler implementation, but:
- Cannot get message count without reading whole file
- No random access to chunks
- Higher latency for metadata queries

### 2. Chunk Decompression

`StreamingBagParser` already handles compressed chunks:
- Detects `compression` field in chunk headers
- Decompresses `bz2`, `lz4`, `none` formats
- Recursively parses inner records

### 3. Channel Mapping

BAG uses `conn_id` (u32), robocodec uses `channel_id` (u16). The transport reader maintains a mapping table.

### 4. Error Handling

Fatal parse errors propagate immediately (corrupted file). Partial reads at end-of-file are handled gracefully (incomplete chunk).

## Performance Considerations

| Aspect | Local (mmap) | Transport (streaming) |
|--------|--------------|----------------------|
| Memory | Maps entire file | Bounded (~1MB buffer) |
| Latency | Zero-copy | Network + parsing overhead |
| Throughput | ~1-2 GB/s | ~50-200 MB/s (S3) |
| Use Case | Local analysis | Cloud archives |

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Large compressed chunks | Memory spike | Parser has 100MB limit per record |
| S3 transfer failures | Partial read | Transport retries at lower level |
| BAG version variants | Parse errors | Parser validates magic + version |
| Performance vs mmap | User complaints | Document trade-offs, keep mmap as default for local files |

## Testing Strategy

### Unit Tests
```rust
#[test]
fn test_transport_reader_local() {
    let reader = BagTransportReader::open("tests/fixtures/test.bag").unwrap();
    assert_eq!(reader.channels().len(), 2);
    assert!(reader.message_count() > 0);
}
```

### Integration Tests
```rust
#[test]
fn test_roboreader_bag_transport() {
    // Via transport (new)
    let transport_reader = RoboReader::open("s3://bucket/data.bag").unwrap();
    
    // Via local file (existing)
    let local_reader = RoboReader::open("data.bag").unwrap();
    
    // Both should yield same messages
    assert_eq!(
        transport_reader.message_count(),
        local_reader.message_count()
    );
}
```

### Fixture Coverage
- Uncompressed BAG
- BZ2 compressed chunks
- LZ4 compressed chunks
- Empty BAG
- Large BAG (>100MB)

## Success Criteria

- [ ] `RoboReader::open("s3://bucket/file.bag")` succeeds
- [ ] Channel info matches between transport and mmap readers
- [ ] Message count matches exactly
- [ ] All messages parse without error
- [ ] Timestamps preserved correctly
- [ ] Test suite passes: `make test`
- [ ] Lint clean: `make lint`
- [ ] Documentation updated

## Timeline

| Day | Tasks |
|-----|-------|
| 1 | Phase 1: `BagTransportReader` implementation |
| 2 | Phase 2: RoboReader integration + Phase 3: Unit tests |
| 3 | Phase 3: Integration tests + Phase 4: Documentation |

## Open Questions

1. Should we support HTTP URLs for BAG (not just S3)?
   - **Recommendation:** Yes, via existing `HttpTransport` - no extra work needed

2. Should we cache parsed messages or stream them?
   - **Recommendation:** Cache in memory (like McapTransportReader) for simplicity
   - Alternative: Stream via callback (more complex, save for future)

3. How to handle BAG files with no index section?
   - **Recommendation:** Parser already handles this - scans entire file

4. Parallel reading for BAG over transport?
   - **Recommendation:** Not supported (requires random access)
   - Can revisit if demand exists

## References

- `McapTransportReader`: `src/io/formats/mcap/transport_reader.rs`
- `StreamingBagParser`: `src/io/formats/bag/stream.rs`
- `FormatReader` trait: `src/io/traits.rs` (lines 65-353)
- RoboReader integration: `src/io/reader/mod.rs` (lines 216-280)
