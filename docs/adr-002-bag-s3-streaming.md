# ADR-002: BAG S3 Streaming Support

**Author**: ArcheBase Team  
**Date**: 2026-02-19  
**Status**: Implemented  

## Context

Robocodec supported S3 streaming for MCAP format via `McapTransportReader`, but BAG format could only be read from local files using memory-mapped I/O. This inconsistency meant users couldn't stream ROS1 BAG files from S3 or other remote sources.

### Problem

- `RoboReader::open("s3://bucket/data.bag")` returned an error
- Users had to download BAG files locally before reading
- API inconsistency between MCAP and BAG formats

### Existing Infrastructure

The codebase already had:
- `StreamingBagParser` - incremental BAG parser for streaming
- `Transport` trait - abstraction for S3, HTTP, local files
- `McapTransportReader` - reference implementation for transport-based reading

## Decision

Implement `BagTransportReader` to enable streaming BAG file reading from any Transport source (S3, HTTP, local files).

### Key Design Decisions

1. **Single-Pass Reading**: BAG format requires sequential reading over transport - no random access to chunks
2. **Reuse StreamingBagParser**: Leverage existing incremental parser with 64KB chunk buffering
3. **Memory vs Performance Trade-off**: Cache all messages in memory (like McapTransportReader) for simplicity
4. **Full FormatReader Implementation**: Support all trait methods including `iter_raw_boxed()`

### Implementation

```rust
pub struct BagTransportReader {
    parser: StreamingBagParser,
    path: String,
    messages: Vec<BagMessageRecord>,
    file_size: u64,
    channels: HashMap<u16, ChannelInfo>,
}
```

**Key Methods**:
- `open()` - Convenience for local files via LocalTransport
- `open_from_transport()` - For S3/HTTP sources (requires `remote` feature)
- `iter_raw_boxed()` - Returns iterator over raw messages

### Integration

Updated `RoboReader::open_with_config()` to dispatch BAG URLs to `BagTransportReader`:

```rust
FileFormat::Bag => {
    return Ok(Self {
        inner: Box::new(
            BagTransportReader::open_from_transport(transport, path)?
        ),
    });
}
```

## Consequences

### Positive

- API consistency: MCAP and BAG both support S3 streaming
- Users can now read BAG files directly from cloud storage
- Reuses existing infrastructure (StreamingBagParser, Transport trait)
- 8 integration tests verify correctness against memory-mapped reader

### Trade-offs

| Aspect | Local (mmap) | Transport (streaming) |
|--------|--------------|----------------------|
| Memory | Maps entire file | Bounded (~1MB buffer) + cached messages |
| Latency | Zero-copy | Network + parsing overhead |
| Throughput | ~1-2 GB/s | ~50-200 MB/s (S3) |
| Use Case | Local analysis | Cloud archives |

### Limitations

- **No parallel reading**: Requires random access not available over transport
- **Single-pass**: Must read entire file to get message count
- **Memory usage**: All messages cached (acceptable for typical BAG sizes)

## Testing

- 8 integration tests comparing transport vs mmap output
- Verified against 3 BAG fixtures with various compression schemes
- All 1834+ existing tests pass
- Format and clippy clean

## References

- Implementation: `src/io/formats/bag/transport_reader.rs`
- Tests: `tests/bag_transport_tests.rs`
- Related: `McapTransportReader` in `src/io/formats/mcap/transport_reader.rs`
- PR: #61
