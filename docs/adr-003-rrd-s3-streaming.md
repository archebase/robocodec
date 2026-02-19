# ADR-003: RRD S3 Streaming Support

**Author**: ArcheBase Team  
**Date**: 2026-02-19  
**Status**: Implemented  

## Context

Following the implementation of BAG S3 streaming (ADR-002), RRD format was the only remaining format that didn't support transport-based reading. This created an API inconsistency where `RoboReader::open("s3://...")` worked for MCAP and BAG but returned an error for RRD.

### Problem

- `RoboReader::open("s3://bucket/data.rrd")` returned an error
- Users had to download RRD files locally before reading
- API inconsistency between formats

### Existing Infrastructure

The codebase already had:
- `StreamingRrdParser` - incremental RRD parser for streaming
- `Transport` trait - abstraction for S3, HTTP, local files
- `BagTransportReader` / `McapTransportReader` - reference implementations

## Decision

Implement `RrdTransportReader` to enable streaming RRD file reading from any Transport source (S3, HTTP, local files).

### Key Design Decisions

1. **Single-Pass Reading**: RRD format requires sequential reading over transport
2. **Reuse StreamingRrdParser**: Leverage existing incremental parser with 64KB chunk buffering
3. **Memory Caching**: Cache all messages in memory (like other transport readers)
4. **Full FormatReader Implementation**: Support all trait methods including `iter_raw_boxed()`

### Implementation

```rust
pub struct RrdTransportReader {
    parser: StreamingRrdParser,
    path: String,
    messages: Vec<RrdMessageRecord>,
    file_size: u64,
    channels: HashMap<u16, ChannelInfo>,
}
```

**Key Methods**:
- `open()` - Convenience for local files via LocalTransport
- `open_from_transport()` - For S3/HTTP sources (requires `remote` feature)
- `iter_raw_boxed()` - Returns iterator over raw messages

### Integration

Updated `RoboReader::open_with_config()` to dispatch RRD URLs to `RrdTransportReader`:

```rust
FileFormat::Rrd => {
    return Ok(Self {
        inner: Box::new(
            RrdTransportReader::open_from_transport(transport, path)?
        ),
    });
}
```

## Consequences

### Positive

- **Complete API consistency**: All formats (MCAP, BAG, RRD) now support S3 streaming
- Users can read RRD files directly from cloud storage
- Reuses existing infrastructure (StreamingRrdParser, Transport trait)
- 8 integration tests verify correctness against parallel reader

### Trade-offs

| Aspect | Local (parallel) | Transport (streaming) |
|--------|-----------------|----------------------|
| Memory | Bounded per chunk | Bounded (~1MB buffer) + cached messages |
| Latency | Optimized for local | Network + parsing overhead |
| Throughput | ~500 MB/s | ~50-200 MB/s (S3) |
| Use Case | Local analysis | Cloud archives |

### Limitations

- **No parallel reading**: Requires random access not available over transport
- **Single-pass**: Must read entire file to get message count
- **RRD uses message index as timestamp**: RRD format doesn't have explicit timestamps like MCAP/BAG

## Testing

- 8 integration tests comparing transport vs parallel output
- Verified against 3 RRD fixtures
- All existing tests pass (1834+)
- Format and clippy clean

## Unified Transport Support

With this implementation, all three formats now support S3/HTTP transport:

| Format | Transport Support | Reader Type |
|--------|------------------|-------------|
| MCAP | ✅ S3, HTTP, Local | `McapTransportReader` |
| BAG | ✅ S3, HTTP, Local | `BagTransportReader` |
| RRD | ✅ S3, HTTP, Local | `RrdTransportReader` |

## References

- Implementation: `src/io/formats/rrd/transport_reader.rs`
- Tests: `tests/rrd_transport_tests.rs`
- Related: ADR-002 (BAG S3 streaming)
- Related: `BagTransportReader` in `src/io/formats/bag/transport_reader.rs`
- PR: #62
