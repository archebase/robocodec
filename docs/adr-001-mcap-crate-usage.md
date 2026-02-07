# Architecture Decision: MCAP Crate Usage

## Status: **IMPLEMENTED**

## Context

Robocodec needs complete MCAP format support including LZ4 frame compression and message index handling for performance-sensitive production use.

## Problem Statement

Should robocodec use the upstream `mcap` crate directly, maintain custom implementation, or use a hybrid approach?

## Analysis

### Current State

| Component | Implementation | LZ4 Support | Index Support | Perf (scan) |
|-----------|----------------|-------------|---------------|-------------|
| sequential.rs | mcap crate | ✓ Complete | ✓ Complete | Good |
| parallel.rs | Custom | ✓ Fixed | ✓ Partial | **Best** |
| s3/reader.rs | Custom | ✗ None | ✗ None | Medium |
| writer.rs | Custom | ✓ Added | ✓ Complete | Good |

### Benchmark Results

```
Memory mapping:   486 GiB/s  (custom advantage)
Zstd decompress:   42 GiB/s  (equal - same library)
LZ4 decompress:   4.4 GiB/s  (equal - same library)
```

### Trade-offs

| Approach | Performance | Maintenance | Features | Risk |
|----------|-------------|-------------|----------|------|
| **Full mcap crate** | Medium | Low | Complete | Low |
| **Full custom** | High | High | Partial | High |
| **Hybrid** | High | Medium | Complete | Low |

## Decision

**Option C: Adaptive Hybrid Approach**

Use the best tool for each use case:

1. **Sequential reads** → mcap crate (already proven)
2. **Parallel full scans** → custom + rayon (performance-critical)
3. **Indexed queries** → mcap crate's IndexedReader (complete index support)
4. **S3 streaming** → mcap crate's LinearReader (async-optimized)

### Architecture

```
Public API (unchanged):
    RoboReader::open(path) ──► AdaptiveMcapReader

Internal strategy selection:
    ┌─► Small files (<100MB)      ──► SequentialReader (mcap crate)
    │
    ├─► Time-range query          ──► IndexedReader (mcap crate)
    │
    ├─► Topic filter               ──► IndexedReader (mcap crate)
    │
    ├─► S3 URL                     ──► S3Reader (mcap crate + async)
    │
    └─► Large file, full scan      ──► ParallelReader (custom + rayon)
```

## Implementation Phases

### Phase 1: Refactor Writer (Complete)
- [x] Add LZ4 compression using lz4 crate
- [x] Enable message indexes by default
- [x] Complete index writing

### Phase 2: Enhance S3 Reader (Next)
- Replace custom S3 reader with mcap LinearReader
- Add async streaming with tokio
- Maintain unified API

### Phase 3: Implement Adaptive Reader (Complete)
- [x] Create strategy selector
- [x] Implement adaptive reader with Sequential/Parallel strategy selection
- [x] Add tests for strategy selection logic
- [x] Export AdaptiveMcapReader from mcap module

### Phase 4: Deprecation (Complete)
- [x] Remove two_pass.rs module (functionality covered by hybrid)
- [x] Remove two_pass_mcap_tests.rs
- [x] Update module exports

## Success Criteria

1. All LZ4 MCAP files readable ✅
2. Message indexes properly read and used
3. Performance maintained or improved
4. Maintenance burden reduced
5. Test coverage >80%

## Consequences

### Positive
- Complete LZ4 support (using proven lz4 crate)
- Complete message index support
- Reduced maintenance for core parsing
- Maximum performance for critical paths
- Future-proof (mcap crate updates)

### Negative
- Additional dependency complexity (mitigated: already depend on mcap)
- Strategy selection overhead (mitigated: negligible ~1-5µs)

## Alternatives Considered

1. **Full mcap crate**: Rejected due to performance regression in parallel scenarios
2. **Full custom**: Rejected due to high maintenance burden and incomplete features

## References

- Benchmark results: `benches/mcap_readers.rs`
- MCAP specification: https://github.com/foxglove/mcap
- mcap crate source: `~/.cargo/registry/src/.../mcap-0.24.0/`
