# ADR-004: Real S3 Streaming Reads with Minimal Public API

**Author**: ArcheBase Team  
**Date**: 2026-02-27  
**Status**: Accepted  

## Context

ADR-002 and ADR-003 added transport readers for BAG and RRD, bringing all formats onto `RoboReader::open("s3://...")`. This closed functional gaps, but current behavior is still not fully aligned with true incremental remote streaming.

Key gaps motivating this ADR:

- Transport readers currently read the entire object before parse completes.
- Retry configuration exists but is not enforced in request paths.
- Range response validation is weak (status/header/length checks are incomplete).

These gaps create correctness and resiliency risk for large remote objects and unstable networks, and they blur the API contract between public reader semantics and internal transport mechanics.

## Decision

Implement real S3 incremental reads behind the existing unified reader API, while freezing and minimizing the public surface.

Decision points:

- Keep the user-facing contract centered on `RoboReader`, unified decoded message types, and `ReaderConfig`.
- Enforce strict HTTP range semantics for S3 reads, including validation and retry behavior.
- Remove full-object preload behavior from transport reader paths; parsing must advance incrementally from fetched ranges/chunks.
- Preserve format-specific parser implementations internally, but unify streaming behavior at iterator level (`decoded()` and raw iteration) across MCAP/BAG/RRD.

## Phased Execution Plan

### Phase 0: API boundary freeze

- Goal: lock public API shape before internal refactor.
- Exit criteria:
  - Public API inventory documented (`RoboReader`, unified result/metadata types, `ReaderConfig`).
  - No new public transport- or S3-specific reader types exported.

### Phase 1: strict S3 range semantics + retries

- Goal: make network fetch semantics correct and deterministic.
- Exit criteria:
  - Range request paths validate HTTP status (`206` for ranged responses where applicable), `Content-Range`, and payload length consistency.
  - Retry policy from S3 config is actually applied in request execution paths.
  - Retry classification cleanly separates recoverable vs fatal errors.

### Phase 2: real incremental parsing (remove full-object preload)

- Goal: ensure remote reads are truly streaming.
- Exit criteria:
  - Transport readers no longer require loading full object before parse completion.
  - Parsing progresses in bounded-memory chunks and yields messages as data arrives.
  - End-of-stream and partial-chunk edge cases are covered by tests.

### Phase 3: unified iterator-level streaming via RoboReader

- Goal: standardize observable streaming behavior at the unified API.
- Exit criteria:
  - `RoboReader::decoded()` behaves consistently for local and S3 sources across MCAP/BAG/RRD.
  - Raw and decoded iterators share the same incremental consumption semantics.
  - Format dispatch in `RoboReader` remains unchanged from a caller perspective.

### Phase 4: local-vs-S3 parity correctness suite

- Goal: verify remote behavior matches local correctness.
- Exit criteria:
  - Fixture-driven tests compare local and S3/transport outputs for channels, message payloads, timestamps, and ordering.
  - Error path tests cover short reads, invalid range headers, and retriable transport failures.
  - Parity suite runs for MCAP, BAG, and RRD.

### Phase 5: performance hardening + CI guardrails

- Goal: prevent regressions in memory profile and throughput.
- Exit criteria:
  - Benchmarks capture latency/throughput for representative object sizes and network conditions.
  - CI gate tracks bounded-memory behavior and fails on major regression thresholds.
  - Retry/backoff behavior validated under fault-injection scenarios.

### Phase 6: docs finalization + API stabilization

- Goal: finalize contract and migration guidance.
- Exit criteria:
  - Rustdoc and architecture docs reflect real streaming semantics and internal/public boundaries.
  - ADR status reviewed for promotion from Proposed when all gates pass.
  - Release notes document behavior guarantees and non-goals.

## Public API Boundary (Minimal Surface)

Public (stable contract):

- `RoboReader` (`open`, `open_with_config`, iterator-facing methods).
- Unified types such as `DecodedMessageResult` and `ChannelInfo`.
- `ReaderConfig` (and builder) as the reader configuration surface.

Internal (not public contract):

- `Transport` trait and concrete transport types.
- S3 client implementations and authentication plumbing.
- Range fetch/retry internals (request policy, backoff, validation details).
- Format-specific remote readers (`*TransportReader`) and parser state machines.

This boundary preserves a small, format-agnostic API while allowing internal transport/parser evolution without downstream breakage.

## Consequences

Positive:

- Stronger correctness guarantees for remote reads.
- Better resiliency on transient network and object-store failures.
- Predictable memory behavior for large S3 objects.
- No public API expansion despite substantial internal improvements.

Trade-offs:

- Increased internal complexity in transport execution and parser coordination.
- More integration and fault-injection test maintenance.
- Potential short-term throughput variance while strict validation and retry logic are tuned.

## Testing and Performance Gates

- Correctness parity tests: local file vs S3 transport for MCAP/BAG/RRD outputs.
- Protocol validation tests: status code, `Content-Range`, and body-length invariants.
- Resilience tests: retry/backoff behavior across recoverable and fatal failure classes.
- Resource gates: bounded-memory checks and regression thresholds in CI.
- Compatibility checks: existing public `RoboReader` usage patterns compile and behave consistently.

## Rollout and Compatibility

- Rollout is internal-first and incremental by phase, with no new public entry points.
- Existing callers using `RoboReader::open("s3://...")` remain source-compatible.
- Behavior changes are semantic hardening (true streaming, stricter validation, retry enforcement), not API shape changes.
- If regressions appear in a format path, rollback is scoped to internal transport/reader strategy without public API breakage.

## Implementation Status (Current)

- [x] **Phase 0: API boundary freeze** - **Completed**
  - Public API surface remains centered on `RoboReader`, unified metadata/result types, and `ReaderConfig`; no new public S3 transport types were introduced.
- [x] **Phase 1: strict S3 range semantics + retries** - **Completed**
  - Strict S3 range validation and retry application are implemented in request paths.
- [x] **Phase 2: real incremental parsing (remove full-object preload)** - **Completed**
  - Transport reader paths no longer rely on full-object preload before parse completion, and incremental parsing behavior is validated across format paths.
- [x] **Phase 3: unified iterator-level streaming via RoboReader** - **Completed**
  - S3 raw and decoded iterator support is implemented with incremental, fail-fast behavior.
- [x] **Phase 4: local-vs-S3 parity correctness suite** - **Completed**
  - Fail-fast local-vs-S3 parity tests are in place for MCAP, BAG, and RRD via `RoboReader` public API.
- [x] **Phase 5: performance hardening + CI guardrails** - **Completed**
  - Fail-fast S3 performance guardrail tests enforce coarse latency/throughput thresholds in CI.
- [x] **Phase 6: docs finalization + API stabilization** - **Completed**
  - ADR status is promoted to `Accepted`, implementation status is finalized, and release notes capture guarantees and non-goals.

## Behavior Guarantees

- `RoboReader::open("s3://...")` resolves to the incremental S3 reader path and supports streaming consumption through `iter_raw()` and `decoded()`.
- S3 range handling enforces strict status/header/length validation with configured retry behavior on recoverable failures.
- CI includes fail-fast parity and performance guardrail gates for S3 paths to catch correctness and major regression issues early.
- The public API remains minimal and stable (`RoboReader`, unified metadata/result types, `ReaderConfig`) with no new public S3-specific reader surface.

## References

- Existing ADRs: `docs/adr-002-bag-s3-streaming.md`, `docs/adr-003-rrd-s3-streaming.md`
- Public API surface: `src/lib.rs`, `src/io/reader/mod.rs`, `src/io/reader/config.rs`, `src/io/metadata.rs`
- Current transport readers: `src/io/formats/mcap/transport_reader.rs`, `src/io/formats/bag/transport_reader.rs`, `src/io/formats/rrd/transport_reader.rs`
- Transport abstraction: `src/io/transport/core.rs`, `src/io/transport/s3/transport.rs`
- S3 request and retry internals: `src/io/s3/client.rs`, `src/io/s3/config.rs`, `src/io/s3/error.rs`
