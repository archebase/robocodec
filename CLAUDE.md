# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

Robocodec is a **common reader and writer library** for robotics data formats (MCAP, ROS1 bag). It provides a unified, format-agnostic API that other projects can depend on for reading and writing robotics data files.

## Common Commands

```bash
# Build
make build              # Debug build
make build-release      # Release build
make build-python-dev   # Install Python package in dev mode

# Test
make test               # Run Rust tests
cargo test test_name    # Run specific test

# Code quality
make fmt                # Format code
make lint               # Run clippy (all features, denies warnings)
make check              # Format + lint
make check-license      # REUSE compliance check

# Coverage
make coverage           # Generate coverage reports (requires cargo-llvm-cov)
```

**Important**: Do NOT use `--all-features` or `--features python` when running tests. PyO3 conflicts with Rust test harness in some configurations.

## Architecture

Robocodec is a **format-centric** robotics data codec library with a layered architecture:

```
┌─────────────────────────────────────────────┐
│  Public API Layer (lib.rs re-exports)       │
│  - RoboReader, RoboWriter, RoboRewriter      │
│  - DecodedMessageIter, DecodedMessageResult   │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Unified I/O Layer                          │
│  - io/reader/mod.rs (RoboReader, iterators) │
│  - io/writer/mod.rs (RoboWriter)            │
│  - io/traits.rs (FormatReader, FormatWriter) │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Format-Specific Layer                      │
│  - io/formats/mcap/ (MCAP read/write)       │
│  - io/formats/bag/ (ROS1 bag read/write)    │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Foundation Layer                           │
│  - core/ (CodecError, Result)               │
│  - encoding/ (CDR, Protobuf, JSON)          │
│  - schema/ (msg, IDL parsing)               │
│  - io/metadata.rs (unified types)           │
└─────────────────────────────────────────────┘
```

### Key Design Principles

1. **Format-Centric**: Each format (MCAP, ROS1 bag) lives in `src/io/formats/{format}/` with its own readers and writers.

2. **Unified Public API**: High-level `RoboReader`, `RoboWriter` provide a consistent interface across formats. Downcasting to format-specific types is intentionally **not** part of the public API.

3. **Simplified Iteration**: Single-level iteration via `reader.decoded()` returns `DecodedMessageIter` directly. No need to call `.stream()` separately.

4. **Unified Result Types**: `DecodedMessageResult` combines message data, channel info, and timestamps in a single type.

5. **Auto-Detection**: Format is detected from file extension automatically.

### Directory Structure

- `src/io/reader/` - Unified reader API (RoboReader, iterators, config)
- `src/io/writer/` - Unified writer API (RoboWriter, config)
- `src/io/formats/mcap/` - MCAP format (read/write)
- `src/io/formats/bag/` - ROS1 bag format (read/write)
- `src/io/metadata.rs` - Unified types (ChannelInfo, RawMessage, DecodedMessageResult)
- `src/io/traits.rs` - FormatReader, FormatWriter traits
- `src/encoding/` - Message codecs (CDR, Protobuf, JSON)
- `src/schema/` - Schema parsers (ROS .msg, ROS2 IDL, OMG IDL)
- `src/rewriter/` - Format conversion with auto-detection
- `src/transform/` - Topic/type transformations
- `src/types/` - Arena allocation, chunk management
- `tests/` - Integration tests with fixtures in `tests/fixtures/`

### Public API Design

The library exports these key types at the top level:

- **`RoboReader`** - Unified reader with format auto-detection
  - `open(path)` - Open file with auto-detection
  - `open_with_config(path, config)` - Open with configuration
  - `decoded()` - Iterate over decoded messages with timestamps (returns `DecodedMessageIter`)
  - `supports_parallel()` - Check if parallel reading is available
  - `chunk_count()` - Get number of chunks for progress tracking

- **`RoboWriter`** - Unified writer with format auto-detection
  - `create(path)` - Create writer based on extension
  - `create_with_config(path, config)` - Create with configuration
  - Inherits `FormatWriter` trait methods (add_channel, write, finish)

- **`DecodedMessageIter`** - Iterator yielding `DecodedMessageResult`

- **`DecodedMessageResult`** - Combined message + metadata
  - `message` - Decoded message fields
  - `channel` - Channel information
  - `log_time`, `publish_time` - Timestamps
  - `sequence` - Sequence number (if available)

### What Does NOT Belong in the Library

As a common library for other projects to use, these do NOT belong:

1. **CLI tools** - Should be in a separate `robocodec-cli` crate
2. **CLI dependencies** - `clap`, `indicatif`, `human-size` should be feature-gated or moved
3. **Development examples** - Files with hardcoded paths in `examples/`
4. **Internal type exposure** - Downcasting methods expose implementation details

## Code Style

- **Naming**: Modules `snake_case`, types `PascalCase`, functions `snake_case`
- **Errors**: Use `CodecError` and `Result<T>` type alias from `src/core/error.rs`
- **Public API**: All public items require rustdoc comments
- **License**: All source files must include SPDX license headers

## Features

- `python` - PyO3 Python bindings
- `jemalloc` - Use jemalloc allocator (Linux only)
