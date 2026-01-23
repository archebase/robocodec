# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
│  User Layer (lib.rs re-exports)              │
│  - RoboReader, RoboWriter, RoboRewriter      │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  High-Level API Layer                       │
│  - io/formats/mcap/reader.rs (auto-decode)  │
│  - io/formats/mcap/writer.rs                │
│  - io/formats/bag/reader.rs, writer.rs      │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Low-Level I/O Layer                        │
│  - io/formats/mcap/parallel.rs              │
│  - io/formats/bag/parallel.rs               │
│  - io/ (unified FormatReader/FormatWriter)  │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Foundation Layer                           │
│  - core/ (CodecError, Result)               │
│  - encoding/ (CDR, Protobuf, JSON)          │
│  - schema/ (msg, IDL parsing)               │
└─────────────────────────────────────────────┘
```

### Key Design Principles

1. **Format-Centric**: Each format (MCAP, ROS1 bag) lives in `src/io/formats/{format}/` with its own readers, writers, and high-level APIs.

2. **Layered APIs**: High-level convenience APIs wrap low-level I/O. Use the high-level APIs for common operations, low-level for specialized needs.

3. **Unified Traits**: `FormatReader` and `FormatWriter` traits provide generic operations across formats.

4. **Auto-Detection**: `RoboRewriter` in `src/rewriter/` detects format from file extension and delegates to format-specific rewriters.

### Directory Structure

- `src/io/formats/mcap/` - MCAP format (read/write)
- `src/io/formats/bag/` - ROS1 bag format (read/write)
- `src/encoding/` - Message codecs (CDR, Protobuf, JSON)
- `src/schema/` - Schema parsers (ROS .msg, ROS2 IDL, OMG IDL)
- `src/rewriter/` - Format conversion with auto-detection
- `src/transform/` - Topic/type transformations
- `src/types/` - Arena allocation, chunk management
- `tests/` - Integration tests with fixtures in `tests/fixtures/`

## Code Style

- **Naming**: Modules `snake_case`, types `PascalCase`, functions `snake_case`
- **Errors**: Use `CodecError` and `Result<T>` type alias from `src/core/error.rs`
- **Public API**: All public items require rustdoc comments
- **License**: All source files must include SPDX license headers

## Features

- `python` - PyO3 Python bindings
- `jemalloc` - Use jemalloc allocator (Linux only)
