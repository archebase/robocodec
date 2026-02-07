# Robocodec Architecture

This document describes the architectural organization of the robocodec crate.

## Overview

Robocodec is organized as a **format-centric** library, where each robotics data format has its own module containing all related functionality (readers, writers, high-level APIs). A unified API layer (`RoboReader`, `RoboWriter`, `RoboRewriter`) provides format-agnostic operations with automatic format detection.

## Key Principles

### 1. Unified User API with Format-Centric Internals

The library provides a single, format-agnostic API at the top level while organizing format-specific implementations in dedicated modules:

**User API (lib.rs)**:
```rust
use robocodec::{RoboReader, RoboWriter, RoboRewriter};

// Format auto-detected from file extension
let reader = RoboReader::open("data.mcap")?;
let writer = RoboWriter::create("output.bag")?;
let rewriter = RoboRewriter::open("input.mcap")?;
```

**Format-Specific APIs (when needed)**:
```rust
use robocodec::io::formats::mcap::reader::McapReader;
use robocodec::io::formats::bag::reader::ParallelBagReader;
use robocodec::io::formats::rrd::reader::RrdReader;
```

### 2. Layered Architecture

```
┌─────────────────────────────────────────────────────────┐
│  User Layer (lib.rs re-exports)                         │
│  - RoboReader, RoboWriter, RoboRewriter                │
│  - FormatReader, FormatWriter traits                   │
│  - TransformBuilder, Transform types                   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Unified I/O Layer                                      │
│  - io/reader/mod.rs (auto-strategy selection)          │
│  - io/writer/mod.rs (auto-strategy selection)          │
│  - io/traits.rs (FormatReader, FormatWriter)           │
│  - io/metadata.rs (FileFormat, ChannelInfo, etc.)      │
│  - io/detection.rs (format detection from extension)   │
│  - io/streaming/ (streaming parser interface)          │
│  - io/filter.rs (message filtering)                     │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Format-Specific Layer                                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/formats/mcap/                                │   │
│  │   - reader.rs (McapReader with auto-decode)     │   │
│  │   - writer.rs (ParallelMcapWriter)              │   │
│  │   - parallel.rs (low-level parallel reader)     │   │
│  │   - sequential.rs (low-level sequential reader) │   │
│  │   - two_pass.rs (two-pass reader strategy)      │   │
│  │   - streaming.rs (streaming MCAP parser)        │   │
│  │   - transport_reader.rs (transport-based)       │   │
│  │   - s3_adapter.rs (S3 streaming adapter)        │   │
│  │   - constants.rs (MCAP format constants)         │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/formats/bag/ (ROS1 bag)                      │   │
│  │   - reader.rs (ParallelBagReader)               │   │
│  │   - writer.rs (BagWriter)                       │   │
│  │   - parallel.rs (low-level parallel reader)     │   │
│  │   - sequential.rs (low-level sequential reader) │   │
│  │   - stream.rs (streaming BAG parser)            │   │
│  │   - parser.rs (Bag format parsing)              │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/formats/rrd/ (Rerun RRD)                     │   │
│  │   - reader.rs (RrdReader)                       │   │
│  │   - writer.rs (RrdWriter)                       │   │
│  │   - parallel.rs (parallel reader)                │   │
│  │   - stream.rs (streaming RRD parser)             │   │
│  │   - arrow_msg.rs (Arrow protobuf encoding)       │   │
│  │   - constants.rs (RRD format constants)          │   │
│  └─────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Transport Layer (requires `remote` feature)            │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/transport/core.rs (Transport trait)          │   │
│  │ io/transport/local.rs (local file transport)     │   │
│  │ io/transport/http/ (HTTP/HTTPS transport)         │   │
│  │ io/transport/s3/ (S3 transport)                  │   │
│  │ io/transport/memory/ (in-memory for testing)     │   │
│  └─────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Remote Storage Layer (requires `remote` feature)       │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/s3/ (S3 client and streaming)                  │   │
│  │   - client.rs (AWS S3 HTTP client with SigV4)    │   │
│  │   - reader.rs (S3Reader for streaming)           │   │
│  │   - writer.rs (S3Writer with multipart upload)    │   │
│  │   - location.rs (S3 URL parsing)                 │   │
│  │   - config.rs (S3 configuration)                  │   │
│  │   - signer.rs (AWS request signing)               │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 3. Remote Storage Architecture

The `remote` feature (previously named `s3`) provides support for reading and writing robotics data files from remote storage sources:

**Supported Protocols**:
- `s3://` - AWS S3 and S3-compatible services (MinIO, R2, etc.)
- `http://` and `https://` - Generic HTTP/HTTPS with authentication

**Transport Abstraction**:
```rust
// Unified transport trait works with any data source
use robocodec::io::transport::{Transport, TransportExt};

// Local file (always available)
let transport = LocalTransport::open("data.mcap")?;

// HTTP/HTTPS (requires `remote` feature)
let transport = HttpTransport::new("https://example.com/data.mcap").await?;

// S3 (requires `remote` feature)
let transport = S3Transport::new(client, location).await?;
```

**Reader Usage**:
```rust
// Auto-detects URL scheme and creates appropriate transport
let reader = RoboReader::open("s3://my-bucket/data.mcap")?;
let reader = RoboReader::open("https://example.com/data.bag")?;
```

**Writer Usage**:
```rust
// S3 multipart upload
let writer = RoboWriter::create("s3://my-bucket/output.mcap")?;

// HTTP PUT upload
let writer = RoboWriter::create("https://example.com/output.bag")?;
```

### 4. Rewriter Architecture

The rewriter module provides a unified facade that:

1. **Auto-detects format** from file extension (`.mcap` → MCAP, `.bag` → ROS1 bag)
2. **Delegates to format-specific rewriters** via the `FormatRewriter` trait
3. **Shares common transformation logic** via the rewrite engine

```
User code
  │
  ├─ RoboRewriter::open("data.mcap")
  │       │
  │       ├─ detect_format() → FileFormat::Mcap
  │       │
  │       └─ creates McapRewriter (internal)
  │
  └─ RoboRewriter::open("data.bag")
          │
          ├─ detect_format() → FileFormat::Bag
          │
          └─ creates BagRewriter (internal)
```

**Rewriter Components**:

- `facade.rs` - `RoboRewriter` enum with format detection
- `engine.rs` - Shared rewrite engine with transformation support
- `mcap/` - MCAP-specific rewriter implementation
- `bag/` - ROS1 bag-specific rewriter implementation

### 5. Auto-Strategy Selection

Readers and writers automatically choose the optimal strategy:

- **Parallel mode**: Used when the format supports chunked reading/writing
- **Sequential mode**: Fallback for non-chunked or small files
- **Two-pass mode**: MCAP-specific strategy for certain access patterns

## Design Decisions

### Why Unified User API?

**Problem**: Users want to work with robotics data files without worrying about format details.

**Solution**: Provide `RoboReader`/`RoboWriter` that:
- Auto-detect format from file extension
- Use optimal strategy automatically
- Provide consistent interface across formats

```rust
// Works for MCAP, ROS1 bag, and RRD
let reader = RoboReader::open(path)?;
let channels = reader.channels();
```

### Why Format-Centric Internals?

**Problem**: Each format has unique characteristics (chunking, indexing, compression).

**Solution**: Organize by format under `io/formats/`:
```rust
// Clear: Everything MCAP-related is in one place
use robocodec::io::formats::mcap::{reader::McapReader, writer::ParallelMcapWriter};
use robocodec::io::formats::bag::{reader::ParallelBagReader, writer::BagWriter};
use robocodec::io::formats::rrd::{reader::RrdReader, writer::RrdWriter};

// For most users, just use the unified API
use robocodec::{RoboReader, RoboWriter};
```

**Benefits**:
- Easy to locate format-specific code
- Simple to add new formats (create a new directory)
- Clear ownership boundaries
- Format-specific optimizations isolated

### Why Transport Abstraction?

**Problem**: Need to support multiple data sources (local files, S3, HTTP) without duplicating parser logic.

**Solution**: Introduce `Transport` trait that abstracts byte I/O:
- `LocalTransport` - Memory-mapped files (always available)
- `HttpTransport` - HTTP/HTTPS with range requests (`remote` feature)
- `S3Transport` - S3 protocol with SigV4 signing (`remote` feature)
- `MemoryTransport` - In-memory for testing (`remote` feature)

**Benefits**:
- Format parsers work with any data source
- No code duplication between local and remote reading
- Easy to add new transports (GCS, Azure Blob, etc.)

### Transformation Architecture

The `transform` module provides flexible data transformation:

- `topic_rename.rs` - Topic renaming with wildcards
- `type_rename.rs` - Type renaming with wildcards
- `pipeline.rs` - Multi-transform orchestration
- `normalization.rs` - Type normalization rules

Transformations are applied during rewriting via the `McapTransform` trait.

## Feature Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `default` | Enables remote storage support | All remote dependencies |
| `remote` | S3 and HTTP/HTTPS support | `reqwest`, `tokio`, `http`, `aws-config`, etc. |
| `python` | Python bindings via PyO3 | `pyo3` |
| `jemalloc` | Use jemalloc allocator (Linux) | `tikv-jemallocator` |
| `cli` | CLI tool support | `clap`, `indicatif`, `human-size` |

## Usage Examples

### Reading with Auto-Detection

```rust
use robocodec::{FormatReader, RoboReader};

// Local file
let reader = RoboReader::open("file.mcap")?;
println!("Channels: {}", reader.channels().len());
println!("Messages: {}", reader.message_count());

// S3 file (requires --features remote)
let reader = RoboReader::open("s3://bucket/file.mcap")?;

// HTTP file (requires --features remote)
let reader = RoboReader::open("https://example.com/file.bag")?;
```

### Reading with HTTP Authentication

```rust
use robocodec::{RoboReader, ReaderConfig};

// Bearer token authentication
let config = ReaderConfig::default()
    .with_http_bearer_token("your-token");
let reader = RoboReader::open_with_config(
    "https://example.com/data.mcap",
    config
)?;

// Basic authentication
let config = ReaderConfig::default()
    .with_http_basic_auth("user", "pass");
let reader = RoboReader::open_with_config(
    "https://example.com/data.mcap",
    config
)?;

// URL query parameters
let reader = RoboReader::open(
    "https://example.com/data.mcap?bearer_token=your-token"
)?;
```

### Format-Specific Reading (when needed)

```rust
use robocodec::io::formats::mcap::reader::McapReader;

let reader = McapReader::open("file.mcap")?;
for result in reader.decode_messages()? {
    let (decoded, channel) = result?;
    println!("Topic: {}, Data: {:?}", channel.topic, decoded);
}
```

### Rewriting with Transformations

```rust
use robocodec::{RoboRewriter, TransformBuilder};

let transform = TransformBuilder::new()
    .with_topic_rename("/old/topic", "/new/topic")
    .with_type_rename("OldType", "NewType")
    .build();

let rewriter = RoboRewriter::with_options(
    "input.mcap",
    RewriteOptions::default().with_transforms(transform)
)?;
rewriter.rewrite("output.mcap")?;
```

### Writing to Remote Storage

```rust
use robocodec::RoboWriter;

// S3 with multipart upload
let writer = RoboWriter::create("s3://my-bucket/output.mcap")?;

// HTTP with PUT
let writer = RoboWriter::create("https://example.com/output.bag")?;
```

## Module Organization

### User-Facing Modules (lib.rs)

| Module | Purpose |
|--------|---------|
| `core` | Core error types and result aliases |
| `io` | Unified I/O traits and reader/writer facades |
| `encoding` | Message codecs (CDR, Protobuf, JSON) |
| `schema` | Schema parsers (ROS .msg, ROS2 IDL, OMG IDL) |
| `transform` | Topic/type transformation support |
| `rewriter` | Unified rewriter with format auto-detection |
| `python` | Python bindings (optional `python` feature) |
| `cli` | CLI tool (optional `cli` feature) |

### Internal I/O Structure

```
io/
├── mod.rs                   # Module exports, feature gates
├── reader/                  # Unified reader with strategy selection
│   ├── config.rs           # ReaderConfig, HttpAuthConfig
│   └── mod.rs              # RoboReader, URL parsing
├── writer/                  # Unified writer with strategy selection
│   ├── builder.rs          # WriterConfig builder
│   └── mod.rs              # RoboWriter, URL handling
├── traits.rs                # FormatReader, FormatWriter traits
├── metadata.rs              # FileFormat, ChannelInfo, FileInfo
├── detection.rs             # Format detection from file path
├── filter.rs                # Message filtering utilities
├── streaming/               # Streaming parser interface (remote feature)
│   ├── mod.rs              # Module exports
│   └── parser.rs           # StreamingParser trait
├── s3/                      # S3 client and streaming (remote feature)
│   ├── client.rs           # AWS S3 HTTP client with SigV4
│   ├── reader.rs           # S3Reader for streaming S3 data
│   ├── writer.rs           # S3Writer with multipart upload
│   ├── location.rs         # S3 URL parsing (s3://...)
│   ├── config.rs           # S3 configuration
│   ├── signer.rs           # AWS request signing
│   └── error.rs            # S3-specific errors
├── transport/               # Transport layer
│   ├── core.rs             # Transport trait definition
│   ├── local.rs            # Local file transport (mmap)
│   ├── http/               # HTTP/HTTPS transport (remote feature)
│   │   ├── transport.rs    # HttpTransport implementation
│   │   ├── writer.rs       # HttpWriter for PUT uploads
│   │   └── upload_strategy.rs
│   ├── s3/                 # S3 transport (remote feature)
│   │   ├── transport.rs    # S3Transport implementation
│   │   └── mod.rs          # Re-exports from io/s3
│   └── memory/             # In-memory transport for testing (remote)
└── formats/
    ├── mod.rs              # Format module exports
    ├── mcap/               # MCAP format implementation
    │   ├── reader.rs       # McapReader with auto-decoding
    │   ├── writer.rs       # ParallelMcapWriter
    │   ├── parallel.rs     # Low-level parallel reader
    │   ├── sequential.rs   # Low-level sequential reader
    │   ├── two_pass.rs     # Two-pass reader strategy
    │   ├── streaming.rs    # Streaming MCAP parser
    │   ├── transport_reader.rs  # Transport-based reader
    │   ├── s3_adapter.rs   # S3 streaming adapter
    │   └── constants.rs    # MCAP format constants
    ├── bag/                # ROS1 bag format implementation
    │   ├── reader.rs       # ParallelBagReader
    │   ├── writer.rs       # BagWriter
    │   ├── parallel.rs     # Low-level parallel reader
    │   ├── sequential.rs   # Low-level sequential reader
    │   ├── stream.rs       # Streaming BAG parser
    │   └── parser.rs       # Bag format parsing
    └── rrd/                # Rerun RRD format implementation
        ├── reader.rs       # RrdReader
        ├── writer.rs       # RrdWriter
        ├── parallel.rs     # Parallel reader
        ├── stream.rs       # Streaming RRD parser
        ├── arrow_msg.rs    # Arrow protobuf encoding
        └── constants.rs    # RRD format constants
```

## Related Documentation

- [CLAUDE.md](CLAUDE.md) - Project overview and build commands
- [README.md](README.md) - User-facing documentation and examples
- [Cargo.toml](Cargo.toml) - Feature flags and dependencies
