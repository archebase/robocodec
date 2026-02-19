# Robocodec Architecture

## Overview

**Robocodec** is a high-performance robotics data format library that provides unified read/write access to multiple robotics data formats (MCAP, ROS1 bag, RRF2). Built in Rust, it combines zero-copy memory-mapped I/O with parallel processing for maximum performance.

## Key Features

| Feature | Benefit |
|---------|---------|
| **Unified API** | Single `RoboReader`/`RoboWriter` interface for all formats |
| **Auto-Detection** | Format detected from file extension automatically |
| **Zero-Copy I/O** | Memory-mapped files for fast reading |
| **Parallel Processing** | Multi-threaded decoding with rayon |
| **Remote Storage** | Native S3 and HTTP/HTTPS support |
| **Transformations** | Topic/type renaming during rewrite |
| **Cross-Language** | Rust library + Python bindings |

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                     User Applications                           │
│  (Rust apps, Python scripts, CLI tool, C bindings - future)     │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    Public API Layer                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  RoboReader  │  RoboWriter  │  RoboRewriter             │   │
│  │  - open()    │  - create()  │  - open()                 │   │
│  │  - decoded() │  - write()   │  - rewrite()              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                     Format auto-detection                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                   Unified I/O Layer                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │           FormatReader / FormatWriter Traits             │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Message Filtering  │  Channel Management  │  Metadata  │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                Format-Specific Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │    MCAP      │  │  ROS1 Bag    │  │    RRF2      │          │
│  │  - Reader    │  │  - Reader    │  │  - Reader    │          │
│  │  - Writer    │  │  - Writer    │  │  - Writer    │          │
│  │  - Streaming │  │  - Streaming │  │  - Streaming │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│              Transport & Streaming Layer                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Transport Trait (unified byte I/O interface)           │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  Local   │  │    S3    │  │   HTTP   │  │  Memory  │        │
│  │  (mmap)  │  │ (remote) │  │ (remote)│  │  (test)  │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│               Foundation Layer                                  │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐    │
│  │   Encoding     │  │    Schema      │  │     Core       │    │
│  │  - CDR         │  │  - ROS .msg    │  │  - Error       │    │
│  │  - Protobuf    │  │  - ROS2 IDL    │  │  - Result      │    │
│  │  - JSON        │  │  - OMG IDL     │  │  - Arena       │    │
│  └────────────────┘  └────────────────┘  └────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Reading Pipeline

```
File/URL → Transport → StreamingParser → FormatReader → Decoder → User
            ↓              ↓                ↓              ↓
        mmap/S3       chunk-based      parallel       CDR/Proto
        HTTP           parsing         rayon          JSON
```

### Writing Pipeline

```
User → Encoder → FormatWriter → Transport → File/URL
       ↓            ↓               ↓
    CDR/Proto    parallel       S3 multipart
    JSON         rayon          HTTP PUT
```

## Format Support Matrix

| Format | Extension | Read | Write | Streaming | Parallel |
|--------|-----------|------|-------|-----------|----------|
| MCAP   | .mcap     | ✅   | ✅    | ✅        | ✅      |
| ROS1   | .bag      | ✅   | ✅    | ✅        | ✅      |
| RRF2   | .rrd      | ✅   | ✅    | ✅        | ✅      |

## Encoding Support

| Encoding | Description | Used By |
|----------|-------------|---------|
| CDR      | Common Data Representation | ROS1, ROS2 |
| Protobuf | Protocol Buffers | MCAP, RRF2 |
| JSON     | JavaScript Object Notation | Config messages |

## Schema Support

| Schema Type | File Extensions | Description |
|-------------|-----------------|-------------|
| ROS .msg    | .msg            | ROS1 message definitions |
| ROS2 IDL    | .idl            | ROS2 interface definitions |
| OMG IDL     | .idl            | CORBA interface definitions |

## Remote Storage Support

### S3-Compatible Storage

```bash
# AWS S3
s3://bucket/path/file.mcap

# MinIO (custom endpoint)
s3://bucket/path/file.mcap?endpoint=http://localhost:9000

# Alibaba Cloud OSS
s3://bucket/path/file.mcap?endpoint=https://oss-cn-hangzhou.aliyuncs.com
```

### HTTP/HTTPS

```bash
# Direct HTTP access
https://example.com/data/file.mcap

# With bearer token
https://example.com/data/file.mcap?bearer_token=xxx
```

## Performance Characteristics

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Sequential Read | ~500 MB/s | Memory-mapped I/O |
| Parallel Decode | ~2 GB/s | 8-core rayon |
| S3 Streaming | ~100 MB/s | Network limited |
| MCAP → Bag | ~1.5 GB/s | Format conversion |

## Library vs CLI

### Library (Rust/Python)

For programmatic access:

```rust
// Rust
use robocodec::RoboReader;

let reader = RoboReader::open("data.mcap")?;
for msg in reader.decoded()? {
    println!("{:?}", msg?);
}
```

```python
# Python
from robocodec import RoboReader

reader = RoboReader("data.mcap")
for msg in reader.decoded():
    print(msg)
```

### CLI Tool

For one-off operations and scripting:

```bash
# Inspect file
robocodec inspect info data.mcap

# Extract topics
robocodec extract topics data.mcap output.mcap /camera,/lidar

# Search
robocodec search topics data.bag sensor
```

## Design Principles

1. **Format-Centric**: Each format lives in its own module
2. **Unified API**: Single interface for all formats
3. **Zero-Copy**: Memory-mapped files when possible
4. **Parallel First**: Multi-threaded by default
5. **Transport Agnostic**: Same parser works for local and remote

## Directory Structure

```
src/
├── lib.rs              # Public API exports
├── core/               # Error types, result aliases
├── encoding/           # Message codecs (CDR, Protobuf, JSON)
├── schema/             # Schema parsers (ROS .msg, ROS2 IDL, OMG IDL)
├── io/
│   ├── reader/         # Unified reader with strategy selection
│   ├── writer/         # Unified writer with strategy selection
│   ├── traits.rs       # FormatReader, FormatWriter traits
│   ├── metadata.rs     # Unified types (ChannelInfo, FileInfo)
│   ├── detection.rs    # Format detection from file extension
│   ├── streaming/      # Streaming parser interface
│   ├── transport/      # Transport layer (Local, S3, HTTP)
│   └── formats/
│       ├── mcap/       # MCAP format implementation
│       ├── bag/        # ROS1 bag format implementation
│       └── rrd/        # RRF2 format implementation
├── transform/          # Topic/type transformation support
└── rewriter/           # Unified rewriter with format auto-detection
```

## Related Documentation

- [CLAUDE.md](CLAUDE.md) - Project overview and build commands
- [README.md](README.md) - User-facing documentation and examples
- [examples/](examples/) - Rust and Python code examples
- [demo/](demo/) - Presentation materials and demo scripts
