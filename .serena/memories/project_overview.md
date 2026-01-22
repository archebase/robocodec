# Robocodec Project Overview

## Purpose

Robocodec is a high-performance robotics data format library for reading and writing MCAP and ROS bag files. It provides a format-centric architecture with parallel processing capabilities, efficient memory management, and support for multiple message encodings.

## Tech Stack

- **Language**: Rust (edition 2021, requires 1.70+)
- **Primary Dependencies**:
  - `mcap` v0.24 - MCAP format support
  - `rosbag` v0.6 - ROS1 bag format support
  - `prost` v0.13 - Protobuf support
  - `serde` v1.0 - Serialization framework
  - `rayon` v1.10 - Parallel processing
  - `thiserror` v1.0 - Error handling
  - `pest` v2.7 - Parser for schema files
- **Optional Features**:
  - `python` - Python bindings via PyO3
  - `jemalloc` - jemalloc allocator (Linux only)

## Codebase Structure

The library follows a **format-centric architecture**:

```
src/
├── lib.rs              # Main entry point, re-exports
├── core/               # Core types, errors, Result
├── encoding/           # Codec implementations (CDR, Protobuf, JSON)
├── schema/             # Schema parsing (ROS .msg, IDL)
├── transform/          # Topic/type transformations
├── types/              # Arena, chunk, buffer pool types
├── io/                 # I/O infrastructure
│   └── formats/        # Format-specific implementations
│       ├── mcap/       # MCAP readers/writers
│       └── bag/        # ROS1 bag readers/writers
├── rewriter/           # Unified rewriter facade
└── python/             # Python bindings (PyO3)
```

## Supported Formats

| Format | Read | Write | Notes |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | Common robotics data format |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag format |
| CDR | ✅ | ✅ | Common Data Representation (ROS1/ROS2) |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON serialization |

## Key Features

- **Multi-Format Support**: Read and write MCAP and ROS1 bag files
- **Message Codecs**: CDR, Protobuf, and JSON encoding/decoding
- **Schema Parsing**: Parse ROS `.msg` files, ROS2 IDL, and OMG IDL
- **High-Performance I/O**: Parallel and sequential reading with memory mapping
- **Data Transformation**: Topic renaming, type normalization, format conversion
- **Memory Efficient**: Arena allocation and zero-copy operations
- **Python Bindings**: Full-featured Python API via PyO3

## Related Projects

- [Roboflow](https://github.com/archebase/roboflow) - High-level pipeline tool built on robocodec
- [MCAP](https://mcap.dev/) - Common data format for robotics
- [ROS](https://www.ros.org/) - Robot Operating System
