# Robocodec

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

**Robocodec** is a high-performance robotics data format library for reading and writing MCAP and ROS bag files. It provides a format-centric architecture with parallel processing capabilities, efficient memory management, and support for multiple message encodings.

## Features

- **Unified API**: Single `RoboReader`/`RoboWriter` interface for all formats with auto-detection
- **Multi-Format Support**: Read and write MCAP and ROS1 bag files
- **Auto-Strategy Selection**: Uses parallel processing when available, falls back to sequential
- **Message Codecs**: CDR (ROS1/ROS2), Protobuf, and JSON encoding/decoding
- **Schema Parsing**: Parse ROS `.msg` files, ROS2 IDL, and OMG IDL formats
- **Data Transformation**: Built-in support for topic renaming, type renaming, and wildcard-based transformations
- **Memory Efficient**: Arena allocation and zero-copy operations via memory-mapped files

## Installation

### Prerequisites

- Rust 1.70 or later

### Building from Source

```bash
# Clone the repository
git clone https://github.com/archebase/robocodec.git
cd robocodec

# Build the library
cargo build --release

# Run tests
cargo test
```

### Using as Rust Dependency

Add the following to your `Cargo.toml`:

```toml
[dependencies]
robocodec = "0.1"
```

Enable optional features as needed:

```toml
robocodec = { version = "0.1", features = ["jemalloc"] }
```

### Optional Features

| Feature | Description |
|---------|-------------|
| `python` | Python bindings via PyO3 |
| `jemalloc` | Use jemalloc allocator (Linux only) |

## Quick Start

### Reading Files (Auto-Detect Format)

```rust
use robocodec::{FormatReader, RoboReader};

// Format auto-detected, parallel mode used when available
let reader = RoboReader::open("data.mcap")?;
println!("Channels: {}", reader.channels().len());
```

### Writing Files (Auto-Detect Format)

```rust
use robocodec::{FormatWriter, RoboWriter};

// Format detected from extension (.mcap or .bag)
let mut writer = RoboWriter::create("output.mcap")?;
let channel_id = writer.add_channel("/topic", "type", "cdr", None)?;
writer.finish()?;
```

### Rewriting with Transformations

```rust
use robocodec::{RoboRewriter, TransformBuilder};

let mut rewriter = RoboRewriter::open("input.mcap")?;

// Optional: Add transformations
let transform = TransformBuilder::new()
    .with_topic_rename("/old/topic", "/new/topic")
    .with_type_rename("OldType", "NewType")
    .build();
rewriter.set_transform(transform);

rewriter.rewrite("output.mcap")?;
```

### Python API

```python
from robocodec import RoboReader, RoboWriter, RoboRewriter

# Reading
reader = RoboReader("data.mcap")
print(f"Channels: {len(reader.channels)}")

# Writing
writer = RoboWriter("output.bag")
channel_id = writer.add_channel("/topic", "type", "cdr")
writer.finish()

# Rewriting with transforms
rewriter = RoboRewriter("input.mcap")
rewriter.rewrite("output.mcap")
```

## Architecture

Robocodec provides a **unified API** through `RoboReader` and `RoboWriter`, with format-specific implementations organized internally.

### Layered Architecture

```
┌─────────────────────────────────────────────┐
│  User Layer (lib.rs)                        │
│  - RoboReader, RoboWriter, RoboRewriter     │
│  - TransformBuilder, FormatReader/Writer    │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Unified I/O Layer                          │
│  - io/reader/mod.rs (auto-strategy)         │
│  - io/writer/mod.rs (auto-strategy)         │
│  - io/traits.rs (FormatReader/Writer)       │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Format-Specific Layer                      │
│  - io/formats/mcap/ (parallel/sequential)   │
│  - io/formats/bag/ (parallel/sequential)    │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Foundation Layer                           │
│  - core/ (errors, types)                    │
│  - encoding/ (CDR, Protobuf, JSON)          │
│  - schema/ (msg, IDL parsers)               │
│  - transform/ (topic/type renaming)         │
│  - types/ (arena, chunk, buffer pool)       │
└─────────────────────────────────────────────┘
```

### Key Principles

1. **Unified User API**: Single `RoboReader`/`RoboWriter` interface for all formats with auto-detection
2. **Auto-Strategy Selection**: Parallel mode used when available, automatic fallback to sequential
3. **Format-Centric Internals**: Each format has its own module with optimized implementations

For more details, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Supported Formats

| Format | Read | Write | Notes |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | Common data format optimized for appending |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag format with v2 support |

## Message Encodings

| Encoding | Read | Write | Notes |
|----------|------|-------|-------|
| CDR | ✅ | ✅ | Common Data Representation (ROS1/ROS2) |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON encoding |

## Schema Support

| Format | Status |
|--------|--------|
| ROS `.msg` files | ✅ |
| ROS2 IDL | ✅ |
| OMG IDL | ✅ |

## Transformations

The `transform` module provides flexible data transformation capabilities:

- **Topic Rename**: Map individual topics or use wildcard patterns
- **Type Rename**: Rename message types with wildcard support
- **Combined Transform**: Rename topics and types together

```rust
use robocodec::TransformBuilder;

let transform = TransformBuilder::new()
    .with_topic_rename("/camera/front", "/sensors/camera_front")
    .with_topic_rename_wildcard("/old/*", "/new/*")
    .with_type_rename("geometry_msgs/Point", "my_pkg/Point3D")
    .with_type_rename_wildcard("std_msgs/*", "my_msgs/*")
    .build();
```

## Python Bindings

Python bindings are available via PyO3:

```bash
# Build Python package
cargo build --release --features python

# Install in development mode
make build-python-dev
```

### Python API

```python
from robocodec import RoboReader, RoboWriter, RoboRewriter, TransformBuilder

# Reading
reader = RoboReader("data.mcap")
for channel in reader.channels:
    print(f"{channel.topic}: {channel.message_type}")

# Writing
writer = RoboWriter("output.mcap")
ch_id = writer.add_channel("/imu", "sensor_msgs/Imu", "cdr")

# Rewriting with transforms
transform = TransformBuilder()
    .with_topic_rename("/old/topic", "/new/topic")
    .build()

rewriter = RoboRewriter("input.bag")
rewriter.with_transforms(transform)
stats = rewriter.rewrite("output.mcap")
print(f"Processed {stats.message_count} messages")
```

## Development

### Building

```bash
# Build library
cargo build --release

# Run tests
cargo test

# Format code
make fmt

# Run linter
make lint

# Run all checks
make check
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MulanPSL v2 - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [Roboflow](https://github.com/archebase/roboflow) - High-level pipeline and conversion tool built on robocodec
- [MCAP](https://mcap.dev/) - Common data format optimized for appending in robotics community
- [ROS](https://www.ros.org/) - Robot Operating System

## Documentation

- [Architecture](ARCHITECTURE.md) - High-level system design

## Links

- [Issue Tracker](https://github.com/archebase/robocodec/issues)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)
