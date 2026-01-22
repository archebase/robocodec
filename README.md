# Robocodec

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

**Robocodec** is a high-performance robotics data format library for reading and writing MCAP and ROS bag files. It provides a format-centric architecture with parallel processing capabilities, efficient memory management, and support for multiple message encodings.

## Features

- **Multi-Format Support**: Read and write MCAP and ROS1 bag files
- **Message Codecs**: CDR (ROS1/ROS2), Protobuf, and JSON encoding/decoding
- **Schema Parsing**: Parse ROS `.msg` files, ROS2 IDL, and OMG IDL formats
- **High-Performance I/O**: Parallel and sequential reading strategies with memory mapping
- **Format-Centric Architecture**: Each format has its own module with readers, writers, and high-level APIs
- **Data Transformation**: Built-in support for topic renaming, type normalization, and format conversion
- **Memory Efficient**: Arena allocation and zero-copy operations
- **Python Bindings**: Full-featured Python API via PyO3 (optional feature)

## Installation

### Prerequisites

- Rust 1.70 or later
- Python 3.11+ (for Python bindings)
- maturin (for building Python package)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/archebase/robocodec.git
cd robocodec

# Build the library
cargo build --release

# Run tests
cargo test

# Build Python package (optional)
pip install maturin
maturin develop
```

### Using as Rust Dependency

Add the following to your `Cargo.toml`:

```toml
[dependencies]
robocodec = "0.1"
```

Enable optional features as needed:

```toml
robocodec = { version = "0.1", features = ["python", "jemalloc"] }
```

### Optional Features

| Feature | Description |
|---------|-------------|
| `python` | Python bindings via PyO3 |
| `jemalloc` | Use jemalloc allocator (Linux only) |

## Quick Start

### Reading MCAP Files

```rust
use robocodec::io::formats::mcap::reader::McapReader;

let reader = McapReader::open("data.mcap")?;
for result in reader.decode_messages()? {
    let (decoded, channel) = result?;
    println!("Topic: {}, Fields: {:?}", channel.topic, decoded);
}
```

### Writing MCAP Files

```rust
use robocodec::io::formats::mcap::writer::ParallelMcapWriter;

let writer = ParallelMcapWriter::create("output.mcap")?;
writer.add_channel(...)?;
writer.write_chunk(...)?;
writer.finish()?;
```

### Rewriting with Auto-Detection

```rust
use robocodec::rewriter::RoboRewriter;

// Format auto-detected from extension
let mut rewriter = RoboRewriter::open("input.mcap")?;
rewriter.rewrite("output.mcap")?;
```

### Python API

```python
from robocodec import McapReader

# Read MCAP file
reader = McapReader("data.mcap")
for message, channel in reader:
    print(f"Topic: {channel.topic}, Data: {message}")
```

## Architecture

Robocodec is organized as a **format-centric** library, where each robotics data format has its own module containing all related functionality (readers, writers, high-level APIs).

### Layered Architecture

```
┌─────────────────────────────────────────────┐
│  User Layer (lib.rs re-exports)              │
│  - McapReader, BagWriter, etc.              │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  High-Level API Layer                       │
│  - io/formats/mcap/reader.rs (auto-decode)  │
│  - io/formats/mcap/writer.rs (custom)       │
│  - io/formats/bag/writer.rs (high-level)    │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Low-Level I/O Layer                        │
│  - io/formats/mcap/parallel.rs, reader.rs   │
│  - io/formats/bag/parallel.rs, reader.rs    │
│  - io/ (unified traits)                      │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│  Foundation Layer                           │
│  - core/ (errors, types)                    │
│  - encoding/ (codecs)                        │
│  - schema/ (parsing)                         │
└─────────────────────────────────────────────┘
```

### Key Principles

1. **Format-Centric Organization**: Each format (MCAP, ROS1 bag) has its own module containing low-level I/O operations, format-specific readers and writers, and high-level convenience APIs.

2. **Layered Architecture**: Clear separation between user-facing APIs, high-level operations, low-level I/O, and foundation layers.

3. **Unified Rewriter Interface**: Auto-detects format from file extension and delegates to format-specific rewriters.

For more details, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Supported Formats

| Format | Read | Write | Notes |
|--------|------|-------|-------|
| MCAP | ✅ | ✅ | Common data format optimized for appending |
| ROS1 Bag | ✅ | ✅ | ROS1 rosbag format |
| CDR | ✅ | ✅ | Common Data Representation (ROS1/ROS2) |
| Protobuf | ✅ | ✅ | Protocol Buffers |
| JSON | ✅ | ✅ | JSON serialization |

## Schema Support

- ROS `.msg` files (ROS1)
- ROS2 IDL (Interface Definition Language)
- OMG IDL (Object Management Group)

## Development

### Building

```bash
# Build library
cargo build --release

# Run tests
cargo test

# Run with specific features
cargo build --features python
```

### Running Examples

```bash
# Read MCAP file
cargo run --example read_mcap -- data.mcap

# Convert between formats
cargo run --example convert -- input.bag output.mcap
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
