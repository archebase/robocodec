# Robocodec

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![codecov](https://codecov.io/gh/archebase/robocodec/branch/main/graph/badge.svg)](https://codecov.io/gh/archebase/robocodec)

**Robocodec** is a robotics data format library for reading, writing, and converting MCAP and ROS1 bag files. It provides a unified API with automatic format detection, parallel processing, and support for multiple message encodings (CDR, Protobuf, JSON) and schema types (ROS .msg, ROS2 IDL, OMG IDL).

## Why Robocodec?

- **Clean API** - Only `RoboReader`, `RoboWriter`, `RoboRewriter` exposed at top level
- **Auto-Detection** - Format detected from file extension or URL scheme
- **Fast** - Parallel processing with rayon, zero-copy memory-mapped files
- **S3-Native** - First-class support for `s3://` URLs (AWS S3, MinIO, Alibaba OSS, etc.)
- **Transformations** - Topic/type renaming and format conversion built-in

## Quick Start

### Rust

```toml
# Cargo.toml
[dependencies]
robocodec = "0.1"
```

```rust
use robocodec::RoboReader;

// Format auto-detected from extension
let reader = RoboReader::open("data.mcap")?;
println!("Found {} channels", reader.channels().len());
```

### Python (from source)

Python bindings are available but must be built from source:

```bash
git clone https://github.com/archebase/robocodec.git
cd robocodec
make build-python-dev
```

```python
from robocodec import RoboReader

reader = RoboReader("data.mcap")
print(f"Found {len(reader.channels)} channels")
```

> **Note:** PyPI release is coming soon. For now, build from source using the instructions above.

## Common Tasks

### Read messages from a file

```rust
use robocodec::RoboReader;

let reader = RoboReader::open("file.mcap")?;

// List all channels
for channel in reader.channels() {
    println!("{}: {} messages", channel.topic, channel.message_count);
}

// Get message count
println!("Total messages: {}", reader.message_count());
```

### Write messages to a file

```rust
use robocodec::RoboWriter;

let mut writer = RoboWriter::create("output.mcap")?;
let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;
// ... write messages ...
writer.finish()?;
```

### Read decoded messages

```rust
use robocodec::RoboReader;

let reader = RoboReader::open("file.mcap")?;

for result in reader.decoded()? {
    let msg = result?;
    println!("Topic: {}", msg.topic());
    println!("Data: {:?}", msg.message);
    println!("Log time: {:?}", msg.log_time);
}
```

### Read from S3

Robocodec supports reading directly from S3-compatible storage using `s3://` URLs:

```rust
use robocodec::RoboReader;

// Format and S3 access auto-detected
let reader = RoboReader::open("s3://my-bucket/path/to/data.mcap")?;
println!("Found {} channels", reader.channels().len());
```

**S3-compatible services** (AWS S3, Alibaba Cloud OSS, MinIO, etc.) require credentials via environment variables:

```bash
# AWS S3
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"  # optional, defaults to us-east-1

# For Alibaba Cloud OSS, MinIO, or other S3-compatible services
export AWS_ACCESS_KEY_ID="your-oss-access-key"
export AWS_SECRET_ACCESS_KEY="your-oss-secret-key"
```

> **Note:** While we use AWS-standard environment variable names for compatibility, robocodec works with any S3-compatible storage service.

### Write to S3

```rust
use robocodec::RoboWriter;

// Format detected from .mcap extension, S3 from s3:// URL
let mut writer = RoboWriter::create("s3://my-bucket/output.mcap")?;
let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;
// ... write messages ...
writer.finish()?;
}
```

### Custom S3 endpoints

For S3-compatible services with custom endpoints:

**Option 1: Environment variable** (global)
```bash
export S3_ENDPOINT="http://localhost:9000"  # MinIO
export S3_ENDPOINT="https://oss-cn-hangzhou.aliyuncs.com"  # Alibaba OSS
```

**Option 2: URL query parameter** (per-request)
```rust
use robocodec::RoboReader;

// MinIO running locally
let reader = RoboReader::open("s3://bucket/data.mcap?endpoint=http://localhost:9000")?;

// Alibaba Cloud OSS (Hangzhou region)
let reader = RoboReader::open(
    "s3://bucket/data.mcap?endpoint=https://oss-cn-hangzhou.aliyuncs.com"
)?;
```

### Rewrite files with transformations

The rewriter processes files in the same format, optionally applying topic and type transformations:

```rust
use robocodec::RoboRewriter;

let rewriter = RoboRewriter::open("input.mcap")?;
rewriter.rewrite("output.mcap")?;
```

**Note:** Cross-format conversion is not currently supported. Use the rewriter to transform data within the same format.

### Rename topics during conversion

```rust
use robocodec::{RoboRewriter, TransformBuilder};

let transform = TransformBuilder::new()
    .with_topic_rename("/old/topic", "/new/topic")
    .build();

let rewriter = RoboRewriter::with_options(
    "input.mcap",
    robocodec::RewriteOptions::default().with_transforms(transform)
)?;
rewriter.rewrite("output.mcap")?;
```

## Installation

### Rust Users

Add to `Cargo.toml`:

```toml
[dependencies]
robocodec = "0.1"
```

Optional features:

```toml
robocodec = { version = "0.1", features = ["jemalloc"] }
```

| Feature | Description | Default |
|---------|-------------|---------|
| `s3` | S3-compatible storage support (AWS S3, MinIO, etc.) | ✅ Yes |
| `python` | Python bindings | ❌ No |
| `jemalloc` | Use jemalloc allocator (Linux only) | ❌ No |

### Python Users

Build from source (PyPI release coming soon):

```bash
git clone https://github.com/archebase/robocodec.git
cd robocodec
make build-python-dev
```

## Supported Formats

| Format | Read | Write |
|:--------|:-----|:-------|
| MCAP | ✅ | ✅ |
| ROS1 Bag | ✅ | ✅ |
| RRF2 (Rerun) | ✅ | ✅ |

> **Note:** RRF2 support is compatible with Rerun **0.27+**. Earlier versions use a different format and are not supported.

## Message Encodings

| Encoding | Description |
|:---------|:------------|
| CDR | Common Data Representation (ROS1/ROS2) |
| Protobuf | Protocol Buffers |
| JSON | JSON encoding |

## Schema Support

- ROS `.msg` files (ROS1)
- ROS2 IDL (Interface Definition Language)
- OMG IDL (Object Management Group)

## License

MulanPSL v2 - see [LICENSE](LICENSE)

## Development

### Testing

```bash
make test              # Run all tests
make test-rust         # Run Rust tests only
make test-python       # Run Python tests only
```

### Fuzzing

Robocodec includes comprehensive fuzzing infrastructure for parser security and robustness testing:

```bash
./scripts/fuzz_init.sh  # Initialize fuzzing infrastructure (one-time setup)
make fuzz               # Quick fuzzing check (30s per target)
make fuzz-all           # Extended fuzzing (1min per target)
make fuzz-mcap          # Fuzz MCAP parser only
```

For detailed fuzzing documentation, see [docs/FUZZING.md](docs/FUZZING.md).

### Benchmarks

```bash
make bench              # Run performance benchmarks
make bench-compare      # Compare against baseline
```

## Links

- [Issue Tracker](https://github.com/archebase/robocodec/issues)
- [Security Policy](SECURITY.md)
- [Fuzzing Guide](docs/FUZZING.md)
