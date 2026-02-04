# Robocodec

[![License: MulanPSL-2.0](https://img.shields.io/badge/License-MulanPSL--2.0-blue.svg)](http://license.coscl.org.cn/MulanPSL2)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![codecov](https://codecov.io/gh/archebase/robocodec/branch/main/graph/badge.svg)](https://codecov.io/gh/archebase/robocodec)

**Robocodec** is a high-performance robotics data codec library for reading, writing, and converting MCAP and ROS1 bag files. It provides a unified API across formats with automatic format detection, parallel message processing, and support for multiple message encodings (CDR, Protobuf, JSON) and schema types (ROS .msg, ROS2 IDL, OMG IDL). Includes topic transformation capabilities and native Rust and Python APIs.

## Why Robocodec?

- **One API, Multiple Formats** - Same code works with MCAP and ROS1 bags
- **Auto-Detection** - Format detected from file extension, no manual configuration
- **Fast** - Parallel processing when available, zero-copy operations
- **Python & Rust** - Native performance in Rust, easy-to-use Python bindings

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

See [examples/python/README.md](examples/python/README.md) for more Python usage examples.

## Common Tasks

### Read messages from a file

```rust
use robocodec::RoboReader;

let reader = RoboReader::open("file.mcap")?;

// List all channels
for channel in reader.channels() {
    println!("{}: {} messages", channel.topic, channel.message_count);
}

// Get message count and time range
println!("Total messages: {}", reader.message_count());
```

```python
from robocodec import RoboReader

reader = RoboReader("file.mcap")

for channel in reader.channels:
    print(f"{channel.topic}: {channel.message_count} messages")

print(f"Total: {reader.message_count} messages")
```

### Write messages to a file

```rust
use robocodec::RoboWriter;

let mut writer = RoboWriter::create("output.mcap")?;
let channel_id = writer.add_channel("/topic", " MessageType", "cdr", None)?;
// ... write messages ...
writer.finish()?;
```

```python
from robocodec import RoboWriter

writer = RoboWriter("output.mcap")
channel_id = writer.add_channel("/topic", "MessageType", "cdr")
# ... write messages ...
writer.finish()
```

### Decode messages from a file

```rust
use robocodec::RoboReader;

// Works for both BAG and MCAP formats
let reader = RoboReader::open("file.bag")?;  // or .mcap
let mut iter = reader.decoded()?;

while let Some(result) = iter.next() {
    let msg = result?;
    println!("Topic: {}", msg.channel.topic);
    println!("Data: {:?}", msg.message);
    println!("Log time: {:?}", msg.log_time);
}
```

Or using the iterator directly:

```rust
use robocodec::RoboReader;

let reader = RoboReader::open("file.mcap")?;

for result in reader.decoded()? {
    let msg = result?;
    // Access msg.message, msg.channel, msg.log_time, msg.publish_time, msg.sequence
}
```

### Read from S3

Robocodec supports reading directly from S3-compatible storage using `s3://` URLs:

```rust
use robocodec::RoboReader;

// Format and S3 access are auto-detected
let reader = RoboReader::open("s3://my-bucket/path/to/data.mcap")?;
println!("Found {} channels", reader.channels().len());
```

**S3-compatible services** (AWS S3, Alibaba Cloud OSS, MinIO, etc.) require credentials set via environment variables:

```bash
# For AWS S3
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"  # optional, defaults to us-east-1

# For Alibaba Cloud OSS, MinIO, or other S3-compatible services
# Use the same env vars above - robocodec works with any S3-compatible API
export AWS_ACCESS_KEY_ID="your-oss-access-key"
export AWS_SECRET_ACCESS_KEY="your-oss-secret-key"
```

> **Note:** While we use AWS-standard environment variable names for compatibility, robocodec works with any S3-compatible storage service (Alibaba OSS, MinIO, Wasabi, etc.) by configuring the appropriate endpoint.

### Write to S3

```rust
use robocodec::RoboWriter;

// Format detected from .mcap extension, S3 from s3:// URL
let mut writer = RoboWriter::create("s3://my-bucket/output.mcap")?;
let channel_id = writer.add_channel("/topic", "MessageType", "cdr", None)?;
// ... write messages ...
writer.finish()?;
```

### Custom S3 Endpoints (MinIO, Alibaba OSS, etc.)

For S3-compatible services with custom endpoints, there are two configuration options:

**Option 1: Environment variable** (applies globally)
```bash
# Set custom endpoint for all s3:// URLs
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

### Convert between formats

```rust
use robocodec::RoboRewriter;

let rewriter = RoboRewriter::open("input.bag")?;
rewriter.rewrite("output.mcap")?;
```

```python
from robocodec import RoboRewriter

rewriter = RoboRewriter("input.bag")
rewriter.rewrite("output.mcap")
```

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

```python
from robocodec import RoboRewriter, TransformBuilder

transform = (TransformBuilder()
    .with_topic_rename("/old/topic", "/new/topic")
    .build())

rewriter = RoboRewriter.with_transforms("input.mcap", transform)
rewriter.rewrite("output.mcap")
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

Then run examples:

```bash
# Using the virtual environment Python
.venv/bin/python3 examples/python/inspect_mcap.py tests/fixtures/robocodec_test_14.mcap

# Or activate venv first
source .venv/bin/activate
python3 examples/python/inspect_mcap.py tests/fixtures/robocodec_test_14.mcap
```

For detailed Python examples and API reference, see [examples/python/README.md](examples/python/README.md).

```bash
git clone https://github.com/archebase/robocodec.git
cd robocodec
make build-python-dev
```

## Supported Formats

| Format | Read | Write |
|--------|------|-------|
| MCAP | ✅ | ✅ |
| ROS1 Bag | ✅ | ✅ |

### Not Supported

| Format | Status | Alternative |
|--------|--------|-------------|
| RRD (Rerun) | Not planned | Use Rerun's tools to convert to MCAP first |

See [docs/formats/rrd.md](docs/formats/rrd.md) for details on why RRD is not natively supported.

## Message Encodings

| Encoding | Description |
|----------|-------------|
| CDR | Common Data Representation (ROS1/ROS2) |
| Protobuf | Protocol Buffers |
| JSON | JSON encoding |

## Schema Support

- ROS `.msg` files (ROS1)
- ROS2 IDL (Interface Definition Language)
- OMG IDL (Object Management Group)

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](ARCHITECTURE.md) | High-level system design |
| [Python Examples](examples/python/README.md) | Python API usage examples |
| [Contributing](CONTRIBUTING.md) | Development setup and guidelines |

## License

MulanPSL v2 - see [LICENSE](LICENSE)

## Links

- [Issue Tracker](https://github.com/archebase/robocodec/issues)
- [Security Policy](SECURITY.md)
