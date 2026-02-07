# Robocodec Rust Examples

This directory contains practical examples demonstrating the public API of the robocodec library for working with robotics data formats (MCAP, ROS1 bag).

## Running Examples

### Local Files

```bash
# Inspect a file
cargo run --example read_file -- tests/fixtures/robocodec_test_14.mcap

# Decode and display messages
cargo run --example decode_messages -- tests/fixtures/robocodec_test_14.mcap

# Rewrite a file (same format)
cargo run --example convert_format -- tests/fixtures/robocodec_test_14.mcap output.mcap

# Transform topics and types
cargo run --example transform -- tests/fixtures/robocodec_test_14.mcap output.mcap
```

### Remote Files (S3)

```bash
# Set S3 credentials (for AWS S3, MinIO, Alibaba OSS, etc.)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# Read from S3
cargo run --example s3_example -- s3://my-bucket/path/to/data.mcap

# Read from S3 with custom endpoint (MinIO, Alibaba OSS, etc.)
cargo run --example s3_example -- "s3://bucket/data.mcap?endpoint=http://localhost:9000"
```

## Examples

### `read_file.rs` - Basic File Inspection

Demonstrates opening a robotics data file and inspecting its metadata, channels, and message counts.

**What you'll learn:**
- Using `RoboReader` for automatic format detection
- Accessing file metadata
- Listing channels with their properties

### `convert_format.rs` - File Rewriting

Demonstrates rewriting a robotics data file in the same format.

**What you'll learn:**
- Using `RoboRewriter` to rewrite files
- Understanding rewrite statistics

**Note:** The rewriter preserves the same format as the input file. Cross-format conversion is not currently supported.

### `decode_messages.rs` - Message Decoding

Demonstrates iterating through decoded messages with timestamps.

**What you'll learn:**
- Using the `decoded()` iterator
- Accessing message data, timestamps, and channel info

### `s3_example.rs` - S3 Remote File Access

Demonstrates reading robotics data files from S3-compatible storage.

**What you'll learn:**
- Reading from S3-compatible storage (AWS S3, MinIO, Alibaba OSS, etc.)
- S3 authentication via environment variables
- Custom S3 endpoints via URL parameters

### `transform.rs` - Topic and Type Transformations

Demonstrates renaming topics and message types during format conversion.

**What you'll learn:**
- Using `TransformBuilder` to create transformation pipelines
- Topic renaming with exact name matching
- Type renaming for schema migration
- Combining transformations with `RoboRewriter`

## Public API

The examples demonstrate the **public API** only:

| Type | Purpose |
|------|---------|
| `RoboReader` | Read files with auto-detection |
| `RoboWriter` | Write files |
| `RoboRewriter` | Convert formats and apply transformations |
| `FormatReader` | Trait for format-agnostic reading |

## Test Fixtures

Examples use test fixtures from `tests/fixtures/`:

```bash
# List available test files
ls tests/fixtures/

# Run with a test file
cargo run --example read_file -- tests/fixtures/robocodec_test_14.mcap
```

## Development Utilities

The `scripts/` directory contains development utilities that use internal APIs for debugging and testing. These are **not** part of the public API and should not be used as examples for library consumers.
