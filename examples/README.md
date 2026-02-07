# Robocodec Rust Examples

This directory contains practical examples demonstrating the public API of the robocodec library for working with robotics data formats (MCAP, ROS1 bag).

## Running Examples

Each example accepts a file path as an argument:

```bash
# Inspect a file
cargo run --example read_file -- tests/fixtures/robocodec_test_14.mcap

# Decode and display messages
cargo run --example decode_messages -- tests/fixtures/robocodec_test_14.mcap

# Convert between formats
cargo run --example convert_format -- tests/fixtures/robocodec_test_14.mcap output.bag
```

## Examples

### `read_file.rs` - Basic File Inspection

Demonstrates opening a robotics data file and inspecting its metadata, channels, and message counts.

**What you'll learn:**
- Using `RoboReader` for automatic format detection
- Accessing file metadata
- Listing channels with their properties

### `convert_format.rs` - Format Conversion

Demonstrates converting between MCAP and ROS1 bag formats.

**What you'll learn:**
- Using `RoboRewriter` for format conversion
- Understanding conversion statistics

### `decode_messages.rs` - Message Decoding

Demonstrates iterating through decoded messages with timestamps.

**What you'll learn:**
- Using the `decoded()` iterator
- Accessing message data, timestamps, and channel info

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
