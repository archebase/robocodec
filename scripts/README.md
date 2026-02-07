# Development Scripts

This directory contains **development utilities** and **debugging tools** that use internal APIs. These are **NOT** part of the public API and should **NOT** be used as examples for library consumers.

## Purpose

These scripts are used by robocodec developers for:
- Testing and debugging specific format readers
- Tracing decoding issues
- Schema parsing validation
- Internal development workflows

## Public API Examples

If you're looking for examples of how to **use** the robocodec library, please see:

- **Rust examples**: `../examples/` - Demonstrates the public API (`RoboReader`, `RoboWriter`, `RoboRewriter`)
- **Python examples**: `../examples/python/` - Python bindings with comprehensive documentation

## Files

| File | Purpose |
|------|---------|
| `test_bag_decode_small.rs` | Test decoding from small ROS bag files |
| `test_bag_dump.rs` | Dump bag file contents |
| `test_decode_debug.rs` | Debug decoding issues |
| `test_decode_trace.rs` | Trace CDR decoding offsets |
| `test_fixture_decode.rs` | Test fixture validation |
| `test_read_mcap.rs` | Quick MCAP reading test |
| `test_ros_version.rs` | ROS version detection test |
| `test_schema_parse.rs` | Schema parsing test |
| `upload-fixtures.rs` | Upload test fixtures to MinIO |
| `setup-hooks.sh` | Setup git hooks |
| `upload-fixtures-to-minio.sh` | Fixture upload script |

## Running These Scripts

These scripts are **not** meant to be run via `cargo run --example`. Instead, compile and run them directly:

```bash
rustc scripts/test_read_mcap.rs -L target/debug/deps --extern robocodec=target/debug/librobocodec.rlib
./test_read_mcap
```

Or use cargo with explicit paths if you've configured them appropriately.
