# Fuzzing Guide for Robocodec

## Overview

This document describes the fuzzing infrastructure for the robocodec library, including setup instructions, usage guidelines, and best practices for finding bugs and security vulnerabilities.

## What is Fuzzing?

Fuzzing is an automated testing technique that provides random, invalid, or unexpected data as inputs to a program. The goals are to:

1. **Find crashes** - Segmentation faults, panics, assertion failures
2. **Find hangs** - Infinite loops, deadlocks, slow operations
3. **Find memory leaks** - Unbounded memory growth
4. **Find logic errors** - Incorrect handling of edge cases

## Fuzzing Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   libFuzzer Engine                     │
│  - Generates random/mutated test cases                 │
│  - Monitors for crashes, hangs, leaks                  │
│  - Minimizes failing test cases                        │
└────────────────┬───────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│              Fuzz Target Function                      │
│  - Receives raw bytes from libFuzzer                   │
│  - Attempts to parse/decode data                       │
│  - Must handle panics gracefully                       │
└────────────────┬───────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│              Robocodec Parsers                         │
│  - MCAP parser (mcap_parser)                           │
│  - ROS1 bag parser (bag_parser)                        │
│  - RRF2 parser (rrd_parser)                            │
│  - CDR decoder (cdr_decoder)                           │
│  - Schema parser (schema_parser)                       │
└─────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Initial Setup

Run the initialization script:

```bash
./scripts/fuzz_init.sh
```

Or manually:

```bash
# Install nightly Rust toolchain
rustup install nightly

# Install cargo-fuzz
cargo +nightly install cargo-fuzz --locked

# Build fuzz targets
cargo +nightly fuzz build
```

### 2. Run a Quick Fuzzing Check

```bash
make fuzz
```

This runs all fuzz targets for 30 seconds each, providing a quick check for obvious issues.

### 3. Run Specific Fuzz Targets

```bash
make fuzz-mcap    # Fuzz MCAP parser only
make fuzz-bag     # Fuzz bag parser only
make fuzz-cdr     # Fuzz CDR decoder only
make fuzz-schema  # Fuzz schema parser only
```

### 4. Extended Fuzzing Runs

For more thorough testing:

```bash
# Run all fuzzers for 1 minute each
make fuzz-all

# Run single fuzzer with custom options
cargo +nightly fuzz run mcap_parser -- \
    -timeout=10 \
    -max_total_time=300 \
    -jobs=4 \
    -dict=fuzz/dictionaries/mcap.dict
```

## Fuzz Targets

### mcap_parser

Tests the MCAP format parser with arbitrary byte sequences. Validates:

- Magic number detection
- Record parsing
- Chunk handling
- Compression/decompression
- Message indexing

**Dictionary**: `fuzz/dictionaries/mcap.dict` contains MCAP magic numbers, opcodes, and common strings.

### bag_parser

Tests the ROS1 bag format parser with arbitrary byte sequences. Validates:

- Header parsing
- Record parsing
- Chunk handling
- Message data extraction
- Connection tracking

**Dictionary**: `fuzz/dictionaries/bag.dict` contains bag magic, opcodes, and common message types.

### rrd_parser

Tests the RRF2 (Rerun Data) format parser with arbitrary byte sequences. Validates:

- Magic number detection
- Chunk parsing
- Arrow message handling
- Compression/decompression

### cdr_decoder

Tests the CDR (Common Data Representation) decoder with arbitrary byte sequences. Validates:

- CDR header parsing
- Primitive type decoding
- Array handling
- String decoding
- Nested structure handling

### schema_parser

Tests the ROS/IDL schema parser with arbitrary text sequences. Validates:

- Type parsing
- Field declaration parsing
- Array notation parsing
- Comment handling
- Multi-file dependencies

**Dictionary**: `fuzz/dictionaries/schema.dict` contains common types, field names, and IDL keywords.

## Interpreting Results

### Successful Run

A successful fuzzing run produces output like:

```
INFO: Seed: 1234567890
INFO: -max_len is not provided; libFuzzer will not generate inputs larger than 4096 bytes
INFO: A corpus is not provided, starting from an empty corpus
#2      INITED cov: 12 ft: 12 corp: 1/1b exec/s: 0 rss: 25Mb
#1024   NEW    cov: 145 ft: 234 corp: 15/234b exec/s: 512 rss: 45Mb
...
```

Key metrics:
- `cov`: Code coverage (number of edges covered)
- `ft`: Number of unique features
- `corp`: Number of interesting test cases in corpus
- `exec/s`: Executions per second
- `rss`: Memory usage

### Crash Found

When a crash is found, libFuzzer will report:

```
==91234==ERROR: libFuzzer: deadly signal
SUMMARY: libFuzzer: deadly signal
artifact_prefix='fuzz/artifacts/mcap_parser/'; Test unit written to fuzz/artifacts/mcap_parser/crash-abc123def456
```

The crashing input is saved to `fuzz/artifacts/<target>/crash-<hash>`.

### Handling Crashes

1. **Reproduce the crash**:
   ```bash
   cargo +nightly fuzz run mcap_parser fuzz/artifacts/mcap_parser/crash-abc123
   ```

2. **Minimize the crash input**:
   ```bash
   cargo +nightly fuzz cmin mcap_parser fuzz/artifacts/mcap_parser/crash-abc123
   ```

3. **Debug the crash**:
   - Add debug prints to the fuzz target
   - Use `gdb` or `lldb` to investigate
   - Check for out-of-bounds access, use-after-free, etc.

4. **Fix the bug** and verify the fix:
   ```bash
   # After fixing, verify the crash no longer occurs
   cargo +nightly fuzz run mcap_parser fuzz/artifacts/mcap_parser/crash-abc123
   ```

## Advanced Usage

### Using Seed Corpus

Provide existing test files as seed corpus for better coverage:

```bash
# Copy test files to corpus
cp tests/fixtures/*.mcap fuzz/corpus/mcap_parser/

# Run fuzzer with seed corpus
cargo +nightly fuzz run mcap_parser
```

### Custom Dictionaries

Create dictionaries with format-specific magic numbers and common values:

```text
# MCAP magic
"\x14\x08\xB2\xC1\x43\x49\x0A\x0A"

# Common opcodes
"\x01"  # Header
"\x05"  # Message
```

Run with dictionary:

```bash
cargo +nightly fuzz run mcap_parser -- -dict=fuzz/dictionaries/mcap.dict
```

### Parallel Fuzzing

Run multiple fuzzing jobs in parallel:

```bash
cargo +nightly fuzz run mcap_parser -- -jobs=4 -workers=4
```

### ASan and UBSan

Enable AddressSanitizer and UndefinedBehaviorSanitizer:

```bash
# Set environment variable
export RUSTFLAGS="-Z sanitizer=address"

# Run fuzzer
cargo +nightly fuzz run mcap_parser -- -sanitizers=address
```

## Integration with CI/CD

Add fuzzing to CI pipelines to catch regressions:

```yaml
# GitHub Actions example
- name: Run fuzzers
  run: |
    ./scripts/fuzz_init.sh
    make fuzz
  continue-on-error: true  # Don't fail CI on fuzzing

- name: Upload corpus artifacts
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: fuzz-corpus
    path: fuzz/corpus/
```

## Best Practices

### 1. Start with Short Runs

When developing new fuzz targets, start with short runs:

```bash
cargo +nightly fuzz run new_target -- -timeout=1 -runs=1000
```

### 2. Use Timeouts

Prevent infinite loops with timeouts:

```bash
cargo +nightly fuzz run mcap_parser -- -timeout=10
```

### 3. Limit Input Size

Prevent memory exhaustion:

```bash
cargo +nightly fuzz run mcap_parser -- -max_len=1048576  # 1 MB
```

### 4. Handle Panics Gracefully

Always use `catch_unwind` in fuzz targets:

```rust
let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    // Fuzzing logic here
}));
```

### 5. Monitor Memory Usage

```bash
cargo +nightly fuzz run mcap_parser -- -rss_limit_mb=512
```

### 6. Use Deterministic Seeds

For reproducible results:

```bash
cargo +nightly fuzz run mcap_parser -- -seed=12345
```

## Troubleshooting

### cargo-fuzz not found

Install cargo-fuzz:

```bash
cargo +nightly install cargo-fuzz --locked
```

### Nightly toolchain issues

Update nightly:

```bash
rustup update nightly
```

### Build errors

Ensure the fuzz target has `#![no_main]` and uses `fuzz_target!` macro.

### No crashes found

- Increase fuzzing time
- Use dictionaries for better coverage
- Add seed corpus from existing test files
- Check if the target is actually parsing the input

## Resources

- [libFuzzer Documentation](https://llvm.org/docs/LibFuzzer.html)
- [cargo-fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [Google/OSS-Fuzz](https://github.com/google/oss-fuzz)
- [Fuzzing Technical Whitepaper](https://github.com/google/fuzzing/blob/master/docs/whitepaper.md)

## License

SPDX-License-Identifier: MulanPSL-2.0
