# Fuzzing Infrastructure for Robocodec

This directory contains fuzzing targets for testing the robustness and security of the robocodec parsers.

## Overview

Fuzzing is a testing technique that provides random, invalid, or unexpected data as inputs to a program to find bugs and vulnerabilities that may not be discovered through traditional testing. The libFuzzer fuzzer automatically generates test cases and monitors for crashes, hangs, and memory leaks.

## Fuzz Targets

The following fuzz targets are available:

### Parser Fuzz Targets

- **`mcap_parser`**: Tests MCAP format parser robustness
- **`bag_parser`**: Tests ROS1 bag format parser robustness
- **`rrd_parser`**: Tests RRF2 (Rerun Data) format parser robustness

### Decoder Fuzz Targets

- **`cdr_decoder`**: Tests CDR (Common Data Representation) decoder robustness

### Schema Parser Fuzz Targets

- **`schema_parser`**: Tests ROS/IDL schema parser robustness

## Prerequisites

### Install `cargo-fuzz`

```bash
cargo install cargo-fuzz --locked
```

### Install Nightly Rust Toolchain

Fuzzing requires the nightly Rust compiler:

```bash
rustup install nightly
```

## Usage

### Run a Specific Fuzz Target

Run the MCAP parser fuzzer for 60 seconds:

```bash
cargo +nightly fuzz run mcap_parser -- -timeout=10
```

Run the ROS1 bag parser fuzzer:

```bash
cargo +nightly fuzz run bag_parser -- -timeout=10
```

Run the CDR decoder fuzzer:

```bash
cargo +nightly fuzz run cdr_decoder -- -timeout=10
```

Run the schema parser fuzzer:

```bash
cargo +nightly fuzz run schema_parser -- -timeout=10
```

### Run All Fuzz Targets

```bash
for target in mcap_parser bag_parser rrd_parser cdr_decoder schema_parser; do
    echo "Fuzzing $target..."
    cargo +nightly fuzz run "$target" -- -timeout=10 -max_total_time=60
done
```

### Common Fuzzer Options

- `-timeout=N`: Timeout for each test case in seconds (default: 1200)
- `-max_total_time=N`: Total fuzzing time in seconds
- `-max_len=N`: Maximum length of generated inputs
- `-runs=N`: Number of test cases to run
- `-jobs=N`: Number of parallel jobs to run
- `-only_ascii`: Only generate ASCII inputs
- `-dict=FILE`: Use a dictionary for better coverage

Example with multiple options:

```bash
cargo +nightly fuzz run mcap_parser -- \
    -timeout=5 \
    -max_total_time=300 \
    -max_len=10000 \
    -jobs=4
```

## Analyzing Crashes

When a crash is found, libFuzzer will save the crashing input to the `fuzz/artifacts/` directory.

### Reproduce a Crash

```bash
cargo +nightly fuzz run mcap_parser fuzz/artifacts/mcap_parser/crash-<hash>
```

### Minimize Crash Input

```bash
cargo +nightly fuzz cmin mcap_parser fuzz/artifacts/mcap_parser/crash-<hash>
```

### Generate Corpus from Directory

```bash
cargo +nightly fuzz corpus mcap_parser -- /path/to/test/files
```

## Best Practices

### 1. Start with Short Runs

When developing new fuzz targets, start with short runs to verify the target works:

```bash
cargo +nightly fuzz run mcap_parser -- -timeout=1 -runs=1000
```

### 2. Use Dictionaries for Better Coverage

Create a dictionary file with common values and magic numbers:

```text
# MCAP magic
"\x14\x08\xB2\xC1\x43\x49\x0A\x0A"

# Common opcodes
"\x00"
"\x01"
"\x02"

# ROS1 bag magic
"#ROS"
```

Run with dictionary:

```bash
cargo +nightly fuzz run mcap_parser -- -dict=fuzz/dictionaries/mcap.dict
```

### 3. Use Existing Test Files as Seed Corpus

```bash
# Copy existing test files to corpus directory
cp tests/fixtures/*.mcap fuzz/corpus/mcap_parser/

# Run fuzzer with seed corpus
cargo +nightly fuzz run mcap_parser
```

### 4. Monitor for Memory Leaks

```bash
cargo +nightly fuzz run mcap_parser -- -detect_leaks=1
```

### 5. Run in CI/CD

Add fuzzing to CI with short time limits:

```yaml
- name: Run fuzzers
  run: |
    for target in mcap_parser bag_parser cdr_decoder; do
      cargo +nightly fuzz run "$target" -- -max_total_time=60 || true
    done
```

## Debugging Crashes

When a crash is found, use these techniques to debug:

### 1. Enable Debug Output

Add `RUST_LOG=debug`:

```bash
RUST_LOG=debug cargo +nightly fuzz run mcap_parser
```

### 2. Use GDB with libFuzzer

```bash
cargo +nightly fuzz run mcap_parser -- -runs=1 \
    fuzz/artifacts/mcap_parser/crash-<hash> \
    -fork=2
```

### 3. Add Debug Prints in Fuzz Target

Modify the fuzz target to print information before the crash:

```rust
fuzz_target!(|data: &[u8]| {
    eprintln!("Input length: {}", data.len());
    // ... rest of fuzz target
});
```

## Adding New Fuzz Targets

To add a new fuzz target:

1. Create a new file in `fuzz/fuzz_targets/<name>.rs`
2. Add the `#![no_main]` attribute and `fuzz_target!` macro
3. Ensure the target handles panics gracefully with `catch_unwind`
4. Test the target compiles:

```bash
cargo +nightly fuzz build
```

5. Run the new target:

```bash
cargo +nightly fuzz run <name>
```

## Integration with Makefile

Add fuzzing commands to the Makefile:

```makefile
.PHONY: fuzz fuzz-all

fuzz: ## Run fuzzers for a short duration
	@echo "Running fuzzers..."
	cargo +nightly fuzz run mcap_parser -- -timeout=10 -max_total_time=60

fuzz-all: ## Run all fuzz targets
	@echo "Running all fuzz targets..."
	for target in mcap_parser bag_parser rrd_parser cdr_decoder schema_parser; do \
		cargo +nightly fuzz run "$$target" -- -timeout=10 -max_total_time=60 || true; \
	done
```

## Coverage Reports

Generate coverage reports for fuzz targets:

```bash
# Build with coverage instrumentation
cargo +nightly fuzz coverage mcap_parser

# Generate report
cargo +nightly fuzz coverage mcap_parser -- -runs=10000
```

## Resources

- [libFuzzer Documentation](https://llvm.org/docs/LibFuzzer.html)
- [cargo-fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [Google/OSS-Fuzz](https://github.com/google/oss-fuzz)

## License

SPDX-License-Identifier: MulanPSL-2.0
