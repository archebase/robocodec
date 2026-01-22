# Robocodec Development Commands

## Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Build with specific features
cargo build --features python
cargo build --features jemalloc
```

## Testing

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run tests in release mode (faster)
cargo test --release
```

**Note**: Do NOT use `--all-features` or `--features python` when testing. PyO3's `extension-module` feature prevents linking in standalone test binaries.

## Formatting

```bash
# Format code
cargo fmt

# Check formatting without modifying
cargo fmt -- --check
```

## Linting

```bash
# Run clippy (standard)
cargo clippy

# Run clippy with all features (as in CI)
cargo clippy --all-targets --all-features -- -D warnings

# Run clippy for specific package
cargo clippy -p robocodec
```

## Coverage

```bash
# Install coverage tool (first time only)
cargo install cargo-llvm-cov

# Generate coverage report
cargo llvm-cov --workspace --lcov --output-path lcov.info

# View coverage in terminal
cargo llvm-cov --workspace
```

## License Compliance

```bash
# Install REUSE tool (first time only)
pip install reuse

# Check license header compliance
reuse lint
```

## Python Bindings

```bash
# Install maturin (first time only)
pip install maturin

# Build and install Python package in development mode
maturin develop

# Build Python wheels
maturin build --release
```

## Examples

```bash
# Read MCAP file
cargo run --example read_mcap -- data.mcap

# Convert between formats (if example exists)
cargo run --example convert -- input.bag output.mcap
```

## Darwin (macOS) Specific Commands

Standard Unix commands work on macOS:
- `git` - Version control
- `ls`, `cd`, `pwd` - Directory navigation
- `grep`, `rg` (ripgrep) - Search
- `find` - File search
- `cat` - File viewing

## CI Workflow Commands (for local verification)

The CI pipeline runs these checks:
1. License compliance: `reuse lint`
2. Format check: `cargo fmt -- --check`
3. Clippy: `cargo clippy --all-targets --all-features -- -D warnings`
4. Tests: `cargo test`
5. Coverage: `cargo llvm-cov --workspace --lcov --output-path lcov.info`
