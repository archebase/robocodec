# Contributing to Robocodec

Thank you for your interest in contributing! This document covers development setup, coding standards, and contribution workflow.

## Quick Start

```bash
# Clone and setup
git clone https://github.com/archebase/robocodec.git
cd robocodec

# Build
make build

# Run tests
make test

# Format and lint
make check
```

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Python 3.11+ (for Python bindings development)
- [maturin](https://maturin.rs/) (for Python development)
- [reuse](https://reuse.software/) (for license compliance checks)

### Installing Tools

```bash
# Rust tools (via rustup)
rustup install stable
rustup component add rustfmt clippy

# Python tools
pip install maturin pytest pytest-cov black ruff

# License compliance tool
pip install reuse
```

## Build Commands

### Using Make (recommended)

| Command | Description |
|---------|-------------|
| `make build` | Build Rust library (debug) |
| `make build-release` | Build Rust library (release) |
| `make build-python-dev` | Install Python package in dev mode |
| `make build-python` | Build Python wheel |

### Using Cargo directly

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Build with features
cargo build --features python
cargo build --features jemalloc
```

## Testing

### Running Tests

```bash
# Run all tests
make test
# or: cargo test

# Run specific test
cargo test test_name

# Run with output
cargo test -- --nocapture

# Release mode (faster)
cargo test --release
```

**Important:** Do NOT use `--all-features` or `--features python` when running tests. PyO3's `extension-module` feature prevents linking in standalone test binaries.

### Test Coverage

```bash
# Install coverage tool (first time)
cargo install cargo-llvm-cov

# Generate coverage report
make coverage
# or: cargo llvm-cov --workspace --html

# View coverage in terminal
cargo llvm-cov --workspace
```

### Python Tests

```bash
# Build extension and run tests
make test-python
# or:
maturin develop --features python
pytest tests/python/ -v
```

## Code Quality

### Format Code

```bash
# Format all code
make fmt

# Format Rust only
cargo fmt

# Format Python only
black python/ tests/python/

# Check format without modifying
cargo fmt -- --check
```

### Lint

```bash
# Lint all code
make lint

# Lint Rust (as in CI)
cargo clippy --all-targets --all-features -- -D warnings

# Lint Python
ruff check python/ tests/python/
```

### Full Check

```bash
# Run format check + lint
make check
```

## License Compliance

All source files must include SPDX license headers:

```rust
// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0
```

Check compliance:

```bash
make check-license
# or: reuse lint
```

## Code Style

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Modules | `snake_case` | `core`, `encoding::cdr` |
| Types/Structs | `PascalCase` | `McapReader`, `DecodedMessage` |
| Functions | `snake_case` | `decode_messages`, `open` |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_BUFFER_SIZE` |
| Traits | `PascalCase` | `FormatReader`, `Decoder` |

### Error Handling

- Use `thiserror` for defining error types
- Use `Result<T>` type alias from `core` module
- Error variant constructors use `snake_case`

```rust
use robocodec::{CodecError, Result};

fn parse(data: &[u8]) -> Result<Output> {
    if data.is_empty() {
        return Err(CodecError::invalid_input("empty data"));
    }
    // ...
}
```

### Documentation

- Public items must have rustdoc comments (`///` or `//!`)
- Module-level docs explain the module's purpose
- Use `no_run` attribute for examples that shouldn't be executed

```rust
//! # MCAP Format Module
//!
//! This module provides MCAP file reading and writing capabilities.

/// Reads MCAP files with automatic format detection.
///
/// # Example
///
/// ```no_run
/// use robocodec::io::formats::mcap::McapReader;
/// let reader = McapReader::open("data.mcap")?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct McapReader { ... }
```

## Architecture

Robocodec uses a **format-centric** architecture:

- `src/io/formats/mcap/` - MCAP format implementation
- `src/io/formats/bag/` - ROS1 bag format implementation
- `src/encoding/` - Message codecs (CDR, Protobuf, JSON)
- `src/schema/` - Schema parsers
- `src/transform/` - Topic/type transformations
- `src/rewriter/` - Unified rewriter with format auto-detection

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## Adding a New Format

To add support for a new format:

1. Create directory: `src/io/formats/{format}/`
2. Implement `FormatReader` and `FormatWriter` traits
3. Add format detection to `io/detection.rs`
4. Update rewriter in `rewriter/facade.rs`
5. Add tests in `tests/`

## Making Changes

1. **Create a branch** from `main`
2. **Make your changes** following the code style guidelines
3. **Run tests** to ensure everything works
4. **Run `make check`** to verify formatting and linting
5. **Commit** with a clear message
6. **Push** and create a pull request

### Commit Message Style

```
type: brief description

- Detailed point 1
- Detailed point 2
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

Example:
```
feat: add support for ROS2 bag format

- Add reader/writer for ROS2 bag files
- Implement FormatReader/FormatWriter traits
- Add format detection for .db3 extension
```

## Pull Request Process

1. Ensure all tests pass: `make test`
2. Ensure code quality checks pass: `make check`
3. Update documentation if needed
4. Reference related issues in your PR description

### CI Pipeline

PRs run these checks automatically:

1. License compliance: `reuse lint`
2. Format check: `cargo fmt -- --check`
3. Clippy: `cargo clippy --all-targets --all-features -- -D warnings`
4. Tests: `cargo test`
5. Coverage: `cargo llvm-cov --workspace --lcov`

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/archebase/robocodec/issues)
- **Discussions**: Use GitHub Issues for questions
- **Documentation**: [README.md](README.md), [ARCHITECTURE.md](ARCHITECTURE.md)
