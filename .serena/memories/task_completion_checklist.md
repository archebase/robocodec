# Robocodec Task Completion Checklist

When completing a development task, ensure the following:

## Code Quality

- [ ] Code follows naming conventions (snake_case for functions, PascalCase for types)
- [ ] Public items have rustdoc comments
- [ ] Error handling uses `CodecError` or appropriate error types
- [ ] No `unwrap()` or `expect()` in production code paths
- [ ] License headers (SPDX) included in new files

## Commands to Run

### Format Check
```bash
cargo fmt -- --check
```

### Lint Check
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Tests
```bash
cargo test
```

**Important**: Do NOT use `--all-features` or `--features python` when testing. PyO3's extension-module feature prevents linking in standalone test binaries.

### License Compliance
```bash
reuse lint
```

## Before Committing

1. **Format**: Ensure code is formatted with `cargo fmt`
2. **Clippy**: All clippy warnings must be addressed
3. **Tests**: All tests must pass
4. **Headers**: Verify license headers with `reuse lint`
5. **Build**: Verify `cargo build --release` succeeds

## For Python Changes

If modifying Python bindings:
- [ ] Test with `maturin develop` + Python import
- [ ] Build wheels: `maturin build --release`

## Architecture Considerations

- **Format-centric**: Put format-specific code in `io/formats/{format}/`
- **Layered**: High-level APIs should use low-level I/O layer
- **Unified traits**: Use `FormatReader`, `FormatWriter` traits for generic operations
- **Backward compatibility**: Add deprecation notices when changing public APIs
