# Robocodec Code Style and Conventions

## File Headers

All source files must include SPDX license headers:

```rust
// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0
```

## Naming Conventions

- **Modules**: `snake_case` (e.g., `core`, `encoding`, `io::formats::mcap`)
- **Types/Structs**: `PascalCase` (e.g., `McapReader`, `DecodedMessage`)
- **Functions**: `snake_case` (e.g., `decode_messages`, `open`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `MAX_BUFFER_SIZE`)
- **Traits**: `PascalCase` (e.g., `FormatReader`, `FormatWriter`, `Decoder`)

## Error Handling

- Use `thiserror` for defining error types
- Use `Result<T>` type alias from `core` module (points to `std::result::Result<T, CodecError>`)
- Error variant constructors are `snake_case` (e.g., `CodecError::invalid_schema()`)

## Documentation

- Public items must have rustdoc comments (`///` or `//!`)
- Module-level docs explain the module's purpose
- Example code in docs uses `no_run` attribute where appropriate

## Code Organization

### Format-Centric Structure

Each format (MCAP, ROS1 bag) has its own module under `io/formats/` containing:
- Low-level I/O operations
- Format-specific readers and writers
- High-level convenience APIs

### Layered Architecture

1. **User Layer** (`lib.rs` re-exports) - User-facing types
2. **High-Level API Layer** - Convenient APIs with auto-decoding
3. **Low-Level I/O Layer** - Parallel/sequential readers
4. **Foundation Layer** - Core types, encoding, schema parsing

## Visibility

- Prefer private items by default
- Re-export commonly used types at appropriate module levels
- Use `#[deprecated]` attributes for backward compatibility re-exports

## License Compliance

- All files must include proper SPDX headers
- Use `reuse lint` to verify compliance (part of CI)
- See REUSE.toml for configuration

## Async/Concurrency

- Uses `rayon` for parallel processing
- Uses `crossbeam` for channel-based communication
- Thread-safe types implement `Send + Sync`
