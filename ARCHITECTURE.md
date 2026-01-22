# Robocodec Architecture

This document describes the architectural organization of the robocodec crate.

## Overview

Robocodec is organized as a **format-centric** library, where each robotics data format has its own module containing all related functionality (readers, writers, high-level APIs). A unified API layer (`RoboReader`, `RoboWriter`, `RoboRewriter`) provides format-agnostic operations with automatic format detection.

## Key Principles

### 1. Unified User API with Format-Centric Internals

The library provides a single, format-agnostic API at the top level while organizing format-specific implementations in dedicated modules:

**User API (lib.rs)**:
```rust
use robocodec::{RoboReader, RoboWriter, RoboRewriter};

// Format auto-detected from file extension
let reader = RoboReader::open("data.mcap")?;
let writer = RoboWriter::create("output.bag")?;
let rewriter = RoboRewriter::open("input.mcap")?;
```

**Format-Specific APIs (when needed)**:
```rust
use robocodec::io::formats::mcap::reader::McapReader;
use robocodec::io::formats::bag::reader::ParallelBagReader;
```

### 2. Layered Architecture

```
┌─────────────────────────────────────────────────────────┐
│  User Layer (lib.rs re-exports)                         │
│  - RoboReader, RoboWriter, RoboRewriter                │
│  - FormatReader, FormatWriter traits                   │
│  - TransformBuilder, Transform types                   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Unified I/O Layer                                      │
│  - io/reader/mod.rs (auto-strategy selection)          │
│  - io/writer/mod.rs (auto-strategy selection)          │
│  - io/traits.rs (FormatReader, FormatWriter)           │
│  - io/metadata.rs (FileFormat, ChannelInfo, etc.)      │
│  - io/detection.rs (format detection from extension)   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Format-Specific Layer                                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/formats/mcap/                                │   │
│  │   - reader.rs (McapReader with auto-decode)     │   │
│  │   - writer.rs (ParallelMcapWriter)              │   │
│  │   - parallel.rs (low-level parallel reader)     │   │
│  │   - sequential.rs (low-level sequential reader) │   │
│  │   - two_pass.rs (two-pass reader strategy)      │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │ io/formats/bag/                                 │   │
│  │   - reader.rs (ParallelBagReader)               │   │
│  │   - writer.rs (BagWriter)                       │   │
│  │   - parallel.rs (low-level parallel reader)     │   │
│  │   - sequential.rs (low-level sequential reader) │   │
│  └─────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Foundation Layer                                       │
│  - core/ (CodecError, Result, types)                   │
│  - encoding/ (CDR, Protobuf, JSON codecs)              │
│  - schema/ (msg, ROS2 IDL, OMG IDL parsers)           │
│  - transform/ (topic/type renaming with wildcards)     │
│  - types/ (arena allocation, chunk management)         │
└─────────────────────────────────────────────────────────┘
```

### 3. Rewriter Architecture

The rewriter module provides a unified facade that:

1. **Auto-detects format** from file extension (`.mcap` → MCAP, `.bag` → ROS1 bag)
2. **Delegates to format-specific rewriters** via the `FormatRewriter` trait
3. **Shares common transformation logic** via the rewrite engine

```
User code
  │
  ├─ RoboRewriter::open("data.mcap")
  │       │
  │       ├─ detect_format() → FileFormat::Mcap
  │       │
  │       └─ creates McapRewriter (internal)
  │
  └─ RoboRewriter::open("data.bag")
          │
          ├─ detect_format() → FileFormat::Bag
          │
          └─ creates BagRewriter (internal)
```

**Rewriter Components**:

- `facade.rs` - `RoboRewriter` enum with format detection
- `engine.rs` - Shared rewrite engine with transformation support
- `mcap/` - MCAP-specific rewriter implementation
- `bag/` - ROS1 bag-specific rewriter implementation

### 4. Auto-Strategy Selection

Readers and writers automatically choose the optimal strategy:

- **Parallel mode**: Used when the format supports chunked reading/writing
- **Sequential mode**: Fallback for non-chunked or small files
- **Two-pass mode**: MCAP-specific strategy for certain access patterns

## Design Decisions

### Why Unified User API?

**Problem**: Users want to work with robotics data files without worrying about format details.

**Solution**: Provide `RoboReader`/`RoboWriter` that:
- Auto-detect format from file extension
- Use optimal strategy automatically
- Provide consistent interface across formats

```rust
// Works for both MCAP and ROS1 bag
let reader = RoboReader::open(path)?;
let channels = reader.channels();
```

### Why Format-Centric Internals?

**Problem**: Each format has unique characteristics (chunking, indexing, compression).

**Solution**: Organize by format under `io/formats/`:
```rust
// Clear: Everything MCAP-related is in one place
use robocodec::io::formats::mcap::{reader::McapReader, writer::ParallelMcapWriter};

// For most users, just use the unified API
use robocodec::{RoboReader, RoboWriter};
```

**Benefits**:
- Easy to locate format-specific code
- Simple to add new formats (create a new directory)
- Clear ownership boundaries
- Format-specific optimizations isolated

### Transformation Architecture

The `transform` module provides flexible data transformation:

- `topic_rename.rs` - Topic renaming with wildcards
- `type_rename.rs` - Type renaming with wildcards
- `pipeline.rs` - Multi-transform orchestration
- `normalization.rs` - Type normalization rules

Transformations are applied during rewriting via the `McapTransform` trait.

## Usage Examples

### Reading with Auto-Detection

```rust
use robocodec::{FormatReader, RoboReader};

let reader = RoboReader::open("file.mcap")?;
println!("Channels: {}", reader.channels().len());
println!("Messages: {}", reader.message_count());
```

### Format-Specific Reading (when needed)

```rust
use robocodec::io::formats::mcap::reader::McapReader;

let reader = McapReader::open("file.mcap")?;
for result in reader.decode_messages()? {
    let (decoded, channel) = result?;
    println!("Topic: {}, Data: {:?}", channel.topic, decoded);
}
```

### Rewriting with Transformations

```rust
use robocodec::{RoboRewriter, TransformBuilder};

let transform = TransformBuilder::new()
    .with_topic_rename("/old/topic", "/new/topic")
    .with_type_rename("OldType", "NewType")
    .build();

let rewriter = RoboRewriter::with_options(
    "input.mcap",
    RewriteOptions::default().with_transforms(transform)
)?;
rewriter.rewrite("output.mcap")?;
```

## Module Organization

### User-Facing Modules (lib.rs)

| Module | Purpose |
|--------|---------|
| `core` | Core error types and result aliases |
| `io` | Unified I/O traits and reader/writer facades |
| `encoding` | Message codecs (CDR, Protobuf, JSON) |
| `schema` | Schema parsers (ROS .msg, ROS2 IDL, OMG IDL) |
| `transform` | Topic/type transformation support |
| `types` | Arena allocation and chunk management |
| `rewriter` | Unified rewriter with format auto-detection |
| `python` | Python bindings (optional feature) |

### Internal I/O Structure

```
io/
├── mod.rs              # Module exports
├── reader/             # Unified reader with strategy selection
├── writer/             # Unified writer with strategy selection
├── traits.rs           # FormatReader, FormatWriter traits
├── metadata.rs         # FileFormat, ChannelInfo, FileInfo
├── detection.rs        # Format detection from file path
├── arena.rs            # Memory-mapped arena allocation
├── filter.rs           # Message filtering utilities
└── formats/
    ├── mod.rs
    ├── mcap/
    │   ├── reader.rs   # McapReader with auto-decoding
    │   ├── writer.rs   # ParallelMcapWriter
    │   ├── parallel.rs # Low-level parallel reader
    │   ├── sequential.rs # Low-level sequential reader
    │   ├── two_pass.rs # Two-pass reader strategy
    │   └── constants.rs # MCAP format constants
    └── bag/
        ├── reader.rs   # ParallelBagReader
        ├── writer.rs   # BagWriter
        ├── parallel.rs # Low-level parallel reader
        ├── sequential.rs # Low-level sequential reader
        └── parser.rs   # Bag format parsing
```

## Adding a New Format

To add a new format (e.g., ROS2 bag):

1. **Create format module**: `src/io/formats/ros2bag/`
2. **Implement traits**:
   - `FormatReader` trait for reading
   - `FormatWriter` trait for writing
   - `FormatRewriter` trait for rewriting (in `rewriter/ros2bag.rs`)
3. **Add low-level I/O**: `reader.rs`, `writer.rs` with parallel/sequential strategies
4. **Update format detection**: Add extension to `io/detection.rs`
5. **Update rewriter facade**: Add format handling to `rewriter/facade.rs`
6. **Export module**: Add to `io/formats/mod.rs` and `lib.rs`

## Related Documentation

- [CLAUDE.md](CLAUDE.md) - Project overview and build commands
- [README.md](README.md) - User-facing documentation and examples
