// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Constants for RRD file format.

/// RRD magic number at the start of the file.
///
/// Format: "RRD\0" (4 bytes)
pub const RRD_MAGIC: &[u8; 4] = b"RRD\0";

/// RRD magic number at the end of the file (footer).
///
/// Same as header magic for validation
pub const RRD_FOOTER_MAGIC: &[u8; 4] = b"RRD\0";

/// Current RRD format version.
pub const RRD_VERSION: u16 = 1;

/// Default chunk size for RRD files (256KB).
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Maximum chunk size supported (16MB).
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Default compression level for LZ4 (0 = fastest, 16 = best).
pub const DEFAULT_COMPRESSION_LEVEL: u32 = 4;

/// Compression type: LZ4.
pub const COMPRESSION_LZ4: u8 = 1;

/// Compression type: Zstd.
pub const COMPRESSION_ZSTD: u8 = 2;

/// Compression type: None.
pub const COMPRESSION_NONE: u8 = 0;

/// Schema encoding: Protobuf.
pub const SCHEMA_ENCODING_PROTOBUF: u8 = 1;

/// Schema encoding: Flatbuffers.
pub const SCHEMA_ENCODING_FLATBUFFERS: u8 = 2;

/// Schema encoding: JSON.
pub const SCHEMA_ENCODING_JSON: u8 = 3;

/// Message encoding: CDR (Common Data Representation).
pub const MESSAGE_ENCODING_CDR: &str = "cdr";

/// Message encoding: Protobuf.
pub const MESSAGE_ENCODING_PROTOBUF: &str = "protobuf";

/// Message encoding: JSON.
pub const MESSAGE_ENCODING_JSON: &str = "json";

/// Message encoding: Arrow IPC.
pub const MESSAGE_ENCODING_ARROW: &str = "arrow";

/// Default topic name in RRD files (RRD uses entity paths as topics).
pub const DEFAULT_TOPIC: &str = "/";

/// RRD file header size (magic + version + flags + reserved).
pub const HEADER_SIZE: usize = 32;

/// RRD file footer size (magic + chunk_count + index_offset).
pub const FOOTER_SIZE: usize = 32;
