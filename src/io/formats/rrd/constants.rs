// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Constants for RRD (Rerun Data) file format.
//!
//! This implements the RRF2 format as defined by rerun:
//! https://github.com/rerun-io/rerun/tree/main/crates/store/re_log_encoding/src/rrd

/// RRF2 magic number - current Rerun RRD format.
///
/// Format: "RRF2" (4 bytes)
pub const RRD_MAGIC: &[u8; 4] = b"RRF2";

/// Old RRD magic numbers - deprecated formats.
pub const OLD_RRD_MAGIC: [[u8; 4]; 2] = [*b"RRF0", *b"RRF1"];

/// RRD stream footer identifier.
pub const RRD_FOOTER_ID: &[u8; 4] = b"FOOT";

/// Alias for footer magic (same as FOOT identifier for RRF2).
pub const RRD_FOOTER_MAGIC: &[u8; 4] = RRD_FOOTER_ID;

/// Current RRD format version (encoded as 4 bytes in the file).
pub const RRD_VERSION: [u8; 4] = [0, 0, 0, 1];

/// StreamHeader size: fourcc(4) + version(4) + options(4).
pub const STREAM_HEADER_SIZE: usize = 12;

/// MessageHeader size: kind(8) + len(8).
pub const MESSAGE_HEADER_SIZE: usize = 16;

/// StreamFooter size (single entry): entries(20) + fourcc(4) + identifier(4) + num_entries(4).
pub const STREAM_FOOTER_SIZE: usize = 32;

/// Encoding options size: compression(1) + serializer(1) + reserved(2).
pub const ENCODING_OPTIONS_SIZE: usize = 4;

/// Compression: Off.
pub const COMPRESSION_OFF: u8 = 0;

/// Compression: LZ4.
pub const COMPRESSION_LZ4: u8 = 1;

/// Compression: Zstd (not used in RRF2 but reserved).
pub const COMPRESSION_ZSTD: u8 = 2;

/// Compression: None (alias for COMPRESSION_OFF).
#[deprecated(note = "Use COMPRESSION_OFF instead")]
pub const COMPRESSION_NONE: u8 = COMPRESSION_OFF;

/// Serializer: Removed MsgPack (historical).
pub const SERIALIZER_MSGPACK: u8 = 1;

/// Serializer: Protobuf.
pub const SERIALIZER_PROTOBUF: u8 = 2;

/// Message kind: End of stream.
pub const MSG_KIND_END: u64 = 0;

/// Message kind: SetStoreInfo.
pub const MSG_KIND_SET_STORE_INFO: u64 = 1;

/// Message kind: ArrowMsg.
pub const MSG_KIND_ARROW_MSG: u64 = 2;

/// Message kind: BlueprintActivationCommand.
pub const MSG_KIND_BLUEPRINT_ACTIVATION_COMMAND: u64 = 3;

/// CRC seed for stream footer (RERUN in base 26).
pub const FOOTER_CRC_SEED: u32 = 7850921;

/// Default topic name for RRD messages.
pub const DEFAULT_TOPIC: &str = "/";

/// Message encoding: Protobuf.
pub const MESSAGE_ENCODING_PROTOBUF: &str = "protobuf";

// Legacy constants for backward compatibility with old RRD format
/// These are deprecated and should not be used for new code.
#[deprecated(note = "RRF2 does not use chunk-based indexing")]
pub const HEADER_SIZE: usize = STREAM_HEADER_SIZE;

#[deprecated(note = "RRF2 does not use chunk-based indexing")]
pub const FOOTER_SIZE: usize = STREAM_FOOTER_SIZE;

#[deprecated(note = "Use STREAM_HEADER_SIZE instead")]
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

#[deprecated(note = "RRF2 does not use chunk-based indexing")]
pub const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Default compression level for LZ4.
pub const DEFAULT_COMPRESSION_LEVEL: u32 = 4;

/// Schema encoding: Protobuf (legacy - RRF2 uses serializer field).
#[deprecated(note = "RRF2 uses serializer field instead")]
pub const SCHEMA_ENCODING_PROTOBUF: u8 = 1;

/// Schema encoding: Flatbuffers.
#[deprecated(note = "RRF2 uses serializer field instead")]
pub const SCHEMA_ENCODING_FLATBUFFERS: u8 = 2;

/// Schema encoding: JSON.
#[deprecated(note = "RRF2 uses serializer field instead")]
pub const SCHEMA_ENCODING_JSON: u8 = 3;

/// Message encoding: CDR (Common Data Representation).
pub const MESSAGE_ENCODING_CDR: &str = "cdr";

/// Message encoding: JSON.
pub const MESSAGE_ENCODING_JSON: &str = "json";

/// Message encoding: Arrow IPC.
pub const MESSAGE_ENCODING_ARROW: &str = "arrow";

/// Legacy RRD version (for backward compatibility).
#[deprecated(note = "RRF2 uses 4-byte version")]
pub const RRD_VERSION_LEGACY: u16 = 1;
