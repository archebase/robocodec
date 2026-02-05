// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ArrowMsg protobuf encoding/decoding with LZ4 compression support.
//!
//! This module handles the ArrowMsg format used by Rerun's RRD files.
//! ArrowMsg contains potentially LZ4-compressed Arrow IPC data.
//!
//! # ArrowMsg Format (Rerun 0.27+)
//!
//! Based on the official protobuf definition from Rerun:
//! ```protobuf
//! message ArrowMsg {
//!   rerun.common.v1alpha1.StoreId store_id = 1;
//!   optional rerun.common.v1alpha1.Tuid chunk_id = 6;
//!   rerun.common.v1alpha1.Compression compression = 2;
//!   uint64 uncompressed_size = 3;
//!   Encoding encoding = 4;
//!   bytes payload = 5;
//!   optional bool is_static = 7;
//! }
//!
//! enum Compression {
//!   COMPRESSION_UNSPECIFIED = 0;
//!   COMPRESSION_NONE = 1;
//!   COMPRESSION_LZ4 = 2;
//! }
//!
//! enum Encoding {
//!   ENCODING_UNSPECIFIED = 0;
//!   ENCODING_ARROW_IPC = 1;
//! }
//!
//! message StoreId {
//!   StoreKind kind = 1;
//!   string recording_id = 2;
//!   ApplicationId application_id = 3;
//! }
//!
//! enum StoreKind {
//!   STORE_KIND_UNSPECIFIED = 0;
//!   STORE_KIND_RECORDING = 1;
//!   STORE_KIND_BLUEPRINT = 2;
//! }
//! ```
//!
//! Reference: https://github.com/rerun-io/rerun/tree/main/crates/store/re_protos/proto/rerun/v1alpha1

use std::io;

use crate::core::Result;

/// Compression type for ArrowMsg payload.
///
/// Matches Rerun's Compression enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowCompression {
    /// Unspecified compression (treated as none)
    Unspecified = 0,
    /// No compression
    None = 1,
    /// LZ4 block compression
    Lz4 = 2,
}

impl ArrowCompression {
    /// Create from u32 value (Rerun's Compression enum values).
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => Self::Unspecified,
            1 => Self::None,
            2 => Self::Lz4,
            _ => Self::Unspecified, // Default to Unspecified for unknown values
        }
    }

    /// Convert to u32.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Check if compression is enabled.
    pub fn is_compressed(self) -> bool {
        self == Self::Lz4
    }

    /// Returns true if compression is explicitly None
    pub fn is_none(self) -> bool {
        self == Self::None
    }
}

/// Encoding type for ArrowMsg payload.
///
/// Matches Rerun's Encoding enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowEncoding {
    /// Unspecified encoding
    Unspecified = 0,
    /// Arrow-IPC encoding
    ArrowIpc = 1,
}

impl ArrowEncoding {
    /// Create from u32 value (Rerun's Encoding enum values).
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => Self::Unspecified,
            1 => Self::ArrowIpc,
            _ => Self::Unspecified,
        }
    }

    /// Convert to u32.
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Store kind for StoreId.
///
/// Matches Rerun's StoreKind enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    /// Unspecified store kind
    Unspecified = 0,
    /// Recording store
    Recording = 1,
    /// Blueprint store
    Blueprint = 2,
}

impl StoreKind {
    /// Create from u32 value.
    pub fn from_u32(value: u32) -> Self {
        match value {
            0 => Self::Unspecified,
            1 => Self::Recording,
            2 => Self::Blueprint,
            _ => Self::Unspecified,
        }
    }

    /// Convert to u32.
    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// StoreId information (optional, can be omitted when writing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreId {
    /// The kind of the store
    pub kind: StoreKind,
    /// The recording ID
    pub recording_id: String,
    /// The application ID
    pub application_id: String,
}

impl StoreId {
    /// Create a new minimal StoreId for a recording.
    pub fn new_recording(recording_id: impl Into<String>) -> Self {
        Self {
            kind: StoreKind::Recording,
            recording_id: recording_id.into(),
            application_id: String::new(),
        }
    }

    /// Create an empty/placeholder StoreId.
    pub fn empty() -> Self {
        Self {
            kind: StoreKind::Recording,
            recording_id: String::new(),
            application_id: String::new(),
        }
    }
}

/// ArrowMsg protobuf structure.
///
/// This represents a Rerun ArrowMsg message with potentially compressed
/// Arrow IPC data as the payload.
#[derive(Debug, Clone)]
pub struct ArrowMsg {
    /// Store ID (optional - can be empty for passthrough)
    pub store_id: Option<StoreId>,
    /// Compression type for the payload
    pub compression: ArrowCompression,
    /// Uncompressed size of the payload (bytes)
    pub uncompressed_size: u64,
    /// Encoding of the payload
    pub encoding: ArrowEncoding,
    /// Payload data (Arrow IPC, potentially LZ4 compressed)
    pub payload: Vec<u8>,
    /// Chunk ID (optional)
    pub chunk_id: Option<[u8; 16]>,
    /// Whether this is static data (optional)
    pub is_static: Option<bool>,
}

impl ArrowMsg {
    /// Create a new ArrowMsg with uncompressed payload.
    pub fn new(payload: Vec<u8>) -> Self {
        let uncompressed_size = payload.len() as u64;
        Self {
            store_id: None,
            compression: ArrowCompression::None,
            uncompressed_size,
            encoding: ArrowEncoding::ArrowIpc,
            payload,
            chunk_id: None,
            is_static: None,
        }
    }

    /// Create a new ArrowMsg with LZ4 compressed payload.
    pub fn with_lz4(payload: Vec<u8>) -> Result<Self> {
        let uncompressed_size = payload.len() as u64;
        let compressed = lz4_flex::block::compress(&payload);
        Ok(Self {
            store_id: None,
            compression: ArrowCompression::Lz4,
            uncompressed_size,
            encoding: ArrowEncoding::ArrowIpc,
            payload: compressed,
            chunk_id: None,
            is_static: None,
        })
    }

    /// Create an ArrowMsg with the specified compression.
    pub fn with_compression(payload: Vec<u8>, compression: ArrowCompression) -> Result<Self> {
        let uncompressed_size = payload.len() as u64;
        let (payload, compression) = match compression {
            ArrowCompression::Lz4 => {
                let compressed = lz4_flex::block::compress(&payload);
                (compressed, ArrowCompression::Lz4)
            }
            ArrowCompression::None | ArrowCompression::Unspecified => {
                (payload, ArrowCompression::None)
            }
        };
        Ok(Self {
            store_id: None,
            compression,
            uncompressed_size,
            encoding: ArrowEncoding::ArrowIpc,
            payload,
            chunk_id: None,
            is_static: None,
        })
    }

    /// Set the store_id for this ArrowMsg.
    pub fn with_store_id(mut self, store_id: StoreId) -> Self {
        self.store_id = Some(store_id);
        self
    }

    /// Set the is_static flag for this ArrowMsg.
    pub fn with_is_static(mut self, is_static: bool) -> Self {
        self.is_static = Some(is_static);
        self
    }

    /// Parse an ArrowMsg from bytes (protobuf format).
    ///
    /// This implements a protobuf parser for the ArrowMsg format defined in
    /// Rerun's official protobuf definition. See module-level docs for reference.
    ///
    /// Fields parsed:
    /// ```text
    /// field 1: store_id (message)
    /// field 2: compression (enum): 0=Unspecified, 1=None, 2=LZ4
    /// field 3: uncompressed_size (varint)
    /// field 4: encoding (enum): 0=Unspecified, 1=ArrowIpc
    /// field 5: payload (bytes)
    /// field 6: chunk_id (message, optional)
    /// field 7: is_static (bool, optional)
    /// ```
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut data = data;
        let mut store_id = None;
        let mut compression = ArrowCompression::Unspecified;
        let mut uncompressed_size = 0u64;
        let mut encoding = ArrowEncoding::Unspecified;
        let mut payload = Vec::new();
        let chunk_id = None;
        let mut is_static = None;

        while !data.is_empty() {
            // Read tag (field_number << 3 | wire_type)
            let tag = read_varint(&mut data)?;
            let field_number = tag >> 3;
            let wire_type = tag & 0x07;

            match field_number {
                1 => {
                    // store_id field (message)
                    if wire_type != 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for store_id field",
                        )
                        .into());
                    }
                    store_id = Some(parse_store_id(&mut data)?);
                }
                2 => {
                    // compression field (enum): 0=Unspecified, 1=None, 2=LZ4
                    if wire_type != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for compression field",
                        )
                        .into());
                    }
                    compression = ArrowCompression::from_u32(read_varint(&mut data)? as u32);
                }
                3 => {
                    // uncompressed_size field (varint)
                    if wire_type != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for uncompressed_size field",
                        )
                        .into());
                    }
                    uncompressed_size = read_varint(&mut data)?;
                }
                4 => {
                    // encoding field (enum): 0=Unspecified, 1=ArrowIpc
                    if wire_type != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for encoding field",
                        )
                        .into());
                    }
                    encoding = ArrowEncoding::from_u32(read_varint(&mut data)? as u32);
                }
                5 => {
                    // payload field (length-delimited bytes)
                    if wire_type != 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for payload field",
                        )
                        .into());
                    }
                    let len = read_varint(&mut data)? as usize;
                    if len > data.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Payload length exceeds remaining data",
                        )
                        .into());
                    }
                    payload = data[..len].to_vec();
                    data = &data[len..];
                }
                6 => {
                    // chunk_id field (message, optional) - Tuid message
                    if wire_type != 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for chunk_id field",
                        )
                        .into());
                    }
                    // Skip chunk_id for now (it's a Tuid with optional time_ns and inc)
                    skip_field(&mut data, wire_type)?;
                }
                7 => {
                    // is_static field (bool, optional)
                    if wire_type != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for is_static field",
                        )
                        .into());
                    }
                    let val = read_varint(&mut data)?;
                    is_static = Some(val != 0);
                }
                _ => {
                    // Skip unknown field
                    skip_field(&mut data, wire_type)?;
                }
            }
        }

        // Default encoding to ArrowIpc if not specified
        if encoding == ArrowEncoding::Unspecified {
            encoding = ArrowEncoding::ArrowIpc;
        }

        Ok(Self {
            store_id,
            compression,
            uncompressed_size,
            encoding,
            payload,
            chunk_id,
            is_static,
        })
    }

    /// Serialize the ArrowMsg to bytes (protobuf format).
    ///
    /// Writes a valid ArrowMsg protobuf that Rerun can read.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Write store_id field (field 1) if present
        if let Some(ref store_id) = self.store_id {
            write_store_id(&mut buf, store_id)?;
        }

        // Write compression field (field 2, varint)
        write_varint(&mut buf, 2 << 3); // tag
        write_varint(&mut buf, self.compression.as_u32() as u64);

        // Write uncompressed_size field (field 3, varint)
        write_varint(&mut buf, 3 << 3); // tag
        write_varint(&mut buf, self.uncompressed_size);

        // Write encoding field (field 4, varint) - must be ArrowIpc=1
        write_varint(&mut buf, 4 << 3); // tag
        write_varint(&mut buf, self.encoding.as_u32() as u64);

        // Write payload field (field 5, length-delimited)
        write_varint(&mut buf, (5 << 3) | 2); // tag
        write_varint(&mut buf, self.payload.len() as u64);
        buf.extend_from_slice(&self.payload);

        // Write is_static field (field 7) if present
        if let Some(is_static) = self.is_static {
            write_varint(&mut buf, 7 << 3); // tag
            write_varint(&mut buf, is_static as u64);
        }

        Ok(buf)
    }

    /// Get the decompressed payload.
    ///
    /// If the payload is compressed, this decompresses it using LZ4.
    /// Otherwise, returns the payload as-is.
    pub fn decompress_payload(&self) -> Result<Vec<u8>> {
        match self.compression {
            ArrowCompression::None | ArrowCompression::Unspecified => Ok(self.payload.clone()),
            ArrowCompression::Lz4 => {
                let decompressed =
                    lz4_flex::block::decompress(&self.payload, self.uncompressed_size as usize)
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("LZ4 decompression failed: {e}"),
                            )
                        })?;

                // Validate decompressed size
                if decompressed.len() as u64 != self.uncompressed_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Decompressed size mismatch: expected {}, got {}",
                            self.uncompressed_size,
                            decompressed.len()
                        ),
                    )
                    .into());
                }

                Ok(decompressed)
            }
        }
    }

    /// Get the compression ratio (compressed_size / uncompressed_size).
    ///
    /// Returns None if compression is not enabled.
    pub fn compression_ratio(&self) -> Option<f64> {
        match self.compression {
            ArrowCompression::None | ArrowCompression::Unspecified => None,
            ArrowCompression::Lz4 => {
                if self.uncompressed_size > 0 {
                    Some(self.payload.len() as f64 / self.uncompressed_size as f64)
                } else {
                    None
                }
            }
        }
    }

    /// Returns true if this ArrowMsg has a store_id set
    pub fn has_store_id(&self) -> bool {
        self.store_id.is_some()
    }

    /// Returns true if this ArrowMsg is marked as static
    pub fn is_static_flag(&self) -> bool {
        self.is_static.unwrap_or(false)
    }
}

/// Parse a StoreId message from bytes.
fn parse_store_id(data: &mut &[u8]) -> Result<StoreId> {
    let len = read_varint(data)? as usize;
    if len > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "StoreId length exceeds remaining data",
        )
        .into());
    }

    let mut store_id_data = &data[..len];
    *data = &data[len..];

    let mut kind = StoreKind::Unspecified;
    let mut recording_id = String::new();
    let mut application_id = String::new();

    while !store_id_data.is_empty() {
        let tag = read_varint(&mut store_id_data)?;
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;

        match field_number {
            1 => {
                // kind (enum)
                if wire_type != 0 {
                    skip_field(&mut store_id_data, wire_type)?;
                } else {
                    kind = StoreKind::from_u32(read_varint(&mut store_id_data)? as u32);
                }
            }
            2 => {
                // recording_id (string)
                if wire_type == 2 {
                    let str_len = read_varint(&mut store_id_data)? as usize;
                    if str_len <= store_id_data.len() {
                        recording_id =
                            String::from_utf8_lossy(&store_id_data[..str_len]).to_string();
                        store_id_data = &store_id_data[str_len..];
                    } else {
                        skip_field(&mut store_id_data, wire_type)?;
                    }
                } else {
                    skip_field(&mut store_id_data, wire_type)?;
                }
            }
            3 => {
                // application_id (message - ApplicationId with string id field)
                if wire_type == 2 {
                    let app_id_len = read_varint(&mut store_id_data)? as usize;
                    if app_id_len <= store_id_data.len() {
                        // ApplicationId is a message with field 1 = id (string)
                        let mut app_id_data = &store_id_data[..app_id_len];
                        store_id_data = &store_id_data[app_id_len..];

                        // Parse ApplicationId
                        while !app_id_data.is_empty() {
                            let app_tag = read_varint(&mut app_id_data)?;
                            let app_field = app_tag >> 3;
                            let app_wire = app_tag & 0x07;

                            if app_field == 1 && app_wire == 2 {
                                let id_len = read_varint(&mut app_id_data)? as usize;
                                if id_len <= app_id_data.len() {
                                    application_id =
                                        String::from_utf8_lossy(&app_id_data[..id_len]).to_string();
                                    app_id_data = &app_id_data[id_len..];
                                } else {
                                    break;
                                }
                            } else {
                                skip_field(&mut app_id_data, app_wire)?;
                            }
                        }
                    } else {
                        skip_field(&mut store_id_data, wire_type)?;
                    }
                } else {
                    skip_field(&mut store_id_data, wire_type)?;
                }
            }
            _ => {
                skip_field(&mut store_id_data, wire_type)?;
            }
        }
    }

    Ok(StoreId {
        kind,
        recording_id,
        application_id,
    })
}

/// Write a StoreId message to a buffer.
fn write_store_id(buf: &mut Vec<u8>, store_id: &StoreId) -> Result<()> {
    // Calculate total length first (for length prefix)
    let mut store_id_buf = Vec::new();

    // Write kind (field 1, varint)
    write_varint(&mut store_id_buf, 1 << 3);
    write_varint(&mut store_id_buf, store_id.kind.as_u32() as u64);

    // Write recording_id (field 2, string)
    if !store_id.recording_id.is_empty() {
        let recording_id_bytes = store_id.recording_id.as_bytes();
        write_varint(&mut store_id_buf, (2 << 3) | 2);
        write_varint(&mut store_id_buf, recording_id_bytes.len() as u64);
        store_id_buf.extend_from_slice(recording_id_bytes);
    }

    // Write application_id (field 3, message) - ApplicationId with id field
    if !store_id.application_id.is_empty() {
        let mut app_id_buf = Vec::new();
        let app_id_bytes = store_id.application_id.as_bytes();
        write_varint(&mut app_id_buf, (1 << 3) | 2);
        write_varint(&mut app_id_buf, app_id_bytes.len() as u64);
        app_id_buf.extend_from_slice(app_id_bytes);

        write_varint(&mut store_id_buf, (3 << 3) | 2);
        write_varint(&mut store_id_buf, app_id_buf.len() as u64);
        store_id_buf.extend_from_slice(&app_id_buf);
    }

    // Write the outer tag and length
    write_varint(buf, (1 << 3) | 2); // tag for field 1 (store_id)
    write_varint(buf, store_id_buf.len() as u64);
    buf.extend_from_slice(&store_id_buf);

    Ok(())
}

/// Read a varint from a slice.
fn read_varint(data: &mut &[u8]) -> io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;

    loop {
        let byte = {
            if data.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected end of varint",
                ));
            }
            let b = data[0];
            *data = &data[1..];
            b
        };

        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }

        if shift >= 70 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Varint too large",
            ));
        }
    }

    Ok(result)
}

/// Write a varint to a buffer.
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Skip a protobuf field based on wire type.
fn skip_field(data: &mut &[u8], wire_type: u64) -> io::Result<()> {
    match wire_type {
        0 => {
            // Varint - skip it
            read_varint(data)?;
        }
        1 => {
            // 64-bit - skip 8 bytes
            if data.len() < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected end of 64-bit field",
                ));
            }
            *data = &data[8..];
        }
        2 => {
            // Length-delimited - skip length bytes
            let len = read_varint(data)? as usize;
            if data.len() < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected end of length-delimited field",
                ));
            }
            *data = &data[len..];
        }
        5 => {
            // 32-bit - skip 4 bytes
            if data.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Unexpected end of 32-bit field",
                ));
            }
            *data = &data[4..];
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown wire type: {}", wire_type),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_compression_from_u32() {
        assert_eq!(ArrowCompression::from_u32(0), ArrowCompression::Unspecified);
        assert_eq!(ArrowCompression::from_u32(1), ArrowCompression::None);
        assert_eq!(ArrowCompression::from_u32(2), ArrowCompression::Lz4);
        assert_eq!(
            ArrowCompression::from_u32(99),
            ArrowCompression::Unspecified
        );
    }

    #[test]
    fn test_arrow_compression_to_u32() {
        assert_eq!(ArrowCompression::Unspecified.as_u32(), 0);
        assert_eq!(ArrowCompression::None.as_u32(), 1);
        assert_eq!(ArrowCompression::Lz4.as_u32(), 2);
    }

    #[test]
    fn test_arrow_compression_is_compressed() {
        assert!(!ArrowCompression::None.is_compressed());
        assert!(!ArrowCompression::Unspecified.is_compressed());
        assert!(ArrowCompression::Lz4.is_compressed());
    }

    #[test]
    fn test_arrow_encoding_from_u32() {
        assert_eq!(ArrowEncoding::from_u32(0), ArrowEncoding::Unspecified);
        assert_eq!(ArrowEncoding::from_u32(1), ArrowEncoding::ArrowIpc);
        assert_eq!(ArrowEncoding::from_u32(99), ArrowEncoding::Unspecified);
    }

    #[test]
    fn test_store_kind_from_u32() {
        assert_eq!(StoreKind::from_u32(0), StoreKind::Unspecified);
        assert_eq!(StoreKind::from_u32(1), StoreKind::Recording);
        assert_eq!(StoreKind::from_u32(2), StoreKind::Blueprint);
    }

    #[test]
    fn test_arrow_msg_new() {
        let payload = b"test data".to_vec();
        let msg = ArrowMsg::new(payload.clone());

        assert_eq!(msg.compression, ArrowCompression::None);
        assert_eq!(msg.encoding, ArrowEncoding::ArrowIpc);
        assert_eq!(msg.uncompressed_size, 9);
        assert_eq!(msg.payload, payload);
        assert!(!msg.has_store_id());
    }

    #[test]
    fn test_arrow_msg_round_trip() {
        let original = ArrowMsg {
            store_id: None,
            compression: ArrowCompression::None,
            uncompressed_size: 100,
            encoding: ArrowEncoding::ArrowIpc,
            payload: vec![1, 2, 3, 4, 5],
            chunk_id: None,
            is_static: None,
        };

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.compression, original.compression);
        assert_eq!(decoded.uncompressed_size, original.uncompressed_size);
        assert_eq!(decoded.encoding, original.encoding);
        assert_eq!(decoded.payload, original.payload);
    }

    #[test]
    fn test_arrow_msg_with_lz4_round_trip() {
        let payload = "Hello, this is a test payload for LZ4 compression! "
            .repeat(100)
            .into_bytes();
        let original = ArrowMsg::with_lz4(payload.clone()).unwrap();

        assert_eq!(original.compression, ArrowCompression::Lz4);
        assert_eq!(original.uncompressed_size, payload.len() as u64);
        assert!(
            original.payload.len() < payload.len(),
            "Payload should be compressed"
        );

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.compression, ArrowCompression::Lz4);
        assert_eq!(decoded.uncompressed_size, payload.len() as u64);
        assert_eq!(decoded.encoding, ArrowEncoding::ArrowIpc);

        let decompressed = decoded.decompress_payload().unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn test_arrow_msg_with_store_id_round_trip() {
        let store_id = StoreId::new_recording("test-recording-123");
        let original = ArrowMsg::new(vec![1, 2, 3]).with_store_id(store_id);

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert!(decoded.has_store_id());
        let decoded_store_id = decoded.store_id.unwrap();
        assert_eq!(decoded_store_id.kind, StoreKind::Recording);
        assert_eq!(decoded_store_id.recording_id, "test-recording-123");
    }

    #[test]
    fn test_arrow_msg_with_is_static_round_trip() {
        let original = ArrowMsg::new(vec![1, 2, 3]).with_is_static(true);

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.is_static, Some(true));
        assert!(decoded.is_static_flag());
    }

    #[test]
    fn test_arrow_msg_decompress_none() {
        let payload = b"test data".to_vec();
        let msg = ArrowMsg::with_compression(payload.clone(), ArrowCompression::None).unwrap();

        let decompressed = msg.decompress_payload().unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn test_arrow_msg_compression_ratio() {
        let payload = b"test data".to_vec();
        let msg = ArrowMsg::with_compression(payload.clone(), ArrowCompression::None).unwrap();
        assert!(msg.compression_ratio().is_none());

        let large_payload = "test data".repeat(100).into_bytes();
        let compressed = ArrowMsg::with_lz4(large_payload).unwrap();
        assert!(compressed.compression_ratio().is_some());
        assert!(compressed.compression_ratio().unwrap() < 1.0);
    }

    #[test]
    fn test_read_varint() {
        let data = [0x96, 0x01]; // 150 in varint encoding
        let mut slice = &data[..];
        assert_eq!(read_varint(&mut slice).unwrap(), 150);
    }

    #[test]
    fn test_write_varint() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 150);
        assert_eq!(buf, vec![0x96, 0x01]);
    }

    #[test]
    fn test_varint_round_trip() {
        let test_values = [0, 1, 127, 128, 150, 255, 256, 16383, 16384, u64::MAX];

        for value in test_values {
            let mut buf = Vec::new();
            write_varint(&mut buf, value);
            let mut slice = buf.as_slice();
            let decoded = read_varint(&mut slice).unwrap();
            assert_eq!(decoded, value, "Failed for value {}", value);
        }
    }

    #[test]
    fn test_encoding_field_written_correctly() {
        let msg = ArrowMsg::new(vec![1, 2, 3]);
        let bytes = msg.to_bytes().unwrap();

        // Parse back and verify encoding is ArrowIpc
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.encoding, ArrowEncoding::ArrowIpc);
    }

    // ===== Edge Case Tests =====

    #[test]
    fn test_empty_payload() {
        let payload = vec![];
        let msg = ArrowMsg::new(payload.clone());
        assert_eq!(msg.uncompressed_size, 0);
        assert!(msg.payload.is_empty());

        let bytes = msg.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_compression_is_none() {
        assert!(ArrowCompression::None.is_none());
        assert!(!ArrowCompression::Lz4.is_none());
        assert!(!ArrowCompression::Unspecified.is_none());
    }

    #[test]
    fn test_compression_unspecified_round_trip() {
        let msg = ArrowMsg {
            store_id: None,
            compression: ArrowCompression::Unspecified,
            uncompressed_size: 100,
            encoding: ArrowEncoding::ArrowIpc,
            payload: vec![1, 2, 3],
            chunk_id: None,
            is_static: None,
        };

        let bytes = msg.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.compression, ArrowCompression::Unspecified);
    }

    #[test]
    fn test_encoding_unspecified_defaults_to_arrowipc() {
        // Create bytes with encoding=0 (Unspecified)
        let mut buf = Vec::new();
        // compression field (field 2)
        write_varint(&mut buf, 2 << 3);
        write_varint(&mut buf, 1u64); // None
        // uncompressed_size (field 3)
        write_varint(&mut buf, 3 << 3);
        write_varint(&mut buf, 3u64);
        // encoding field (field 4) - set to Unspecified (0)
        write_varint(&mut buf, 4 << 3);
        write_varint(&mut buf, 0u64);
        // payload (field 5)
        write_varint(&mut buf, (5 << 3) | 2);
        write_varint(&mut buf, 3u64);
        buf.extend_from_slice(&[1, 2, 3]);

        let decoded = ArrowMsg::from_bytes(&buf).unwrap();
        // Should default to ArrowIpc
        assert_eq!(decoded.encoding, ArrowEncoding::ArrowIpc);
    }

    #[test]
    fn test_is_static_false() {
        let original = ArrowMsg::new(vec![1, 2, 3]).with_is_static(false);

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.is_static, Some(false));
        assert!(!decoded.is_static_flag());
    }

    #[test]
    fn test_is_static_none() {
        let msg = ArrowMsg::new(vec![1, 2, 3]);
        assert_eq!(msg.is_static, None);
        assert!(!msg.is_static_flag());
    }

    #[test]
    fn test_blueprint_store_kind() {
        let store_id = StoreId {
            kind: StoreKind::Blueprint,
            recording_id: "blueprint-123".to_string(),
            application_id: String::new(),
        };

        let original = ArrowMsg::new(vec![1, 2, 3]).with_store_id(store_id);

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.store_id.unwrap().kind, StoreKind::Blueprint);
    }

    #[test]
    fn test_store_id_with_application_id() {
        let store_id = StoreId {
            kind: StoreKind::Recording,
            recording_id: "rec-123".to_string(),
            application_id: "my-app-v1.0".to_string(),
        };

        let original = ArrowMsg::new(vec![1, 2, 3]).with_store_id(store_id.clone());

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        let decoded_store_id = decoded.store_id.unwrap();
        assert_eq!(decoded_store_id.recording_id, "rec-123");
        assert_eq!(decoded_store_id.application_id, "my-app-v1.0");
    }

    #[test]
    fn test_compression_ratio_zero_uncompressed_size() {
        let mut msg = ArrowMsg::new(vec![1, 2, 3]);
        msg.compression = ArrowCompression::Lz4;
        msg.uncompressed_size = 0;

        // Should return None when uncompressed_size is 0
        assert!(msg.compression_ratio().is_none());
    }

    #[test]
    fn test_with_compression_unspecified() {
        let payload = vec![1, 2, 3];
        let msg =
            ArrowMsg::with_compression(payload.clone(), ArrowCompression::Unspecified).unwrap();

        // Unspecified should be treated as None
        assert_eq!(msg.compression, ArrowCompression::None);
        assert_eq!(msg.payload, payload);
    }

    #[test]
    fn test_store_id_empty() {
        let store_id = StoreId::empty();
        assert_eq!(store_id.kind, StoreKind::Recording);
        assert!(store_id.recording_id.is_empty());
        assert!(store_id.application_id.is_empty());
    }

    // ===== Error Path Tests =====

    #[test]
    fn test_invalid_wire_type_for_store_id() {
        let mut buf = Vec::new();
        // Field 1 with wire type 0 (varint) instead of 2 (length-delimited)
        write_varint(&mut buf, 1 << 3);
        write_varint(&mut buf, 123u64);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid wire type for store_id"));
    }

    #[test]
    fn test_invalid_wire_type_for_compression() {
        let mut buf = Vec::new();
        // Field 2 with wire type 2 (length-delimited) instead of 0 (varint)
        write_varint(&mut buf, (2 << 3) | 2);
        write_varint(&mut buf, 4u64);
        buf.extend_from_slice(&[1, 2, 3, 4]);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid wire type for compression"));
    }

    #[test]
    fn test_invalid_wire_type_for_uncompressed_size() {
        let mut buf = Vec::new();
        // Field 3 with wire type 2 (length-delimited) instead of 0 (varint)
        write_varint(&mut buf, (3 << 3) | 2);
        write_varint(&mut buf, 4u64);
        buf.extend_from_slice(&[1, 2, 3, 4]);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid wire type for uncompressed_size"));
    }

    #[test]
    fn test_invalid_wire_type_for_encoding() {
        let mut buf = Vec::new();
        // Field 4 with wire type 2 (length-delimited) instead of 0 (varint)
        write_varint(&mut buf, (4 << 3) | 2);
        write_varint(&mut buf, 4u64);
        buf.extend_from_slice(&[1, 2, 3, 4]);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid wire type for encoding"));
    }

    #[test]
    fn test_invalid_wire_type_for_payload() {
        let mut buf = Vec::new();
        // Field 5 with wire type 0 (varint) instead of 2 (length-delimited)
        write_varint(&mut buf, 5 << 3);
        write_varint(&mut buf, 123u64);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid wire type for payload"));
    }

    #[test]
    fn test_invalid_wire_type_for_is_static() {
        let mut buf = Vec::new();
        // Field 7 with wire type 2 (length-delimited) instead of 0 (varint)
        write_varint(&mut buf, (7 << 3) | 2);
        write_varint(&mut buf, 4u64);
        buf.extend_from_slice(&[1, 2, 3, 4]);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid wire type for is_static"));
    }

    #[test]
    fn test_payload_length_exceeds_data() {
        let mut buf = Vec::new();
        // compression field
        write_varint(&mut buf, 2 << 3);
        write_varint(&mut buf, 1u64);
        // uncompressed_size
        write_varint(&mut buf, 3 << 3);
        write_varint(&mut buf, 100u64);
        // encoding
        write_varint(&mut buf, 4 << 3);
        write_varint(&mut buf, 1u64);
        // payload with length exceeding remaining data
        write_varint(&mut buf, (5 << 3) | 2);
        write_varint(&mut buf, 1000u64); // Claim 1000 bytes but only 0 available
        // No actual payload data

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceeds remaining data") || err_msg.contains("UnexpectedEof"));
    }

    #[test]
    fn test_lz4_decompress_failure_corrupted_data() {
        let msg = ArrowMsg {
            store_id: None,
            compression: ArrowCompression::Lz4,
            uncompressed_size: 1000,
            encoding: ArrowEncoding::ArrowIpc,
            payload: vec![1, 2, 3, 4, 5], // Invalid LZ4 data
            chunk_id: None,
            is_static: None,
        };

        let result = msg.decompress_payload();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("LZ4 decompression failed") || err_msg.contains("decompression"));
    }

    #[test]
    fn test_read_varint_empty_data() {
        let data: &[u8] = &[];
        let mut slice = data;
        let result = read_varint(&mut slice);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unexpected end") || err_msg.contains("varint"));
    }

    #[test]
    fn test_read_varint_incomplete() {
        // Varint with continuation bit set but no following byte
        let data = [0x80]; // Continuation bit set but no more data
        let mut slice = &data[..];
        let result = read_varint(&mut slice);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_field_varint() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 150u64);
        let mut slice = buf.as_slice();
        let result = skip_field(&mut slice, 0);
        assert!(result.is_ok());
        assert!(slice.is_empty());
    }

    #[test]
    fn test_skip_field_64bit() {
        let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 8 bytes for field + extra
        let mut slice = &data[..];
        let result = skip_field(&mut slice, 1);
        assert!(result.is_ok());
        assert_eq!(slice.len(), 2); // 8 bytes skipped, 2 remaining
    }

    #[test]
    fn test_skip_field_64bit_insufficient_data() {
        let data = [1, 2, 3]; // Less than 8 bytes
        let mut slice = &data[..];
        let result = skip_field(&mut slice, 1);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unexpected end") || err_msg.contains("64-bit"));
    }

    #[test]
    fn test_skip_field_length_delimited() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 5u64);
        buf.extend_from_slice(&[1, 2, 3, 4, 5]);
        buf.extend_from_slice(&[9, 10]); // Extra data

        let mut slice = buf.as_slice();
        let result = skip_field(&mut slice, 2);
        assert!(result.is_ok());
        assert_eq!(slice, &[9, 10]); // 5 bytes skipped
    }

    #[test]
    fn test_skip_field_length_delimited_insufficient_data() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 10u64);
        buf.extend_from_slice(&[1, 2, 3]); // Only 3 bytes, claimed 10

        let mut slice = buf.as_slice();
        let result = skip_field(&mut slice, 2);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unexpected end") || err_msg.contains("length-delimited"));
    }

    #[test]
    fn test_skip_field_32bit() {
        let data = [1, 2, 3, 4, 5, 6]; // 4 bytes for field + extra
        let mut slice = &data[..];
        let result = skip_field(&mut slice, 5);
        assert!(result.is_ok());
        assert_eq!(slice.len(), 2); // 4 bytes skipped, 2 remaining
    }

    #[test]
    fn test_skip_field_32bit_insufficient_data() {
        let data = [1, 2]; // Less than 4 bytes
        let mut slice = &data[..];
        let result = skip_field(&mut slice, 5);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unexpected end") || err_msg.contains("32-bit"));
    }

    #[test]
    fn test_skip_field_unknown_wire_type() {
        let data = [1, 2, 3];
        let mut slice = &data[..];
        let result = skip_field(&mut slice, 7); // Invalid wire type
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Unknown wire type") || err_msg.contains("Invalid"));
    }

    #[test]
    fn test_unknown_field_skipped() {
        let mut buf = Vec::new();
        // Known fields
        write_varint(&mut buf, 2 << 3); // compression
        write_varint(&mut buf, 1u64);
        write_varint(&mut buf, 3 << 3); // uncompressed_size
        write_varint(&mut buf, 0u64);
        write_varint(&mut buf, 4 << 3); // encoding
        write_varint(&mut buf, 1u64);
        write_varint(&mut buf, (5 << 3) | 2); // payload
        write_varint(&mut buf, 0u64);
        // Unknown field (field 99)
        write_varint(&mut buf, 99 << 3);
        write_varint(&mut buf, 999u64);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_ok(), "Unknown field should be skipped");
    }

    #[test]
    fn test_store_id_length_exceeds_data() {
        let mut buf = Vec::new();
        // Field 1 (store_id) with length exceeding remaining data
        write_varint(&mut buf, (1 << 3) | 2);
        write_varint(&mut buf, 1000u64); // Claim 1000 bytes
        buf.extend_from_slice(&[1, 2, 3]); // Only 3 bytes available

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("exceeds remaining data") || err_msg.contains("UnexpectedEof"));
    }

    #[test]
    fn test_decompressed_size_mismatch() {
        // Create valid LZ4 data but lie about uncompressed_size
        let payload = b"test data for compression".to_vec();
        let compressed = lz4_flex::block::compress(&payload);

        let msg = ArrowMsg {
            store_id: None,
            compression: ArrowCompression::Lz4,
            uncompressed_size: 9999, // Wrong size
            encoding: ArrowEncoding::ArrowIpc,
            payload: compressed,
            chunk_id: None,
            is_static: None,
        };

        let result = msg.decompress_payload();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("size mismatch"));
    }

    #[test]
    fn test_chunk_id_field_is_skipped() {
        let mut buf = Vec::new();
        // Known fields
        write_varint(&mut buf, 2 << 3); // compression
        write_varint(&mut buf, 1u64);
        write_varint(&mut buf, 3 << 3); // uncompressed_size
        write_varint(&mut buf, 0u64);
        write_varint(&mut buf, 4 << 3); // encoding
        write_varint(&mut buf, 1u64);
        write_varint(&mut buf, (5 << 3) | 2); // payload
        write_varint(&mut buf, 0u64);
        // chunk_id (field 6) - should be skipped
        write_varint(&mut buf, (6 << 3) | 2);
        write_varint(&mut buf, 16u64); // 16-byte TUID
        buf.extend_from_slice(&[1; 16]);

        let result = ArrowMsg::from_bytes(&buf);
        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert!(decoded.chunk_id.is_none()); // chunk_id is skipped and remains None
    }

    #[test]
    fn test_arrow_encoding_as_u32() {
        assert_eq!(ArrowEncoding::Unspecified.as_u32(), 0);
        assert_eq!(ArrowEncoding::ArrowIpc.as_u32(), 1);
    }

    #[test]
    fn test_store_kind_as_u32() {
        assert_eq!(StoreKind::Unspecified.as_u32(), 0);
        assert_eq!(StoreKind::Recording.as_u32(), 1);
        assert_eq!(StoreKind::Blueprint.as_u32(), 2);
    }

    #[test]
    fn test_has_store_id_true() {
        let store_id = StoreId::new_recording("test");
        let msg = ArrowMsg::new(vec![1, 2, 3]).with_store_id(store_id);
        assert!(msg.has_store_id());
    }

    #[test]
    fn test_roundtrip_with_all_fields() {
        let store_id = StoreId {
            kind: StoreKind::Recording,
            recording_id: "rec-123".to_string(),
            application_id: "app-456".to_string(),
        };

        let original = ArrowMsg {
            store_id: Some(store_id),
            compression: ArrowCompression::None,
            uncompressed_size: 42,
            encoding: ArrowEncoding::ArrowIpc,
            payload: vec![1, 2, 3, 4, 5],
            chunk_id: None,
            is_static: Some(true),
        };

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.store_id.as_ref().unwrap().recording_id, "rec-123");
        assert_eq!(decoded.store_id.as_ref().unwrap().application_id, "app-456");
        assert_eq!(decoded.uncompressed_size, 42);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4, 5]);
        assert_eq!(decoded.is_static, Some(true));
    }
}
