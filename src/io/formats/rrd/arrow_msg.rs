// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! ArrowMsg protobuf encoding/decoding with LZ4 compression support.
//!
//! This module handles the ArrowMsg format used by Rerun's RRD files.
//! ArrowMsg contains potentially LZ4-compressed Arrow IPC data.
//!
//! # ArrowMsg Format
//!
//! ```text
//! ArrowMsg (Protobuf):
//!   - compression: int32 (0=Off, 1=LZ4)
//!   - uncompressed_size: uint64
//!   - payload: bytes (Arrow IPC data, potentially compressed)
//! ```

use std::io;

use crate::core::Result;

/// Compression type for ArrowMsg payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrowCompression {
    Off = 0,
    Lz4 = 1,
}

impl ArrowCompression {
    /// Create from u32 value.
    pub fn from_u32(value: u32) -> Self {
        const OFF: u32 = 0;
        const LZ4: u32 = 1;

        match value {
            OFF => Self::Off,
            LZ4 => Self::Lz4,
            _ => Self::Off, // Default to Off for unknown values
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
}

/// ArrowMsg protobuf structure.
///
/// This represents a Rerun ArrowMsg message with potentially compressed
/// Arrow IPC data as the payload.
#[derive(Debug, Clone)]
pub struct ArrowMsg {
    /// Compression type for the payload
    pub compression: ArrowCompression,
    /// Uncompressed size of the payload (bytes)
    pub uncompressed_size: u64,
    /// Payload data (Arrow IPC, potentially LZ4 compressed)
    pub payload: Vec<u8>,
}

impl ArrowMsg {
    /// Create a new ArrowMsg with uncompressed payload.
    pub fn new(payload: Vec<u8>) -> Self {
        let uncompressed_size = payload.len() as u64;
        Self {
            compression: ArrowCompression::Off,
            uncompressed_size,
            payload,
        }
    }

    /// Create a new ArrowMsg with LZ4 compressed payload.
    pub fn with_lz4(payload: Vec<u8>) -> Result<Self> {
        let uncompressed_size = payload.len() as u64;
        let compressed = lz4_flex::block::compress(&payload);
        Ok(Self {
            compression: ArrowCompression::Lz4,
            uncompressed_size,
            payload: compressed,
        })
    }

    /// Parse an ArrowMsg from bytes (protobuf format).
    ///
    /// This implements a minimal protobuf parser for the ArrowMsg format.
    /// The format is:
    /// ```text
    /// field 1 (compression): varint, tag=0x08
    /// field 2 (uncompressed_size): varint, tag=0x10
    /// field 3 (payload): length-delimited, tag=0x1A
    /// ```
    pub fn from_bytes(mut data: &[u8]) -> Result<Self> {
        let mut compression = ArrowCompression::Off;
        let mut uncompressed_size = 0u64;
        let mut payload = Vec::new();

        while !data.is_empty() {
            // Read tag (field_number << 3 | wire_type)
            let tag = read_varint(&mut data)?;
            let field_number = tag >> 3;
            let wire_type = tag & 0x07;

            match field_number {
                1 => {
                    // compression field (varint)
                    if wire_type != 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid wire type for compression field",
                        )
                        .into());
                    }
                    compression = ArrowCompression::from_u32(read_varint(&mut data)? as u32);
                }
                2 => {
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
                3 => {
                    // payload field (length-delimited)
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
                _ => {
                    // Skip unknown field
                    skip_field(&mut data, wire_type)?;
                }
            }
        }

        Ok(Self {
            compression,
            uncompressed_size,
            payload,
        })
    }

    /// Serialize the ArrowMsg to bytes (protobuf format).
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Write compression field (field 1, varint)
        write_varint(&mut buf, (1 << 3) | 0); // tag
        write_varint(&mut buf, self.compression.as_u32() as u64);

        // Write uncompressed_size field (field 2, varint)
        write_varint(&mut buf, (2 << 3) | 0); // tag
        write_varint(&mut buf, self.uncompressed_size);

        // Write payload field (field 3, length-delimited)
        write_varint(&mut buf, (3 << 3) | 2); // tag
        write_varint(&mut buf, self.payload.len() as u64);
        buf.extend_from_slice(&self.payload);

        Ok(buf)
    }

    /// Get the decompressed payload.
    ///
    /// If the payload is compressed, this decompresses it using LZ4.
    /// Otherwise, returns the payload as-is.
    pub fn decompress_payload(&self) -> Result<Vec<u8>> {
        match self.compression {
            ArrowCompression::Off => Ok(self.payload.clone()),
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
    /// Returns None if compression is Off.
    pub fn compression_ratio(&self) -> Option<f64> {
        match self.compression {
            ArrowCompression::Off => None,
            ArrowCompression::Lz4 => {
                if self.uncompressed_size > 0 {
                    Some(self.payload.len() as f64 / self.uncompressed_size as f64)
                } else {
                    None
                }
            }
        }
    }
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
        assert_eq!(ArrowCompression::from_u32(0), ArrowCompression::Off);
        assert_eq!(ArrowCompression::from_u32(1), ArrowCompression::Lz4);
        assert_eq!(ArrowCompression::from_u32(99), ArrowCompression::Off); // Default
    }

    #[test]
    fn test_arrow_compression_to_u32() {
        assert_eq!(ArrowCompression::Off.as_u32(), 0);
        assert_eq!(ArrowCompression::Lz4.as_u32(), 1);
    }

    #[test]
    fn test_arrow_compression_is_compressed() {
        assert!(!ArrowCompression::Off.is_compressed());
        assert!(ArrowCompression::Lz4.is_compressed());
    }

    #[test]
    fn test_arrow_msg_new() {
        let payload = b"test data".to_vec();
        let msg = ArrowMsg::new(payload.clone());

        assert_eq!(msg.compression, ArrowCompression::Off);
        assert_eq!(msg.uncompressed_size, 9);
        assert_eq!(msg.payload, payload);
    }

    #[test]
    fn test_arrow_msg_round_trip() {
        let original = ArrowMsg {
            compression: ArrowCompression::Off,
            uncompressed_size: 100,
            payload: vec![1, 2, 3, 4, 5],
        };

        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.compression, original.compression);
        assert_eq!(decoded.uncompressed_size, original.uncompressed_size);
        assert_eq!(decoded.payload, original.payload);
    }

    #[test]
    fn test_arrow_msg_with_lz4_round_trip() {
        // Use a larger payload that will actually compress with LZ4
        let payload = "Hello, this is a test payload for LZ4 compression! "
            .repeat(100)
            .into_bytes();
        let original = ArrowMsg::with_lz4(payload.clone()).unwrap();

        // Verify compression actually happened
        assert_eq!(original.compression, ArrowCompression::Lz4);
        assert_eq!(original.uncompressed_size, payload.len() as u64);
        assert!(
            original.payload.len() < payload.len(),
            "Payload should be compressed"
        );

        // Round trip
        let bytes = original.to_bytes().unwrap();
        let decoded = ArrowMsg::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.compression, ArrowCompression::Lz4);
        assert_eq!(decoded.uncompressed_size, payload.len() as u64);

        // Decompress and verify
        let decompressed = decoded.decompress_payload().unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn test_arrow_msg_decompress_off() {
        let payload = b"test data".to_vec();
        let msg = ArrowMsg::new(payload.clone());

        let decompressed = msg.decompress_payload().unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn test_arrow_msg_compression_ratio() {
        // Small payload - no compression ratio available
        let payload = b"test data".to_vec();
        let msg = ArrowMsg::new(payload.clone());
        assert!(msg.compression_ratio().is_none());

        // Larger payload - LZ4 should compress
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
}
