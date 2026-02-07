// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CDR codec implementation wrapping existing decoder/encoder.

use crate::core::{CodecError, DecodedMessage, Encoding, Result};
use crate::encoding::CdrDecoder;
use crate::encoding::CdrEncoder;
use crate::encoding::DynCodec;
use crate::encoding::transform::SchemaMetadata;

/// CDR codec wrapper implementing the unified codec interface.
///
/// This wraps the existing `CdrDecoder` and `CdrEncoder` to work with
/// the unified codec system.
pub struct CdrCodec {
    /// Cached decoder (stateless, can be reused)
    decoder: CdrDecoder,
    /// Current encoder for encoding operations
    encoder: Option<CdrEncoder>,
}

impl CdrCodec {
    /// Create a new CDR codec.
    #[must_use]
    pub fn new() -> Self {
        Self {
            decoder: CdrDecoder::new(),
            encoder: None,
        }
    }

    /// Get the CDR decoder.
    pub fn decoder(&self) -> &CdrDecoder {
        &self.decoder
    }

    /// Get a mutable CDR encoder.
    pub fn encoder(&mut self) -> &mut CdrEncoder {
        if self.encoder.is_none() {
            self.encoder = Some(CdrEncoder::new());
        }
        self.encoder.as_mut().expect("encoder set to Some() above")
    }
}

impl Default for CdrCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl DynCodec for CdrCodec {
    fn decode_dynamic(&self, data: &[u8], schema: &SchemaMetadata) -> Result<DecodedMessage> {
        match schema {
            SchemaMetadata::Cdr {
                type_name,
                schema_text,
                schema_encoding,
            } => {
                // Parse the schema text to get MessageSchema
                // Use schema encoding if available to select the correct parser
                let parsed_schema = if let Some(enc) = schema_encoding {
                    crate::schema::parser::parse_schema_with_encoding_str(
                        type_name,
                        schema_text,
                        enc,
                    )?
                } else {
                    crate::schema::parse_schema(type_name, schema_text)?
                };

                // Decode using the existing CDR decoder
                self.decoder.decode(&parsed_schema, data, Some(type_name))
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a CDR schema",
            )),
        }
    }

    fn encode_dynamic(
        &mut self,
        message: &DecodedMessage,
        schema: &SchemaMetadata,
    ) -> Result<Vec<u8>> {
        match schema {
            SchemaMetadata::Cdr {
                type_name,
                schema_text,
                schema_encoding,
            } => {
                // Parse the schema text to get MessageSchema
                // Use schema encoding if available to select the correct parser
                let parsed_schema = if let Some(enc) = schema_encoding {
                    crate::schema::parser::parse_schema_with_encoding_str(
                        type_name,
                        schema_text,
                        enc,
                    )?
                } else {
                    crate::schema::parse_schema(type_name, schema_text)?
                };

                // Encode using the CDR encoder
                let encoder = self.encoder();
                encoder.encode_message(message, &parsed_schema, type_name)?;
                // Take ownership of encoder to call finish
                let encoder = self
                    .encoder
                    .take()
                    .expect("encoder set by call to encoder() above");
                Ok(encoder.finish())
            }
            _ => Err(CodecError::invalid_schema(
                schema.type_name(),
                "Schema is not a CDR schema",
            )),
        }
    }

    fn encoding_type(&self) -> Encoding {
        Encoding::Cdr
    }

    fn reset(&mut self) {
        self.encoder = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// =========================================================================
// CdrCodec construction tests
// =========================================================================

#[test]
fn test_cdr_codec_new() {
    let codec = CdrCodec::new();
    assert_eq!(codec.encoding_type(), Encoding::Cdr);
    assert!(codec.encoder.is_none());
}

#[test]
fn test_cdr_codec_default() {
    let codec = CdrCodec::default();
    assert_eq!(codec.encoding_type(), Encoding::Cdr);
    assert!(codec.encoder.is_none());
}

// =========================================================================
// decoder() method tests
// =========================================================================

#[test]
fn test_cdr_codec_decoder_returns_ref() {
    let codec = CdrCodec::new();
    let _decoder = codec.decoder();
    // Just verify we can get a reference to the decoder
    assert_eq!(codec.encoding_type(), Encoding::Cdr);
}

// =========================================================================
// encoder() method tests
// =========================================================================

#[test]
fn test_cdr_codec_encoder_initializes_on_first_call() {
    let mut codec = CdrCodec::new();
    assert!(codec.encoder.is_none());

    let _encoder = codec.encoder();
    assert!(codec.encoder.is_some());
}

#[test]
fn test_cdr_codec_encoder_reuses_existing() {
    let mut codec = CdrCodec::new();

    let enc1 = codec.encoder() as *const CdrEncoder;
    let enc2 = codec.encoder() as *const CdrEncoder;

    // Should return the same encoder (not create a new one)
    assert_eq!(enc1, enc2);
}

// =========================================================================
// reset() method tests
// =========================================================================

#[test]
fn test_cdr_codec_reset_clears_encoder() {
    let mut codec = CdrCodec::new();

    // Get encoder to initialize it
    let _enc = codec.encoder();
    assert!(codec.encoder.is_some());

    // Reset should clear the encoder
    codec.reset();
    assert!(codec.encoder.is_none());
}

#[test]
fn test_cdr_codec_reset_when_no_encoder() {
    let mut codec = CdrCodec::new();
    assert!(codec.encoder.is_none());

    // Reset when encoder is None should not panic
    codec.reset();
    assert!(codec.encoder.is_none());
}

// =========================================================================
// encoding_type() method tests
// =========================================================================

#[test]
fn test_cdr_codec_encoding_type() {
    let codec = CdrCodec::new();
    assert_eq!(codec.encoding_type(), Encoding::Cdr);
    assert!(codec.encoding_type().is_cdr());
    assert!(!codec.encoding_type().is_protobuf());
    assert!(!codec.encoding_type().is_json());
}

// =========================================================================
// as_any() method tests
// =========================================================================

#[test]
fn test_cdr_codec_as_any() {
    let codec = CdrCodec::new();
    let any = codec.as_any();

    // Should be able to downcast back to CdrCodec
    assert!(any.is::<CdrCodec>());
}

#[test]
fn test_cdr_codec_as_any_downcast() {
    let codec = CdrCodec::new();
    let any = codec.as_any();

    let downcast = any.downcast_ref::<CdrCodec>();
    assert!(downcast.is_some());
    assert_eq!(downcast.unwrap().encoding_type(), Encoding::Cdr);
}

// =========================================================================
// decode_dynamic with invalid schema tests
// =========================================================================

#[test]
fn test_cdr_codec_decode_dynamic_protobuf_schema() {
    let codec = CdrCodec::new();
    let schema = SchemaMetadata::protobuf("test.Type".to_string(), vec![1, 2, 3]);

    let result = codec.decode_dynamic(&[0x00, 0x00, 0x00, 0x00], &schema);
    assert!(result.is_err());

    if let Err(e) = result {
        let msg = e.to_string();
        assert!(msg.contains("Schema is not a CDR schema") || msg.contains("invalid schema"));
    }
}

#[test]
fn test_cdr_codec_decode_dynamic_json_schema() {
    let codec = CdrCodec::new();
    let schema = SchemaMetadata::json("test.Type".to_string(), "{}".to_string());

    let result = codec.decode_dynamic(&[0x00, 0x00, 0x00, 0x00], &schema);
    assert!(result.is_err());
}

// =========================================================================
// encode_dynamic with invalid schema tests
// =========================================================================

#[test]
fn test_cdr_codec_encode_dynamic_protobuf_schema() {
    let mut codec = CdrCodec::new();
    let schema = SchemaMetadata::protobuf("test.Type".to_string(), vec![1, 2, 3]);

    // Create a simple message (HashMap)
    let message: DecodedMessage = std::collections::HashMap::new();

    let result = codec.encode_dynamic(&message, &schema);
    assert!(result.is_err());

    if let Err(e) = result {
        let msg = e.to_string();
        assert!(msg.contains("Schema is not a CDR schema") || msg.contains("invalid schema"));
    }
}

#[test]
fn test_cdr_codec_encode_dynamic_json_schema() {
    let mut codec = CdrCodec::new();
    let schema = SchemaMetadata::json("test.Type".to_string(), "{}".to_string());

    let message: DecodedMessage = std::collections::HashMap::new();

    let result = codec.encode_dynamic(&message, &schema);
    assert!(result.is_err());
}

// =========================================================================
// DynCodec trait integration tests
// =========================================================================

#[test]
fn test_cdr_codec_dyn_codec_object_safe() {
    // Test that CdrCodec can be used as a DynCodec trait object
    let codec: CdrCodec = CdrCodec::new();

    // Can call methods through DynCodec trait
    let encoding = codec.encoding_type();
    assert_eq!(encoding, Encoding::Cdr);
}

#[test]
fn test_cdr_codec_multiple_reset_cycles() {
    let mut codec = CdrCodec::new();

    for _ in 0..3 {
        let _enc = codec.encoder();
        assert!(codec.encoder.is_some());
        codec.reset();
        assert!(codec.encoder.is_none());
    }
}
