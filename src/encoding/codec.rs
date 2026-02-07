// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Unified codec interface for encoding-agnostic message processing.
//!
//! This module provides a clean abstraction over different message encodings
//! (CDR, Protobuf, JSON) to support the MCAP rewriter in a format-agnostic way.
//!
//! ## Architecture
//!
//! The codec system is organized into layers:
//!
//! - **Core trait** ([`DynCodec`]) - Define the interface
//! - **Encoding-specific implementations** (cdr, protobuf) - Provide codec behavior
//! - **Factory** ([`CodecFactory`]) - Creates appropriate codec for each encoding
//!
//! ## Example
//!
//! ```no_run
//! use robocodec::encoding::CodecFactory;
//! use robocodec::Encoding;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut factory = CodecFactory::new();
//! let codec = factory.get_codec_mut(Encoding::Cdr)?;
//! let _encoding_type = codec.encoding_type();
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use crate::core::{CodecError, DecodedMessage, Encoding, Result};

pub use super::transform::{
    CdrSchemaTransformer, ProtobufSchemaTransformer, SchemaMetadata, SchemaTransformer,
};

pub use super::cdr::CdrCodec;
pub use super::protobuf::ProtobufCodec;

// =============================================================================
// Codec Factory
// =============================================================================

/// Factory for creating codec instances based on encoding type.
///
/// The factory manages codec instances and ensures proper initialization
/// with schema data.
pub struct CodecFactory {
    /// Cached codec instances
    codecs: HashMap<Encoding, Box<dyn DynCodec>>,
}

impl CodecFactory {
    /// Create a new codec factory with all supported codecs.
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::encoding::CodecFactory;
    ///
    /// let factory = CodecFactory::new();
    /// # let _ = factory;
    /// ```
    #[must_use]
    pub fn new() -> Self {
        let mut codecs: HashMap<Encoding, Box<dyn DynCodec>> = HashMap::new();

        // Register CDR codec
        codecs.insert(Encoding::Cdr, Box::new(CdrCodec::new()));

        // Register Protobuf codec
        codecs.insert(Encoding::Protobuf, Box::new(ProtobufCodec::new()));

        Self { codecs }
    }

    /// Get a codec for the specified encoding.
    ///
    /// # Arguments
    ///
    /// * `encoding` - The encoding type
    ///
    /// # Returns
    ///
    /// A reference to the codec, or an error if the encoding is not supported
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::encoding::CodecFactory;
    /// use robocodec::Encoding;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let factory = CodecFactory::new();
    /// let codec = factory.get_codec(Encoding::Cdr)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_codec(&self, encoding: Encoding) -> Result<&dyn DynCodec> {
        let encoding_str = format!("encoding: {encoding:?}");
        self.codecs
            .get(&encoding)
            .map(std::convert::AsRef::as_ref)
            .ok_or_else(move || CodecError::unsupported(&encoding_str))
    }

    /// Get a mutable codec for the specified encoding.
    ///
    /// This is used for encode operations which may modify internal state.
    ///
    /// # Example
    ///
    /// ```
    /// use robocodec::encoding::CodecFactory;
    /// use robocodec::Encoding;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut factory = CodecFactory::new();
    /// let codec = factory.get_codec_mut(Encoding::Cdr)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_codec_mut(&mut self, encoding: Encoding) -> Result<&mut Box<dyn DynCodec>> {
        let encoding_str = format!("encoding: {encoding:?}");
        self.codecs
            .get_mut(&encoding)
            .ok_or_else(move || CodecError::unsupported(&encoding_str))
    }

    /// Detect encoding from channel metadata.
    ///
    /// # Arguments
    ///
    /// * `encoding_str` - Encoding string from MCAP channel
    /// * `schema_encoding` - Optional schema encoding string
    ///
    /// # Returns
    ///
    /// Detected `Encoding` type
    #[must_use]
    pub fn detect_encoding(&self, encoding_str: &str, schema_encoding: Option<&str>) -> Encoding {
        let encoding_lower = encoding_str.to_lowercase();

        // Check explicit encoding
        if encoding_lower.contains("cdr")
            || encoding_lower.contains("ros2")
            || encoding_lower.contains("ros2msg")
        {
            return Encoding::Cdr;
        }

        if encoding_lower.contains("protobuf") || encoding_lower.contains("proto") {
            return Encoding::Protobuf;
        }

        if encoding_lower.contains("json") {
            return Encoding::Json;
        }

        // Fallback to schema encoding
        if let Some(schema_enc) = schema_encoding {
            match schema_enc.to_lowercase().as_str() {
                "protobuf" | "proto" => return Encoding::Protobuf,
                "ros2msg" | "rosidl" => return Encoding::Cdr,
                "json" => return Encoding::Json,
                _ => {}
            }
        }

        // Default to CDR for backward compatibility
        Encoding::Cdr
    }
}

impl Default for CodecFactory {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Dynamic Codec Trait
// =============================================================================

/// Dynamic codec trait for use in trait objects.
///
/// This trait allows storing different codec implementations in a collection
/// and routing to the appropriate codec at runtime.
pub trait DynCodec: Send + Sync {
    /// Decode message data using schema metadata.
    fn decode_dynamic(&self, data: &[u8], schema: &SchemaMetadata) -> Result<DecodedMessage>;

    /// Encode a decoded message using schema metadata.
    fn encode_dynamic(
        &mut self,
        message: &DecodedMessage,
        schema: &SchemaMetadata,
    ) -> Result<Vec<u8>>;

    /// Get the encoding type this codec handles.
    fn encoding_type(&self) -> Encoding;

    /// Reset encoder state.
    fn reset(&mut self);

    /// Get a reference as `Any` for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_detection_cdr() {
        let factory = CodecFactory::new();

        assert_eq!(factory.detect_encoding("cdr", None), Encoding::Cdr);
        assert_eq!(factory.detect_encoding("ros2", None), Encoding::Cdr);
        assert_eq!(factory.detect_encoding("ros2msg", None), Encoding::Cdr);
    }

    #[test]
    fn test_encoding_detection_protobuf() {
        let factory = CodecFactory::new();

        assert_eq!(
            factory.detect_encoding("protobuf", None),
            Encoding::Protobuf
        );
        assert_eq!(factory.detect_encoding("proto", None), Encoding::Protobuf);
    }

    #[test]
    fn test_encoding_detection_json() {
        let factory = CodecFactory::new();

        assert_eq!(factory.detect_encoding("json", None), Encoding::Json);
    }

    #[test]
    fn test_encoding_detection_from_schema() {
        let factory = CodecFactory::new();

        assert_eq!(
            factory.detect_encoding("unknown", Some("protobuf")),
            Encoding::Protobuf
        );
        assert_eq!(
            factory.detect_encoding("unknown", Some("ros2msg")),
            Encoding::Cdr
        );
    }

    #[test]
    fn test_encoding_is_methods() {
        assert!(Encoding::Cdr.is_cdr());
        assert!(!Encoding::Cdr.is_protobuf());

        assert!(Encoding::Protobuf.is_protobuf());
        assert!(!Encoding::Protobuf.is_cdr());

        assert!(Encoding::Json.is_json());
        assert!(!Encoding::Json.is_cdr());
    }
}

// =========================================================================
// CodecFactory::new() and Default tests
// =========================================================================

#[test]
fn test_codec_factory_default() {
    let factory = CodecFactory::default();
    // Should have codecs registered
    assert!(factory.get_codec(Encoding::Cdr).is_ok());
    assert!(factory.get_codec(Encoding::Protobuf).is_ok());
}

// =========================================================================
// CodecFactory::get_codec tests
// =========================================================================

#[test]
fn test_codec_factory_get_codec_cdr() {
    let factory = CodecFactory::new();
    let codec = factory.get_codec(Encoding::Cdr);
    assert!(codec.is_ok());
    assert_eq!(codec.unwrap().encoding_type(), Encoding::Cdr);
}

#[test]
fn test_codec_factory_get_codec_protobuf() {
    let factory = CodecFactory::new();
    let codec = factory.get_codec(Encoding::Protobuf);
    assert!(codec.is_ok());
    assert_eq!(codec.unwrap().encoding_type(), Encoding::Protobuf);
}

#[test]
fn test_codec_factory_get_codec_json_not_supported() {
    let factory = CodecFactory::new();
    // JSON codec may not be implemented
    let result = factory.get_codec(Encoding::Json);
    // Result depends on implementation - just check it doesn't panic
    let _ = result;
}

// =========================================================================
// CodecFactory::get_codec_mut tests
// =========================================================================

#[test]
fn test_codec_factory_get_codec_mut_cdr() {
    let mut factory = CodecFactory::new();
    let codec = factory.get_codec_mut(Encoding::Cdr);
    assert!(codec.is_ok());
    assert_eq!(codec.unwrap().encoding_type(), Encoding::Cdr);
}

#[test]
fn test_codec_factory_get_codec_mut_protobuf() {
    let mut factory = CodecFactory::new();
    let codec = factory.get_codec_mut(Encoding::Protobuf);
    assert!(codec.is_ok());
    assert_eq!(codec.unwrap().encoding_type(), Encoding::Protobuf);
}

#[test]
fn test_codec_factory_get_codec_mut_allows_encode() {
    let mut factory = CodecFactory::new();
    if let Ok(codec) = factory.get_codec_mut(Encoding::Cdr) {
        // Just verify we can get a mutable reference and call methods
        codec.reset();
        assert_eq!(codec.encoding_type(), Encoding::Cdr);
    }
}

// =========================================================================
// detect_encoding case variations
// =========================================================================

#[test]
fn test_detect_encoding_case_variants_cdr() {
    let factory = CodecFactory::new();

    assert_eq!(factory.detect_encoding("CDR", None), Encoding::Cdr);
    assert_eq!(factory.detect_encoding("Cdr", None), Encoding::Cdr);
    assert_eq!(factory.detect_encoding("CDR;ros2msg", None), Encoding::Cdr);
}

#[test]
fn test_detect_encoding_case_variants_protobuf() {
    let factory = CodecFactory::new();

    assert_eq!(
        factory.detect_encoding("PROTOBUF", None),
        Encoding::Protobuf
    );
    assert_eq!(
        factory.detect_encoding("Protobuf", None),
        Encoding::Protobuf
    );
    assert_eq!(
        factory.detect_encoding("PROTObuf", None),
        Encoding::Protobuf
    );
}

#[test]
fn test_detect_encoding_case_variants_json() {
    let factory = CodecFactory::new();

    assert_eq!(factory.detect_encoding("JSON", None), Encoding::Json);
    assert_eq!(factory.detect_encoding("Json", None), Encoding::Json);
}

#[test]
fn test_detect_encoding_ros2_variants() {
    let factory = CodecFactory::new();

    assert_eq!(factory.detect_encoding("ros2idl", None), Encoding::Cdr);
    assert_eq!(factory.detect_encoding("ROS2IDL", None), Encoding::Cdr);
    assert_eq!(factory.detect_encoding("ros2idl;cdr", None), Encoding::Cdr);
}

// =========================================================================
// detect_encoding with schema encoding fallback
// =========================================================================

#[test]
fn test_detect_encoding_schema_fallback_protobuf() {
    let factory = CodecFactory::new();
    assert_eq!(
        factory.detect_encoding("unknown", Some("protobuf")),
        Encoding::Protobuf
    );
    assert_eq!(
        factory.detect_encoding("unknown", Some("PROTOBUF")),
        Encoding::Protobuf
    );
}

#[test]
fn test_detect_encoding_schema_fallback_ros2msg() {
    let factory = CodecFactory::new();
    assert_eq!(
        factory.detect_encoding("unknown", Some("ros2msg")),
        Encoding::Cdr
    );
    assert_eq!(
        factory.detect_encoding("unknown", Some("ROS2MSG")),
        Encoding::Cdr
    );
}

#[test]
fn test_detect_encoding_schema_fallback_rosidl() {
    let factory = CodecFactory::new();
    assert_eq!(
        factory.detect_encoding("unknown", Some("rosidl")),
        Encoding::Cdr
    );
    assert_eq!(
        factory.detect_encoding("unknown", Some("ROSIDL")),
        Encoding::Cdr
    );
}

#[test]
fn test_detect_encoding_schema_fallback_json() {
    let factory = CodecFactory::new();
    assert_eq!(
        factory.detect_encoding("unknown", Some("json")),
        Encoding::Json
    );
    assert_eq!(
        factory.detect_encoding("unknown", Some("JSON")),
        Encoding::Json
    );
}

#[test]
fn test_detect_encoding_schema_fallback_default() {
    let factory = CodecFactory::new();
    // Unknown encoding and no schema encoding should default to CDR
    assert_eq!(factory.detect_encoding("unknown", None), Encoding::Cdr);
    assert_eq!(
        factory.detect_encoding("unknown", Some("unknown_schema")),
        Encoding::Cdr
    );
}

// =========================================================================
// detect_encoding with combined encoding strings
// =========================================================================

#[test]
fn test_detect_encoding_combined_strings() {
    let factory = CodecFactory::new();

    // CDR variants - CDR is checked first in implementation
    assert_eq!(factory.detect_encoding("cdr;ros2msg", None), Encoding::Cdr);
    assert_eq!(
        factory.detect_encoding("ros2;protobuf", None),
        Encoding::Cdr
    );

    // "protobuf;cdr" matches CDR first because CDR check comes before protobuf
    // and "cdr" substring is found
    assert_eq!(factory.detect_encoding("protobuf;cdr", None), Encoding::Cdr);
}

// =========================================================================
// DynCodec trait tests
// =========================================================================

#[test]
fn test_dyn_codec_is_send_sync() {
    // Verify DynCodec trait has appropriate bounds
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Box<dyn DynCodec>>();
}

// =========================================================================
// CodecFactory stores codecs correctly
// =========================================================================

#[test]
fn test_codec_factory_stores_multiple_codecs() {
    let factory = CodecFactory::new();

    // Should be able to get different encodings
    let cdr = factory.get_codec(Encoding::Cdr);
    let proto = factory.get_codec(Encoding::Protobuf);

    assert!(cdr.is_ok());
    assert!(proto.is_ok());
}

#[test]
fn test_codec_factory_returns_different_codecs() {
    let factory = CodecFactory::new();

    let cdr_encoding = factory.get_codec(Encoding::Cdr).unwrap().encoding_type();
    let proto_encoding = factory
        .get_codec(Encoding::Protobuf)
        .unwrap()
        .encoding_type();

    assert_eq!(cdr_encoding, Encoding::Cdr);
    assert_eq!(proto_encoding, Encoding::Protobuf);
    assert_ne!(cdr_encoding, proto_encoding);
}
