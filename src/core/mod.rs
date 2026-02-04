// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Core types used throughout robocodec.
//!
//! This module provides the foundational types for the library:
//! - [`CodecError`] - Comprehensive error handling
//! - [`CodecValue`] - Unified value representation
//! - [`TypeRegistry`] - Schema type registry
//! - [`Encoding`] - Message encoding format identifier

pub mod error;
pub mod registry;
pub mod value;

pub use error::{CodecError, Result};
pub use registry::{SchemaProvider, TypeAccessor, TypeRegistry};
pub use value::{CodecValue, DecodedMessage, PrimitiveType};

/// Encoding format identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// CDR (Common Data Representation) encoding
    Cdr,
    /// Protobuf encoding
    Protobuf,
    /// JSON encoding
    Json,
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Encoding enum tests
    // =========================================================================

    #[test]
    fn test_encoding_cdr_variant_exists() {
        let enc = Encoding::Cdr;
        assert_eq!(enc.as_str(), "cdr");
        assert!(enc.is_cdr());
        assert!(!enc.is_protobuf());
        assert!(!enc.is_json());
    }

    #[test]
    fn test_encoding_protobuf_variant_exists() {
        let enc = Encoding::Protobuf;
        assert_eq!(enc.as_str(), "protobuf");
        assert!(!enc.is_cdr());
        assert!(enc.is_protobuf());
        assert!(!enc.is_json());
    }

    #[test]
    fn test_encoding_json_variant_exists() {
        let enc = Encoding::Json;
        assert_eq!(enc.as_str(), "json");
        assert!(!enc.is_cdr());
        assert!(!enc.is_protobuf());
        assert!(enc.is_json());
    }

    #[test]
    fn test_encoding_is_cdr() {
        assert!(Encoding::Cdr.is_cdr());
        assert!(!Encoding::Protobuf.is_cdr());
        assert!(!Encoding::Json.is_cdr());
    }

    #[test]
    fn test_encoding_is_protobuf() {
        assert!(!Encoding::Cdr.is_protobuf());
        assert!(Encoding::Protobuf.is_protobuf());
        assert!(!Encoding::Json.is_protobuf());
    }

    #[test]
    fn test_encoding_is_json() {
        assert!(!Encoding::Cdr.is_json());
        assert!(!Encoding::Protobuf.is_json());
        assert!(Encoding::Json.is_json());
    }

    #[test]
    fn test_encoding_as_str_cdr() {
        assert_eq!(Encoding::Cdr.as_str(), "cdr");
    }

    #[test]
    fn test_encoding_as_str_protobuf() {
        assert_eq!(Encoding::Protobuf.as_str(), "protobuf");
    }

    #[test]
    fn test_encoding_as_str_json() {
        assert_eq!(Encoding::Json.as_str(), "json");
    }

    // =========================================================================
    // Encoding derive traits tests
    // =========================================================================

    #[test]
    fn test_encoding_debug() {
        assert!(format!("{:?}", Encoding::Cdr).contains("Cdr"));
    }

    #[test]
    fn test_encoding_clone() {
        let enc = Encoding::Protobuf;
        let cloned = enc.clone();
        assert_eq!(enc, cloned);
    }

    #[test]
    fn test_encoding_copy() {
        let enc = Encoding::Json;
        let copied = enc;
        assert_eq!(enc, copied);
    }

    #[test]
    fn test_encoding_partial_eq() {
        assert_eq!(Encoding::Cdr, Encoding::Cdr);
        assert_ne!(Encoding::Cdr, Encoding::Protobuf);
    }

    #[test]
    fn test_encoding_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Encoding::Cdr);
        set.insert(Encoding::Protobuf);
        set.insert(Encoding::Json);
        assert_eq!(set.len(), 3);
    }

    // =========================================================================
    // FromStr tests
    // =========================================================================

    #[test]
    fn test_encoding_from_str_cdr_lower() {
        assert_eq!("cdr".parse::<Encoding>(), Ok(Encoding::Cdr));
    }

    #[test]
    fn test_encoding_from_str_cdr_upper() {
        assert_eq!("CDR".parse::<Encoding>(), Ok(Encoding::Cdr));
    }

    #[test]
    fn test_encoding_from_str_cdr_mixed() {
        assert_eq!("CdR".parse::<Encoding>(), Ok(Encoding::Cdr));
    }

    #[test]
    fn test_encoding_from_str_protobuf_lower() {
        assert_eq!("protobuf".parse::<Encoding>(), Ok(Encoding::Protobuf));
    }

    #[test]
    fn test_encoding_from_str_protobuf_upper() {
        assert_eq!("PROTOBUF".parse::<Encoding>(), Ok(Encoding::Protobuf));
    }

    #[test]
    fn test_encoding_from_str_protobuf_mixed() {
        assert_eq!("ProToBuf".parse::<Encoding>(), Ok(Encoding::Protobuf));
    }

    #[test]
    fn test_encoding_from_str_json_lower() {
        assert_eq!("json".parse::<Encoding>(), Ok(Encoding::Json));
    }

    #[test]
    fn test_encoding_from_str_json_upper() {
        assert_eq!("JSON".parse::<Encoding>(), Ok(Encoding::Json));
    }

    #[test]
    fn test_encoding_from_str_json_mixed() {
        assert_eq!("JsOn".parse::<Encoding>(), Ok(Encoding::Json));
    }

    #[test]
    fn test_encoding_from_str_invalid_empty() {
        assert!("".parse::<Encoding>().is_err());
    }

    #[test]
    fn test_encoding_from_str_invalid_random() {
        assert!("xml".parse::<Encoding>().is_err());
        assert!("binary".parse::<Encoding>().is_err());
        assert!("cbor".parse::<Encoding>().is_err());
    }

    #[test]
    fn test_encoding_from_str_invalid_whitespace() {
        assert!(" cdr ".parse::<Encoding>().is_err());
        assert!("cdr ".parse::<Encoding>().is_err());
        assert!(" cdr".parse::<Encoding>().is_err());
    }

    // =========================================================================
    // ParseEncodingError tests
    // =========================================================================

    #[test]
    fn test_parse_encoding_error_display() {
        let err = ParseEncodingError { _private: () };
        let msg = format!("{}", err);
        assert!(msg.contains("invalid encoding name"));
        assert!(msg.contains("cdr"));
        assert!(msg.contains("protobuf"));
        assert!(msg.contains("json"));
    }

    #[test]
    fn test_parse_encoding_error_debug() {
        let err = ParseEncodingError { _private: () };
        assert!(format!("{:?}", err).contains("ParseEncodingError"));
    }

    #[test]
    fn test_parse_encoding_error_clone() {
        let err = ParseEncodingError { _private: () };
        let cloned = err.clone();
        let _ = cloned;
    }

    #[test]
    fn test_parse_encoding_error_copy() {
        let err = ParseEncodingError { _private: () };
        let copied = err;
        let _ = copied;
    }

    #[test]
    fn test_parse_encoding_error_partial_eq() {
        let err1 = ParseEncodingError { _private: () };
        let err2 = ParseEncodingError { _private: () };
        assert_eq!(err1, err2);
    }
}

/// Error returned when parsing an `Encoding` from string fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseEncodingError {
    _private: (),
}

impl std::fmt::Display for ParseEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid encoding name, expected 'cdr', 'protobuf', or 'json'"
        )
    }
}

impl std::error::Error for ParseEncodingError {}

impl std::str::FromStr for Encoding {
    type Err = ParseEncodingError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cdr" => Ok(Encoding::Cdr),
            "protobuf" => Ok(Encoding::Protobuf),
            "json" => Ok(Encoding::Json),
            _ => Err(ParseEncodingError { _private: () }),
        }
    }
}

impl Encoding {
    /// Check if this encoding is CDR.
    pub fn is_cdr(&self) -> bool {
        matches!(self, Encoding::Cdr)
    }

    /// Check if this encoding is Protobuf.
    pub fn is_protobuf(&self) -> bool {
        matches!(self, Encoding::Protobuf)
    }

    /// Check if this encoding is JSON.
    pub fn is_json(&self) -> bool {
        matches!(self, Encoding::Json)
    }

    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Cdr => "cdr",
            Encoding::Protobuf => "protobuf",
            Encoding::Json => "json",
        }
    }
}
