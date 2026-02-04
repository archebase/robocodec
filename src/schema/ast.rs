// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! AST types for parsed ROS .msg schemas.

use std::collections::HashMap;

/// A parsed ROS message schema.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageSchema {
    /// Schema name (e.g., "std_msgs/msg/Header" or just "Header")
    pub name: String,
    /// Package name (e.g., "std_msgs")
    pub package: Option<String>,
    /// All types defined in this schema (main type + nested types)
    pub types: HashMap<String, MessageType>,
}

/// A message type definition with its fields.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageType {
    /// Type name including package if available
    pub name: String,
    /// Ordered list of fields
    pub fields: Vec<Field>,
    /// Maximum alignment required for this type
    pub max_alignment: u64,
}

/// A field in a message type.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Field type
    pub type_name: FieldType,
}

/// Field type - can be primitive, array, or nested message.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    /// Primitive type
    Primitive(PrimitiveType),
    /// Array type
    Array {
        /// Base type (element type)
        base_type: Box<FieldType>,
        /// Array size (None = dynamic, Some(N) = fixed)
        size: Option<usize>,
    },
    /// Nested message type
    Nested(String),
}

/// Primitive ROS types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    /// Boolean
    Bool,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 8-bit unsigned integer
    UInt8,
    /// 16-bit unsigned integer
    UInt16,
    /// 32-bit unsigned integer
    UInt32,
    /// 64-bit unsigned integer
    UInt64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    Float64,
    /// String
    String,
    /// Wide string (UTF-16)
    WString,
    /// Byte (alias for UInt8)
    Byte,
    /// Char (alias for Int8)
    Char,
    /// Time (ROS timestamp: sec:int32, nsec:uint32)
    Time,
    /// Duration (ROS duration: sec:int32, nsec:uint32)
    Duration,
}

impl PrimitiveType {
    /// Get the alignment requirement for this primitive type.
    pub fn alignment(self) -> u64 {
        match self {
            PrimitiveType::Bool
            | PrimitiveType::Int8
            | PrimitiveType::UInt8
            | PrimitiveType::Byte
            | PrimitiveType::Char => 1,
            PrimitiveType::Int16 | PrimitiveType::UInt16 => 2,
            PrimitiveType::Int32 | PrimitiveType::UInt32 | PrimitiveType::Float32 => 4,
            PrimitiveType::Int64 | PrimitiveType::UInt64 | PrimitiveType::Float64 => 8,
            PrimitiveType::String | PrimitiveType::WString => 4, // Length prefix is 4-byte aligned
            PrimitiveType::Time | PrimitiveType::Duration => 4,  // 8 bytes total, 4-byte alignment
        }
    }

    /// Get the size in bytes for this primitive type, if fixed.
    pub fn size(self) -> Option<usize> {
        match self {
            PrimitiveType::Bool => Some(1),
            PrimitiveType::Int8
            | PrimitiveType::UInt8
            | PrimitiveType::Byte
            | PrimitiveType::Char => Some(1),
            PrimitiveType::Int16 | PrimitiveType::UInt16 => Some(2),
            PrimitiveType::Int32 | PrimitiveType::UInt32 | PrimitiveType::Float32 => Some(4),
            PrimitiveType::Int64 | PrimitiveType::UInt64 | PrimitiveType::Float64 => Some(8),
            PrimitiveType::String | PrimitiveType::WString => None, // Variable length
            PrimitiveType::Time | PrimitiveType::Duration => Some(8), // sec:int32 + nsec:uint32
        }
    }

    /// Parse a primitive type from a string.
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "bool" | "boolean" => Some(PrimitiveType::Bool),
            "int8" => Some(PrimitiveType::Int8),
            "int16" => Some(PrimitiveType::Int16),
            "int32" => Some(PrimitiveType::Int32),
            "int64" => Some(PrimitiveType::Int64),
            "uint8" => Some(PrimitiveType::UInt8),
            "uint16" => Some(PrimitiveType::UInt16),
            "uint32" => Some(PrimitiveType::UInt32),
            "uint64" => Some(PrimitiveType::UInt64),
            "float32" | "float" => Some(PrimitiveType::Float32),
            "float64" | "double" => Some(PrimitiveType::Float64),
            "string" => Some(PrimitiveType::String),
            "wstring" => Some(PrimitiveType::WString),
            "byte" => Some(PrimitiveType::Byte),
            "char" => Some(PrimitiveType::Char),
            "time" => Some(PrimitiveType::Time),
            "duration" => Some(PrimitiveType::Duration),
            _ => None,
        }
    }

    /// Convert to the core PrimitiveType.
    pub fn to_core(self) -> crate::PrimitiveType {
        match self {
            PrimitiveType::Bool => crate::PrimitiveType::Bool,
            PrimitiveType::Int8 => crate::PrimitiveType::Int8,
            PrimitiveType::Int16 => crate::PrimitiveType::Int16,
            PrimitiveType::Int32 => crate::PrimitiveType::Int32,
            PrimitiveType::Int64 => crate::PrimitiveType::Int64,
            PrimitiveType::UInt8 => crate::PrimitiveType::UInt8,
            PrimitiveType::UInt16 => crate::PrimitiveType::UInt16,
            PrimitiveType::UInt32 => crate::PrimitiveType::UInt32,
            PrimitiveType::UInt64 => crate::PrimitiveType::UInt64,
            PrimitiveType::Float32 => crate::PrimitiveType::Float32,
            PrimitiveType::Float64 => crate::PrimitiveType::Float64,
            PrimitiveType::String | PrimitiveType::WString => crate::PrimitiveType::String,
            PrimitiveType::Byte | PrimitiveType::Char => crate::PrimitiveType::Byte,
            PrimitiveType::Time | PrimitiveType::Duration => crate::PrimitiveType::Int64, // Fallback
        }
    }
}

impl FieldType {
    /// Get the alignment requirement for this field type.
    pub fn alignment(&self) -> u64 {
        match self {
            FieldType::Primitive(p) => p.alignment(),
            FieldType::Array { base_type, .. } => base_type.alignment(),
            FieldType::Nested(_) => 4, // Nested structs have 4-byte alignment in CDR
        }
    }

    /// Check if this is a complex type (requires per-element alignment in arrays).
    pub fn is_complex(&self) -> bool {
        !matches!(
            self,
            FieldType::Primitive(
                PrimitiveType::Bool
                    | PrimitiveType::Int8
                    | PrimitiveType::UInt8
                    | PrimitiveType::Byte
                    | PrimitiveType::Char
                    | PrimitiveType::Int16
                    | PrimitiveType::UInt16
                    | PrimitiveType::Int32
                    | PrimitiveType::UInt32
                    | PrimitiveType::Float32
                    | PrimitiveType::Int64
                    | PrimitiveType::UInt64
                    | PrimitiveType::Float64
            )
        )
    }
}

impl MessageSchema {
    /// Create an empty schema.
    pub fn new(name: String) -> Self {
        Self {
            package: extract_package(&name),
            name,
            types: HashMap::new(),
        }
    }

    /// Register a type in this schema.
    pub fn add_type(&mut self, msg_type: MessageType) {
        self.types.insert(msg_type.name.clone(), msg_type);
    }

    /// Look up a type by name.
    pub fn get_type(&self, name: &str) -> Option<&MessageType> {
        self.types.get(name)
    }

    /// Look up a type by name with variant resolution.
    pub fn get_type_variants(&self, name: &str) -> Option<&MessageType> {
        // Try exact match first
        if let Some(t) = self.types.get(name) {
            return Some(t);
        }

        // Convert :: to / (IDL uses :: but we store with /)
        let normalized_name = name.replace("::", "/");

        // Try with normalized name
        if let Some(t) = self.types.get(&normalized_name) {
            return Some(t);
        }

        // Try with /msg/ suffix
        if !normalized_name.contains("/msg/") {
            let with_msg = normalized_name.replace('/', "/msg/");
            if let Some(t) = self.types.get(&with_msg) {
                return Some(t);
            }
        }

        // Try without /msg/ suffix
        if normalized_name.contains("/msg/") {
            let without_msg = normalized_name.replace("/msg/", "/");
            if let Some(t) = self.types.get(&without_msg) {
                return Some(t);
            }
        }

        // Try short name match
        if !normalized_name.contains('/') {
            for (full_name, msg_type) in &self.types {
                if full_name.ends_with(&format!("/{normalized_name}"))
                    || full_name.ends_with(&format!("/msg/{normalized_name}"))
                    || full_name.as_str() == normalized_name
                {
                    return Some(msg_type);
                }
            }
        }

        None
    }

    /// Rename all types in the schema by applying a package name transformation.
    ///
    /// This updates:
    /// - The schema's own name and package
    /// - All type names in the types HashMap
    /// - All nested type references in field types
    ///
    /// # Arguments
    ///
    /// * `old_package` - The old package name (e.g., "genie_msgs")
    /// * `new_package` - The new package name (e.g., "archebase")
    pub fn rename_package(&mut self, old_package: &str, new_package: &str) {
        // Update schema name
        self.name = self
            .name
            .replace(&format!("{old_package}/"), &format!("{new_package}/"));
        self.name = self
            .name
            .replace(&format!("{old_package}::"), &format!("{new_package}::"));

        // Update package field
        if self.package.as_deref() == Some(old_package) {
            self.package = Some(new_package.to_string());
        }

        // Build new types HashMap with updated keys and values
        let mut new_types = HashMap::new();
        for (old_key, mut msg_type) in self.types.drain() {
            // Update the type's name
            let new_key = old_key.replace(&format!("{old_package}/"), &format!("{new_package}/"));
            let new_key = new_key.replace(&format!("{old_package}::"), &format!("{new_package}::"));

            msg_type.name = msg_type
                .name
                .replace(&format!("{old_package}/"), &format!("{new_package}/"));
            msg_type.name = msg_type
                .name
                .replace(&format!("{old_package}::"), &format!("{new_package}::"));

            // Update field type references
            for field in &mut msg_type.fields {
                Self::rename_field_type(&mut field.type_name, old_package, new_package);
            }

            new_types.insert(new_key, msg_type);
        }
        self.types = new_types;
    }

    /// Rename package in a field type recursively.
    fn rename_field_type(field_type: &mut FieldType, old_package: &str, new_package: &str) {
        match field_type {
            FieldType::Nested(type_name) => {
                *type_name =
                    type_name.replace(&format!("{old_package}/"), &format!("{new_package}/"));
                *type_name =
                    type_name.replace(&format!("{old_package}::"), &format!("{new_package}::"));
            }
            FieldType::Array { base_type, .. } => {
                Self::rename_field_type(base_type, old_package, new_package);
            }
            FieldType::Primitive(_) => {}
        }
    }
}

impl MessageType {
    /// Create a new message type.
    pub fn new(name: String) -> Self {
        Self {
            name,
            fields: Vec::new(),
            max_alignment: 1,
        }
    }

    /// Add a field to this message type.
    pub fn add_field(&mut self, field: Field) {
        // Update max alignment
        let field_alignment = field.type_name.alignment();
        self.max_alignment = self.max_alignment.max(field_alignment);
        self.fields.push(field);
    }
}

/// Extract package name from a fully-qualified type name.
fn extract_package(name: &str) -> Option<String> {
    if name.contains('/') {
        let parts: Vec<&str> = name.split('/').collect();
        if parts.len() >= 2 {
            Some(parts[0].to_string())
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // PrimitiveType::try_from_str tests
    // =========================================================================

    #[test]
    fn test_primitive_type_from_str() {
        assert_eq!(
            PrimitiveType::try_from_str("int32"),
            Some(PrimitiveType::Int32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("float64"),
            Some(PrimitiveType::Float64)
        );
        assert_eq!(PrimitiveType::try_from_str("unknown"), None);
    }

    #[test]
    fn test_primitive_type_from_str_bool_variants() {
        assert_eq!(
            PrimitiveType::try_from_str("bool"),
            Some(PrimitiveType::Bool)
        );
        assert_eq!(
            PrimitiveType::try_from_str("boolean"),
            Some(PrimitiveType::Bool)
        );
    }

    #[test]
    fn test_primitive_type_from_str_float_variants() {
        assert_eq!(
            PrimitiveType::try_from_str("float32"),
            Some(PrimitiveType::Float32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("float"),
            Some(PrimitiveType::Float32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("float64"),
            Some(PrimitiveType::Float64)
        );
        assert_eq!(
            PrimitiveType::try_from_str("double"),
            Some(PrimitiveType::Float64)
        );
    }

    #[test]
    fn test_primitive_type_from_str_all_int_types() {
        assert_eq!(
            PrimitiveType::try_from_str("int8"),
            Some(PrimitiveType::Int8)
        );
        assert_eq!(
            PrimitiveType::try_from_str("int16"),
            Some(PrimitiveType::Int16)
        );
        assert_eq!(
            PrimitiveType::try_from_str("int32"),
            Some(PrimitiveType::Int32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("int64"),
            Some(PrimitiveType::Int64)
        );
        assert_eq!(
            PrimitiveType::try_from_str("uint8"),
            Some(PrimitiveType::UInt8)
        );
        assert_eq!(
            PrimitiveType::try_from_str("uint16"),
            Some(PrimitiveType::UInt16)
        );
        assert_eq!(
            PrimitiveType::try_from_str("uint32"),
            Some(PrimitiveType::UInt32)
        );
        assert_eq!(
            PrimitiveType::try_from_str("uint64"),
            Some(PrimitiveType::UInt64)
        );
    }

    #[test]
    fn test_primitive_type_from_str_special_types() {
        assert_eq!(
            PrimitiveType::try_from_str("string"),
            Some(PrimitiveType::String)
        );
        assert_eq!(
            PrimitiveType::try_from_str("wstring"),
            Some(PrimitiveType::WString)
        );
        assert_eq!(
            PrimitiveType::try_from_str("byte"),
            Some(PrimitiveType::Byte)
        );
        assert_eq!(
            PrimitiveType::try_from_str("char"),
            Some(PrimitiveType::Char)
        );
        assert_eq!(
            PrimitiveType::try_from_str("time"),
            Some(PrimitiveType::Time)
        );
        assert_eq!(
            PrimitiveType::try_from_str("duration"),
            Some(PrimitiveType::Duration)
        );
    }

    #[test]
    fn test_primitive_type_from_str_invalid() {
        assert_eq!(PrimitiveType::try_from_str(""), None);
        assert_eq!(PrimitiveType::try_from_str("invalid"), None);
        assert_eq!(PrimitiveType::try_from_str("INT32"), None); // case sensitive
        assert_eq!(PrimitiveType::try_from_str("Float32"), None);
    }

    // =========================================================================
    // PrimitiveType::alignment tests
    // =========================================================================

    #[test]
    fn test_primitive_type_alignment() {
        assert_eq!(PrimitiveType::Bool.alignment(), 1);
        assert_eq!(PrimitiveType::Int16.alignment(), 2);
        assert_eq!(PrimitiveType::Int32.alignment(), 4);
        assert_eq!(PrimitiveType::Int64.alignment(), 8);
        assert_eq!(PrimitiveType::String.alignment(), 4);
    }

    #[test]
    fn test_primitive_type_alignment_1_byte() {
        assert_eq!(PrimitiveType::Bool.alignment(), 1);
        assert_eq!(PrimitiveType::Int8.alignment(), 1);
        assert_eq!(PrimitiveType::UInt8.alignment(), 1);
        assert_eq!(PrimitiveType::Byte.alignment(), 1);
        assert_eq!(PrimitiveType::Char.alignment(), 1);
    }

    #[test]
    fn test_primitive_type_alignment_2_byte() {
        assert_eq!(PrimitiveType::Int16.alignment(), 2);
        assert_eq!(PrimitiveType::UInt16.alignment(), 2);
    }

    #[test]
    fn test_primitive_type_alignment_4_byte() {
        assert_eq!(PrimitiveType::Int32.alignment(), 4);
        assert_eq!(PrimitiveType::UInt32.alignment(), 4);
        assert_eq!(PrimitiveType::Float32.alignment(), 4);
        assert_eq!(PrimitiveType::String.alignment(), 4);
        assert_eq!(PrimitiveType::WString.alignment(), 4);
        assert_eq!(PrimitiveType::Time.alignment(), 4);
        assert_eq!(PrimitiveType::Duration.alignment(), 4);
    }

    #[test]
    fn test_primitive_type_alignment_8_byte() {
        assert_eq!(PrimitiveType::Int64.alignment(), 8);
        assert_eq!(PrimitiveType::UInt64.alignment(), 8);
        assert_eq!(PrimitiveType::Float64.alignment(), 8);
    }

    // =========================================================================
    // PrimitiveType::size tests
    // =========================================================================

    #[test]
    fn test_primitive_type_size_fixed() {
        assert_eq!(PrimitiveType::Bool.size(), Some(1));
        assert_eq!(PrimitiveType::Int8.size(), Some(1));
        assert_eq!(PrimitiveType::Int16.size(), Some(2));
        assert_eq!(PrimitiveType::Int32.size(), Some(4));
        assert_eq!(PrimitiveType::Int64.size(), Some(8));
    }

    #[test]
    fn test_primitive_type_size_variable() {
        assert_eq!(PrimitiveType::String.size(), None);
        assert_eq!(PrimitiveType::WString.size(), None);
    }

    #[test]
    fn test_primitive_type_size_time() {
        assert_eq!(PrimitiveType::Time.size(), Some(8));
        assert_eq!(PrimitiveType::Duration.size(), Some(8));
    }

    #[test]
    fn test_primitive_type_size_floats() {
        assert_eq!(PrimitiveType::Float32.size(), Some(4));
        assert_eq!(PrimitiveType::Float64.size(), Some(8));
    }

    // =========================================================================
    // PrimitiveType::to_core tests
    // =========================================================================

    #[test]
    fn test_primitive_type_to_core_basic() {
        assert_eq!(PrimitiveType::Bool.to_core(), crate::PrimitiveType::Bool);
        assert_eq!(PrimitiveType::Int32.to_core(), crate::PrimitiveType::Int32);
        assert_eq!(
            PrimitiveType::Float64.to_core(),
            crate::PrimitiveType::Float64
        );
    }

    #[test]
    fn test_primitive_type_to_core_string() {
        assert_eq!(
            PrimitiveType::String.to_core(),
            crate::PrimitiveType::String
        );
        assert_eq!(
            PrimitiveType::WString.to_core(),
            crate::PrimitiveType::String
        );
    }

    #[test]
    fn test_primitive_type_to_core_byte_char() {
        assert_eq!(PrimitiveType::Byte.to_core(), crate::PrimitiveType::Byte);
        assert_eq!(PrimitiveType::Char.to_core(), crate::PrimitiveType::Byte);
    }

    #[test]
    fn test_primitive_type_to_core_time_duration() {
        // Time and Duration fallback to Int64
        assert_eq!(PrimitiveType::Time.to_core(), crate::PrimitiveType::Int64);
        assert_eq!(
            PrimitiveType::Duration.to_core(),
            crate::PrimitiveType::Int64
        );
    }

    // =========================================================================
    // FieldType::is_complex tests
    // =========================================================================

    #[test]
    fn test_field_type_is_complex() {
        assert!(!FieldType::Primitive(PrimitiveType::Int32).is_complex());
        assert!(FieldType::Primitive(PrimitiveType::String).is_complex());
        assert!(
            FieldType::Array {
                base_type: Box::new(FieldType::Primitive(PrimitiveType::Int32)),
                size: None,
            }
            .is_complex()
        );
    }

    #[test]
    fn test_field_type_is_complex_primitive_numeric() {
        // All numeric primitives are not complex
        assert!(!FieldType::Primitive(PrimitiveType::Bool).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Int8).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::UInt8).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Byte).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Char).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Int16).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::UInt16).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Int32).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::UInt32).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Float32).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Int64).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::UInt64).is_complex());
        assert!(!FieldType::Primitive(PrimitiveType::Float64).is_complex());
    }

    #[test]
    fn test_field_type_is_complex_string_and_nested() {
        assert!(FieldType::Primitive(PrimitiveType::String).is_complex());
        assert!(FieldType::Primitive(PrimitiveType::WString).is_complex());
        assert!(FieldType::Primitive(PrimitiveType::Time).is_complex());
        assert!(FieldType::Primitive(PrimitiveType::Duration).is_complex());
        assert!(FieldType::Nested("some/Type".to_string()).is_complex());
    }

    #[test]
    fn test_field_type_is_complex_array() {
        // Array of primitive is complex (needs per-element alignment)
        let arr = FieldType::Array {
            base_type: Box::new(FieldType::Primitive(PrimitiveType::Int32)),
            size: Some(10),
        };
        assert!(arr.is_complex());
    }

    // =========================================================================
    // FieldType::alignment tests
    // =========================================================================

    #[test]
    fn test_field_type_alignment_primitive() {
        assert_eq!(FieldType::Primitive(PrimitiveType::Bool).alignment(), 1);
        assert_eq!(FieldType::Primitive(PrimitiveType::Int32).alignment(), 4);
        assert_eq!(FieldType::Primitive(PrimitiveType::Int64).alignment(), 8);
    }

    #[test]
    fn test_field_type_alignment_nested() {
        assert_eq!(FieldType::Nested("some/Type".to_string()).alignment(), 4);
    }

    #[test]
    fn test_field_type_alignment_array() {
        let arr = FieldType::Array {
            base_type: Box::new(FieldType::Primitive(PrimitiveType::Int64)),
            size: None,
        };
        assert_eq!(arr.alignment(), 8); // Uses base type alignment
    }

    // =========================================================================
    // MessageSchema tests
    // =========================================================================

    #[test]
    fn test_message_schema_new() {
        let schema = MessageSchema::new("std_msgs/msg/String".to_string());
        assert_eq!(schema.name, "std_msgs/msg/String");
        assert_eq!(schema.package, Some("std_msgs".to_string()));
        assert!(schema.types.is_empty());
    }

    #[test]
    fn test_message_schema_new_no_package() {
        let schema = MessageSchema::new("String".to_string());
        assert_eq!(schema.name, "String");
        assert!(schema.package.is_none());
    }

    #[test]
    fn test_message_schema_add_type() {
        let mut schema = MessageSchema::new("test/Msg".to_string());
        let msg_type = MessageType::new("test/Msg".to_string());
        schema.add_type(msg_type);
        assert!(schema.types.contains_key("test/Msg"));
    }

    #[test]
    fn test_message_schema_get_type() {
        let mut schema = MessageSchema::new("test/Msg".to_string());
        let msg_type = MessageType::new("test/Msg".to_string());
        schema.add_type(msg_type);
        assert!(schema.get_type("test/Msg").is_some());
        assert!(schema.get_type("nonexistent").is_none());
    }

    #[test]
    fn test_message_schema_get_type_variants_exact_match() {
        let mut schema = MessageSchema::new("test/Msg".to_string());
        let msg_type = MessageType::new("test/Msg".to_string());
        schema.add_type(msg_type);
        assert!(schema.get_type_variants("test/Msg").is_some());
    }

    #[test]
    fn test_message_schema_get_type_variants_with_msg_separator() {
        let mut schema = MessageSchema::new("test/Msg".to_string());
        let msg_type = MessageType::new("test/Msg".to_string());
        schema.add_type(msg_type);
        // Without /msg/ should find type with /msg/ equivalent
        assert!(schema.get_type_variants("test/msg/Msg").is_some());
    }

    #[test]
    fn test_message_schema_get_type_variants_without_msg_separator() {
        let mut schema = MessageSchema::new("test/msg/Msg".to_string());
        let msg_type = MessageType::new("test/msg/Msg".to_string());
        schema.add_type(msg_type);
        // With /msg/ removed should find
        assert!(schema.get_type_variants("test/Msg").is_some());
    }

    #[test]
    fn test_message_schema_get_type_variants_double_colon() {
        let mut schema = MessageSchema::new("test/Msg".to_string());
        let msg_type = MessageType::new("test/Msg".to_string());
        schema.add_type(msg_type);
        // IDL-style :: should work
        assert!(schema.get_type_variants("test::Msg").is_some());
    }

    #[test]
    fn test_message_schema_get_type_variants_short_name() {
        let mut schema = MessageSchema::new("std_msgs/msg/Header".to_string());
        let msg_type = MessageType::new("std_msgs/msg/Header".to_string());
        schema.add_type(msg_type);
        // Short name should work
        assert!(schema.get_type_variants("Header").is_some());
    }

    #[test]
    fn test_message_schema_rename_package() {
        let mut schema = MessageSchema::new("old_pkg/Msg".to_string());
        schema.package = Some("old_pkg".to_string());

        let mut msg_type = MessageType::new("old_pkg/Msg".to_string());
        msg_type.fields.push(Field {
            name: "data".to_string(),
            type_name: FieldType::Nested("old_pkg/Nested".to_string()),
        });
        schema.add_type(msg_type);

        schema.rename_package("old_pkg", "new_pkg");

        assert_eq!(schema.name, "new_pkg/Msg");
        assert_eq!(schema.package, Some("new_pkg".to_string()));
        assert!(schema.types.contains_key("new_pkg/Msg"));
        assert!(!schema.types.contains_key("old_pkg/Msg"));
    }

    #[test]
    fn test_message_schema_rename_package_double_colon() {
        let mut schema = MessageSchema::new("old_pkg::Msg".to_string());
        schema.package = Some("old_pkg".to_string());

        let mut msg_type = MessageType::new("old_pkg::Msg".to_string());
        msg_type.fields.push(Field {
            name: "nested".to_string(),
            type_name: FieldType::Nested("old_pkg::Nested".to_string()),
        });
        schema.add_type(msg_type);

        schema.rename_package("old_pkg", "new_pkg");

        assert_eq!(schema.name, "new_pkg::Msg");
        assert!(schema.types.contains_key("new_pkg::Msg"));
    }

    #[test]
    fn test_message_schema_rename_package_array_field() {
        let mut schema = MessageSchema::new("old_pkg/Msg".to_string());
        schema.package = Some("old_pkg".to_string());

        let mut msg_type = MessageType::new("old_pkg/Msg".to_string());
        msg_type.fields.push(Field {
            name: "items".to_string(),
            type_name: FieldType::Array {
                base_type: Box::new(FieldType::Nested("old_pkg/Item".to_string())),
                size: None,
            },
        });
        schema.add_type(msg_type);

        schema.rename_package("old_pkg", "new_pkg");

        let updated_type = schema.get_type("new_pkg/Msg").unwrap();
        if let FieldType::Array { base_type, .. } = &updated_type.fields[0].type_name
            && let FieldType::Nested(name) = base_type.as_ref()
        {
            assert!(name.starts_with("new_pkg/"));
        }
    }

    // =========================================================================
    // MessageType tests
    // =========================================================================

    #[test]
    fn test_message_type_new() {
        let msg_type = MessageType::new("test/Msg".to_string());
        assert_eq!(msg_type.name, "test/Msg");
        assert!(msg_type.fields.is_empty());
        assert_eq!(msg_type.max_alignment, 1);
    }

    #[test]
    fn test_message_type_add_field() {
        let mut msg_type = MessageType::new("test/Msg".to_string());
        msg_type.add_field(Field {
            name: "data".to_string(),
            type_name: FieldType::Primitive(PrimitiveType::Int32),
        });
        assert_eq!(msg_type.fields.len(), 1);
        assert_eq!(msg_type.fields[0].name, "data");
    }

    #[test]
    fn test_message_type_add_field_updates_alignment() {
        let mut msg_type = MessageType::new("test/Msg".to_string());
        msg_type.add_field(Field {
            name: "a".to_string(),
            type_name: FieldType::Primitive(PrimitiveType::Int8),
        });
        assert_eq!(msg_type.max_alignment, 1);

        msg_type.add_field(Field {
            name: "b".to_string(),
            type_name: FieldType::Primitive(PrimitiveType::Int64),
        });
        assert_eq!(msg_type.max_alignment, 8);
    }

    // =========================================================================
    // Field tests
    // =========================================================================

    #[test]
    fn test_field_new() {
        let field = Field {
            name: "test_field".to_string(),
            type_name: FieldType::Primitive(PrimitiveType::Float32),
        };
        assert_eq!(field.name, "test_field");
    }

    // =========================================================================
    // Extract package function tests
    // =========================================================================

    #[test]
    fn test_extract_package_from_fully_qualified() {
        // The function is private but tested through MessageSchema::new
        let schema = MessageSchema::new("std_msgs/msg/String".to_string());
        assert_eq!(schema.package, Some("std_msgs".to_string()));
    }

    #[test]
    fn test_extract_package_no_separator() {
        let schema = MessageSchema::new("String".to_string());
        assert!(schema.package.is_none());
    }

    #[test]
    fn test_extract_package_single_part() {
        let schema = MessageSchema::new("single".to_string());
        assert!(schema.package.is_none());
    }

    // =========================================================================
    // PartialEq tests for AST types
    // =========================================================================

    #[test]
    fn test_primitive_type_partial_eq() {
        assert_eq!(PrimitiveType::Int32, PrimitiveType::Int32);
        assert_ne!(PrimitiveType::Int32, PrimitiveType::Int64);
    }

    #[test]
    fn test_field_type_partial_eq() {
        let ft1 = FieldType::Primitive(PrimitiveType::Int32);
        let ft2 = FieldType::Primitive(PrimitiveType::Int32);
        assert_eq!(ft1, ft2);

        let ft3 = FieldType::Primitive(PrimitiveType::Int64);
        assert_ne!(ft1, ft3);
    }

    #[test]
    fn test_field_type_partial_eq_array() {
        let arr1 = FieldType::Array {
            base_type: Box::new(FieldType::Primitive(PrimitiveType::Int32)),
            size: Some(10),
        };
        let arr2 = FieldType::Array {
            base_type: Box::new(FieldType::Primitive(PrimitiveType::Int32)),
            size: Some(10),
        };
        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_message_type_partial_eq() {
        let mt1 = MessageType {
            name: "test/Msg".to_string(),
            fields: vec![],
            max_alignment: 4,
        };
        let mt2 = MessageType {
            name: "test/Msg".to_string(),
            fields: vec![],
            max_alignment: 4,
        };
        assert_eq!(mt1, mt2);
    }

    #[test]
    fn test_message_schema_partial_eq() {
        let ms1 = MessageSchema {
            name: "test/Msg".to_string(),
            package: Some("test".to_string()),
            types: std::collections::HashMap::new(),
        };
        let ms2 = MessageSchema {
            name: "test/Msg".to_string(),
            package: Some("test".to_string()),
            types: std::collections::HashMap::new(),
        };
        assert_eq!(ms1, ms2);
    }

    // =========================================================================
    // Debug tests for AST types
    // =========================================================================

    #[test]
    fn test_primitive_type_debug() {
        let debug_str = format!("{:?}", PrimitiveType::Int32);
        assert!(debug_str.contains("Int32"));
    }

    #[test]
    fn test_field_type_debug() {
        let debug_str = format!("{:?}", FieldType::Primitive(PrimitiveType::Int32));
        assert!(debug_str.contains("Primitive"));
    }

    #[test]
    fn test_message_type_debug() {
        let mt = MessageType::new("test/Msg".to_string());
        let debug_str = format!("{:?}", mt);
        assert!(debug_str.contains("MessageType"));
    }

    // =========================================================================
    // Clone tests for AST types
    // =========================================================================

    #[test]
    fn test_field_type_clone() {
        let ft = FieldType::Array {
            base_type: Box::new(FieldType::Nested("test/Type".to_string())),
            size: Some(5),
        };
        let cloned = ft.clone();
        assert_eq!(ft, cloned);
    }

    #[test]
    fn test_message_type_clone() {
        let mut mt = MessageType::new("test/Msg".to_string());
        mt.add_field(Field {
            name: "field".to_string(),
            type_name: FieldType::Primitive(PrimitiveType::Int32),
        });
        let cloned = mt.clone();
        assert_eq!(mt.name, cloned.name);
        assert_eq!(mt.fields.len(), cloned.fields.len());
    }

    #[test]
    fn test_message_schema_clone() {
        let mut ms = MessageSchema::new("test/Msg".to_string());
        let mt = MessageType::new("test/Msg".to_string());
        ms.add_type(mt);
        let cloned = ms.clone();
        assert_eq!(ms.name, cloned.name);
        assert_eq!(ms.types.len(), cloned.types.len());
    }
}
