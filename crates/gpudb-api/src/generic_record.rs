//! Generic Record Support for Dynamic Type Handling
//!
//! This module provides `Type` and `GenericRecord` types for handling records
//! without compile-time type information.
//!
//! # Overview
//!
//! `GenericRecord` is for cases where you need to work with records
//! dynamically at runtime, without defining a specific struct:
//!
//! - **[`Type`]** - Immutable collection of metadata about a Kinetica type (schema, columns)
//! - **[`GenericRecord`]** - A record instance that stores values by column index
//!
//! # When to Use GenericRecord
//!
//! Use `GenericRecord` when:
//! - Table schema is not known at compile time
//! - You're building generic data pipelines
//! - You need to copy data between tables with different schemas
//! - You're reading data from CSV/JSON and inserting dynamically
//!
//! Use statically-typed records (implementing `KineticaRecord`) when:
//! - Schema is known at compile time
//! - You want maximum type safety and performance
//! - You're building application-specific data models
//!
//! # Example
//!
//! ```no_run
//! use gpudb_api::{BulkInserter, GenericRecord, Kinetica, Type};
//! use std::sync::Arc;
//! # async fn example(kinetica: Arc<Kinetica>) -> Result<(), Box<dyn std::error::Error>> {
//!
//! // Fetch type from existing table
//! let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
//!
//! // Or create type programmatically
//! let record_type = Type::builder("my_record")
//!     .add_int_column("id").primary_key()
//!     .add_string_column("name")
//!     .add_double_column("score")
//!     .build_arc()?;
//!
//! // Create records, which share their type through the Arc
//! let mut record = record_type.new_record();
//! record.set(0, 42)?;                   // by index
//! record.set_by_name("name", "Alice")?; // by name
//! record.set_by_name("score", 95.5)?;
//!
//! // Use with BulkInserter (requires schema string since GenericRecord can't implement Default)
//! let inserter = BulkInserter::<GenericRecord>::with_schema(
//!     kinetica,
//!     "my_table".to_string(),
//!     10000,  // batch_size
//!     8,      // num_workers
//!     None,   // options
//!     record_type.schema_string(),
//! ).await?;
//! inserter.insert(Arc::new(record))?;
//! inserter.flush().await?;
//! # Ok(())
//! # }
//! ```

use crate::avro_value::AvroValue;
use crate::{
    Kinetica, KineticaError, KineticaRecord, ShowTableRequest, ShowTableResponse,
    bulk_inserter_common::{ShardKeyExtractor, ShardKeyValue, ShardKeyValues},
};
use serde::{Serialize, Serializer, ser::SerializeMap};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ============================================================================
// Column Type Enums
// ============================================================================

/// Base column types (maps to Avro primitive types)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnBaseType {
    /// 32-bit signed integer
    Integer,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 float
    Float,
    /// 64-bit IEEE 754 double
    Double,
    /// UTF-8 string
    String,
    /// Raw bytes
    Bytes,
}

impl ColumnBaseType {
    /// Get the Avro type name for this base type
    pub fn avro_type(&self) -> &'static str {
        match self {
            ColumnBaseType::Integer => "int",
            ColumnBaseType::Long => "long",
            ColumnBaseType::Float => "float",
            ColumnBaseType::Double => "double",
            ColumnBaseType::String => "string",
            ColumnBaseType::Bytes => "bytes",
        }
    }
}

/// Column types including subtypes determined by properties
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    // Base types
    Integer,
    Long,
    Float,
    Double,
    String,
    Bytes,
    // Integer subtypes
    Boolean,
    Int8,
    Int16,
    // Long subtypes
    Timestamp,
    // String subtypes
    Array,
    Date,
    DateTime,
    Time,
    Decimal,
    Char1,
    Char2,
    Char4,
    Char8,
    Char16,
    Char32,
    Char64,
    Char128,
    Char256,
    Ipv4,
    Json,
    Ulong,
    Uuid,
    Wkt,
    // Bytes subtypes
    Vector,
    Wkb,
}

impl ColumnType {
    /// The property that makes a column of this sub-type, where that one
    /// property alone does: `boolean`, `timestamp`, `char16`, `decimal`
    /// (18,4), `wkt` and so on. `None` for a base type, and for an array or
    /// a vector, whose property names its element type or dimensions.
    pub fn property(&self) -> Option<&'static str> {
        Some(match self {
            ColumnType::Boolean => "boolean",
            ColumnType::Int8 => "int8",
            ColumnType::Int16 => "int16",
            ColumnType::Timestamp => "timestamp",
            ColumnType::Date => "date",
            ColumnType::DateTime => "datetime",
            ColumnType::Time => "time",
            ColumnType::Decimal => "decimal",
            ColumnType::Char1 => "char1",
            ColumnType::Char2 => "char2",
            ColumnType::Char4 => "char4",
            ColumnType::Char8 => "char8",
            ColumnType::Char16 => "char16",
            ColumnType::Char32 => "char32",
            ColumnType::Char64 => "char64",
            ColumnType::Char128 => "char128",
            ColumnType::Char256 => "char256",
            ColumnType::Ipv4 => "ipv4",
            ColumnType::Json => "json",
            ColumnType::Ulong => "ulong",
            ColumnType::Uuid => "uuid",
            ColumnType::Wkt | ColumnType::Wkb => "wkt",
            ColumnType::Integer
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::String
            | ColumnType::Bytes
            | ColumnType::Array
            | ColumnType::Vector => return None,
        })
    }

    /// Get the base type for this column type
    pub fn base_type(&self) -> ColumnBaseType {
        match self {
            ColumnType::Integer | ColumnType::Boolean | ColumnType::Int8 | ColumnType::Int16 => {
                ColumnBaseType::Integer
            }
            ColumnType::Long | ColumnType::Timestamp => ColumnBaseType::Long,
            ColumnType::Float => ColumnBaseType::Float,
            ColumnType::Double => ColumnBaseType::Double,
            ColumnType::String
            | ColumnType::Array
            | ColumnType::Date
            | ColumnType::DateTime
            | ColumnType::Time
            | ColumnType::Decimal
            | ColumnType::Char1
            | ColumnType::Char2
            | ColumnType::Char4
            | ColumnType::Char8
            | ColumnType::Char16
            | ColumnType::Char32
            | ColumnType::Char64
            | ColumnType::Char128
            | ColumnType::Char256
            | ColumnType::Ipv4
            | ColumnType::Json
            | ColumnType::Ulong
            | ColumnType::Uuid
            | ColumnType::Wkt => ColumnBaseType::String,
            ColumnType::Bytes | ColumnType::Vector | ColumnType::Wkb => ColumnBaseType::Bytes,
        }
    }

    /// Resolves a column's type from its Avro type and properties: the base
    /// type from the Avro type, overridden by the first sub-type property in a
    /// fixed order of precedence (`wkt` giving WKT on a string column and WKB
    /// on a bytes one), and failing those by the first property starting
    /// `array`, `vector` or `decimal`.
    pub fn from_avro_and_properties(avro_type: &str, properties: &[String]) -> Self {
        let base = match avro_type {
            "int" => ColumnType::Integer,
            "long" => ColumnType::Long,
            "float" => ColumnType::Float,
            "double" => ColumnType::Double,
            "bytes" => ColumnType::Bytes,
            _ => ColumnType::String,
        };
        Self::resolve(base, properties)
    }

    /// The sub-type `properties` make of a column of type `base`, or `base`
    /// itself.
    fn resolve(base: ColumnType, properties: &[String]) -> Self {
        let has = |property: &str| properties.iter().any(|p| p == property);
        const BY_PRECEDENCE: [(&str, ColumnType); 20] = [
            ("boolean", ColumnType::Boolean),
            ("int8", ColumnType::Int8),
            ("int16", ColumnType::Int16),
            ("timestamp", ColumnType::Timestamp),
            ("date", ColumnType::Date),
            ("time", ColumnType::Time),
            ("datetime", ColumnType::DateTime),
            ("char1", ColumnType::Char1),
            ("char2", ColumnType::Char2),
            ("char4", ColumnType::Char4),
            ("char8", ColumnType::Char8),
            ("char16", ColumnType::Char16),
            ("char32", ColumnType::Char32),
            ("char64", ColumnType::Char64),
            ("char128", ColumnType::Char128),
            ("char256", ColumnType::Char256),
            ("ipv4", ColumnType::Ipv4),
            ("json", ColumnType::Json),
            ("ulong", ColumnType::Ulong),
            ("uuid", ColumnType::Uuid),
        ];
        if let Some((_, column_type)) = BY_PRECEDENCE.iter().find(|(property, _)| has(property)) {
            return *column_type;
        }
        if has("wkt") {
            return match base {
                ColumnType::String => ColumnType::Wkt,
                ColumnType::Bytes => ColumnType::Wkb,
                other => other,
            };
        }
        for property in properties {
            if property.starts_with("array") {
                return ColumnType::Array;
            }
            if property.starts_with("vector") {
                return ColumnType::Vector;
            }
            if property.starts_with("decimal") {
                return ColumnType::Decimal;
            }
        }
        base
    }
}

/// The precision and scale of a `decimal` column whose property gives none.
const DEFAULT_DECIMAL_PRECISION: i32 = Column::DEFAULT_DECIMAL_PRECISION;
const DEFAULT_DECIMAL_SCALE: i32 = Column::DEFAULT_DECIMAL_SCALE;

/// The precision and scale of a decimal column's property: `decimal(P,S)`,
/// else the defaults.
fn decimal_info(properties: &[String]) -> (i32, i32) {
    let Some(property) = properties.iter().find(|p| p.starts_with("decimal")) else {
        return (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
    };
    property
        .to_lowercase()
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')').map(str::to_string))
        .and_then(|inner| {
            let (p, s) = inner.split_once(',')?;
            let digits = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit());
            (digits(p) && digits(s)).then(|| Some((p.parse().ok()?, s.parse().ok()?))).flatten()
        })
        .unwrap_or((DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE))
}

// ============================================================================
// Column - Metadata about a single column
// ============================================================================

/// Immutable metadata about a column in a Kinetica type.
#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    /// The Avro type, kept apart from the sub-type.
    base_type: ColumnBaseType,
    column_type: ColumnType,
    properties: Vec<String>,
    is_nullable: bool,
}

impl Column {
    /// A `decimal` column's precision where its property gives none.
    pub const DEFAULT_DECIMAL_PRECISION: i32 = 18;
    /// A `decimal` column's scale where its property gives none.
    pub const DEFAULT_DECIMAL_SCALE: i32 = 4;
    /// The least default decimal, `i64::MIN` at scale 4, as text.
    pub const DEFAULT_DECIMAL_MIN: &'static str = "-922337203685477.5808";
    /// The greatest default decimal, `i64::MAX` at scale 4, as text.
    pub const DEFAULT_DECIMAL_MAX: &'static str = "922337203685477.5807";
    /// The widest decimal held in eight bytes.
    pub const DECIMAL8_MAX_PRECISION: i32 = 18;

    /// Create a new column with the given name and type.
    ///
    /// A sub-type's property is added (see [`ColumnType::property`]), so a
    /// `Timestamp` column has the `timestamp` property, and is registered as
    /// one. See [`Self::with_properties`].
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self::with_properties(name, column_type, Vec::new())
    }

    /// Create a column with properties.
    ///
    /// The column's type is resolved from its base type and properties, so
    /// the properties decide: a string column with the `date` property is a
    /// date. A sub-type `column_type` that they do not already make has its
    /// property added first (see [`ColumnType::property`]). An array or
    /// vector type, which no single property makes, is kept as given.
    ///
    /// This cannot fail, so an empty name or property is refused by
    /// [`Type::new`] instead.
    pub fn with_properties(name: impl Into<String>, column_type: ColumnType, properties: Vec<String>) -> Self {
        let mut properties = properties;
        if let Some(property) = column_type.property() {
            let present = properties.iter().any(|p| p == property || (property == "decimal" && p.starts_with("decimal")));
            if !present {
                properties.push(property.to_string());
            }
        }
        let base_type = column_type.base_type();
        let resolved = ColumnType::from_avro_and_properties(base_type.avro_type(), &properties);
        let column_type = match column_type {
            ColumnType::Array | ColumnType::Vector if resolved == ColumnType::from_avro_and_properties(base_type.avro_type(), &[]) => {
                column_type
            }
            _ => resolved,
        };
        let is_nullable = properties.iter().any(|p| p == "nullable");
        Self { name: name.into(), base_type, column_type, properties, is_nullable }
    }

    /// A column of the given base (Avro) type and properties, its type
    /// resolved from both.
    pub fn from_base_type(name: impl Into<String>, base_type: ColumnBaseType, properties: Vec<String>) -> Self {
        let base = ColumnType::from_avro_and_properties(base_type.avro_type(), &[]);
        let column_type = ColumnType::resolve(base, &properties);
        let is_nullable = properties.iter().any(|p| p == "nullable");
        Self { name: name.into(), base_type, column_type, properties, is_nullable }
    }

    /// The precision of a decimal column. 0 for a column of another type.
    pub fn decimal_precision(&self) -> i32 {
        if self.is_decimal() { decimal_info(&self.properties).0 } else { 0 }
    }

    /// The scale of a decimal column. 0 for a column of another type.
    pub fn decimal_scale(&self) -> i32 {
        if self.is_decimal() { decimal_info(&self.properties).1 } else { 0 }
    }

    /// Whether this is a decimal column.
    pub fn is_decimal(&self) -> bool {
        self.column_type == ColumnType::Decimal
    }

    /// Whether the column has a default: a `default(...)` property,
    /// `init_with_now` or `init_with_uuid`.
    pub fn has_default(&self) -> bool {
        self.properties.iter().any(|p| {
            (p.starts_with("default(") && p.ends_with(')')) || p == "init_with_now" || p == "init_with_uuid"
        })
    }

    /// The column's default: the text of its first `default(...)`, `NOW()`
    /// for `init_with_now`, or `NEW_UUID()` for `init_with_uuid`; `None`
    /// without one.
    pub fn default_value(&self) -> Option<String> {
        self.properties.iter().find_map(|p| {
            if p.starts_with("default(") && p.ends_with(')') && p.len() >= 9 {
                Some(p[8..p.len() - 1].to_string())
            } else if p == "init_with_now" {
                Some("NOW()".to_string())
            } else if p == "init_with_uuid" {
                Some("NEW_UUID()".to_string())
            } else {
                None
            }
        })
    }

    /// Whether a property starts `array`.
    pub fn is_array(&self) -> bool {
        self.properties.iter().any(|p| p.starts_with("array"))
    }

    /// The element type of an array column, read from the first `array(...)`
    /// property: `int...` an integer, `bool...` a boolean, and `long`,
    /// `float`, `double`, `string`, `ulong` or `bytes` themselves. `None` for
    /// a column with no such property; any other element type fails with
    /// "Unknown array type: <type>".
    pub fn array_type(&self) -> Result<Option<ColumnType>, KineticaError> {
        for property in self.properties.iter().filter(|p| p.starts_with("array")) {
            let (Some(open), Some(close)) = (property.find('('), property.rfind(')')) else {
                continue;
            };
            if close <= open {
                continue;
            }
            let end = property[open..close].find(',').map_or(close, |comma| open + comma);
            let element = property[open + 1..end].to_lowercase();
            return match element.as_str() {
                e if e.starts_with("int") => Ok(Some(ColumnType::Integer)),
                e if e.starts_with("bool") => Ok(Some(ColumnType::Boolean)),
                "long" => Ok(Some(ColumnType::Long)),
                "float" => Ok(Some(ColumnType::Float)),
                "double" => Ok(Some(ColumnType::Double)),
                "string" => Ok(Some(ColumnType::String)),
                "ulong" => Ok(Some(ColumnType::Ulong)),
                "bytes" => Ok(Some(ColumnType::Bytes)),
                _ => Err(KineticaError::Api(format!("Unknown array type: {element}"))),
            };
        }
        Ok(None)
    }

    /// Whether a property starts `vector`.
    pub fn is_vector(&self) -> bool {
        self.properties.iter().any(|p| p.starts_with("vector"))
    }

    /// The dimensions of a vector column, from its first `vector(N)`
    /// property. -1 without one, and for dimensions that are not a number.
    pub fn vector_dimensions(&self) -> i32 {
        let Some(property) = self.properties.iter().find(|p| p.starts_with("vector")) else {
            return -1;
        };
        match (property.find('('), property.rfind(')')) {
            (Some(open), Some(close)) if close > open => property[open + 1..close].parse().unwrap_or(-1),
            _ => -1,
        }
    }

    /// Whether the column has exactly this property.
    pub fn has_property(&self, property: &str) -> bool {
        self.properties.iter().any(|p| p == property)
    }

    /// Get the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column type.
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    /// Get the base type.
    pub fn base_type(&self) -> ColumnBaseType {
        self.base_type
    }

    /// Get the column properties.
    pub fn properties(&self) -> &[String] {
        &self.properties
    }

    /// Check if this column is nullable.
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    /// Check if this column is a primary key.
    pub fn is_primary_key(&self) -> bool {
        self.properties.iter().any(|p| p == "primary_key")
    }

    /// Check if this column is a shard key.
    pub fn is_shard_key(&self) -> bool {
        self.properties.iter().any(|p| p == "shard_key")
    }
}

/// A string quoted and escaped, as the text of a record, type or column
/// writes names and values: `"`, `\` and the usual control characters
/// escaped, and any other character in U+0000-001F, U+007F-009F or
/// U+2000-20FF as `\uXXXX`.
pub(crate) fn avro_quoted(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for ch in text.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\u{8}' => quoted.push_str("\\b"),
            '\u{c}' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\u{0}'..='\u{1f}' | '\u{7f}'..='\u{9f}' | '\u{2000}'..='\u{20ff}' => {
                quoted.push_str(&format!("\\u{:04X}", ch as u32));
            }
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

/// A value as a record's text writes it: integers as their digits, floating
/// point numbers always with a fraction (`1.0`, `1.0E10`) and with NaN and
/// the infinities quoted, text quoted, bytes as their ISO-8859-1 characters,
/// quoted, and null as `null`.
fn avro_value_text(value: &RecordValue) -> String {
    let number = |text: String, finite: bool| if finite { text } else { format!("\"{text}\"") };
    match value {
        RecordValue::Null => "null".to_string(),
        RecordValue::Int(v) => v.to_string(),
        RecordValue::Long(v) => v.to_string(),
        RecordValue::Float(v) => number(crate::text_format::float_to_string(*v), v.is_finite()),
        RecordValue::Double(v) => number(crate::text_format::double_to_string(*v), v.is_finite()),
        RecordValue::String(v) => avro_quoted(v),
        RecordValue::Bytes(v) => avro_quoted(&v.iter().map(|&b| char::from(b)).collect::<String>()),
    }
}

/// The same name, Avro type and properties, in order.
impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.base_type == other.base_type && self.properties == other.properties
    }
}

impl Eq for Column {}

impl std::hash::Hash for Column {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.base_type.hash(state);
        self.properties.hash(state);
    }
}

/// `{"name":..,"type":"Integer","properties":[..]}`, the type named by its
/// [`ColumnBaseType`].
impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let type_name = match self.base_type {
            ColumnBaseType::Integer => "Integer",
            ColumnBaseType::Long => "Long",
            ColumnBaseType::Float => "Float",
            ColumnBaseType::Double => "Double",
            ColumnBaseType::String => "String",
            ColumnBaseType::Bytes => "Bytes",
        };
        let properties: Vec<String> = self.properties.iter().map(|p| avro_quoted(p)).collect();
        write!(
            f,
            "{{\"name\":{},\"type\":\"{type_name}\",\"properties\":[{}]}}",
            avro_quoted(&self.name),
            properties.join(",")
        )
    }
}

// ============================================================================
// Type - Immutable collection of metadata about a Kinetica type
// ============================================================================

/// Immutable collection of metadata about a Kinetica type.
///
/// It contains all the information needed to create and encode records of
/// this type.
///
/// # Creating a Type
///
/// ```no_run
/// # use gpudb_api::{Column, ColumnType, Kinetica, Type};
/// # async fn example(kinetica: Kinetica) -> Result<(), gpudb_api::KineticaError> {
/// // From existing table
/// let record_type = Type::from_table(&kinetica, "my_table").await?;
///
/// // Programmatically
/// let record_type = Type::builder("my_type")
///     .add_column(Column::new("id", ColumnType::Integer))
///     .add_column(Column::new("name", ColumnType::String))
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Type {
    /// Type label/name
    label: String,
    /// Columns in order
    columns: Vec<Column>,
    /// Column name to index mapping
    column_indices: HashMap<String, usize>,
    /// Indices of shard key columns
    shard_key_indices: Vec<usize>,
    /// Indices of primary key columns
    primary_key_indices: Vec<usize>,
    /// Cached Avro schema JSON string
    schema_string: String,
    /// The schema string with a `'static` lifetime, as
    /// `KineticaRecord::avro_schema` returns it: leaked once for the
    /// type, on first use, rather than on every call.
    static_schema_string: std::sync::OnceLock<&'static str>,
    /// Type ID (from Kinetica, if available)
    type_id: Option<String>,
}

impl Type {
    /// Creates a Type from column definitions: at least one column, no two
    /// with the same name.
    pub fn new(label: impl Into<String>, columns: Vec<Column>) -> Result<Self, KineticaError> {
        let label = label.into();
        // Refused here, as making a column cannot fail
        for column in &columns {
            if column.name.is_empty() {
                return Err(KineticaError::Config("Name must not be empty.".to_string()));
            }
            if column.properties.iter().any(String::is_empty) {
                return Err(KineticaError::Config("Properties must not be empty.".to_string()));
            }
        }
        if columns.is_empty() {
            return Err(KineticaError::Config("At least one column must be specified.".to_string()));
        }

        let mut column_indices = HashMap::new();
        let mut shard_key_indices = Vec::new();
        let mut primary_key_indices = Vec::new();

        for (idx, col) in columns.iter().enumerate() {
            if column_indices.insert(col.name.clone(), idx).is_some() {
                return Err(KineticaError::Config(format!("Duplicate column name {} specified.", col.name)));
            }
            if col.is_shard_key() {
                shard_key_indices.push(idx);
            }
            if col.is_primary_key() {
                primary_key_indices.push(idx);
            }
        }

        let schema_string = Self::create_schema(&columns, &column_indices);

        Ok(Self {
            label,
            columns,
            column_indices,
            shard_key_indices,
            primary_key_indices,
            schema_string,
            static_schema_string: std::sync::OnceLock::new(),
            type_id: None,
        })
    }

    /// The type of the records `record` stands for, from the Avro schema and
    /// the column properties its [`crate::KineticaRecord`] implementation
    /// gives. The label is the schema's record name. [`Type::create`] then
    /// creates it on the server.
    ///
    /// Fails as [`Type::from_type_schema`] does, for a schema whose fields
    /// are not of the types a Kinetica type takes.
    pub fn from_record<R: crate::KineticaRecord>(record: &R) -> Result<Self, KineticaError> {
        let schema = record.avro_schema();
        let label = serde_json::from_str::<serde_json::Value>(schema)
            .ok()
            .and_then(|root| root.get("name").and_then(serde_json::Value::as_str).map(str::to_string))
            .unwrap_or_default();
        Self::from_type_schema(label, schema, &R::type_properties())
    }

    /// Creates a Type from a type schema and its column properties.
    ///
    /// Each field must be one of the Avro types `bytes`, `double`, `float`,
    /// `int`, `long` or `string`, or a union of one of them followed by
    /// `null`, which makes the column nullable.
    pub fn from_type_schema(
        label: impl Into<String>,
        type_schema: &str,
        properties: &HashMap<String, Vec<String>>,
    ) -> Result<Self, KineticaError> {
        let invalid = |message: String| KineticaError::Config(message);
        let root: serde_json::Value =
            serde_json::from_str(type_schema).map_err(|_| invalid("Schema is invalid.".to_string()))?;
        if root.get("type").and_then(|t| t.as_str()) != Some("record") {
            return Err(invalid("Schema must be of type record.".to_string()));
        }
        let fields = match root.get("fields").and_then(|f| f.as_array()) {
            Some(fields) if !fields.is_empty() => fields,
            _ => return Err(invalid("Schema has no fields.".to_string())),
        };

        let mut columns = Vec::with_capacity(fields.len());
        let mut names: HashSet<String> = HashSet::new();
        for field in fields {
            if !field.is_object() {
                return Err(invalid("Schema has invalid field.".to_string()));
            }
            let Some(name) = field.get("name").and_then(|n| n.as_str()) else {
                return Err(invalid("Schema has unnamed field.".to_string()));
            };
            if !names.insert(name.to_string()) {
                return Err(invalid(format!("Duplicate field name {name}.")));
            }
            let Some(field_type) = field.get("type") else {
                return Err(invalid(format!("Field {name} has no type.")));
            };

            // A type name, or a union of one followed by "null"
            let (type_name, is_nullable) = match field_type {
                serde_json::Value::String(t) => (t.as_str(), false),
                serde_json::Value::Array(members) => {
                    let mut type_name: Option<&str> = None;
                    let mut is_nullable = false;
                    for member in members {
                        match (member.as_str(), type_name) {
                            (Some("null"), Some(_)) => is_nullable = true,
                            (Some(t), None) => type_name = Some(t),
                            _ => return Err(invalid(format!("Field {name} has invalid type."))),
                        }
                    }
                    match type_name {
                        Some(t) => (t, is_nullable),
                        None => return Err(invalid(format!("Field {name} has invalid type."))),
                    }
                }
                _ => return Err(invalid(format!("Field {name} has invalid type."))),
            };

            let base_type = match type_name {
                "bytes" => ColumnBaseType::Bytes,
                "double" => ColumnBaseType::Double,
                "float" => ColumnBaseType::Float,
                "int" => ColumnBaseType::Integer,
                "long" => ColumnBaseType::Long,
                "string" => ColumnBaseType::String,
                _ => {
                    return Err(invalid(format!(
                        "Field {name} must be of type bytes, double, float, int, long or string."
                    )))
                }
            };

            let mut column_properties = properties.get(name).cloned().unwrap_or_default();
            if is_nullable && !column_properties.iter().any(|p| p == "nullable") {
                column_properties.push("nullable".to_string());
            }
            columns.push(Column::from_base_type(name, base_type, column_properties));
        }

        Self::new(label, columns)
    }

    /// Create a TypeBuilder for fluent construction.
    pub fn builder(label: impl Into<String>) -> TypeBuilder {
        TypeBuilder::new(label)
    }

    /// Fetches the type of an existing table.
    pub async fn from_table(kinetica: &Kinetica, table_name: &str) -> Result<Self, KineticaError> {
        let req = ShowTableRequest {
            table_name: table_name.to_string(),
            options: HashMap::new(),
        };

        let resp = kinetica.show_table_raw(&req).await?;
        Self::from_table_response(table_name, &resp)
    }

    /// The type a type ID names, from `/show/types`. Fails with "Type <id>
    /// does not exist." where the server knows none.
    pub async fn from_type(kinetica: &Kinetica, type_id: &str) -> Result<Self, KineticaError> {
        let req = crate::ShowTypesRequest {
            type_id: type_id.to_string(),
            label: String::new(),
            options: HashMap::new(),
        };
        let resp = kinetica.show_types_raw(&req).await?;
        let (Some(found_id), Some(label), Some(schema)) =
            (resp.type_ids.first(), resp.labels.first(), resp.type_schemas.first())
        else {
            return Err(KineticaError::Api(format!("Type {type_id} does not exist.")));
        };
        let empty = HashMap::new();
        let mut record_type = Self::from_type_schema(label.clone(), schema, resp.properties.first().unwrap_or(&empty))?;
        record_type.type_id = Some(found_id.clone());
        Ok(record_type)
    }

    /// Builds the type of a table from its `/show/table` response: the table
    /// must exist and have a single type.
    pub fn from_table_response(table_name: &str, resp: &ShowTableResponse) -> Result<Self, KineticaError> {
        let Some(type_id) = resp.type_ids.first() else {
            return Err(KineticaError::Api(format!("Table {table_name} does not exist.")));
        };
        if resp.type_ids.iter().any(|t| t != type_id) {
            return Err(KineticaError::Api(format!("Table {table_name} is not homogeneous.")));
        }

        let empty = HashMap::new();
        let mut record_type = Self::from_type_schema(
            resp.type_labels.first().cloned().unwrap_or_default(),
            resp.type_schemas.first().map(String::as_str).unwrap_or_default(),
            resp.properties.first().unwrap_or(&empty),
        )?;
        record_type.type_id = Some(type_id.clone());
        Ok(record_type)
    }

    /// Builds the type of a dynamic (column-wise) response from its schema and
    /// its data.
    ///
    /// The schema's fields are arrays, one per column, followed by
    /// `column_headers` and `column_datatypes`, which the data carries. A
    /// column is named by its header, with `_2`, `_3`, ... added where a header
    /// repeats, and its data type becomes its sub-type property (`geometry` as
    /// `wkt`).
    pub fn from_dynamic_schema(schema_string: &str, encoded_data: &[u8]) -> Result<Type, KineticaError> {
        let (schema, data) = decode_dynamic(schema_string, encoded_data)?;
        let fields = schema["fields"].as_array().map(Vec::as_slice).unwrap_or_default();
        let field_count = fields.len().saturating_sub(2);
        let api = |message: String| KineticaError::Api(message);

        let strings = |value: Option<&AvroValue>| -> Vec<String> {
            match value {
                Some(AvroValue::Array(items)) => items
                    .iter()
                    .map(|item| match item {
                        AvroValue::String(s) => s.clone(),
                        _ => String::new(),
                    })
                    .collect(),
                _ => Vec::new(),
            }
        };
        let expressions = strings(data.get(field_count));
        let types = strings(data.get(field_count + 1));
        let record_count = column_len(data.first());

        if expressions.len() < field_count {
            return Err(api("Every field must have a corresponding expression.".to_string()));
        }
        if types.len() < field_count {
            return Err(api("Every field must have a corresponding type.".to_string()));
        }

        let mut columns: Vec<Column> = Vec::with_capacity(field_count);
        for (i, field) in fields.iter().take(field_count).enumerate() {
            let field_name = field["name"].as_str().unwrap_or_default();
            let field_schema = &field["type"];
            if field_schema["type"].as_str() != Some("array") {
                return Err(api(format!("Field {field_name} must be of type array.")));
            }

            let mut column_properties = Vec::new();
            let items = &field_schema["items"];
            let item_type = match items {
                serde_json::Value::Array(members) => {
                    if members.len() == 2 && avro_type_name(&members[1]) == Some("null") {
                        column_properties.push("nullable".to_string());
                        avro_type_name(&members[0])
                    } else {
                        return Err(api(format!("Field {field_name} has invalid type.")));
                    }
                }
                other => avro_type_name(other),
            };
            let base_type = match item_type {
                Some("bytes") => ColumnBaseType::Bytes,
                Some("double") => ColumnBaseType::Double,
                Some("float") => ColumnBaseType::Float,
                Some("int") => ColumnBaseType::Integer,
                Some("long") => ColumnBaseType::Long,
                Some("string") => ColumnBaseType::String,
                _ => {
                    return Err(api(format!(
                        "Field {field_name} must be of type bytes, double, float, int, long or string."
                    )))
                }
            };

            if column_len(data.get(i)) != record_count {
                return Err(api("Fields must all have the same number of elements.".to_string()));
            }

            // A header that repeats an earlier column's name takes the first
            // free suffix, skipping names later headers will claim
            let mut name = expressions[i].clone();
            if columns.iter().any(|c| c.name == name) {
                let mut n = 2;
                name = loop {
                    let candidate = format!("{}_{n}", expressions[i]);
                    let taken = columns.iter().any(|c| c.name == candidate)
                        || expressions[i + 1..field_count].contains(&candidate);
                    if !taken {
                        break candidate;
                    }
                    n += 1;
                };
            }

            let data_type = types[i].as_str();
            match data_type {
                "boolean" | "char1" | "char2" | "char4" | "char8" | "char16" | "char32" | "char64"
                | "char128" | "char256" | "date" | "datetime" | "int8" | "int16" | "ipv4" | "json"
                | "time" | "timestamp" | "ulong" | "uuid" | "wkt" => column_properties.push(data_type.to_string()),
                // WKT columns can be reported as "geometry"
                "geometry" => column_properties.push("wkt".to_string()),
                _ => {
                    if data_type.starts_with("array")
                        || data_type.starts_with("vector")
                        || data_type.starts_with("decimal")
                    {
                        column_properties.push(data_type.to_string());
                    }
                }
            }

            columns.push(Column::from_base_type(name, base_type, column_properties));
        }

        Type::new("", columns)
    }

    /// The type's base definition: each column's name and Avro type, and
    /// `nullable` for one that is, as
    /// `[["id","int"],["name","string","nullable"]]`. Names are written as
    /// they are.
    pub fn base_definition(&self) -> String {
        let mut definition = String::from("[");
        for column in &self.columns {
            if definition.len() > 1 {
                definition.push(',');
            }
            definition.push_str(&format!("[\"{}\",\"{}\"", column.name, column.base_type().avro_type()));
            if column.is_nullable() {
                definition.push_str(",\"nullable\"");
            }
            definition.push(']');
        }
        definition.push(']');
        definition
    }

    /// The type's full definition. Each column's name; its type property
    /// (such as `char16`, `decimal(10,2)` or `vector(3)`), the last if it has
    /// more than one, or else its Avro type; and its other properties joined
    /// by commas, if it has any, as
    /// `[["id","int","primary_key"],["name","char16","nullable"]]`.
    pub fn full_definition(&self) -> String {
        const TYPE_PROPERTIES: [&str; 20] = [
            "boolean", "char1", "char2", "char4", "char8", "char16", "char32", "char64", "char128", "char256", "date",
            "datetime", "int8", "int16", "ipv4", "json", "time", "timestamp", "ulong", "wkt",
        ];
        let is_type = |p: &str| {
            TYPE_PROPERTIES.contains(&p) || ["array", "decimal", "vector"].iter().any(|prefix| p.starts_with(prefix))
        };
        let mut definition = String::from("[");
        for column in &self.columns {
            if definition.len() > 1 {
                definition.push(',');
            }
            let (types, others): (Vec<&String>, Vec<&String>) = column.properties.iter().partition(|p| is_type(p));
            let column_type = types.last().map_or(column.base_type().avro_type(), |p| p.as_str());
            definition.push_str(&format!("[\"{}\",\"{column_type}\"", column.name));
            if !others.is_empty() {
                let others: Vec<&str> = others.iter().map(|p| p.as_str()).collect();
                definition.push_str(&format!(",\"{}\"", others.join(",")));
            }
            definition.push(']');
        }
        definition.push(']');
        definition
    }

    /// Creates this type in Kinetica and returns its type ID, or the ID of an
    /// identical type that already exists.
    ///
    /// The type definition names each field after its column, as it is, with
    /// its Avro type (a union with `null` for a nullable column), and the
    /// properties of each column that has any are sent with it.
    pub async fn create(&self, kinetica: &Kinetica) -> Result<String, KineticaError> {
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut properties: HashMap<String, Vec<String>> = HashMap::new();
        for column in &self.columns {
            let name = serde_json::to_string(&column.name).expect("a string is JSON");
            let avro_type = column.base_type().avro_type();
            let field_type = if column.is_nullable() {
                format!(r#"["{avro_type}","null"]"#)
            } else {
                format!(r#""{avro_type}""#)
            };
            fields.push(format!(r#"{{"name":{name},"type":{field_type}}}"#));
            if !column.properties.is_empty() {
                properties.insert(column.name.clone(), column.properties.clone());
            }
        }
        let definition = format!(r#"{{"type":"record","name":"{RECORD_NAME}","fields":[{}]}}"#, fields.join(","));
        Ok(kinetica.create_type(definition, self.label.clone(), properties).await?.type_id)
    }

    /// Builds the type's Avro schema: a record named `type_name`, with one
    /// field per column in order. A field is named after its column with
    /// every character other than a letter, an underscore or a digit (not
    /// first) replaced by `_`, and with `_2`, `_3`, ... added where that would
    /// clash with another column or field. A nullable column's type is the
    /// union of its type and `null`.
    fn create_schema(columns: &[Column], column_indices: &HashMap<String, usize>) -> String {
        let mut fields = Vec::with_capacity(columns.len());
        let mut field_names: HashSet<String> = HashSet::new();

        for (i, column) in columns.iter().enumerate() {
            // A character outside the BMP becomes two underscores, one per
            // UTF-16 unit
            let mut sanitized = String::with_capacity(column.name.len());
            for (j, c) in column.name.chars().enumerate() {
                let keep = (j > 0 && c.is_ascii_digit()) || c.is_ascii_alphabetic() || c == '_';
                if keep {
                    sanitized.push(c);
                } else {
                    for _ in 0..c.len_utf16() {
                        sanitized.push('_');
                    }
                }
            }

            let mut n = 1;
            let field_name = loop {
                let candidate = if n > 1 { format!("{sanitized}_{n}") } else { sanitized.clone() };
                let own = column_indices.get(&candidate).is_none_or(|&index| index == i);
                if own && !field_names.contains(&candidate) {
                    break candidate;
                }
                n += 1;
            };

            // A field name is letters, digits and underscores only, so needs no
            // escaping.
            let avro_type = column.base_type().avro_type();
            let field_type = if column.is_nullable() {
                format!(r#"["{avro_type}","null"]"#)
            } else {
                format!(r#""{avro_type}""#)
            };
            fields.push(format!(r#"{{"name":"{field_name}","type":{field_type}}}"#));
            field_names.insert(field_name);
        }

        format!(r#"{{"type":"record","name":"{RECORD_NAME}","fields":[{}]}}"#, fields.join(","))
    }

    /// Get the type label/name.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Get the Avro schema string.
    pub fn schema_string(&self) -> &str {
        &self.schema_string
    }

    /// Get the type ID (if fetched from Kinetica).
    pub fn type_id(&self) -> Option<&str> {
        self.type_id.as_deref()
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a column by index.
    pub fn get_column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Get a column by name.
    pub fn get_column_by_name(&self, name: &str) -> Option<&Column> {
        self.column_indices.get(name).map(|&idx| &self.columns[idx])
    }

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_indices.get(name).copied()
    }

    /// Get column index by name, or return error.
    pub fn column_index_or_error(&self, name: &str) -> Result<usize, KineticaError> {
        self.column_indices.get(name).copied()
            .ok_or_else(|| KineticaError::Config(format!("Field '{name}' does not exist.")))
    }

    /// Iterate over columns.
    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    /// Get shard key column indices.
    pub fn shard_key_indices(&self) -> &[usize] {
        &self.shard_key_indices
    }

    /// Get primary key column indices.
    pub fn primary_key_indices(&self) -> &[usize] {
        &self.primary_key_indices
    }

    /// Check if this type has a shard key.
    pub fn has_shard_key(&self) -> bool {
        !self.shard_key_indices.is_empty()
    }

    /// A new record of this type, every value null.
    pub fn new_record(self: &Arc<Self>) -> GenericRecord {
        GenericRecord::new(self.clone())
    }
}

// ============================================================================
// TypeBuilder - Fluent builder for Type
// ============================================================================

/// Builder for constructing a Type with a fluent API.
pub struct TypeBuilder {
    label: String,
    columns: Vec<Column>,
    current_column: Option<(String, ColumnType, Vec<String>)>,
}

impl TypeBuilder {
    /// Create a new TypeBuilder.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            columns: Vec::new(),
            current_column: None,
        }
    }

    fn finish_current_column(&mut self) {
        if let Some((name, col_type, props)) = self.current_column.take() {
            self.columns.push(Column::with_properties(name, col_type, props));
        }
    }

    /// Add a column with the specified type.
    pub fn add_column(mut self, column: Column) -> Self {
        self.finish_current_column();
        self.columns.push(column);
        self
    }

    /// Add an integer column.
    pub fn add_int_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Integer, Vec::new()));
        self
    }

    /// Add a long column.
    pub fn add_long_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Long, Vec::new()));
        self
    }

    /// Add a float column.
    pub fn add_float_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Float, Vec::new()));
        self
    }

    /// Add a double column.
    pub fn add_double_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Double, Vec::new()));
        self
    }

    /// Add a string column.
    pub fn add_string_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::String, Vec::new()));
        self
    }

    /// Add a bytes column.
    pub fn add_bytes_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Bytes, Vec::new()));
        self
    }

    /// Add a timestamp column.
    pub fn add_timestamp_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Timestamp, vec!["timestamp".into()]));
        self
    }

    /// Add a date column.
    pub fn add_date_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Date, vec!["date".into()]));
        self
    }

    /// Add a boolean column (stored as int).
    pub fn add_boolean_column(mut self, name: impl Into<String>) -> Self {
        self.finish_current_column();
        self.current_column = Some((name.into(), ColumnType::Boolean, vec!["boolean".into()]));
        self
    }

    /// Mark the current column as primary key.
    pub fn primary_key(mut self) -> Self {
        if let Some((_, _, ref mut props)) = self.current_column {
            if !props.contains(&"primary_key".to_string()) {
                props.push("primary_key".to_string());
            }
        }
        self
    }

    /// Mark the current column as shard key.
    pub fn shard_key(mut self) -> Self {
        if let Some((_, _, ref mut props)) = self.current_column {
            if !props.contains(&"shard_key".to_string()) {
                props.push("shard_key".to_string());
            }
        }
        self
    }

    /// Mark the current column as nullable.
    pub fn nullable(mut self) -> Self {
        if let Some((_, _, ref mut props)) = self.current_column {
            if !props.contains(&"nullable".to_string()) {
                props.push("nullable".to_string());
            }
        }
        self
    }

    /// Build the Type.
    pub fn build(mut self) -> Result<Type, KineticaError> {
        self.finish_current_column();
        Type::new(self.label, self.columns)
    }

    /// Build the Type wrapped in Arc for sharing.
    pub fn build_arc(self) -> Result<Arc<Type>, KineticaError> {
        Ok(Arc::new(self.build()?))
    }
}

/// The same label and columns.
impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label && self.columns == other.columns
    }
}

impl Eq for Type {}

impl std::hash::Hash for Type {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.columns.hash(state);
    }
}

/// `{"label":..,"columns":[..]}`, each column as [`Column`] displays.
impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns: Vec<String> = self.columns.iter().map(ToString::to_string).collect();
        write!(f, "{{\"label\":{},\"columns\":[{}]}}", avro_quoted(&self.label), columns.join(","))
    }
}

// ============================================================================
// RecordValue - Type-safe value container
// ============================================================================

/// A typed value that can be stored in a GenericRecord.
#[derive(Debug, Clone, PartialEq)]
pub enum RecordValue {
    /// Null value
    Null,
    /// 32-bit integer (also used for boolean, int8, int16)
    Int(i32),
    /// 64-bit integer (also used for timestamp)
    Long(i64),
    /// 32-bit float
    Float(f32),
    /// 64-bit double
    Double(f64),
    /// String (also used for date, time, datetime, char, uuid, ipv4, decimal)
    String(String),
    /// Raw bytes
    Bytes(Vec<u8>),
}

impl RecordValue {
    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, RecordValue::Null)
    }

    /// Get as i32 if applicable.
    pub fn as_int(&self) -> Option<i32> {
        match self {
            RecordValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// Get as i64 if applicable.
    pub fn as_long(&self) -> Option<i64> {
        match self {
            RecordValue::Long(v) => Some(*v),
            RecordValue::Int(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Get as f32 if applicable.
    pub fn as_float(&self) -> Option<f32> {
        match self {
            RecordValue::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Get as f64 if applicable.
    pub fn as_double(&self) -> Option<f64> {
        match self {
            RecordValue::Double(v) => Some(*v),
            RecordValue::Float(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Get as string if applicable.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            RecordValue::String(v) => Some(v),
            _ => None,
        }
    }

    /// Get as bytes if applicable.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RecordValue::Bytes(v) => Some(v),
            _ => None,
        }
    }

    /// Get as boolean (from int).
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            RecordValue::Int(v) => Some(*v != 0),
            _ => None,
        }
    }
}

// Convenient From implementations
impl From<i32> for RecordValue {
    fn from(v: i32) -> Self { RecordValue::Int(v) }
}
impl From<i64> for RecordValue {
    fn from(v: i64) -> Self { RecordValue::Long(v) }
}
impl From<f32> for RecordValue {
    fn from(v: f32) -> Self { RecordValue::Float(v) }
}
impl From<f64> for RecordValue {
    fn from(v: f64) -> Self { RecordValue::Double(v) }
}
impl From<String> for RecordValue {
    fn from(v: String) -> Self { RecordValue::String(v) }
}
impl From<&str> for RecordValue {
    fn from(v: &str) -> Self { RecordValue::String(v.to_string()) }
}
impl From<Vec<u8>> for RecordValue {
    fn from(v: Vec<u8>) -> Self { RecordValue::Bytes(v) }
}
impl From<bool> for RecordValue {
    fn from(v: bool) -> Self { RecordValue::Int(if v { 1 } else { 0 }) }
}
impl<T: Into<RecordValue>> From<Option<T>> for RecordValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => RecordValue::Null,
        }
    }
}

// ============================================================================
// GenericRecord - Dynamic record implementation
// ============================================================================

/// A generic record that can hold values for any Kinetica type.
///
/// It stores values in an array indexed by column position, and uses
/// the associated `Type` for schema information.
///
/// # Example
///
/// ```no_run
/// # use gpudb_api::{Kinetica, Type};
/// # use std::sync::Arc;
/// # async fn example(kinetica: Kinetica) -> Result<(), gpudb_api::KineticaError> {
/// let record_type = Type::from_table(&kinetica, "my_table").await?;
/// let type_arc = Arc::new(record_type);
///
/// let mut record = type_arc.new_record();
///
/// // Set values by index
/// record.set(0, 42)?;
/// record.set(1, "Alice")?;
///
/// // Set values by name; a name the type does not have is an error
/// record.set_by_name("score", 95.5)?;
///
/// // Get values
/// let id: i32 = record.get_int(0).unwrap();
/// let name: &str = record.get_string_by_name("name").unwrap();
/// # Ok(())
/// # }
/// ```
///
/// # Dates and times
///
/// A date, time or datetime column holds text in Kinetica's formats:
/// `yyyy-MM-dd`, `HH:mm:ss.SSS` and `yyyy-MM-dd HH:mm:ss.SSS`. A record reads
/// no other formats; format the value with chrono first:
///
/// ```
/// use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
///
/// // Text without an offset is kept as it is
/// let date = NaiveDate::parse_from_str("01/15/2024", "%m/%d/%Y").unwrap();
/// assert_eq!(date.format("%Y-%m-%d").to_string(), "2024-01-15");
/// let time = NaiveTime::parse_from_str("10:30:45.123999", "%H:%M:%S%.f").unwrap();
/// assert_eq!(time.format("%H:%M:%S%.3f").to_string(), "10:30:45.123");
/// let datetime = NaiveDateTime::parse_from_str("15.01.2024 10:30", "%d.%m.%Y %H:%M").unwrap();
/// assert_eq!(datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string(), "2024-01-15 10:30:00.000");
/// // A date alone, for a datetime column, is the start of the day
/// let midnight = date.and_time(NaiveTime::MIN);
/// assert_eq!(midnight.format("%Y-%m-%d %H:%M:%S%.3f").to_string(), "2024-01-15 00:00:00.000");
///
/// // Text with an offset is converted to the offset of the time zone given
/// // (a fixed offset, without daylight saving)
/// let india = FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
/// let zoned = DateTime::parse_from_rfc3339("2024-01-15T23:30:00Z").unwrap().with_timezone(&india);
/// assert_eq!(zoned.format("%Y-%m-%d %H:%M:%S%.3f").to_string(), "2024-01-16 05:00:00.000");
/// assert_eq!(zoned.format("%Y-%m-%d").to_string(), "2024-01-16");
/// ```
///
/// Fractions of a second past the millisecond are dropped, not rounded.
/// chrono refuses a day past the end of its month (`2023-02-30`) and the time
/// `24:00`. Zone names (`America/New_York`, `EST`) need a zone database, such
/// as the `chrono-tz` crate. A fixed offset ignores daylight saving, so give
/// the offset in effect if that is what is meant.
#[derive(Debug, Clone)]
pub struct GenericRecord {
    /// Reference to the type definition
    record_type: Arc<Type>,
    /// Values stored by column index
    values: Vec<RecordValue>,
}

impl GenericRecord {
    /// Decodes a record of `record_type` from its Avro binary form.
    pub fn decode(record_type: &Arc<Type>, encoded: &[u8]) -> Result<GenericRecord, KineticaError> {
        let mut records = Self::decode_all(record_type, &[encoded])?;
        Ok(records.remove(0))
    }

    /// Decodes records of `record_type` from their Avro binary forms.
    pub fn decode_all<B: AsRef<[u8]>>(
        record_type: &Arc<Type>,
        encoded: &[B],
    ) -> Result<Vec<GenericRecord>, KineticaError> {
        let schema: serde_json::Value = serde_json::from_str(record_type.schema_string())
            .map_err(|e| KineticaError::Api(format!("Could not decode object: {e}")))?;
        encoded
            .iter()
            .map(|record| {
                let AvroValue::Record(values) = crate::avro_value::decode(&schema, record.as_ref())? else {
                    return Err(KineticaError::Api("Could not decode object".to_string()));
                };
                Ok(GenericRecord {
                    record_type: record_type.clone(),
                    values: values.into_iter().map(record_value).collect(),
                })
            })
            .collect()
    }

    /// The records of a dynamic (column-wise) response, one per row, typed by
    /// `record_type` (see [`Type::from_dynamic_schema`]).
    pub fn transpose(
        schema_string: &str,
        encoded_data: &[u8],
        record_type: &Arc<Type>,
    ) -> Result<Vec<GenericRecord>, KineticaError> {
        let (_, data) = decode_dynamic(schema_string, encoded_data)?;
        let record_count = column_len(data.first());
        let column_count = record_type.column_count();
        let columns: Vec<&[AvroValue]> = data
            .iter()
            .take(column_count)
            .map(|column| match column {
                AvroValue::Array(items) => items.as_slice(),
                _ => &[],
            })
            .collect();
        Ok((0..record_count)
            .map(|row| GenericRecord {
                record_type: record_type.clone(),
                values: (0..column_count)
                    .map(|c| columns.get(c).and_then(|items| items.get(row)).cloned().map_or(RecordValue::Null, record_value))
                    .collect(),
            })
            .collect())
    }

    /// Create a new GenericRecord for the given type.
    ///
    /// All values are initialized to Null.
    pub fn new(record_type: Arc<Type>) -> Self {
        let values = vec![RecordValue::Null; record_type.column_count()];
        Self { record_type, values }
    }

    /// Get the type definition for this record.
    pub fn record_type(&self) -> &Arc<Type> {
        &self.record_type
    }

    /// Get a value by index.
    pub fn get(&self, index: usize) -> Option<&RecordValue> {
        self.values.get(index)
    }

    /// Get a value by column name.
    pub fn get_by_name(&self, name: &str) -> Option<&RecordValue> {
        self.record_type.column_index(name)
            .and_then(|idx| self.values.get(idx))
    }

    /// Sets the value at `index`. The value is not checked against the
    /// column's type here; encoding the record does that. An index past the
    /// last column fails.
    pub fn set<V: Into<RecordValue>>(&mut self, index: usize, value: V) -> Result<(), KineticaError> {
        let length = self.values.len();
        let slot = self.values.get_mut(index).ok_or_else(|| {
            KineticaError::Config(format!("Column index {index} is out of range; the record has {length} columns"))
        })?;
        *slot = value.into();
        Ok(())
    }

    /// Sets the value of the column `name`. A name the type does not have
    /// fails with "Field '<name>' does not exist."
    pub fn set_by_name<V: Into<RecordValue>>(&mut self, name: &str, value: V) -> Result<(), KineticaError> {
        let index = self.record_type.column_index_or_error(name)?;
        self.set(index, value)
    }

    /// Sets the value at `index` to null; see [`GenericRecord::set`].
    pub fn set_null(&mut self, index: usize) -> Result<(), KineticaError> {
        self.set(index, RecordValue::Null)
    }

    /// Sets the value of the column `name` to null; see
    /// [`GenericRecord::set_by_name`].
    pub fn set_null_by_name(&mut self, name: &str) -> Result<(), KineticaError> {
        self.set_by_name(name, RecordValue::Null)
    }

    // Typed getters by index

    /// Get integer value by index.
    pub fn get_int(&self, index: usize) -> Option<i32> {
        self.get(index).and_then(|v| v.as_int())
    }

    /// Get long value by index. An int value is widened.
    pub fn get_long(&self, index: usize) -> Option<i64> {
        self.get(index).and_then(|v| v.as_long())
    }

    /// Get float value by index.
    pub fn get_float(&self, index: usize) -> Option<f32> {
        self.get(index).and_then(|v| v.as_float())
    }

    /// Get double value by index. A float value is widened.
    pub fn get_double(&self, index: usize) -> Option<f64> {
        self.get(index).and_then(|v| v.as_double())
    }

    /// Get string value by index.
    pub fn get_string(&self, index: usize) -> Option<&str> {
        self.get(index).and_then(|v| v.as_string())
    }

    /// Get bytes value by index.
    pub fn get_bytes(&self, index: usize) -> Option<&[u8]> {
        self.get(index).and_then(|v| v.as_bytes())
    }

    /// Get boolean value by index.
    pub fn get_bool(&self, index: usize) -> Option<bool> {
        self.get(index).and_then(|v| v.as_bool())
    }

    // Typed getters by name

    /// Get integer value by name.
    pub fn get_int_by_name(&self, name: &str) -> Option<i32> {
        self.get_by_name(name).and_then(|v| v.as_int())
    }

    /// Get long value by name.
    pub fn get_long_by_name(&self, name: &str) -> Option<i64> {
        self.get_by_name(name).and_then(|v| v.as_long())
    }

    /// Get float value by name.
    pub fn get_float_by_name(&self, name: &str) -> Option<f32> {
        self.get_by_name(name).and_then(|v| v.as_float())
    }

    /// Get double value by name.
    pub fn get_double_by_name(&self, name: &str) -> Option<f64> {
        self.get_by_name(name).and_then(|v| v.as_double())
    }

    /// Get string value by name.
    pub fn get_string_by_name(&self, name: &str) -> Option<&str> {
        self.get_by_name(name).and_then(|v| v.as_string())
    }

    /// Get bytes value by name.
    pub fn get_bytes_by_name(&self, name: &str) -> Option<&[u8]> {
        self.get_by_name(name).and_then(|v| v.as_bytes())
    }

    /// Get boolean value by name.
    pub fn get_bool_by_name(&self, name: &str) -> Option<bool> {
        self.get_by_name(name).and_then(|v| v.as_bool())
    }

    // ==================== Column value helpers ====================

    /// The column at `index`, or an error for an index past the last column.
    fn column_at(&self, index: usize) -> Result<&Column, KineticaError> {
        self.record_type.get_column(index).ok_or_else(|| {
            KineticaError::Config(format!(
                "Column index {index} is out of range; the type has {} columns",
                self.record_type.column_count()
            ))
        })
    }

    /// The elements of an array column, or the floats of a vector column. An
    /// array column's value is JSON text, read as an array of its element
    /// type (see [`Column::array_type`]), each element `None` where it is
    /// null, with these coercions: a number from text, an integer from a
    /// fraction (truncated), a boolean from a number. Bytes elements arrive
    /// as text whose characters are the bytes, or as integers, which become
    /// their minimal big-endian bytes.
    pub fn get_array(&self, index: usize) -> Result<ArrayValue, KineticaError> {
        use serde_json::Value as Json;
        let column = self.column_at(index)?;
        if column.is_vector() {
            return self.get_vector(index).map(ArrayValue::Vector);
        }
        let element_type = column.array_type()?;
        // The element type is checked before anything is read
        let known = matches!(
            element_type,
            Some(
                ColumnType::Boolean
                    | ColumnType::Integer
                    | ColumnType::Long
                    | ColumnType::Ulong
                    | ColumnType::Float
                    | ColumnType::Double
                    | ColumnType::String
                    | ColumnType::Bytes
            )
        );
        if !known {
            let name = element_type.map_or_else(|| "null".to_string(), |t| format!("{t:?}").to_uppercase());
            return Err(KineticaError::Api(format!("Unknown array type: {name}")));
        }
        let text = self.get_string(index).unwrap_or("null");
        let elements: Vec<Json> = match serde_json::from_str(text) {
            Ok(Json::Array(elements)) => elements,
            Ok(other) => return Err(KineticaError::Api(format!("Cannot read {other} as an array"))),
            Err(e) => return Err(KineticaError::Api(e.to_string())),
        };
        let unreadable = |element: &Json| KineticaError::Api(format!("Cannot read {element} as an array element"));
        let number = |element: &Json| -> Result<Option<f64>, KineticaError> {
            match element {
                Json::Null => Ok(None),
                Json::Number(n) => Ok(n.as_f64()),
                Json::String(text) => text.trim().parse().map(Some).map_err(|_| unreadable(element)),
                _ => Err(unreadable(element)),
            }
        };
        let integer = |element: &Json| -> Result<Option<i64>, KineticaError> {
            match element {
                Json::Number(n) if n.is_i64() => Ok(n.as_i64()),
                Json::String(text) if text.trim().parse::<i64>().is_ok() => Ok(text.trim().parse().ok()),
                _ => Ok(number(element)?.map(|v| v as i64)),
            }
        };
        Ok(match element_type {
            Some(ColumnType::Boolean) => ArrayValue::Boolean(
                elements
                    .iter()
                    .map(|e| match e {
                        Json::Bool(b) => Ok(Some(*b)),
                        Json::Null => Ok(None),
                        Json::Number(n) => Ok(Some(n.as_f64() != Some(0.0))),
                        Json::String(t) if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") => {
                            Ok(Some(t.eq_ignore_ascii_case("true")))
                        }
                        _ => Err(unreadable(e)),
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Some(ColumnType::Integer) => ArrayValue::Integer(
                elements.iter().map(|e| Ok(integer(e)?.map(|v| v as i32))).collect::<Result<_, KineticaError>>()?,
            ),
            Some(ColumnType::Long) => ArrayValue::Long(elements.iter().map(integer).collect::<Result<_, _>>()?),
            Some(ColumnType::Ulong) => ArrayValue::Ulong(
                elements
                    .iter()
                    .map(|e| match e {
                        Json::Null => Ok(None),
                        Json::Number(n) => n.as_u64().map(Some).ok_or_else(|| unreadable(e)),
                        Json::String(t) => t.trim().parse::<u64>().map(Some).map_err(|_| unreadable(e)),
                        _ => Err(unreadable(e)),
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Some(ColumnType::Float) => ArrayValue::Float(
                elements.iter().map(|e| Ok(number(e)?.map(|v| v as f32))).collect::<Result<_, KineticaError>>()?,
            ),
            Some(ColumnType::Double) => ArrayValue::Double(elements.iter().map(number).collect::<Result<_, _>>()?),
            Some(ColumnType::String) => ArrayValue::String(
                elements
                    .iter()
                    .map(|e| match e {
                        Json::Null => Ok(None),
                        Json::String(t) => Ok(Some(t.clone())),
                        Json::Number(_) | Json::Bool(_) => Ok(Some(e.to_string())),
                        _ => Err(unreadable(e)),
                    })
                    .collect::<Result<_, _>>()?,
            ),
            Some(ColumnType::Bytes) => ArrayValue::Bytes(
                elements
                    .iter()
                    .map(|e| match e {
                        // Each character is a byte, as the server writes them
                        Json::String(t) => Some(t.chars().map(|c| c as u32 as u8).collect()),
                        Json::Number(n) => n.as_i64().map(minimal_big_endian),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => unreachable!("an element type checked above"),
        })
    }

    /// [`Self::get_array`], by column name.
    pub fn get_array_by_name(&self, name: &str) -> Result<ArrayValue, KineticaError> {
        self.get_array(self.record_type.column_index_or_error(name)?)
    }

    /// The floats of a vector column, read little-endian from its bytes.
    /// Fails with "Not a vector column" for a column without a `vector(N)`
    /// property.
    pub fn get_vector(&self, index: usize) -> Result<Vec<f32>, KineticaError> {
        let dims = self.column_at(index)?.vector_dimensions();
        if dims < 0 {
            return Err(KineticaError::Api("Not a vector column".to_string()));
        }
        let dims = dims as usize;
        let bytes = self.get_bytes(index).unwrap_or_default();
        if bytes.len() < dims * 4 {
            return Err(KineticaError::Api(format!(
                "Vector value of {} bytes is too short for {dims} dimensions",
                bytes.len()
            )));
        }
        Ok(bytes.as_chunks::<4>().0.iter().take(dims).map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]])).collect())
    }

    /// [`Self::get_vector`], by column name.
    pub fn get_vector_by_name(&self, name: &str) -> Result<Vec<f32>, KineticaError> {
        self.get_vector(self.record_type.column_index_or_error(name)?)
    }

    /// A JSON column's object.
    pub fn get_json(&self, index: usize) -> Result<serde_json::Map<String, serde_json::Value>, KineticaError> {
        self.column_at(index)?;
        let text = self.get_string(index).ok_or_else(|| KineticaError::Api("The JSON column's value is null".to_string()))?;
        match serde_json::from_str(text) {
            Ok(serde_json::Value::Object(object)) => Ok(object),
            Ok(other) => Err(KineticaError::Api(format!("Cannot read {other} as a JSON object"))),
            Err(e) => Err(KineticaError::Api(e.to_string())),
        }
    }

    /// [`Self::get_json`], by column name.
    pub fn get_json_by_name(&self, name: &str) -> Result<serde_json::Map<String, serde_json::Value>, KineticaError> {
        self.get_json(self.record_type.column_index_or_error(name)?)
    }

    /// Puts a value given as JSON: text as it is, null as null, anything else
    /// as its JSON text.
    fn set_as_json<V: Serialize>(&mut self, index: usize, value: &V) -> Result<(), KineticaError> {
        self.column_at(index)?;
        let value = serde_json::to_value(value).map_err(|e| KineticaError::Api(e.to_string()))?;
        match value {
            serde_json::Value::Null => self.set_null(index),
            serde_json::Value::String(text) => self.set(index, text),
            other => self.set(index, other.to_string()),
        }
    }

    /// Puts an array column's value. Text goes as it is, `None` as null, and
    /// anything else (a `Vec<i32>`, say) as its JSON text.
    pub fn set_array<V: Serialize>(&mut self, index: usize, value: &V) -> Result<(), KineticaError> {
        self.set_as_json(index, value)
    }

    /// [`Self::set_array`], by column name.
    pub fn set_array_by_name<V: Serialize>(&mut self, name: &str, value: &V) -> Result<(), KineticaError> {
        let index = self.record_type.column_index_or_error(name)?;
        self.set_array(index, value)
    }

    /// Puts a JSON column's value. Text goes as it is, `None` as null, and
    /// anything else (a map, say) as its JSON text.
    pub fn set_json<V: Serialize>(&mut self, index: usize, value: &V) -> Result<(), KineticaError> {
        self.set_as_json(index, value)
    }

    /// [`Self::set_json`], by column name.
    pub fn set_json_by_name<V: Serialize>(&mut self, name: &str, value: &V) -> Result<(), KineticaError> {
        let index = self.record_type.column_index_or_error(name)?;
        self.set_json(index, value)
    }

    /// Puts a bytes column's value, or null.
    pub fn set_bytes(&mut self, index: usize, value: Option<Vec<u8>>) -> Result<(), KineticaError> {
        match value {
            Some(bytes) => self.set(index, bytes),
            None => self.set_null(index),
        }
    }

    /// [`Self::set_bytes`], by column name.
    pub fn set_bytes_by_name(&mut self, name: &str, value: Option<Vec<u8>>) -> Result<(), KineticaError> {
        let index = self.record_type.column_index_or_error(name)?;
        self.set_bytes(index, value)
    }

    /// Puts a vector column's floats, little-endian. Fails with "Not a vector
    /// column" for a column without a `vector(N)` property, and with
    /// "Unmatched vector dimensions: found <n> but expected <N>" for another
    /// number of floats.
    pub fn set_vector(&mut self, index: usize, value: &[f32]) -> Result<(), KineticaError> {
        let dims = self.column_at(index)?.vector_dimensions();
        if dims < 0 {
            return Err(KineticaError::Api("Not a vector column".to_string()));
        }
        if value.len() != dims as usize {
            return Err(KineticaError::Api(format!(
                "Unmatched vector dimensions: found {} but expected {dims}",
                value.len()
            )));
        }
        self.set(index, value.iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>())
    }

    /// Puts a vector column's floats given as a JSON array: "Error reading
    /// vector string" where it is not one.
    pub fn set_vector_text(&mut self, index: usize, text: &str) -> Result<(), KineticaError> {
        let floats: Vec<f32> = serde_json::from_str(text)
            .map_err(|e| KineticaError::caused("Error reading vector string", KineticaError::Api(e.to_string())))?;
        self.set_vector(index, &floats)
    }

    /// [`Self::set_vector`], by column name.
    pub fn set_vector_by_name(&mut self, name: &str, value: &[f32]) -> Result<(), KineticaError> {
        let index = self.record_type.column_index_or_error(name)?;
        self.set_vector(index, value)
    }

    /// Puts a decimal column's value: text as it is, null as null, and a
    /// number at the column's scale, failing where that would drop a digit,
    /// and for a NaN or infinity. Trailing zeros
    /// after the decimal point are stripped ("120.00" is "120."). A whole
    /// number at scale 0 has no decimal point, so it keeps its value.
    pub fn set_decimal(&mut self, index: usize, value: impl Into<RecordValue>) -> Result<(), KineticaError> {
        let scale = self.column_at(index)?.decimal_scale().max(0) as usize;
        let not_finite = |v: f64| KineticaError::Config(format!("A decimal cannot hold {v}"));
        let number = match value.into() {
            RecordValue::Null => return self.set_null(index),
            RecordValue::String(text) => return self.set(index, text),
            RecordValue::Int(v) => v.to_string(),
            RecordValue::Long(v) => v.to_string(),
            RecordValue::Float(v) if v.is_finite() => crate::text_format::double_to_string(f64::from(v)),
            RecordValue::Double(v) if v.is_finite() => crate::text_format::double_to_string(v),
            RecordValue::Float(v) => return Err(not_finite(f64::from(v))),
            RecordValue::Double(v) => return Err(not_finite(v)),
            RecordValue::Bytes(_) => {
                return Err(KineticaError::Config(
                    "Unsupported value type: bytes. Expected a number or decimal text.".to_string(),
                ))
            }
        };
        let text = crate::big_decimal::number_text_at_scale(&number, scale).ok_or_else(|| {
            KineticaError::Config(format!("{number} has more decimal places than the column's scale of {scale}"))
        })?;
        self.set(index, text)
    }

    /// [`Self::set_decimal`], by column name.
    pub fn set_decimal_by_name(&mut self, name: &str, value: impl Into<RecordValue>) -> Result<(), KineticaError> {
        let index = self.record_type.column_index_or_error(name)?;
        self.set_decimal(index, value)
    }

    /// The record's values by column name, in column order.
    pub fn data_map(&self) -> Vec<(&str, &RecordValue)> {
        self.record_type.columns().map(Column::name).zip(&self.values).collect()
    }
}

/// The elements of an array or vector column, as
/// [`GenericRecord::get_array`] gives them: an array of the element type,
/// each element `None` where it is null, or a vector's floats.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayValue {
    /// `array(boolean)`
    Boolean(Vec<Option<bool>>),
    /// `array(int)`
    Integer(Vec<Option<i32>>),
    /// `array(long)`
    Long(Vec<Option<i64>>),
    /// `array(ulong)`
    Ulong(Vec<Option<u64>>),
    /// `array(float)`
    Float(Vec<Option<f32>>),
    /// `array(double)`
    Double(Vec<Option<f64>>),
    /// `array(string)`
    String(Vec<Option<String>>),
    /// `array(bytes)`
    Bytes(Vec<Option<Vec<u8>>>),
    /// `vector(N)`
    Vector(Vec<f32>),
}

/// An integer as its minimal two's complement big-endian bytes, as
/// [`GenericRecord::get_array`] reads a bytes element given as an integer: 0
/// as one zero byte.
fn minimal_big_endian(value: i64) -> Vec<u8> {
    if value == 0 {
        return vec![0];
    }
    let magnitude = if value < 0 { !value } else { value };
    let count = ((64 - magnitude.leading_zeros()) as usize + 8) / 8;
    value.to_be_bytes()[8 - count..].to_vec()
}

/// Whether two values are equal: a float or double by its bits, every NaN
/// alike, so NaN equals NaN and 0.0 does not equal -0.0.
fn values_equal(a: &RecordValue, b: &RecordValue) -> bool {
    match (a, b) {
        (RecordValue::Float(x), RecordValue::Float(y)) => {
            (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits()
        }
        (RecordValue::Double(x), RecordValue::Double(y)) => {
            (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits()
        }
        _ => a == b,
    }
}

/// The same type, and every value equal, floats by their bits with every NaN
/// alike.
impl PartialEq for GenericRecord {
    fn eq(&self, other: &Self) -> bool {
        (Arc::ptr_eq(&self.record_type, &other.record_type) || self.record_type == other.record_type)
            && self.values.len() == other.values.len()
            && self.values.iter().zip(&other.values).all(|(a, b)| values_equal(a, b))
    }
}

impl Eq for GenericRecord {}

/// Consistent with [`GenericRecord`]'s equality: floats by their bits,
/// every NaN alike.
impl std::hash::Hash for GenericRecord {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for value in &self.values {
            std::mem::discriminant(value).hash(state);
            match value {
                RecordValue::Null => {}
                RecordValue::Int(v) => v.hash(state),
                RecordValue::Long(v) => v.hash(state),
                RecordValue::Float(v) => (if v.is_nan() { f32::NAN } else { *v }).to_bits().hash(state),
                RecordValue::Double(v) => (if v.is_nan() { f64::NAN } else { *v }).to_bits().hash(state),
                RecordValue::String(v) => v.hash(state),
                RecordValue::Bytes(v) => v.hash(state),
            }
        }
    }
}

/// `{"name":value,..}`, each name quoted and escaped, and each value as its
/// text: a number bare (NaN and the infinities quoted), text and bytes
/// quoted, and null as `null`.
impl std::fmt::Display for GenericRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields: Vec<String> = self
            .record_type
            .columns()
            .zip(&self.values)
            .map(|(column, value)| format!("{}:{}", avro_quoted(column.name()), avro_value_text(value)))
            .collect();
        write!(f, "{{{}}}", fields.join(","))
    }
}

// Implement Serialize for Avro encoding
/// Encodes the record for its Avro schema, field by field in column order,
/// each value converted for its column:
///
/// - a number goes into an `int`, `long`, `float` or `double` column whatever
///   its own type, converted as `as` converts it (truncated, or rounded to
///   the nearest);
/// - a string goes only into a `string` column, and bytes only into `bytes`;
/// - a nullable column is an Avro union, which takes a value only of the
///   column's own type, unconverted, or null.
///
/// A value the column cannot take fails the record with a message that
/// begins with `ENCODE_FAILURE_PREFIX`.
impl Serialize for GenericRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.values.len()))?;
        for (col, value) in self.record_type.columns.iter().zip(&self.values) {
            let written = written_value(col, value).map_err(serde::ser::Error::custom)?;
            map.serialize_entry(&col.name, &written)?;
        }
        map.end()
    }
}

/// How the message for a record that could not be encoded begins, before
/// the encoder's own message.
pub(crate) const ENCODE_FAILURE_PREFIX: &str = "Could not encode object: ";

/// The message for a record that did not encode, from the encoder's error: a
/// `GenericRecord`'s failure already begins with `ENCODE_FAILURE_PREFIX`, and
/// any other is given it and `record :: ` before it.
pub(crate) fn encode_failure_message(error: &impl std::fmt::Display) -> String {
    let message = error.to_string();
    if message.starts_with(ENCODE_FAILURE_PREFIX) {
        message
    } else {
        format!("{ENCODE_FAILURE_PREFIX}record :: {message}")
    }
}

/// The record name of every type's schema.
const RECORD_NAME: &str = "type_name";

/// A value as it is written to its column's Avro field.
enum Written<'a> {
    Null,
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(&'a str),
    Bytes(&'a [u8]),
}

impl Serialize for Written<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match *self {
            Written::Null => serializer.serialize_none(),
            Written::Int(v) => serializer.serialize_i32(v),
            Written::Long(v) => serializer.serialize_i64(v),
            Written::Float(v) => serializer.serialize_f32(v),
            Written::Double(v) => serializer.serialize_f64(v),
            Written::String(v) => serializer.serialize_str(v),
            // As Avro bytes: a slice serializes as a sequence of numbers,
            // which an Avro `bytes` field refuses
            Written::Bytes(v) => serializer.serialize_bytes(v),
        }
    }
}

/// `value` as it is written to `col`'s field, or the message it fails with: a
/// null for a column that is not nullable, a value of a type the field cannot
/// take, or one not in a nullable column's union, each message beginning
/// with `ENCODE_FAILURE_PREFIX`.
fn written_value<'a>(col: &Column, value: &'a RecordValue) -> Result<Written<'a>, String> {
    let avro_type = col.base_type.avro_type();
    let name = &col.name;

    // A union is resolved by the value's own type, with no conversion
    if col.is_nullable {
        return match value {
            RecordValue::Null => Ok(Written::Null),
            value if value_avro_type(value) == avro_type => Ok(unconverted(value)),
            value => Err(format!(
                "{ENCODE_FAILURE_PREFIX}Field has incorrect data type in record :: Not in union [\"{avro_type}\",\"null\"]: {} (field={name})",
                value_text(value)
            )),
        };
    }

    let cast_failure = || {
        format!(
            "{ENCODE_FAILURE_PREFIX}Field has incorrect data type in record :: value {} ({}) cannot be cast to expected type {avro_type} at {RECORD_NAME}.{name}",
            value_text(value),
            value_kind(value)
        )
    };
    match (col.base_type, value) {
        (_, RecordValue::Null) => Err(format!(
            "{ENCODE_FAILURE_PREFIX}record :: null value for (non-nullable) {avro_type} at {RECORD_NAME}.{name}"
        )),

        // A number converts as Rust's `as` converts it: truncated, saturated
        // or rounded to the nearest
        (ColumnBaseType::Integer, RecordValue::Int(v)) => Ok(Written::Int(*v)),
        (ColumnBaseType::Integer, RecordValue::Long(v)) => Ok(Written::Int(*v as i32)),
        (ColumnBaseType::Integer, RecordValue::Float(v)) => Ok(Written::Int(*v as i32)),
        (ColumnBaseType::Integer, RecordValue::Double(v)) => Ok(Written::Int(*v as i32)),
        (ColumnBaseType::Long, RecordValue::Int(v)) => Ok(Written::Long(i64::from(*v))),
        (ColumnBaseType::Long, RecordValue::Long(v)) => Ok(Written::Long(*v)),
        (ColumnBaseType::Long, RecordValue::Float(v)) => Ok(Written::Long(*v as i64)),
        (ColumnBaseType::Long, RecordValue::Double(v)) => Ok(Written::Long(*v as i64)),
        (ColumnBaseType::Float, RecordValue::Int(v)) => Ok(Written::Float(*v as f32)),
        (ColumnBaseType::Float, RecordValue::Long(v)) => Ok(Written::Float(*v as f32)),
        (ColumnBaseType::Float, RecordValue::Float(v)) => Ok(Written::Float(*v)),
        (ColumnBaseType::Float, RecordValue::Double(v)) => Ok(Written::Float(*v as f32)),
        (ColumnBaseType::Double, RecordValue::Int(v)) => Ok(Written::Double(f64::from(*v))),
        (ColumnBaseType::Double, RecordValue::Long(v)) => Ok(Written::Double(*v as f64)),
        (ColumnBaseType::Double, RecordValue::Float(v)) => Ok(Written::Double(f64::from(*v))),
        (ColumnBaseType::Double, RecordValue::Double(v)) => Ok(Written::Double(*v)),

        (ColumnBaseType::String, RecordValue::String(v)) => Ok(Written::String(v)),
        (ColumnBaseType::Bytes, RecordValue::Bytes(v)) => Ok(Written::Bytes(v)),

        _ => Err(cast_failure()),
    }
}

/// A non-null value as it is, for a field of its own type.
fn unconverted(value: &RecordValue) -> Written<'_> {
    match value {
        RecordValue::Null => Written::Null,
        RecordValue::Int(v) => Written::Int(*v),
        RecordValue::Long(v) => Written::Long(*v),
        RecordValue::Float(v) => Written::Float(*v),
        RecordValue::Double(v) => Written::Double(*v),
        RecordValue::String(v) => Written::String(v),
        RecordValue::Bytes(v) => Written::Bytes(v),
    }
}

/// The Avro type a value resolves a union by.
fn value_avro_type(value: &RecordValue) -> &'static str {
    match value {
        RecordValue::Null => "null",
        RecordValue::Int(_) => "int",
        RecordValue::Long(_) => "long",
        RecordValue::Float(_) => "float",
        RecordValue::Double(_) => "double",
        RecordValue::String(_) => "string",
        RecordValue::Bytes(_) => "bytes",
    }
}

/// The kind of a record value, as messages name it: the Rust type it holds.
fn value_kind(value: &RecordValue) -> &'static str {
    match value {
        RecordValue::Null => "null",
        RecordValue::Int(_) => "i32",
        RecordValue::Long(_) => "i64",
        RecordValue::Float(_) => "f32",
        RecordValue::Double(_) => "f64",
        RecordValue::String(_) => "string",
        RecordValue::Bytes(_) => "bytes",
    }
}

/// A record value's text, as messages show it: a number in its shortest
/// round-trip form, and bytes by their count.
fn value_text(value: &RecordValue) -> String {
    match value {
        RecordValue::Null => "null".to_string(),
        RecordValue::Int(v) => v.to_string(),
        RecordValue::Long(v) => v.to_string(),
        RecordValue::Float(v) => crate::text_format::float_to_string(*v),
        RecordValue::Double(v) => crate::text_format::double_to_string(*v),
        RecordValue::String(v) => v.clone(),
        RecordValue::Bytes(v) => format!("<{} bytes>", v.len()),
    }
}

/// Parses a dynamic response's schema and decodes its data, which must be a
/// record: the first steps of `Type::from_dynamic_schema` and
/// `GenericRecord::transpose`.
fn decode_dynamic(schema_string: &str, encoded_data: &[u8]) -> Result<(serde_json::Value, Vec<AvroValue>), KineticaError> {
    let schema: serde_json::Value =
        serde_json::from_str(schema_string).map_err(|_| KineticaError::Api("Schema is invalid.".to_string()))?;
    if schema.get("type").and_then(|t| t.as_str()) != Some("record") {
        return Err(KineticaError::Api("Schema must be of type record.".to_string()));
    }
    match crate::avro_value::decode(&schema, encoded_data)? {
        AvroValue::Record(fields) => Ok((schema, fields)),
        _ => Err(KineticaError::Api("Could not decode object".to_string())),
    }
}

/// The number of elements of a column of decoded dynamic data.
fn column_len(column: Option<&AvroValue>) -> usize {
    match column {
        Some(AvroValue::Array(items)) => items.len(),
        _ => 0,
    }
}

/// The name of a primitive Avro type schema, written as a name or as
/// `{"type": name}`.
fn avro_type_name(schema: &serde_json::Value) -> Option<&str> {
    schema.as_str().or_else(|| schema.get("type").and_then(|t| t.as_str()))
}

/// A decoded Avro value as a record value.
fn record_value(value: AvroValue) -> RecordValue {
    match value {
        AvroValue::Int(v) => RecordValue::Int(v),
        AvroValue::Long(v) => RecordValue::Long(v),
        AvroValue::Float(v) => RecordValue::Float(v),
        AvroValue::Double(v) => RecordValue::Double(v),
        AvroValue::String(v) => RecordValue::String(v),
        AvroValue::Bytes(v) => RecordValue::Bytes(v),
        AvroValue::Boolean(v) => RecordValue::Int(i32::from(v)),
        AvroValue::Null | AvroValue::Array(_) | AvroValue::Map(_) | AvroValue::Record(_) => RecordValue::Null,
    }
}

// Implement KineticaRecord for BulkInserter compatibility
impl KineticaRecord for GenericRecord {
    fn avro_schema(&self) -> &'static str {
        // The trait wants a static reference; the type leaks its schema once
        // for all its records
        self.record_type
            .static_schema_string
            .get_or_init(|| Box::leak(self.record_type.schema_string.clone().into_boxed_str()))
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        HashMap::new()
    }
}

// Implement ShardKeyExtractor for shard-aware routing
impl ShardKeyExtractor for GenericRecord {
    /// Returns the values of the columns the table is sharded on: its shard
    /// key columns, or its primary key columns when it has no shard key.
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        let mut values = ShardKeyValues::new();

        let key_indices = if self.record_type.shard_key_indices.is_empty() {
            &self.record_type.primary_key_indices
        } else {
            &self.record_type.shard_key_indices
        };

        for &idx in key_indices {
            let col = &self.record_type.columns[idx];
            let value = &self.values[idx];

            let shard_value = match value {
                RecordValue::Int(v) => ShardKeyValue::Int(*v),
                RecordValue::Long(v) => ShardKeyValue::Long(*v),
                RecordValue::Float(v) => ShardKeyValue::Float(*v),
                RecordValue::Double(v) => ShardKeyValue::Double(*v),
                RecordValue::String(v) => match col.column_type {
                    ColumnType::Date => ShardKeyValue::Date(v),
                    ColumnType::DateTime => ShardKeyValue::DateTime(v),
                    ColumnType::Time => ShardKeyValue::Time(v),
                    ColumnType::Ipv4 => ShardKeyValue::Ipv4(v),
                    ColumnType::Decimal => ShardKeyValue::Decimal(v),
                    ColumnType::Uuid => ShardKeyValue::Uuid(v),
                    _ => ShardKeyValue::String(v),
                },
                RecordValue::Null => ShardKeyValue::Null,
                RecordValue::Bytes(_) => ShardKeyValue::String(""),
            };

            values.push((col.name.as_str(), shard_value));
        }

        values
    }
}

// ============================================================================
// BulkInsertStats - Statistics from bulk insert operations
// ============================================================================

/// Statistics from a bulk insert operation.
///
/// This struct is returned by bulk insert methods in `SqlClient` and `KineticaSqlxPool`
/// to provide information about the operation's results.
#[derive(Debug, Clone, Default)]
pub struct BulkInsertStats {
    /// Number of records successfully inserted
    pub records_inserted: i64,
    /// Number of records updated (for upsert mode)
    pub records_updated: i64,
    /// Number of records that failed to insert
    pub records_failed: i64,
    /// Total elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Records per second
    pub records_per_second: f64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Each value, the bytes written for a record it encodes, and the
    // message for one it cannot.
    #[test]
    fn values_encode_or_fail_with_their_messages() {
        use RecordValue::*;
        let col = |name: &str, column_type, nullable: bool| {
            if nullable {
                Column::with_properties(name, column_type, vec!["nullable".into()])
            } else {
                Column::new(name, column_type)
            }
        };
        let record_type = Arc::new(
            Type::new(
                "t",
                vec![
                    col("id", ColumnType::Integer, false),
                    col("name", ColumnType::String, false),
                    col("note", ColumnType::String, true),
                    col("score", ColumnType::Double, false),
                    col("big", ColumnType::Long, false),
                    col("f", ColumnType::Float, false),
                    col("b", ColumnType::Bytes, false),
                    col("n2", ColumnType::Integer, true),
                ],
            )
            .unwrap(),
        );
        let schema = crate::parse_fast_schema(record_type.schema_string()).unwrap();
        let encode = |change: Option<(usize, RecordValue)>| {
            let mut record = GenericRecord::new(record_type.clone());
            for (i, v) in [Int(1), String("n".into()), Null, Double(1.0), Long(2), Float(3.0), Bytes(vec![9]), Null]
                .into_iter()
                .enumerate()
            {
                record.set(i, v).unwrap();
            }
            if let Some((i, v)) = change {
                record.set(i, v).unwrap();
            }
            let mut config = serde_avro_fast::ser::SerializerConfig::new(&schema);
            serde_avro_fast::to_datum(&record, Vec::new(), &mut config)
                .map(|bytes| bytes.iter().map(|b| format!("{b:02x}")).collect::<std::string::String>())
                .map_err(|e| e.to_string())
        };
        let ok = |hex: &str| Ok::<_, std::string::String>(hex.to_string());
        let cast = |detail: &str| Err::<std::string::String, _>(format!("Could not encode object: Field has incorrect data type in record :: {detail}"));
        let null = |detail: &str| Err::<std::string::String, _>(format!("Could not encode object: record :: {detail}"));

        let cases = [
            ("valid", None, ok("02026e02000000000000f03f0400004040020902")),
            ("null in int", Some((0, Null)), null("null value for (non-nullable) int at type_name.id")),
            ("string in int", Some((0, String("hello".into()))),
                cast("value hello (string) cannot be cast to expected type int at type_name.id")),
            ("long in int", Some((0, Long(7))), ok("0e026e02000000000000f03f0400004040020902")),
            ("huge long in int", Some((0, Long(1 << 40))), ok("00026e02000000000000f03f0400004040020902")),
            ("double in int", Some((0, Double(1.5))), ok("02026e02000000000000f03f0400004040020902")),
            ("int in string", Some((1, Int(5))),
                cast("value 5 (i32) cannot be cast to expected type string at type_name.name")),
            ("null in string", Some((1, Null)), null("null value for (non-nullable) string at type_name.name")),
            ("int in nullable string", Some((2, Int(5))), cast(r#"Not in union ["string","null"]: 5 (field=note)"#)),
            ("string in double", Some((3, String("x".into()))),
                cast("value x (string) cannot be cast to expected type double at type_name.score")),
            ("int in double", Some((3, Int(5))), ok("02026e0200000000000014400400004040020902")),
            ("int in long", Some((4, Int(5))), ok("02026e02000000000000f03f0a00004040020902")),
            ("double in float", Some((5, Double(2.5))), ok("02026e02000000000000f03f0400002040020902")),
            ("string in bytes", Some((6, String("s".into()))),
                cast("value s (string) cannot be cast to expected type bytes at type_name.b")),
            ("string in nullable int", Some((7, String("s".into()))), cast(r#"Not in union ["int","null"]: s (field=n2)"#)),
            ("long in nullable int", Some((7, Long(7))), cast(r#"Not in union ["int","null"]: 7 (field=n2)"#)),
            ("null in long", Some((4, Null)), null("null value for (non-nullable) long at type_name.big")),
            ("null in bytes", Some((6, Null)), null("null value for (non-nullable) bytes at type_name.b")),
            ("double in nullable int", Some((7, Double(1.5))), cast(r#"Not in union ["int","null"]: 1.5 (field=n2)"#)),
            ("big double in nullable string", Some((2, Double(1e20))),
                cast(r#"Not in union ["string","null"]: 1.0E20 (field=note)"#)),
            ("float in string", Some((1, Float(2.5))),
                cast("value 2.5 (f32) cannot be cast to expected type string at type_name.name")),
            ("bytes in string", Some((1, Bytes(vec![1, 2]))),
                cast("value <2 bytes> (bytes) cannot be cast to expected type string at type_name.name")),
            ("nan in int", Some((0, Double(f64::NAN))), ok("00026e02000000000000f03f0400004040020902")),
            ("huge double in int", Some((0, Double(1e300))), ok("feffffff0f026e02000000000000f03f0400004040020902")),
            ("negative double in long", Some((4, Double(-2.7))), ok("02026e02000000000000f03f0300004040020902")),
            ("long in float", Some((5, Long(123_456_789_123))), ok("02026e02000000000000f03f04c9f4e551020902")),
            ("tiny double in string", Some((1, Double(1e-5))),
                cast("value 1.0E-5 (f64) cannot be cast to expected type string at type_name.name")),
        ];
        for (name, change, expected) in cases {
            assert_eq!(encode(change), expected, "{name}");
        }
    }

    /// A record with a bytes column encodes, and decodes back to its values.
    #[test]
    fn a_bytes_column_encodes_as_avro_bytes() {
        let record_type = Arc::new(
            Type::new(
                "t",
                vec![
                    Column::new("id", ColumnType::Integer),
                    Column::new("shape", ColumnType::Bytes),
                    Column::with_properties("maybe", ColumnType::Bytes, vec!["nullable".into()]),
                ],
            )
            .unwrap(),
        );
        let schema = crate::parse_fast_schema(record_type.schema_string()).unwrap();
        for maybe in [RecordValue::Null, RecordValue::Bytes(vec![7, 8])] {
            let mut record = GenericRecord::new(record_type.clone());
            record.set(0, RecordValue::Int(1)).unwrap();
            record.set(1, RecordValue::Bytes(vec![0, 1, 2, 255])).unwrap();
            record.set(2, maybe.clone()).unwrap();

            let encoded = crate::encode_single(&schema, &record).expect("encodes");
            let decoded = GenericRecord::decode_all(&record_type, &[encoded]).unwrap();
            assert_eq!(decoded[0].get(1), Some(&RecordValue::Bytes(vec![0, 1, 2, 255])));
            assert_eq!(decoded[0].get(2), Some(&maybe));
        }
    }

    /// The key builder the bulk inserter would build for `record_type`'s table.
    fn key_builder_for(record_type: &Type) -> crate::record_key_builder::RecordKeyBuilder {
        use crate::record_key_builder::{Column as KeyColumn, KineticaType, RecordKeyBuilder};
        let columns = record_type
            .columns()
            .map(|c| KeyColumn {
                name: c.name.clone(),
                data_type: c.column_type.base_type().avro_type().to_string(),
                properties: c.properties.clone(),
            })
            .collect();
        RecordKeyBuilder::new(&KineticaType { columns }).expect("shardable key")
    }

    fn pk_only_type() -> Arc<Type> {
        Arc::new(
            Type::builder("pk_only")
                .add_int_column("a").primary_key()
                .add_string_column("b").primary_key()
                .add_string_column("c")
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn a_table_without_a_shard_key_is_sharded_on_its_primary_key() {
        let t = pk_only_type();
        let mut r = t.new_record();
        r.set(0, 7).unwrap();
        r.set(1, "x").unwrap();
        r.set(2, "not a key").unwrap();

        let values = r.shard_key_values();
        let names: Vec<&str> = values.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, ["a", "b"]);
        assert!(matches!(values[0].1, ShardKeyValue::Int(7)));
        assert!(matches!(values[1].1, ShardKeyValue::String("x")));
    }

    #[test]
    fn a_null_key_value_is_passed_on_as_null() {
        let t = pk_only_type();
        let mut r = t.new_record();
        r.set_null(0).unwrap();
        r.set(1, "x").unwrap();

        let values = r.shard_key_values();
        assert!(matches!(values[0].1, ShardKeyValue::Null));
    }

    #[test]
    fn a_shard_key_is_used_instead_of_the_primary_key() {
        let t = Arc::new(
            Type::builder("pk_and_sk")
                .add_int_column("a").primary_key().shard_key()
                .add_string_column("b").primary_key()
                .build()
                .unwrap(),
        );
        let mut r = t.new_record();
        r.set(0, 7).unwrap();
        r.set(1, "x").unwrap();

        let names: Vec<&str> = r.shard_key_values().iter().map(|(n, _)| *n).collect();
        assert_eq!(names, ["a"]);
    }

    #[test]
    fn primary_key_records_route_by_their_own_key() {
        // Every record used to supply no values here, so each got the same
        // all-zero key and went to the same rank.
        let t = pk_only_type();
        let builder = key_builder_for(&t);

        let key_of = |a: i32, b: &str| {
            let mut r = t.new_record();
            r.set(0, a).unwrap();
            r.set(1, b).unwrap();
            r.set(2, "ignored").unwrap();
            let key = builder.build_typed(&r.shard_key_values()).expect("built").expect("key");
            key.bytes().to_vec()
        };

        let mut expected = crate::record_key::RecordKey::new(16);
        expected.push_i32(7);
        expected.push_string("x");
        assert_eq!(key_of(7, "x"), expected.bytes());
        assert_ne!(key_of(7, "x"), key_of(8, "x"));
        assert_ne!(key_of(7, "x"), key_of(7, "y"));
    }

    fn props(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.iter().map(|p| p.to_string()).collect())).collect()
    }

    #[test]
    fn a_type_is_read_from_its_schema() {
        let t = Type::from_type_schema(
            "label",
            r#"{"type":"record","name":"x","fields":[
                {"name":"id","type":"int"},
                {"name":"maybe","type":["string","null"]},
                {"name":"shape","type":"bytes"}]}"#,
            &props(&[("id", &["primary_key"]), ("shape", &["wkt"])]),
        )
        .unwrap();
        assert_eq!(t.label(), "label");
        assert!(t.get_column_by_name("maybe").unwrap().is_nullable(), "a union with null is nullable");
        assert_eq!(t.get_column_by_name("shape").unwrap().column_type(), ColumnType::Wkb);
        assert_eq!(
            t.schema_string(),
            r#"{"type":"record","name":"type_name","fields":[{"name":"id","type":"int"},{"name":"maybe","type":["string","null"]},{"name":"shape","type":"bytes"}]}"#
        );
    }

    #[test]
    fn an_invalid_type_schema_is_refused_with_its_message() {
        let fail = |schema: &str| Type::from_type_schema("", schema, &HashMap::new()).unwrap_err().to_string();
        assert_eq!(fail("{"), "Configuration error: Schema is invalid.");
        assert_eq!(fail(r#"{"type":"enum"}"#), "Configuration error: Schema must be of type record.");
        assert_eq!(fail(r#"{"type":"record","fields":[]}"#), "Configuration error: Schema has no fields.");
        assert_eq!(
            fail(r#"{"type":"record","fields":[{"name":"a","type":["null","int"]}]}"#),
            "Configuration error: Field a has invalid type.",
            "null must follow the type"
        );
        assert_eq!(
            fail(r#"{"type":"record","fields":[{"name":"a","type":"boolean"}]}"#),
            "Configuration error: Field a must be of type bytes, double, float, int, long or string."
        );
        assert_eq!(
            fail(r#"{"type":"record","fields":[{"name":"a","type":"int"},{"name":"a","type":"int"}]}"#),
            "Configuration error: Duplicate field name a."
        );
    }

    #[test]
    fn field_names_are_made_avro_safe_and_unique() {
        let t = Type::new(
            "",
            vec![
                Column::new("a b", ColumnType::Integer),
                Column::new("a_b", ColumnType::Integer),
                Column::new("1x", ColumnType::Integer),
                Column::with_properties("n", ColumnType::Long, vec!["nullable".to_string()]),
            ],
        )
        .unwrap();
        let schema: serde_json::Value = serde_json::from_str(t.schema_string()).unwrap();
        let names: Vec<&str> = schema["fields"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["a_b_2", "a_b", "_x", "n"]);
        assert_eq!(schema["fields"][3]["type"], serde_json::json!(["long", "null"]), "the type, then null");
    }

    #[test]
    fn a_type_needs_columns_with_unique_names() {
        assert_eq!(
            Type::new("", Vec::new()).unwrap_err().to_string(),
            "Configuration error: At least one column must be specified."
        );
        let dup = vec![Column::new("a", ColumnType::Integer), Column::new("a", ColumnType::Long)];
        assert_eq!(Type::new("", dup).unwrap_err().to_string(), "Configuration error: Duplicate column name a specified.");
    }

    #[test]
    fn column_types_resolve_in_order_of_precedence() {
        let resolve = |avro: &str, props: &[&str]| {
            ColumnType::from_avro_and_properties(avro, &props.iter().map(|p| p.to_string()).collect::<Vec<_>>())
        };
        assert_eq!(resolve("string", &["char16", "date"]), ColumnType::Date);
        assert_eq!(resolve("string", &["wkt"]), ColumnType::Wkt);
        assert_eq!(resolve("bytes", &["wkt"]), ColumnType::Wkb);
        assert_eq!(resolve("bytes", &["vector(8)"]), ColumnType::Vector);
        assert_eq!(resolve("string", &["array(int,3)"]), ColumnType::Array);
        assert_eq!(resolve("string", &["decimal(10,2)"]), ColumnType::Decimal);
        assert_eq!(resolve("string", &["ulong"]), ColumnType::Ulong);
        assert_eq!(ColumnType::Vector.base_type(), ColumnBaseType::Bytes);
        assert_eq!(ColumnType::Wkt.base_type(), ColumnBaseType::String);
        let c = Column::from_base_type("d", ColumnBaseType::String, vec!["decimal(10,2)".to_string()]);
        assert_eq!((c.decimal_precision(), c.decimal_scale()), (10, 2));
    }

    #[test]
    fn a_table_must_exist_and_have_one_type() {
        let mut resp = ShowTableResponse::default();
        assert_eq!(
            Type::from_table_response("t", &resp).unwrap_err().to_string(),
            "API error: Table t does not exist."
        );
        resp.type_ids = vec!["1".into(), "2".into()];
        assert_eq!(
            Type::from_table_response("t", &resp).unwrap_err().to_string(),
            "API error: Table t is not homogeneous."
        );
    }

    fn encode<T: serde::Serialize>(schema: &str, value: &T) -> Vec<u8> {
        let schema: serde_avro_fast::Schema = schema.parse().unwrap();
        let mut config = serde_avro_fast::ser::SerializerConfig::new(&schema);
        serde_avro_fast::to_datum(value, Vec::new(), &mut config).unwrap()
    }

    #[test]
    fn a_record_decodes_as_its_type() {
        let t = Arc::new(
            Type::new(
                "",
                vec![
                    Column::new("id", ColumnType::Integer),
                    Column::with_properties("name", ColumnType::String, vec!["nullable".to_string()]),
                    Column::with_properties("note", ColumnType::String, vec!["nullable".to_string()]),
                ],
            )
            .unwrap(),
        );
        #[derive(serde::Serialize)]
        struct Row {
            id: i32,
            name: Option<String>,
            note: Option<String>,
        }
        let bytes = encode(t.schema_string(), &Row { id: 7, name: Some("x".into()), note: None });
        let r = GenericRecord::decode(&t, &bytes).unwrap();
        assert_eq!(r.get(0), Some(&RecordValue::Int(7)));
        assert_eq!(r.get(1), Some(&RecordValue::String("x".into())));
        assert_eq!(r.get(2), Some(&RecordValue::Null));
    }

    const DYNAMIC_SCHEMA: &str = r#"{"type":"record","name":"generic_response","fields":[
        {"name":"column_1","type":{"type":"array","items":"int"}},
        {"name":"column_2","type":{"type":"array","items":["string","null"]}},
        {"name":"column_3","type":{"type":"array","items":"string"}},
        {"name":"column_headers","type":{"type":"array","items":"string"}},
        {"name":"column_datatypes","type":{"type":"array","items":"string"}}]}"#;

    #[derive(serde::Serialize)]
    struct Dynamic {
        column_1: Vec<i32>,
        column_2: Vec<Option<String>>,
        column_3: Vec<String>,
        column_headers: Vec<String>,
        column_datatypes: Vec<String>,
    }

    fn dynamic(headers: [&str; 3], types: [&str; 3]) -> Vec<u8> {
        encode(
            DYNAMIC_SCHEMA,
            &Dynamic {
                column_1: vec![1, 2],
                column_2: vec![Some("a".into()), None],
                column_3: vec!["POINT(1 2)".into(), "POINT(3 4)".into()],
                column_headers: headers.iter().map(|h| h.to_string()).collect(),
                column_datatypes: types.iter().map(|t| t.to_string()).collect(),
            },
        )
    }

    #[test]
    fn a_dynamic_response_is_typed_and_transposed() {
        let data = dynamic(["x", "x", "shape"], ["int8", "char4", "geometry"]);
        let t = Arc::new(Type::from_dynamic_schema(DYNAMIC_SCHEMA, &data).unwrap());

        let names: Vec<&str> = t.columns().map(Column::name).collect();
        assert_eq!(names, ["x", "x_2", "shape"], "a repeated header takes a suffix");
        assert_eq!(t.get_column(0).unwrap().column_type(), ColumnType::Int8);
        assert!(t.get_column(1).unwrap().is_nullable());
        assert_eq!(t.get_column(1).unwrap().column_type(), ColumnType::Char4);
        assert_eq!(t.get_column(2).unwrap().column_type(), ColumnType::Wkt, "geometry is WKT");

        let rows = GenericRecord::transpose(DYNAMIC_SCHEMA, &data, &t).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0), Some(&RecordValue::Int(1)));
        assert_eq!(rows[0].get(1), Some(&RecordValue::String("a".into())));
        assert_eq!(rows[1].get(1), Some(&RecordValue::Null));
        assert_eq!(rows[1].get(2), Some(&RecordValue::String("POINT(3 4)".into())));
    }

    #[test]
    fn a_suffix_skips_names_later_headers_claim() {
        let data = dynamic(["x", "x", "x_2"], ["int", "string", "string"]);
        let t = Type::from_dynamic_schema(DYNAMIC_SCHEMA, &data).unwrap();
        let names: Vec<&str> = t.columns().map(Column::name).collect();
        assert_eq!(names, ["x", "x_3", "x_2"]);
    }

    #[test]
    fn a_malformed_dynamic_response_fails() {
        let data = dynamic(["a", "b", "c"], ["int", "string", "string"]);
        assert_eq!(
            Type::from_dynamic_schema("{", &data).unwrap_err().to_string(),
            "API error: Schema is invalid."
        );
        let short = encode(
            DYNAMIC_SCHEMA,
            &Dynamic {
                column_1: vec![1],
                column_2: vec![],
                column_3: vec![],
                column_headers: vec!["a".into()],
                column_datatypes: vec!["int".into(), "string".into(), "string".into()],
            },
        );
        assert_eq!(
            Type::from_dynamic_schema(DYNAMIC_SCHEMA, &short).unwrap_err().to_string(),
            "API error: Every field must have a corresponding expression."
        );
    }

    // Names as they are, not made Avro-safe.
    #[test]
    fn the_base_definition_names_columns_as_they_are() {
        let t = Type::new(
            "t",
            vec![
                Column::new("my id", ColumnType::Integer),
                Column::with_properties("name", ColumnType::Char16, vec!["char16".to_string(), "nullable".to_string()]),
            ],
        )
        .unwrap();
        assert_eq!(t.base_definition(), r#"[["my id","int"],["name","string","nullable"]]"#);
    }

    // The type property, the last of several, in place of the Avro type,
    // and the others after it.
    #[test]
    fn the_full_definition_names_type_properties() {
        let t = Type::new(
            "t",
            vec![
                Column::with_properties("id", ColumnType::Integer, vec!["primary_key".to_string()]),
                Column::with_properties("name", ColumnType::Char16, vec!["char16".to_string(), "nullable".to_string()]),
                Column::with_properties("d", ColumnType::Decimal, vec!["decimal(10,2)".to_string()]),
                Column::with_properties("u", ColumnType::Uuid, vec!["uuid".to_string(), "shard_key".to_string()]),
                Column::new("x", ColumnType::Double),
            ],
        )
        .unwrap();
        assert_eq!(
            t.full_definition(),
            r#"[["id","int","primary_key"],["name","char16","nullable"],["d","decimal(10,2)"],["u","string","uuid,shard_key"],["x","double"]]"#
        );
        assert_eq!(t.columns().nth(1).unwrap().properties(), ["char16", "nullable"], "the column is left as it is");
    }

    // The definition and properties sent, and the ID returned.
    #[tokio::test]
    async fn a_type_is_created_from_its_definition() {
        use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
        use crate::{CreateTypeRequest, CreateTypeResponse, KineticaOptions};
        let server = fake_server::start("127.0.0.99").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let sent: Arc<parking_lot::Mutex<Option<CreateTypeRequest>>> = Arc::default();
        let log = sent.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if path != "/create/type" {
                return None;
            }
            let request: CreateTypeRequest =
                serde_avro_fast::from_datum_slice(body, &crate::generated::CREATE_TYPE_REQUEST_SCHEMA).unwrap();
            let response = CreateTypeResponse {
                type_id: "id-1".into(),
                type_definition: request.type_definition.clone(),
                label: request.label.clone(),
                properties: request.properties.clone(),
                info: HashMap::new(),
            };
            *log.lock() = Some(request);
            Some(Ok(encode(&crate::generated::CREATE_TYPE_RESPONSE_SCHEMA, &response)))
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");
        let t = Type::new(
            "people",
            vec![
                Column::with_properties("id", ColumnType::Integer, vec!["primary_key".to_string()]),
                Column::with_properties("full name", ColumnType::String, vec!["nullable".to_string()]),
                Column::new("age", ColumnType::Integer),
            ],
        )
        .unwrap();

        assert_eq!(t.create(&k).await.unwrap(), "id-1");

        let request = sent.lock().clone().unwrap();
        assert_eq!(
            request.type_definition,
            r#"{"type":"record","name":"type_name","fields":[{"name":"id","type":"int"},{"name":"full name","type":["string","null"]},{"name":"age","type":"int"}]}"#
        );
        assert_eq!(request.label, "people");
        assert_eq!(request.properties.len(), 2, "only columns with properties");
        assert_eq!(request.properties["full name"], ["nullable"]);
    }

    #[test]
    fn test_type_builder() {
        let record_type = Type::builder("test_record")
            .add_int_column("id").primary_key().shard_key()
            .add_string_column("name")
            .add_double_column("score").nullable()
            .build()
            .unwrap();

        assert_eq!(record_type.column_count(), 3);
        assert_eq!(record_type.label(), "test_record");

        let id_col = record_type.get_column_by_name("id").unwrap();
        assert!(id_col.is_primary_key());
        assert!(id_col.is_shard_key());

        let score_col = record_type.get_column_by_name("score").unwrap();
        assert!(score_col.is_nullable());
    }

    #[test]
    fn test_generic_record_by_index() {
        let record_type = Arc::new(
            Type::builder("test")
                .add_int_column("id")
                .add_string_column("name")
                .add_double_column("value")
                .build()
                .unwrap()
        );

        let mut record = record_type.new_record();

        record.set(0, 42).unwrap();
        record.set(1, "Alice").unwrap();
        record.set(2, 95.5).unwrap();

        assert_eq!(record.get_int(0), Some(42));
        assert_eq!(record.get_string(1), Some("Alice"));
        assert_eq!(record.get_double(2), Some(95.5));
    }

    #[test]
    fn test_generic_record_by_name() {
        let record_type = Arc::new(
            Type::builder("test")
                .add_int_column("id")
                .add_string_column("name")
                .build()
                .unwrap()
        );

        let mut record = record_type.new_record();

        record.set_by_name("id", 42).unwrap();
        record.set_by_name("name", "Bob").unwrap();

        assert_eq!(record.get_int_by_name("id"), Some(42));
        assert_eq!(record.get_string_by_name("name"), Some("Bob"));
    }

    #[test]
    fn test_record_value_conversions() {
        let v: RecordValue = 42i32.into();
        assert_eq!(v.as_int(), Some(42));

        let v: RecordValue = "hello".into();
        assert_eq!(v.as_string(), Some("hello"));

        let v: RecordValue = true.into();
        assert_eq!(v.as_bool(), Some(true));
        assert_eq!(v.as_int(), Some(1));

        let v: RecordValue = Option::<i32>::None.into();
        assert!(v.is_null());
    }

    #[test]
    fn test_column_types() {
        assert_eq!(ColumnType::Integer.base_type(), ColumnBaseType::Integer);
        assert_eq!(ColumnType::Boolean.base_type(), ColumnBaseType::Integer);
        assert_eq!(ColumnType::Timestamp.base_type(), ColumnBaseType::Long);
        assert_eq!(ColumnType::Date.base_type(), ColumnBaseType::String);
    }

    // ==================== Equality and text ====================

    // The type and records the tests below print and compare.
    fn probe_type() -> Arc<Type> {
        let column = |name: &str, base: ColumnBaseType, properties: &[&str]| {
            Column::from_base_type(name, base, properties.iter().map(|p| p.to_string()).collect())
        };
        Arc::new(
            Type::new(
                "my \"type\"\n",
                vec![
                    column("i", ColumnBaseType::Integer, &["shard_key"]),
                    column("l", ColumnBaseType::Long, &["timestamp", "nullable"]),
                    column("f", ColumnBaseType::Float, &[]),
                    column("d", ColumnBaseType::Double, &["nullable"]),
                    column("s", ColumnBaseType::String, &["char16", "nullable"]),
                    column("b", ColumnBaseType::Bytes, &["nullable"]),
                    column("u\u{2028}x", ColumnBaseType::String, &["nullable"]),
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn types_columns_and_records_print_as_json() {
        let record_type = probe_type();
        assert_eq!(record_type.to_string(), "{\"label\":\"my \\\"type\\\"\\n\",\"columns\":[{\"name\":\"i\",\"type\":\"Integer\",\"properties\":[\"shard_key\"]},{\"name\":\"l\",\"type\":\"Long\",\"properties\":[\"timestamp\",\"nullable\"]},{\"name\":\"f\",\"type\":\"Float\",\"properties\":[]},{\"name\":\"d\",\"type\":\"Double\",\"properties\":[\"nullable\"]},{\"name\":\"s\",\"type\":\"String\",\"properties\":[\"char16\",\"nullable\"]},{\"name\":\"b\",\"type\":\"Bytes\",\"properties\":[\"nullable\"]},{\"name\":\"u\\u2028x\",\"type\":\"String\",\"properties\":[\"nullable\"]}]}");
        assert_eq!(record_type.get_column(0).unwrap().to_string(), "{\"name\":\"i\",\"type\":\"Integer\",\"properties\":[\"shard_key\"]}");

        let record = |values: Vec<RecordValue>| {
            let mut record = GenericRecord::new(record_type.clone());
            for (i, value) in values.into_iter().enumerate() {
                record.set(i, value).unwrap();
            }
            record
        };
        let first = record(vec![
            RecordValue::Int(42),
            RecordValue::Long(1_700_000_000_000),
            RecordValue::Float(1.5),
            RecordValue::Double(1e20),
            RecordValue::String("tab\there \"q\" \u{1} \u{e9} \u{20ac} \u{9f}".into()),
            RecordValue::Bytes(vec![0, 65, 200, 10]),
            RecordValue::Null,
        ]);
        assert_eq!(first.to_string(), "{\"i\":42,\"l\":1700000000000,\"f\":1.5,\"d\":1.0E20,\"s\":\"tab\\there \\\"q\\\" \\u0001 \u{e9} \\u20AC \\u009F\",\"b\":\"\\u0000A\u{c8}\\n\",\"u\\u2028x\":null}");
        let values = |f: f32, d: f64| vec![
            RecordValue::Int(-1),
            RecordValue::Null,
            RecordValue::Float(f),
            RecordValue::Double(d),
            RecordValue::String(String::new()),
            RecordValue::Null,
            RecordValue::String("x".into()),
        ];
        assert_eq!(record(values(f32::NAN, f64::NEG_INFINITY)).to_string(), "{\"i\":-1,\"l\":null,\"f\":\"NaN\",\"d\":\"-Infinity\",\"s\":\"\",\"b\":null,\"u\\u2028x\":\"x\"}");
        assert_eq!(record(values(100.0, 0.1)).to_string(), "{\"i\":-1,\"l\":null,\"f\":100.0,\"d\":0.1,\"s\":\"\",\"b\":null,\"u\\u2028x\":\"x\"}");
        assert_eq!(record(values(1.0e-5, 12_345_678.9)).to_string(), "{\"i\":-1,\"l\":null,\"f\":1.0E-5,\"d\":1.23456789E7,\"s\":\"\",\"b\":null,\"u\\u2028x\":\"x\"}");
    }

    // Columns compare by name, base type and properties; types by label and
    // columns; records by type and values, a NaN equal to a NaN.
    #[test]
    fn types_columns_and_records_compare_by_value() {
        let plain = Column::from_base_type("i", ColumnBaseType::Integer, Vec::new());
        assert_eq!(plain, Column::from_base_type("i", ColumnBaseType::Integer, Vec::new()));
        assert_ne!(plain, Column::from_base_type("i", ColumnBaseType::Integer, vec!["nullable".into()]));
        assert_ne!(plain, Column::from_base_type("i", ColumnBaseType::Long, Vec::new()));

        let (a, b) = (probe_type(), probe_type());
        assert_eq!(*a, *b);
        let mut x = GenericRecord::new(a.clone());
        let mut y = GenericRecord::new(b.clone());
        x.set(2, RecordValue::Float(f32::NAN)).unwrap();
        y.set(2, RecordValue::Float(f32::NAN)).unwrap();
        assert_eq!(x, y, "a NaN equals a NaN, as Float.equals has it");
        y.set(3, RecordValue::Double(-0.0)).unwrap();
        x.set(3, RecordValue::Double(0.0)).unwrap();
        assert_ne!(x, y, "0.0 is not -0.0");
    }

    // A column's type follows its properties, and a sub-type given has its
    // property added, so the column registers as what it reports.
    #[test]
    fn a_column_s_type_follows_its_properties() {
        let timestamp = Column::new("t", ColumnType::Timestamp);
        assert_eq!((timestamp.column_type(), timestamp.properties()), (ColumnType::Timestamp, &["timestamp".to_string()][..]));
        let date = Column::with_properties("d", ColumnType::String, vec!["date".into()]);
        assert_eq!(date.column_type(), ColumnType::Date, "the properties decide");
        let wkb = Column::new("g", ColumnType::Wkb);
        assert_eq!((wkb.column_type(), wkb.base_type()), (ColumnType::Wkb, ColumnBaseType::Bytes));
        let kept = Column::with_properties("c", ColumnType::Char16, vec!["char16".into(), "nullable".into()]);
        assert_eq!(kept.properties(), ["char16", "nullable"], "not added twice");

        let decimal = Column::new("x", ColumnType::Decimal);
        assert_eq!((decimal.decimal_precision(), decimal.decimal_scale()), (18, 4));
        let sized = Column::with_properties("x", ColumnType::String, vec!["decimal(10,2)".into()]);
        assert_eq!((sized.is_decimal(), sized.decimal_precision(), sized.decimal_scale()), (true, 10, 2));
        let plain = Column::new("x", ColumnType::String);
        assert_eq!((plain.decimal_precision(), plain.decimal_scale()), (0, 0), "left 0 without a decimal property");

        let record_type = Type::new("t", vec![timestamp]).unwrap();
        assert!(record_type.schema_string().contains(r#""type":"long""#));
    }

    // The column helpers.
    #[test]
    fn a_column_s_helpers_read_its_properties() {
        let column = |properties: &[&str]| {
            Column::with_properties("c", ColumnType::String, properties.iter().map(|p| p.to_string()).collect())
        };
        assert_eq!(column(&["default(5)"]).default_value().as_deref(), Some("5"));
        assert_eq!(column(&["init_with_now"]).default_value().as_deref(), Some("NOW()"));
        assert_eq!(column(&["init_with_uuid"]).default_value().as_deref(), Some("NEW_UUID()"));
        assert!(column(&["init_with_uuid"]).has_default());
        assert!(!column(&["nullable"]).has_default());
        assert_eq!(column(&["nullable"]).default_value(), None);

        assert!(column(&["array(int)"]).is_array());
        assert_eq!(column(&["array(int)"]).array_type().unwrap(), Some(ColumnType::Integer));
        assert_eq!(column(&["array(boolean,3)"]).array_type().unwrap(), Some(ColumnType::Boolean));
        assert_eq!(column(&["array(ulong)"]).array_type().unwrap(), Some(ColumnType::Ulong));
        assert_eq!(column(&["array(foo)"]).array_type().unwrap_err().to_string(), "API error: Unknown array type: foo");
        assert_eq!(column(&["nullable"]).array_type().unwrap(), None);

        let vector = Column::with_properties("v", ColumnType::Bytes, vec!["vector(3)".into()]);
        assert!(vector.is_vector());
        assert_eq!(vector.vector_dimensions(), 3);
        assert_eq!(column(&[]).vector_dimensions(), -1);
        assert!(vector.has_property("vector(3)"));
        assert!(!vector.has_property("vector"));
    }

    // A type refuses an empty column name or property.
    #[test]
    fn a_type_refuses_an_invalid_column() {
        let refused = |column: Column| Type::new("t", vec![column]).unwrap_err().to_string();
        assert_eq!(refused(Column::new("", ColumnType::Integer)), "Configuration error: Name must not be empty.");
        assert_eq!(
            refused(Column::with_properties("c", ColumnType::Integer, vec![String::new()])),
            "Configuration error: Properties must not be empty."
        );
    }

    // A record of the columns the helpers read and write.
    fn helper_record() -> GenericRecord {
        let column = |name: &str, base: ColumnBaseType, properties: &[&str]| {
            Column::from_base_type(name, base, properties.iter().map(|p| p.to_string()).collect())
        };
        let record_type = Type::new(
            "t",
            vec![
                column("ints", ColumnBaseType::String, &["array(int)"]),
                column("flags", ColumnBaseType::String, &["array(boolean)"]),
                column("words", ColumnBaseType::String, &["array(string)"]),
                column("blobs", ColumnBaseType::String, &["array(bytes)"]),
                column("v", ColumnBaseType::Bytes, &["vector(3)"]),
                column("doc", ColumnBaseType::String, &["json"]),
                column("price", ColumnBaseType::String, &["decimal(10,2)"]),
                column("plain", ColumnBaseType::String, &[]),
            ],
        )
        .unwrap();
        GenericRecord::new(Arc::new(record_type))
    }

    // `get_array`, `set_array`, `get_vector`, `set_vector`, `get_json` and
    // `set_json`.
    #[test]
    fn arrays_vectors_and_json_are_read_and_written() {
        let mut record = helper_record();

        record.set_array(0, &vec![Some(1), None, Some(3)]).unwrap();
        assert_eq!(record.get_string(0), Some("[1,null,3]"));
        assert_eq!(record.get_array(0).unwrap(), ArrayValue::Integer(vec![Some(1), None, Some(3)]));
        record.set_array(0, &"[2.9, \"4\"]").unwrap();
        assert_eq!(record.get_array(0).unwrap(), ArrayValue::Integer(vec![Some(2), Some(4)]), "values coerced to integers");

        record.set_array_by_name("flags", &vec![true, false]).unwrap();
        assert_eq!(record.get_array_by_name("flags").unwrap(), ArrayValue::Boolean(vec![Some(true), Some(false)]));
        record.set(2, "[\"a\",null,1]").unwrap();
        assert_eq!(record.get_array(2).unwrap(), ArrayValue::String(vec![Some("a".into()), None, Some("1".into())]));
        record.set(3, "[\"AB\",258,0,-1,255,null]").unwrap();
        assert_eq!(
            record.get_array(3).unwrap(),
            ArrayValue::Bytes(vec![Some(b"AB".to_vec()), Some(vec![1, 2]), Some(vec![0]), Some(vec![0xff]), Some(vec![0, 0xff]), None])
        );
        assert_eq!(record.get_array(7).unwrap_err().to_string(), "API error: Unknown array type: null");

        record.set_vector(4, &[1.0, -2.5, 3.25]).unwrap();
        assert_eq!(record.get_vector(4).unwrap(), [1.0, -2.5, 3.25]);
        assert_eq!(record.get_array(4).unwrap(), ArrayValue::Vector(vec![1.0, -2.5, 3.25]));
        assert_eq!(
            record.set_vector(4, &[1.0]).unwrap_err().to_string(),
            "API error: Unmatched vector dimensions: found 1 but expected 3"
        );
        assert_eq!(record.set_vector(0, &[1.0]).unwrap_err().to_string(), "API error: Not a vector column");
        record.set_vector_text(4, "[4, 5, 6]").unwrap();
        assert_eq!(record.get_vector_by_name("v").unwrap(), [4.0, 5.0, 6.0]);

        record.set_json(5, &serde_json::json!({"a": [1, 2]})).unwrap();
        assert_eq!(record.get_string(5), Some(r#"{"a":[1,2]}"#));
        assert_eq!(record.get_json(5).unwrap()["a"], serde_json::json!([1, 2]));
        record.set_json_by_name("doc", &r#"{"b":true}"#).unwrap();
        assert_eq!(record.get_json_by_name("doc").unwrap()["b"], serde_json::json!(true), "text as it is");
        record.set_json(5, &None::<String>).unwrap();
        assert!(record.get(5).unwrap().is_null());

        assert_eq!(
            record.get_array_by_name("missing").unwrap_err().to_string(),
            "Configuration error: Field 'missing' does not exist."
        );
    }

    // An index past the last column, or a name the type does not have,
    // fails and changes nothing.
    #[test]
    fn a_put_to_no_such_column_fails_and_changes_nothing() {
        let mut record = helper_record();
        let columns = record.record_type().column_count();
        record.set(0, 7).unwrap();
        assert_eq!(
            record.set(columns, 1).unwrap_err().to_string(),
            format!("Configuration error: Column index {columns} is out of range; the record has {columns} columns")
        );
        assert_eq!(
            record.set_by_name("nope", 1).unwrap_err().to_string(),
            "Configuration error: Field 'nope' does not exist."
        );
        assert_eq!(
            record.set_null_by_name("nope").unwrap_err().to_string(),
            "Configuration error: Field 'nope' does not exist."
        );
        assert!(record.set_null(columns).is_err());
        assert_eq!(record.get_int(0), Some(7));
    }

    // Text as it is, a number at the column's scale with trailing zeros
    // stripped (even to "100."), and a dropped digit refused.
    #[test]
    fn a_decimal_is_put_at_its_column_s_scale() {
        let mut record = helper_record();
        record.set_decimal_by_name("price", 1.5).unwrap();
        assert_eq!(record.get_string(6), Some("1.5"));
        record.set_decimal(6, 100).unwrap();
        assert_eq!(record.get_string(6), Some("100."), "the trailing zeros go; the point stays");
        record.set_decimal(6, "12.345").unwrap();
        assert_eq!(record.get_string(6), Some("12.345"), "text as it is");
        assert_eq!(
            record.set_decimal(6, 1.555).unwrap_err().to_string(),
            "Configuration error: 1.555 has more decimal places than the column's scale of 2"
        );
        assert_eq!(
            record.set_decimal(6, f64::NAN).unwrap_err().to_string(),
            "Configuration error: A decimal cannot hold NaN"
        );

        let map: Vec<&str> = record.data_map().into_iter().map(|(name, _)| name).collect();
        assert_eq!(map, ["ints", "flags", "words", "blobs", "v", "doc", "price", "plain"]);
    }
}

/// A record type's Type, from its schema and properties.
#[cfg(test)]
mod record_type_tests {
    use super::*;
    use crate::{avro_schema, AvroSchemaBuilder, KineticaColumnProperties, KineticaRecord, TypePropertiesBuilder};

    #[derive(serde::Serialize, Default)]
    struct Order {
        id: i64,
        customer: String,
        note: Option<String>,
        paid: i32,
    }

    impl KineticaRecord for Order {
        fn avro_schema(&self) -> &'static str {
            avro_schema!(AvroSchemaBuilder::new("order")
                .long_field("id")
                .string_field("customer")
                .nullable_string_field("note")
                .int_field("paid"))
        }

        fn type_properties() -> HashMap<String, Vec<String>> {
            TypePropertiesBuilder::new()
                .column("id", KineticaColumnProperties::new().primary_key().shard_key())
                .column("customer", KineticaColumnProperties::new().char_n(16))
                .column("paid", KineticaColumnProperties::new().boolean())
                .build()
        }
    }

    #[test]
    fn a_records_type_comes_from_its_schema_and_properties() {
        let record_type = Type::from_record(&Order::default()).unwrap();
        assert_eq!(record_type.label(), "order");
        let columns: Vec<(&str, ColumnType, bool)> = (0..record_type.column_count())
            .map(|i| record_type.get_column(i).unwrap())
            .map(|c| (c.name(), c.column_type(), c.is_nullable()))
            .collect();
        assert_eq!(
            columns,
            [
                ("id", ColumnType::Long, false),
                ("customer", ColumnType::Char16, false),
                ("note", ColumnType::String, true),
                ("paid", ColumnType::Boolean, false),
            ]
        );
        assert!(record_type.get_column_by_name("id").unwrap().has_property("primary_key"));
    }
}

#[cfg(test)]
mod date_time_recipe_tests {
    use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};

    const DATE: &str = "%Y-%m-%d";
    const TIME: &str = "%H:%M:%S%.3f";
    const DATETIME: &str = "%Y-%m-%d %H:%M:%S%.3f";

    /// How the recipe reads a value: as local text in a chrono format, or as
    /// RFC 3339 text with an offset, converted to the given offset.
    enum Read {
        Date(&'static str),
        DateTime(&'static str),
        Time(&'static str),
        Zoned,
    }

    fn date(text: &str, read: &Read, offset: FixedOffset) -> String {
        match read {
            Read::Date(format) => NaiveDate::parse_from_str(text, format).unwrap().format(DATE).to_string(),
            Read::DateTime(format) => NaiveDateTime::parse_from_str(text, format).unwrap().format(DATE).to_string(),
            Read::Zoned => DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&offset).format(DATE).to_string(),
            Read::Time(_) => unreachable!(),
        }
    }

    fn time(text: &str, read: &Read, offset: FixedOffset) -> String {
        match read {
            Read::Time(format) => NaiveTime::parse_from_str(text, format).unwrap().format(TIME).to_string(),
            // A time with an offset, on any day
            Read::Zoned => DateTime::parse_from_rfc3339(&format!("2000-01-01T{text}"))
                .unwrap()
                .with_timezone(&offset)
                .format(TIME)
                .to_string(),
            _ => unreachable!(),
        }
    }

    fn datetime(text: &str, read: &Read, offset: FixedOffset) -> String {
        match read {
            Read::Date(format) => {
                NaiveDate::parse_from_str(text, format).unwrap().and_time(NaiveTime::MIN).format(DATETIME).to_string()
            }
            Read::DateTime(format) => NaiveDateTime::parse_from_str(text, format).unwrap().format(DATETIME).to_string(),
            Read::Zoned => DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&offset).format(DATETIME).to_string(),
            Read::Time(_) => unreachable!(),
        }
    }

    // Each column, input and how the recipe reads it, with the text it
    // writes at a fixed offset of +05:30 and of -05:00 (New York without
    // daylight saving).
    #[test]
    fn the_recipe_writes_the_expected_text() {
        let cases: &[(&str, &str, Read, &str, &str)] = &[
            ("d", "2024-01-15", Read::Date("%Y-%m-%d"), "2024-01-15", "2024-01-15"),
            ("d", "01/15/2024", Read::Date("%m/%d/%Y"), "2024-01-15", "2024-01-15"),
            ("d", "15.01.2024", Read::Date("%d.%m.%Y"), "2024-01-15", "2024-01-15"),
            ("d", "2024-01-15 23:30", Read::DateTime("%Y-%m-%d %H:%M"), "2024-01-15", "2024-01-15"),
            ("d", "2024-01-15T23:30:00Z", Read::Zoned, "2024-01-16", "2024-01-15"),
            ("d", "2024-01-15T20:00:00-05:00", Read::Zoned, "2024-01-16", "2024-01-15"),
            ("t", "10:30", Read::Time("%H:%M"), "10:30:00.000", "10:30:00.000"),
            ("t", "10:30:45", Read::Time("%H:%M:%S"), "10:30:45.000", "10:30:45.000"),
            ("t", "10:30:45.123999", Read::Time("%H:%M:%S%.f"), "10:30:45.123", "10:30:45.123"),
            ("t", "10:30:45.5", Read::Time("%H:%M:%S%.f"), "10:30:45.500", "10:30:45.500"),
            ("t", "10:30:00Z", Read::Zoned, "16:00:00.000", "05:30:00.000"),
            ("t", "23:30:00+05:30", Read::Zoned, "23:30:00.000", "13:00:00.000"),
            ("dt", "2024-01-15", Read::Date("%Y-%m-%d"), "2024-01-15 00:00:00.000", "2024-01-15 00:00:00.000"),
            ("dt", "2024-01-15 10:30", Read::DateTime("%Y-%m-%d %H:%M"), "2024-01-15 10:30:00.000", "2024-01-15 10:30:00.000"),
            ("dt", "01/15/2024 10:30:45", Read::DateTime("%m/%d/%Y %H:%M:%S"), "2024-01-15 10:30:45.000", "2024-01-15 10:30:45.000"),
            ("dt", "15.01.2024 10:30:45.123456", Read::DateTime("%d.%m.%Y %H:%M:%S%.f"), "2024-01-15 10:30:45.123", "2024-01-15 10:30:45.123"),
            ("dt", "2024-01-15T10:30:00Z", Read::Zoned, "2024-01-15 16:00:00.000", "2024-01-15 05:30:00.000"),
            ("dt", "2024-07-15T10:30:00Z", Read::Zoned, "2024-07-15 16:00:00.000", "2024-07-15 05:30:00.000"),
            ("dt", "2024-07-15T10:30:00.987654+02:00", Read::Zoned, "2024-07-15 14:00:00.987", "2024-07-15 03:30:00.987"),
            ("dt", "0999-01-15 10:30", Read::DateTime("%Y-%m-%d %H:%M"), "0999-01-15 10:30:00.000", "0999-01-15 10:30:00.000"),
        ];
        let offsets = [FixedOffset::east_opt(5 * 3600 + 1800).unwrap(), FixedOffset::west_opt(5 * 3600).unwrap()];
        for (column, text, read, india, new_york) in cases {
            for (offset, expected) in offsets.iter().zip([india, new_york]) {
                let written = match *column {
                    "d" => date(text, read, *offset),
                    "t" => time(text, read, *offset),
                    _ => datetime(text, read, *offset),
                };
                assert_eq!(&written, expected, "{column} {text:?} at {offset}");
            }
        }
    }

    // chrono refuses a day past the end of its month, and the time 24:00.
    #[test]
    fn the_recipe_refuses_out_of_range_dates_and_times() {
        assert!(NaiveDate::parse_from_str("2023-02-30", "%Y-%m-%d").is_err());
        assert!(NaiveTime::parse_from_str("24:00", "%H:%M").is_err());
        assert!(NaiveDateTime::parse_from_str("2024-01-15 24:00", "%Y-%m-%d %H:%M").is_err());
    }
}
