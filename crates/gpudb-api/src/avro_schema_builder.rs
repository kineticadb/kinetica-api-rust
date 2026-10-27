//! Avro Schema Builder for Kinetica Records
//!
//! This module provides a builder-style API for constructing Avro schemas programmatically,
//! eliminating the need to hand-write JSON schema strings.
//!
//! # Example
//!
//! ```rust
//! use gpudb_api::avro_schema_builder::{AvroSchemaBuilder, AvroType};
//!
//! let schema = AvroSchemaBuilder::new("MyRecord")
//!     .field("id", AvroType::Int)
//!     .field("name", AvroType::String)
//!     .field("timestamp", AvroType::Long)
//!     .nullable_field("description", AvroType::String)
//!     .field("data", AvroType::Bytes)
//!     .build();
//!
//! // schema is now a valid Avro JSON schema string
//! ```

use std::collections::HashMap;

/// Avro primitive and complex types supported by Kinetica
#[derive(Debug, Clone, PartialEq)]
pub enum AvroType {
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 floating-point
    Float,
    /// 64-bit IEEE 754 floating-point
    Double,
    /// Unicode string
    String,
    /// A Kinetica boolean, which is an Avro `int` (the column also takes the
    /// `boolean` property): Kinetica has no Avro `boolean`, and refuses a type
    /// with one ("Invalid type \"boolean\""). A Rust `bool` must be written as
    /// an int for it: see [`bool_as_int`].
    Boolean,
    /// Sequence of bytes
    Bytes,
    /// Nullable type: a union of the type and null, in that order, as the
    /// server keeps a nullable column's type.
    Nullable(Box<AvroType>),
}

impl AvroType {
    /// Returns the Avro JSON representation of this type
    fn to_json(&self) -> String {
        match self {
            AvroType::Int => "\"int\"".to_string(),
            AvroType::Long => "\"long\"".to_string(),
            AvroType::Float => "\"float\"".to_string(),
            AvroType::Double => "\"double\"".to_string(),
            AvroType::String => "\"string\"".to_string(),
            AvroType::Boolean => "\"int\"".to_string(),
            AvroType::Bytes => "\"bytes\"".to_string(),
            // The type first, as the server keeps it. A record is encoded
            // against this schema and decoded against the table's, and the
            // union's index is what is sent, so null first would swap value and
            // null.
            AvroType::Nullable(inner) => {
                format!("[{},\"null\"]", inner.to_json())
            }
        }
    }

    /// Creates a nullable version of this type
    pub fn nullable(self) -> Self {
        AvroType::Nullable(Box::new(self))
    }
}

/// Writes a `bool` as the Avro int a Kinetica boolean column holds, 1 or 0,
/// and reads any int other than 0 as `true`. For a field declared with
/// [`AvroSchemaBuilder::bool_field`]:
///
/// ```
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Reading {
/// #[serde(with = "gpudb_api::avro_schema_builder::bool_as_int")]
/// is_valid: bool,
/// # }
/// ```
pub mod bool_as_int {
    /// Writes `true` as 1 and `false` as 0.
    pub fn serialize<S: serde::Serializer>(value: &bool, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(i32::from(*value))
    }

    /// Reads an int, `true` unless 0.
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
        Ok(<i32 as serde::Deserialize>::deserialize(deserializer)? != 0)
    }
}

/// [`bool_as_int`] for an `Option<bool>`, a nullable boolean column.
pub mod option_bool_as_int {
    /// Writes `Some(true)` as 1, `Some(false)` as 0, and `None` as null.
    pub fn serialize<S: serde::Serializer>(value: &Option<bool>, serializer: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_some(&i32::from(*value)),
            None => serializer.serialize_none(),
        }
    }

    /// Reads a nullable int, `true` unless 0.
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<bool>, D::Error> {
        Ok(<Option<i32> as serde::Deserialize>::deserialize(deserializer)?.map(|value| value != 0))
    }
}

/// A field definition in an Avro record schema
#[derive(Debug, Clone)]
struct FieldDef {
    name: String,
    avro_type: AvroType,
    doc: Option<String>,
}

/// Renders `s` as a JSON string literal, quotes included, escaping `"`, `\`
/// and control characters.
///
/// The builder writes each name, namespace and doc through this, so every
/// string in the schema it emits is escaped correctly.
fn json_string(s: &str) -> String {
    // Serialising a &str cannot fail.
    serde_json::to_string(s).expect("serialising a string to JSON is infallible")
}

impl FieldDef {
    fn to_json(&self) -> String {
        // The name is escaped but otherwise used as given. It is not made a
        // legal Avro name by rewriting every character outside [A-Za-z0-9_]
        // (and a leading digit) to '_', because a record's serde field names
        // must match the schema's field names for encoding to work.
        let mut parts = vec![format!("\"name\":{}", json_string(&self.name))];
        parts.push(format!("\"type\":{}", self.avro_type.to_json()));
        if let Some(ref doc) = self.doc {
            parts.push(format!("\"doc\":{}", json_string(doc)));
        }
        format!("{{{}}}", parts.join(","))
    }
}

/// Builder for constructing Avro record schemas programmatically
///
/// # Example
///
/// ```rust
/// use gpudb_api::avro_schema_builder::{AvroSchemaBuilder, AvroType};
///
/// let schema = AvroSchemaBuilder::new("IngestRecord")
///     .field("timestampMillis", AvroType::Long)
///     .field("source", AvroType::Int)
///     .field("payload", AvroType::Bytes)
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct AvroSchemaBuilder {
    name: String,
    namespace: Option<String>,
    doc: Option<String>,
    fields: Vec<FieldDef>,
}

impl AvroSchemaBuilder {
    /// Creates a new schema builder with the given record name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            doc: None,
            fields: Vec::new(),
        }
    }

    /// Sets the namespace for the record
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sets the documentation for the record
    pub fn doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }

    /// Adds a field with the given name and type
    pub fn field(mut self, name: impl Into<String>, avro_type: AvroType) -> Self {
        self.fields.push(FieldDef {
            name: name.into(),
            avro_type,
            doc: None,
        });
        self
    }

    /// Adds a field with documentation
    pub fn field_with_doc(
        mut self,
        name: impl Into<String>,
        avro_type: AvroType,
        doc: impl Into<String>,
    ) -> Self {
        self.fields.push(FieldDef {
            name: name.into(),
            avro_type,
            doc: Some(doc.into()),
        });
        self
    }

    /// Adds a nullable field (shorthand for `field(name, AvroType::Nullable(...))`)
    pub fn nullable_field(self, name: impl Into<String>, avro_type: AvroType) -> Self {
        self.field(name, AvroType::Nullable(Box::new(avro_type)))
    }

    // =========================================================================
    // Convenience methods for common Kinetica field types
    // =========================================================================

    /// Adds an integer field
    pub fn int_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::Int)
    }

    /// Adds a long (i64) field
    pub fn long_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::Long)
    }

    /// Adds a float field
    pub fn float_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::Float)
    }

    /// Adds a double field
    pub fn double_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::Double)
    }

    /// Adds a string field
    pub fn string_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::String)
    }

    /// Adds a boolean field: an Avro `int`, as Kinetica stores a boolean
    /// (give the column the `boolean` property). The struct's `bool` field
    /// must be written as an int, with
    /// `#[serde(with = "gpudb_api::avro_schema_builder::bool_as_int")]`
    /// (see [`bool_as_int`]); unannotated, it fails to encode.
    pub fn bool_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::Boolean)
    }

    /// Adds a bytes field
    pub fn bytes_field(self, name: impl Into<String>) -> Self {
        self.field(name, AvroType::Bytes)
    }

    /// Adds a timestamp field (long type, typically milliseconds since epoch)
    pub fn timestamp_field(self, name: impl Into<String>) -> Self {
        self.long_field(name)
    }

    /// Adds a nullable string field
    pub fn nullable_string_field(self, name: impl Into<String>) -> Self {
        self.nullable_field(name, AvroType::String)
    }

    /// Adds a nullable int field
    pub fn nullable_int_field(self, name: impl Into<String>) -> Self {
        self.nullable_field(name, AvroType::Int)
    }

    /// Adds a nullable long field
    pub fn nullable_long_field(self, name: impl Into<String>) -> Self {
        self.nullable_field(name, AvroType::Long)
    }

    /// Adds a nullable double field
    pub fn nullable_double_field(self, name: impl Into<String>) -> Self {
        self.nullable_field(name, AvroType::Double)
    }

    /// Adds a nullable bytes field
    pub fn nullable_bytes_field(self, name: impl Into<String>) -> Self {
        self.nullable_field(name, AvroType::Bytes)
    }

    /// Builds the Avro schema JSON string
    pub fn build(&self) -> String {
        let mut parts = vec!["\"type\":\"record\"".to_string()];
        parts.push(format!("\"name\":{}", json_string(&self.name)));

        if let Some(ref ns) = self.namespace {
            parts.push(format!("\"namespace\":{}", json_string(ns)));
        }

        if let Some(ref doc) = self.doc {
            parts.push(format!("\"doc\":{}", json_string(doc)));
        }

        let fields_json: Vec<String> = self.fields.iter().map(|f| f.to_json()).collect();
        parts.push(format!("\"fields\":[{}]", fields_json.join(",")));

        format!("{{{}}}", parts.join(","))
    }

    /// Builds the schema and leaks it to get a `&'static str`
    ///
    /// This is useful for implementing `KineticaRecord::avro_schema()` which
    /// requires returning `&'static str`. The memory is intentionally leaked
    /// since schemas are typically created once and live for the program's lifetime.
    ///
    /// # Example
    ///
    /// ```rust
    /// use gpudb_api::avro_schema_builder::{AvroSchemaBuilder, AvroType};
    ///
    /// let schema: &'static str = AvroSchemaBuilder::new("MyRecord")
    ///     .int_field("id")
    ///     .string_field("name")
    ///     .build_static();
    /// ```
    pub fn build_static(&self) -> &'static str {
        let schema = self.build();
        Box::leak(schema.into_boxed_str())
    }
}

/// Helper macro to define a static Avro schema using the builder pattern.
///
/// This creates a `OnceLock` that initializes the schema exactly once and returns
/// a `&'static str` reference.
///
/// # Limitation: one schema per macro call site, not per type
///
/// The plain form `avro_schema!(builder)` expands to a single `static` at the call
/// site. Rust has no generic statics, so **inside a generic function or a generic
/// `impl` every instantiation shares that one static**: the first instantiation to
/// run builds the schema and every other type parameter gets the same string back,
/// even when its builder expression would produce a different schema. Use the
/// plain form only where the schema does not depend on a type parameter.
///
/// Where it does, use the keyed form `avro_schema!(for T => builder)`, which caches
/// one schema per `T` (keyed by [`TypeId`](std::any::TypeId), so `T: 'static`).
/// Each distinct schema is built once and its string lives for the rest of the
/// program, the same as with the plain form.
///
/// # Example
///
/// ```rust
/// use gpudb_api::{avro_schema, avro_schema_builder::{AvroSchemaBuilder, AvroType}};
///
/// fn my_schema() -> &'static str {
///     avro_schema!(
///         AvroSchemaBuilder::new("MyRecord")
///             .int_field("id")
///             .string_field("name")
///             .bytes_field("payload")
///     )
/// }
/// ```
///
/// A schema that varies with a type parameter:
///
/// ```rust
/// use gpudb_api::{avro_schema, avro_schema_builder::AvroSchemaBuilder};
///
/// fn schema_for<T: 'static>() -> &'static str {
///     avro_schema!(for T =>
///         AvroSchemaBuilder::new(std::any::type_name::<T>().replace("::", "_"))
///             .int_field("id")
///     )
/// }
///
/// assert_ne!(schema_for::<u8>(), schema_for::<u16>());
/// ```
#[macro_export]
macro_rules! avro_schema {
    // Keyed form: one cached schema per type. This arm must come first -- it
    // starts with the literal `for`, so ordinary builder expressions fall
    // through to the arm below without being parsed as a type.
    (for $key:ty => $builder:expr) => {{
        static SCHEMAS: std::sync::OnceLock<
            std::sync::RwLock<std::collections::HashMap<std::any::TypeId, &'static str>>,
        > = std::sync::OnceLock::new();
        let schemas = SCHEMAS.get_or_init(Default::default);
        let key = std::any::TypeId::of::<$key>();
        let cached = schemas
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .copied();
        match cached {
            Some(schema) => schema,
            None => *schemas
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .entry(key)
                .or_insert_with(|| $builder.build_static()),
        }
    }};
    // Plain form: one schema per call site (see the limitation above).
    ($builder:expr) => {{
        static SCHEMA: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(|| $builder.build()).as_str()
    }};
}

// ============================================================================
// Kinetica Type Properties Builder
// ============================================================================

/// Kinetica column properties
#[derive(Debug, Clone, Default)]
pub struct KineticaColumnProperties {
    properties: Vec<String>,
}

impl KineticaColumnProperties {
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks this column as a primary key
    pub fn primary_key(mut self) -> Self {
        self.properties.push("primary_key".to_string());
        self
    }

    /// Marks this column as a shard key
    pub fn shard_key(mut self) -> Self {
        self.properties.push("shard_key".to_string());
        self
    }

    /// Marks this column as nullable
    pub fn nullable(mut self) -> Self {
        self.properties.push("nullable".to_string());
        self
    }

    /// Marks this column as a timestamp
    pub fn timestamp(mut self) -> Self {
        self.properties.push("timestamp".to_string());
        self
    }

    /// Marks this column as data (generic data property)
    pub fn data(mut self) -> Self {
        self.properties.push("data".to_string());
        self
    }

    /// Marks this column as storing an IPv4 address
    pub fn ipv4(mut self) -> Self {
        self.properties.push("ipv4".to_string());
        self
    }

    /// Marks this column as storing a WKT geometry
    pub fn wkt(mut self) -> Self {
        self.properties.push("wkt".to_string());
        self
    }

    /// Marks this column with char(N) type: `charN`, for N of 1, 2, 4, 8,
    /// 16, 32, 64, 128 or 256. The server refuses another size when the type
    /// is created ("Unknown property: char3").
    pub fn char_n(mut self, n: usize) -> Self {
        self.properties.push(format!("char{n}"));
        self
    }

    /// Marks this column as int8 (1 byte signed integer)
    pub fn int8(mut self) -> Self {
        self.properties.push("int8".to_string());
        self
    }

    /// Marks this column as int16 (2 byte signed integer)
    pub fn int16(mut self) -> Self {
        self.properties.push("int16".to_string());
        self
    }

    /// Marks this column as a date
    pub fn date(mut self) -> Self {
        self.properties.push("date".to_string());
        self
    }

    /// Marks this column as a time
    pub fn time(mut self) -> Self {
        self.properties.push("time".to_string());
        self
    }

    /// Marks this column as a datetime
    pub fn datetime(mut self) -> Self {
        self.properties.push("datetime".to_string());
        self
    }

    /// Marks this column as a decimal (SQL `NUMERIC(19,4)`, the server default)
    pub fn decimal(mut self) -> Self {
        self.properties.push("decimal".to_string());
        self
    }

    /// Marks this column as a decimal with the given precision and scale,
    /// i.e. `decimal(P,S)`
    ///
    /// Written with no space after the comma: the form
    /// (`decimal\((\d+),(\d+)\)`) from which precision and scale are read
    /// back out of a type's properties.
    pub fn decimal_with_precision(mut self, precision: u32, scale: u32) -> Self {
        self.properties.push(format!("decimal({precision},{scale})"));
        self
    }

    /// Marks this column as storing a UUID
    pub fn uuid(mut self) -> Self {
        self.properties.push("uuid".to_string());
        self
    }

    /// Marks this column as an unsigned long (0 to 18446744073709551615)
    pub fn ulong(mut self) -> Self {
        self.properties.push("ulong".to_string());
        self
    }

    /// Marks this column as a boolean (stored as int with 0/1 values)
    pub fn boolean(mut self) -> Self {
        self.properties.push("boolean".to_string());
        self
    }

    // Further column properties. Parameterised ones are written in the form
    // 'array(int, 10)', 'vector(1000)' or 'compress(zstd(7))', for example.

    /// Enables full text search on this (string) column
    pub fn text_search(mut self) -> Self {
        self.properties.push("text_search".to_string());
        self
    }

    /// Marks this (string) column as holding JSON values
    pub fn json(mut self) -> Self {
        self.properties.push("json".to_string());
        self
    }

    /// Marks this (string) column as an unbounded array of `item_type`
    /// (e.g. `"int"`, `"long"`, `"double"`, `"string"`), i.e. `array(item_type)`
    pub fn array(mut self, item_type: &str) -> Self {
        self.properties.push(format!("array({item_type})"));
        self
    }

    /// Marks this (string) column as an array of `item_type` holding at most
    /// `len` items, i.e. `array(item_type, len)`; a `len` of -1 means unbounded
    pub fn array_n(mut self, item_type: &str, len: i64) -> Self {
        self.properties.push(format!("array({item_type}, {len})"));
        self
    }

    /// Marks this (bytes) column as a vector of `dimensions` floats, i.e.
    /// `vector(dimensions)`
    pub fn vector(mut self, dimensions: usize) -> Self {
        self.properties.push(format!("vector({dimensions})"));
        self
    }

    /// Marks this column as part of (or all of) the soft primary key
    pub fn soft_primary_key(mut self) -> Self {
        self.properties.push("soft_primary_key".to_string());
        self
    }

    /// Compresses this column with the given codec and optional level,
    /// i.e. `compress(codec)`; pass e.g. `"snappy"` or `"zstd(7)"`
    pub fn compress(mut self, codec: &str) -> Self {
        self.properties.push(format!("compress({codec})"));
        self
    }

    /// Dictionary-encodes this column (charN, int, long or date columns)
    pub fn dict(mut self) -> Self {
        self.properties.push("dict".to_string());
        self
    }

    /// Replaces empty or invalid date/time/datetime/timestamp values with
    /// `NOW()` on insert
    pub fn init_with_now(mut self) -> Self {
        self.properties.push("init_with_now".to_string());
        self
    }

    /// Replaces empty or invalid UUID values with a random UUID on insert
    pub fn init_with_uuid(mut self) -> Self {
        self.properties.push("init_with_uuid".to_string());
        self
    }

    /// Sets this date/time/datetime/timestamp column to `NOW()` on any update
    pub fn update_with_now(mut self) -> Self {
        self.properties.push("update_with_now".to_string());
        self
    }

    /// Sets this charN or string column to the current user's name on any update
    pub fn update_with_user(mut self) -> Self {
        self.properties.push("update_with_user".to_string());
        self
    }

    /// Sets a default value expression, i.e. `default(expr)`; pass e.g. `"0"`,
    /// `"'pending'"` or `"NOW()"`
    pub fn default_value(mut self, expr: &str) -> Self {
        self.properties.push(format!("default({expr})"));
        self
    }

    /// Sets the SQL-syntax default expression shown in generated DDL, i.e.
    /// `default_sql(expr)`
    ///
    /// The parenthesised form here follows `default(expr)`. Use
    /// [`property`](Self::property) if a server expects something else.
    pub fn default_sql(mut self, expr: &str) -> Self {
        self.properties.push(format!("default_sql({expr})"));
        self
    }

    /// Adds a custom property
    pub fn property(mut self, prop: impl Into<String>) -> Self {
        self.properties.push(prop.into());
        self
    }

    /// Converts to the vector format expected by Kinetica
    pub fn build(self) -> Vec<String> {
        self.properties
    }
}

/// Builder for Kinetica type properties map
///
/// # Example
///
/// ```rust
/// use gpudb_api::avro_schema_builder::{TypePropertiesBuilder, KineticaColumnProperties};
///
/// let props = TypePropertiesBuilder::new()
///     .column("id", KineticaColumnProperties::new().primary_key())
///     .column("timestamp", KineticaColumnProperties::new().timestamp())
///     .column("name", KineticaColumnProperties::new().char_n(64))
///     .column("data", KineticaColumnProperties::new().data())
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct TypePropertiesBuilder {
    properties: HashMap<String, Vec<String>>,
}

impl TypePropertiesBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds column properties for the given column name
    ///
    /// Properties accumulate: calling this (or [`column_property`](Self::column_property)
    /// or any shorthand such as [`primary_key`](Self::primary_key)) again for the same
    /// column appends to that column's list rather than replacing it, so
    /// `.primary_key("id").shard_key("id")` yields `["primary_key", "shard_key"]`.
    /// A property already present on the column is not added a second time; the
    /// first-seen order is kept.
    pub fn column(mut self, name: impl Into<String>, props: KineticaColumnProperties) -> Self {
        let entry = self.properties.entry(name.into()).or_default();
        for prop in props.build() {
            Self::push_unique(entry, prop);
        }
        self
    }

    /// Adds a single property for a column
    ///
    /// Appends to the column's existing properties (see [`column`](Self::column));
    /// a property already present is not repeated.
    pub fn column_property(
        mut self,
        name: impl Into<String>,
        property: impl Into<String>,
    ) -> Self {
        let entry = self.properties.entry(name.into()).or_default();
        Self::push_unique(entry, property.into());
        self
    }

    /// Appends `prop` to `props` unless it is already there.
    fn push_unique(props: &mut Vec<String>, prop: String) {
        if !props.contains(&prop) {
            props.push(prop);
        }
    }

    /// Shorthand: marks a column as primary key
    pub fn primary_key(self, name: impl Into<String>) -> Self {
        self.column(name, KineticaColumnProperties::new().primary_key())
    }

    /// Shorthand: marks a column as timestamp
    pub fn timestamp(self, name: impl Into<String>) -> Self {
        self.column(name, KineticaColumnProperties::new().timestamp())
    }

    /// Shorthand: marks a column as data
    pub fn data(self, name: impl Into<String>) -> Self {
        self.column(name, KineticaColumnProperties::new().data())
    }

    /// Shorthand: marks a column as shard key
    pub fn shard_key(self, name: impl Into<String>) -> Self {
        self.column(name, KineticaColumnProperties::new().shard_key())
    }

    /// Builds the HashMap expected by Kinetica APIs
    pub fn build(self) -> HashMap<String, Vec<String>> {
        self.properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_schema() {
        let schema = AvroSchemaBuilder::new("TestRecord")
            .int_field("id")
            .string_field("name")
            .build();

        assert!(schema.contains("\"type\":\"record\""));
        assert!(schema.contains("\"name\":\"TestRecord\""));
        assert!(schema.contains("\"name\":\"id\""));
        assert!(schema.contains("\"type\":\"int\""));
        assert!(schema.contains("\"name\":\"name\""));
        assert!(schema.contains("\"type\":\"string\""));
    }

    #[test]
    fn test_ingest_record_schema() {
        let schema = AvroSchemaBuilder::new("IngestRecord")
            .long_field("timestampMillis")
            .int_field("source")
            .bytes_field("payload")
            .build();

        // Should match the hand-written schema
        let expected = r#"{"type":"record","name":"IngestRecord","fields":[{"name":"timestampMillis","type":"long"},{"name":"source","type":"int"},{"name":"payload","type":"bytes"}]}"#;
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_nullable_fields() {
        let schema = AvroSchemaBuilder::new("NullableRecord")
            .int_field("id")
            .nullable_string_field("description")
            .build();

        assert!(schema.contains("[\"string\",\"null\"]"));
    }

    // A Kinetica boolean is an Avro int, and a `bool` goes in as 1 or 0 with
    // `bool_as_int`.
    // Written as an Avro boolean, `true` would have read back as -1.
    #[test]
    fn a_boolean_field_is_an_int_written_one_or_zero() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct Row {
            #[serde(with = "super::bool_as_int")]
            ok: bool,
            #[serde(with = "super::option_bool_as_int")]
            maybe: Option<bool>,
        }

        let built = AvroSchemaBuilder::new("Row").bool_field("ok").nullable_field("maybe", AvroType::Boolean).build();
        assert!(built.contains(r#"{"name":"ok","type":"int"}"#), "{built}");
        assert!(built.contains(r#"{"name":"maybe","type":["int","null"]}"#), "{built}");

        let schema = crate::avro::parse_fast_schema(&built).unwrap();
        let row = Row { ok: true, maybe: Some(false) };
        let bytes = crate::avro::encode_single(&schema, &row).unwrap();
        // ok: int 1; maybe: branch 0, then int 0
        assert_eq!(bytes, [0x02, 0x00, 0x00]);
        let back: Row = serde_avro_fast::from_datum_slice(&bytes, &schema).unwrap();
        assert_eq!(back, row);
        let none = crate::avro::encode_single(&schema, &Row { ok: false, maybe: None }).unwrap();
        assert_eq!(none, [0x00, 0x02]);
    }

    /// A value takes the union's first branch and a null its second, the
    /// indices a table's `[T,"null"]` type gives them.
    #[test]
    fn a_nullable_field_encodes_against_the_tables_union_order() {
        #[derive(serde::Serialize)]
        struct Row {
            description: Option<String>,
            count: Option<i32>,
        }

        let schema = crate::avro::parse_fast_schema(
            &AvroSchemaBuilder::new("Row")
                .nullable_string_field("description")
                .nullable_int_field("count")
                .build(),
        )
        .unwrap();

        let some = crate::avro::encode_single(&schema, &Row { description: Some("x".into()), count: None }).unwrap();
        // description: branch 0, then "x"; count: branch 1 (null)
        assert_eq!(some, vec![0x00, 0x02, b'x', 0x02]);

        let none = crate::avro::encode_single(&schema, &Row { description: None, count: Some(3) }).unwrap();
        // description: branch 1 (null); count: branch 0, then 3
        assert_eq!(none, vec![0x02, 0x00, 0x06]);
    }

    #[test]
    fn test_with_namespace() {
        let schema = AvroSchemaBuilder::new("MyRecord")
            .namespace("com.example")
            .int_field("id")
            .build();

        assert!(schema.contains("\"namespace\":\"com.example\""));
    }

    #[test]
    fn test_type_properties_builder() {
        let props = TypePropertiesBuilder::new()
            .timestamp("timestampMillis")
            .data("source")
            .data("payload")
            .build();

        assert_eq!(props.get("timestampMillis").unwrap(), &vec!["timestamp".to_string()]);
        assert_eq!(props.get("source").unwrap(), &vec!["data".to_string()]);
    }

    #[test]
    fn test_complex_column_properties() {
        let props = TypePropertiesBuilder::new()
            .column("id", KineticaColumnProperties::new().primary_key().int16())
            .column("name", KineticaColumnProperties::new().char_n(64).nullable())
            .build();

        let id_props = props.get("id").unwrap();
        assert!(id_props.contains(&"primary_key".to_string()));
        assert!(id_props.contains(&"int16".to_string()));

        let name_props = props.get("name").unwrap();
        assert!(name_props.contains(&"char64".to_string()));
        assert!(name_props.contains(&"nullable".to_string()));
    }

    #[test]
    fn test_type_properties_accumulate_per_column() {
        // Shorthands for the same column used to replace one another, so the
        // primary key was lost here.
        let props = TypePropertiesBuilder::new()
            .primary_key("id")
            .shard_key("id")
            .build();
        assert_eq!(
            props.get("id").unwrap(),
            &vec!["primary_key".to_string(), "shard_key".to_string()]
        );

        // column(), column_property() and the shorthands all append, and a
        // repeated property is kept once.
        let props = TypePropertiesBuilder::new()
            .column("name", KineticaColumnProperties::new().char_n(64))
            .column_property("name", "nullable")
            .column("name", KineticaColumnProperties::new().char_n(64).primary_key())
            .primary_key("name")
            .build();
        assert_eq!(
            props.get("name").unwrap(),
            &vec![
                "char64".to_string(),
                "nullable".to_string(),
                "primary_key".to_string()
            ]
        );
    }

    #[test]
    fn test_type_properties_empty_column_kept() {
        // A column given no properties still gets an (empty) entry, as before.
        let props = TypePropertiesBuilder::new()
            .column("id", KineticaColumnProperties::new())
            .build();
        assert_eq!(props.get("id").unwrap(), &Vec::<String>::new());
    }

    #[test]
    fn test_schema_strings_are_json_escaped() {
        let schema = AvroSchemaBuilder::new("Rec\"ord")
            .namespace("ns\\x")
            .doc("line1\nsays \"hi\" \\ tab\t")
            .field_with_doc("we\"ird", AvroType::Int, "back\\slash \"q\"\u{1}")
            .build();

        // The whole schema must be valid JSON and round-trip every string.
        let v: serde_json::Value = serde_json::from_str(&schema).expect("valid JSON");
        assert_eq!(v["name"], "Rec\"ord");
        assert_eq!(v["namespace"], "ns\\x");
        assert_eq!(v["doc"], "line1\nsays \"hi\" \\ tab\t");
        assert_eq!(v["fields"][0]["name"], "we\"ird");
        assert_eq!(v["fields"][0]["doc"], "back\\slash \"q\"\u{1}");
    }

    #[test]
    fn test_column_property_shortcuts() {
        let props = KineticaColumnProperties::new()
            .decimal_with_precision(10, 2)
            .text_search()
            .json()
            .array("int")
            .array_n("double", 10)
            .vector(1000)
            .soft_primary_key()
            .compress("zstd(7)")
            .dict()
            .init_with_now()
            .init_with_uuid()
            .update_with_now()
            .update_with_user()
            .default_value("0")
            .default_sql("NOW()")
            .build();
        assert_eq!(
            props,
            vec![
                "decimal(10,2)",
                "text_search",
                "json",
                "array(int)",
                "array(double, 10)",
                "vector(1000)",
                "soft_primary_key",
                "compress(zstd(7))",
                "dict",
                "init_with_now",
                "init_with_uuid",
                "update_with_now",
                "update_with_user",
                "default(0)",
                "default_sql(NOW())",
            ]
        );
    }

    #[test]
    fn test_avro_schema_macro() {
        fn macro_schema() -> &'static str {
            avro_schema!(
                AvroSchemaBuilder::new("MacroTest")
                    .int_field("x")
                    .int_field("y")
            )
        }

        let schema = macro_schema();
        assert!(schema.contains("MacroTest"));
        // Should return the same pointer on subsequent calls
        assert_eq!(macro_schema() as *const str, schema as *const str);
    }

    #[test]
    fn test_avro_schema_macro_keyed_per_type() {
        trait Named {
            const NAME: &'static str;
        }
        struct A;
        struct B;
        impl Named for A {
            const NAME: &'static str = "RecA";
        }
        impl Named for B {
            const NAME: &'static str = "RecB";
        }

        // The plain form shares one static across instantiations; that is the
        // documented limitation.
        fn plain<T: Named>() -> &'static str {
            avro_schema!(AvroSchemaBuilder::new(T::NAME).int_field("x"))
        }
        assert_eq!(plain::<A>(), plain::<B>());

        // The keyed form builds one schema per type and caches each.
        fn keyed<T: Named + 'static>() -> &'static str {
            avro_schema!(for T => AvroSchemaBuilder::new(T::NAME).int_field("x"))
        }
        assert!(keyed::<A>().contains("\"RecA\""));
        assert!(keyed::<B>().contains("\"RecB\""));
        assert_eq!(keyed::<A>() as *const str, keyed::<A>() as *const str);
    }
}
