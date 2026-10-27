use crate::KineticaError;
use crate::bulk_inserter_common::ShardKeyValue;
use crate::generic_record::Type;
use crate::text_format::{double_to_string, float_to_string};
use crate::record_key::RecordKey;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct KineticaType { pub columns: Vec<Column> }
#[derive(Clone)]
pub struct Column { pub name: String, pub data_type: String, pub properties: Vec<String> }

/// A `decimal` column's precision where its property gives none.
const DEFAULT_DECIMAL_PRECISION: usize = 18;
/// A `decimal` column's scale where its property gives none.
const DEFAULT_DECIMAL_SCALE: usize = 4;

/// The type of a shard-key column, resolved once from the table schema.
///
/// This exists so the mapping from column type to key encoding is a `match` on
/// a fieldless-ish enum rather than on a `&str`. Both the ingest and look-up
/// paths dispatch through [`add_shard_value`], and an enum makes a missing arm
/// a compile error in both.
///
/// It was a `String` matched with `match ….as_str()`, which meant every
/// dispatch needed a `_` catch-all — and the catch-all silently absorbed
/// `decimal` and `ulong` on the look-up path, hashing them as strings while
/// ingest encoded them numerically. Records went to one rank and look-ups to
/// another.
///
/// The decimal and char parameters live in the variants rather than in side
/// tables keyed by column index, so a type can no longer be separated from the
/// parameters it needs to encode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardColumnType {
    Boolean,
    Int8,
    Int16,
    Int,
    Long,
    Float,
    Double,
    Timestamp,
    DateTime,
    Date,
    Time,
    Ipv4,
    Uuid,
    Ulong,
    Decimal { precision: usize, scale: usize },
    Char { length: usize },
    /// Anything else, hashed as a string. This is the encoding for `string`
    /// columns and the fallback for a type the server adds that this client
    /// does not know yet.
    String,
}

impl ShardColumnType {
    /// Resolves a column's shard-key type from its schema entry: a sub-type
    /// property, checked in a fixed order of precedence, overrides the
    /// column's base type.
    ///
    /// `data_type` is the base (Avro) type: `int`, `long`, `float`, `double`,
    /// `string` or `bytes`. A hand-built [`KineticaType`] may name a sub-type
    /// there instead, such as `int8`; that is used when no property names one.
    ///
    /// Fails for a type that cannot be a key: json, wkt, array, vector and
    /// bytes.
    fn from_column(col: &Column) -> Result<Self, KineticaError> {
        let has = |property: &str| col.properties.iter().any(|p| p == property);
        // Named by the property that makes it unusable, or by its type
        let not_a_key = || {
            let what = col
                .properties
                .iter()
                .find(|p| *p == "json" || *p == "wkt" || p.starts_with("array") || p.starts_with("vector"))
                .unwrap_or(&col.data_type);
            Err(KineticaError::Config(format!("Cannot use column <{}> as a key; type/property <{what}>", col.name)))
        };

        if has("boolean") {
            return Ok(Self::Boolean);
        }
        if has("int8") {
            return Ok(Self::Int8);
        }
        if has("int16") {
            return Ok(Self::Int16);
        }
        if has("timestamp") {
            return Ok(Self::Timestamp);
        }
        if has("date") {
            return Ok(Self::Date);
        }
        if has("time") {
            return Ok(Self::Time);
        }
        if has("datetime") {
            return Ok(Self::DateTime);
        }
        for length in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
            if has(&format!("char{length}")) {
                return Ok(Self::Char { length });
            }
        }
        if has("ipv4") {
            return Ok(Self::Ipv4);
        }
        if has("json") {
            return not_a_key();
        }
        if has("ulong") {
            return Ok(Self::Ulong);
        }
        if has("uuid") {
            return Ok(Self::Uuid);
        }
        if has("wkt") {
            return not_a_key();
        }
        for property in &col.properties {
            if property.starts_with("array") || property.starts_with("vector") {
                return not_a_key();
            }
            if property.starts_with("decimal") {
                return Ok(Self::decimal(&property.to_lowercase()));
            }
        }

        Ok(match col.data_type.as_str() {
            "boolean" => Self::Boolean,
            "int8" => Self::Int8,
            "int16" => Self::Int16,
            "int" => Self::Int,
            "long" => Self::Long,
            "float" => Self::Float,
            "double" => Self::Double,
            "timestamp" => Self::Timestamp,
            "datetime" => Self::DateTime,
            "date" => Self::Date,
            "time" => Self::Time,
            "ipv4" => Self::Ipv4,
            "uuid" => Self::Uuid,
            "ulong" => Self::Ulong,
            "decimal" => Self::decimal("decimal"),
            "bytes" => return not_a_key(),
            _ => Self::String,
        })
    }

    /// The decimal type a `decimal` property describes: `decimal(P,S)` gives
    /// the precision and scale, and anything else the defaults of (18, 4).
    fn decimal(property: &str) -> Self {
        let precision_and_scale = property
            .strip_prefix("decimal(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|inner| inner.split_once(','))
            .filter(|(p, s)| {
                let digits = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit());
                digits(p) && digits(s)
            })
            .and_then(|(p, s)| Some((p.parse().ok()?, s.parse().ok()?)));

        let (precision, scale) = precision_and_scale.unwrap_or((
            DEFAULT_DECIMAL_PRECISION,
            DEFAULT_DECIMAL_SCALE,
        ));
        Self::Decimal { precision, scale }
    }

    /// The number of key bytes this type contributes.
    ///
    /// Must agree with what [`add_shard_value`] writes; the two are kept
    /// adjacent for that reason.
    fn key_size(self) -> usize {
        match self {
            Self::Boolean | Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int | Self::Float | Self::Date | Self::Ipv4 | Self::Time => 4,
            Self::Long
            | Self::Double
            | Self::DateTime
            | Self::Timestamp
            | Self::Ulong
            | Self::String => 8,
            Self::Uuid => 16,
            Self::Decimal { precision, .. } => if precision > 18 { 12 } else { 8 },
            Self::Char { length } => length,
        }
    }

}

/// The message for a record whose key cannot be built.
pub(crate) const UNSHARDABLE_VALUE_MESSAGE: &str =
    "Unable to calculate shard/primary key; please check data for unshardable values";

/// A key value its column does not accept, with the message it fails with:
/// that of a value of the wrong kind for the column, or of one that does not
/// convert to a decimal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnshardableValue(pub(crate) String);

/// The kind of a key value, as messages name it: the Rust type it holds, or
/// `text` for the variants that hold text.
fn value_kind(value: &ShardKeyValue<'_>) -> &'static str {
    match value {
        ShardKeyValue::Int(_) => "i32",
        ShardKeyValue::Int8(_) => "i8",
        ShardKeyValue::Int16(_) => "i16",
        ShardKeyValue::Long(_) | ShardKeyValue::Timestamp(_) => "i64",
        ShardKeyValue::Float(_) => "f32",
        ShardKeyValue::Double(_) => "f64",
        ShardKeyValue::Boolean(_) => "bool",
        ShardKeyValue::Null => "null",
        ShardKeyValue::String(_)
        | ShardKeyValue::Date(_)
        | ShardKeyValue::Ipv4(_)
        | ShardKeyValue::Decimal(_)
        | ShardKeyValue::DateTime(_)
        | ShardKeyValue::Time(_)
        | ShardKeyValue::Uuid(_)
        | ShardKeyValue::Ulong(_) => "text",
    }
}

/// The message for a key value of another kind than its column takes.
fn wrong_kind_message(value: &ShardKeyValue<'_>, expected: &str) -> String {
    format!("Key value must be {expected}; found {}", value_kind(value))
}

/// A key value's text: a number in its shortest round-trip form.
fn value_text(value: &ShardKeyValue<'_>) -> Cow<'static, str> {
    match *value {
        ShardKeyValue::Int(v) => Cow::Owned(v.to_string()),
        ShardKeyValue::Int8(v) => Cow::Owned(v.to_string()),
        ShardKeyValue::Int16(v) => Cow::Owned(v.to_string()),
        ShardKeyValue::Long(v) | ShardKeyValue::Timestamp(v) => Cow::Owned(v.to_string()),
        ShardKeyValue::Float(v) => Cow::Owned(float_to_string(v)),
        ShardKeyValue::Double(v) => Cow::Owned(double_to_string(v)),
        ShardKeyValue::Boolean(v) => Cow::Borrowed(if v { "true" } else { "false" }),
        ShardKeyValue::Null => Cow::Borrowed("null"),
        _ => Cow::Owned(value.as_text().unwrap_or_default().to_string()),
    }
}

/// Appends one shard-key value to `key`, encoded for `ty`.
///
/// **The single dispatch from column type to encoding.** Both the ingest path
/// ([`RecordKeyBuilder::build_typed`]) and the look-up path
/// (`RecordRetriever`) come through here, which is what keeps them agreeing on
/// where a record lives.
///
/// A null value, or one the record did not supply, encodes as zero bytes in
/// the column's width. A null in a string column, for one, writes a zero
/// long, not the hash of "".
///
/// Otherwise the value must be of a kind its column takes, or this fails: an
/// int column takes an `Int`, `Int8` or `Int16`, a long or timestamp column a
/// `Long` or `Timestamp`, a float column a `Float`, a double column a
/// `Double`, the text types any text variant, and a boolean column anything.
/// Text that does not parse as its type (a malformed date, say) is not an
/// error: each type's encoding writes zero for it. The exception is `ulong`,
/// whose text must be digits.
///
/// Note there is no `_` arm, and there must not be one: a new
/// [`ShardColumnType`] has to fail to compile until it is encoded here.
pub(crate) fn add_shard_value(
    key: &mut RecordKey,
    ty: ShardColumnType,
    value: Option<&ShardKeyValue<'_>>,
) -> Result<(), UnshardableValue> {
    let value = match value {
        None | Some(ShardKeyValue::Null) => {
            // A decimal column is validated even for a null
            if let ShardColumnType::Decimal { precision, scale } = ty {
                check_decimal_column(precision, scale)?;
            }
            key.push_null(ty.key_size());
            return Ok(());
        }
        Some(value) => value,
    };

    let cast = |expected: &str| UnshardableValue(wrong_kind_message(value, expected));
    let integer = || match *value {
        ShardKeyValue::Int(v) => Ok(v),
        ShardKeyValue::Int8(v) => Ok(i32::from(v)),
        ShardKeyValue::Int16(v) => Ok(i32::from(v)),
        _ => Err(cast("i32")),
    };
    let long = || match *value {
        ShardKeyValue::Long(v) | ShardKeyValue::Timestamp(v) => Ok(v),
        _ => Err(cast("i64")),
    };
    let text = || value.as_text().ok_or_else(|| cast("text"));

    match ty {
        ShardColumnType::Int => key.push_i32(integer()?),
        // The narrowing casts keep the low bits.
        ShardColumnType::Int8 => key.push_i8(integer()? as i8),
        ShardColumnType::Int16 => key.push_i16(integer()? as i16),
        ShardColumnType::Long => key.push_i64(long()?),
        ShardColumnType::Timestamp => key.push_timestamp(long()?),
        ShardColumnType::Float => match *value {
            ShardKeyValue::Float(v) => key.push_f32(v),
            _ => return Err(cast("f32")),
        },
        ShardColumnType::Double => match *value {
            ShardKeyValue::Double(v) => key.push_f64(v),
            _ => return Err(cast("f64")),
        },
        // A `Boolean` as itself, a number by whether it is non-zero (a long
        // cut to its low 32 bits, a float or double truncated toward zero),
        // and anything else as text, true only for "true" in any case.
        ShardColumnType::Boolean => key.push_bool(match *value {
            ShardKeyValue::Boolean(v) => v,
            ShardKeyValue::Int(v) => v != 0,
            ShardKeyValue::Int8(v) => v != 0,
            ShardKeyValue::Int16(v) => v != 0,
            ShardKeyValue::Long(v) | ShardKeyValue::Timestamp(v) => (v as i32) != 0,
            ShardKeyValue::Float(v) => (v as i32) != 0,
            ShardKeyValue::Double(v) => (v as i32) != 0,
            _ => text()?.eq_ignore_ascii_case("true"),
        }),
        ShardColumnType::DateTime => key.push_datetime(text()?),
        ShardColumnType::Time => key.push_time(text()?),
        ShardColumnType::Uuid => key.push_uuid(text()?),
        ShardColumnType::Ulong => key
            .push_ulong(text()?)
            .map_err(|e| UnshardableValue(e.message()))?,
        ShardColumnType::Ipv4 => key.push_ipv4(text()?),
        ShardColumnType::Date => key.push_date(text()?),
        ShardColumnType::Decimal { precision, scale } => {
            // The value is converted first: text is used as it is, and a
            // number is put at the column's scale, failing if that would drop
            // a digit, then written out as text. A Boolean is refused, and so
            // is a NaN or infinity, which no decimal can hold. A float is
            // widened to a double.
            let not_finite = |v: f64| UnshardableValue(format!("A decimal cannot hold {v}"));
            let number = match *value {
                ShardKeyValue::Boolean(_) => {
                    return Err(UnshardableValue(
                        "Unsupported key value type: bool. Expected a number or decimal text.".to_string(),
                    ))
                }
                ShardKeyValue::Int(v) => Some(v.to_string()),
                ShardKeyValue::Int8(v) => Some(v.to_string()),
                ShardKeyValue::Int16(v) => Some(v.to_string()),
                ShardKeyValue::Long(v) | ShardKeyValue::Timestamp(v) => Some(v.to_string()),
                ShardKeyValue::Float(v) if v.is_finite() => Some(double_to_string(f64::from(v))),
                ShardKeyValue::Double(v) if v.is_finite() => Some(double_to_string(v)),
                ShardKeyValue::Float(v) => return Err(not_finite(f64::from(v))),
                ShardKeyValue::Double(v) => return Err(not_finite(v)),
                _ => None,
            };
            let text = match number {
                Some(number) => Cow::Owned(crate::big_decimal::number_text_at_scale(&number, scale).ok_or_else(
                    || UnshardableValue(format!("{number} has more decimal places than the column's scale of {scale}")),
                )?),
                None => Cow::Borrowed(text()?),
            };
            // Then the column is validated, and the text encoded
            check_decimal_column(precision, scale)?;
            key.push_decimal(&text, precision, scale);
        }
        ShardColumnType::Char { length } => key.push_char(text()?, length),
        ShardColumnType::String => key.push_string(text()?),
    }
    Ok(())
}

/// Checks a decimal column's precision and scale.
fn check_decimal_column(precision: usize, scale: usize) -> Result<(), UnshardableValue> {
    if precision < 1 {
        return Err(UnshardableValue(format!(
            "Precision must be a positive integer, got {precision}"
        )));
    }
    if scale > precision {
        return Err(UnshardableValue(format!(
            "Scale must be an integer between 0 and {precision}, got {scale}"
        )));
    }
    Ok(())
}

/// Converts the text form of a boolean shard-key value to a `bool`.
///
/// A number is true unless it is 0, and anything else is true only for
/// `"true"` in any case — so `"TRUE"` is true, and every other word, `"yes"`
/// included, is false.
///
/// A boolean column's Avro type is `int`, so a record carries an integer 1
/// or 0 there; text that is an integer stands for that number and is taken
/// by `!= 0`.
///
/// Only [`RecordKeyBuilder::build_from_map`], which is given every value as
/// text, comes through here. An ingest or a look-up is given typed values, so
/// [`add_shard_value`] takes text as a word, true only for `"true"`.
pub(crate) fn parse_boolean_text(value: &str) -> bool {
    match value.parse::<i64>() {
        // Only the low 32 bits count.
        Ok(n) => (n as i32) != 0,
        Err(_) => value.eq_ignore_ascii_case("true"),
    }
}

/// Whether `value` is the text of an unsigned long: one to twenty
/// characters, every one a digit.
///
/// This checks the form, not the range, so a twenty-digit value above
/// `u64::MAX` passes. Only ASCII digits are accepted, since this check is all
/// that keeps an unquoted value from carrying anything but a number into a
/// filter expression.
pub(crate) fn is_unsigned_long(value: &str) -> bool {
    (1..=20).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_digit())
}

/// Converts a key value from its string form into the typed form
/// [`add_shard_value`] encodes, failing for text that does not parse as its
/// column's type.
pub(crate) fn parse_shard_value<'a>(
    ty: ShardColumnType,
    value: &'a str,
) -> Result<ShardKeyValue<'a>, KineticaError> {
    fn bad(value: &str, type_name: &str, e: impl std::fmt::Display) -> KineticaError {
        KineticaError::Config(format!("Failed to parse '{value}' as {type_name}: {e}"))
    }

    Ok(match ty {
        ShardColumnType::Int => {
            ShardKeyValue::Int(value.parse().map_err(|e| bad(value, "int", e))?)
        }
        ShardColumnType::Int8 => {
            ShardKeyValue::Int8(value.parse().map_err(|e| bad(value, "int8", e))?)
        }
        ShardColumnType::Int16 => {
            ShardKeyValue::Int16(value.parse().map_err(|e| bad(value, "int16", e))?)
        }
        ShardColumnType::Long => {
            ShardKeyValue::Long(value.parse().map_err(|e| bad(value, "long", e))?)
        }
        ShardColumnType::Float => {
            ShardKeyValue::Float(value.parse().map_err(|e| bad(value, "float", e))?)
        }
        ShardColumnType::Double => {
            ShardKeyValue::Double(value.parse().map_err(|e| bad(value, "double", e))?)
        }
        ShardColumnType::Timestamp => {
            ShardKeyValue::Timestamp(value.parse().map_err(|e| bad(value, "timestamp", e))?)
        }
        // Never an error: any word not recognised maps to false. See
        // `parse_boolean_text`.
        ShardColumnType::Boolean => ShardKeyValue::Boolean(parse_boolean_text(value)),
        ShardColumnType::DateTime => ShardKeyValue::DateTime(value),
        ShardColumnType::Date => ShardKeyValue::Date(value),
        ShardColumnType::Time => ShardKeyValue::Time(value),
        ShardColumnType::Ipv4 => ShardKeyValue::Ipv4(value),
        ShardColumnType::Uuid => ShardKeyValue::Uuid(value),
        // Both of these used to fall through a `_` arm and be hashed as
        // strings, while ingest encoded them numerically.
        ShardColumnType::Ulong => ShardKeyValue::Ulong(value),
        ShardColumnType::Decimal { .. } => ShardKeyValue::Decimal(value),
        ShardColumnType::Char { .. } | ShardColumnType::String => ShardKeyValue::String(value),
    })
}

pub struct RecordKeyBuilder {
    pub shard_indices: Vec<usize>,
    pub shard_types: Vec<ShardColumnType>,
    pub buffer_size: usize,
    /// Column names for shard key columns (for fast lookup)
    pub shard_column_names: Vec<String>,
}

impl RecordKeyBuilder {
    /// Builds the key builder for a table of type `table_type`.
    ///
    /// Fails when a key column has a type that cannot be a key (json, wkt,
    /// array, vector or bytes).
    pub fn new(table_type: &KineticaType) -> Result<Self, KineticaError> {
        // The server shards on the explicit shard-key columns, and only when a
        // table has none does it shard on the primary key. Taking the union of
        // the two would hash PRIMARY KEY(a,b), SHARD KEY(a) on (a,b) and target
        // the wrong rank.
        let columns_with = |property: &str| -> Vec<usize> {
            table_type.columns.iter().enumerate()
                .filter(|(_, col)| col.properties.iter().any(|p| p == property))
                .map(|(i, _)| i)
                .collect()
        };
        let mut shard_indices = columns_with("shard_key");
        if shard_indices.is_empty() {
            shard_indices = columns_with("primary_key");
        }

        let shard_types: Vec<ShardColumnType> = shard_indices
            .iter()
            .map(|&idx| ShardColumnType::from_column(&table_type.columns[idx]))
            .collect::<Result<_, _>>()?;

        let buffer_size = shard_types.iter().map(|t| t.key_size()).sum();

        // Build column name list for fast lookup
        let shard_column_names: Vec<String> = shard_indices
            .iter()
            .map(|&idx| table_type.columns[idx].name.clone())
            .collect();

        Ok(Self { shard_indices, shard_types, buffer_size, shard_column_names })
    }

    /// Builds the key builder for records of `record_type`.
    pub fn from_type(record_type: &Type) -> Result<Self, KineticaError> {
        let columns = record_type
            .columns()
            .map(|c| Column {
                name: c.name().to_string(),
                data_type: c.base_type().avro_type().to_string(),
                properties: c.properties().to_vec(),
            })
            .collect();
        Self::new(&KineticaType { columns })
    }

    /// Whether the table has a key, shard or primary, to build keys from.
    pub fn has_key(&self) -> bool {
        !self.shard_indices.is_empty()
    }

    /// Build record key from typed shard key values (optimized path - no string parsing).
    ///
    /// This method uses pre-typed values from `ShardKeyExtractor` to avoid
    /// string parsing overhead for numeric types.
    ///
    /// **Performance Note:** For best performance, return shard key values in the same
    /// order as the table's shard columns. This enables O(1) lookup instead of O(n) search.
    ///
    /// Returns `None` if the table has no shard key, and fails if a value is
    /// not of a type its column accepts (see [`add_shard_value`]).
    #[inline]
    pub fn build_typed(
        &self,
        shard_values: &[(&str, ShardKeyValue<'_>)],
    ) -> Result<Option<RecordKey>, KineticaError> {
        if self.shard_indices.is_empty() { return Ok(None); }

        let mut key = RecordKey::new(self.buffer_size);

        // For each shard column, find the matching value from shard_values
        // Optimization: First try positional match (O(1)), then fall back to linear search (O(n))
        for (i, col_name) in self.shard_column_names.iter().enumerate() {
            // Fast path: check if value at same index matches (common case when order matches)
            let value = if i < shard_values.len() && shard_values[i].0 == col_name {
                Some(&shard_values[i].1)
            } else {
                // Slow path: linear search
                shard_values.iter()
                    .find(|(name, _)| *name == col_name)
                    .map(|(_, v)| v)
            };

            add_shard_value(&mut key, self.shard_types[i], value)
                .map_err(|_| KineticaError::Api(UNSHARDABLE_VALUE_MESSAGE.to_string()))?;
        }

        key.compute_hashes();
        Ok(Some(key))
    }

    /// Builds a record key from a column-name → value map, for an ingest.
    ///
    /// Lenient: a value that will not parse as its column's type encodes as a
    /// null rather than failing, because a record may carry a placeholder for
    /// a column the server initialises. Use [`Self::build`] for a
    /// look-up, where an unusable value is an error.
    ///
    /// Returns `None` if the table has no shard key.
    pub fn build_from_map(&self, routing_map: &HashMap<String, String>) -> Option<RecordKey> {
        if self.shard_indices.is_empty() {
            return None;
        }

        let mut key = RecordKey::new(self.buffer_size);

        for (i, col_name) in self.shard_column_names.iter().enumerate() {
            // Every value arrives as text here; the shared dispatch's string
            // handling is what turns it into the column's encoding.
            let ty = self.shard_types[i];
            let value = routing_map.get(col_name).map(String::as_str).unwrap_or_default();
            let parsed = parse_shard_value(ty, value).unwrap_or(ShardKeyValue::Null);
            // A parsed value is always of a type its column accepts, but a
            // ulong's text may still not be digits; that fails before writing
            // anything, so it is written as the null it stands for here.
            if add_shard_value(&mut key, ty, Some(&parsed)).is_err() {
                key.push_null(ty.key_size());
            }
        }

        key.compute_hashes();
        Some(key)
    }

    /// Builds the record key of the given key values, one per key column in
    /// key order, for a look-up.
    ///
    /// Shares [`add_shard_value`] with [`Self::build_typed`], so a look-up is
    /// routed to the rank the matching record was ingested to. `None` when the
    /// table has no key. Fails for a count that is not the number of key
    /// columns, a value its column's type does not accept, and a UUID that
    /// does not read as one, which an ingest would write as zero: nothing will
    /// substitute a usable value for one given expressly to be looked up.
    pub fn build(&self, key_values: &[ShardKeyValue<'_>]) -> Result<Option<RecordKey>, KineticaError> {
        if self.shard_indices.is_empty() {
            return Ok(None);
        }
        if key_values.len() != self.shard_types.len() {
            return Err(KineticaError::Config(
                "Incorrect number of key values specified.".to_string(),
            ));
        }

        let mut key = RecordKey::new(self.buffer_size);
        for (ty, value) in self.shard_types.iter().zip(key_values) {
            add_shard_value(&mut key, *ty, Some(value))
                .map_err(|UnshardableValue(message)| KineticaError::Config(message))?;
        }
        if let Some(reason) = key.take_invalid_reason() {
            return Err(KineticaError::Config(reason));
        }

        key.compute_hashes();
        Ok(Some(key))
    }

    /// Builds a filter expression associating the given key values, one per
    /// key column in key order, with the table's key columns.
    ///
    /// - `None` when the table has no key.
    /// - The number of values must equal the number of key columns exactly.
    /// - Terms are `<column> = <value>`, joined by `" and "`, and a null
    ///   value is `is_null(<column>)`.
    /// - The text types (`char*`, `date`, `datetime`, `decimal`, `ipv4`,
    ///   `string`, `time`, `uuid`) take text, written in double quotes with
    ///   every embedded double quote doubled.
    /// - `ulong` takes any value, written out as text, but only if that is
    ///   the text of an unsigned long.
    /// - `int`, `int8` and `int16` take an `Int`, `Int8` or `Int16`, `long`
    ///   and `timestamp` a `Long` or `Timestamp`, `float` a `Float` and
    ///   `double` a `Double`, written always with a fraction, and in `E`
    ///   notation below `10^-3` and from `10^7` on (`1.0`, `1.0E10`). An
    ///   `int8` or `int16` value is not range checked.
    /// - `boolean` takes any value: a `Boolean` as `1` or `0`, a number as
    ///   `1` unless it is 0 once cut to an `i32` (a long to its low 32 bits, a
    ///   float or double truncated toward zero), and text as `1`.
    ///
    /// A value of another type fails.
    ///
    /// Known risk: column names are written as they are, not quoted. A key
    /// column whose name needs quoting in an expression — a reserved word, or
    /// one with special characters — yields a filter the server rejects. The
    /// names come from the table's own schema rather than from the caller, so
    /// this is not a way to inject into the filter.
    pub fn build_expression(
        &self,
        key_values: &[ShardKeyValue<'_>],
    ) -> Result<Option<String>, KineticaError> {
        if self.shard_indices.is_empty() {
            return Ok(None);
        }

        if key_values.len() != self.shard_types.len() {
            return Err(KineticaError::Config(format!(
                "Incorrect number of key values specified: need <{}>, got <{}>",
                self.shard_types.len(),
                key_values.len()
            )));
        }

        let mut result = String::new();

        for ((name, ty), value) in self
            .shard_column_names
            .iter()
            .zip(&self.shard_types)
            .zip(key_values)
        {
            if !result.is_empty() {
                result.push_str(" and ");
            }

            if let ShardKeyValue::Null = value {
                result.push_str("is_null(");
                result.push_str(name);
                result.push(')');
                continue;
            }

            result.push_str(name);
            result.push_str(" = ");

            let cast = |expected: &str| KineticaError::Config(wrong_kind_message(value, expected));

            // Exhaustive on purpose: a new type must be given its literal form
            // here rather than inheriting a `_` arm's guess.
            match ty {
                ShardColumnType::Char { .. }
                | ShardColumnType::Date
                | ShardColumnType::DateTime
                | ShardColumnType::Decimal { .. }
                | ShardColumnType::Ipv4
                | ShardColumnType::String
                | ShardColumnType::Time
                | ShardColumnType::Uuid => {
                    let text = value.as_text().ok_or_else(|| cast("text"))?;
                    result.push('"');
                    result.push_str(&text.replace('"', "\"\""));
                    result.push('"');
                }
                ShardColumnType::Ulong => {
                    let text = value_text(value);
                    if !is_unsigned_long(&text) {
                        return Err(KineticaError::Api(format!(
                            "Unable to parse string value '{text}' as an unsigned \
                             long while building expression"
                        )));
                    }
                    // For querying purposes, unsigned long values are treated
                    // as numbers, not strings.
                    result.push_str(&text);
                }
                ShardColumnType::Double => match *value {
                    ShardKeyValue::Double(v) => result.push_str(&double_to_string(v)),
                    _ => return Err(cast("f64")),
                },
                ShardColumnType::Float => match *value {
                    ShardKeyValue::Float(v) => result.push_str(&float_to_string(v)),
                    _ => return Err(cast("f32")),
                },
                ShardColumnType::Boolean => {
                    let truth = match *value {
                        ShardKeyValue::Boolean(v) => v,
                        // A number by whether it is non-zero: a long cut to its
                        // low 32 bits, a float or double truncated toward zero
                        ShardKeyValue::Int(v) => v != 0,
                        ShardKeyValue::Int8(v) => v != 0,
                        ShardKeyValue::Int16(v) => v != 0,
                        ShardKeyValue::Long(v) | ShardKeyValue::Timestamp(v) => (v as i32) != 0,
                        ShardKeyValue::Float(v) => (v as i32) != 0,
                        ShardKeyValue::Double(v) => (v as i32) != 0,
                        // Anything else, text included, is true
                        _ => true,
                    };
                    result.push(if truth { '1' } else { '0' });
                }
                ShardColumnType::Int | ShardColumnType::Int8 | ShardColumnType::Int16 => match *value {
                    ShardKeyValue::Int(v) => result.push_str(&v.to_string()),
                    ShardKeyValue::Int8(v) => result.push_str(&v.to_string()),
                    ShardKeyValue::Int16(v) => result.push_str(&v.to_string()),
                    _ => return Err(cast("i32")),
                },
                ShardColumnType::Long | ShardColumnType::Timestamp => match *value {
                    ShardKeyValue::Long(v) | ShardKeyValue::Timestamp(v) => result.push_str(&v.to_string()),
                    _ => return Err(cast("i64")),
                },
            }
        }

        Ok(Some(result))
    }
}

/// Ingest and look-up must agree on where a record lives.
///
/// Both paths run through [`add_shard_value`]; these pin that they produce the
/// same key bytes for the same value, for every shard-key type. Before the two
/// dispatches were unified, `decimal` and `ulong` fell through a `_` arm on the
/// look-up side and were hashed as strings — so a record ingested to one rank
/// was looked up on another and `get_by_key` returned nothing.
#[cfg(test)]
mod shard_dispatch_tests {
    use super::*;

    fn col(name: &str, data_type: &str, props: &[&str]) -> Column {
        let mut properties: Vec<String> = props.iter().map(|p| (*p).to_string()).collect();
        properties.push("shard_key".to_string());
        Column { name: name.to_string(), data_type: data_type.to_string(), properties }
    }

    fn builder_for(data_type: &str, props: &[&str]) -> RecordKeyBuilder {
        RecordKeyBuilder::new(&KineticaType { columns: vec![col("k", data_type, props)] }).expect("shardable key")
    }

    // Every shard-key type, as (data_type, properties, width in bytes).
    const ALL_KEY_TYPES: &[(&str, &[&str], usize)] = &[
        ("boolean", &[], 1),
        ("int8", &[], 1),
        ("int16", &[], 2),
        ("int", &[], 4),
        ("long", &[], 8),
        ("float", &[], 4),
        ("double", &[], 8),
        ("timestamp", &[], 8),
        ("datetime", &[], 8),
        ("date", &[], 4),
        ("time", &[], 4),
        ("ipv4", &[], 4),
        ("uuid", &[], 16),
        ("ulong", &[], 8),
        ("decimal", &[], 8),
        ("string", &["decimal(27,4)"], 12),
        ("string", &["char16"], 16),
        ("string", &[], 8),
    ];

    #[test]
    fn a_null_value_encodes_as_zero_bytes() {
        for &(data_type, props, width) in ALL_KEY_TYPES {
            let b = builder_for(data_type, props);
            let key = b.build_typed(&[("k", ShardKeyValue::Null)]).expect("built").expect("key");
            assert_eq!(key.bytes(), vec![0u8; width], "{data_type} {props:?}");
        }
    }

    #[test]
    fn a_value_the_record_does_not_supply_encodes_as_null() {
        for &(data_type, props, width) in ALL_KEY_TYPES {
            let b = builder_for(data_type, props);
            let key = b.build_typed(&[("other", ShardKeyValue::Int(1))]).expect("built").expect("key");
            assert_eq!(key.bytes(), vec![0u8; width], "{data_type} {props:?}");
        }
    }

    #[test]
    fn a_null_string_is_not_the_empty_string() {
        // A null writes a zero long; the empty string writes its hash.
        let b = builder_for("string", &[]);
        let null = b.build_typed(&[("k", ShardKeyValue::Null)]).expect("built").expect("key");
        let empty = b.build_typed(&[("k", ShardKeyValue::String(""))]).expect("built").expect("key");
        assert_ne!(null.bytes(), empty.bytes());
    }

    /// Encode the same value through both paths and compare the routing hash —
    /// which is what actually selects the rank.
    fn routes_alike(data_type: &str, props: &[&str], typed: ShardKeyValue<'_>) {
        let b = builder_for(data_type, props);

        let ingest = b.build_typed(&[("k", typed)]).expect("built").expect("ingest key");
        let lookup = b.build(&[typed]).expect("lookup key").expect("keyed");

        assert_eq!(
            ingest.bytes(), lookup.bytes(),
            "{data_type}: ingest and look-up encoded {typed:?} differently"
        );
        assert_eq!(
            ingest.routing_hash(), lookup.routing_hash(),
            "{data_type}: ingest and look-up would route {typed:?} to different ranks"
        );
        assert_eq!(
            ingest.bytes().len(),
            b.buffer_size,
            "{data_type}: encoded width disagrees with the reserved buffer size"
        );
    }

    // ==================== The two that were broken ====================

    #[test]
    fn ulong_routes_the_same_on_both_paths() {
        routes_alike("ulong", &[], ShardKeyValue::Ulong("18446744073709551615"));
        routes_alike("ulong", &[], ShardKeyValue::Ulong("0"));
        routes_alike("ulong", &[], ShardKeyValue::Ulong("42"));
    }

    #[test]
    fn decimal_routes_the_same_on_both_paths() {
        routes_alike("decimal", &[], ShardKeyValue::Decimal("12345.6789"));
        routes_alike("decimal", &[], ShardKeyValue::Decimal("-0.5"));
    }

    /// `decimal(precision > 18)` reserves 12 bytes. The old look-up path wrote
    /// an 8-byte string hash, so the buffer was short as well as wrong.
    #[test]
    fn wide_decimal_uses_its_full_reserved_width() {
        let b = builder_for("string", &["decimal(30,4)"]);
        assert_eq!(b.buffer_size, 12);

        let lookup = b.build(&[ShardKeyValue::Decimal("123.45")]).expect("lookup key").expect("keyed");
        assert_eq!(lookup.bytes().len(), 12, "look-up must fill the reserved width");

        routes_alike("string", &["decimal(30,4)"], ShardKeyValue::Decimal("123.45"));
    }

    /// The regression guard for the two above: the numeric encoding and the
    /// string hash of the same text really are different bytes. Without this,
    /// the tests above would pass even if both paths quietly fell back to
    /// hashing the string.
    #[test]
    fn the_old_string_fallback_really_did_encode_differently() {
        for (ty, text) in [
            (ShardColumnType::Ulong, "18446744073709551615"),
            (ShardColumnType::Decimal { precision: 18, scale: 4 }, "12345.6789"),
        ] {
            let mut correct = RecordKey::new(16);
            add_shard_value(&mut correct, ty, Some(&ShardKeyValue::String(text))).expect("text");

            // What the look-up path used to do: fall through to a string hash.
            let mut fallback = RecordKey::new(16);
            fallback.push_string(text);

            assert_ne!(
                correct.bytes(), fallback.bytes(),
                "{ty:?}: numeric encoding and string hash coincide, so these \
                 tests would not detect the regression they exist for"
            );
        }
    }

    // ==================== The rest, so they cannot regress ====================

    #[test]
    fn every_other_shard_type_routes_alike() {
        routes_alike("int", &[], ShardKeyValue::Int(42));
        routes_alike("int8", &[], ShardKeyValue::Int8(7));
        routes_alike("int16", &[], ShardKeyValue::Int16(1234));
        routes_alike("long", &[], ShardKeyValue::Long(9_876_543_210));
        routes_alike("float", &[], ShardKeyValue::Float(1.5));
        routes_alike("double", &[], ShardKeyValue::Double(2.25));
        routes_alike("boolean", &[], ShardKeyValue::Boolean(true));
        routes_alike("timestamp", &[], ShardKeyValue::Timestamp(1_600_000_000_000));
        routes_alike("date", &[], ShardKeyValue::Date("2020-06-15"));
        routes_alike("datetime", &[], ShardKeyValue::DateTime("2020-06-15 12:30:45.123"));
        routes_alike("time", &[], ShardKeyValue::Time("12:30:45.123"));
        routes_alike("ipv4", &[], ShardKeyValue::Ipv4("192.168.1.100"));
        routes_alike("uuid", &[], ShardKeyValue::Uuid("123e4567-e89b-12d3-a456-426614174000"));
        routes_alike("string", &[], ShardKeyValue::String("hello"));
        routes_alike("string", &["char16"], ShardKeyValue::String("hello"));
    }

    // ==================== Type resolution ====================

    #[test]
    fn decimal_and_char_carry_their_parameters() {
        let b = builder_for("string", &["decimal(30,6)"]);
        assert_eq!(b.shard_types[0], ShardColumnType::Decimal { precision: 30, scale: 6 });

        let b = builder_for("string", &["char32"]);
        assert_eq!(b.shard_types[0], ShardColumnType::Char { length: 32 });
        assert_eq!(b.buffer_size, 32);
    }

    fn resolved(data_type: &str, props: &[&str]) -> ShardColumnType {
        builder_for(data_type, props).shard_types[0]
    }

    #[test]
    fn a_decimal_property_is_read() {
        assert_eq!(resolved("string", &["decimal(10,2)"]), ShardColumnType::Decimal { precision: 10, scale: 2 });
        // The `decimal` prefix is matched case-sensitively.
        assert_eq!(resolved("string", &["DECIMAL(27,8)"]), ShardColumnType::String);
        // The pattern must match the whole property; anything else, a bare
        // "decimal" included, gets the defaults.
        for other in ["decimal", "decimal(10, 2)", "decimal(10)", "decimal(a,b)", "decimal(10,2)x"] {
            assert_eq!(
                resolved("string", &[other]),
                ShardColumnType::Decimal { precision: 18, scale: 4 },
                "{other}"
            );
        }
    }

    #[test]
    fn sub_type_properties_take_their_order_of_precedence() {
        // Date is checked before the char sizes, and ipv4 after them,
        // whatever order the server lists the properties in.
        assert_eq!(resolved("string", &["char16", "date"]), ShardColumnType::Date);
        assert_eq!(resolved("string", &["ipv4", "char16"]), ShardColumnType::Char { length: 16 });
        assert_eq!(resolved("int", &["int16", "boolean"]), ShardColumnType::Boolean);
        // Only the char sizes from 1 to 256, in powers of two, are char types.
        assert_eq!(resolved("string", &["char3"]), ShardColumnType::String);
    }

    #[test]
    fn a_type_that_cannot_be_sharded_on_is_refused_as_a_key() {
        for (data_type, props, named) in [
            ("string", &["json"][..], "json"),
            ("string", &["wkt"][..], "wkt"),
            ("bytes", &["wkt"][..], "wkt"),
            ("string", &["array(int,3)"][..], "array(int,3)"),
            ("bytes", &["vector(8)"][..], "vector(8)"),
            ("bytes", &[][..], "bytes"),
        ] {
            let err = RecordKeyBuilder::new(&KineticaType { columns: vec![col("k", data_type, props)] })
                .err()
                .unwrap_or_else(|| panic!("{data_type} {props:?} was accepted as a key"));
            let KineticaError::Config(msg) = err else {
                panic!("{data_type} {props:?}: expected a configuration error");
            };
            assert_eq!(msg, format!("Cannot use column <k> as a key; type/property <{named}>"));
        }
    }

    #[test]
    fn a_column_that_is_not_a_key_can_have_any_type() {
        let b = RecordKeyBuilder::new(&KineticaType {
            columns: vec![
                col("k", "int", &[]),
                Column { name: "j".into(), data_type: "string".into(), properties: vec!["json".into()] },
                Column { name: "b".into(), data_type: "bytes".into(), properties: vec![] },
            ],
        })
        .expect("only k is a key");
        assert_eq!(b.shard_types, [ShardColumnType::Int]);
    }

    #[test]
    fn the_base_type_applies_without_a_sub_type_property() {
        assert_eq!(resolved("int", &[]), ShardColumnType::Int);
        assert_eq!(resolved("long", &["nullable"]), ShardColumnType::Long);
        assert_eq!(resolved("float", &[]), ShardColumnType::Float);
        assert_eq!(resolved("double", &[]), ShardColumnType::Double);
        assert_eq!(resolved("string", &[]), ShardColumnType::String);
        // A hand-built type may name the sub-type itself.
        assert_eq!(resolved("int8", &[]), ShardColumnType::Int8);
        assert_eq!(resolved("decimal", &[]), ShardColumnType::Decimal { precision: 18, scale: 4 });
    }

    #[test]
    fn an_unknown_column_type_falls_back_to_string() {
        let b = builder_for("some_future_type", &[]);
        assert_eq!(b.shard_types[0], ShardColumnType::String);
    }

    // ==================== Ingest tolerates, look-up reports ====================

    fn lookup_error(data_type: &str, props: &[&str], value: ShardKeyValue<'_>) -> String {
        match builder_for(data_type, props).build(&[value]) {
            Err(KineticaError::Config(message)) => message,
            other => panic!("{data_type} {props:?} {value:?}: expected a failure, got {:?}", other.map(|k| k.is_some())),
        }
    }

    // A look-up fails for a value its column cannot take, and the retriever
    // passes the error on.
    #[test]
    fn a_lookup_value_its_column_cannot_take_fails() {
        assert_eq!(
            lookup_error("int", &[], ShardKeyValue::Long(1)),
            "Key value must be i32; found i64"
        );
        assert_eq!(
            lookup_error("string", &["ulong"], ShardKeyValue::Int(1)),
            "Key value must be text; found i32"
        );
        assert_eq!(
            lookup_error("string", &["decimal(10,2)"], ShardKeyValue::Boolean(true)),
            "Unsupported key value type: bool. Expected a number or decimal text."
        );
        assert_eq!(
            lookup_error("string", &["decimal(10,2)"], ShardKeyValue::Double(1.234)),
            "1.234 has more decimal places than the column's scale of 2"
        );
        assert_eq!(
            lookup_error("string", &["decimal(10,2)"], ShardKeyValue::Double(f64::NAN)),
            "A decimal cannot hold NaN"
        );
        assert_eq!(
            lookup_error("string", &["decimal(10,2)"], ShardKeyValue::Float(f32::NEG_INFINITY)),
            "A decimal cannot hold -inf"
        );
        // The column is checked even for a null
        assert_eq!(
            lookup_error("string", &["decimal(2,5)"], ShardKeyValue::Null),
            "Scale must be an integer between 0 and 2, got 5"
        );
    }

    // A UUID its type cannot read, which an ingest writes as zero, is
    // reported, as the key records a reason for it. Other text that does not
    // read as its type is keyed as zero on both paths.
    #[test]
    fn a_lookup_reports_a_uuid_its_type_cannot_read() {
        assert_eq!(
            lookup_error("string", &["uuid"], ShardKeyValue::Uuid("not-a-uuid")),
            "Value 'not-a-uuid' could not be parsed as a UUID!"
        );
        assert!(builder_for("string", &["uuid"]).build_typed(&[("k", ShardKeyValue::Uuid("not-a-uuid"))]).is_ok());
        assert!(builder_for("string", &["date"]).build(&[ShardKeyValue::Date("not-a-date")]).is_ok());
    }

    // Text that is not an unsigned long fails both paths, where other
    // unreadable text is keyed as zero.
    #[test]
    fn a_ulong_that_is_not_digits_fails_ingest_and_lookup() {
        let b = builder_for("string", &["ulong"]);
        let err = b.build_typed(&[("k", ShardKeyValue::Ulong("-1"))]).expect_err("-1 accepted");
        assert_eq!(err.message(), UNSHARDABLE_VALUE_MESSAGE);
        assert_eq!(
            lookup_error("string", &["ulong"], ShardKeyValue::Ulong("-1")),
            "Value must be a positive integer less than or equal to the maximum unsigned long value, got -1"
        );

        // The lenient map path keys it as a null, which is eight zero bytes.
        let key = b.build_from_map(&HashMap::from([("k".to_string(), "-1".to_string())])).unwrap();
        assert_eq!(key.bytes(), &[0u8; 8]);
    }

    #[test]
    fn a_lookup_null_is_keyed_as_zero_bytes() {
        for &(data_type, props, width) in ALL_KEY_TYPES {
            let key = builder_for(data_type, props).build(&[ShardKeyValue::Null]).expect("built").expect("key");
            assert_eq!(key.bytes(), vec![0u8; width], "{data_type} {props:?}");
        }
    }

    #[test]
    fn a_table_without_a_key_has_no_lookup_key() {
        let b = RecordKeyBuilder::new(&KineticaType {
            columns: vec![Column { name: "a".into(), data_type: "int".into(), properties: vec![] }],
        })
        .unwrap();
        assert!(!b.has_key());
        assert!(b.build(&[ShardKeyValue::Int(1)]).unwrap().is_none());
    }

    #[test]
    fn an_ingest_tolerates_text_that_does_not_parse_as_its_type() {
        // A placeholder for a column the server initialises: a uuid or date
        // column writes zero for text it cannot read.
        for (data_type, props) in [("string", &["uuid"][..]), ("string", &["date"][..])] {
            let b = builder_for(data_type, props);
            let key = b.build_typed(&[("k", ShardKeyValue::String("placeholder"))]);
            assert!(matches!(key, Ok(Some(_))), "{props:?}");
        }
    }

    #[test]
    fn an_ingest_value_of_the_wrong_type_fails() {
        for (data_type, props, value) in [
            ("int", &[][..], ShardKeyValue::String("1")),
            ("int", &[][..], ShardKeyValue::Long(1)),
            ("int", &["int8"][..], ShardKeyValue::Long(1)),
            ("long", &[][..], ShardKeyValue::Int(1)),
            ("long", &["timestamp"][..], ShardKeyValue::String("1")),
            ("float", &[][..], ShardKeyValue::Double(1.0)),
            ("double", &[][..], ShardKeyValue::Float(1.0)),
            ("string", &[][..], ShardKeyValue::Int(1)),
            ("string", &["datetime"][..], ShardKeyValue::Timestamp(1)),
            ("string", &["ulong"][..], ShardKeyValue::Long(1)),
            ("string", &["char8"][..], ShardKeyValue::Int(1)),
            ("string", &["decimal(10,2)"][..], ShardKeyValue::Boolean(true)),
        ] {
            let b = builder_for(data_type, props);
            let err = b.build_typed(&[("k", value)]).expect_err("wrong type accepted");
            assert_eq!(
                err.to_string(),
                "API error: Unable to calculate shard/primary key; please check data for \
                 unshardable values",
                "{data_type} {props:?} {value:?}"
            );
        }
    }

    #[test]
    fn an_ingest_value_of_its_column_s_type_is_accepted() {
        for (data_type, props, value) in [
            ("int", &["int8"][..], ShardKeyValue::Int(300)),
            ("int", &["int16"][..], ShardKeyValue::Int8(1)),
            ("int", &[][..], ShardKeyValue::Int16(1)),
            ("long", &[][..], ShardKeyValue::Timestamp(1)),
            ("long", &["timestamp"][..], ShardKeyValue::Long(1)),
            ("int", &["boolean"][..], ShardKeyValue::Double(1.5)),
            ("int", &["boolean"][..], ShardKeyValue::String("TRUE")),
            ("string", &["date"][..], ShardKeyValue::String("2020-01-02")),
            ("string", &["ipv4"][..], ShardKeyValue::Uuid("10.0.0.1")),
            ("string", &["decimal(10,2)"][..], ShardKeyValue::Double(1.5)),
        ] {
            let b = builder_for(data_type, props);
            assert!(b.build_typed(&[("k", value)]).is_ok(), "{data_type} {props:?} {value:?}");
        }
    }

    fn decimal_key(props: &[&str], value: ShardKeyValue<'_>) -> Result<Vec<u8>, KineticaError> {
        let b = builder_for("string", props);
        let key = b.build_typed(&[("k", value)])?.expect("key");
        Ok(key.bytes().to_vec())
    }

    #[test]
    fn a_decimal_number_is_keyed_by_its_text() {
        let text = |t| decimal_key(&["decimal(10,2)"], ShardKeyValue::Decimal(t)).unwrap();
        assert_eq!(decimal_key(&["decimal(10,2)"], ShardKeyValue::Double(1.5)).unwrap(), text("1.5"));
        assert_eq!(decimal_key(&["decimal(10,2)"], ShardKeyValue::Int(7)).unwrap(), text("7"));
        assert_eq!(decimal_key(&["decimal(10,2)"], ShardKeyValue::Long(-7)).unwrap(), text("-7"));
        // A number that cannot be put at the scale without rounding is
        // refused, as are a NaN and an infinity.
        for v in [ShardKeyValue::Double(1.234), ShardKeyValue::Float(0.1), ShardKeyValue::Double(f64::NAN),
                  ShardKeyValue::Double(f64::INFINITY), ShardKeyValue::Float(f32::NEG_INFINITY)] {
            assert!(decimal_key(&["decimal(10,2)"], v).is_err(), "{v:?}");
        }
    }

    #[test]
    fn a_whole_decimal_number_at_scale_0_keeps_its_value() {
        // No trailing zeros are stripped from a number with no decimal point:
        // 100 is keyed as the text "100" is, not as 1.
        let hundred = decimal_key(&["decimal(10,0)"], ShardKeyValue::Decimal("100")).unwrap();
        assert_eq!(decimal_key(&["decimal(10,0)"], ShardKeyValue::Int(100)).unwrap(), hundred);
        assert_eq!(decimal_key(&["decimal(10,0)"], ShardKeyValue::Double(100.0)).unwrap(), hundred);
        assert_ne!(decimal_key(&["decimal(10,0)"], ShardKeyValue::Decimal("1")).unwrap(), hundred);
    }

    #[test]
    fn a_decimal_is_rounded_half_up_from_its_exact_text() {
        let key = |t| decimal_key(&["decimal(10,2)"], ShardKeyValue::Decimal(t)).unwrap();
        assert_eq!(key("1.005"), 101i64.to_le_bytes());
        assert_eq!(key("-1.005"), (-101i64).to_le_bytes());
        // Not a number, or too wide for the precision: zero.
        assert_eq!(key("abc"), 0i64.to_le_bytes());
        assert_eq!(key("123456789.12"), 0i64.to_le_bytes());
    }

    #[test]
    fn a_wide_negative_decimal_is_twos_complement_in_12_bytes() {
        let bytes = decimal_key(&["decimal(27,4)"], ShardKeyValue::Decimal("-1")).unwrap();
        let mut expected = (-10000i64).to_le_bytes().to_vec();
        expected.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn a_decimal_column_with_an_impossible_scale_cannot_be_keyed() {
        assert!(decimal_key(&["decimal(2,5)"], ShardKeyValue::Decimal("1")).is_err());
    }

    #[test]
    fn boolean_text_is_parsed_ignoring_case_on_ingest() {
        // A String is taken as a word, so "1" is false.
        let b = builder_for("int", &["boolean"]);
        let key_of = |v| {
            let key = b.build_typed(&[("k", v)]).expect("built").expect("key");
            key.bytes().to_vec()
        };
        assert_eq!(key_of(ShardKeyValue::String("1")), [0]);
        assert_eq!(key_of(ShardKeyValue::String("tRuE")), [1]);
        assert_eq!(key_of(ShardKeyValue::Int(2)), [1]);
        assert_eq!(key_of(ShardKeyValue::Double(0.9)), [0]);
    }

    // Any count mismatch is rejected: too few values used to fail one index
    // at a time, and extras were silently ignored.
    #[test]
    fn a_lookup_needs_exactly_one_value_per_key_column() {
        let b = builder_for("int", &[]);
        for values in [vec![], vec![ShardKeyValue::Int(1), ShardKeyValue::Int(2)]] {
            let err = b.build(&values).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Configuration error: Incorrect number of key values specified.",
                "{values:?}"
            );
        }
    }

    // ==================== Booleans ====================

    /// A word is read ignoring case, so `"TRUE"` is true — it used to be
    /// taken as false, sending the look-up to the rank of `false`.
    #[test]
    fn boolean_text_is_parsed_ignoring_case() {
        for text in ["true", "TRUE", "True", "1", "-1", "42"] {
            assert!(parse_boolean_text(text), "{text} should be true");
        }
        for text in ["false", "FALSE", "0", "yes", "t", "", " true"] {
            assert!(!parse_boolean_text(text), "{text:?} should be false");
        }
        // Only the low 32 bits count, so 2^32 is false.
        assert!(!parse_boolean_text("4294967296"));
    }

    #[test]
    fn boolean_values_of_any_type_route_alike() {
        let b = builder_for("boolean", &[]);
        let key = |v| b.build(&[v]).expect("built").expect("key").bytes().to_vec();
        for (value, truth) in [
            (ShardKeyValue::Boolean(true), true),
            (ShardKeyValue::String("TRUE"), true),
            (ShardKeyValue::String("1"), false),
            (ShardKeyValue::String("maybe"), false),
            (ShardKeyValue::Int(2), true),
            (ShardKeyValue::Long(1 << 32), false),
            (ShardKeyValue::Double(0.9), false),
        ] {
            assert_eq!(key(value), [u8::from(truth)], "{value:?}");
            routes_alike("boolean", &[], value);
        }
    }

    // ==================== Unsigned long form ====================

    #[test]
    fn unsigned_long_form_is_canonical() {
        assert!(is_unsigned_long("0"));
        assert!(is_unsigned_long("18446744073709551615"));
        // The form is checked, not the range.
        assert!(is_unsigned_long("99999999999999999999"));
        for bad in ["", "-1", "1.5", "1 or 1=1", "123456789012345678901", "１"] {
            assert!(!is_unsigned_long(bad), "{bad:?} should be rejected");
        }
    }
}

/// The look-up filter expression, as `RecordKeyBuilder::build_expression`
/// writes it.
#[cfg(test)]
mod build_expression_tests {
    use super::*;

    fn key_col(name: &str, data_type: &str, props: &[&str]) -> Column {
        let mut properties: Vec<String> = props.iter().map(|p| (*p).to_string()).collect();
        properties.push("shard_key".to_string());
        Column { name: name.to_string(), data_type: data_type.to_string(), properties }
    }

    fn builder(columns: Vec<Column>) -> RecordKeyBuilder {
        RecordKeyBuilder::new(&KineticaType { columns }).expect("shardable key")
    }

    fn expr(data_type: &str, props: &[&str], value: ShardKeyValue<'_>) -> Result<Option<String>, KineticaError> {
        builder(vec![key_col("k", data_type, props)]).build_expression(&[value])
    }

    fn written(data_type: &str, props: &[&str], value: ShardKeyValue<'_>) -> String {
        expr(data_type, props, value).expect("expression").expect("keyed")
    }

    fn cast_message(found: &str, expected: &str) -> String {
        format!("Configuration error: Key value must be {expected}; found {found}")
    }

    #[test]
    fn terms_are_joined_with_and_in_key_order() {
        let b = builder(vec![
            key_col("name_sk", "string", &[]),
            Column { name: "other".into(), data_type: "int".into(), properties: vec![] },
            key_col("value_sk", "int", &[]),
        ]);
        assert_eq!(
            b.build_expression(&[ShardKeyValue::String("abc"), ShardKeyValue::Int(7)]).unwrap().as_deref(),
            Some("name_sk = \"abc\" and value_sk = 7")
        );
        assert_eq!(
            b.build_expression(&[ShardKeyValue::Null, ShardKeyValue::Int(7)]).unwrap().as_deref(),
            Some("is_null(name_sk) and value_sk = 7"),
            "a null is is_null(column)"
        );
    }

    #[test]
    fn a_table_without_a_key_has_no_expression() {
        let b = builder(vec![Column { name: "a".into(), data_type: "int".into(), properties: vec![] }]);
        assert_eq!(b.build_expression(&[ShardKeyValue::Int(1)]).unwrap(), None);
    }

    /// Too few values used to produce a partial filter matching far more than
    /// the caller asked for; extras were ignored.
    #[test]
    fn the_value_count_must_match_the_key_exactly() {
        let b = builder(vec![key_col("a", "int", &[]), key_col("b", "int", &[])]);
        for v in [vec![ShardKeyValue::Int(1)], vec![ShardKeyValue::Int(1); 3], vec![]] {
            let err = b.build_expression(&v).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("Configuration error: Incorrect number of key values specified: need <2>, got <{}>", v.len())
            );
        }
    }

    // Text types are double-quoted with embedded double quotes doubled — not
    // single-quoted — and take any text.
    #[test]
    fn text_values_are_double_quoted_with_quotes_doubled() {
        for (ty, props) in [("string", &[][..]), ("string", &["char16"][..]), ("date", &[]),
                            ("datetime", &[]), ("decimal", &[]), ("ipv4", &[]),
                            ("time", &[]), ("uuid", &[])] {
            assert_eq!(written(ty, props, ShardKeyValue::String("a\"b'c")), "k = \"a\"\"b'c\"", "{ty} {props:?}");
            assert_eq!(written(ty, props, ShardKeyValue::Uuid("x")), "k = \"x\"", "{ty} {props:?}");
        }
    }

    // A value is written only if it is of a kind its column takes; anything
    // else fails, so a filter cannot be widened through a key value.
    #[test]
    fn a_value_of_another_type_fails() {
        for (ty, value, from, to) in [
            ("string", ShardKeyValue::Int(1), "i32", "text"),
            ("decimal", ShardKeyValue::Double(1.5), "f64", "text"),
            ("int", ShardKeyValue::String("1 or 1=1"), "text", "i32"),
            ("int", ShardKeyValue::Long(1), "i64", "i32"),
            ("long", ShardKeyValue::Int(1), "i32", "i64"),
            ("timestamp", ShardKeyValue::Double(1.0), "f64", "i64"),
            ("float", ShardKeyValue::Double(1.0), "f64", "f32"),
            ("double", ShardKeyValue::Float(1.0), "f32", "f64"),
            ("double", ShardKeyValue::Boolean(true), "bool", "f64"),
        ] {
            assert_eq!(expr(ty, &[], value).unwrap_err().to_string(), cast_message(from, to), "{ty} {value:?}");
        }
    }

    #[test]
    fn numbers_are_written_in_their_shortest_form() {
        assert_eq!(written("int", &[], ShardKeyValue::Int(42)), "k = 42");
        assert_eq!(written("int8", &[], ShardKeyValue::Int(300)), "k = 300", "no range check");
        assert_eq!(written("int16", &[], ShardKeyValue::Int16(-1234)), "k = -1234");
        assert_eq!(written("int", &[], ShardKeyValue::Int8(7)), "k = 7");
        assert_eq!(written("long", &[], ShardKeyValue::Long(9_876_543_210)), "k = 9876543210");
        assert_eq!(written("timestamp", &[], ShardKeyValue::Timestamp(1_600_000_000_000)), "k = 1600000000000");
        assert_eq!(written("float", &[], ShardKeyValue::Float(1.5)), "k = 1.5");
        assert_eq!(written("float", &[], ShardKeyValue::Float(1e10)), "k = 1.0E10");
        assert_eq!(written("double", &[], ShardKeyValue::Double(2.0)), "k = 2.0");
        assert_eq!(written("double", &[], ShardKeyValue::Double(1e-5)), "k = 1.0E-5");
        assert_eq!(written("double", &[], ShardKeyValue::Double(f64::NAN)), "k = NaN");
    }

    // Any value, written out as text, once that is seen to be the text of an
    // unsigned long.
    #[test]
    fn ulong_takes_any_value_that_reads_as_an_unsigned_long() {
        assert_eq!(written("ulong", &[], ShardKeyValue::Ulong("18446744073709551615")), "k = 18446744073709551615");
        assert_eq!(written("ulong", &[], ShardKeyValue::Long(5)), "k = 5");
        for (value, text) in [
            (ShardKeyValue::Ulong("1 or 1=1"), "1 or 1=1"),
            (ShardKeyValue::Double(5.0), "5.0"),
            (ShardKeyValue::Long(-1), "-1"),
            (ShardKeyValue::Boolean(true), "true"),
        ] {
            let err = expr("ulong", &[], value).unwrap_err();
            assert!(matches!(err, KineticaError::Api(_)), "got {err:?}");
            assert_eq!(
                err.to_string(),
                format!("API error: Unable to parse string value '{text}' as an unsigned long while building expression")
            );
        }
    }

    /// A Boolean as 1 or 0, a number as 1 unless it is 0 once cut to an
    /// `i32`, and anything else as 1.
    #[test]
    fn boolean_is_written_as_one_or_zero() {
        for (value, literal) in [
            (ShardKeyValue::Boolean(true), "1"),
            (ShardKeyValue::Boolean(false), "0"),
            (ShardKeyValue::Int(2), "1"),
            (ShardKeyValue::Int16(0), "0"),
            (ShardKeyValue::Long(1 << 32), "0"),
            (ShardKeyValue::Double(0.5), "0"),
            (ShardKeyValue::Float(-1.5), "1"),
            (ShardKeyValue::String("false"), "1"),
            (ShardKeyValue::String("0"), "1"),
        ] {
            assert_eq!(written("boolean", &[], value), format!("k = {literal}"), "{value:?}");
        }
    }
}

/// Which columns a table is sharded on: the shard key when there is one, and
/// the primary key only when there is not, matching the server.
#[cfg(test)]
mod shard_column_selection_tests {
    use super::*;

    fn col(name: &str, props: &[&str]) -> Column {
        Column {
            name: name.to_string(),
            data_type: "int".to_string(),
            properties: props.iter().map(|p| (*p).to_string()).collect(),
        }
    }

    fn shard_columns(columns: Vec<Column>) -> Vec<String> {
        RecordKeyBuilder::new(&KineticaType { columns }).expect("shardable key").shard_column_names
    }

    #[test]
    fn shard_key_wins_over_a_wider_primary_key() {
        // PRIMARY KEY(a, b), SHARD KEY(a)
        let cols = vec![
            col("a", &["primary_key", "shard_key"]),
            col("b", &["primary_key"]),
            col("c", &[]),
        ];
        assert_eq!(shard_columns(cols), vec!["a"]);
    }

    #[test]
    fn only_shard_key_columns_are_taken_when_both_are_tagged() {
        let cols = vec![col("a", &["primary_key"]), col("b", &["shard_key"])];
        assert_eq!(shard_columns(cols), vec!["b"]);
    }

    #[test]
    fn primary_key_is_used_when_there_is_no_shard_key() {
        let cols = vec![col("a", &["primary_key"]), col("b", &["primary_key"]), col("c", &[])];
        assert_eq!(shard_columns(cols), vec!["a", "b"]);
    }

    #[test]
    fn shard_key_without_primary_key() {
        let cols = vec![col("a", &[]), col("b", &["shard_key"]), col("c", &["shard_key"])];
        assert_eq!(shard_columns(cols), vec!["b", "c"]);
    }

    #[test]
    fn no_key_columns_means_no_shard_key() {
        let b = RecordKeyBuilder::new(&KineticaType { columns: vec![col("a", &[])] }).expect("shardable key");
        assert!(b.shard_indices.is_empty());
        assert_eq!(b.buffer_size, 0);
    }
}
