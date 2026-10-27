//! Values for Kinetica's PostgreSQL wire protocol that SQLx's own PostgreSQL
//! types cannot read or write, or cannot reliably.
//!
//! Kinetica's PostgreSQL protocol differs from PostgreSQL's in ways SQLx does
//! not expect. Measured against Kinetica 7.2:
//!
//! - **Parameters.** Kinetica reads every parameter after a statement's first
//!   as text. A second integer, float or date parameter fails to convert in an
//!   `INSERT`; in a `WHERE` clause it matches nothing, so an `UPDATE` or
//!   `DELETE` silently changes no rows. Text parameters work in every
//!   position, and Kinetica converts each to its column's type. Bind values
//!   wrapped in [`KineticaText`], or the other wrappers here, which all send
//!   text.
//! - **`JSON`** parameters bound as SQLx's `serde_json::Value` or
//!   `sqlx::types::Json` go as PostgreSQL's `JSONB`, whose leading version
//!   byte Kinetica refuses as invalid JSON. [`KineticaJson`] binds them as
//!   `JSON`, which Kinetica takes in any position.
//! - **Arrays** arrive as PostgreSQL arrays numbered from 0, which SQLx's
//!   `Vec<T>` refuses, and an array a function computes (`ARRAY_APPEND`,
//!   `ARRAY_SLICE`, ...) arrives as JSON text. [`KineticaArray`] reads both.
//!   A numeric `ARRAY_CONTAINS(col, $n)` finds nothing whatever is bound;
//!   `ARRAY_CONTAINS_ANY(col, $n)` with a one-element `KineticaArray` works.
//! - **`TIMESTAMP`** columns hold milliseconds since the epoch, and arrive as
//!   `BIGINT`. [`KineticaTimestamp`] reads and writes them as a
//!   `chrono::DateTime<Utc>`.
//! - **`DATETIME`** arrives as a PostgreSQL `TIMESTAMP` without its
//!   milliseconds. Select it as text, `CAST(col AS VARCHAR)`, and read it as
//!   `KineticaText<NaiveDateTime>`. A `NaiveDateTime` parameter loses its
//!   milliseconds too; `KineticaText` keeps them.
//! - **`TIME`, `UUID`, `IPV4` and `UNSIGNED BIGINT`** arrive as text, and
//!   their PostgreSQL parameter types are refused. `KineticaText` reads and
//!   writes them as `NaiveTime`, `uuid::Uuid`, `Ipv4Addr` and `u64`.
//! - **`BLOB`** parameters (`BYTEA`) are refused. [`KineticaBytes`] writes them.
//! - **`VECTOR`** columns arrive as text. Kinetica takes a vector parameter in
//!   two forms, depending on the statement: the raw floats [`KineticaVector`]
//!   sends in a plain `INSERT`, and text, `KineticaText(vector.as_slice())`,
//!   everywhere else.
//!
//! Geometry (`WKT`) columns cannot be selected over this protocol at all: the
//! query fails on the server. Select `ST_ASTEXT(col)` instead, and write
//! geometry as `KineticaText` WKT.
//!
//! Read arrays through `sqlx::query`, not `sqlx::raw_sql`: in the text form
//! `raw_sql` returns, Kinetica writes `null` in capitals wherever it occurs in
//! a string element, so `"nullable"` reads back as `"NULLable"`.
//!
//! ```no_run
//! use gpudb_api::sqlx_support::{
//!     connect, KineticaArray, KineticaJson, KineticaText, KineticaTimestamp, KineticaVector,
//! };
//! use serde_json::{json, Value};
//! use sqlx::Row;
//!
//! # async fn example() -> Result<(), sqlx::Error> {
//! let pool = connect("localhost", "admin", "password").await?;
//! let now = chrono::Utc::now();
//!
//! // Every parameter through a wrapper, so that all of them reach their columns
//! sqlx::query(
//!     "INSERT INTO events (id, active, at, tags, attrs, embedding) VALUES ($1, $2, $3, $4, $5, $6)",
//! )
//! .bind(KineticaText(42))
//! .bind(KineticaText(true))
//! .bind(KineticaTimestamp(now))
//! .bind(KineticaArray(vec!["a".to_string(), "b".to_string()]))
//! .bind(KineticaJson(json!({"source": "sensor", "level": 3})))
//! .bind(KineticaVector(vec![0.5, 1.5, 2.5]))
//! .execute(&pool)
//! .await?;
//!
//! // Outside a plain INSERT, a vector parameter is text
//! let probe = vec![0.5_f32, 1.0, 2.0];
//! let row = sqlx::query(
//!     "SELECT at, tags, attrs, embedding, L2_DISTANCE(embedding, $1) AS distance
//!      FROM events WHERE id = $2",
//! )
//! .bind(KineticaText(probe.as_slice()))
//! .bind(KineticaText(42))
//! .fetch_one(&pool)
//! .await?;
//! let at: KineticaTimestamp = row.get("at");
//! let tags: KineticaArray<String> = row.get("tags");
//! let attrs: KineticaJson<Value> = row.get("attrs");
//! let embedding: KineticaVector = row.get("embedding");
//! let distance: f32 = row.get("distance");
//! # Ok(())
//! # }
//! ```

use std::fmt::Write as _;
use std::net::Ipv4Addr;

use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgTypeKind, PgValueFormat, PgValueRef, Postgres};
use sqlx::{Decode, Encode, Type, ValueRef};

/// The PostgreSQL type of a parameter sent as text.
fn text_type_info() -> PgTypeInfo {
    <String as Type<Postgres>>::type_info()
}

/// Whether a column's type carries text.
fn is_text(ty: &PgTypeInfo) -> bool {
    <String as Type<Postgres>>::compatible(ty)
}

/// Writes `bytes` as a parameter's value.
fn put(buf: &mut PgArgumentBuffer, bytes: &[u8]) -> Result<IsNull, BoxDynError> {
    buf.extend_from_slice(bytes);
    Ok(IsNull::No)
}

// ============================================================================
// Values carried as text
// ============================================================================

/// A value carried as text, both ways, which Kinetica converts to and from its
/// column's type.
///
/// As a parameter, this is the form Kinetica reads correctly in any position
/// of a statement (see [`sqlx_support`](crate::sqlx_support)). Booleans are sent as `1` and
/// `0`, as Kinetica's boolean columns are integers; dates and times as Kinetica
/// writes them, to the millisecond.
///
/// Read from a column Kinetica sends as text, it parses the text: `TIME`,
/// `UUID`, `IPV4`, `UNSIGNED BIGINT`, and any column selected with
/// `CAST(col AS VARCHAR)`, such as a `DATETIME` with its milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct KineticaText<T>(pub T);

/// A value Kinetica takes as text: see [`KineticaText`].
pub trait ToKineticaText {
    /// The text Kinetica converts to the value.
    fn to_kinetica_text(&self) -> String;
}

/// A value read from the text Kinetica sends: see [`KineticaText`].
pub trait FromKineticaText: Sized {
    /// Parses the text Kinetica sends for the value.
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError>;
}

impl<T> Type<Postgres> for KineticaText<T> {
    fn type_info() -> PgTypeInfo {
        text_type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        is_text(ty)
    }
}

impl<T: ToKineticaText> Encode<'_, Postgres> for KineticaText<T> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        put(buf, self.0.to_kinetica_text().as_bytes())
    }
}

impl<'r, T: FromKineticaText> Decode<'r, Postgres> for KineticaText<T> {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        T::from_kinetica_text(value.as_str()?).map(KineticaText)
    }
}

impl<T: ToKineticaText + ?Sized> ToKineticaText for &T {
    fn to_kinetica_text(&self) -> String {
        (**self).to_kinetica_text()
    }
}

macro_rules! displayed_as_text {
    ($($t:ty),*) => {$(
        impl ToKineticaText for $t {
            fn to_kinetica_text(&self) -> String {
                self.to_string()
            }
        }
    )*};
}
displayed_as_text!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, str, String, Ipv4Addr);

macro_rules! parsed_from_text {
    ($($t:ty),*) => {$(
        impl FromKineticaText for $t {
            fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
                Ok(text.parse::<$t>()?)
            }
        }
    )*};
}
parsed_from_text!(i32, i64, u64, f32, f64, String, Ipv4Addr);

/// Kinetica's boolean columns are integers: `1` and `0`.
impl ToKineticaText for bool {
    fn to_kinetica_text(&self) -> String {
        if *self { "1" } else { "0" }.to_string()
    }
}

impl FromKineticaText for bool {
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
        match text {
            "1" | "true" | "TRUE" => Ok(true),
            "0" | "false" | "FALSE" => Ok(false),
            _ => Err(format!("'{text}' is not a Kinetica boolean").into()),
        }
    }
}

impl ToKineticaText for uuid::Uuid {
    fn to_kinetica_text(&self) -> String {
        self.hyphenated().to_string()
    }
}

impl FromKineticaText for uuid::Uuid {
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
        Ok(uuid::Uuid::parse_str(text)?)
    }
}

/// Kinetica's dates: `YYYY-MM-DD`.
const DATE_FORMAT: &str = "%Y-%m-%d";
/// Kinetica's times, to the millisecond: `HH:MM:SS.mmm`.
const TIME_FORMAT: &str = "%H:%M:%S%.3f";
/// Kinetica's date-times, to the millisecond: `YYYY-MM-DD HH:MM:SS.mmm`.
const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

impl ToKineticaText for chrono::NaiveDate {
    fn to_kinetica_text(&self) -> String {
        self.format(DATE_FORMAT).to_string()
    }
}

impl FromKineticaText for chrono::NaiveDate {
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
        Ok(chrono::NaiveDate::parse_from_str(text, DATE_FORMAT)?)
    }
}

impl ToKineticaText for chrono::NaiveTime {
    fn to_kinetica_text(&self) -> String {
        self.format(TIME_FORMAT).to_string()
    }
}

impl FromKineticaText for chrono::NaiveTime {
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
        Ok(chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f")?)
    }
}

impl ToKineticaText for chrono::NaiveDateTime {
    fn to_kinetica_text(&self) -> String {
        self.format(DATETIME_FORMAT).to_string()
    }
}

impl FromKineticaText for chrono::NaiveDateTime {
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
        Ok(chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")?)
    }
}

/// JSON, compact.
impl ToKineticaText for serde_json::Value {
    fn to_kinetica_text(&self) -> String {
        self.to_string()
    }
}

impl FromKineticaText for serde_json::Value {
    fn from_kinetica_text(text: &str) -> Result<Self, BoxDynError> {
        Ok(serde_json::from_str(text)?)
    }
}

/// A vector as Kinetica's text for one, `[1,2.5,3]`. This is what an `UPDATE
/// ... SET` of a `VECTOR` column, `VECTOR($n, dims)` in an `INSERT`, and the
/// vector functions (`L2_DISTANCE`, `COSINE_DISTANCE`, `DOT_PRODUCT`, ...) take
/// for a parameter; a plain `INSERT ... VALUES ($n)` takes [`KineticaVector`].
impl ToKineticaText for [f32] {
    fn to_kinetica_text(&self) -> String {
        let mut out = String::from("[");
        for (i, value) in self.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{value}");
        }
        out.push(']');
        out
    }
}

impl ToKineticaText for Vec<f32> {
    fn to_kinetica_text(&self) -> String {
        self.as_slice().to_kinetica_text()
    }
}

// ============================================================================
// JSON
// ============================================================================

/// A `JSON` column's value: anything serde serializes, bound directly as
/// JSON, and read back from the JSON Kinetica sends.
///
/// SQLx's own `serde_json::Value` and `sqlx::types::Json` bind as PostgreSQL's
/// `JSONB`, whose binary form starts with a version byte, which Kinetica
/// refuses as invalid JSON. This binds the `JSON` type instead, whose value is
/// the JSON text itself, so it reaches its column in any parameter position.
/// It reads `JSON` and `JSONB` columns, and text such as `JSON_QUERY`'s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct KineticaJson<T>(pub T);

/// PostgreSQL's `JSON` type. Named by its OID: by name, SQLx would look it up
/// in a catalog that Kinetica answers with null.
const JSON_OID: Oid = Oid(114);
/// PostgreSQL's `JSONB` type.
const JSONB_OID: Oid = Oid(3802);

impl<T> Type<Postgres> for KineticaJson<T> {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(JSON_OID)
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <serde_json::Value as Type<Postgres>>::compatible(ty) || is_text(ty)
    }
}

impl<T: Serialize> Encode<'_, Postgres> for KineticaJson<T> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        put(buf, &serde_json::to_vec(&self.0)?)
    }
}

impl<'r, T: DeserializeOwned> Decode<'r, Postgres> for KineticaJson<T> {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let jsonb = value.format() == PgValueFormat::Binary && value.type_info().oid() == Some(JSONB_OID);
        let bytes = value.as_bytes()?;
        // JSONB's binary form starts with its version, 1
        let bytes = match bytes.split_first() {
            Some((1, rest)) if jsonb => rest,
            _ => bytes,
        };
        Ok(Self(serde_json::from_slice(bytes)?))
    }
}

// ============================================================================
// TIMESTAMP
// ============================================================================

/// A `TIMESTAMP` column's value: milliseconds since the Unix epoch, which
/// Kinetica sends as `BIGINT` and takes as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KineticaTimestamp(pub chrono::DateTime<chrono::Utc>);

impl KineticaTimestamp {
    /// The timestamp `millis` milliseconds after the Unix epoch.
    pub fn from_millis(millis: i64) -> Option<Self> {
        chrono::DateTime::from_timestamp_millis(millis).map(Self)
    }

    /// Milliseconds since the Unix epoch, as Kinetica holds it.
    pub fn millis(&self) -> i64 {
        self.0.timestamp_millis()
    }
}

impl Type<Postgres> for KineticaTimestamp {
    fn type_info() -> PgTypeInfo {
        text_type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <i64 as Type<Postgres>>::compatible(ty) || is_text(ty)
    }
}

impl Encode<'_, Postgres> for KineticaTimestamp {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        put(buf, self.millis().to_string().as_bytes())
    }
}

impl<'r> Decode<'r, Postgres> for KineticaTimestamp {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let millis = if is_text(&value.type_info()) {
            value.as_str()?.parse::<i64>()?
        } else {
            <i64 as Decode<Postgres>>::decode(value)?
        };
        Self::from_millis(millis).ok_or_else(|| format!("{millis} ms is not a representable timestamp").into())
    }
}

// ============================================================================
// BLOB
// ============================================================================

/// A `BLOB` column's value. Kinetica refuses a `BYTEA` parameter, but takes the
/// bytes of a text parameter as they are, so this sends them under the text
/// type; it reads the `BYTEA` Kinetica sends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct KineticaBytes(pub Vec<u8>);

impl Type<Postgres> for KineticaBytes {
    fn type_info() -> PgTypeInfo {
        text_type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <Vec<u8> as Type<Postgres>>::compatible(ty)
    }
}

impl Encode<'_, Postgres> for KineticaBytes {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        put(buf, &self.0)
    }
}

impl<'r> Decode<'r, Postgres> for KineticaBytes {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        <Vec<u8> as Decode<Postgres>>::decode(value).map(Self)
    }
}

// ============================================================================
// VECTOR
// ============================================================================

/// A `VECTOR(n)` column's value. Kinetica sends it as text, `[1,2.5,3]`, and
/// reads it from a parameter in two ways, depending on where it is:
///
/// - **A plain `INSERT ... VALUES ($n)`** takes the parameter's bytes as the
///   vector's own, little-endian floats, and refuses text. This sends those
///   bytes.
/// - **Everywhere else**, the parameter must be text, `[1,2.5,3]`: bind
///   `KineticaText(vector.as_slice())`. That covers `UPDATE ... SET v = $n`,
///   `VECTOR($n, dims)` in an `INSERT`, and the arguments of `L2_DISTANCE`,
///   `L1_DISTANCE`, `COSINE_DISTANCE` and `DOT_PRODUCT`. Kinetica refuses these
///   bytes in an `UPDATE`, and cannot read them as a function's argument.
#[derive(Debug, Clone, PartialEq, PartialOrd, Default)]
pub struct KineticaVector(pub Vec<f32>);

impl Type<Postgres> for KineticaVector {
    fn type_info() -> PgTypeInfo {
        text_type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        is_text(ty)
    }
}

impl Encode<'_, Postgres> for KineticaVector {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let bytes: Vec<u8> = self.0.iter().flat_map(|v| v.to_le_bytes()).collect();
        put(buf, &bytes)
    }
}

impl<'r> Decode<'r, Postgres> for KineticaVector {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        parse_vector(value.as_str()?).map(Self)
    }
}

/// Parses Kinetica's text for a vector: `[1,2.5,3]`.
fn parse_vector(text: &str) -> Result<Vec<f32>, BoxDynError> {
    let inner = text
        .trim()
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .ok_or_else(|| format!("'{text}' is not a Kinetica vector"))?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner.split(',').map(|v| Ok(v.trim().parse::<f32>()?)).collect()
}

// ============================================================================
// Arrays
// ============================================================================

/// An array column's value. Kinetica sends arrays numbered from 0, which SQLx's
/// own `Vec<T>` refuses; this reads them, in the binary form a query's results
/// come in and in the text form `sqlx::raw_sql` gives, and writes them as the
/// JSON text Kinetica takes. It also reads the JSON text Kinetica sends for an
/// array a function computes, such as `ARRAY_APPEND(col, 4)`.
///
/// Use `KineticaArray<Option<T>>` for an array that may hold nulls. The
/// element types are those of Kinetica's arrays: `i32` (`INTEGER[]`), `i64`
/// (`BIGINT[]`), `u64` (`UNSIGNED BIGINT[]`), `f32` (`REAL[]`), `f64`
/// (`DOUBLE[]`), `bool` (`BOOLEAN[]`) and `String` (`VARCHAR[]`).
#[derive(Debug, Clone, PartialEq, PartialOrd, Default)]
pub struct KineticaArray<T>(pub Vec<T>);

/// An element of a Kinetica array: see [`KineticaArray`].
pub trait KineticaArrayElement: Sized {
    /// The OIDs of the PostgreSQL element types Kinetica sends this as.
    const ELEMENT_OIDS: &'static [u32];

    /// An element from its binary form, `None` for a null.
    fn from_binary(bytes: Option<&[u8]>) -> Result<Self, BoxDynError>;

    /// An element from its text form, `None` for a null.
    fn from_text(text: Option<&str>) -> Result<Self, BoxDynError>;

    /// Appends the element as JSON.
    fn write_json(&self, out: &mut String);
}

/// Fails for a null element of an array read without `Option`.
fn present<T>(value: Option<T>) -> Result<T, BoxDynError> {
    value.ok_or_else(|| "the array holds a null; read it as KineticaArray<Option<_>>".into())
}

/// Takes the `N` bytes of a fixed-width element.
fn fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], BoxDynError> {
    bytes.try_into().map_err(|_| format!("an array element of {} bytes where {N} were expected", bytes.len()).into())
}

macro_rules! numeric_element {
    ($t:ty, $oids:expr, $n:literal) => {
        impl KineticaArrayElement for $t {
            const ELEMENT_OIDS: &'static [u32] = $oids;

            fn from_binary(bytes: Option<&[u8]>) -> Result<Self, BoxDynError> {
                Ok(<$t>::from_be_bytes(fixed::<$n>(present(bytes)?)?))
            }

            fn from_text(text: Option<&str>) -> Result<Self, BoxDynError> {
                Ok(present(text)?.parse::<$t>()?)
            }

            fn write_json(&self, out: &mut String) {
                let _ = write!(out, "{self}");
            }
        }
    };
}
numeric_element!(i32, &[23], 4);
numeric_element!(i64, &[20], 8);
numeric_element!(f32, &[700], 4);
numeric_element!(f64, &[701], 8);

impl KineticaArrayElement for bool {
    const ELEMENT_OIDS: &'static [u32] = &[16];

    fn from_binary(bytes: Option<&[u8]>) -> Result<Self, BoxDynError> {
        Ok(fixed::<1>(present(bytes)?)?[0] != 0)
    }

    fn from_text(text: Option<&str>) -> Result<Self, BoxDynError> {
        match present(text)? {
            "t" | "true" | "1" => Ok(true),
            "f" | "false" | "0" => Ok(false),
            other => Err(format!("'{other}' is not a boolean").into()),
        }
    }

    fn write_json(&self, out: &mut String) {
        out.push_str(if *self { "true" } else { "false" });
    }
}

/// `TEXT`, `VARCHAR`, `BPCHAR` and `NAME`.
const TEXT_OIDS: &[u32] = &[25, 1043, 1042, 19];

impl KineticaArrayElement for String {
    const ELEMENT_OIDS: &'static [u32] = TEXT_OIDS;

    fn from_binary(bytes: Option<&[u8]>) -> Result<Self, BoxDynError> {
        Ok(std::str::from_utf8(present(bytes)?)?.to_string())
    }

    fn from_text(text: Option<&str>) -> Result<Self, BoxDynError> {
        Ok(present(text)?.to_string())
    }

    fn write_json(&self, out: &mut String) {
        out.push_str(&serde_json::Value::from(self.as_str()).to_string());
    }
}

/// Kinetica sends `UNSIGNED BIGINT[]` as text elements.
impl KineticaArrayElement for u64 {
    const ELEMENT_OIDS: &'static [u32] = TEXT_OIDS;

    fn from_binary(bytes: Option<&[u8]>) -> Result<Self, BoxDynError> {
        Ok(std::str::from_utf8(present(bytes)?)?.parse::<u64>()?)
    }

    fn from_text(text: Option<&str>) -> Result<Self, BoxDynError> {
        Ok(present(text)?.parse::<u64>()?)
    }

    fn write_json(&self, out: &mut String) {
        let _ = write!(out, "{self}");
    }
}

impl<T: KineticaArrayElement> KineticaArrayElement for Option<T> {
    const ELEMENT_OIDS: &'static [u32] = T::ELEMENT_OIDS;

    fn from_binary(bytes: Option<&[u8]>) -> Result<Self, BoxDynError> {
        bytes.map(|b| T::from_binary(Some(b))).transpose()
    }

    fn from_text(text: Option<&str>) -> Result<Self, BoxDynError> {
        text.map(|t| T::from_text(Some(t))).transpose()
    }

    fn write_json(&self, out: &mut String) {
        match self {
            Some(value) => value.write_json(out),
            None => out.push_str("null"),
        }
    }
}

impl<T> Type<Postgres> for KineticaArray<T> {
    fn type_info() -> PgTypeInfo {
        text_type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        matches!(ty.kind(), PgTypeKind::Array(_)) || is_text(ty)
    }
}

impl<T: KineticaArrayElement> Encode<'_, Postgres> for KineticaArray<T> {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        put(buf, array_json(&self.0).as_bytes())
    }
}

impl<'r, T: KineticaArrayElement> Decode<'r, Postgres> for KineticaArray<T> {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        // A computed array, such as ARRAY_APPEND's, arrives as text, in
        // Kinetica's own JSON form
        if is_text(&value.type_info()) {
            return decode_array_text(value.as_str()?).map(Self);
        }
        match value.format() {
            PgValueFormat::Binary => decode_binary_array(value.as_bytes()?),
            PgValueFormat::Text => decode_text_array(value.as_str()?),
        }
        .map(Self)
    }
}

/// An array sent as text: in Kinetica's JSON form, `[1,2,3]`, or in
/// PostgreSQL's, `{1,2,3}`, perhaps after its bounds (`[0:2]={1,2,3}`).
fn decode_array_text<T: KineticaArrayElement>(text: &str) -> Result<Vec<T>, BoxDynError> {
    let text = text.trim();
    if text.starts_with('{') || text.contains("]={") {
        decode_text_array(text)
    } else {
        decode_json_array(text)
    }
}

/// An array in Kinetica's JSON form: `[1,2,3]`, `["x",null]`.
fn decode_json_array<T: KineticaArrayElement>(text: &str) -> Result<Vec<T>, BoxDynError> {
    serde_json::from_str::<Vec<serde_json::Value>>(text)?
        .into_iter()
        .map(|element| match element {
            serde_json::Value::Null => T::from_text(None),
            serde_json::Value::String(s) => T::from_text(Some(&s)),
            serde_json::Value::Bool(b) => T::from_text(Some(if b { "true" } else { "false" })),
            serde_json::Value::Number(n) => T::from_text(Some(&n.to_string())),
            other => Err(format!("an array element {other} is neither a scalar nor null").into()),
        })
        .collect()
}

/// An array as the JSON text Kinetica takes for one: `[1,2,3]`,
/// `["x",null]`.
fn array_json<T: KineticaArrayElement>(elements: &[T]) -> String {
    let mut out = String::from("[");
    for (i, element) in elements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        element.write_json(&mut out);
    }
    out.push(']');
    out
}

/// Reads a big-endian `i32` at `*at`, moving past it.
fn read_i32(bytes: &[u8], at: &mut usize) -> Result<i32, BoxDynError> {
    let value = bytes
        .get(*at..*at + 4)
        .ok_or("the array ends early")?;
    *at += 4;
    Ok(i32::from_be_bytes(fixed::<4>(value)?))
}

/// An array in PostgreSQL's binary form: dimensions, flags and element type,
/// then each dimension's length and lower bound, then each element's length
/// (-1 for a null) and bytes. Kinetica's arrays have one dimension, numbered
/// from 0, which is why SQLx's own decoding refuses them.
fn decode_binary_array<T: KineticaArrayElement>(bytes: &[u8]) -> Result<Vec<T>, BoxDynError> {
    let mut at = 0;
    let dimensions = read_i32(bytes, &mut at)?;
    let _flags = read_i32(bytes, &mut at)?;
    let element_oid = read_i32(bytes, &mut at)? as u32;
    if dimensions == 0 {
        return Ok(Vec::new());
    }
    if dimensions != 1 {
        return Err(format!("an array of {dimensions} dimensions; Kinetica's have one").into());
    }
    if !T::ELEMENT_OIDS.contains(&element_oid) {
        return Err(format!(
            "an array of elements of type OID {element_oid} cannot be read as {}",
            std::any::type_name::<T>()
        )
        .into());
    }
    let length = read_i32(bytes, &mut at)?;
    let _lower_bound = read_i32(bytes, &mut at)?;

    let mut elements = Vec::with_capacity(length.max(0) as usize);
    for _ in 0..length {
        let size = read_i32(bytes, &mut at)?;
        let element = if size < 0 {
            None
        } else {
            let end = at + size as usize;
            let element = bytes.get(at..end).ok_or("the array ends early")?;
            at = end;
            Some(element)
        };
        elements.push(T::from_binary(element)?);
    }
    Ok(elements)
}

/// An array in PostgreSQL's text form, `{1,2,3}` or `{"x",NULL}`, perhaps
/// after its bounds (`[0:2]=`).
///
/// Kinetica escapes a quoted element as JSON does (`\n`, `\t`, `\uXXXX`),
/// where PostgreSQL takes a backslash to quote the character after it; both
/// read, since PostgreSQL escapes only `\\` and `\"`, which mean the same in
/// each.
fn decode_text_array<T: KineticaArrayElement>(text: &str) -> Result<Vec<T>, BoxDynError> {
    let text = text.trim();
    let text = match text.strip_prefix('[') {
        Some(_) => text.split_once('=').map(|(_, rest)| rest).ok_or("an array's bounds without its values")?,
        None => text,
    };
    let inner = text
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .ok_or_else(|| format!("'{text}' is not an array"))?;

    let mut elements = Vec::new();
    let mut chars = inner.chars().peekable();
    while chars.peek().is_some() {
        let element = if chars.peek() == Some(&'"') {
            chars.next();
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some('\\') => match chars.next().ok_or("an array ends inside an escape")? {
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        'r' => value.push('\r'),
                        'b' => value.push('\u{8}'),
                        'f' => value.push('\u{c}'),
                        'u' => value.push(unicode_escape(&mut chars)?),
                        other => value.push(other),
                    },
                    Some('"') => break,
                    Some(c) => value.push(c),
                    None => return Err("an array ends inside a quoted element".into()),
                }
            }
            Some(value)
        } else {
            let mut value = String::new();
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                value.push(c);
                chars.next();
            }
            let value = value.trim().to_string();
            (!value.eq_ignore_ascii_case("NULL")).then_some(value)
        };
        elements.push(T::from_text(element.as_deref())?);
        match chars.next() {
            None | Some(',') => {}
            Some(other) => return Err(format!("'{other}' after an array element").into()),
        }
    }
    Ok(elements)
}

/// The character of a JSON `\\uXXXX` escape, whose `\\u` has been read: one
/// code unit, or a surrogate pair written as two escapes.
fn unicode_escape(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<char, BoxDynError> {
    fn code_unit(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<u32, BoxDynError> {
        let hex: String = chars.by_ref().take(4).collect();
        if hex.len() != 4 {
            return Err("an array ends inside a \\u escape".into());
        }
        Ok(u32::from_str_radix(&hex, 16)?)
    }
    let first = code_unit(chars)?;
    let code = if (0xD800..0xDC00).contains(&first) {
        if chars.next() != Some('\\') || chars.next() != Some('u') {
            return Err("a lone surrogate in a \\u escape".into());
        }
        let second = code_unit(chars)?;
        0x10000 + ((first - 0xD800) << 10) + (second.wrapping_sub(0xDC00) & 0x3FF)
    } else {
        first
    };
    char::from_u32(code).ok_or_else(|| format!("\\u escape {code:#x} is not a character").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The binary form of a one-dimensional array numbered from 0, as
    /// Kinetica sends it.
    fn binary(element_oid: u32, elements: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = Vec::new();
        for v in [1i32, 1, element_oid as i32, elements.len() as i32, 0] {
            out.extend_from_slice(&v.to_be_bytes());
        }
        for element in elements {
            match element {
                Some(bytes) => {
                    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    out.extend_from_slice(bytes);
                }
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        out
    }

    /// The arrays Kinetica sent for `[1,2,3]` and `["x",null,"zz"]`, as
    /// captured from the server.
    #[test]
    fn kinetica_s_binary_arrays_are_read() {
        let ints = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 3];
        assert_eq!(decode_binary_array::<i32>(&ints).unwrap(), vec![1, 2, 3]);
        let strings = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 4, 19, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 120, 255, 255, 255, 255, 0, 0, 0, 2, 122, 122];
        assert_eq!(
            decode_binary_array::<Option<String>>(&strings).unwrap(),
            vec![Some("x".to_string()), None, Some("zz".to_string())]
        );
        assert!(decode_binary_array::<String>(&strings).unwrap_err().to_string().contains("KineticaArray<Option<_>>"));
        let empty = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(decode_binary_array::<i32>(&empty).unwrap().is_empty());
    }

    #[test]
    fn every_element_type_is_read_in_binary() {
        assert_eq!(decode_binary_array::<i64>(&binary(20, &[Some(&9_000_000_000i64.to_be_bytes())])).unwrap(), vec![9_000_000_000]);
        assert_eq!(decode_binary_array::<f32>(&binary(700, &[Some(&1.5f32.to_be_bytes())])).unwrap(), vec![1.5]);
        assert_eq!(decode_binary_array::<f64>(&binary(701, &[Some(&2.5f64.to_be_bytes())])).unwrap(), vec![2.5]);
        assert_eq!(decode_binary_array::<bool>(&binary(16, &[Some(&[1]), Some(&[0])])).unwrap(), vec![true, false]);
        assert_eq!(
            decode_binary_array::<u64>(&binary(1043, &[Some(b"18446744073709551615")])).unwrap(),
            vec![u64::MAX]
        );
        assert!(decode_binary_array::<i32>(&binary(20, &[Some(&1i64.to_be_bytes())])).is_err(), "a BIGINT[] is not an INTEGER[]");
    }

    /// `sqlx::raw_sql` gets arrays as text.
    #[test]
    fn text_arrays_are_read() {
        assert_eq!(decode_text_array::<i32>("{1,2,3}").unwrap(), vec![1, 2, 3]);
        assert_eq!(decode_text_array::<i32>("[0:2]={1,2,3}").unwrap(), vec![1, 2, 3]);
        assert_eq!(
            decode_text_array::<Option<String>>(r#"{"x",NULL,"z\"z",plain}"#).unwrap(),
            vec![Some("x".into()), None, Some("z\"z".into()), Some("plain".into())]
        );
        assert_eq!(decode_text_array::<bool>("{t,f}").unwrap(), vec![true, false]);
        // Kinetica escapes quoted elements as JSON does
        assert_eq!(
            decode_text_array::<String>(r#"{"new\nline","tab\tx","cr\rx","back\\slash","q\"uote","\u00e9\ud83d\ude00"}"#).unwrap(),
            vec!["new\nline", "tab\tx", "cr\rx", "back\\slash", "q\"uote", "é😀"]
        );
        assert!(decode_text_array::<i32>("{}").unwrap().is_empty());
    }

    #[test]
    fn arrays_are_written_as_kinetica_s_json() {
        assert_eq!(array_json(&[1i32, 2, 3]), "[1,2,3]");
        assert_eq!(array_json(&[Some("x".to_string()), None, Some("a\"b".to_string())]), r#"["x",null,"a\"b"]"#);
        assert_eq!(array_json(&[true, false]), "[true,false]");
        assert_eq!(array_json(&[1.5f64]), "[1.5]");
        assert_eq!(array_json::<i32>(&[]), "[]");
    }

    #[test]
    fn values_are_written_as_the_text_kinetica_converts() {
        assert_eq!(true.to_kinetica_text(), "1");
        assert_eq!(false.to_kinetica_text(), "0");
        assert_eq!(42i32.to_kinetica_text(), "42");
        assert_eq!(2.25f64.to_kinetica_text(), "2.25");
        assert_eq!(u64::MAX.to_kinetica_text(), "18446744073709551615");
        let at = chrono::NaiveDate::from_ymd_opt(2026, 10, 8).unwrap().and_hms_milli_opt(12, 34, 56, 789).unwrap();
        assert_eq!(at.to_kinetica_text(), "2026-10-08 12:34:56.789");
        assert_eq!(at.date().to_kinetica_text(), "2026-10-08");
        assert_eq!(at.time().to_kinetica_text(), "12:34:56.789");
        assert_eq!(uuid::Uuid::nil().to_kinetica_text(), "00000000-0000-0000-0000-000000000000");
        assert_eq!(serde_json::json!({"a": [1, null]}).to_kinetica_text(), r#"{"a":[1,null]}"#);
    }

    #[test]
    fn values_are_read_from_kinetica_s_text() {
        let at = chrono::NaiveDateTime::from_kinetica_text("2026-10-08 12:34:56.789").unwrap();
        assert_eq!(at.and_utc().timestamp_subsec_millis(), 789);
        assert_eq!(chrono::NaiveTime::from_kinetica_text("12:34:56.789").unwrap().to_kinetica_text(), "12:34:56.789");
        assert_eq!(chrono::NaiveTime::from_kinetica_text("12:34:56").unwrap().to_kinetica_text(), "12:34:56.000");
        assert_eq!(u64::from_kinetica_text("18446744073709551615").unwrap(), u64::MAX);
        assert_eq!(Ipv4Addr::from_kinetica_text("192.168.1.1").unwrap(), Ipv4Addr::new(192, 168, 1, 1));
        assert!(bool::from_kinetica_text("1").unwrap());
    }

    /// Computed arrays arrive as text in Kinetica's JSON form.
    #[test]
    fn json_form_arrays_are_read() {
        assert_eq!(decode_array_text::<i32>("[1,2,3]").unwrap(), vec![1, 2, 3]);
        assert_eq!(decode_array_text::<u64>("[18446744073709551615]").unwrap(), vec![u64::MAX]);
        assert_eq!(decode_array_text::<f64>("[1.5,-2e-3]").unwrap(), vec![1.5, -0.002]);
        assert_eq!(decode_array_text::<bool>("[true,false]").unwrap(), vec![true, false]);
        assert_eq!(
            decode_array_text::<Option<String>>(r#"["a\"b",null,"{x}"]"#).unwrap(),
            vec![Some("a\"b".into()), None, Some("{x}".into())]
        );
        assert!(decode_array_text::<i32>("[]").unwrap().is_empty());
        assert!(decode_array_text::<i32>("[[1]]").is_err());
        // PostgreSQL's text forms still read
        assert_eq!(decode_array_text::<i32>("{1,2}").unwrap(), vec![1, 2]);
        assert_eq!(decode_array_text::<i32>("[0:1]={1,2}").unwrap(), vec![1, 2]);
    }

    #[test]
    fn a_vector_is_written_as_kinetica_s_text() {
        assert_eq!([1.0f32, 2.5, -3.0].to_kinetica_text(), "[1,2.5,-3]");
        assert_eq!(vec![0.1f32].to_kinetica_text(), "[0.1]");
        assert_eq!(Vec::<f32>::new().to_kinetica_text(), "[]");
    }

    #[test]
    fn vectors_and_timestamps_convert() {
        assert_eq!(parse_vector("[1,2.5,3]").unwrap(), vec![1.0, 2.5, 3.0]);
        assert!(parse_vector("[]").unwrap().is_empty());
        assert!(parse_vector("1,2").is_err());
        let at = KineticaTimestamp::from_millis(1_791_462_896_789).unwrap();
        assert_eq!(at.0.to_rfc3339(), "2026-10-08T12:34:56.789+00:00");
        assert_eq!(at.millis(), 1_791_462_896_789);
    }
}
