//! Decodes Avro binary data against a schema known only at runtime, into
//! values the caller then interprets: a record whose schema comes from the
//! server, such as a table's type or a dynamic (column-wise) response.
//!
//! The typed decoders in [`crate::avro`] need a Rust type to decode into; these
//! records have none until their schema has been read.

use crate::KineticaError;

/// An Avro value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AvroValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Array(Vec<AvroValue>),
    Map(Vec<(String, AvroValue)>),
    /// A record's field values, in schema order.
    Record(Vec<AvroValue>),
}

/// Decodes one value of `schema` from the start of `data`; anything after it
/// is ignored, as Avro's reader ignores it. A failure is reported as "Could
/// not decode object", followed by the reason where Avro gives one.
///
/// Values are read as follows: a boolean is true only for the byte 1, an int
/// takes at most 5 bytes and a long at most 10 (bits past their width are
/// dropped), and a string that is not valid UTF-8 is decoded with U+FFFD for
/// each bad sequence.
pub(crate) fn decode(schema: &serde_json::Value, data: &[u8]) -> Result<AvroValue, KineticaError> {
    let mut reader = Reader { data, position: 0 };
    reader.read(schema).map_err(|e| {
        KineticaError::Api(match e {
            // Avro's EOFException carries no message
            DecodeError::EndOfData => "Could not decode object".to_string(),
            DecodeError::Invalid(reason) => format!("Could not decode object: {reason}"),
        })
    })
}

/// Why a value could not be read.
enum DecodeError {
    /// The data ended inside the value.
    EndOfData,
    /// The data or the schema is not valid, as the message says.
    Invalid(String),
}

impl From<String> for DecodeError {
    fn from(reason: String) -> Self {
        DecodeError::Invalid(reason)
    }
}

impl From<&str> for DecodeError {
    fn from(reason: &str) -> Self {
        DecodeError::Invalid(reason.to_string())
    }
}

struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

impl Reader<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8], DecodeError> {
        let end = self.position.checked_add(count).filter(|&end| end <= self.data.len());
        let Some(end) = end else {
            return Err(DecodeError::EndOfData);
        };
        let bytes = &self.data[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    /// A zig-zag variable-length number of at most `max_bytes` bytes, whose
    /// bits past 64 are dropped.
    fn read_varint(&mut self, max_bytes: u32, invalid: &str) -> Result<u64, DecodeError> {
        let mut value: u64 = 0;
        for i in 0..max_bytes {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 0x7f).checked_shl(7 * i).unwrap_or(0);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(invalid.into())
    }

    fn read_long(&mut self) -> Result<i64, DecodeError> {
        let value = self.read_varint(10, "Invalid long encoding")?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    /// An int: 32 bits, the rest of a fifth byte dropped.
    fn read_int(&mut self) -> Result<i32, DecodeError> {
        let value = self.read_varint(5, "Invalid int encoding")? as u32;
        Ok(((value >> 1) as i32) ^ -((value & 1) as i32))
    }

    fn read_length(&mut self) -> Result<usize, DecodeError> {
        let length = self.read_long()?;
        usize::try_from(length).map_err(|_| format!("Malformed data: negative length {length}").into())
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let length = self.read_length()?;
        Ok(self.take(length)?.to_vec())
    }

    fn read_string(&mut self) -> Result<String, DecodeError> {
        Ok(utf8_with_replacements(self.read_bytes()?))
    }

    /// The item count of the next block of an array or map, 0 at the end. A
    /// negative count is followed by the block's size in bytes, unused here.
    fn read_block_count(&mut self) -> Result<usize, DecodeError> {
        let count = self.read_long()?;
        if count < 0 {
            self.read_long()?;
        }
        usize::try_from(count.unsigned_abs()).map_err(|_| "block too large".into())
    }

    fn read(&mut self, schema: &serde_json::Value) -> Result<AvroValue, DecodeError> {
        match schema {
            serde_json::Value::String(name) => self.read_named(name, schema),
            serde_json::Value::Array(branches) => {
                let index = self.read_int()?;
                let branch = usize::try_from(index).ok().and_then(|i| branches.get(i)).ok_or_else(|| {
                    format!("Union branch {index} is out of range; the union has {}", branches.len())
                })?;
                self.read(branch)
            }
            serde_json::Value::Object(object) => {
                let Some(type_name) = object.get("type") else {
                    return Err("schema has no type".into());
                };
                match type_name.as_str() {
                    Some(name) => self.read_named(name, schema),
                    // {"type": [...]} or {"type": {...}}
                    None => self.read(type_name),
                }
            }
            _ => Err(format!("invalid schema {schema}").into()),
        }
    }

    fn read_named(&mut self, name: &str, schema: &serde_json::Value) -> Result<AvroValue, DecodeError> {
        Ok(match name {
            "null" => AvroValue::Null,
            "boolean" => AvroValue::Boolean(self.take(1)?[0] == 1),
            "int" => AvroValue::Int(self.read_int()?),
            "long" => AvroValue::Long(self.read_long()?),
            "float" => AvroValue::Float(f32::from_le_bytes(self.take(4)?.try_into().unwrap())),
            "double" => AvroValue::Double(f64::from_le_bytes(self.take(8)?.try_into().unwrap())),
            "bytes" => AvroValue::Bytes(self.read_bytes()?),
            "string" => AvroValue::String(self.read_string()?),
            "array" => {
                let items = schema.get("items").ok_or("array schema has no items")?;
                let mut values = Vec::new();
                loop {
                    let count = self.read_block_count()?;
                    if count == 0 {
                        break;
                    }
                    for _ in 0..count {
                        values.push(self.read(items)?);
                    }
                }
                AvroValue::Array(values)
            }
            "map" => {
                let value_schema = schema.get("values").ok_or("map schema has no values")?;
                let mut entries = Vec::new();
                loop {
                    let count = self.read_block_count()?;
                    if count == 0 {
                        break;
                    }
                    for _ in 0..count {
                        let key = self.read_string()?;
                        entries.push((key, self.read(value_schema)?));
                    }
                }
                AvroValue::Map(entries)
            }
            "record" => {
                let fields = schema
                    .get("fields")
                    .and_then(|f| f.as_array())
                    .ok_or("record schema has no fields")?;
                let mut values = Vec::with_capacity(fields.len());
                for field in fields {
                    let field_type = field.get("type").ok_or("field has no type")?;
                    values.push(self.read(field_type)?);
                }
                AvroValue::Record(values)
            }
            other => return Err(format!("unsupported schema type {other}").into()),
        })
    }
}

/// The text of UTF-8 `bytes`, each malformed sequence replaced by U+FFFD.
/// That is Unicode's "maximal subpart" practice, as `from_utf8_lossy`
/// follows, with one exception: `ED` followed by `A0..BF`, the start of an
/// encoded surrogate, is read as the first two bytes of a three-byte
/// sequence, where `from_utf8_lossy` stops at `ED`. So `ED A0 80` is replaced
/// once, and `ED A0` once wherever the third byte is missing or wrong, where
/// `from_utf8_lossy` replaces each byte.
fn utf8_with_replacements(bytes: Vec<u8>) -> String {
    let bytes = match String::from_utf8(bytes) {
        Ok(text) => return text,
        Err(e) => e.into_bytes(),
    };
    let mut text = String::with_capacity(bytes.len());
    let mut rest = bytes.as_slice();
    while let Some(chunk) = rest.utf8_chunks().next() {
        text.push_str(chunk.valid());
        let invalid = chunk.invalid();
        let mut after = &rest[chunk.valid().len() + invalid.len()..];
        if !invalid.is_empty() {
            text.push(char::REPLACEMENT_CHARACTER);
            if invalid == [0xed] {
                // The second byte, and the third where it is a continuation
                if let [0xa0..=0xbf, past_second @ ..] = after {
                    after = match past_second {
                        [0x80..=0xbf, past_third @ ..] => past_third,
                        _ => past_second,
                    };
                }
            }
        }
        rest = after;
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn long_bytes(v: i64) -> Vec<u8> {
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let byte = (z & 0x7f) as u8;
            z >>= 7;
            if z == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn string_bytes(s: &str) -> Vec<u8> {
        let mut out = long_bytes(s.len() as i64);
        out.extend_from_slice(s.as_bytes());
        out
    }

    #[test]
    fn primitives_unions_and_records_decode() {
        let schema = json!({"type": "record", "name": "r", "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": ["long", "null"]},
            {"name": "c", "type": ["long", "null"]},
            {"name": "d", "type": "string"},
            {"name": "e", "type": "double"},
        ]});
        let mut data = long_bytes(-3);
        data.extend(long_bytes(0)); // branch long
        data.extend(long_bytes(1 << 40));
        data.extend(long_bytes(1)); // branch null
        data.extend(string_bytes("héllo"));
        data.extend(2.5f64.to_le_bytes());
        assert_eq!(
            decode(&schema, &data).unwrap(),
            AvroValue::Record(vec![
                AvroValue::Int(-3),
                AvroValue::Long(1 << 40),
                AvroValue::Null,
                AvroValue::String("héllo".to_string()),
                AvroValue::Double(2.5),
            ])
        );
    }

    #[test]
    fn arrays_decode_across_blocks_including_sized_ones() {
        let schema = json!({"type": "array", "items": "int"});
        let mut data = long_bytes(2);
        data.extend(long_bytes(1));
        data.extend(long_bytes(2));
        data.extend(long_bytes(-1)); // a block of 1, with its size
        data.extend(long_bytes(1));
        data.extend(long_bytes(3));
        data.extend(long_bytes(0));
        assert_eq!(
            decode(&schema, &data).unwrap(),
            AvroValue::Array(vec![AvroValue::Int(1), AvroValue::Int(2), AvroValue::Int(3)])
        );
    }

    #[test]
    fn truncated_data_fails_at_the_end_of_input() {
        let err = decode(&json!("string"), &long_bytes(10)).unwrap_err().to_string();
        assert_eq!(err, "API error: Could not decode object");
    }

    /// A one-field record of `field_type`, decoded from `data`: the value, or
    /// the error's text.
    fn field(field_type: serde_json::Value, data: &[u8]) -> Result<AvroValue, String> {
        let schema = json!({"type": "record", "name": "r", "fields": [{"name": "f", "type": field_type}]});
        match decode(&schema, data) {
            Ok(AvroValue::Record(mut values)) => Ok(values.remove(0)),
            Ok(other) => panic!("{other:?}"),
            Err(e) => Err(e.to_string()),
        }
    }

    // A negative length and a union branch out of range are decoding errors.
    #[test]
    fn values_are_read_as_avro_encodes_them() {
        let int = || json!("int");
        let long = || json!("long");
        assert_eq!(field(json!("boolean"), &[0]), Ok(AvroValue::Boolean(false)));
        assert_eq!(field(json!("boolean"), &[1]), Ok(AvroValue::Boolean(true)));
        assert_eq!(field(json!("boolean"), &[2]), Ok(AvroValue::Boolean(false)));
        assert_eq!(field(json!("boolean"), &[0xff]), Ok(AvroValue::Boolean(false)));
        assert_eq!(field(json!("boolean"), &[]), Err("API error: Could not decode object".into()));
        assert_eq!(field(int(), &[0xff, 0xff, 0xff, 0xff, 0x0f]), Ok(AvroValue::Int(i32::MIN)));
        assert_eq!(field(int(), &[0xfe, 0xff, 0xff, 0xff, 0x0f]), Ok(AvroValue::Int(i32::MAX)));
        assert_eq!(field(int(), &[0xff, 0xff, 0xff, 0xff, 0x7f]), Ok(AvroValue::Int(i32::MIN)));
        assert_eq!(field(int(), &[0x80, 0x80, 0x80, 0x80, 0x10]), Ok(AvroValue::Int(0)));
        assert_eq!(
            field(int(), &[0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
            Err("API error: Could not decode object: Invalid int encoding".into())
        );
        assert_eq!(field(long(), &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]), Ok(AvroValue::Long(i64::MIN)));
        assert_eq!(field(long(), &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]), Ok(AvroValue::Long(i64::MIN)));
        assert_eq!(
            field(long(), &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
            Err("API error: Could not decode object: Invalid long encoding".into())
        );
        assert_eq!(field(long(), &[0x80]), Err("API error: Could not decode object".into()));
        assert_eq!(field(json!("string"), &[2, 0x61]), Ok(AvroValue::String("a".into())));
        assert_eq!(
            field(json!("string"), &[1]),
            Err("API error: Could not decode object: Malformed data: negative length -1".into())
        );
        assert_eq!(field(json!("string"), &[4, 0xff, 0x61]), Ok(AvroValue::String("\u{fffd}a".into())));
        assert_eq!(field(json!("string"), &[6, 0xed, 0xa0, 0x80]), Ok(AvroValue::String("\u{fffd}".into())));
        assert_eq!(
            field(json!("bytes"), &[3, 0x61]),
            Err("API error: Could not decode object: Malformed data: negative length -2".into())
        );
        assert_eq!(field(json!("bytes"), &[4, 0x61]), Err("API error: Could not decode object".into()));
        let nullable_int = || json!(["null", "int"]);
        assert_eq!(
            field(nullable_int(), &[4]),
            Err("API error: Could not decode object: Union branch 2 is out of range; the union has 2".into())
        );
        assert_eq!(
            field(nullable_int(), &[1]),
            Err("API error: Could not decode object: Union branch -1 is out of range; the union has 2".into())
        );
        assert_eq!(field(nullable_int(), &[2, 2]), Ok(AvroValue::Int(1)));
        let ints = || json!({"type": "array", "items": "int"});
        assert_eq!(field(ints(), &[1, 2, 4, 0]), Ok(AvroValue::Array(vec![AvroValue::Int(2)])));
        assert_eq!(field(ints(), &[3, 2, 2, 0]), Err("API error: Could not decode object".into()));
        let map = json!({"type": "map", "values": "int"});
        let a1 = Ok(AvroValue::Map(vec![("a".into(), AvroValue::Int(1))]));
        assert_eq!(field(map.clone(), &[2, 2, 0x61, 2, 0]), a1);
        assert_eq!(field(map, &[1, 2, 2, 0x61, 2, 0]), a1);
        assert_eq!(field(json!("float"), &[0, 0, 0x80]), Err("API error: Could not decode object".into()));
        assert_eq!(field(json!("double"), &[0, 0, 0, 0, 0, 0, 0xf0, 0x3f, 9, 9]), Ok(AvroValue::Double(1.0)));
    }

    // Each byte string, and the text it decodes to.
    #[test]
    fn invalid_utf8_decodes_with_replacement_characters() {
        let cases: &[(&[u8], &str)] = &[
            (&[0xed, 0xa0, 0x80], "\u{fffd}"),
            (&[0xed, 0xa0, 0x41], "\u{fffd}A"),
            (&[0x61, 0xed, 0xa0], "a\u{fffd}"),
            (&[0xed, 0xbf, 0xbf], "\u{fffd}"),
            (&[0xed, 0x9f, 0xbf], "\u{d7ff}"),
            (&[0xed, 0xa0], "\u{fffd}"),
            (&[0xed], "\u{fffd}"),
            (&[0xed, 0xbf, 0x80, 0xed, 0xb0, 0x80], "\u{fffd}\u{fffd}"),
            (&[0xe2, 0x82, 0x61], "\u{fffd}a"),
            (&[0xe2, 0x82], "\u{fffd}"),
            (&[0xf0, 0x9f, 0x98, 0x41], "\u{fffd}A"),
            (&[0xf0, 0x9f, 0x98], "\u{fffd}"),
            (&[0xc0, 0xaf], "\u{fffd}\u{fffd}"),
            (&[0xf8, 0x80, 0x80, 0x80, 0x80], "\u{fffd}\u{fffd}\u{fffd}\u{fffd}\u{fffd}"),
            (&[0xff, 0x61], "\u{fffd}a"),
            (&[0x61, 0xe2], "a\u{fffd}"),
            (&[0xed, 0xa0, 0xed, 0xa0, 0x80], "\u{fffd}\u{fffd}"),
            (&[0xed, 0x41], "\u{fffd}A"),
            (&[0xed, 0xc0], "\u{fffd}\u{fffd}"),
            (&[0xef, 0xbf, 0xbf], "\u{ffff}"),
            (&[0xf4, 0x90, 0x80, 0x80], "\u{fffd}\u{fffd}\u{fffd}\u{fffd}"),
            (&[0xc3, 0x28], "\u{fffd}("),
            (&[0xe0, 0xa0, 0x80], "\u{800}"),
            (&[0xe0, 0x80, 0x80], "\u{fffd}\u{fffd}\u{fffd}"),
            (&[0xf0, 0x80, 0x80, 0x80], "\u{fffd}\u{fffd}\u{fffd}\u{fffd}"),
            (&[0xf0, 0x9f, 0x98, 0x80], "\u{1f600}"),
        ];
        for (bytes, expected) in cases {
            assert_eq!(utf8_with_replacements(bytes.to_vec()), *expected, "{bytes:02x?}");
        }
    }
}
