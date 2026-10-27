//! The column properties a Kinetica type can give its columns, as text. A
//! [`Column`](crate::Column) takes them in its properties, and
//! [`KineticaColumnProperties`](crate::KineticaColumnProperties) builds them.

/// Data is stored for this column; the default.
pub const DATA: &str = "data";
/// Full-text search is enabled on this column.
pub const TEXT_SEARCH: &str = "text_search";
/// A long column holding milliseconds since the epoch.
pub const TIMESTAMP: &str = "timestamp";
/// A string column holding an unsigned long.
pub const ULONG: &str = "ulong";
/// A string column holding a UUID.
pub const UUID: &str = "uuid";
/// A string column holding a decimal, `decimal(P,S)` for its precision and
/// scale.
pub const DECIMAL: &str = "decimal";
/// A string column holding a date, `YYYY-MM-DD`.
pub const DATE: &str = "date";
/// A string column holding a time, `HH:MM:SS[.mmm]`.
pub const TIME: &str = "time";
/// A string column holding a date and time, `YYYY-MM-DD HH:MM:SS[.mmm]`.
pub const DATETIME: &str = "datetime";
/// A string column of at most 1 character.
pub const CHAR1: &str = "char1";
/// A string column of at most 2 characters.
pub const CHAR2: &str = "char2";
/// A string column of at most 4 characters.
pub const CHAR4: &str = "char4";
/// A string column of at most 8 characters.
pub const CHAR8: &str = "char8";
/// A string column of at most 16 characters.
pub const CHAR16: &str = "char16";
/// A string column of at most 32 characters.
pub const CHAR32: &str = "char32";
/// A string column of at most 64 characters.
pub const CHAR64: &str = "char64";
/// A string column of at most 128 characters.
pub const CHAR128: &str = "char128";
/// A string column of at most 256 characters.
pub const CHAR256: &str = "char256";
/// An int column holding a boolean, 0 or 1.
pub const BOOLEAN: &str = "boolean";
/// An int column holding an 8-bit integer.
pub const INT8: &str = "int8";
/// An int column holding a 16-bit integer.
pub const INT16: &str = "int16";
/// A string column holding an IPv4 address.
pub const IPV4: &str = "ipv4";
/// A string column holding an array, `array(<type>[,<size>])`.
pub const ARRAY: &str = "array";
/// A string column holding JSON.
pub const JSON: &str = "json";
/// A bytes column holding a vector of floats, `vector(<dimensions>)`.
pub const VECTOR: &str = "vector";
/// A string or bytes column holding a geometry, as WKT or WKB.
pub const WKT: &str = "wkt";
/// The column is part of the primary key.
pub const PRIMARY_KEY: &str = "primary_key";
/// The column is part of a primary key that is not enforced.
pub const SOFT_PRIMARY_KEY: &str = "soft_primary_key";
/// The column is part of the shard key.
pub const SHARD_KEY: &str = "shard_key";
/// The column may hold nulls.
pub const NULLABLE: &str = "nullable";
/// The column's data is compressed.
pub const COMPRESS: &str = "compress";
/// The column's values are dictionary-encoded.
pub const DICT: &str = "dict";
/// The column takes the time of insertion where no value is given.
pub const INIT_WITH_NOW: &str = "init_with_now";
/// The column takes a new UUID where no value is given.
pub const INIT_WITH_UUID: &str = "init_with_uuid";
/// The column takes the time of each update.
pub const UPDATE_WITH_NOW: &str = "update_with_now";
/// The column takes the user making each update.
pub const UPDATE_WITH_USER: &str = "update_with_user";
/// The column's default, `default(<value>)`.
pub const DEFAULT: &str = "default";
/// The column's default as an SQL expression.
pub const DEFAULT_SQL: &str = "default_sql";

#[cfg(test)]
mod tests {
    #[test]
    fn the_constants_have_their_wire_values() {
        use super::*;
        let all = [
            DATA, TEXT_SEARCH, TIMESTAMP, ULONG, UUID, DECIMAL, DATE, TIME, DATETIME, CHAR1, CHAR2, CHAR4, CHAR8,
            CHAR16, CHAR32, CHAR64, CHAR128, CHAR256, BOOLEAN, INT8, INT16, IPV4, ARRAY, JSON, VECTOR, WKT,
            PRIMARY_KEY, SOFT_PRIMARY_KEY, SHARD_KEY, NULLABLE, COMPRESS, DICT, INIT_WITH_NOW, INIT_WITH_UUID,
            UPDATE_WITH_NOW, UPDATE_WITH_USER, DEFAULT, DEFAULT_SQL,
        ];
        assert_eq!(all.len(), 38);
        assert_eq!(all.join(","), "data,text_search,timestamp,ulong,uuid,decimal,date,time,datetime,char1,char2,char4,\
            char8,char16,char32,char64,char128,char256,boolean,int8,int16,ipv4,array,json,vector,wkt,primary_key,\
            soft_primary_key,shard_key,nullable,compress,dict,init_with_now,init_with_uuid,update_with_now,\
            update_with_user,default,default_sql");
    }
}
