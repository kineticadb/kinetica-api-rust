//! High-performance Avro encoding and decoding utilities.
//!
//! This module provides optimized Avro serialization and deserialization using
//! `serde_avro_fast` for best performance, with `rayon` parallelism for large batches.
//!
//! # Performance
//!
//! - **Encoding**: Uses `serde_avro_fast` for 2-3x faster encoding than `apache_avro`
//! - **Decoding**: Uses `serde_avro_fast` + rayon for up to 8x faster decoding
//! - **Parallelism**: Automatically enabled for batches above threshold (500 records)
//!
//! These helpers split large batches over rayon's threads, with the results
//! of a sequential pass: records come back in order, and a batch with several
//! bad records reports the first of them. Failures read "Could not encode
//! object: ..." and "Could not decode object: ...".
//!
//! # Usage
//!
//! ## Encoding Records
//!
//! ```
//! use gpudb_api::avro::{encode_records, parse_fast_schema};
//! # #[derive(serde::Serialize)]
//! # struct Employee { id: i32, name: String }
//! # let schema_str = r#"{"type":"record","name":"Employee","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
//! # let records = vec![Employee { id: 1, name: "Ann".to_string() }];
//!
//! let schema = parse_fast_schema(schema_str)?;
//! let encoded: Vec<Vec<u8>> = encode_records(&schema, &records)?;
//! # assert_eq!(encoded.len(), 1);
//! # Ok::<(), gpudb_api::KineticaError>(())
//! ```
//!
//! ## Decoding Records
//!
//! ```no_run
//! use gpudb_api::avro::{decode_records, parse_fast_schema};
//! # use gpudb_api::{GetRecordsRequest, Kinetica};
//!
//! #[derive(serde::Deserialize)]
//! struct Employee { id: i32, name: String }
//!
//! # async fn example(kinetica: Kinetica, request: GetRecordsRequest) -> Result<(), gpudb_api::KineticaError> {
//! # let response = kinetica.get_records_raw(&request).await?;
//! let schema = parse_fast_schema(&response.type_schema)?;
//! let employees: Vec<Employee> = decode_records(&schema, &response.records_binary)?;
//! # Ok(())
//! # }
//! ```

use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_avro_fast::Schema;
use std::str::FromStr;
use std::sync::Arc;

use crate::KineticaError;

// ============================================================================
// Constants
// ============================================================================

/// Threshold for switching to parallel processing.
/// Below this count, sequential processing is faster due to thread spawning overhead.
/// Based on benchmarks: parallel becomes beneficial around 500 records.
pub const PARALLEL_THRESHOLD: usize = 500;

/// The error for a record that could not be encoded.
fn encode_error(e: impl std::fmt::Display) -> KineticaError {
    KineticaError::Avro(format!("Could not encode object: {e}"))
}

/// The error for a record that could not be decoded.
fn decode_error(e: impl std::fmt::Display) -> KineticaError {
    KineticaError::Avro(format!("Could not decode object: {e}"))
}

/// Encodes one record with a configuration of its own.
fn encode_one<T: Serialize + ?Sized>(schema: &Schema, record: &T) -> Result<Vec<u8>, KineticaError> {
    let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
    serde_avro_fast::to_datum(record, Vec::new(), &mut config).map_err(encode_error)
}

/// Decodes one record.
fn decode_one<T: DeserializeOwned>(schema: &Schema, binary_data: &[u8]) -> Result<T, KineticaError> {
    serde_avro_fast::from_datum_slice(binary_data, schema).map_err(decode_error)
}

/// Encodes records one after another, with one serializer configuration for
/// them all, stopping at the first that fails.
fn encode_in_order<'a, T: Serialize + 'a>(
    schema: &Schema,
    records: impl ExactSizeIterator<Item = &'a T>,
) -> Result<Vec<Vec<u8>>, KineticaError> {
    let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
    let mut encoded = Vec::with_capacity(records.len());
    // Each record's buffer starts a little larger than the one before, which
    // records of one type are usually close to
    let mut size_hint = 64;
    for record in records {
        let bytes = serde_avro_fast::to_datum(record, Vec::with_capacity(size_hint), &mut config).map_err(encode_error)?;
        size_hint = bytes.len() + 16;
        encoded.push(bytes);
    }
    Ok(encoded)
}

/// The result of a parallel pass over a batch, with the error of the first
/// record in order where several failed.
///
/// Rayon's `collect` into a `Result` stops at a failure, but at whichever
/// one a thread met first. A sequential pass reports the first; so on that
/// rare path the batch is gone over again in order, and the happy path costs
/// nothing extra.
fn first_failure_in_order<T>(
    parallel: Result<Vec<T>, KineticaError>,
    sequential: impl FnOnce() -> Result<Vec<T>, KineticaError>,
) -> Result<Vec<T>, KineticaError> {
    parallel.or_else(|_| sequential())
}

// ============================================================================
// Schema Parsing Utilities
// ============================================================================

/// Parses a schema string into a serde_avro_fast Schema.
///
/// This is a convenience function for parsing schema strings returned
/// by Kinetica's `type_schema` field.
///
/// # Example
///
/// ```
/// use gpudb_api::avro::parse_schema;
/// # let response = gpudb_api::RawGetRecordsResponse {
/// #     type_schema: r#"{"type":"record","name":"r","fields":[{"name":"id","type":"int"}]}"#.to_string(),
/// #     ..Default::default()
/// # };
///
/// let schema = parse_schema(&response.type_schema)?;
/// # Ok::<(), gpudb_api::KineticaError>(())
/// ```
pub fn parse_schema(schema_str: &str) -> Result<Schema, KineticaError> {
    Schema::from_str(schema_str)
        .map_err(|e| KineticaError::Avro(format!("Failed to parse schema: {e}")))
}

/// Parses a schema string into a serde_avro_fast Schema for direct use.
///
/// This is an alias for `parse_schema` provided for clarity.
pub fn parse_fast_schema(schema_str: &str) -> Result<Schema, KineticaError> {
    parse_schema(schema_str)
}

// ============================================================================
// ENCODING API
// ============================================================================

/// Encodes records to Avro binary format.
///
/// Automatically chooses between sequential and parallel encoding based on
/// record count. Uses `serde_avro_fast` for optimal performance.
///
/// # Type Parameters
///
/// * `T` - The record type. Must implement `serde::Serialize`.
///
/// # Arguments
///
/// * `schema` - The serde_avro_fast schema to use for encoding
/// * `records` - Slice of records to encode
///
/// # Returns
///
/// A vector of binary-encoded Avro records.
///
/// # Example
///
/// ```
/// use gpudb_api::avro::{encode_records, parse_fast_schema};
/// # #[derive(serde::Serialize)]
/// # struct Employee { id: i32, name: String }
/// # let schema_str = r#"{"type":"record","name":"Employee","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
/// # let records = vec![Employee { id: 1, name: "Ann".to_string() }, Employee { id: 2, name: "Bo".to_string() }];
///
/// let schema = parse_fast_schema(schema_str)?;
/// let encoded = encode_records(&schema, &records)?;
/// # assert_eq!(encoded.len(), 2);
/// # Ok::<(), gpudb_api::KineticaError>(())
/// ```
pub fn encode_records<T: Serialize + Sync>(
    schema: &Schema,
    records: &[T],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    if records.len() >= PARALLEL_THRESHOLD {
        encode_records_parallel(schema, records)
    } else {
        encode_records_sequential(schema, records)
    }
}

/// Encodes records in parallel using serde_avro_fast + rayon.
///
/// Use this when you have a large number of records (500+) and want
/// maximum throughput. For automatic threshold handling, use `encode_records`.
pub fn encode_records_parallel<T: Serialize + Sync>(
    schema: &Schema,
    records: &[T],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    first_failure_in_order(records.par_iter().map(|record| encode_one(schema, record)).collect(), || {
        encode_in_order(schema, records.iter())
    })
}

/// Encodes records sequentially using serde_avro_fast.
///
/// Use this for small batches (<500 records) where parallel overhead
/// would outweigh benefits. For automatic threshold handling, use `encode_records`.
pub fn encode_records_sequential<T: Serialize>(
    schema: &Schema,
    records: &[T],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    encode_in_order(schema, records.iter())
}

/// Encodes Arc-wrapped records in parallel.
///
/// This is useful when records are already wrapped in Arc for shared ownership.
pub fn encode_arc_records_parallel<T: Serialize + Sync + Send>(
    schema: &Schema,
    records: &[Arc<T>],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    first_failure_in_order(records.par_iter().map(|record| encode_one(schema, record.as_ref())).collect(), || {
        encode_arc_records_sequential(schema, records)
    })
}

/// Encodes Arc-wrapped records, partitioning the batch over threads.
///
/// This strategy partitions the batch into chunks (one per thread) and processes
/// each chunk sequentially within the thread. This reduces:
/// - Lock contention (each thread has its own chunk)
/// - SerializerConfig creation overhead (reused within chunks)
/// - Memory allocation (pre-allocated buffers per chunk)
pub fn encode_arc_records_partitioned<T: Serialize + Sync + Send>(
    schema: &Schema,
    records: &[Arc<T>],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // For small batches, use sequential encoding
    if records.len() < PARALLEL_THRESHOLD {
        return encode_arc_records_sequential(schema, records);
    }

    // Get number of threads
    let num_threads = rayon::current_num_threads();
    let count = records.len();
    let partition_size = count / num_threads;
    let partition_extras = count % num_threads;

    // Partition the records and encode each partition in parallel
    // Each thread gets a contiguous chunk
    let results: Vec<Result<Vec<Vec<u8>>, KineticaError>> = (0..num_threads)
        .into_par_iter()
        .map(|i| {
            // Calculate partition bounds
            let start = i * partition_size + i.min(partition_extras);
            let end = start + partition_size + if i < partition_extras { 1 } else { 0 };

            if start >= end {
                return Ok(Vec::new());
            }

            // Each partition on one thread, with one serializer configuration
            encode_arc_records_sequential(schema, &records[start..end])
        })
        .collect();

    // Flatten results while preserving order
    let mut final_result = Vec::with_capacity(count);
    for partition_result in results {
        final_result.extend(partition_result?);
    }

    Ok(final_result)
}

/// Encodes Arc-wrapped records one after another on the calling thread, with
/// one serializer configuration for them all.
pub(crate) fn encode_arc_records_sequential<T: Serialize>(
    schema: &Schema,
    records: &[Arc<T>],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    encode_in_order(schema, records.iter().map(Arc::as_ref))
}

/// Encodes records, partitioning the batch over threads (non-Arc version).
///
/// Same strategy as `encode_arc_records_partitioned` but for non-Arc records.
pub fn encode_records_partitioned<T: Serialize + Sync>(
    schema: &Schema,
    records: &[T],
) -> Result<Vec<Vec<u8>>, KineticaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // For small batches, use sequential encoding
    if records.len() < PARALLEL_THRESHOLD {
        return encode_records_sequential(schema, records);
    }

    let num_threads = rayon::current_num_threads();
    let count = records.len();
    let partition_size = count / num_threads;
    let partition_extras = count % num_threads;

    let results: Vec<Result<Vec<Vec<u8>>, KineticaError>> = (0..num_threads)
        .into_par_iter()
        .map(|i| {
            let start = i * partition_size + i.min(partition_extras);
            let end = start + partition_size + if i < partition_extras { 1 } else { 0 };

            if start >= end {
                return Ok(Vec::new());
            }

            encode_in_order(schema, records[start..end].iter())
        })
        .collect();

    let mut final_result = Vec::with_capacity(count);
    for partition_result in results {
        final_result.extend(partition_result?);
    }

    Ok(final_result)
}

/// Encodes a single record to Avro binary format.
///
/// Useful for encoding individual records without batch overhead.
pub fn encode_single<T: Serialize>(
    schema: &Schema,
    record: &T,
) -> Result<Vec<u8>, KineticaError> {
    encode_one(schema, record)
}

// ============================================================================
// DECODING API
// ============================================================================

/// Decodes binary Avro records to typed Rust structs.
///
/// This is the **recommended** decoding function. It automatically chooses
/// between sequential and parallel decoding based on record count, and uses
/// `serde_avro_fast` for optimal performance.
///
/// # Type Parameters
///
/// * `T` - The target type to decode into. Must implement `serde::Deserialize`.
///
/// # Arguments
///
/// * `schema` - The serde_avro_fast schema to use for decoding
/// * `binary_records` - Slice of binary Avro-encoded records
///
/// # Returns
///
/// A vector of decoded typed records, or an error if any record fails to decode.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::avro::{decode_records, parse_fast_schema};
/// # use gpudb_api::{GetRecordsRequest, Kinetica};
///
/// #[derive(serde::Deserialize)]
/// struct Employee { id: i32, name: String, salary: f64 }
///
/// # async fn example(kinetica: Kinetica, request: GetRecordsRequest) -> Result<(), gpudb_api::KineticaError> {
/// let response = kinetica.get_records_raw(&request).await?;
/// let schema = parse_fast_schema(&response.type_schema)?;
/// let employees: Vec<Employee> = decode_records(&schema, &response.records_binary)?;
/// # Ok(())
/// # }
/// ```
pub fn decode_records<T: DeserializeOwned + Send, B: AsRef<[u8]> + Sync>(
    schema: &Schema,
    binary_records: &[B],
) -> Result<Vec<T>, KineticaError> {
    if binary_records.is_empty() {
        return Ok(Vec::new());
    }

    if binary_records.len() >= PARALLEL_THRESHOLD {
        decode_records_parallel(schema, binary_records)
    } else {
        decode_records_sequential(schema, binary_records)
    }
}

/// Decodes binary Avro records in parallel using serde_avro_fast + rayon.
///
/// Use this when you have a large number of records (500+) and want
/// maximum throughput. For automatic threshold handling, use `decode_records`.
///
/// # Performance
///
/// Achieves up to 5.3M records/second on modern hardware.
pub fn decode_records_parallel<T: DeserializeOwned + Send, B: AsRef<[u8]> + Sync>(
    schema: &Schema,
    binary_records: &[B],
) -> Result<Vec<T>, KineticaError> {
    if binary_records.is_empty() {
        return Ok(Vec::new());
    }

    first_failure_in_order(binary_records.par_iter().map(|data| decode_one(schema, data.as_ref())).collect(), || {
        decode_records_sequential(schema, binary_records)
    })
}

/// Decodes binary Avro records sequentially using serde_avro_fast.
///
/// Use this for small result sets (<500 records) where parallel overhead
/// would outweigh benefits. For automatic threshold handling, use `decode_records`.
///
/// # Performance
///
/// Achieves 2-4M records/second, 3-5x faster than apache_avro.
pub fn decode_records_sequential<T: DeserializeOwned, B: AsRef<[u8]>>(
    schema: &Schema,
    binary_records: &[B],
) -> Result<Vec<T>, KineticaError> {
    if binary_records.is_empty() {
        return Ok(Vec::new());
    }

    binary_records.iter().map(|data| decode_one(schema, data.as_ref())).collect()
}

/// Decodes binary Avro records using a pre-parsed serde_avro_fast schema.
///
/// This is equivalent to `decode_records` but accepts the schema by reference.
/// Useful when decoding multiple batches with the same schema to avoid
/// repeated schema parsing.
///
/// # Example
///
/// ```
/// # use gpudb_api::avro::{decode_with_fast_schema, encode_records, parse_fast_schema};
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct MyRecord { id: i32 }
/// # let type_schema = r#"{"type":"record","name":"MyRecord","fields":[{"name":"id","type":"int"}]}"#;
/// # let response = gpudb_api::RawGetRecordsResponse { type_schema: type_schema.to_string(), ..Default::default() };
/// # let one = parse_fast_schema(type_schema)?;
/// # let batches = vec![encode_records(&one, &[MyRecord { id: 1 }])?, encode_records(&one, &[MyRecord { id: 2 }])?];
/// let schema = parse_fast_schema(&response.type_schema)?;
///
/// // Decode multiple batches without re-parsing schema
/// for batch in batches {
///     let records: Vec<MyRecord> = decode_with_fast_schema(&schema, &batch)?;
/// #   assert_eq!(records.len(), 1);
/// }
/// # Ok::<(), gpudb_api::KineticaError>(())
/// ```
pub fn decode_with_fast_schema<T: DeserializeOwned + Send, B: AsRef<[u8]> + Sync>(
    schema: &Schema,
    binary_records: &[B],
) -> Result<Vec<T>, KineticaError> {
    decode_records(schema, binary_records)
}

/// Decodes a single binary Avro record using a pre-parsed schema.
///
/// Useful for decoding individual records without the overhead of
/// parallel processing setup.
pub fn decode_single<T: DeserializeOwned>(
    schema: &Schema,
    binary_data: &[u8],
) -> Result<T, KineticaError> {
    decode_one(schema, binary_data)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_threshold() {
        assert_eq!(PARALLEL_THRESHOLD, 500);
    }

    // `new_values_maps` values are the union `["string","null"]`, so a map
    // value may be `null`: a `None` value must encode as the null branch
    // (setting the column to NULL) and decode back as `None`.
    #[test]
    fn test_update_records_null_map_value_round_trips() {
        use crate::generated::api::{RawUpdateRecordsRequest, UPDATE_RECORDS_REQUEST_SCHEMA};
        use std::collections::HashMap;

        let request = RawUpdateRecordsRequest {
            table_name: "t".to_string(),
            expressions: vec!["id = 1".to_string()],
            new_values_maps: vec![HashMap::from([
                ("a".to_string(), None),
                ("b".to_string(), Some("x".to_string())),
            ])],
            ..Default::default()
        };
        let schema = &*UPDATE_RECORDS_REQUEST_SCHEMA;
        let mut config = serde_avro_fast::ser::SerializerConfig::new(schema);
        let bytes = serde_avro_fast::to_datum(&request, Vec::new(), &mut config).unwrap();

        let decoded: RawUpdateRecordsRequest =
            serde_avro_fast::from_datum_slice(&bytes, schema).unwrap();
        assert_eq!(decoded.new_values_maps, request.new_values_maps);
        assert_eq!(decoded.new_values_maps[0]["a"], None);
        assert_eq!(decoded.new_values_maps[0]["b"].as_deref(), Some("x"));

        // A lone `None` value goes out as union branch 1 (`null`).
        let request = RawUpdateRecordsRequest {
            table_name: "t".to_string(),
            new_values_maps: vec![HashMap::from([("a".to_string(), None)])],
            ..Default::default()
        };
        let bytes = serde_avro_fast::to_datum(&request, Vec::new(), &mut config).unwrap();
        #[rustfmt::skip]
        let expected = [
            0x02, b't',       // table_name
            0x00,             // expressions: []
            0x02,             // new_values_maps: one element
            0x02, 0x02, b'a', //   map: one entry, key "a"
            0x02,             //   value: union branch 1 (null)
            0x00, 0x00,       //   end of map, end of array
            0x00, 0x00,       // records_to_insert, records_to_insert_str
            0x0c, b'b', b'i', b'n', b'a', b'r', b'y', // record_encoding: "binary", the default
            0x00,             // options: {}
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_empty_records_decode() {
        let schema = parse_schema(
            r#"{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}"#,
        )
        .unwrap();
        let empty: Vec<Vec<u8>> = vec![];

        let result: Result<Vec<serde_json::Value>, _> = decode_records(&schema, &empty);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_empty_records_encode() {
        let schema = parse_fast_schema(
            r#"{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}"#,
        )
        .unwrap();
        let empty: Vec<serde_json::Value> = vec![];

        let result = encode_records(&schema, &empty);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_roundtrip_single() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct TestRecord {
            id: i32,
            name: String,
        }

        let schema_str =
            r#"{"type": "record", "name": "TestRecord", "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}"#;

        let schema = parse_fast_schema(schema_str).unwrap();

        let original = TestRecord {
            id: 42,
            name: "test".to_string(),
        };

        // Encode
        let encoded = encode_single(&schema, &original).unwrap();

        // Decode
        let decoded: TestRecord = decode_records(&schema, &[encoded]).unwrap().pop().unwrap();

        assert_eq!(original, decoded);
    }

    /// One serializer configuration shared by a batch's records, on one thread
    /// or a partition per thread, gives each record the bytes it has encoded
    /// on its own, whatever their sizes.
    #[test]
    fn a_batch_encodes_each_record_as_it_encodes_alone() {
        #[derive(serde::Serialize)]
        struct TestRecord {
            id: i64,
            name: String,
            note: Option<String>,
        }

        let schema = parse_fast_schema(
            r#"{"type": "record", "name": "TestRecord", "fields": [{"name": "id", "type": "long"},
                {"name": "name", "type": "string"}, {"name": "note", "type": ["string", "null"]}]}"#,
        )
        .unwrap();
        let records: Vec<Arc<TestRecord>> = (0..PARALLEL_THRESHOLD as i64 * 3)
            .map(|i| {
                Arc::new(TestRecord {
                    id: i * 1_000_003,
                    name: "x".repeat((i % 200) as usize),
                    note: (i % 3 == 0).then(|| "y".repeat((i % 50) as usize)),
                })
            })
            .collect();
        let alone: Vec<Vec<u8>> = records.iter().map(|r| encode_single(&schema, r.as_ref()).unwrap()).collect();

        assert_eq!(encode_arc_records_sequential(&schema, &records).unwrap(), alone);
        assert_eq!(encode_arc_records_partitioned(&schema, &records).unwrap(), alone);
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct Mixed {
        v: i64,
        s: Option<String>,
    }

    /// Record 1 fails one way and every record after it another; the error is
    /// record 1's however the batch is split, as a sequential pass gives it.
    #[test]
    fn a_failing_batch_reports_its_first_bad_record() {
        let schema =
            parse_schema(r#"{"type":"record","name":"r","fields":[{"name":"v","type":"int"},{"name":"s","type":"string"}]}"#)
                .unwrap();
        let mut records: Vec<Mixed> = (0..PARALLEL_THRESHOLD * 8).map(|_| Mixed { v: 3_000_000_001, s: Some("x".into()) }).collect();
        records[0].v = 1;
        records[1] = Mixed { v: 1, s: None };
        let first = encode_single(&schema, &records[1]).unwrap_err().to_string();
        assert!(first.starts_with("Avro error: Could not encode object: "), "{first}");
        assert_ne!(first, encode_single(&schema, &records[2]).unwrap_err().to_string());

        let arcs: Vec<Arc<Mixed>> = records.iter().map(|r| Arc::new(Mixed { v: r.v, s: r.s.clone() })).collect();
        for _ in 0..20 {
            assert_eq!(encode_records(&schema, &records).unwrap_err().to_string(), first);
            assert_eq!(encode_records_parallel(&schema, &records).unwrap_err().to_string(), first);
            assert_eq!(encode_records_partitioned(&schema, &records).unwrap_err().to_string(), first);
            assert_eq!(encode_arc_records_parallel(&schema, &arcs).unwrap_err().to_string(), first);
            assert_eq!(encode_arc_records_partitioned(&schema, &arcs).unwrap_err().to_string(), first);
        }

        let union = parse_schema(r#"{"type":"record","name":"r","fields":[{"name":"v","type":["null","int"]}]}"#).unwrap();
        let mut data: Vec<Vec<u8>> = vec![vec![12]; PARALLEL_THRESHOLD * 8];
        data[0] = vec![2, 2];
        data[1] = Vec::new();
        let first = decode_single::<Wide>(&union, &data[1]).unwrap_err().to_string();
        assert!(first.starts_with("Avro error: Could not decode object: "), "{first}");
        assert_ne!(first, decode_single::<Wide>(&union, &data[2]).unwrap_err().to_string());
        for _ in 0..20 {
            assert_eq!(decode_records::<Wide, _>(&union, &data).unwrap_err().to_string(), first);
            assert_eq!(decode_records_parallel::<Wide, _>(&union, &data).unwrap_err().to_string(), first);
        }
    }

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct Wide {
        v: Option<i32>,
    }
}

