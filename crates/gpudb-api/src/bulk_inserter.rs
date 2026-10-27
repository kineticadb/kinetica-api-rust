//! Shard-aware high-performance bulk insert using serde_avro_fast for encoding.
//!
//! `BulkInserter` routes records to the correct worker based on shard key
//! computation, enabling true multi-head ingest where each record is sent
//! directly to the worker that owns its shard.
//!
//! # How Shard-Aware Routing Works
//!
//! 1. On initialization, fetch the routing table via `admin_show_shards`
//! 2. Fetch the table schema via `show_table` to identify shard key columns
//! 3. Build a `RecordKeyBuilder` to compute routing keys
//! 4. For each record, compute its shard key hash using all shard key columns
//! 5. Use the routing table to determine which worker owns that shard
//! 6. Route the record directly to that worker
//!
//! # Performance
//!
//! - Shard-aware routing eliminates server-side record redistribution
//! - Each worker receives only records that belong to its shards
//! - Faster Avro encoding with `serde_avro_fast` (~6-7x faster than apache-avro)
//! - Parallel encoding with rayon for maximum throughput
//!
//! # Insert Methods
//!
//! The `BulkInserter` provides four insertion methods:
//!
//! | Method | Use Case | Queue Throughput |
//! |--------|----------|------------------|
//! | [`insert`](BulkInserter::insert) | Single record streaming | ~3-7M rec/s |
//! | [`insert_with_backpressure`](BulkInserter::insert_with_backpressure) | Memory-bounded streaming | ~3-7M rec/s |
//! | [`insert_batch`](BulkInserter::insert_batch) | Bulk loading | ~20-30M rec/s |
//! | [`insert_batch_arc`](BulkInserter::insert_batch_arc) | Pre-wrapped records | ~30-35M rec/s |
//!
//! The batch methods use parallel shard key computation and reduce lock contention
//! by grouping records before acquiring locks. This provides up to 8x faster
//! client-side queue throughput for large batches.
//!
//! # JSON Records
//!
//! [`BulkInserter::new_json`] and [`BulkInserter::with_json_options`] make a
//! `BulkInserter<String>` taking JSON objects as text: each record goes to the
//! rank of a random shard, and each batch is sent as one JSON array to
//! `/insert/records/json`.
//!
//! # Further Features
//!
//! ## Backpressure Control
//!
//! Use `insert_with_backpressure()` for memory-bounded operation:
//!
//! ```no_run
//! # use gpudb_api::{BulkInserter, BulkInserterOptions, Kinetica};
//! # use std::sync::Arc;
//! # async fn example<T>(kinetica: Arc<Kinetica>, records: Vec<T>) -> Result<(), gpudb_api::KineticaError>
//! # where T: serde::Serialize + gpudb_api::KineticaRecord + gpudb_api::ShardKeyExtractor + Send + Sync + 'static + Default {
//! let mut options = BulkInserterOptions::new();
//! options.set_max_in_flight_batches(50);  // Limit concurrent batches
//!
//! let inserter = BulkInserter::with_options(kinetica, "my_table".into(), 50000, 8, Some(options)).await?;
//!
//! for record in records {
//!     inserter.insert_with_backpressure(Arc::new(record)).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Timed Flush
//!
//! Configure automatic periodic flush:
//!
//! ```no_run
//! # use gpudb_api::{BulkInserter, BulkInserterOptions, FlushOptions, Kinetica};
//! # use std::sync::Arc;
//! # async fn example<T>(kinetica: Arc<Kinetica>) -> Result<(), gpudb_api::KineticaError>
//! # where T: serde::Serialize + gpudb_api::KineticaRecord + gpudb_api::ShardKeyExtractor + Send + Sync + 'static + Default {
//! let mut options = BulkInserterOptions::new();
//! options.set_flush_options(FlushOptions::new(false, 30));  // Flush every 30 seconds
//!
//! let inserter = BulkInserter::with_options(kinetica, "my_table".into(), 50000, 8, Some(options)).await?;
//! // Timed flush runs automatically in background
//! # let _: &Arc<BulkInserter<T>> = &inserter;
//! # Ok(())
//! # }
//! ```
//!
//! ## Error Queue
//!
//! Non-blocking async error collection:
//!
//! ```no_run
//! # fn example<T>(inserter: &gpudb_api::BulkInserter<T>) where T: serde::Serialize + gpudb_api::KineticaRecord + gpudb_api::ShardKeyExtractor + Send + Sync + 'static {
//! // After insertion, check for errors
//! let errors = inserter.drain_errors();
//! for err in errors {
//!     eprintln!("Error: {} at {}", err.message, err.worker_url);
//! }
//! # }
//! ```
//!
//! ## Batch Callbacks
//!
//! Real-time monitoring via `BatchInsertionListener`:
//!
//! ```
//! use gpudb_api::{BatchInsertionListener, BatchInsertionResult};
//!
//! struct MyListener;
//! impl BatchInsertionListener for MyListener {
//!     fn on_batch_inserted(&self, result: &BatchInsertionResult) {
//!         println!("Batch: {} records, success={}", result.batch_size, result.success);
//!     }
//! }
//! ```
//!
//! # Example
//!
//! ```no_run
//! use gpudb_api::{Kinetica, BulkInserter, BulkInserterOptions, KineticaRecord, ShardKeyExtractor, ShardKeyValue, ShardKeyValues};
//! use smallvec::smallvec;
//! use std::sync::Arc;
//!
//! #[derive(serde::Serialize, Default)]
//! struct MyRecord { id: i32, name: String }
//!
//! impl ShardKeyExtractor for MyRecord {
//!     fn shard_key_values(&self) -> ShardKeyValues<'_> {
//!         smallvec![("id", ShardKeyValue::Int(self.id))]
//!     }
//! }
//!
//! # impl KineticaRecord for MyRecord {
//! #     fn avro_schema(&self) -> &'static str { "" }
//! #     fn type_properties() -> std::collections::HashMap<String, Vec<String>> { unimplemented!() }
//! # }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], Default::default()).await?);
//! // Simple usage with defaults
//! let inserter = BulkInserter::new(
//!     kinetica.clone(),
//!     "my_schema.my_table".into(),
//!     50000,  // batch size
//!     16      // num workers
//! ).await?;
//!
//! // Or with options
//! let mut options = BulkInserterOptions::default();
//! options.set_update_on_existing_pk(true);
//! let inserter_with_options = BulkInserter::<MyRecord>::with_options(
//!     kinetica,
//!     "my_schema.my_table".into(),
//!     50000,
//!     16,
//!     Some(options),
//! ).await?;
//!
//! // Single record insertion (good for streaming)
//! for i in 0..1_000_000 {
//!     let record = Arc::new(MyRecord { id: i, name: format!("name_{}", i) });
//!     inserter.insert(record)?;
//! }
//!
//! // Or batch insertion (faster for bulk loading)
//! let records: Vec<MyRecord> = (0..100_000)
//!     .map(|i| MyRecord { id: i, name: format!("name_{}", i) })
//!     .collect();
//! inserter.insert_batch(records)?;
//!
//! inserter.flush().await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    Kinetica, KineticaError, RawInsertRecordsRequest, InsertRecordsResponse, KineticaRecord,
    AdminShowShardsRequest, ShowTableRequest, HasPermissionRequest,
    record_key_builder::{RecordKeyBuilder, KineticaType},
    worker_list::WorkerList,
    bulk_inserter_common::{
        FlushJob, WorkerQueue, WorkerUrlCache,
        INSERT_RECORDS_RESPONSE_SCHEMA,
        build_kinetica_type, compute_random_worker_index, current_thread_stripe, encode_insert_records_body,
        EncodeFailure,
        compute_worker_index_from_values, map_ranks_to_workers,
        pooled_vec, return_to_pool,
    },
};
use parking_lot::RwLock;
use std::sync::{Arc, atomic::{AtomicI64, AtomicUsize, AtomicBool, Ordering}};
use std::str::FromStr;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::Mutex;
use serde::Serialize;
use serde_avro_fast::Schema as FastSchema;
use rayon::prelude::*;
use log::{debug, error, warn, trace};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;
use arc_swap::ArcSwap;
use crate::gpudb::MultiHeadSnapshot;
use crate::text_format::equals_ignore_case;
use crate::json_records::{json_kind, json_text, type_mismatch, JsonOptions};
use crate::json_utils;

/// Milliseconds since the epoch, for shard update times.
fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Whether two multi-head snapshots are the same one, by identity.
pub(crate) fn same_snapshot(a: &Option<Arc<MultiHeadSnapshot>>, b: &Option<Arc<MultiHeadSnapshot>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}

// Re-export shard key types for public API
pub use crate::bulk_inserter_common::{ShardKeyExtractor, ShardKeyValue, ShardKeyValues};

// ============================================================================
/// The first server version that takes a batch's `request_schema_str`.
const REQUEST_SCHEMA_VERSION: crate::KineticaVersion = crate::KineticaVersion::new(7, 2, 3, 18);

/// The base definition of the record type `schema_str` describes: each
/// column's name and Avro type, and `nullable` for one that is, as
/// `[["id","int"],["name","string","nullable"]]`.
fn base_definition(schema_str: &str) -> Result<String, KineticaError> {
    let schema: serde_json::Value = serde_json::from_str(schema_str)
        .map_err(|e| KineticaError::Avro(format!("serde_avro_fast schema error: {e}")))?;
    let fields = schema.get("fields").and_then(|f| f.as_array()).map(Vec::as_slice).unwrap_or_default();
    let mut definition = String::from("[");
    for field in fields {
        if definition.len() > 1 {
            definition.push(',');
        }
        let name = field.get("name").and_then(|n| n.as_str()).unwrap_or_default();
        let (avro_type, nullable) = match field.get("type") {
            Some(serde_json::Value::Array(members)) => (
                members.iter().filter_map(|m| m.as_str()).find(|m| *m != "null").unwrap_or_default(),
                members.iter().any(|m| m.as_str() == Some("null")),
            ),
            Some(member) => (member.as_str().unwrap_or_default(), false),
            None => ("", false),
        };
        definition.push_str(&format!("[\"{name}\",\"{avro_type}\""));
        if nullable {
            definition.push_str(",\"nullable\"");
        }
        definition.push(']');
    }
    definition.push(']');
    Ok(definition)
}

/// How an inserter sends its records.
enum RecordFormat<T> {
    /// Avro-encoded with the record type's schema, to `/insert/records`.
    Avro(Arc<FastSchema>),
    /// As the JSON text the function gives for a record, to
    /// `/insert/records/json`.
    Json(for<'a> fn(&'a T) -> &'a str),
}

/// What a new inserter is to send its records as.
enum NewFormat<'a, T> {
    /// Avro, with the record type's schema.
    Avro(&'a str),
    /// JSON, with the record's text and the JSON options.
    Json(for<'b> fn(&'b T) -> &'b str, JsonOptions),
}

/// A batch, ready to send.
enum BatchRequest {
    /// The `/insert/records` request's body, Snappy-compressed if `snappy`.
    /// `Bytes`, so that a retry sends it again without copying it. The
    /// records that did not encode were sent empty, and are reported with the
    /// server's per-record errors once it has answered.
    Avro { body: hyper::body::Bytes, snappy: bool, encode_failures: Vec<EncodeFailure> },
    Json(String),
}

impl BatchRequest {
    /// The records of the batch that did not encode.
    fn encode_failures(&self) -> &[EncodeFailure] {
        match self {
            BatchRequest::Avro { encode_failures, .. } => encode_failures,
            BatchRequest::Json(_) => &[],
        }
    }
}

/// What the server reported for a batch it took.
struct BatchOutcome {
    inserted: i64,
    updated: i64,
    /// The `info` of an Avro insert; a JSON insert's is not read.
    info: Option<HashMap<String, String>>,
}

/// Reads the answer to a JSON batch: a `status` of `OK`, in any case, is
/// success, with the counts in its `data`; any other status fails the batch
/// with the server's `message`.
fn json_insert_counts(
    response: &serde_json::Map<String, serde_json::Value>,
    record_count: usize,
) -> Result<(i64, i64), KineticaError> {
    fn present(value: Option<&serde_json::Value>) -> Option<&serde_json::Value> {
        value.filter(|v| !v.is_null())
    }
    let missing = |path: &str| KineticaError::Api(format!("The response has no `{path}`"));
    let Some(status) = present(response.get("status")) else {
        return Err(missing("status"));
    };
    if !equals_ignore_case(&json_text(status), "OK") {
        let message = present(response.get("message")).map_or_else(|| "(no message)".to_string(), json_text);
        return Err(KineticaError::Api(format!(
            "Error inserting <{record_count}> JSON objects: {message}"
        )));
    }
    let data = match present(response.get("data")) {
        Some(serde_json::Value::Object(data)) => data,
        Some(other) => return Err(KineticaError::Api(type_mismatch("data", json_kind(other), "object"))),
        None => return Err(missing("data")),
    };
    let count = |key: &str| {
        let path = format!("data.{key}");
        match present(data.get(key)) {
            Some(value @ serde_json::Value::Number(n)) => match n.as_i64().filter(|v| i32::try_from(*v).is_ok()) {
                Some(count) => Ok(count),
                None => Err(KineticaError::Api(type_mismatch(&path, json_kind(value), "i32"))),
            },
            Some(other) => Err(KineticaError::Api(type_mismatch(&path, json_kind(other), "i32"))),
            None => Err(missing(&path)),
        }
    };
    Ok((count("count_inserted")?, count("count_updated")?))
}

// InsertRecordsRequest Options Constants
// ============================================================================

/// Constants for InsertRecordsRequest options.
pub mod insert_records_options {
    /// Specifies the record collision policy for inserting into a table with a primary key.
    /// If set to `true`, any existing table record with primary key values that match those
    /// of a record being inserted will be replaced by that new record.
    /// If set to `false`, any existing table record with primary key values that match those
    /// of a record being inserted will remain unchanged, while the new record will be rejected
    /// and the error handled as determined by `IGNORE_EXISTING_PK`, `ALLOW_PARTIAL_BATCH`,
    /// and `RETURN_INDIVIDUAL_ERRORS`.
    pub const UPDATE_ON_EXISTING_PK: &str = "update_on_existing_pk";

    /// Specifies the record collision error-suppression policy for inserting into a table
    /// with a primary key, only used when not in upsert mode (`UPDATE_ON_EXISTING_PK` is `false`).
    /// If set to `true`, any record being inserted that is rejected for having primary key values
    /// that match those of an existing table record will be ignored with no error generated.
    /// If `false`, the rejection of any record for having primary key values matching an existing
    /// record will result in an error.
    pub const IGNORE_EXISTING_PK: &str = "ignore_existing_pk";

    /// If set to `true`, success will always be returned, and any errors found will be included
    /// in the info map in the response.
    pub const RETURN_INDIVIDUAL_ERRORS: &str = "return_individual_errors";

    /// If set to `true`, the insertion of records will be allowed to complete even if some
    /// records have errors. Otherwise, the entire batch will fail if any records have errors.
    pub const ALLOW_PARTIAL_BATCH: &str = "allow_partial_batch";

    /// If set to `true`, records being inserted will be truncated to fit into the target table.
    pub const TRUNCATE_STRINGS: &str = "truncate_strings";

    /// Value constant for boolean true in options
    pub const TRUE: &str = "true";

    /// Value constant for boolean false in options
    pub const FALSE: &str = "false";
}

// ============================================================================
// FlushOptions
// ============================================================================

/// Options for controlling automatic flush behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushOptions {
    /// Time interval in seconds for automatic periodic flush.
    /// Set to -1 (NO_PERIODIC_FLUSH) to disable automatic flush.
    pub flush_interval: i32,

    /// What the timed flush sends: if true (the default) only queues that
    /// have reached the batch size; if false, every buffered record
    /// regardless of queue fullness.
    ///
    /// Only the timed flush reads this; [`BulkInserter::flush`] and
    /// [`BulkInserter::close`] always send everything. Because the insert
    /// paths already send a queue the moment it fills, a timed flush with
    /// `true` usually finds nothing to send. Use `false` if the timer should
    /// push partial batches out.
    pub flush_when_full: bool,
}

impl FlushOptions {
    /// Constant indicating no periodic flush (-1).
    pub const NO_PERIODIC_FLUSH: i32 = -1;

    /// Default value for flush_when_full.
    pub const FLUSH_WHEN_FULL: bool = true;

    /// Creates FlushOptions with specified values.
    pub fn new(flush_when_full: bool, flush_interval: i32) -> Self {
        Self {
            flush_interval: if flush_interval < 0 { Self::NO_PERIODIC_FLUSH } else { flush_interval },
            flush_when_full,
        }
    }

    /// Whether the timed flush sends only queues that have reached the batch
    /// size.
    pub fn is_flush_when_full(&self) -> bool {
        self.flush_when_full
    }

    /// Sets whether the timed flush sends only queues that have reached the
    /// batch size.
    pub fn set_flush_when_full(&mut self, flush_when_full: bool) {
        self.flush_when_full = flush_when_full;
    }

    /// The timed flush interval, in seconds.
    pub fn flush_interval(&self) -> i32 {
        self.flush_interval
    }

    /// Sets the timed flush interval, in seconds; the value is kept as given.
    pub fn set_flush_interval(&mut self, flush_interval: i32) {
        self.flush_interval = flush_interval;
    }
}

impl Default for FlushOptions {
    /// No periodic flush, and a flush sends only full queues.
    fn default() -> Self {
        Self {
            flush_interval: Self::NO_PERIODIC_FLUSH,
            flush_when_full: Self::FLUSH_WHEN_FULL,
        }
    }
}

// ============================================================================
// BulkInserterOptions
// ============================================================================

/// Options for configuring the BulkInserter.
///
/// The insert options map is passed directly to `insert_records` calls and supports
/// the same options as the `/insert/records` endpoint.
#[derive(Clone)]
pub struct BulkInserterOptions {
    /// Optional parameters to pass to GPUdb while inserting.
    /// These are passed directly to the `insert_records` API call.
    /// See `insert_records_options` module for available option keys.
    pub insert_options: Option<HashMap<String, String>>,

    /// Options for controlling automatic/timed flush behavior.
    pub flush_options: FlushOptions,

    /// Maximum number of retries for failed insertions.
    /// Default is 3.
    ///
    /// This counts retries after the first attempt (0 means one attempt),
    /// only connection failures are retried, and the count starts again on
    /// another cluster after an HA failover.
    pub max_retries: usize,

    /// Optional worker list for multi-head ingest.
    /// If None, the worker list will be automatically configured from the server.
    /// Pass an empty WorkerList to disable multi-head ingest.
    pub worker_list: Option<WorkerList>,

    // ========================================================================
    // Backpressure and Callbacks
    // ========================================================================

    /// Maximum number of concurrent in-flight batch requests.
    /// Limits memory usage by blocking when this limit is reached.
    /// Default is 100.
    pub max_in_flight_batches: usize,

    /// Optional batch insertion listener for callbacks.
    /// Called when each batch completes (success or failure).
    pub batch_listener: Option<Arc<dyn BatchInsertionListener>>,
}

/// Default maximum in-flight batches
const DEFAULT_MAX_IN_FLIGHT_BATCHES: usize = 100;

impl std::fmt::Debug for BulkInserterOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkInserterOptions")
            .field("insert_options", &self.insert_options)
            .field("flush_options", &self.flush_options)
            .field("max_retries", &self.max_retries)
            .field("worker_list", &self.worker_list)
            .field("max_in_flight_batches", &self.max_in_flight_batches)
            .field("batch_listener", &self.batch_listener.as_ref().map(|_| "<BatchInsertionListener>"))
            .finish()
    }
}

impl Default for BulkInserterOptions {
    fn default() -> Self {
        Self {
            insert_options: None,
            flush_options: FlushOptions::default(),
            max_retries: DEFAULT_INSERTION_RETRY_COUNT,
            worker_list: None,
            max_in_flight_batches: DEFAULT_MAX_IN_FLIGHT_BATCHES,
            batch_listener: None,
        }
    }
}

impl BulkInserterOptions {
    /// Creates a new BulkInserterOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to update existing records on primary key collision.
    pub fn set_update_on_existing_pk(&mut self, value: bool) -> &mut Self {
        self.ensure_insert_options();
        if let Some(ref mut opts) = self.insert_options {
            opts.insert(
                insert_records_options::UPDATE_ON_EXISTING_PK.to_string(),
                if value { insert_records_options::TRUE } else { insert_records_options::FALSE }.to_string()
            );
        }
        self
    }

    /// Sets whether to ignore existing primary key collisions (suppress errors).
    pub fn set_ignore_existing_pk(&mut self, value: bool) -> &mut Self {
        self.ensure_insert_options();
        if let Some(ref mut opts) = self.insert_options {
            opts.insert(
                insert_records_options::IGNORE_EXISTING_PK.to_string(),
                if value { insert_records_options::TRUE } else { insert_records_options::FALSE }.to_string()
            );
        }
        self
    }

    /// Sets whether to return individual errors for each record.
    pub fn set_return_individual_errors(&mut self, value: bool) -> &mut Self {
        self.ensure_insert_options();
        if let Some(ref mut opts) = self.insert_options {
            opts.insert(
                insert_records_options::RETURN_INDIVIDUAL_ERRORS.to_string(),
                if value { insert_records_options::TRUE } else { insert_records_options::FALSE }.to_string()
            );
        }
        self
    }

    /// Sets whether to allow partial batch insertions.
    pub fn set_allow_partial_batch(&mut self, value: bool) -> &mut Self {
        self.ensure_insert_options();
        if let Some(ref mut opts) = self.insert_options {
            opts.insert(
                insert_records_options::ALLOW_PARTIAL_BATCH.to_string(),
                if value { insert_records_options::TRUE } else { insert_records_options::FALSE }.to_string()
            );
        }
        self
    }

    /// Sets whether to truncate strings to fit the column width.
    pub fn set_truncate_strings(&mut self, value: bool) -> &mut Self {
        self.ensure_insert_options();
        if let Some(ref mut opts) = self.insert_options {
            opts.insert(
                insert_records_options::TRUNCATE_STRINGS.to_string(),
                if value { insert_records_options::TRUE } else { insert_records_options::FALSE }.to_string()
            );
        }
        self
    }

    /// Sets the flush options for automatic/timed flush.
    pub fn set_flush_options(&mut self, flush_options: FlushOptions) -> &mut Self {
        self.flush_options = flush_options;
        self
    }

    /// Sets the maximum number of retries for failed insertions.
    pub fn set_max_retries(&mut self, max_retries: usize) -> &mut Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the worker list for multi-head ingest.
    pub fn set_worker_list(&mut self, worker_list: Option<WorkerList>) -> &mut Self {
        self.worker_list = worker_list;
        self
    }

    /// Adds a custom option to the insert options map.
    pub fn set_option(&mut self, key: &str, value: &str) -> &mut Self {
        self.ensure_insert_options();
        if let Some(ref mut opts) = self.insert_options {
            opts.insert(key.to_string(), value.to_string());
        }
        self
    }

    /// Sets the maximum number of concurrent in-flight batch requests.
    /// This limits memory usage by blocking when the limit is reached.
    pub fn set_max_in_flight_batches(&mut self, max_batches: usize) -> &mut Self {
        self.max_in_flight_batches = max_batches.max(1);
        self
    }

    /// Sets the batch insertion listener for callbacks.
    /// The listener will be called when each batch completes (success or failure).
    ///
    /// # Example
    ///
    /// ```
    /// use gpudb_api::{BatchInsertionListener, BatchInsertionResult, BulkInserterOptions};
    /// use std::sync::Arc;
    ///
    /// struct MyListener;
    /// impl BatchInsertionListener for MyListener {
    ///     fn on_batch_inserted(&self, result: &BatchInsertionResult) {
    ///         println!("Batch completed: {} records", result.batch_size);
    ///     }
    /// }
    ///
    /// let mut options = BulkInserterOptions::new();
    /// options.set_batch_listener(Arc::new(MyListener));
    /// ```
    pub fn set_batch_listener(&mut self, listener: Arc<dyn BatchInsertionListener>) -> &mut Self {
        self.batch_listener = Some(listener);
        self
    }

    fn ensure_insert_options(&mut self) {
        if self.insert_options.is_none() {
            self.insert_options = Some(HashMap::new());
        }
    }

    /// Whether the inserter emulates "stop on error".
    ///
    /// It needs `return_individual_errors` set to `true`, so that the server
    /// reports per-record errors. Then, if `error_handling` is given, it must be
    /// `abort`; otherwise `allow_partial_batch` must not be `true`. The `true`
    /// values are matched exactly, and `abort` ignoring case.
    fn requires_individual_error_handling(&self) -> bool {
        let Some(ref opts) = self.insert_options else {
            return false;
        };
        let is_true = |key: &str| opts.get(key).is_some_and(|v| v == insert_records_options::TRUE);

        if !is_true(insert_records_options::RETURN_INDIVIDUAL_ERRORS) {
            return false;
        }
        match opts.get("error_handling").filter(|v| !v.is_empty()) {
            Some(error_handling) => error_handling.eq_ignore_ascii_case("abort"),
            None => !is_true(insert_records_options::ALLOW_PARTIAL_BATCH),
        }
    }
}

/// Default number of times insertions will be re-attempted.
const DEFAULT_INSERTION_RETRY_COUNT: usize = 3;


// ============================================================================
// Backpressure Metrics
// ============================================================================

/// Metrics for monitoring backpressure behavior.
///
/// Use `BulkInserter::backpressure_metrics()` to retrieve current metrics.
#[derive(Debug, Clone)]
pub struct BackpressureMetrics {
    /// Maximum number of in-flight batches allowed
    pub max_in_flight_batches: usize,

    /// Number of permits currently available (batches that can be sent without waiting)
    pub available_permits: usize,

    /// Number of batches currently in-flight (being processed by workers)
    pub in_flight_batches: usize,

    /// Number of batches pending in the queue (waiting for workers)
    pub pending_batches: i64,

    /// Utilization percentage (0-100): how much of the in-flight capacity is used
    pub utilization_pct: f64,
}

// ============================================================================
// Batch Insertion Result
// ============================================================================

/// Result of a batch insertion attempt.
/// This is passed to the batch listener callback after each batch completes.
#[derive(Debug, Clone)]
pub struct BatchInsertionResult {
    /// Worker URL the batch was sent to
    pub worker_url: String,
    /// Number of records in the batch
    pub batch_size: usize,
    /// Number of records successfully inserted
    pub count_inserted: i64,
    /// Number of records successfully updated (upserts)
    pub count_updated: i64,
    /// Whether the insertion succeeded
    pub success: bool,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Time taken to build the request (milliseconds): encoding the records,
    /// and compressing them if Snappy applies
    pub encode_time_ms: f64,
    /// Time taken for network transfer (milliseconds)
    pub network_time_ms: f64,
    /// Total time for the batch (milliseconds)
    pub total_time_ms: f64,
    /// Retry attempt number (0 = first attempt)
    pub attempt: usize,
}

/// Callback trait for batch insertion events.
/// Implement this trait to receive notifications when batches complete.
pub trait BatchInsertionListener: Send + Sync {
    /// Called when a batch insertion completes (success or failure).
    fn on_batch_inserted(&self, result: &BatchInsertionResult);
}

/// Simple function-based listener wrapper
pub struct FnBatchListener<F>(pub F);

impl<F: Fn(&BatchInsertionResult) + Send + Sync> BatchInsertionListener for FnBatchListener<F> {
    fn on_batch_inserted(&self, result: &BatchInsertionResult) {
        (self.0)(result)
    }
}

// ============================================================================
// Insert Exception
// ============================================================================

/// An error that occurred during batch insertion.
/// These are collected in the error queue for async error handling.
pub struct InsertError<T> {
    /// Worker URL where the error occurred
    pub worker_url: String,
    /// The head node URL in use when the error occurred. `None` only where
    /// the connection had no URL.
    pub url: Option<url::Url>,
    /// Number of records in the failed batch
    pub record_count: usize,
    /// The records that were not inserted, for a caller to retry or report:
    /// a failed batch's records, or the one record a per-record
    /// error names. Empty where the server named a record the batch does not
    /// hold, and where the records were lost with an encoding task that
    /// panicked; `record_count` still counts them.
    pub records: Vec<Arc<T>>,
    /// Error message: the message of the error underneath, without its kind
    /// (as `KineticaError::message` gives it)
    pub message: String,
    /// Timestamp when the error occurred
    pub timestamp: std::time::Instant,
}

impl<T> Clone for InsertError<T> {
    fn clone(&self) -> Self {
        Self {
            worker_url: self.worker_url.clone(),
            url: self.url.clone(),
            record_count: self.record_count,
            records: self.records.clone(),
            message: self.message.clone(),
            timestamp: self.timestamp,
        }
    }
}

/// The records are summarized by their number, so no `Debug` is needed of
/// `T`.
impl<T> std::fmt::Debug for InsertError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertError")
            .field("worker_url", &self.worker_url)
            .field("record_count", &self.record_count)
            .field("records", &format_args!("[{} record(s)]", self.records.len()))
            .field("message", &self.message)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl<T> std::fmt::Display for InsertError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InsertError at {}: {} ({} records)",
               self.worker_url, self.message, self.record_count)
    }
}

impl<T> std::error::Error for InsertError<T> {}

/// Records the inserter did not queue, and why, for the caller to retry or
/// report.
///
/// For [`BulkInserter::insert`], the record. For
/// [`BulkInserter::insert_batch`]: the records before the first that cannot
/// be queued are queued, and that one and every one after it come back here;
/// a batch whose JSON does not validate comes back whole. Where the inserter
/// is closed, a full batch it could not send comes back too.
///
/// Converts into its [`KineticaError`], so `?` works where one is returned,
/// and displays as it does.
pub struct QueueError<T> {
    /// Why the records were not queued
    pub error: KineticaError,
    /// The records that were not queued, in the order they were given
    pub records: Vec<Arc<T>>,
}

impl<T> QueueError<T> {
    fn new(error: KineticaError, records: Vec<Arc<T>>) -> Self {
        Self { error, records }
    }
}

/// The records are summarized by their number, so no `Debug` is needed of
/// `T`.
impl<T> std::fmt::Debug for QueueError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueError")
            .field("error", &self.error)
            .field("records", &format_args!("[{} record(s)]", self.records.len()))
            .finish()
    }
}

impl<T> std::fmt::Display for QueueError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<T> std::error::Error for QueueError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl<T> From<QueueError<T>> for KineticaError {
    fn from(error: QueueError<T>) -> Self {
        error.error
    }
}

// ============================================================================
// RoutingState - Atomically swappable routing configuration for HA failover
// ============================================================================

/// Holds all routing-related state that needs to be atomically updated during HA failover.
///
/// This struct is wrapped in `ArcSwap` to allow lock-free atomic updates during failover
/// while concurrent insert operations continue using the current routing state.
///
/// # Thread Safety
///
/// Using `ArcSwap` provides:
/// - Lock-free reads (no contention during normal insert operations)
/// - Atomic updates (routing state is updated atomically during failover)
/// - No torn reads (readers always see a consistent state)
/// - Memory safety (old state is kept alive until all readers are done)
pub(crate) struct RoutingState<T> {
    /// Whether records go to the head node rather than to worker ranks.
    ///
    /// Derived, never supplied; see [`RoutingState::use_head_node_for`].
    pub use_head_node: bool,

    /// Whether multi-head I/O is available for this inserter.
    pub multi_head_enabled: bool,

    /// Whether the table is replicated, kept so the rule can be re-applied.
    pub table_replicated: bool,

    /// The queue per destination.
    ///
    /// When [`Self::use_head_node`] is set this holds exactly one queue, for the
    /// head node. Otherwise entry `i` is the queue for the worker at index `i`
    /// of [`Self::worker_url_cache`], and [`Self::rank_to_worker_array`] is what
    /// maps a rank number to one of these slots.
    ///
    /// Each queue is behind an `Arc` so that a new routing state carrying only a
    /// new shard mapping shares the *same* queues rather than copying them —
    /// the records already buffered must not be split across two states.
    pub queues: Vec<Arc<WorkerQueue<T>>>,

    /// Routing table from admin_show_shards (maps hash index to worker rank)
    pub routing_table: Vec<i32>,

    /// Rank-to-worker array for fast O(1) lookup (index = rank, value = worker_idx, MAX = invalid)
    pub rank_to_worker_array: Vec<Option<usize>>,

    /// Pre-computed URLs for each worker (avoids per-batch string formatting)
    pub worker_url_cache: Vec<WorkerUrlCache>,

    /// Version number for detecting updates (incremented on each failover)
    pub version: u64,
}

impl<T> RoutingState<T> {
    /// The one statement of when routing goes to the head node: multi-head is
    /// unusable, or the table is replicated so every rank already holds it.
    ///
    /// Exposed because callers need the answer *before* a [`RoutingState`]
    /// exists — it decides whether to build per-rank queues at all. Having them
    /// call this rather than restate the expression is what keeps a second
    /// version of the rule from appearing.
    pub fn use_head_node_for(multi_head_enabled: bool, table_replicated: bool) -> bool {
        !multi_head_enabled || table_replicated
    }

    /// Creates a new RoutingState, deriving `use_head_node` from the rule above.
    ///
    /// # Panics
    ///
    /// Panics if the queues cannot serve the routing mode, which would mean the
    /// caller built an inconsistent state: the insert path resolves head-node
    /// routing to `queues[0]`, so that has to be the only queue there is.
    pub fn new(
        multi_head_enabled: bool,
        table_replicated: bool,
        queues: Vec<Arc<WorkerQueue<T>>>,
        routing_table: Vec<i32>,
        rank_to_worker_array: Vec<Option<usize>>,
        worker_url_cache: Vec<WorkerUrlCache>,
        version: u64,
    ) -> Self {
        let use_head_node = Self::use_head_node_for(multi_head_enabled, table_replicated);

        assert!(!queues.is_empty(), "Routing needs at least one queue");
        assert!(
            !use_head_node || queues.len() == 1,
            "Head-node routing needs exactly one queue, got {}",
            queues.len()
        );

        Self {
            use_head_node,
            multi_head_enabled,
            table_replicated,
            queues,
            routing_table,
            rank_to_worker_array,
            worker_url_cache,
            version,
        }
    }

    /// Returns a copy of this routing state carrying a different shard mapping.
    ///
    /// The queues are **shared**, not rebuilt: a new shard mapping describes the
    /// same set of destinations, so the records already buffered stay exactly
    /// where they are — cloning the `Vec` clones the `Arc`s, not the queues.
    pub fn with_routing_table(&self, routing_table: Vec<i32>) -> Self {
        Self {
            use_head_node: self.use_head_node,
            multi_head_enabled: self.multi_head_enabled,
            table_replicated: self.table_replicated,
            queues: self.queues.clone(),
            routing_table,
            rank_to_worker_array: self.rank_to_worker_array.clone(),
            worker_url_cache: self.worker_url_cache.clone(),
            version: self.version + 1,
        }
    }
}

/// How long each stage of one batch took.
///
/// Grouped rather than passed as three positional `Duration`s: transposing two
/// of those compiles cleanly and silently misreports the timing to the caller's
/// listener.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BatchTimings {
    pub encode: std::time::Duration,
    pub network: std::time::Duration,
    pub total: std::time::Duration,
}

/// Splits an insert response's `info` map into its per-record errors
/// (`error_<index>`, in record order) and its warnings (`warning_*`). Keys are
/// matched ignoring case.
///
/// The records that did not encode are among the errors too: one error per
/// record index, the client's message replacing the server's for a record the
/// client sent empty.
fn record_messages_from_info(
    info: &HashMap<String, String>,
    encode_failures: &[EncodeFailure],
) -> (Vec<(Option<usize>, String)>, Vec<String>) {
    let with_prefix = |prefix: &str| -> Vec<(&String, &String)> {
        info.iter().filter(|(k, _)| k.to_lowercase().starts_with(prefix)).collect()
    };

    // The errors, keyed and so ordered by record index
    let mut by_index: std::collections::BTreeMap<usize, &str> = std::collections::BTreeMap::new();
    let mut unindexed = Vec::new();
    for (key, message) in with_prefix("error_") {
        match key.get(6..).and_then(|i| i.parse::<usize>().ok()) {
            Some(index) => {
                by_index.insert(index, message);
            }
            None => unindexed.push((None, message.clone())),
        }
    }
    for (index, message) in encode_failures {
        by_index.insert(*index, message);
    }
    let mut errors: Vec<(Option<usize>, String)> =
        by_index.into_iter().map(|(index, message)| (Some(index), message.to_string())).collect();
    errors.extend(unindexed);

    let mut warnings = with_prefix("warning_");
    warnings.sort_by(|a, b| a.0.cmp(b.0));

    (errors, warnings.into_iter().map(|(_, v)| v.clone()).collect())
}

/// Whether an error calls for a retry or an HA failover: a server exit or a
/// connection failure.
///
/// `Connection` covers both an unreachable server and the connection
/// failure the client reports once its own failover has run out.
/// `Http` is deliberately not included: the server may have applied the
/// request, so it is not retried here.
fn is_connection_error(error: &KineticaError) -> bool {
    matches!(error, KineticaError::Connection(_) | KineticaError::Exit(_))
}

/// The account of why a batch's worker failed, added to the message of a
/// batch that gives up: one entry, for the one worker this batch went to.
fn original_causes(worker_url: &str, error: &KineticaError) -> String {
    format!("[ worker URL {worker_url}: {};  ]", error.cause_description())
}

/// How many flush workers to start for a requested `num_workers`.
///
/// A request of `0` gets one worker per CPU (`cpus`).
/// Any other request is honoured as given -- the caller asked for it, and
/// this crate's constructors have always taken it -- where it used to be
/// silently clamped to between 4 and twice the CPU count.
fn flush_worker_count(requested: usize, cpus: usize) -> usize {
    if requested == 0 {
        cpus.max(1)
    } else {
        requested
    }
}

/// Whether a flush takes a queue holding `len` records: a non-empty queue, or
/// with `only_full` only one that has reached the batch size.
fn should_take_queue(len: usize, batch_size: usize, only_full: bool) -> bool {
    // len > 0 keeps an empty batch from being sent.
    len > 0 && (!only_full || len >= batch_size)
}

/// Shard-aware high-performance bulk inserter.
///
/// Routes records to the correct worker based on shard key computation,
/// enabling true multi-head ingest. Uses `serde_avro_fast` for fast Avro encoding
/// and rayon for parallel processing.
///
/// It also provides:
/// - Support for insert options (update_on_existing_pk, etc.)
/// - Permission checking before insertion
/// - Table replication detection (replicated tables use head node only)
/// - Multi-head ingest configuration
/// - Retry logic with configurable max retries
///
/// # HA Failover Safety
///
/// This implementation uses `ArcSwap` for lock-free atomic updates of routing state
/// during HA failover. This ensures:
/// - No race conditions when routing table changes during insert operations
/// - No out-of-bounds indexing from stale routing data
/// - Records are always routed using consistent, up-to-date routing information
pub struct BulkInserter<T> {
    // ========================================================================
    // Table and Connection Members
    // ========================================================================

    /// The Kinetica client instance
    kinetica: Arc<Kinetica>,

    /// Name of the table to insert records into
    table_name: Arc<str>,

    /// Number of records to insert at a time (per worker for multi-head)
    batch_size: usize,

    /// Options to pass to insert_records API calls (immutable after construction)
    options: Option<HashMap<String, String>>,

    /// Whether the target table is replicated
    is_table_replicated: bool,

    // ========================================================================
    // Ingest Processing Members
    // ========================================================================

    /// Number of stripes of each worker queue; each producer thread fills
    /// one of them (see `current_thread_stripe`)
    num_stripes: usize,

    /// Channel for sending flush jobs to worker threads
    flush_tx: flume::Sender<FlushJob<T>>,

    /// Flush options for timed/automatic flush behavior; the timed flush
    /// reads them on every run
    flush_options: parking_lot::RwLock<FlushOptions>,

    /// The timed flush task, while one is scheduled.
    timed_flush_task: Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Maximum number of retries for failed insertions
    max_retries: AtomicUsize,

    /// Whether a queue's batches are sent in the order they were dispatched,
    /// one at a time: for a table with a primary key, where two batches
    /// holding one key must reach the server in order (see `BatchOrder`).
    ordered_batches: bool,

    /// Whether return_individual_errors is set without allow_partial_batch
    /// (requires special error handling)
    return_individual_errors: bool,

    /// Set once a per-record error arrives while `return_individual_errors`
    /// is active. From then on, inserts are silently ignored and a flush
    /// discards what is buffered, emulating the server stopping at the first
    /// error.
    simulate_error_mode: AtomicBool,

    /// Count of successfully inserted records
    count_inserted: Arc<AtomicI64>,

    /// Count of successfully updated records (for upserts)
    count_updated: Arc<AtomicI64>,

    /// Count of pending batches (sent to channel but not yet completed)
    pending_batches: Arc<AtomicI64>,

    // ========================================================================
    // Sharding Members
    // ========================================================================

    /// Current routing state - atomically swappable for HA failover safety.
    ///
    /// Everything a record's destination depends on lives in here, behind one
    /// pointer: whether to use the head node, the queues, the shard mapping and
    /// the rank -> queue mapping. Holding them together means a rebuild cannot
    /// be observed part way through — a reader sees either the whole old state
    /// or the whole new one.
    ///
    /// Every read on the insert path must take this reference *once* into a
    /// local and use that local throughout. Re-loading mid-decision
    /// reintroduces exactly the inconsistency the snapshot exists to prevent.
    routing_state: ArcSwap<RoutingState<T>>,

    /// RecordKeyBuilder for computing shard keys
    key_builder: RecordKeyBuilder,

    /// KineticaType for schema reference during key building
    kinetica_type: KineticaType,


    // ========================================================================
    // Schema and Encoding Members
    // ========================================================================

    /// How the records are sent: Avro-encoded with the record type's schema,
    /// or as JSON
    format: RecordFormat<T>,

    /// The JSON options, given to a JSON inserter
    json_options: RwLock<Option<JsonOptions>>,

    /// serde_avro_fast schemas for request/response encoding
    req_schema: Arc<FastSchema>,
    resp_schema: Arc<FastSchema>,

    /// Pool for reusing batch vectors
    batch_pool: Arc<Mutex<Vec<Vec<Arc<T>>>>>,

    // ========================================================================
    // Backpressure, Callbacks, Errors and Shutdown Members
    // ========================================================================

    /// Semaphore to limit in-flight batches (backpressure control). Each
    /// queued batch that takes a permit carries it in its `FlushJob`, so it is
    /// returned exactly once, when the batch finishes.
    in_flight_semaphore: Arc<Semaphore>,

    /// The configured `max_in_flight_batches`.
    max_in_flight_batches: usize,

    /// Optional batch insertion listener for callbacks
    batch_listener: Option<Arc<dyn BatchInsertionListener>>,

    /// Error queue for async error handling (non-blocking)
    error_queue: Arc<Mutex<Vec<InsertError<T>>>>,

    /// Warning queue for async warning handling
    warning_queue: Arc<Mutex<Vec<String>>>,

    /// Whether timed flush is running
    timed_flush_running: Arc<AtomicBool>,

    /// Tells the flush workers and the timed-flush task to stop: set by
    /// `close()`, and dropped with the inserter, which stops them too.
    ///
    /// The tasks hold only a `Weak` reference to the inserter, so they do not
    /// keep it alive; dropping the last handle frees it even without a
    /// `close()`.
    shutdown: tokio::sync::watch::Sender<bool>,

    /// Set by `close()`. Checked before a batch is dispatched: the workers
    /// exit asynchronously after the shutdown signal, so the channel alone
    /// would still accept a batch sent just after `close()` returned.
    closed: AtomicBool,

    // ========================================================================
    // HA (High Availability) Members
    // ========================================================================

    /// Size of the HA ring (number of clusters)
    db_ha_ring_size: usize,

    /// The connection's multi-head snapshot this inserter last built its
    /// queues from. Compared by identity to tell whether the connection has
    /// moved to another cluster, or re-probed this one, since; see
    /// [`MultiHeadSnapshot`].
    last_multi_head_snapshot: Mutex<Option<Arc<MultiHeadSnapshot>>>,

    /// Held by a failover and by an update or rebuild of the worker queues, so
    /// none of them runs alongside another.
    monitor: tokio::sync::Mutex<()>,

    /// The shard mapping most recently fetched, which may not have been
    /// published yet. Only touched while holding `monitor`.
    pending_routing_table: Mutex<Option<Vec<i32>>>,

    /// Shard version for detecting routing table changes
    shard_version: AtomicI64,

    /// Timestamp of last shard update (milliseconds since epoch)
    shard_update_time: AtomicI64,

    /// Current head node URL (updated on failover)
    current_head_node_url: RwLock<Option<Url>>,

    /// Worker list for multi-head ingest (can be updated on failover)
    worker_list: RwLock<Option<WorkerList>>,
}

// Implementation block for types that implement Default (backwards compatible convenience methods)
impl<T: Serialize + KineticaRecord + ShardKeyExtractor + Send + Sync + 'static + Default> BulkInserter<T> {
    /// Creates a new shard-aware BulkInserter instance with default options.
    ///
    /// This is a convenience method that calls `with_options` with `None` for options.
    /// For more control over insert behavior, use `with_options` directly.
    ///
    /// # Arguments
    ///
    /// * `kinetica` - Shared Kinetica client
    /// * `table_name` - Target table for inserts (including schema, e.g., "my_schema.my_table")
    /// * `batch_size` - Records per batch (recommended: 50000)
    /// * `num_workers` - Number of flush workers, i.e. of batches sent concurrently;
    ///   used exactly as given. `0` sizes the pool at one worker per available
    ///   CPU.
    pub async fn new(
        kinetica: Arc<Kinetica>,
        table_name: String,
        batch_size: usize,
        num_workers: usize,
    ) -> Result<Arc<Self>, KineticaError> {
        Self::with_options(kinetica, table_name, batch_size, num_workers, None).await
    }

    /// Creates a new shard-aware BulkInserter instance with the specified options.
    ///
    /// This constructor:
    /// 1. Validates user permissions for the table
    /// 2. Validates batch size
    /// 3. Checks if the table is replicated
    /// 4. Configures multi-head ingest (if available and table is not replicated)
    /// 5. Sets up worker queues and routing
    ///
    /// # Arguments
    ///
    /// * `kinetica` - Shared Kinetica client
    /// * `table_name` - Target table for inserts (including schema, e.g., "my_schema.my_table")
    /// * `batch_size` - Records per batch (recommended: 50000); must be > 0
    /// * `num_workers` - Number of flush workers, i.e. of batches sent concurrently;
    ///   used exactly as given. `0` sizes the pool at one worker per available
    ///   CPU.
    /// * `options` - Optional BulkInserterOptions for configuring insert behavior
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User doesn't have required permissions (insert, and update if using upsert)
    /// - Table doesn't exist
    /// - Batch size is less than 1
    /// - Unable to configure multi-head ingest
    pub async fn with_options(
        kinetica: Arc<Kinetica>,
        table_name: String,
        batch_size: usize,
        num_workers: usize,
        options: Option<BulkInserterOptions>,
    ) -> Result<Arc<Self>, KineticaError> {
        // Get schema from default instance and delegate to with_schema
        let schema_str = T::default().avro_schema();
        Self::with_schema(kinetica, table_name, batch_size, num_workers, options, schema_str).await
    }
}

// Implementation block for Avro record types (no Default bound required)
impl<T: Serialize + KineticaRecord + ShardKeyExtractor + Send + Sync + 'static> BulkInserter<T> {
    /// Creates a new shard-aware BulkInserter instance with an explicit schema.
    ///
    /// This constructor is useful for types that cannot implement `Default`, such as
    /// `GenericRecord` which requires a `Type` reference at construction time.
    ///
    /// # Arguments
    ///
    /// * `kinetica` - Shared Kinetica client
    /// * `table_name` - Target table for inserts (including schema, e.g., "my_schema.my_table")
    /// * `batch_size` - Records per batch (recommended: 50000); must be > 0
    /// * `num_workers` - Number of flush workers, i.e. of batches sent concurrently;
    ///   used exactly as given. `0` sizes the pool at one worker per available
    ///   CPU.
    /// * `options` - Optional BulkInserterOptions for configuring insert behavior
    /// * `schema_str` - The Avro schema JSON string for the record type
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::{BulkInserter, GenericRecord, Type};
    /// # use gpudb_api::Kinetica;
    /// # use std::sync::Arc;
    /// # async fn example(kinetica: Arc<Kinetica>) -> Result<(), gpudb_api::KineticaError> {
    ///
    /// let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
    /// let schema_str = record_type.schema_string();
    ///
    /// let inserter = BulkInserter::<GenericRecord>::with_schema(
    ///     kinetica,
    ///     "my_table".into(),
    ///     50000,
    ///     8,
    ///     None,
    ///     schema_str,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_schema(
        kinetica: Arc<Kinetica>,
        table_name: String,
        batch_size: usize,
        num_workers: usize,
        options: Option<BulkInserterOptions>,
        schema_str: &str,
    ) -> Result<Arc<Self>, KineticaError> {
        Self::create(kinetica, table_name, batch_size, num_workers, options, NewFormat::Avro(schema_str)).await
    }
}

/// An inserter of JSON records.
///
/// Each record is one JSON object, as text. Records are sent as JSON arrays
/// to the ranks' `/insert/records/json`, each to the rank of a random shard:
/// the client does not read their keys. A batch the server fails is reported
/// as any other, through [`drain_errors`](Self::drain_errors), and one that
/// meets a connection failure is retried and failed over like any other.
impl BulkInserter<String> {
    /// Creates a JSON inserter with the default [`JsonOptions`] and options.
    ///
    /// `num_workers` is as for [`BulkInserter::with_schema`].
    pub async fn new_json(
        kinetica: Arc<Kinetica>,
        table_name: String,
        batch_size: usize,
        num_workers: usize,
    ) -> Result<Arc<Self>, KineticaError> {
        Self::with_json_options(kinetica, table_name, batch_size, num_workers, None, JsonOptions::default()).await
    }

    /// Creates a JSON inserter. With [`JsonOptions::is_validate_json`], each
    /// record inserted must be a JSON object.
    pub async fn with_json_options(
        kinetica: Arc<Kinetica>,
        table_name: String,
        batch_size: usize,
        num_workers: usize,
        options: Option<BulkInserterOptions>,
        json_options: JsonOptions,
    ) -> Result<Arc<Self>, KineticaError> {
        Self::create(kinetica, table_name, batch_size, num_workers, options, NewFormat::Json(String::as_str, json_options))
            .await
    }
}

// Implementation block for all record types
impl<T: Serialize + ShardKeyExtractor + Send + Sync + 'static> BulkInserter<T> {
    /// Creates an inserter sending its records as `format` says.
    async fn create(
        kinetica: Arc<Kinetica>,
        table_name: String,
        batch_size: usize,
        num_workers: usize,
        options: Option<BulkInserterOptions>,
        format: NewFormat<'_, T>,
    ) -> Result<Arc<Self>, KineticaError> {
        debug!("Creating BulkInserter for table '{table_name}' with batch_size={batch_size}, num_workers={num_workers}");

        let options = options.unwrap_or_default();
        let mut insert_options = options.insert_options.clone();

        // ====================================================================
        // Step 1: Check permissions
        // ====================================================================
        debug!("Checking permissions for table '{table_name}'");
        Self::check_permissions(&kinetica, &table_name, &insert_options).await?;

        // ====================================================================
        // Step 2: Validate batch size
        // ====================================================================
        if batch_size < 1 {
            return Err(KineticaError::Config(
                "Batch size must be greater than zero.".to_string()
            ));
        }

        // The record type's base definition is sent with each batch, to a
        // server that takes it; a JSON inserter has no record type
        if let NewFormat::Avro(schema_str) = format {
            if kinetica
                .server_version()
                .is_some_and(|server| REQUEST_SCHEMA_VERSION <= *server)
            {
                insert_options
                    .get_or_insert_with(HashMap::new)
                    .insert("request_schema_str".to_string(), base_definition(schema_str)?);
            }
        }

        // ====================================================================
        // Step 3: Call show_table to check table existence & replicated nature
        // ====================================================================
        let table_resp = kinetica.show_table_raw(&ShowTableRequest {
            table_name: table_name.clone(),
            options: HashMap::new(),
        }).await.map_err(|_| {
            KineticaError::Config(format!("Table '{table_name}' does not exist!"))
        })?;

        // Check if table is replicated
        let is_table_replicated = if !table_resp.table_descriptions.is_empty() {
            table_resp.table_descriptions[0].iter().any(|desc| desc == "REPLICATED")
        } else {
            false
        };
        debug!("Table '{table_name}' is_replicated={is_table_replicated}");

        // ====================================================================
        // Step 4: Configure worker list
        // ====================================================================
        // A caller-supplied list is honored as given; otherwise use the rank
        // addresses the connection already resolved for its current cluster.
        // Those were filtered through the user's hostname regex at discovery, so
        // this honors it without a second /show/system/properties round trip
        // that could itself fail.
        let workers = if let Some(ref worker_list) = options.worker_list {
            worker_list.clone()
        } else {
            WorkerList::from_cluster(&kinetica)
        };

        // Set if multi-head I/O is turned on at the server and rank URLs are accessible
        let multi_head_enabled = !workers.is_empty();

        // We should use the head node if multi-head is turned off at the server
        // or if we're working with a replicated table
        let use_head_node = !multi_head_enabled || is_table_replicated;
        debug!("Multi-head configuration: multi_head_enabled={multi_head_enabled}, use_head_node={use_head_node}");

        // ====================================================================
        // Step 5: Fetch routing table (only needed for multi-head)
        // ====================================================================
        // The shard version and update time fetched here are recorded. Where
        // the server cannot be reached, the inserter is built without a
        // mapping, which a later update fetches; until then, a record that
        // needs one is refused ("No shard mapping is available ..."). Any
        // other failure fails the construction.
        let (routing_table, initial_shard_version, initial_shard_update_time) = if !use_head_node {
            match kinetica.admin_show_shards_raw(&AdminShowShardsRequest::default()).await {
                Ok(shards_resp) => {
                    debug!("Fetched routing table with {} entries", shards_resp.rank.len());
                    (shards_resp.rank.clone(), shards_resp.version, now_millis())
                }
                Err(e) if is_connection_error(&e) => {
                    debug!("Had connection failure: {}", e.message());
                    (Vec::new(), 0, 0)
                }
                Err(e) => return Err(e),
            }
        } else {
            (Vec::new(), 0, 0)
        };

        // ====================================================================
        // Step 6: Build KineticaType and RecordKeyBuilder from table schema;
        // a JSON inserter reads no keys
        // ====================================================================
        // The key builder is built from the table's own keys, so a record goes
        // to the rank that holds its key whatever its record type says, and
        // the server need not pass it on.
        let kinetica_type = match format {
            NewFormat::Avro(_) => build_kinetica_type(&table_resp)?,
            NewFormat::Json(..) => KineticaType { columns: Vec::new() },
        };
        let key_builder = RecordKeyBuilder::new(&kinetica_type)?;
        // A JSON inserter routes each batch to a random worker, so no key
        // keeps to one queue and there is no order to keep
        let ordered_batches = kinetica_type
            .columns
            .iter()
            .any(|column| column.properties.iter().any(|p| p == "primary_key"));
        debug!("Built RecordKeyBuilder with {} shard columns, buffer size {} bytes",
               key_builder.shard_indices.len(), key_builder.buffer_size);
        if !key_builder.shard_indices.is_empty() {
            debug!("Shard columns: {:?}",
                   key_builder.shard_indices.iter()
                       .map(|&i| &kinetica_type.columns[i].name)
                       .collect::<Vec<_>>());
        }

        // ====================================================================
        // Step 7: Parse schemas for encoding
        // ====================================================================
        let (format, json_options) = match format {
            NewFormat::Avro(schema_str) => (
                RecordFormat::Avro(Arc::new(
                    FastSchema::from_str(schema_str)
                        .map_err(|e| KineticaError::Avro(format!("serde_avro_fast schema error: {e}")))?,
                )),
                None,
            ),
            NewFormat::Json(text, json_options) => (RecordFormat::Json(text), Some(json_options)),
        };

        let req_schema_str = RawInsertRecordsRequest::default().avro_schema();
        let req_schema = Arc::new(FastSchema::from_str(req_schema_str)
            .map_err(|e| KineticaError::Avro(format!("RawInsertRecordsRequest schema error: {e}")))?);

        let resp_schema = Arc::new(FastSchema::from_str(INSERT_RECORDS_RESPONSE_SCHEMA)
            .map_err(|e| KineticaError::Avro(format!("InsertRecordsResponse schema error: {e}")))?);

        // ====================================================================
        // Step 8: Build worker URLs
        // ====================================================================
        let worker_urls: Vec<Arc<str>>;
        let mut rank_to_worker_array: Vec<Option<usize>>;

        if use_head_node {
            // Use head node only for replicated tables or when multi-head is disabled.
            //
            // Take the cluster the connection is on now: `Kinetica::url` is the
            // URL given at construction and is not updated by a failover, so an
            // inserter built after one would have posted to the cluster that
            // failed.
            // With no cluster information at all, fall back to that URL.
            let head_url = kinetica
                .url()
                .map(|u| u.as_str().trim_end_matches('/').to_string())
                .unwrap_or_else(|| kinetica.url.clone());
            worker_urls = vec![Arc::from(head_url.as_str())];
            rank_to_worker_array = vec![None; 3];
            // Single worker queue pointing to head node
            rank_to_worker_array[0] = Some(0);
        } else {
            // Multi-head. The worker list keeps an empty slot for every rank
            // removed from the cluster, so that slot `i` is always rank `i + 1`
            // — the numbering the shard routing table uses. Queues are only
            // created for live ranks, so the rank -> queue mapping is built
            // here from the slot position rather than from the queue position;
            // compacting first would shift every rank above a removed one and
            // silently misroute its records.
            let (urls, mapping) = map_ranks_to_workers(workers.urls());
            worker_urls = urls;
            rank_to_worker_array = mapping;
        }
        debug!("Configured {} worker URLs", worker_urls.len());
        trace!("Worker URLs: {:?}", worker_urls.iter().map(|u| u.as_ref()).collect::<Vec<_>>());

        // ====================================================================
        // Step 9: Create worker queues and channels
        // ====================================================================
        let (tx, rx) = flume::unbounded();
        let count_inserted = Arc::new(AtomicI64::new(0));
        let count_updated = Arc::new(AtomicI64::new(0));
        let pending_batches = Arc::new(AtomicI64::new(0));

        // One stripe per CPU, so that as many threads as can run at once can
        // each fill a stripe of their own
        let num_stripes = num_cpus::get();

        let worker_queues: Vec<Arc<WorkerQueue<T>>> = worker_urls
            .iter()
            .map(|_| Arc::new(WorkerQueue::new(num_stripes, batch_size)))
            .collect();

        let batch_pool = Arc::new(Mutex::new(Vec::new()));

        // Pre-compute insert URLs for each worker
        let worker_url_cache: Vec<WorkerUrlCache> = worker_urls
            .iter()
            .map(|url| WorkerUrlCache::new(url.clone()))
            .collect();

        // ====================================================================
        // Step 10: Determine if special error handling is needed
        // ====================================================================
        let return_individual_errors = options.requires_individual_error_handling();

        // ====================================================================
        // Step 11: Create in-flight semaphore and error queues
        // ====================================================================
        let in_flight_semaphore = Arc::new(Semaphore::new(options.max_in_flight_batches));
        let error_queue = Arc::new(Mutex::new(Vec::new()));
        let warning_queue = Arc::new(Mutex::new(Vec::new()));
        let timed_flush_running = Arc::new(AtomicBool::new(false));

        // ====================================================================
        // Step 12: Create the BulkInserter instance
        // ====================================================================
        let bi = Arc::new(Self {
            kinetica: kinetica.clone(),
            table_name: Arc::from(table_name.as_str()),
            batch_size,
            options: insert_options,
            is_table_replicated,
            num_stripes,
            flush_tx: tx,
            flush_options: parking_lot::RwLock::new(options.flush_options.clone()),
            timed_flush_task: Mutex::new(None),
            max_retries: AtomicUsize::new(options.max_retries),
            ordered_batches,
            return_individual_errors,
            simulate_error_mode: AtomicBool::new(false),
            count_inserted: count_inserted.clone(),
            count_updated: count_updated.clone(),
            pending_batches: pending_batches.clone(),
            // Publish the initial routing state as one value, before anything
            // can read it.
            routing_state: ArcSwap::from_pointee(RoutingState::new(
                multi_head_enabled,
                is_table_replicated,
                worker_queues,
                routing_table.clone(),
                rank_to_worker_array.clone(),
                worker_url_cache.clone(),
                0,
            )),
            key_builder,
            kinetica_type,
            format,
            json_options: RwLock::new(json_options),
            req_schema,
            resp_schema,
            batch_pool,
            // Backpressure, callbacks, errors and shutdown
            in_flight_semaphore,
            max_in_flight_batches: options.max_in_flight_batches,
            batch_listener: options.batch_listener.clone(),
            error_queue,
            warning_queue,
            timed_flush_running,
            shutdown: tokio::sync::watch::channel(false).0,
            closed: AtomicBool::new(false),

            // HA members
            db_ha_ring_size: kinetica.ha_ring_size(),
            // Remember the multi-head state the queues were built from, to
            // detect a change after a failover, fail-back or rebalance
            last_multi_head_snapshot: Mutex::new(kinetica.current_multi_head_snapshot()),
            monitor: tokio::sync::Mutex::new(()),
            pending_routing_table: Mutex::new(None),
            shard_version: AtomicI64::new(initial_shard_version),
            shard_update_time: AtomicI64::new(initial_shard_update_time),
            current_head_node_url: RwLock::new(kinetica.url()),
            worker_list: RwLock::new(Some(workers.clone())),
        });

        // ====================================================================
        // Step 13: Start worker threads with semaphore-based backpressure
        // ====================================================================
        let num_flush_workers = flush_worker_count(num_workers, num_cpus::get());
        debug!("Starting {num_flush_workers} flush workers (requested={num_workers}, stripes={num_stripes})");

        // Each worker holds a `Weak` reference, taking a strong one only while
        // it processes a batch, so the workers do not keep the inserter alive.
        // They stop when `close()` signals shutdown, or when the inserter is
        // dropped (which closes the channel and drops the shutdown sender).
        for _ in 0..num_flush_workers {
            let weak = Arc::downgrade(&bi);
            let rx_worker = rx.clone();
            let pending = pending_batches.clone();
            let mut shutdown = bi.shutdown.subscribe();
            tokio::spawn(async move {
                loop {
                    let job = tokio::select! {
                        // Drain queued batches before honouring a shutdown.
                        biased;
                        job = rx_worker.recv_async() => match job {
                            Ok(job) => job,
                            Err(_) => break,
                        },
                        _ = shutdown.changed() => break,
                    };
                    let Some(bi) = weak.upgrade() else {
                        warn!(
                            "BulkInserter was dropped with batches still queued; \
                             {} record(s) were not sent. Call close() before dropping it.",
                            job.records.len()
                        );
                        break;
                    };
                    let FlushJob { worker_idx, insert_url, records, permit, mut ticket } = job;
                    // The queue's earlier batches holding one of its keys
                    // first, where order matters; the keys are hashed here,
                    // off the inserting thread
                    if let Some(ticket) = &mut ticket {
                        ticket.wait_for_turn(bi.routing_key_hashes(&records)).await;
                    }
                    let _ = bi.do_insert(worker_idx, insert_url, records).await;
                    // The turn passes to the queue's next batch, and the
                    // in-flight permit, if any, is released
                    drop(ticket);
                    drop(permit);
                    // Decrement pending batch count after completion.
                    pending.fetch_sub(1, Ordering::Release);
                }
            });
        }

        // ====================================================================
        // Step 14: Start timed flush task if configured
        // ====================================================================
        bi.start_timed_flush(false);

        debug!("BulkInserter ready for table '{}': {} workers, batch_size={}, multi_head={}, max_in_flight={}",
              table_name, worker_urls.len(), batch_size, multi_head_enabled, options.max_in_flight_batches);
        Ok(bi)
    }

    /// Fails unless the user may insert into the table, and update it where
    /// `update_on_existing_pk` asks for upserts.
    ///
    /// Both permissions are asked for before either answer is looked at, and
    /// a missing one fails with one message, naming the user.
    async fn check_permissions(
        kinetica: &Arc<Kinetica>,
        table_name: &str,
        insert_options: &Option<HashMap<String, String>>,
    ) -> Result<(), KineticaError> {
        let has_permission = |permission: &str| HasPermissionRequest {
            principal: String::new(), // the current user
            object: table_name.to_string(),
            object_type: "table".to_string(),
            permission: permission.to_string(),
            options: HashMap::new(),
        };

        let insert_permission_ok = kinetica.has_permission_raw(&has_permission("insert")).await?.has_permission;

        // If the user requests upserts, check update permission on the table
        let upserts = insert_options
            .as_ref()
            .and_then(|options| options.get(insert_records_options::UPDATE_ON_EXISTING_PK))
            .is_some_and(|value| value.eq_ignore_ascii_case(insert_records_options::TRUE));
        let update_permission_ok =
            !upserts || kinetica.has_permission_raw(&has_permission("update")).await?.has_permission;

        if !(insert_permission_ok && update_permission_ok) {
            return Err(KineticaError::Api(format!(
                "User {} doesn't have requisite permissions on the table {table_name} to use BulkInserter",
                kinetica.username()
            )));
        }
        Ok(())
    }

    /// Resolves the worker queue a record belongs to.
    ///
    /// With a single queue — head-node mode, or a cluster down to one worker —
    /// there is nothing to route and no shard mapping is needed, so the only
    /// queue takes everything. This short-circuit must come first: without
    /// it, a replicated or unsharded table would be reported as having no
    /// shard mapping.
    ///
    /// Otherwise the shard mapping decides, and a record that cannot be routed
    /// is an error rather than being handed to worker 0.
    #[inline]
    fn route_to_worker(
        &self,
        shard_key_values: &[(&str, ShardKeyValue<'_>)],
        routing: &RoutingState<T>,
    ) -> Result<usize, KineticaError> {
        if routing.use_head_node {
            // Routing::new guarantees the head-node queue is the only queue,
            // so index 0 is it.
            return Ok(0);
        }

        // A cluster of one worker rank, not one with a single rank left: the
        // mapping keeps one entry per worker rank slot, a removed rank's
        // included, after one for the head.
        if routing.rank_to_worker_array.len() == 2 {
            // A single destination, so there is nothing to choose: skip the
            // shard key work. Note this is worker rank 1 — not the head node,
            // which the branch above handles. If rank 1 was removed it has no
            // queue, and the record is refused.
            return routing.rank_to_worker_array[1]
                .ok_or_else(|| crate::bulk_inserter_common::RouteFailure::RankRemoved.into_error(&self.table_name));
        }

        let worker_idx = match self.format {
            // No key of a JSON record is read: it goes to the rank of a random
            // shard
            RecordFormat::Json(_) => {
                compute_random_worker_index(&routing.routing_table, &routing.rank_to_worker_array)
            }
            RecordFormat::Avro(_) => compute_worker_index_from_values(
                shard_key_values,
                &self.key_builder,
                &routing.routing_table,
                &routing.rank_to_worker_array,
            ),
        }
        .map_err(|f| f.into_error(&self.table_name))?;

        // The queues and the rank mapping are rebuilt together and published as
        // one value, so this should be unreachable. Refuse the record rather
        // than index out of bounds if it ever is.
        if worker_idx >= routing.queues.len() {
            return Err(KineticaError::Api(format!(
                "Shard mapping for table '{}' routed a record to worker {}, but \
                 this routing state has only {} worker queue(s).",
                self.table_name,
                worker_idx,
                routing.queues.len()
            )));
        }

        Ok(worker_idx)
    }

    /// Takes an in-flight permit if one is free, without waiting.
    ///
    /// The non-blocking insert paths (`insert`, `insert_batch*`, `flush`)
    /// cannot wait for a permit, so they send a full batch whether or not one
    /// is free. Only [`insert_with_backpressure`](Self::insert_with_backpressure)
    /// waits, so `max_in_flight_batches` bounds that path alone; a caller
    /// mixing the two can have more than `max_in_flight_batches` batches in
    /// flight.
    fn try_in_flight_permit(&self) -> Option<OwnedSemaphorePermit> {
        self.in_flight_semaphore.clone().try_acquire_owned().ok()
    }

    /// Hands a batch to the flush workers.
    ///
    /// Fails once the inserter has been closed: the workers have stopped, so
    /// the batch could never be sent.
    fn dispatch(
        &self,
        routing: &RoutingState<T>,
        worker_idx: usize,
        records: Vec<Arc<T>>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<(), QueueError<T>> {
        let insert_url = routing.worker_url_cache[worker_idx].insert_url.clone();
        let count = records.len();
        let closed = || KineticaError::Config(format!(
            "BulkInserter for table '{}' is closed; {count} record(s) were not sent",
            self.table_name
        ));
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueError::new(closed(), records));
        }
        // Increment pending batch count before sending
        self.pending_batches.fetch_add(1, Ordering::Acquire);
        let send = |ticket| self.flush_tx.send(FlushJob { worker_idx, insert_url, records, permit, ticket });
        let sent = if self.ordered_batches {
            routing.queues[worker_idx].order.dispatch(|ticket| send(Some(ticket)))
        } else {
            send(None)
        };
        if let Err(flume::SendError(job)) = sent {
            self.pending_batches.fetch_sub(1, Ordering::Release);
            return Err(QueueError::new(closed(), job.records));
        }
        Ok(())
    }

    /// The hashes of `records`' routing keys, by which a queue keeps batches
    /// sharing a key in order (see `BatchOrder`). A record whose key cannot
    /// be built, which only the head-node mode lets through, gets one hash
    /// shared by all such records.
    fn routing_key_hashes(&self, records: &[Arc<T>]) -> Vec<u64> {
        records
            .iter()
            .map(|record| match self.key_builder.build_typed(&record.shard_key_values()) {
                Ok(Some(key)) => key.routing_hash(),
                _ => u64::MAX,
            })
            .collect()
    }

    /// Inserts a single record with shard-aware routing.
    ///
    /// The record's shard key is computed using `ShardKeyExtractor::shard_key_values()`,
    /// and the record is routed to the worker that owns the corresponding shard.
    ///
    /// # Errors
    ///
    /// Returns an error if the record cannot be routed — see
    /// [`Self::route_to_worker`]. The record is not queued in that case, and
    /// comes back in the [`QueueError`].
    ///
    /// Also returns an error after [`close`](Self::close) when this record
    /// fills a batch: the batch cannot be sent, and its records come back.
    ///
    /// # HA Failover Safety
    ///
    /// This method uses `ArcSwap::load()` to get the current routing state, which provides:
    /// - Lock-free atomic read of the routing configuration
    /// - Consistent routing even during HA failover (no torn reads)
    /// - No race conditions with concurrent routing updates
    ///
    /// A JSON inserter whose [`JsonOptions`] ask for it first checks that the
    /// record is a JSON object.
    #[inline]
    pub fn insert(&self, record: Arc<T>) -> Result<(), QueueError<T>> {
        if let Err(error) = self.check_json_record(&record) {
            return Err(QueueError::new(error, vec![record]));
        }
        self.queue_record(record)
    }

    /// Whether the JSON options ask for each record to be checked.
    fn validate_json(&self) -> bool {
        matches!(self.format, RecordFormat::Json(_))
            && self.json_options.read().is_some_and(|options| options.is_validate_json())
    }

    /// Checks a JSON record inserted alone.
    fn check_json_record(&self, record: &T) -> Result<(), KineticaError> {
        if let RecordFormat::Json(text) = self.format {
            if self.validate_json() && !json_utils::is_valid_json(text(record)) {
                return Err(KineticaError::Api("String passed in is Not a valid JSON record".to_string()));
            }
        }
        Ok(())
    }

    /// Checks JSON records inserted together: all of them, before any is
    /// queued.
    fn check_json_records<R: Sync>(&self, records: &[R], record: impl Fn(&R) -> &T + Sync) -> Result<(), KineticaError> {
        if let RecordFormat::Json(text) = self.format {
            if self.validate_json() && !records.par_iter().all(|r| json_utils::is_valid_json(text(record(r)))) {
                return Err(KineticaError::Api("List passed in is not a list of valid JSON strings".to_string()));
            }
        }
        Ok(())
    }

    /// Queues a record, routed by its key, sending its queue's batch once
    /// full: `insert` once the record has been checked.
    fn queue_record(&self, record: Arc<T>) -> Result<(), QueueError<T>> {
        // Don't accept new records once an error has arrived in "stop on
        // error" mode. The records are dropped without an error; the one
        // that triggered the mode is in the error queue.
        if self.simulate_error_mode.load(Ordering::Acquire) {
            return Ok(());
        }

        // Load current routing state atomically (lock-free)
        let routing = self.routing_state.load();

        // The key values borrow the record for this statement only, so the
        // record can move into its queue below, or back to the caller
        let routed = self.route_to_worker(&record.shard_key_values(), &routing);
        let worker_idx = match routed {
            Ok(worker_idx) => worker_idx,
            Err(error) => return Err(QueueError::new(error, vec![record])),
        };

        // This thread's stripe of the destination's queue
        let stripe = &routing.queues[worker_idx].stripes[current_thread_stripe(self.num_stripes)];

        let batch_to_flush = {
            let mut stripe_guard = stripe.lock();
            stripe_guard.push(record);

            if stripe_guard.len() >= self.batch_size {
                let batch = std::mem::replace(&mut *stripe_guard, pooled_vec(&self.batch_pool, self.batch_size));
                Some(batch)
            } else {
                None
            }
        };

        if let Some(batch) = batch_to_flush {
            // Non-blocking: send with a permit if one is free, without otherwise.
            let permit = self.try_in_flight_permit();
            self.dispatch(&routing, worker_idx, batch, permit)?;
        }

        Ok(())
    }

    /// Inserts a single record with backpressure control.
    ///
    /// This async method will wait if the maximum in-flight batches limit is reached,
    /// providing memory-bounded operation for high-throughput scenarios.
    ///
    /// # HA Failover Safety
    ///
    /// This method uses `ArcSwap::load()` to get the current routing state atomically.
    pub async fn insert_with_backpressure(&self, record: Arc<T>) -> Result<(), KineticaError> {
        self.check_json_record(&record)?;

        // Don't accept new records once an error has arrived in "stop on
        // error" mode. The records are dropped without an error; the one
        // that triggered the mode is in the error queue.
        if self.simulate_error_mode.load(Ordering::Acquire) {
            return Ok(());
        }

        // Load current routing state atomically (lock-free)
        let routing = self.routing_state.load();

        let worker_idx = self.route_to_worker(&record.shard_key_values(), &routing)?;
        let stripe = &routing.queues[worker_idx].stripes[current_thread_stripe(self.num_stripes)];

        let batch_to_flush = {
            let mut stripe_guard = stripe.lock();
            stripe_guard.push(record);

            if stripe_guard.len() >= self.batch_size {
                let batch = std::mem::replace(&mut *stripe_guard, pooled_vec(&self.batch_pool, self.batch_size));
                Some(batch)
            } else {
                None
            }
        };

        if let Some(batch) = batch_to_flush {
            // Wait for a permit if at capacity (backpressure). The batch
            // carries it, and it is released when the batch finishes.
            let permit = self.in_flight_semaphore.clone().acquire_owned().await
                .map_err(|_| KineticaError::Internal)?;
            self.dispatch(&routing, worker_idx, batch, Some(permit))?;
        }

        Ok(())
    }

    /// Inserts a batch of records with shard-aware routing.
    ///
    /// This method is more efficient than calling `insert()` in a loop because:
    /// 1. Worker indices are computed in parallel using rayon
    /// 2. Records are grouped by destination before any lock is taken
    /// 3. Each destination's queue is locked once per call, not once per record
    ///
    /// The records go into the calling thread's stripe of each queue, as
    /// `insert` puts them, and a batch is sent each time that stripe reaches
    /// the batch size. No batch is larger than the batch size.
    ///
    /// # Arguments
    ///
    /// * `records` - A vector of records to insert. Each record will be wrapped in Arc internally.
    ///
    /// # Errors
    ///
    /// The records before the first that cannot be routed are queued, and
    /// that record and every one after it come back in the [`QueueError`]
    /// with its error. A list whose JSON does not validate is not queued at
    /// all, and comes back whole.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use gpudb_api::{avro_schema, AvroSchemaBuilder, BulkInserter, KineticaRecord};
    /// # use gpudb_api::{ShardKeyExtractor, ShardKeyValue, ShardKeyValues};
    /// # use std::collections::HashMap;
    /// # #[derive(serde::Serialize, Default)]
    /// # struct MyRecord { id: i32, name: String }
    /// # impl KineticaRecord for MyRecord {
    /// #     fn avro_schema(&self) -> &'static str {
    /// #         avro_schema!(AvroSchemaBuilder::new("MyRecord").int_field("id").string_field("name"))
    /// #     }
    /// #     fn type_properties() -> HashMap<String, Vec<String>> { HashMap::new() }
    /// # }
    /// # impl ShardKeyExtractor for MyRecord {
    /// #     fn shard_key_values(&self) -> ShardKeyValues<'_> {
    /// #         smallvec::smallvec![("id", ShardKeyValue::Int(self.id))]
    /// #     }
    /// # }
    /// # fn example(inserter: &BulkInserter<MyRecord>) -> Result<(), Box<dyn std::error::Error>> {
    /// let records: Vec<MyRecord> = (0..100_000)
    ///     .map(|i| MyRecord { id: i, name: format!("name_{}", i) })
    ///     .collect();
    /// inserter.insert_batch(records)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_batch(&self, records: Vec<T>) -> Result<(), QueueError<T>> {
        if records.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.check_json_records(&records, |record| record) {
            return Err(QueueError::new(error, records.into_iter().map(Arc::new).collect()));
        }
        // Don't accept new records once an error has arrived in "stop on
        // error" mode. The records are dropped without an error; the one
        // that triggered the mode is in the error queue.
        if self.simulate_error_mode.load(Ordering::Acquire) {
            return Ok(());
        }

        self.queue_in_order(records.into_par_iter().map(Arc::new).collect())
    }

    /// Inserts a batch of records that are already wrapped in Arc.
    ///
    /// This is useful when records are shared or when the caller already has Arc-wrapped records.
    ///
    /// # Arguments
    ///
    /// * `records` - A vector of Arc-wrapped records to insert.
    pub fn insert_batch_arc(&self, records: Vec<Arc<T>>) -> Result<(), QueueError<T>> {
        if records.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.check_json_records(&records, |record| &**record) {
            return Err(QueueError::new(error, records));
        }
        // Don't accept new records once an error has arrived in "stop on
        // error" mode. The records are dropped without an error; the one
        // that triggered the mode is in the error queue.
        if self.simulate_error_mode.load(Ordering::Acquire) {
            return Ok(());
        }

        self.queue_in_order(records)
    }

    /// Queues records in order, until one cannot be routed, which comes back
    /// to the caller with every record after it, while the ones before it
    /// stay queued.
    ///
    /// The routing itself runs in parallel, from one snapshot of the routing
    /// state, and the queueing groups the records by destination; only which
    /// records are queued follows the order.
    fn queue_in_order(&self, mut records: Vec<Arc<T>>) -> Result<(), QueueError<T>> {
        let routing = self.routing_state.load();
        let routing_ref: &RoutingState<T> = &routing;

        let mut routes: Vec<Result<usize, KineticaError>> = records
            .par_iter()
            .map(|record| self.route_to_worker(&record.shard_key_values(), routing_ref))
            .collect();

        // The first record that cannot be routed, with every one after it
        let refused = routes.iter().position(Result::is_err).map(|first| {
            let error = routes[first].clone().err().unwrap_or(KineticaError::Internal);
            routes.truncate(first);
            QueueError::new(error, records.split_off(first))
        });

        let routed = routes.into_iter().flatten().zip(records).collect();
        if let Err(mut not_sent) = self.queue_routed(routing_ref, routed) {
            if let Some(refused) = refused {
                not_sent.records.extend(refused.records);
            }
            return Err(not_sent);
        }
        refused.map_or(Ok(()), Err)
    }

    /// Queues records routed against `routing` into the calling thread's
    /// stripe of each destination's queue, locking each queue once, and sends
    /// a batch each time the stripe reaches the batch size: the second half of
    /// `insert_batch` and `insert_batch_arc`.
    fn queue_routed(&self, routing: &RoutingState<T>, routed: Vec<(usize, Arc<T>)>) -> Result<(), QueueError<T>> {
        // Taken here, on the caller's thread; the routing ran on rayon's
        let stripe_idx = current_thread_stripe(self.num_stripes);

        // Group the records by destination, keeping their order
        let mut by_worker: Vec<Vec<Arc<T>>> = (0..routing.queues.len()).map(|_| Vec::new()).collect();
        for (worker_idx, record) in routed {
            by_worker[worker_idx].push(record);
        }

        // One lock acquisition per destination
        let mut batches_to_flush: Vec<(usize, Vec<Arc<T>>)> = Vec::new();
        for (worker_idx, records) in by_worker.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }

            let mut stripe_guard = routing.queues[worker_idx].stripes[stripe_idx].lock();
            for record in records {
                stripe_guard.push(record);

                if stripe_guard.len() >= self.batch_size {
                    let batch = std::mem::replace(
                        &mut *stripe_guard,
                        pooled_vec(&self.batch_pool, self.batch_size),
                    );
                    batches_to_flush.push((worker_idx, batch));
                }
            }
        }

        // Send the full batches, once every lock is released. Where one
        // cannot be sent, neither can the rest, and they all go back.
        let mut batches = batches_to_flush.into_iter();
        while let Some((worker_idx, batch)) = batches.next() {
            let permit = self.try_in_flight_permit();
            if let Err(mut not_sent) =
                self.dispatch(routing, worker_idx, batch, permit)
            {
                not_sent.records.extend(batches.flat_map(|(_, batch)| batch));
                return Err(not_sent);
            }
        }

        Ok(())
    }

    /// Internal method using serde_avro_fast for encoding.
    /// This method handles retries with the configured max_retries.
    /// Also invokes batch listener callbacks and queues errors for async handling.
    /// Includes HA failover support for connection errors.
    async fn do_insert(
        &self,
        worker_idx: usize,
        queued_url: Arc<str>,
        records: Vec<Arc<T>>,
    ) -> Result<(), KineticaError> {
        let batch_start = std::time::Instant::now();
        let record_count = records.len();

        // A fail-back moves the connection without a failure here. Notice it
        // by the connection's URL having changed, and rebuild for the cluster
        // now in use before sending.
        if self.current_head_node_url() != self.kinetica.url() {
            if let Err(e) = self.update_worker_queues(true).await {
                let message = e.message();
                self.notify_batch_failure(&queued_url, records.len(), &message, std::time::Duration::ZERO, 0);
                self.queue_error(&queued_url, records.len(), records, &message);
                return Err(e);
            }
        }

        // The connection's cluster switch count as of this attempt, for the
        // failover to tell a first switch from one already made.
        let current_cluster_switch_count = self.kinetica.num_cluster_switches();

        // The URL the batch was queued against, carried with it rather than
        // looked up by index now. A rebuild may have replaced the routing state
        // since — with a different number of destinations — so `worker_idx` is
        // no longer a safe index into the current URL cache.
        let worker_url = queued_url;

        // Measure encoding time
        let encode_start = std::time::Instant::now();

        // A JSON batch is the records' texts as one JSON array
        let schema = match &self.format {
            RecordFormat::Avro(schema) => Arc::clone(schema),
            RecordFormat::Json(text) => {
                let payload = json_utils::to_json_array(records.iter().map(|record| text(record)));
                let encode_duration = encode_start.elapsed();
                return self
                    .send_batch(
                        worker_idx,
                        worker_url,
                        records,
                        BatchRequest::Json(payload),
                        (batch_start, encode_duration),
                        current_cluster_switch_count,
                    )
                    .await;
            }
        };

        // The batch is encoded on one thread. The flush workers already
        // encode their batches at the same time, so splitting each one over
        // every CPU as well would gain nothing: it took about three times the
        // CPU of encoding it on one thread, CPU taken from the producers and,
        // on a shared host, from the server.
        //
        // The records are encoded straight into the request's body, which is
        // then compressed if Snappy applies. Building the request whole would
        // hold the batch twice, a buffer per record and then the request
        // copied from them.
        //
        // This is CPU-bound, so it runs on Tokio's blocking pool rather than on
        // this async worker thread: done inline it would stall every other task
        // scheduled on the thread for the length of it. The records move into
        // the closure and come back with the result, since the retry and
        // pooling below still need them.
        let request_schema = Arc::clone(&self.req_schema);
        let table_name = Arc::clone(&self.table_name);
        let options = self.options.clone().unwrap_or_default();
        let kinetica = Arc::clone(&self.kinetica);
        let encoded = tokio::task::spawn_blocking(move || {
            let body = encode_insert_records_body(&schema, &request_schema, &table_name, &options, &records)
                .and_then(|(body, failures)| {
                    let (body, snappy) = kinetica.compress_request_body::<RawInsertRecordsRequest>(body)?;
                    Ok((body, snappy, failures))
                });
            (records, body)
        })
        .await;

        let encode_duration = encode_start.elapsed();

        let (records, body) = match encoded {
            Ok(pair) => pair,
            Err(join_error) => {
                // The encode panicked (or the runtime is shutting down). The
                // records were dropped with the closure; report the batch as
                // failed rather than letting the panic take this worker down.
                let msg = format!("Avro encoding task failed: {join_error}");
                self.queue_error(&worker_url, record_count, Vec::new(), &msg);
                self.notify_batch_failure(&worker_url, record_count, &msg, encode_duration, 0);
                return Err(KineticaError::Avro(msg));
            }
        };

        let (body, snappy, encode_failures) = match body {
            Ok(body) => body,
            Err(e) => {
                // Queue encoding error and notify listener
                self.notify_batch_failure(&worker_url, record_count, &e.message(), encode_duration, 0);
                self.queue_error(&worker_url, record_count, records, &e.message());
                return Err(e);
            }
        };

        self.send_batch(
            worker_idx,
            worker_url,
            records,
            BatchRequest::Avro { body, snappy, encode_failures },
            (batch_start, encode_duration),
            current_cluster_switch_count,
        )
        .await
    }

    /// Sends a batch, retrying and failing over, and reports how it went.
    async fn send_batch(
        &self,
        worker_idx: usize,
        worker_url: Arc<str>,
        records: Vec<Arc<T>>,
        req: BatchRequest,
        (batch_start, encode_duration): (std::time::Instant, std::time::Duration),
        current_cluster_switch_count: usize,
    ) -> Result<(), KineticaError> {
        let record_count = records.len();
        let network_start = std::time::Instant::now();
        let max_retries = self.max_retries.load(Ordering::Relaxed);
        let current_worker_url = worker_url.clone();

        // How a batch is retried:
        //
        // - Only a connection failure or an exit is retried. Anything else,
        //   such as a server-side data error, fails the batch at once.
        // - `max_retries` counts retries after the first attempt, so 0 means
        //   one attempt.
        // - Retries are immediate, with no back-off. A brief outage can
        //   therefore use up the retries in quick succession before a
        //   failover is tried.
        // - When they run out, a single cluster gives up. Otherwise the
        //   inserter fails over and starts again on the new cluster with the
        //   full retry count. That ends when the failover walk has tried every
        //   cluster in the ring and reports HA unavailable.
        let mut attempt = 0usize;
        let mut retries_left = max_retries;

        let failure = loop {
            match self.submit_batch(&current_worker_url, &req, record_count).await {
                Ok(outcome) => {
                    let network_duration = network_start.elapsed();
                    let batch_duration = batch_start.elapsed();

                    // Track inserted and updated counts separately
                    let (inserted, updated) = (outcome.inserted, outcome.updated);
                    self.count_inserted.fetch_add(inserted, Ordering::Relaxed);
                    self.count_updated.fetch_add(updated, Ordering::Relaxed);

                    if let Some(info) = &outcome.info {
                        self.gather_record_errors(&current_worker_url, info, req.encode_failures(), &records);
                    }

                    trace!("Batch complete: sent={} inserted={} updated={} encode={:.1}ms network={:.1}ms total={:.1}ms worker={}",
                           record_count, inserted, updated,
                           encode_duration.as_secs_f64() * 1000.0,
                           network_duration.as_secs_f64() * 1000.0,
                           batch_duration.as_secs_f64() * 1000.0,
                           worker_idx);

                    // Notify batch listener of success
                    self.notify_batch_success(
                        &current_worker_url, record_count, inserted, updated,
                        BatchTimings { encode: encode_duration, network: network_duration, total: batch_duration },
                        attempt,
                    );

                    return_to_pool(&self.batch_pool, records);

                    // The server accepted the batch but rerouted it: shard
                    // rebalancing is under way and our mapping is stale, so
                    // refresh it, and rebuild the worker queues, before routing
                    // more records. The queues are rebuilt, not only the
                    // mapping: a rebalance can add or remove ranks, and a
                    // mapping naming a rank with no queue refuses every record
                    // bound for it. The
                    // batch itself succeeded, so a failed refresh is logged and
                    // left for the next batch rather than turning a successful
                    // insert into an error.
                    if outcome.info.as_ref().is_some_and(crate::gpudb::is_data_rerouted) {
                        debug!(
                            "Server reported rerouted data for table '{}'; refreshing shard mapping",
                            self.table_name
                        );
                        if let Err(e) = self.update_worker_queues(true).await {
                            // Surfaced to the caller as well as logged: the
                            // batch succeeded, so this is not an error, but it
                            // means the mapping in force is known-stale and the
                            // caller may want to know.
                            let message = format!(
                                "Failed to refresh shard mapping for table '{}' after \
                                 the server reported rerouted data: {e}",
                                self.table_name
                            );
                            warn!("{message}");
                            self.queue_warning(message);
                        }
                    }

                    return Ok(());
                }
                Err(e) if !is_connection_error(&e) => {
                    warn!("Batch insert to {current_worker_url} failed, not retrying: {e}");
                    break e;
                }
                Err(e) => {
                    warn!("Batch insert attempt {} to {current_worker_url} failed: {e}", attempt + 1);

                    if retries_left > 0 {
                        retries_left -= 1;
                        attempt += 1;
                        continue;
                    }

                    if self.db_ha_ring_size <= 1 {
                        break KineticaError::caused(
                            format!(
                                "Insertion failed; ran out of retries.  Original causes encountered by workers: {}",
                                original_causes(&current_worker_url, &e)
                            ),
                            e,
                        );
                    }

                    debug!("Retries exhausted after a connection failure; attempting HA failover...");
                    match self.handle_connection_error(
                        &e,
                        &current_worker_url,
                        current_cluster_switch_count,
                    ).await {
                        Ok(()) => {
                            // After a failover, rebuild the worker queues for
                            // the cluster now in use, failing the batch with
                            // the rebuild's own message if that fails...
                            debug!("Before calling update_worker_queues()");
                            if let Err(rebuild_err) = self.update_worker_queues(true).await {
                                debug!("update_worker_queues() failed with error: {}", rebuild_err.message());
                                break rebuild_err;
                            }
                            // ...then re-insert the failed records, routing
                            // each of them again: the new cluster's ranks and
                            // shard mapping decide where it goes, not this
                            // batch's old worker index. They go out as batches
                            // of their own, each with the full retry count.
                            debug!("HA failover completed; re-routing the batch's {record_count} record(s)");
                            self.reroute_records(records);
                            return Ok(());
                        }
                        Err(failover_err) => {
                            // A failover that has tried every cluster is
                            // reported with the failover's message, then what
                            // failed on the workers
                            warn!("HA failover failed: {failover_err}");
                            break KineticaError::caused(
                                format!(
                                    "{}.  Original causes  encountered by workers: {}",
                                    failover_err.message(),
                                    original_causes(&current_worker_url, &e)
                                ),
                                failover_err,
                            );
                        }
                    }
                }
            }
        };

        // The batch failed - queue error and notify listener, with the error's
        // own message, without its kind
        let error_msg = failure.message();

        let batch_duration = batch_start.elapsed();
        let network_duration = network_start.elapsed();
        self.notify_batch_failure_with_timing(
            &worker_url, record_count, &error_msg,
            BatchTimings { encode: encode_duration, network: network_duration, total: batch_duration },
            attempt,
        );

        // The batch's records go with the error
        self.queue_error(&worker_url, record_count, records, &error_msg);
        warn!("Batch of {record_count} records failed after {} attempt(s): {error_msg}", attempt + 1);
        Err(failure)
    }

    /// Sends a batch once to `url`.
    async fn submit_batch(&self, url: &str, req: &BatchRequest, record_count: usize) -> Result<BatchOutcome, KineticaError> {
        match req {
            BatchRequest::Avro { body, snappy, .. } => {
                let resp = self
                    .kinetica
                    .submit_encoded_request_raw::<InsertRecordsResponse>(url, body.clone(), *snappy, &self.resp_schema)
                    .await?;
                Ok(BatchOutcome {
                    inserted: resp.count_inserted as i64,
                    updated: resp.count_updated as i64,
                    info: Some(resp.info),
                })
            }
            BatchRequest::Json(payload) => {
                let response = self
                    .kinetica
                    .insert_records_from_json_at(url, payload, &self.table_name, self.options.as_ref())
                    .await?;
                let (inserted, updated) = json_insert_counts(&response, record_count)?;
                Ok(BatchOutcome { inserted, updated, info: None })
            }
        }
    }

    /// Queues the per-record errors and the warnings a successful insert
    /// reported in its `info` map: each `error_<index>` entry becomes an error
    /// and each `warning_*` entry a warning. Each record of the batch that did
    /// not encode becomes an error too, with the client's message in place of
    /// the server's.
    ///
    /// Each error carries the record its index names, and counts one. A
    /// per-record error switches on "stop on error" mode when
    /// `return_individual_errors` is active.
    fn gather_record_errors(
        &self,
        worker_url: &str,
        info: &HashMap<String, String>,
        encode_failures: &[EncodeFailure],
        records: &[Arc<T>],
    ) {
        let (errors, warnings) = record_messages_from_info(info, encode_failures);
        for (index, message) in errors {
            if self.return_individual_errors {
                self.simulate_error_mode.store(true, Ordering::Release);
            }
            let record = index.and_then(|i| records.get(i)).cloned();
            self.queue_error(worker_url, 1, record.into_iter().collect(), &message);
        }
        for message in warnings {
            self.queue_warning(message);
        }
    }

    /// Queues an error for the caller to collect via [`Self::drain_errors`].
    ///
    /// Unbounded: nothing is dropped, so a caller that never drains the queue
    /// holds every error in memory. This queue also receives whole failed
    /// batches: they are sent by background workers, with no caller to return
    /// the error to.
    fn queue_error(&self, worker_url: &str, record_count: usize, records: Vec<Arc<T>>, message: &str) {
        self.error_queue.lock().push(InsertError {
            worker_url: worker_url.to_string(),
            url: self.kinetica.url(),
            record_count,
            records,
            message: message.to_string(),
            timestamp: std::time::Instant::now(),
        });
    }

    /// Queues a warning for the caller to collect via
    /// [`Self::drain_warnings`].
    ///
    /// For conditions the caller may want to act on but which did not fail the
    /// insert — a batch that succeeded against a shard mapping we then could
    /// not refresh, for instance.
    ///
    /// Unbounded.
    fn queue_warning(&self, message: String) {
        self.warning_queue.lock().push(message);
    }

    /// Notifies the batch listener of a successful insertion.
    fn notify_batch_success(
        &self,
        worker_url: &str,
        batch_size: usize,
        count_inserted: i64,
        count_updated: i64,
        timings: BatchTimings,
        attempt: usize,
    ) {
        let BatchTimings { encode: encode_duration, network: network_duration, total: total_duration } = timings;
        if let Some(ref listener) = self.batch_listener {
            listener.on_batch_inserted(&BatchInsertionResult {
                worker_url: worker_url.to_string(),
                batch_size,
                count_inserted,
                count_updated,
                success: true,
                error_message: None,
                encode_time_ms: encode_duration.as_secs_f64() * 1000.0,
                network_time_ms: network_duration.as_secs_f64() * 1000.0,
                total_time_ms: total_duration.as_secs_f64() * 1000.0,
                attempt,
            });
        }
    }

    /// Notifies the batch listener of a failed insertion (encoding error).
    fn notify_batch_failure(
        &self,
        worker_url: &str,
        batch_size: usize,
        error_message: &str,
        encode_duration: std::time::Duration,
        attempt: usize,
    ) {
        if let Some(ref listener) = self.batch_listener {
            listener.on_batch_inserted(&BatchInsertionResult {
                worker_url: worker_url.to_string(),
                batch_size,
                count_inserted: 0,
                count_updated: 0,
                success: false,
                error_message: Some(error_message.to_string()),
                encode_time_ms: encode_duration.as_secs_f64() * 1000.0,
                network_time_ms: 0.0,
                total_time_ms: encode_duration.as_secs_f64() * 1000.0,
                attempt,
            });
        }
    }

    /// Notifies the batch listener of a failed insertion (network error).
    fn notify_batch_failure_with_timing(
        &self,
        worker_url: &str,
        batch_size: usize,
        error_message: &str,
        timings: BatchTimings,
        attempt: usize,
    ) {
        let BatchTimings { encode: encode_duration, network: network_duration, total: total_duration } = timings;
        if let Some(ref listener) = self.batch_listener {
            listener.on_batch_inserted(&BatchInsertionResult {
                worker_url: worker_url.to_string(),
                batch_size,
                count_inserted: 0,
                count_updated: 0,
                success: false,
                error_message: Some(error_message.to_string()),
                encode_time_ms: encode_duration.as_secs_f64() * 1000.0,
                network_time_ms: network_duration.as_secs_f64() * 1000.0,
                total_time_ms: total_duration.as_secs_f64() * 1000.0,
                attempt,
            });
        }
    }

    // ========================================================================
    // Getters
    // ========================================================================

    /// Returns the total number of records successfully inserted.
    pub fn count_inserted(&self) -> i64 {
        self.count_inserted.load(Ordering::Relaxed)
    }

    /// Returns the total number of records successfully updated (for upserts).
    pub fn count_updated(&self) -> i64 {
        self.count_updated.load(Ordering::Relaxed)
    }

    /// Returns the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns the batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Returns whether the target table is replicated.
    pub fn is_table_replicated(&self) -> bool {
        self.is_table_replicated
    }

    /// Returns whether multi-head ingest is enabled.
    pub fn is_multi_head_enabled(&self) -> bool {
        self.routing_state.load().multi_head_enabled
    }

    /// Returns whether the head node is being used (replicated table or multi-head disabled).
    pub fn is_using_head_node(&self) -> bool {
        self.routing_state.load().use_head_node
    }

    /// Returns the current maximum retries for failed insertions.
    pub fn max_retries(&self) -> usize {
        self.max_retries.load(Ordering::Relaxed)
    }

    /// Sets the maximum retries for failed insertions.
    pub fn set_max_retries(&self, max_retries: usize) {
        self.max_retries.store(max_retries, Ordering::Relaxed);
    }

    /// Returns a reference to the insert options.
    pub fn options(&self) -> Option<&HashMap<String, String>> {
        self.options.as_ref()
    }

    /// The JSON options of a JSON inserter, or those set since.
    pub fn json_options(&self) -> Option<JsonOptions> {
        *self.json_options.read()
    }

    /// Replaces the JSON options, which a JSON inserter reads for each record
    /// it is given. They do not make an inserter of Avro records a JSON one.
    pub fn set_json_options(&self, json_options: JsonOptions) {
        *self.json_options.write() = Some(json_options);
    }

    /// Returns the flush options.
    pub fn flush_options(&self) -> FlushOptions {
        self.flush_options.read().clone()
    }

    /// The timed flush options.
    pub fn timed_flush_options(&self) -> FlushOptions {
        self.flush_options()
    }

    /// Replaces the timed flush options, `None` restoring the defaults (no
    /// timed flush), and restarts the timed flush with them. A scheduled timed
    /// flush is stopped first, and a new one starts if the new interval is
    /// positive, its first run one interval from now.
    pub fn set_timed_flush_options(self: &Arc<Self>, flush_options: Option<FlushOptions>) {
        *self.flush_options.write() = flush_options.unwrap_or_default();
        if let Some(task) = self.timed_flush_task.lock().take() {
            task.abort();
            self.timed_flush_running.store(false, Ordering::Release);
            debug!("Timed flush executor service terminated ...");
        }
        self.start_timed_flush(true);
    }

    /// Schedules the timed flush the flush options ask for, if their
    /// interval is positive: a run every interval, the next one an interval
    /// after the last. Each run reads the options then: with
    /// `flush_when_full`, only full queues are sent, otherwise every queue.
    ///
    /// A run hands its batches off rather than waiting for them to land, as
    /// every flush in this inserter does; a failure is logged, and the
    /// schedule goes on.
    fn start_timed_flush(self: &Arc<Self>, restart: bool) {
        let interval_secs = self.flush_options.read().flush_interval;
        if interval_secs <= 0 {
            debug!("Timed flush turned off, flush interval set to negative value");
            return;
        }
        if restart {
            debug!("Timed flush restarted, flush interval set to <{interval_secs}>");
        } else {
            debug!("Timed flush turned on, flush interval set to <{interval_secs}>");
        }

        // Like the flush workers, the task holds only a `Weak` reference
        let weak = Arc::downgrade(self);
        let mut shutdown = self.shutdown.subscribe();
        let interval = std::time::Duration::from_secs(interval_secs as u64);
        self.timed_flush_running.store(true, Ordering::Release);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown.changed() => break,
                }
                let Some(inserter) = weak.upgrade() else { break };
                if !inserter.timed_flush_running.load(Ordering::Acquire) {
                    break;
                }
                let started = std::time::Instant::now();
                debug!("Timed flush started at : {}", chrono::Local::now().naive_local());
                let flush_when_full = inserter.flush_options.read().flush_when_full;
                let result = if flush_when_full {
                    inserter.flush_full_queues_no_wait()
                } else {
                    inserter.flush_no_wait().await
                };
                match result {
                    Ok(()) => debug!("Timed flush completed in : {} seconds", started.elapsed().as_secs()),
                    Err(e) => error!("{e}"),
                }
            }
            debug!("Timed flush task stopped");
        });
        *self.timed_flush_task.lock() = Some(task);
    }

    /// Returns whether return_individual_errors mode is active.
    pub fn is_return_individual_errors(&self) -> bool {
        self.return_individual_errors
    }

    /// Returns the number of records currently buffered in worker queues.
    ///
    /// These are records that have been queued via `insert()` but not yet
    /// sent to the flush workers. Call `flush()` to send these records.
    pub fn buffered_record_count(&self) -> usize {
        let mut count = 0;
        let routing = self.routing_state.load();
        for worker_queue in &routing.queues {
            for stripe in &worker_queue.stripes {
                count += stripe.lock().len();
            }
        }
        count
    }

    /// Returns the estimated number of records in-flight (sent but not yet confirmed).
    ///
    /// This is an estimate based on pending_batches * batch_size.
    /// The actual number may be slightly different due to partial batches.
    pub fn in_flight_record_estimate(&self) -> i64 {
        self.pending_batches.load(Ordering::Acquire) * self.batch_size as i64
    }

    // ========================================================================
    // Error/Warning Queue Methods
    // ========================================================================

    /// The connection this inserter sends through.
    pub fn kinetica(&self) -> &Arc<Kinetica> {
        &self.kinetica
    }

    /// Drains and returns all queued errors.
    /// This clears the error queue, so subsequent calls return new errors only.
    pub fn drain_errors(&self) -> Vec<InsertError<T>> {
        let mut queue = self.error_queue.lock();
        std::mem::take(&mut *queue)
    }

    /// Returns the number of queued errors without draining.
    pub fn error_count(&self) -> usize {
        self.error_queue.lock().len()
    }

    /// Returns the total number of records that failed to insert.
    /// This sums the record_count from all errors in the queue.
    pub fn failed_record_count(&self) -> usize {
        self.error_queue.lock().iter().map(|e| e.record_count).sum()
    }

    /// Drains and returns all queued warnings.
    pub fn drain_warnings(&self) -> Vec<String> {
        let mut queue = self.warning_queue.lock();
        std::mem::take(&mut *queue)
    }

    /// Returns the number of queued warnings without draining.
    pub fn warning_count(&self) -> usize {
        self.warning_queue.lock().len()
    }

    /// Returns whether timed flush is currently running.
    pub fn is_timed_flush_running(&self) -> bool {
        self.timed_flush_running.load(Ordering::Acquire)
    }

    /// Stops the timed flush task.
    pub fn stop_timed_flush(&self) {
        self.timed_flush_running.store(false, Ordering::Release);
    }

    /// Returns the maximum number of in-flight batches configured.
    pub fn max_in_flight_batches(&self) -> usize {
        self.max_in_flight_batches
    }

    /// Returns the number of available in-flight permits.
    pub fn available_in_flight_permits(&self) -> usize {
        self.in_flight_semaphore.available_permits()
    }

    /// Returns backpressure metrics for monitoring.
    ///
    /// This provides visibility into the backpressure state of the inserter,
    /// useful for monitoring and tuning `max_in_flight_batches`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn example<T>(inserter: &gpudb_api::BulkInserter<T>) where T: serde::Serialize + gpudb_api::KineticaRecord + gpudb_api::ShardKeyExtractor + Send + Sync + 'static {
    /// let metrics = inserter.backpressure_metrics();
    /// println!("In-flight: {}/{} ({:.1}% utilized)",
    ///     metrics.in_flight_batches,
    ///     metrics.max_in_flight_batches,
    ///     metrics.utilization_pct
    /// );
    /// # }
    /// ```
    pub fn backpressure_metrics(&self) -> BackpressureMetrics {
        let available = self.in_flight_semaphore.available_permits();
        let pending = self.pending_batches.load(Ordering::Acquire);
        let max_in_flight = self.max_in_flight_batches;
        // Every queued batch counts, including those the non-blocking paths
        // sent without a permit, so this can exceed 100%.
        let in_flight = pending.max(0) as usize;
        let utilization = if max_in_flight > 0 {
            (in_flight as f64 / max_in_flight as f64) * 100.0
        } else {
            0.0
        };

        BackpressureMetrics {
            max_in_flight_batches: max_in_flight,
            available_permits: available,
            in_flight_batches: in_flight,
            pending_batches: pending,
            utilization_pct: utilization,
        }
    }

    // ========================================================================
    // Flush Methods
    // ========================================================================

    /// Flushes all buffered records to the server and waits until they have
    /// been acknowledged.
    ///
    /// When this returns `Ok(())`, every record queued before the call has
    /// either been inserted or recorded in the error queue — so a subsequent
    /// read is guaranteed to see them.
    ///
    /// Check [`error_count`](Self::error_count) or
    /// [`drain_errors`](Self::drain_errors) afterwards to see whether any
    /// records failed; a returned `Ok` means "all batches finished", not "all
    /// batches succeeded".
    ///
    /// Use [`flush_no_wait`](Self::flush_no_wait) if you deliberately want to
    /// hand the batches off without waiting.
    pub async fn flush(&self) -> Result<(), KineticaError> {
        self.flush_no_wait().await?;
        self.wait_for_pending_batches().await;
        Ok(())
    }

    /// Hands all buffered records to the flush workers without waiting for
    /// them to complete.
    ///
    /// Prefer [`flush`](Self::flush) — after this returns, the records have
    /// *not* necessarily reached the server, so reading the table back may not
    /// see them. This exists for background flushing (the timed-flush task
    /// uses it) where blocking would stall the caller.
    pub async fn flush_no_wait(&self) -> Result<(), KineticaError> {
        debug!("Flushing all buffered records");
        let batches_flushed = self.dispatch_buffered(false)?;
        debug!("Flushed {batches_flushed} batches to workers");
        Ok(())
    }

    /// Hands only the full stripes (those holding at least `batch_size`
    /// records) to the flush workers, without waiting. The timed flush uses
    /// this when [`FlushOptions::flush_when_full`] is set.
    fn flush_full_queues_no_wait(&self) -> Result<(), KineticaError> {
        debug!("Flushing full queues");
        let batches_flushed = self.dispatch_buffered(true)?;
        debug!("Flushed {batches_flushed} full batches to workers");
        Ok(())
    }

    /// Takes every buffered batch -- or, with `only_full`, only the full ones
    /// -- and dispatches it. Returns how many batches were sent.
    ///
    /// A worker's records are spread over stripes and a stripe is what
    /// becomes one batch, so "full" is judged per stripe, the unit the insert
    /// paths also dispatch at.
    fn dispatch_buffered(&self, only_full: bool) -> Result<usize, KineticaError> {
        let mut batches_flushed = 0;
        let routing = self.routing_state.load();
        for (worker_idx, worker_queue) in routing.queues.iter().enumerate() {
            for stripe in &worker_queue.stripes {
                let batch = {
                    let mut s = stripe.lock();
                    // In "stop on error" mode each queue is cleared instead
                    // of sent.
                    if self.simulate_error_mode.load(Ordering::Acquire) {
                        s.clear();
                        continue;
                    }
                    if !should_take_queue(s.len(), self.batch_size, only_full) {
                        continue;
                    }
                    std::mem::replace(&mut *s, pooled_vec(&self.batch_pool, self.batch_size))
                };

                batches_flushed += 1;
                let permit = self.try_in_flight_permit();
                self.dispatch(&routing, worker_idx, batch, permit)?;
            }
        }
        Ok(batches_flushed)
    }

    /// Polls until no batches are in flight.
    ///
    /// Returns when `pending_batches` reaches zero, i.e. every dispatched batch
    /// has either succeeded or been recorded as an error. Shared by
    /// [`flush`](Self::flush) and [`close`](Self::close).
    async fn wait_for_pending_batches(&self) {
        let poll_interval = std::time::Duration::from_millis(10);
        let mut last_log_time = std::time::Instant::now();
        let log_interval = std::time::Duration::from_secs(10);

        while self.pending_batches.load(Ordering::Acquire) > 0 {
            if last_log_time.elapsed() > log_interval {
                debug!(
                    "Waiting for {} pending batches to complete... ({} inserted, {} updated so far)",
                    self.pending_batches.load(Ordering::Acquire),
                    self.count_inserted(),
                    self.count_updated()
                );
                last_log_time = std::time::Instant::now();
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Signals the flush workers and the timed-flush task to stop.
    fn shutdown_workers(&self) {
        self.closed.store(true, Ordering::Release);
        self.stop_timed_flush();
        self.shutdown.send_replace(true);
    }

    /// Returns the number of pending batches (sent but not yet completed).
    pub fn pending_batches(&self) -> i64 {
        self.pending_batches.load(Ordering::Acquire)
    }

    /// Closes the BulkInserter, flushing all records and waiting for completion.
    ///
    /// This method:
    /// 1. Flushes all buffered records to the flush workers
    /// 2. Waits for all pending batches to complete
    /// 3. Stops the flush workers and the timed flush
    ///
    /// The inserter cannot send anything afterwards: an insert still buffers,
    /// but one that fills a batch, or a `flush()` with records buffered, fails.
    ///
    /// **IMPORTANT**: This method will wait indefinitely until all pending batches
    /// complete. It only returns when:
    /// 1. All records have been successfully inserted, OR
    /// 2. All records have either succeeded or failed (with errors queued)
    ///
    /// Failed records are tracked in the error queue and can be retrieved via
    /// `drain_errors()`. The method guarantees that `inserted + updated + failed = queued`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when all batches have completed (success or failure).
    /// Check `error_count()` or `drain_errors()` to see if any records failed.
    pub async fn close(&self) -> Result<(), KineticaError> {
        debug!("Closing BulkInserter, flushing all remaining records...");

        // Step 0: Stop timed flush if running
        self.stop_timed_flush();

        // Step 1 + 2: Flush remaining records and wait for every batch to land.
        // No timeout, no giving up.
        let start = std::time::Instant::now();
        self.flush().await?;

        // Step 3: Stop the flush workers and the timed flush. Every batch
        // has landed, so none is abandoned; a later insert that fills a batch
        // fails instead of queueing it for workers that no longer run.
        self.shutdown_workers();

        let error_count = self.error_count();
        if error_count > 0 {
            debug!("BulkInserter closed: {} inserted, {} updated, {} failed ({} errors) in {:?}",
                  self.count_inserted(), self.count_updated(),
                  self.failed_record_count(), error_count, start.elapsed());
        } else {
            debug!("BulkInserter closed successfully: {} inserted, {} updated in {:?}",
                  self.count_inserted(), self.count_updated(), start.elapsed());
        }
        Ok(())
    }

    /// Closes the BulkInserter with an optional timeout.
    ///
    /// If timeout is reached, returns an error but records may still be in-flight.
    /// **Prefer using `close()` without timeout to ensure all records are flushed.**
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for pending batches to complete
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all batches completed, or `Err` if timeout was reached.
    pub async fn close_with_timeout(&self, timeout: std::time::Duration) -> Result<(), KineticaError> {
        debug!("Closing BulkInserter with timeout {timeout:?}");

        // Step 0: Stop timed flush if running
        self.stop_timed_flush();

        // Step 1: Hand off remaining records. Deliberately the non-waiting
        // variant: `flush` waits unboundedly, which would defeat the timeout
        // this method exists to enforce.
        self.flush_no_wait().await?;

        // Step 2: Wait for pending batches with timeout
        let start = std::time::Instant::now();
        let poll_interval = std::time::Duration::from_millis(10);

        loop {
            let pending = self.pending_batches.load(Ordering::Acquire);
            if pending <= 0 {
                self.shutdown_workers();
                let error_count = self.error_count();
                if error_count > 0 {
                    debug!("BulkInserter closed with {} errors: {} inserted, {} updated in {:?}",
                          error_count, self.count_inserted(), self.count_updated(), start.elapsed());
                } else {
                    debug!("BulkInserter closed successfully: {} inserted, {} updated in {:?}",
                          self.count_inserted(), self.count_updated(), start.elapsed());
                }
                return Ok(());
            }

            if start.elapsed() > timeout {
                // The workers are left running so the pending batches can still
                // finish; they stop when the inserter is dropped.
                warn!("Timeout waiting for {pending} pending batches after {timeout:?}");
                return Err(KineticaError::Config(format!(
                    "Timeout waiting for {pending} pending batches to complete after {timeout:?}. \
                     Use close() without timeout to wait indefinitely."
                )));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    // ========================================================================
    // Shard Information Methods
    // ========================================================================

    /// Returns the number of shard key columns used for routing.
    pub fn shard_column_count(&self) -> usize {
        self.key_builder.shard_indices.len()
    }

    /// Returns the names of shard key columns.
    pub fn shard_column_names(&self) -> Vec<String> {
        self.key_builder.shard_indices.iter()
            .map(|&i| self.kinetica_type.columns[i].name.clone())
            .collect()
    }

    /// Returns the routing table size (number of shards).
    pub fn routing_table_size(&self) -> usize {
        self.routing_state.load().routing_table.len()
    }

    /// Returns the number of worker queues.
    pub fn worker_count(&self) -> usize {
        self.routing_state.load().queues.len()
    }

    // ========================================================================
    // HA (High Availability) Methods
    // ========================================================================

    /// Returns the number of times the connection has switched clusters due to
    /// failover.
    pub fn num_cluster_switches(&self) -> usize {
        self.kinetica.num_cluster_switches()
    }

    /// Returns the current HA ring size (number of clusters).
    pub fn ha_ring_size(&self) -> usize {
        self.db_ha_ring_size
    }

    /// Returns the current head node URL.
    pub fn current_head_node_url(&self) -> Option<Url> {
        self.current_head_node_url.read().clone()
    }

    /// Sets the current head node URL.
    fn set_current_head_node_url(&self, url: Option<Url>) {
        *self.current_head_node_url.write() = url;
    }


    /// Asks the connection to fail over to another cluster, and records where
    /// it ended up.
    ///
    /// The selection is entirely the connection's: `switch_url` walks the HA
    /// ring and returns the first cluster it found usable, having probed it.
    /// This contributes the caller's vantage point -- the URL it was using and
    /// the switch count it last saw -- which tells a first failover from one
    /// piggybacking on a failover already in progress.
    async fn force_failover(
        &self,
        old_url: &Url,
        old_cluster_switch_count: usize,
    ) -> Result<(), KineticaError> {
        let _monitor = self.monitor.lock().await;
        self.kinetica.switch_url(old_url, old_cluster_switch_count).await?;
        // Record where the connection ended up
        self.set_current_head_node_url(self.kinetica.url());
        Ok(())
    }

    /// Updates the shard mapping, and optionally rebuilds the worker queues,
    /// after a cluster change or a shard version change.
    ///
    /// With the shard version unchanged and the connection's multi-head
    /// snapshot the one the queues were built from, nothing has moved, and
    /// only a rebuild that was asked for is attempted. Otherwise the new
    /// mapping is published, and the queues are rebuilt if asked for, the
    /// rebuild re-probing the cluster when the shard version changed, since a
    /// rebalance can move rank addresses without moving the connection.
    ///
    /// The queues are never rebuilt in head-node mode (a replicated table, or
    /// multi-head off), so the head-node queue keeps the URL it was built with
    /// across a failover. A known limitation.
    ///
    /// Returns whether the shard mapping was updated.
    async fn update_worker_queues(&self, do_reconstruct_worker_queues: bool) -> Result<bool, KineticaError> {
        let _monitor = self.monitor.lock().await;
        self.update_worker_queues_locked(do_reconstruct_worker_queues, true).await
    }

    /// [`Self::update_worker_queues`], for a caller holding `monitor`.
    ///
    /// `publish_shard_mapping` is whether a newly fetched shard mapping is
    /// published on its own. The rebuild, about to publish a whole routing
    /// state of its own, passes `false` and takes the mapping from
    /// `pending_routing_table`, so a new mapping is never published alongside
    /// queues it does not describe.
    fn update_worker_queues_locked(
        &self,
        do_reconstruct_worker_queues: bool,
        publish_shard_mapping: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, KineticaError>> + Send + '_>> {
        Box::pin(async move {
            let reconstruct_worker_queues = do_reconstruct_worker_queues && !self.routing_state.load().use_head_node;
            debug!("Reconstruct worker URLs?: {reconstruct_worker_queues}");

            // Get the latest shard mapping information; this request may fail over
            let shard_info = match self
                .kinetica
                .admin_show_shards_raw(&AdminShowShardsRequest::default())
                .await
            {
                Ok(info) => info,
                Err(e) => {
                    if is_connection_error(&e) {
                        debug!("Had connection failure: {e}");
                        return Ok(false);
                    }
                    return Err(e);
                }
            };

            let new_shard_version = shard_info.version;
            let shard_mapping_changed = self.shard_version.load(Ordering::SeqCst) != new_shard_version;

            if !shard_mapping_changed {
                // Also check whether the connection moved to another cluster -- by
                // failover or fail-back -- or re-probed this one, since the queues
                // were last built.
                let current_snapshot = self.kinetica.current_multi_head_snapshot();
                let unchanged = same_snapshot(&current_snapshot, &self.last_multi_head_snapshot.lock());
                if unchanged {
                    debug!("Same cluster and shard version");
                    if reconstruct_worker_queues {
                        return self.reconstruct_worker_queues_locked(false).await;
                    }
                    debug!("Returning false");
                    return Ok(false);
                }
                // Record the snapshot now current, so the next call compares
                // against it rather than against the one left behind
                *self.last_multi_head_snapshot.lock() = current_snapshot;
            }

            // Save the new shard version and when it was updated
            self.shard_version.store(new_shard_version, Ordering::SeqCst);
            self.shard_update_time.store(now_millis(), Ordering::SeqCst);

            // Record the new shard mapping, and publish it unless the caller is
            // about to publish a routing state of its own. The queues and their
            // destinations are untouched, so the new state shares them.
            *self.pending_routing_table.lock() = Some(shard_info.rank.clone());
            if publish_shard_mapping {
                let new_routing = self.routing_state.load().with_routing_table(shard_info.rank);
                self.routing_state.store(Arc::new(new_routing));
            }

            // We may have failed over during /admin/show/shards
            self.set_current_head_node_url(self.kinetica.url());

            if reconstruct_worker_queues {
                self.reconstruct_worker_queues_locked(shard_mapping_changed).await?;
            }

            debug!("Worker queues updated, new shard version: {new_shard_version}");
            Ok(true)
        })
    }

    /// Rebuilds the worker queues for the cluster the connection is on now,
    /// and re-queues the records they held.
    ///
    /// `topology_may_have_moved` is what the caller observed: `true` when the
    /// server reported a shard mapping change, which can move rank addresses
    /// without moving the connection, so the connection re-probes before
    /// answering; `false` after a cluster change, which the switch already
    /// probed. The rebuild uses the snapshot the connection answers with, and
    /// records it, so a later re-probe or move shows as a change.
    ///
    /// A cluster with no usable workers gets a single head-node queue.
    /// Returns whether the queues were rebuilt. The caller holds `monitor`.
    async fn reconstruct_worker_queues_locked(&self, topology_may_have_moved: bool) -> Result<bool, KineticaError> {
        // A caller-supplied empty list is authoritative and is never replaced:
        // it declines multi-head for the life of this inserter.
        if self.worker_list.read().as_ref().is_some_and(|wl| wl.disables_multi_head()) {
            debug!("Worker list declines multi-head; not rebuilding");
            return Ok(false);
        }

        // Ask the connection for the current cluster's addresses, and build from
        // that same answer, so the addresses and the snapshot recorded below
        // cannot come from different probes.
        let snapshot = self.kinetica.acquire_multi_head_snapshot(topology_may_have_moved).await;
        let new_worker_list = WorkerList::from_snapshot(snapshot.as_deref());

        if self.worker_list.read().as_ref() == Some(&new_worker_list) {
            debug!("Worker list remained the same; returning false");
            return Ok(false);
        }

        // Update the worker list, and remember the exact answer these
        // addresses came from, not the cluster
        *self.worker_list.write() = Some(new_worker_list.clone());
        *self.last_multi_head_snapshot.lock() = snapshot;

        let multi_head_enabled = !new_worker_list.is_empty();
        let use_head_node = RoutingState::<T>::use_head_node_for(multi_head_enabled, self.is_table_replicated);

        let (new_worker_urls, new_rank_to_worker_array, new_routing_table) = if !use_head_node {
            // The list keeps an empty slot for each removed rank; the rank ->
            // queue mapping comes from the slot positions, so a removed rank
            // never shifts the ones above it.
            let (urls, rank_to_worker) = map_ranks_to_workers(new_worker_list.urls());

            // Fetch the shard mapping that goes with the new queues, without
            // letting it be published on its own: it describes the new set of
            // ranks. The fetch leaves nothing pending when the shard version
            // has not moved, and the mapping in force then still applies. A
            // failure here leaves the new worker list recorded but the routing
            // state as it was.
            *self.pending_routing_table.lock() = None;
            self.update_worker_queues_locked(false, false).await?;
            let routing_table = self
                .pending_routing_table
                .lock()
                .clone()
                .unwrap_or_else(|| self.routing_state.load().routing_table.clone());
            (urls, rank_to_worker, routing_table)
        } else {
            // The head node of the cluster in use now
            let head_url = self
                .kinetica
                .url()
                .map(|u| u.as_str().trim_end_matches('/').to_string())
                .unwrap_or_else(|| self.kinetica.url.clone());
            let mut mapping = vec![None; 3];
            mapping[0] = Some(0);
            // The head-node routing state carries no shard mapping
            (vec![Arc::from(head_url.as_str())], mapping, Vec::new())
        };

        let new_url_cache: Vec<WorkerUrlCache> =
            new_worker_urls.iter().map(|url| WorkerUrlCache::new(url.clone())).collect();
        let new_queues: Vec<Arc<WorkerQueue<T>>> = new_worker_urls
            .iter()
            .map(|_| Arc::new(WorkerQueue::new(self.num_stripes, self.batch_size)))
            .collect();

        // Publish the whole new routing state with one store, so that a
        // concurrent insert sees either all of the old state or all of the new
        // one, before the re-queue below routes through it.
        let previous = self.routing_state.load_full();
        let new_routing = RoutingState::new(
            multi_head_enabled,
            self.is_table_replicated,
            new_queues,
            new_routing_table,
            new_rank_to_worker_array,
            new_url_cache,
            previous.version + 1,
        );
        self.routing_state.store(Arc::new(new_routing));

        // Re-queue whatever the old queues were still holding, routed against
        // the state just published
        self.requeue_records(&previous);

        debug!("Worker list was updated, returning true");
        Ok(true)
    }

    /// Drains the queues of a superseded routing state and re-inserts their
    /// records through the current one.
    ///
    /// A record buffered against the old rank layout has no meaning in the new
    /// one — its queue index named a rank that may not exist any more, or may
    /// now be a different host. Re-routing each record is the only way to get it
    /// to the rank that actually owns its shard.
    ///
    /// Records that fail to route are reported as errors rather than dropped;
    /// there is nowhere left to hold them, and losing them silently is worse
    /// than surfacing the failure.
    fn requeue_records(&self, old_routing: &RoutingState<T>) {
        let mut requeued = 0usize;

        for old_queue in &old_routing.queues {
            for stripe in &old_queue.stripes {
                let records = {
                    let mut guard = stripe.lock();
                    if guard.is_empty() {
                        continue;
                    }
                    std::mem::take(&mut *guard)
                };

                for record in records {
                    match self.queue_record(record) {
                        Ok(()) => requeued += 1,
                        Err(not_queued) => {
                            warn!(
                                "Could not re-queue a buffered record for table '{}' \
                                 after the worker queues were rebuilt: {}",
                                self.table_name, not_queued
                            );
                            let count = not_queued.records.len();
                            self.queue_error(&self.table_name.clone(), count, not_queued.records, &not_queued.error.message());
                        }
                    }
                }
            }
        }

        if requeued > 0 {
            debug!("Re-queued {requeued} buffered record(s) onto the new worker queues");
        }
    }

    /// Fails over after a connection failure that has used up the retries.
    /// Fails with the failover's own error where it has tried every cluster.
    async fn handle_connection_error(
        &self,
        error: &KineticaError,
        worker_url: &str,
        current_count_cluster_switches: usize,
    ) -> Result<(), KineticaError> {
        let had_connection_failure = is_connection_error(error);
        let mut did_failover_succeed = false;

        if had_connection_failure {
            warn!("Caught connection failure on worker {worker_url}: {error}");

            // Try to get the current head node URL for failover
            if let Some(ref curr_url) = self.current_head_node_url() {
                match self
                    .force_failover(curr_url, current_count_cluster_switches)
                    .await
                {
                    Ok(()) => {
                        did_failover_succeed = true;
                        debug!("HA failover succeeded");
                    }
                    Err(e2) => {
                        // The failover's own error: the caller reports it
                        // with the workers' failure
                        error!("HA failover failed: {e2}");
                        return Err(e2);
                    }
                }
            }
        }

        debug!("Did failover succeed? {did_failover_succeed}");
        Ok(())
    }

    /// Routes each record through the current routing state and dispatches
    /// the records bound for each destination as a batch, at once.
    ///
    /// Used after a failover, for a batch whose records were routed against
    /// the cluster that failed. A record that cannot be routed is reported as
    /// an error, as `insert` would refuse it.
    fn reroute_records(&self, records: Vec<Arc<T>>) {
        let routing = self.routing_state.load();
        let mut by_destination: HashMap<usize, Vec<Arc<T>>> = HashMap::new();
        for record in records {
            let routed = self.route_to_worker(&record.shard_key_values(), &routing);
            match routed {
                Ok(worker_idx) => by_destination.entry(worker_idx).or_default().push(record),
                Err(e) => {
                    warn!("Could not re-route a record for table '{}' after a failover: {e}", self.table_name);
                    self.queue_error(&self.table_name.clone(), 1, vec![record], &e.message());
                }
            }
        }
        for (worker_idx, batch) in by_destination {
            let count = batch.len();
            let permit = self.try_in_flight_permit();
            let url = routing.worker_url_cache[worker_idx].insert_url.clone();
            if let Err(not_sent) = self.dispatch(&routing, worker_idx, batch, permit) {
                self.queue_error(&url, count, not_sent.records, &not_sent.error.message());
            }
        }
    }

}

/// Dropping an inserter stops its background tasks (they hold only `Weak`
/// references, and the shutdown sender goes with it), so nothing leaks. It
/// cannot flush, since that is asynchronous: records still buffered, or
/// queued but not yet sent, are lost. Say so, rather than lose them silently.
impl<T> Drop for BulkInserter<T> {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);

        let routing = self.routing_state.load();
        let buffered: usize = routing
            .queues
            .iter()
            .flat_map(|q| q.stripes.iter())
            .map(|s| s.lock().len())
            .sum();
        let queued = self.flush_tx.len();
        if buffered > 0 || queued > 0 {
            warn!(
                "BulkInserter for table '{}' dropped without close(): {} buffered record(s) \
                 and {} queued batch(es) were not sent",
                self.table_name, buffered, queued
            );
        }
    }
}

/// Invariants of the routing snapshot.
///
/// The destination of a record depends on several things that have to agree
/// with one another. These pin the rules that keep them from disagreeing —
/// the reason the state is one immutable value rather than a set of fields.
#[cfg(test)]
mod routing_state_tests {
    use super::*;

    /// A stand-in record type; these tests exercise the routing state, which is
    /// generic over the record and never inspects one.
    #[derive(Default)]
    struct Rec;

    fn queues(n: usize) -> Vec<Arc<WorkerQueue<Rec>>> {
        (0..n).map(|_| Arc::new(WorkerQueue::new(1, 8))).collect()
    }

    fn url_cache(n: usize) -> Vec<WorkerUrlCache> {
        (0..n)
            .map(|i| WorkerUrlCache::new(Arc::from(format!("http://w{i}:9191").as_str())))
            .collect()
    }

    fn state(multi_head: bool, replicated: bool, n: usize) -> RoutingState<Rec> {
        RoutingState::new(
            multi_head,
            replicated,
            queues(n),
            vec![1; 8],
            (0..n).map(Some).collect(),
            url_cache(n),
            0,
        )
    }

    // ==================== The head-node rule ====================

    /// Stated once, in one place. It used to be written out at each construction
    /// site, with `use_head_node` then passed in and checked for consistency
    /// rather than derived.
    #[test]
    fn head_node_routing_is_derived_from_the_two_inputs() {
        assert!(RoutingState::<Rec>::use_head_node_for(false, false));
        assert!(RoutingState::<Rec>::use_head_node_for(false, true));
        assert!(RoutingState::<Rec>::use_head_node_for(true, true));
        assert!(!RoutingState::<Rec>::use_head_node_for(true, false));
    }

    #[test]
    fn a_replicated_table_routes_to_the_head_node_even_with_multi_head() {
        let s = state(true, true, 1);
        assert!(s.use_head_node);
        assert!(s.multi_head_enabled, "multi-head is still available");
    }

    #[test]
    fn multi_head_on_a_sharded_table_routes_to_workers() {
        let s = state(true, false, 3);
        assert!(!s.use_head_node);
        assert_eq!(s.queues.len(), 3);
    }

    // ==================== Constructible states are consistent ====================

    /// The insert path resolves head-node routing to `queues[0]`, so that has to
    /// be the only queue there is.
    #[test]
    #[should_panic(expected = "Head-node routing needs exactly one queue")]
    fn head_node_routing_cannot_carry_worker_queues() {
        let _ = state(false, false, 3);
    }

    #[test]
    #[should_panic(expected = "at least one queue")]
    fn a_routing_state_cannot_be_built_without_queues() {
        let _ = state(true, false, 0);
    }

    // ==================== Sharing on a mapping-only change ====================

    /// A new shard mapping describes the same destinations, so the records
    /// already buffered must stay exactly where they are. Cloning the queues
    /// instead of sharing them would split them across two states, and the
    /// records in the old one would never be flushed.
    #[test]
    fn a_new_shard_mapping_shares_the_very_same_queues() {
        let before = state(true, false, 3);

        // Buffer a record so there is something to lose.
        before.queues[1].stripes[0].lock().push(Arc::new(Rec));

        let after = before.with_routing_table(vec![2; 8]);

        assert_eq!(after.routing_table, vec![2; 8]);
        assert_eq!(
            after.queues[1].stripes[0].lock().len(),
            1,
            "the buffered record must still be reachable through the new state"
        );
        for i in 0..3 {
            assert!(
                Arc::ptr_eq(&before.queues[i], &after.queues[i]),
                "queue {i} must be the same object, not a copy"
            );
        }
    }

    #[test]
    fn a_new_shard_mapping_carries_the_routing_mode_over() {
        let before = state(true, true, 1);
        let after = before.with_routing_table(vec![1; 4]);

        assert_eq!(after.use_head_node, before.use_head_node);
        assert_eq!(after.multi_head_enabled, before.multi_head_enabled);
        assert_eq!(after.table_replicated, before.table_replicated);
        assert_eq!(after.version, before.version + 1);
    }
}

/// Which insert failures are retried and can trigger a failover: a server
/// exit or a connection failure.
#[cfg(test)]
mod retry_policy_tests {
    use super::*;

    #[test]
    fn exits_and_connection_failures_are_retried() {
        assert!(is_connection_error(&KineticaError::Exit("Kinetica is exiting".into())));
        assert!(is_connection_error(&KineticaError::Connection("refused".into())));
    }

    #[test]
    fn other_failures_are_not() {
        for e in [
            // A submit failure: the server may already have applied the insert.
            KineticaError::Http("Failed to read response".into()),
            // A server-side data error: retrying cannot help.
            KineticaError::Api("Invalid value for column".into()),
            KineticaError::Unauthorized("401".into()),
            KineticaError::Avro("encode".into()),
            KineticaError::Config("SSL".into()),
        ] {
            assert!(!is_connection_error(&e), "{e:?} must not be retried");
        }
    }
}

/// Which queues a flush takes, for `flush()` (all non-empty queues) and for
/// the timed flush with `FlushOptions::flush_when_full` (full queues only).
#[cfg(test)]
mod flush_selection_tests {
    use super::*;

    #[test]
    fn a_full_flush_takes_every_non_empty_queue() {
        assert!(!should_take_queue(0, 10, false));
        assert!(should_take_queue(1, 10, false));
        assert!(should_take_queue(10, 10, false));
    }

    #[test]
    fn a_flush_when_full_takes_only_full_queues() {
        assert!(!should_take_queue(0, 10, true));
        assert!(!should_take_queue(9, 10, true));
        assert!(should_take_queue(10, 10, true));
        assert!(should_take_queue(11, 10, true));
    }

    #[test]
    fn flush_workers_default_to_the_cpu_count() {
        assert_eq!(flush_worker_count(0, 12), 12);
        assert_eq!(flush_worker_count(0, 0), 1);
    }

    #[test]
    fn a_requested_worker_count_is_used_as_given() {
        // No longer raised to at least 4, nor capped at twice the CPU count.
        assert_eq!(flush_worker_count(1, 12), 1);
        assert_eq!(flush_worker_count(2, 12), 2);
        assert_eq!(flush_worker_count(64, 4), 64);
    }

    #[test]
    fn flush_when_full_defaults_to_true() {
        assert!(FlushOptions::default().flush_when_full);
        assert_eq!(FlushOptions::default().flush_interval, FlushOptions::NO_PERIODIC_FLUSH);
        assert_eq!(FlushOptions::new(false, -5).flush_interval, FlushOptions::NO_PERIODIC_FLUSH);
    }
}

/// Per-record errors and the "stop on error" rule, as the inserter reads and
/// applies them.
#[cfg(test)]
mod individual_error_tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> BulkInserterOptions {
        let mut o = BulkInserterOptions::new();
        for (k, v) in pairs {
            o.set_option(k, v);
        }
        o
    }

    #[test]
    fn stop_on_error_needs_individual_errors() {
        assert!(!opts(&[]).requires_individual_error_handling());
        assert!(opts(&[("return_individual_errors", "true")]).requires_individual_error_handling());
        // Matched exactly.
        assert!(!opts(&[("return_individual_errors", "TRUE")]).requires_individual_error_handling());
    }

    #[test]
    fn a_partial_batch_is_not_stop_on_error() {
        let o = opts(&[("return_individual_errors", "true"), ("allow_partial_batch", "true")]);
        assert!(!o.requires_individual_error_handling());
    }

    #[test]
    fn error_handling_decides_when_given() {
        let abort = opts(&[("return_individual_errors", "true"), ("error_handling", "Abort")]);
        assert!(abort.requires_individual_error_handling());
        // error_handling overrides allow_partial_batch.
        let abort_partial = opts(&[
            ("return_individual_errors", "true"),
            ("allow_partial_batch", "true"),
            ("error_handling", "abort"),
        ]);
        assert!(abort_partial.requires_individual_error_handling());
        let skip = opts(&[("return_individual_errors", "true"), ("error_handling", "skip")]);
        assert!(!skip.requires_individual_error_handling());
    }

    #[test]
    fn errors_and_warnings_are_read_from_info_in_record_order() {
        let info: HashMap<String, String> = [
            ("error_10", "bad ten"),
            ("ERROR_2", "bad two"),
            ("warning_b", "warn b"),
            ("warning_a", "warn a"),
            ("count", "3"),
            ("data_rerouted", "false"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let (errors, warnings) = record_messages_from_info(&info, &[]);
        assert_eq!(errors, vec![(Some(2), "bad two".to_string()), (Some(10), "bad ten".to_string())]);
        assert_eq!(warnings, vec!["warn a", "warn b"]);
    }

    // One error per record index, the client's message for a record it could
    // not encode replacing the server's for it.
    #[test]
    fn records_that_did_not_encode_are_reported_in_record_order() {
        let info: HashMap<String, String> = [("error_1", "server one"), ("error_4", "server four")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let failures = [(2, "client two".to_string()), (1, "client one".to_string())];

        let (errors, _) = record_messages_from_info(&info, &failures);
        assert_eq!(
            errors,
            vec![
                (Some(1), "client one".to_string()),
                (Some(2), "client two".to_string()),
                (Some(4), "server four".to_string())
            ]
        );
    }
}

#[cfg(test)]
mod request_schema_tests {
    use super::*;
    use crate::KineticaVersion;

    #[test]
    fn the_base_definition_is_written_without_properties() {
        let schema = r#"{"type":"record","name":"type_name","fields":[
            {"name":"id","type":"int"},{"name":"name","type":["string","null"]},{"name":"b","type":"bytes"}]}"#;
        assert_eq!(
            base_definition(schema).unwrap(),
            r#"[["id","int"],["name","string","nullable"],["b","bytes"]]"#
        );
    }

    // `REQUEST_SCHEMA_VERSION` is 7.2.3.18 with no build, so every build of
    // it takes the definition.
    #[test]
    fn servers_from_7_2_3_18_take_the_definition() {
        let takes = |v: KineticaVersion| REQUEST_SCHEMA_VERSION <= v;
        assert!(takes(KineticaVersion::with_build(7, 2, 3, 18, 20260101)));
        assert!(takes(KineticaVersion::new(7, 2, 4, 0)));
        assert!(!takes(KineticaVersion::with_build(7, 2, 3, 17, 99999999)));
    }
}

/// The timed flush, against a stand-in server.
#[cfg(test)]
mod timed_flush_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{HasPermissionResponse, KineticaOptions, ShowTableResponse};
    use std::time::Duration;

    /// A one-cluster server taking inserts into a table `t` of one int.
    async fn inserter(host: &str, flush_options: FlushOptions) -> (fake_server::FakeServer, Arc<BulkInserter<GenericRecord>>, Arc<Type>) {
        let record_type = Arc::new(Type::new("t", vec![Column::new("id", ColumnType::Integer)]).unwrap());
        let schema = record_type.schema_string().to_string();
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::new()],
                        ..Default::default()
                    },
                ),
                "/insert/records" => encode(
                    &crate::generated::INSERT_RECORDS_RESPONSE_SCHEMA,
                    &InsertRecordsResponse { count_inserted: 1, ..Default::default() },
                ),
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        let mut inserter_options = BulkInserterOptions::new();
        inserter_options.set_flush_options(flush_options);
        let schema = record_type.schema_string().to_string();
        let inserter = BulkInserter::<GenericRecord>::with_schema(k, "t".into(), 100, 1, Some(inserter_options), &schema)
            .await
            .expect("inserter");
        (server, inserter, record_type)
    }

    fn record(record_type: &Arc<Type>, id: i32) -> Arc<GenericRecord> {
        let mut record = GenericRecord::new(record_type.clone());
        record.set(0, RecordValue::Int(id)).unwrap();
        Arc::new(record)
    }

    // Setting the timed flush options: the timer stops, and starts again with
    // the new options, each run reading them.
    #[tokio::test]
    async fn the_timed_flush_can_be_stopped_and_restarted() {
        let (server, inserter, record_type) = inserter("127.0.0.111", FlushOptions::new(false, 1)).await;
        let inserts = || server.behavior.count("/insert/records");
        assert!(inserter.is_timed_flush_running());

        inserter.insert(record(&record_type, 1)).unwrap();
        assert!(crate::test_wait::eventually(|| inserts() == 1).await, "the timer sent the partial batch");

        inserter.set_timed_flush_options(None);
        assert!(!inserter.is_timed_flush_running());
        assert_eq!(inserter.timed_flush_options(), FlushOptions::default());
        inserter.insert(record(&record_type, 2)).unwrap();
        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert_eq!(inserts(), 1, "no timer, nothing sent");

        inserter.set_timed_flush_options(Some(FlushOptions::new(true, 1)));
        assert!(inserter.is_timed_flush_running());
        tokio::time::sleep(Duration::from_millis(1600)).await;
        assert_eq!(inserts(), 1, "flush_when_full sends only full queues");

        let mut options = inserter.timed_flush_options();
        options.set_flush_when_full(false);
        inserter.set_timed_flush_options(Some(options));
        assert!(crate::test_wait::eventually(|| inserts() == 2).await, "the timer sends a partial batch again");
        inserter.close().await.unwrap();
    }
}

/// A queue's batches reach the server in the order they were dispatched,
/// where the table's primary key makes the order matter.
#[cfg(test)]
mod batch_order_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{HasPermissionResponse, KineticaOptions, RawInsertRecordsRequest, ShowTableResponse};
    use std::time::Duration;

    /// The second id of each batch, in the order the server finished them.
    type Finished = Arc<parking_lot::Mutex<Vec<i32>>>;

    /// Sends `ids`, two to a batch, into a table `t` whose `id` has
    /// `properties`, through one queue, the server holding back the batch
    /// whose second id is 1; returns the order the server finished the
    /// batches in, by their second ids.
    /// How the server holds the batch labelled 1 before answering it.
    #[derive(Clone, Copy)]
    enum Hold {
        /// For a fixed time, in which a batch that must wait for it would
        /// otherwise overtake it.
        For(Duration),
        /// Until this many other batches have been answered, or five seconds
        /// at most: batches that go at once all overtake it, however slowly
        /// they are sent.
        UntilAnswered(usize),
    }

    async fn finished_order(host: &str, properties: Vec<String>, ids: &[i32], hold: Hold) -> Vec<i32> {
        let record_type =
            Arc::new(Type::new("t", vec![Column::with_properties("id", ColumnType::Integer, properties.clone())]).unwrap());
        let schema = record_type.schema_string().to_string();
        let table_schema = schema.clone();
        let finished: Finished = Default::default();
        let log = finished.clone();
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![table_schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::from([("id".to_string(), properties.clone())])],
                        ..Default::default()
                    },
                ),
                "/insert/records" => {
                    let request: RawInsertRecordsRequest =
                        serde_avro_fast::from_datum_slice(body, &crate::generated::INSERT_RECORDS_REQUEST_SCHEMA).unwrap();
                    // The second id, a zig-zag varint of one byte here
                    let label = i32::from(request.list[1][0] >> 1);
                    if label == 1 {
                        match hold {
                            Hold::For(time) => std::thread::sleep(time),
                            Hold::UntilAnswered(count) => {
                                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                                while log.lock().len() < count && std::time::Instant::now() < deadline {
                                    std::thread::sleep(Duration::from_millis(5));
                                }
                            }
                        }
                    }
                    log.lock().push(label);
                    encode(
                        &crate::generated::INSERT_RECORDS_RESPONSE_SCHEMA,
                        &InsertRecordsResponse { count_inserted: request.list.len() as i32, ..Default::default() },
                    )
                }
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(2000).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        let inserter = BulkInserter::<GenericRecord>::with_schema(k, "t".into(), 2, 4, None, &schema).await.expect("inserter");
        for id in ids {
            let mut record = GenericRecord::new(record_type.clone());
            record.set(0, RecordValue::Int(*id)).unwrap();
            inserter.insert(Arc::new(record)).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), inserter.flush()).await.expect("no batch waits forever").unwrap();
        inserter.close().await.unwrap();
        assert_eq!(inserter.count_inserted(), ids.len() as i64);
        let order = finished.lock().clone();
        order
    }

    // With a primary key, batches holding one key arrive in order, each
    // waiting for the one before it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batches_sharing_a_primary_key_arrive_in_order() {
        let held = Hold::For(Duration::from_millis(300));
        let order = finished_order("127.0.0.164", vec!["primary_key".into()], &[0, 1, 0, 2, 0, 3], held).await;
        assert_eq!(order, [1, 2, 3]);
    }

    /// Batches of distinct keys go at once: the held one finishes last.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batches_of_distinct_keys_go_at_once() {
        let held = Hold::UntilAnswered(2);
        let order = finished_order("127.0.0.165", vec!["primary_key".into()], &[0, 1, 2, 3, 4, 5], held).await;
        assert_eq!(order.last(), Some(&1));
    }

    /// Without a primary key, the order changes no row, and batches go at
    /// once whatever they hold.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batches_without_a_primary_key_go_at_once() {
        let order = finished_order("127.0.0.166", Vec::new(), &[0, 1, 0, 2, 0, 3], Hold::UntilAnswered(2)).await;
        assert_eq!(order.last(), Some(&1));
    }
}

/// JSON mode: records as JSON text, routed to a random shard's rank and sent
/// as one array per batch to `/insert/records/json`.
#[cfg(test)]
mod json_tests {
    use super::*;
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{AdminShowShardsResponse, HasPermissionResponse, KineticaOptions, ShowTableResponse};

    /// Answers the head node's calls for a table `t` of one int, and the
    /// shard mapping `ranks`.
    fn head_handler(server: &fake_server::FakeServer, ranks: Vec<i32>) {
        let schema = r#"{"type":"record","name":"t","fields":[{"name":"id","type":"int"}]}"#.to_string();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::new()],
                        ..Default::default()
                    },
                ),
                "/admin/show/shards" => encode(
                    &crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA,
                    &AdminShowShardsResponse { version: 1, rank: ranks.clone(), ..Default::default() },
                ),
                _ => return None,
            };
            Some(Ok(reply))
        }));
    }

    /// The batches a server took at `/insert/records/json`, with their
    /// queries, each answered with `answer`, or with the count of its records.
    type Sent = Arc<parking_lot::Mutex<Vec<(String, String)>>>;
    fn json_handler(server: &fake_server::FakeServer, answer: Option<(&'static str, &'static str)>) -> Sent {
        let sent: Sent = Default::default();
        let log = sent.clone();
        *server.behavior.json_handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            let query = path.strip_prefix("/insert/records/json?")?;
            let body = String::from_utf8_lossy(body).into_owned();
            let count = serde_json::from_str::<Vec<serde_json::Value>>(&body).map_or(0, |records| records.len());
            log.lock().push((query.to_string(), body));
            Some(match answer {
                Some((status, json)) => (status.to_string(), json.to_string()),
                None => (
                    "200 OK".to_string(),
                    format!(r#"{{"status":"OK","message":"","data":{{"count_inserted":{count},"count_updated":0}}}}"#),
                ),
            })
        }));
        sent
    }

    async fn client(server: &fake_server::FakeServer) -> Arc<Kinetica> {
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"))
    }

    fn records(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|t| t.to_string()).collect()
    }

    // Each worker takes its batches as JSON arrays at its own
    // /insert/records/json, with the options and the table name in the query,
    // and the counts come from the answers.
    #[tokio::test]
    async fn json_records_go_to_random_shards_as_arrays() {
        let head = fake_server::start("127.0.0.121").await;
        let (w1, w2) = (fake_server::start("127.0.0.122").await, fake_server::start("127.0.0.123").await);
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&w1), &address(&w2)]);
        head_handler(&head, vec![1, 2, 1, 2]);
        let (sent1, sent2) = (json_handler(&w1, None), json_handler(&w2, None));
        let k = client(&head).await;

        let mut options = BulkInserterOptions::new();
        options.set_update_on_existing_pk(true);
        let inserter = BulkInserter::with_json_options(k, "t".into(), 5, 1, Some(options), JsonOptions::default())
            .await
            .expect("inserter");
        assert!(inserter.is_multi_head_enabled());
        assert_eq!(inserter.json_options(), Some(JsonOptions::default()));

        let texts: Vec<String> = (0..100).map(|i| format!(r#"{{"id":{i}}}"#)).collect();
        inserter.insert_batch(texts.clone()).unwrap();
        inserter.flush().await.unwrap();
        assert_eq!(inserter.count_inserted(), 100);
        assert!(inserter.drain_errors().is_empty());

        let (sent1, sent2) = (sent1.lock().clone(), sent2.lock().clone());
        assert!(!sent1.is_empty() && !sent2.is_empty(), "both ranks take records");
        let mut received: Vec<String> = Vec::new();
        for (query, body) in sent1.iter().chain(&sent2) {
            let mut query: Vec<&str> = query.split('&').collect();
            query.sort_unstable();
            assert_eq!(query, ["table_name=t", "update_on_existing_pk=true"], "no request_schema_str without a type");
            assert!(body.starts_with("[{") && body.ends_with("}]"));
            let batch: Vec<serde_json::Value> = serde_json::from_str(body).unwrap();
            received.extend(batch.iter().map(|r| r.to_string()));
        }
        received.sort();
        let mut expected = texts;
        expected.sort();
        assert_eq!(received, expected);
        assert_eq!(head.behavior.count("/insert/records"), 0);
        inserter.close().await.unwrap();
    }

    // With validation asked for, a record must be a JSON object, read
    // leniently, and a list is checked whole before any of it is queued.
    #[tokio::test]
    async fn json_records_are_checked_when_asked() {
        let head = fake_server::start("127.0.0.124").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);
        head_handler(&head, Vec::new());
        let sent = json_handler(&head, None);
        let k = client(&head).await;

        let inserter = BulkInserter::with_json_options(k, "t".into(), 10, 1, None, JsonOptions::new(true, true))
            .await
            .expect("inserter");
        assert!(inserter.is_using_head_node());

        let refused = inserter.insert(Arc::new("[1]".to_string())).unwrap_err();
        assert_eq!(refused.to_string(), "API error: String passed in is Not a valid JSON record");
        assert_eq!(refused.records, [Arc::new("[1]".to_string())], "the record comes back");
        let refused = inserter.insert_batch(records(&["{a:1}", "{\"a\":1,\"a\":2}"])).unwrap_err();
        assert_eq!(refused.to_string(), "API error: List passed in is not a list of valid JSON strings");
        assert_eq!(refused.records.len(), 2, "the whole list comes back");
        inserter.insert(Arc::new("{a:'lenient'}".to_string())).unwrap();
        assert_eq!(inserter.buffered_record_count(), 1, "nothing of the refused list was queued");

        inserter.set_json_options(JsonOptions::new(false, true));
        inserter.insert(Arc::new("not json".to_string())).unwrap();
        inserter.flush().await.unwrap();
        assert_eq!(sent.lock()[0].1, "[{a:'lenient'},not json]");
        inserter.close().await.unwrap();
    }

    // A batch the server fails is reported, with the server's message.
    #[tokio::test]
    async fn a_failed_json_batch_is_reported() {
        let head = fake_server::start("127.0.0.125").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);
        head_handler(&head, Vec::new());
        json_handler(&head, Some(("400 Bad Request", r#"{"status":"ERROR","message":"bad record"}"#)));
        let inserter = BulkInserter::new_json(client(&head).await, "t".into(), 10, 1).await.expect("inserter");

        inserter.insert_batch(records(&[r#"{"id":1}"#, r#"{"id":2}"#])).unwrap();
        inserter.flush().await.unwrap();
        let errors = inserter.drain_errors();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].message, "Error inserting <2> JSON objects: bad record");
        assert_eq!(errors[0].record_count, 2);
        assert_eq!(errors[0].url, Some(head.url.clone()), "the head node URL");
        assert!(errors[0].worker_url.starts_with(head.url.as_str().trim_end_matches('/')), "{}", errors[0].worker_url);
        assert_eq!(inserter.count_inserted(), 0);
        inserter.close().await.unwrap();
    }

    // A JSON batch that meets an exit is retried as any other batch.
    #[tokio::test]
    async fn a_json_batch_meeting_an_exit_is_retried() {
        let head = fake_server::start("127.0.0.126").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);
        head_handler(&head, Vec::new());
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = attempts.clone();
        *head.behavior.json_handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            path.starts_with("/insert/records/json?").then(|| {
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    ("503 Service Unavailable".to_string(), "{}".to_string())
                } else {
                    ("200 OK".to_string(), r#"{"status":"OK","data":{"count_inserted":1,"count_updated":0}}"#.to_string())
                }
            })
        }));
        let inserter = BulkInserter::new_json(client(&head).await, "t".into(), 10, 1).await.expect("inserter");

        inserter.insert(Arc::new(r#"{"id":1}"#.to_string())).unwrap();
        inserter.flush().await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(inserter.count_inserted(), 1);
        assert!(inserter.drain_errors().is_empty());
        inserter.close().await.unwrap();
    }

    #[test]
    fn json_answers_are_read_for_their_counts() {
        let read = |json: &str| json_insert_counts(serde_json::from_str::<serde_json::Value>(json).unwrap().as_object().unwrap(), 3);
        assert_eq!(read(r#"{"status":"ok","data":{"count_inserted":2,"count_updated":1}}"#).unwrap(), (2, 1));
        assert_eq!(
            read(r#"{"status":"ERROR"}"#).unwrap_err().to_string(),
            "API error: Error inserting <3> JSON objects: (no message)"
        );
        assert!(read(r#"{"status":"OK","data":{"count_inserted":3000000000,"count_updated":0}}"#)
            .unwrap_err()
            .to_string()
            .contains("`data.count_inserted` must be i32; found i64"));
        assert!(read(r#"{"message":"x"}"#).is_err());
    }
}

/// When batches leave: one producer fills one stripe per rank, so a rank's
/// batch is sent once `batch_size` of its records are in, and no batch is
/// larger than that.
#[cfg(test)]
mod batching_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{AdminShowShardsResponse, HasPermissionResponse, KineticaOptions, ShowTableResponse};

    const BATCH_SIZE: usize = 10;

    /// The record counts of the batches a rank took, in order.
    type Batches = Arc<parking_lot::Mutex<Vec<usize>>>;

    /// A rank taking batches at `/insert/records`, Snappy-compressed if
    /// `snappy`, recording each one's size.
    fn rank_handler(server: &fake_server::FakeServer, snappy: bool) -> Batches {
        let batches: Batches = Default::default();
        let log = batches.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if path != "/insert/records" {
                return None;
            }
            let body = if snappy { snap::raw::Decoder::new().decompress_vec(body).unwrap() } else { body.to_vec() };
            let request: RawInsertRecordsRequest =
                serde_avro_fast::from_datum_slice(&body, &crate::generated::INSERT_RECORDS_REQUEST_SCHEMA).unwrap();
            assert_eq!((request.table_name.as_str(), request.list_encoding.as_str()), ("t", "binary"));
            let count = request.list.len();
            log.lock().push(count);
            Some(Ok(encode(
                &crate::generated::INSERT_RECORDS_RESPONSE_SCHEMA,
                &InsertRecordsResponse { count_inserted: count as i32, ..Default::default() },
            )))
        }));
        batches
    }

    /// A cluster of a head node and two ranks holding a table `t` sharded on
    /// its one int column `id`, and an inserter into it, over a connection
    /// using Snappy if `snappy`.
    async fn cluster(
        hosts: [&str; 3],
        snappy: bool,
    ) -> (Vec<fake_server::FakeServer>, [Batches; 2], Arc<BulkInserter<GenericRecord>>, Arc<Type>) {
        let record_type = Arc::new(
            Type::new("t", vec![Column::with_properties("id", ColumnType::Integer, vec!["shard_key".into()])]).unwrap(),
        );
        let head = fake_server::start(hosts[0]).await;
        let (w1, w2) = (fake_server::start(hosts[1]).await, fake_server::start(hosts[2]).await);
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&w1), &address(&w2)]);
        let schema = record_type.schema_string().to_string();
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::from([("id".to_string(), vec!["shard_key".to_string()])])],
                        ..Default::default()
                    },
                ),
                "/admin/show/shards" => encode(
                    &crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA,
                    &AdminShowShardsResponse { version: 1, rank: vec![1, 2, 1, 2, 2, 1, 2, 1], ..Default::default() },
                ),
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let batches = [rank_handler(&w1, snappy), rank_handler(&w2, snappy)];

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(snappy);
        let k = Arc::new(Kinetica::new(vec![head.url.to_string()], options).await.expect("client"));
        let inserter = BulkInserter::<GenericRecord>::with_schema(k, "t".into(), BATCH_SIZE, 2, None, record_type.schema_string())
            .await
            .expect("inserter");
        assert_eq!(inserter.worker_count(), 2);
        assert_eq!(inserter.shard_column_names(), ["id"]);
        (vec![head, w1, w2], batches, inserter, record_type)
    }

    fn records(record_type: &Arc<Type>, ids: std::ops::Range<i32>) -> Vec<GenericRecord> {
        ids.map(|id| {
            let mut record = GenericRecord::new(record_type.clone());
            record.set(0, RecordValue::Int(id)).unwrap();
            record
        })
        .collect()
    }

    /// Every batch the ranks took: none over the batch size, and at most one
    /// short one per rank, sent by the flush.
    fn check_batches(batches: &[Batches; 2], total: usize) {
        let sizes: Vec<Vec<usize>> = batches.iter().map(|b| b.lock().clone()).collect();
        assert_eq!(sizes.iter().flatten().sum::<usize>(), total, "{sizes:?}");
        for rank in &sizes {
            assert!(rank.iter().all(|&n| n <= BATCH_SIZE), "no batch over the batch size: {sizes:?}");
            assert!(rank.iter().filter(|&&n| n < BATCH_SIZE).count() <= 1, "one partial batch per rank: {sizes:?}");
        }
    }

    #[tokio::test]
    async fn one_producer_sends_a_rank_s_batch_once_it_holds_the_batch_size() {
        let (_servers, batches, inserter, record_type) = cluster(["127.0.0.181", "127.0.0.182", "127.0.0.183"], false).await;

        for record in records(&record_type, 0..100) {
            inserter.insert(Arc::new(record)).unwrap();
        }
        // Everything but less than a batch per rank has been handed off; with
        // the records spread over a stripe per CPU, far more stayed behind
        assert!(
            inserter.buffered_record_count() < 2 * BATCH_SIZE,
            "{} records still buffered",
            inserter.buffered_record_count()
        );

        inserter.flush().await.unwrap();
        assert_eq!(inserter.count_inserted(), 100);
        check_batches(&batches, 100);
        inserter.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_list_is_cut_into_batches_of_the_batch_size() {
        let (_servers, batches, inserter, record_type) = cluster(["127.0.0.184", "127.0.0.185", "127.0.0.186"], false).await;

        inserter.insert_batch(records(&record_type, 0..95)).unwrap();
        assert!(inserter.buffered_record_count() < 2 * BATCH_SIZE);
        inserter
            .insert_batch_arc(records(&record_type, 95..150).into_iter().map(Arc::new).collect())
            .unwrap();
        assert!(inserter.buffered_record_count() < 2 * BATCH_SIZE);

        inserter.flush().await.unwrap();
        assert_eq!(inserter.count_inserted(), 150);
        check_batches(&batches, 150);
        inserter.close().await.unwrap();
    }

    // Where the server cannot be reached when the inserter fetches the shard
    // mapping, the inserter is built without one. A record that needs the
    // mapping is then refused.
    #[tokio::test]
    async fn an_unreachable_shard_mapping_does_not_fail_the_construction() {
        let record_type = Arc::new(
            Type::new("t", vec![Column::with_properties("id", ColumnType::Integer, vec!["shard_key".into()])]).unwrap(),
        );
        let head = fake_server::start("127.0.0.206").await;
        let (w1, w2) = (fake_server::start("127.0.0.207").await, fake_server::start("127.0.0.208").await);
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&w1), &address(&w2)]);
        let schema = record_type.schema_string().to_string();
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::from([("id".to_string(), vec!["shard_key".to_string()])])],
                        ..Default::default()
                    },
                ),
                // The server going down: an exit, which is a connection failure
                "/admin/show/shards" => return Some(Err("Kinetica is exiting".to_string())),
                _ => return None,
            };
            Some(Ok(reply))
        }));
        rank_handler(&w1, false);
        rank_handler(&w2, false);

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![head.url.to_string()], options).await.expect("client"));
        let inserter = BulkInserter::<GenericRecord>::with_schema(k, "t".into(), BATCH_SIZE, 2, None, record_type.schema_string())
            .await
            .expect("built without a shard mapping");
        assert_eq!(inserter.worker_count(), 2);

        let refused = inserter.insert(Arc::new(records(&record_type, 0..1).remove(0))).unwrap_err();
        assert_eq!(
            refused.error.message(),
            "No shard mapping is available for table 't'; cannot route the record to a worker rank.  Maybe need to \
             update the shard mapping."
        );
        inserter.close().await.unwrap();
    }

    // The rank mapping keeps a slot for a removed rank, so a cluster whose
    // rank 1 was removed still has two slots, and the shard mapping decides:
    // a record of a shard on the removed rank is refused, not sent to the
    // rank left.
    #[tokio::test]
    async fn a_removed_rank_still_counts_toward_the_single_queue_shortcut() {
        let record_type = Arc::new(
            Type::new("t", vec![Column::with_properties("id", ColumnType::Integer, vec!["shard_key".into()])]).unwrap(),
        );
        let head = fake_server::start("127.0.0.209").await;
        let w2 = fake_server::start("127.0.0.210").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&w2)]);
        let schema = record_type.schema_string().to_string();
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::from([("id".to_string(), vec!["shard_key".to_string()])])],
                        ..Default::default()
                    },
                ),
                "/admin/show/shards" => encode(
                    &crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA,
                    &AdminShowShardsResponse { version: 1, rank: vec![1, 2, 1, 2, 2, 1, 2, 1], ..Default::default() },
                ),
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let taken = rank_handler(&w2, false);

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![head.url.to_string()], options).await.expect("client"));
        let mut inserter_options = BulkInserterOptions::new();
        inserter_options.set_worker_list(Some(WorkerList::with_urls(vec![None, Some(w2.url.clone())])));
        let inserter = BulkInserter::<GenericRecord>::with_schema(
            k, "t".into(), BATCH_SIZE, 2, Some(inserter_options), record_type.schema_string(),
        )
        .await
        .expect("inserter");

        let mut refused = 0;
        for record in records(&record_type, 0..40) {
            if let Err(e) = inserter.insert(Arc::new(record)) {
                assert_eq!(
                    e.error.message(),
                    "Attempted to insert into worker rank that has been removed!  Maybe need to update the shard \
                     mapping."
                );
                refused += 1;
            }
        }
        inserter.flush().await.unwrap();
        let sent: usize = taken.lock().iter().sum();
        assert!(refused > 0, "some records belong to the removed rank");
        assert_eq!(sent + refused, 40, "the rest went to rank 2");
        inserter.close().await.unwrap();
    }

    // insert_batch: the records before the first that cannot be routed are
    // queued, and that one and every one after it come back with the error,
    // for the caller to retry. insert gives the record back the same way.
    #[tokio::test]
    async fn a_batch_queues_up_to_the_first_record_that_cannot_be_routed() {
        let (_servers, batches, inserter, record_type) = cluster(["127.0.0.198", "127.0.0.199", "127.0.0.200"], false).await;
        let mut list = records(&record_type, 0..6);
        list[3].set(0, RecordValue::String("not an int".into())).unwrap();

        let refused = inserter.insert_batch(list).unwrap_err();
        assert_eq!(
            refused.error.message(),
            "Unable to calculate shard/primary key; please check data for unshardable values"
        );
        let ids: Vec<Option<RecordValue>> = refused.records.iter().map(|r| r.get(0).cloned()).collect();
        assert_eq!(
            ids,
            [Some(RecordValue::String("not an int".into())), Some(RecordValue::Int(4)), Some(RecordValue::Int(5))],
            "the record that could not be routed, and every one after it"
        );
        assert_eq!(inserter.buffered_record_count(), 3, "the three before it are queued");

        let mut bad = GenericRecord::new(record_type.clone());
        bad.set(0, RecordValue::String("x".into())).unwrap();
        let refused = inserter.insert(Arc::new(bad)).unwrap_err();
        assert_eq!(refused.records.len(), 1, "the record comes back");
        let as_kinetica: KineticaError = refused.into();
        assert!(matches!(as_kinetica, KineticaError::Api(_)), "{as_kinetica:?}");

        inserter.flush().await.unwrap();
        assert_eq!(inserter.count_inserted(), 3);
        check_batches(&batches, 3);
        inserter.close().await.unwrap();
    }

    // The request is built and compressed off the async runtime, and arrives
    // Snappy-compressed when the connection uses Snappy, with the records
    // intact.
    #[tokio::test]
    async fn with_snappy_each_batch_arrives_compressed() {
        let (_servers, batches, inserter, record_type) = cluster(["127.0.0.187", "127.0.0.188", "127.0.0.189"], true).await;

        inserter.insert_batch(records(&record_type, 0..100)).unwrap();
        inserter.flush().await.unwrap();
        assert_eq!(inserter.count_inserted(), 100);
        assert!(inserter.drain_errors().is_empty());
        check_batches(&batches, 100);
        inserter.close().await.unwrap();
    }
}

/// A record that does not encode: sent empty in its place while the rest of
/// the batch goes in, and reported on its own with the client's message.
#[cfg(test)]
mod encode_failure_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{HasPermissionResponse, KineticaOptions, ShowTableResponse};

    #[tokio::test]
    async fn a_record_that_does_not_encode_fails_alone() {
        let record_type = Arc::new(
            Type::new("t", vec![Column::new("id", ColumnType::Integer), Column::new("name", ColumnType::String)]).unwrap(),
        );
        let schema = record_type.schema_string().to_string();
        let server = fake_server::start("127.0.0.191").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::new()],
                        ..Default::default()
                    },
                ),
                // As the server does with return_individual_errors: an
                // empty record is an error of its own, the rest go in
                "/insert/records" => {
                    let request: RawInsertRecordsRequest =
                        serde_avro_fast::from_datum_slice(body, &crate::generated::INSERT_RECORDS_REQUEST_SCHEMA).unwrap();
                    assert_eq!(request.options.get("return_individual_errors").map(String::as_str), Some("true"));
                    let info: HashMap<String, String> = request
                        .list
                        .iter()
                        .enumerate()
                        .filter(|(_, record)| record.is_empty())
                        .map(|(i, _)| (format!("error_{i}"), "Failed to decode record".to_string()))
                        .collect();
                    let inserted = request.list.len() - info.len();
                    encode(
                        &crate::generated::INSERT_RECORDS_RESPONSE_SCHEMA,
                        &InsertRecordsResponse { count_inserted: inserted as i32, info, ..Default::default() },
                    )
                }
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        let mut inserter_options = BulkInserterOptions::new();
        inserter_options.set_return_individual_errors(true);
        let inserter = BulkInserter::<GenericRecord>::with_schema(
            k, "t".into(), 100, 1, Some(inserter_options), record_type.schema_string(),
        )
        .await
        .expect("inserter");

        let record = |id: i32, name: RecordValue| {
            let mut record = GenericRecord::new(record_type.clone());
            record.set(0, RecordValue::Int(id)).unwrap();
            record.set(1, name).unwrap();
            record
        };
        inserter
            .insert_batch(vec![
                record(0, RecordValue::String("a".into())),
                record(1, RecordValue::Int(5)),
                record(2, RecordValue::String("c".into())),
                record(3, RecordValue::Null),
                record(4, RecordValue::String("e".into())),
            ])
            .unwrap();
        inserter.flush().await.unwrap();

        assert_eq!(inserter.count_inserted(), 3, "the records that encoded went in");
        let drained = inserter.drain_errors();
        // Each error carries its record
        let ids: Vec<Vec<RecordValue>> =
            drained.iter().map(|e| e.records.iter().map(|r| r.get(0).cloned().unwrap()).collect()).collect();
        assert_eq!(ids, [vec![RecordValue::Int(1)], vec![RecordValue::Int(3)]]);
        let errors: Vec<(String, usize)> = drained.into_iter().map(|e| (e.message, e.record_count)).collect();
        assert_eq!(
            errors,
            vec![
                (
                    "Could not encode object: Field has incorrect data type in record :: value 5 (i32) \
                     cannot be cast to expected type string at type_name.name"
                        .to_string(),
                    1
                ),
                ("Could not encode object: record :: null value for (non-nullable) string at type_name.name".to_string(), 1),
            ]
        );

        // As with any record error under return_individual_errors, the inserter
        // now drops what it is given
        inserter.insert(Arc::new(record(5, RecordValue::String("f".into())))).unwrap();
        inserter.flush().await.unwrap();
        assert_eq!(inserter.count_inserted(), 3);
        assert_eq!(server.behavior.count("/insert/records"), 1);
        inserter.close().await.unwrap();
    }
}

/// The messages of batches that fail.
#[cfg(test)]
mod batch_failure_message_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{AdminShowShardsResponse, HasPermissionResponse, KineticaOptions, ShowTableResponse};

    #[test]
    fn a_workers_failure_is_described_by_its_cause() {
        assert_eq!(
            original_causes("http://w:9192/insert/records", &KineticaError::Exit("gone".into())),
            "[ worker URL http://w:9192/insert/records: Server exit: gone;  ]"
        );
        assert_eq!(
            original_causes("http://w:9192/insert/records", &KineticaError::caused("wrapped", KineticaError::Exit("gone".into()))),
            "[ worker URL http://w:9192/insert/records: Server exit: gone;  ]",
            "a failure with a cause is described by its cause"
        );
    }

    // A batch whose worker cannot be reached fails, once its retries are
    // spent on a lone cluster, with the causes its worker met.
    #[tokio::test]
    async fn a_batch_out_of_retries_fails_with_its_causes() {
        let record_type = Arc::new(Type::new("t", vec![Column::new("id", ColumnType::Integer)]).unwrap());
        let schema = record_type.schema_string().to_string();
        let head = fake_server::start("127.0.0.193").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::new()],
                        ..Default::default()
                    },
                ),
                "/admin/show/shards" => encode(
                    &crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA,
                    &AdminShowShardsResponse { version: 1, rank: vec![1], ..Default::default() },
                ),
                _ => return None,
            };
            Some(Ok(reply))
        }));

        // A worker address nothing listens on
        let closed = tokio::net::TcpListener::bind("127.0.0.194:0").await.unwrap();
        let worker = Url::parse(&format!("http://{}", closed.local_addr().unwrap())).unwrap();
        drop(closed);

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![head.url.to_string()], options).await.expect("client"));
        let mut inserter_options = BulkInserterOptions::new();
        inserter_options.set_worker_list(Some(WorkerList::with_urls(vec![Some(worker.clone())])));
        let inserter = BulkInserter::<GenericRecord>::with_schema(
            k, "t".into(), 10, 1, Some(inserter_options), record_type.schema_string(),
        )
        .await
        .expect("inserter");

        let mut record = GenericRecord::new(record_type.clone());
        record.set(0, RecordValue::Int(1)).unwrap();
        inserter.insert(Arc::new(record)).unwrap();
        inserter.flush().await.unwrap();

        let errors = inserter.drain_errors();
        assert_eq!(errors.len(), 1);
        let insert_url = format!("{}/insert/records", worker.as_str().trim_end_matches('/'));
        let prefix = format!(
            "Insertion failed; ran out of retries.  Original causes encountered by workers: \
             [ worker URL {insert_url}: Connection error: "
        );
        assert!(errors[0].message.starts_with(&prefix), "{}", errors[0].message);
        assert!(errors[0].message.ends_with(";  ]"), "{}", errors[0].message);
        assert_eq!(errors[0].record_count, 1);
        assert_eq!(errors[0].records.len(), 1, "the batch's records go with the error");
        assert_eq!(errors[0].records[0].get(0), Some(&RecordValue::Int(1)));
        inserter.close().await.unwrap();
    }

    // After a failover, the inserter rebuilds the worker queues for the
    // cluster now in use and, where that fails, fails the batch with the
    // rebuild's own message. Here the batch's worker cannot be reached, so
    // the inserter fails over from A to B, whose shard mapping cannot be read.
    #[tokio::test]
    async fn a_failed_rebuild_after_a_failover_fails_the_batch_with_its_message() {
        let record_type = Arc::new(Type::new("t", vec![Column::new("id", ColumnType::Integer)]).unwrap());
        let schema = record_type.schema_string().to_string();
        let handler = |shards_fail: bool| -> fake_server::Handler {
            let schema = schema.clone();
            Arc::new(move |path: &str, _: &[u8]| {
                let reply = match path {
                    "/has/permission" => encode(
                        &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                        &HasPermissionResponse { has_permission: true, ..Default::default() },
                    ),
                    "/show/table" => encode(
                        &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                        &ShowTableResponse {
                            table_name: "t".into(),
                            table_names: vec!["t".into()],
                            table_descriptions: vec![Vec::new()],
                            type_ids: vec!["tid".into()],
                            type_schemas: vec![schema.clone()],
                            type_labels: vec!["t".into()],
                            properties: vec![HashMap::new()],
                            ..Default::default()
                        },
                    ),
                    "/admin/show/shards" if shards_fail => return Some(Err("shards unavailable".to_string())),
                    "/admin/show/shards" => encode(
                        &crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA,
                        &AdminShowShardsResponse { version: 1, rank: vec![1], ..Default::default() },
                    ),
                    _ => return None,
                };
                Some(Ok(reply))
            })
        };
        let a = fake_server::start("127.0.0.195").await;
        let b = fake_server::start("127.0.0.196").await;
        *a.behavior.properties.lock() = cluster_properties(&address(&a), &[]);
        *b.behavior.properties.lock() = cluster_properties(&address(&b), &[]);
        *a.behavior.handler.lock() = Some(handler(false));
        *b.behavior.handler.lock() = Some(handler(true));

        // A worker address nothing listens on
        let closed = tokio::net::TcpListener::bind("127.0.0.197:0").await.unwrap();
        let worker = Url::parse(&format!("http://{}", closed.local_addr().unwrap())).unwrap();
        drop(closed);

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![a.url.to_string(), b.url.to_string()], options).await.expect("client"));
        assert_eq!(k.ha_ring_size(), 2);
        let mut inserter_options = BulkInserterOptions::new();
        inserter_options.set_worker_list(Some(WorkerList::with_urls(vec![Some(worker.clone())])));
        let inserter = BulkInserter::<GenericRecord>::with_schema(
            k.clone(), "t".into(), 10, 1, Some(inserter_options), record_type.schema_string(),
        )
        .await
        .expect("inserter");
        assert!(Arc::ptr_eq(inserter.kinetica(), &k), "the inserter's client");

        let mut record = GenericRecord::new(record_type.clone());
        record.set(0, RecordValue::Int(1)).unwrap();
        inserter.insert(Arc::new(record)).unwrap();
        inserter.flush().await.unwrap();

        assert_eq!(k.url(), Some(b.url.clone()), "failed over to B");
        let errors = inserter.drain_errors();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].message, "shards unavailable");
        assert_eq!(errors[0].record_count, 1);
        assert_eq!(errors[0].records.len(), 1, "the batch's records go with the error");
        inserter.close().await.unwrap();
    }

    // The constructor asks for both permissions an upserting inserter needs,
    // then refuses with one message naming the user.
    #[tokio::test]
    async fn a_missing_permission_is_refused_naming_the_user() {
        let server = fake_server::start("127.0.0.205").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let asked: Arc<parking_lot::Mutex<Vec<String>>> = Default::default();
        let log = asked.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if path != "/has/permission" {
                return None;
            }
            let request: HasPermissionRequest =
                serde_avro_fast::from_datum_slice(body, &crate::generated::HAS_PERMISSION_REQUEST_SCHEMA).unwrap();
            log.lock().push(request.permission.clone());
            Some(Ok(encode(
                &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                &HasPermissionResponse { has_permission: request.permission == "insert", ..Default::default() },
            )))
        }));

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_username("ann").set_password("p");
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        let record_type = Arc::new(Type::new("t", vec![Column::new("id", ColumnType::Integer)]).unwrap());
        let mut inserter_options = BulkInserterOptions::new();
        inserter_options.set_update_on_existing_pk(true);
        let err = BulkInserter::<GenericRecord>::with_schema(k, "t".into(), 10, 1, Some(inserter_options), record_type.schema_string())
            .await
            .err()
            .expect("refused");

        assert_eq!(err.message(), "User ann doesn't have requisite permissions on the table t to use BulkInserter");
        assert!(matches!(err, KineticaError::Api(_)), "an API error: {err:?}");
        assert_eq!(*asked.lock(), ["insert", "update"]);
    }
}

/// A shard rebalance that the server reports by rerouting a batch: the worker
/// queues are rebuilt for the ranks the cluster has now.
#[cfg(test)]
mod rebalance_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::{AdminShowShardsResponse, HasPermissionResponse, KineticaOptions, ShowTableResponse};

    /// A rank taking batches at `/insert/records`, counting the records it
    /// took, and reporting them rerouted while `rerouting` is set.
    fn rank_handler(server: &fake_server::FakeServer, rerouting: Arc<AtomicBool>) -> Arc<AtomicUsize> {
        let taken = Arc::new(AtomicUsize::new(0));
        let count = taken.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if path != "/insert/records" {
                return None;
            }
            let request: RawInsertRecordsRequest =
                serde_avro_fast::from_datum_slice(body, &crate::generated::INSERT_RECORDS_REQUEST_SCHEMA).unwrap();
            count.fetch_add(request.list.len(), Ordering::SeqCst);
            let info = if rerouting.load(Ordering::SeqCst) {
                HashMap::from([("data_rerouted".to_string(), "true".to_string())])
            } else {
                HashMap::new()
            };
            Some(Ok(encode(
                &crate::generated::INSERT_RECORDS_RESPONSE_SCHEMA,
                &InsertRecordsResponse { count_inserted: request.list.len() as i32, info, ..Default::default() },
            )))
        }));
        taken
    }

    fn records(record_type: &Arc<Type>, ids: std::ops::Range<i32>) -> Vec<GenericRecord> {
        ids.map(|id| {
            let mut record = GenericRecord::new(record_type.clone());
            record.set(0, RecordValue::Int(id)).unwrap();
            record
        })
        .collect()
    }

    // A rank joins a two-rank cluster and the shard mapping moves some shards
    // to it. The batches still routed by the old mapping come back rerouted,
    // and from then on the new rank takes its share. Rebuilding only the
    // mapping would name a rank with no queue, refusing its records.
    #[tokio::test]
    async fn rerouted_data_rebuilds_the_worker_queues() {
        let record_type = Arc::new(
            Type::new("t", vec![Column::with_properties("id", ColumnType::Integer, vec!["shard_key".into()])]).unwrap(),
        );
        let head = fake_server::start("127.0.0.201").await;
        let w1 = fake_server::start("127.0.0.202").await;
        let w2 = fake_server::start("127.0.0.203").await;
        let w3 = fake_server::start("127.0.0.204").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&w1), &address(&w2)]);

        let rebalanced = Arc::new(AtomicBool::new(false));
        let schema = record_type.schema_string().to_string();
        let shards_moved = rebalanced.clone();
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            let reply = match path {
                "/has/permission" => encode(
                    &crate::generated::HAS_PERMISSION_RESPONSE_SCHEMA,
                    &HasPermissionResponse { has_permission: true, ..Default::default() },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        table_descriptions: vec![Vec::new()],
                        type_ids: vec!["tid".into()],
                        type_schemas: vec![schema.clone()],
                        type_labels: vec!["t".into()],
                        properties: vec![HashMap::from([("id".to_string(), vec!["shard_key".to_string()])])],
                        ..Default::default()
                    },
                ),
                "/admin/show/shards" => {
                    let shards = if shards_moved.load(Ordering::SeqCst) {
                        AdminShowShardsResponse { version: 2, rank: vec![1, 2, 3, 1, 2, 3, 1, 2, 3], ..Default::default() }
                    } else {
                        AdminShowShardsResponse { version: 1, rank: vec![1, 2, 1, 2, 2, 1, 2, 1], ..Default::default() }
                    };
                    encode(&crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA, &shards)
                }
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let rerouting = Arc::new(AtomicBool::new(false));
        let taken = [&w1, &w2, &w3].map(|w| rank_handler(w, rerouting.clone()));

        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![head.url.to_string()], options).await.expect("client"));
        let inserter = BulkInserter::<GenericRecord>::with_schema(k, "t".into(), 10, 2, None, record_type.schema_string())
            .await
            .expect("inserter");
        assert_eq!(inserter.worker_count(), 2);

        // The third rank joins, and the server moves shards to it
        *head.behavior.properties.lock() =
            cluster_properties(&address(&head), &[&address(&w1), &address(&w2), &address(&w3)]);
        rebalanced.store(true, Ordering::SeqCst);
        rerouting.store(true, Ordering::SeqCst);

        inserter.insert_batch(records(&record_type, 0..40)).unwrap();
        inserter.flush().await.unwrap();
        rerouting.store(false, Ordering::SeqCst);
        assert_eq!(taken[2].load(Ordering::SeqCst), 0, "the old mapping knows no third rank");
        assert_eq!(inserter.worker_count(), 3, "the queues were rebuilt for the new rank");

        inserter.insert_batch(records(&record_type, 40..140)).unwrap();
        inserter.flush().await.unwrap();
        assert!(inserter.drain_errors().is_empty());
        assert_eq!(inserter.count_inserted(), 140);
        assert!(taken[2].load(Ordering::SeqCst) > 0, "the new rank takes its share");
        inserter.close().await.unwrap();
    }
}
