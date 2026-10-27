//! Common types and utilities shared between bulk inserter implementations.
//!
//! This module contains the shared infrastructure used by `BulkInserter`.

use crate::{
    KineticaError, RawInsertRecordsRequest, ShowTableResponse,
    record_key_builder::{KineticaType, Column},
};
use serde::Serialize;
use serde_avro_fast::Schema as FastSchema;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot::Mutex;
use smallvec::SmallVec;

// ============================================================================
// Shard Key Types
// ============================================================================

/// Type alias for shard key values - stack-allocated for up to 4 columns (covers most use cases).
///
/// Column names may borrow from the record, like the values, so a record whose
/// columns are only known at runtime (such as a `GenericRecord`) can name them
/// without allocating; a string literal still works, being `'static`.
pub type ShardKeyValues<'a> = SmallVec<[(&'a str, ShardKeyValue<'a>); 4]>;

/// Typed shard key value to avoid string parsing overhead.
///
/// Use typed variants when possible for better performance.
/// String variants are provided for complex types that require parsing.
///
/// # Performance Notes
///
/// - Numeric types (`Int`, `Long`, `Float`, `Double`, `Boolean`, `Int8`, `Int16`) are `Copy` and zero-cost
/// - String references (`String`, `Date`, `Ipv4`, `Decimal`, etc.) are borrowed - no allocation
/// - All variants are `Copy` for zero-cost passing
#[derive(Clone, Copy, Debug)]
pub enum ShardKeyValue<'a> {
    /// 32-bit integer value
    Int(i32),
    /// 64-bit integer value
    Long(i64),
    /// 32-bit float value
    Float(f32),
    /// 64-bit double value
    Double(f64),
    /// String value (also used for char(N) types) - borrowed reference
    String(&'a str),
    /// Date in YYYY-MM-DD format - borrowed reference
    Date(&'a str),
    /// IPv4 in dotted-quad format (e.g., "192.168.1.100") - borrowed reference
    Ipv4(&'a str),
    /// Decimal as string (e.g., "12345.6789") - borrowed reference
    Decimal(&'a str),
    /// Boolean value (true/false)
    Boolean(bool),
    /// 8-bit signed integer (-128 to 127)
    Int8(i8),
    /// 16-bit signed integer (-32768 to 32767)
    Int16(i16),
    /// Timestamp as milliseconds since Unix epoch
    Timestamp(i64),
    /// DateTime in YYYY-MM-DD HH:MM:SS.mmm format - borrowed reference
    DateTime(&'a str),
    /// Time in HH:MM:SS.mmm format - borrowed reference
    Time(&'a str),
    /// UUID in standard format (e.g., "550e8400-e29b-41d4-a716-446655440000") - borrowed reference
    Uuid(&'a str),
    /// Unsigned long as string (0 to 18446744073709551615) - borrowed reference
    Ulong(&'a str),
    /// A null value, for a column of any type. It encodes as zero bytes in
    /// the width of the column's encoding.
    Null,
}

impl<'a> ShardKeyValue<'a> {
    /// The text a value carries: every variant that carries a `&str`. `None`
    /// for the numeric, boolean and null variants.
    pub(crate) fn as_text(&self) -> Option<&'a str> {
        match *self {
            ShardKeyValue::String(s)
            | ShardKeyValue::Date(s)
            | ShardKeyValue::Ipv4(s)
            | ShardKeyValue::Decimal(s)
            | ShardKeyValue::DateTime(s)
            | ShardKeyValue::Time(s)
            | ShardKeyValue::Uuid(s)
            | ShardKeyValue::Ulong(s) => Some(s),
            _ => None,
        }
    }
}

/// Trait for extracting shard key values from a record.
///
/// Records must implement this trait to enable shard-aware routing.
/// Returns a SmallVec of (column_name, typed_value) pairs for shard key columns.
///
/// # Performance
///
/// - Returns `ShardKeyValues` (SmallVec) which is stack-allocated for up to 4 columns
/// - Use typed variants (`Int`, `Long`, `Float`, `Double`) when possible to avoid
///   string parsing overhead
/// - The `String` variants are for string/char columns, and specialized variants
///   (`Date`, `Ipv4`, `Decimal`) handle formatted types
/// - Use the `smallvec!` macro for convenient construction
///
/// # Example
///
/// ```
/// use smallvec::smallvec;
/// use gpudb_api::{ShardKeyExtractor, ShardKeyValue, ShardKeyValues};
/// # struct MyRecord { id: i32, region: String }
///
/// impl ShardKeyExtractor for MyRecord {
///     fn shard_key_values(&self) -> ShardKeyValues<'_> {
///         smallvec![
///             ("id", ShardKeyValue::Int(self.id)),
///             ("region", ShardKeyValue::String(&self.region)),
///         ]
///     }
/// }
/// ```
pub trait ShardKeyExtractor {
    /// Returns shard key column names and their typed values.
    ///
    /// Only columns that are part of the shard key need to be included.
    /// Returns a SmallVec which is stack-allocated for up to 4 shard columns.
    fn shard_key_values(&self) -> ShardKeyValues<'_>;
}

/// A JSON record, as a [`BulkInserter`](crate::BulkInserter) in JSON mode
/// takes it. Its key is not read: it has no shard key values, and each record
/// goes to the rank of a random shard.
impl ShardKeyExtractor for String {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        SmallVec::new()
    }
}

// ============================================================================
// Internal Structs
// ============================================================================

/// Internal struct representing a batch of records to flush.
pub(crate) struct FlushJob<T> {
    /// The destination this batch was routed to, kept for logging and for
    /// re-resolving the URL after a failover.
    pub worker_idx: usize,

    /// The URL this batch was routed against, carried with it rather than
    /// looked up by index at flush time.
    ///
    /// The routing state may be rebuilt between a batch being queued and being
    /// flushed, and the new one can have a different number of destinations —
    /// so `worker_idx` is not a safe index into it.
    pub insert_url: Arc<str>,

    pub records: Vec<Arc<T>>,

    /// The batch's `max_in_flight_batches` permit, if it took one. Released
    /// when the job is dropped, i.e. once the batch has finished, whether it
    /// succeeded or failed.
    #[allow(dead_code, reason = "held only so that dropping the job releases it")]
    pub permit: Option<tokio::sync::OwnedSemaphorePermit>,

    /// The batch's place in its queue's order, where the order matters (see
    /// [`BatchOrder`]). Dropping it, once the batch has been sent or if it
    /// never will be, passes the turn to the next batch.
    pub ticket: Option<Ticket>,
}

/// The order one queue's batches are sent in, where it matters.
///
/// For a table with a primary key, two batches holding one key must reach
/// the server in the order they were dispatched: otherwise an older value
/// can land last, or, without `update_on_existing_pk`, the later record
/// survive in place of the earlier one. A batch waits only for the earlier
/// batches still unsent that hold one of its keys, so batches of distinct
/// keys still go at once.
///
/// The keys are the records' routing keys: the shard key, or the primary key
/// where there is none. Records with one primary key have one routing key,
/// as the shard key is part of the primary key; records that only share a
/// routing key, or its hash, are kept in order too, which costs a wait and
/// changes nothing.
#[derive(Debug, Default)]
pub(crate) struct BatchOrder {
    state: Mutex<OrderState>,
    turn: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct OrderState {
    next_ticket: u64,
    /// The tickets below this have registered their keys, or never will.
    registered: u64,
    /// Tickets dropped before their turn to register.
    dropped_early: std::collections::BTreeSet<u64>,
    /// The unsent batches holding each key's hash, earliest first.
    holders: HashMap<u64, SmallVec<[u64; 2]>>,
    /// The batches registered and not yet sent.
    unsent: std::collections::HashSet<u64>,
}

impl OrderState {
    /// Moves the registration turn past the tickets dropped before theirs.
    fn skip_dropped(&mut self) {
        while self.dropped_early.remove(&self.registered) {
            self.registered += 1;
        }
    }
}

impl BatchOrder {
    /// Hands out the next ticket, and calls `send` with it while no other can
    /// be handed out: the channel then holds the queue's batches in ticket
    /// order, so a batch never waits on one still queued behind it.
    pub(crate) fn dispatch<R>(self: &Arc<Self>, send: impl FnOnce(Ticket) -> R) -> R {
        let mut state = self.state.lock();
        let number = state.next_ticket;
        state.next_ticket += 1;
        send(Ticket { order: self.clone(), number, keys: Vec::new(), registered: false })
    }

    /// Waits until `ready` holds of the state.
    async fn wait_until(&self, ready: impl Fn(&OrderState) -> bool) {
        loop {
            let notified = self.turn.notified();
            tokio::pin!(notified);
            // Registered before the check, so that a change in between still
            // wakes this one
            notified.as_mut().enable();
            if ready(&self.state.lock()) {
                return;
            }
            notified.await;
        }
    }
}

/// A batch's place in its queue's [`BatchOrder`].
#[derive(Debug)]
pub(crate) struct Ticket {
    order: Arc<BatchOrder>,
    number: u64,
    /// The batch's routing key hashes, once registered.
    keys: Vec<u64>,
    registered: bool,
}

impl Ticket {
    /// Waits until the earlier batches holding one of `keys`, the hashes of
    /// this batch's routing keys, have been sent, or will never be.
    ///
    /// The keys are registered in ticket order, after the earlier batches'
    /// own, which takes no longer than their registering; then this batch
    /// waits for every one of them still unsent that shares a key, not just
    /// the latest, so that one dropped unsent does not let it past those
    /// before.
    pub(crate) async fn wait_for_turn(&mut self, keys: Vec<u64>) {
        let number = self.number;
        self.order.wait_until(|state| state.registered == number).await;
        let after = {
            let mut state = self.order.state.lock();
            let mut after = Vec::new();
            for key in &keys {
                let holders = state.holders.entry(*key).or_default();
                if holders.last() != Some(&number) {
                    after.extend_from_slice(holders);
                    holders.push(number);
                }
            }
            state.unsent.insert(number);
            state.registered += 1;
            state.skip_dropped();
            after
        };
        self.keys = keys;
        self.registered = true;
        self.order.turn.notify_waiters();
        self.order.wait_until(|state| after.iter().all(|earlier| !state.unsent.contains(earlier))).await;
    }
}

impl Drop for Ticket {
    /// The batch has been sent, or never will be: the batches after it need
    /// not wait for it.
    fn drop(&mut self) {
        let mut state = self.order.state.lock();
        if self.registered {
            state.unsent.remove(&self.number);
            for key in &self.keys {
                if let Some(holders) = state.holders.get_mut(key) {
                    holders.retain(|holder| *holder != self.number);
                    if holders.is_empty() {
                        state.holders.remove(key);
                    }
                }
            }
        } else if self.number == state.registered {
            state.registered += 1;
            state.skip_dropped();
        } else {
            state.dropped_early.insert(self.number);
        }
        drop(state);
        self.order.turn.notify_waiters();
    }
}

/// The queue of records bound for one destination, split into stripes so that
/// threads inserting at once fill stripes of their own (see
/// [`current_thread_stripe`]). Each stripe is sent as a batch once it holds
/// `batch_size` records.
pub(crate) struct WorkerQueue<T> {
    pub stripes: Vec<Arc<Mutex<Vec<Arc<T>>>>>,
    /// The order its batches are sent in, where that matters.
    pub order: Arc<BatchOrder>,
}

impl<T> WorkerQueue<T> {
    pub fn new(num_stripes: usize, batch_size: usize) -> Self {
        Self {
            stripes: (0..num_stripes)
                .map(|_| Arc::new(Mutex::new(Vec::with_capacity(batch_size))))
                .collect(),
            order: Arc::new(BatchOrder::default()),
        }
    }
}

/// Pre-computed URL for each worker to avoid per-batch string formatting
#[derive(Clone, Debug)]
pub(crate) struct WorkerUrlCache {
    /// Full insert URL (e.g., "http://172.17.0.2:9192/insert/records")
    pub insert_url: Arc<str>,
}

impl WorkerUrlCache {
    pub fn new(base_url: Arc<str>) -> Self {
        // Text that is not a URL is joined as it is, though the addresses
        // here always are URLs
        let insert_url = match url::Url::parse(&base_url) {
            Ok(url) => Arc::from(crate::gpudb::append_path_to_url(&url, "/insert/records").as_str()),
            Err(_) => Arc::from(format!("{}/insert/records", base_url.trim_end_matches('/'))),
        };
        Self { insert_url }
    }
}

// ============================================================================
// Constants
// ============================================================================

/// The Avro schema for insert_records response, used by all bulk inserter variants.
pub(crate) const INSERT_RECORDS_RESPONSE_SCHEMA: &str = r###"{"fields":[{"name":"record_ids","type":{"items":"string","type":"array"}},{"name":"count_inserted","type":"int"},{"name":"count_updated","type":"int"},{"name":"info","type":{"type":"map","values":"string"}}],"name":"insert_records_response","type":"record"}"###;

/// Maximum number of Vecs to keep in the batch pool.
pub(crate) const BATCH_POOL_MAX_SIZE: usize = 32;

// ============================================================================
// Utility Functions
// ============================================================================

/// The stripe of each worker queue that the calling thread fills.
///
/// Each thread is given a slot, in turn, the first time it asks, and fills
/// stripe `slot % num_stripes` of every worker queue. A thread inserting alone
/// therefore fills one stripe per rank, so a rank's batch is sent once
/// `batch_size` of its records have been inserted; threads inserting at once
/// take different stripes, so they rarely wait on each other's lock.
///
/// Choosing the stripe by the record's key instead spread one thread's records
/// for a rank over every stripe: nothing was sent until about `num_stripes`
/// times as many records had been inserted, and a flush then sent that many
/// partial batches per rank.
#[inline]
pub(crate) fn current_thread_stripe(num_stripes: usize) -> usize {
    static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static SLOT: usize = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    }
    // A thread whose locals are already gone (one inserting from the
    // destructor of another thread local) shares the first stripe.
    SLOT.try_with(|slot| *slot).unwrap_or(0) % num_stripes.max(1)
}

/// Writes `value` as an Avro `long`: zig-zag encoded, then seven bits to a
/// byte, low bits first.
fn write_avro_long(out: &mut Vec<u8>, value: i64) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// A record that did not encode: its index in the batch, and the message for
/// it.
pub(crate) type EncodeFailure = (usize, String);

/// The body of the `/insert/records` request carrying `records`: the bytes
/// `serde_avro_fast` gives a `RawInsertRecordsRequest` holding each record
/// encoded, with `list_encoding` "binary".
///
/// Each record is encoded straight into the body, so the batch is held once,
/// rather than as a buffer per record copied into the request when it is
/// serialized. Every other field is encoded as part of the request, with no
/// records, and the records are written where its empty list was.
///
/// A record that does not encode is sent empty, and returned among the
/// failures with its message: the rest of the batch is still sent, the
/// server reports the empty record, and the client's message replaces the
/// server's for it (see `BulkInserter::gather_record_errors`).
pub(crate) fn encode_insert_records_body<T: Serialize>(
    record_schema: &FastSchema,
    request_schema: &FastSchema,
    table_name: &str,
    options: &HashMap<String, String>,
    records: &[Arc<T>],
) -> Result<(Vec<u8>, Vec<EncodeFailure>), KineticaError> {
    let request = RawInsertRecordsRequest {
        table_name: table_name.to_string(),
        list: Vec::new(),
        list_str: Vec::new(),
        list_encoding: "binary".to_string(),
        options: options.clone(),
    };
    let mut config = serde_avro_fast::ser::SerializerConfig::new(request_schema);
    let request = serde_avro_fast::to_datum(&request, Vec::new(), &mut config)
        .map_err(|e| KineticaError::Avro(format!("Request encode failed: {e}")))?;

    // The table name comes first and the records second, so the empty list,
    // a single 0, follows the name's length and bytes
    let mut body = Vec::with_capacity(request.len() + records.len() * 64 + 16);
    write_avro_long(&mut body, table_name.len() as i64);
    body.extend_from_slice(table_name.as_bytes());
    let list_at = body.len();
    if request.get(..list_at) != Some(&body[..]) || request.get(list_at) != Some(&0) {
        return Err(KineticaError::Avro(
            "Request encode failed: the /insert/records request does not begin with its table name and records"
                .to_string(),
        ));
    }

    // The records, as one block of `bytes` items: each one's length, then the
    // record, encoded on this thread with one serializer configuration
    let mut failures = Vec::new();
    if !records.is_empty() {
        write_avro_long(&mut body, records.len() as i64);
        let mut config = serde_avro_fast::ser::SerializerConfig::new(record_schema);
        let mut record_bytes = Vec::with_capacity(256);
        for (index, record) in records.iter().enumerate() {
            record_bytes.clear();
            match serde_avro_fast::to_datum(record.as_ref(), std::mem::take(&mut record_bytes), &mut config) {
                Ok(bytes) => {
                    write_avro_long(&mut body, bytes.len() as i64);
                    body.extend_from_slice(&bytes);
                    record_bytes = bytes;
                }
                Err(e) => {
                    // An empty record in its place, and a fresh configuration
                    // rather than one left part way through a record
                    write_avro_long(&mut body, 0);
                    failures.push((index, crate::generic_record::encode_failure_message(&e)));
                    config = serde_avro_fast::ser::SerializerConfig::new(record_schema);
                }
            }
        }
    }

    // The end of the list, and the fields after it as the request encoded them
    body.push(0);
    body.extend_from_slice(&request[list_at + 1..]);
    Ok((body, failures))
}

// ============================================================================
// Schema Parsing Functions
// ============================================================================

/// Build KineticaType from ShowTableResponse
pub(crate) fn build_kinetica_type(table_resp: &ShowTableResponse) -> Result<KineticaType, KineticaError> {
    if table_resp.type_schemas.is_empty() {
        return Err(KineticaError::Internal);
    }

    // Parse the Avro schema to get column information
    let schema_str = &table_resp.type_schemas[0];
    let schema: serde_json::Value = serde_json::from_str(schema_str)
        .map_err(|e| KineticaError::Avro(format!("Failed to parse schema JSON: {e}")))?;

    let fields = schema.get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| KineticaError::Avro("Schema missing fields array".into()))?;

    // Get properties from the first table (index 0)
    let properties = if !table_resp.properties.is_empty() {
        &table_resp.properties[0]
    } else {
        return Err(KineticaError::Avro("No properties in response".into()));
    };

    let mut columns = Vec::new();

    for field in fields {
        let name = field.get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| KineticaError::Avro("Field missing name".into()))?
            .to_string();

        // Determine data type from Avro schema
        let avro_type = field.get("type");
        let data_type = extract_data_type(avro_type)?;

        // Get properties for this column
        let col_props = properties.get(&name)
            .cloned()
            .unwrap_or_default();

        // The key type is resolved from the base type and the properties
        // together; see ShardColumnType.
        columns.push(Column {
            name,
            data_type,
            properties: col_props,
        });
    }

    Ok(KineticaType { columns })
}

/// Extract base data type from Avro type definition
pub(crate) fn extract_data_type(avro_type: Option<&serde_json::Value>) -> Result<String, KineticaError> {
    match avro_type {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(serde_json::Value::Array(arr)) => {
            // Union type like ["null", "int"] - find non-null type
            for item in arr {
                if let Some(s) = item.as_str() {
                    if s != "null" {
                        return Ok(s.to_string());
                    }
                }
            }
            Ok("string".into())
        }
        Some(serde_json::Value::Object(obj)) => {
            // Complex type
            if let Some(t) = obj.get("type").and_then(|v| v.as_str()) {
                Ok(t.to_string())
            } else {
                Ok("string".into())
            }
        }
        _ => Ok("string".into()),
    }
}

// ============================================================================
// Batch Pool Utilities
// ============================================================================

/// Get a Vec from the batch pool or create a new one with the specified capacity.
#[inline]
pub(crate) fn pooled_vec<T>(pool: &Mutex<Vec<Vec<Arc<T>>>>, batch_size: usize) -> Vec<Arc<T>> {
    pool.lock().pop().unwrap_or_else(|| Vec::with_capacity(batch_size))
}

/// Return a Vec to the batch pool for reuse.
#[inline]
pub(crate) fn return_to_pool<T>(pool: &Mutex<Vec<Vec<Arc<T>>>>, mut vec: Vec<Arc<T>>) {
    vec.clear();
    let mut pool_guard = pool.lock();
    if pool_guard.len() < BATCH_POOL_MAX_SIZE {
        pool_guard.push(vec);
    }
}

// ============================================================================
// Worker Index Computation
// ============================================================================

/// Builds the compacted worker URL list and the rank -> queue-index mapping
/// from a worker list that keeps an empty slot for every removed rank.
///
/// Slot `i` of `worker_slots` is rank `i + 1`; a `None` slot is a rank that has
/// been removed from the cluster. Queues exist only for live ranks, so the two
/// lists are different lengths, and the returned mapping is the only correct way
/// to get from a rank number to a queue index. Deriving that mapping from the
/// compacted position instead — `rank = index + 1` — shifts every rank above a
/// removed one and silently misroutes its records.
///
/// A removed or unknown rank maps to `None`, which routing must treat as an
/// error rather than a reason to fall back to another rank. The mapping is
/// indexed by rank, so index 0 — the head rank, which has no worker queue — is
/// `None` too.
///
/// `None` rather than a `usize::MAX` sentinel: a sentinel is a valid index as
/// far as the type system is concerned, so forgetting to test for it indexes
/// out of bounds at runtime instead of failing to compile.
pub(crate) fn map_ranks_to_workers(
    worker_slots: &[Option<url::Url>],
) -> (Vec<Arc<str>>, Vec<Option<usize>>) {
    let mut urls: Vec<Arc<str>> = Vec::new();
    // Ranks are 1-based, so the mapping needs one extra entry for rank 0.
    let mut rank_to_worker = vec![None; worker_slots.len() + 1];

    for (slot, maybe_url) in worker_slots.iter().enumerate() {
        let Some(url) = maybe_url else {
            // Removed rank: leave the slot empty and create no queue. Its rank
            // number stays reserved either way.
            continue;
        };
        rank_to_worker[slot + 1] = Some(urls.len());
        urls.push(Arc::from(url.as_str()));
    }

    (urls, rank_to_worker)
}

/// Why a record could not be routed to a worker rank.
///
/// Routing used to fall back to worker 0 whenever it could not resolve a rank.
/// That silently sends the record to the wrong rank; each of these cases is an
/// error instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteFailure {
    /// The server supplied no shard mapping for the table, so there is nothing
    /// to route against.
    NoShardMapping,
    /// The shard mapping named a rank that has been removed from the cluster.
    /// Its slot is empty, so it can serve nothing; the mapping is stale.
    RankRemoved,
    /// The record's key could not be built: a key value was not of a type
    /// its column accepts.
    Unshardable,
}

impl RouteFailure {
    /// Renders this failure as the error the caller should surface.
    pub(crate) fn into_error(self, table_name: &str) -> crate::KineticaError {
        match self {
            RouteFailure::NoShardMapping => crate::KineticaError::Api(format!(
                "No shard mapping is available for table '{table_name}'; cannot route the \
                 record to a worker rank.  Maybe need to update the shard mapping."
            )),
            RouteFailure::RankRemoved => crate::KineticaError::Api(
                "Attempted to insert into worker rank that has been removed!  Maybe need to \
                 update the shard mapping."
                    .to_string(),
            ),
            RouteFailure::Unshardable => crate::KineticaError::Api(
                crate::record_key_builder::UNSHARDABLE_VALUE_MESSAGE.to_string(),
            ),
        }
    }
}

/// Compute the worker index from pre-computed shard key values.
///
/// Callers must short-circuit the single-queue case (head-node mode, or one
/// worker queue) before calling this — there the only queue is index 0 and no
/// shard mapping is needed.
#[inline]
pub(crate) fn compute_worker_index_from_values(
    shard_values: &[(&str, ShardKeyValue<'_>)],
    key_builder: &crate::record_key_builder::RecordKeyBuilder,
    routing_table: &[i32],
    rank_to_worker_array: &[Option<usize>],
) -> Result<usize, RouteFailure> {
    // Routing, by key or at random, needs the shard mapping, so it is checked
    // first, whatever the table's key.
    if routing_table.is_empty() {
        return Err(RouteFailure::NoShardMapping);
    }

    let key = key_builder
        .build_typed(shard_values)
        .map_err(|_| RouteFailure::Unshardable)?;
    let rank = match key {
        // route() returns 0-based, ranks are 1-based
        Some(key) => key.route(routing_table) as i32 + 1,
        // A table with neither a shard key nor a primary key is randomly
        // sharded. Each record goes to the rank of a random entry of the
        // routing table, so a rank takes records in proportion to its shards.
        None => random_rank(routing_table),
    };
    worker_for_rank(rank, rank_to_worker_array)
}

/// Compute the worker index of a record whose key is not read, such as a JSON
/// record: to the rank of a random entry of the shard mapping.
///
/// As with [`compute_worker_index_from_values`], the single-queue case must be
/// short-circuited first.
#[inline]
pub(crate) fn compute_random_worker_index(
    routing_table: &[i32],
    rank_to_worker_array: &[Option<usize>],
) -> Result<usize, RouteFailure> {
    if routing_table.is_empty() {
        return Err(RouteFailure::NoShardMapping);
    }
    worker_for_rank(random_rank(routing_table), rank_to_worker_array)
}

/// The rank of a random entry of a non-empty shard mapping.
fn random_rank(routing_table: &[i32]) -> i32 {
    routing_table[rand::random_range(0..routing_table.len())]
}

/// The queue serving `rank`.
fn worker_for_rank(rank: i32, rank_to_worker_array: &[Option<usize>]) -> Result<usize, RouteFailure> {
    // Map rank to worker index using array lookup (faster than HashMap). An
    // empty slot is a rank removed from the cluster, and an out-of-range rank
    // means the mapping is ahead of the worker list; neither can serve the
    // record, and both collapse to the same `None` here.
    match rank_to_worker_array.get(rank as usize) {
        Some(Some(worker_idx)) => Ok(*worker_idx),
        _ => Err(RouteFailure::RankRemoved),
    }
}

#[cfg(test)]
mod removed_rank_routing_tests {
    use super::*;
    use crate::record_key_builder::RecordKeyBuilder;
    use url::Url;

    fn slot(host: &str) -> Option<Url> {
        Some(Url::parse(&format!("http://{host}:9191")).unwrap())
    }

    /// With every rank present, rank N maps to queue N-1. This is the baseline
    /// the removed-rank cases are measured against.
    #[test]
    fn all_ranks_present_maps_rank_to_slot() {
        let slots = vec![slot("w1"), slot("w2"), slot("w3")];
        let (urls, map) = map_ranks_to_workers(&slots);

        assert_eq!(urls.len(), 3);
        assert_eq!(map[1], Some(0));
        assert_eq!(map[2], Some(1));
        assert_eq!(map[3], Some(2));
        // Rank 0 is the head rank and has no worker queue.
        assert_eq!(map[0], None);
    }

    /// The regression this port exists for: with a middle rank removed,
    /// compacting the list would map rank 3 to rank 2's old queue index and
    /// send its records to the wrong host. Rank 3 must still resolve to the
    /// queue holding w3.
    #[test]
    fn removed_middle_rank_does_not_shift_higher_ranks() {
        let slots = vec![slot("w1"), None, slot("w3")];
        let (urls, map) = map_ranks_to_workers(&slots);

        // Only live ranks get queues.
        assert_eq!(urls.len(), 2);
        assert_eq!(map[1], Some(0));
        assert_eq!(urls[map[1].expect("live rank")].as_ref(), "http://w1:9191/");

        // Rank 2 is gone: unroutable, not silently redirected.
        assert_eq!(map[2], None);

        // Rank 3 still reaches w3, not w1.
        assert_eq!(urls[map[3].expect("live rank")].as_ref(), "http://w3:9191/");
    }

    /// Removing the highest rank used to leave the mapping too short, so
    /// routing to it read past the end. The slot must stay reserved.
    #[test]
    fn removed_highest_rank_keeps_its_slot() {
        let slots = vec![slot("w1"), slot("w2"), None];
        let (urls, map) = map_ranks_to_workers(&slots);

        assert_eq!(urls.len(), 2);
        // The mapping is still wide enough to be asked about rank 3.
        assert_eq!(map.len(), 4);
        assert_eq!(map[3], None);
    }

    #[test]
    fn all_ranks_removed_yields_no_queues() {
        let slots = vec![None, None];
        let (urls, map) = map_ranks_to_workers(&slots);

        assert!(urls.is_empty());
        assert!(map.iter().all(Option::is_none));
    }

    #[test]
    fn route_failure_messages_name_the_cause() {
        let KineticaError::Api(msg) = RouteFailure::NoShardMapping.into_error("s.t") else {
            panic!("expected an API error");
        };
        assert_eq!(
            msg,
            "No shard mapping is available for table 's.t'; cannot route the record to a \
             worker rank.  Maybe need to update the shard mapping."
        );

        let KineticaError::Api(msg) = RouteFailure::RankRemoved.into_error("s.t") else {
            panic!("expected an API error");
        };
        assert_eq!(
            msg,
            "Attempted to insert into worker rank that has been removed!  Maybe need to \
             update the shard mapping."
        );
    }

    fn builder(columns: &[(&str, &str, &[&str])]) -> RecordKeyBuilder {
        RecordKeyBuilder::new(&KineticaType {
            columns: columns
                .iter()
                .map(|(name, data_type, props)| Column {
                    name: (*name).to_string(),
                    data_type: (*data_type).to_string(),
                    properties: props.iter().map(|p| (*p).to_string()).collect(),
                })
                .collect(),
        })
        .expect("shardable key")
    }

    #[test]
    fn a_table_without_a_key_spreads_records_over_the_routing_table() {
        // Ranks 1..=3 live in worker queues 0..=2; rank 3 holds two shards.
        let unkeyed = builder(&[("a", "int", &[])]);
        let routing_table = [1, 2, 3, 3];
        let rank_to_worker = [None, Some(0), Some(1), Some(2)];

        let mut counts = [0usize; 3];
        for _ in 0..4000 {
            let worker = compute_worker_index_from_values(
                &[("a", ShardKeyValue::Int(1))], &unkeyed, &routing_table, &rank_to_worker,
            )
            .expect("routed");
            counts[worker] += 1;
        }

        // Every worker gets records, the one holding two shards about twice as
        // many; the bounds are loose enough never to fail by chance.
        assert!(counts.iter().all(|&c| c > 600), "counts: {counts:?}");
        assert!(counts[2] > counts[0] && counts[2] > counts[1], "counts: {counts:?}");
    }

    #[test]
    fn a_table_without_a_key_still_needs_the_shard_mapping() {
        let unkeyed = builder(&[("a", "int", &[])]);
        let routed = compute_worker_index_from_values(
            &[("a", ShardKeyValue::Int(1))], &unkeyed, &[], &[None, Some(0)],
        );
        assert!(matches!(routed, Err(RouteFailure::NoShardMapping)));
    }

    #[test]
    fn a_keyed_record_goes_to_the_rank_its_key_routes_to() {
        let keyed = builder(&[("a", "int", &["shard_key"])]);
        let routing_table = [1, 2, 3, 3];
        let rank_to_worker = [None, Some(0), Some(1), Some(2)];

        let key = keyed.build_typed(&[("a", ShardKeyValue::Int(42))]).expect("built").expect("key");
        let expected = rank_to_worker[key.route(&routing_table) + 1].expect("live rank");
        for _ in 0..10 {
            let worker = compute_worker_index_from_values(
                &[("a", ShardKeyValue::Int(42))], &keyed, &routing_table, &rank_to_worker,
            );
            assert_eq!(worker.expect("routed"), expected);
        }
    }
}

#[cfg(test)]
mod insert_body_tests {
    use super::*;
    use serde_bytes::ByteBuf;

    #[derive(serde::Serialize)]
    struct Rec {
        id: i64,
        name: String,
    }

    fn schemas() -> (FastSchema, FastSchema) {
        let record = crate::parse_fast_schema(
            r#"{"type":"record","name":"Rec","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#,
        )
        .unwrap();
        let request = crate::parse_fast_schema(crate::KineticaRecord::avro_schema(&RawInsertRecordsRequest {
            table_name: String::new(),
            list: Vec::new(),
            list_str: Vec::new(),
            list_encoding: String::new(),
            options: HashMap::new(),
        }))
        .unwrap();
        (record, request)
    }

    fn records(n: i64) -> Vec<Arc<Rec>> {
        (0..n).map(|i| Arc::new(Rec { id: i * 7_919, name: "n".repeat((i % 300) as usize) })).collect()
    }

    /// The request as it was sent before: each record encoded alone, then the
    /// whole request serialized.
    fn serialized(
        record_schema: &FastSchema,
        table: &str,
        options: &HashMap<String, String>,
        records: &[Arc<Rec>],
    ) -> RawInsertRecordsRequest {
        let list = records.iter().map(|r| ByteBuf::from(crate::encode_single(record_schema, r.as_ref()).unwrap())).collect();
        RawInsertRecordsRequest {
            table_name: table.to_string(),
            list,
            list_str: Vec::new(),
            list_encoding: "binary".to_string(),
            options: options.clone(),
        }
    }

    fn to_bytes(request_schema: &FastSchema, request: &RawInsertRecordsRequest) -> Vec<u8> {
        let mut config = serde_avro_fast::ser::SerializerConfig::new(request_schema);
        serde_avro_fast::to_datum(request, Vec::new(), &mut config).unwrap()
    }

    /// Byte for byte what serializing the request gives, for counts, record
    /// sizes and table names whose lengths take one byte or several.
    #[test]
    fn the_body_is_the_serialized_request() {
        let schemas = schemas();
        let options = HashMap::from([("update_on_existing_pk".to_string(), "true".to_string())]);
        for (table, n) in [("t", 1), ("perf.t", 63), (&*"long_".repeat(30), 1000), ("t", 0)] {
            for options in [&HashMap::new(), &options] {
                let records = records(n);
                let (body, failures) = encode_insert_records_body(&schemas.0, &schemas.1, table, options, &records).unwrap();
                assert_eq!(body, to_bytes(&schemas.1, &serialized(&schemas.0, table, options, &records)), "{table}, {n}");
                assert!(failures.is_empty());
            }
        }
    }

    /// With several options, whose order a map does not fix, the body reads
    /// back as the request.
    #[test]
    fn the_body_reads_back_as_the_request() {
        let schemas = schemas();
        let options: HashMap<String, String> =
            (0..20).map(|i| (format!("option_{i}"), "v".repeat(i))).collect();
        let records = records(500);
        let (body, _) = encode_insert_records_body(&schemas.0, &schemas.1, "s.t", &options, &records).unwrap();
        let read: RawInsertRecordsRequest = serde_avro_fast::from_datum_slice(&body, &schemas.1).unwrap();
        let expected = serialized(&schemas.0, "s.t", &options, &records);
        assert_eq!(read.table_name, expected.table_name);
        assert_eq!(read.list, expected.list);
        assert!(read.list_str.is_empty());
        assert_eq!(read.list_encoding, "binary");
        assert_eq!(read.options, options);
    }

    // A record that does not encode is sent empty in its place, and reported
    // with its index and its message; the others are sent.
    #[test]
    fn a_record_that_does_not_encode_is_sent_empty_and_reported() {
        use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue, Type};
        let record_type = Arc::new(Type::new("t", vec![Column::new("id", ColumnType::Integer)]).unwrap());
        let record_schema = crate::parse_fast_schema(record_type.schema_string()).unwrap();
        let (_, request_schema) = schemas();
        let record = |value: RecordValue| {
            let mut record = GenericRecord::new(record_type.clone());
            record.set(0, value).unwrap();
            Arc::new(record)
        };
        let records = vec![
            record(RecordValue::Int(1)),
            record(RecordValue::String("hello".into())),
            record(RecordValue::Int(3)),
            record(RecordValue::Null),
            record(RecordValue::Int(5)),
        ];

        let (body, failures) =
            encode_insert_records_body(&record_schema, &request_schema, "t", &HashMap::new(), &records).unwrap();
        assert_eq!(
            failures,
            vec![
                (1, "Could not encode object: Field has incorrect data type in record :: value hello \
                     (string) cannot be cast to expected type int at type_name.id"
                    .to_string()),
                (3, "Could not encode object: record :: null value for (non-nullable) int at type_name.id".to_string()),
            ]
        );
        let read: RawInsertRecordsRequest = serde_avro_fast::from_datum_slice(&body, &request_schema).unwrap();
        let encoded = |id: i32| ByteBuf::from(crate::encode_single(&record_schema, &*record(RecordValue::Int(id))).unwrap());
        assert_eq!(read.list, vec![encoded(1), ByteBuf::new(), encoded(3), ByteBuf::new(), encoded(5)]);
    }

    // A record type that is not a GenericRecord fails with its encoder's
    // message.
    #[test]
    fn another_record_types_failure_reads_as_its_encoders() {
        let (record_schema, request_schema) = schemas();
        #[derive(serde::Serialize)]
        struct Wrong {
            id: String,
            name: String,
        }
        let records = vec![Arc::new(Wrong { id: "x".into(), name: "n".into() })];
        let (_, failures) =
            encode_insert_records_body(&record_schema, &request_schema, "t", &HashMap::new(), &records).unwrap();
        assert_eq!(failures.len(), 1);
        assert!(failures[0].1.starts_with("Could not encode object: record :: "), "{failures:?}");
    }

    #[test]
    fn a_long_is_written_as_avro_writes_it() {
        let write = |v: i64| {
            let mut out = Vec::new();
            write_avro_long(&mut out, v);
            out
        };
        assert_eq!(write(0), [0]);
        assert_eq!(write(-1), [1]);
        assert_eq!(write(1), [2]);
        assert_eq!(write(63), [126]);
        assert_eq!(write(64), [0x80, 1]);
        assert_eq!(write(-65), [0x81, 1]);
        assert_eq!(write(i64::MAX), [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1]);
    }
}

#[cfg(test)]
mod stripe_tests {
    use super::current_thread_stripe;

    /// A thread fills the same stripe every time, so one producer's records for
    /// a rank collect in one place, and threads inserting at once fill
    /// different ones.
    #[test]
    fn a_thread_keeps_its_stripe_and_threads_take_different_ones() {
        const STRIPES: usize = 1 << 20;
        let mine = current_thread_stripe(STRIPES);
        assert!((0..1000).all(|_| current_thread_stripe(STRIPES) == mine));

        let others: Vec<usize> = (0..8)
            .map(|_| std::thread::spawn(|| current_thread_stripe(STRIPES)).join().unwrap())
            .collect();
        let mut all = others.clone();
        all.push(mine);
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 9, "each thread has a stripe of its own: {mine} and {others:?}");
    }

    #[test]
    fn the_stripe_is_always_one_of_the_queue_s() {
        for stripes in [1, 2, 3, 12] {
            assert!(current_thread_stripe(stripes) < stripes);
        }
        assert_eq!(current_thread_stripe(0), 0, "a queue cannot have no stripes, but this does not divide by zero");
    }
}

/// A queue's batches are sent in the order they were dispatched, where the
/// table's primary key makes that matter.
#[cfg(test)]
mod batch_order_tests {
    use super::*;
    use std::time::Duration;

    fn tickets(order: &Arc<BatchOrder>, count: usize) -> Vec<Ticket> {
        (0..count).map(|_| order.dispatch(|ticket| ticket)).collect()
    }

    /// Each waits for the ones dispatched before it, whatever order the
    /// workers take them in.
    #[tokio::test]
    async fn batches_take_their_turns_in_dispatch_order() {
        let order = Arc::new(BatchOrder::default());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        // Started last first, and the first slowest
        for (number, mut ticket) in tickets(&order, 5).into_iter().enumerate().rev() {
            let sent = sent.clone();
            tasks.push(tokio::spawn(async move {
                ticket.wait_for_turn(vec![7]).await;
                if number == 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                sent.lock().push(number);
                drop(ticket);
            }));
        }
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(5), task).await.expect("no batch waits forever").unwrap();
        }
        assert_eq!(*sent.lock(), [0, 1, 2, 3, 4]);
    }

    /// A batch dropped unsent does not hold up the ones after it, nor let
    /// them past the ones before it.
    #[tokio::test]
    async fn a_batch_never_sent_passes_its_turn_on() {
        let order = Arc::new(BatchOrder::default());
        let mut all = tickets(&order, 3);
        let mut third = all.pop().unwrap();
        let second = all.pop().unwrap();
        let mut first = all.pop().unwrap();
        first.wait_for_turn(vec![7]).await;
        drop(second);
        let waiting = tokio::spawn(async move { third.wait_for_turn(vec![7]).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "still behind the first");
        drop(first);
        tokio::time::timeout(Duration::from_secs(2), waiting).await.expect("the turn came").unwrap();
    }
}
