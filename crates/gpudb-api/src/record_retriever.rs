//! Record retriever for efficient record retrieval from Kinetica with multi-head support.
//!
//! This module provides the [`RecordRetriever`] struct which allows efficient retrieval
//! of records from GPUdb, with support for multi-head access. `RecordRetriever` instances
//! are thread safe and may be used from any number of tasks simultaneously.
//!
//! A retriever is given the [`Type`] of the table's records, takes typed key
//! values ([`ShardKeyValue`], with [`ShardKeyValue::Null`] for a null), and
//! returns the records decoded: as [`GenericRecord`]s, or as any type serde can
//! deserialize from the type's Avro schema.
//!
//! # Example
//!
//! ```no_run
//! use gpudb_api::{Kinetica, KineticaOptions, RecordRetriever, ShardKeyValue, Type};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let options = KineticaOptions::new();
//! let kinetica = Arc::new(
//!     Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
//! );
//! let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
//!
//! // Create retriever with default options
//! let retriever = RecordRetriever::new(kinetica.clone(), "my_table", record_type).await?;
//!
//! // Retrieve all records
//! let response = retriever.get_by_key(&[], None).await?;
//!
//! // Retrieve records by key
//! let response = retriever.get_by_key(&[ShardKeyValue::Int(123)], None).await?;
//! for record in &response.data {
//!     println!("{:?}", record.get(0));
//! }
//!
//! // Retrieve with expression filter
//! let response = retriever.get_by_key(&[], Some("status = 'active'")).await?;
//! # Ok(())
//! # }
//! ```

use crate::bulk_inserter::same_snapshot;
use crate::bulk_inserter_common::ShardKeyValue;
use crate::generic_record::{GenericRecord, Type};
use crate::gpudb::MultiHeadSnapshot;
use crate::record_key_builder::RecordKeyBuilder;
use crate::worker_list::WorkerList;
use crate::{
    AdminShowShardsRequest, GetRecordsByColumnRequest, GetRecordsByColumnResponse,
    GetRecordsRequest, GetRecordsResponse, Kinetica, KineticaError, ShowTableRequest,
};
use arc_swap::ArcSwap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use rand::Rng;
use serde::de::DeserializeOwned;
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

/// End of set marker for retrieving all records
pub const END_OF_SET: i64 = -9999;

/// Options for the RecordRetriever.
///
/// This struct provides configuration options that can be passed to the
/// `GetRecords` and `GetRecordsByColumn` calls.
#[derive(Debug, Clone, Default)]
pub struct RecordRetrieverOptions {
    /// Options map to pass to retrieval calls
    pub options: HashMap<String, String>,
}

impl RecordRetrieverOptions {
    /// Creates a new RecordRetrieverOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets an option. Returns self for method chaining.
    pub fn set_option(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Sets the expression option. Returns self for method chaining.
    pub fn set_expression(&mut self, expression: impl Into<String>) -> &mut Self {
        self.set_option("expression", expression)
    }

    /// Sets the fast index lookup option. Returns self for method chaining.
    pub fn set_fast_index_lookup(&mut self, enabled: bool) -> &mut Self {
        self.set_option("fast_index_lookup", if enabled { "true" } else { "false" })
    }
}

/// A record a [`RecordRetriever`] returns, decoded from the Avro binary form
/// the server sends by the [`Type`] the retriever was given.
///
/// Implemented for [`GenericRecord`], and for any type serde can deserialize,
/// whose fields are then read by the names in the type's Avro schema.
pub trait RetrievedRecord: Sized + Send + 'static {
    /// Decodes records of `record_type`. A failure reads "Could not decode
    /// object: ...".
    fn decode_records(record_type: &Arc<Type>, encoded: &[ByteBuf]) -> Result<Vec<Self>, KineticaError>;
}

impl RetrievedRecord for GenericRecord {
    fn decode_records(record_type: &Arc<Type>, encoded: &[ByteBuf]) -> Result<Vec<Self>, KineticaError> {
        GenericRecord::decode_all(record_type, encoded)
    }
}

impl<T: DeserializeOwned + Send + 'static> RetrievedRecord for T {
    fn decode_records(record_type: &Arc<Type>, encoded: &[ByteBuf]) -> Result<Vec<Self>, KineticaError> {
        let could_not_decode = |e: &dyn std::fmt::Display| KineticaError::Api(format!("Could not decode object: {e}"));
        let schema: serde_avro_fast::Schema =
            record_type.schema_string().parse().map_err(|e| could_not_decode(&e))?;
        encoded
            .iter()
            .map(|record| serde_avro_fast::from_datum_slice(&record[..], &schema).map_err(|e| could_not_decode(&e)))
            .collect()
    }
}

/// An immutable snapshot of where lookups get routed.
///
/// The counterpart of the bulk inserter's `RoutingState`, with two deliberate
/// differences:
///
/// - There is no `use_head_node`. The inserter sends a replicated table's
///   records to the head node; retrieval instead picks any live worker, since
///   every worker holds the whole table. So for retrieval "not using
///   multi-head" is exactly `!multi_head_enabled`, and a second flag could only
///   disagree with the first.
/// - `live_worker_indices` is *derived* here rather than passed in. It holds
///   indices **into** `worker_urls`, so computing it from that list in the
///   constructor makes a mismatched pair unconstructible rather than merely
///   rejected.
pub(crate) struct RetrieverRouting {
    /// Whether multi-head lookups are usable for this retriever.
    multi_head_enabled: bool,

    /// The `/get/records` URL per rank. Entry `i` is rank `i + 1`, and a rank
    /// removed from the cluster keeps its slot as `None` so the indices stay
    /// aligned with the rank numbering the shard routing table refers to.
    worker_urls: Vec<Option<Url>>,

    /// The indices of `worker_urls` holding a live worker. Derived from
    /// `worker_urls`, never supplied.
    live_worker_indices: Vec<usize>,

    /// The shard-to-rank mapping; `None` when not yet known.
    routing_table: Option<Vec<i32>>,
}

impl RetrieverRouting {
    fn new(
        multi_head_enabled: bool,
        worker_urls: Vec<Option<Url>>,
        routing_table: Option<Vec<i32>>,
    ) -> Self {
        let live_worker_indices = worker_urls
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| slot.as_ref().map(|_| i))
            .collect();

        Self {
            multi_head_enabled,
            worker_urls,
            live_worker_indices,
            routing_table,
        }
    }

    /// Returns a copy carrying a different shard mapping. The URLs are
    /// unchanged, so the derived live-worker indices come out identical.
    fn with_routing_table(&self, routing_table: Option<Vec<i32>>) -> Self {
        Self {
            multi_head_enabled: self.multi_head_enabled,
            worker_urls: self.worker_urls.clone(),
            live_worker_indices: self.live_worker_indices.clone(),
            routing_table,
        }
    }
}

/// Whether an error is a connection failure: the server could not be reached,
/// including the connection failure the client reports once its own failover
/// has run out.
///
/// A server exit (`Exit`) is not one, and neither is a submit failure
/// (`Http`).
fn had_connection_failure(error: &KineticaError) -> bool {
    matches!(error, KineticaError::Connection(_))
}

/// Whether a failed look-up should fail over to another cluster: a server
/// exit or a connection failure.
///
/// `Http` is deliberately not included: a submit failure does not fail over.
/// See `is_connection_error` in the bulk inserter, which makes the same
/// distinction.
fn is_failover_trigger(error: &KineticaError) -> bool {
    matches!(error, KineticaError::Exit(_)) || had_connection_failure(error)
}

/// Object that permits efficient retrieval of records from GPUdb, with support
/// for multi-head access.
///
/// `RecordRetriever` instances are thread safe and may be used from any number
/// of tasks simultaneously. Records are returned as `R`: [`GenericRecord`]s by
/// default, or any type serde can deserialize (see [`RetrievedRecord`]).
pub struct RecordRetriever<R = GenericRecord> {
    // Table members
    kinetica: Arc<Kinetica>,
    table_name: String,
    record_type: Arc<Type>,
    options: RwLock<HashMap<String, String>>,
    table_replicated: bool,

    // Sharding members
    worker_list: RwLock<Option<WorkerList>>,

    /// Where lookups get routed, published as a single immutable value.
    ///
    /// The destination of a lookup depends on several things that must agree:
    /// whether multi-head is usable at all, the worker URL per rank, which of
    /// those slots hold a live worker, and the shard mapping. Holding them in
    /// one immutable value behind one pointer means a rebuild cannot be
    /// observed part way through: a reader sees either the whole old state or
    /// the whole new one.
    ///
    /// Every read on the lookup path must take this reference *once* into a
    /// local and use that local throughout. Re-loading mid-decision
    /// reintroduces exactly the inconsistency the snapshot exists to prevent —
    /// which is what four independent locks here used to allow.
    routing: ArcSwap<RetrieverRouting>,

    shard_key_builder: RecordKeyBuilder,

    shard_version: AtomicI64,
    shard_update_time: AtomicI64,

    // HA members
    num_cluster_switches: AtomicUsize,
    /// The connection's multi-head snapshot this retriever last built its
    /// worker URLs from, compared by identity to tell whether the connection
    /// has moved or re-probed since; see [`MultiHeadSnapshot`].
    last_multi_head_snapshot: parking_lot::Mutex<Option<Arc<MultiHeadSnapshot>>>,
    /// Held by a failover and by an update or rebuild of the worker URLs, so
    /// none of them runs alongside another.
    monitor: tokio::sync::Mutex<()>,
    /// The shard mapping most recently fetched, which may not have been
    /// published yet. Only touched while holding `monitor`.
    pending_routing_table: parking_lot::Mutex<Option<Vec<i32>>>,
    current_head_node_url: RwLock<Option<Url>>,
    last_used_url: RwLock<Option<Url>>,

    /// The records this retriever returns.
    records: PhantomData<fn() -> R>,
}

impl RecordRetriever<GenericRecord> {
    /// Creates a retriever of the records of `table_name`, which are of
    /// `record_type`, returning them as [`GenericRecord`]s.
    ///
    /// The worker list is derived from the connection's own worker addresses,
    /// and records are retrieved with no options.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::{Kinetica, KineticaOptions, RecordRetriever, Type};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let options = KineticaOptions::new();
    /// let kinetica = Arc::new(
    ///     Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
    /// );
    /// let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
    ///
    /// let retriever = RecordRetriever::new(kinetica, "my_table", record_type).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(kinetica: Arc<Kinetica>, table_name: &str, record_type: Arc<Type>) -> Result<Self, KineticaError> {
        Self::create(kinetica, table_name, record_type, None, None).await
    }

    /// Creates a retriever passing `options` to its retrievals.
    pub async fn with_retriever_options(
        kinetica: Arc<Kinetica>,
        table_name: &str,
        record_type: Arc<Type>,
        options: RecordRetrieverOptions,
    ) -> Result<Self, KineticaError> {
        Self::create(kinetica, table_name, record_type, None, Some(options.options)).await
    }

    /// Creates a retriever with the given worker list.
    ///
    /// `None` derives the list from the connection's own worker addresses,
    /// which is the default; an empty list ([`WorkerList::empty`]) declines
    /// multi-head for this retriever.
    pub async fn with_workers(
        kinetica: Arc<Kinetica>,
        table_name: &str,
        record_type: Arc<Type>,
        workers: Option<WorkerList>,
    ) -> Result<Self, KineticaError> {
        Self::create(kinetica, table_name, record_type, workers, None).await
    }

    /// Creates a retriever with the given worker list and options.
    pub async fn with_options(
        kinetica: Arc<Kinetica>,
        table_name: &str,
        record_type: Arc<Type>,
        workers: Option<WorkerList>,
        options: Option<HashMap<String, String>>,
    ) -> Result<Self, KineticaError> {
        Self::create(kinetica, table_name, record_type, workers, options).await
    }
}

impl<T: DeserializeOwned + Send + 'static> RecordRetriever<T> {
    /// Creates a retriever returning records decoded as `T`, from the Avro
    /// schema of `record_type`. `workers` and `options` are as for
    /// [`RecordRetriever::with_options`].
    ///
    /// ```no_run
    /// use gpudb_api::{Kinetica, KineticaOptions, RecordRetriever, ShardKeyValue, Type};
    /// use std::sync::Arc;
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Row {
    ///     id: i32,
    ///     name: Option<String>,
    /// }
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?);
    /// let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
    /// let retriever = RecordRetriever::<Row>::new_typed(kinetica, "my_table", record_type, None, None).await?;
    /// let rows: Vec<Row> = retriever.get_by_key(&[ShardKeyValue::Int(1)], None).await?.data;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new_typed(
        kinetica: Arc<Kinetica>,
        table_name: &str,
        record_type: Arc<Type>,
        workers: Option<WorkerList>,
        options: Option<HashMap<String, String>>,
    ) -> Result<Self, KineticaError> {
        Self::create(kinetica, table_name, record_type, workers, options).await
    }
}

impl<R: RetrievedRecord> RecordRetriever<R> {
    /// The constructor the others call.
    async fn create(
        kinetica: Arc<Kinetica>,
        table_name: &str,
        record_type: Arc<Type>,
        workers: Option<WorkerList>,
        options: Option<HashMap<String, String>>,
    ) -> Result<Self, KineticaError> {
        let options = options.unwrap_or_default();

        // Keep track of how many times the connection has switched clusters,
        // and which one it is on, to tell later whether it failed over
        let num_cluster_switches = kinetica.num_cluster_switches();
        let current_head_node_url = kinetica.url();

        // Check whether the table is replicated; in case we can't figure it
        // out, we will pretend it is not. It's OK if the table does not exist
        // yet: the retriever may well be created before the table is.
        let table_replicated = match kinetica
            .show_table_raw(&ShowTableRequest {
                table_name: table_name.to_string(),
                options: HashMap::new(),
            })
            .await
        {
            Ok(response) => match response.table_descriptions.first() {
                Some(descriptions) => descriptions.iter().any(|d| d == "REPLICATED"),
                None => {
                    return Err(KineticaError::Config(
                        "The /show/table response has no table descriptions".to_string(),
                    ))
                }
            },
            Err(e) => {
                debug!("Could not read table '{table_name}' for record retrieval: {e}");
                false
            }
        };

        // If no worker list is given, use the rank URLs the connection has
        // already resolved, which is the route the bulk inserter takes.
        //
        // Those URLs were filtered through the user's hostname regex when the
        // cluster was discovered, and they already carry the empty slots for
        // ranks removed from the cluster. Building from them therefore honors
        // set_hostname_regex — and it avoids a second /show/system/properties
        // round trip that could itself fail.
        //
        // Note passing `None` does *not* disable multi-head; it is the default
        // behavior. To decline multi-head for one retriever, pass
        // `WorkerList::empty()`.
        let worker_list = match workers {
            Some(workers) => workers,
            None => {
                let workers = WorkerList::from_cluster(&kinetica);
                if workers.is_empty() {
                    info!("No worker rank URLs available for record retrieval; using head node instead.");
                }
                workers
            }
        };

        // Multi-head lookups are usable if they are turned on at the server and
        // the rank URLs are reachable from this client
        let multi_head_enabled = !worker_list.is_empty();

        let shard_key_builder = RecordKeyBuilder::from_type(&record_type)?;

        let worker_urls = if multi_head_enabled {
            Self::build_worker_urls_from_list(&worker_list)
        } else {
            Vec::new()
        };

        let last_multi_head_snapshot = kinetica.current_multi_head_snapshot();
        let retriever = Self {
            kinetica,
            table_name: table_name.to_string(),
            record_type,
            options: RwLock::new(options),
            table_replicated,
            worker_list: RwLock::new(Some(worker_list)),
            // Publish the initial routing state as one value, before anything
            // can read it. The shard mapping is not known yet; it is fetched
            // below and published as a snapshot of its own.
            routing: ArcSwap::from_pointee(RetrieverRouting::new(
                multi_head_enabled,
                worker_urls,
                None,
            )),
            shard_key_builder,
            shard_version: AtomicI64::new(0),
            shard_update_time: AtomicI64::new(0),
            num_cluster_switches: AtomicUsize::new(num_cluster_switches),
            // Remember the initial multi-head state, to detect a change after a
            // failover, fail-back or rebalance
            last_multi_head_snapshot: parking_lot::Mutex::new(last_multi_head_snapshot),
            monitor: tokio::sync::Mutex::new(()),
            pending_routing_table: parking_lot::Mutex::new(None),
            current_head_node_url: RwLock::new(current_head_node_url),
            last_used_url: RwLock::new(None),
            records: PhantomData,
        };

        if multi_head_enabled {
            // Fetch the shard mapping that goes with these URLs. A missing
            // mapping is reported when a lookup needs it.
            retriever.update_worker_queues(false).await?;
        }

        Ok(retriever)
    }

    // ==================== Getter Methods ====================

    /// Gets the connection from which records will be retrieved.
    pub fn kinetica(&self) -> &Arc<Kinetica> {
        &self.kinetica
    }

    /// Gets the name of the table from which records will be retrieved.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns whether this RecordRetriever is using the head rank for simple
    /// record fetching (not utilizing the server's key lookup feature).
    pub fn is_using_head_rank(&self) -> bool {
        !self.routing.load().multi_head_enabled
    }

    /// Returns whether this RecordRetriever is using worker ranks for key lookups.
    pub fn is_doing_worker_lookup(&self) -> bool {
        self.routing.load().multi_head_enabled
    }

    /// Gets the options currently used for the retriever methods.
    pub fn options(&self) -> HashMap<String, String> {
        self.options.read().clone()
    }

    /// Sets the options to be used for the retriever methods.
    pub fn set_options(&self, options: HashMap<String, String>) {
        *self.options.write() = options;
    }

    // ==================== Thread-safe URL Management ====================

    fn current_head_node_url(&self) -> Option<Url> {
        self.current_head_node_url.read().clone()
    }

    fn set_current_head_node_url(&self, url: Option<Url>) {
        *self.current_head_node_url.write() = url;
    }

    fn current_cluster_switch_count(&self) -> usize {
        self.num_cluster_switches.load(Ordering::SeqCst)
    }

    fn set_current_cluster_switch_count(&self, value: usize) {
        self.num_cluster_switches.store(value, Ordering::SeqCst);
    }

    /// Builds worker URLs from a worker list by appending /get/records path.
    fn build_worker_urls_from_list(worker_list: &WorkerList) -> Vec<Option<Url>> {
        worker_list
            .iter()
            .map(|opt_url| {
                opt_url.as_ref().map(|url| crate::gpudb::append_path_to_url(url, "/get/records"))
            })
            .collect()
    }

    // ==================== HA Failover Methods ====================

    /// Asks the connection to fail over to another cluster, and records where
    /// it ended up. The selection is the connection's; `switch_url` returns a
    /// cluster it found usable, having probed it.
    async fn force_failover(
        &self,
        old_url: &Url,
        old_cluster_switch_count: usize,
    ) -> Result<(), KineticaError> {
        let _monitor = self.monitor.lock().await;
        self.kinetica.switch_url(old_url, old_cluster_switch_count).await?;

        // Record where the connection ended up
        self.set_current_head_node_url(self.kinetica.url());
        self.set_current_cluster_switch_count(self.kinetica.num_cluster_switches());
        Ok(())
    }

    // ==================== Worker Queue Update Methods ====================

    /// Updates the shard mapping and, optionally, rebuilds the worker URLs.
    ///
    /// With the shard version unchanged and the connection's multi-head
    /// snapshot the one the URLs were built from, nothing has moved, and only
    /// a rebuild that was asked for is attempted. Otherwise the new mapping is
    /// published and the URLs are rebuilt if asked for, re-probing the cluster
    /// when the shard version changed.
    ///
    /// Returns whether the shard mapping was updated.
    async fn update_worker_queues(&self, do_reconstruct_worker_urls: bool) -> Result<bool, KineticaError> {
        let _monitor = self.monitor.lock().await;
        self.update_worker_queues_locked(do_reconstruct_worker_urls, true).await
    }

    /// [`Self::update_worker_queues`], for a caller holding `monitor`.
    ///
    /// `publish_shard_mapping` is whether a newly fetched shard mapping is
    /// published on its own. The rebuild, about to publish a whole routing
    /// state of its own, passes `false` and takes the mapping from
    /// `pending_routing_table`, so a new mapping is never published alongside
    /// URLs it does not describe.
    fn update_worker_queues_locked(
        &self,
        do_reconstruct_worker_urls: bool,
        publish_shard_mapping: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, KineticaError>> + Send + '_>> {
        Box::pin(async move {
            let reconstruct_worker_urls = do_reconstruct_worker_urls && self.routing.load().multi_head_enabled;
            debug!("Reconstruct worker URLs?: {reconstruct_worker_urls}");

            // Get the latest shard mapping information; this request may fail over
            let shard_info = match self
                .kinetica
                .admin_show_shards_raw(&AdminShowShardsRequest::default())
                .await
            {
                Ok(info) => info,
                Err(e) => {
                    // Only a connection failure returns false; any other failure,
                    // a submit failure (`Http`) included, is returned as an error.
                    if had_connection_failure(&e) {
                        debug!("Had connection failure: {e}");
                        return Ok(false);
                    }
                    return Err(e);
                }
            };

            let new_shard_version = shard_info.version;
            let shard_mapping_changed = self.shard_version.load(Ordering::SeqCst) != new_shard_version;

            if !shard_mapping_changed {
                // Also check whether the connection moved to another cluster, or
                // re-probed this one, since the URLs were last built
                let current_snapshot = self.kinetica.current_multi_head_snapshot();
                let unchanged = same_snapshot(&current_snapshot, &self.last_multi_head_snapshot.lock());
                if unchanged {
                    debug!("Same cluster and shard version");
                    if reconstruct_worker_urls {
                        return self.reconstruct_worker_urls_locked(false).await;
                    }
                    debug!("Returning false");
                    return Ok(false);
                }
                // Record the snapshot now current, so the next call compares
                // against it rather than against the one left behind
                *self.last_multi_head_snapshot.lock() = current_snapshot;
                self.set_current_cluster_switch_count(self.kinetica.num_cluster_switches());
            }

            // Save the new shard version and when it was updated
            self.shard_version.store(new_shard_version, Ordering::SeqCst);
            self.shard_update_time.store(now_millis(), Ordering::SeqCst);

            // Record the new shard mapping, and publish it unless the caller is
            // about to publish a routing state of its own
            *self.pending_routing_table.lock() = Some(shard_info.rank.clone());
            if publish_shard_mapping {
                let new_routing = self.routing.load().with_routing_table(Some(shard_info.rank));
                self.routing.store(Arc::new(new_routing));
            }

            // We may have failed over during /admin/show/shards
            self.set_current_head_node_url(self.kinetica.url());
            self.set_current_cluster_switch_count(self.kinetica.num_cluster_switches());

            if reconstruct_worker_urls {
                self.reconstruct_worker_urls_locked(shard_mapping_changed).await?;
            }

            debug!("Returning true");
            Ok(true)
        })
    }

    /// Rebuilds the worker URLs for the cluster the connection is on now.
    ///
    /// `topology_may_have_moved` is what the caller observed: `true` when the
    /// server reported a shard mapping change, so the connection re-probes
    /// before answering; `false` after a cluster change, which the switch
    /// already probed. Returns whether the URLs were rebuilt. The caller holds
    /// `monitor`.
    async fn reconstruct_worker_urls_locked(&self, topology_may_have_moved: bool) -> Result<bool, KineticaError> {
        // A caller-supplied empty list is authoritative and never replaced; see
        // the matching note in the bulk inserter's rebuild.
        {
            let worker_list = self.worker_list.read();
            let Some(worker_list) = worker_list.as_ref() else {
                return Err(KineticaError::Config("No worker list exists!".to_string()));
            };
            if worker_list.disables_multi_head() {
                debug!("Worker list declines multi-head; not rebuilding");
                return Ok(false);
            }
        }

        // Ask the connection for the current cluster's addresses, and build
        // from that same answer
        let snapshot = self.kinetica.acquire_multi_head_snapshot(topology_may_have_moved).await;
        let new_worker_list = WorkerList::from_snapshot(snapshot.as_deref());

        // Two lists are equal when they name the same ranks, holes included
        if self.worker_list.read().as_ref() == Some(&new_worker_list) {
            debug!("Worker list remained the same; returning false");
            return Ok(false);
        }

        // Update the worker list, and remember the exact answer these
        // addresses came from, not the cluster
        *self.worker_list.write() = Some(new_worker_list.clone());
        *self.last_multi_head_snapshot.lock() = snapshot;

        // Recompute whether multi-head is still usable
        let multi_head_enabled = !new_worker_list.is_empty();
        let worker_urls = Self::build_worker_urls_from_list(&new_worker_list);

        // Fetch the shard mapping for the new URL set without letting it be
        // published on its own, so it is never paired with the URLs still in
        // place. The fetch leaves nothing pending when the shard version has
        // not moved, and the mapping in force then still applies. A failure
        // here leaves the new worker list recorded but the routing state as it
        // was.
        let mut routing_table = self.routing.load().routing_table.clone();
        if multi_head_enabled {
            *self.pending_routing_table.lock() = None;
            self.update_worker_queues_locked(false, false).await?;
            if let Some(pending) = self.pending_routing_table.lock().clone() {
                routing_table = Some(pending);
            }
        }

        // Publish the whole new routing state with one store, so a concurrent
        // lookup sees either all of the old state or all of the new one
        self.routing.store(Arc::new(RetrieverRouting::new(
            multi_head_enabled,
            worker_urls,
            routing_table,
        )));

        debug!("Worker list was updated, returning true");
        Ok(true)
    }

    // ==================== Record Retrieval Methods ====================

    /// Decides where a retrieval goes and builds its filter: returns whether
    /// to look up on a worker, the filter, and whether key values were given.
    ///
    /// No key values (an empty slice) reads via the head node, unless the
    /// table is replicated and, for `get_by_key`, an expression is given. Key
    /// values must be given for a keyed table; they become a filter, joined
    /// with the expression if there is one.
    fn prepare_retrieval_params(
        &self,
        routing: &RetrieverRouting,
        key_values: &[ShardKeyValue<'_>],
        expression: Option<&str>,
        offset: i64,
        check_expression_for_replicated: bool,
    ) -> Result<(bool, Option<String>, bool), KineticaError> {
        let mut do_worker_lookup = routing.multi_head_enabled;
        let mut composite_expression = expression.map(str::to_string);
        let key_values_specified = !key_values.is_empty();

        // A new query (not a later page) may go to another worker
        if offset < 1 {
            *self.last_used_url.write() = None;
        }

        if !key_values_specified {
            // Use head rank if table is [randomly] sharded and no keys are given,
            // or if the table is replicated and no expression (or keys) is given.
            if !self.table_replicated
                || (check_expression_for_replicated && expression.is_none_or(str::is_empty))
            {
                do_worker_lookup = false;
            }
        } else {
            // Eliminate the case where key values are given, but the table has
            // no key columns with which to associate them
            if !self.shard_key_builder.has_key() {
                return Err(KineticaError::Config(
                    "Cannot associate the specified key values with columns, as the table has no primary or shard key.".to_string(),
                ));
            }

            // Since we have a keyed table, build the composite expression to be
            // used for both sharded-with-PK/SK and replicated-with-PK tables.
            // This runs before the retrieval is attempted, so a bad key value
            // is reported directly — never retried, and never the
            // cause of a shard-mapping refresh.
            let key_expression = self.shard_key_builder.build_expression(key_values)?;

            // If the key expression exists, but the filter expression doesn't,
            // use the key, otherwise if both exist, concatenate both
            if let Some(key_expression) = key_expression.filter(|k| !k.is_empty()) {
                composite_expression = match expression {
                    Some(expr) if !expr.is_empty() => {
                        Some(format!("({key_expression}) and ({expr})"))
                    }
                    _ => Some(key_expression),
                };
            }
        }

        Ok((do_worker_lookup, composite_expression, key_values_specified))
    }

    /// Recovers from a failed retrieval, and decides whether to retry.
    ///
    /// Every failure but an authorization one (which the caller propagates
    /// before getting here) comes through, and is handled in two steps:
    ///
    /// 1. A failover trigger ([`is_failover_trigger`]) fails over to another
    ///    cluster in the HA ring. Anything else — a server-side error such as
    ///    a bad filter, a submit failure, an unroutable key — does not.
    /// 2. Whatever the failure, the shard mapping and worker URLs are
    ///    refreshed, because a failure may be the symptom of a changed
    ///    cluster. Known cost: a failure that has nothing to do with the
    ///    cluster, a bad filter say, still costs an `/admin/show/shards` round
    ///    trip, and is retried if the mapping happened to change meanwhile.
    ///
    /// Returns Ok(true) if the retrieval should be retried, Ok(false) if not,
    /// or Err when failover has run out of clusters.
    async fn handle_retrieval_error(
        &self,
        error: &KineticaError,
        curr_url: &Option<Url>,
        current_count_cluster_switches: usize,
        retrieval_attempt_timestamp: i64,
    ) -> Result<bool, KineticaError> {
        let mut did_failover_succeed = false;

        if is_failover_trigger(error) {
            warn!("Got an exit or other connection error: {error}");

            if let Some(ref url) = curr_url {
                match self
                    .force_failover(url, current_count_cluster_switches)
                    .await
                {
                    Ok(()) => did_failover_succeed = true,
                    Err(e2) => {
                        // We have now tried every cluster in the HA ring and
                        // circled back, so propagate to the caller: what
                        // failed, as its error (or that error's cause)
                        // prints, then why the failover did. Running out of
                        // clusters is reported as a connection failure, so it
                        // is a `Connection` error. The cost is the error
                        // chain — the original is in the message, not
                        // attached as a source.
                        return Err(KineticaError::Connection(format!(
                            "{}; {}",
                            error.cause_description(),
                            e2.message()
                        )));
                    }
                }
            }
        } else {
            debug!("Caught an error: {error}");
        }

        debug!("Did failover succeed? {did_failover_succeed}");

        // Update worker queues. A failure to rebuild must not displace the error
        // we are recovering from; report it and carry on to the retry decision
        // with the queues left unchanged.
        debug!("Updating worker queues");
        let updated_worker_queues = match self
            .update_worker_queues(true)
            .await
        {
            Ok(updated) => updated,
            Err(rebuild_err) => {
                warn!(
                    "Could not update the worker queues while recovering from <{error}>: {rebuild_err}"
                );
                false
            }
        };
        debug!("Did we update the worker queue? {updated_worker_queues}");

        let shard_update_time = self.shard_update_time.load(Ordering::SeqCst);
        let retry = did_failover_succeed
            || updated_worker_queues
            || retrieval_attempt_timestamp < shard_update_time;

        debug!("'retry' value: {retry}");
        Ok(retry)
    }

    /// Retrieves records with the given key values and filter expression.
    ///
    /// This method operates in four modes, depending on the parameters passed:
    /// - **key values only**: attempts a direct-to-rank lookup for records matching the given key values
    /// - **key values and expression**: attempts a direct-to-rank lookup for records matching the
    ///   given key values, filtering them by the given expression
    /// - **expression only**: requests, via the head rank, all records in the table matching
    ///   the given filter expression
    /// - **neither**: retrieves all records from the table via the head rank
    ///
    /// # Arguments
    ///
    /// * `key_values` - The key values to use for the lookup, one per key column in
    ///   key order, or none; these must correspond to either the explicit or implicit
    ///   shard key for sharded tables or the primary key of replicated tables. Each
    ///   must be of a type its column takes (see [`RecordKeyBuilder::build_expression`]).
    /// * `expression` - A filter expression that will be applied to the data requested by the
    ///   key values; if no key values are specified this filter will be applied to all of the
    ///   data in the target table
    ///
    /// # Returns
    ///
    /// The requested records, decoded.
    pub async fn get_by_key(
        &self,
        key_values: &[ShardKeyValue<'_>],
        expression: Option<&str>,
    ) -> Result<GetRecordsResponse<R>, KineticaError> {
        self.get_by_key_with_offset(key_values, expression, 0).await
    }

    /// Retrieves records with the given key values and filter expression, starting from an offset.
    ///
    /// # Arguments
    ///
    /// * `key_values` - The key values to use for the lookup, or none
    /// * `expression` - A filter expression
    /// * `offset` - Offset of the record(s) within the result set to return
    ///
    /// # Returns
    ///
    /// The requested records, decoded.
    pub async fn get_by_key_with_offset(
        &self,
        key_values: &[ShardKeyValue<'_>],
        expression: Option<&str>,
        offset: i64,
    ) -> Result<GetRecordsResponse<R>, KineticaError> {
        // Take the routing state once and route this lookup entirely from that
        // snapshot. A rebuild running concurrently publishes a new one; this
        // lookup then goes wherever the state it was routed against said, which
        // is consistent, rather than to a destination assembled from both.
        // `load_full` rather than `load`: the snapshot is held across awaits,
        // and an ArcSwap guard is not Send.
        let routing = self.routing.load_full();

        let (do_worker_lookup, composite_expression, key_values_specified) =
            self.prepare_retrieval_params(&routing, key_values, expression, offset, true)?;

        // Create the options for the key lookup; first include general options
        let mut retrieval_options = self.options.read().clone();

        // Add the retrieval expression to the options, if any
        if let Some(expr) = composite_expression.as_deref().filter(|e| !e.is_empty()) {
            retrieval_options.insert("expression".to_string(), expr.to_string());

            // If key values were specified, add fast index lookup option
            if key_values_specified {
                retrieval_options.insert("fast_index_lookup".to_string(), "true".to_string());
            }
        }

        let request = GetRecordsRequest {
            table_name: self.table_name.clone(),
            offset,
            limit: END_OF_SET,
            encoding: "binary".to_string(),
            options: retrieval_options,
        };

        let retrieval_attempt_timestamp = now_millis();
        let curr_url = self.current_head_node_url();
        let current_count_cluster_switches = self.current_cluster_switch_count();

        let result = self
            .try_get_by_key(&routing, do_worker_lookup, key_values, &request, composite_expression.as_deref())
            .await;

        match result {
            Ok(records) => Ok(records),
            // Any permission related problem should get propagated
            Err(KineticaError::Unauthorized(msg)) => Err(KineticaError::Unauthorized(msg)),
            Err(e) => {
                let should_retry = self
                    .handle_retrieval_error(
                        &e,
                        &curr_url,
                        current_count_cluster_switches,
                        retrieval_attempt_timestamp,
                    )
                    .await?;

                if should_retry {
                    // Retried, with two known risks:
                    //
                    // - The retry is
                    //   `get_by_key_with_offset(key_values, expression, 0)`,
                    //   which starts again from offset 0: a failed read of a
                    //   later page is answered with the first page. The
                    //   original expression is reused, not the composite one.
                    // - Nothing bounds the number of retries. Each one is a
                    //   whole new attempt with its own recovery, so a cluster
                    //   that keeps failing over successfully, or a shard
                    //   mapping that keeps changing, retries again. The
                    //   recursion is boxed, so it grows the heap rather than
                    //   the stack.
                    //
                    // Keep the original failure if the retry also fails. It is the
                    // diagnosis; the retry's own failure is usually a consequence
                    // of it, and replacing it loses the only useful message.
                    return Box::pin(self.get_by_key_with_offset(key_values, expression, 0))
                        .await
                        .map_err(|retry_err| {
                            KineticaError::caused(
                                format!(
                                    "{}; the retry after recovery also failed: {}",
                                    e.message(),
                                    retry_err.message()
                                ),
                                e,
                            )
                        });
                }

                Err(KineticaError::caused(e.message(), e))
            }
        }
    }

    /// One attempt at a `get_by_key`. A failure anywhere in it, decoding and
    /// the refresh after rerouted data included, goes through the caller's
    /// recovery.
    async fn try_get_by_key(
        &self,
        routing: &RetrieverRouting,
        do_worker_lookup: bool,
        key_values: &[ShardKeyValue<'_>],
        request: &GetRecordsRequest,
        composite_expression: Option<&str>,
    ) -> Result<GetRecordsResponse<R>, KineticaError> {
        let expression_text = composite_expression.unwrap_or("null");
        let response = if !do_worker_lookup {
            debug!("Retrieving records from rank-0 with <{expression_text}>");
            self.kinetica.get_records_raw(request).await?
        } else {
            let url = self.select_worker_url(routing, key_values)?;
            debug!("Retrieving records from <{url}> with <{expression_text}>");
            self.kinetica.get_records_from_url(url.as_str(), request).await?
        };

        // Check if shard re-balancing is under way at the server; if so, we
        // need to update the shard mapping
        if crate::gpudb::is_data_rerouted(&response.info) {
            self.update_worker_queues(true).await?;
        }

        Ok(GetRecordsResponse {
            data: R::decode_records(&self.record_type, &response.records_binary)?,
            table_name: response.table_name,
            type_name: response.type_name,
            type_schema: response.type_schema,
            total_number_of_records: response.total_number_of_records,
            has_more_records: response.has_more_records,
            info: HashMap::new(),
        })
    }

    /// Retrieves specific columns with the given key values and filter expression.
    ///
    /// # Arguments
    ///
    /// * `columns` - The requested columns (which can include expressions). May use "*" for all columns.
    /// * `key_values` - The key values to use for the lookup, or none
    /// * `expression` - A filter expression
    ///
    /// # Returns
    ///
    /// The requested records, typed by the response.
    pub async fn get_columns_by_key(
        &self,
        columns: Vec<String>,
        key_values: &[ShardKeyValue<'_>],
        expression: Option<&str>,
    ) -> Result<GetRecordsByColumnResponse, KineticaError> {
        self.get_columns_by_key_with_offset(columns, key_values, expression, 0)
            .await
    }

    /// Retrieves specific columns with the given key values and filter expression, starting from an offset.
    ///
    /// # Arguments
    ///
    /// * `columns` - The requested columns
    /// * `key_values` - The key values to use for the lookup, or none
    /// * `expression` - A filter expression
    /// * `offset` - Offset of the record(s) within the result set to return
    ///
    /// # Returns
    ///
    /// The requested records, typed by the response.
    pub async fn get_columns_by_key_with_offset(
        &self,
        columns: Vec<String>,
        key_values: &[ShardKeyValue<'_>],
        expression: Option<&str>,
        offset: i64,
    ) -> Result<GetRecordsByColumnResponse, KineticaError> {
        // Take the routing state once; see the note in get_by_key_with_offset.
        let routing = self.routing.load_full();

        let (do_worker_lookup, composite_expression, _) =
            self.prepare_retrieval_params(&routing, key_values, expression, offset, false)?;

        // Create the options for the key lookup; first include general options
        let mut retrieval_options = self.options.read().clone();

        // Add the retrieval expression to the options, if any
        if let Some(expr) = composite_expression.as_deref().filter(|e| !e.is_empty()) {
            retrieval_options.insert("expression".to_string(), expr.to_string());
        }

        let request = GetRecordsByColumnRequest {
            table_name: self.table_name.clone(),
            column_names: columns,
            offset,
            limit: END_OF_SET,
            encoding: "binary".to_string(),
            options: retrieval_options,
        };

        let retrieval_attempt_timestamp = now_millis();
        let curr_url = self.current_head_node_url();
        let current_count_cluster_switches = self.current_cluster_switch_count();

        let result = self
            .try_get_columns_by_key(&routing, do_worker_lookup, key_values, &request, composite_expression.as_deref())
            .await;

        match result {
            Ok(records) => Ok(records),
            // Any permission related problem should get propagated
            Err(KineticaError::Unauthorized(msg)) => Err(KineticaError::Unauthorized(msg)),
            Err(e) => {
                let should_retry = self
                    .handle_retrieval_error(
                        &e,
                        &curr_url,
                        current_count_cluster_switches,
                        retrieval_attempt_timestamp,
                    )
                    .await?;

                if should_retry {
                    // Retried from offset 0 and without a bound; see the
                    // matching note in get_by_key_with_offset for the risks
                    // that carries. Keep the original failure if the retry also
                    // fails.
                    return Box::pin(self.get_columns_by_key_with_offset(
                        request.column_names,
                        key_values,
                        expression,
                        0,
                    ))
                    .await
                    .map_err(|retry_err| {
                        KineticaError::caused(
                            format!("{}; the retry after recovery also failed: {}", e.message(), retry_err.message()),
                            e,
                        )
                    });
                }

                Err(KineticaError::caused(e.message(), e))
            }
        }
    }

    /// One attempt at a `get_columns_by_key`.
    async fn try_get_columns_by_key(
        &self,
        routing: &RetrieverRouting,
        do_worker_lookup: bool,
        key_values: &[ShardKeyValue<'_>],
        request: &GetRecordsByColumnRequest,
        composite_expression: Option<&str>,
    ) -> Result<GetRecordsByColumnResponse, KineticaError> {
        let expression_text = composite_expression.unwrap_or("null");
        let response = if !do_worker_lookup {
            debug!("Retrieving records from rank-0 with <{expression_text}>");
            self.kinetica.get_records_by_column_raw(request).await?
        } else {
            let url = self.select_worker_url(routing, key_values)?;
            debug!("Retrieving records from <{url}/bycolumn> with <{expression_text}>");
            let bycolumn_url = crate::gpudb::append_path_to_url(&url, "/bycolumn").to_string();
            self.kinetica.get_records_by_column_from_url(&bycolumn_url, request).await?
        };

        // Check if shard re-balancing is under way at the server; if so, we
        // need to update the shard mapping
        if crate::gpudb::is_data_rerouted(&response.info) {
            self.update_worker_queues(true).await?;
        }

        let data_type = Arc::new(Type::from_dynamic_schema(
            &response.response_schema_str,
            &response.binary_encoded_response,
        )?);
        let data = GenericRecord::transpose(
            &response.response_schema_str,
            &response.binary_encoded_response,
            &data_type,
        )?;
        Ok(GetRecordsByColumnResponse {
            table_name: response.table_name,
            data_type,
            data,
            total_number_of_records: response.total_number_of_records,
            has_more_records: response.has_more_records,
            info: response.info,
        })
    }

    // ==================== Helper Methods ====================

    /// Selects the worker to look up on: any live one for a replicated table,
    /// keeping it for later pages, and otherwise the one owning the key's
    /// shard.
    fn select_worker_url(
        &self,
        routing: &RetrieverRouting,
        key_values: &[ShardKeyValue<'_>],
    ) -> Result<Url, KineticaError> {
        if self.table_replicated {
            // For replicated tables, use the same worker for new pages (i.e.,
            // offset > 0) in case the data is in a different order on
            // different workers
            if let Some(url) = self.last_used_url.read().clone() {
                return Ok(url);
            }
            let url = Self::random_live_worker_url(routing)?;
            *self.last_used_url.write() = Some(url.clone());
            return Ok(url);
        }

        // Not a replicated table; so calculate the shard to figure out which
        // worker rank contains the requested records
        let shard_key = self.shard_key_builder.build(key_values).map_err(|e| {
            KineticaError::caused(
                format!(
                    "Unable to calculate the shard value; please check data for unshardable values: {}",
                    e.message()
                ),
                e,
            )
        })?;

        // Routing by key requires the shard mapping. If the server could not
        // supply one, surface it.
        let Some(routing_table) = routing.routing_table.as_ref().filter(|t| !t.is_empty()) else {
            return Err(KineticaError::Api(format!(
                "No shard mapping is available for table '{}'; cannot route the lookup to a worker rank.",
                self.table_name
            )));
        };

        // A worker lookup on a sharded table has key values, which a table
        // without a key has been refused
        let shard_key = shard_key.expect("a keyed table has a shard key");
        // The index is signed: a rank of 0 in the mapping is -1
        Self::live_worker_url(routing, shard_key.route(routing_table) as isize)
    }

    /// Returns the URL of a randomly chosen live worker rank, for lookups that
    /// are not routed by a shard key.
    fn random_live_worker_url(routing: &RetrieverRouting) -> Result<Url, KineticaError> {
        if routing.live_worker_indices.is_empty() {
            return Err(KineticaError::Api(format!(
                "No live worker rank is available for record retrieval; all {} \
                 worker rank slot(s) are empty (removed ranks)",
                routing.worker_urls.len()
            )));
        }

        let idx = rand::rng().random_range(0..routing.live_worker_indices.len());
        Ok(routing.worker_urls[routing.live_worker_indices[idx]]
            .as_ref()
            .expect("live_worker_indices only names occupied slots")
            .clone())
    }

    /// Returns the URL of the worker rank at `worker_index`, as produced by
    /// the shard mapping. Fails for an index past either end of the worker
    /// list, and for the empty slot a removed rank leaves behind.
    fn live_worker_url(routing: &RetrieverRouting, worker_index: isize) -> Result<Url, KineticaError> {
        let count = routing.worker_urls.len();
        let slot = usize::try_from(worker_index).ok().and_then(|i| routing.worker_urls.get(i));
        match slot {
            Some(Some(url)) => Ok(url.clone()),
            Some(None) => Err(KineticaError::Api(format!(
                "Worker rank with index {worker_index} has been removed from the cluster; \
                 it cannot serve records (# worker ranks {count}); the shard mapping may \
                 need to be updated"
            ))),
            None => Err(KineticaError::Api(format!(
                "Sharded worker index is out of bound: {worker_index} (# worker ranks {count})"
            ))),
        }
    }
}

/// The time now, in milliseconds since the epoch.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_retriever_options() {
        let mut options = RecordRetrieverOptions::new();
        options
            .set_expression("id > 100")
            .set_fast_index_lookup(true);

        assert_eq!(options.options.get("expression"), Some(&"id > 100".to_string()));
        assert_eq!(options.options.get("fast_index_lookup"), Some(&"true".to_string()));
    }

    #[test]
    fn test_end_of_set_constant() {
        assert_eq!(END_OF_SET, -9999);
    }

    // ==================== Error classification ====================

    // An exit or a connection failure fails over.
    #[test]
    fn exits_and_connection_failures_trigger_failover() {
        assert!(is_failover_trigger(&KineticaError::Exit("Kinetica is exiting".into())));
        assert!(is_failover_trigger(&KineticaError::Connection("refused".into())));
    }

    // A submit failure (`Http`) does not fail over, though it used to be taken
    // as a connection failure. Nor does an ordinary server error such as a bad
    // filter.
    #[test]
    fn other_failures_do_not_trigger_failover() {
        for e in [
            KineticaError::Http("Failed to read response".into()),
            KineticaError::Api("Invalid expression".into()),
            KineticaError::Config("No shard mapping".into()),
            KineticaError::Unauthorized("denied".into()),
            KineticaError::caused("wrapped", KineticaError::Connection("refused".into())),
        ] {
            assert!(!is_failover_trigger(&e), "{e:?} must not fail over");
        }
    }

    // The shard-mapping refresh swallows only a connection failure: an exit
    // and a submit failure are reported.
    #[test]
    fn only_a_connection_failure_is_swallowed_by_the_shard_refresh() {
        assert!(had_connection_failure(&KineticaError::Connection("refused".into())));
        for e in [
            KineticaError::Exit("Kinetica is exiting".into()),
            KineticaError::Http("Failed to read response".into()),
            KineticaError::Api("boom".into()),
        ] {
            assert!(!had_connection_failure(&e), "{e:?} must be reported");
        }
    }
}

/// Invariants of the lookup routing snapshot.
///
/// The destination of a lookup depends on the worker URLs, which of those slots
/// hold a live worker, and the shard mapping. These pin the rules that keep the
/// three from disagreeing — the reason they live in one immutable value rather
/// than behind four independent locks, which is what allowed a lookup to be
/// routed against a half-rebuilt state.
#[cfg(test)]
mod retriever_routing_tests {
    use super::*;

    fn url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}/get/records")).unwrap()
    }

    // ==================== Live-worker indices are derived ====================

    /// They hold indices *into* the URL list, so computing them from that list
    /// in the constructor makes a mismatched pair unconstructible rather than
    /// merely rejected.
    #[test]
    fn live_worker_indices_name_only_occupied_slots() {
        let routing = RetrieverRouting::new(
            true,
            vec![Some(url(9192)), None, Some(url(9194))],
            None,
        );

        assert_eq!(routing.live_worker_indices, vec![0, 2]);
    }

    /// A removed rank keeps its slot, so the indices are rank-aligned rather
    /// than compacted — compacting would send every rank above the hole to the
    /// wrong host.
    #[test]
    fn a_removed_rank_keeps_its_slot_in_the_url_list() {
        let routing = RetrieverRouting::new(
            true,
            vec![Some(url(9192)), None, Some(url(9194))],
            None,
        );

        assert_eq!(routing.worker_urls.len(), 3);
        assert!(routing.worker_urls[1].is_none());
    }

    #[test]
    fn an_all_holes_list_has_no_live_workers() {
        let routing = RetrieverRouting::new(true, vec![None, None], None);
        assert!(routing.live_worker_indices.is_empty());
    }

    #[test]
    fn no_urls_means_no_live_workers() {
        let routing = RetrieverRouting::new(false, Vec::new(), None);
        assert!(routing.live_worker_indices.is_empty());
        assert!(!routing.multi_head_enabled);
    }

    // ==================== Mapping-only changes ====================

    /// The mapping describes shards, not URLs, so it can be replaced on its own
    /// — and the derived indices must come out identical, not merely equal by
    /// luck.
    #[test]
    fn a_new_shard_mapping_leaves_the_urls_and_derived_indices_alone() {
        let before = RetrieverRouting::new(
            true,
            vec![Some(url(9192)), None, Some(url(9194))],
            Some(vec![1, 3]),
        );

        let after = before.with_routing_table(Some(vec![3, 1]));

        assert_eq!(after.routing_table, Some(vec![3, 1]));
        assert_eq!(after.worker_urls, before.worker_urls);
        assert_eq!(after.live_worker_indices, before.live_worker_indices);
        assert_eq!(after.multi_head_enabled, before.multi_head_enabled);
    }

    /// A rebuild that drops to head-node-only must take the mapping's
    /// applicability with it rather than leaving a stale one addressable.
    #[test]
    fn a_rebuild_can_turn_multi_head_off() {
        let before = RetrieverRouting::new(true, vec![Some(url(9192))], Some(vec![1]));
        let after = RetrieverRouting::new(false, Vec::new(), before.routing_table.clone());

        assert!(!after.multi_head_enabled);
        assert!(after.live_worker_indices.is_empty());
    }
}

/// Retrieving, and rebuilding, against stand-in servers.
#[cfg(test)]
mod server_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, RecordValue};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode, FakeServer};
    use crate::{
        AdminShowShardsResponse, KineticaOptions, RawGetRecordsByColumnResponse, RawGetRecordsResponse,
        ShowTableResponse,
    };
    use parking_lot::Mutex;

    fn options() -> KineticaOptions {
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        options
    }

    /// The table's type: keyed on `id`, with a nullable `name`.
    fn record_type() -> Arc<Type> {
        Arc::new(
            Type::new(
                "",
                vec![
                    Column::with_properties("id", ColumnType::Integer, vec!["primary_key".to_string()]),
                    Column::with_properties("name", ColumnType::String, vec!["nullable".to_string()]),
                ],
            )
            .unwrap(),
        )
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Row {
        id: i32,
        name: Option<String>,
    }

    fn records_response(rows: &[Row], info: &[(&str, &str)]) -> Vec<u8> {
        let schema: serde_avro_fast::Schema = record_type().schema_string().parse().unwrap();
        let response = RawGetRecordsResponse {
            table_name: "t".to_string(),
            type_name: "t_type".to_string(),
            type_schema: record_type().schema_string().to_string(),
            records_binary: rows.iter().map(|r| ByteBuf::from(encode(&schema, r))).collect(),
            records_json: Vec::new(),
            total_number_of_records: rows.len() as i64,
            has_more_records: false,
            info: info.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        };
        encode(&crate::generated::GET_RECORDS_RESPONSE_SCHEMA, &response)
    }

    fn shards_response(version: i64) -> Vec<u8> {
        let response = AdminShowShardsResponse { version, rank: vec![1; 16], tom: Vec::new(), info: HashMap::new() };
        encode(&crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA, &response)
    }

    fn table_response(descriptions: &[&str]) -> Vec<u8> {
        let response = ShowTableResponse {
            table_name: "t".to_string(),
            table_names: vec!["t".to_string()],
            table_descriptions: vec![descriptions.iter().map(|d| d.to_string()).collect()],
            ..Default::default()
        };
        encode(&crate::generated::SHOW_TABLE_RESPONSE_SCHEMA, &response)
    }

    /// The bodies of the `/get/records` requests a server received.
    type Requests = Arc<Mutex<Vec<GetRecordsRequest>>>;

    /// Serves the table from `head`: `/show/table` with `descriptions`,
    /// `/admin/show/shards` with every shard on rank 1 (failing after the
    /// first answer if `shards_fail_later`), and `/get/records` with `rows`.
    fn serve_table(head: &FakeServer, descriptions: &'static [&'static str], shards_fail_later: bool) -> Requests {
        let requests: Requests = Arc::default();
        let received = requests.clone();
        let shard_answers = Arc::new(AtomicUsize::new(0));
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if path.ends_with("/show/table") {
                Some(Ok(table_response(descriptions)))
            } else if path.ends_with("/admin/show/shards") {
                if shards_fail_later && shard_answers.fetch_add(1, Ordering::SeqCst) > 0 {
                    Some(Err("shards unavailable".to_string()))
                } else {
                    Some(Ok(shards_response(1)))
                }
            } else if path.ends_with("/get/records") {
                received.lock().push(
                    serde_avro_fast::from_datum_slice(body, &crate::generated::GET_RECORDS_REQUEST_SCHEMA).unwrap(),
                );
                Some(Ok(records_response(&[Row { id: 1, name: None }], &[])))
            } else {
                None
            }
        }));
        requests
    }

    /// Serves `/get/records` from a worker with `rows` and `info`.
    fn serve_records(worker: &FakeServer, rows: Vec<(i32, Option<&'static str>)>, info: &'static [(&'static str, &'static str)]) -> Requests {
        let requests: Requests = Arc::default();
        let received = requests.clone();
        *worker.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if !path.ends_with("/get/records") {
                return None;
            }
            received.lock().push(
                serde_avro_fast::from_datum_slice(body, &crate::generated::GET_RECORDS_REQUEST_SCHEMA).unwrap(),
            );
            let rows: Vec<Row> = rows.iter().map(|(id, name)| Row { id: *id, name: name.map(str::to_string) }).collect();
            Some(Ok(records_response(&rows, info)))
        }));
        requests
    }

    async fn connect(head: &FakeServer, workers: &[&FakeServer]) -> Arc<Kinetica> {
        let workers: Vec<String> = workers.iter().map(|w| address(w)).collect();
        let workers: Vec<&str> = workers.iter().map(String::as_str).collect();
        *head.behavior.properties.lock() = cluster_properties(&address(head), &workers);
        Arc::new(Kinetica::new(vec![head.url.to_string()], options()).await.expect("client"))
    }

    // A lookup the server refuses fails with the server's message, with no
    // kind of error before it.
    #[tokio::test]
    async fn a_refused_lookup_fails_with_the_servers_message() {
        let head = fake_server::start("127.0.0.63").await;
        let worker = fake_server::start("127.0.0.64").await;
        serve_table(&head, &[], false);
        *worker.behavior.handler.lock() = Some(Arc::new(|path: &str, _: &[u8]| {
            path.ends_with("/get/records").then(|| Err("Table t is locked".to_string()))
        }));
        let k = connect(&head, &[&worker]).await;
        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");

        let err = retriever.get_by_key(&[ShardKeyValue::Int(7)], None).await.unwrap_err();
        assert_eq!(err.message(), "Table t is locked");
        assert_eq!(err.to_string(), "Table t is locked");
    }

    // A lookup with key values: a filter of the key, with a fast
    // index lookup, sent to the worker owning the key's shard, and the
    // records decoded by the retriever's type.
    #[tokio::test]
    async fn a_key_lookup_goes_to_the_worker_owning_the_key_and_is_decoded() {
        let head = fake_server::start("127.0.0.51").await;
        let worker = fake_server::start("127.0.0.52").await;
        let head_requests = serve_table(&head, &[], false);
        let worker_requests = serve_records(&worker, vec![(7, Some("seven"))], &[("note", "dropped")]);
        let k = connect(&head, &[&worker]).await;

        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");
        let result = retriever.get_by_key(&[ShardKeyValue::Int(7)], None).await.expect("records");

        assert_eq!(result.table_name, "t");
        assert_eq!(result.type_name, "t_type");
        assert_eq!(result.total_number_of_records, 1);
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].get(0), Some(&RecordValue::Int(7)));
        assert_eq!(result.data[0].get(1), Some(&RecordValue::String("seven".to_string())));
        assert!(result.info.is_empty(), "the retriever leaves the info empty");

        assert!(head_requests.lock().is_empty());
        let sent = worker_requests.lock();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].options.get("expression").map(String::as_str), Some("id = 7"));
        assert_eq!(sent[0].options.get("fast_index_lookup").map(String::as_str), Some("true"));
        assert_eq!((sent[0].offset, sent[0].limit), (0, END_OF_SET));
    }

    #[tokio::test]
    async fn a_typed_retriever_decodes_into_its_type() {
        let head = fake_server::start("127.0.0.53").await;
        let worker = fake_server::start("127.0.0.54").await;
        serve_table(&head, &[], false);
        serve_records(&worker, vec![(7, None), (8, Some("eight"))], &[]);
        let k = connect(&head, &[&worker]).await;

        let retriever = RecordRetriever::<Row>::new_typed(k, "t", record_type(), None, None).await.expect("retriever");
        let rows = retriever.get_by_key(&[ShardKeyValue::Int(7)], Some("name is null")).await.expect("records").data;

        assert_eq!(rows, [Row { id: 7, name: None }, Row { id: 8, name: Some("eight".to_string()) }]);
    }

    #[tokio::test]
    async fn a_key_of_the_wrong_type_fails_before_anything_is_sent() {
        let head = fake_server::start("127.0.0.55").await;
        let worker = fake_server::start("127.0.0.56").await;
        serve_table(&head, &[], false);
        let worker_requests = serve_records(&worker, Vec::new(), &[]);
        let k = connect(&head, &[&worker]).await;
        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");

        let err = retriever.get_by_key(&[ShardKeyValue::Long(7)], None).await.unwrap_err();

        assert_eq!(
            err.to_string(),
            "Configuration error: Key value must be i32; found i64"
        );
        assert!(worker_requests.lock().is_empty());
    }

    // The refresh after the server reports rerouted data is part of the
    // attempt, so its failure goes through the recovery and is reported, even
    // though the records arrived.
    #[tokio::test]
    async fn a_failed_refresh_after_rerouted_data_fails_the_retrieval() {
        let head = fake_server::start("127.0.0.57").await;
        let worker = fake_server::start("127.0.0.58").await;
        serve_table(&head, &[], true);
        serve_records(&worker, vec![(7, None)], &[("data_rerouted", "true")]);
        let k = connect(&head, &[&worker]).await;
        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");

        let err = retriever.get_by_key(&[ShardKeyValue::Int(7)], None).await.unwrap_err();

        assert!(err.to_string().contains("shards unavailable"), "got {err}");
    }

    /// A replicated table is read from any live worker when an expression is
    /// given, from the same one for a later page, and from the head node
    /// without one.
    #[tokio::test]
    async fn a_replicated_table_reads_from_one_worker_per_query() {
        let head = fake_server::start("127.0.0.59").await;
        let first = fake_server::start("127.0.0.60").await;
        let second = fake_server::start("127.0.0.61").await;
        let head_requests = serve_table(&head, &["REPLICATED"], false);
        let first_requests = serve_records(&first, vec![(1, None)], &[]);
        let second_requests = serve_records(&second, vec![(1, None)], &[]);
        let k = connect(&head, &[&first, &second]).await;
        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");

        retriever.get_by_key(&[], Some("id > 0")).await.expect("first page");
        retriever.get_by_key_with_offset(&[], Some("id > 0"), 1).await.expect("second page");
        let (first_count, second_count) = (first_requests.lock().len(), second_requests.lock().len());
        assert!(matches!((first_count, second_count), (2, 0) | (0, 2)), "{first_count} and {second_count}");
        {
            let sent = if first_count == 2 { first_requests.lock() } else { second_requests.lock() };
            assert_eq!(sent[0].options.get("fast_index_lookup"), None, "no key, no fast index lookup");
        }

        retriever.get_by_key(&[], None).await.expect("all records");
        assert_eq!(head_requests.lock().len(), 1);
    }

    // A column retrieval types the response by its own schema and passes its
    // info on.
    #[tokio::test]
    async fn a_column_retrieval_is_typed_by_the_response() {
        let head = fake_server::start("127.0.0.62").await;
        serve_table(&head, &[], false);
        let k = connect(&head, &[]).await;
        let schema = r#"{"type":"record","name":"generic_response","fields":[
            {"name":"column_1","type":{"type":"array","items":"int"}},
            {"name":"column_headers","type":{"type":"array","items":"string"}},
            {"name":"column_datatypes","type":{"type":"array","items":"string"}}]}"#;
        #[derive(serde::Serialize)]
        struct Columns {
            column_1: Vec<i32>,
            column_headers: Vec<String>,
            column_datatypes: Vec<String>,
        }
        let data = encode(
            &schema.parse().unwrap(),
            &Columns { column_1: vec![3, 4], column_headers: vec!["id".into()], column_datatypes: vec!["int".into()] },
        );
        let previous = head.behavior.handler.lock().clone().unwrap();
        *head.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            if !path.ends_with("/get/records/bycolumn") {
                return previous(path, body);
            }
            let request: GetRecordsByColumnRequest =
                serde_avro_fast::from_datum_slice(body, &crate::generated::GET_RECORDS_BY_COLUMN_REQUEST_SCHEMA).unwrap();
            assert_eq!(request.column_names, ["id"]);
            assert_eq!(request.options.get("expression").map(String::as_str), Some("id > 2"));
            let response = RawGetRecordsByColumnResponse {
                table_name: "t".to_string(),
                response_schema_str: schema.to_string(),
                binary_encoded_response: ByteBuf::from(data.clone()),
                json_encoded_response: String::new(),
                total_number_of_records: 2,
                has_more_records: true,
                info: HashMap::from([("k".to_string(), "v".to_string())]),
            };
            Some(Ok(encode(&crate::generated::GET_RECORDS_BY_COLUMN_RESPONSE_SCHEMA, &response)))
        }));
        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");

        let result = retriever.get_columns_by_key(vec!["id".to_string()], &[], Some("id > 2")).await.expect("columns");

        assert_eq!(result.data_type.columns().map(Column::name).collect::<Vec<_>>(), ["id"]);
        assert_eq!(result.data.iter().map(|r| r.get(0).cloned()).collect::<Vec<_>>(), [Some(RecordValue::Int(3)), Some(RecordValue::Int(4))]);
        assert!(result.has_more_records);
        assert_eq!(result.info.get("k").map(String::as_str), Some("v"));
    }

    /// Answers `/admin/show/shards` with a new version each time, whose
    /// mapping has as many shards as the version number, so a mapping tells
    /// which fetch it came from.
    fn serve_moving_shards(server: &FakeServer) {
        let version = Arc::new(AtomicI64::new(0));
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, _: &[u8]| {
            if path.ends_with("/admin/show/shards") {
                let version = version.fetch_add(1, Ordering::SeqCst) + 1;
                let response = AdminShowShardsResponse {
                    version,
                    rank: vec![1; version as usize],
                    tom: Vec::new(),
                    info: HashMap::new(),
                };
                Some(Ok(encode(&crate::generated::ADMIN_SHOW_SHARDS_RESPONSE_SCHEMA, &response)))
            } else if path.ends_with("/show/table") {
                Some(Err("Table t does not exist".to_string()))
            } else {
                None
            }
        }));
    }

    // A rebuild fetches the shard mapping that goes with the ranks it
    // moves to, and publishes the two together, rather than pairing the new
    // ranks with the mapping fetched before it.
    #[tokio::test]
    async fn a_rebuild_fetches_the_shard_mapping_of_the_ranks_it_moves_to() {
        let head = fake_server::start("127.0.0.41").await;
        let first = fake_server::start("127.0.0.42").await;
        let second = fake_server::start("127.0.0.43").await;
        serve_moving_shards(&head);
        let k = connect(&head, &[&first]).await;
        let retriever = RecordRetriever::new(k, "t", record_type()).await.expect("retriever");
        assert_eq!(retriever.routing.load().routing_table.as_ref().map(Vec::len), Some(1));

        // The worker moves, and the shard version with it
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[&address(&second)]);
        assert!(retriever.update_worker_queues(true).await.unwrap());

        let routing = retriever.routing.load();
        assert_eq!(routing.worker_urls, [Some(second.url.join("get/records").unwrap())]);
        assert_eq!(
            routing.routing_table.as_ref().map(Vec::len),
            Some(3),
            "the mapping fetched for the new ranks, not the one fetched before the rebuild"
        );
        assert_eq!(retriever.shard_version.load(Ordering::SeqCst), 3);
        assert_eq!(head.behavior.count("/admin/show/shards"), 3);
    }
}

// Routing for clusters that have had a rank removed (KECO-2689).
//
// A rank removed from the middle of the rank list is reported by the server as
// a blank entry in `conf.worker_http_server_urls`. The client keeps an empty
// slot, `None`, for such a rank so that the worker indices produced by the
// server's shard routing table stay aligned with the rank numbering. These
// tests pin down both halves of that contract: that the placeholder survives,
// and that no worker index handed out for use ever names such a slot. Parsing
// the property, and mapping rank numbers to queue indices across a hole, are
// covered beside that code, in gpudb.rs and bulk_inserter_common.rs.
#[cfg(test)]
mod removed_rank_tests {
    use super::*;
    use crate::gpudb::ClusterAddressInfo;
    use std::collections::HashSet;

    fn url(port: u16) -> Url {
        Url::parse(&format!("http://127.0.0.1:{port}")).unwrap()
    }

    // The worker ranks of a four-rank cluster (ranks 0-3) whose rank 2 has
    // been removed, as the client holds them: the head rank is not included,
    // so entry `i` is rank `i + 1`.
    fn workers_with_removed_rank_2() -> WorkerList {
        WorkerList::with_urls(vec![
            Some(url(9192)), // rank 1
            None,            // rank 2, removed
            Some(url(9194)), // rank 3
        ])
    }

    // The indices a retriever picks from for a replicated table, as its
    // routing derives them from the list.
    fn live_worker_indices(workers: &WorkerList) -> Vec<usize> {
        RetrieverRouting::new(true, workers.urls().clone(), None).live_worker_indices
    }

    // Worker rank N lives at index N-1. This is the contract `RecordKey::route`
    // relies on, and the one a compacted list breaks.
    #[test]
    fn worker_indices_stay_aligned_with_rank_numbering() {
        let workers = workers_with_removed_rank_2();

        assert_eq!(workers.len(), 3, "the removed rank must keep its slot");
        assert_eq!(workers.get(0).map(Url::as_str), Some("http://127.0.0.1:9192/"), "rank 1 at index 0");
        assert!(workers.get(1).is_none(), "removed rank 2 must be an empty slot at index 1");
        assert_eq!(
            workers.get(2).map(Url::as_str),
            Some("http://127.0.0.1:9194/"),
            "rank 3 must be reachable at index 2; a compacted list would hand back rank 3's URL for rank 2 \
             and run off the end for rank 3"
        );
    }

    // The server's routing table names live ranks only; rank 2 is gone, so it
    // can only ever hand back 1 or 3. Turning a rank number into an index
    // subtracts 1.
    #[test]
    fn routing_table_entries_name_live_workers() {
        let workers = workers_with_removed_rank_2();
        for rank in [1usize, 3] {
            let worker_index = rank - 1;
            assert!(worker_index < workers.len(), "index for rank {rank} must be in bounds");
            assert!(workers.get(worker_index).is_some(), "rank {rank} must resolve to a live worker");
        }
    }

    // Index 1 holds removed rank 2; a random pick, as made for a replicated
    // table, where every live rank holds the whole table, must never land
    // there.
    #[test]
    fn live_worker_indices_skip_removed_ranks() {
        let workers = workers_with_removed_rank_2();
        let live = live_worker_indices(&workers);
        assert_eq!(live, vec![0, 2], "only the slots holding a live worker may be picked");
        for i in live {
            assert!(workers.get(i).is_some(), "index {i} must hold a live worker");
        }
    }

    #[test]
    fn no_live_workers_yields_no_live_indices() {
        let workers = WorkerList::with_urls(vec![None, None]);
        assert!(
            live_worker_indices(&workers).is_empty(),
            "an all-empty worker list must yield no live indices, so that the caller reports it rather than \
             picking a hole"
        );
    }

    #[test]
    fn no_removed_ranks_yields_no_empty_slots() {
        let workers = WorkerList::with_urls(vec![Some(url(9192)), Some(url(9193)), Some(url(9194))]);
        assert_eq!(workers.len(), 3);
        for i in 0..workers.len() {
            assert!(workers.get(i).is_some(), "index {i} must hold a live worker");
        }
        assert_eq!(live_worker_indices(&workers), vec![0, 1, 2]);
    }

    // `str::split` keeps trailing empty strings, so the slot of a removed
    // *last* rank survives: `"a;b;c;".split(';')` yields four entries. That is
    // benign for routing: every surviving rank keeps its index, and the
    // routing table can never name the departed rank.
    #[test]
    fn removed_last_rank_keeps_its_slot() {
        let property = "http://127.0.0.1:9191;http://127.0.0.1:9192;http://127.0.0.1:9193;";
        let entries: Vec<&str> = property.split(';').collect();
        assert_eq!(entries.len(), 4, "split keeps the trailing empty entry");
        assert_eq!(entries[3], "", "the removed last rank is the empty entry");

        // Held as the client holds them, the head rank dropped: rank 3 removed
        let workers = WorkerList::with_urls(vec![Some(url(9192)), Some(url(9193)), None]);
        assert_eq!(workers.len(), 3);
        assert!(workers.get(2).is_none());
        // The ranks below it are untouched, which is why the difference is benign
        assert_eq!(live_worker_indices(&workers), vec![0, 1]);
    }

    // A removed rank contributes no hostname and must not be counted as a node
    // of the cluster, but it must still occupy its slot in the rank list.
    #[test]
    fn cluster_address_info_skips_removed_ranks() {
        let head = Url::parse("http://head:9191").unwrap();
        let info = ClusterAddressInfo::new(
            head.clone(),
            HashMap::new(),
            vec![Some(url(9192)), None, Some(url(9194))],
            HashSet::new(),
            head,
        );

        assert_eq!(info.worker_rank_urls().len(), 3, "the rank list keeps the removed rank's slot");
        assert_eq!(
            info.live_worker_rank_urls().count(),
            2,
            "only live ranks are visited by callers that do not care about rank numbers"
        );
        assert!(info.does_cluster_contain_node("head"));
        assert!(info.does_cluster_contain_node("127.0.0.1"));
        // Head plus the one distinct worker host; the removed rank added nothing
        assert_eq!(info.host_names().len(), 2);
    }
}
