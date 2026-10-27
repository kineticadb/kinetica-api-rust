//! BulkInserter Full Integration Example
//!
//! This example demonstrates a complete, production-like integration of the
//! BulkInserter with all features working together:
//!
//! 1. **Multiple Data Sources** - Simulates multiple producers (threads/tasks)
//! 2. **Real-Time Monitoring** - Dashboard tracks throughput, queue depth, errors
//! 3. **Backpressure Control** - Memory-bounded operation with semaphore limits
//! 4. **Error Handling** - Comprehensive error tracking and reporting
//! 5. **Graceful Shutdown** - Signal handling with proper flush and cleanup
//! 6. **Batch Callbacks** - Real-time notifications on batch completion
//! 7. **Progress Tracking** - Periodic status updates during ingestion
//!
//! ## Architecture
//!
//! ```
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                        Full Integration Example                          │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
//! │  │ Producer #1  │  │ Producer #2  │  │ Producer #3  │  Data Sources    │
//! │  │ (Sensors)    │  │ (Logs)       │  │ (Metrics)    │                   │
//! │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                   │
//! │         │                 │                 │                            │
//! │         └────────────────┼─────────────────┘                            │
//! │                          │                                               │
//! │                          ▼                                               │
//! │  ┌───────────────────────────────────────────────────────────────────┐  │
//! │  │                     BulkInserter (Arc-shared)                      │  │
//! │  │  ┌─────────────────────────────────────────────────────────────┐  │  │
//! │  │  │  Worker Queues │ Flush Channel │ Semaphore │ Error Queue    │  │  │
//! │  │  └─────────────────────────────────────────────────────────────┘  │  │
//! │  └───────────────────────────────────────────────────────────────────┘  │
//! │         │                          │                                     │
//! │         │ Metrics                  │ Batches                            │
//! │         ▼                          ▼                                     │
//! │  ┌──────────────┐           ┌──────────────┐                            │
//! │  │ Monitoring   │           │  Kinetica    │                            │
//! │  │ Dashboard    │           │  Workers     │                            │
//! │  └──────────────┘           └──────────────┘                            │
//! │                                                                          │
//! │  ┌──────────────┐                                                       │
//! │  │ Signal       │  Ctrl+C → Graceful Shutdown                           │
//! │  │ Handler      │                                                       │
//! │  └──────────────┘                                                       │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Environment Variables
//!
//! **Connection:**
//! - `KINETICA_URL` - Server URL (default: `http://localhost:9191`)
//! - `KINETICA_USER` - Username (default: `admin`)
//! - `KINETICA_PASSWORD` - Password (default: `password`)
//!
//! **Configuration:**
//! - `RECORDS_PER_PRODUCER` - Records each producer generates (default: `1000000`)
//! - `NUM_PRODUCERS` - Number of concurrent producer tasks (default: `3`)
//! - `BATCH_SIZE` - BulkInserter batch size (default: `10000`)
//! - `NUM_WORKERS` - Number of flush workers (default: `8`)
//! - `MAX_IN_FLIGHT` - Max concurrent in-flight batches (default: `50`)
//! - `MONITOR_INTERVAL_MS` - Dashboard update interval in ms (default: `500`)
//!
//! ## Running
//!
//! ```bash
//! # With defaults (3M total records)
//! KINETICA_URL="http://localhost:9191" cargo run --release --bin bulk_inserter_full_integration
//!
//! # With custom configuration (10M total records, smaller batches for more dashboard updates)
//! KINETICA_URL="http://localhost:9191" \
//!   RECORDS_PER_PRODUCER=5000000 \
//!   NUM_PRODUCERS=2 \
//!   BATCH_SIZE=5000 \
//!   MONITOR_INTERVAL_MS=250 \
//!   cargo run --release --bin bulk_inserter_full_integration
//! ```

use gpudb_api::{
    BatchInsertionListener, BatchInsertionResult, BulkInserter, BulkInserterOptions,
    CreateSchemaRequest, CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest,
    FlushOptions, Kinetica, KineticaError, KineticaRecord,
    ShardKeyExtractor, ShardKeyValue, ShardKeyValues, TypePropertiesBuilder,
};
use gpudb_examples::common::connection_config;
use smallvec::smallvec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Configuration for the integration example.
///
/// All parameters can be configured via environment variables:
///
/// - `RECORDS_PER_PRODUCER` - Records each producer generates (default: 1,000,000)
/// - `NUM_PRODUCERS` - Number of concurrent producer tasks (default: 3)
/// - `BATCH_SIZE` - BulkInserter batch size (default: 10,000)
/// - `NUM_WORKERS` - Number of flush workers (default: 8)
/// - `MAX_IN_FLIGHT` - Max concurrent in-flight batches (default: 50)
/// - `MONITOR_INTERVAL_MS` - Dashboard update interval in ms (default: 500)
#[allow(dead_code)]
struct Config {
    /// Total records to generate per producer
    records_per_producer: usize,
    /// Number of producer tasks
    num_producers: usize,
    /// Batch size for BulkInserter
    batch_size: usize,
    /// Number of flush workers
    num_workers: usize,
    /// Max in-flight batches (backpressure)
    max_in_flight: usize,
    /// Monitoring interval
    monitor_interval: Duration,
    /// Progress report interval (reserved for future use)
    progress_interval: Duration,
}

impl Config {
    /// Load configuration from environment variables with defaults.
    fn from_env() -> Self {
        let records_per_producer = std::env::var("RECORDS_PER_PRODUCER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000);

        let num_producers = std::env::var("NUM_PRODUCERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        let batch_size = std::env::var("BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        let num_workers = std::env::var("NUM_WORKERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);

        let max_in_flight = std::env::var("MAX_IN_FLIGHT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);

        let monitor_interval_ms = std::env::var("MONITOR_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);

        Self {
            records_per_producer,
            num_producers,
            batch_size,
            num_workers,
            max_in_flight,
            monitor_interval: Duration::from_millis(monitor_interval_ms),
            progress_interval: Duration::from_secs(5),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::from_env()
    }
}

// ============================================================================
// RECORD TYPES - Multiple data sources with different schemas
// ============================================================================

/// Sensor reading from IoT devices
#[derive(serde::Serialize, Clone, Default)]
struct SensorReading {
    sensor_id: i32,
    location: String,
    temperature: f64,
    humidity: f64,
    timestamp: i64,
}

impl ShardKeyExtractor for SensorReading {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![
            ("sensor_id", ShardKeyValue::Int(self.sensor_id)),
            ("location", ShardKeyValue::String(&self.location)),
        ]
    }
}

const SENSOR_SCHEMA: &str = r#"{
    "type": "record",
    "name": "sensor_reading",
    "fields": [
        {"name": "sensor_id", "type": "int"},
        {"name": "location", "type": "string"},
        {"name": "temperature", "type": "double"},
        {"name": "humidity", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

impl KineticaRecord for SensorReading {
    fn avro_schema(&self) -> &'static str {
        SENSOR_SCHEMA
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .shard_key("sensor_id")
            .shard_key("location")
            .build()
    }
}

// ============================================================================
// BATCH CALLBACK LISTENER
// ============================================================================

/// Tracks batch statistics and timing
struct BatchStatsListener {
    total_batches: AtomicU64,
    total_records: AtomicU64,
    total_encode_ms: AtomicU64,
    total_network_ms: AtomicU64,
    failed_batches: AtomicU64,
}

impl BatchStatsListener {
    fn new() -> Self {
        Self {
            total_batches: AtomicU64::new(0),
            total_records: AtomicU64::new(0),
            total_encode_ms: AtomicU64::new(0),
            total_network_ms: AtomicU64::new(0),
            failed_batches: AtomicU64::new(0),
        }
    }

    fn stats(&self) -> BatchStats {
        let batches = self.total_batches.load(Ordering::Relaxed);
        let records = self.total_records.load(Ordering::Relaxed);
        let encode_ms = self.total_encode_ms.load(Ordering::Relaxed);
        let network_ms = self.total_network_ms.load(Ordering::Relaxed);
        let failed = self.failed_batches.load(Ordering::Relaxed);

        BatchStats {
            total_batches: batches,
            total_records: records,
            avg_encode_ms: if batches > 0 { encode_ms as f64 / batches as f64 } else { 0.0 },
            avg_network_ms: if batches > 0 { network_ms as f64 / batches as f64 } else { 0.0 },
            failed_batches: failed,
        }
    }
}

impl BatchInsertionListener for BatchStatsListener {
    fn on_batch_inserted(&self, result: &BatchInsertionResult) {
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_records.fetch_add(result.batch_size as u64, Ordering::Relaxed);
        self.total_encode_ms.fetch_add(result.encode_time_ms as u64, Ordering::Relaxed);
        self.total_network_ms.fetch_add(result.network_time_ms as u64, Ordering::Relaxed);

        if !result.success {
            self.failed_batches.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct BatchStats {
    total_batches: u64,
    total_records: u64,
    avg_encode_ms: f64,
    avg_network_ms: f64,
    failed_batches: u64,
}

// ============================================================================
// MONITORING - Reusable metrics collection
// ============================================================================

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MonitoringSnapshot {
    timestamp: Instant,
    inserted: i64,
    updated: i64,
    pending_batches: i64,
    utilization_pct: f64,
    error_count: usize,
    insert_rate: f64,
}

impl Default for MonitoringSnapshot {
    fn default() -> Self {
        Self {
            timestamp: Instant::now(),
            inserted: 0,
            updated: 0,
            pending_batches: 0,
            utilization_pct: 0.0,
            error_count: 0,
            insert_rate: 0.0,
        }
    }
}

struct Monitor<T> {
    inserter: Arc<BulkInserter<T>>,
    prev_inserted: AtomicU64,
    prev_timestamp: RwLock<Instant>,
}

impl<T: serde::Serialize + KineticaRecord + ShardKeyExtractor + Send + Sync + 'static + Default>
    Monitor<T>
{
    fn new(inserter: Arc<BulkInserter<T>>) -> Self {
        Self {
            inserter,
            prev_inserted: AtomicU64::new(0),
            prev_timestamp: RwLock::new(Instant::now()),
        }
    }

    async fn snapshot(&self) -> MonitoringSnapshot {
        let now = Instant::now();
        let inserted = self.inserter.count_inserted();
        let updated = self.inserter.count_updated();
        let bp = self.inserter.backpressure_metrics();
        let errors = self.inserter.error_count();

        let prev_timestamp = *self.prev_timestamp.read().await;
        let prev_inserted = self.prev_inserted.load(Ordering::Relaxed) as i64;
        let elapsed = now.duration_since(prev_timestamp).as_secs_f64();

        let insert_rate = if elapsed > 0.0 {
            (inserted - prev_inserted) as f64 / elapsed
        } else {
            0.0
        };

        self.prev_inserted.store(inserted as u64, Ordering::Relaxed);
        *self.prev_timestamp.write().await = now;

        MonitoringSnapshot {
            timestamp: now,
            inserted,
            updated,
            pending_batches: bp.pending_batches,
            utilization_pct: bp.utilization_pct,
            error_count: errors,
            insert_rate,
        }
    }
}

// ============================================================================
// PRODUCER - Generates and inserts data
// ============================================================================

struct ProducerStats {
    records_generated: AtomicU64,
    records_queued: AtomicU64,
    batches_sent: AtomicU64,
}

impl ProducerStats {
    fn new() -> Self {
        Self {
            records_generated: AtomicU64::new(0),
            records_queued: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
        }
    }
}

async fn run_producer(
    producer_id: usize,
    inserter: Arc<BulkInserter<SensorReading>>,
    records_to_generate: usize,
    running: Arc<AtomicBool>,
    stats: Arc<ProducerStats>,
) -> Result<usize, KineticaError> {
    let locations = ["warehouse-a", "warehouse-b", "factory-1", "factory-2",
        "office-hq", "office-branch", "datacenter-1", "datacenter-2"];

    let batch_size = 1000;
    let mut total_sent = 0;
    let mut sensor_id_base = (producer_id * 10000) as i32;

    while total_sent < records_to_generate && running.load(Ordering::SeqCst) {
        // Generate a batch of records
        let records_in_batch = std::cmp::min(batch_size, records_to_generate - total_sent);

        let batch: Vec<SensorReading> = (0..records_in_batch)
            .map(|i| {
                sensor_id_base = sensor_id_base.wrapping_add(1);
                SensorReading {
                    sensor_id: sensor_id_base,
                    location: locations[i % locations.len()].to_string(),
                    temperature: 20.0 + (sensor_id_base as f64 % 15.0),
                    humidity: 40.0 + (sensor_id_base as f64 % 30.0),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                }
            })
            .collect();

        stats.records_generated.fetch_add(records_in_batch as u64, Ordering::Relaxed);

        // Insert batch with backpressure control
        inserter.insert_batch(batch)?;

        stats.records_queued.fetch_add(records_in_batch as u64, Ordering::Relaxed);
        stats.batches_sent.fetch_add(1, Ordering::Relaxed);

        total_sent += records_in_batch;

        // Small yield to allow other tasks to run
        if total_sent % 10000 == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(total_sent)
}

// ============================================================================
// MAIN INTEGRATION
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = Config::default();

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║              BULKINSERTER FULL INTEGRATION EXAMPLE                           ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║                                                                              ║");
    println!("║  This example demonstrates:                                                  ║");
    println!("║    • Multiple concurrent producers inserting data                            ║");
    println!("║    • Real-time monitoring of throughput and queue depth                      ║");
    println!("║    • Backpressure control to bound memory usage                              ║");
    println!("║    • Batch callbacks for timing statistics                                   ║");
    println!("║    • Graceful shutdown with Ctrl+C                                           ║");
    println!("║                                                                              ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!();

    // ========================================================================
    // SETUP: Connect to Kinetica and create table
    // ========================================================================

    // Load connection config from environment variables:
    //   KINETICA_URL      - Server URL (default: http://localhost:9191)
    //   KINETICA_USER     - Username (default: admin)
    //   KINETICA_PASSWORD - Password (default: password)
    let (urls, options) = connection_config();
    let url = &urls[0];

    println!("Configuration:");
    println!("  Kinetica URL: {url}");
    println!("  Producers: {}", config.num_producers);
    println!("  Records per producer: {}", config.records_per_producer);
    println!("  Total records: {}", config.num_producers * config.records_per_producer);
    println!("  Batch size: {}", config.batch_size);
    println!("  Flush workers: {}", config.num_workers);
    println!("  Max in-flight batches: {}", config.max_in_flight);
    println!();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    let schema_name = "test_schema";
    let table_name = format!("{schema_name}.integration_test");

    // Create schema
    let _ = kinetica
        .create_schema_raw(&CreateSchemaRequest {
            schema_name: schema_name.to_string(),
            options: HashMap::new(),
        })
        .await;

    // Drop existing table
    let _ = kinetica
        .execute_sql_raw(&ExecuteSqlRequest {
            statement: format!("DROP TABLE IF EXISTS {table_name}"),
            ..Default::default()
        })
        .await;

    // Create type
    let type_resp = kinetica
        .create_type_raw(&CreateTypeRequest {
            type_definition: SensorReading::default().avro_schema().to_string(),
            label: "integration_test_type".to_string(),
            properties: SensorReading::type_properties(),
            options: HashMap::new(),
        })
        .await?;

    // Create table
    kinetica
        .create_table_raw(&CreateTableRequest {
            table_name: table_name.clone(),
            type_id: type_resp.type_id,
            options: HashMap::new(),
        })
        .await?;

    println!("✓ Table created: {table_name}");

    // ========================================================================
    // CREATE BULKINSERTER with all features enabled
    // ========================================================================

    // Create batch callback listener
    let batch_listener = Arc::new(BatchStatsListener::new());

    // Configure BulkInserter
    let mut bulk_options = BulkInserterOptions::new();
    bulk_options
        .set_max_in_flight_batches(config.max_in_flight)
        .set_max_retries(3)
        .set_return_individual_errors(true)
        .set_allow_partial_batch(true)
        .set_flush_options(FlushOptions::new(false, 30))
        .set_batch_listener(batch_listener.clone());

    let inserter: Arc<BulkInserter<SensorReading>> = BulkInserter::with_options(
        kinetica.clone(),
        table_name.clone(),
        config.batch_size,
        config.num_workers,
        Some(bulk_options),
    )
    .await?;

    println!("✓ BulkInserter created");
    println!("  Workers: {}", inserter.worker_count());
    println!("  Multi-head: {}", if inserter.is_multi_head_enabled() { "enabled" } else { "disabled" });
    println!("  Shard columns: {}", inserter.shard_column_count());
    println!();

    // ========================================================================
    // SETUP: Shutdown signal handling
    // ========================================================================

    let running = Arc::new(AtomicBool::new(true));
    let running_signal = running.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\n\n⚠ Shutdown signal received (Ctrl+C)");
        running_signal.store(false, Ordering::SeqCst);
    });

    // ========================================================================
    // START: Monitoring task
    // ========================================================================

    let monitor = Arc::new(Monitor::new(inserter.clone()));
    let monitor_clone = monitor.clone();
    let running_monitor = running.clone();
    let monitor_interval = config.monitor_interval;

    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(monitor_interval);
        let start = Instant::now();

        while running_monitor.load(Ordering::SeqCst) {
            interval.tick().await;
            let snapshot = monitor_clone.snapshot().await;

            // Simple inline monitoring output
            let elapsed = start.elapsed().as_secs_f64();
            print!("\r[{:>6.1}s] Inserted: {:>10} | Rate: {:>10.0}/s | Queue: {:>3} ({:>5.1}%) | Errors: {}   ",
                elapsed,
                format_number(snapshot.inserted),
                snapshot.insert_rate,
                snapshot.pending_batches,
                snapshot.utilization_pct,
                snapshot.error_count
            );
            use std::io::Write;
            std::io::stdout().flush().ok();
        }
    });

    // ========================================================================
    // START: Producer tasks
    // ========================================================================

    println!("Starting {} producers...", config.num_producers);
    println!();

    let producer_stats: Vec<Arc<ProducerStats>> = (0..config.num_producers)
        .map(|_| Arc::new(ProducerStats::new()))
        .collect();

    let start_time = Instant::now();

    let mut producer_handles = Vec::new();
    for (i, producer_stat) in producer_stats.iter().enumerate().take(config.num_producers) {
        let inserter_clone = inserter.clone();
        let running_clone = running.clone();
        let stats_clone = producer_stat.clone();
        let records = config.records_per_producer;

        let handle = tokio::spawn(async move {
            run_producer(i, inserter_clone, records, running_clone, stats_clone).await
        });
        producer_handles.push(handle);
    }

    // ========================================================================
    // WAIT: For producers to complete
    // ========================================================================

    let mut total_produced = 0usize;
    for (i, handle) in producer_handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(count)) => {
                total_produced += count;
            }
            Ok(Err(e)) => {
                eprintln!("\nProducer {i} error: {e}");
            }
            Err(e) => {
                eprintln!("\nProducer {i} panicked: {e}");
            }
        }
    }

    let produce_duration = start_time.elapsed();

    // ========================================================================
    // FLUSH: Wait for all batches to complete
    // ========================================================================

    println!("\n\nProducers complete. Flushing remaining batches...");

    // Stop monitoring before final stats
    running.store(false, Ordering::SeqCst);
    monitor_handle.abort();

    // Flush all remaining records and wait for completion
    // Use close() which properly waits for all pending batches
    let flush_start = Instant::now();

    // Show current state before flush
    let buffered_before = inserter.buffered_record_count();
    let pending_before = inserter.pending_batches();
    let in_flight_estimate = inserter.in_flight_record_estimate();

    println!("Pre-flush state:");
    println!("  Buffered records (not yet sent): {buffered_before}");
    println!("  Pending batches: {pending_before}");
    println!("  Estimated in-flight records: {in_flight_estimate}");

    // close() waits indefinitely until ALL records are flushed (success or failure)
    // It will NOT return until every record is accounted for
    inserter.close().await?;
    println!("All batches completed.");

    let flush_duration = flush_start.elapsed();
    let total_duration = start_time.elapsed();

    // ========================================================================
    // RESULTS: Final statistics
    // ========================================================================

    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              FINAL RESULTS                                   ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    // Record counts - get failed count BEFORE draining errors
    let failed_record_count = inserter.failed_record_count();
    let inserted = inserter.count_inserted();
    let updated = inserter.count_updated();
    let total_db = inserted + updated;
    let total_accounted = total_db + failed_record_count as i64;

    println!("║  RECORDS                                                                     ║");
    println!("║    Generated:     {:>12}                                               ║", format_number(total_produced as i64));
    println!("║    Inserted:      {:>12}                                               ║", format_number(inserted));
    println!("║    Updated:       {:>12}                                               ║", format_number(updated));
    println!("║    Failed:        {:>12}                                               ║", format_number(failed_record_count as i64));
    println!("║    Total in DB:   {:>12}                                               ║", format_number(total_db));
    println!("║    Accounted:     {:>12}  (inserted + updated + failed)                ║", format_number(total_accounted));

    // Timing
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  TIMING                                                                      ║");
    println!("║    Produce time:  {:>10.2}s                                                ║", produce_duration.as_secs_f64());
    println!("║    Flush time:    {:>10.2}s                                                ║", flush_duration.as_secs_f64());
    println!("║    Total time:    {:>10.2}s                                                ║", total_duration.as_secs_f64());

    // Throughput
    let throughput = if total_duration.as_secs_f64() > 0.0 {
        total_db as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  THROUGHPUT                                                                  ║");
    println!("║    Overall:       {throughput:>10.0} rec/s                                          ║");

    // Batch statistics
    let batch_stats = batch_listener.stats();
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  BATCH STATISTICS                                                            ║");
    println!("║    Total batches: {:>12}                                               ║", batch_stats.total_batches);
    println!("║    Failed batches:{:>12}                                               ║", batch_stats.failed_batches);
    println!("║    Avg encode:    {:>10.1}ms                                               ║", batch_stats.avg_encode_ms);
    println!("║    Avg network:   {:>10.1}ms                                               ║", batch_stats.avg_network_ms);

    // Errors
    let errors = inserter.drain_errors();
    let warnings = inserter.drain_warnings();

    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  ERRORS & WARNINGS                                                           ║");
    println!("║    Errors:        {:>12}                                               ║", errors.len());
    println!("║    Warnings:      {:>12}                                               ║", warnings.len());

    if !errors.is_empty() {
        println!("║                                                                              ║");
        println!("║    Recent errors:                                                            ║");
        for (i, err) in errors.iter().take(5).enumerate() {
            let msg = if err.message.len() > 40 {
                format!("{}...", &err.message[..37])
            } else {
                err.message.clone()
            };
            println!("║      {}. {} ({} records)                       ║", i + 1, msg, err.record_count);
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    // ========================================================================
    // DATA INTEGRITY CHECK
    // ========================================================================

    // Records are "lost" if we can't account for them (not inserted, updated, OR failed)
    let records_unaccounted = total_produced as i64 - total_accounted;
    let records_failed = failed_record_count as i64;
    let has_failures = records_failed > 0 || batch_stats.failed_batches > 0;

    if records_unaccounted > 0 {
        // This should NEVER happen - indicates a bug in the BulkInserter
        println!();
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║  🚨 CRITICAL: UNACCOUNTED RECORDS                                             ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║    Unaccounted:   {:>12} ({:.2}% of generated)                       ║",
            format_number(records_unaccounted),
            (records_unaccounted as f64 / total_produced as f64) * 100.0);
        println!("║                                                                              ║");
        println!("║  This indicates a BUG - records were neither inserted nor tracked as failed! ║");
        println!("║  Please report this issue with logs and reproduction steps.                  ║");
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    } else if has_failures {
        // Records failed but are accounted for - user can decide what to do
        println!();
        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║  ⚠️  SOME RECORDS FAILED TO INSERT                                            ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║    Failed records:{:>12} ({:.2}% of generated)                       ║",
            format_number(records_failed),
            (records_failed as f64 / total_produced as f64) * 100.0);
        println!("║    Failed batches:{:>12}                                               ║", batch_stats.failed_batches);
        println!("║                                                                              ║");
        println!("║  All records are accounted for (inserted + failed = generated).              ║");
        println!("║  To retry failed records, check the error queue and resubmit.                ║");
        println!("║                                                                              ║");
        println!("║  Common causes:                                                              ║");
        println!("║    1. Network timeouts (increase max_retries)                                ║");
        println!("║    2. Schema mismatch (check column types)                                   ║");
        println!("║    3. Server overload (reduce batch_size or num_workers)                     ║");
        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    } else {
        println!();
        println!("✅ DATA INTEGRITY CHECK PASSED: All {} records inserted successfully.", format_number(total_produced as i64));
    }

    // ========================================================================
    // CLEANUP
    // ========================================================================

    println!("\nCleaning up...");

    // Note: inserter.close() was already called above during flush

    // Drop table
    let _ = kinetica
        .execute_sql_raw(&ExecuteSqlRequest {
            statement: format!("DROP TABLE IF EXISTS {table_name}"),
            ..Default::default()
        })
        .await;

    println!("✓ Cleanup complete");

    // Return error if records were unaccounted for (non-zero exit code)
    if records_unaccounted > 0 {
        println!();
        eprintln!("❌ CRITICAL FAILURE: {records_unaccounted} records unaccounted for!");
        std::process::exit(2);
    }

    // Return error if records failed (non-zero exit code, different from unaccounted)
    if records_failed > 0 {
        println!();
        eprintln!("❌ FAILED: {records_failed} records failed to insert (but are tracked in error queue)");
        std::process::exit(1);
    }

    println!();
    println!("Integration test finished successfully!");

    Ok(())
}

fn format_number(n: i64) -> String {
    if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        format!("{n}")
    }
}
