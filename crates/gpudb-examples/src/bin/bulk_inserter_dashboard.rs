//! BulkInserter Monitoring Dashboard
//!
//! This example demonstrates how to build a real-time monitoring dashboard
//! for an **existing** BulkInserter instance. The dashboard components
//! (`MetricsCollector` and `ConsoleDashboard`) are designed to be reusable
//! and can be integrated into any application using BulkInserter.
//!
//! ## How to Connect to an Existing BulkInserter
//!
//! `BulkInserter::with_options()` returns `Arc<BulkInserter<T>>` directly,
//! allowing it to be shared between:
//! 1. Your producer code (calling insert/insert_batch)
//! 2. The monitoring dashboard (reading metrics)
//!
//! ```rust
//! // Create your BulkInserter - already wrapped in Arc
//! let inserter = BulkInserter::with_options(kinetica, table, 50000, 16, None).await?;
//!
//! // Share with dashboard - just clone the Arc (cheap reference count increment)
//! let collector = MetricsCollector::new(inserter.clone());
//!
//! // Your producer code uses the same inserter
//! inserter.insert_batch(records)?;
//!
//! // Dashboard reads metrics without interfering with inserts
//! let metrics = collector.collect().await;
//! ```
//!
//! Run this example with:
//! ```
//! KINETICA_URL="http://localhost:9191" cargo run --release --bin bulk_inserter_dashboard
//! ```

use gpudb_api::{
    BulkInserter, BulkInserterOptions,
    CreateSchemaRequest, CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest,
    FlushOptions, Kinetica,
    KineticaOptions, KineticaRecord, ShardKeyExtractor, ShardKeyValue, ShardKeyValues,
    TypePropertiesBuilder,
};
use smallvec::smallvec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ============================================================================
// REUSABLE DASHBOARD COMPONENTS
// ============================================================================
// These components can be copied into your own application to monitor
// any BulkInserter instance.
// ============================================================================

/// Comprehensive metrics collected from a BulkInserter.
///
/// This struct contains all the metrics that can be displayed in a dashboard.
/// It's designed to be a snapshot that can be serialized, logged, or displayed.
#[derive(Debug, Clone, Default)]
pub struct DashboardMetrics {
    // Timestamp
    pub timestamp: u64,
    pub uptime_secs: f64,

    // Record counts
    pub total_inserted: i64,
    pub total_updated: i64,
    pub total_processed: i64,

    // Rates (per second)
    pub insert_rate: f64,
    pub update_rate: f64,
    pub throughput_rate: f64,

    // Queue metrics
    pub pending_batches: i64,
    pub in_flight_batches: usize,
    pub available_permits: usize,
    pub max_in_flight: usize,
    pub utilization_pct: f64,

    // Error metrics
    pub error_count: usize,
    pub warning_count: usize,
    pub error_rate: f64,
    pub recent_errors: Vec<ErrorSummary>,

    // Configuration
    pub worker_count: usize,
    pub batch_size: usize,
    pub multi_head_enabled: bool,
    pub shard_columns: usize,

    // Derived metrics
    pub avg_batch_time_ms: f64,
    pub estimated_queue_drain_secs: f64,
}

#[derive(Debug, Clone)]
pub struct ErrorSummary {
    pub worker_url: String,
    pub message: String,
    pub record_count: usize,
    pub age_secs: f64,
}

// ============================================================================
// MetricsCollector - Connects to an existing BulkInserter
// ============================================================================

/// Collects and aggregates metrics from an existing BulkInserter.
///
/// ## Connection to Existing BulkInserter
///
/// The `MetricsCollector` takes an `Arc<BulkInserter<T>>` reference, which means:
/// - It does NOT own the BulkInserter
/// - It does NOT interfere with insert operations
/// - Multiple collectors can monitor the same inserter
/// - The inserter can be used normally while being monitored
///
/// ## Example: Integrating with Your Application
///
/// ```rust
/// // In your application code:
/// async fn run_ingestion_with_monitoring() {
///     // 1. Create your BulkInserter (already returns Arc<BulkInserter<T>>)
///     let inserter = BulkInserter::with_options(
///         kinetica, table_name, 50000, 16, None
///     ).await?;
///
///     // 2. Create a MetricsCollector that REFERENCES the same inserter
///     //    inserter.clone() creates another Arc reference, not a copy
///     let collector = MetricsCollector::new(inserter.clone());
///
///     // 3. Spawn a monitoring task
///     let collector_handle = tokio::spawn(async move {
///         loop {
///             let metrics = collector.collect().await;
///             println!("Throughput: {:.0} rec/s", metrics.throughput_rate);
///             tokio::time::sleep(Duration::from_secs(1)).await;
///         }
///     });
///
///     // 4. Your producer code uses the inserter normally
///     for batch in data_source {
///         inserter.insert_batch(batch)?;
///     }
///
///     // 5. Cleanup - close() waits until ALL records are flushed
///     inserter.close().await?;
///     collector_handle.abort();
/// }
/// ```
pub struct MetricsCollector<T> {
    /// Reference to the BulkInserter being monitored.
    /// This is an Arc clone - the collector doesn't own the inserter.
    inserter: Arc<BulkInserter<T>>,

    /// When the collector was created (for uptime calculation)
    start_time: Instant,

    // Previous values for rate calculation
    prev_inserted: AtomicU64,
    prev_updated: AtomicU64,
    prev_errors: AtomicU64,
    prev_timestamp: RwLock<Instant>,

    // Historical data for averaging
    rate_history: RwLock<Vec<f64>>,
    max_history_size: usize,
}

impl<T: serde::Serialize + KineticaRecord + ShardKeyExtractor + Send + Sync + 'static + Default>
    MetricsCollector<T>
{
    /// Create a new MetricsCollector that monitors an existing BulkInserter.
    ///
    /// # Arguments
    ///
    /// * `inserter` - An Arc reference to the BulkInserter to monitor.
    ///   This is typically created by cloning your existing Arc:
    ///   `MetricsCollector::new(my_inserter.clone())`
    ///
    /// # Note
    ///
    /// The collector does NOT take ownership of the inserter. Your application
    /// continues to use the inserter for insert operations while the collector
    /// reads metrics in the background.
    pub fn new(inserter: Arc<BulkInserter<T>>) -> Self {
        Self {
            inserter,
            start_time: Instant::now(),
            prev_inserted: AtomicU64::new(0),
            prev_updated: AtomicU64::new(0),
            prev_errors: AtomicU64::new(0),
            prev_timestamp: RwLock::new(Instant::now()),
            rate_history: RwLock::new(Vec::new()),
            max_history_size: 60, // Keep 60 samples for averaging
        }
    }

    /// Collect current metrics snapshot from the BulkInserter.
    ///
    /// This method reads all available metrics from the inserter and calculates
    /// derived values like rates and utilization. It's safe to call this while
    /// the inserter is actively processing records.
    ///
    /// # Thread Safety
    ///
    /// All BulkInserter metric methods are thread-safe and use atomic operations,
    /// so calling `collect()` does not interfere with insert operations.
    pub async fn collect(&self) -> DashboardMetrics {
        let now = Instant::now();
        let uptime = now.duration_since(self.start_time);

        // ----------------------------------------------------------------
        // Read metrics from the BulkInserter
        // These methods are all thread-safe and non-blocking
        // ----------------------------------------------------------------

        // Record counts (atomic reads)
        let total_inserted = self.inserter.count_inserted();
        let total_updated = self.inserter.count_updated();
        let total_processed = total_inserted + total_updated;

        // Backpressure metrics (atomic reads)
        let bp_metrics = self.inserter.backpressure_metrics();

        // Error counts (atomic reads)
        let error_count = self.inserter.error_count();
        let warning_count = self.inserter.warning_count();

        // Configuration (immutable after construction)
        let worker_count = self.inserter.worker_count();
        let batch_size = self.inserter.batch_size();
        let multi_head_enabled = self.inserter.is_multi_head_enabled();
        let shard_columns = self.inserter.shard_column_count();

        // ----------------------------------------------------------------
        // Calculate rates based on delta since last collection
        // ----------------------------------------------------------------

        let prev_timestamp = *self.prev_timestamp.read().await;
        let elapsed = now.duration_since(prev_timestamp).as_secs_f64();

        let prev_inserted = self.prev_inserted.load(Ordering::Relaxed) as i64;
        let prev_updated = self.prev_updated.load(Ordering::Relaxed) as i64;
        let prev_errors = self.prev_errors.load(Ordering::Relaxed) as usize;

        let insert_rate = if elapsed > 0.0 {
            (total_inserted - prev_inserted) as f64 / elapsed
        } else {
            0.0
        };

        let update_rate = if elapsed > 0.0 {
            (total_updated - prev_updated) as f64 / elapsed
        } else {
            0.0
        };

        let throughput_rate = insert_rate + update_rate;

        let error_rate = if elapsed > 0.0 {
            (error_count.saturating_sub(prev_errors)) as f64 / elapsed
        } else {
            0.0
        };

        // Update rate history for averaging
        {
            let mut history = self.rate_history.write().await;
            history.push(throughput_rate);
            if history.len() > self.max_history_size {
                history.remove(0);
            }
        }

        // Store current values for next rate calculation
        self.prev_inserted.store(total_inserted as u64, Ordering::Relaxed);
        self.prev_updated.store(total_updated as u64, Ordering::Relaxed);
        self.prev_errors.store(error_count as u64, Ordering::Relaxed);
        *self.prev_timestamp.write().await = now;

        // ----------------------------------------------------------------
        // Calculate derived metrics
        // ----------------------------------------------------------------

        let avg_batch_time_ms = if throughput_rate > 0.0 {
            (batch_size as f64 / throughput_rate) * 1000.0
        } else {
            0.0
        };

        let estimated_queue_drain_secs = if throughput_rate > 0.0 {
            let pending = bp_metrics.pending_batches as f64;
            (pending * batch_size as f64) / throughput_rate
        } else {
            f64::INFINITY
        };

        DashboardMetrics {
            timestamp: now.duration_since(self.start_time).as_millis() as u64,
            uptime_secs: uptime.as_secs_f64(),

            total_inserted,
            total_updated,
            total_processed,

            insert_rate,
            update_rate,
            throughput_rate,

            pending_batches: bp_metrics.pending_batches,
            in_flight_batches: bp_metrics.in_flight_batches,
            available_permits: bp_metrics.available_permits,
            max_in_flight: bp_metrics.max_in_flight_batches,
            utilization_pct: bp_metrics.utilization_pct,

            error_count,
            warning_count,
            error_rate,
            recent_errors: Vec::new(), // Populated separately via drain_error_summaries()

            worker_count,
            batch_size,
            multi_head_enabled,
            shard_columns,

            avg_batch_time_ms,
            estimated_queue_drain_secs,
        }
    }

    /// Get average throughput over the history window.
    pub async fn avg_throughput(&self) -> f64 {
        let history = self.rate_history.read().await;
        if history.is_empty() {
            return 0.0;
        }
        history.iter().sum::<f64>() / history.len() as f64
    }

    /// Drain errors from the inserter and convert to summaries.
    ///
    /// Note: This REMOVES errors from the inserter's queue, so call this
    /// only when you want to process/display them.
    pub fn drain_error_summaries(&self) -> Vec<ErrorSummary> {
        let errors = self.inserter.drain_errors();
        let now = Instant::now();

        errors
            .into_iter()
            .map(|e| ErrorSummary {
                worker_url: e.worker_url,
                message: e.message,
                record_count: e.record_count,
                age_secs: now.duration_since(e.timestamp).as_secs_f64(),
            })
            .collect()
    }
}

// ============================================================================
// ConsoleDashboard - Renders metrics to terminal
// ============================================================================

/// Renders DashboardMetrics to the console with a visual layout.
///
/// This is a simple console-based dashboard. You could replace this with:
/// - A web-based dashboard (expose metrics via HTTP/WebSocket)
/// - Prometheus metrics exporter
/// - Logging to a monitoring system
/// - Custom visualization
pub struct ConsoleDashboard {
    #[allow(dead_code)]
    refresh_interval: Duration,
    show_errors: bool,
}

impl ConsoleDashboard {
    pub fn new(refresh_interval: Duration) -> Self {
        Self {
            refresh_interval,
            show_errors: true,
        }
    }

    /// Clear screen and move cursor to top
    fn clear_screen(&self) {
        print!("\x1B[2J\x1B[H");
    }

    /// Render the dashboard with current metrics
    pub fn render(&self, metrics: &DashboardMetrics) {
        self.clear_screen();

        println!("╔══════════════════════════════════════════════════════════════════════════════╗");
        println!("║                    BULKINSERTER MONITORING DASHBOARD                         ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");

        // Uptime and timestamp
        println!(
            "║  Uptime: {:>10.1}s                                   {:>19} ║",
            metrics.uptime_secs,
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║  THROUGHPUT                                                                  ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");

        // Record counts
        println!(
            "║  Records Inserted: {:>15}    Records Updated: {:>15}      ║",
            format_number(metrics.total_inserted),
            format_number(metrics.total_updated)
        );

        println!(
            "║  Total Processed:  {:>15}                                            ║",
            format_number(metrics.total_processed)
        );

        println!("║                                                                              ║");

        // Rates
        let rate_bar = create_rate_bar(metrics.throughput_rate, 500_000.0, 30);
        println!(
            "║  Insert Rate:  {:>12.0} rec/s                                          ║",
            metrics.insert_rate
        );
        println!(
            "║  Update Rate:  {:>12.0} rec/s                                          ║",
            metrics.update_rate
        );
        println!(
            "║  Total Rate:   {:>12.0} rec/s  [{}]       ║",
            metrics.throughput_rate, rate_bar
        );

        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║  QUEUE & BACKPRESSURE                                                        ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");

        // Queue status
        println!(
            "║  Pending Batches:  {:>6}    In-Flight: {:>6}    Available: {:>6}          ║",
            metrics.pending_batches, metrics.in_flight_batches, metrics.available_permits
        );

        // Utilization bar
        let util_bar = create_utilization_bar(metrics.utilization_pct, 40);
        let util_status = if metrics.utilization_pct > 90.0 {
            "BACKPRESSURE"
        } else if metrics.utilization_pct > 70.0 {
            "HIGH"
        } else if metrics.utilization_pct > 30.0 {
            "NORMAL"
        } else {
            "LOW"
        };

        println!(
            "║  Utilization: {:>5.1}%  [{}]  {:>12} ║",
            metrics.utilization_pct, util_bar, util_status
        );

        if metrics.estimated_queue_drain_secs.is_finite() {
            println!(
                "║  Est. Queue Drain: {:>6.1}s                                                  ║",
                metrics.estimated_queue_drain_secs
            );
        } else {
            println!(
                "║  Est. Queue Drain:    N/A (no throughput)                                    ║"
            );
        }

        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║  ERRORS & WARNINGS                                                           ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");

        let error_status = if metrics.error_count > 0 { "⚠" } else { "✓" };
        println!(
            "║  Errors: {:>6} {}    Warnings: {:>6}    Error Rate: {:>8.2}/s              ║",
            metrics.error_count, error_status, metrics.warning_count, metrics.error_rate
        );

        // Show recent errors if any
        if self.show_errors && !metrics.recent_errors.is_empty() {
            println!("║                                                                              ║");
            println!("║  Recent Errors:                                                              ║");
            for (i, err) in metrics.recent_errors.iter().take(3).enumerate() {
                let msg = if err.message.len() > 50 {
                    format!("{}...", &err.message[..47])
                } else {
                    err.message.clone()
                };
                println!("║    {}. {} ({} recs)                              ║", i + 1, msg, err.record_count);
            }
        }

        println!("╠══════════════════════════════════════════════════════════════════════════════╣");
        println!("║  CONFIGURATION                                                               ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════╣");

        let multi_head = if metrics.multi_head_enabled {
            "Enabled"
        } else {
            "Disabled"
        };
        println!(
            "║  Workers: {:>3}    Batch Size: {:>6}    Multi-Head: {:>8}    Shard Cols: {} ║",
            metrics.worker_count, metrics.batch_size, multi_head, metrics.shard_columns
        );
        println!(
            "║  Max In-Flight: {:>3}    Avg Batch Time: {:>8.1}ms                            ║",
            metrics.max_in_flight, metrics.avg_batch_time_ms
        );

        println!("╚══════════════════════════════════════════════════════════════════════════════╝");
        println!();
        println!("Press Ctrl+C to stop");
    }
}

fn format_number(n: i64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        format!("{n}")
    }
}

fn create_rate_bar(rate: f64, max_rate: f64, width: usize) -> String {
    let filled = ((rate / max_rate) * width as f64).min(width as f64) as usize;
    let empty = width - filled;
    format!("{}{}",
        "█".repeat(filled),
        "░".repeat(empty)
    )
}

fn create_utilization_bar(pct: f64, width: usize) -> String {
    let filled = ((pct / 100.0) * width as f64).min(width as f64) as usize;
    let empty = width - filled;

    let bar_char = if pct > 90.0 {
        "▓"  // Critical
    } else if pct > 70.0 {
        "▒"  // Warning
    } else {
        "█"  // Normal
    };

    format!("{}{}", bar_char.repeat(filled), "░".repeat(empty))
}

// ============================================================================
// EXAMPLE: Test Record Type
// ============================================================================

#[derive(serde::Serialize, Clone, Default)]
struct DashboardTestRecord {
    id: i32,
    category: String,
    value: f64,
    timestamp: i64,
}

impl ShardKeyExtractor for DashboardTestRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![("id", ShardKeyValue::Int(self.id))]
    }
}

const DASHBOARD_TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "dashboard_test_record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "category", "type": "string"},
        {"name": "value", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

impl KineticaRecord for DashboardTestRecord {
    fn avro_schema(&self) -> &'static str {
        DASHBOARD_TEST_SCHEMA
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .shard_key("id")
            .build()
    }
}

// ============================================================================
// MAIN: Demonstrates connecting dashboard to an existing BulkInserter
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("=============================================================");
    println!("  BulkInserter Monitoring Dashboard Example");
    println!("=============================================================");
    println!();
    println!("This example demonstrates how to:");
    println!("  1. Create a BulkInserter (wrapped in Arc for sharing)");
    println!("  2. Create a MetricsCollector that REFERENCES the inserter");
    println!("  3. Run monitoring in parallel with data ingestion");
    println!();

    // Get Kinetica URL
    let url = std::env::var("KINETICA_URL").unwrap_or_else(|_| "http://localhost:9191".to_string());
    println!("Connecting to Kinetica at: {url}");

    // Create client
    let options = KineticaOptions::default();
    let kinetica = Arc::new(Kinetica::new(vec![url], options).await?);

    // Setup table
    let schema_name = "test_schema";
    let table_name = format!("{schema_name}.dashboard_test");

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
            type_definition: DashboardTestRecord::default().avro_schema().to_string(),
            label: "dashboard_test_type".to_string(),
            properties: DashboardTestRecord::type_properties(),
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

    println!("Created table: {table_name}");

    // ========================================================================
    // STEP 1: Create BulkInserter (already wrapped in Arc)
    // ========================================================================
    // BulkInserter::with_options() returns Arc<BulkInserter<T>> directly.
    // The Arc wrapper allows the inserter to be shared between:
    // - The producer task (inserting records)
    // - The monitoring dashboard (reading metrics)
    // ========================================================================

    let mut bulk_options = BulkInserterOptions::new();
    bulk_options
        .set_max_in_flight_batches(50)
        .set_flush_options(FlushOptions::new(false, 10));

    // Note: BulkInserter::with_options already returns Arc<BulkInserter<T>>
    let inserter: Arc<BulkInserter<DashboardTestRecord>> = BulkInserter::with_options(
        kinetica.clone(),
        table_name.clone(),
        10_000, // batch size
        8,      // workers
        Some(bulk_options),
    )
    .await?;

    println!("BulkInserter created and wrapped in Arc");
    println!();

    // ========================================================================
    // STEP 2: Create MetricsCollector with a CLONE of the Arc
    // ========================================================================
    // This is the key connection: collector.new() takes Arc<BulkInserter<T>>
    // By calling inserter.clone(), we create a new Arc pointer to the SAME
    // underlying BulkInserter. The collector doesn't own it - it references it.
    // ========================================================================

    let collector = Arc::new(MetricsCollector::new(inserter.clone()));
    // ↑ inserter.clone() creates a new Arc reference, NOT a copy of the inserter

    println!("MetricsCollector created, connected to the same BulkInserter");
    println!();

    // Create console dashboard
    let dashboard = ConsoleDashboard::new(Duration::from_secs(1));

    // Flag for graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        running_clone.store(false, Ordering::SeqCst);
    });

    // ========================================================================
    // STEP 3: Start producer task (simulates your application's data source)
    // ========================================================================
    // This task uses the SAME inserter (via Arc clone) to insert records.
    // The monitoring dashboard reads metrics while inserts are happening.
    // ========================================================================

    let inserter_producer = inserter.clone();  // Another Arc clone for the producer
    let running_producer = running.clone();
    let producer_handle = tokio::spawn(async move {
        let mut id = 0i32;
        let categories = ["A", "B", "C", "D", "E"];

        println!("Producer started - inserting records...");

        while running_producer.load(Ordering::SeqCst) {
            // Generate batch of records
            let batch: Vec<DashboardTestRecord> = (0..1000)
                .map(|i| {
                    id = id.wrapping_add(1);
                    DashboardTestRecord {
                        id,
                        category: categories[i % categories.len()].to_string(),
                        value: (id as f64) * 0.01,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                    }
                })
                .collect();

            // Insert using the shared inserter
            if let Err(e) = inserter_producer.insert_batch(batch) {
                eprintln!("Insert error: {e}");
            }

            // Small delay to control rate
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // ========================================================================
    // STEP 4: Dashboard update loop (reads metrics while inserts happen)
    // ========================================================================
    // The collector reads metrics from the inserter without blocking inserts.
    // All metric reads are atomic and thread-safe.
    // ========================================================================

    println!("Starting dashboard - monitoring the active BulkInserter...\n");
    tokio::time::sleep(Duration::from_secs(1)).await;

    while running.load(Ordering::SeqCst) {
        // Collect metrics from the inserter (non-blocking, atomic reads)
        let mut metrics = collector.collect().await;

        // Optionally drain errors (this removes them from the inserter's queue)
        metrics.recent_errors = collector.drain_error_summaries();

        // Render to console
        dashboard.render(&metrics);

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ========================================================================
    // STEP 5: Graceful shutdown
    // ========================================================================

    println!("\nShutting down...");

    // Stop producer
    producer_handle.abort();

    // Flush and close inserter - waits until ALL records are flushed
    inserter.close().await?;

    println!("Final stats:");
    println!("  Inserted: {}", inserter.count_inserted());
    println!("  Updated: {}", inserter.count_updated());
    println!("  Errors: {}", inserter.error_count());

    // Cleanup
    let _ = kinetica
        .execute_sql_raw(&ExecuteSqlRequest {
            statement: format!("DROP TABLE IF EXISTS {table_name}"),
            ..Default::default()
        })
        .await;

    println!("Done!");
    Ok(())
}
