//! Kinetica Load Generator
//!
//! A configurable multi-threaded load generator for benchmarking Kinetica bulk insert
//! performance using BulkInserter (serde_avro_fast encoding).
//!
//! # Environment Variables
//!
//! Required:
//! - `KINETICA_URL` - Kinetica server URL
//! - `KINETICA_TABLE` - Target table name (will be created if not exists)
//! - `TOTAL_RECORDS` - Total number of records to insert
//!
//! Optional:
//! - `KINETICA_USER` - Username (default: "admin")
//! - `KINETICA_PASSWORD` - Password (default: "password")
//! - `KINETICA_BATCH_SIZE` - Records per batch (default: 10000)
//! - `KINETICA_MULTIHEAD` - Enable multi-head ingest (default: true)
//! - `SNAPPY` - Enable Snappy compression (default: true)
//! - `NUM_THREADS` - Number of producer threads (default: 1)
//! - `NUM_WORKERS` - Number of flush workers (default: 16)
//! - `MESSAGE_SIZE` - Payload size in bytes (default: 1024)
//! - `PAYLOAD_RANDOM` - Randomize payload each record (default: false)
//! - `BETTER_SCHEMA` - Use extended schema with threadId/localCount/nanosecond (default: false)
//! - `PROGRESS_INTERVAL_MS` - Progress report interval (default: 1000)
//! - `LOG_EVERY_N` - Log every N records (default: 0 = disabled)
//! - `CSV_OUTPUT` - Output CSV summary line (default: true)
//! - `TRUNCATE_TABLE` - Truncate table and exit (default: false)
//!
//! # Example
//!
//! ```bash
//! KINETICA_URL="http://localhost:9191" \
//! KINETICA_TABLE="test_load" \
//! TOTAL_RECORDS=1000000 \
//! NUM_THREADS=4 \
//! MESSAGE_SIZE=1024 \
//! cargo run -p gpudb-examples --bin load_generator --release
//! ```

use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaRecord, KineticaError, TypePropertiesBuilder,
    Kinetica, BulkInserter, ShardKeyExtractor, ShardKeyValues,
    CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest, CreateSchemaRequest, HasTableRequest,
};
use smallvec::smallvec;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use std::env;
use tokio::sync::Barrier;
use chrono::{Local, Utc};
use rand::Rng;

// ============================================================================
// Record Schemas
// ============================================================================

/// Basic ingest record schema:
///   - timestampMillis: long (TIMESTAMP property)
///   - source: int (DATA property)
///   - payload: bytes (DATA property)
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct IngestRecord {
    #[serde(rename = "timestampMillis")]
    pub timestamp_millis: i64,
    pub source: i32,
    #[serde(with = "arc_bytes_serde")]
    pub payload: Arc<[u8]>,
}

/// Custom serde for Arc<[u8]> to serialize as bytes
mod arc_bytes_serde {
    use std::sync::Arc;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(data: &Arc<[u8]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(data)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<[u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Arc::from(bytes.into_boxed_slice()))
    }
}

impl ShardKeyExtractor for IngestRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        // No explicit shard key - use default routing
        smallvec![]
    }
}

impl KineticaRecord for IngestRecord {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("IngestRecord")
                .long_field("timestampMillis")
                .int_field("source")
                .bytes_field("payload")
        )
    }

    fn type_properties() -> std::collections::HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .timestamp("timestampMillis")
            .data("source")
            .data("payload")
            .build()
    }
}

/// Extended ingest record schema:
///   - timestampMillis: long (TIMESTAMP property)
///   - source: int (DATA property)
///   - threadId: int (DATA property)
///   - localCount: long (DATA property)
///   - nanosecond: long (DATA property)
///   - payload: bytes (DATA property)
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct IngestRecord2 {
    #[serde(rename = "timestampMillis")]
    pub timestamp_millis: i64,
    pub source: i32,
    #[serde(rename = "threadId")]
    pub thread_id: i32,
    #[serde(rename = "localCount")]
    pub local_count: i64,
    pub nanosecond: i64,
    #[serde(with = "arc_bytes_serde")]
    pub payload: Arc<[u8]>,
}

impl ShardKeyExtractor for IngestRecord2 {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        // No explicit shard key - use default routing
        smallvec![]
    }
}

impl KineticaRecord for IngestRecord2 {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("IngestRecord2")
                .long_field("timestampMillis")
                .int_field("source")
                .int_field("threadId")
                .long_field("localCount")
                .long_field("nanosecond")
                .bytes_field("payload")
        )
    }

    fn type_properties() -> std::collections::HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .timestamp("timestampMillis")
            .data("source")
            .data("threadId")
            .data("localCount")
            .data("nanosecond")
            .data("payload")
            .build()
    }
}

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    url: String,
    username: String,
    password: String,
    table_name: String,
    total_records: i64,
    batch_size: usize,
    num_threads: usize,
    num_workers: usize,
    message_size: usize,
    payload_random: bool,
    better_schema: bool,
    use_multihead: bool,
    use_snappy: bool,
    progress_interval_ms: u64,
    log_every_n: i64,
    csv_output: bool,
    truncate_table: bool,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            url: required_env("KINETICA_URL")?,
            username: env::var("KINETICA_USER").unwrap_or_else(|_| "admin".to_string()),
            password: env::var("KINETICA_PASSWORD").unwrap_or_else(|_| "password".to_string()),
            table_name: required_env("KINETICA_TABLE")?,
            total_records: required_env_i64("TOTAL_RECORDS")?,
            batch_size: env_or_default_i32("KINETICA_BATCH_SIZE", 10_000) as usize,
            num_threads: env_or_default_i32("NUM_THREADS", 1) as usize,
            num_workers: env_or_default_i32("NUM_WORKERS", 16) as usize,
            message_size: env_or_default_i32("MESSAGE_SIZE", 1024) as usize,
            payload_random: env_or_default_bool("PAYLOAD_RANDOM", false),
            better_schema: env_or_default_bool("BETTER_SCHEMA", false),
            use_multihead: env_or_default_bool("KINETICA_MULTIHEAD", true),
            use_snappy: env_or_default_bool("SNAPPY", true),
            progress_interval_ms: env_or_default_i64("PROGRESS_INTERVAL_MS", 1000) as u64,
            log_every_n: env_or_default_i64("LOG_EVERY_N", 0),
            csv_output: env_or_default_bool("CSV_OUTPUT", true),
            truncate_table: env_or_default_bool("TRUNCATE_TABLE", false),
        })
    }
}

// ============================================================================
// Environment Variable Helpers
// ============================================================================

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("Missing required environment variable: {name}"))
}

fn required_env_i64(name: &str) -> Result<i64, String> {
    let value = required_env(name)?;
    value.parse::<i64>().map_err(|_| {
        format!("Environment variable {name} must be a valid i64, but was: {value}")
    })
}

fn env_or_default_i32(name: &str, default: i32) -> i32 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_or_default_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_or_default_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            let v = v.to_lowercase();
            v == "true" || v == "t" || v == "yes" || v == "y" || v == "1"
        })
        .unwrap_or(default)
}

// ============================================================================
// Timestamp formatting
// ============================================================================

fn format_timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration error: {e}");
            std::process::exit(1);
        }
    };

    // Print startup info (unless CSV output mode)
    if !config.csv_output {
        println!(
            "Starting KineticaLoadGenerator (Rust)\n\
             url={}\n\
             table={}\n\
             totalRecords={}\n\
             messageSize={}\n\
             payloadRandom={}\n\
             numThreads={}\n\
             batchSize={}\n\
             progressIntervalMs={}\n\
             logEveryN={}\n\
             multihead={}\n\
             snappy={}\n\
             betterSchema={}",
            config.url, config.table_name, config.total_records, config.message_size,
            config.payload_random, config.num_threads, config.batch_size,
            config.progress_interval_ms, config.log_every_n, config.use_multihead,
            config.use_snappy, config.better_schema
        );
    }

    // Connect to Kinetica
    let mut options = gpudb_api::KineticaOptions::new();
    options.set_username(config.username.clone())
           .set_password(config.password.clone())
           .set_bypass_ssl_cert_check(false)
           .set_use_snappy(config.use_snappy);

    let kinetica = Arc::new(Kinetica::new(vec![config.url.clone()], options).await?);

    // Check if table exists, create if needed
    let exists = kinetica.has_table_raw(&HasTableRequest {
        table_name: config.table_name.clone(),
        options: std::collections::HashMap::new(),
    }).await?.table_exists;

    if !exists {
        // Extract schema name if qualified table name
        if let Some(dot_pos) = config.table_name.find('.') {
            let schema_name = &config.table_name[..dot_pos];
            println!("Creating schema '{schema_name}'...");
            let _ = kinetica.create_schema_raw(&CreateSchemaRequest {
                schema_name: schema_name.to_string(),
                options: std::collections::HashMap::new(),
            }).await;
        }

        // Create type based on schema choice
        let type_id = if config.better_schema {
            let res = kinetica.create_type_raw(&CreateTypeRequest {
                type_definition: IngestRecord2::default().avro_schema().to_string(),
                label: "IngestRecord2".into(),
                properties: IngestRecord2::type_properties(),
                ..Default::default()
            }).await?;
            res.type_id
        } else {
            let res = kinetica.create_type_raw(&CreateTypeRequest {
                type_definition: IngestRecord::default().avro_schema().to_string(),
                label: "IngestRecord".into(),
                properties: IngestRecord::type_properties(),
                ..Default::default()
            }).await?;
            res.type_id
        };

        // Create table
        println!("Creating table '{}'...", config.table_name);
        let mut create_options = std::collections::HashMap::new();
        create_options.insert("no_error_if_exists".to_string(), "true".to_string());

        kinetica.create_table_raw(&CreateTableRequest {
            table_name: config.table_name.clone(),
            type_id,
            options: create_options
        }).await?;
    } else if config.truncate_table {
        // Table exists and truncate requested - clear the table before inserting
        println!("Truncating table '{}'...", config.table_name);
        let truncate_sql = format!("TRUNCATE TABLE {}", config.table_name);
        kinetica.execute_sql_raw(&ExecuteSqlRequest {
            statement: truncate_sql,
            offset: 0,
            limit: -9999,
            encoding: "json".to_string(),
            ..Default::default()
        }).await?;
        println!("Table '{}' truncated.", config.table_name);
    }

    // Global progress counter
    let global_inserted = Arc::new(AtomicI64::new(0));

    // Start timestamp
    let start_ts = format_timestamp();
    let start_time = Instant::now();

    // Progress reporter task
    let progress_global = global_inserted.clone();
    let progress_interval = config.progress_interval_ms;
    let csv_output = config.csv_output;

    let progress_handle = if progress_interval > 0 {
        Some(tokio::spawn(async move {
            let mut last_count = 0i64;
            let mut interval = tokio::time::interval(Duration::from_millis(progress_interval));

            loop {
                interval.tick().await;
                let total = progress_global.load(Ordering::Relaxed);
                let delta = total - last_count;
                last_count = total;

                let rate = delta as f64 / (progress_interval as f64 / 1000.0);
                let elapsed_sec = start_time.elapsed().as_secs();

                if !csv_output {
                    println!("[PROGRESS] {} elapsed={}s total={} rate={:.2} rec/s",
                        format_timestamp(), elapsed_sec, total, rate);
                }
            }
        }))
    } else {
        None
    };

    // Compute per-thread work slices
    let base_per_thread = config.total_records / config.num_threads as i64;
    let remainder = config.total_records % config.num_threads as i64;

    // Run the appropriate inserter based on schema choice
    if config.better_schema {
        run_insert::<IngestRecord2>(
            kinetica,
            &config,
            global_inserted.clone(),
            base_per_thread,
            remainder,
        ).await?;
    } else {
        run_insert::<IngestRecord>(
            kinetica,
            &config,
            global_inserted.clone(),
            base_per_thread,
            remainder,
        ).await?;
    }

    // Stop progress reporter
    if let Some(handle) = progress_handle {
        handle.abort();
    }

    // Final summary
    let elapsed_ms = start_time.elapsed().as_millis() as f64;
    let elapsed_sec = elapsed_ms / 1000.0;
    let final_total = global_inserted.load(Ordering::Relaxed);
    let avg_rate = if elapsed_sec > 0.0 { final_total as f64 / elapsed_sec } else { 0.0 };
    let end_ts = format_timestamp();

    // CSV output
    if config.csv_output {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "Unknown".to_string());

        eprintln!(
            "{},{},{},{},RustAPI,{},{},{},{},{:.2},{:.2}",
            start_ts, hostname, config.url, config.table_name,
            config.total_records, config.message_size, config.num_threads, config.batch_size,
            elapsed_sec, avg_rate
        );
    }

    println!("----------------------------------------------------");
    println!("[SUMMARY] Start: {start_ts}");
    println!("[SUMMARY] End: {end_ts}");
    println!("[SUMMARY] Rust URL: {}", config.url);
    println!("[SUMMARY] Total Records: {final_total}");
    println!("[SUMMARY] Batch Size: {}", config.batch_size);
    println!("[SUMMARY] Payload Size: {}", config.message_size);
    println!("[SUMMARY] Number of Threads: {}", config.num_threads);
    println!("[SUMMARY] Elapsed Time: {elapsed_sec:.2} sec");
    println!("[SUMMARY] Average Rate: {avg_rate:.2} rec/s");
    println!("----------------------------------------------------");
    println!("Done.");

    Ok(())
}

// ============================================================================
// Generic insert runner
// ============================================================================

async fn run_insert<T>(
    kinetica: Arc<Kinetica>,
    config: &Config,
    global_inserted: Arc<AtomicI64>,
    base_per_thread: i64,
    remainder: i64,
) -> Result<(), KineticaError>
where
    T: serde::Serialize + KineticaRecord + ShardKeyExtractor + Send + Sync + 'static + Default + Clone + RecordBuilder,
{
    let inserter = BulkInserter::<T>::new(
        kinetica.clone(),
        config.table_name.clone(),
        config.batch_size,
        config.num_workers,
    ).await?;

    let barrier = Arc::new(Barrier::new(config.num_threads));
    let mut handles = vec![];

    // How often threads update the global progress counter
    const PROGRESS_BATCH: i64 = 10;

    for t in 0..config.num_threads {
        let thread_id = t as i32;
        // First 'remainder' threads get +1 record
        let thread_records = base_per_thread + if (t as i64) < remainder { 1 } else { 0 };

        let ins = inserter.clone();
        let bar = barrier.clone();
        let global = global_inserted.clone();
        let message_size = config.message_size;
        let payload_random = config.payload_random;
        let log_every_n = config.log_every_n;
        let csv_output = config.csv_output;

        handles.push(tokio::spawn(async move {
            bar.wait().await;

            // Pre-generate payload as Arc<[u8]> (reused unless payload_random is true)
            // This eliminates cloning - we just increment Arc refcount
            let mut rng = rand::rng();
            let payload_vec: Vec<u8> = (0..message_size).map(|_| rng.random()).collect();
            let mut payload: Arc<[u8]> = Arc::from(payload_vec.into_boxed_slice());

            let mut local_count: i64 = 0;
            let mut last_reported: i64 = 0;

            for _j in 0..thread_records {
                let now = Utc::now().timestamp_millis();
                let nano = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;

                // Optionally randomize payload each record
                if payload_random {
                    let new_payload: Vec<u8> = (0..message_size).map(|_| rng.random()).collect();
                    payload = Arc::from(new_payload.into_boxed_slice());
                }

                // Build record using trait - Arc::clone is cheap (just refcount increment)
                let rec = T::build_record(
                    now,
                    message_size as i32,
                    thread_id,
                    local_count,
                    nano,
                    Arc::clone(&payload),
                );

                if let Err(e) = ins.insert(Arc::new(rec)) {
                    eprintln!("Thread {thread_id} insert error: {e:?}");
                }

                local_count += 1;

                // Periodically bump the global progress counter in batches
                if local_count - last_reported >= PROGRESS_BATCH {
                    let delta = local_count - last_reported;
                    global.fetch_add(delta, Ordering::Relaxed);
                    last_reported = local_count;
                }

                // Optional logging for every N global records
                if log_every_n > 0 {
                    let global_so_far = global.load(Ordering::Relaxed);
                    if global_so_far > 0 && global_so_far % log_every_n == 0
                        && !csv_output {
                            println!(
                                "[LOG] {} - approx {} records inserted so far (thread {})",
                                format_timestamp(), global_so_far, thread_id
                            );
                        }
                }
            }

            // Final progress update for any leftover below batch size
            let leftover = local_count - last_reported;
            if leftover > 0 {
                global.fetch_add(leftover, Ordering::Relaxed);
            }
        }));
    }

    // Wait for all threads to finish
    for h in handles {
        h.await.unwrap();
    }

    if !config.csv_output {
        println!("All threads finished; flushing BulkInserter...");
    }

    inserter.flush().await?;

    // Wait for flush workers to complete
    if !config.csv_output {
        println!("Waiting for flush workers to complete...");
    }

    let mut prev_count = 0i64;
    let mut stable_iterations = 0;

    for _ in 0..30 {  // Max 30 seconds
        tokio::time::sleep(Duration::from_secs(1)).await;
        let current_count = inserter.count_inserted();

        if current_count == prev_count {
            stable_iterations += 1;
            if stable_iterations >= 3 {  // Stable for 3 seconds
                break;
            }
        } else {
            stable_iterations = 0;
            if !config.csv_output {
                println!("  Progress: {current_count} records inserted...");
            }
        }
        prev_count = current_count;
    }

    println!("Bulk insert complete.");
    Ok(())
}

// ============================================================================
// Record builder trait for generic insert
// ============================================================================

trait RecordBuilder {
    fn build_record(
        timestamp_millis: i64,
        source: i32,
        thread_id: i32,
        local_count: i64,
        nanosecond: i64,
        payload: Arc<[u8]>,
    ) -> Self;
}

impl RecordBuilder for IngestRecord {
    fn build_record(
        timestamp_millis: i64,
        source: i32,
        _thread_id: i32,
        _local_count: i64,
        _nanosecond: i64,
        payload: Arc<[u8]>,
    ) -> Self {
        Self {
            timestamp_millis,
            source,
            payload,
        }
    }
}

impl RecordBuilder for IngestRecord2 {
    fn build_record(
        timestamp_millis: i64,
        source: i32,
        thread_id: i32,
        local_count: i64,
        nanosecond: i64,
        payload: Arc<[u8]>,
    ) -> Self {
        Self {
            timestamp_millis,
            source,
            thread_id,
            local_count,
            nanosecond,
            payload,
        }
    }
}
