//! Example: Shard-Aware Bulk Insert
//!
//! `BulkInserter` routes each record straight to the worker that owns its
//! shard (multi-head ingest), so the server does not have to redistribute it.
//!
//! **How Shard-Aware Routing Works:**
//!
//! 1. On init, fetches routing table via `admin_show_shards`
//! 2. Fetches table schema via `show_table` to identify shard key columns
//! 3. Builds a `RecordKeyBuilder` for computing routing keys
//! 4. For each record, implements `ShardKeyExtractor` to provide shard key values
//! 5. Computes routing hash and maps to target worker via routing table
//!
//! Run with: cargo run --release --bin insert_sharded_aware
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)
//!   TOTAL_RECORDS     - Number of records to insert (default: 1000000)
//!   NUM_THREADS       - Number of producer threads (default: 4)
//!   BATCH_SIZE        - Records per batch (default: 50000)
//!   NUM_WORKERS       - Number of flush workers (default: 16)

use gpudb_examples::common;

use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaColumnProperties,
    Kinetica, BulkInserter, ShardKeyExtractor, ShardKeyValue, ShardKeyValues, KineticaRecord, KineticaError, TypePropertiesBuilder,
    CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest, CreateSchemaRequest,
};
use smallvec::smallvec;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashMap;
use std::env;
use tokio::sync::Barrier;

fn env_or_default<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Record type with composite shard key (id + region)
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct ShardedRecord {
    pub id: i32,
    pub region: String,      // Part of composite shard key
    pub category: String,    // Part of composite shard key
    pub value: f64,
    pub timestamp: i64,
    pub payload: String,
}

/// Implement ShardKeyExtractor to provide shard key values for routing
impl ShardKeyExtractor for ShardedRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        // Include ALL shard key columns (typed for performance)
        smallvec![
            ("id", ShardKeyValue::Int(self.id)),
            ("region", ShardKeyValue::String(&self.region)),
            ("category", ShardKeyValue::String(&self.category)),
        ]
    }
}

impl KineticaRecord for ShardedRecord {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("ShardedRecord")
                .int_field("id")
                .string_field("region")
                .string_field("category")
                .double_field("value")
                .long_field("timestamp")
                .string_field("payload")
        )
    }

    fn type_properties() -> std::collections::HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            // Composite primary key (must include shard key columns)
            .column("id", KineticaColumnProperties::new().primary_key().shard_key())
            .column("region", KineticaColumnProperties::new().char_n(32).primary_key().shard_key())
            .column("category", KineticaColumnProperties::new().char_n(32).primary_key().shard_key())
            .column("timestamp", KineticaColumnProperties::new().timestamp())
            .column("payload", KineticaColumnProperties::new().char_n(64))
            .build()
    }
}

/// Generate a record with deterministic values based on index
fn generate_record(idx: usize, thread_id: usize, records_per_thread: usize) -> ShardedRecord {
    let id = (thread_id * records_per_thread + idx) as i32;

    // Create varied region and category values for distribution testing
    let regions = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"];
    let categories = ["electronics", "clothing", "food", "home", "sports", "toys", "books", "auto"];

    ShardedRecord {
        id,
        region: regions[idx % regions.len()].to_string(),
        category: categories[(idx / regions.len()) % categories.len()].to_string(),
        value: (idx as f64) * 0.99 + (thread_id as f64) * 100.0,
        timestamp: chrono::Utc::now().timestamp_millis(),
        payload: format!("data_t{thread_id}_r{idx}"),
    }
}

#[tokio::main]
async fn main() -> Result<(), KineticaError> {
    // Configuration from environment
    let total_records: usize = env_or_default("TOTAL_RECORDS", 1_000_000);
    let num_threads: usize = env_or_default("NUM_THREADS", 4);
    let batch_size: usize = env_or_default("BATCH_SIZE", 50_000);
    let num_workers: usize = env_or_default("NUM_WORKERS", 16);
    let records_per_thread = total_records / num_threads;

    let schema_name = "sharded_test";
    let table_name = "sharded_aware";
    let table = format!("{schema_name}.{table_name}");

    let (urls, options) = common::connection_config();

    println!("==============================================");
    println!(" Shard-Aware Bulk Insert - Composite Shard Key");
    println!("==============================================\n");
    println!("Configuration:");
    println!("  Kinetica URL:    {}", urls.first().unwrap_or(&String::new()));
    println!("  Table:           {table}");
    println!("  Total Records:   {total_records}");
    println!("  Threads:         {num_threads}");
    println!("  Batch Size:      {batch_size}");
    println!("  Flush Workers:   {num_workers}");
    println!();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    // Setup schema and table
    println!("Setting up schema and table...");

    let drop_schema_sql = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_schema_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;

    let _ = kinetica.create_schema_raw(&CreateSchemaRequest {
        schema_name: schema_name.to_string(),
        options: HashMap::new(),
    }).await;

    let type_res = kinetica.create_type_raw(&CreateTypeRequest {
        type_definition: ShardedRecord::default().avro_schema().to_string(),
        label: "sharded_record".into(),
        properties: ShardedRecord::type_properties(),
        ..Default::default()
    }).await?;

    let type_id = type_res.type_id;

    kinetica.create_table_raw(&CreateTableRequest {
        table_name: table.clone(),
        type_id: type_id.clone(),
        options: HashMap::new()
    }).await?;

    println!("  Type ID: {type_id}");
    println!("  Table created with composite shard key: (id, region, category)");
    println!();

    // Create shard-aware bulk inserter
    // This will fetch routing table and table schema automatically
    println!("Creating shard-aware bulk inserter...\n");
    let inserter = BulkInserter::new(
        kinetica.clone(),
        table.clone(),
        batch_size,
        num_workers,
    ).await?;

    println!("Shard Configuration:");
    println!("  Shard columns: {:?}", inserter.shard_column_names());
    println!("  Routing table size: {} shards", inserter.routing_table_size());
    println!();

    // Progress tracking
    let global_count = Arc::new(AtomicI64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));

    println!("Starting shard-aware bulk insert...\n");
    let start = std::time::Instant::now();

    // Spawn producer threads
    let mut handles = vec![];
    for t in 0..num_threads {
        let ins = inserter.clone();
        let bar = barrier.clone();
        let counter = global_count.clone();

        handles.push(tokio::spawn(async move {
            bar.wait().await;

            for i in 0..records_per_thread {
                let record = generate_record(i, t, records_per_thread);
                // The inserter routes the record by its
                // ShardKeyExtractor::shard_key_values()
                ins.insert(Arc::new(record)).unwrap();

                // Update progress periodically
                if (i + 1) % 10000 == 0 {
                    counter.fetch_add(10000, Ordering::Relaxed);
                }
            }
            // Final update for remainder
            let remainder = records_per_thread % 10000;
            if remainder > 0 {
                counter.fetch_add(remainder as i64, Ordering::Relaxed);
            }
        }));
    }

    // Wait for all threads
    for h in handles {
        h.await.unwrap();
    }

    println!("All producer threads completed, flushing...\n");
    inserter.flush().await?;

    // Wait for flush workers to complete
    let mut prev_count = 0i64;
    let mut stable_iterations = 0;

    for _ in 0..30 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let current_count = inserter.count_inserted();

        if current_count == prev_count {
            stable_iterations += 1;
            if stable_iterations >= 3 {
                break;
            }
        } else {
            stable_iterations = 0;
            let elapsed = start.elapsed().as_secs_f64();
            let rate = current_count as f64 / elapsed;
            println!("  Progress: {current_count} records ({rate:.0} rec/s)");
        }
        prev_count = current_count;
    }

    let duration = start.elapsed();
    let final_count = inserter.count_inserted();
    let throughput = final_count as f64 / duration.as_secs_f64();

    // Verify with SQL
    let count_sql = format!("SELECT COUNT(*) FROM {table}");
    let count_result = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: count_sql,
        offset: 0,
        limit: 1,
        encoding: "json".to_string(),
        ..Default::default()
    }).await?;

    // Get distribution by region to verify routing
    let dist_sql = format!("SELECT region, COUNT(*) as cnt FROM {table} GROUP BY region ORDER BY region");
    let dist_result = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: dist_sql,
        offset: 0,
        limit: 100,
        encoding: "json".to_string(),
        ..Default::default()
    }).await?;

    println!();
    println!("==============================================");
    println!(" SHARD-AWARE BULK INSERT COMPLETE");
    println!("==============================================");
    println!("Total Records Inserted: {final_count}");
    println!("Table Row Count:        {:?}", count_result.total_number_of_records);
    println!("Time Elapsed:           {:.2} seconds", duration.as_secs_f64());
    println!("Throughput:             {throughput:.2} rec/s");
    println!("==============================================\n");

    println!("Shard Key Configuration:");
    println!("  Composite key: (id, region, category)");
    println!("  - id (int):        Sequential ID");
    println!("  - region (char32): Geographic region");
    println!("  - category (char32): Product category");
    println!();

    println!("Distribution by Region:");
    if !dist_result.json_encoded_response.is_empty() {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&dist_result.json_encoded_response) {
            if let Some(records) = parsed.get("data") {
                println!("{}", serde_json::to_string_pretty(records).unwrap_or_default());
            }
        }
    }

    println!("\nThis example demonstrates shard-aware routing where each record");
    println!("is sent directly to the worker that owns its shard, eliminating");
    println!("server-side record redistribution.");

    Ok(())
}
