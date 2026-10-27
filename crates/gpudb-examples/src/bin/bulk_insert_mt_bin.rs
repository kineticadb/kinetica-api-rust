//! Example: Multi-threaded Bulk Insert Benchmark (Binary Encoding)
//!
//! This example demonstrates high-performance bulk insertion using multiple threads with binary (Avro) encoding.
//!
//! Run with: cargo run -p gpudb-examples --bin bulk_insert_mt_bin --release
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

// Note: jemalloc was tested but made performance 10% worse (see JEMALLOC_RESULTS.md)
// System allocator is better for this workload

use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaColumnProperties,
    Kinetica, BulkInserter, KineticaRecord, KineticaError, TypePropertiesBuilder, ShardKeyExtractor, ShardKeyValue, ShardKeyValues,
    CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest, CreateSchemaRequest,
};
use smallvec::smallvec;
use std::sync::Arc;
use tokio::sync::Barrier;

const RECS_PER_THREAD: usize = 1_000_000;

#[derive(serde::Serialize, Clone, Default)] // Add Default
struct BenchmarkRecord {
    pub id: i32, pub thread_id: i32, pub timestamp: i64, pub name: String, pub score: f64,
}

impl KineticaRecord for BenchmarkRecord {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("BenchmarkRecord")
                .int_field("id")
                .int_field("thread_id")
                .long_field("timestamp")
                .string_field("name")
                .double_field("score")
        )
    }
    fn type_properties() -> std::collections::HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .column("id", KineticaColumnProperties::new().primary_key())
            .column("name", KineticaColumnProperties::new().char_n(32))
            .build()
    }
}

impl ShardKeyExtractor for BenchmarkRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![("id", ShardKeyValue::Int(self.id))]
    }
}

#[tokio::main]
async fn main() -> Result<(), KineticaError> {
    let threads = 4;
    let schema_name = "bulk_insert_mt_bin";
    let table_name = "mt_bench_rust_bin";
    let table = format!("{schema_name}.{table_name}");

    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("--- Initializing Schema and Table '{table}' (Binary Encoding) ---");

    // Drop schema if it exists (with CASCADE to drop all contained objects)
    println!("Dropping schema '{schema_name}' if exists...");
    let drop_schema_sql = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_schema_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;

    // Create schema
    println!("Creating schema '{schema_name}'...");
    let _ = kinetica.create_schema_raw(&CreateSchemaRequest {
        schema_name: schema_name.to_string(),
        options: std::collections::HashMap::new(),
    }).await;

    // Drop table if it exists (no longer necessary due to CASCADE, but kept for clarity)
    println!("Dropping table if exists...");
    let drop_sql = format!("DROP TABLE IF EXISTS {table}");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;

    // Create type
    println!("Creating type...");
    let res = kinetica.create_type_raw(&CreateTypeRequest {
        type_definition: BenchmarkRecord::default().avro_schema().to_string(),
        label: "bench_type_bin".into(),
        properties: BenchmarkRecord::type_properties(),
        ..Default::default()
    }).await?;

    // Create table
    println!("Creating table...");
    kinetica.create_table_raw(&CreateTableRequest {
        table_name: table.clone(),
        type_id: res.type_id,
        ..Default::default()
    }).await?;

    // 50K records per batch suits most systems
    // Note: 20K batches were tested but gave similar/slightly worse performance
    let ins = BulkInserter::new(kinetica.clone(), table, 50000, 16).await?;

    let barrier = Arc::new(Barrier::new(threads));
    let start = std::time::Instant::now();

    println!("Starting {} threads to insert {} records each ({} total) using binary encoding...", threads, RECS_PER_THREAD, threads * RECS_PER_THREAD);
    println!("Configuration: 50K batch size, 16 flush workers, Arc<T> zero-copy, flume MPMC channel, hyper client, binary (Avro) encoding");
    let mut handles = vec![];
    for t in 0..threads {
        let (ins, bar) = (ins.clone(), barrier.clone());
        handles.push(tokio::spawn(async move {
            bar.wait().await;

            // OPTIMIZATION: Pre-create all Arc<BenchmarkRecord> for this thread
            // This amortizes allocation cost and improves cache locality
            let mut records = Vec::with_capacity(RECS_PER_THREAD);
            for i in 0..RECS_PER_THREAD {
                let id = (t * RECS_PER_THREAD + i) as i32;
                records.push(Arc::new(BenchmarkRecord {
                    id,
                    thread_id: t as i32,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    name: format!("u_{id}"),
                    score: 1.0
                }));
            }

            // Insert all pre-created records
            for rec in records.into_iter() {
                ins.insert(rec).unwrap();
            }
        }));
    }

    for h in handles { h.await.unwrap(); }

    println!("Flushing remaining buffered records...");
    ins.flush().await?;

    // Wait for all batches to complete
    println!("Waiting for flush workers to complete...");
    let mut prev_count = 0i64;
    let mut stable_iterations = 0;

    for _ in 0..30 {  // Max 30 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let current_count = ins.count_inserted();

        if current_count == prev_count {
            stable_iterations += 1;
            if stable_iterations >= 3 {  // Stable for 3 seconds
                break;
            }
        } else {
            stable_iterations = 0;
            println!("  Progress: {current_count} records inserted...");
        }
        prev_count = current_count;
    }

    let d = start.elapsed();
    let final_count = ins.count_inserted();
    let actual_throughput = final_count as f64 / d.as_secs_f64();

    println!("\n========================================");
    println!(" RUST BINARY BULK INSERTER BENCHMARK  ");
    println!("========================================");
    println!("Total Records Attempted : {}", threads * RECS_PER_THREAD);
    println!("Total Records Inserted  : {final_count}");
    println!("Success Rate            : {:.2}%", (final_count as f64 / (threads * RECS_PER_THREAD) as f64) * 100.0);
    println!("Total Time Elapsed      : {:.2} seconds", d.as_secs_f64());
    println!("Overall Throughput      : {actual_throughput:.2} rec/s");
    println!("Encoding                : Binary (Avro)");
    println!("========================================");
    Ok(())
}
