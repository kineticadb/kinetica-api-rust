//! Example: Multi-threaded Bulk Insert Benchmark using serde_avro_fast
//!
//! Inserts records from several threads through one BulkInserter, which
//! encodes them in Avro with serde_avro_fast.
//!
//! Run with: cargo run -p gpudb-examples --bin bulk_insert_mt_fast --release
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaColumnProperties,
    Kinetica, BulkInserter, ShardKeyExtractor, ShardKeyValue, ShardKeyValues, KineticaRecord, KineticaError, TypePropertiesBuilder,
    CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest, CreateSchemaRequest,
};
use smallvec::smallvec;
use std::sync::Arc;
use tokio::sync::Barrier;

const RECS_PER_THREAD: usize = 1_000_000;

#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct BenchmarkRecord {
    pub id: i32,
    pub thread_id: i32,
    pub timestamp: i64,
    pub name: String,
    pub score: f64,
}

impl ShardKeyExtractor for BenchmarkRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![("id", ShardKeyValue::Int(self.id))]
    }
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

#[tokio::main]
async fn main() -> Result<(), KineticaError> {
    // Initialize logging (respects RUST_LOG env var, defaults to warn)
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let threads = 4;
    let schema_name = "bulk_insert_mt_fast";
    let table_name = "mt_bench_rust_fast";
    let table = format!("{schema_name}.{table_name}");

    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("--- Initializing Schema and Table '{table}' (serde_avro_fast Encoding) ---");

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

    // Drop table if it exists
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
        label: "bench_type_fast".into(),
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

    // Use BulkInserter with serde_avro_fast encoding
    let ins = BulkInserter::new(kinetica.clone(), table, 50000, 16).await?;

    let barrier = Arc::new(Barrier::new(threads));
    let start = std::time::Instant::now();

    println!("Starting {} threads to insert {} records each ({} total) using serde_avro_fast encoding...", threads, RECS_PER_THREAD, threads * RECS_PER_THREAD);
    println!("Configuration: 50K batch size, 16 flush workers, Arc<T> zero-copy, flume MPMC channel, hyper client, serde_avro_fast encoding");
    let mut handles = vec![];
    for t in 0..threads {
        let (ins, bar) = (ins.clone(), barrier.clone());
        handles.push(tokio::spawn(async move {
            bar.wait().await;

            // Pre-create all Arc<BenchmarkRecord> for this thread
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

    println!("Flushing and waiting for all batches to complete...");
    // Use close() to flush and wait for ALL pending batches to complete (no timeout)
    ins.close().await?;

    let d = start.elapsed();
    let final_count = ins.count_inserted();
    let actual_throughput = final_count as f64 / d.as_secs_f64();

    println!("\n========================================");
    println!(" RUST BULK INSERTER (serde_avro_fast)  ");
    println!("========================================");
    println!("Total Records Attempted : {}", threads * RECS_PER_THREAD);
    println!("Total Records Inserted  : {final_count}");
    println!("Success Rate            : {:.2}%", (final_count as f64 / (threads * RECS_PER_THREAD) as f64) * 100.0);
    println!("Total Time Elapsed      : {:.2} seconds", d.as_secs_f64());
    println!("Overall Throughput      : {actual_throughput:.2} rec/s");
    println!("Encoding                : serde_avro_fast");
    println!("========================================");
    Ok(())
}
