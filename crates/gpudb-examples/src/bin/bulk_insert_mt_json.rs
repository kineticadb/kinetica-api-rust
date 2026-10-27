//! Example: Multi-threaded Bulk Insert Benchmark (JSON Encoding)
//!
//! This example demonstrates high-performance bulk insertion using multiple threads with JSON encoding.
//!
//! Run with: cargo run -p gpudb-examples --bin bulk_insert_mt_json --release
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, RawInsertRecordsRequest, CreateTableRequest, CreateTypeRequest, ExecuteSqlRequest, CreateSchemaRequest};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Barrier;

const RECS_PER_THREAD: usize = 1_000_000;

#[derive(serde::Serialize, Clone, Default)]
struct BenchmarkRecord {
    pub id: i32,
    pub thread_id: i32,
    pub timestamp: i64,
    pub name: String,
    pub score: f64,
}

fn avro_schema() -> &'static str {
    r#"{"type":"record","name":"BenchmarkRecord","fields":[{"name":"id","type":"int"},{"name":"thread_id","type":"int"},{"name":"timestamp","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"}]}"#
}

fn type_properties() -> HashMap<String, Vec<String>> {
    let mut m = HashMap::new();
    m.insert("id".into(), vec!["int".into(), "primary_key".into()]);
    m.insert("name".into(), vec!["char32".into()]);
    m
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let threads = 4;
    let schema_name = "bulk_insert_mt_json";
    let table_name = "mt_bench_rust_json";
    let table = format!("{schema_name}.{table_name}");

    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("--- Initializing Schema and Table '{table}' (JSON Encoding) ---");

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
        options: HashMap::new(),
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
        type_definition: avro_schema().to_string(),
        label: "bench_type_json".into(),
        properties: type_properties(),
        ..Default::default()
    }).await?;

    // Create table
    println!("Creating table...");
    kinetica.create_table_raw(&CreateTableRequest {
        table_name: table.clone(),
        type_id: res.type_id,
        ..Default::default()
    }).await?;

    let barrier = Arc::new(Barrier::new(threads));
    let start = std::time::Instant::now();

    println!("Starting {} threads to insert {} records each ({} total) using JSON encoding...",
             threads, RECS_PER_THREAD, threads * RECS_PER_THREAD);

    let mut handles = vec![];
    let batch_size = 10000; // Batch size for JSON inserts
    let insert_options: HashMap<String, String> = HashMap::new();

    for t in 0..threads {
        let (kin, bar, tbl) = (kinetica.clone(), barrier.clone(), table.to_string());
        let opts = insert_options.clone();

        handles.push(tokio::spawn(async move {
            bar.wait().await;

            let mut total_inserted = 0;
            let mut batch = Vec::new();

            for i in 0..RECS_PER_THREAD {
                let id = (t * RECS_PER_THREAD + i) as i32;
                let record = BenchmarkRecord {
                    id,
                    thread_id: t as i32,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    name: format!("u_{id}"),
                    score: 1.0
                };

                batch.push(serde_json::to_string(&record).unwrap());

                // Insert batch when full
                if batch.len() >= batch_size {
                    let insert_req = RawInsertRecordsRequest {
                        table_name: tbl.clone(),
                        list: Vec::new(),
                        list_str: batch.clone(),
                        list_encoding: "json".to_string(),
                        options: opts.clone(),
                    };

                    match kin.insert_records_raw(&insert_req).await {
                        Ok(resp) => {
                            total_inserted += resp.count_inserted;
                        }
                        Err(e) => {
                            eprintln!("Thread {t} insert error: {e}");
                        }
                    }
                    batch.clear();
                }
            }

            // Insert remaining records
            if !batch.is_empty() {
                let insert_req = RawInsertRecordsRequest {
                    table_name: tbl.clone(),
                    list: Vec::new(),
                    list_str: batch,
                    list_encoding: "json".to_string(),
                    options: opts.clone(),
                };

                match kin.insert_records_raw(&insert_req).await {
                    Ok(resp) => {
                        total_inserted += resp.count_inserted;
                    }
                    Err(e) => {
                        eprintln!("Thread {t} final insert error: {e}");
                    }
                }
            }

            total_inserted
        }));
    }

    let mut final_count = 0i64;
    for h in handles {
        final_count += h.await.unwrap() as i64;
    }

    let d = start.elapsed();
    let actual_throughput = final_count as f64 / d.as_secs_f64();

    println!("\n========================================");
    println!("  RUST JSON BULK INSERT BENCHMARK     ");
    println!("========================================");
    println!("Total Records Attempted : {}", threads * RECS_PER_THREAD);
    println!("Total Records Inserted  : {final_count}");
    println!("Success Rate            : {:.2}%", (final_count as f64 / (threads * RECS_PER_THREAD) as f64) * 100.0);
    println!("Total Time Elapsed      : {:.2} seconds", d.as_secs_f64());
    println!("Overall Throughput      : {actual_throughput:.2} rec/s");
    println!("Encoding                : JSON");
    println!("========================================");
    Ok(())
}
