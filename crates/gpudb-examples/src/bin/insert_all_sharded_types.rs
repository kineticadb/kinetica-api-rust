//! Example: Shard-Aware High-Performance Bulk Insert with All Data Types
//!
//! This example demonstrates bulk insertion into a sharded table using BulkInserter
//! with records containing ALL 18 data types that can be used for shard key computation.
//!
//! BulkInserter automatically computes shard keys and routes records to the correct
//! worker based on the routing table. Records implement `ShardKeyExtractor` to provide
//! shard key values for routing.
//!
//! Data types demonstrated (all 18 shardable types):
//!
//!   Primitive Types:
//!   - int (i32), long (i64), float (f32), double (f64)
//!   - boolean (1 byte), int8 (1 byte), int16 (2 bytes)
//!
//!   String Types:
//!   - string (murmur3 hashed), char(N) (fixed-width)
//!
//!   Date/Time Types:
//!   - date (YYYY-MM-DD), datetime (YYYY-MM-DD HH:MM:SS.mmm)
//!   - time (HH:MM:SS.mmm), timestamp (milliseconds since epoch)
//!
//!   Special Types:
//!   - ipv4 (dotted quad), uuid (8-4-4-4-12 hex format)
//!   - ulong (unsigned 64-bit), decimal (both 8-byte and 12-byte variants)
//!
//! Run with: cargo run --release --bin insert_all_sharded_types
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

/// Record type with ALL 18 data types supported for shard key computation
#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
struct AllTypesRecord {
    // Primitive types (7)
    pub id: i32,
    pub long_val: i64,
    pub float_val: f32,
    pub double_val: f64,
    pub bool_val: i32,       // boolean stored as int (0/1)
    pub int8_val: i32,       // int8 stored as int
    pub int16_val: i32,      // int16 stored as int
    // String types (2)
    pub string_val: String,
    pub char_val: String,
    // Date/Time types (4)
    pub date_val: String,
    pub datetime_val: String,
    pub time_val: String,
    pub timestamp_val: i64,
    // Special types (5)
    pub ipv4_val: String,
    pub uuid_val: String,
    pub ulong_val: String,
    pub decimal_small: String,
    pub decimal_large: String,
    // Payload (not a shard key)
    pub payload: String,
}

/// Implement ShardKeyExtractor to provide shard key values for routing
impl ShardKeyExtractor for AllTypesRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        // Only id is the shard key for this table (typed for performance)
        // Note: We use only 'id' as the shard key for simplicity, but
        // all other fields demonstrate how to store and encode each type.
        smallvec![("id", ShardKeyValue::Int(self.id))]
    }
}

impl KineticaRecord for AllTypesRecord {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("AllTypesRecord")
                // Primitive types
                .int_field("id")
                .long_field("long_val")
                .float_field("float_val")
                .double_field("double_val")
                .int_field("bool_val")      // boolean stored as int
                .int_field("int8_val")      // int8 stored as int
                .int_field("int16_val")     // int16 stored as int
                // String types
                .string_field("string_val")
                .string_field("char_val")
                // Date/Time types
                .string_field("date_val")
                .string_field("datetime_val")
                .string_field("time_val")
                .long_field("timestamp_val")
                // Special types
                .string_field("ipv4_val")
                .string_field("uuid_val")
                .string_field("ulong_val")
                .string_field("decimal_small")
                .string_field("decimal_large")
                // Payload
                .string_field("payload")
        )
    }

    fn type_properties() -> std::collections::HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            // Primary key (also shard key by Kinetica requirement)
            .column("id", KineticaColumnProperties::new().primary_key().shard_key())
            // Primitive type properties
            .column("bool_val", KineticaColumnProperties::new().boolean())
            .column("int8_val", KineticaColumnProperties::new().int8())
            .column("int16_val", KineticaColumnProperties::new().int16())
            // String type properties
            .column("char_val", KineticaColumnProperties::new().char_n(16))
            // Date/Time type properties
            .column("date_val", KineticaColumnProperties::new().date())
            .column("datetime_val", KineticaColumnProperties::new().datetime())
            .column("time_val", KineticaColumnProperties::new().time())
            .column("timestamp_val", KineticaColumnProperties::new().timestamp())
            // Special type properties
            .column("ipv4_val", KineticaColumnProperties::new().ipv4())
            .column("uuid_val", KineticaColumnProperties::new().uuid())
            .column("ulong_val", KineticaColumnProperties::new().ulong())
            .column("decimal_small", KineticaColumnProperties::new().char_n(32))
            .column("decimal_large", KineticaColumnProperties::new().char_n(64))
            // Payload
            .column("payload", KineticaColumnProperties::new().char_n(64))
            .build()
    }
}

/// Generate a record with deterministic values based on index
fn generate_record(idx: usize, thread_id: usize, records_per_thread: usize) -> AllTypesRecord {
    let id = (thread_id * records_per_thread + idx) as i32;

    // Date components
    let date_day = 1 + (idx % 28);
    let date_month = 1 + (idx % 12);
    let date_year = 2020 + (idx % 5);

    // Time components
    let hour = idx % 24;
    let minute = idx % 60;
    let second = (idx * 7) % 60;
    let millis = (idx * 13) % 1000;

    // IP address components
    let ip_octet1 = 10 + (idx % 240);
    let ip_octet2 = (idx / 240) % 256;
    let ip_octet3 = thread_id % 256;
    let ip_octet4 = idx % 256;

    AllTypesRecord {
        // Primitive types
        id,
        long_val: (id as i64) * 1_000_000 + (thread_id as i64),
        float_val: (idx as f32) * 0.5 + (thread_id as f32) * 100.0,
        double_val: (idx as f64) * 0.123456789 + (thread_id as f64) * 1000.0,
        bool_val: if idx.is_multiple_of(2) { 1 } else { 0 },
        int8_val: ((idx % 256) as i32 - 128),
        int16_val: ((idx % 65536) as i32 - 32768),
        // String types
        string_val: format!("str_{thread_id}_{idx}"),
        char_val: format!("c{id:06}"),
        // Date/Time types
        date_val: format!("{date_year:04}-{date_month:02}-{date_day:02}"),
        datetime_val: format!("{date_year:04}-{date_month:02}-{date_day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}"),
        time_val: format!("{hour:02}:{minute:02}:{second:02}.{millis:03}"),
        timestamp_val: chrono::Utc::now().timestamp_millis(),
        // Special types
        ipv4_val: format!("{ip_octet1}.{ip_octet2}.{ip_octet3}.{ip_octet4}"),
        uuid_val: format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            id as u32, (idx % 65536) as u16, ((idx * 7) % 65536) as u16,
            ((idx * 13) % 65536) as u16, (idx as u64 * 123456789) % 0xFFFFFFFFFFFF),
        ulong_val: format!("{}", (idx as u64) * 1_000_000_000_000),
        decimal_small: format!("{}.{:04}", idx % 10000, (idx * 7) % 10000),
        decimal_large: format!("{}.{:010}",
            (idx as u64) * 1_000_000_000 % 10_000_000_000_000_000,
            (idx as u64 * 123456789) % 10_000_000_000),
        // Payload
        payload: format!("payload_t{thread_id}_r{idx}"),
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

    let schema_name = "all_types_test";
    let table_name = "all_sharded_types";
    let table = format!("{schema_name}.{table_name}");

    let (urls, options) = common::connection_config();

    println!("==============================================");
    println!(" Bulk Insert - All Sharded Data Types");
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
        type_definition: AllTypesRecord::default().avro_schema().to_string(),
        label: "all_types_record".into(),
        properties: AllTypesRecord::type_properties(),
        ..Default::default()
    }).await?;

    let type_id = type_res.type_id;

    kinetica.create_table_raw(&CreateTableRequest {
        table_name: table.clone(),
        type_id: type_id.clone(),
        options: HashMap::new()
    }).await?;

    println!("  Type ID: {type_id}");
    println!("  Table created successfully\n");

    // Create bulk inserter - routing is handled internally
    let inserter = BulkInserter::new(
        kinetica.clone(),
        table.clone(),
        batch_size,
        num_workers,
    ).await?;

    // Progress tracking
    let global_count = Arc::new(AtomicI64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));

    println!("Starting bulk insert...\n");
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
                // BulkInserter automatically computes shard key hash using
                // ShardKeyExtractor::shard_key_values() and routes to the
                // correct worker based on the routing table.
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

    println!();
    println!("==============================================");
    println!(" BULK INSERT COMPLETE");
    println!("==============================================");
    println!("Total Records Inserted: {final_count}");
    println!("Table Row Count:        {:?}", count_result.total_number_of_records);
    println!("Time Elapsed:           {:.2} seconds", duration.as_secs_f64());
    println!("Throughput:             {throughput:.2} rec/s");
    println!("==============================================\n");

    println!("All 18 data types in record:\n");
    println!("  Primitive Types (7):");
    println!("    - id (int):           Primary key, shard key");
    println!("    - long_val (long):    8-byte integer");
    println!("    - float_val (float):  4-byte float");
    println!("    - double_val (double): 8-byte double");
    println!("    - bool_val (boolean): 1-byte boolean (0/1)");
    println!("    - int8_val (int8):    1-byte signed integer");
    println!("    - int16_val (int16):  2-byte signed integer");
    println!();
    println!("  String Types (2):");
    println!("    - string_val (string): Variable-length string");
    println!("    - char_val (char16):  Fixed-width 16-char");
    println!();
    println!("  Date/Time Types (4):");
    println!("    - date_val (date):    YYYY-MM-DD format");
    println!("    - datetime_val:       YYYY-MM-DD HH:MM:SS.mmm format");
    println!("    - time_val (time):    HH:MM:SS.mmm format");
    println!("    - timestamp_val:      Milliseconds since epoch");
    println!();
    println!("  Special Types (5):");
    println!("    - ipv4_val (ipv4):    Dotted-quad IP address");
    println!("    - uuid_val (uuid):    8-4-4-4-12 hex format");
    println!("    - ulong_val (ulong):  Unsigned 64-bit integer");
    println!("    - decimal_small:      Decimal (precision <= 18)");
    println!("    - decimal_large:      Decimal (precision > 18)");
    println!();
    println!("  Payload (not shardable):");
    println!("    - payload (char64):   64-char payload");

    Ok(())
}
