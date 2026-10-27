//! Example demonstrating the use of RecordRetriever for efficient record retrieval.
//!
//! This example:
//! 1. Creates a table with shard key columns
//! 2. Inserts data using BulkInserter
//! 3. Uses RecordRetriever to fetch records by shard key
//!
//! Run with:
//! ```
//! KINETICA_URL="http://localhost:9191" cargo run --bin record_retriever
//! ```

use gpudb_api::{
    avro_schema, AlterTableRequest, AvroSchemaBuilder, BulkInserter, ClearTableRequest,
    CreateTableRequest, CreateTypeRequest, Kinetica, KineticaColumnProperties, KineticaOptions,
    KineticaRecord, RecordRetriever, ShardKeyExtractor, ShardKeyValue, ShardKeyValues, Type,
    TypePropertiesBuilder,
};
use serde::Serialize;
use smallvec::smallvec;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

/// Test record with shard key columns
#[derive(Serialize, Clone, Default, Debug)]
struct TestRecord {
    i8_val: i32,  // int8 type (stored as i32)
    i16_sk: i32,  // int16, shard_key
    s1: String,   // string, shard_key
    i16_2: i32,   // int16
    i1_sk: i32,   // int, shard_key
    d_sk: f64,    // double, shard_key
    f_sk: f32,    // float, shard_key
}

impl ShardKeyExtractor for TestRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![
            ("i16_sk", ShardKeyValue::Int(self.i16_sk)),
            ("s1", ShardKeyValue::String(&self.s1)),
            ("i1_sk", ShardKeyValue::Int(self.i1_sk)),
            ("d_sk", ShardKeyValue::Double(self.d_sk)),
            ("f_sk", ShardKeyValue::Float(self.f_sk)),
        ]
    }
}

impl KineticaRecord for TestRecord {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("test_record")
                .int_field("i8_val")
                .int_field("i16_sk")
                .string_field("s1")
                .int_field("i16_2")
                .int_field("i1_sk")
                .double_field("d_sk")
                .float_field("f_sk")
        )
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .column("i8_val", KineticaColumnProperties::new().int8())
            .column(
                "i16_sk",
                KineticaColumnProperties::new().int16().shard_key(),
            )
            .column("s1", KineticaColumnProperties::new().shard_key())
            .column("i16_2", KineticaColumnProperties::new().int16())
            .column("i1_sk", KineticaColumnProperties::new().shard_key())
            .column("d_sk", KineticaColumnProperties::new().shard_key())
            .column("f_sk", KineticaColumnProperties::new().shard_key())
            .build()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Configuration from environment
    let kinetica_url =
        env::var("KINETICA_URL").unwrap_or_else(|_| "http://localhost:9191".to_string());
    let username = env::var("KINETICA_USER").unwrap_or_else(|_| "admin".to_string());
    let password = env::var("KINETICA_PASSWORD").unwrap_or_else(|_| "password".to_string());
    let table_name = "test_shard_key_rust";

    println!("=== RecordRetriever Example (Rust) ===");
    println!("Connecting to Kinetica at: {kinetica_url}");
    println!("Table name: {table_name}\n");

    // Create Kinetica client
    let mut options = KineticaOptions::new();
    options
        .set_username(username)
        .set_password(password)
        .set_use_snappy(false);

    let kinetica = Arc::new(Kinetica::new(vec![kinetica_url], options).await?);
    println!("Connected to Kinetica successfully!");

    // Clear existing table
    let _ = kinetica
        .clear_table_raw(&ClearTableRequest {
            table_name: table_name.to_string(),
            options: HashMap::from([("no_error_if_not_exists".to_string(), "true".to_string())]),
            ..Default::default()
        })
        .await;

    // Create the type
    let sample = TestRecord::default();
    let type_definition = sample.avro_schema();

    let type_resp = kinetica
        .create_type_raw(&CreateTypeRequest {
            type_definition: type_definition.to_string(),
            label: "test_shard_key_type".to_string(),
            properties: TestRecord::type_properties(),
            options: Default::default(),
        })
        .await?;
    println!("Created type: {}", type_resp.type_id);

    // Create the table
    kinetica
        .create_table_raw(&CreateTableRequest {
            table_name: table_name.to_string(),
            type_id: type_resp.type_id,
            options: Default::default(),
        })
        .await?;
    println!("Created table: {table_name}");

    // Create indices on shard key columns (required for multi-head key lookup)
    let shard_columns = vec!["i16_sk", "s1", "i1_sk", "d_sk", "f_sk"];
    println!("Creating indices on shard key columns...");
    for col in &shard_columns {
        let _ = kinetica
            .alter_table_raw(&AlterTableRequest {
                table_name: table_name.to_string(),
                action: "create_index".to_string(),
                value: col.to_string(),
                options: Default::default(),
            })
            .await;
    }
    println!("Indices created");

    // Generate test data
    let test_data: Vec<TestRecord> = vec![
        TestRecord { i8_val: 1, i16_sk: 100, s1: "A".to_string(), i16_2: 200, i1_sk: 1000, d_sk: 123.45, f_sk: 67.89 },
        TestRecord { i8_val: 2, i16_sk: 101, s1: "Hello World".to_string(), i16_2: 201, i1_sk: 2000, d_sk: 234.56, f_sk: 78.90 },
        TestRecord { i8_val: 3, i16_sk: 102, s1: "The quick brown fox".to_string(), i16_2: 202, i1_sk: 3000, d_sk: 345.67, f_sk: 89.01 },
        TestRecord { i8_val: 4, i16_sk: 103, s1: "Systems programming".to_string(), i16_2: 203, i1_sk: 4000, d_sk: 456.78, f_sk: 90.12 },
        TestRecord { i8_val: 5, i16_sk: 104, s1: "Rust programming".to_string(), i16_2: 204, i1_sk: 5000, d_sk: 567.89, f_sk: 1.23 },
        TestRecord { i8_val: 6, i16_sk: 105, s1: "Data structures".to_string(), i16_2: 205, i1_sk: 6000, d_sk: 678.90, f_sk: 12.34 },
        TestRecord { i8_val: 7, i16_sk: 106, s1: "Object-oriented design".to_string(), i16_2: 206, i1_sk: 7000, d_sk: 789.01, f_sk: 23.45 },
        TestRecord { i8_val: 8, i16_sk: 107, s1: "Big Data".to_string(), i16_2: 207, i1_sk: 8000, d_sk: 890.12, f_sk: 34.56 },
        TestRecord { i8_val: 9, i16_sk: 108, s1: "Machine Learning".to_string(), i16_2: 208, i1_sk: 9000, d_sk: 901.23, f_sk: 45.67 },
        TestRecord { i8_val: 10, i16_sk: 109, s1: "Cloud computing".to_string(), i16_2: 209, i1_sk: 10000, d_sk: 112.34, f_sk: 56.78 },
    ];

    // Insert data using BulkInserter
    println!(
        "\nInserting {} records using BulkInserter...",
        test_data.len()
    );
    let inserter =
        BulkInserter::<TestRecord>::new(kinetica.clone(), table_name.into(), 1000, 4).await?;

    for record in &test_data {
        inserter.insert(Arc::new(record.clone()))?;
    }
    inserter.flush().await?;
    println!("Inserted {} records", test_data.len());

    // Create RecordRetriever
    println!("\n--- Creating RecordRetriever ---");
    let record_type = Arc::new(Type::from_table(&kinetica, table_name).await?);
    let retriever = RecordRetriever::new(kinetica.clone(), table_name, record_type).await?;
    println!("RecordRetriever created");
    println!("  - Using head rank: {}", retriever.is_using_head_rank());
    println!(
        "  - Worker lookup supported: {}",
        retriever.is_doing_worker_lookup()
    );

    // Verify lookups using shard key values
    println!("\n--- Verifying Key Lookups ---");
    let mut records_found = 0i64;
    let mut lookup_errors = 0i64;

    for record in &test_data {
        // The shard key values, in key order
        let shard_key_values = [
            ShardKeyValue::Int(record.i16_sk),
            ShardKeyValue::String(&record.s1),
            ShardKeyValue::Int(record.i1_sk),
            ShardKeyValue::Double(record.d_sk),
            ShardKeyValue::Float(record.f_sk),
        ];

        match retriever.get_by_key(&shard_key_values, None).await {
            Ok(response) => {
                records_found += response.total_number_of_records;
            }
            Err(e) => {
                println!("Key lookup error for {shard_key_values:?}: {e}");
                lookup_errors += 1;
            }
        }
    }

    println!("\n=== Results ===");
    println!("Total Records:          {:5}", test_data.len());
    println!("Total Records Found:    {records_found:5}");
    println!(
        "Total Lookup Successes: {:5}",
        test_data.len() as i64 - lookup_errors
    );
    println!("Total Lookup Failures:  {lookup_errors:5}");

    // Test retrieval with expression filter
    println!("\n--- Test: Retrieve with Expression Filter ---");
    let response = retriever.get_by_key(&[], Some("i1_sk > 5000")).await?;
    println!(
        "Records with i1_sk > 5000: {} records",
        response.total_number_of_records
    );

    // Test retrieval of all records
    println!("\n--- Test: Retrieve All Records ---");
    let response = retriever.get_by_key(&[], None).await?;
    println!(
        "All records in table: {} records",
        response.total_number_of_records
    );

    // Test get_columns_by_key
    println!("\n--- Test: Retrieve Specific Columns ---");
    let columns = vec!["i8_val".to_string(), "s1".to_string(), "d_sk".to_string()];
    let response = retriever
        .get_columns_by_key(columns, &[], Some("i8_val <= 5"))
        .await?;
    println!(
        "Retrieved {} records with specific columns",
        response.total_number_of_records
    );

    println!("\n=== RecordRetriever Example Complete ===");

    Ok(())
}
