//! Example: Complete Workflow - Type, Table, and Data Management
//!
//! This example demonstrates a complete workflow that includes:
//! 1. Creating a type (schema)
//! 2. Checking if the type exists
//! 3. Creating a table based on the type
//! 4. Checking if the table exists
//! 5. Clearing the table (if needed)
//!
//! Run with: cargo run -p gpudb-examples --bin complete_workflow
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{
    Kinetica,
    CreateTypeRequest, HasTypeRequest,
    CreateTableRequest, HasTableRequest, ClearTableRequest, CreateSchemaRequest, ExecuteSqlRequest,
};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Kinetica Complete Workflow Example ===\n");

    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    // Configuration
    let schema_name = "complete_workflow";
    let type_label = "sensor_data_type";
    let table_name = format!("{schema_name}.sensor_readings");

    // ========================================
    // Step 1: Drop Schema if Exists
    // ========================================
    println!("Step 1: Dropping schema '{schema_name}' if exists...");
    let drop_schema_sql = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_schema_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;
    println!("  ✓ Schema dropped if it existed\n");

    // ========================================
    // Step 2: Create Schema
    // ========================================
    println!("Step 2: Creating schema '{schema_name}'...");
    let _ = kinetica.create_schema_raw(&CreateSchemaRequest {
        schema_name: schema_name.to_string(),
        options: HashMap::new(),
    }).await;
    println!("  ✓ Schema created\n");

    // ========================================
    // Step 3: Create Type
    // ========================================
    println!("Step 3: Creating type '{type_label}'...");

    let type_definition = r#"{
        "type": "record",
        "name": "sensor_data",
        "fields": [
            {"name": "sensor_id", "type": "int"},
            {"name": "timestamp", "type": "long"},
            {"name": "temperature", "type": "double"},
            {"name": "humidity", "type": "double"},
            {"name": "location", "type": "string"}
        ]
    }"#;

    let mut properties = HashMap::new();
    properties.insert(
        "sensor_id".to_string(),
        vec!["int".to_string(), "primary_key".to_string()],
    );

    let create_type_req = CreateTypeRequest {
        type_definition: type_definition.to_string(),
        label: type_label.to_string(),
        properties,
        options: HashMap::new(),
    };

    let type_id = match kinetica.create_type_raw(&create_type_req).await {
        Ok(response) => {
            println!("  ✓ Type created successfully");
            println!("    Type ID: {}", response.type_id);
            response.type_id
        }
        Err(e) => {
            // Type might already exist - that's okay
            println!("  ⚠ Type creation note: {e}");
            println!("    (Type may already exist - continuing...)");
            type_label.to_string()
        }
    };

    // ========================================
    // Step 4: Verify Type Exists
    // ========================================
    println!("\nStep 4: Verifying type exists...");

    let has_type_req = HasTypeRequest {
        type_id: type_id.clone(),
        options: HashMap::new(),
    };

    match kinetica.has_type_raw(&has_type_req).await {
        Ok(response) => {
            if response.type_exists {
                println!("  ✓ Type '{type_id}' confirmed to exist");
            } else {
                eprintln!("  ✗ Type verification failed!");
                return Err("Type does not exist".into());
            }
        }
        Err(e) => {
            eprintln!("  ✗ Error verifying type: {e}");
            return Err(e.into());
        }
    }

    // ========================================
    // Step 5: Create Table
    // ========================================
    println!("\nStep 5: Creating table '{table_name}'...");

    let create_table_req = CreateTableRequest {
        table_name: table_name.to_string(),
        type_id: type_id.clone(),
        options: HashMap::new(),
    };

    match kinetica.create_table_raw(&create_table_req).await {
        Ok(response) => {
            println!("  ✓ Table created successfully");
            println!("    Table: {}", response.table_name);
            println!("    Type: {}", response.type_id);
        }
        Err(e) => {
            println!("  ⚠ Table creation note: {e}");
            println!("    (Table may already exist - continuing...)");
        }
    }

    // ========================================
    // Step 6: Verify Table Exists
    // ========================================
    println!("\nStep 6: Verifying table exists...");

    let has_table_req = HasTableRequest {
        table_name: table_name.to_string(),
        options: HashMap::new(),
    };

    match kinetica.has_table_raw(&has_table_req).await {
        Ok(response) => {
            if response.table_exists {
                println!("  ✓ Table '{table_name}' confirmed to exist");
            } else {
                eprintln!("  ✗ Table verification failed!");
                return Err("Table does not exist".into());
            }
        }
        Err(e) => {
            eprintln!("  ✗ Error verifying table: {e}");
            return Err(e.into());
        }
    }

    // ========================================
    // Step 7: Clear Table (Optional)
    // ========================================
    println!("\nStep 7: Clearing table data...");

    let clear_table_req = ClearTableRequest {
        table_name: table_name.to_string(),
        authorization: String::new(),
        options: HashMap::new(),
    };

    match kinetica.clear_table_raw(&clear_table_req).await {
        Ok(response) => {
            println!("  ✓ Table cleared successfully");
            if let Some(count) = response.info.get("records_affected") {
                println!("    Records deleted: {count}");
            }
        }
        Err(e) => {
            println!("  ⚠ Clear table note: {e}");
        }
    }

    // ========================================
    // Summary
    // ========================================
    println!("\n=== Workflow Complete ===");
    println!("✓ Type '{type_label}' is ready");
    println!("✓ Table '{table_name}' is ready for data");
    println!("\nNext steps:");
    println!("  - Insert data using BulkInserter or insert_records");
    println!("  - Query data using get_records or execute_sql");
    println!("  - See README.md for more examples");

    Ok(())
}
