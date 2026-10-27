//! Example: Create a Table in Kinetica
//!
//! This example demonstrates how to create a table based on a previously
//! created type. You must create a type first (see create_type.rs example).
//!
//! Run with: cargo run -p gpudb-examples --bin create_table
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, CreateTableRequest, CreateSchemaRequest, ExecuteSqlRequest};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    // Schema, table and type information
    let schema_name = "create_table";
    let table_name = format!("{schema_name}.employees");
    let type_id = "employee_type"; // This type should exist (create it first)

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
    println!("✓ Schema dropped if it existed\n");

    // Create schema
    println!("Creating schema '{schema_name}'...");
    let _ = kinetica.create_schema_raw(&CreateSchemaRequest {
        schema_name: schema_name.to_string(),
        options: HashMap::new(),
    }).await;
    println!("✓ Schema created\n");

    println!("Creating table '{table_name}' with type '{type_id}'...");

    // Optional: Configure table options
    let options = HashMap::new();

    // Uncomment to enable various table features:
    // let mut options = HashMap::new();
    // options.insert("collection_name".to_string(), "my_collection".to_string());
    // options.insert("is_replicated".to_string(), "true".to_string());
    // options.insert("ttl".to_string(), "60".to_string()); // Time-to-live in minutes

    let request = CreateTableRequest {
        table_name: table_name.to_string(),
        type_id: type_id.to_string(),
        options,
    };

    match kinetica.create_table_raw(&request).await {
        Ok(response) => {
            println!("✓ Table created successfully!");
            println!("  Table Name: {}", response.table_name);
            println!("  Type ID: {}", response.type_id);
            println!("  Is Collection: {}", response.is_collection);
        }
        Err(e) => {
            eprintln!("✗ Error creating table: {e}");
            eprintln!("\nTip: Make sure the type '{type_id}' exists first.");
            eprintln!("     Run: cargo run -p gpudb-examples --bin create_type");
            return Err(e.into());
        }
    }

    println!("\nTable '{table_name}' is now ready for data insertion!");

    Ok(())
}
