//! Example: Check if a Table Exists in Kinetica
//!
//! This example demonstrates how to check whether a specific table exists
//! in the Kinetica database.
//!
//! Run with: cargo run -p gpudb-examples --bin has_table
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, HasTableRequest};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("=== Checking if Table Exists ===\n");

    // Table name to check
    let table_name = "employees";

    println!("Checking if table '{table_name}' exists...");

    let request = HasTableRequest {
        table_name: table_name.to_string(),
        options: HashMap::new(),
    };

    match kinetica.has_table_raw(&request).await {
        Ok(response) => {
            if response.table_exists {
                println!("✓ Table '{table_name}' EXISTS in the database");
                println!("  Table Name: {}", response.table_name);
            } else {
                println!("✗ Table '{table_name}' DOES NOT EXIST in the database");
            }
        }
        Err(e) => {
            eprintln!("✗ Error checking table: {e}");
            return Err(e.into());
        }
    }

    // Example: Check multiple tables
    println!("\nChecking multiple tables:");
    let tables_to_check = vec!["employees", "customers", "orders"];

    for table in tables_to_check {
        let request = HasTableRequest {
            table_name: table.to_string(),
            options: HashMap::new(),
        };

        match kinetica.has_table_raw(&request).await {
            Ok(response) => {
                let status = if response.table_exists { "✓ EXISTS" } else { "✗ DOES NOT EXIST" };
                println!("  Checking '{table}'... {status}");
            }
            Err(e) => {
                println!("  Checking '{table}'... ERROR: {e}");
            }
        }
    }

    Ok(())
}
