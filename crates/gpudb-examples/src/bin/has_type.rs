//! Example: Check if a Type Exists in Kinetica
//!
//! This example demonstrates how to check whether a specific type exists
//! in the Kinetica database.
//!
//! Run with: cargo run -p gpudb-examples --bin has_type
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, HasTypeRequest};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("=== Checking if Type Exists ===\n");

    // Type ID to check
    let type_id = "employee_type";

    println!("Checking if type '{type_id}' exists...");

    let request = HasTypeRequest {
        type_id: type_id.to_string(),
        options: HashMap::new(),
    };

    match kinetica.has_type_raw(&request).await {
        Ok(response) => {
            if response.type_exists {
                println!("✓ Type '{type_id}' EXISTS in the database");
                println!("  Type ID: {}", response.type_id);
            } else {
                println!("✗ Type '{type_id}' DOES NOT EXIST in the database");
            }
        }
        Err(e) => {
            eprintln!("✗ Error checking type: {e}");
            return Err(e.into());
        }
    }

    // Example: Check multiple types
    println!("\nChecking multiple types:");
    let types_to_check = vec!["employee_type", "customer_type", "order_type"];

    for type_name in types_to_check {
        let request = HasTypeRequest {
            type_id: type_name.to_string(),
            options: HashMap::new(),
        };

        match kinetica.has_type_raw(&request).await {
            Ok(response) => {
                let status = if response.type_exists { "✓ EXISTS" } else { "✗ DOES NOT EXIST" };
                println!("  Checking '{type_name}'... {status}");
            }
            Err(e) => {
                println!("  Checking '{type_name}'... ERROR: {e}");
            }
        }
    }

    Ok(())
}
