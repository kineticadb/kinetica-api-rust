//! Example: Create a Type in Kinetica
//!
//! This example demonstrates how to create a custom type (schema) in Kinetica.
//! A type defines the structure of data that will be stored in tables.
//!
//! Run with: cargo run -p gpudb-examples --bin create_type
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, CreateTypeRequest};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("Creating a new type in Kinetica...");

    // Define the type schema (Avro-like JSON format)
    let type_definition = r#"{
        "type": "record",
        "name": "employee",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "department", "type": "string"},
            {"name": "salary", "type": "double"},
            {"name": "hire_date", "type": "long"}
        ]
    }"#;

    // Define properties for the fields
    // This marks 'id' as the primary key
    let mut properties = HashMap::new();
    properties.insert(
        "id".to_string(),
        vec!["int".to_string(), "primary_key".to_string()],
    );

    // Create the type
    let request = CreateTypeRequest {
        type_definition: type_definition.to_string(),
        label: "employee_type".to_string(),
        properties,
        options: HashMap::new(),
    };

    match kinetica.create_type_raw(&request).await {
        Ok(response) => {
            println!("✓ Type created successfully!");
            println!("  Type ID: {}", response.type_id);
            println!("  Type Schema: {}", type_definition.replace("\n", "").replace(" ", ""));
        }
        Err(e) => {
            eprintln!("✗ Error creating type: {e}");
            return Err(e.into());
        }
    }

    println!("\nType 'employee_type' is now registered and ready to use!");

    Ok(())
}
