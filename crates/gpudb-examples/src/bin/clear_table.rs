//! Example: Clear (Drop) a Table
//!
//! Drops a table with `/clear/table`, which deletes the table itself, not only
//! its records. To keep the table and delete its records, use `delete_records`
//! with the `delete_all_records` option.
//!
//! ⚠️  WARNING: This operation is IRREVERSIBLE!
//!
//! Run with: cargo run -p gpudb-examples --bin clear_table
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, ClearTableRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("=== Clear Table Example ===\n");

    let table_name = "employees";

    println!("⚠ WARNING: This will drop table '{table_name}' and all its data");
    println!("This operation is IRREVERSIBLE!\n");

    println!("Waiting 3 seconds... (Press Ctrl+C to cancel)");
    for i in (1..=3).rev() {
        println!("{i}...");
        sleep(Duration::from_secs(1)).await;
    }

    println!("\nDropping table '{table_name}'...");

    let request = ClearTableRequest {
        table_name: table_name.to_string(),
        authorization: String::new(),
        options: HashMap::new(),
    };

    match kinetica.clear_table_raw(&request).await {
        Ok(response) => {
            println!("✓ Table dropped successfully!");
            println!("  Table Name: {}", response.table_name);
            if let Some(count) = response.info.get("records_affected") {
                println!("  Records Deleted: {count}");
            }
        }
        Err(e) => {
            eprintln!("✗ Error dropping table: {e}");
            return Err(e.into());
        }
    }

    println!("\nTable '{table_name}' and all its data have been removed");

    Ok(())
}
