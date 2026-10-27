//! Example: Insert and Get Records (Binary Encoding with Native Rust Structs)
//!
//! This example demonstrates a complete workflow using native Rust structs:
//! 1. Defining a Rust struct that implements KineticaRecord trait
//! 2. Creating a type (schema) from the struct
//! 3. Creating a table
//! 4. Inserting records using insert_records API with binary (Avro) encoding
//! 5. Retrieving records using get_records API
//! 6. Parsing and displaying the results
//!
//! Run with: cargo run -p gpudb-examples --bin insert_and_get_records_bin
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaColumnProperties,
    Kinetica, KineticaRecord, TypePropertiesBuilder,
    CreateTypeRequest, CreateTableRequest, CreateSchemaRequest,
    RawInsertRecordsRequest, GetRecordsRequest, ExecuteSqlRequest,
};
use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use gpudb_api::avro::{encode_records, decode_records, parse_fast_schema};

// Define our Employee record as a native Rust struct
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
struct Employee {
    id: i32,
    name: String,
    department: String,
    salary: f64,
    hire_date: i64,  // milliseconds since epoch
}

// Implement the KineticaRecord trait to enable binary encoding
impl KineticaRecord for Employee {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("employee")
                .int_field("id")
                .string_field("name")
                .string_field("department")
                .double_field("salary")
                .long_field("hire_date")
        )
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .column("id", KineticaColumnProperties::new().primary_key())
            .build()
    }
}

impl Employee {
    fn new(id: i32, name: &str, department: &str, salary: f64, hire_date: i64) -> Self {
        Employee {
            id,
            name: name.to_string(),
            department: department.to_string(),
            salary,
            hire_date,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Insert and Get Records (Binary Encoding) Example ===\n");

    // Load configuration from environment variables
    let (urls, options) = common::connection_config();

    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    let schema_name = "insert_and_get_records_bin";
    let type_label = "employee_type_bin";
    let table_name = format!("{schema_name}.employees_bin_example");

    // ========================================
    // Step 0: Drop Schema if Exists
    // ========================================
    println!("Step 0: Dropping schema '{schema_name}' if exists...");
    let drop_schema_sql = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_schema_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;

    // ========================================
    // Step 1: Create Schema
    // ========================================
    println!("\nStep 1: Creating schema '{schema_name}'...");
    let _ = kinetica.create_schema_raw(&CreateSchemaRequest {
        schema_name: schema_name.to_string(),
        options: HashMap::new(),
    }).await;

    // ========================================
    // Step 2: Drop Table if Exists
    // ========================================
    println!("\nStep 2: Dropping table '{table_name}' if exists...");

    let drop_sql = format!("DROP TABLE IF EXISTS {table_name}");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;

    // ========================================
    // Step 3: Create Type from Rust Struct
    // ========================================
    println!("\nStep 3: Creating type from Rust struct...");

    let sample_employee = Employee::default();
    let type_definition = sample_employee.avro_schema();
    let properties = Employee::type_properties();

    let create_type_req = CreateTypeRequest {
        type_definition: type_definition.to_string(),
        label: type_label.to_string(),
        properties,
        options: HashMap::new(),
    };

    let type_id = match kinetica.create_type_raw(&create_type_req).await {
        Ok(response) => {
            println!("  ✓ Type created: {}", response.type_id);
            response.type_id
        }
        Err(e) => {
            println!("  ⚠ Type may already exist: {e}");
            type_label.to_string()
        }
    };

    // ========================================
    // Step 4: Create Table
    // ========================================
    println!("\nStep 4: Creating table '{table_name}'...");

    let create_table_req = CreateTableRequest {
        table_name: table_name.to_string(),
        type_id: type_id.clone(),
        options: HashMap::new(),
    };

    match kinetica.create_table_raw(&create_table_req).await {
        Ok(response) => {
            println!("  ✓ Table created: {}", response.table_name);
        }
        Err(e) => {
            println!("  ⚠ Table may already exist: {e}");
        }
    }

    // ========================================
    // Step 5: Insert Records (Binary Encoding)
    // ========================================
    println!("\nStep 5: Inserting records with binary (Avro) encoding...");

    // Create sample employee records as native Rust structs
    let employees = vec![
        Employee::new(1, "Alice Johnson", "Engineering", 95000.0, 1609459200000),
        Employee::new(2, "Bob Smith", "Sales", 75000.0, 1617235200000),
        Employee::new(3, "Carol Williams", "Engineering", 105000.0, 1625097600000),
        Employee::new(4, "David Brown", "Marketing", 68000.0, 1633046400000),
        Employee::new(5, "Eve Davis", "Engineering", 98000.0, 1640995200000),
    ];

    // Parse the Avro schema for encoding
    let schema_str = sample_employee.avro_schema();
    let schema = parse_fast_schema(schema_str)?;

    // Encode each employee to binary Avro format
    println!("  Encoding {} records to Avro binary format...", employees.len());
    let binary_records = encode_records(&schema, &employees)?;

    println!("  ✓ Encoded {} records to binary", binary_records.len());

    // Prepare insert request with binary encoding
    let mut insert_options = HashMap::new();
    insert_options.insert("update_on_existing_pk".to_string(), "true".to_string());

    let insert_req = RawInsertRecordsRequest {
        table_name: table_name.to_string(),
        list: binary_records.into_iter().map(gpudb_api::ByteBuf::from).collect(),  // Binary-encoded Avro records
        list_str: Vec::new(),  // Empty for binary encoding
        list_encoding: "binary".to_string(),
        options: insert_options,
    };

    match kinetica.insert_records_raw(&insert_req).await {
        Ok(response) => {
            println!("  ✓ Records inserted successfully!");
            println!("    Inserted: {}", response.count_inserted);
            println!("    Updated: {}", response.count_updated);
        }
        Err(e) => {
            eprintln!("  ✗ Error inserting records: {e}");
            return Err(e.into());
        }
    }

    // ========================================
    // Step 6: Get Records
    // ========================================
    println!("\nStep 6: Retrieving records...");

    // Example 6a: Get all records (using JSON encoding for easier parsing in response)
    println!("\n  6a. Getting all records:");
    let get_req = GetRecordsRequest {
        table_name: table_name.to_string(),
        offset: 0,
        limit: -9999,  // Get all records
        encoding: "json".to_string(),  // Use JSON for response to make parsing easier
        options: HashMap::new(),
    };

    match kinetica.get_records_raw(&get_req).await {
        Ok(response) => {
            println!("    ✓ Retrieved {} records", response.records_json.len());
            println!("    Total records in table: {}", response.total_number_of_records);

            for (i, record_json) in response.records_json.iter().enumerate() {
                let record: serde_json::Value = serde_json::from_str(record_json)?;
                println!("    Record {}: id={}, name={}, department={}, salary={}",
                    i + 1,
                    record["id"],
                    record["name"].as_str().unwrap_or("N/A"),
                    record["department"].as_str().unwrap_or("N/A"),
                    record["salary"]
                );
            }
        }
        Err(e) => {
            eprintln!("    ✗ Error getting records: {e}");
            return Err(e.into());
        }
    }

    // Example 6b: Get records with filtering
    println!("\n  6b. Getting records filtered by department='Engineering':");
    let mut filter_options = HashMap::new();
    filter_options.insert("expression".to_string(), "department = 'Engineering'".to_string());

    let get_filtered_req = GetRecordsRequest {
        table_name: table_name.to_string(),
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        options: filter_options,
    };

    match kinetica.get_records_raw(&get_filtered_req).await {
        Ok(response) => {
            println!("    ✓ Retrieved {} engineering records", response.records_json.len());

            for (i, record_json) in response.records_json.iter().enumerate() {
                let record: serde_json::Value = serde_json::from_str(record_json)?;
                println!("    Record {}: id={}, name={}, salary={}",
                    i + 1,
                    record["id"],
                    record["name"].as_str().unwrap_or("N/A"),
                    record["salary"]
                );
            }
        }
        Err(e) => {
            eprintln!("    ✗ Error getting filtered records: {e}");
            return Err(e.into());
        }
    }

    // Example 6c: Get records with sorting
    println!("\n  6c. Getting records sorted by salary (descending):");
    let mut sort_options = HashMap::new();
    sort_options.insert("sort_by".to_string(), "salary".to_string());
    sort_options.insert("sort_order".to_string(), "descending".to_string());

    let get_sorted_req = GetRecordsRequest {
        table_name: table_name.to_string(),
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        options: sort_options,
    };

    match kinetica.get_records_raw(&get_sorted_req).await {
        Ok(response) => {
            println!("    ✓ Retrieved {} records (sorted)", response.records_json.len());

            for (i, record_json) in response.records_json.iter().enumerate() {
                let record: serde_json::Value = serde_json::from_str(record_json)?;
                println!("    #{}: {} - ${:.2}",
                    i + 1,
                    record["name"].as_str().unwrap_or("N/A"),
                    record["salary"].as_f64().unwrap_or(0.0)
                );
            }
        }
        Err(e) => {
            eprintln!("    ✗ Error getting sorted records: {e}");
            return Err(e.into());
        }
    }

    // Example 6d: Get records with pagination
    println!("\n  6d. Getting records with pagination (2 records per page):");
    let page_size = 2i64;
    let mut offset = 0i64;
    let mut page_num = 1;

    loop {
        let get_page_req = GetRecordsRequest {
            table_name: table_name.to_string(),
            offset,
            limit: page_size,
            encoding: "json".to_string(),
            options: HashMap::new(),
        };

        match kinetica.get_records_raw(&get_page_req).await {
            Ok(response) => {
                if response.records_json.is_empty() {
                    break;
                }

                println!("    Page {} ({} records):", page_num, response.records_json.len());

                for record_json in response.records_json.iter() {
                    let record: serde_json::Value = serde_json::from_str(record_json)?;
                    println!("      - {} ({})",
                        record["name"].as_str().unwrap_or("N/A"),
                        record["department"].as_str().unwrap_or("N/A")
                    );
                }

                if !response.has_more_records {
                    println!("    (No more records)");
                    break;
                }

                offset += page_size;
                page_num += 1;
            }
            Err(e) => {
                eprintln!("    ✗ Error getting page: {e}");
                return Err(e.into());
            }
        }
    }

    // Example 6e: Get records with binary encoding (decode back to structs)
    println!("\n  6e. Getting records with binary encoding and decoding to Rust structs:");

    let get_binary_req = GetRecordsRequest {
        table_name: table_name.to_string(),
        offset: 0,
        limit: 3,  // Just get first 3 for demonstration
        encoding: "binary".to_string(),  // Request binary Avro encoding
        options: HashMap::new(),
    };

    match kinetica.get_records_raw(&get_binary_req).await {
        Ok(response) => {
            println!("    ✓ Retrieved {} binary records", response.records_binary.len());
            println!("    Decoding Avro binary data back to Employee structs...\n");

            // Use the schema returned by Kinetica (this is the actual schema used for the data)
            let kinetica_schema = parse_fast_schema(&response.type_schema)?;

            // Decode all records using serde_avro_fast
            let employees_decoded: Vec<Employee> = decode_records(&kinetica_schema, &response.records_binary)?;

            for (i, employee) in employees_decoded.iter().enumerate() {
                println!("    Employee {}: {:?}", i + 1, employee);
            }
        }
        Err(e) => {
            eprintln!("    ✗ Error getting binary records: {e}");
            return Err(e.into());
        }
    }

    // ========================================
    // Summary
    // ========================================
    println!("\n=== Example Complete ===");
    println!("✓ Successfully demonstrated:");
    println!("  - Defining native Rust struct (Employee)");
    println!("  - Implementing KineticaRecord trait");
    println!("  - Creating type and table from struct");
    println!("  - Encoding Rust structs to Avro binary format");
    println!("  - Inserting records via insert_records API (binary encoding)");
    println!("  - Retrieving all records");
    println!("  - Filtering records by expression");
    println!("  - Sorting records by column");
    println!("  - Paginating through results");
    println!("  - Decoding binary Avro back to Rust structs");

    Ok(())
}
