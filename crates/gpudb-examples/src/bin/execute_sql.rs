//! Example: Execute SQL - Query and Insert Operations
//!
//! This comprehensive example demonstrates the execute_sql endpoint for:
//! 1. Creating tables with SQL DDL
//! 2. Inserting data using parameterized SQL (single and batch inserts)
//! 3. Retrieving query results with various options
//! 4. Handling paging for large result sets
//! 5. Using JSON-encoded responses
//!
//! Run with: cargo run -p gpudb-examples --bin execute_sql
//!
//! Environment Variables:
//!   KINETICA_URL      - Server URL (default: http://localhost:9191)
//!   KINETICA_USER     - Username (default: admin)
//!   KINETICA_PASSWORD - Password (default: password)

use gpudb_examples::common;

use gpudb_api::{Kinetica, ExecuteSqlRequest};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Kinetica Execute SQL Example ===\n");

    // Load configuration from environment variables
    let (urls, options) = common::connection_config();
    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    let schema_name = "execute_sql_demo";
    let table_name = &format!("{schema_name}.employee_data");

    // ========================================
    // Setup: Drop and recreate schema for clean state
    // ========================================
    println!("Setting up schema '{schema_name}'...");

    // Drop schema if exists (CASCADE drops all tables)
    let drop_schema_sql = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    let _ = kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: drop_schema_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await;

    // Create fresh schema
    let create_schema_sql = format!("CREATE SCHEMA {schema_name}");
    kinetica.execute_sql_raw(&ExecuteSqlRequest {
        statement: create_schema_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        ..Default::default()
    }).await?;
    println!("  ✓ Schema '{schema_name}' created\n");

    // ========================================
    // Part 1: CREATE TABLE using DDL
    // ========================================
    println!("Part 1: Creating table with SQL DDL...");

    let create_table_sql = format!(
        "CREATE OR REPLACE TABLE {table_name} (
            employee_id INT NOT NULL,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            department VARCHAR(50),
            salary DOUBLE,
            hire_date LONG,
            PRIMARY KEY (employee_id)
        )"
    );

    let create_req = ExecuteSqlRequest {
        statement: create_table_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: HashMap::new(),
    };

    match kinetica.execute_sql_raw(&create_req).await {
        Ok(response) => {
            println!("  ✓ Table created successfully");
            println!("    Count affected: {}", response.count_affected);
        }
        Err(e) => {
            println!("  ⚠ Table creation note: {e}");
        }
    }

    // ========================================
    // Part 2: INSERT - Single Row with Parameters
    // ========================================
    println!("\nPart 2: Inserting single row with parameterized SQL...");

    let insert_single_sql = format!(
        "INSERT INTO {table_name} (employee_id, first_name, last_name, department, salary, hire_date)
         VALUES (?, ?, ?, ?, ?, ?)"
    );

    let mut insert_options = HashMap::new();
    // Using JSON array for query parameters (easier than binary encoding)
    insert_options.insert(
        "query_parameters".to_string(),
        r#"[[1, "John", "Doe", "Engineering", 95000.0, 1609459200000]]"#.to_string(),
    );

    let insert_single_req = ExecuteSqlRequest {
        statement: insert_single_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: insert_options,
    };

    match kinetica.execute_sql_raw(&insert_single_req).await {
        Ok(response) => {
            println!("  ✓ Single row inserted successfully");
            println!("    Rows inserted: {}", response.count_affected);
        }
        Err(e) => {
            eprintln!("  ✗ Insert failed: {e}");
        }
    }

    // ========================================
    // Part 3: INSERT - Multiple Rows (Batch Insert)
    // ========================================
    println!("\nPart 3: Inserting multiple rows (batch insert)...");

    let insert_batch_sql = format!(
        "INSERT INTO {table_name} (employee_id, first_name, last_name, department, salary, hire_date)
         VALUES (?, ?, ?, ?, ?, ?)"
    );

    let mut batch_options = HashMap::new();
    // Multiple parameter arrays for batch insert
    batch_options.insert(
        "query_parameters".to_string(),
        r#"[
            [2, "Jane", "Smith", "Sales", 85000.0, 1612137600000],
            [3, "Mike", "Johnson", "Engineering", 98000.0, 1614556800000],
            [4, "Sarah", "Williams", "Marketing", 78000.0, 1617235200000],
            [5, "Tom", "Brown", "Engineering", 92000.0, 1619827200000],
            [6, "Emily", "Davis", "Sales", 88000.0, 1622505600000],
            [7, "David", "Miller", "HR", 72000.0, 1625097600000],
            [8, "Lisa", "Wilson", "Engineering", 105000.0, 1627776000000],
            [9, "James", "Moore", "Marketing", 81000.0, 1630454400000],
            [10, "Anna", "Taylor", "Sales", 90000.0, 1633046400000]
        ]"#.to_string(),
    );

    let insert_batch_req = ExecuteSqlRequest {
        statement: insert_batch_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: batch_options,
    };

    match kinetica.execute_sql_raw(&insert_batch_req).await {
        Ok(response) => {
            println!("  ✓ Batch insert completed successfully");
            println!("    Rows inserted: {}", response.count_affected);
        }
        Err(e) => {
            eprintln!("  ✗ Batch insert failed: {e}");
        }
    }

    // ========================================
    // Part 4: INSERT with UPDATE on Primary Key Collision
    // ========================================
    println!("\nPart 4: Insert with update on primary key collision...");

    let insert_update_sql = format!(
        "INSERT INTO {table_name} (employee_id, first_name, last_name, department, salary, hire_date)
         VALUES (?, ?, ?, ?, ?, ?)"
    );

    let mut update_options = HashMap::new();
    update_options.insert(
        "query_parameters".to_string(),
        r#"[[1, "John", "Doe", "Management", 105000.0, 1609459200000]]"#.to_string(),
    );
    // Update existing record if primary key collision occurs
    update_options.insert("update_on_existing_pk".to_string(), "true".to_string());

    let insert_update_req = ExecuteSqlRequest {
        statement: insert_update_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: update_options,
    };

    match kinetica.execute_sql_raw(&insert_update_req).await {
        Ok(response) => {
            println!("  ✓ Insert/Update completed successfully");
            println!("    Rows affected: {}", response.count_affected);
        }
        Err(e) => {
            eprintln!("  ✗ Insert/Update failed: {e}");
        }
    }

    // ========================================
    // Part 5: QUERY - Simple SELECT
    // ========================================
    println!("\nPart 5: Retrieving all records...");

    let select_all_sql = format!("SELECT * FROM {table_name} ORDER BY employee_id");

    let select_req = ExecuteSqlRequest {
        statement: select_all_sql,
        offset: 0,
        limit: -9999,  // Get all records (up to server max)
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: HashMap::new(),
    };

    match kinetica.execute_sql_raw(&select_req).await {
        Ok(response) => {
            println!("  ✓ Query executed successfully");
            println!("    Total records: {}", response.total_number_of_records);
            println!("    Has more records: {}", response.has_more_records);

            // Parse and display JSON response
            if !response.json_encoded_response.is_empty() {
                println!("\n  Records (JSON):");
                if let Ok(records) = serde_json::from_str::<serde_json::Value>(&response.json_encoded_response) {
                    println!("{}", serde_json::to_string_pretty(&records)?);
                }
            }
        }
        Err(e) => {
            eprintln!("  ✗ Query failed: {e}");
        }
    }

    // ========================================
    // Part 6: QUERY - SELECT with WHERE clause
    // ========================================
    println!("\nPart 6: Querying with WHERE clause (Engineering department)...");

    let select_where_sql = format!(
        "SELECT employee_id, first_name, last_name, salary
         FROM {table_name}
         WHERE department = 'Engineering'
         ORDER BY salary DESC"
    );

    let select_where_req = ExecuteSqlRequest {
        statement: select_where_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: HashMap::new(),
    };

    match kinetica.execute_sql_raw(&select_where_req).await {
        Ok(response) => {
            println!("  ✓ Filtered query executed successfully");
            println!("    Matching records: {}", response.total_number_of_records);

            if !response.json_encoded_response.is_empty() {
                println!("\n  Engineering Employees:");
                if let Ok(records) = serde_json::from_str::<serde_json::Value>(&response.json_encoded_response) {
                    println!("{}", serde_json::to_string_pretty(&records)?);
                }
            }
        }
        Err(e) => {
            eprintln!("  ✗ Filtered query failed: {e}");
        }
    }

    // ========================================
    // Part 7: QUERY - Aggregate Functions
    // ========================================
    println!("\nPart 7: Using aggregate functions...");

    let aggregate_sql = format!(
        "SELECT department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
         FROM {table_name}
         GROUP BY department
         ORDER BY avg_salary DESC"
    );

    let aggregate_req = ExecuteSqlRequest {
        statement: aggregate_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: HashMap::new(),
    };

    match kinetica.execute_sql_raw(&aggregate_req).await {
        Ok(response) => {
            println!("  ✓ Aggregate query executed successfully");
            println!("    Result groups: {}", response.total_number_of_records);

            if !response.json_encoded_response.is_empty() {
                println!("\n  Department Statistics:");
                if let Ok(records) = serde_json::from_str::<serde_json::Value>(&response.json_encoded_response) {
                    println!("{}", serde_json::to_string_pretty(&records)?);
                }
            }
        }
        Err(e) => {
            eprintln!("  ✗ Aggregate query failed: {e}");
        }
    }

    // ========================================
    // Part 8: QUERY - Paging with Limit and Offset
    // ========================================
    println!("\nPart 8: Demonstrating paging with limit and offset...");

    let page_size = 3;
    let mut current_offset = 0;
    let mut page_number = 1;

    loop {
        println!("\n  Page {page_number}:");

        let paging_sql = format!(
            "SELECT employee_id, first_name, last_name, department, salary
             FROM {table_name}
             ORDER BY employee_id"
        );

        let paging_req = ExecuteSqlRequest {
            statement: paging_sql,
            offset: current_offset,
            limit: page_size,
            encoding: "json".to_string(),
            request_schema_str: String::new(),
            data: vec![],
            options: HashMap::new(),
        };

        match kinetica.execute_sql_raw(&paging_req).await {
            Ok(response) => {
                if !response.json_encoded_response.is_empty() {
                    if let Ok(records) = serde_json::from_str::<serde_json::Value>(&response.json_encoded_response) {
                        println!("{}", serde_json::to_string_pretty(&records)?);
                    }
                }

                println!("  Records on this page: {}", response.count_affected);
                println!("  Total records available: {}", response.total_number_of_records);
                println!("  Has more records: {}", response.has_more_records);

                if !response.has_more_records {
                    println!("\n  ✓ Reached end of results");
                    break;
                }

                current_offset += page_size;
                page_number += 1;

                // Limit demonstration to 3 pages
                if page_number > 3 {
                    println!("\n  (Stopping after 3 pages for demonstration)");
                    break;
                }
            }
            Err(e) => {
                eprintln!("  ✗ Paging query failed: {e}");
                break;
            }
        }
    }

    // ========================================
    // Part 9: QUERY - Using Paging Table for Large Results
    // ========================================
    println!("\nPart 9: Using paging table for efficient result retrieval...");

    let paging_table_name = format!("{table_name}_paging_temp");

    let mut paging_table_options = HashMap::new();
    paging_table_options.insert("paging_table".to_string(), paging_table_name.clone());
    paging_table_options.insert("paging_table_ttl".to_string(), "300".to_string()); // 5 minutes TTL

    let paging_table_sql = format!(
        "SELECT * FROM {table_name} ORDER BY employee_id"
    );

    let paging_table_req = ExecuteSqlRequest {
        statement: paging_table_sql,
        offset: 0,
        limit: 5,  // First 5 records
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: paging_table_options,
    };

    match kinetica.execute_sql_raw(&paging_table_req).await {
        Ok(response) => {
            println!("  ✓ First page retrieved with paging table");
            println!("    Records returned: {}", response.count_affected);
            println!("    Total available: {}", response.total_number_of_records);
            println!("    Has more: {}", response.has_more_records);

            if !response.paging_table.is_empty() {
                println!("    Paging table: {}", response.paging_table);
                println!("    (Query results cached for efficient subsequent retrieval)");
            }

            // Note: In a real application, you would:
            // 1. Store the paging_table name
            // 2. Make subsequent requests using the same paging_table
            // 3. Clear the paging table when done
        }
        Err(e) => {
            eprintln!("  ✗ Paging table query failed: {e}");
        }
    }

    // ========================================
    // Part 10: UPDATE Statement
    // ========================================
    println!("\nPart 10: Updating records with SQL...");

    let update_sql = format!(
        "UPDATE {table_name} SET salary = salary * 1.10 WHERE department = 'Engineering'"
    );

    let update_req = ExecuteSqlRequest {
        statement: update_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: HashMap::new(),
    };

    match kinetica.execute_sql_raw(&update_req).await {
        Ok(response) => {
            println!("  ✓ Update completed successfully");
            println!("    Rows updated: {}", response.count_affected);
        }
        Err(e) => {
            eprintln!("  ✗ Update failed: {e}");
        }
    }

    // ========================================
    // Part 11: DELETE Statement
    // ========================================
    println!("\nPart 11: Deleting records with SQL...");

    let delete_sql = format!(
        "DELETE FROM {table_name} WHERE employee_id > 8"
    );

    let delete_req = ExecuteSqlRequest {
        statement: delete_sql,
        offset: 0,
        limit: -9999,
        encoding: "json".to_string(),
        request_schema_str: String::new(),
        data: vec![],
        options: HashMap::new(),
    };

    match kinetica.execute_sql_raw(&delete_req).await {
        Ok(response) => {
            println!("  ✓ Delete completed successfully");
            println!("    Rows deleted: {}", response.count_affected);
        }
        Err(e) => {
            eprintln!("  ✗ Delete failed: {e}");
        }
    }

    // ========================================
    // Summary
    // ========================================
    println!("\n=== Execute SQL Example Complete ===");
    println!("\nKey Capabilities Demonstrated:");
    println!("  ✓ DDL: CREATE TABLE");
    println!("  ✓ DML: INSERT (single and batch)");
    println!("  ✓ DML: INSERT with UPDATE on PK collision");
    println!("  ✓ DML: UPDATE and DELETE");
    println!("  ✓ Query: SELECT with WHERE, ORDER BY");
    println!("  ✓ Query: Aggregate functions (COUNT, AVG, MIN, MAX)");
    println!("  ✓ Paging: Using offset and limit");
    println!("  ✓ Paging: Using paging tables for large results");
    println!("  ✓ JSON-encoded response handling");
    println!("\nImportant Options:");
    println!("  - query_parameters: Pass data as JSON instead of binary");
    println!("  - update_on_existing_pk: Update on primary key collision");
    println!("  - paging_table: Cache results for efficient paging");
    println!("  - paging_table_ttl: Auto-clear paging table after timeout");
    println!("  - encoding: 'json' or 'binary' for response format");

    Ok(())
}
