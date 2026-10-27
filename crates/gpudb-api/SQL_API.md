# SQL Client API

This document describes the SQL client API for Kinetica's Rust client. Statements are executed through Kinetica's HTTP `/execute/sql` endpoint over the same `Kinetica` connection as the rest of the API, so they share its authentication, TLS and HA failover. For the PostgreSQL wire protocol, see the `sqlx_support` module (feature `sqlx`).

## Overview

| Component | Description |
|-----------|-------------|
| `SqlClient` | Executes SQL and pages through large result sets |
| `SqlResult` / `Row` | Wrappers around query results with convenient parsing methods |
| `SqlHints` | Kinetica-specific execution options and inline hints |
| `BatchingSqlClient` | Routes `INSERT` statements through `BulkInserter` for high-throughput loading |

## Quick Start

```rust
use gpudb_api::{Kinetica, KineticaOptions, SqlClient};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Kinetica client
    let mut options = KineticaOptions::new();
    options.set_username("admin").set_password("password");

    let kinetica = Arc::new(
        Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
    );

    // Create SqlClient wrapper
    let client = SqlClient::new(kinetica);

    // Execute a query
    let mut result = client.execute("SELECT * FROM my_table LIMIT 10").await?;

    // Process results
    for row in result.rows()? {
        let id: i64 = row.get("column_1")?;
        let name: String = row.get("column_2")?;
        println!("id={}, name={}", id, name);
    }

    Ok(())
}
```

## SqlClient

The `SqlClient` wraps a `Kinetica` instance and provides convenient methods for query execution.

### Basic Execution

```rust
use gpudb_api::SqlClient;

let client = SqlClient::new(kinetica);

// Execute raw SQL
let result = client.execute("SELECT COUNT(*) FROM users").await?;

// Execute with pagination
let result = client.execute_paged("SELECT * FROM users", 10, 100).await?;
// offset=10, limit=100

// Execute with custom options
let mut options = HashMap::new();
options.insert("parallel_execution".to_string(), "true".to_string());
let result = client.execute_with_options(
    "SELECT * FROM large_table",
    0,
    1000,
    Some(options)
).await?;
```

### Batching INSERT Statements

`BatchingSqlClient` accepts plain SQL but parses `INSERT ... VALUES` statements, buffers the rows per table, and flushes them through shard-aware `BulkInserter<GenericRecord>`. Other statements are executed immediately.

```rust
use gpudb_api::{BatchingSqlClient, SqlHints};

let hints = SqlHints::new().update_on_existing_pk(true); // upsert
let mut client = BatchingSqlClient::with_hints(kinetica, 10000, 8, hints);

for i in 0..1_000_000 {
    client.execute(&format!("INSERT INTO my_table (id, name) VALUES ({i}, 'user_{i}')")).await?;
}

let stats = client.close().await?;
println!("{} inserted, {} updated at {:.0} rec/sec",
    stats.records_inserted, stats.records_updated, stats.records_per_second);
```

## SqlResult

The `SqlResult` type wraps query responses and provides convenient data extraction methods.

### Metadata

```rust
let result = client.execute("SELECT * FROM users").await?;

// Total records (before LIMIT)
let total = result.total_records();

// Rows affected (for INSERT/UPDATE/DELETE)
let affected = result.count_affected();

// Check for more records
if result.has_more() {
    println!("More records available");
}

// Get paging table name (if any)
if let Some(paging_table) = result.paging_table() {
    println!("Paging table: {}", paging_table);
}

// Access raw response
let info = result.info();
let json = result.json_response();
```

### Extracting Data

```rust
let mut result = client.execute("SELECT id, name, score FROM users").await?;

// Get single scalar value (first row, first column)
let count: i64 = result.scalar()?;

// Get a specific value
let name: String = result.get(0, "column_2")?;  // row 0, column_2

// Extract entire column as Vec
let ids: Vec<i64> = result.column("column_1")?;
let names: Vec<String> = result.column("column_2")?;

// Get row count
let num_rows = result.row_count()?;

// Get column names
let columns = result.column_names()?;
```

### Row Iteration

```rust
let mut result = client.execute("SELECT id, name FROM users").await?;

for row in result.rows()? {
    let id: i64 = row.get("column_1")?;
    let name: String = row.get("column_2")?;
    println!("User {}: {}", id, name);
}
```

### Deserialization

```rust
use serde::Deserialize;

#[derive(Deserialize)]
struct User {
    column_1: i64,   // id
    column_2: String, // name
}

let mut result = client.execute("SELECT id, name FROM users").await?;
let users: Vec<User> = result.deserialize()?;

for user in users {
    println!("User {}: {}", user.column_1, user.column_2);
}
```

## Pagination

The SQL API provides comprehensive pagination support for handling large result sets.

### Manual Pagination

```rust
let client = SqlClient::new(kinetica);

// Get first page
let result = client.execute_paged("SELECT * FROM large_table", 0, 1000).await?;

// If there's a paging table, use it for subsequent pages
if let Some(paging_table) = result.paging_table() {
    let page2 = client.fetch_next_page(&paging_table, 1000, 1000).await?;
    let page3 = client.fetch_next_page(&paging_table, 2000, 1000).await?;
}
```

### Automatic Pagination (Fetch All)

```rust
let client = SqlClient::new(kinetica);

// Fetch all records, 10000 at a time
let mut result = client.execute_all(
    "SELECT * FROM large_table",
    10000  // page size
).await?;

// All records are now in memory
println!("Total records: {}", result.row_count()?);
```

### Page-by-Page Processing (Memory Efficient)

```rust
let client = SqlClient::new(kinetica);

let stats = client.execute_paginated(
    "SELECT * FROM large_table",
    10000,  // page size
    |mut page, page_num| {
        println!("Processing page {}", page_num);

        for row in page.rows()? {
            let id: i64 = row.get("column_1")?;
            // Process each row...
        }

        Ok(true)  // Return false to stop early
    }
).await?;

println!("Processed {} pages, {} total rows",
    stats.pages_fetched, stats.rows_fetched);
```

## Bulk Insert

For inserting large numbers of records efficiently, use `BulkInserter<GenericRecord>` directly (or `BatchingSqlClient` above when the input is SQL). It uses:
- **Shard-aware multi-head ingest** - Routes records directly to appropriate worker nodes
- **Parallel batch processing** - Multiple workers encode and send batches concurrently
- **Avro binary encoding** - Compact wire format for maximum throughput

```rust
use gpudb_api::{BulkInserter, BulkInserterOptions, GenericRecord, Type};
use std::sync::Arc;

// Fetch table type
let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);

// Optional: upsert mode
let mut options = BulkInserterOptions::new();
options.set_update_on_existing_pk(true);

let inserter = BulkInserter::<GenericRecord>::with_schema(
    kinetica.clone(),
    "my_table".to_string(),
    10000,          // batch size
    8,              // num workers
    Some(options),
    record_type.schema_string(),
).await?;

for i in 0..100_000 {
    let mut record = record_type.new_record();
    record.set_by_name("id", i)?;
    record.set_by_name("name", format!("user_{}", i))?;
    inserter.insert(Arc::new(record))?;
}

inserter.close().await?;
println!("Inserted {}, updated {}", inserter.count_inserted(), inserter.count_updated());
```

## GenericRecord

`GenericRecord` allows creating records at runtime based on table schema.

### Creating Records

```rust
use gpudb_api::{GenericRecord, Type, RecordValue};
use std::sync::Arc;

// Fetch type from database
let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);

// Create record and set values with set_by_name
let mut record = record_type.new_record();
record.set_by_name("id", 42)?;
record.set_by_name("name", "Alice")?;
record.set_by_name("score", 95.5)?;
record.set_null_by_name("optional_field")?;

// Or use put() with column index
record.set(0, 42)?;  // First column
record.set(1, "Bob")?;

// Get values back
let id: Option<&RecordValue> = record.get_by_name("id");
let name: Option<&RecordValue> = record.get(1);
```

### Supported Value Types

```rust
use gpudb_api::RecordValue;

// All supported types via RecordValue
RecordValue::Null                         // NULL
RecordValue::Int(42)                      // 32-bit integer
RecordValue::Long(9999999999)             // 64-bit integer
RecordValue::Float(3.14f32)               // 32-bit float
RecordValue::Double(3.14159265359)        // 64-bit double
RecordValue::String("hello".to_string())  // String
RecordValue::Bytes(vec![1, 2, 3])         // Raw bytes

// Automatic conversion via set_by_name()
record.set_by_name("int_col", 42i32)?;
record.set_by_name("long_col", 9999i64)?;
record.set_by_name("str_col", "hello")?;
record.set_by_name("float_col", 3.14f32)?;
record.set_by_name("double_col", 3.14159f64)?;
```

### Type (Table Schema)

```rust
use gpudb_api::Type;

// Fetch type from database
let record_type = Type::from_table(&kinetica, "my_schema.my_table").await?;

// Inspect schema
println!("Columns: {}", record_type.column_count());
println!("Avro schema: {}", record_type.schema_string());

// Check column existence
if let Some(col) = record_type.get_column_by_name("id") {
    println!("Type: {:?}", col.column_type());
    println!("Base type: {:?}", col.base_type());
    println!("Is nullable: {}", col.is_nullable());
}

// Get column by index
let first_col = record_type.get_column(0);

// Get primary key column indices
let pk_indices = record_type.primary_key_indices();
```

### Building a Type Programmatically

```rust
use gpudb_api::{TypeBuilder, ColumnType};

// Build a type without fetching from database
let record_type = TypeBuilder::new("MyRecord")
    .add_column("id", ColumnType::Int, &["primary_key", "shard_key"])
    .add_column("name", ColumnType::String, &["char64"])
    .add_column("score", ColumnType::Double, &[])
    .add_column("created_at", ColumnType::Long, &["timestamp"])
    .build()?;

// Create instances from the built type
let mut record = record_type.new_record();
record.set_by_name("id", 1)?;
record.set_by_name("name", "Alice")?;
```

## Error Handling

```rust
use gpudb_api::{SqlError, SqlOpResult};

async fn count_users(client: &SqlClient) -> SqlOpResult<i64> {
    client.execute("SELECT COUNT(*) FROM users").await?.scalar()
}

match count_users(&client).await {
    Ok(count) => println!("Count: {}", count),
    Err(SqlError::Build(msg)) => println!("Build error: {}", msg),
    Err(SqlError::Parse(msg)) => println!("Result parse error: {}", msg),
    Err(SqlError::TypeConversion(msg)) => println!("Type error: {}", msg),
    Err(SqlError::ColumnNotFound(col)) => println!("Column not found: {}", col),
    Err(SqlError::RowIndexOutOfBounds { index, len }) => {
        println!("Row {} out of bounds (len={})", index, len);
    }
    Err(SqlError::EmptyResult) => println!("No results"),
    Err(SqlError::Kinetica(e)) => println!("Database error: {}", e),
}
```

## Performance Tips

1. **Use Bulk Insert for Large Datasets**: `BulkInserter` (or `BatchingSqlClient` for SQL input) is 10-100x faster than individual INSERT statements.

2. **Use Pagination for Large Results**: Use `execute_paginated()` to process large result sets without loading everything into memory.

3. **Reuse TableSchema**: Fetch the schema once and reuse it for creating multiple records.

4. **Batch Size**: For bulk inserts, use batch sizes between 10,000-50,000 records for optimal performance.

5. **Never interpolate untrusted input into SQL**: `SqlClient` sends statements as-is. Validate or escape external values, or use SQLx (`sqlx_support`) for bound parameters.

## Column Name Mapping

Kinetica returns columns as `column_1`, `column_2`, etc. Use `SqlResult::with_column_names()` to map them to actual names:

```rust
use gpudb_api::SqlResult;

let response = client.execute("SELECT id, name, email FROM users").await?.into_inner();
let mut result = SqlResult::with_column_names(response, &["id", "name", "email"]);

// Now you can use actual column names
for row in result.rows()? {
    let id: i64 = row.get("id")?;      // Instead of "column_1"
    let name: String = row.get("name")?; // Instead of "column_2"
}
```

## Thread Safety

- `SqlClient` is `Clone` and can be shared across tasks
- `GenericRecord` is `Clone` and `Send + Sync`
- `Type` should be wrapped in `Arc` for sharing across threads
