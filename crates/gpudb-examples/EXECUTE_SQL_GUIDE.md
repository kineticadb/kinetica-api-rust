# Execute SQL - Comprehensive Guide

The `execute_sql` endpoint is one of the most important and versatile endpoints in the Kinetica API. It allows you to execute SQL statements for querying data, inserting records, updating tables, and performing DDL operations.

## Quick Start

```bash
cargo run --example execute_sql
```

## Overview

The `execute_sql` endpoint supports:
- **DDL**: CREATE, ALTER, DROP operations
- **DML**: INSERT, UPDATE, DELETE operations
- **Queries**: SELECT with filtering, aggregation, joins, etc.
- **Parameterized queries**: Using `query_parameters` option
- **Result paging**: For large datasets
- **Binary or JSON encoding**: Flexible response format

## Key Features

### 1. Inserting Data

#### Single Row Insert
```rust
let insert_sql = "INSERT INTO employees (id, name, dept) VALUES (?, ?, ?)";

let mut options = HashMap::new();
options.insert(
    "query_parameters".to_string(),
    r#"[[1, "John Doe", "Engineering"]]"#.to_string(),
);

let request = ExecuteSqlRequest {
    statement: insert_sql.to_string(),
    offset: 0,
    limit: -9999,
    encoding: "json".to_string(),
    request_schema_str: String::new(),
    data: vec![],
    options,
};

let response = kinetica.execute_sql(&request).await?;
println!("Rows inserted: {}", response.count_affected);
```

#### Batch Insert
```rust
let insert_sql = "INSERT INTO employees (id, name, dept) VALUES (?, ?, ?)";

let mut options = HashMap::new();
options.insert(
    "query_parameters".to_string(),
    r#"[
        [1, "John Doe", "Engineering"],
        [2, "Jane Smith", "Sales"],
        [3, "Mike Johnson", "Marketing"]
    ]"#.to_string(),
);

let request = ExecuteSqlRequest {
    statement: insert_sql.to_string(),
    encoding: "json".to_string(),
    options,
    ..Default::default()
};

let response = kinetica.execute_sql(&request).await?;
println!("Rows inserted: {}", response.count_affected);
```

#### Insert with Update on Primary Key Collision
```rust
let mut options = HashMap::new();
options.insert(
    "query_parameters".to_string(),
    r#"[[1, "John Doe", "Management"]]"#.to_string(),
);
// Update if primary key exists
options.insert("update_on_existing_pk".to_string(), "true".to_string());

let request = ExecuteSqlRequest {
    statement: insert_sql.to_string(),
    encoding: "json".to_string(),
    options,
    ..Default::default()
};
```

### 2. Querying Data

#### Simple SELECT
```rust
let query = "SELECT * FROM employees ORDER BY id";

let request = ExecuteSqlRequest {
    statement: query.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};

let response = kinetica.execute_sql(&request).await?;
println!("Total records: {}", response.total_number_of_records);

// Parse JSON response
if let Ok(records) = serde_json::from_str::<serde_json::Value>(&response.json_encoded_response) {
    println!("{}", serde_json::to_string_pretty(&records)?);
}
```

#### SELECT with WHERE Clause
```rust
let query = "SELECT id, name, salary FROM employees WHERE dept = 'Engineering' ORDER BY salary DESC";

let request = ExecuteSqlRequest {
    statement: query.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};
```

#### Aggregate Queries
```rust
let query = "SELECT dept, COUNT(*) as count, AVG(salary) as avg_salary
             FROM employees
             GROUP BY dept
             ORDER BY avg_salary DESC";

let request = ExecuteSqlRequest {
    statement: query.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};
```

### 3. Paging Large Results

#### Using Offset and Limit
```rust
let page_size = 100;
let mut offset = 0;

loop {
    let request = ExecuteSqlRequest {
        statement: "SELECT * FROM large_table ORDER BY id".to_string(),
        offset,
        limit: page_size,
        encoding: "json".to_string(),
        ..Default::default()
    };

    let response = kinetica.execute_sql(&request).await?;

    // Process records...

    if !response.has_more_records {
        break;
    }

    offset += page_size;
}
```

#### Using Paging Tables (Recommended for Large Datasets)
```rust
let mut options = HashMap::new();
options.insert("paging_table".to_string(), "my_paging_table".to_string());
options.insert("paging_table_ttl".to_string(), "300".to_string()); // 5 min TTL

let request = ExecuteSqlRequest {
    statement: "SELECT * FROM large_table".to_string(),
    offset: 0,
    limit: 100,
    encoding: "json".to_string(),
    options,
    ..Default::default()
};

let response = kinetica.execute_sql(&request).await?;

// Subsequent requests will use cached results from paging table
// No need to re-execute the query

if !response.paging_table.is_empty() {
    println!("Results cached in: {}", response.paging_table);
    // Remember to clear the paging table when done!
}
```

### 4. DDL Operations

#### CREATE TABLE
```rust
let create_sql = "CREATE TABLE employees (
    id INT NOT NULL,
    name VARCHAR(100),
    dept VARCHAR(50),
    salary DOUBLE,
    PRIMARY KEY (id)
)";

let request = ExecuteSqlRequest {
    statement: create_sql.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};
```

#### ALTER TABLE
```rust
let alter_sql = "ALTER TABLE employees ADD COLUMN hire_date LONG";

let request = ExecuteSqlRequest {
    statement: alter_sql.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};
```

#### DROP TABLE
```rust
let drop_sql = "DROP TABLE IF EXISTS temp_table";

let request = ExecuteSqlRequest {
    statement: drop_sql.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};
```

### 5. UPDATE and DELETE

#### UPDATE
```rust
let update_sql = "UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Engineering'";

let request = ExecuteSqlRequest {
    statement: update_sql.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};

let response = kinetica.execute_sql(&request).await?;
println!("Rows updated: {}", response.count_affected);
```

#### DELETE
```rust
let delete_sql = "DELETE FROM employees WHERE id > 1000";

let request = ExecuteSqlRequest {
    statement: delete_sql.to_string(),
    encoding: "json".to_string(),
    ..Default::default()
};

let response = kinetica.execute_sql(&request).await?;
println!("Rows deleted: {}", response.count_affected);
```

## Important Options

### query_parameters
Pass data as JSON instead of binary encoding. Much easier for most use cases.

```rust
options.insert("query_parameters".to_string(), r#"[[val1, val2, val3]]"#.to_string());
```

### update_on_existing_pk
When inserting into a table with a primary key, update existing records on collision.

```rust
options.insert("update_on_existing_pk".to_string(), "true".to_string());
```

### ignore_existing_pk
Ignore (don't error) on primary key collisions. Only works when `update_on_existing_pk` is false.

```rust
options.insert("ignore_existing_pk".to_string(), "true".to_string());
```

### paging_table
Name for a temporary table to cache query results for efficient paging.

```rust
options.insert("paging_table".to_string(), "my_results_cache".to_string());
```

### paging_table_ttl
Auto-clear paging table after specified seconds. -1 = no timeout.

```rust
options.insert("paging_table_ttl".to_string(), "600".to_string()); // 10 minutes
```

### cost_based_optimization
Enable/disable cost-based query optimization (default: true).

```rust
options.insert("cost_based_optimization".to_string(), "false".to_string());
```

### distributed_joins
Enable distributed joins for complex queries (default: false).

```rust
options.insert("distributed_joins".to_string(), "true".to_string());
```

## Response Fields

### count_affected
Number of rows inserted, updated, or deleted. For SELECT queries, number of rows in the current response.

### total_number_of_records
Total number of records matching the query (before limit/offset).

### has_more_records
Boolean indicating if there are more records available beyond the current response.

### json_encoded_response
Query results encoded as JSON. Parse with `serde_json`:

```rust
if let Ok(records) = serde_json::from_str::<serde_json::Value>(&response.json_encoded_response) {
    // Use records...
}
```

### binary_encoded_response
Query results encoded in Avro binary format. Use when `encoding: "binary"`.

### response_schema_str
Avro schema for the response data.

### paging_table
Name of the paging table created (if any). Remember to clear when done!

### info
Additional information map. May contain:
- `count`: Record count before limits
- `result_table_list`: Comma-separated list of temporary tables created

## Best Practices

1. **Use parameterized queries**: Always use `query_parameters` for values to prevent SQL injection

2. **Enable update_on_existing_pk for upserts**: When you want insert-or-update behavior

3. **Use paging tables for large results**: More efficient than offset/limit for big datasets

4. **Set paging_table_ttl**: Auto-cleanup temporary tables to avoid clutter

5. **Choose encoding wisely**:
   - JSON: Easier to debug and work with
   - Binary: More efficient for large datasets

6. **Check has_more_records**: Always verify if you got all results

7. **Clean up paging tables**: Clear them when done to free resources

8. **Batch inserts**: Use multi-row query_parameters for better performance

9. **Use appropriate indexes**: Create indexes on frequently queried columns

10. **Monitor count_affected**: Verify operations affected expected number of rows

## Common Patterns

### Safe Upsert Pattern
```rust
let mut options = HashMap::new();
options.insert("query_parameters".to_string(), params);
options.insert("update_on_existing_pk".to_string(), "true".to_string());
// This will insert new records or update existing ones
```

### Paginated Results Pattern
```rust
let mut options = HashMap::new();
options.insert("paging_table".to_string(), "results_cache".to_string());
options.insert("paging_table_ttl".to_string(), "300".to_string());

// First request
let response = kinetica.execute_sql(&request).await?;

// Subsequent pages use same paging_table automatically
// Results are cached, query not re-executed
```

### Bulk Delete with Confirmation
```rust
// First, count what will be deleted
let count_sql = "SELECT COUNT(*) FROM table WHERE condition";
let count_response = kinetica.execute_sql(&count_request).await?;

// Then delete
let delete_sql = "DELETE FROM table WHERE condition";
let delete_response = kinetica.execute_sql(&delete_request).await?;
assert_eq!(delete_response.count_affected, expected_count);
```

## Performance Tips

1. Use batch inserts instead of individual inserts
2. Enable paging tables for queries returning > 10K records
3. Use binary encoding for large data transfers
4. Create appropriate indexes before large queries
5. Use LIMIT to restrict result set size
6. Consider `cost_based_optimization` for complex queries
7. Enable `distributed_joins` for multi-table queries on large datasets

## See Also

- [SQL Support Documentation](https://docs.kinetica.com/sql/)
- [Primary Keys](https://docs.kinetica.com/concepts/tables/#primary-keys)
- [Table TTL](https://docs.kinetica.com/concepts/ttl/)
- Complete example: `examples/execute_sql.rs`
