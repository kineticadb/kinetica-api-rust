# SQLx Support for Kinetica

This document describes the SQLx integration for Kinetica's Rust client. SQLx connects to Kinetica via its PostgreSQL wire protocol support, enabling use of standard SQLx patterns and compile-time checked queries.

## Overview

Kinetica supports the PostgreSQL wire protocol on port 5432, which means you can use [SQLx](https://github.com/launchbadge/sqlx)'s PostgreSQL driver directly. This module provides:

| Component | Description |
|-----------|-------------|
| `KineticaPool` | Type alias for `PgPool` with Kinetica configuration |
| `KineticaPoolOptions` | Builder for creating connection pools |
| `KineticaPoolExt` | Extension trait with Kinetica-specific helpers |
| `SqlxBulkInserter` | Bridge to HTTP API for efficient bulk loading |
| Connection helpers | `connect()`, `connect_with_url()`, etc. |
| Value wrappers | `KineticaText`, `KineticaTimestamp`, `KineticaBytes`, `KineticaJson`, `KineticaVector`, `KineticaArray`: the Kinetica types the PostgreSQL protocol does not carry as PostgreSQL does |

## Quick Start

```rust
use gpudb_api::sqlx_support::{KineticaPoolOptions, connect, KineticaPoolExt};
use sqlx::Row;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // Simple connection
    let pool = connect("localhost", "admin", "password").await?;

    // Execute queries using standard SQLx
    let rows = sqlx::query("SELECT id, name FROM users")
        .fetch_all(&pool)
        .await?;

    for row in rows {
        let id: i32 = row.get("id");
        let name: String = row.get("name");
        println!("User: id={}, name={}", id, name);
    }

    Ok(())
}
```

## Connection Options

### Simple Connection

```rust
use gpudb_api::sqlx_support::connect;

// Connect with defaults (port 5432, database "postgres")
let pool = connect("localhost", "admin", "password").await?;
```

### Full Configuration

```rust
use gpudb_api::sqlx_support::{KineticaPoolOptions, SslMode};
use std::time::Duration;

let pool = KineticaPoolOptions::new()
    .host("kinetica-server.example.com")
    .port(5432)
    .username("admin")
    .password("secret")
    .database("postgres")
    .max_connections(20)
    .min_connections(5)
    .acquire_timeout(Duration::from_secs(30))
    .idle_timeout(Some(Duration::from_secs(600)))
    .ssl_mode(SslMode::Prefer)
    .connect()
    .await?;
```

### From URL

```rust
use gpudb_api::sqlx_support::connect_with_url;

let pool = connect_with_url(
    "postgres://admin:password@localhost:5432/postgres"
).await?;
```

### From Environment Variables

```rust
use gpudb_api::sqlx_support::connect_from_env;

// Reads from KINETICA_SQLX_URL or KINETICA_HOST/USER/PASSWORD
let pool = connect_from_env().await?;
```

Environment variables:
- `KINETICA_SQLX_URL` - Full connection URL (takes precedence)
- `KINETICA_HOST` - Server hostname (default: localhost)
- `KINETICA_PORT` - PostgreSQL port (default: 5432)
- `KINETICA_USER` - Username
- `KINETICA_PASSWORD` - Password

## Type Mappings

Kinetica's PostgreSQL protocol carries its types as below, as measured against
Kinetica 7.2. Where the PostgreSQL type does not carry the value whole, read and
write it through the wrappers in `gpudb_api::sqlx_support`.

| Kinetica type | Sent as | Read as | Bind as |
|---|---|---|---|
| `BOOLEAN` | `BOOL` | `bool` | `KineticaText(bool)` |
| `TINYINT`, `SMALLINT`, `INTEGER` | `INT4` | `i32` | `KineticaText(i32)` |
| `BIGINT` | `INT8` | `i64` | `KineticaText(i64)` |
| `UNSIGNED BIGINT` | `TEXT` | `KineticaText<u64>` | `KineticaText(u64)` |
| `REAL` | `FLOAT4` | `f32` | `KineticaText(f32)` |
| `DOUBLE` | `FLOAT8` | `f64` | `KineticaText(f64)` |
| `DECIMAL(p,s)` | `NUMERIC` | a decimal type, with SQLx's `bigdecimal` or `rust_decimal` feature; or `CAST(col AS VARCHAR)` as `KineticaText<String>` | `KineticaText` of its text |
| `VARCHAR`, `VARCHAR(n)` | `TEXT` | `String` | `KineticaText(&str)` |
| `IPV4` | `TEXT` | `KineticaText<Ipv4Addr>` | `KineticaText(Ipv4Addr)` |
| `UUID` | `TEXT` | `KineticaText<uuid::Uuid>` | `KineticaText(uuid::Uuid)` |
| `DATE` | `DATE` | `chrono::NaiveDate` | `KineticaText(NaiveDate)` |
| `TIME` | `TEXT` | `KineticaText<NaiveTime>` | `KineticaText(NaiveTime)` |
| `DATETIME` | `TIMESTAMP`, **without its milliseconds** | `CAST(col AS VARCHAR)` as `KineticaText<NaiveDateTime>` | `KineticaText(NaiveDateTime)` |
| `TIMESTAMP` | `INT8`: milliseconds since the epoch | `KineticaTimestamp` | `KineticaTimestamp` |
| `BLOB` | `BYTEA` | `Vec<u8>` or `KineticaBytes` | `KineticaBytes` |
| `GEOMETRY`, `BLOB(WKT)` | none: **the query fails** | `ST_ASTEXT(col)` as `String` | `KineticaText` of its WKT |
| `JSON` | `JSON` | `serde_json::Value` or `KineticaJson<T>` | `KineticaJson(value)` |
| `VECTOR(n)` | `TEXT`, e.g. `[1,2.5,3]` | `KineticaVector` | `KineticaVector` in a plain `INSERT`; `KineticaText(vector.as_slice())` elsewhere |
| arrays, e.g. `INTEGER[3]` | PostgreSQL arrays numbered from 0 | `KineticaArray<T>` (`Option<T>` elements for nulls) | `KineticaArray<T>` |

The crate enables SQLx's `chrono` and `json` features, so `NaiveDate` and
`serde_json::Value` read as they are. Nulls read as `Option<_>` of any of these,
and bind as `None`.

What goes wrong without the wrappers:

- **Arrays:** SQLx's `Vec<T>` refuses Kinetica's arrays, which are numbered from 0.
- **`DATETIME`:** a value read as a `NaiveDateTime`, or bound as one, loses its milliseconds.
- **Refused parameters:** Kinetica refuses `uuid::Uuid`, `NaiveTime`, decimal and `Vec<u8>` (`BYTEA`) parameters.
- **JSON:** SQLx binds `serde_json::Value` and `sqlx::types::Json` as `JSONB`, whose leading version byte Kinetica refuses as invalid JSON.
- **Vectors:** a plain `INSERT` refuses a vector as text, and everything else refuses one as raw floats.
- **Geometry:** any query that selects a geometry column, including `SELECT *` on a table that has one, fails on the server.

For every Kinetica type with its column's properties, use the native API
([SQL_API.md](SQL_API.md), `Kinetica::execute_sql`, `GenericRecord`).

## JSON, Arrays and Vectors

Each is inserted, updated, filtered on and deleted by with ordinary SQL, binding
the wrappers. The `sqlx_complex_types` example runs these statements end to
end.

### JSON

`KineticaJson<T>` binds any `Serialize` value as `JSON`, in any parameter
position, and reads any `DeserializeOwned` one:

```rust
use gpudb_api::sqlx_support::{KineticaJson, KineticaText};
use serde_json::{json, Value};

sqlx::query("INSERT INTO docs (id, doc) VALUES ($1, $2)")
    .bind(KineticaText(1))
    .bind(KineticaJson(json!({"title": "Report", "meta": {"priority": 2}})))
    .execute(&pool)
    .await?;

// The path may be a parameter too
let rows = sqlx::query("SELECT id, doc FROM docs WHERE JSON_VALUE(doc, $1) = $2")
    .bind(KineticaText("$.meta.priority"))
    .bind(KineticaText("2"))
    .fetch_all(&pool)
    .await?;
let doc: KineticaJson<Value> = rows[0].get("doc");
```

`JSON_VALUE`, `JSON_QUERY` (returning JSON), `JSON_EXISTS` and `IS_JSON`
(`bool`), `JSON_LENGTH` (`i32`), `JSON_TYPE` (`String`), `JSON_OBJECT` and
`JSON_ARRAY` work. A SQL `NULL` (`None::<KineticaJson<_>>`) and the JSON `null`
(`KineticaJson(Value::Null)`) stay distinct. Invalid JSON is refused.

Nested paths (`$.customer.tier`, `$.labels[0]`) work wherever an expression
does: in `GROUP BY`, where the path may again be a parameter, and as a `JOIN`
key. `JSON_VALUE` returns text, a missing key `NULL`, and a number or boolean as
its text (`"30"`, `"true"`), so compare numbers through a cast:

```rust
let rows = sqlx::query(
    "SELECT JSON_VALUE(meta, $1) AS tier, COUNT(*) FROM tickets
     WHERE CAST(JSON_VALUE(meta, '$.hours') AS DOUBLE) > $2
     GROUP BY JSON_VALUE(meta, $1)",
)
.bind(KineticaText("$.customer.tier"))
.bind(KineticaText(3.5))
.fetch_all(&pool)
.await?;
let counts: Vec<(String, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
```

Declare JSON columns as `JSON`. `VARCHAR(json)` and `STRING(json)` lose the
`json` property on the way to the server: they are plain strings, which take
text that is not JSON and arrive as `TEXT`. `KineticaJson` reads them all.

### Full-text search of JSON

With text search enabled on the server (`enable_text_search = true` in
`gpudb.conf`, which takes a restart on Kinetica 7.2.3), a `JSON(text_search)`
column is searchable by key: `text_match(col, query)` is a `bool`, and
`text_match_bm25(col, query)` and `text_match_bm25_global(col, query)` are `f64`
relevance scores, exactly `0.0` for rows that do not match. The query binds as
text, like any other parameter:

```rust
use gpudb_api::sqlx_support::{KineticaJson, KineticaText};
use serde_json::json;

sqlx::query(
    "CREATE TABLE tickets (id INT NOT NULL, subject VARCHAR(200, text_search),
                           meta JSON(text_search), SHARD KEY (id))",
)
.execute(&pool)
.await?;
sqlx::query("INSERT INTO tickets VALUES ($1, $2, $3)")
    .bind(KineticaText(1))
    .bind(KineticaText("Login fails on mobile"))
    .bind(KineticaJson(json!({"customer": {"tier": "Enterprise", "region": "EMEA"},
                              "labels": ["mobile", "login"], "priority": "high"})))
    .execute(&pool)
    .await?;

// Nested keys are dotted, an array matches by element, and terms combine
let rows = sqlx::query(
    "SELECT id, text_match_bm25_global(meta, $1) AS score FROM tickets
     WHERE text_match_bm25_global(meta, $1) > 0 ORDER BY score DESC, id LIMIT 10",
)
.bind(KineticaText("customer.region:EMEA AND labels:login -priority:low"))
.fetch_all(&pool)
.await?;
let score: f64 = rows[0].get("score");
```

- Searches see each INSERT, UPDATE and DELETE at once, and `text_match` works
  in the `WHERE` clause of an UPDATE or DELETE.
- **Do not UPDATE a `JSON(text_search)` column.** On Kinetica 7.2.3.21 that
  crashes a worker rank (a segmentation fault in `updateRecordsWithMaps`),
  through SQLx and native SQL alike, and after a second crash the cluster needs
  a restart. Delete the row and insert it again instead. UPDATEs of `VARCHAR`
  and `STRING` text-search columns work.
- On a sharded table, `text_match_bm25` scores each shard against its own rows;
  `text_match_bm25_global` scores against the whole table, as a replicated table
  would.
- The per-chunk cap, `text_match_bm25(col, CAST($1 AS VARCHAR), 10)`, needs the
  query cast to text and the cap written as an integer literal: a bare `$1` is
  refused ("Cannot apply 'TEXT_MATCH_BM25'"), and so is a parameter cap.
- On a JSON column a term without a key matches nothing; write `key:term`. A
  range such as `hours:[1 TO 5]` fails.
- `*` and `?` are wildcards (`dash*`, `*board`, `dash?oard`), and `/dash.*/` is a
  regular expression. On a JSON column either needs the path written from the
  column, `meta.labels:dash*`: `labels:dash*` fails ("no such text or JSON
  column"), and so does `dash*` ("needs a sub-path").
- `STRING(json, text_search)` is indexed as one string: keys fail with "Field
  does not exist", and bare terms match.
- A column without the property fails with "has no text search index", and a
  table cannot be created with one while text search is off ("... while text
  search is disabled").

### Arrays

`KineticaArray<T>` binds and reads arrays of `i32`, `i64`, `u64`, `f32`, `f64`,
`bool` and `String`; use `KineticaArray<Option<T>>` for arrays holding nulls.

```rust
use gpudb_api::sqlx_support::{KineticaArray, KineticaText};

sqlx::query("UPDATE items SET nums = ARRAY_APPEND(nums, $1) WHERE ARRAY_CONTAINS(tags, $2)")
    .bind(KineticaText(4))
    .bind(KineticaText("red"))
    .execute(&pool)
    .await?;

let rows = sqlx::query("SELECT id, nums FROM items WHERE ARRAY_CONTAINS_ANY(nums, $1)")
    .bind(KineticaArray(vec![4]))
    .fetch_all(&pool)
    .await?;
let nums: KineticaArray<i32> = rows[0].get("nums");
```

- `ARRAY_CONTAINS`, `ARRAY_CONTAINS_ANY`, `ARRAY_CONTAINS_ALL`, `ARRAY_LENGTH`,
  `col[i]` (numbered from 1), `ARRAY_APPEND`, `ARRAY_CONCAT`, `ARRAY_SLICE` and
  `ARRAY_TO_STRING` work. Arrays these functions compute arrive as JSON text,
  `[1,2,3]`, which `KineticaArray` reads as well.
- A numeric `ARRAY_CONTAINS(nums, $1)` finds nothing whatever is bound, even
  through a `CAST`. Write `ARRAY_CONTAINS_ANY(nums, $1)` with a one-element
  `KineticaArray`. On strings, `ARRAY_CONTAINS(tags, $1)` works.
- A fixed size, as in `INTEGER[3]`, is not enforced.
- An infinite float in a `REAL[]` or `DOUBLE[]` is refused.
- `UNSIGNED BIGINT[]` arrives as text elements; `KineticaArray<u64>` reads it.
- Read arrays through `sqlx::query`. In the text form `sqlx::raw_sql` returns,
  Kinetica writes `null` in capitals wherever it occurs in a string element:
  `"nullable"` comes back `"NULLable"`.

### Vectors

Kinetica takes a vector parameter in one of two forms, depending on the
statement:

| Statement | Bind |
|---|---|
| `INSERT ... VALUES ($n)` | `KineticaVector(vec)`: raw floats; text is refused ("Incorrect vector size") |
| `INSERT ... VALUES (VECTOR($n, dims))` | `KineticaText(vec.as_slice())` |
| `UPDATE ... SET v = $n` | `KineticaText(vec.as_slice())`; raw floats are refused |
| `L2_DISTANCE(v, $n)`, `L1_DISTANCE`, `COSINE_DISTANCE`, `DOT_PRODUCT` | `KineticaText(vec.as_slice())` |
| `v <-> CAST($n AS VECTOR(dims))` | `KineticaText(vec.as_slice())`; a bare `v <-> $n` is refused |

```rust
use gpudb_api::sqlx_support::{KineticaText, KineticaVector};

sqlx::query("INSERT INTO points (id, v) VALUES ($1, $2)")
    .bind(KineticaText(1))
    .bind(KineticaVector(vec![0.1, 0.2, 0.3]))
    .execute(&pool)
    .await?;

let probe = [0.1_f32, 0.2, 0.25];
let rows = sqlx::query("SELECT id, v, L2_DISTANCE(v, $1) AS d FROM points ORDER BY d LIMIT 5")
    .bind(KineticaText(probe.as_slice()))
    .fetch_all(&pool)
    .await?;
let v: KineticaVector = rows[0].get("v");
let d: f32 = rows[0].get("d");
```

The distance functions and `L2_NORM` return `f32`. A vector of the wrong size
is refused. Vectors of up to 4096 dimensions were tested.

## Executing Queries

### Basic Queries

```rust
use sqlx::Row;

// Simple query
let rows = sqlx::query("SELECT id, name FROM users WHERE active = true")
    .fetch_all(&pool)
    .await?;

for row in rows {
    let id: i32 = row.get("id");
    let name: String = row.get("name");
    println!("{}: {}", id, name);
}

// Single row
let row = sqlx::query("SELECT COUNT(*) as cnt FROM users")
    .fetch_one(&pool)
    .await?;
let count: i64 = row.get("cnt");
```

### Typed Queries with `query_as`

```rust
use sqlx::FromRow;

#[derive(FromRow, Debug)]
struct User {
    id: i32,
    name: String,
    email: Option<String>,
    score: f64,
}

let users: Vec<User> = sqlx::query_as("SELECT id, name, email, score FROM users")
    .fetch_all(&pool)
    .await?;

for user in users {
    println!("{:?}", user);
}
```

### With Bind Parameters

Kinetica reads every parameter after a statement's first as text, whatever type
SQLx sends it as:

- **In an `INSERT`:** a second integer, float or date parameter fails to convert
  (`Cannot convert field ...`).
- **In a `WHERE` clause:** it matches nothing, so an `UPDATE` or `DELETE` reports
  success and changes no rows.
- **Beside a literal:** a literal next to such a parameter can be stored as NULL.

Text parameters work in every position, and Kinetica converts each to its
column's type. So bind every parameter through `KineticaText`, or the wrapper
its column's type calls for (see [Type Mappings](#type-mappings)):

```rust
use gpudb_api::sqlx_support::KineticaText;

// One parameter
let rows = sqlx::query("SELECT * FROM users WHERE id = $1")
    .bind(KineticaText(42))
    .fetch_all(&pool)
    .await?;

// Several parameters: each must be text
let rows = sqlx::query("SELECT * FROM users WHERE score > $1 AND active = $2")
    .bind(KineticaText(80.0))
    .bind(KineticaText(true))
    .fetch_all(&pool)
    .await?;
```

### Execute (INSERT/UPDATE/DELETE)

```rust
use gpudb_api::sqlx_support::{KineticaText, KineticaTimestamp};

// INSERT
let result = sqlx::query("INSERT INTO users (id, name, joined) VALUES ($1, $2, $3)")
    .bind(KineticaText(1))
    .bind(KineticaText("Alice"))
    .bind(KineticaTimestamp(chrono::Utc::now()))
    .execute(&pool)
    .await?;
println!("Rows affected: {}", result.rows_affected());

// UPDATE: with typed parameters this would report success and change nothing
let result = sqlx::query("UPDATE users SET score = $1 WHERE id = $2")
    .bind(KineticaText(95.5))
    .bind(KineticaText(1))
    .execute(&pool)
    .await?;

// DELETE
let result = sqlx::query("DELETE FROM users WHERE active = false")
    .execute(&pool)
    .await?;
```

## KineticaPoolExt

The `KineticaPoolExt` trait provides Kinetica-specific helper methods.

### Basic Helpers

```rust
use gpudb_api::sqlx_support::KineticaPoolExt;

// Execute raw SQL
let rows_affected = pool.execute_sql("TRUNCATE TABLE temp_data").await?;

// Check if table exists
if pool.table_exists("users").await? {
    println!("Table exists");
}

// Count rows
let count = pool.count_rows("users", Some("active = true")).await?;
let total = pool.count_rows("users", None).await?;
```

### Pagination

```rust
use gpudb_api::sqlx_support::KineticaPoolExt;
use sqlx::Row;

// Fetch single page with total count
let (rows, total) = pool.fetch_page(
    "SELECT * FROM users ORDER BY id",
    0,    // offset
    100   // limit
).await?;
println!("Got {} rows out of {} total", rows.len(), total);

// Process all pages with callback (memory efficient)
let stats = pool.fetch_all_pages(
    "SELECT * FROM large_table ORDER BY id",
    10000,  // page size
    |rows, page_num| {
        println!("Processing page {} with {} rows", page_num, rows.len());
        for row in &rows {
            let id: i32 = row.get("id");
            // Process row...
        }
        Ok(true)  // Return false to stop early
    }
).await?;
println!("Processed {} pages, {} rows", stats.pages_fetched, stats.rows_fetched);

// Fetch all into memory (convenience method)
let all_rows = pool.fetch_all_paginated(
    "SELECT * FROM users ORDER BY id",
    10000  // page size
).await?;
```

## Bulk Insert

SQLx uses the PostgreSQL wire protocol which is less efficient for bulk loading than Kinetica's native HTTP API. The `SqlxBulkInserter` bridges this gap by using:

- **`BulkInserter<GenericRecord>`** - The high-performance shard-aware bulk inserter
- **Multi-head ingest** - Routes records directly to appropriate worker nodes
- **Parallel batch processing** - Multiple workers for maximum throughput
- **Avro binary encoding** - Compact wire format

### Creating the Bulk Inserter

```rust
use gpudb_api::sqlx_support::SqlxBulkInserter;
use gpudb_api::{GenericRecord, Kinetica, KineticaOptions, Type};
use std::sync::Arc;

// The HTTP API client the records are sent through: port 9191, not 5432,
// configured (TLS, HA failover and the rest) as for any HTTP API use
let mut options = KineticaOptions::new();
options.set_username("admin").set_password("password");
let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?);

let bulk_inserter = SqlxBulkInserter::new(kinetica);
```

### Fetching Type Schema

```rust
// Fetch table type
let record_type = Arc::new(bulk_inserter.fetch_type("my_table").await?);

// Inspect schema
println!("Columns: {:?}", record_type.column_count());
let pk_indices = record_type.primary_key_indices();
for &idx in pk_indices {
    if let Some(col) = record_type.get_column(idx) {
        println!("Primary key: {}", col.name());
    }
}
```

### Performing Bulk Insert

```rust
use gpudb_api::{GenericRecord, BulkInserterOptions};

// Create records using GenericRecord
let records: Vec<GenericRecord> = (0..10000)
    .map(|i| {
        let mut record = record_type.new_record();
        record.set_by_name("id", i)?;
        record.set_by_name("name", format!("user_{}", i))?;
        record.set_by_name("score", i as f64 * 1.5)?;
        record
    })
    .collect();

// Simple bulk insert
let stats = bulk_inserter.insert(
    "my_table",
    records,
    10000,  // batch size
).await?;

println!("Inserted {} records in {}ms ({:.0} rec/sec)",
    stats.records_inserted,
    stats.elapsed_ms,
    stats.records_per_second);
```

### With Options (Upsert Mode)

```rust
use gpudb_api::BulkInserterOptions;

let mut options = BulkInserterOptions::new();
options.set_update_on_existing_pk(true);  // Enable upsert

let stats = bulk_inserter.insert_with_options(
    "my_table",
    records.clone(),
    10000,       // batch size
    8,           // num workers
    Some(options)
).await?;

// Or use the convenience method
let stats = bulk_inserter.upsert("my_table", records, 10000).await?;
```

### Complete Example

```rust
use gpudb_api::sqlx_support::{connect, SqlxBulkInserter, KineticaPoolExt};
use gpudb_api::{GenericRecord, Kinetica, KineticaOptions};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect via SQLx for queries
    let pool = connect("localhost", "admin", "password").await?;

    // Create table via SQLx
    sqlx::query("CREATE TABLE IF NOT EXISTS test_bulk (
        id INT NOT NULL,
        name VARCHAR(64),
        value DOUBLE,
        PRIMARY KEY (id)
    )").execute(&pool).await?;

    // Create bulk inserter for efficient loading
    let mut options = KineticaOptions::new();
    options.set_username("admin").set_password("password");
    let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?);
    let bulk_inserter = SqlxBulkInserter::new(kinetica);

    // Fetch type and create records using GenericRecord
    let record_type = Arc::new(bulk_inserter.fetch_type("test_bulk").await?);

    let records: Vec<GenericRecord> = (0..10000)
        .map(|i| {
            let mut record = record_type.new_record();
            record.set_by_name("id", i)?;
            record.set_by_name("name", format!("item_{}", i))?;
            record.set_by_name("value", i as f64)?;
            record
        })
        .collect();

    // Bulk insert
    let stats = bulk_inserter.insert("test_bulk", records, 5000).await?;
    println!("Inserted {} records", stats.records_inserted);

    // Verify via SQLx
    let count = pool.count_rows("test_bulk", None).await?;
    println!("Table now has {} rows", count);

    Ok(())
}
```

## Limitations

Kinetica's PostgreSQL wire protocol has some limitations compared to a full PostgreSQL server:

### Transactions Not Supported

```rust
// This fails: "got unexpected connection status after attempting to begin transaction"
let tx = pool.begin().await?;
```

Each statement applies when it runs. Kinetica's native SQL has no transactions
either.

### Types and Parameters

- `INTEGER`, `SMALLINT` and `TINYINT` columns are `INT4` (`i32`), not `INT8` (`i64`)
- Several Kinetica types need the value wrappers: see [Type Mappings](#type-mappings)
- Every parameter after a statement's first must be text: see [With Bind Parameters](#with-bind-parameters)
- Geometry columns cannot be selected; select `ST_ASTEXT(col)`
- JSON, arrays and vectors have quirks of their own: see [JSON, Arrays and Vectors](#json-arrays-and-vectors)

### Sessions

The server serves one session per thread, up to its `postgres_proxy.max_threads`
(64 by default), shared by every client. It ends a session when its client sends
PostgreSQL's Terminate message, which `pool.close()` and `Connection::close()`
send. A connection closed without it, as when a process exits or a task panics
with the connection open, keeps its session until the server's
`postgres_proxy.idle_connection_timeout` (300 seconds by default). Pools that
outgrow the server's sessions wait, then fail with `sqlx::Error::PoolTimedOut`.

SQLx 0.8's `pool.close()` can return while a connection on its way back to the
pool is still open. If the program then ends, that connection is dropped without
a Terminate, and its session lingers. A pool of one connection avoids this, as
does closing again once the connection is back:

```rust
pool.close().await;
for _ in 0..10 {
    if pool.size() == 0 {
        break;
    }
    tokio::task::yield_now().await;
    pool.close().await;
}
```

### Performance

- For bulk loading, use `SqlxBulkInserter` instead of individual INSERTs
- For complex analytics, consider using the HTTP API directly
- SQLx is ideal for OLTP-style queries

## SSL/TLS Configuration

```rust
use gpudb_api::sqlx_support::{KineticaPoolOptions, SslMode};

let pool = KineticaPoolOptions::new()
    .host("secure-kinetica.example.com")
    .username("admin")
    .password("password")
    .ssl_mode(SslMode::Require)  // or Prefer, Disable
    .connect()
    .await?;
```

SSL modes:
- `SslMode::Disable` - No SSL (default)
- `SslMode::Prefer` - Use SSL if available
- `SslMode::Require` - Require SSL connection

## Connection Pool Management

```rust
use gpudb_api::sqlx_support::KineticaPoolOptions;
use std::time::Duration;

let pool = KineticaPoolOptions::new()
    .host("localhost")
    .username("admin")
    .password("password")
    // Pool sizing
    .max_connections(20)
    .min_connections(5)
    // Timeouts
    .acquire_timeout(Duration::from_secs(30))
    .idle_timeout(Some(Duration::from_secs(600)))
    .max_lifetime(Some(Duration::from_secs(1800)))
    .connect()
    .await?;

// Later, close the pool gracefully
pool.close().await;
```

## Error Handling

```rust
use sqlx::Error as SqlxError;

match pool.execute_sql("SELECT * FROM nonexistent").await {
    Ok(_) => println!("Success"),
    Err(SqlxError::Database(db_err)) => {
        println!("Database error: {}", db_err.message());
    }
    Err(SqlxError::PoolTimedOut) => {
        println!("Connection pool timeout");
    }
    Err(e) => {
        println!("Other error: {}", e);
    }
}
```

## Comparison: SQL API vs SQLx

| Feature | SQL API (HTTP) | SQLx (PostgreSQL) |
|---------|---------------|-------------------|
| Protocol | HTTP + Avro | PostgreSQL wire |
| Port | 9191 | 5432 |
| Compile-time checks | No | Yes (with `query!`) |
| Bulk insert | Native, very fast | Via SqlxBulkInserter |
| Transactions | Not supported | Not supported |
| Kinetica types | All, with their column properties | See [Type Mappings](#type-mappings) |
| Connection pooling | Manual | Built-in |
| Ecosystem | Kinetica-specific | Standard Rust SQL |

**When to use SQL API:**
- Maximum bulk insert performance
- Kinetica-specific features
- Integration with BulkInserter

**When to use SQLx:**
- Standard SQL patterns
- Compile-time query checking
- Familiar Rust SQL ecosystem
- Connection pooling
- Type-safe row mapping

## Thread Safety

- `KineticaPool` (`PgPool`) is `Clone`, `Send`, and `Sync`
- Safe to share across async tasks
- `SqlxBulkInserter` is `Send` and `Sync`

```rust
use std::sync::Arc;
use tokio::task;

let pool = connect("localhost", "admin", "password").await?;

// Share pool across tasks
let handles: Vec<_> = (0..10)
    .map(|i| {
        let pool = pool.clone();
        task::spawn(async move {
            let count = pool.count_rows("users", None).await?;
            Ok::<_, sqlx::Error>(count)
        })
    })
    .collect();

for handle in handles {
    let count = handle.await??;
    println!("Count: {}", count);
}
```
