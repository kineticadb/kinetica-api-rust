# SQLx Support Guide

Kinetica supports the PostgreSQL wire protocol on port 5432, so SQLx, a popular async SQL toolkit for Rust, can be used against it directly. The `sqlx_support` module (feature `sqlx`) adds connection helpers, extension traits and a batching client on top.

```rust
use gpudb_api::sqlx_support::{connect, KineticaText};
use sqlx::Row;

let pool = connect("localhost", "admin", "password").await?;
let rows = sqlx::query("SELECT id, name FROM users WHERE active = $1")
    .bind(KineticaText(true))
    .fetch_all(&pool)
    .await?;
```

Kinetica's PostgreSQL protocol differs from PostgreSQL's in ways that matter:

- **Parameters:** every parameter after a statement's first is read as text.
- **Types:** several Kinetica types arrive as something other than their
  PostgreSQL counterparts.
- **Transactions:** there are none.

Bind parameters through `KineticaText`, and read and write the affected types
with the wrappers described in
[SQLX_API.md](../gpudb-api/SQLX_API.md#type-mappings).

## Table of Contents

- [SqlClient or SQLx?](#sqlclient-or-sqlx)
- [SQLx Support (PostgreSQL Wire Protocol)](#sqlx-support-postgresql-wire-protocol)
  - [Setup](#setup)
  - [Connection](#connection)
  - [Basic Queries](#basic-queries)
  - [Parameterized Queries](#parameterized-queries)
  - [Struct Mapping](#struct-mapping)
  - [Transactions (not supported)](#transactions-not-supported)
  - [Connection Pooling](#connection-pooling)
  - [Extension Traits](#extension-traits)
- [Type Mappings](#type-mappings)
- [Best Practices](#best-practices)
- [Complete Example](#complete-example)

## SqlClient or SQLx?

The core library's `SqlClient` (see [SQL_API.md](../gpudb-api/SQL_API.md)) executes SQL over Kinetica's HTTP API instead.

| Feature | `SqlClient` (HTTP) | SQLx (PostgreSQL) |
|---------|--------------------|-------------------|
| Compile-time SQL checking | No | Yes (with `query!` macro) |
| Bind parameters | Yes (`SqlParameters`, the `query_parameters` hint) | Yes, sent as text (`KineticaText`) |
| Every Kinetica type, with its column properties | Yes | Some, with wrappers; geometry cannot be selected |
| Bulk operations | Better (native Avro via `BatchingSqlClient`) | Via `BatchingSqlxClient` |
| Connection pooling | Shared `Kinetica` client | Built-in |
| Transactions | Not supported | Not supported |
| Type-safe struct mapping | Manual / serde | `FromRow` derive |
| Kinetica execution options (`SqlHints`) | Request options | Inline hints only |
| HA failover | Yes (same client) | No |
| Extra port / dependency | None | Port 5432, `sqlx` crate |

**Use `SqlClient` when:**
- You want to stay within the existing Kinetica HTTP API (same auth, TLS and failover)
- You need to combine SQL with BulkInserter or other native operations
- The PostgreSQL port is not reachable, or you prefer minimal dependencies

**Use SQLx when:**
- You want compile-time SQL verification or `FromRow` struct mapping
- You are already familiar with SQLx, or need its connection pooling

## SQLx Support (PostgreSQL Wire Protocol)

### Setup

Add the `sqlx` feature to your `Cargo.toml`:

```toml
[dependencies]
gpudb-api = { version = "0.1", features = ["sqlx"] }
```

### Connection

```rust
use gpudb_api::sqlx_support::{
    connect, connect_with_url, KineticaPoolOptions,
};

// Simple connection
let pool = connect("localhost", "admin", "password").await?;

// Connection URL
let pool = connect_with_url("postgres://admin:password@localhost:5432/postgres").await?;

// Full options
let pool = KineticaPoolOptions::new()
    .host("kinetica.example.com")
    .port(5432)
    .username("admin")
    .password("secure_password")
    .database("postgres")
    .max_connections(20)
    .min_connections(5)
    .acquire_timeout(Duration::from_secs(30))
    .idle_timeout(Some(Duration::from_secs(600)))
    .ssl_mode(SslMode::Require)
    .connect()
    .await?;

// From environment variables
// Set: KINETICA_HOST, KINETICA_USER, KINETICA_PASSWORD
// Or: KINETICA_SQLX_URL
let pool = connect_from_env().await?;
```

### Basic Queries

```rust
use sqlx::Row;

// SELECT all rows
let rows = sqlx::query("SELECT id, name FROM users")
    .fetch_all(&pool)
    .await?;

for row in rows {
    // Kinetica sends INT columns as INT4
    let id: i32 = row.get("id");
    let name: String = row.get("name");
    println!("{}: {}", id, name);
}

// SELECT single row
let row = sqlx::query("SELECT COUNT(*) as cnt FROM users")
    .fetch_one(&pool)
    .await?;
let count: i64 = row.get("cnt");

// SELECT optional row
let maybe_row = sqlx::query("SELECT * FROM users WHERE id = 999")
    .fetch_optional(&pool)
    .await?;

// INSERT/UPDATE/DELETE
let result = sqlx::query("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    .execute(&pool)
    .await?;
println!("Rows affected: {}", result.rows_affected());
```

### Parameterized Queries

Kinetica reads every parameter after a statement's first as text, whatever
type SQLx sends it as. A second integer, float or date parameter therefore
fails to convert in an `INSERT`. In a `WHERE` clause it matches nothing, so an
`UPDATE` or `DELETE` reports success and changes no rows. Bind every parameter
through `KineticaText`, which sends text that Kinetica converts to the column's
type:

```rust
use gpudb_api::sqlx_support::KineticaText;

// Positional parameters ($1, $2, ...)
let rows = sqlx::query("SELECT * FROM users WHERE status = $1 AND age > $2")
    .bind(KineticaText("active"))
    .bind(KineticaText(18))
    .fetch_all(&pool)
    .await?;

// INSERT with parameters
let result = sqlx::query("INSERT INTO users (id, name, email) VALUES ($1, $2, $3)")
    .bind(KineticaText(1))
    .bind(KineticaText("Alice"))
    .bind(KineticaText("alice@example.com"))
    .execute(&pool)
    .await?;

// UPDATE with parameters: with typed ones this reports success and changes nothing
let result = sqlx::query("UPDATE users SET status = $1 WHERE id = $2")
    .bind(KineticaText("inactive"))
    .bind(KineticaText(1))
    .execute(&pool)
    .await?;
```

### Struct Mapping

```rust
use sqlx::FromRow;

#[derive(FromRow, Debug)]
struct User {
    id: i32,               // INT arrives as INT4
    name: String,
    email: Option<String>,
    score: f64,
}

// Fetch as typed structs
let users: Vec<User> = sqlx::query_as("SELECT id, name, email, score FROM users")
    .fetch_all(&pool)
    .await?;

// With parameters
let user: User = sqlx::query_as("SELECT * FROM users WHERE id = $1")
    .bind(KineticaText(1))
    .fetch_one(&pool)
    .await?;

// Tuples for simple queries
let row: (i32, String) = sqlx::query_as("SELECT id, name FROM users WHERE id = $1")
    .bind(KineticaText(1))
    .fetch_one(&pool)
    .await?;
```

### Transactions (not supported)

Kinetica's PostgreSQL protocol has no transactions: `pool.begin()` fails ("got
unexpected connection status after attempting to begin transaction"), and each
statement applies when it runs. Kinetica's native SQL has none either.

### Connection Pooling

```rust
use gpudb_api::sqlx_support::KineticaPoolOptions;
use std::time::Duration;

let pool = KineticaPoolOptions::new()
    .host("localhost")
    .username("admin")
    .password("password")
    // Pool configuration
    .max_connections(20)      // Maximum connections in pool
    .min_connections(5)       // Minimum idle connections
    .acquire_timeout(Duration::from_secs(30))  // Timeout for acquiring connection
    .idle_timeout(Some(Duration::from_secs(600)))  // Close idle connections after 10 min
    .max_lifetime(Some(Duration::from_secs(1800))) // Recycle connections after 30 min
    .connect()
    .await?;

// Pool usage is automatic - connections are borrowed and returned
let rows = sqlx::query("SELECT * FROM users")
    .fetch_all(&pool)  // Borrows connection, returns when done
    .await?;

// Close pool gracefully
pool.close().await;
```

### Extension Traits

```rust
use gpudb_api::sqlx_support::KineticaPoolExt;

// Execute raw SQL
pool.execute_sql("CREATE TABLE test (id INT PRIMARY KEY)").await?;

// Count rows
let count = pool.count_rows("users", None).await?;
let count = pool.count_rows("users", Some("active = true")).await?;

// Check table exists
let exists = pool.table_exists("my_schema.my_table").await?;
```

## Type Mappings

Kinetica's PostgreSQL protocol carries several Kinetica types differently from
their PostgreSQL counterparts:

- `TIMESTAMP` arrives as epoch milliseconds.
- `DATETIME` arrives without its milliseconds.
- Arrays arrive numbered from 0, and arrays that functions compute arrive as
  JSON text.
- `TIME`, `UUID`, `IPV4`, `UNSIGNED BIGINT` and `VECTOR` arrive as text.
- `JSON` must be bound as `JSON`: SQLx's own JSON types bind as `JSONB`, which
  Kinetica refuses. Declare JSON columns as `JSON`; `VARCHAR(json)` is a plain
  string.
- A `JSON(text_search)` column is searched by key with `text_match` and ranked
  with `text_match_bm25`, but an UPDATE of one crashes a worker rank on
  Kinetica 7.2.3.21.
- A vector parameter is raw floats in a plain `INSERT`, and text everywhere else.
- Geometry cannot be selected at all.

The full table, and the wrappers that read and write each type (`KineticaText`,
`KineticaTimestamp`, `KineticaBytes`, `KineticaJson`, `KineticaVector`,
`KineticaArray`), are in
[SQLX_API.md](../gpudb-api/SQLX_API.md#type-mappings). How to insert,
update, filter on and delete by JSON, arrays and vectors, and the SQL functions
that work on each, are in
[SQLX_API.md](../gpudb-api/SQLX_API.md#json-arrays-and-vectors); the
`sqlx_complex_types` example runs them. For every type with its column's
properties, use the native API ([EXECUTE_SQL_GUIDE.md](./EXECUTE_SQL_GUIDE.md)).

## Best Practices


1. **Use parameterized queries**, each parameter through `KineticaText`, to
   prevent SQL injection:
   ```rust
   // Good
   sqlx::query("SELECT * FROM users WHERE name = $1").bind(KineticaText(user_input))

   // Bad - vulnerable to SQL injection
   sqlx::query(&format!("SELECT * FROM users WHERE name = '{}'", user_input))
   ```

2. **Use `FromRow` derive** for type safety:
   ```rust
   #[derive(FromRow)]
   struct User { id: i32, name: String }
   let users: Vec<User> = sqlx::query_as("...").fetch_all(&pool).await?;
   ```

3. **Do not rely on transactions**: there are none, so each statement applies
   when it runs.

4. **Configure connection pool** appropriately:
   - `max_connections`: Based on your workload and Kinetica capacity
   - `min_connections`: Keep warm for low-latency queries
   - `idle_timeout`: Prevent stale connections

5. **Close pools** with `pool.close().await`. The server serves a limited number
   of sessions, `postgres_proxy.max_threads` (64 by default), shared by every
   client, and ends one only on PostgreSQL's Terminate message. A connection
   dropped without it keeps its session until
   `postgres_proxy.idle_connection_timeout` (300 seconds by default), and pools
   that outgrow the remaining sessions fail with `PoolTimedOut`. SQLx 0.8's
   `close()` can miss a connection on its way back to the pool; see
   [SQLX_API.md](../gpudb-api/SQLX_API.md#sessions) for closing it too.

## Complete Example


```rust
use gpudb_api::sqlx_support::{KineticaPoolOptions, KineticaText};
use sqlx::FromRow;

#[derive(FromRow, Debug)]
struct Product {
    id: i32,
    name: String,
    category: String,
    price: f64,
    stock: i32,
}

#[derive(FromRow, Debug)]
struct CategorySummary {
    category: String,
    cnt: i64,
    value: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = KineticaPoolOptions::new()
        .host("localhost")
        .username("admin")
        .password("password")
        .max_connections(10)
        .connect()
        .await?;

    // Create table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS products (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            price DOUBLE,
            stock INT
        )"
    )
    .execute(&pool)
    .await?;

    // Insert, every parameter as text
    let products = vec![
        (1, "Laptop", "Electronics", 999.99, 50),
        (2, "Mouse", "Electronics", 29.99, 200),
        (3, "Desk", "Furniture", 299.99, 30),
    ];

    for (id, name, category, price, stock) in products {
        sqlx::query(
            "INSERT INTO products (id, name, category, price, stock) VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(KineticaText(id))
        .bind(KineticaText(name))
        .bind(KineticaText(category))
        .bind(KineticaText(price))
        .bind(KineticaText(stock))
        .execute(&pool)
        .await?;
    }

    // Query with type-safe struct
    let summaries: Vec<CategorySummary> = sqlx::query_as(
        "SELECT category, COUNT(*) as cnt, SUM(price * stock) as value
         FROM products
         GROUP BY category
         ORDER BY value DESC"
    )
    .fetch_all(&pool)
    .await?;

    for summary in summaries {
        println!("{}: {} products, ${:.2} total value",
            summary.category, summary.cnt, summary.value);
    }

    pool.close().await;
    Ok(())
}
```

## See Also

- [SQL_API.md](../gpudb-api/SQL_API.md) - `SqlClient` over the HTTP API
- [EXECUTE_SQL_GUIDE.md](./EXECUTE_SQL_GUIDE.md) - Direct `execute_sql` API usage
- [Kinetica SQL Reference](https://docs.kinetica.com/7.2/sql/) - Complete SQL syntax
- [PostgreSQL Wire Protocol](https://docs.kinetica.com/7.2/connectors/psql_wire_protocol/) - Protocol details
- [SQLx Documentation](https://docs.rs/sqlx) - Full SQLx documentation
