//! SQLx Support for Kinetica via PostgreSQL Wire Protocol
//!
//! This module provides helpers for using [SQLx](https://github.com/launchbadge/sqlx)
//! with Kinetica's PostgreSQL wire protocol support.
//!
//! # Overview
//!
//! Kinetica supports the PostgreSQL wire protocol on port 5432, which means you can
//! use SQLx's PostgreSQL driver directly. This module provides:
//!
//! - [`KineticaPool`] - A type alias for `PgPool` with Kinetica-specific configuration
//! - [`KineticaPoolOptions`] - Builder for creating connection pools with sensible defaults
//! - [`connect`], [`connect_with_url`] and [`connect_with_options`] - Convenience functions for establishing connections
//! - [`KineticaText`], [`KineticaTimestamp`], [`KineticaBytes`], [`KineticaJson`],
//!   [`KineticaVector`] and [`KineticaArray`] - for the Kinetica types the protocol does not
//!   carry as PostgreSQL does
//!
//! # Quick Start
//!
//! ```no_run
//! use gpudb_api::sqlx_support::{KineticaPoolOptions, connect};
//! use sqlx::Row;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), sqlx::Error> {
//!     // Simple connection
//!     let pool = connect("localhost", "admin", "password").await?;
//!
//!     // Or with full options
//!     let pool = KineticaPoolOptions::new()
//!         .host("localhost")
//!         .port(5432)
//!         .username("admin")
//!         .password("password")
//!         .max_connections(10)
//!         .connect()
//!         .await?;
//!
//!     // Execute queries using standard SQLx
//!     let rows = sqlx::query("SELECT id, name FROM users")
//!         .fetch_all(&pool)
//!         .await?;
//!
//!     for row in rows {
//!         let id: i64 = row.get("id");
//!         let name: String = row.get("name");
//!         println!("User: id={}, name={}", id, name);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Type Mappings
//!
//! Kinetica's PostgreSQL protocol carries its types as below, as measured
//! against Kinetica 7.2. Where the PostgreSQL type does not carry the value
//! whole, read and write it through the wrappers this module provides (see
//! [`KineticaText`] and its siblings).
//!
//! | Kinetica type | Sent as | Read as | Bind as |
//! |---|---|---|---|
//! | `BOOLEAN` | `BOOL` | `bool` | `KineticaText(bool)` |
//! | `TINYINT`, `SMALLINT`, `INTEGER` | `INT4` | `i32` | `KineticaText(i32)` |
//! | `BIGINT` | `INT8` | `i64` | `KineticaText(i64)` |
//! | `UNSIGNED BIGINT` | `TEXT` | `KineticaText<u64>` | `KineticaText(u64)` |
//! | `REAL` | `FLOAT4` | `f32` | `KineticaText(f32)` |
//! | `DOUBLE` | `FLOAT8` | `f64` | `KineticaText(f64)` |
//! | `DECIMAL(p,s)` | `NUMERIC` | a decimal type, with SQLx's `bigdecimal` or `rust_decimal` feature; or `CAST(col AS VARCHAR)` as `KineticaText<String>` | `KineticaText` of its text |
//! | `VARCHAR`, `VARCHAR(n)` | `TEXT` | `String` | `KineticaText(&str)` |
//! | `IPV4` | `TEXT` | `KineticaText<Ipv4Addr>` | `KineticaText(Ipv4Addr)` |
//! | `UUID` | `TEXT` | `KineticaText<uuid::Uuid>` | `KineticaText(uuid::Uuid)` |
//! | `DATE` | `DATE` | `chrono::NaiveDate` | `KineticaText(NaiveDate)` |
//! | `TIME` | `TEXT` | `KineticaText<NaiveTime>` | `KineticaText(NaiveTime)` |
//! | `DATETIME` | `TIMESTAMP`, **without its milliseconds** | `CAST(col AS VARCHAR)` as `KineticaText<NaiveDateTime>` | `KineticaText(NaiveDateTime)` |
//! | `TIMESTAMP` | `INT8`: milliseconds since the epoch | `KineticaTimestamp` | `KineticaTimestamp` |
//! | `BLOB` | `BYTEA` | `Vec<u8>` or `KineticaBytes` | `KineticaBytes` |
//! | `GEOMETRY`, `BLOB(WKT)` | none: **the query fails** | `ST_ASTEXT(col)` as `String` | `KineticaText` of its WKT |
//! | `JSON` | `JSON` | `serde_json::Value` or `KineticaJson<T>` | `KineticaJson(value)` |
//! | `VECTOR(n)` | `TEXT`, e.g. `[1,2.5,3]` | `KineticaVector` | `KineticaVector` in a plain `INSERT`; `KineticaText(vector.as_slice())` elsewhere |
//! | arrays, e.g. `INTEGER[3]` | PostgreSQL arrays numbered from 0 | `KineticaArray<T>` | `KineticaArray<T>` |
//!
//! This crate enables SQLx's `chrono` and `json` features, so `NaiveDate` and
//! `serde_json::Value` read as they are. Nulls read as `Option<_>` of any of
//! these, and bind as `None`.
//!
//! What goes wrong without the wrappers:
//!
//! - SQLx's `Vec<T>` refuses Kinetica's arrays, which are numbered from 0.
//! - A `DATETIME` read as a `NaiveDateTime`, or bound as one, loses its
//!   milliseconds.
//! - Kinetica refuses `uuid::Uuid`, `NaiveTime`, decimal and `Vec<u8>` (`BYTEA`)
//!   parameters.
//! - SQLx binds `serde_json::Value` and `sqlx::types::Json` as `JSONB`, which
//!   Kinetica refuses as invalid JSON.
//! - A plain `INSERT` refuses a vector as text, and everything else refuses
//!   one as raw floats.
//!
//! # Parameters
//!
//! Kinetica reads every parameter after a statement's first as text, whatever
//! type SQLx sends it as. A second integer, float or date parameter therefore
//! fails to convert in an `INSERT`. In a `WHERE` clause it matches nothing, so
//! an `UPDATE` or `DELETE` reports success and changes no rows. A literal
//! beside such a parameter can be stored as NULL. Text parameters work in every
//! position, and Kinetica converts each to its column's type, so bind every
//! parameter through a wrapper, which sends text:
//!
//! ```no_run
//! use gpudb_api::sqlx_support::{KineticaPool, KineticaText, KineticaTimestamp};
//!
//! async fn record_login(pool: &KineticaPool, user_id: i64, active: bool) -> Result<u64, sqlx::Error> {
//!     let result = sqlx::query("UPDATE users SET active = $1, last_login = $2 WHERE id = $3")
//!         .bind(KineticaText(active))
//!         .bind(KineticaTimestamp(chrono::Utc::now()))
//!         .bind(KineticaText(user_id))
//!         .execute(pool)
//!         .await?;
//!     Ok(result.rows_affected())
//! }
//! ```
//!
//! # JSON, Arrays and Vectors
//!
//! Each is inserted, updated, filtered on and deleted by with ordinary SQL,
//! binding the wrappers above. Measured against Kinetica 7.2:
//!
//! - **JSON.** Bind documents with [`KineticaJson`], which takes any
//!   `Serialize` value; read them as `KineticaJson<T>` for any `DeserializeOwned`
//!   `T`. `JSON_VALUE(col, $1)` (whose path may be a parameter), `JSON_QUERY`,
//!   `JSON_EXISTS`, `JSON_LENGTH`, `JSON_TYPE`, `IS_JSON`, `JSON_OBJECT` and
//!   `JSON_ARRAY` work, with nested paths in `GROUP BY` and `JOIN` too. A SQL
//!   `NULL` and the JSON `null` stay distinct. Declare JSON columns as `JSON`:
//!   `VARCHAR(json)` and `STRING(json)` are plain strings on the server.
//! - **Full-text search.** A `JSON(text_search)` column, on a server with
//!   `enable_text_search`, is searched by key with `text_match(col, $1)`
//!   (`bool`) and ranked with `text_match_bm25` or `text_match_bm25_global`
//!   (`f64`), the query bound as `KineticaText("customer.tier:Enterprise")`.
//!   Do not UPDATE such a column: on Kinetica 7.2.3.21 that crashes a worker
//!   rank. Delete the row and insert it again.
//! - **Arrays.** Bind with [`KineticaArray`], and use
//!   `KineticaArray<Option<T>>` for arrays holding nulls. `ARRAY_CONTAINS`,
//!   `ARRAY_CONTAINS_ANY`, `ARRAY_CONTAINS_ALL`, `ARRAY_LENGTH`, `col[i]`
//!   (numbered from 1), `ARRAY_APPEND`, `ARRAY_CONCAT`, `ARRAY_SLICE` and
//!   `ARRAY_TO_STRING` work; arrays they compute arrive as JSON text, which
//!   `KineticaArray` reads too. A numeric `ARRAY_CONTAINS(col, $1)` finds
//!   nothing whatever is bound: write `ARRAY_CONTAINS_ANY(col, $1)` with a
//!   one-element `KineticaArray`. A fixed size, as in `INTEGER[3]`, is not
//!   enforced, and an infinite float is refused.
//! - **Vectors.** A plain `INSERT ... VALUES ($n)` takes [`KineticaVector`];
//!   `UPDATE ... SET v = $n`, `VECTOR($n, dims)` and function arguments take
//!   `KineticaText(vector.as_slice())`. `L1_DISTANCE`, `L2_DISTANCE`,
//!   `COSINE_DISTANCE`, `DOT_PRODUCT` and `L2_NORM` return `f32`, as does
//!   `v <-> CAST($1 AS VECTOR(n))`; a bare `v <-> $1` is refused.
//!
//! ```no_run
//! use gpudb_api::sqlx_support::{
//!     KineticaArray, KineticaJson, KineticaPool, KineticaText, KineticaVector,
//! };
//! use serde::{Deserialize, Serialize};
//! use sqlx::Row;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Profile {
//!     name: String,
//!     score: f64,
//! }
//!
//! async fn nearest_tagged(pool: &KineticaPool, tag: &str, probe: &[f32]) -> Result<Vec<(i64, Profile)>, sqlx::Error> {
//!     sqlx::query("INSERT INTO docs (id, profile, tags, embedding) VALUES ($1, $2, $3, $4)")
//!         .bind(KineticaText(1_i64))
//!         .bind(KineticaJson(Profile { name: "first".into(), score: 0.5 }))
//!         .bind(KineticaArray(vec![tag.to_string()]))
//!         .bind(KineticaVector(probe.to_vec()))
//!         .execute(pool)
//!         .await?;
//!
//!     let rows = sqlx::query(
//!         "SELECT id, profile FROM docs WHERE ARRAY_CONTAINS(tags, $1)
//!          ORDER BY COSINE_DISTANCE(embedding, $2) LIMIT 5",
//!     )
//!     .bind(KineticaText(tag))
//!     .bind(KineticaText(probe))
//!     .fetch_all(pool)
//!     .await?;
//!     Ok(rows
//!         .iter()
//!         .map(|row| (row.get("id"), row.get::<KineticaJson<Profile>, _>("profile").0))
//!         .collect())
//! }
//! ```
//!
//! Read arrays through `sqlx::query`: in the text form `sqlx::raw_sql` returns,
//! Kinetica writes `null` in capitals wherever it occurs in a string element.
//!
//! # Compile-Time Checked Queries
//!
//! SQLx supports compile-time checked queries with the `query!` macro. However, this
//! requires a live database connection during compilation. For Kinetica, you can:
//!
//! 1. Use the offline mode with `sqlx prepare`
//! 2. Use runtime queries with `sqlx::query()`
//!
//! ```no_run
//! use gpudb_api::sqlx_support::KineticaPool;
//!
//! // Runtime query (always works)
//! async fn get_users(pool: &KineticaPool) -> Result<Vec<(i64, String)>, sqlx::Error> {
//!     let rows = sqlx::query_as::<_, (i64, String)>("SELECT id, name FROM users")
//!         .fetch_all(pool)
//!         .await?;
//!     Ok(rows)
//! }
//!
//! // With a custom struct
//! #[derive(sqlx::FromRow)]
//! struct User {
//!     id: i64,
//!     name: String,
//!     email: Option<String>,
//! }
//!
//! async fn get_users_typed(pool: &KineticaPool) -> Result<Vec<User>, sqlx::Error> {
//!     sqlx::query_as::<_, User>("SELECT id, name, email FROM users")
//!         .fetch_all(pool)
//!         .await
//! }
//! ```
//!
//! # Transactions
//!
//! Kinetica's PostgreSQL protocol does not support transactions:
//! `pool.begin()` fails ("got unexpected connection status after attempting to
//! begin transaction"), and each statement applies when it runs.
//!
//! # Connection Pooling Best Practices
//!
//! For optimal performance with Kinetica:
//!
//! - Set `max_connections` based on your workload (default: 10)
//! - Use `min_connections` for warm connection pools
//! - Set appropriate timeouts for your network latency
//!
//! The server serves one session per thread, up to its
//! `postgres_proxy.max_threads` (64 by default), shared by every client. It
//! ends a session when its client sends PostgreSQL's Terminate message, which
//! `pool.close()` sends. A connection closed without it, as when a process
//! exits or a task panics with the connection open, keeps its session until
//! the server's `postgres_proxy.idle_connection_timeout` (300 seconds by
//! default). Pools that outgrow the server's sessions wait, then fail with
//! `sqlx::Error::PoolTimedOut`. SQLx 0.8's `pool.close()` can return while a
//! connection on its way back to the pool is still open: before a program
//! ends, close again once `pool.size()` shows it is back (see SQLX_API.md).
//!
//! ```no_run
//! use gpudb_api::sqlx_support::KineticaPoolOptions;
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), sqlx::Error> {
//! let pool = KineticaPoolOptions::new()
//!     .host("kinetica-cluster.example.com")
//!     .username("admin")
//!     .password("secure_password")
//!     .max_connections(20)
//!     .min_connections(5)
//!     .acquire_timeout(Duration::from_secs(30))
//!     .idle_timeout(Some(Duration::from_secs(600)))
//!     .connect()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Limitations
//!
//! Besides the types and parameters above:
//!
//! - Transactions are not supported.
//! - Sessions are limited by the server, and outlive connections that are not
//!   closed (see above).
//! - PostgreSQL-specific features, such as `LISTEN`/`NOTIFY`, are not available.
//! - Kinetica-specific SQL extensions should be written as raw SQL strings.
//!
//! For full fidelity on every Kinetica type, and for Kinetica-specific
//! operations (bulk insert, type management, KiFS), use the native API of the
//! [`Kinetica`](crate::Kinetica) client. It carries each value with its
//! column's properties.

use sqlx::postgres::{PgPool, PgPoolOptions, PgConnectOptions};

/// Values for the Kinetica types SQLx's own PostgreSQL types cannot read or
/// write over Kinetica's protocol, or cannot reliably: see their module.
pub use crate::sqlx_types::{
    FromKineticaText, KineticaArray, KineticaArrayElement, KineticaBytes, KineticaJson, KineticaText,
    KineticaTimestamp, KineticaVector, ToKineticaText,
};
use sqlx::Error as SqlxError;
use std::time::Duration;

/// Type alias for SQLx PostgreSQL connection pool configured for Kinetica.
///
/// This is simply a `PgPool` but named to make the intent clear in your code.
pub type KineticaPool = PgPool;

/// Type alias for SQLx PostgreSQL connection.
pub type KineticaConnection = sqlx::pool::PoolConnection<sqlx::Postgres>;

/// Type alias for SQLx PostgreSQL transaction.
///
/// Kinetica's PostgreSQL protocol does not support transactions, so one cannot
/// be begun; see the module docs.
pub type KineticaTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

/// Default PostgreSQL wire protocol port for Kinetica.
pub const DEFAULT_PORT: u16 = 5432;

/// Default database name (PostgreSQL compatibility).
pub const DEFAULT_DATABASE: &str = "postgres";

/// Builder for creating Kinetica connection pools with sensible defaults.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::KineticaPoolOptions;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let pool = KineticaPoolOptions::new()
///     .host("localhost")
///     .port(5432)
///     .username("admin")
///     .password("password")
///     .database("postgres")
///     .max_connections(10)
///     .min_connections(2)
///     .acquire_timeout(Duration::from_secs(30))
///     .connect()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct KineticaPoolOptions {
    host: String,
    port: u16,
    username: String,
    password: String,
    database: String,
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    ssl_mode: SslMode,
}

/// SSL mode for connections.
#[derive(Debug, Clone, Copy, Default)]
pub enum SslMode {
    /// No SSL (default for local development).
    #[default]
    Disable,
    /// Prefer SSL but allow non-SSL.
    Prefer,
    /// Require SSL.
    Require,
}

impl Default for KineticaPoolOptions {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: DEFAULT_PORT,
            username: String::new(),
            password: String::new(),
            database: DEFAULT_DATABASE.to_string(),
            max_connections: 10,
            min_connections: 0,
            acquire_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1800)),
            ssl_mode: SslMode::default(),
        }
    }
}

impl KineticaPoolOptions {
    /// Create a new `KineticaPoolOptions` with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the host address.
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    /// Set the port (default: 5432).
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the username for authentication.
    pub fn username(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self
    }

    /// Set the password for authentication.
    pub fn password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self
    }

    /// Set the database name (default: "postgres").
    pub fn database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self
    }

    /// Set the maximum number of connections in the pool (default: 10).
    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Set the minimum number of connections to maintain (default: 0).
    pub fn min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    /// Set the timeout for acquiring a connection from the pool (default: 30s).
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set the idle timeout for connections (default: 600s).
    /// Set to `None` to disable idle timeout.
    pub fn idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set the maximum lifetime of connections (default: 1800s).
    /// Set to `None` to disable max lifetime.
    pub fn max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Set the SSL mode for connections.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Build the connection URL string.
    pub fn connection_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }

    /// Build `PgConnectOptions` from the current settings.
    pub fn connect_options(&self) -> Result<PgConnectOptions, SqlxError> {
        let mut options = PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username(&self.username)
            .password(&self.password)
            .database(&self.database);

        // Set SSL mode
        options = match self.ssl_mode {
            SslMode::Disable => options.ssl_mode(sqlx::postgres::PgSslMode::Disable),
            SslMode::Prefer => options.ssl_mode(sqlx::postgres::PgSslMode::Prefer),
            SslMode::Require => options.ssl_mode(sqlx::postgres::PgSslMode::Require),
        };

        Ok(options)
    }

    /// Connect and create a connection pool.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::sqlx_support::KineticaPoolOptions;
    ///
    /// # async fn example() -> Result<(), sqlx::Error> {
    /// let pool = KineticaPoolOptions::new()
    ///     .host("localhost")
    ///     .username("admin")
    ///     .password("password")
    ///     .connect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(&self) -> Result<KineticaPool, SqlxError> {
        let connect_options = self.connect_options()?;

        let mut pool_options = PgPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(self.acquire_timeout);

        if let Some(idle_timeout) = self.idle_timeout {
            pool_options = pool_options.idle_timeout(idle_timeout);
        }

        if let Some(max_lifetime) = self.max_lifetime {
            pool_options = pool_options.max_lifetime(max_lifetime);
        }

        pool_options.connect_with(connect_options).await
    }
}

/// Connect to Kinetica with minimal configuration.
///
/// Uses default port (5432) and database ("postgres").
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::connect;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let pool = connect("localhost", "admin", "password").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(host: &str, username: &str, password: &str) -> Result<KineticaPool, SqlxError> {
    KineticaPoolOptions::new()
        .host(host)
        .username(username)
        .password(password)
        .connect()
        .await
}

/// Connect to Kinetica using a connection URL.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::connect_with_url;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let pool = connect_with_url("postgres://admin:password@localhost:5432/postgres").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_url(url: &str) -> Result<KineticaPool, SqlxError> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(url)
        .await
}

/// Connect to Kinetica using `PgConnectOptions`.
///
/// This gives you full control over connection options.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::connect_with_options;
/// use sqlx::postgres::PgConnectOptions;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let options = PgConnectOptions::new()
///     .host("localhost")
///     .port(5432)
///     .username("admin")
///     .password("password")
///     .database("postgres");
///
/// let pool = connect_with_options(options, 10).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_options(
    options: PgConnectOptions,
    max_connections: u32,
) -> Result<KineticaPool, SqlxError> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
}

/// Create a connection URL from components.
///
/// # Example
///
/// ```
/// use gpudb_api::sqlx_support::build_connection_url;
///
/// let url = build_connection_url("localhost", 5432, "admin", "password", "postgres");
/// assert_eq!(url, "postgres://admin:password@localhost:5432/postgres");
/// ```
pub fn build_connection_url(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    database: &str,
) -> String {
    format!("postgres://{}:{}@{}:{}/{}", username, password, host, port, database)
}

// ============================================================================
// Extension traits for ergonomic usage
// ============================================================================

/// Extension trait for `PgPool` with Kinetica-specific helpers.
pub trait KineticaPoolExt {
    /// Execute a raw SQL statement and return the number of rows affected.
    fn execute_sql(
        &self,
        sql: &str,
    ) -> impl std::future::Future<Output = Result<u64, SqlxError>> + Send;

    /// Check if a table exists.
    fn table_exists(
        &self,
        table_name: &str,
    ) -> impl std::future::Future<Output = Result<bool, SqlxError>> + Send;

    /// Get the count of rows in a table.
    fn count_rows(
        &self,
        table_name: &str,
        condition: Option<&str>,
    ) -> impl std::future::Future<Output = Result<i64, SqlxError>> + Send;

    /// Execute a query with pagination (LIMIT/OFFSET).
    ///
    /// Returns rows for the specified page along with the total count.
    ///
    /// # Arguments
    ///
    /// * `sql` - The base SQL query (without LIMIT/OFFSET)
    /// * `offset` - Number of rows to skip
    /// * `limit` - Maximum number of rows to return
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::sqlx_support::{KineticaPoolExt, KineticaPool};
    /// use sqlx::Row;
    ///
    /// # async fn example(pool: &KineticaPool) -> Result<(), sqlx::Error> {
    /// let (rows, total) = pool.fetch_page(
    ///     "SELECT * FROM users ORDER BY id",
    ///     0,   // offset
    ///     100  // limit
    /// ).await?;
    ///
    /// println!("Fetched {} rows out of {} total", rows.len(), total);
    /// for row in rows {
    ///     let id: i32 = row.get("id");
    ///     println!("id: {}", id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn fetch_page(
        &self,
        sql: &str,
        offset: i64,
        limit: i64,
    ) -> impl std::future::Future<Output = Result<(Vec<sqlx::postgres::PgRow>, i64), SqlxError>> + Send;

    /// Execute a query and process all results page by page with a callback.
    ///
    /// This is memory-efficient as it only loads one page at a time.
    ///
    /// # Arguments
    ///
    /// * `sql` - The base SQL query (without LIMIT/OFFSET)
    /// * `page_size` - Number of rows per page
    /// * `callback` - Function called for each page; return `false` to stop early
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::sqlx_support::{KineticaPoolExt, KineticaPool, SqlxPaginationStats};
    /// use sqlx::Row;
    ///
    /// # async fn example(pool: &KineticaPool) -> Result<(), sqlx::Error> {
    /// let stats = pool.fetch_all_pages(
    ///     "SELECT * FROM large_table ORDER BY id",
    ///     10000,
    ///     |rows, page_num| {
    ///         println!("Processing page {} with {} rows", page_num, rows.len());
    ///         for row in &rows {
    ///             let id: i32 = row.get("id");
    ///             // Process row...
    ///         }
    ///         Ok(true) // Continue to next page
    ///     }
    /// ).await?;
    ///
    /// println!("Processed {} pages, {} total rows", stats.pages_fetched, stats.rows_fetched);
    /// # Ok(())
    /// # }
    /// ```
    fn fetch_all_pages<F>(
        &self,
        sql: &str,
        page_size: i64,
        callback: F,
    ) -> impl std::future::Future<Output = Result<SqlxPaginationStats, SqlxError>> + Send
    where
        F: FnMut(Vec<sqlx::postgres::PgRow>, u64) -> Result<bool, SqlxError> + Send;

    /// Fetch all rows from a query, automatically handling pagination.
    ///
    /// **Warning**: This loads all results into memory. For very large result sets,
    /// use `fetch_all_pages` to process results in batches.
    ///
    /// # Arguments
    ///
    /// * `sql` - The base SQL query (without LIMIT/OFFSET)
    /// * `page_size` - Number of rows to fetch per page
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::sqlx_support::{KineticaPoolExt, KineticaPool};
    /// use sqlx::Row;
    ///
    /// # async fn example(pool: &KineticaPool) -> Result<(), sqlx::Error> {
    /// let rows = pool.fetch_all_paginated(
    ///     "SELECT * FROM users ORDER BY id",
    ///     10000
    /// ).await?;
    ///
    /// println!("Fetched {} total rows", rows.len());
    /// # Ok(())
    /// # }
    /// ```
    fn fetch_all_paginated(
        &self,
        sql: &str,
        page_size: i64,
    ) -> impl std::future::Future<Output = Result<Vec<sqlx::postgres::PgRow>, SqlxError>> + Send;
}

/// Statistics from a paginated SQLx query execution.
#[derive(Debug, Clone)]
pub struct SqlxPaginationStats {
    /// Total number of records in the full result set.
    pub total_records: i64,
    /// Number of pages fetched.
    pub pages_fetched: u64,
    /// Total rows fetched across all pages.
    pub rows_fetched: i64,
}

impl KineticaPoolExt for KineticaPool {
    async fn execute_sql(&self, sql: &str) -> Result<u64, SqlxError> {
        let result = sqlx::query(sql).execute(self).await?;
        Ok(result.rows_affected())
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, SqlxError> {
        // Parse schema.table or just table
        let (schema, table) = if let Some(dot_pos) = table_name.find('.') {
            (&table_name[..dot_pos], &table_name[dot_pos + 1..])
        } else {
            ("", table_name)
        };

        let sql = if schema.is_empty() {
            format!(
                "SELECT COUNT(*) FROM ki_catalog.ki_tables WHERE table_name = '{}'",
                table
            )
        } else {
            format!(
                "SELECT COUNT(*) FROM ki_catalog.ki_tables WHERE schema_name = '{}' AND table_name = '{}'",
                schema, table
            )
        };

        let row: (i64,) = sqlx::query_as(&sql).fetch_one(self).await?;
        Ok(row.0 > 0)
    }

    async fn count_rows(&self, table_name: &str, condition: Option<&str>) -> Result<i64, SqlxError> {
        let sql = match condition {
            Some(cond) => format!("SELECT COUNT(*) FROM {} WHERE {}", table_name, cond),
            None => format!("SELECT COUNT(*) FROM {}", table_name),
        };

        let row: (i64,) = sqlx::query_as(&sql).fetch_one(self).await?;
        Ok(row.0)
    }

    async fn fetch_page(
        &self,
        sql: &str,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<sqlx::postgres::PgRow>, i64), SqlxError> {
        // Get total count by wrapping the query
        // Note: This assumes the SQL is a SELECT query
        let count_sql = format!("SELECT COUNT(*) FROM ({}) AS __count_subquery", sql);
        let total: (i64,) = sqlx::query_as(&count_sql).fetch_one(self).await?;

        // Fetch the page
        let page_sql = format!("{} LIMIT {} OFFSET {}", sql, limit, offset);
        let rows = sqlx::query(&page_sql).fetch_all(self).await?;

        Ok((rows, total.0))
    }

    async fn fetch_all_pages<F>(
        &self,
        sql: &str,
        page_size: i64,
        mut callback: F,
    ) -> Result<SqlxPaginationStats, SqlxError>
    where
        F: FnMut(Vec<sqlx::postgres::PgRow>, u64) -> Result<bool, SqlxError> + Send,
    {
        // Get total count
        let count_sql = format!("SELECT COUNT(*) FROM ({}) AS __count_subquery", sql);
        let total: (i64,) = sqlx::query_as(&count_sql).fetch_one(self).await?;
        let total_records = total.0;

        let mut offset = 0i64;
        let mut page_num = 0u64;
        let mut rows_fetched = 0i64;

        while offset < total_records {
            let page_sql = format!("{} LIMIT {} OFFSET {}", sql, page_size, offset);
            let rows = sqlx::query(&page_sql).fetch_all(self).await?;

            let rows_in_page = rows.len() as i64;
            rows_fetched += rows_in_page;

            let should_continue = callback(rows, page_num)?;

            page_num += 1;
            offset += page_size;

            if !should_continue {
                break;
            }
        }

        Ok(SqlxPaginationStats {
            total_records,
            pages_fetched: page_num,
            rows_fetched,
        })
    }

    async fn fetch_all_paginated(
        &self,
        sql: &str,
        page_size: i64,
    ) -> Result<Vec<sqlx::postgres::PgRow>, SqlxError> {
        // Get total count
        let count_sql = format!("SELECT COUNT(*) FROM ({}) AS __count_subquery", sql);
        let total: (i64,) = sqlx::query_as(&count_sql).fetch_one(self).await?;
        let total_records = total.0;

        // If small enough, fetch all at once
        if total_records <= page_size {
            return sqlx::query(sql).fetch_all(self).await;
        }

        // Fetch in pages
        let mut all_rows = Vec::with_capacity(total_records as usize);
        let mut offset = 0i64;

        while offset < total_records {
            let page_sql = format!("{} LIMIT {} OFFSET {}", sql, page_size, offset);
            let rows = sqlx::query(&page_sql).fetch_all(self).await?;
            all_rows.extend(rows);
            offset += page_size;
        }

        Ok(all_rows)
    }
}

// ============================================================================
// Bulk Insert Support
// ============================================================================

/// Bulk inserter for SQLx that uses Kinetica's HTTP API for efficient data loading.
///
/// SQLx uses the PostgreSQL wire protocol which is less efficient for bulk loading
/// than Kinetica's native HTTP API with Avro encoding. This helper bridges the gap
/// by sending the records through a [`Kinetica`](crate::Kinetica) client, connected
/// and configured (TLS, HA failover and the rest) as for any other HTTP API use.
///
/// Uses `GenericRecord` for dynamic record creation.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::SqlxBulkInserter;
/// use gpudb_api::{GenericRecord, Kinetica, KineticaError, KineticaOptions};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // The HTTP API client the records are sent through
/// let mut options = KineticaOptions::new();
/// options.set_username("admin").set_password("password");
/// let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?);
///
/// let bulk_inserter = SqlxBulkInserter::new(kinetica);
///
/// // Fetch type schema and create records
/// let record_type = Arc::new(bulk_inserter.fetch_type("my_table").await?);
///
/// // Setting a column fails on a name the type does not have
/// let records = (0..10000)
///     .map(|i| {
///         let mut r = record_type.new_record();
///         r.set_by_name("id", i)?;
///         r.set_by_name("name", format!("user_{i}"))?;
///         Ok(r)
///     })
///     .collect::<Result<Vec<GenericRecord>, KineticaError>>()?;
///
/// // Bulk insert
/// let stats = bulk_inserter.insert("my_table", records, 10000).await?;
/// println!("Inserted {} records", stats.records_inserted);
/// # Ok(())
/// # }
/// ```
pub struct SqlxBulkInserter {
    kinetica: std::sync::Arc<crate::Kinetica>,
}

impl SqlxBulkInserter {
    /// Create a bulk inserter that sends records through `kinetica`, a client of
    /// Kinetica's HTTP API.
    pub fn new(kinetica: std::sync::Arc<crate::Kinetica>) -> Self {
        Self { kinetica }
    }

    /// Get a reference to the underlying Kinetica client.
    pub fn kinetica(&self) -> &std::sync::Arc<crate::Kinetica> {
        &self.kinetica
    }

    /// Fetch the Type for creating GenericRecords.
    ///
    /// Returns a `Type` that can be used to create `GenericRecord` instances.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # async fn example(inserter: &gpudb_api::sqlx_support::SqlxBulkInserter) -> Result<(), gpudb_api::KineticaError> {
    /// let record_type = Arc::new(inserter.fetch_type("my_table").await?);
    /// let mut record = record_type.new_record();
    /// record.set_by_name("id", 42)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_type(&self, table_name: &str) -> Result<crate::Type, crate::KineticaError> {
        crate::Type::from_table(&self.kinetica, table_name).await
    }

    /// Perform a bulk insert of GenericRecords with shard-aware multi-head ingest.
    ///
    /// This uses the high-performance `BulkInserter<GenericRecord>` which provides:
    /// - Shard-aware routing to worker nodes (multi-head ingest)
    /// - Parallel batch processing
    /// - Avro binary encoding for maximum throughput
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to insert into
    /// * `records` - Vector of GenericRecords to insert
    /// * `batch_size` - Number of records per batch (recommended: 10000-50000)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use gpudb_api::{GenericRecord, KineticaError};
    /// # use std::sync::Arc;
    /// # async fn example(inserter: &gpudb_api::sqlx_support::SqlxBulkInserter) -> Result<(), KineticaError> {
    /// let record_type = Arc::new(inserter.fetch_type("my_table").await?);
    /// // Setting a column fails on a name the type does not have
    /// let records = (0..1000).map(|i| {
    ///     let mut r = record_type.new_record();
    ///     r.set_by_name("id", i)?;
    ///     Ok(r)
    /// }).collect::<Result<Vec<GenericRecord>, KineticaError>>()?;
    /// let stats = inserter.insert("my_table", records, 10000).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn insert(
        &self,
        table_name: &str,
        records: Vec<crate::GenericRecord>,
        batch_size: usize,
    ) -> Result<crate::BulkInsertStats, crate::KineticaError> {
        self.insert_with_options(table_name, records, batch_size, 8, None).await
    }

    /// Perform a bulk insert of GenericRecords with custom options and shard-aware routing.
    ///
    /// Uses `BulkInserter<GenericRecord>` for optimal performance with multi-head ingest.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to insert into
    /// * `records` - Vector of GenericRecords to insert
    /// * `batch_size` - Number of records per batch (recommended: 10000-50000)
    /// * `num_workers` - Number of parallel workers (recommended: 4-16)
    /// * `options` - Optional BulkInserterOptions for controlling insert behavior
    pub async fn insert_with_options(
        &self,
        table_name: &str,
        records: Vec<crate::GenericRecord>,
        batch_size: usize,
        num_workers: usize,
        options: Option<crate::BulkInserterOptions>,
    ) -> Result<crate::BulkInsertStats, crate::KineticaError> {
        use std::time::Instant;

        if records.is_empty() {
            return Ok(crate::BulkInsertStats::default());
        }

        let start = Instant::now();
        let record_count = records.len();

        // Get the schema from the first record's type
        let record_type = records[0].record_type();
        let schema_str = record_type.schema_string();

        // Create shard-aware BulkInserter for optimal performance
        let inserter = crate::BulkInserter::<crate::GenericRecord>::with_schema(
            self.kinetica.clone(),
            table_name.to_string(),
            batch_size,
            num_workers,
            options,
            schema_str,
        ).await?;

        // Insert all records through the shard-aware BulkInserter
        for record in records {
            inserter.insert(std::sync::Arc::new(record))?;
        }

        // close() flushes and waits for every batch to land
        inserter.close().await?;

        // Get final counts
        let total_inserted = inserter.count_inserted();
        let total_updated = inserter.count_updated();
        let total_failed = inserter.error_count() as i64;

        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let records_per_second = if elapsed_ms > 0 {
            (record_count as f64) / (elapsed_ms as f64 / 1000.0)
        } else {
            record_count as f64
        };

        Ok(crate::BulkInsertStats {
            records_inserted: total_inserted,
            records_updated: total_updated,
            records_failed: total_failed,
            elapsed_ms,
            records_per_second,
        })
    }

    /// Perform a bulk insert with upsert mode (update on existing primary key).
    ///
    /// Updates existing records if primary key already exists.
    /// Uses shard-aware multi-head ingest for optimal performance.
    pub async fn upsert(
        &self,
        table_name: &str,
        records: Vec<crate::GenericRecord>,
        batch_size: usize,
    ) -> Result<crate::BulkInsertStats, crate::KineticaError> {
        let mut options = crate::BulkInserterOptions::new();
        options.set_update_on_existing_pk(true);
        self.insert_with_options(table_name, records, batch_size, 8, Some(options)).await
    }
}

// ============================================================================
// BatchingSqlxClient - Auto-batching SQLx client
// ============================================================================

/// Error type for BatchingSqlxClient operations
#[derive(Debug)]
pub enum BatchingSqlxError {
    /// Kinetica HTTP API error
    Kinetica(crate::KineticaError),
    /// SQLx error
    Sqlx(SqlxError),
    /// SQL parsing error
    Parse(String),
    /// Column not found
    ColumnNotFound(String),
}

impl std::fmt::Display for BatchingSqlxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchingSqlxError::Kinetica(e) => write!(f, "Kinetica error: {}", e),
            BatchingSqlxError::Sqlx(e) => write!(f, "SQLx error: {}", e),
            BatchingSqlxError::Parse(e) => write!(f, "Parse error: {}", e),
            BatchingSqlxError::ColumnNotFound(c) => write!(f, "Column not found: {}", c),
        }
    }
}

impl std::error::Error for BatchingSqlxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BatchingSqlxError::Kinetica(e) => Some(e),
            BatchingSqlxError::Sqlx(e) => Some(e),
            _ => None,
        }
    }
}

impl From<crate::KineticaError> for BatchingSqlxError {
    fn from(e: crate::KineticaError) -> Self {
        BatchingSqlxError::Kinetica(e)
    }
}

impl From<SqlxError> for BatchingSqlxError {
    fn from(e: SqlxError) -> Self {
        BatchingSqlxError::Sqlx(e)
    }
}

impl From<crate::sql::SqlError> for BatchingSqlxError {
    fn from(e: crate::sql::SqlError) -> Self {
        use crate::sql::SqlError;
        match e {
            SqlError::Kinetica(e) => BatchingSqlxError::Kinetica(e),
            SqlError::ColumnNotFound(c) => BatchingSqlxError::ColumnNotFound(c),
            SqlError::Parse(msg) => BatchingSqlxError::Parse(msg),
            other => BatchingSqlxError::Parse(other.to_string()),
        }
    }
}

/// Statistics from batching operations.
#[derive(Debug, Clone, Default)]
pub struct BatchingSqlxStats {
    /// Number of INSERT statements processed
    pub insert_count: u64,
    /// Total records inserted
    pub records_inserted: i64,
    /// Total records updated (for upsert)
    pub records_updated: i64,
    /// Number of flush operations
    pub flush_count: u64,
    /// Total elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Records per second throughput
    pub records_per_second: f64,
}

impl From<crate::BatchingStats> for BatchingSqlxStats {
    fn from(s: crate::BatchingStats) -> Self {
        Self {
            insert_count: s.insert_count,
            records_inserted: s.records_inserted,
            records_updated: s.records_updated,
            flush_count: s.flush_count,
            elapsed_ms: s.elapsed_ms,
            records_per_second: s.records_per_second,
        }
    }
}

/// A SQLx client that automatically batches INSERT statements and routes them
/// through `BulkInserter<GenericRecord>` for high-performance shard-aware ingestion.
///
/// This client intercepts INSERT statements executed in a loop, accumulates them,
/// and flushes them through the high-performance `BulkInserter` when the batch
/// size is reached. Non-INSERT statements are executed immediately via SQLx.
///
/// # Features
///
/// - **Auto-batching**: INSERT statements are automatically accumulated
/// - **Shard-aware routing**: Uses `BulkInserter<GenericRecord>` for multi-head ingest
/// - **Mixed statements**: SELECT, CREATE, etc. work normally alongside INSERTs
/// - **Performance**: Achieves high throughput for bulk INSERT operations
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::{BatchingSqlxClient, connect};
/// use gpudb_api::{Kinetica, KineticaOptions};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // The SQLx pool for other statements, and the HTTP API client INSERT
/// // batches are flushed through
/// let pool = connect("localhost", "admin", "password").await?;
/// let mut options = KineticaOptions::new();
/// options.set_username("admin").set_password("password");
/// let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?);
///
/// let mut client = BatchingSqlxClient::new(pool, kinetica, 10000, 8);
///
/// // Execute INSERT statements in a loop - they get auto-batched
/// for i in 0..100000 {
///     client.execute(&format!(
///         "INSERT INTO my_table (id, value) VALUES ({}, {})", i, i * 10
///     )).await?;
/// }
///
/// // Flush remaining records and get stats
/// let stats = client.close().await?;
/// println!("Inserted {} records at {:.0} rec/sec",
///     stats.records_inserted, stats.records_per_second);
/// # Ok(())
/// # }
/// ```
pub struct BatchingSqlxClient {
    pool: KineticaPool,
    batcher: crate::sql::InsertBatcher,
}

impl BatchingSqlxClient {
    /// Create a new batching SQLx client.
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLx PostgreSQL connection pool, for statements other than INSERT
    /// * `kinetica` - The Kinetica HTTP API client INSERT batches are flushed through
    /// * `batch_size` - Number of records to accumulate before flushing (recommended: 10000-50000)
    /// * `num_workers` - Number of parallel workers for BulkInserter (recommended: 4-16)
    pub fn new(
        pool: KineticaPool,
        kinetica: std::sync::Arc<crate::Kinetica>,
        batch_size: usize,
        num_workers: usize,
    ) -> Self {
        Self {
            pool,
            batcher: crate::sql::InsertBatcher::new(
                kinetica, batch_size, num_workers, None, crate::SqlHints::new(),
            ),
        }
    }

    /// Create a batching client with custom options (e.g., upsert mode).
    pub fn with_options(
        pool: KineticaPool,
        kinetica: std::sync::Arc<crate::Kinetica>,
        batch_size: usize,
        num_workers: usize,
        options: crate::BulkInserterOptions,
    ) -> Self {
        Self {
            pool,
            batcher: crate::sql::InsertBatcher::new(
                kinetica, batch_size, num_workers, Some(options), crate::SqlHints::new(),
            ),
        }
    }

    /// Create a batching client with SQL hints.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::sqlx_support::{BatchingSqlxClient, connect};
    /// use gpudb_api::{Kinetica, KineticaOptions, SqlHints};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = connect("localhost", "admin", "password").await?;
    /// let mut options = KineticaOptions::new();
    /// options.set_username("admin").set_password("password");
    /// let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?);
    ///
    /// let hints = SqlHints::new()
    ///     .update_on_existing_pk(true)
    ///     .distributed_operations(true);
    ///
    /// let mut client = BatchingSqlxClient::with_hints(pool, kinetica, 10000, 8, hints);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_hints(
        pool: KineticaPool,
        kinetica: std::sync::Arc<crate::Kinetica>,
        batch_size: usize,
        num_workers: usize,
        hints: crate::SqlHints,
    ) -> Self {
        // Convert hints to BulkInserterOptions for INSERT statements
        let mut options = None;
        crate::sql::apply_pk_hints(&mut options, &hints);

        Self {
            pool,
            batcher: crate::sql::InsertBatcher::new(kinetica, batch_size, num_workers, options, hints),
        }
    }

    /// Create a batching client with both BulkInserter options and SQL hints.
    pub fn with_options_and_hints(
        pool: KineticaPool,
        kinetica: std::sync::Arc<crate::Kinetica>,
        batch_size: usize,
        num_workers: usize,
        options: crate::BulkInserterOptions,
        hints: crate::SqlHints,
    ) -> Self {
        Self {
            pool,
            batcher: crate::sql::InsertBatcher::new(kinetica, batch_size, num_workers, Some(options), hints),
        }
    }

    /// Set SQL hints for the client.
    ///
    /// Note: For INSERT statements, hints like `update_on_existing_pk` are automatically
    /// applied through the BulkInserter options.
    pub fn set_hints(&mut self, hints: crate::SqlHints) {
        self.batcher.set_hints(hints);
    }

    /// Get the current SQL hints.
    pub fn hints(&self) -> &crate::SqlHints {
        self.batcher.hints()
    }

    /// Execute a SQL statement.
    ///
    /// - INSERT statements are accumulated and batched automatically
    /// - Other statements (SELECT, CREATE, etc.) are executed immediately via SQLx
    ///
    /// When the batch size is reached, records are automatically flushed
    /// through `BulkInserter<GenericRecord>` with shard-aware routing.
    pub async fn execute(&mut self, sql: &str) -> Result<Option<Vec<sqlx::postgres::PgRow>>, BatchingSqlxError> {
        if self.batcher.begin_statement(sql) {
            self.batcher.buffer_insert(sql).await?;
            return Ok(None);
        }

        // Execute non-INSERT statements via SQLx
        let rows = sqlx::query(sql)
            .fetch_all(&self.pool)
            .await?;
        Ok(Some(rows))
    }

    /// Flush all pending records for all tables.
    pub async fn flush(&mut self) -> Result<(), BatchingSqlxError> {
        Ok(self.batcher.flush().await?)
    }

    /// Flush all pending records and return final statistics.
    ///
    /// This should be called when you're done inserting to ensure all records are flushed.
    pub async fn close(mut self) -> Result<BatchingSqlxStats, BatchingSqlxError> {
        self.batcher.flush().await?;
        Ok(self.batcher.stats().into())
    }

    /// Get the number of pending (unflushed) records across all tables.
    pub fn pending_count(&self) -> usize {
        self.batcher.pending_count()
    }

    /// Get current statistics without closing.
    pub fn stats(&self) -> BatchingSqlxStats {
        self.batcher.stats().into()
    }

    /// Get a reference to the SQLx pool for direct queries.
    pub fn pool(&self) -> &KineticaPool {
        &self.pool
    }
}

// ============================================================================
// Utility functions
// ============================================================================

/// Parse a Kinetica connection URL from environment variables.
///
/// Looks for:
/// - `KINETICA_SQLX_URL` - Full connection URL
/// - Or individual: `KINETICA_HOST`, `KINETICA_PORT`, `KINETICA_USER`, `KINETICA_PASSWORD`
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::connection_url_from_env;
///
/// // Set KINETICA_SQLX_URL=postgres://admin:password@localhost:5432/postgres
/// // Or set KINETICA_HOST, KINETICA_USER, KINETICA_PASSWORD
///
/// let url = connection_url_from_env();
/// ```
pub fn connection_url_from_env() -> Option<String> {
    // First try the full URL
    if let Ok(url) = std::env::var("KINETICA_SQLX_URL") {
        return Some(url);
    }

    // Otherwise build from components
    let host = std::env::var("KINETICA_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("KINETICA_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let username = std::env::var("KINETICA_USER").ok()?;
    let password = std::env::var("KINETICA_PASSWORD").ok()?;
    let database = std::env::var("KINETICA_DATABASE").unwrap_or_else(|_| DEFAULT_DATABASE.to_string());

    Some(build_connection_url(&host, port, &username, &password, &database))
}

/// Connect to Kinetica using environment variables.
///
/// See [`connection_url_from_env`] for the expected environment variables.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::sqlx_support::connect_from_env;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = connect_from_env().await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_from_env() -> Result<KineticaPool, SqlxError> {
    let url = connection_url_from_env()
        .ok_or_else(|| SqlxError::Configuration(
            "Missing KINETICA_SQLX_URL or KINETICA_USER/KINETICA_PASSWORD environment variables".into()
        ))?;

    connect_with_url(&url).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connection_url() {
        let url = build_connection_url("localhost", 5432, "admin", "secret", "mydb");
        assert_eq!(url, "postgres://admin:secret@localhost:5432/mydb");
    }

    #[test]
    fn test_kinetica_pool_options_defaults() {
        let opts = KineticaPoolOptions::new();
        assert_eq!(opts.host, "localhost");
        assert_eq!(opts.port, 5432);
        assert_eq!(opts.database, "postgres");
        assert_eq!(opts.max_connections, 10);
    }

    #[test]
    fn test_kinetica_pool_options_builder() {
        let opts = KineticaPoolOptions::new()
            .host("db.example.com")
            .port(5433)
            .username("user")
            .password("pass")
            .database("mydb")
            .max_connections(20)
            .min_connections(5);

        assert_eq!(opts.host, "db.example.com");
        assert_eq!(opts.port, 5433);
        assert_eq!(opts.username, "user");
        assert_eq!(opts.password, "pass");
        assert_eq!(opts.database, "mydb");
        assert_eq!(opts.max_connections, 20);
        assert_eq!(opts.min_connections, 5);
    }

    #[test]
    fn test_connection_url_generation() {
        let opts = KineticaPoolOptions::new()
            .host("myhost")
            .port(5432)
            .username("admin")
            .password("secret")
            .database("testdb");

        assert_eq!(opts.connection_url(), "postgres://admin:secret@myhost:5432/testdb");
    }
}
