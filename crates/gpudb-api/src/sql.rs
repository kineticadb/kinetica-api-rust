//! SQL client and result parser for Kinetica's HTTP API
//!
//! Statements are sent to `/execute/sql` over the same [`Kinetica`] connection used by the
//! rest of the API, so they share its authentication, TLS and HA failover. For the
//! PostgreSQL wire protocol, see the `sqlx_support` module (feature `sqlx`).
//!
//! # Overview
//!
//! - **[`SqlClient`]** - Executes SQL, with paging support for large result sets
//! - **[`SqlResult`]** / **[`Row`]** - Wrap `RawExecuteSqlResponse` with convenient parsing methods
//! - **[`SqlHints`]** - Kinetica-specific execution options and inline hints
//! - **[`BatchingSqlClient`]** - Routes `INSERT` statements through `BulkInserter`
//!
//! # Quick Start
//!
//! ```no_run
//! use gpudb_api::{Kinetica, KineticaOptions, SqlClient};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let options = KineticaOptions::new();
//!     let kinetica = Arc::new(
//!         Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
//!     );
//!
//!     let client = SqlClient::new(kinetica);
//!     let mut result = client
//!         .execute("SELECT id, name FROM my_table WHERE value > 100 ORDER BY id LIMIT 10")
//!         .await?;
//!
//!     for row in result.rows()? {
//!         let id: i64 = row.get("id")?;
//!         let name: String = row.get("name")?;
//!         println!("id={}, name={}", id, name);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Result Parsing
//!
//! ```no_run
//! use gpudb_api::SqlResult;
//!
//! # fn example(mut result: SqlResult) -> Result<(), Box<dyn std::error::Error>> {
//! // Get total count
//! let total = result.total_records();
//!
//! // Check if there are more records
//! if result.has_more() {
//!     println!("More records available");
//! }
//!
//! // Get a single scalar value
//! let count: i64 = result.scalar()?;
//!
//! // Get a column as a vector
//! let names: Vec<String> = result.column("name")?;
//!
//! // Iterate over rows
//! for row in result.rows()? {
//!     let id: i64 = row.get("id")?;
//!     let name: String = row.get("name")?;
//!     // Process row...
//! }
//!
//! // Deserialize into structs
//! #[derive(serde::Deserialize)]
//! struct User { id: i64, name: String }
//! let users: Vec<User> = result.deserialize()?;
//! # Ok(())
//! # }
//! ```

use crate::{ExecuteSqlRequest, RawExecuteSqlResponse, Kinetica, KineticaError};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// Re-exported so existing `crate::sql::` paths keep working after the parser moved.
pub use crate::sql_insert_parser::{parse_insert_statement, parse_sql_value_to_record_value, ParsedInsert};

/// Error type for SQL operations
#[derive(Debug, Clone)]
pub enum SqlError {
    /// Query building error
    Build(String),
    /// Result parsing error
    Parse(String),
    /// Type conversion error
    TypeConversion(String),
    /// Column not found
    ColumnNotFound(String),
    /// Row index out of bounds
    RowIndexOutOfBounds { index: usize, len: usize },
    /// Empty result set when value expected
    EmptyResult,
    /// Underlying Kinetica error
    Kinetica(KineticaError),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::Build(msg) => write!(f, "Query build error: {msg}"),
            SqlError::Parse(msg) => write!(f, "Result parse error: {msg}"),
            SqlError::TypeConversion(msg) => write!(f, "Type conversion error: {msg}"),
            SqlError::ColumnNotFound(col) => write!(f, "Column not found: {col}"),
            SqlError::RowIndexOutOfBounds { index, len } => {
                write!(f, "Row index {index} out of bounds (len={len})")
            }
            SqlError::EmptyResult => write!(f, "Empty result set when value expected"),
            SqlError::Kinetica(e) => write!(f, "Kinetica error: {e}"),
        }
    }
}

impl std::error::Error for SqlError {}

impl From<KineticaError> for SqlError {
    fn from(e: KineticaError) -> Self {
        SqlError::Kinetica(e)
    }
}

impl From<serde_json::Error> for SqlError {
    fn from(e: serde_json::Error) -> Self {
        SqlError::Parse(e.to_string())
    }
}

/// Type alias for SQL operation results
pub type SqlOpResult<T> = Result<T, SqlError>;

// ============================================================================
// SQL Hints (Global Hints)
// ============================================================================

/// SQL hints that can be embedded in statements or passed as execute_sql options.
///
/// Kinetica supports SQL hints (KI_HINT) that can be added as comments within queries
/// to control query execution behavior. This struct provides a builder interface for
/// configuring hints programmatically.
///
/// # Hint Types
///
/// There are two ways to apply hints:
/// 1. **Inline hints**: Embedded in the SQL statement as comments (`/* KI_HINT_... */`)
/// 2. **Options hints**: Passed in the `options` map of the execute_sql API
///
/// This struct supports both approaches.
///
/// # Example
///
/// ```
/// use gpudb_api::SqlHints;
///
/// // Create hints for upsert operation
/// let hints = SqlHints::new()
///     .update_on_existing_pk(true)
///     .distributed_operations(true);
///
/// // Convert to inline SQL comment
/// let sql_with_hints = format!(
///     "INSERT INTO my_table {} (id, name) VALUES (1, 'test')",
///     hints.to_inline_hint()
/// );
/// // Result: "INSERT INTO my_table /* KI_HINT_UPDATE_ON_EXISTING_PK */ (id, name) VALUES (1, 'test')"
///
/// // Or convert to options map for execute_sql API
/// let options = hints.to_options_map();
/// ```
#[derive(Debug, Clone, Default)]
pub struct SqlHints {
    // DML-related hints
    update_on_existing_pk: Option<bool>,
    ignore_existing_pk: Option<bool>,

    // Batch size hint for INSERT/bulk operations
    batch_size: Option<usize>,

    // Query execution hints
    distributed_operations: Option<bool>,
    distributed_joins: Option<bool>,
    cost_based_optimization: Option<bool>,
    rule_based_optimization: Option<bool>,
    parallel_execution: Option<bool>,
    plan_cache: Option<bool>,
    results_caching: Option<bool>,
    late_materialization: Option<bool>,
    ssq_optimization: Option<bool>,
    preserve_dict_encoding: Option<bool>,

    // Result table hints
    ttl: Option<i32>,
    paging_table: Option<String>,
    paging_table_ttl: Option<i32>,

    // Prepared statement hints
    prepare_mode: Option<bool>,

    // Query parameters (for parameterized queries)
    query_parameters: Option<String>,

    // Validation hints
    validate_change_column: Option<bool>,

    // Approximate count hint
    use_approx_count_distinct: Option<bool>,

    // Custom hints (for hints not covered by the typed fields)
    custom_inline_hints: Vec<String>,
}

impl SqlHints {
    /// Create a new empty SqlHints builder.
    pub fn new() -> Self {
        Self::default()
    }

    // ========================================================================
    // DML Hints (INSERT, UPDATE)
    // ========================================================================

    /// Set the `update_on_existing_pk` hint (KI_HINT_UPDATE_ON_EXISTING_PK).
    ///
    /// When `true`, enables upsert functionality - inserts new records and updates
    /// existing ones matched by primary key. If the target table has no primary key,
    /// this hint will be ignored.
    ///
    /// # Example
    ///
    /// ```sql
    /// INSERT INTO table /* KI_HINT_UPDATE_ON_EXISTING_PK */ (id, value) VALUES (1, 'test')
    /// ```
    pub fn update_on_existing_pk(mut self, value: bool) -> Self {
        self.update_on_existing_pk = Some(value);
        self
    }

    /// Set the `ignore_existing_pk` hint (KI_HINT_IGNORE_EXISTING_PK).
    ///
    /// When `true`, discards duplicate records (by primary key) during insertion
    /// without throwing errors. Only effective when `update_on_existing_pk` is `false`.
    ///
    /// # Example
    ///
    /// ```sql
    /// INSERT INTO table /* KI_HINT_IGNORE_EXISTING_PK */ (id, value) VALUES (1, 'test')
    /// ```
    pub fn ignore_existing_pk(mut self, value: bool) -> Self {
        self.ignore_existing_pk = Some(value);
        self
    }

    /// Set the batch size hint (KI_HINT_BATCH_SIZE).
    ///
    /// Specifies the batch size for bulk INSERT operations. This controls how many
    /// records are sent to the server in each batch during ingestion.
    ///
    /// # Example
    ///
    /// ```sql
    /// INSERT INTO table /* KI_HINT_BATCH_SIZE(10000) */ (id, value) VALUES (1, 'test')
    /// ```
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Get the batch size hint.
    pub fn get_batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    // ========================================================================
    // Query Execution Hints
    // ========================================================================

    /// Set the `distributed_operations` hint.
    ///
    /// When `true`, enables the use of distributed operations in servicing the given query.
    pub fn distributed_operations(mut self, value: bool) -> Self {
        self.distributed_operations = Some(value);
        self
    }

    /// Set the `distributed_joins` hint.
    ///
    /// When `true`, enables the use of distributed joins in servicing the given query.
    pub fn distributed_joins(mut self, value: bool) -> Self {
        self.distributed_joins = Some(value);
        self
    }

    /// Set the `cost_based_optimization` hint.
    ///
    /// When `false`, disables the cost-based optimization of the given query.
    pub fn cost_based_optimization(mut self, value: bool) -> Self {
        self.cost_based_optimization = Some(value);
        self
    }

    /// Set the `rule_based_optimization` hint.
    ///
    /// When `false`, disables rule-based rewrite optimizations.
    pub fn rule_based_optimization(mut self, value: bool) -> Self {
        self.rule_based_optimization = Some(value);
        self
    }

    /// Set the `parallel_execution` hint.
    ///
    /// When `false`, disables the parallel step execution of the given query.
    pub fn parallel_execution(mut self, value: bool) -> Self {
        self.parallel_execution = Some(value);
        self
    }

    /// Set the `plan_cache` hint.
    ///
    /// When `false`, disables plan caching for the given query.
    pub fn plan_cache(mut self, value: bool) -> Self {
        self.plan_cache = Some(value);
        self
    }

    /// Set the `results_caching` hint.
    ///
    /// When `false`, disables caching of the results of the given query.
    pub fn results_caching(mut self, value: bool) -> Self {
        self.results_caching = Some(value);
        self
    }

    /// Set the `late_materialization` hint.
    ///
    /// When `true`, Joins/Filters results will always be late materialized.
    pub fn late_materialization(mut self, value: bool) -> Self {
        self.late_materialization = Some(value);
        self
    }

    /// Set the `ssq_optimization` hint.
    ///
    /// When `false`, scalar subqueries will be translated into joins.
    pub fn ssq_optimization(mut self, value: bool) -> Self {
        self.ssq_optimization = Some(value);
        self
    }

    /// Set the `preserve_dict_encoding` hint.
    ///
    /// When `true`, columns that were dict encoded in the source table will be
    /// dict encoded in the result table.
    pub fn preserve_dict_encoding(mut self, value: bool) -> Self {
        self.preserve_dict_encoding = Some(value);
        self
    }

    // ========================================================================
    // Result Table Hints
    // ========================================================================

    /// Set the TTL (time-to-live) in minutes for intermediate result tables.
    ///
    /// -1 indicates no timeout.
    pub fn ttl(mut self, minutes: i32) -> Self {
        self.ttl = Some(minutes);
        self
    }

    /// Set the paging table name for paginated queries.
    ///
    /// When specified, the system will create a paging table to hold query results
    /// between pagination calls.
    pub fn paging_table(mut self, table_name: impl Into<String>) -> Self {
        self.paging_table = Some(table_name.into());
        self
    }

    /// Set the TTL for the paging table in minutes.
    ///
    /// -1 indicates no timeout. Setting this option also creates a paging table.
    pub fn paging_table_ttl(mut self, minutes: i32) -> Self {
        self.paging_table_ttl = Some(minutes);
        self
    }

    // ========================================================================
    // Prepared Statement Hints
    // ========================================================================

    /// Set the `prepare_mode` hint.
    ///
    /// When `true`, compiles a query into an execution plan and saves it for later execution.
    pub fn prepare_mode(mut self, value: bool) -> Self {
        self.prepare_mode = Some(value);
        self
    }

    // ========================================================================
    // Query Parameters
    // ========================================================================

    /// Set query parameters in JSON array format.
    ///
    /// This can be used instead of binary-encoded data for parameterized queries.
    pub fn query_parameters(mut self, json: impl Into<String>) -> Self {
        self.query_parameters = Some(json.into());
        self
    }

    // ========================================================================
    // Validation Hints
    // ========================================================================

    /// Set the `validate_change_column` hint.
    ///
    /// When changing a column using ALTER TABLE, validate the change before applying it.
    pub fn validate_change_column(mut self, value: bool) -> Self {
        self.validate_change_column = Some(value);
        self
    }

    // ========================================================================
    // Approximate Count Hint
    // ========================================================================

    /// Set the `use_approx_count_distinct` hint.
    ///
    /// When `true`, for all COUNT_DISTINCT aggregate functions, use the faster
    /// but less accurate APPROXIMATE_COUNT_DISTINCT aggregate function.
    pub fn use_approx_count_distinct(mut self, value: bool) -> Self {
        self.use_approx_count_distinct = Some(value);
        self
    }

    // ========================================================================
    // Custom Inline Hints
    // ========================================================================

    /// Add a custom inline hint that will be included in the SQL comment.
    ///
    /// Use this for hints not covered by the typed methods, like:
    /// - `KI_HINT_GROUP_BY_PK`
    /// - `KI_HINT_INDEX(column)`
    /// - `KI_HINT_PROJECT_MATERIALIZED_VIEW`
    /// - `KI_HINT_JOBID_PREFIX(prefix)`
    ///
    /// # Example
    ///
    /// ```
    /// use gpudb_api::SqlHints;
    ///
    /// let hints = SqlHints::new()
    ///     .add_custom_hint("KI_HINT_GROUP_BY_PK")
    ///     .add_custom_hint("KI_HINT_INDEX(my_column)");
    /// ```
    pub fn add_custom_hint(mut self, hint: impl Into<String>) -> Self {
        self.custom_inline_hints.push(hint.into());
        self
    }

    // ========================================================================
    // Conversion Methods
    // ========================================================================

    /// Convert the hints to an inline SQL comment string.
    ///
    /// Returns an empty string if no inline hints are set. Only includes hints
    /// that can be expressed as inline comments (KI_HINT_*).
    ///
    /// # Example
    ///
    /// ```
    /// use gpudb_api::SqlHints;
    ///
    /// let hints = SqlHints::new()
    ///     .update_on_existing_pk(true)
    ///     .ignore_existing_pk(false);
    ///
    /// assert_eq!(hints.to_inline_hint(), "/* KI_HINT_UPDATE_ON_EXISTING_PK */");
    /// ```
    pub fn to_inline_hint(&self) -> String {
        let mut hints = Vec::new();

        // DML hints (only add if true for inline)
        if self.update_on_existing_pk == Some(true) {
            hints.push("KI_HINT_UPDATE_ON_EXISTING_PK".to_string());
        }
        if self.ignore_existing_pk == Some(true) {
            hints.push("KI_HINT_IGNORE_EXISTING_PK".to_string());
        }
        // Batch size hint
        if let Some(size) = self.batch_size {
            hints.push(format!("KI_HINT_BATCH_SIZE({size})"));
        }

        // Add custom inline hints
        hints.extend(self.custom_inline_hints.iter().cloned());

        if hints.is_empty() {
            String::new()
        } else {
            format!("/* {} */", hints.join(", "))
        }
    }

    /// Convert the hints to an options map for the execute_sql API.
    ///
    /// Returns a HashMap containing all set hints as key-value pairs suitable
    /// for passing to the execute_sql `options` parameter.
    pub fn to_options_map(&self) -> HashMap<String, String> {
        let mut options = HashMap::new();

        // DML hints
        if let Some(v) = self.update_on_existing_pk {
            options.insert("update_on_existing_pk".to_string(), v.to_string());
        }
        if let Some(v) = self.ignore_existing_pk {
            options.insert("ignore_existing_pk".to_string(), v.to_string());
        }
        if let Some(v) = self.batch_size {
            options.insert("batch_size".to_string(), v.to_string());
        }

        // Query execution hints
        if let Some(v) = self.distributed_operations {
            options.insert("distributed_operations".to_string(), v.to_string());
        }
        if let Some(v) = self.distributed_joins {
            options.insert("distributed_joins".to_string(), v.to_string());
        }
        if let Some(v) = self.cost_based_optimization {
            options.insert("cost_based_optimization".to_string(), v.to_string());
        }
        if let Some(v) = self.rule_based_optimization {
            options.insert("rule_based_optimization".to_string(), v.to_string());
        }
        if let Some(v) = self.parallel_execution {
            options.insert("parallel_execution".to_string(), v.to_string());
        }
        if let Some(v) = self.plan_cache {
            options.insert("plan_cache".to_string(), v.to_string());
        }
        if let Some(v) = self.results_caching {
            options.insert("results_caching".to_string(), v.to_string());
        }
        if let Some(v) = self.late_materialization {
            options.insert("late_materialization".to_string(), v.to_string());
        }
        if let Some(v) = self.ssq_optimization {
            options.insert("ssq_optimization".to_string(), v.to_string());
        }
        if let Some(v) = self.preserve_dict_encoding {
            options.insert("preserve_dict_encoding".to_string(), v.to_string());
        }

        // Result table hints
        if let Some(v) = self.ttl {
            options.insert("ttl".to_string(), v.to_string());
        }
        if let Some(ref v) = self.paging_table {
            options.insert("paging_table".to_string(), v.clone());
        }
        if let Some(v) = self.paging_table_ttl {
            options.insert("paging_table_ttl".to_string(), v.to_string());
        }

        // Prepared statement hints
        if let Some(v) = self.prepare_mode {
            options.insert("prepare_mode".to_string(), v.to_string());
        }

        // Query parameters
        if let Some(ref v) = self.query_parameters {
            options.insert("query_parameters".to_string(), v.clone());
        }

        // Validation hints
        if let Some(v) = self.validate_change_column {
            options.insert("validate_change_column".to_string(), v.to_string());
        }

        // Approximate count hint
        if let Some(v) = self.use_approx_count_distinct {
            options.insert("use_approx_count_distinct".to_string(), v.to_string());
        }

        options
    }

    /// Merge these hints into an existing options map.
    ///
    /// Hints from this struct will overwrite existing values in the map.
    pub fn merge_into(&self, options: &mut HashMap<String, String>) {
        for (key, value) in self.to_options_map() {
            options.insert(key, value);
        }
    }

    /// Check if any hints are set.
    pub fn is_empty(&self) -> bool {
        self.update_on_existing_pk.is_none()
            && self.ignore_existing_pk.is_none()
            && self.batch_size.is_none()
            && self.distributed_operations.is_none()
            && self.distributed_joins.is_none()
            && self.cost_based_optimization.is_none()
            && self.rule_based_optimization.is_none()
            && self.parallel_execution.is_none()
            && self.plan_cache.is_none()
            && self.results_caching.is_none()
            && self.late_materialization.is_none()
            && self.ssq_optimization.is_none()
            && self.preserve_dict_encoding.is_none()
            && self.ttl.is_none()
            && self.paging_table.is_none()
            && self.paging_table_ttl.is_none()
            && self.prepare_mode.is_none()
            && self.query_parameters.is_none()
            && self.validate_change_column.is_none()
            && self.use_approx_count_distinct.is_none()
            && self.custom_inline_hints.is_empty()
    }

    /// Get the update_on_existing_pk setting.
    pub fn get_update_on_existing_pk(&self) -> Option<bool> {
        self.update_on_existing_pk
    }

    /// Get the ignore_existing_pk setting.
    pub fn get_ignore_existing_pk(&self) -> Option<bool> {
        self.ignore_existing_pk
    }
}

// ============================================================================
// Result Parsing
// ============================================================================

/// A single row from a SQL result set.
///
/// Provides type-safe access to column values by name.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::Row;
///
/// # fn example(row: Row) -> Result<(), Box<dyn std::error::Error>> {
/// let id: i64 = row.get("id")?;
/// let name: String = row.get("name")?;
/// let score: Option<f64> = row.get_opt("score")?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Row {
    values: HashMap<String, Value>,
}

impl Row {
    /// Create a new row from a map of column values
    pub fn new(values: HashMap<String, Value>) -> Self {
        Row { values }
    }

    /// Get a column value, returning an error if the column doesn't exist or can't be converted.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The Rust type to convert to. Supported types:
    ///   - Integers: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
    ///   - Floats: `f32`, `f64`
    ///   - Boolean: `bool`
    ///   - String: `String`
    ///   - Any type implementing `DeserializeOwned`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::Row;
    ///
    /// # fn example(row: Row) -> Result<(), Box<dyn std::error::Error>> {
    /// let id: i64 = row.get("id")?;
    /// let name: String = row.get("name")?;
    /// let price: f64 = row.get("price")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get<T: DeserializeOwned>(&self, column: &str) -> SqlOpResult<T> {
        let value = self.values.get(column)
            .ok_or_else(|| SqlError::ColumnNotFound(column.to_string()))?;

        serde_json::from_value(value.clone())
            .map_err(|e| SqlError::TypeConversion(format!(
                "Failed to convert column '{column}': {e}"
            )))
    }

    /// Get a column value as an Option, returning None if the value is null.
    ///
    /// Returns an error only if the column doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::Row;
    ///
    /// # fn example(row: Row) -> Result<(), Box<dyn std::error::Error>> {
    /// let score: Option<f64> = row.get_opt("score")?;
    /// if let Some(s) = score {
    ///     println!("Score: {}", s);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_opt<T: DeserializeOwned>(&self, column: &str) -> SqlOpResult<Option<T>> {
        let value = self.values.get(column)
            .ok_or_else(|| SqlError::ColumnNotFound(column.to_string()))?;

        if value.is_null() {
            return Ok(None);
        }

        serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|e| SqlError::TypeConversion(format!(
                "Failed to convert column '{column}': {e}"
            )))
    }

    /// Get the raw JSON value for a column
    pub fn get_raw(&self, column: &str) -> SqlOpResult<&Value> {
        self.values.get(column)
            .ok_or_else(|| SqlError::ColumnNotFound(column.to_string()))
    }

    /// Check if a column exists in this row
    pub fn has_column(&self, column: &str) -> bool {
        self.values.contains_key(column)
    }

    /// Get all column names in this row
    pub fn columns(&self) -> Vec<&str> {
        self.values.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a column value is null
    pub fn is_null(&self, column: &str) -> SqlOpResult<bool> {
        let value = self.values.get(column)
            .ok_or_else(|| SqlError::ColumnNotFound(column.to_string()))?;
        Ok(value.is_null())
    }
}

/// Wrapper around `RawExecuteSqlResponse` with convenient parsing methods.
///
/// `SqlResult` provides ergonomic access to query results including:
/// - Row iteration
/// - Column extraction
/// - Scalar value retrieval
/// - Direct deserialization to structs
///
/// # Example
///
/// ```no_run
/// use gpudb_api::SqlResult;
///
/// # fn example(mut result: SqlResult) -> Result<(), Box<dyn std::error::Error>> {
/// // Check metadata
/// println!("Total records: {}", result.total_records());
/// println!("Has more: {}", result.has_more());
///
/// // Get a single value
/// let count: i64 = result.scalar()?;
///
/// // Extract a column
/// let names: Vec<String> = result.column("name")?;
///
/// // Iterate rows
/// for row in result.rows()? {
///     let id: i64 = row.get("id")?;
///     println!("ID: {}", id);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SqlResult {
    response: RawExecuteSqlResponse,
    /// Cached parsed JSON data
    parsed_data: Option<HashMap<String, Vec<Value>>>,
    /// Column name mapping (column_N -> actual name if available)
    column_mapping: HashMap<String, String>,
}

impl SqlResult {
    /// Create a new SqlResult from an RawExecuteSqlResponse
    pub fn new(response: RawExecuteSqlResponse) -> Self {
        SqlResult {
            response,
            parsed_data: None,
            column_mapping: HashMap::new(),
        }
    }

    /// Create a SqlResult with column name mapping
    ///
    /// The column_names slice should contain the column names in order,
    /// which will be mapped to column_1, column_2, etc.
    pub fn with_column_names(response: RawExecuteSqlResponse, column_names: &[&str]) -> Self {
        let mut mapping = HashMap::new();
        for (i, name) in column_names.iter().enumerate() {
            mapping.insert(format!("column_{}", i + 1), name.to_string());
        }
        SqlResult {
            response,
            parsed_data: None,
            column_mapping: mapping,
        }
    }

    /// Get the underlying RawExecuteSqlResponse
    pub fn inner(&self) -> &RawExecuteSqlResponse {
        &self.response
    }

    /// Consume and return the underlying RawExecuteSqlResponse
    pub fn into_inner(self) -> RawExecuteSqlResponse {
        self.response
    }

    /// Get the total number of records matching the query (before LIMIT)
    pub fn total_records(&self) -> i64 {
        self.response.total_number_of_records
    }

    /// Get the number of records affected by the query (INSERT/UPDATE/DELETE)
    pub fn count_affected(&self) -> i64 {
        self.response.count_affected
    }

    /// Check if there are more records available (for paginated queries)
    pub fn has_more(&self) -> bool {
        self.response.has_more_records
    }

    /// Get the paging table name (if any)
    pub fn paging_table(&self) -> Option<&str> {
        if self.response.paging_table.is_empty() {
            None
        } else {
            Some(&self.response.paging_table)
        }
    }

    /// Get info metadata from the response
    pub fn info(&self) -> &HashMap<String, String> {
        &self.response.info
    }

    /// Get the raw JSON response string
    pub fn json_response(&self) -> &str {
        &self.response.json_encoded_response
    }

    /// Parse and cache the JSON response data
    fn ensure_parsed(&mut self) -> SqlOpResult<()> {
        if self.parsed_data.is_some() {
            return Ok(());
        }

        if self.response.json_encoded_response.is_empty() {
            self.parsed_data = Some(HashMap::new());
            return Ok(());
        }

        let data: HashMap<String, Vec<Value>> = serde_json::from_str(&self.response.json_encoded_response)?;

        // Filter out metadata fields - Kinetica includes column_headers and column_datatypes
        // which are not data columns
        let filtered: HashMap<String, Vec<Value>> = data
            .into_iter()
            .filter(|(key, _)| key.starts_with("column_") && key != "column_headers" && key != "column_datatypes")
            .collect();

        self.parsed_data = Some(filtered);
        Ok(())
    }

    /// Get the number of rows in the result
    pub fn row_count(&mut self) -> SqlOpResult<usize> {
        self.ensure_parsed()?;

        if let Some(data) = &self.parsed_data {
            // Get count from first column
            if let Some((_, values)) = data.iter().next() {
                return Ok(values.len());
            }
        }
        Ok(0)
    }

    /// Get all column names in the result
    ///
    /// Returns the raw column names (column_1, column_2, etc.) unless
    /// column mapping was provided via `with_column_names`.
    pub fn column_names(&mut self) -> SqlOpResult<Vec<String>> {
        self.ensure_parsed()?;

        if let Some(data) = &self.parsed_data {
            let names: Vec<String> = data.keys().cloned().collect();
            // Apply mapping if available
            Ok(names.into_iter().map(|name| {
                self.column_mapping.get(&name).cloned().unwrap_or(name)
            }).collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Extract a single column as a Vec
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlResult;
    ///
    /// # fn example(mut result: SqlResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let ids: Vec<i64> = result.column("column_1")?;
    /// let names: Vec<String> = result.column("column_2")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn column<T: DeserializeOwned>(&mut self, name: &str) -> SqlOpResult<Vec<T>> {
        self.ensure_parsed()?;

        let col_name = self.resolve_column_name(name);

        if let Some(data) = &self.parsed_data {
            if let Some(values) = data.get(&col_name) {
                return values.iter()
                    .map(|v| serde_json::from_value(v.clone())
                        .map_err(|e| SqlError::TypeConversion(format!(
                            "Failed to convert column '{name}': {e}"
                        ))))
                    .collect();
            }
        }

        Err(SqlError::ColumnNotFound(name.to_string()))
    }

    /// Get a single scalar value from the first row, first column
    ///
    /// Useful for queries like `SELECT COUNT(*) FROM table`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlResult;
    ///
    /// # fn example(mut result: SqlResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let count: i64 = result.scalar()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn scalar<T: DeserializeOwned>(&mut self) -> SqlOpResult<T> {
        self.ensure_parsed()?;

        if let Some(data) = &self.parsed_data {
            // Get first column's first value
            if let Some((_, values)) = data.iter().next() {
                if let Some(value) = values.first() {
                    return serde_json::from_value(value.clone())
                        .map_err(|e| SqlError::TypeConversion(e.to_string()));
                }
            }
        }

        Err(SqlError::EmptyResult)
    }

    /// Get a value from a specific row and column
    pub fn get<T: DeserializeOwned>(&mut self, row: usize, column: &str) -> SqlOpResult<T> {
        self.ensure_parsed()?;

        let col_name = self.resolve_column_name(column);

        if let Some(data) = &self.parsed_data {
            if let Some(values) = data.get(&col_name) {
                if row >= values.len() {
                    return Err(SqlError::RowIndexOutOfBounds { index: row, len: values.len() });
                }
                return serde_json::from_value(values[row].clone())
                    .map_err(|e| SqlError::TypeConversion(format!(
                        "Failed to convert row {row} column '{column}': {e}"
                    )));
            }
        }

        Err(SqlError::ColumnNotFound(column.to_string()))
    }

    /// Iterate over rows in the result
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlResult;
    ///
    /// # fn example(mut result: SqlResult) -> Result<(), Box<dyn std::error::Error>> {
    /// for row in result.rows()? {
    ///     let id: i64 = row.get("column_1")?;
    ///     let name: String = row.get("column_2")?;
    ///     println!("id={}, name={}", id, name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn rows(&mut self) -> SqlOpResult<Vec<Row>> {
        self.ensure_parsed()?;

        let mut rows = Vec::new();

        if let Some(data) = &self.parsed_data {
            // Determine row count from first column
            let row_count = data.values().next().map(|v| v.len()).unwrap_or(0);

            for i in 0..row_count {
                let mut row_data = HashMap::new();
                for (col, values) in data {
                    if let Some(val) = values.get(i) {
                        // Apply column mapping
                        let col_name = self.column_mapping.get(col).cloned().unwrap_or_else(|| col.clone());
                        row_data.insert(col_name, val.clone());
                    }
                }
                rows.push(Row::new(row_data));
            }
        }

        Ok(rows)
    }

    /// Deserialize all rows into a Vec of structs
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlResult;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct User {
    ///     column_1: i64,  // id
    ///     column_2: String,  // name
    /// }
    ///
    /// # fn example(mut result: SqlResult) -> Result<(), Box<dyn std::error::Error>> {
    /// let users: Vec<User> = result.deserialize()?;
    /// for user in users {
    ///     println!("id={}, name={}", user.column_1, user.column_2);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn deserialize<T: DeserializeOwned>(&mut self) -> SqlOpResult<Vec<T>> {
        let rows = self.rows()?;
        rows.into_iter()
            .map(|row| {
                let json = serde_json::to_value(row.values)?;
                serde_json::from_value(json)
                    .map_err(|e| SqlError::TypeConversion(e.to_string()))
            })
            .collect()
    }

    /// Resolve column name (handles mapping)
    fn resolve_column_name(&self, name: &str) -> String {
        // Check if it's a mapped name (reverse lookup)
        for (raw, mapped) in &self.column_mapping {
            if mapped == name {
                return raw.clone();
            }
        }
        name.to_string()
    }
}

// ============================================================================
// SQL Client
// ============================================================================

/// High-level SQL client that wraps Kinetica with result parsing.
///
/// `SqlClient` executes SQL through Kinetica's `/execute/sql` endpoint and returns
/// [`SqlResult`]s, with helpers for paging through large result sets.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::{Kinetica, KineticaOptions, SqlClient};
/// use std::sync::Arc;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let options = KineticaOptions::new();
///     let kinetica = Arc::new(
///         Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
///     );
///
///     let client = SqlClient::new(kinetica);
///
///     // Execute a SELECT query
///     let result = client.execute("SELECT * FROM my_table LIMIT 10").await?;
///     println!("Found {} records", result.total_records());
///
///     // Fetch a specific page
///     let page = client.execute_paged("SELECT * FROM my_table ORDER BY id", 100, 50).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct SqlClient {
    kinetica: Arc<Kinetica>,
    /// Default encoding (json or binary)
    default_encoding: String,
    /// Default limit (-9999 for no limit)
    default_limit: i64,
}

impl SqlClient {
    /// Create a new SqlClient wrapping a Kinetica instance
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::{Kinetica, KineticaOptions, SqlClient};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let kinetica = Arc::new(
    ///     Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?
    /// );
    /// let client = SqlClient::new(kinetica);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(kinetica: Arc<Kinetica>) -> Self {
        SqlClient {
            kinetica,
            default_encoding: "json".to_string(),
            default_limit: -9999,
        }
    }

    /// Create a SqlClient with custom defaults
    pub fn with_defaults(kinetica: Arc<Kinetica>, encoding: &str, limit: i64) -> Self {
        SqlClient {
            kinetica,
            default_encoding: encoding.to_string(),
            default_limit: limit,
        }
    }

    /// Get a reference to the underlying Kinetica client
    pub fn kinetica(&self) -> &Arc<Kinetica> {
        &self.kinetica
    }

    /// Execute a raw SQL string
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlClient;
    ///
    /// # async fn example(client: SqlClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut result = client.execute("SELECT COUNT(*) FROM users WHERE active = true").await?;
    /// let count: i64 = result.scalar()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&self, sql: &str) -> SqlOpResult<SqlResult> {
        self.execute_with_options(sql, 0, self.default_limit, None).await
    }

    /// Execute a raw SQL string with custom limit and offset
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlClient;
    ///
    /// # async fn example(client: SqlClient) -> Result<(), Box<dyn std::error::Error>> {
    /// // Get records 10-19
    /// let result = client.execute_paged("SELECT * FROM users", 10, 10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_paged(&self, sql: &str, offset: i64, limit: i64) -> SqlOpResult<SqlResult> {
        self.execute_with_options(sql, offset, limit, None).await
    }

    /// Execute SQL with full control over options
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlClient;
    /// use std::collections::HashMap;
    ///
    /// # async fn example(client: SqlClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut options = HashMap::new();
    /// options.insert("parallel_execution".to_string(), "true".to_string());
    ///
    /// let result = client.execute_with_options(
    ///     "SELECT * FROM large_table",
    ///     0,
    ///     1000,
    ///     Some(options)
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_with_options(
        &self,
        sql: &str,
        offset: i64,
        limit: i64,
        options: Option<HashMap<String, String>>,
    ) -> SqlOpResult<SqlResult> {
        let request = ExecuteSqlRequest {
            statement: sql.to_string(),
            offset,
            limit,
            encoding: self.default_encoding.clone(),
            options: options.unwrap_or_default(),
            ..Default::default()
        };

        let response = self.kinetica.execute_sql_raw(&request).await?;
        Ok(SqlResult::new(response))
    }

    // ========================================================================
    // Paging Support
    // ========================================================================

    /// Fetch the next page of results using a paging table
    ///
    /// When a query returns `has_more_records = true`, Kinetica creates a temporary
    /// paging table containing the full result set. Use this method to fetch
    /// subsequent pages from that paging table.
    ///
    /// # Arguments
    ///
    /// * `paging_table` - The paging table name from `SqlResult::paging_table()`
    /// * `offset` - The offset within the paging table to start fetching from
    /// * `limit` - The maximum number of records to fetch
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlClient;
    ///
    /// # async fn example(client: SqlClient) -> Result<(), Box<dyn std::error::Error>> {
    /// // Initial query with a limit
    /// let mut result = client.execute_paged("SELECT * FROM large_table", 0, 1000).await?;
    /// let mut all_rows = result.rows()?.to_vec();
    ///
    /// // Fetch remaining pages if there are more records
    /// let paging_table = result.paging_table().map(|s| s.to_string());
    /// if let Some(pt) = paging_table {
    ///     let mut offset = 1000i64;
    ///     while result.has_more() {
    ///         result = client.fetch_next_page(&pt, offset, 1000).await?;
    ///         all_rows.extend(result.rows()?.to_vec());
    ///         offset += 1000;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_next_page(
        &self,
        paging_table: &str,
        offset: i64,
        limit: i64,
    ) -> SqlOpResult<SqlResult> {
        // Query the paging table directly
        let sql = format!("SELECT * FROM {paging_table}");
        self.execute_with_options(&sql, offset, limit, None).await
    }

    /// Execute a query and iterate through all pages automatically
    ///
    /// This method handles pagination transparently, fetching all pages from
    /// the paging table and returning an iterator over all results.
    ///
    /// **Warning**: This loads all results into memory. For very large result sets,
    /// consider using `execute_paged` and `fetch_next_page` to process results
    /// in batches.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to execute
    /// * `page_size` - The number of records to fetch per page
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlClient;
    ///
    /// # async fn example(client: SqlClient) -> Result<(), Box<dyn std::error::Error>> {
    /// // Fetch all records, 10000 at a time
    /// let mut result = client.execute_all("SELECT * FROM large_table", 10000).await?;
    ///
    /// println!("Total records fetched: {}", result.row_count()?);
    ///
    /// for row in result.rows()? {
    ///     let id: i64 = row.get("column_1")?;
    ///     // Process each row...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_all(&self, sql: &str, page_size: i64) -> SqlOpResult<SqlResult> {
        // Get first page
        let first_result = self.execute_with_options(sql, 0, page_size, None).await?;
        let total_records = first_result.total_records();

        // If all records fit in one page, return as-is
        if total_records <= page_size {
            return Ok(first_result);
        }

        // Accumulate all JSON data
        let mut all_json_data: HashMap<String, Vec<Value>> = HashMap::new();

        // Parse first page
        if !first_result.response.json_encoded_response.is_empty() {
            let data: HashMap<String, Vec<Value>> =
                serde_json::from_str(&first_result.response.json_encoded_response)?;
            for (key, values) in data {
                if key.starts_with("column_") && key != "column_headers" && key != "column_datatypes" {
                    all_json_data.entry(key).or_default().extend(values);
                }
            }
        }

        // Check if we have a paging table or need to use offset/limit
        let paging_table = first_result.paging_table().map(|s| s.to_string());

        // Fetch remaining pages
        let mut offset = page_size;

        while offset < total_records {
            let page_result = if let Some(ref pt) = paging_table {
                // Use paging table if available (more efficient)
                self.fetch_next_page(pt, offset, page_size).await?
            } else {
                // Fall back to offset/limit on original query
                self.execute_with_options(sql, offset, page_size, None).await?
            };

            // Parse and accumulate this page's data
            if !page_result.response.json_encoded_response.is_empty() {
                let data: HashMap<String, Vec<Value>> =
                    serde_json::from_str(&page_result.response.json_encoded_response)?;
                for (key, values) in data {
                    if key.starts_with("column_") && key != "column_headers" && key != "column_datatypes" {
                        all_json_data.entry(key).or_default().extend(values);
                    }
                }
            }

            offset += page_size;
        }

        // Create a combined response
        let combined_json = serde_json::to_string(&all_json_data)?;
        let combined_response = RawExecuteSqlResponse {
            count_affected: first_result.response.count_affected,
            response_schema_str: first_result.response.response_schema_str.clone(),
            binary_encoded_response: serde_bytes::ByteBuf::new(),
            json_encoded_response: combined_json,
            total_number_of_records: total_records,
            has_more_records: false, // We fetched everything
            paging_table: String::new(),
            info: first_result.response.info.clone(),
        };

        Ok(SqlResult::new(combined_response))
    }

    /// Execute a query and process results page by page with a callback
    ///
    /// This is the most memory-efficient way to process large result sets,
    /// as it only loads one page at a time.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to execute
    /// * `page_size` - The number of records to fetch per page
    /// * `callback` - A function called for each page of results
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::SqlClient;
    ///
    /// # async fn example(client: SqlClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut total_processed = 0u64;
    ///
    /// client.execute_paginated(
    ///     "SELECT * FROM large_table",
    ///     10000,
    ///     |mut page, page_num| {
    ///         println!("Processing page {}", page_num);
    ///         for row in page.rows()? {
    ///             let id: i64 = row.get("column_1")?;
    ///             // Process row...
    ///         }
    ///         Ok(true) // Return false to stop pagination early
    ///     }
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_paginated<F>(
        &self,
        sql: &str,
        page_size: i64,
        mut callback: F,
    ) -> SqlOpResult<PaginationStats>
    where
        F: FnMut(SqlResult, u64) -> SqlOpResult<bool>,
    {
        let mut page_num = 0u64;
        let mut total_rows_fetched = 0i64;

        // Get first page
        let mut first_result = self.execute_with_options(sql, 0, page_size, None).await?;
        let total_records = first_result.total_records();
        let paging_table = first_result.paging_table().map(|s| s.to_string());

        total_rows_fetched += first_result.row_count().unwrap_or(0) as i64;

        // Process first page
        let should_continue = callback(first_result, page_num)?;
        if !should_continue {
            return Ok(PaginationStats {
                total_records,
                pages_fetched: 1,
                rows_fetched: total_rows_fetched,
            });
        }

        // Fetch remaining pages
        let mut offset = page_size;

        while offset < total_records {
            page_num += 1;
            let mut page_result = if let Some(ref pt) = paging_table {
                // Use paging table if available (more efficient)
                self.fetch_next_page(pt, offset, page_size).await?
            } else {
                // Fall back to offset/limit on original query
                self.execute_with_options(sql, offset, page_size, None).await?
            };

            let rows_in_page = page_result.row_count().unwrap_or(0) as i64;
            total_rows_fetched += rows_in_page;

            let should_continue = callback(page_result, page_num)?;
            if !should_continue {
                break;
            }

            offset += page_size;
        }

        Ok(PaginationStats {
            total_records,
            pages_fetched: page_num + 1,
            rows_fetched: total_rows_fetched,
        })
    }
}

/// Statistics from a paginated query execution
#[derive(Debug, Clone)]
pub struct PaginationStats {
    /// Total number of records in the full result set
    pub total_records: i64,
    /// Number of pages fetched
    pub pages_fetched: u64,
    /// Total rows fetched across all pages
    pub rows_fetched: i64,
}

// ============================================================================
// Batching SQL Client - Auto-detects INSERT statements and batches them
// ============================================================================

/// Statistics from batched SQL execution
#[derive(Debug, Clone, Default)]
pub struct BatchingStats {
    /// Total INSERT statements executed
    pub insert_count: u64,
    /// Total records inserted via BulkInserter
    pub records_inserted: i64,
    /// Total records updated (for upsert mode)
    pub records_updated: i64,
    /// Number of batch flushes performed
    pub flush_count: u64,
    /// Total elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Records per second throughput
    pub records_per_second: f64,
}

/// Apply the primary-key hints (`update_on_existing_pk`, `ignore_existing_pk`) to the
/// BulkInserter options used for INSERT statements, creating the options if needed.
pub(crate) fn apply_pk_hints(options: &mut Option<crate::BulkInserterOptions>, hints: &SqlHints) {
    let update = hints.get_update_on_existing_pk() == Some(true);
    let ignore = hints.get_ignore_existing_pk() == Some(true);
    if update || ignore {
        let opts = options.get_or_insert_with(crate::BulkInserterOptions::new);
        if update {
            opts.set_update_on_existing_pk(true);
        }
        if ignore {
            opts.set_ignore_existing_pk(true);
        }
    }
}

/// INSERT batching shared by [`BatchingSqlClient`] and `sqlx_support::BatchingSqlxClient`.
///
/// Parses INSERT statements, buffers the rows per table as `GenericRecord`s, and flushes
/// them through `BulkInserter<GenericRecord>` when a table's buffer reaches the batch size.
/// The owning client decides how non-INSERT statements are executed.
pub(crate) struct InsertBatcher {
    kinetica: Arc<Kinetica>,
    batch_size: usize,
    num_workers: usize,
    options: Option<crate::BulkInserterOptions>,
    hints: SqlHints,

    // Per-table state
    table_buffers: HashMap<String, TableBuffer>,

    // Statistics
    start_time: Option<std::time::Instant>,
    total_insert_count: u64,
    total_records_inserted: i64,
    total_records_updated: i64,
    total_flush_count: u64,
}

struct TableBuffer {
    record_type: Arc<crate::Type>,
    records: Vec<crate::GenericRecord>,
}

impl InsertBatcher {
    pub(crate) fn new(
        kinetica: Arc<Kinetica>,
        batch_size: usize,
        num_workers: usize,
        options: Option<crate::BulkInserterOptions>,
        hints: SqlHints,
    ) -> Self {
        Self {
            kinetica,
            batch_size,
            num_workers,
            options,
            hints,
            table_buffers: HashMap::new(),
            start_time: None,
            total_insert_count: 0,
            total_records_inserted: 0,
            total_records_updated: 0,
            total_flush_count: 0,
        }
    }

    pub(crate) fn kinetica(&self) -> &Arc<Kinetica> {
        &self.kinetica
    }

    pub(crate) fn hints(&self) -> &SqlHints {
        &self.hints
    }

    pub(crate) fn set_hints(&mut self, hints: SqlHints) {
        apply_pk_hints(&mut self.options, &hints);
        self.hints = hints;
    }

    /// Start the throughput clock on the first statement and report whether `sql`
    /// is an INSERT that should be batched.
    pub(crate) fn begin_statement(&mut self, sql: &str) -> bool {
        if self.start_time.is_none() {
            self.start_time = Some(std::time::Instant::now());
        }
        sql.trim().to_uppercase().starts_with("INSERT INTO")
    }

    /// Parse an INSERT statement and buffer its rows, flushing the table if its
    /// buffer has reached the batch size.
    pub(crate) async fn buffer_insert(&mut self, sql: &str) -> SqlOpResult<()> {
        let parsed = parse_insert_statement(sql)?;

        self.total_insert_count += 1;

        // Get or create buffer for this table
        if !self.table_buffers.contains_key(&parsed.table_name) {
            let record_type = Arc::new(
                crate::Type::from_table(&self.kinetica, &parsed.table_name).await
                    .map_err(SqlError::Kinetica)?
            );
            self.table_buffers.insert(parsed.table_name.clone(), TableBuffer {
                record_type,
                records: Vec::with_capacity(self.batch_size),
            });
        }

        let buffer = self.table_buffers.get_mut(&parsed.table_name).unwrap();

        // Convert parsed values to GenericRecord
        for row_values in parsed.value_rows {
            let mut record = buffer.record_type.new_record();

            for (col_name, value_str) in parsed.columns.iter().zip(row_values.iter()) {
                let not_found = || SqlError::ColumnNotFound(col_name.clone());
                let col = buffer.record_type.get_column_by_name(col_name).ok_or_else(not_found)?;
                let value = parse_sql_value_to_record_value(value_str, col).map_err(SqlError::Parse)?;
                record.set_by_name(col_name, value).map_err(|_| not_found())?;
            }

            buffer.records.push(record);
        }

        // Check if we need to flush this table's buffer
        if buffer.records.len() >= self.batch_size {
            self.flush_table(&parsed.table_name).await?;
        }

        Ok(())
    }

    /// Flush a specific table's buffer through BulkInserter.
    async fn flush_table(&mut self, table_name: &str) -> SqlOpResult<()> {
        let buffer = match self.table_buffers.get_mut(table_name) {
            Some(b) if !b.records.is_empty() => b,
            _ => return Ok(()),
        };

        let records = std::mem::take(&mut buffer.records);
        let schema_str = buffer.record_type.schema_string();

        // Create BulkInserter and insert all records
        let inserter = crate::BulkInserter::<crate::GenericRecord>::with_schema(
            self.kinetica.clone(),
            table_name.to_string(),
            self.batch_size,
            self.num_workers,
            self.options.clone(),
            schema_str,
        ).await.map_err(SqlError::Kinetica)?;

        for record in records {
            inserter.insert(Arc::new(record)).map_err(|e| SqlError::Kinetica(e.into()))?;
        }

        inserter.close().await.map_err(SqlError::Kinetica)?;

        self.total_records_inserted += inserter.count_inserted();
        self.total_records_updated += inserter.count_updated();
        self.total_flush_count += 1;

        // Pre-allocate new buffer
        buffer.records = Vec::with_capacity(self.batch_size);

        Ok(())
    }

    /// Flush all pending records for all tables.
    pub(crate) async fn flush(&mut self) -> SqlOpResult<()> {
        let table_names: Vec<String> = self.table_buffers.keys().cloned().collect();
        for table_name in table_names {
            self.flush_table(&table_name).await?;
        }
        Ok(())
    }

    /// Number of pending (unflushed) records across all tables.
    pub(crate) fn pending_count(&self) -> usize {
        self.table_buffers.values().map(|b| b.records.len()).sum()
    }

    /// Statistics so far; throughput counts both inserted and updated records.
    pub(crate) fn stats(&self) -> BatchingStats {
        let elapsed_ms = self.start_time
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);
        let total_records = self.total_records_inserted + self.total_records_updated;
        let records_per_second = if elapsed_ms > 0 {
            (total_records as f64) / (elapsed_ms as f64 / 1000.0)
        } else {
            total_records as f64
        };

        BatchingStats {
            insert_count: self.total_insert_count,
            records_inserted: self.total_records_inserted,
            records_updated: self.total_records_updated,
            flush_count: self.total_flush_count,
            elapsed_ms,
            records_per_second,
        }
    }
}

/// A SQL client that automatically batches INSERT statements for high-performance bulk loading.
///
/// When you execute INSERT statements in a loop, this client accumulates them and
/// automatically flushes through `BulkInserter<GenericRecord>` when the batch size is reached.
/// This provides massive performance improvements over executing individual INSERT statements.
///
/// # Example
///
/// ```no_run
/// use gpudb_api::{Kinetica, KineticaOptions, BatchingSqlClient};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let kinetica = Arc::new(Kinetica::new(
///     vec!["http://localhost:9191".to_string()],
///     KineticaOptions::new()
/// ).await?);
///
/// // Create batching client with 10000 record batch size
/// let mut client = BatchingSqlClient::new(kinetica, 10000, 8);
///
/// // Execute 1 million INSERT statements - they are automatically batched!
/// for i in 0..1_000_000 {
///     client.execute(&format!(
///         "INSERT INTO my_table (id, name, value) VALUES ({}, 'user_{}', {})",
///         i, i, i as f64 * 1.5
///     )).await?;
/// }
///
/// // Flush any remaining records
/// let stats = client.close().await?;
/// println!("Inserted {} records at {:.0} rec/sec",
///     stats.records_inserted, stats.records_per_second);
/// # Ok(())
/// # }
/// ```
pub struct BatchingSqlClient {
    batcher: InsertBatcher,
}

impl BatchingSqlClient {
    /// Create a new batching SQL client.
    ///
    /// # Arguments
    ///
    /// * `kinetica` - The Kinetica client
    /// * `batch_size` - Number of records to accumulate before flushing (recommended: 10000-50000)
    /// * `num_workers` - Number of parallel workers for BulkInserter (recommended: 4-16)
    pub fn new(kinetica: Arc<Kinetica>, batch_size: usize, num_workers: usize) -> Self {
        Self {
            batcher: InsertBatcher::new(kinetica, batch_size, num_workers, None, SqlHints::new()),
        }
    }

    /// Create a batching client with custom options (e.g., upsert mode).
    pub fn with_options(
        kinetica: Arc<Kinetica>,
        batch_size: usize,
        num_workers: usize,
        options: crate::BulkInserterOptions,
    ) -> Self {
        Self {
            batcher: InsertBatcher::new(kinetica, batch_size, num_workers, Some(options), SqlHints::new()),
        }
    }

    /// Create a batching client with SQL hints.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gpudb_api::{Kinetica, KineticaOptions, BatchingSqlClient, SqlHints};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let kinetica = Arc::new(Kinetica::new(
    ///     vec!["http://localhost:9191".to_string()],
    ///     KineticaOptions::new()
    /// ).await?);
    ///
    /// let hints = SqlHints::new()
    ///     .update_on_existing_pk(true)
    ///     .distributed_operations(true);
    ///
    /// let mut client = BatchingSqlClient::with_hints(kinetica, 10000, 8, hints);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_hints(
        kinetica: Arc<Kinetica>,
        batch_size: usize,
        num_workers: usize,
        hints: SqlHints,
    ) -> Self {
        // Convert hints to BulkInserterOptions for INSERT statements
        let mut options = None;
        apply_pk_hints(&mut options, &hints);
        Self {
            batcher: InsertBatcher::new(kinetica, batch_size, num_workers, options, hints),
        }
    }

    /// Create a batching client with both BulkInserter options and SQL hints.
    pub fn with_options_and_hints(
        kinetica: Arc<Kinetica>,
        batch_size: usize,
        num_workers: usize,
        options: crate::BulkInserterOptions,
        hints: SqlHints,
    ) -> Self {
        Self {
            batcher: InsertBatcher::new(kinetica, batch_size, num_workers, Some(options), hints),
        }
    }

    /// Set SQL hints for non-INSERT statements.
    ///
    /// Note: For INSERT statements, hints like `update_on_existing_pk` are automatically
    /// applied through the BulkInserter options. Use `with_hints()` constructor to set
    /// both at creation time.
    pub fn set_hints(&mut self, hints: SqlHints) {
        self.batcher.set_hints(hints);
    }

    /// Get the current SQL hints.
    pub fn hints(&self) -> &SqlHints {
        self.batcher.hints()
    }

    /// Execute a SQL statement.
    ///
    /// - INSERT statements are accumulated and batched automatically
    /// - Other statements (SELECT, CREATE, etc.) are executed immediately with hints applied
    ///
    /// When the batch size is reached, records are automatically flushed
    /// through `BulkInserter<GenericRecord>` with shard-aware routing.
    ///
    /// For non-INSERT statements, any configured SQL hints (via `set_hints()` or `with_hints()`)
    /// will be applied to the execute_sql request.
    pub async fn execute(&mut self, sql: &str) -> SqlOpResult<Option<SqlResult>> {
        if self.batcher.begin_statement(sql) {
            self.batcher.buffer_insert(sql).await?;
            return Ok(None);
        }

        // Execute non-INSERT statements directly with hints
        let client = SqlClient::new(self.batcher.kinetica().clone());
        let hints = self.batcher.hints();
        let result = if hints.is_empty() {
            client.execute(sql).await?
        } else {
            client.execute_with_options(sql, 0, -9999, Some(hints.to_options_map())).await?
        };
        Ok(Some(result))
    }

    /// Flush all pending records for all tables.
    pub async fn flush(&mut self) -> SqlOpResult<()> {
        self.batcher.flush().await
    }

    /// Flush all pending records and return final statistics.
    ///
    /// This should be called when you're done inserting to ensure all records are flushed.
    pub async fn close(mut self) -> SqlOpResult<BatchingStats> {
        self.batcher.flush().await?;
        Ok(self.batcher.stats())
    }

    /// Get current statistics without closing.
    pub fn stats(&self) -> BatchingStats {
        self.batcher.stats()
    }

    /// Get the number of pending (unflushed) records across all tables.
    pub fn pending_count(&self) -> usize {
        self.batcher.pending_count()
    }

    /// Get a reference to the underlying Kinetica client.
    pub fn kinetica(&self) -> &Arc<Kinetica> {
        self.batcher.kinetica()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Row Unit Tests
    // ========================================================================

    #[test]
    fn test_row_get() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Number(42.into()));
        values.insert("name".to_string(), Value::String("Alice".to_string()));

        let row = Row::new(values);

        let id: i64 = row.get("id").unwrap();
        let name: String = row.get("name").unwrap();

        assert_eq!(id, 42);
        assert_eq!(name, "Alice");
    }

    #[test]
    fn test_row_get_opt() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Number(42.into()));
        values.insert("nullable".to_string(), Value::Null);

        let row = Row::new(values);

        let id: Option<i64> = row.get_opt("id").unwrap();
        let nullable: Option<String> = row.get_opt("nullable").unwrap();

        assert_eq!(id, Some(42));
        assert_eq!(nullable, None);
    }

    #[test]
    fn test_row_column_not_found() {
        let row = Row::new(HashMap::new());
        let result: SqlOpResult<i64> = row.get("missing");

        assert!(matches!(result, Err(SqlError::ColumnNotFound(_))));
    }

    #[test]
    fn test_row_type_conversion_error() {
        let mut values = HashMap::new();
        values.insert("name".to_string(), Value::String("not a number".to_string()));

        let row = Row::new(values);
        let result: SqlOpResult<i64> = row.get("name");

        assert!(matches!(result, Err(SqlError::TypeConversion(_))));
    }

    #[test]
    fn test_row_has_column() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Number(1.into()));

        let row = Row::new(values);

        assert!(row.has_column("id"));
        assert!(!row.has_column("missing"));
    }

    #[test]
    fn test_row_is_null() {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Number(1.into()));
        values.insert("nullable".to_string(), Value::Null);

        let row = Row::new(values);

        assert!(!row.is_null("id").unwrap());
        assert!(row.is_null("nullable").unwrap());
    }

    // ========================================================================
    // SqlResult Unit Tests
    // ========================================================================

    #[test]
    fn test_sql_result_metadata() {
        let response = RawExecuteSqlResponse {
            count_affected: 5,
            total_number_of_records: 100,
            has_more_records: true,
            paging_table: "temp_123".to_string(),
            json_encoded_response: "{}".to_string(),
            ..Default::default()
        };

        let result = SqlResult::new(response);

        assert_eq!(result.count_affected(), 5);
        assert_eq!(result.total_records(), 100);
        assert!(result.has_more());
        assert_eq!(result.paging_table(), Some("temp_123"));
    }

    #[test]
    fn test_sql_result_empty_paging_table() {
        let response = RawExecuteSqlResponse {
            paging_table: "".to_string(),
            json_encoded_response: "{}".to_string(),
            ..Default::default()
        };

        let result = SqlResult::new(response);
        assert_eq!(result.paging_table(), None);
    }

    #[test]
    fn test_sql_result_scalar() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [42]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);
        let value: i64 = result.scalar().unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn test_sql_result_column() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [1, 2, 3], "column_2": ["a", "b", "c"]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);

        let ids: Vec<i64> = result.column("column_1").unwrap();
        let names: Vec<String> = result.column("column_2").unwrap();

        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_sql_result_rows() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [1, 2], "column_2": ["Alice", "Bob"]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);
        let rows = result.rows().unwrap();

        assert_eq!(rows.len(), 2);

        let id1: i64 = rows[0].get("column_1").unwrap();
        let name1: String = rows[0].get("column_2").unwrap();
        assert_eq!(id1, 1);
        assert_eq!(name1, "Alice");

        let id2: i64 = rows[1].get("column_1").unwrap();
        let name2: String = rows[1].get("column_2").unwrap();
        assert_eq!(id2, 2);
        assert_eq!(name2, "Bob");
    }

    #[test]
    fn test_sql_result_with_column_names() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [1], "column_2": ["Alice"]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::with_column_names(response, &["id", "name"]);
        let rows = result.rows().unwrap();

        let id: i64 = rows[0].get("id").unwrap();
        let name: String = rows[0].get("name").unwrap();

        assert_eq!(id, 1);
        assert_eq!(name, "Alice");
    }

    #[test]
    fn test_sql_result_row_count() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [1, 2, 3, 4, 5]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);
        assert_eq!(result.row_count().unwrap(), 5);
    }

    #[test]
    fn test_sql_result_get_specific() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [10, 20, 30]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);

        let val: i64 = result.get(1, "column_1").unwrap();
        assert_eq!(val, 20);
    }

    #[test]
    fn test_sql_result_empty() {
        let response = RawExecuteSqlResponse {
            json_encoded_response: "{}".to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);

        assert_eq!(result.row_count().unwrap(), 0);
        assert!(result.scalar::<i64>().is_err());
    }

    #[test]
    fn test_sql_result_deserialize() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug, PartialEq)]
        struct Record {
            column_1: i64,
            column_2: String,
        }

        let response = RawExecuteSqlResponse {
            json_encoded_response: r#"{"column_1": [1, 2], "column_2": ["Alice", "Bob"]}"#.to_string(),
            ..Default::default()
        };

        let mut result = SqlResult::new(response);
        let records: Vec<Record> = result.deserialize().unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], Record { column_1: 1, column_2: "Alice".to_string() });
        assert_eq!(records[1], Record { column_1: 2, column_2: "Bob".to_string() });
    }

    // ========================================================================
    // SqlHints Unit Tests
    // ========================================================================

    #[test]
    fn test_sql_hints_default() {
        let hints = SqlHints::new();
        assert!(hints.is_empty());
        assert_eq!(hints.to_inline_hint(), "");
        assert!(hints.to_options_map().is_empty());
    }

    #[test]
    fn test_sql_hints_update_on_existing_pk() {
        let hints = SqlHints::new().update_on_existing_pk(true);

        assert!(!hints.is_empty());
        assert_eq!(hints.get_update_on_existing_pk(), Some(true));
        assert_eq!(hints.to_inline_hint(), "/* KI_HINT_UPDATE_ON_EXISTING_PK */");

        let options = hints.to_options_map();
        assert_eq!(options.get("update_on_existing_pk"), Some(&"true".to_string()));
    }

    #[test]
    fn test_sql_hints_ignore_existing_pk() {
        let hints = SqlHints::new().ignore_existing_pk(true);

        assert!(!hints.is_empty());
        assert_eq!(hints.get_ignore_existing_pk(), Some(true));
        assert_eq!(hints.to_inline_hint(), "/* KI_HINT_IGNORE_EXISTING_PK */");

        let options = hints.to_options_map();
        assert_eq!(options.get("ignore_existing_pk"), Some(&"true".to_string()));
    }

    #[test]
    fn test_sql_hints_multiple_inline_hints() {
        let hints = SqlHints::new()
            .update_on_existing_pk(true)
            .add_custom_hint("KI_HINT_GROUP_BY_PK");

        let inline = hints.to_inline_hint();
        assert!(inline.contains("KI_HINT_UPDATE_ON_EXISTING_PK"));
        assert!(inline.contains("KI_HINT_GROUP_BY_PK"));
    }

    #[test]
    fn test_sql_hints_query_execution_options() {
        let hints = SqlHints::new()
            .distributed_operations(true)
            .distributed_joins(true)
            .cost_based_optimization(false)
            .parallel_execution(false);

        let options = hints.to_options_map();
        assert_eq!(options.get("distributed_operations"), Some(&"true".to_string()));
        assert_eq!(options.get("distributed_joins"), Some(&"true".to_string()));
        assert_eq!(options.get("cost_based_optimization"), Some(&"false".to_string()));
        assert_eq!(options.get("parallel_execution"), Some(&"false".to_string()));
    }

    #[test]
    fn test_sql_hints_result_table_options() {
        let hints = SqlHints::new()
            .ttl(60)
            .paging_table("my_paging_table")
            .paging_table_ttl(30);

        let options = hints.to_options_map();
        assert_eq!(options.get("ttl"), Some(&"60".to_string()));
        assert_eq!(options.get("paging_table"), Some(&"my_paging_table".to_string()));
        assert_eq!(options.get("paging_table_ttl"), Some(&"30".to_string()));
    }

    #[test]
    fn test_sql_hints_custom_hints() {
        let hints = SqlHints::new()
            .add_custom_hint("KI_HINT_INDEX(my_column)")
            .add_custom_hint("KI_HINT_JOBID_PREFIX(my_job)");

        let inline = hints.to_inline_hint();
        assert!(inline.contains("KI_HINT_INDEX(my_column)"));
        assert!(inline.contains("KI_HINT_JOBID_PREFIX(my_job)"));
    }

    #[test]
    fn test_sql_hints_merge_into() {
        let mut options = HashMap::new();
        options.insert("existing_key".to_string(), "existing_value".to_string());

        let hints = SqlHints::new()
            .update_on_existing_pk(true)
            .ttl(60);

        hints.merge_into(&mut options);

        assert_eq!(options.get("existing_key"), Some(&"existing_value".to_string()));
        assert_eq!(options.get("update_on_existing_pk"), Some(&"true".to_string()));
        assert_eq!(options.get("ttl"), Some(&"60".to_string()));
    }

    #[test]
    fn test_sql_hints_inline_only_true_values() {
        // Inline hints should only appear when true
        let hints = SqlHints::new()
            .update_on_existing_pk(false)
            .ignore_existing_pk(false);

        // Neither should appear in inline since they're false
        assert_eq!(hints.to_inline_hint(), "");

        // But they should appear in options map
        let options = hints.to_options_map();
        assert_eq!(options.get("update_on_existing_pk"), Some(&"false".to_string()));
        assert_eq!(options.get("ignore_existing_pk"), Some(&"false".to_string()));
    }

    #[test]
    fn test_sql_hints_all_options() {
        let hints = SqlHints::new()
            .update_on_existing_pk(true)
            .ignore_existing_pk(false)
            .batch_size(10000)
            .distributed_operations(true)
            .distributed_joins(true)
            .cost_based_optimization(false)
            .rule_based_optimization(false)
            .parallel_execution(true)
            .plan_cache(false)
            .results_caching(false)
            .late_materialization(true)
            .ssq_optimization(false)
            .preserve_dict_encoding(true)
            .ttl(-1)
            .paging_table("test_paging")
            .paging_table_ttl(10)
            .prepare_mode(false)
            .query_parameters("[1, 2, 3]")
            .validate_change_column(true)
            .use_approx_count_distinct(true);

        let options = hints.to_options_map();
        assert_eq!(options.len(), 20);
        assert!(!hints.is_empty());
    }

    #[test]
    fn test_sql_hints_batch_size() {
        let hints = SqlHints::new().batch_size(50000);

        assert!(!hints.is_empty());
        assert_eq!(hints.get_batch_size(), Some(50000));
        assert_eq!(hints.to_inline_hint(), "/* KI_HINT_BATCH_SIZE(50000) */");

        let options = hints.to_options_map();
        assert_eq!(options.get("batch_size"), Some(&"50000".to_string()));
    }

    #[test]
    fn test_sql_hints_batch_size_with_upsert() {
        let hints = SqlHints::new()
            .update_on_existing_pk(true)
            .batch_size(25000);

        let inline = hints.to_inline_hint();
        assert!(inline.contains("KI_HINT_UPDATE_ON_EXISTING_PK"));
        assert!(inline.contains("KI_HINT_BATCH_SIZE(25000)"));
    }
}
