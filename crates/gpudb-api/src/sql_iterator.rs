//! Iterating over the records an SQL query returns, a batch at a time.
//!
//! The query runs against a paging table of the iterator's own, named by a
//! random UUID, so batches come from one result set in one order, and the
//! server-side tables the iteration creates are dropped by
//! [`SqlIterator::close`].
//!
//! ```no_run
//! use gpudb_api::{Kinetica, KineticaOptions, SqlIterator};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let kinetica = Arc::new(Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?);
//! let mut records = SqlIterator::new(kinetica, "SELECT * FROM my_table").await?;
//! while records.has_next() {
//!     println!("{:?}", records.next().await?);
//! }
//! records.close().await;
//! # Ok(())
//! # }
//! ```

use crate::address_resolution::split_dropping_trailing_empty;
use crate::generic_record::GenericRecord;
use crate::{ClearTableRequestOptions, ExecuteSqlRequest, Kinetica, KineticaError};
use log::{debug, log_enabled, trace, Level};
use std::collections::HashMap;
use std::sync::Arc;

/// The query parameters of [`Kinetica::execute_with_parameters`] and
/// [`Kinetica::query_with_parameters`], sent as the `query_parameters` SQL
/// option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlParameters {
    /// Parameters given as text, sent as they are.
    Text(String),
    /// Parameter values, sent as a JSON array.
    Values(Vec<String>),
    /// Parameters of any JSON type -- numbers, booleans, nulls, text, arrays
    /// and objects -- sent as JSON text. So `[1, 2.5, null]` goes as
    /// `[1,2.5,null]`, typed, where [`Self::Values`] would send text. Build it
    /// with `serde_json::json!`, or from a `Vec<serde_json::Value>`.
    ///
    /// The server decides what it takes. Kinetica 7.2.3.21 binds numbers,
    /// text and nulls with their types (`SELECT $1` of `[1, null]` is an int
    /// and a null), refuses a boolean ("Unexpected json value in
    /// query_parameters"), and takes nested arrays only for an INSERT.
    Json(serde_json::Value),
}

impl From<&str> for SqlParameters {
    fn from(text: &str) -> Self {
        SqlParameters::Text(text.to_string())
    }
}

impl From<String> for SqlParameters {
    fn from(text: String) -> Self {
        SqlParameters::Text(text)
    }
}

impl From<Vec<String>> for SqlParameters {
    fn from(values: Vec<String>) -> Self {
        SqlParameters::Values(values)
    }
}

impl From<&[&str]> for SqlParameters {
    fn from(values: &[&str]) -> Self {
        SqlParameters::Values(values.iter().map(|v| v.to_string()).collect())
    }
}

impl From<serde_json::Value> for SqlParameters {
    fn from(value: serde_json::Value) -> Self {
        SqlParameters::Json(value)
    }
}

impl From<Vec<serde_json::Value>> for SqlParameters {
    fn from(values: Vec<serde_json::Value>) -> Self {
        SqlParameters::Json(serde_json::Value::Array(values))
    }
}

/// The SQL options with the parameters added.
fn sql_options_with(
    parameters: Option<SqlParameters>,
    sql_options: Option<HashMap<String, String>>,
) -> HashMap<String, String> {
    let mut options = sql_options.unwrap_or_default();
    let parameters = match parameters {
        Some(SqlParameters::Text(text)) => Some(text),
        Some(SqlParameters::Values(values)) => {
            Some(serde_json::to_string(&values).expect("a list of strings is JSON"))
        }
        Some(SqlParameters::Json(value)) => Some(value.to_string()),
        None => None,
    };
    if let Some(parameters) = parameters {
        options.insert("query_parameters".to_string(), parameters);
    }
    options
}

impl Kinetica {
    /// Runs an SQL statement (DML or DDL, say) and returns the number of
    /// records it affected. An empty `query_parameters` option is sent.
    pub async fn execute(&self, sql: &str) -> Result<i64, KineticaError> {
        self.execute_with_parameters(sql, Some(SqlParameters::Text(String::new())), None).await
    }

    /// Runs an SQL statement with query parameters and SQL options, and
    /// returns the number of records it affected.
    pub async fn execute_with_parameters(
        &self,
        sql: &str,
        parameters: Option<SqlParameters>,
        sql_options: Option<HashMap<String, String>>,
    ) -> Result<i64, KineticaError> {
        let request = ExecuteSqlRequest {
            statement: sql.to_string(),
            offset: 0,
            limit: 1,
            encoding: "binary".to_string(),
            request_schema_str: String::new(),
            data: Vec::new(),
            options: sql_options_with(parameters, sql_options),
        };
        Ok(self.execute_sql_request(&request).await?.count_affected)
    }

    /// Runs an SQL query, returning an iterator over its records.
    pub async fn query(self: &Arc<Self>, sql: &str) -> Result<SqlIterator, KineticaError> {
        self.query_with_parameters(sql, None, None).await
    }

    /// Runs an SQL query with query parameters and SQL options, returning an
    /// iterator over its records.
    pub async fn query_with_parameters(
        self: &Arc<Self>,
        sql: &str,
        parameters: Option<SqlParameters>,
        sql_options: Option<HashMap<String, String>>,
    ) -> Result<SqlIterator, KineticaError> {
        SqlIterator::with_options(self.clone(), sql, sql_options_with(parameters, sql_options)).await
    }
}

/// The number of records fetched at a time unless another is given.
const DEFAULT_BATCH_SIZE: i32 = 10000;

/// Iterates over the records an SQL query returns.
///
/// The first batch is fetched when the iterator is created. Call
/// [`SqlIterator::close`] when done to drop the paging tables; nothing drops
/// them otherwise.
pub struct SqlIterator {
    db: Arc<Kinetica>,
    sql: String,
    batch_size: i32,
    sql_options: HashMap<String, String>,

    record_position: usize,
    /// The current batch; a record is taken out when it is returned.
    records: Vec<Option<GenericRecord>>,
    offset: i64,
    total_count: i64,
    has_more_records: bool,
    paging_table_name: String,

    /// The server-side tables this iteration has created, to be dropped on
    /// close, in the order they were reported. Each is kept once: the
    /// paging table is reported with every batch.
    paging_table_names: Vec<String>,
}

impl SqlIterator {
    /// Runs `sql`, fetching 10,000 records at a time.
    pub async fn new(db: Arc<Kinetica>, sql: &str) -> Result<Self, KineticaError> {
        Self::with_batch_size_and_options(db, sql, DEFAULT_BATCH_SIZE, None).await
    }

    /// Runs `sql`, fetching `batch_size` records at a time.
    pub async fn with_batch_size(db: Arc<Kinetica>, sql: &str, batch_size: i32) -> Result<Self, KineticaError> {
        Self::with_batch_size_and_options(db, sql, batch_size, None).await
    }

    /// Runs `sql` with the given SQL options, fetching 10,000 records at a
    /// time.
    pub async fn with_options(
        db: Arc<Kinetica>,
        sql: &str,
        sql_options: HashMap<String, String>,
    ) -> Result<Self, KineticaError> {
        Self::with_batch_size_and_options(db, sql, DEFAULT_BATCH_SIZE, Some(sql_options)).await
    }

    /// Runs `sql` with the given SQL options, fetching `batch_size` records
    /// at a time. A `paging_table` option is replaced: the iterator pages
    /// through a table of its own, and drops it on close.
    pub async fn with_batch_size_and_options(
        db: Arc<Kinetica>,
        sql: &str,
        batch_size: i32,
        sql_options: Option<HashMap<String, String>>,
    ) -> Result<Self, KineticaError> {
        let mut sql_options = sql_options.unwrap_or_default();
        let paging_table_name = uuid::Uuid::new_v4().to_string().replace('-', "_");
        sql_options.insert("paging_table".to_string(), paging_table_name.clone());

        let mut iterator = SqlIterator {
            db,
            sql: sql.to_string(),
            batch_size,
            sql_options,
            record_position: 0,
            records: Vec::new(),
            offset: 0,
            total_count: 0,
            has_more_records: false,
            paging_table_name,
            paging_table_names: Vec::new(),
        };
        iterator.check_and_fetch_records().await?;
        Ok(iterator)
    }

    /// Replaces the SQL options, keeping this iterator's paging table. A
    /// `paging_table` option is replaced, as when the iterator is created.
    pub fn set_sql_options(&mut self, sql_options: Option<HashMap<String, String>>) {
        let mut sql_options = sql_options.unwrap_or_default();
        sql_options.insert("paging_table".to_string(), self.paging_table_name.clone());
        self.sql_options = sql_options;
    }

    /// The number of records the query returns in all.
    pub fn total_records(&self) -> i64 {
        self.total_count
    }

    /// Whether another record is to come.
    pub fn has_next(&self) -> bool {
        !self.records.is_empty() && self.record_position < self.records.len() || self.has_more_records
    }

    /// The next record, fetching the next batch when this one is done.
    /// Fails when there are no more.
    pub async fn next(&mut self) -> Result<GenericRecord, KineticaError> {
        self.check_and_fetch_records().await?;

        let Some(slot) = self.records.get_mut(self.record_position) else {
            return Err(KineticaError::Config("No more records exist in result set.".to_string()));
        };
        self.record_position += 1;
        Ok(slot.take().expect("each record is returned once"))
    }

    /// Drops the server-side tables the iteration created. A table that
    /// cannot be dropped is logged and skipped.
    pub async fn close(&mut self) {
        let options = ClearTableRequestOptions { no_error_if_not_exists: "true".to_string() };
        for table_name in &self.paging_table_names {
            if let Err(e) = self.db.clear_table_with_opts(table_name.clone(), String::new(), options.clone()).await {
                debug!("Error in deleting paging table <{table_name}>: {e}");
            }
        }
    }

    async fn check_and_fetch_records(&mut self) -> Result<(), KineticaError> {
        if !self.records.is_empty() && self.record_position < self.records.len() {
            return Ok(());
        }
        self.records.clear();
        self.record_position = 0;
        if self.offset > self.total_count {
            return Ok(());
        }

        self.execute_sql().await?;
        // Account for max_get_records_size limiting the server's result set
        self.offset += self.records.len() as i64;
        Ok(())
    }

    async fn execute_sql(&mut self) -> Result<(), KineticaError> {
        let request = ExecuteSqlRequest {
            statement: self.sql.clone(),
            offset: self.offset,
            limit: i64::from(self.batch_size),
            encoding: "binary".to_string(),
            request_schema_str: String::new(),
            data: Vec::new(),
            options: self.sql_options.clone(),
        };
        let response = self.db.execute_sql_request(&request).await?;
        self.records = response.data.into_iter().map(Some).collect();
        self.total_count = response.total_number_of_records;
        self.has_more_records = response.has_more_records && !self.records.is_empty();

        if log_enabled!(Level::Trace) {
            trace!(
                "Retrieved <{}/{}> records using offset/batch <{}/{}> for query <{}>",
                self.records.len(),
                self.total_count,
                self.offset,
                self.batch_size,
                self.sql
            );
        }

        if self.total_count == 0 {
            return Ok(());
        }

        self.paging_table_name = response.paging_table;
        if !self.paging_table_name.is_empty() {
            let mut created = vec![self.paging_table_name.clone()];
            if let Some(result_tables) = response.info.get("result_table_list").filter(|l| !l.is_empty()) {
                created.extend(split_dropping_trailing_empty(result_tables, ",").into_iter().map(str::to_string));
            }
            for table_name in created {
                if !self.paging_table_names.contains(&table_name) {
                    self.paging_table_names.push(table_name);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::generic_record::RecordValue;
    use crate::{ClearTableRequest, ClearTableResponse, KineticaOptions, RawExecuteSqlResponse};
    use parking_lot::Mutex;
    use serde_bytes::ByteBuf;

    const SCHEMA: &str = r#"{"type":"record","name":"generic_response","fields":[
        {"name":"column_1","type":{"type":"array","items":"int"}},
        {"name":"column_headers","type":{"type":"array","items":"string"}},
        {"name":"column_datatypes","type":{"type":"array","items":"string"}}]}"#;

    /// A batch of the query's result: `values` from a total of 3, from the
    /// paging table `p1` with result tables `r1` and `r2`.
    fn batch(values: Vec<i32>, has_more: bool) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct Columns {
            column_1: Vec<i32>,
            column_headers: Vec<String>,
            column_datatypes: Vec<String>,
        }
        let data = encode(
            &SCHEMA.parse().unwrap(),
            &Columns { column_1: values, column_headers: vec!["n".into()], column_datatypes: vec!["int".into()] },
        );
        encode(
            &crate::generated::EXECUTE_SQL_RESPONSE_SCHEMA,
            &RawExecuteSqlResponse {
                response_schema_str: SCHEMA.into(),
                binary_encoded_response: ByteBuf::from(data),
                total_number_of_records: 3,
                has_more_records: has_more,
                paging_table: "p1".into(),
                info: HashMap::from([("result_table_list".to_string(), "r1,p1,r2".to_string())]),
                ..Default::default()
            },
        )
    }

    /// The queries and table clears a server running a three-record query
    /// two at a time received.
    type Received = Arc<Mutex<(Vec<ExecuteSqlRequest>, Vec<ClearTableRequest>)>>;

    async fn iterator(host: &str) -> (SqlIterator, Received) {
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let received: Received = Arc::default();
        let log = received.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| match path {
            "/execute/sql" => {
                let request: ExecuteSqlRequest =
                    serde_avro_fast::from_datum_slice(body, &crate::generated::EXECUTE_SQL_REQUEST_SCHEMA).unwrap();
                let reply = match request.offset {
                    0 => batch(vec![1, 2], true),
                    2 => batch(vec![3], false),
                    _ => batch(Vec::new(), false),
                };
                log.lock().0.push(request);
                Some(Ok(reply))
            }
            "/clear/table" => {
                log.lock().1.push(
                    serde_avro_fast::from_datum_slice(body, &crate::generated::CLEAR_TABLE_REQUEST_SCHEMA).unwrap(),
                );
                Some(Ok(encode(&crate::generated::CLEAR_TABLE_RESPONSE_SCHEMA, &ClearTableResponse::default())))
            }
            _ => None,
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        let user_options = HashMap::from([("paging_table".to_string(), "mine".to_string())]);
        let iterator = SqlIterator::with_batch_size_and_options(k, "SELECT n FROM t", 2, Some(user_options))
            .await
            .expect("iterator");
        (iterator, received)
    }

    // Records come a batch at a time from the iterator's own paging table,
    // then every table it created is dropped once on close.
    #[tokio::test]
    async fn records_come_a_batch_at_a_time_and_the_tables_are_dropped() {
        let (mut records, received) = iterator("127.0.0.91").await;
        assert_eq!(records.total_records(), 3);

        let mut values = Vec::new();
        while records.has_next() {
            values.push(records.next().await.unwrap().get(0).cloned());
        }
        assert_eq!(values, [Some(RecordValue::Int(1)), Some(RecordValue::Int(2)), Some(RecordValue::Int(3))]);

        // A call past the end asks the server once more
        let err = records.next().await.unwrap_err();
        assert_eq!(err.to_string(), "Configuration error: No more records exist in result set.");

        records.close().await;
        let (queries, clears) = &*received.lock();
        assert_eq!(queries.iter().map(|q| (q.offset, q.limit)).collect::<Vec<_>>(), [(0, 2), (2, 2), (3, 2)]);
        let paging_table = &queries[0].options["paging_table"];
        assert_ne!(paging_table, "mine", "the iterator pages through a table of its own");
        assert_eq!(paging_table.len(), 36);
        assert!(!paging_table.contains('-'));
        assert_eq!(
            clears.iter().map(|c| c.table_name.as_str()).collect::<Vec<_>>(),
            ["p1", "r1", "r2"],
            "each created table, once"
        );
        assert!(clears.iter().all(|c| c.options.get("no_error_if_not_exists").map(String::as_str) == Some("true")));
    }

    // `execute` runs the statement for one record, sending an empty
    // `query_parameters` when given none; `query` iterates.
    #[tokio::test]
    async fn statements_run_once_and_queries_iterate() {
        let (records, received) = iterator("127.0.0.93").await;
        let k = records.db.clone();

        assert_eq!(k.execute("DELETE FROM t").await.unwrap(), 0);
        let values: SqlParameters = (&["a", "b"][..]).into();
        k.execute_with_parameters("DELETE FROM t WHERE x IN (?, ?)", Some(values), None).await.unwrap();
        let mut iterator = k.query("SELECT n FROM t").await.unwrap();
        assert_eq!(iterator.total_records(), 3);
        iterator.close().await;

        let (queries, _) = &*received.lock();
        let executed: Vec<_> = queries
            .iter()
            .filter(|q| q.statement.starts_with("DELETE"))
            .map(|q| (q.offset, q.limit, q.options.get("query_parameters").cloned()))
            .collect();
        assert_eq!(executed, [(0, 1, Some(String::new())), (0, 1, Some(r#"["a","b"]"#.to_string()))]);
        let queried = queries.iter().find(|q| q.statement.starts_with("SELECT n FROM t") && q.limit == 10000);
        assert!(queried.is_some_and(|q| !q.options.contains_key("query_parameters")));
    }

    // Text is sent as it is and any other parameters as JSON, so typed values
    // keep their JSON types.
    #[test]
    fn parameters_keep_their_json_types() {
        use serde_json::json;
        let sent = |parameters: SqlParameters| sql_options_with(Some(parameters), None)["query_parameters"].clone();

        assert_eq!(sent("[1, 'a']".into()), "[1, 'a']", "text as it is");
        assert_eq!(sent(vec!["a".to_string(), "1".to_string()].into()), r#"["a","1"]"#);
        assert_eq!(
            sent(vec![json!(1), json!(2.5), json!(null), json!(true), json!("x")].into()),
            r#"[1,2.5,null,true,"x"]"#
        );
        assert_eq!(sent(json!({"a": [1, "b"]}).into()), r#"{"a":[1,"b"]}"#);

        let options = sql_options_with(Some("[1]".into()), Some(HashMap::from([("k".into(), "v".into())])));
        assert_eq!(options.len(), 2, "the caller's options are kept");
        assert!(!sql_options_with(None, None).contains_key("query_parameters"));
    }

    // New options keep the paging table, which is the server's after the
    // first batch.
    #[tokio::test]
    async fn new_options_keep_the_paging_table() {
        let (mut records, _) = iterator("127.0.0.92").await;
        records.set_sql_options(Some(HashMap::from([("paging_table".to_string(), "other".to_string())])));
        assert_eq!(records.sql_options["paging_table"], "p1");
    }
}
