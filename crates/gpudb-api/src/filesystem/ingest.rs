//! Ingesting local files into a table through KiFS.
//!
//! The files are uploaded to a directory of their own under the user's KiFS
//! home directory, `~/<uuid>`, inserted into the table from there with
//! `/insert/records/fromfiles`, and the directory is deleted.

use std::collections::HashMap;
use std::sync::Arc;

use log::{error, warn};
use uuid::Uuid;

use crate::{InsertRecordsFromFilesRequest, InsertRecordsFromFilesResponse, Kinetica, KineticaError};

use super::options::{KifsFileHandlerOptions, UploadOptions};
use super::uploader::FileUploader;
use super::{KIFS_PATH_PREFIX, KIFS_PATH_SEPARATOR, REMOTE_USER_HOME_DIR_PREFIX};

/// How the server handles a record it cannot insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorHandlingMode {
    /// Insert what can be, setting what cannot to null
    Permissive,
    /// Skip a record that cannot be inserted
    IgnoreBadRecords,
    /// Stop at the first record that cannot be inserted
    Abort,
}

impl ErrorHandlingMode {
    /// The mode's name, as the server takes it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Permissive => "permissive",
            Self::IgnoreBadRecords => "ignore_bad_records",
            Self::Abort => "abort",
        }
    }

    /// The default mode's name.
    pub fn default_mode(&self) -> &'static str {
        Self::Permissive.as_str()
    }
}

/// The type of the files ingested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Delimited text, such as CSV
    DelimitedText,
    /// Parquet
    Parquet,
    /// JSON
    Json,
    /// Shapefile
    Shapefile,
}

impl FileType {
    /// The type's name, as the server takes it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DelimitedText => "delimited_text",
            Self::Parquet => "parquet",
            Self::Json => "json",
            Self::Shapefile => "shapefile",
        }
    }

    /// The default type's name.
    pub fn default_partition_type(&self) -> &'static str {
        Self::DelimitedText.as_str()
    }
}

/// How a table created for the ingest is partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionType {
    /// By ranges of values
    Range,
    /// By intervals of values
    Interval,
    /// By lists of values
    List,
    /// By hash
    Hash,
}

impl PartitionType {
    /// The type's name, as the server takes it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Interval => "interval",
            Self::List => "list",
            Self::Hash => "hash",
        }
    }

    /// The default type's name.
    pub fn default_partition_type(&self) -> &'static str {
        Self::Range.as_str()
    }
}

impl std::fmt::Display for PartitionType {
    /// The type's name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How the server infers the types of the columns of a table it creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeInferenceMode {
    /// Scan all the data for the best types
    Accuracy,
    /// Infer from a sample, quickly
    Speed,
}

impl TypeInferenceMode {
    /// The mode's name, as the server takes it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accuracy => "accuracy",
            Self::Speed => "speed",
        }
    }

    /// The default mode's name.
    pub fn default_partition_type(&self) -> &'static str {
        Self::Speed.as_str()
    }
}

/// Names of the ingest options.
pub mod ingest_option {
    pub const BAD_RECORD_TABLE_NAME: &str = "bad_record_table_name";
    pub const BAD_RECORD_TABLE_LIMIT: &str = "bad_record_table_limit";
    pub const BATCH_SIZE: &str = "batch_size";
    pub const COLUMN_FORMATS: &str = "column_formats";
    pub const COLUMNS_TO_LOAD: &str = "columns_to_load";
    pub const COLUMNS_TO_SKIP: &str = "columns_to_skip";
    pub const DATASOURCE_NAME: &str = "datasource_name";
    pub const DEFAULT_COLUMN_FORMATS: &str = "default_column_formats";
    pub const ERROR_HANDLING: &str = "error_handling";
    pub const FILE_TYPE: &str = "file_type";
    pub const INGESTION_MODE: &str = "ingestion_mode";
    pub const LOADING_MODE: &str = "loading_mode";
    pub const PRIMARY_KEYS: &str = "primary_keys";
    pub const SHARD_KEYS: &str = "shard_keys";
    pub const SUBSCRIBE: &str = "subscribe";
    pub const POLL_INTERVAL: &str = "poll_interval";
    pub const TEXT_COMMENT_STRING: &str = "text_comment_string";
    pub const TEXT_DELIMITER: &str = "text_delimiter";
    pub const TEXT_ESCAPE_CHARACTER: &str = "text_escape_character";
    pub const TEXT_HAS_HEADER: &str = "text_has_header";
    pub const TEXT_HEADER_PROPERTY_DELIMITER: &str = "text_header_property_delimiter";
    pub const TEXT_NULL_STRING: &str = "text_null_string";
    pub const TEXT_QUOTE_CHARACTER: &str = "text_quote_character";
    pub const TRUNCATE_TABLE: &str = "truncate_table";
    pub const NUM_TASKS_PER_RANK: &str = "num_tasks_per_rank";
    pub const TYPE_INFERENCE_MODE: &str = "type_inference_mode";
    pub const TABLE_INSERT_MODE: &str = "table_insert_mode";
}

/// Names of the table creation options.
pub mod table_creation_option {
    pub const TYPE_ID: &str = "type_id";
    pub const NO_ERROR_IF_EXISTS: &str = "no_error_if_exists";
    pub const IS_REPLICATED: &str = "is_replicated";
    pub const FOREIGN_KEYS: &str = "foreign_keys";
    pub const FOREIGN_SHARD_KEY: &str = "foreign_shard_key";
    pub const PARTITION_TYPE: &str = "partition_type";
    pub const PARTITION_KEYS: &str = "partition_keys";
    pub const PARTITION_DEFINITIONS: &str = "partition_definitions";
    pub const IS_AUTOMATIC_PARTITION: &str = "is_automatic_partition";
    pub const TTL: &str = "ttl";
    pub const CHUNK_SIZE: &str = "chunk_size";
    pub const CHUNK_BYTES: &str = "chunk_bytes";
    pub const IS_RESULT_TABLE: &str = "is_result_table";
    pub const STRATEGY_DEFINITION: &str = "strategy_definition";
}

/// Options set in order, a name set again keeping its place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct OrderedOptions(Vec<(String, String)>);

impl OrderedOptions {
    fn insert(&mut self, key: &str, value: String) {
        match self.0.iter_mut().find(|(k, _)| k == key) {
            Some(entry) => entry.1 = value,
            None => self.0.push((key.to_string(), value)),
        }
    }

    /// The options, with `additional` over them.
    fn merged(&self, additional: &OrderedOptions) -> HashMap<String, String> {
        self.0.iter().chain(&additional.0).cloned().collect()
    }
}

fn true_false(value: bool) -> String {
    if value { "true" } else { "false" }.to_string()
}

/// The names, each once, in order.
fn distinct(names: Vec<String>) -> Vec<String> {
    let mut seen = Vec::with_capacity(names.len());
    for name in names {
        if !seen.contains(&name) {
            seen.push(name);
        }
    }
    seen
}

/// Options for ingesting files: the options of `/insert/records/fromfiles`.
///
/// Each setter sets its option as well as its value; an option without a
/// setter can be given with [`set_option`](Self::set_option).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestOptions {
    bad_record_table_name: Option<String>,
    bad_record_table_limit: i64,
    column_formats: Option<Vec<(String, (String, String))>>,
    columns_to_load: Option<Vec<String>>,
    columns_to_skip: Option<Vec<String>>,
    default_column_formats: Option<Vec<(String, String)>>,
    error_handling_mode: Option<ErrorHandlingMode>,
    file_type: Option<FileType>,
    primary_keys: Option<Vec<String>>,
    shard_keys: Option<Vec<String>>,
    text_comment_string: Option<String>,
    text_delimiter: Option<String>,
    text_escape_character: Option<String>,
    text_has_header: bool,
    text_header_property_delimiter: Option<String>,
    text_null_string: Option<String>,
    text_quote_character: Option<String>,
    truncate_table: bool,
    type_inference_mode: Option<TypeInferenceMode>,
    additional_options: OrderedOptions,
    options: OrderedOptions,
}

impl IngestOptions {
    /// Options with nothing set.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bad_record_table_name(&self) -> Option<&str> {
        self.bad_record_table_name.as_deref()
    }

    /// Sets the table bad records go to; an empty name is ignored, with a
    /// warning.
    pub fn set_bad_record_table_name(&mut self, bad_record_table_name: &str) -> &mut Self {
        if bad_record_table_name.is_empty() {
            warn!("BAD_RECORD_TABLE_NAME is empty; value not set");
        } else {
            self.bad_record_table_name = Some(bad_record_table_name.to_string());
            self.options.insert(ingest_option::BAD_RECORD_TABLE_NAME, bad_record_table_name.to_string());
        }
        self
    }

    pub fn bad_record_table_limit(&self) -> i64 {
        self.bad_record_table_limit
    }

    pub fn set_bad_record_table_limit(&mut self, bad_record_table_limit: i64) -> &mut Self {
        self.bad_record_table_limit = bad_record_table_limit;
        self.options.insert(ingest_option::BAD_RECORD_TABLE_LIMIT, bad_record_table_limit.to_string());
        self
    }

    /// The format of each column named, as a pair such as `("date",
    /// "%Y-%m-%d")`.
    pub fn column_formats(&self) -> Option<&[(String, (String, String))]> {
        self.column_formats.as_deref()
    }

    /// Sets the format of each column named, sent as a JSON object such as
    /// `{"column":{"date":"%Y-%m-%d"}}`; an empty list is ignored, with a
    /// warning.
    pub fn set_column_formats(&mut self, column_formats: Vec<(String, (String, String))>) -> &mut Self {
        if column_formats.is_empty() {
            warn!("COLUMN_FORMATS is empty; value not set");
            return self;
        }
        let json: serde_json::Map<String, serde_json::Value> = column_formats
            .iter()
            .map(|(column, (kind, format))| {
                let mut pair = serde_json::Map::new();
                pair.insert(kind.clone(), serde_json::Value::String(format.clone()));
                (column.clone(), serde_json::Value::Object(pair))
            })
            .collect();
        self.options.insert(ingest_option::COLUMN_FORMATS, serde_json::Value::Object(json).to_string());
        self.column_formats = Some(column_formats);
        self
    }

    pub fn columns_to_load(&self) -> Option<&[String]> {
        self.columns_to_load.as_deref()
    }

    /// Sets the columns to load, sent as a JSON array, each once.
    ///
    /// # Errors
    ///
    /// Fails if a column is also one to skip.
    pub fn set_columns_to_load(&mut self, columns_to_load: Vec<String>) -> Result<&mut Self, KineticaError> {
        let columns_to_load = distinct(columns_to_load);
        if let Some(skip) = &self.columns_to_skip {
            if columns_to_load.iter().any(|c| skip.contains(c)) {
                return Err(KineticaError::Config(
                    " Columns to load and columns to skip are mutually exclusive ..".to_string(),
                ));
            }
        }
        self.options.insert(
            ingest_option::COLUMNS_TO_LOAD,
            serde_json::to_string(&columns_to_load).expect("strings are JSON"),
        );
        self.columns_to_load = Some(columns_to_load);
        Ok(self)
    }

    pub fn columns_to_skip(&self) -> Option<&[String]> {
        self.columns_to_skip.as_deref()
    }

    /// Sets the columns to skip, sent as a JSON array, each once.
    ///
    /// # Errors
    ///
    /// Fails if a column is also one to load.
    pub fn set_columns_to_skip(&mut self, columns_to_skip: Vec<String>) -> Result<&mut Self, KineticaError> {
        let columns_to_skip = distinct(columns_to_skip);
        if let Some(load) = &self.columns_to_load {
            if columns_to_skip.iter().any(|c| load.contains(c)) {
                return Err(KineticaError::Config(
                    " Columns to load and columns to skip are mutually exclusive ..".to_string(),
                ));
            }
        }
        self.options.insert(
            ingest_option::COLUMNS_TO_SKIP,
            serde_json::to_string(&columns_to_skip).expect("strings are JSON"),
        );
        self.columns_to_skip = Some(columns_to_skip);
        Ok(self)
    }

    pub fn default_column_formats(&self) -> Option<&[(String, String)]> {
        self.default_column_formats.as_deref()
    }

    /// Sets the default format of each kind of column, sent as a JSON
    /// object; an empty list is ignored, with a warning.
    pub fn set_default_column_formats(&mut self, default_column_formats: Vec<(String, String)>) -> &mut Self {
        if default_column_formats.is_empty() {
            warn!("DEFAULT_COLUMN_FORMATS cannot be empty; value not set");
            return self;
        }
        let json: serde_json::Map<String, serde_json::Value> = default_column_formats
            .iter()
            .map(|(kind, format)| (kind.clone(), serde_json::Value::String(format.clone())))
            .collect();
        self.options.insert(ingest_option::DEFAULT_COLUMN_FORMATS, serde_json::Value::Object(json).to_string());
        self.default_column_formats = Some(default_column_formats);
        self
    }

    pub fn error_handling_mode(&self) -> Option<ErrorHandlingMode> {
        self.error_handling_mode
    }

    pub fn set_error_handling_mode(&mut self, error_handling_mode: ErrorHandlingMode) -> &mut Self {
        self.error_handling_mode = Some(error_handling_mode);
        self.options.insert(ingest_option::ERROR_HANDLING, error_handling_mode.as_str().to_string());
        self
    }

    pub fn file_type(&self) -> Option<FileType> {
        self.file_type
    }

    pub fn set_file_type(&mut self, file_type: FileType) -> &mut Self {
        self.file_type = Some(file_type);
        self.options.insert(ingest_option::FILE_TYPE, file_type.as_str().to_string());
        self
    }

    pub fn primary_keys(&self) -> Option<&[String]> {
        self.primary_keys.as_deref()
    }

    /// Sets the primary key columns, sent joined by commas; an empty list is
    /// ignored, with a warning.
    pub fn set_primary_keys(&mut self, primary_keys: Vec<String>) -> &mut Self {
        if primary_keys.is_empty() {
            warn!("PRIMARY_KEYS cannot be empty; value not set");
        } else {
            self.options.insert(ingest_option::PRIMARY_KEYS, primary_keys.join(","));
            self.primary_keys = Some(primary_keys);
        }
        self
    }

    pub fn shard_keys(&self) -> Option<&[String]> {
        self.shard_keys.as_deref()
    }

    /// Sets the shard key columns, sent joined by commas; an empty list is
    /// ignored, with a warning.
    pub fn set_shard_keys(&mut self, shard_keys: Vec<String>) -> &mut Self {
        if shard_keys.is_empty() {
            warn!("SHARD_KEYS cannot be empty; value not set");
        } else {
            self.options.insert(ingest_option::SHARD_KEYS, shard_keys.join(","));
            self.shard_keys = Some(shard_keys);
        }
        self
    }

    pub fn text_comment_string(&self) -> Option<&str> {
        self.text_comment_string.as_deref()
    }

    pub fn set_text_comment_string(&mut self, text_comment_string: &str) -> &mut Self {
        self.text_comment_string = Some(text_comment_string.to_string());
        self.options.insert(ingest_option::TEXT_COMMENT_STRING, text_comment_string.to_string());
        self
    }

    pub fn text_delimiter(&self) -> Option<&str> {
        self.text_delimiter.as_deref()
    }

    pub fn set_text_delimiter(&mut self, text_delimiter: &str) -> &mut Self {
        self.text_delimiter = Some(text_delimiter.to_string());
        self.options.insert(ingest_option::TEXT_DELIMITER, text_delimiter.to_string());
        self
    }

    pub fn text_escape_character(&self) -> Option<&str> {
        self.text_escape_character.as_deref()
    }

    pub fn set_text_escape_character(&mut self, text_escape_character: &str) -> &mut Self {
        self.text_escape_character = Some(text_escape_character.to_string());
        self.options.insert(ingest_option::TEXT_ESCAPE_CHARACTER, text_escape_character.to_string());
        self
    }

    pub fn is_text_has_header(&self) -> bool {
        self.text_has_header
    }

    pub fn set_text_has_header(&mut self, text_has_header: bool) -> &mut Self {
        self.text_has_header = text_has_header;
        self.options.insert(ingest_option::TEXT_HAS_HEADER, true_false(text_has_header));
        self
    }

    pub fn text_header_property_delimiter(&self) -> Option<&str> {
        self.text_header_property_delimiter.as_deref()
    }

    pub fn set_text_header_property_delimiter(&mut self, text_header_property_delimiter: &str) -> &mut Self {
        self.text_header_property_delimiter = Some(text_header_property_delimiter.to_string());
        self.options.insert(ingest_option::TEXT_HEADER_PROPERTY_DELIMITER, text_header_property_delimiter.to_string());
        self
    }

    pub fn text_null_string(&self) -> Option<&str> {
        self.text_null_string.as_deref()
    }

    pub fn set_text_null_string(&mut self, text_null_string: &str) -> &mut Self {
        self.text_null_string = Some(text_null_string.to_string());
        self.options.insert(ingest_option::TEXT_NULL_STRING, text_null_string.to_string());
        self
    }

    pub fn text_quote_character(&self) -> Option<&str> {
        self.text_quote_character.as_deref()
    }

    pub fn set_text_quote_character(&mut self, text_quote_character: &str) -> &mut Self {
        self.text_quote_character = Some(text_quote_character.to_string());
        self.options.insert(ingest_option::TEXT_QUOTE_CHARACTER, text_quote_character.to_string());
        self
    }

    pub fn is_truncate_table(&self) -> bool {
        self.truncate_table
    }

    pub fn set_truncate_table(&mut self, truncate_table: bool) -> &mut Self {
        self.truncate_table = truncate_table;
        self.options.insert(ingest_option::TRUNCATE_TABLE, true_false(truncate_table));
        self
    }

    pub fn type_inference_mode(&self) -> Option<TypeInferenceMode> {
        self.type_inference_mode
    }

    pub fn set_type_inference_mode(&mut self, type_inference_mode: TypeInferenceMode) -> &mut Self {
        self.type_inference_mode = Some(type_inference_mode);
        self.options.insert(ingest_option::TYPE_INFERENCE_MODE, type_inference_mode.as_str().to_string());
        self
    }

    /// Sets any option, by name, over what the setters set.
    pub fn set_option(&mut self, key: &str, value: impl ToString) -> &mut Self {
        self.additional_options.insert(key, value.to_string());
        self
    }

    /// The options to send.
    pub fn options(&self) -> HashMap<String, String> {
        self.options.merged(&self.additional_options)
    }
}

/// Options for the table an ingest creates: the create table options of
/// `/insert/records/fromfiles`.
///
/// Each setter sets its option as well as its value; an option without a
/// setter can be given with [`set_option`](Self::set_option).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableCreationOptions {
    type_id: Option<String>,
    no_error_if_exists: bool,
    is_replicated: bool,
    foreign_keys: Option<Vec<String>>,
    foreign_shard_key: Option<String>,
    partition_type: Option<PartitionType>,
    partition_keys: Option<Vec<String>>,
    partition_definitions: Option<Vec<String>>,
    automatic_partition: bool,
    ttl: i32,
    result_table: bool,
    additional_options: OrderedOptions,
    options: OrderedOptions,
}

impl TableCreationOptions {
    /// Options with nothing set.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn type_id(&self) -> Option<&str> {
        self.type_id.as_deref()
    }

    pub fn set_type_id(&mut self, type_id: &str) -> &mut Self {
        self.type_id = Some(type_id.to_string());
        self.options.insert(table_creation_option::TYPE_ID, type_id.to_string());
        self
    }

    pub fn is_no_error_if_exists(&self) -> bool {
        self.no_error_if_exists
    }

    pub fn set_no_error_if_exists(&mut self, no_error_if_exists: bool) -> &mut Self {
        self.no_error_if_exists = no_error_if_exists;
        self.options.insert(table_creation_option::NO_ERROR_IF_EXISTS, true_false(no_error_if_exists));
        self
    }

    pub fn is_replicated(&self) -> bool {
        self.is_replicated
    }

    /// Sets whether the table is replicated.
    pub fn set_replicated(&mut self, replicated: bool) -> &mut Self {
        self.is_replicated = replicated;
        self.options.insert(table_creation_option::IS_REPLICATED, true_false(replicated));
        self
    }

    pub fn foreign_keys(&self) -> Option<&[String]> {
        self.foreign_keys.as_deref()
    }

    /// Sets the foreign keys, sent joined by commas; an empty list is
    /// ignored, with a warning.
    pub fn set_foreign_keys(&mut self, foreign_keys: Vec<String>) -> &mut Self {
        if foreign_keys.is_empty() {
            warn!("FOREIGN_KEYS cannot be empty; value not set");
        } else {
            self.options.insert(table_creation_option::FOREIGN_KEYS, foreign_keys.join(","));
            self.foreign_keys = Some(foreign_keys);
        }
        self
    }

    pub fn foreign_shard_key(&self) -> Option<&str> {
        self.foreign_shard_key.as_deref()
    }

    /// Sets the foreign shard key; an empty one is ignored, with a warning.
    pub fn set_foreign_shard_key(&mut self, foreign_shard_key: &str) -> &mut Self {
        if foreign_shard_key.is_empty() {
            warn!("FOREIGN_SHARD_KEY cannot be empty; value not set");
        } else {
            self.foreign_shard_key = Some(foreign_shard_key.to_string());
            self.options.insert(table_creation_option::FOREIGN_SHARD_KEY, foreign_shard_key.to_string());
        }
        self
    }

    pub fn partition_type(&self) -> Option<PartitionType> {
        self.partition_type
    }

    pub fn set_partition_type(&mut self, partition_type: PartitionType) -> &mut Self {
        self.partition_type = Some(partition_type);
        self.options.insert(table_creation_option::PARTITION_TYPE, partition_type.as_str().to_string());
        self
    }

    pub fn partition_keys(&self) -> Option<&[String]> {
        self.partition_keys.as_deref()
    }

    /// Sets the partition keys, sent joined by commas; an empty list is
    /// ignored, with a warning.
    pub fn set_partition_keys(&mut self, partition_keys: Vec<String>) -> &mut Self {
        if partition_keys.is_empty() {
            warn!("PARTITION_KEYS list cannot be empty; value not set");
        } else {
            self.options.insert(table_creation_option::PARTITION_KEYS, partition_keys.join(","));
            self.partition_keys = Some(partition_keys);
        }
        self
    }

    pub fn partition_definitions(&self) -> Option<&[String]> {
        self.partition_definitions.as_deref()
    }

    /// Sets the partition definitions, sent joined by commas; an empty list
    /// is ignored, with a warning.
    pub fn set_partition_definitions(&mut self, partition_definitions: Vec<String>) -> &mut Self {
        if partition_definitions.is_empty() {
            warn!("PARTITION_DEFINITIONS list cannot be empty; value not set");
        } else {
            self.options.insert(table_creation_option::PARTITION_DEFINITIONS, partition_definitions.join(","));
            self.partition_definitions = Some(partition_definitions);
        }
        self
    }

    pub fn is_automatic_partition(&self) -> bool {
        self.automatic_partition
    }

    pub fn set_automatic_partition(&mut self, automatic_partition: bool) -> &mut Self {
        self.automatic_partition = automatic_partition;
        self.options.insert(table_creation_option::IS_AUTOMATIC_PARTITION, true_false(automatic_partition));
        self
    }

    pub fn ttl(&self) -> i32 {
        self.ttl
    }

    pub fn set_ttl(&mut self, ttl: i32) -> &mut Self {
        self.ttl = ttl;
        self.options.insert(table_creation_option::TTL, ttl.to_string());
        self
    }

    pub fn is_result_table(&self) -> bool {
        self.result_table
    }

    pub fn set_result_table(&mut self, result_table: bool) -> &mut Self {
        self.result_table = result_table;
        self.options.insert(table_creation_option::IS_RESULT_TABLE, true_false(result_table));
        self
    }

    /// Sets any option, by name, over what the setters set.
    pub fn set_option(&mut self, key: &str, value: impl ToString) -> &mut Self {
        self.additional_options.insert(key, value.to_string());
        self
    }

    /// The options to send.
    pub fn options(&self) -> HashMap<String, String> {
        self.options.merged(&self.additional_options)
    }
}

/// What an ingest did.
#[derive(Debug, Clone, Default)]
pub struct IngestResult {
    successful: bool,
    error: Option<KineticaError>,
    error_message: Option<String>,
    files: Vec<String>,
    count_inserted: i64,
    count_skipped: i64,
    count_updated: i64,
    table_name: Option<String>,
}

impl IngestResult {
    pub fn is_successful(&self) -> bool {
        self.successful
    }

    pub fn error(&self) -> Option<&KineticaError> {
        self.error.as_ref()
    }

    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    /// The files the server ingested.
    pub fn files(&self) -> &[String] {
        &self.files
    }

    pub fn count_inserted(&self) -> i64 {
        self.count_inserted
    }

    pub fn count_skipped(&self) -> i64 {
        self.count_skipped
    }

    pub fn count_updated(&self) -> i64 {
        self.count_updated
    }

    /// Sets whether the ingest succeeded.
    pub fn set_successful(&mut self, successful: bool) -> &mut Self {
        self.successful = successful;
        self
    }

    /// Sets the error the ingest failed with.
    pub fn set_error(&mut self, error: Option<KineticaError>) -> &mut Self {
        self.error = error;
        self
    }

    /// Sets the message of the error the ingest failed with.
    pub fn set_error_message(&mut self, error_message: Option<String>) -> &mut Self {
        self.error_message = error_message;
        self
    }

    /// Sets the name of the table ingested into.
    pub fn set_table_name(&mut self, table_name: Option<String>) -> &mut Self {
        self.table_name = table_name;
        self
    }

    /// Sets the files the server ingested.
    pub fn set_files(&mut self, files: Vec<String>) -> &mut Self {
        self.files = files;
        self
    }

    /// Sets the number of records inserted.
    pub fn set_count_inserted(&mut self, count: i64) -> &mut Self {
        self.count_inserted = count;
        self
    }

    /// Sets the number of records skipped.
    pub fn set_count_skipped(&mut self, count: i64) -> &mut Self {
        self.count_skipped = count;
        self
    }

    /// Sets the number of records updated.
    pub fn set_count_updated(&mut self, count: i64) -> &mut Self {
        self.count_updated = count;
        self
    }

    /// The result of an ingest that gave `response`, or failed with `error`.
    fn from(response: Option<InsertRecordsFromFilesResponse>, error: Option<KineticaError>) -> Self {
        let mut result = IngestResult {
            successful: error.is_none(),
            error_message: error.as_ref().map(KineticaError::message),
            error,
            ..Default::default()
        };
        if let Some(response) = response {
            result.files = response.files;
            result.table_name = Some(response.table_name);
            result.count_inserted = response.count_inserted;
            result.count_skipped = response.count_skipped;
            result.count_updated = response.count_updated;
        }
        result
    }
}

/// Uploads files and inserts their records into a table.
pub(crate) struct FileIngestor {
    db: Arc<Kinetica>,
    table_name: String,
    file_names: Vec<String>,
    ingest_options: IngestOptions,
    create_table_options: TableCreationOptions,
}

impl FileIngestor {
    pub(crate) fn new(
        db: Arc<Kinetica>,
        table_name: &str,
        file_names: Vec<String>,
        ingest_options: Option<IngestOptions>,
        create_table_options: Option<TableCreationOptions>,
    ) -> Self {
        Self {
            db,
            table_name: table_name.to_string(),
            file_names,
            ingest_options: ingest_options.unwrap_or_default(),
            create_table_options: create_table_options.unwrap_or_default(),
        }
    }

    /// Uploads the files to a directory of their own under the user's KiFS
    /// home directory, inserts their records from there, and deletes the
    /// directory.
    ///
    /// The files are uploaded with the default upload and file handler
    /// options. Each is then named by the path it was uploaded to.
    // A bare file name would lose the subdirectory of a file found by a
    // recursive search.
    pub(crate) async fn ingest_from_files(&self) -> IngestResult {
        let staging_dir = format!("{REMOTE_USER_HOME_DIR_PREFIX}{KIFS_PATH_SEPARATOR}{}", Uuid::new_v4());
        let uploader = FileUploader::new(
            self.db.clone(),
            self.file_names.clone(),
            staging_dir.clone(),
            UploadOptions::default(),
            KifsFileHandlerOptions::default(),
            None,
        );

        let outcome = async {
            let (local_files, remote_files) = uploader.resolve().await?;
            uploader.upload_resolved(&local_files, &remote_files).await?;
            // Each file once, in the order first found: a file two of the
            // names given both match is uploaded once, and must be loaded
            // once.
            let mut named = std::collections::HashSet::new();
            let filepaths = remote_files
                .iter()
                .filter(|remote| named.insert(remote.as_str()))
                .map(|remote| format!("{KIFS_PATH_PREFIX}{remote}"))
                .collect();
            let request = InsertRecordsFromFilesRequest {
                table_name: self.table_name.clone(),
                filepaths,
                modify_columns: HashMap::new(),
                create_table_options: self.create_table_options.options(),
                options: self.ingest_options.options(),
            };
            self.db.insert_records_from_files_raw(&request).await
        }
        .await;

        if let Err(e) = &outcome {
            error!("{}", e.message());
        }
        self.delete_staging_dir(&staging_dir).await;
        match outcome {
            Ok(response) => IngestResult::from(Some(response), None),
            Err(e) => IngestResult::from(None, Some(e)),
        }
    }

    /// Deletes the directory the files were uploaded to, and all in it.
    async fn delete_staging_dir(&self, staging_dir: &str) {
        let request = crate::DeleteDirectoryRequest {
            directory_name: staging_dir.to_string(),
            options: HashMap::from([
                ("recursive".to_string(), "true".to_string()),
                ("no_error_if_not_exists".to_string(), "true".to_string()),
            ]),
        };
        if let Err(e) = self.db.delete_directory_raw(&request).await {
            error!("{}", e.message());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_are_written_by_their_setters() {
        let mut options = IngestOptions::new();
        options
            .set_bad_record_table_name("bad")
            .set_bad_record_table_limit(10)
            .set_column_formats(vec![("d".into(), ("date".into(), "%Y".into()))])
            .set_default_column_formats(vec![("time".into(), "%H".into())])
            .set_error_handling_mode(ErrorHandlingMode::IgnoreBadRecords)
            .set_file_type(FileType::Parquet)
            .set_primary_keys(vec!["a".into(), "b".into()])
            .set_text_has_header(true)
            .set_type_inference_mode(TypeInferenceMode::Accuracy)
            .set_option(ingest_option::BATCH_SIZE, 1000)
            .set_option(ingest_option::FILE_TYPE, "json");
        options.set_columns_to_load(vec!["a".into(), "c".into(), "a".into()]).unwrap();
        assert_eq!(
            options.set_columns_to_skip(vec!["c".into()]).unwrap_err().to_string(),
            "Configuration error:  Columns to load and columns to skip are mutually exclusive .."
        );

        let sent = options.options();
        let expected: HashMap<String, String> = [
            ("bad_record_table_name", "bad"),
            ("bad_record_table_limit", "10"),
            ("column_formats", r#"{"d":{"date":"%Y"}}"#),
            ("default_column_formats", r#"{"time":"%H"}"#),
            ("error_handling", "ignore_bad_records"),
            ("file_type", "json"),
            ("primary_keys", "a,b"),
            ("text_has_header", "true"),
            ("type_inference_mode", "accuracy"),
            ("batch_size", "1000"),
            ("columns_to_load", r#"["a","c"]"#),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(sent, expected, "what put sets wins");
        assert_eq!(options.file_type(), Some(FileType::Parquet));

        // Empty values are ignored
        let mut empty = IngestOptions::new();
        empty.set_bad_record_table_name("").set_primary_keys(Vec::new()).set_shard_keys(Vec::new());
        assert!(empty.options().is_empty());
    }

    /// What a stand-in server was asked: the uploads' file names, the
    /// ingest's paths and options, and the directories deleted.
    #[derive(Default)]
    struct Asked {
        uploaded: Vec<String>,
        ingested: Vec<String>,
        options: HashMap<String, String>,
        create_table_options: HashMap<String, String>,
        deleted: Vec<String>,
    }

    async fn server(host: &str, fail_insert: bool) -> (crate::gpudb::fake_server::FakeServer, KifsFileHandler, Arc<parking_lot::Mutex<Asked>>) {
        use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
        #[derive(serde::Deserialize)]
        struct Upload {
            file_names: Vec<String>,
        }
        #[derive(serde::Deserialize)]
        struct Insert {
            filepaths: Vec<String>,
            create_table_options: HashMap<String, String>,
            options: HashMap<String, String>,
        }
        #[derive(serde::Deserialize)]
        struct Delete {
            directory_name: String,
        }
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let asked: Arc<parking_lot::Mutex<Asked>> = Default::default();
        let log = asked.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            let reply = match path {
                "/upload/files" => {
                    let request: Upload =
                        serde_avro_fast::from_datum_slice(body, &crate::generated::UPLOAD_FILES_REQUEST_SCHEMA).unwrap();
                    log.lock().uploaded.extend(request.file_names);
                    encode(&crate::generated::UPLOAD_FILES_RESPONSE_SCHEMA, &crate::UploadFilesResponse::default())
                }
                "/insert/records/fromfiles" => {
                    let request: Insert = serde_avro_fast::from_datum_slice(
                        body,
                        &crate::generated::INSERT_RECORDS_FROM_FILES_REQUEST_SCHEMA,
                    )
                    .unwrap();
                    let mut asked = log.lock();
                    asked.ingested = request.filepaths.clone();
                    asked.options = request.options;
                    asked.create_table_options = request.create_table_options;
                    if fail_insert {
                        return Some(Err("no such table".to_string()));
                    }
                    encode(
                        &crate::generated::INSERT_RECORDS_FROM_FILES_RESPONSE_SCHEMA,
                        &InsertRecordsFromFilesResponse {
                            table_name: "t".into(),
                            count_inserted: 3,
                            files: request.filepaths,
                            ..Default::default()
                        },
                    )
                }
                "/delete/directory" => {
                    let request: Delete =
                        serde_avro_fast::from_datum_slice(body, &crate::generated::DELETE_DIRECTORY_REQUEST_SCHEMA).unwrap();
                    log.lock().deleted.push(request.directory_name);
                    encode(&crate::generated::DELETE_DIRECTORY_RESPONSE_SCHEMA, &crate::DeleteDirectoryResponse::default())
                }
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let mut options = crate::KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Arc::new(Kinetica::new(vec![server.url.to_string()], options).await.expect("client"));
        (server, KifsFileHandler::new(k, None), asked)
    }

    use super::super::file_operation::test_support::TempDir;
    use super::super::KifsFileHandler;

    // The files uploaded to a directory of their own under the home
    // directory, inserted from there with the options given, and the
    // directory deleted -- the right one, and each file by the path it was
    // uploaded to.
    #[tokio::test]
    async fn files_are_ingested_through_a_staging_directory() {
        let (_server, handler, asked) = server("127.0.0.161", false).await;
        let dir = TempDir::new("ingest");
        dir.write("a.csv", b"1");
        dir.write("sub/b.csv", b"2");
        let mut ingest_options = IngestOptions::new();
        ingest_options.set_file_type(FileType::DelimitedText);
        let mut table_options = TableCreationOptions::new();
        table_options.set_replicated(true);

        let result = handler
            .ingest(vec![format!("{}/*.csv", dir.str())], "t", Some(ingest_options), Some(table_options))
            .await
            .unwrap();
        assert!(result.is_successful());
        assert_eq!(result.count_inserted(), 3);
        assert_eq!(result.table_name(), Some("t"));

        let asked = asked.lock();
        let staging = asked.deleted[0].clone();
        assert!(staging.starts_with("~/") && staging.len() == 2 + 36, "{staging}");
        let mut uploaded = asked.uploaded.clone();
        uploaded.sort();
        assert_eq!(uploaded, [format!("{staging}/a.csv"), format!("{staging}/sub/b.csv")], "found recursively");
        let mut ingested = asked.ingested.clone();
        ingested.sort();
        assert_eq!(ingested, [format!("kifs://{staging}/a.csv"), format!("kifs://{staging}/sub/b.csv")]);
        assert_eq!(asked.options["file_type"], "delimited_text");
        assert_eq!(asked.create_table_options["is_replicated"], "true");
        assert_eq!(asked.deleted, std::slice::from_ref(&staging));
    }

    // A file two of the names given both match is uploaded once and loaded
    // once.
    #[tokio::test]
    async fn a_file_matched_twice_is_ingested_once() {
        let (_server, handler, asked) = server("127.0.0.163", false).await;
        let dir = TempDir::new("ingest_twice");
        dir.write("a.csv", b"1");
        dir.write("b.csv", b"2");

        handler
            .ingest(vec![format!("{}/a.csv", dir.str()), format!("{}/*.csv", dir.str())], "t", None, None)
            .await
            .unwrap();

        let asked = asked.lock();
        let staging = asked.deleted[0].clone();
        assert_eq!(asked.ingested, [format!("kifs://{staging}/a.csv"), format!("kifs://{staging}/b.csv")]);
        let mut uploaded = asked.uploaded.clone();
        uploaded.sort();
        assert_eq!(uploaded, [format!("{staging}/a.csv"), format!("{staging}/b.csv")]);
    }

    /// A failed insert is the ingest's error, and the staging directory is
    /// deleted all the same.
    #[tokio::test]
    async fn a_failed_ingest_still_cleans_up() {
        let (_server, handler, asked) = server("127.0.0.162", true).await;
        let dir = TempDir::new("ingest_failure");
        let file = dir.write("a.csv", b"1").to_string_lossy().to_string();

        let err = handler.ingest(vec![file], "t", None, None).await.unwrap_err();
        assert_eq!(err.to_string(), "API error: no such table");
        assert_eq!(asked.lock().deleted.len(), 1);

        assert_eq!(
            handler.ingest(Vec::new(), "t", None, None).await.unwrap_err().to_string(),
            "Configuration error: List of files to ingest cannot be empty"
        );
        assert_eq!(
            handler.ingest(vec!["/no/such.csv".into()], "t", None, None).await.unwrap_err().to_string(),
            "Configuration error: Input file names [[/no/such.csv]] to ingest are not found "
        );
    }

    #[test]
    fn a_replicated_table_is_asked_for_as_given() {
        let mut options = TableCreationOptions::new();
        options
            .set_no_error_if_exists(false)
            .set_replicated(true)
            .set_partition_type(PartitionType::Hash)
            .set_partition_keys(vec!["x".into(), "y".into()])
            .set_ttl(5);
        let sent = options.options();
        assert_eq!(sent["is_replicated"], "true");
        assert_eq!(sent["no_error_if_exists"], "false");
        assert_eq!(sent["partition_type"], "hash");
        assert_eq!(sent["partition_keys"], "x,y");
        assert_eq!(sent["ttl"], "5");
        assert_eq!(PartitionType::Interval.to_string(), "interval");
        assert_eq!(FileType::Json.default_partition_type(), "delimited_text");
    }
}
