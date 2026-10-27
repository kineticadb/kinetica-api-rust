//! Inserting and reading records as JSON.
//!
//! These go to `/insert/records/json` and `/get/records/json`, with their
//! parameters in the URL's query, the records to insert as the request body,
//! and the answer as JSON. They fail over on an exit, a connection failure or
//! a submit failure.

use crate::hyper_client::RawResponse;
use crate::text_format::double_to_string;
use crate::{Kinetica, KineticaError, END_OF_SET};
use log::{debug, trace, warn};
use std::collections::HashMap;
use std::fmt::Write as _;

/// How records are sent as JSON.
///
/// `compression_on` changes nothing: JSON requests are not compressed.
/// `validate_json` has a JSON [`BulkInserter`](crate::BulkInserter) check
/// each record it is given; `insert_records_from_json` leaves the checking to
/// the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JsonOptions {
    validate_json: bool,
    compression_on: bool,
}

impl Default for JsonOptions {
    /// No validation, compression on.
    fn default() -> Self {
        JsonOptions { validate_json: Self::VALIDATE_JSON, compression_on: Self::COMPRESSION_ON }
    }
}

impl std::fmt::Display for JsonOptions {
    /// `JsonOptions{validate_json=false, compression_on=true}`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsonOptions{{validate_json={}, compression_on={}}}", self.validate_json, self.compression_on)
    }
}

impl JsonOptions {
    /// Whether records are checked by default.
    pub const VALIDATE_JSON: bool = false;

    /// Whether compression is on by default.
    pub const COMPRESSION_ON: bool = true;

    /// Options with validation and compression on or off as given.
    pub fn new(validate_json: bool, compression_on: bool) -> Self {
        JsonOptions { validate_json, compression_on }
    }

    /// Whether records are checked.
    pub fn is_validate_json(&self) -> bool {
        self.validate_json
    }

    /// Whether compression is on.
    pub fn is_compression_on(&self) -> bool {
        self.compression_on
    }
}

/// A request to insert JSON records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InsertRecordsJsonRequest {
    /// The table to insert into.
    pub table_name: String,
    /// The records, as JSON text.
    pub data_text: String,
    /// The options of the table to create, if it does not exist.
    pub create_table_options: HashMap<String, String>,
    /// The insertion options.
    pub options: HashMap<String, String>,
}

/// The records `/get/records/json` returned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetRecordsJsonResponse {
    total_records: i32,
    has_more_records: bool,
    json_records: String,
}

impl GetRecordsJsonResponse {
    /// A response with every field given. [`Default`] gives one with no
    /// records.
    pub fn new(total_records: i32, has_more_records: bool, json_records: impl Into<String>) -> Self {
        GetRecordsJsonResponse { total_records, has_more_records, json_records: json_records.into() }
    }

    /// The total number of records the server reported.
    pub fn total_records(&self) -> i32 {
        self.total_records
    }

    /// Whether more records are left past those returned.
    pub fn has_more_records(&self) -> bool {
        self.has_more_records
    }

    /// The records, as a JSON array.
    pub fn json_records(&self) -> &str {
        &self.json_records
    }
}

impl std::fmt::Display for GetRecordsJsonResponse {
    /// `GetRecordsJsonResponse [total_records=<n>, has_more_records=<b>,
    /// json_records=<records>]`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetRecordsJsonResponse [total_records={}, has_more_records={}, json_records={}]",
            self.total_records, self.has_more_records, self.json_records
        )
    }
}

impl Kinetica {
    /// Inserts JSON records into `table_name`.
    pub async fn insert_records_from_json(
        &self,
        json_records: &str,
        table_name: &str,
    ) -> Result<serde_json::Map<String, serde_json::Value>, KineticaError> {
        self.insert_records_from_json_with_options(json_records, table_name, None, None, None).await
    }

    /// Inserts the records of `request`.
    pub async fn insert_records_from_json_request(
        &self,
        request: &InsertRecordsJsonRequest,
        json_options: Option<JsonOptions>,
    ) -> Result<serde_json::Map<String, serde_json::Value>, KineticaError> {
        self.insert_records_from_json_with_options(
            &request.data_text,
            &request.table_name,
            json_options,
            Some(request.create_table_options.clone()),
            Some(request.options.clone()),
        )
        .await
    }

    /// Inserts JSON records into `table_name`, creating it with
    /// `create_table_options` if need be.
    ///
    /// Returns the server's whole answer, without checking its `status`: a
    /// failure the server reports in it is for the caller to read.
    pub async fn insert_records_from_json_with_options(
        &self,
        json_records: &str,
        table_name: &str,
        json_options: Option<JsonOptions>,
        create_table_options: Option<HashMap<String, String>>,
        insert_records_options: Option<HashMap<String, String>>,
    ) -> Result<serde_json::Map<String, serde_json::Value>, KineticaError> {
        // The options change nothing that is sent
        let _ = json_options.unwrap_or_default();
        if json_records.is_empty() {
            return Err(KineticaError::Api("No records to insert".to_string()));
        }
        if table_name.is_empty() {
            return Err(KineticaError::Api("Table name not given, cannot insert".to_string()));
        }

        let mut insert_options = insert_records_options.unwrap_or_default();
        insert_options.insert("table_name".to_string(), table_name.to_string());
        // A key in both keeps the table creation option's value
        let mut combined: Vec<(String, String)> = create_table_options.unwrap_or_default().into_iter().collect();
        for (key, value) in insert_options {
            if !combined.iter().any(|(k, _)| *k == key) {
                combined.push((key, value));
            }
        }

        let endpoint = endpoint_url("/insert/records/json", &combined);
        let json = self.submit_json_request(&endpoint, Some(json_records)).await?;
        match serde_json::from_str(&json).expect("checked when the response was read") {
            serde_json::Value::Object(map) => Ok(map),
            _ => unreachable!("checked when the response was read"),
        }
    }

    /// Inserts JSON records through `insert_url`, a rank's `/insert/records`,
    /// to `/insert/records/json` beside it, as a JSON `BulkInserter` sends a
    /// batch. It does not fail over.
    pub(crate) async fn insert_records_from_json_at(
        &self,
        insert_url: &str,
        json_records: &str,
        table_name: &str,
        insert_records_options: Option<&HashMap<String, String>>,
    ) -> Result<serde_json::Map<String, serde_json::Value>, KineticaError> {
        if json_records.is_empty() {
            return Err(KineticaError::Api("No records to insert".to_string()));
        }
        if table_name.is_empty() {
            return Err(KineticaError::Api("Table name not given, cannot insert".to_string()));
        }

        let mut options: Vec<(String, String)> = insert_records_options
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        // The table name given wins over one in the options
        match options.iter_mut().find(|(k, _)| k == "table_name") {
            Some(option) => option.1 = table_name.to_string(),
            None => options.push(("table_name".to_string(), table_name.to_string())),
        }

        let url = format!("{}{}", insert_url.trim_end_matches('/'), endpoint_url("/json", &options));
        let json = self.submit_json_request_raw(&url, Some(json_records)).await?;
        match serde_json::from_str(&json).expect("checked when the response was read") {
            serde_json::Value::Object(map) => Ok(map),
            _ => unreachable!("checked when the response was read"),
        }
    }

    /// Reads records of `table_name` as JSON.
    pub async fn get_records_json(
        &self,
        table_name: &str,
        column_names: &[String],
        offset: i64,
        limit: i64,
    ) -> Result<GetRecordsJsonResponse, KineticaError> {
        self.get_records_json_with_options(table_name, column_names, offset, limit, None, &[], None).await
    }

    /// Reads records of `table_name` as JSON, filtered by `expression`,
    /// ordered by `order_by_columns` and filtered after grouping by
    /// `having_clause`, each where given.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_records_json_with_options(
        &self,
        table_name: &str,
        column_names: &[String],
        offset: i64,
        limit: i64,
        expression: Option<&str>,
        order_by_columns: &[String],
        having_clause: Option<&str>,
    ) -> Result<GetRecordsJsonResponse, KineticaError> {
        if table_name.is_empty() {
            return Err(KineticaError::Api("Table name not given, cannot retrieve records".to_string()));
        }

        let mut options: Vec<(String, String)> = vec![("table_name".to_string(), table_name.to_string())];
        if !column_names.is_empty() {
            options.push(("column_names".to_string(), column_names.join(",")));
        }
        options.push(("offset".to_string(), offset.max(0).to_string()));
        options.push(("limit".to_string(), if limit < 0 { END_OF_SET } else { limit }.to_string()));
        if let Some(expression) = expression.filter(|e| !e.is_empty()) {
            options.push(("expression".to_string(), expression.to_string()));
        }
        if !order_by_columns.is_empty() {
            options.push(("order_by".to_string(), order_by_columns.join(",")));
        }
        if let Some(having) = having_clause.filter(|h| !h.is_empty()) {
            options.push(("having".to_string(), having.to_string()));
        }

        let endpoint = endpoint_url("/get/records/json", &options);
        let json = self.submit_json_request(&endpoint, None).await?;
        records_json_response(&json)
    }

    /// Sends a JSON request to the head node, failing over on an exit, a
    /// connection failure or a submit failure, and returns the answer's text.
    async fn submit_json_request(&self, endpoint: &str, payload: Option<&str>) -> Result<String, KineticaError> {
        let mut url = self.url().ok_or_else(|| KineticaError::Connection("No URL available".to_string()))?;
        let original_url = url.clone();

        loop {
            let current_cluster_switch_count = self.num_cluster_switches();
            let full_url = crate::gpudb::append_path_to_url(&url, endpoint).to_string();
            let error = match self.submit_json_request_raw(&full_url, payload).await {
                Ok(json) => return Ok(json),
                Err(e @ (KineticaError::Exit(_) | KineticaError::Connection(_))) => {
                    warn!("Got an exit error when trying endpoint {endpoint} at {url}: {e}; switch URL...");
                    e
                }
                Err(e @ KineticaError::Http(_)) => {
                    warn!("Got an HTTP error when trying endpoint {endpoint} at {url}: {e}; switch URL...");
                    e
                }
                Err(e) => {
                    debug!("Got a non-HTTP error, so propagating: {e}");
                    return Err(e);
                }
            };
            match self.switch_url(&original_url, current_cluster_switch_count).await {
                Ok(new_url) => {
                    debug!("Switched to {new_url}");
                    url = new_url;
                }
                Err(KineticaError::HaUnavailable(ha) | KineticaError::FailoverDisabled(ha)) => {
                    return Err(crate::gpudb::failover_exhausted(&error, &ha));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Sends one JSON request to `url`, without failing over.
    async fn submit_json_request_raw(&self, url: &str, payload: Option<&str>) -> Result<String, KineticaError> {
        trace!("Sending request to {url}");
        let resp = self
            .hyper_client()
            .post_json_raw(url, payload.map(str::to_string), &self.request_headers())
            .await?;
        interpret_json_response(url, &resp, payload.is_some())
    }
}

/// Interprets a JSON answer. With a payload, the answer must be a JSON
/// object, which is read before anything else; a 400 whose message says the
/// server is shutting down is an exit.
fn interpret_json_response(url: &str, resp: &RawResponse, posted: bool) -> Result<String, KineticaError> {
    let (status, reason) = (resp.status, resp.reason.as_str());
    let json = String::from_utf8_lossy(&resp.body).into_owned();

    let object = if posted {
        match serde_json::from_str::<serde_json::Value>(&json) {
            Ok(serde_json::Value::Object(map)) => Some(map),
            Ok(other) => {
                return Err(KineticaError::Http(format!(
                    "Expected a JSON object in the response; found {}",
                    json_kind(&other)
                )))
            }
            Err(e) => return Err(KineticaError::Http(e.to_string())),
        }
    } else {
        None
    };
    trace!("<Status|Message|Type|Response>: <{status}|{reason}|{:?}|{json}>", resp.content_type);

    // An HTML page from the httpd, say
    if resp.content_type.as_deref().is_some_and(|ct| ct.starts_with("text")) {
        return Err(KineticaError::Http(format!(
            "Cannot parse response from server: '{reason}'; status code: {status}"
        )));
    }

    if status == 401 || status == 403 {
        debug!("Got status code: {status}");
        return Err(KineticaError::Unauthorized(format!("Server response status: {status} : {reason}")));
    }

    let exit_messages = ["Kinetica is exiting", "Connection refused", "Connection reset", "system-limited-fatal", "Kinetica is offline"];
    if matches!(status, 500 | 502 | 503 | 504) || exit_messages.iter().any(|m| reason.contains(m)) {
        debug!("Returning an exit error from {url}; response_code: {status}; content type {:?}; response message: {reason}", resp.content_type);
        return Err(KineticaError::Exit(reason.to_string()));
    }

    if status == 400 {
        if let Some(object) = &object {
            let Some(message) = object.get("message") else {
                return Err(KineticaError::Http("The response has no `message`".to_string()));
            };
            if json_text(message).contains("System shutting down") {
                return Err(KineticaError::Exit("Kinetica shutting down".to_string()));
            }
        }
    }

    Ok(json)
}

/// Reads `/get/records/json`'s answer.
fn records_json_response(json: &str) -> Result<GetRecordsJsonResponse, KineticaError> {
    let unreadable = |reason: String| KineticaError::caused("Error reading JSON response", KineticaError::Api(reason));
    let empty = || KineticaError::Api("Empty response from server gettting JSON records".to_string());
    // A null answer is empty, and anything but an object cannot be read
    let fields = match OrderedJson::parse(json).map_err(unreadable)? {
        OrderedJson::Object(fields) if fields.is_empty() => return Err(empty()),
        OrderedJson::Object(fields) => fields,
        OrderedJson::Null => return Err(empty()),
        other => return Err(unreadable(format!("Expected a JSON object; found {}", other.kind()))),
    };
    let fields = &fields;
    let get = |fields: &[(String, OrderedJson)], key: &str| fields.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone());

    let status = get(fields, "status");
    if !matches!(&status, Some(OrderedJson::String(s)) if s == "OK") {
        let message = get(fields, "message")
            .filter(|m| !matches!(m, OrderedJson::Null))
            .map_or_else(|| "(no message)".to_string(), |m| m.text());
        return Err(KineticaError::Api(format!("Error getting JSON records: {message}")));
    }

    let missing = |path: &str| KineticaError::Config(format!("The response has no `{path}`"));
    let mismatch = |path: &str, value: &OrderedJson, expected: &str| {
        KineticaError::Config(type_mismatch(path, value.kind(), expected))
    };
    let data = match get(fields, "data") {
        Some(OrderedJson::Object(data)) => data,
        Some(other) => return Err(mismatch("data", &other, "object")),
        None => return Err(missing("data")),
    };
    let total_records = match get(&data, "total_number_of_records") {
        Some(value) => match &value {
            OrderedJson::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
            _ => None,
        }
        .ok_or_else(|| mismatch("data.total_number_of_records", &value, "i32"))?,
        None => return Err(missing("data.total_number_of_records")),
    };
    let has_more_records = match get(&data, "has_more_records") {
        Some(OrderedJson::Bool(b)) => b,
        Some(other) => return Err(mismatch("data.has_more_records", &other, "boolean")),
        None => return Err(missing("data.has_more_records")),
    };
    let json_records = match get(&data, "records") {
        Some(records @ OrderedJson::Array(_)) => records.to_jackson_string(),
        Some(OrderedJson::Null) | None => "null".to_string(),
        Some(other) => return Err(mismatch("data.records", &other, "array")),
    };
    Ok(GetRecordsJsonResponse { total_records, has_more_records, json_records })
}

/// The message for a response field of the wrong type.
pub(crate) fn type_mismatch(path: &str, found: &str, expected: &str) -> String {
    format!("`{path}` must be {expected}; found {found}")
}

/// The kind of a JSON value, as messages name it: an integer by the width it
/// needs.
pub(crate) fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => match n.as_i64() {
            Some(v) if i32::try_from(v).is_ok() => "i32",
            Some(_) => "i64",
            None => "integer beyond i64",
        },
        serde_json::Value::Number(_) => "f64",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// A parsed JSON value's text: a string as it is, anything else as JSON.
pub(crate) fn json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `root` with `params` as its query, each name and value encoded by
/// [`url_encode`].
fn endpoint_url(root: &str, params: &[(String, String)]) -> String {
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
        .collect();
    format!("{root}?{}", query.join("&"))
}

/// `text` form-encoded: letters, digits and `.-*_` as they are, a space as
/// `+`, and every other byte of the UTF-8 as `%XX`.
fn url_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => out.push(byte as char),
            b' ' => out.push('+'),
            other => {
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

/// A parsed JSON value, with an object's keys in order and each number typed
/// as a [`JsonNumber`].
#[derive(Debug, Clone, PartialEq)]
enum OrderedJson {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<OrderedJson>),
    Object(Vec<(String, OrderedJson)>),
}

/// A JSON number: an integer, kept to every digit, or a double.
#[derive(Debug, Clone, PartialEq)]
enum JsonNumber {
    /// A number without a fraction or exponent, of any size, every digit
    /// kept. Held as the digits it is written back as.
    Integer(String),
    /// Any other number, infinite where the text overflows an `f64`.
    Double(f64),
}

impl JsonNumber {
    /// The value as an `i64`, where it is an integer that fits one.
    fn as_i64(&self) -> Option<i64> {
        match self {
            JsonNumber::Integer(digits) => digits.parse().ok(),
            JsonNumber::Double(_) => None,
        }
    }

    /// The number's kind, as messages name it (see [`json_kind`]).
    fn kind(&self) -> &'static str {
        match self {
            JsonNumber::Integer(_) => match self.as_i64() {
                Some(v) if i32::try_from(v).is_ok() => "i32",
                Some(_) => "i64",
                None => "integer beyond i64",
            },
            JsonNumber::Double(_) => "f64",
        }
    }
}

impl OrderedJson {
    /// Reads `text` as strict JSON, with at most 1,000 levels of nesting and
    /// 1,000 digits in an integer, and whatever follows the first value
    /// ignored.
    fn parse(text: &str) -> Result<OrderedJson, String> {
        let mut reader = JsonReader { text: text.as_bytes(), position: 0, depth: 0 };
        reader.skip_whitespace();
        if reader.position == reader.text.len() {
            return Err("The text holds no JSON value".to_string());
        }
        reader.value()
    }

    /// The value as compact JSON, with keys in order, a double as
    /// [`double_to_string`] writes it and an infinite one as the string
    /// `"Infinity"` or `"-Infinity"`.
    fn to_jackson_string(&self) -> String {
        let mut out = String::new();
        self.write_jackson(&mut out);
        out
    }

    fn write_jackson(&self, out: &mut String) {
        match self {
            OrderedJson::Null => out.push_str("null"),
            OrderedJson::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            OrderedJson::Number(JsonNumber::Integer(digits)) => out.push_str(digits),
            OrderedJson::Number(JsonNumber::Double(v)) if v.is_infinite() => {
                out.push_str(if *v > 0.0 { "\"Infinity\"" } else { "\"-Infinity\"" })
            }
            OrderedJson::Number(JsonNumber::Double(v)) => out.push_str(&double_to_string(*v)),
            OrderedJson::String(s) => out.push_str(&serde_json::to_string(s).expect("a string is JSON")),
            OrderedJson::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_jackson(out);
                }
                out.push(']');
            }
            OrderedJson::Object(fields) => {
                out.push('{');
                for (i, (key, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(key).expect("a string is JSON"));
                    out.push(':');
                    value.write_jackson(out);
                }
                out.push('}');
            }
        }
    }

    /// The value's text: a string as it is, a double in its shortest
    /// round-trip form, anything else as JSON.
    fn text(&self) -> String {
        match self {
            OrderedJson::String(s) => s.clone(),
            OrderedJson::Number(JsonNumber::Double(v)) => double_to_string(*v),
            other => other.to_jackson_string(),
        }
    }

    /// The value's kind, as messages name it (see [`json_kind`]).
    fn kind(&self) -> &'static str {
        match self {
            OrderedJson::Null => "null",
            OrderedJson::Bool(_) => "boolean",
            OrderedJson::Number(n) => n.kind(),
            OrderedJson::String(_) => "string",
            OrderedJson::Array(_) => "array",
            OrderedJson::Object(_) => "object",
        }
    }
}

/// The most levels of nesting, and the most digits in an integer, that a
/// parse allows.
const MAX_NESTING_DEPTH: usize = 1000;
const MAX_NUMBER_LENGTH: usize = 1000;

/// Reads strict JSON text.
struct JsonReader<'a> {
    text: &'a [u8],
    position: usize,
    depth: usize,
}

impl JsonReader<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.get(self.position).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.position += 1;
        }
    }

    fn unexpected(&self, expecting: &str) -> String {
        match self.peek() {
            Some(c) => format!("Unexpected character '{}' at byte {}: {expecting}", c.escape_ascii(), self.position),
            None => format!("Unexpected end of input: {expecting}"),
        }
    }

    fn value(&mut self) -> Result<OrderedJson, String> {
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(OrderedJson::String(self.string()?)),
            Some(b'-' | b'0'..=b'9') => Ok(OrderedJson::Number(self.number()?)),
            Some(b't' | b'f' | b'n') => self.literal(),
            _ => Err(self.unexpected("expected a JSON value")),
        }
    }

    /// Opens an object or array, within [`MAX_NESTING_DEPTH`].
    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_NESTING_DEPTH {
            return Err(format!(
                "Nesting depth {} exceeds the limit of {MAX_NESTING_DEPTH}",
                self.depth
            ));
        }
        self.position += 1;
        self.skip_whitespace();
        Ok(())
    }

    fn object(&mut self) -> Result<OrderedJson, String> {
        self.enter()?;
        let mut fields: Vec<(String, OrderedJson)> = Vec::new();
        if self.peek() == Some(b'}') {
            self.position += 1;
            self.depth -= 1;
            return Ok(OrderedJson::Object(fields));
        }
        loop {
            if self.peek() != Some(b'"') {
                return Err(self.unexpected("expected '\"' to start an object key"));
            }
            let key = self.string()?;
            self.skip_whitespace();
            if self.peek() != Some(b':') {
                return Err(self.unexpected("expected ':' after an object key"));
            }
            self.position += 1;
            self.skip_whitespace();
            let value = self.value()?;
            // A repeated key keeps its first place and its last value
            match fields.iter_mut().find(|(k, _)| *k == key) {
                Some(field) => field.1 = value,
                None => fields.push((key, value)),
            }
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.position += 1;
                    self.skip_whitespace();
                }
                Some(b'}') => {
                    self.position += 1;
                    self.depth -= 1;
                    return Ok(OrderedJson::Object(fields));
                }
                _ => return Err(self.unexpected("expected ',' or '}' in an object")),
            }
        }
    }

    fn array(&mut self) -> Result<OrderedJson, String> {
        self.enter()?;
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.position += 1;
            self.depth -= 1;
            return Ok(OrderedJson::Array(items));
        }
        loop {
            items.push(self.value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.position += 1;
                    self.skip_whitespace();
                }
                Some(b']') => {
                    self.position += 1;
                    self.depth -= 1;
                    return Ok(OrderedJson::Array(items));
                }
                _ => return Err(self.unexpected("expected ',' or ']' in an array")),
            }
        }
    }

    /// `true`, `false` or `null`, not followed by more of a word.
    fn literal(&mut self) -> Result<OrderedJson, String> {
        let rest = &self.text[self.position..];
        let word_end = rest.iter().position(|c| !(c.is_ascii_alphanumeric() || *c == b'_')).unwrap_or(rest.len());
        let value = match &rest[..word_end] {
            b"true" => OrderedJson::Bool(true),
            b"false" => OrderedJson::Bool(false),
            b"null" => OrderedJson::Null,
            word => {
                return Err(format!(
                    "Unrecognized token '{}': expected a JSON value",
                    String::from_utf8_lossy(word)
                ))
            }
        };
        self.position += word_end;
        Ok(value)
    }

    /// A number: an integer where it has no fraction or exponent, else a
    /// double.
    fn number(&mut self) -> Result<JsonNumber, String> {
        let start = self.position;
        let digits = |reader: &mut Self| {
            let from = reader.position;
            while matches!(reader.peek(), Some(b'0'..=b'9')) {
                reader.position += 1;
            }
            reader.position - from
        };
        if self.peek() == Some(b'-') {
            self.position += 1;
        }
        let integer_start = self.position;
        match digits(self) {
            0 => return Err(self.unexpected("expected a digit after '-'")),
            n if n > 1 && self.text[integer_start] == b'0' => {
                return Err("Invalid number: leading zeros are not allowed".to_string())
            }
            _ => {}
        }
        let mut is_integer = true;
        if self.peek() == Some(b'.') {
            is_integer = false;
            self.position += 1;
            if digits(self) == 0 {
                return Err(self.unexpected("expected a digit after the decimal point"));
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_integer = false;
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            if digits(self) == 0 {
                return Err(self.unexpected("expected a digit in the exponent"));
            }
        }
        let text = std::str::from_utf8(&self.text[start..self.position]).expect("ASCII");
        if !is_integer {
            return Ok(JsonNumber::Double(text.parse().expect("a JSON number is a Rust float")));
        }
        let length = self.position - integer_start;
        if length > MAX_NUMBER_LENGTH {
            return Err(format!(
                "A number of {length} digits exceeds the limit of {MAX_NUMBER_LENGTH}"
            ));
        }
        // -0 is written back as 0
        Ok(JsonNumber::Integer(if text == "-0" { "0".to_string() } else { text.to_string() }))
    }

    /// A string, its escapes undone. A lone surrogate, which a Rust string
    /// cannot hold, becomes U+FFFD.
    fn string(&mut self) -> Result<String, String> {
        self.position += 1;
        let mut out = String::new();
        loop {
            let run_start = self.position;
            while matches!(self.peek(), Some(c) if c != b'"' && c != b'\\' && c >= 0x20) {
                self.position += 1;
            }
            out.push_str(std::str::from_utf8(&self.text[run_start..self.position]).expect("UTF-8 text"));
            match self.peek() {
                Some(b'"') => {
                    self.position += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.position += 1;
                    self.escape(&mut out)?;
                }
                Some(c) => {
                    return Err(format!(
                        "Control character (code {c}) in a string must be escaped"
                    ))
                }
                None => return Err("Unexpected end of input: expected the closing '\"' of a string".to_string()),
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), String> {
        let Some(c) = self.peek() else {
            return Err("Unexpected end of input in an escape sequence".to_string());
        };
        self.position += 1;
        out.push(match c {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => self.unicode_escape()?,
            other => return Err(format!("Invalid escape '\\{}' in a string", other.escape_ascii())),
        });
        Ok(())
    }

    /// The char of a `\uXXXX` escape, taking the escaped low surrogate that
    /// follows a high one with it. A lone surrogate is U+FFFD.
    fn unicode_escape(&mut self) -> Result<char, String> {
        let unit = self.hex4()?;
        if (0xd800..0xdc00).contains(&unit) && self.text[self.position..].starts_with(b"\\u") {
            let before_low = self.position;
            self.position += 2;
            match self.hex4() {
                Ok(low) if (0xdc00..0xe000).contains(&low) => {
                    let code = 0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00);
                    return Ok(char::from_u32(code).expect("a surrogate pair is a char"));
                }
                // Not a low surrogate: read again as an escape of its own
                _ => self.position = before_low,
            }
        }
        Ok(char::from_u32(unit).unwrap_or(char::REPLACEMENT_CHARACTER))
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let digits = self.text.get(self.position..self.position + 4).ok_or("Unexpected end of input in an escape sequence")?;
        let text = std::str::from_utf8(digits).map_err(|_| "Expected four hex digits in a \\u escape".to_string())?;
        let unit = u32::from_str_radix(text, 16)
            .ok()
            .filter(|_| text.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or_else(|| format!("Expected four hex digits in a \\u escape; found '{text}'"))?;
        self.position += 4;
        Ok(unit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_values_are_form_url_encoded() {
        assert_eq!(url_encode("a b&c=d/é.*-_~"), "a+b%26c%3Dd%2F%C3%A9.*-_%7E");
        assert_eq!(
            endpoint_url("/get/records/json", &[("table_name".into(), "s.t".into()), ("limit".into(), "-9999".into())]),
            "/get/records/json?table_name=s.t&limit=-9999"
        );
    }

    // The records written back with their keys in order, and the messages
    // for a failure.
    #[test]
    fn records_are_read_and_written_back_as_jackson_does() {
        let json = r#"{"status":"OK","message":"","data":{"total_number_of_records":2,"has_more_records":false,
            "records":[{"z":1,"a":"x"},{"z":2.5,"a":null,"b":1e10}]}}"#;
        let response = records_json_response(json).unwrap();
        assert_eq!(response.total_records(), 2);
        assert!(!response.has_more_records());
        assert_eq!(response.json_records(), r#"[{"z":1,"a":"x"},{"z":2.5,"a":null,"b":1.0E10}]"#);

        assert_eq!(
            records_json_response(r#"{"status":"ERROR","message":"no such table"}"#).unwrap_err().to_string(),
            "API error: Error getting JSON records: no such table"
        );
        assert_eq!(
            records_json_response("{}").unwrap_err().to_string(),
            "API error: Empty response from server gettting JSON records"
        );
        assert_eq!(records_json_response("{").unwrap_err().to_string(), "Error reading JSON response");
        assert!(records_json_response(
            r#"{"status":"OK","data":{"total_number_of_records":3000000000,"has_more_records":false,"records":[]}}"#
        )
        .unwrap_err()
        .to_string()
        .contains("`data.total_number_of_records` must be i32; found i64"));
    }

    // Against a stand-in server: the parameters in the query, the records as
    // the body, and the answer returned or read.
    #[tokio::test]
    async fn json_records_are_sent_and_read() {
        use crate::gpudb::fake_server::{self, address, cluster_properties};
        let server = fake_server::start("127.0.0.98").await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let sent: std::sync::Arc<parking_lot::Mutex<Vec<(String, String)>>> = Default::default();
        let log = sent.clone();
        *server.behavior.json_handler.lock() = Some(std::sync::Arc::new(move |path: &str, body: &[u8]| {
            if path.contains("/json") {
                log.lock().push((path.to_string(), String::from_utf8_lossy(body).into_owned()));
            }
            if path.starts_with("/insert/records/json") {
                Some(("200 OK".to_string(), r#"{"status":"OK","message":"","data":{"count_inserted":2}}"#.to_string()))
            } else if path.starts_with("/get/records/json") {
                Some((
                    "200 OK".to_string(),
                    r#"{"status":"OK","message":"","data":{"total_number_of_records":1,"has_more_records":true,"records":[{"b":1,"a":2}]}}"#.to_string(),
                ))
            } else {
                None
            }
        }));
        let mut options = crate::KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");

        let inserted = k
            .insert_records_from_json_with_options(
                r#"[{"a":1}]"#,
                "s.t",
                None,
                Some(HashMap::from([("table_name".to_string(), "kept".to_string())])),
                Some(HashMap::from([("update_on_existing_pk".to_string(), "true".to_string())])),
            )
            .await
            .unwrap();
        assert_eq!(inserted["data"]["count_inserted"], 2);

        let records = k
            .get_records_json_with_options("s.t", &["a".into(), "b".into()], -5, -1, Some("a > 1"), &[], None)
            .await
            .unwrap();
        assert!(records.has_more_records());
        assert_eq!(records.json_records(), r#"[{"b":1,"a":2}]"#, "the server's key order");

        assert_eq!(
            k.insert_records_from_json("", "t").await.unwrap_err().to_string(),
            "API error: No records to insert"
        );

        let sent = sent.lock();
        let (insert_path, insert_body) = &sent[0];
        assert_eq!(insert_body, r#"[{"a":1}]"#);
        let mut query: Vec<&str> = insert_path.split_once('?').unwrap().1.split('&').collect();
        query.sort_unstable();
        assert_eq!(query, ["table_name=kept", "update_on_existing_pk=true"], "the creation option's table name wins");
        assert_eq!(sent[1].0, "/get/records/json?table_name=s.t&column_names=a%2Cb&offset=0&limit=-9999&expression=a+%3E+1");
        let head = server.behavior.heads.lock()["/get/records/json?table_name=s.t&column_names=a%2Cb&offset=0&limit=-9999&expression=a+%3E+1"].clone();
        assert!(!head.to_lowercase().contains("content-type"), "no body, no content type");
        let head = server.behavior.heads.lock()[insert_path.as_str()].clone();
        assert!(head.to_lowercase().contains("content-type: application/json; charset=utf-8"));
    }

    fn response(status: u16, reason: &str, content_type: Option<&str>, body: &str) -> RawResponse {
        RawResponse {
            status,
            reason: reason.to_string(),
            content_type: content_type.map(str::to_string),
            body: body.to_string().into(),
        }
    }

    // What fails over and what does not.
    #[test]
    fn json_answers_are_classified_for_failover() {
        let classify = |status, reason, ct, body, posted| interpret_json_response("http://h/x", &response(status, reason, ct, body), posted);

        assert_eq!(classify(200, "OK", Some("application/json"), r#"{"status":"OK"}"#, true).unwrap(), r#"{"status":"OK"}"#);
        // A failure the server reports in the answer is returned
        assert!(classify(400, "Bad Request", None, r#"{"status":"ERROR","message":"bad"}"#, true).is_ok());
        assert!(matches!(
            classify(400, "Bad Request", None, r#"{"status":"ERROR","message":"System shutting down"}"#, true),
            Err(KineticaError::Exit(m)) if m == "Kinetica shutting down"
        ));
        assert!(matches!(classify(503, "Service Unavailable", None, "{}", true), Err(KineticaError::Exit(_))));
        assert!(matches!(classify(200, "Kinetica is exiting", None, "{}", true), Err(KineticaError::Exit(_))));
        assert!(matches!(classify(401, "Unauthorized", None, "{}", true), Err(KineticaError::Unauthorized(_))));
        assert!(matches!(classify(200, "OK", Some("text/html"), "{}", true), Err(KineticaError::Http(_))));
        // Posted, the answer must be an object, read before the status
        assert!(matches!(classify(503, "Service Unavailable", None, "<html>", true), Err(KineticaError::Http(_))));
        // Without a payload it is not read
        assert_eq!(classify(200, "OK", None, "[1]", false).unwrap(), "[1]");
    }

    /// A response whose one record has `v` and whose `data` has `t`, both
    /// the number `number`.
    fn number_response(number: &str) -> String {
        format!(
            r#"{{"status":"OK","data":{{"total_number_of_records":1,"has_more_records":false,"records":[{{"v":{number}}}],"t":{number}}}}}"#
        )
    }

    // Each number, what is written back for it, and the kind it is read as.
    #[test]
    fn numbers_are_read_and_written_back_as_jackson_does() {
        for (number, written, kind) in [
            ("12345678901234567890", "12345678901234567890", "integer beyond i64"),
            ("123456789012345678901234567890", "123456789012345678901234567890", "integer beyond i64"),
            ("-9223372036854775809", "-9223372036854775809", "integer beyond i64"),
            ("-9223372036854775808", "-9223372036854775808", "i64"),
            ("9223372036854775807", "9223372036854775807", "i64"),
            ("9223372036854775808", "9223372036854775808", "integer beyond i64"),
            ("1e400", "\"Infinity\"", "f64"),
            ("-1e400", "\"-Infinity\"", "f64"),
            ("1e-400", "0.0", "f64"),
            ("-0", "0", "i32"),
            ("-0.0", "-0.0", "f64"),
            ("0.1234567890123456789", "0.12345678901234568", "f64"),
            ("1.0E10", "1.0E10", "f64"),
            ("1E2", "100.0", "f64"),
            ("2147483648", "2147483648", "i64"),
            ("-2147483649", "-2147483649", "i64"),
            ("100000000000000000000000000000000000000000000.5", "1.0E44", "f64"),
            ("1.7976931348623157e308", "1.7976931348623157E308", "f64"),
            ("4.9e-324", "4.9E-324", "f64"),
            ("0e0", "0.0", "f64"),
            ("-1e-400", "-0.0", "f64"),
            ("123.456e-2", "1.23456", "f64"),
        ] {
            let response = records_json_response(&number_response(number)).unwrap();
            assert_eq!(response.json_records(), format!(r#"[{{"v":{written}}}]"#), "{number}");
            let OrderedJson::Object(fields) = OrderedJson::parse(&number_response(number)).unwrap() else { panic!() };
            let OrderedJson::Object(data) = &fields[1].1 else { panic!() };
            assert_eq!(data[3].1.kind(), kind, "{number}");
        }
        // Past an int, the record count cannot be cast
        let big = r#"{"status":"OK","data":{"total_number_of_records":9223372036854775808,"has_more_records":false}}"#;
        assert!(records_json_response(big)
            .unwrap_err()
            .to_string()
            .contains("`data.total_number_of_records` must be i32; found integer beyond i64"));
    }

    /// Which answers are read (and what follows ignored), and which are not
    /// readable.
    #[test]
    fn answers_are_read_as_strictly_as_jackson_reads_them() {
        let unreadable = "Error reading JSON response";
        let empty = "API error: Empty response from server gettting JSON records";
        let error_of = |json: &str| records_json_response(json).unwrap_err().to_string();
        assert_eq!(error_of(""), unreadable);
        assert_eq!(error_of(" "), unreadable);
        assert_eq!(error_of("null"), empty);
        assert_eq!(error_of("{}"), empty);
        assert_eq!(error_of("[]"), unreadable);
        assert_eq!(error_of("5"), unreadable);
        assert_eq!(error_of(r#""x""#), unreadable);
        for json in [
            r#"{"a":01}"#,
            r#"{"a":NaN}"#,
            r#"{'a':1}"#,
            r#"{"a":1,}"#,
            r#"{"a":+1}"#,
            r#"{"a":.5}"#,
            r#"{"a":1.}"#,
            "{\"a\":\"x\ty\"}",
            r#"{"a":[1,2,]}"#,
            r#"{"a":1 "b":2}"#,
            r#"{"a":tru}"#,
        ] {
            assert_eq!(error_of(json), unreadable, "{json}");
        }
        // Read, with whatever follows ignored; a status-less answer then
        // fails as an error answer does
        for json in [r#"{"a":1} trailing"#, r#"{"a":1}}"#, r#"{"a":1}/*c*/"#, "{\"a\":1}\0"] {
            assert_eq!(error_of(json), "API error: Error getting JSON records: (no message)", "{json}");
        }
        let lone = OrderedJson::parse(r#"{"a":"\ud800","b":"\ud83d\ude00\ud800\u0041"}"#).unwrap();
        assert_eq!(lone.to_jackson_string(), "{\"a\":\"\u{fffd}\",\"b\":\"\u{1f600}\u{fffd}A\"}");

        let nested = |depth: usize| format!(r#"{{"a":{}1{}}}"#, "[".repeat(depth), "]".repeat(depth));
        assert!(OrderedJson::parse(&nested(999)).is_ok());
        assert!(OrderedJson::parse(&nested(1000)).unwrap_err().starts_with("Nesting depth 1001 exceeds"));
        let digits = |n: usize| format!(r#"{{"a":{}}}"#, "1".repeat(n));
        assert!(OrderedJson::parse(&digits(1000)).is_ok());
        assert!(OrderedJson::parse(&digits(1001)).unwrap_err().starts_with("A number of 1001 digits exceeds"));
        assert!(OrderedJson::parse(&format!(r#"{{"a":1.{}}}"#, "1".repeat(999))).is_ok());
    }

    #[test]
    fn json_options_and_responses_are_made_and_printed() {
        assert_eq!(JsonOptions::default(), JsonOptions::new(JsonOptions::VALIDATE_JSON, JsonOptions::COMPRESSION_ON));
        assert_eq!(JsonOptions::default().to_string(), "JsonOptions{validate_json=false, compression_on=true}");
        let response = GetRecordsJsonResponse::new(2, true, "[1,2]");
        assert_eq!((response.total_records(), response.has_more_records(), response.json_records()), (2, true, "[1,2]"));
        assert_eq!(response.to_string(), "GetRecordsJsonResponse [total_records=2, has_more_records=true, json_records=[1,2]]");
    }
}
