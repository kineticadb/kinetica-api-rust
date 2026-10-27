//! The record types a connection knows by type ID, and the encoding and
//! decoding of the records the data endpoints carry.
//!
//! A data endpoint called without a record type decodes by the type ID the
//! server returns, looking the type up here and, failing that, asking the
//! server for it with `/show/types`. The endpoints that describe types
//! (`/create/type`, `/show/table`, `/show/types`) add the types they describe.
//!
//! The cache holds the [`Type`] alone; what records are decoded into is the
//! [`RetrievedRecord`] the caller asks for, read by that type's schema.

use crate::generic_record::Type;
use crate::record_retriever::RetrievedRecord;
use crate::{Kinetica, KineticaError, KineticaRecord, ShowTableRequest, ShowTableResponse, ShowTypesRequest, ShowTypesResponse};
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::sync::Arc;

/// The type ID the server gives a collection, which has no record type.
const COLLECTION_TYPE_ID: &str = "<collection>";

impl Kinetica {
    /// Makes `record_type` the type records of `type_id` are decoded by when
    /// a data endpoint is called without one; `None` forgets the type.
    pub fn add_known_type(&self, type_id: &str, record_type: Option<Arc<Type>>) {
        let mut known = self.known_types.write();
        match record_type {
            Some(record_type) => {
                known.insert(type_id.to_string(), record_type);
            }
            None => {
                known.remove(type_id);
            }
        }
    }

    /// Makes `record_type` the type records of `table_name`'s type are
    /// decoded by, as [`Kinetica::add_known_type`] does for a type ID. Fails
    /// if the table does not exist or holds records of more than one type.
    pub async fn add_known_type_from_table(
        &self,
        table_name: &str,
        record_type: Option<Arc<Type>>,
    ) -> Result<(), KineticaError> {
        let request = ShowTableRequest { table_name: table_name.to_string(), options: HashMap::new() };
        let response: ShowTableResponse = self
            .submit_request(
                "/show/table",
                &crate::generated::SHOW_TABLE_REQUEST_SCHEMA,
                &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                &request,
            )
            .await?;

        let Some(type_id) = response.type_ids.first() else {
            return Err(KineticaError::Api(format!("Table {table_name} does not exist.")));
        };
        if response.type_ids.iter().any(|id| id != type_id) {
            return Err(KineticaError::Api(format!("Table {table_name} is not homogeneous.")));
        }

        self.add_known_type(type_id, record_type);
        Ok(())
    }

    /// The type of records of `type_id`, from those known, or else from the
    /// server's `/show/types`, which is then kept.
    #[doc(hidden)]
    pub async fn get_type_descriptor(&self, type_id: &str) -> Result<Arc<Type>, KineticaError> {
        if let Some(record_type) = self.known_types.read().get(type_id) {
            return Ok(record_type.clone());
        }

        let request = ShowTypesRequest {
            type_id: type_id.to_string(),
            label: String::new(),
            options: HashMap::new(),
        };
        let response: ShowTypesResponse = self
            .submit_request(
                "/show/types",
                &crate::generated::SHOW_TYPES_REQUEST_SCHEMA,
                &crate::generated::SHOW_TYPES_RESPONSE_SCHEMA,
                &request,
            )
            .await?;

        let (Some(label), Some(schema), Some(properties)) =
            (response.labels.first(), response.type_schemas.first(), response.properties.first())
        else {
            return Err(KineticaError::Api(format!(
                "Unable to obtain type information for type {type_id}."
            )));
        };

        let record_type = Arc::new(Type::from_type_schema(label.clone(), schema, properties)?);
        self.known_types
            .write()
            .entry(type_id.to_string())
            .or_insert_with(|| record_type.clone());
        Ok(record_type)
    }

    /// Keeps the type a response describes, unless one is known for its ID
    /// already or it is a collection's. Fails for a schema it cannot read.
    #[doc(hidden)]
    pub fn set_type_descriptor_if_missing(
        &self,
        type_id: &str,
        label: &str,
        type_schema: &str,
        properties: &HashMap<String, Vec<String>>,
    ) -> Result<(), KineticaError> {
        if type_id == COLLECTION_TYPE_ID || self.known_types.read().contains_key(type_id) {
            return Ok(());
        }
        let record_type = Arc::new(Type::from_type_schema(label.to_string(), type_schema, properties)?);
        self.known_types.write().entry(type_id.to_string()).or_insert(record_type);
        Ok(())
    }

    /// Keeps the types a response describes, one per entry, skipping those
    /// with no schema.
    #[doc(hidden)]
    pub fn set_type_descriptors_if_missing(
        &self,
        type_ids: &[String],
        labels: &[String],
        type_schemas: &[String],
        properties: &[HashMap<String, Vec<String>>],
    ) -> Result<(), KineticaError> {
        for (i, type_id) in type_ids.iter().enumerate() {
            let schema = entry(type_schemas, i)?;
            if schema.is_empty() {
                // Skip generating a type if no schema is available
                continue;
            }
            self.set_type_descriptor_if_missing(type_id, entry(labels, i)?, schema, entry(properties, i)?)?;
        }
        Ok(())
    }

    /// Decodes records of the type `type_id` names, looked up as
    /// [`Kinetica::get_type_descriptor`] looks it up. The type is looked up
    /// even when there are no records.
    #[doc(hidden)]
    pub async fn decode_records_of_type<R: RetrievedRecord>(
        &self,
        type_id: &str,
        data: &[ByteBuf],
    ) -> Result<Vec<R>, KineticaError> {
        let record_type = self.get_type_descriptor(type_id).await?;
        R::decode_records(&record_type, data)
    }

    /// Decodes records each of the type its entry in `type_ids` names, a run
    /// of records of one type at a time.
    #[doc(hidden)]
    pub async fn decode_records_of_types<R: RetrievedRecord>(
        &self,
        type_ids: &[String],
        data: &[ByteBuf],
    ) -> Result<Vec<R>, KineticaError> {
        let mut result = Vec::with_capacity(data.len());
        if data.is_empty() {
            return Ok(result);
        }

        let mut last_type_id = entry(type_ids, 0)?;
        let mut start = 0;
        for i in 1..data.len() {
            let type_id = entry(type_ids, i)?;
            if type_id != last_type_id {
                result.extend(self.decode_records_of_type::<R>(last_type_id, &data[start..i]).await?);
                last_type_id = type_id;
                start = i;
            }
        }
        result.extend(self.decode_records_of_type::<R>(last_type_id, &data[start..]).await?);
        Ok(result)
    }

    /// Decodes lists of records, each of the type its entry in `type_ids`
    /// names.
    #[doc(hidden)]
    pub async fn decode_record_lists_of_types<R: RetrievedRecord>(
        &self,
        type_ids: &[String],
        data: &[Vec<ByteBuf>],
    ) -> Result<Vec<Vec<R>>, KineticaError> {
        let mut result = Vec::with_capacity(data.len());
        for (i, records) in data.iter().enumerate() {
            result.push(self.decode_records_of_type::<R>(entry(type_ids, i)?, records).await?);
        }
        Ok(result)
    }
}

/// Decodes lists of records, all of `record_type`.
#[doc(hidden)]
pub fn decode_record_lists<R: RetrievedRecord>(
    record_type: &Arc<Type>,
    data: &[Vec<ByteBuf>],
) -> Result<Vec<Vec<R>>, KineticaError> {
    data.iter().map(|records| R::decode_records(record_type, records)).collect()
}

/// Encodes records for a data endpoint: all by the schema of the first, and a
/// record that does not encode as an empty buffer, for the server to report.
#[doc(hidden)]
pub fn encode_records_for_request<T: KineticaRecord>(data: &[T]) -> Result<Vec<ByteBuf>, KineticaError> {
    let Some(first) = data.first() else {
        return Ok(Vec::new());
    };
    let schema: serde_avro_fast::Schema = first
        .avro_schema()
        .parse()
        .map_err(|e| KineticaError::Api(format!("Could not encode object: {e}")))?;
    let mut config = serde_avro_fast::ser::SerializerConfig::new(&schema);
    Ok(data
        .iter()
        .enumerate()
        .map(|(i, record)| match serde_avro_fast::to_datum(record, Vec::new(), &mut config) {
            Ok(bytes) => ByteBuf::from(bytes),
            Err(e) => {
                log::debug!("{i}:{}", crate::generic_record::encode_failure_message(&e));
                ByteBuf::new()
            }
        })
        .collect())
}

/// Entry `index` of a list in a response, failing for one that is too short.
fn entry<T>(list: &[T], index: usize) -> Result<&T, KineticaError> {
    list.get(index).ok_or_else(|| {
        KineticaError::Config(format!("The response has no entry {index}; it has {}", list.len()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode};
    use crate::KineticaOptions;

    fn options() -> KineticaOptions {
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0);
        options
    }

    fn id_type() -> Arc<Type> {
        Arc::new(Type::new("ids", vec![Column::new("id", ColumnType::Integer)]).unwrap())
    }

    fn id_record(id: i32) -> ByteBuf {
        #[derive(serde::Serialize)]
        struct Row {
            id: i32,
        }
        ByteBuf::from(encode(&id_type().schema_string().parse().unwrap(), &Row { id }))
    }

    /// Serves `/show/types` for one type and `/show/table` for a table of it.
    async fn server() -> (fake_server::FakeServer, Kinetica) {
        let head = fake_server::start("127.0.0.71").await;
        *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);
        *head.behavior.handler.lock() = Some(Arc::new(|path: &str, _: &[u8]| {
            if path.ends_with("/show/types") {
                let response = ShowTypesResponse {
                    type_ids: vec!["t1".to_string()],
                    type_schemas: vec![id_type().schema_string().to_string()],
                    labels: vec!["ids".to_string()],
                    properties: vec![HashMap::new()],
                    info: HashMap::new(),
                };
                Some(Ok(encode(&crate::generated::SHOW_TYPES_RESPONSE_SCHEMA, &response)))
            } else if path.ends_with("/show/table") {
                let response = ShowTableResponse {
                    type_ids: vec!["t1".to_string(), "t2".to_string()],
                    ..Default::default()
                };
                Some(Ok(encode(&crate::generated::SHOW_TABLE_RESPONSE_SCHEMA, &response)))
            } else {
                None
            }
        }));
        let k = Kinetica::new(vec![head.url.to_string()], options()).await.expect("client");
        (head, k)
    }

    /// An unknown type is read from the server once, then kept.
    #[tokio::test]
    async fn an_unknown_type_is_asked_for_once() {
        let (head, k) = server().await;

        let first: Vec<GenericRecord> = k.decode_records_of_type("t1", &[id_record(7)]).await.unwrap();
        let none: Vec<GenericRecord> = k.decode_records_of_type("t1", &[]).await.unwrap();

        assert_eq!(first[0].get(0), Some(&RecordValue::Int(7)));
        assert!(none.is_empty());
        assert_eq!(head.behavior.count("/show/types"), 1);
    }

    /// A known type is used as it is, and records of a run of types decode
    /// each by its own.
    #[tokio::test]
    async fn known_types_decode_their_runs() {
        let (head, k) = server().await;
        let other = Arc::new(Type::new("other", vec![Column::new("n", ColumnType::Integer)]).unwrap());
        k.add_known_type("t2", Some(other.clone()));
        k.add_known_type("t1", Some(id_type()));

        let types = ["t1", "t1", "t2"].map(String::from);
        let records: Vec<GenericRecord> =
            k.decode_records_of_types(&types, &[id_record(1), id_record(2), id_record(3)]).await.unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[1].record_type().label(), "ids");
        assert_eq!(records[2].record_type().label(), "other");
        assert_eq!(head.behavior.count("/show/types"), 0);

        k.add_known_type("t1", None);
        let _: Vec<GenericRecord> = k.decode_records_of_type("t1", &[]).await.unwrap();
        assert_eq!(head.behavior.count("/show/types"), 1, "a forgotten type is asked for");
    }

    #[tokio::test]
    async fn a_table_of_two_types_is_refused() {
        let (_head, k) = server().await;
        let err = k.add_known_type_from_table("t", Some(id_type())).await.unwrap_err();
        assert_eq!(err.to_string(), "API error: Table t is not homogeneous.");
    }

    #[test]
    fn a_described_type_is_kept_unless_known_or_a_collection() {
        let k = futures_lite_block_on(async {
            let head = fake_server::start("127.0.0.72").await;
            *head.behavior.properties.lock() = cluster_properties(&address(&head), &[]);
            Kinetica::new(vec![head.url.to_string()], options()).await.expect("client")
        });
        let schema = id_type().schema_string().to_string();
        k.set_type_descriptor_if_missing("<collection>", "", "not a schema", &HashMap::new()).unwrap();
        assert!(k.known_types.read().is_empty());

        k.set_type_descriptors_if_missing(
            &["a".into(), "b".into()],
            &["first".into(), "second".into()],
            &[schema.clone(), String::new()],
            &[HashMap::new(), HashMap::new()],
        )
        .unwrap();
        assert_eq!(k.known_types.read().get("a").map(|t| t.label().to_string()), Some("first".to_string()));
        assert!(!k.known_types.read().contains_key("b"), "an entry without a schema is skipped");

        k.set_type_descriptor_if_missing("a", "replaced", &schema, &HashMap::new()).unwrap();
        assert_eq!(k.known_types.read()["a"].label(), "first");

        assert!(k.set_type_descriptor_if_missing("c", "", "{", &HashMap::new()).is_err());
    }

    fn futures_lite_block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(future)
    }

    #[test]
    fn records_are_encoded_by_the_first_ones_schema() {
        let t = id_type();
        let mut good = GenericRecord::new(t.clone());
        good.set(0, RecordValue::Int(5)).unwrap();
        let mut bad = GenericRecord::new(t);
        bad.set(0, RecordValue::String("not an int".into())).unwrap();

        let encoded = encode_records_for_request(&[good, bad]).unwrap();
        assert_eq!(encoded[0], id_record(5));
        assert!(encoded[1].is_empty(), "a record that does not encode is sent empty");
        assert!(encode_records_for_request::<GenericRecord>(&[]).unwrap().is_empty());
    }
}

/// The generated data endpoints, against a stand-in server.
#[cfg(test)]
mod data_endpoint_tests {
    use super::*;
    use crate::generic_record::{Column, ColumnType, GenericRecord, RecordValue};
    use crate::gpudb::fake_server::{self, address, cluster_properties, encode, FakeServer};
    use crate::{
        ExecuteSqlRequest, GetRecordsRequest, InsertRecordsResponse, KineticaOptions, RawExecuteSqlResponse,
        RawGetRecordsResponse, RawInsertRecordsRequest,
    };
    use parking_lot::Mutex;

    fn id_type() -> Arc<Type> {
        Arc::new(Type::new("ids", vec![Column::new("id", ColumnType::Integer)]).unwrap())
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct Row {
        id: i32,
    }

    fn id_record(id: i32) -> ByteBuf {
        ByteBuf::from(encode(&id_type().schema_string().parse().unwrap(), &Row { id }))
    }

    /// What each path was sent.
    type Bodies = Arc<Mutex<HashMap<String, Vec<u8>>>>;

    /// A client of a server answering the data endpoints: `/get/records`
    /// with one record of type `t1`, `/show/types` and `/show/table`
    /// describing `t1`, `/execute/sql` with a dynamic response, and
    /// `/insert/records` with a count.
    async fn client(host: &str) -> (FakeServer, Kinetica, Bodies) {
        let server = fake_server::start(host).await;
        *server.behavior.properties.lock() = cluster_properties(&address(&server), &[]);
        let bodies: Bodies = Arc::default();
        let sent = bodies.clone();
        *server.behavior.handler.lock() = Some(Arc::new(move |path: &str, body: &[u8]| {
            let endpoint = path.to_string();
            sent.lock().insert(endpoint.clone(), body.to_vec());
            let schema = id_type().schema_string().to_string();
            let reply = match endpoint.as_str() {
                "/get/records" => encode(
                    &crate::generated::GET_RECORDS_RESPONSE_SCHEMA,
                    &RawGetRecordsResponse {
                        table_name: "t".into(),
                        type_name: "t1".into(),
                        type_schema: schema,
                        records_binary: vec![id_record(7)],
                        total_number_of_records: 1,
                        ..Default::default()
                    },
                ),
                "/show/types" => encode(
                    &crate::generated::SHOW_TYPES_RESPONSE_SCHEMA,
                    &ShowTypesResponse {
                        type_ids: vec!["t1".into()],
                        type_schemas: vec![schema],
                        labels: vec!["ids".into()],
                        properties: vec![HashMap::new()],
                        info: HashMap::new(),
                    },
                ),
                "/show/table" => encode(
                    &crate::generated::SHOW_TABLE_RESPONSE_SCHEMA,
                    &ShowTableResponse {
                        table_name: "t".into(),
                        table_names: vec!["t".into()],
                        type_ids: vec!["t1".into()],
                        type_schemas: vec![schema],
                        type_labels: vec!["ids".into()],
                        properties: vec![HashMap::new()],
                        ..Default::default()
                    },
                ),
                "/execute/sql" => {
                    let dynamic = r#"{"type":"record","name":"generic_response","fields":[
                        {"name":"column_1","type":{"type":"array","items":"int"}},
                        {"name":"column_headers","type":{"type":"array","items":"string"}},
                        {"name":"column_datatypes","type":{"type":"array","items":"string"}}]}"#;
                    #[derive(serde::Serialize)]
                    struct Columns {
                        column_1: Vec<i32>,
                        column_headers: Vec<String>,
                        column_datatypes: Vec<String>,
                    }
                    let data = encode(
                        &dynamic.parse().unwrap(),
                        &Columns { column_1: vec![1, 2], column_headers: vec!["n".into()], column_datatypes: vec!["int".into()] },
                    );
                    encode(
                        &crate::generated::EXECUTE_SQL_RESPONSE_SCHEMA,
                        &RawExecuteSqlResponse {
                            response_schema_str: dynamic.into(),
                            binary_encoded_response: ByteBuf::from(data),
                            total_number_of_records: 2,
                            paging_table: "p".into(),
                            ..Default::default()
                        },
                    )
                }
                "/insert/records" => encode(
                    &crate::generated::INSERT_RECORDS_RESPONSE_SCHEMA,
                    &InsertRecordsResponse { count_inserted: 2, ..Default::default() },
                ),
                _ => return None,
            };
            Some(Ok(reply))
        }));
        let mut options = KineticaOptions::default();
        options.set_server_connection_timeout(500).set_max_retries(0).set_use_snappy(false);
        let k = Kinetica::new(vec![server.url.to_string()], options).await.expect("client");
        (server, k, bodies)
    }

    // Records by table name: binary encoding, and records decoded by the type
    // the server names, read once from the server.
    #[tokio::test]
    async fn records_are_decoded_by_the_type_the_server_names() {
        let (server, k, bodies) = client("127.0.0.81").await;

        let generic = k.get_records::<GenericRecord>("t".into(), 0, 10).await.unwrap();
        let typed = k.get_records::<Row>("t".into(), 0, 10).await.unwrap();

        assert_eq!(generic.data[0].get(0), Some(&RecordValue::Int(7)));
        assert_eq!(generic.data[0].record_type().label(), "ids");
        assert_eq!(typed.data, [Row { id: 7 }]);
        assert_eq!(typed.total_number_of_records, 1);
        assert_eq!(server.behavior.count("/show/types"), 1, "the type is read once, then kept");
        let request: GetRecordsRequest = serde_avro_fast::from_datum_slice(
            &bodies.lock()["/get/records"],
            &crate::generated::GET_RECORDS_REQUEST_SCHEMA,
        )
        .unwrap();
        assert_eq!(request.encoding, "binary");
    }

    // A type a `/show/table` response describes is kept.
    #[tokio::test]
    async fn a_described_type_needs_no_lookup() {
        let (server, k, _) = client("127.0.0.82").await;
        k.show_table("t".into()).await.unwrap();

        let _ = k.get_records::<GenericRecord>("t".into(), 0, 10).await.unwrap();

        assert_eq!(server.behavior.count("/show/types"), 0);
    }

    // Records by a given type are decoded by that type, and the request-taking
    // form refuses a non-binary encoding.
    #[tokio::test]
    async fn a_given_type_decodes_and_only_binary_is_decoded() {
        let (server, k, _) = client("127.0.0.83").await;
        let given = Arc::new(Type::new("given", vec![Column::new("id", ColumnType::Integer)]).unwrap());

        let records = k
            .get_records_with_type::<GenericRecord>(&given, "t".into(), 0, 10, Default::default())
            .await
            .unwrap();
        assert_eq!(records.data[0].record_type().label(), "given");
        assert_eq!(server.behavior.count("/show/types"), 0);

        let json = GetRecordsRequest { table_name: "t".into(), encoding: "json".into(), ..Default::default() };
        let err = k.get_records_request::<GenericRecord>(&json, None).await.unwrap_err();
        assert_eq!(err.to_string(), "API error: This method only supports binary encoding.");
        assert_eq!(server.behavior.count("/get/records"), 1, "nothing is sent");
        assert_eq!(GetRecordsRequest::default().encoding, "binary", "the default encoding");
    }

    #[tokio::test]
    async fn sql_results_are_typed_by_the_response() {
        let (_server, k, bodies) = client("127.0.0.84").await;

        let result = k
            .execute_sql("SELECT n FROM t".into(), 0, -9999, String::new(), Vec::new())
            .await
            .unwrap();

        assert_eq!(result.data_type.columns().map(Column::name).collect::<Vec<_>>(), ["n"]);
        assert_eq!(result.data.iter().map(|r| r.get(0).cloned()).collect::<Vec<_>>(), [Some(RecordValue::Int(1)), Some(RecordValue::Int(2))]);
        assert_eq!(result.paging_table, "p");
        let request: ExecuteSqlRequest = serde_avro_fast::from_datum_slice(
            &bodies.lock()["/execute/sql"],
            &crate::generated::EXECUTE_SQL_REQUEST_SCHEMA,
        )
        .unwrap();
        assert_eq!(request.encoding, "binary");
    }

    // Records inserted by table name are encoded and sent in binary.
    #[tokio::test]
    async fn records_are_inserted_encoded() {
        let (_server, k, bodies) = client("127.0.0.85").await;
        let records: Vec<GenericRecord> = [3, 4]
            .into_iter()
            .map(|id| {
                let mut record = GenericRecord::new(id_type());
                record.set(0, RecordValue::Int(id)).unwrap();
                record
            })
            .collect();

        let response = k.insert_records("t".into(), records).await.unwrap();

        assert_eq!(response.count_inserted, 2);
        let request: RawInsertRecordsRequest = serde_avro_fast::from_datum_slice(
            &bodies.lock()["/insert/records"],
            &crate::generated::INSERT_RECORDS_REQUEST_SCHEMA,
        )
        .unwrap();
        assert_eq!(request.list, [id_record(3), id_record(4)]);
        assert_eq!(request.list_encoding, "binary");
        assert!(request.list_str.is_empty());
    }
}
