<h3 align="center" style="margin:0px">
	<img width="200" src="https://kinetica-web-assets.s3.us-east-1.amazonaws.com/assets/kinetica_logo_gray.svg" alt="Kinetica Logo"/>
</h3>
<h5 align="center" style="margin:0px">
	<a href="https://www.kinetica.com/">Website</a>
	|
	<a href="https://docs.kinetica.com/">Docs</a>
	|
	<a href="https://docs.kinetica.com/content/api/rust/">API Docs</a>
	|
	<a href="https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg">Community Slack</a>
</h5>


# Kinetica Rust API

-  [Overview](#overview)
-  [Installation](#installation)
-  [Quick Start](#quick-start)
-  [Logging](#logging)
-  [Calling Endpoints](#calling-endpoints)
-  [Bulk Inserting](#bulk-inserting)
-  [Defining Record Types](#defining-record-types)
-  [Retrieving Records by Key](#retrieving-records-by-key)
-  [SQL](#sql)
-  [JSON Records](#json-records)
-  [KiFS Files](#kifs-files)
-  [Configuration](#configuration)
-  [Error Handling](#error-handling)
-  [Troubleshooting](#troubleshooting)
-  [Examples](#examples)
-  [Support](#support)
-  [Contact Us](#contact-us)


## Overview

This is the 7.2.x.y version of the Kinetica Rust API, an async client for
Kinetica built on [Tokio](https://tokio.rs/). It covers every endpoint of the
Kinetica API, and adds:

- **`BulkInserter`**, which routes each record straight to the worker rank that
  owns its shard, with batching, backpressure, timed flushes and retries
- **`RecordRetriever`**, which looks records up by shard key on the rank that
  holds them
- **High availability**, failing over across an HA ring of clusters and back to
  the primary
- **TLS** through rustls, trusting the system's roots or a trust store (PKCS#12,
  JKS or PEM)
- **SQL**, **JSON records** and **KiFS** file upload, download and ingest
- **SQLx** support over Kinetica's PostgreSQL wire protocol, as the optional
  `sqlx` feature

It runs on Linux, macOS and Windows.

The repository holds two crates: `gpudb-api`, the API, and `gpudb-examples`,
programs that use it (see [Examples](#examples)).

For changes to the client-side API, please refer to
[CHANGELOG.md](https://github.com/kineticadb/kinetica-api-rust/blob/master/CHANGELOG.md). For changes to Kinetica functions, please refer
to [CHANGELOG-FUNCTIONS.md](https://github.com/kineticadb/kinetica-api-rust/blob/master/CHANGELOG-FUNCTIONS.md).


## Installation

The API needs Rust 1.88 or later, and a Tokio runtime. Add it to your
`Cargo.toml`:

```toml
[dependencies]
gpudb-api = "7.2.3-0"
tokio = { version = "1", features = ["full"] }
```

For SQLx support, enable the `sqlx` feature:

```toml
gpudb-api = { version = "7.2.3-0", features = ["sqlx"] }
```

### Versions

The crate's version is the Kinetica version it implements, `MAJOR.MINOR.REVISION`,
with this client's own release of it after a `-`: `7.2.3-0` is release `0` of the
client for Kinetica 7.2.3. Cargo takes a version with a `-` suffix only when
asked for one, so depend on the crate as `"7.2.3-0"` (any release for 7.2.3) or
`"=7.2.3-N"` (that release only); a plain `"7.2.3"` matches none of them.

`Kinetica::api_version()` gives the version with the time of the build
(`7.2.3.0-20261010120000`), and the `User-Agent` header carries it without
(`kinetica-api-rust/7.2.3.0`).

### Building for Other Platforms

The crate is compiled by each application's own build for the platform it
targets, so there are no prebuilt libraries to fetch. To build an application
for another platform, add the target and build for it:

```bash
rustup target add aarch64-unknown-linux-gnu
cargo build --release --target aarch64-unknown-linux-gnu
```

TLS is pure Rust (rustls), so no system OpenSSL is needed on any target. The
`ring` crate it uses compiles some C, so cross-compiling needs a C compiler and
linker for the target (for example `gcc-aarch64-linux-gnu` on Ubuntu), or use
[`cross`](https://github.com/cross-rs/cross), which supplies them in Docker.


## Quick Start

```rust
use gpudb_api::{Kinetica, KineticaOptions};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client with authentication
    let mut options = KineticaOptions::new();
    options.set_username("admin")
           .set_password("password")
           .set_bypass_ssl_cert_check(false);

    let kinetica = Arc::new(
        Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
    );

    // Generated endpoint methods take their required parameters positionally
    let response = kinetica.create_table(
        "my_table".to_string(),
        "type_id_from_create_type".to_string(),
    ).await?;

    // ...or pass a request struct directly with the `_raw` form:
    // let response = kinetica.create_table_raw(&CreateTableRequest {
    //     table_name: "my_table".into(),
    //     type_id: "type_id_from_create_type".into(),
    //     ..Default::default()
    // }).await?;

    println!("Table created: {}", response.table_name);

    // Drop the SQL #name temp tables this connection created, if any
    kinetica.close().await;
    Ok(())
}
```

Each `Kinetica` is a server session of its own: its SQL `#name` temp tables
belong to it alone, and `close()` drops them. A connection dropped without
being closed has them dropped as it goes, on a best-effort basis; failing that,
the server reclaims them. A closed connection cannot be used. Against a server
without session support nothing is dropped.

To share one session's temp tables among several connections, give each the
same ID with `KineticaOptions::set_session_id`. That session is the caller's
to drop, with `drop_session()` on any connection given it; neither `close()`
nor dropping a connection drops it.


## Logging

The API logs through the [`log`](https://docs.rs/log) crate. Its level is the
application logger's to set, for the `gpudb_api` target; at the usual default,
WARN, normal operation produces no output.

Add a logging implementation to your application (e.g., `env_logger`):

```toml
[dependencies]
env_logger = "0.11"
```

Initialize the logger in your main function:

```rust
fn main() {
    // Initialize with default WARN level (quiet operation)
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn")
    ).init();

    // Your application code...
}
```

Control verbosity via the `RUST_LOG` environment variable:

```bash
# Default (WARN) - silent unless errors/warnings occur
cargo run --release

# INFO level - high-level operations (recommended for monitoring)
RUST_LOG=gpudb_api=info cargo run --release

# DEBUG level - configuration details, worker setup, routing info
RUST_LOG=gpudb_api=debug cargo run --release

# TRACE level - per-request/batch details (very verbose, for debugging)
RUST_LOG=gpudb_api=trace cargo run --release
```

| Level | Information |
|-------|-------------|
| **ERROR** | A request that could not be sent ("Giving up with an exit error after <N>ms ..."), and failures a call goes on past, such as a KiFS file not transferred |
| **WARN** | Failing over to another cluster, retries, settings that are ignored |
| **INFO** | Failing back to the primary cluster, KiFS files transferred |
| **DEBUG** | Connecting, BulkInserter creation/close, table properties, multi-head config, shard columns, worker URLs, flush operations |
| **TRACE** | Per-batch completion times, HTTP request payloads, detailed routing |

Example output at DEBUG level:

```
[2026-03-04T06:56:45Z DEBUG gpudb_api::gpudb] Initializing Kinetica client with 1 URL(s)
[2026-03-04T06:56:45Z DEBUG gpudb_api::gpudb] Kinetica client initialized successfully, connected to http://localhost:9191
[2026-03-04T06:56:45Z DEBUG gpudb_api::bulk_inserter] Creating BulkInserter for table 'test.my_table' with batch_size=50000, num_workers=16
[2026-03-04T06:56:45Z DEBUG gpudb_api::bulk_inserter] BulkInserter ready for table 'test.my_table': 2 workers, batch_size=50000, multi_head=true
[2026-03-04T06:56:49Z DEBUG gpudb_api::bulk_inserter] BulkInserter closed successfully: 4000000 inserted, 0 updated in 2.19s
```


## Calling Endpoints

Every Kinetica endpoint is a method on `Kinetica`, in four shapes. Using
`/show/table`:

```rust
// 1. Positional - required parameters as arguments (usually what you want)
let r = kinetica.show_table(table.clone()).await?;

// 2. With typed options
let mut opts = ShowTableRequestOptions::default();
opts.get_sizes = "true".to_string();
let r = kinetica.show_table_with_opts(table.clone(), opts).await?;

// 3. Raw - you build the request struct yourself
let r = kinetica.show_table_raw(&ShowTableRequest {
    table_name: table.clone(),
    options: HashMap::new(),
}).await?;

// 4. From a specific worker URL, bypassing HA head-node selection (multi-head)
let r = kinetica.show_table_from_url(&worker_url, &request).await?;
```

### Method Naming

Method names are the endpoint's name in snake case:

| Endpoint | Method |
|----------|--------|
| `/execute/sql` | `execute_sql` |
| `/get/records/bycolumn` | `get_records_by_column` |
| `/aggregate/groupby` | `aggregate_group_by` |
| `/filter/byradius` | `filter_by_radius` |
| `/insert/records/fromfiles` | `insert_records_from_files` |
| `/visualize/getfeatureinfo` | `visualize_get_feature_info` |

### Endpoints That Carry Records

The endpoints that send or return records encode and decode them, and keep a
`Raw...` form with the records as the server sends them
(`RawGetRecordsResponse`, `RawInsertRecordsRequest`, ...):

```rust
// Records decoded by the type the server names, known to the client or read
// from the server once; any serde type works in place of GenericRecord
let r = kinetica.get_records::<GenericRecord>(table.clone(), 0, -9999).await?;

// ...or by a type you give
let r = kinetica.get_records_with_type::<MyRow>(&record_type, table.clone(), 0, -9999, opts).await?;

// SQL and aggregations return records typed by the response
let r = kinetica.execute_sql(sql, 0, -9999, String::new(), Vec::new()).await?;
println!("{} columns, {} rows", r.data_type.column_count(), r.data.len());

// Records to insert are encoded by the schema of the first
let r = kinetica.insert_records(table.clone(), records).await?;

// The raw forms take and return what goes over the wire
let raw = kinetica.get_records_raw(&GetRecordsRequest { table_name: table.clone(), ..Default::default() }).await?;
```

`add_known_type` and `add_known_type_from_table` set the type records of a
type ID decode by; `/create/type`, `/show/table` and `/show/types`
responses add the types they describe.

### Endpoint Options

`<Endpoint>RequestOptions` exposes the endpoint's documented option keys as
typed fields, each carrying its doc comment and, where there is one, a
`Server default: ...` note.

`Default::default()` gives an **empty** option set. Only fields you set are sent;
anything left empty falls back to the server's own default.

```rust
let mut opts = InsertRecordsRequestOptions::default();
opts.update_on_existing_pk = "true".to_string();   // only this key is sent
```

Options are deliberately *not* filled in with their defaults: a server older
than the API rejects a key it does not recognise (`Parameter map key: '...' is
not a valid parameter`).

### Binary Fields

Avro `bytes` fields map to `serde_bytes::ByteBuf`, re-exported for convenience
so you don't need your own `serde_bytes` dependency:

```rust
use gpudb_api::ByteBuf;

let req = RawInsertRecordsRequest {
    table_name: table.clone(),
    list: encoded.into_iter().map(ByteBuf::from).collect(),  // moves, no copy
    ..Default::default()
};
```

`ByteBuf` encodes as Avro `bytes` rather than an array of integers. Decoding
helpers (`decode_records`, `decode_records_parallel`, `decode_records_sequential`,
`decode_with_fast_schema`) are generic over `AsRef<[u8]>`, so they accept both
`Vec<Vec<u8>>` from the encoders and `Vec<ByteBuf>` from response fields with no
conversion.

### Constants

The `constants` module holds the string constants of each request's and
response's fields:

```rust
use gpudb_api::constants;

let encoding = constants::get_records_request::encoding::BINARY;   // "binary"
let primary_key = constants::column_property::PRIMARY_KEY;        // "primary_key"
```


## Bulk Inserting

`BulkInserter` batches records and sends each batch straight to the worker rank
that owns its records' shard, encoding batches on several workers at once.

```rust
use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaColumnProperties,
    Kinetica, KineticaOptions, BulkInserter, KineticaRecord, TypePropertiesBuilder,
    ShardKeyExtractor, ShardKeyValue, ShardKeyValues,
};
use smallvec::smallvec;
use std::sync::Arc;
use serde::Serialize;

#[derive(Serialize, Clone, Default)]
struct MyRecord {
    id: i32,
    name: String,
    score: f64,
}

// Required for shard-aware routing
impl ShardKeyExtractor for MyRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        // Return shard key columns (typically primary key)
        smallvec![("id", ShardKeyValue::Int(self.id))]
    }
}

impl KineticaRecord for MyRecord {
    fn avro_schema(&self) -> &'static str {
        // Use the builder API for type-safe schema generation
        avro_schema!(
            AvroSchemaBuilder::new("MyRecord")
                .int_field("id")
                .string_field("name")
                .double_field("score")
        )
    }

    fn type_properties() -> std::collections::HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .column("id", KineticaColumnProperties::new().primary_key())
            .build()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut options = KineticaOptions::new();
    options.set_username("admin")
           .set_password("password")
           .set_bypass_ssl_cert_check(false)
           .set_use_snappy(false);

    let kinetica = Arc::new(
        Kinetica::new(vec!["http://localhost:9191".to_string()], options).await?
    );

    // Create bulk inserter
    // - 50,000 record batch size
    // - 16 parallel workers
    let inserter = BulkInserter::new(
        kinetica,
        "my_table".into(),
        50000,
        16
    ).await?;

    // Option 1: Insert records one at a time (good for streaming)
    for i in 0..1_000_000 {
        let record = MyRecord {
            id: i,
            name: format!("user_{}", i),
            score: i as f64 * 1.5,
        };
        inserter.insert(Arc::new(record))?;
    }

    // Option 2: Insert records in batches (faster for bulk loading)
    // Uses parallel shard key computation and reduced lock contention
    let records: Vec<MyRecord> = (0..100_000)
        .map(|i| MyRecord {
            id: i,
            name: format!("user_{}", i),
            score: i as f64 * 1.5,
        })
        .collect();
    inserter.insert_batch(records)?;

    // Flush remaining records. `flush()` returns only once every batch has
    // been acknowledged, so no sleep is needed and a read-back is safe.
    inserter.flush().await?;

    println!("Inserted {} records", inserter.count_inserted());

    Ok(())
}
```

How to define a record type like `MyRecord` is described in
[Defining Record Types](#defining-record-types).

### GenericRecord (Dynamic Schema)

For scenarios where the schema is not known at compile time, use `GenericRecord`, with full shard-aware routing support:

```rust
use gpudb_api::{Kinetica, KineticaOptions, Type, GenericRecord, BulkInserter};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kinetica = Arc::new(
        Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?
    );

    // Fetch schema from existing table
    let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
    let schema_str = record_type.schema_string();

    // Create BulkInserter with shard-aware routing
    let inserter = BulkInserter::<GenericRecord>::with_schema(
        kinetica.clone(),
        "my_table".into(),
        50000,
        16,
        None,
        schema_str,
    ).await?;

    // Create and insert records dynamically
    for i in 0..1_000_000 {
        let mut record = record_type.new_record();
        record.set_by_name("id", i)?;
        record.set_by_name("name", format!("user_{}", i))?;
        record.set_by_name("score", i as f64 * 1.5)?;
        inserter.insert(Arc::new(record))?;
    }

    inserter.flush().await?;
    println!("Inserted {} records", inserter.count_inserted());

    Ok(())
}
```

A number in a `GenericRecord` goes into any `int`, `long`, `float` or `double`
column, converted as Rust's `as` converts it (so a `RecordValue::Long` in an
`int` column is truncated). A string goes only into a `string` column, and bytes
only into a `bytes` column. A nullable column takes only its own type, or null.
A value its column cannot take fails that record with a message such as
`Could not encode object: Field has incorrect data type in record :: value hello
(string) cannot be cast to expected type int at type_name.id`.

### Choosing Between KineticaRecord and GenericRecord

| Feature | KineticaRecord (Static) | GenericRecord (Dynamic) |
|---------|------------------------|-------------------------|
| **Schema** | Compile-time, type-safe | Runtime, schema from database |
| **Performance** | Maximum (zero runtime schema lookups) | High (runtime type checks) |
| **Use Case** | Known, fixed schemas | Dynamic schemas, ETL pipelines |
| **Shard-Aware Routing** | Yes via `ShardKeyExtractor` | Yes via `BulkInserter::with_schema()` |
| **BulkInserter** | `BulkInserter::new()` | `BulkInserter::with_schema()` |

Use `KineticaRecord` when you have a fixed schema and want compile-time
safety, and `GenericRecord` with `BulkInserter::with_schema()` when the
schema is known only at run time.

### Insert Methods

| Method | Use Case |
|--------|----------|
| `insert(Arc<T>)` | Single record streaming |
| `insert_with_backpressure(Arc<T>)` | Memory-bounded streaming (async; waits for a permit) |
| `insert_batch(Vec<T>)` | Bulk loading |
| `insert_batch_arc(Vec<Arc<T>>)` | Bulk loading of records already in an `Arc` |

The batch methods compute shard keys in parallel and take each queue's lock
once per group of records, so they cost less client CPU per record than
inserting one at a time.

An insert method fails only for its own records: one it cannot route (an
unshardable key value, no shard mapping, a removed rank) or, after `close()`, a
record that fills a batch. A batch the server rejects is reported through the
[error queue](#error-queue).

```rust
// Single record: good for streaming data or when records arrive one at a time
inserter.insert(Arc::new(record))?;

// Batch: recommended for bulk loading
let records: Vec<MyRecord> = generate_records(100_000);
inserter.insert_batch(records)?;

// Batch of records already in an Arc
let arc_records: Vec<Arc<MyRecord>> = records.into_iter().map(Arc::new).collect();
inserter.insert_batch_arc(arc_records)?;
```

### Flushing and Closing

`insert()` only queues; records reach the server when a batch fills, when the
timed flush fires, or when you flush explicitly.

| Method | Waits for the server? | Use for |
|--------|----------------------|---------|
| `flush()` | **Yes** | Making queued records durable mid-stream. Safe to read the table straight afterwards. |
| `flush_no_wait()` | No | Handing batches off without blocking. The records may not be visible yet. |
| `close()` | **Yes** | Finishing with the inserter. Also stops the timed-flush task and the flush workers, and logs a summary. |
| `close_with_timeout(d)` | Bounded | Same as `close()` but gives up after `d`, returning an error and leaving the workers running so the batches still in flight can finish. |

After `close()`, an insert that only buffers its record still succeeds, but one
whose record fills a batch fails, since no worker is left to send it.

```rust
let expected = records.len();
for record in records {
    inserter.insert(Arc::new(record))?;
}

// Returns only once every queued batch has been acknowledged.
inserter.flush().await?;

// So this read is guaranteed to see them - no sleep required.
let resp = kinetica.get_records::<GenericRecord>(table_name.clone(), 0, -9999).await?;
assert_eq!(resp.total_number_of_records, expected as i64);
```

`Ok(())` from `flush()` or `close()` means *every batch finished*, not that
every record succeeded. Check for failures explicitly:

```rust
inserter.close().await?;
if inserter.error_count() > 0 {
    for err in inserter.drain_errors() {
        eprintln!("{} records failed: {}", err.record_count, err.message);
    }
}
```

### Backpressure

For memory-bounded operation in high-throughput scenarios, use `insert_with_backpressure()`:

```rust
// Configure maximum in-flight batches
let mut options = BulkInserterOptions::new();
options.set_max_in_flight_batches(50);  // Limit concurrent batches

let inserter = BulkInserter::with_options(
    kinetica, table_name, 50000, 16, Some(options)
).await?;

// This will wait if at capacity (backpressure)
for record in records {
    inserter.insert_with_backpressure(Arc::new(record)).await?;
}

// Monitor backpressure
let metrics = inserter.backpressure_metrics();
println!("In-flight: {}/{} ({:.1}% utilized)",
    metrics.in_flight_batches,
    metrics.max_in_flight_batches,
    metrics.utilization_pct
);
```

### Timed Flush

For streaming workloads with latency requirements, enable timed flush:

```rust
use gpudb_api::{BulkInserter, BulkInserterOptions, FlushOptions};

let mut options = BulkInserterOptions::new();
// flush_when_full = false: send every buffered record every 5 seconds
options.set_flush_options(FlushOptions::new(false, 5));

let inserter = BulkInserter::with_options(
    kinetica, table_name, 50000, 16, Some(options)
).await?;

// Records will be automatically flushed every 5 seconds
// even if batches aren't full
for record in streaming_records {
    inserter.insert(Arc::new(record))?;
}

// Check timed flush status
println!("Timed flush running: {}", inserter.is_timed_flush_running());

// Optionally stop timed flush manually
inserter.stop_timed_flush();
```

In `FlushOptions::new(flush_when_full, flush_interval)`, with
`flush_when_full = true` (the default) the timer sends only queues that have
reached the batch size. The insert paths already send a queue as soon as it
fills, so in that mode a timed flush rarely has anything to send and partial
batches wait for `flush()`/`close()`. Pass `false` to have the timer push
partial batches out.

### Error Queue

Errors are collected in a non-blocking queue for async handling:

```rust
// Insert records (errors are queued, not thrown immediately)
for record in records {
    inserter.insert(Arc::new(record))?;
}

// Check for errors periodically
if inserter.error_count() > 0 {
    let errors = inserter.drain_errors();
    for error in errors {
        eprintln!("Insert error at {}: {} ({} records)",
            error.worker_url, error.message, error.record_count);
    }
}

// Also check warnings
let warnings = inserter.drain_warnings();
```

### Batch Callbacks

The `BatchInsertionListener` trait enables real-time monitoring of batch operations:

```rust
use gpudb_api::{BatchInsertionResult, BulkInserterOptions, FnBatchListener};
use std::sync::Arc;

// Define a callback function
let listener = FnBatchListener(|result: &BatchInsertionResult| {
    if result.success {
        println!("Batch completed: {} inserted, {} updated in {:.1}ms",
            result.count_inserted, result.count_updated, result.total_time_ms);
    } else {
        eprintln!("Batch failed: {:?}", result.error_message);
    }
});

// The listener is set on the options the inserter is created with
let mut options = BulkInserterOptions::new();
options.set_batch_listener(Arc::new(listener));
```

It is called once per batch, after the batch succeeds or finally fails.

### BulkInserter Options

```rust
use gpudb_api::{BulkInserter, BulkInserterOptions, FlushOptions};

let mut options = BulkInserterOptions::new();
options
    // Insert behavior
    .set_update_on_existing_pk(true)     // Enable upsert mode
    .set_return_individual_errors(true)   // Get per-record error info
    .set_allow_partial_batch(true)        // Continue on partial failures
    .set_truncate_strings(true)           // Auto-truncate long strings

    // Retry behavior: retries after a connection failure (default 3); then
    // the inserter fails over to the next cluster, if there is one
    .set_max_retries(5)

    // Backpressure control
    .set_max_in_flight_batches(100)       // Limit concurrent batches

    // Timed flush (streaming)
    .set_flush_options(FlushOptions::new(false, 30)); // Flush everything every 30 seconds

let inserter = BulkInserter::<MyRecord>::with_options(
    kinetica,
    table_name,
    batch_size,
    num_workers,
    Some(options)
).await?;
```

**Insert options (the `insert_records_options` module):**

| Option | Description |
|--------|-------------|
| `update_on_existing_pk` | Update existing records on primary key collision (upsert mode) |
| `ignore_existing_pk` | Silently ignore records with duplicate primary keys |
| `return_individual_errors` | Return per-record error info in response |
| `allow_partial_batch` | Allow partial batch success on errors |
| `truncate_strings` | Truncate strings to fit column width |

**Additional options:**

| Option | Description | Default |
|--------|-------------|---------|
| `max_in_flight_batches` | Maximum concurrent batches for backpressure control | 100 |
| `flush_interval` | Automatic flush interval in seconds (-1 to disable) | -1 |
| `flush_when_full` | Timed flush sends only full queues (`true`) or everything (`false`) | true |

### Tuning

`BulkInserter::new(kinetica, table_name, batch_size, num_workers)` takes:

- **`batch_size`**: records per batch. 50,000 suits most systems; use 25,000 on
  small systems and 100,000 or more on large ones.
- **`num_workers`**: the number of batches sent concurrently, used exactly as
  given; `0` gives one per available CPU. 4 to 16 suits most systems, and more
  helps when the network, not the CPU, is the limit.

For lower memory use and controlled latency, use smaller batches with
[backpressure](#backpressure).


## Defining Record Types

The `KineticaRecord` trait enables Rust structs to be serialized and inserted
into Kinetica tables:

```rust
pub trait KineticaRecord: serde::Serialize {
    /// Returns the Avro schema as a JSON string for this record type.
    fn avro_schema(&self) -> &'static str;

    /// Returns type properties including field types and attributes (e.g., primary_key).
    fn type_properties() -> std::collections::HashMap<String, Vec<String>>;

    /// Whether requests of this type are Snappy-compressed. Only the generated
    /// /insert/records and /update/records requests override it.
    fn use_compression() -> bool {
        false
    }
}
```

The trait requires `serde::Serialize` as a supertrait, which is what encodes
the record in Avro.

### Step 1: Define Your Struct

Your struct must derive certain traits:

```rust
#[derive(serde::Serialize, Default)]
struct MyRecord {
    id: i32,
    name: String,
    value: f64,
    timestamp: i64,
}
```

| Derive | Purpose |
|--------|---------|
| `serde::Serialize` | Encodes the record in Avro |
| `Default` | Required by `BulkInserter::new` and `with_options`, which read the schema from `T::default()` |

`Clone` is not needed: a `BulkInserter` shares records as `Arc<T>`.

A `Vec<u8>` field needs `serde_bytes`:

```rust
#[derive(serde::Serialize, Default)]
struct RecordWithBytes {
    id: i32,
    #[serde(with = "serde_bytes")]  // Required for Vec<u8> fields
    raw_data: Vec<u8>,
}
```

### Step 2: Implement `avro_schema()`

Use the `AvroSchemaBuilder` with the `avro_schema!` macro for type-safe, efficient schema generation:

```rust
use gpudb_api::{avro_schema, AvroSchemaBuilder};

fn avro_schema(&self) -> &'static str {
    avro_schema!(
        AvroSchemaBuilder::new("MyRecord")
            .int_field("id")
            .string_field("name")
            .double_field("value")
            .timestamp_field("timestamp")
    )
}
```

> **Generic code:** `avro_schema!(builder)` caches one schema per call site,
> so inside a generic function every type parameter gets the schema built by
> whichever instantiation ran first. When the schema depends on a type
> parameter, use `avro_schema!(for T => builder)`, which caches one schema per
> type.

**Field methods:**

| Method | Rust Type | Avro Type |
|--------|-----------|-----------|
| `.int_field("name")` | `i32` | int |
| `.long_field("name")` | `i64` | long |
| `.float_field("name")` | `f32` | float |
| `.double_field("name")` | `f64` | double |
| `.string_field("name")` | `String` | string |
| `.bool_field("name")` | `bool` | boolean |
| `.bytes_field("name")` | `Vec<u8>` | bytes |
| `.timestamp_field("name")` | `i64` | long (with timestamp semantics) |

**Nullable field variants:**

```rust
.nullable_string_field("optional_name")
.nullable_int_field("optional_count")
.nullable_long_field("optional_id")
.nullable_double_field("optional_value")
.nullable_bytes_field("optional_data")
```

**Schema metadata:**

```rust
AvroSchemaBuilder::new("MyRecord")
    .namespace("com.example.myapp")
    .doc("Description of this record type")
    .int_field("id")
    // ...
```

### Step 3: Implement `type_properties()`

Use `TypePropertiesBuilder` to define column properties:

```rust
use gpudb_api::{TypePropertiesBuilder, KineticaColumnProperties};
use std::collections::HashMap;

fn type_properties() -> HashMap<String, Vec<String>> {
    TypePropertiesBuilder::new()
        .primary_key("id")
        .timestamp("timestamp")
        .data("name")
        .data("value")
        .build()
}
```

**Builder shortcuts:**

| Method | Purpose |
|--------|---------|
| `.primary_key("col")` | Mark column as primary key |
| `.shard_key("col")` | Mark column as shard key |
| `.timestamp("col")` | Mark column as timestamp |
| `.data("col")` | Regular data column |

Properties for the same column accumulate, whichever method adds them:
`.primary_key("id").shard_key("id")` gives `id` both properties, and a
property already on the column is not repeated.

**Advanced column properties:**

```rust
fn type_properties() -> HashMap<String, Vec<String>> {
    TypePropertiesBuilder::new()
        .column("id", KineticaColumnProperties::new()
            .primary_key()
            .shard_key())
        .column("name", KineticaColumnProperties::new()
            .char_n(64))       // Fixed char length
        .column("email", KineticaColumnProperties::new()
            .char_n(128)
            .nullable())
        .column("ip_address", KineticaColumnProperties::new()
            .ipv4())
        .column("created_at", KineticaColumnProperties::new()
            .timestamp())
        .build()
}
```

| Method | Description |
|--------|-------------|
| `.primary_key()` | Mark as primary key |
| `.shard_key()` | Mark as shard key for routing |
| `.nullable()` | Allow null values |
| `.timestamp()` | Timestamp type |
| `.date()` | Date type |
| `.time()` | Time type |
| `.datetime()` | DateTime type |
| `.char_n(N)` | Fixed char length (N bytes) |
| `.int8()` | 8-bit integer |
| `.int16()` | 16-bit integer |
| `.ipv4()` | IPv4 address |
| `.uuid()` | UUID type |
| `.wkt()` | Well-Known Text (geometry) |
| `.decimal()` | Decimal type (`NUMERIC(19,4)`) |
| `.decimal_with_precision(P, S)` | Decimal with precision and scale, `decimal(P,S)` |
| `.ulong()` | Unsigned 64-bit integer (string column) |
| `.boolean()` | Boolean (int column) |
| `.text_search()` | Full text search |
| `.json()` | JSON values (string column) |
| `.array("int")` / `.array_n("int", N)` | Array of the item type, optionally at most N items |
| `.vector(N)` | Vector of N floats (bytes column) |
| `.soft_primary_key()` | Mark as soft primary key |
| `.compress("zstd(7)")` | Column compression codec |
| `.dict()` | Dictionary encoding |
| `.init_with_now()` / `.init_with_uuid()` | Fill empty/invalid values on insert |
| `.update_with_now()` / `.update_with_user()` | Overwrite on every update |
| `.default_value("0")` / `.default_sql("0")` | Default value expression |
| `.property("...")` | Any other property string |

These set the properties named by the `column_property` module's constants.
Record names, namespaces, field names and docs given to `AvroSchemaBuilder` are
JSON-escaped; field names are otherwise used as given.

### Step 4: Implement `ShardKeyExtractor` (for BulkInserter)

If using `BulkInserter` with shard-aware routing, also implement this trait:

```rust
use gpudb_api::{ShardKeyExtractor, ShardKeyValue, ShardKeyValues};
use smallvec::smallvec;

impl ShardKeyExtractor for MyRecord {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![("id", ShardKeyValue::Int(self.id))]
    }
}
```

**`ShardKeyValue` variants:**

```rust
ShardKeyValue::Int(i32)
ShardKeyValue::Long(i64)
ShardKeyValue::Float(f32)
ShardKeyValue::Double(f64)
ShardKeyValue::Boolean(bool)      // boolean column
ShardKeyValue::Int8(i8)           // int8 column
ShardKeyValue::Int16(i16)         // int16 column
ShardKeyValue::Timestamp(i64)     // timestamp column, milliseconds since the epoch
ShardKeyValue::String(&str)       // string and charN columns
ShardKeyValue::Date(&str)         // "YYYY-MM-DD"
ShardKeyValue::DateTime(&str)     // "YYYY-MM-DD HH:MM:SS[.mmm]"
ShardKeyValue::Time(&str)         // "HH:MM:SS[.mmm]"
ShardKeyValue::Ipv4(&str)         // "a.b.c.d"
ShardKeyValue::Decimal(&str)      // "123.45"
ShardKeyValue::Uuid(&str)
ShardKeyValue::Ulong(&str)
ShardKeyValue::Null               // a null value
```

A value must be of a kind its column takes; one that is not fails the insert
with an `Unable to calculate shard/primary key` message.

`ShardKeyValues` is a `SmallVec` that holds up to 4 shard key columns without
a heap allocation. Keep shard keys to 1 or 2 columns, preferably integers,
which hash fastest; a key of more than 4 columns works, but allocates per
record.

```rust
// Single integer primary/shard key
impl ShardKeyExtractor for User {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![("user_id", ShardKeyValue::Long(self.user_id))]
    }
}

// Composite key with 2 columns
impl ShardKeyExtractor for TenantData {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![
            ("tenant_id", ShardKeyValue::Int(self.tenant_id)),
            ("data_id", ShardKeyValue::Long(self.data_id)),
        ]
    }
}
```

### Complete Example

```rust
use gpudb_api::{
    avro_schema, AvroSchemaBuilder, KineticaRecord, KineticaColumnProperties,
    TypePropertiesBuilder, ShardKeyExtractor, ShardKeyValue, ShardKeyValues,
};
use smallvec::smallvec;
use std::collections::HashMap;

#[derive(serde::Serialize, Clone, Default)]
struct SensorReading {
    sensor_id: i32,
    timestamp_ms: i64,
    temperature: f64,
    humidity: Option<f64>,
    location: String,
}

impl KineticaRecord for SensorReading {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("SensorReading")
                .namespace("com.example.sensors")
                .doc("Sensor telemetry data")
                .int_field("sensor_id")
                .long_field("timestamp_ms")
                .double_field("temperature")
                .nullable_double_field("humidity")
                .string_field("location")
        )
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .column("sensor_id", KineticaColumnProperties::new()
                .primary_key()
                .shard_key())
            .column("timestamp_ms", KineticaColumnProperties::new()
                .timestamp())
            .data("temperature")
            .data("humidity")
            .column("location", KineticaColumnProperties::new()
                .char_n(64))
            .build()
    }
}

impl ShardKeyExtractor for SensorReading {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        smallvec![("sensor_id", ShardKeyValue::Int(self.sensor_id))]
    }
}
```

For more, see the `schema_builder_demo`, `insert_all_sharded_types` and
`insert_and_get_records_bin` [examples](#examples).


## Retrieving Records by Key

`RecordRetriever` looks records up by shard key on the worker rank that holds
them:

```rust
let record_type = Arc::new(Type::from_table(&kinetica, "my_table").await?);
let retriever = RecordRetriever::new(kinetica.clone(), "my_table", record_type).await?;

let response = retriever.get_by_key(&[ShardKeyValue::Int(123)], None).await?;
for record in &response.data {
    println!("{:?}", record.get(0));
}
```


## SQL

Besides the generated `execute_sql`, the connection runs statements with
`execute` and iterates over query results with `query`:

```rust
let affected = kinetica.execute("DELETE FROM my_table WHERE id < 100").await?;

// A batch at a time, from a paging table the iterator drops when closed
let mut records = kinetica.query("SELECT * FROM my_table").await?;
while records.has_next() {
    let record = records.next().await?;   // a GenericRecord
    println!("{record:?}");
}
records.close().await;
```

`SqlClient` and `BatchingSqlClient` (which sends `INSERT ... VALUES` statements
through a `BulkInserter`) are in the `sql` module; with the `sqlx` feature,
`sqlx_support` connects over the PostgreSQL wire protocol. Over that protocol:

- **Types:** several Kinetica types arrive as something other than their
  PostgreSQL counterparts, and geometry cannot be selected.
- **Parameters:** every parameter after a statement's first is read as text.
- **Transactions:** there are none.

`sqlx_support` provides wrappers for these cases (`KineticaText`,
`KineticaTimestamp`, `KineticaArray` and others). The native API carries every
type. See [SQL_API.md](https://github.com/kineticadb/kinetica-api-rust/blob/master/crates/gpudb-api/SQL_API.md) and
[SQLX_API.md](https://github.com/kineticadb/kinetica-api-rust/blob/master/crates/gpudb-api/SQLX_API.md).


## JSON Records

```rust
// One JSON object, or an array of them
let response = kinetica
    .insert_records_from_json(r#"[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]"#, "my_table")
    .await?;

// Records as JSON
let records = kinetica.get_records_json("my_table", &[], 0, 100).await?;
println!("{} records in all; these: {}", records.total_records(), records.json_records());

// A JSON BulkInserter: records as JSON text, sent to /insert/records/json
let inserter = BulkInserter::new_json(kinetica.clone(), "my_table".into(), 10_000, 4).await?;
inserter.insert(Arc::new(r#"{"id": 3, "name": "c"}"#.to_string()))?;
inserter.close().await?;
```


## KiFS Files

`KifsFileHandler` uploads, downloads and manages files in the Kinetica File
System (KiFS):

- **Upload** single files, or many with glob patterns such as `*.csv` or
  `data/**/*.json`
- **Download** single files or entire directories
- **Manage directories**: create, delete and list them
- **Multi-part transfers** of files over `file_size_to_split` (60 MB by default)
- **Progress callbacks** for uploads and downloads
- **Ingest**: upload local files and insert their records into a table in one call

### Basic Usage

```rust
use gpudb_api::{Kinetica, KineticaOptions};
use gpudb_api::filesystem::{KifsFileHandler, KifsFileHandlerOptions, UploadOptions};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kinetica = Arc::new(
        Kinetica::new(vec!["http://localhost:9191".to_string()], KineticaOptions::new()).await?
    );

    // Create a file handler with default options
    let handler = KifsFileHandler::new(kinetica, None);

    // Create a directory
    handler.create_directory("my_directory", true).await?;

    // Upload a single file
    handler.upload("/path/to/file.csv", "my_directory", None, None).await?;

    // Upload multiple files with glob pattern
    handler.upload("/data/*.csv", "my_directory", None, None).await?;

    // Download a file
    handler.download("my_directory/file.csv", "/local/path", None, None).await?;

    // List files in a directory
    let files = handler.show_files(vec!["my_directory".to_string()]).await?;
    for file in files {
        println!("File: {} ({} bytes)", file.file_name, file.file_size);
    }

    // Delete a file
    handler.delete_files(vec!["my_directory/file.csv".to_string()], true).await?;

    // Delete a directory
    handler.delete_directory("my_directory", true, true).await?;

    Ok(())
}
```

### Upload Options

```rust
use gpudb_api::filesystem::UploadOptions;

// Create custom upload options
let options = UploadOptions::new()
    .with_recursive(true)    // Recursively search directories
    .with_ttl(3600);         // Files expire in 1 hour (TTL in seconds)

handler.upload("/data/**/*.json", "kifs_dir", Some(options), None).await?;
```

`recursive` defaults to `true` and applies to patterns, and to a directory given
by name, which uploads its files as `<directory>/*` does. A file given by name
is uploaded alone: `upload("/data/file.csv", ...)` does not also upload a
`file.csv` under a subdirectory of `/data`.

Patterns use glob syntax: `*` and `?` stay within one path component, `**`
crosses components, `[a-z]` / `[!a-z]` are character classes, `{csv,json}` is a
group of alternatives, and `\` escapes the next character. A name containing
any of `* ? [ { \` is treated as a pattern.

### Upload and Download Behaviour

Note these, including where they carry a risk:

- A plain file name that does not exist fails the whole upload before anything
  is sent; a *pattern* that matches nothing only logs a warning, and the upload
  returns `Ok` with no results.
- With `overwrite_existing(false)`, a file that already exists locally is left
  alone but still reported as downloaded.
- A multi-part upload that fails after it has started is cancelled on the
  server; one whose future is dropped is not.
- Downloads keep each file's path below the KiFS path asked for: downloading
  `dir` saves `dir/a.csv` as `<local dir>/a.csv` and `dir/sub/x.csv` as
  `<local dir>/sub/x.csv`, so `dir/a/x.csv` and `dir/b/x.csv` are not both
  saved as `<local dir>/x.csv`. A file whose local path another file of the same
  download already has fails, rather than overwriting it, and a file named twice
  is downloaded once.
- A download is written to a temporary file beside the local one, which takes
  its place only once complete. A failed or cancelled download deletes the
  temporary file and leaves an existing local copy as it was.

### Download Options

```rust
use gpudb_api::filesystem::DownloadOptions;

// The default options, which a download given none uses, overwrite existing
// local files; these keep them
let options = DownloadOptions::default().with_overwrite_existing(false);

handler.download("kifs_dir/file.csv", "/local/path", Some(options), None).await?;
```

### File Handler Options

```rust
use gpudb_api::filesystem::KifsFileHandlerOptions;

// Configure multi-part transfer thresholds
// Each setter fails for a value it refuses
let options = KifsFileHandlerOptions::new()
    .with_file_size_to_split(50 * 1024 * 1024)?      // 50 MB threshold (max 60 MB)
    .with_thread_pool_size(2)?                       // full-file batches sent at once (1..=CPUs)
    .with_max_upload_bytes_in_flight(64 * 1024 * 1024)?; // cap on payload bytes held at once

let handler = KifsFileHandler::new(kinetica, Some(options));
```

Small files are uploaded in batches of up to `file_size_to_split` bytes. Up to
`thread_pool_size` batches (default 5) are sent concurrently, and a wave of
batches is waited for before the next once it reaches that count or
`max_upload_bytes_in_flight` (default 128 MB). The option fields are private;
the `with_*` setters validate them and return an error for a value out of
range.

### Progress Callbacks

Track upload/download progress with listeners:

```rust
use gpudb_api::filesystem::{
    FileUploadListener, FileDownloadListener, OperationResult,
    FnUploadListener, FnDownloadListener,
};

// Simple closure-based upload listener
let upload_listener = FnUploadListener::new(
    |results| println!("Multi-part upload complete: {} parts", results.len()),
    |result| {
        if let Some(info) = &result.upload_info {
            println!("Uploaded part {} of {}", info.upload_part_number, info.total_parts);
        }
    },
    // A batch of whole files sent, or a file that could not be read
    |result| println!("Full-file upload batch finished: successful={}", result.successful),
);

handler.upload("/large/file.dat", "kifs_dir", None, Some(&upload_listener)).await?;

// Simple closure-based download listener
let download_listener = FnDownloadListener::new(
    |results| println!("Multi-part download complete: {} parts", results.len()),
    |result| {
        if let Some(info) = &result.download_info {
            // Download parts are numbered from 0
            println!("Downloaded part {} of {}", info.download_part_number + 1, info.total_parts);
        }
    },
    // The KiFS names of the files of a batch saved
    |file_names| println!("Full-file download batch saved: {file_names:?}"),
);

handler.download("kifs_dir/large_file.dat", "/local/path", None, Some(&download_listener)).await?;
```

For more control, implement the listener traits directly:

```rust
use gpudb_api::filesystem::{FileUploadListener, OperationResult};
use std::sync::atomic::{AtomicUsize, Ordering};

// Listeners are called from the transfer tasks, so they must be Send + Sync
#[derive(Default)]
struct MyUploadListener {
    parts_uploaded: AtomicUsize,
}

impl FileUploadListener for MyUploadListener {
    fn on_multi_part_upload_complete(&self, results: Vec<OperationResult>) {
        println!("Upload complete: {} parts", results.len());
    }

    fn on_part_upload(&self, result: OperationResult) {
        if let Some(info) = &result.upload_info {
            let done = self.parts_uploaded.fetch_add(1, Ordering::Relaxed) + 1;
            println!("Part {} of {} ({done} so far)", info.upload_part_number, info.total_parts);
        }
    }

    fn on_full_file_upload(&self, result: OperationResult) {
        println!("Uploaded: {:?}", result.full_file_names);
    }
}
```

`on_multi_part_upload_cancel` has a default that does nothing.

### Ingesting Files

`ingest` uploads local files (or glob patterns) and inserts their records into
a table: the files go to a directory of their own under the user's KiFS home
directory, are inserted from there with `/insert/records/fromfiles`, and the
directory is deleted.

```rust
use gpudb_api::filesystem::{FileType, IngestOptions, TableCreationOptions};

let mut ingest_options = IngestOptions::new();
ingest_options.set_file_type(FileType::DelimitedText).set_text_has_header(true);

let mut table_options = TableCreationOptions::new();
table_options.set_no_error_if_exists(true);

let result = handler
    .ingest(vec!["/data/*.csv".to_string()], "my_schema.my_table", Some(ingest_options), Some(table_options))
    .await?;
println!("Inserted {} records", result.count_inserted());
```

### Directory and File Operations

```rust
// Create a directory (no error if exists)
handler.create_directory("my_dir", true).await?;

// Show all directories
let all_dirs = handler.show_all_directories().await?;
for dir in all_dirs {
    println!("{} (created by: {}, permissions: {})",
        dir.kifs_path, dir.created_by, dir.permission);
}

// Check if directory exists
if handler.kifs_directory_exists("my_dir").await {
    println!("Directory exists");
}

// List files in a path
let files = handler.show_files(vec!["my_dir".to_string()]).await?;
for file in &files {
    println!("{}: {} bytes, created at {}",
        file.file_name, file.file_size, file.creation_time);
}

// Check if file exists
if handler.kifs_file_exists("my_dir/file.csv").await {
    println!("File exists");
}

// Delete specific files (no error if not exists)
handler.delete_files(vec![
    "my_dir/file1.csv".to_string(),
    "my_dir/file2.csv".to_string(),
], true).await?;

// Delete all files in a directory
handler.delete_files_in_dir("my_dir", true).await?;

// Delete a directory (recursive, no error if not exists)
handler.delete_directory("my_dir", true, true).await?;
```

### KiFS Types

| Type | Description |
|------|-------------|
| `KifsFileHandler` | Main API for file operations |
| `KifsFileHandlerOptions` | Configuration for multi-part transfers |
| `UploadOptions` | Upload configuration (recursive, TTL) |
| `DownloadOptions` | Download configuration (overwrite) |
| `KifsFileInfo` | File metadata (name, size, owner, creation time) |
| `KifsDirectoryInfo` | Directory metadata (path, owner, permissions) |
| `OperationResult` | Result of upload/download operation |
| `FileUploadListener` | Trait for upload progress callbacks |
| `FileDownloadListener` | Trait for download progress callbacks |
| `FnUploadListener` | Closure-based upload listener |
| `FnDownloadListener` | Closure-based download listener |
| `NoOpUploadListener` / `NoOpDownloadListener` | Listeners that ignore every event |
| `MultiPartUploadInfo` / `MultiPartDownloadInfo` | A part's number, count and file, in an `OperationResult` |
| `IngestOptions` | How `ingest` reads the files (file type, header, error handling, ...) |
| `TableCreationOptions` | How `ingest` creates the table, if it does |
| `IngestResult` | Whether it succeeded, the files, and the records inserted, updated and skipped |
| `FileType`, `ErrorHandlingMode`, `TypeInferenceMode`, `PartitionType` | Choices for the ingest options |


## Configuration

`KineticaOptions` configures a connection. Each field has a `set_` method that
can be chained, as the examples show.

```rust
pub struct KineticaOptions {
    pub primary_url: String,              // URL of the primary cluster in HA environment
    pub username: String,                 // Basic auth username
    pub password: String,                 // Basic auth password
    pub oauth_token: String,              // OAuth2 bearer token; used instead of username/password when set
    pub hostname_regex: Option<String>,   // Regex pattern to filter URLs by hostname/IP
    pub use_snappy: bool,                 // Enable Snappy compression for payloads
    pub bypass_ssl_cert_check: bool,      // Skip SSL verification (dev only)
    pub trust_store_file_path: String,    // Trust store (PKCS#12, JKS or PEM) to trust servers by
    pub trust_store_password: String,     // The trust store's password; both are needed for it to be used
    pub disable_failover: bool,           // Disable failover upon failures
    pub disable_auto_discovery: bool,     // Disable automatic cluster/worker discovery
    pub ha_failover_order: HaFailoverOrder, // Failover order (Sequential, the default, or Random)
    pub http_headers: HashMap<String, String>, // Extra HTTP headers sent with each request
    pub client_name: Option<String>,      // Client program name, first in the User-Agent
    pub client_version: Option<String>,   // Client program version
    pub tcp_keep_alive: bool,             // TCP keep-alive probes (default on, system timings)
    pub tcp_keep_idle: u32,               // Seconds idle before the first probe (0 = system's)
    pub tcp_keep_interval: u32,           // Seconds between probes (0 = system's)
    pub tcp_keep_count: u32,              // Probes before dropping (0 = system's)
    pub timeout: u64,                     // Request timeout in milliseconds (0 = none)
    pub server_connection_timeout: u64,   // Connect timeout in milliseconds (default 5000)
    pub host_manager_port: u16,           // Host manager port number
    pub initial_connection_attempt_timeout: u64, // How long to keep retrying the first connection, in ms
    pub max_total_connections: usize,     // Requests in flight across all hosts (default 40)
    pub max_connections_per_host: usize,  // Requests in flight to one host (default 10)
    pub max_retries: usize,               // HTTP retries on the same server before failing over (default 5)
    pub failback_polling_interval: u64,   // Seconds between checks for the primary after a failover
}
```

### HTTP Headers

Every request carries the `http_headers`, a `User-Agent`
(`MyApp/1.0 kinetica-api-rust/<version> (Rust/<rustc>; <os>/<version>; <arch>)`, the
client program first when `set_client_info` names one), the `Authorization` and,
when an HA synchronicity override is set, `X-Kinetica-Group`. The headers the client
sets itself cannot be set through `http_headers`, except that `Basic` credentials in
an `Authorization` header are used, and an `X-Kinetica-Group` header names the
starting `HaSynchronicityMode`. Headers can be added and removed later with
`Kinetica::add_http_header` and `remove_http_header`, and the mode changed with
`set_ha_sync_mode`.

### HA Failover and Fail-Back

A request is first retried on the same server, at once and up to `max_retries`
times (default 5), when it cannot be sent or the server answers 502 or 503. It
then fails over to the next cluster when the server cannot serve it (it is
unreachable, exiting, offline or returns a gateway error), but not for an
ordinary API error or an authentication failure. Once a failover has moved the
connection off the primary cluster, the primary is checked every
`failback_polling_interval` seconds (default 5), and the connection switches
back to it when it is running, its HA queues have drained and it answers a
query.

### Snappy Compression

When `use_snappy` is enabled (the default), the payloads of `/insert/records` and
`/update/records` requests, including the `BulkInserter`'s, are compressed with
Snappy before sending, as `application/x-snappy`. Every other request is sent
uncompressed, as is a payload too large for Snappy. Compression most helps large
bulk inserts over slower networks.

```rust
let mut options = KineticaOptions::new();
options.set_username("admin")
       .set_password("password")
       .set_use_snappy(false);  // Disable compression
```

### TLS

```rust
// Trust a server's self-signed certificate, or its CA, from a trust store: a
// PKCS#12 or JKS store made with keytool, or a PEM file of certificates. The
// server's name is not checked, and a store that cannot be read fails the
// connection.
let mut options = KineticaOptions::new();
options
    .set_trust_store_file_path("/path/to/truststore.jks")
    .set_trust_store_password("password_for_truststore");

// Or skip the check entirely (development only)
options.set_bypass_ssl_cert_check(true);
```


## Error Handling

All errors are wrapped in `KineticaError`:

```rust
pub enum KineticaError {
    Http(String),                 // The response broke off or could not be read
    Avro(String),                 // Serialization errors
    Api(String),                  // Server-side errors
    Unauthorized(String),         // Authentication failures (401/403)
    HostnameRegexFailure(String), // The hostname regex matched no address
    Connection(String),           // Not sent, or no response in time
    Config(String),               // Configuration errors (invalid params, missing permissions, TLS)
    FailoverDisabled(String),     // A failover was needed but is disabled
    HaUnavailable(String),        // Every cluster in the HA ring was tried
    Exit(String),                 // The server is exiting, offline or draining
    Internal,                     // Internal client errors
    Caused { message: String, source: Box<KineticaError> }, // An error and its cause
}
```

`message()` gives the error's message without the kind of error before it, and
`root_cause()` follows a `Caused` chain to the original error.

```rust
match kinetica.create_table("my_table".to_string(), type_id).await {
    Ok(response) => println!("Success: {}", response.table_name),
    Err(KineticaError::Api(msg)) => eprintln!("API error: {}", msg),
    Err(KineticaError::Http(msg)) | Err(KineticaError::Connection(msg)) => eprintln!("Network error: {}", msg),
    Err(KineticaError::Config(msg)) => eprintln!("Configuration error: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

A `BulkInserter` validates its table when it is created, and returns
`KineticaError::Config` when:

- the table doesn't exist
- the user lacks insert permission on it, or update permission when using
  `update_on_existing_pk`
- the batch size is less than 1


## Troubleshooting

### Connection Errors

A server whose certificate is not trusted by the system fails to connect over
HTTPS; trust it with a trust store, as in [TLS](#tls).

### Schema Errors

Ensure the Avro schema matches the table definition:

- Field names must match exactly
- Types must be compatible
- Required fields cannot be null

### Insert Performance

1. **Low throughput**: Increase batch size to 50,000-100,000
2. **High CPU**: Reduce parallel workers
3. **High memory**: Decrease batch size, or use [backpressure](#backpressure)
4. **Network bottleneck**: Increase workers, and keep Snappy compression on


## Examples

The `gpudb-examples` crate holds programs that use the API. Run one from the
root of this repository:

```bash
cargo run -p gpudb-examples --bin complete_workflow --release

# The SQLx examples need the sqlx feature
cargo run -p gpudb-examples --features sqlx --bin sqlx_example --release
```

They connect to the Kinetica named by these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KINETICA_URL` | `http://localhost:9191` | Kinetica server URL |
| `KINETICA_USER` | `admin` | Authentication username |
| `KINETICA_PASSWORD` | `password` | Authentication password |
| `KINETICA_USE_SNAPPY` | `false` | Enable Snappy compression (`true` or `1`) |
| `KINETICA_BYPASS_SSL` | unset | `1` accepts any server certificate, such as a self-signed one |
| `KINETICA_HOST` | `localhost` | Host of the PostgreSQL endpoint (port 5432), for the SQLx examples |

```bash
KINETICA_URL="http://192.168.1.100:9191" KINETICA_PASSWORD="mypass" \
    cargo run -p gpudb-examples --bin bulk_insert_mt_fast --release
```

On Windows, set them first: `$env:KINETICA_URL = "http://localhost:9191"` in
PowerShell, or `set KINETICA_URL=http://localhost:9191` at a Command Prompt.

| Example | Description | API Used |
|---------|-------------|----------|
| **Bulk Insert** | | |
| `bulk_insert_mt_fast` | Bulk insert, the recommended way | `BulkInserter` |
| `load_generator` | Configurable load generator | `BulkInserter` |
| `bulk_insert_mt_bin` | Multi-threaded bulk insert (binary encoding) | `BulkInserter` |
| `bulk_insert_mt_json` | Multi-threaded bulk insert (JSON encoding) | Direct API |
| `bulk_inserter_dashboard` | Real-time monitoring dashboard for BulkInserter | `BulkInserter` |
| `bulk_inserter_full_integration` | Full integration demo with all features | `BulkInserter` |
| `insert_sharded_aware` | Shard-aware insert demonstration | `BulkInserter` |
| `insert_all_sharded_types` | All data types demo with sharding | `BulkInserter` |
| **SQL & Query** | | |
| `execute_sql` | Execute SQL queries | Direct API |
| `sqlx_example` | SQLx integration (PostgreSQL wire protocol) | `sqlx_support` |
| `sqlx_complex_types` | JSON, arrays and vectors over SQLx | `sqlx_support` |
| **Schema & Table Management** | | |
| `create_type` | Create a custom type/schema | Direct API |
| `create_table` | Create a table | Direct API |
| `has_type` | Check if a type exists | Direct API |
| `has_table` | Check if a table exists | Direct API |
| `clear_table` | Delete (drop) a table | Direct API |
| `schema_builder_demo` | Avro schema builder usage | `AvroSchemaBuilder` |
| **Data Operations** | | |
| `insert_and_get_records_bin` | Insert/retrieve with binary encoding | Direct API |
| `insert_and_get_records_json` | Insert/retrieve with JSON encoding | Direct API |
| `record_retriever` | Efficient multi-head record retrieval | `RecordRetriever` |
| `shard_key_computation` | Shard key details | Direct API |
| **File System** | | |
| `kifs_demo` | KiFS file system operations | `KifsFileHandler` |
| **Utilities** | | |
| `complete_workflow` | End-to-end workflow demonstration | Direct API |
| `cleanup_all` | Drop the schemas and tables the examples create | Direct API |

See [crates/gpudb-examples/README.md](https://github.com/kineticadb/kinetica-api-rust/blob/master/crates/gpudb-examples/README.md) for more
on each.


## Support

For bugs, please submit an
[issue on Github](https://github.com/kineticadb/kinetica-api-rust/issues).

For support, you can post on
[stackoverflow](https://stackoverflow.com/questions/tagged/kinetica) under the
``kinetica`` tag or
[Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg).


## Contact Us

* Ask a question on Slack:
  [Slack](https://join.slack.com/t/kinetica-community/shared_invite/zt-1bt9x3mvr-uMKrXlSDXfy3oU~sKi84qg)
* Follow on GitHub:
  [Follow @kineticadb](https://github.com/kineticadb)
* Email us:  <support@kinetica.com>
* Visit:  <https://www.kinetica.com/contact/>
