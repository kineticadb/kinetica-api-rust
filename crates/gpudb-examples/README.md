# Kinetica Rust API Examples

Programs that use the Kinetica Rust API, from creating a type and table to bulk
inserting, retrieving by key, SQL, SQLx and KiFS.

## Running the Examples

Each example connects to a running Kinetica, named by these environment
variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KINETICA_URL` | `http://localhost:9191` | Kinetica server URL |
| `KINETICA_USER` | `admin` | Authentication username |
| `KINETICA_PASSWORD` | `password` | Authentication password |
| `KINETICA_USE_SNAPPY` | `false` | Enable Snappy compression (`true` or `1`) |
| `KINETICA_BYPASS_SSL` | unset | `1` accepts any server certificate, such as a self-signed one |
| `KINETICA_HOST` | `localhost` | Host of the PostgreSQL endpoint (port 5432), for the SQLx examples |

Run one from the root of the repository:

```bash
# With the defaults
cargo run -p gpudb-examples --bin create_type

# Against another server
KINETICA_URL="http://your-server:9191" KINETICA_USER="your_username" KINETICA_PASSWORD="your_password" \
    cargo run -p gpudb-examples --bin create_type --release

# The SQLx examples need the sqlx feature
cargo run -p gpudb-examples --features sqlx --bin sqlx_example
```

On Windows, set the variables first: `$env:KINETICA_URL = "http://your-server:9191"`
in PowerShell, or `set KINETICA_URL=http://your-server:9191` at a Command Prompt.

`load_generator` takes its own variables; see [load_generator.md](load_generator.md).

## Examples

### Types and Tables

| Example | What it does |
|---------|--------------|
| `create_type` | Creates a type (a table's schema) |
| `has_type` | Checks whether a type exists |
| `create_table` | Creates a table of a type made by `create_type`, which must run first |
| `has_table` | Checks whether a table exists |
| `clear_table` | Deletes a table, with its records; this cannot be undone |
| `schema_builder_demo` | Builds Avro schemas with `AvroSchemaBuilder` |
| `complete_workflow` | All of the above, end to end |

### Inserting and Reading Records

| Example | What it does |
|---------|--------------|
| `insert_and_get_records_bin` | Inserts records and reads them back, in binary (Avro) encoding |
| `insert_and_get_records_json` | The same, in JSON encoding |
| `record_retriever` | Inserts records with a shard key, then fetches them by key with `RecordRetriever` |
| `shard_key_computation` | Shows how a record's shard key is computed, which decides the worker it goes to |

### Bulk Inserting

| Example | What it does |
|---------|--------------|
| `bulk_insert_mt_fast` | Bulk inserts with `BulkInserter`, the recommended way |
| `bulk_insert_mt_bin` | Bulk inserts from several threads at once, in binary encoding |
| `bulk_insert_mt_json` | The same, in JSON encoding |
| `insert_sharded_aware` | Sends each record straight to the worker that owns its shard |
| `insert_all_sharded_types` | Bulk inserts records holding every Kinetica column type, sharded |
| `bulk_inserter_full_integration` | Feeds one `BulkInserter` from several producers, with monitoring, backpressure, error tracking, batch callbacks and a graceful shutdown |
| `bulk_inserter_dashboard` | A console dashboard of a running `BulkInserter`: throughput, queue depth, backpressure, errors and its configuration |
| `load_generator` | A configurable load generator; see [load_generator.md](load_generator.md) |

Run the bulk-insert examples with `--release`: a debug build is much slower.

### SQL

| Example | What it does |
|---------|--------------|
| `execute_sql` | Runs DDL, DML and queries through `execute_sql`; see [EXECUTE_SQL_GUIDE.md](EXECUTE_SQL_GUIDE.md) |
| `sqlx_example` | Uses SQLx over Kinetica's PostgreSQL wire protocol: a `KineticaPool`, `FromRow` structs and bound parameters (needs `--features sqlx`) |
| `sqlx_complex_types` | Inserts, selects, updates and deletes by `JSON`, arrays and `VECTOR` over SQLx, through `KineticaJson`, `KineticaArray` and `KineticaVector` (needs `--features sqlx`) |

[SQLX_GUIDE.md](SQLX_GUIDE.md) covers SQLx in more depth.

### Files

| Example | What it does |
|---------|--------------|
| `kifs_demo` | Uploads files with glob patterns, creates and lists KiFS directories, and downloads with progress callbacks, in multiple parts for large files |

### Cleaning Up

| Example | What it does |
|---------|--------------|
| `cleanup_all` | Drops the schemas and tables the examples create |

## Creating a Table, Step by Step

```bash
cargo run -p gpudb-examples --bin create_type    # 1. the type (schema)
cargo run -p gpudb-examples --bin has_type       # 2. check it exists
cargo run -p gpudb-examples --bin create_table   # 3. the table
cargo run -p gpudb-examples --bin has_table      # 4. check it exists
```

`complete_workflow` does all four, and more, in one run.

## Troubleshooting

| Error | Fix |
|-------|-----|
| "Type does not exist" | Run `create_type` first |
| "Table already exists" | Usually fine: the table was made by an earlier run |
| "Connection refused" | Check that Kinetica is running at `KINETICA_URL` |
| "Authentication failed" | Check `KINETICA_USER` and `KINETICA_PASSWORD` |

## More Documentation

- [The API's README](https://github.com/kineticadb/kinetica-api-rust/blob/master/README.md): how to use the API
- [EXECUTE_SQL_GUIDE.md](EXECUTE_SQL_GUIDE.md): SQL through `execute_sql`
- [SQLX_GUIDE.md](SQLX_GUIDE.md): SQLx over the PostgreSQL wire protocol
- [Kinetica documentation](https://docs.kinetica.com/)
