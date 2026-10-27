# Kinetica Load Generator

A multi-threaded load generator for measuring Kinetica's bulk insert performance on your own system, through `BulkInserter`.

## Features

- Multi-threaded record generation with coordinated start
- Configurable payload size and randomization
- Progress reporting with throughput metrics
- CSV output mode for benchmark comparison
- Two schema options (basic and extended)
- Snappy compression support
- Multi-head ingest support

## Usage

```bash
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="test_load" \
TOTAL_RECORDS=1000000 \
NUM_THREADS=4 \
MESSAGE_SIZE=1024 \
cargo run -p gpudb-examples --bin load_generator --release
```

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `KINETICA_URL` | Kinetica server URL (e.g., `http://localhost:9191`) |
| `KINETICA_TABLE` | Target table name (will be created if not exists) |
| `TOTAL_RECORDS` | Total number of records to insert |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `KINETICA_USER` | `admin` | Authentication username |
| `KINETICA_PASSWORD` | `password` | Authentication password |
| `KINETICA_BATCH_SIZE` | `10000` | Records per batch sent to server |
| `KINETICA_MULTIHEAD` | `true` | Enable multi-head ingest (direct to workers) |
| `NUM_THREADS` | `1` | Number of producer threads |
| `NUM_WORKERS` | `16` | Number of flush workers |
| `MESSAGE_SIZE` | `1024` | Payload size in bytes |
| `PAYLOAD_RANDOM` | `false` | Randomize payload for each record |
| `BETTER_SCHEMA` | `false` | Use extended schema with extra fields |
| `SNAPPY` | `true` | Enable Snappy compression |
| `PROGRESS_INTERVAL_MS` | `1000` | Progress report interval in milliseconds |
| `LOG_EVERY_N` | `0` | Log every N records (0 = disabled) |
| `CSV_OUTPUT` | `true` | Output CSV summary line to stderr |
| `TRUNCATE_TABLE` | `false` | Truncate existing table before inserting (creates table if not exists) |

## Record Schemas

### Basic Schema (`BETTER_SCHEMA=false`)

```json
{
  "type": "record",
  "name": "IngestRecord",
  "fields": [
    {"name": "timestampMillis", "type": "long"},
    {"name": "source", "type": "int"},
    {"name": "payload", "type": "bytes"}
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `timestampMillis` | long | Record creation timestamp (ms since epoch) |
| `source` | int | Set to message size |
| `payload` | bytes | Binary payload of configurable size |

### Extended Schema (`BETTER_SCHEMA=true`)

```json
{
  "type": "record",
  "name": "IngestRecord2",
  "fields": [
    {"name": "timestampMillis", "type": "long"},
    {"name": "source", "type": "int"},
    {"name": "threadId", "type": "int"},
    {"name": "localCount", "type": "long"},
    {"name": "nanosecond", "type": "long"},
    {"name": "payload", "type": "bytes"}
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `timestampMillis` | long | Record creation timestamp (ms since epoch) |
| `source` | int | Set to message size |
| `threadId` | int | Producer thread ID |
| `localCount` | long | Thread-local record counter |
| `nanosecond` | long | High-precision timestamp (ns) |
| `payload` | bytes | Binary payload of configurable size |

## Output

### Progress Output

When `CSV_OUTPUT=false`, progress is reported periodically:

```
[PROGRESS] 2024-01-15 10:30:45 elapsed=5s total=500000 rate=100000.00 rec/s
```

### Summary Output

```
----------------------------------------------------
[SUMMARY] Start: 2024-01-15 10:30:40
[SUMMARY] End: 2024-01-15 10:30:50
[SUMMARY] Rust URL: http://localhost:9191
[SUMMARY] Total Records: 1000000
[SUMMARY] Batch Size: 10000
[SUMMARY] Payload Size: 1024
[SUMMARY] Number of Threads: 4
[SUMMARY] Elapsed Time: 10.25 sec
[SUMMARY] Average Rate: 97560.98 rec/s
----------------------------------------------------
Done.
```

### CSV Output

When `CSV_OUTPUT=true`, a CSV line is written to stderr for benchmarking:

```
start_ts,hostname,url,table,api,total_records,message_size,num_threads,batch_size,elapsed_sec,avg_rate
2024-01-15 10:30:40,myhost,http://localhost:9191,test_load,RustAPI,1000000,1024,4,10000,10.25,97560.98
```

## Examples

### Basic Benchmark

```bash
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="benchmark_test" \
TOTAL_RECORDS=1000000 \
cargo run -p gpudb-examples --bin load_generator --release
```

### High-Throughput Test

```bash
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="high_throughput_test" \
TOTAL_RECORDS=10000000 \
NUM_THREADS=8 \
NUM_WORKERS=32 \
KINETICA_BATCH_SIZE=50000 \
MESSAGE_SIZE=512 \
cargo run -p gpudb-examples --bin load_generator --release
```

### Extended Schema with Random Payloads

```bash
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="extended_test" \
TOTAL_RECORDS=1000000 \
BETTER_SCHEMA=true \
PAYLOAD_RANDOM=true \
MESSAGE_SIZE=2048 \
NUM_THREADS=4 \
cargo run -p gpudb-examples --bin load_generator --release
```

### Compression Comparison

```bash
# With Snappy compression
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="compression_test" \
TOTAL_RECORDS=1000000 \
SNAPPY=true \
MESSAGE_SIZE=4096 \
cargo run -p gpudb-examples --bin load_generator --release

# Without compression
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="compression_test" \
TOTAL_RECORDS=1000000 \
SNAPPY=false \
MESSAGE_SIZE=4096 \
cargo run -p gpudb-examples --bin load_generator --release
```

### Truncate and Insert

When `TRUNCATE_TABLE=true`, the load generator will:
1. Check if the table exists
2. Create the table if it doesn't exist
3. Truncate the table if it exists
4. Proceed with the insert

```bash
# Truncate existing table and insert 1M records
KINETICA_URL="http://localhost:9191" \
KINETICA_TABLE="test_load" \
TOTAL_RECORDS=1000000 \
TRUNCATE_TABLE=true \
cargo run -p gpudb-examples --bin load_generator --release
```

## Performance Tips

1. **Batch Size**: Use 10K-50K for optimal throughput
2. **Workers**: Match to CPU cores (16-32 for high-core systems)
3. **Threads**: Use 4-8 producer threads
4. **Compression**: Enable Snappy for large payloads or slow networks
5. **Multi-head**: Keep enabled for distributed clusters
6. **Payload**: Smaller payloads = higher record throughput
