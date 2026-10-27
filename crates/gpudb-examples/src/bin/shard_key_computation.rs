//! Example: Shard Key Computation for All Data Types
//!
//! This example demonstrates how RecordKey and RecordKeyBuilder compute routing keys
//! for ALL 18 data types supported by Kinetica's shard key mechanism:
//!
//! Primitive Types:
//!   - int (i32) - 4 bytes, little-endian
//!   - long (i64) - 8 bytes, little-endian
//!   - float (f32) - 4 bytes, little-endian
//!   - double (f64) - 8 bytes, little-endian
//!   - boolean - 1 byte (0 or 1)
//!   - int8 (i8) - 1 byte, signed
//!   - int16 (i16) - 2 bytes, little-endian
//!
//! String Types:
//!   - string - 8 bytes (murmur3 hash of UTF-8 bytes)
//!   - char(N) - N bytes (reversed byte packing)
//!
//! Date/Time Types:
//!   - date (YYYY-MM-DD) - 4 bytes (packed encoding)
//!   - datetime (YYYY-MM-DD HH:MM:SS.mmm) - 8 bytes (packed encoding)
//!   - time (HH:MM:SS.mmm) - 4 bytes (packed encoding)
//!   - timestamp - 8 bytes (milliseconds since epoch, packed encoding)
//!
//! Special Types:
//!   - ipv4 (dotted quad notation) - 4 bytes (packed)
//!   - uuid (8-4-4-4-12 hex format) - 16 bytes (special little-endian packing)
//!   - ulong (unsigned 64-bit) - 8 bytes, little-endian
//!   - decimal (precision <= 18) - 8 bytes (scaled i64)
//!   - decimal (precision > 18) - 12 bytes (scaled BigInt)
//!
//! The example shows:
//!   1. How each data type is encoded into the routing key buffer
//!   2. How the final murmur3_x64_128 hash is computed
//!   3. How records are distributed across shards
//!
//! Run with: cargo run --release --bin shard_key_computation

use gpudb_api::{
    record_key::RecordKey,
    record_key_builder::{RecordKeyBuilder, KineticaType, Column},
};
use std::collections::HashMap;

/// Sample record with all 18 supported shard key types
#[derive(Clone, Debug)]
struct AllTypesRecord {
    // Primitive types
    pub id: i32,
    pub long_val: i64,
    pub float_val: f32,
    pub double_val: f64,
    pub bool_val: bool,
    pub int8_val: i8,
    pub int16_val: i16,
    // String types
    pub string_val: String,
    pub char_val: String,
    // Date/Time types
    pub date_val: String,
    pub datetime_val: String,
    pub time_val: String,
    pub timestamp_val: i64,
    // Special types
    pub ipv4_val: String,
    pub uuid_val: String,
    pub ulong_val: String,
    pub decimal_small: String,  // precision <= 18
    pub decimal_large: String,  // precision > 18
}

impl AllTypesRecord {
    fn to_routing_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        // Primitive types
        map.insert("id".to_string(), self.id.to_string());
        map.insert("long_val".to_string(), self.long_val.to_string());
        map.insert("float_val".to_string(), self.float_val.to_string());
        map.insert("double_val".to_string(), self.double_val.to_string());
        map.insert("bool_val".to_string(), self.bool_val.to_string());
        map.insert("int8_val".to_string(), self.int8_val.to_string());
        map.insert("int16_val".to_string(), self.int16_val.to_string());
        // String types
        map.insert("string_val".to_string(), self.string_val.clone());
        map.insert("char_val".to_string(), self.char_val.clone());
        // Date/Time types
        map.insert("date_val".to_string(), self.date_val.clone());
        map.insert("datetime_val".to_string(), self.datetime_val.clone());
        map.insert("time_val".to_string(), self.time_val.clone());
        map.insert("timestamp_val".to_string(), self.timestamp_val.to_string());
        // Special types
        map.insert("ipv4_val".to_string(), self.ipv4_val.clone());
        map.insert("uuid_val".to_string(), self.uuid_val.clone());
        map.insert("ulong_val".to_string(), self.ulong_val.clone());
        map.insert("decimal_small".to_string(), self.decimal_small.clone());
        map.insert("decimal_large".to_string(), self.decimal_large.clone());
        map
    }
}

/// Create the KineticaType definition with all 18 types as shard keys
fn create_all_types_schema() -> KineticaType {
    KineticaType {
        columns: vec![
            // Primitive types - using Kinetica-specific data_type values
            Column { name: "id".to_string(), data_type: "int".to_string(),
                     properties: vec!["primary_key".to_string(), "shard_key".to_string()] },
            Column { name: "long_val".to_string(), data_type: "long".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "float_val".to_string(), data_type: "float".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "double_val".to_string(), data_type: "double".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "bool_val".to_string(), data_type: "boolean".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "int8_val".to_string(), data_type: "int8".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "int16_val".to_string(), data_type: "int16".to_string(),
                     properties: vec!["shard_key".to_string()] },
            // String types
            Column { name: "string_val".to_string(), data_type: "string".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "char_val".to_string(), data_type: "string".to_string(),
                     properties: vec!["char16".to_string(), "shard_key".to_string()] },
            // Date/Time types
            Column { name: "date_val".to_string(), data_type: "date".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "datetime_val".to_string(), data_type: "datetime".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "time_val".to_string(), data_type: "time".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "timestamp_val".to_string(), data_type: "timestamp".to_string(),
                     properties: vec!["shard_key".to_string()] },
            // Special types
            Column { name: "ipv4_val".to_string(), data_type: "ipv4".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "uuid_val".to_string(), data_type: "uuid".to_string(),
                     properties: vec!["shard_key".to_string()] },
            Column { name: "ulong_val".to_string(), data_type: "ulong".to_string(),
                     properties: vec!["shard_key".to_string()] },
            // Decimal with precision <= 18 (8 bytes)
            Column { name: "decimal_small".to_string(), data_type: "decimal".to_string(),
                     properties: vec!["decimal(18,4)".to_string(), "shard_key".to_string()] },
            // Decimal with precision > 18 (12 bytes)
            Column { name: "decimal_large".to_string(), data_type: "decimal".to_string(),
                     properties: vec!["decimal(38,10)".to_string(), "shard_key".to_string()] },
        ],
    }
}

/// Generate sample records for testing
fn generate_sample_records(count: usize) -> Vec<AllTypesRecord> {
    (0..count).map(|idx| {
        let date_day = 1 + (idx % 28);
        let date_month = 1 + (idx % 12);
        let date_year = 2020 + (idx % 5);

        let ip_octet1 = 10 + (idx % 240);
        let ip_octet2 = (idx / 240) % 256;
        let ip_octet3 = (idx / 61440) % 256;
        let ip_octet4 = idx % 256;

        let hour = idx % 24;
        let minute = idx % 60;
        let second = (idx * 7) % 60;
        let millis = (idx * 13) % 1000;

        AllTypesRecord {
            // Primitive types
            id: idx as i32,
            long_val: (idx as i64) * 1_000_000,
            float_val: (idx as f32) * 0.5,
            double_val: (idx as f64) * 0.123456789,
            bool_val: idx % 2 == 0,
            int8_val: ((idx % 256) as i32 - 128) as i8,
            int16_val: ((idx % 65536) as i32 - 32768) as i16,
            // String types
            string_val: format!("str_{idx}"),
            char_val: format!("c{idx:06}"),
            // Date/Time types
            date_val: format!("{date_year:04}-{date_month:02}-{date_day:02}"),
            datetime_val: format!("{date_year:04}-{date_month:02}-{date_day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}"),
            time_val: format!("{hour:02}:{minute:02}:{second:02}.{millis:03}"),
            timestamp_val: 1700000000000_i64 + (idx as i64 * 1000),  // Timestamps from Nov 2023
            // Special types
            ipv4_val: format!("{ip_octet1}.{ip_octet2}.{ip_octet3}.{ip_octet4}"),
            uuid_val: format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                idx as u32, (idx % 65536) as u16, ((idx * 7) % 65536) as u16,
                ((idx * 13) % 65536) as u16, (idx as u64 * 123456789) % 0xFFFFFFFFFFFF),
            ulong_val: format!("{}", (idx as u64) * 1_000_000_000_000),
            decimal_small: format!("{}.{:04}", idx % 10000, (idx * 7) % 10000),
            decimal_large: format!("{}.{:010}",
                (idx as u64) * 1_000_000_000 % 10_000_000_000_000_000,
                (idx as u64 * 123456789) % 10_000_000_000),
        }
    }).collect()
}

fn main() {
    println!("==============================================");
    println!(" Shard Key Computation - All Data Types");
    println!("==============================================\n");

    // Create schema and builder
    let ktype = create_all_types_schema();
    let builder = RecordKeyBuilder::new(&ktype).expect("every column in the schema can be a key");

    // Display configuration
    println!("=== RecordKeyBuilder Configuration ===\n");
    println!("Number of shard columns: {}", builder.shard_indices.len());
    println!("Shard column names: {:?}", builder.shard_indices.iter()
        .map(|&i| &ktype.columns[i].name)
        .collect::<Vec<_>>());
    println!("Shard column types: {:?}", builder.shard_types);
    println!("Total buffer size: {} bytes", builder.buffer_size);
    println!();

    // Explain buffer size breakdown
    println!("=== Buffer Size Breakdown ===\n");
    println!("  Primitive Types:");
    println!("    int (id):              4 bytes (little-endian)");
    println!("    long (long_val):       8 bytes (little-endian)");
    println!("    float (float_val):     4 bytes (IEEE 754 LE)");
    println!("    double (double_val):   8 bytes (IEEE 754 LE)");
    println!("    boolean (bool_val):    1 byte  (0 or 1)");
    println!("    int8 (int8_val):       1 byte  (signed)");
    println!("    int16 (int16_val):     2 bytes (little-endian)");
    println!();
    println!("  String Types:");
    println!("    string (string_val):   8 bytes (murmur3 hash)");
    println!("    char16 (char_val):    16 bytes (reversed packing)");
    println!();
    println!("  Date/Time Types:");
    println!("    date (date_val):       4 bytes (packed encoding)");
    println!("    datetime (datetime):   8 bytes (packed y/m/d/h/m/s/ms)");
    println!("    time (time_val):       4 bytes (packed h/m/s/ms)");
    println!("    timestamp:             8 bytes (packed from millis)");
    println!();
    println!("  Special Types:");
    println!("    ipv4 (ipv4_val):       4 bytes (packed octets)");
    println!("    uuid (uuid_val):      16 bytes (special LE packing)");
    println!("    ulong (ulong_val):     8 bytes (little-endian)");
    println!("    decimal(18,4):         8 bytes (scaled i64)");
    println!("    decimal(38,10):       12 bytes (scaled BigInt)");
    println!("  ---------------------------------");
    println!("  Total:                 {} bytes", builder.buffer_size);
    println!();

    // Generate sample records
    let records = generate_sample_records(1000);

    // Demonstrate computation for first 5 records
    println!("=== Sample Record Key Computations ===\n");

    for (i, record) in records.iter().take(3).enumerate() {
        let routing_map = record.to_routing_map();
        if let Some(key) = builder.build_from_map(&routing_map) {
            println!("Record {i}:");
            println!("  Input values:");
            println!("    Primitives: id={}, long={}, float={:.2}, double={:.6}",
                     record.id, record.long_val, record.float_val, record.double_val);
            println!("    Primitives: bool={}, int8={}, int16={}",
                     record.bool_val, record.int8_val, record.int16_val);
            println!("    Strings:    string='{}', char='{}'",
                     record.string_val, record.char_val);
            println!("    Date/Time:  date='{}', time='{}'",
                     record.date_val, record.time_val);
            println!("    Date/Time:  datetime='{}', timestamp={}",
                     record.datetime_val, record.timestamp_val);
            println!("    Special:    ipv4='{}', uuid='{}'",
                     record.ipv4_val, record.uuid_val);
            println!("    Special:    ulong='{}', decimal_sm='{}', decimal_lg='{}'",
                     record.ulong_val, record.decimal_small, record.decimal_large);
            println!();
            println!("  Output:");
            println!("    Routing hash: 0x{:016X}", key.routing_hash());
            println!("    Buffer ({} bytes):", key.bytes().len());

            // Show buffer in chunks for readability
            for (chunk_idx, chunk) in key.bytes().chunks(16).enumerate() {
                let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02X}")).collect();
                println!("      {:3}: {}", chunk_idx * 16, hex.join(" "));
            }
            println!("    Is valid: {}", key.is_valid());
            println!();
        }
    }

    // Test routing distribution
    println!("=== Routing Distribution Test ===\n");

    let shard_counts = [4, 8, 16, 32];

    for num_shards in shard_counts {
        let routing_table: Vec<i32> = (1..=num_shards).collect();
        let mut distribution: HashMap<usize, usize> = HashMap::new();

        for record in &records {
            let routing_map = record.to_routing_map();
            if let Some(key) = builder.build_from_map(&routing_map) {
                let shard = key.route(&routing_table);
                *distribution.entry(shard).or_insert(0) += 1;
            }
        }

        println!("Distribution across {} shards ({} records):", num_shards, records.len());

        let expected = records.len() / num_shards as usize;
        let mut min_count = usize::MAX;
        let mut max_count = 0;

        for shard in 0..num_shards as usize {
            let count = *distribution.get(&shard).unwrap_or(&0);
            min_count = min_count.min(count);
            max_count = max_count.max(count);
        }

        let variance = ((max_count - min_count) as f64 / expected as f64) * 100.0;

        println!("  Expected per shard: ~{expected}");
        println!("  Actual range: {min_count} - {max_count} (variance: {variance:.1}%)");

        // Show distribution bar chart for small shard counts
        if num_shards <= 8 {
            for shard in 0..num_shards as usize {
                let count = *distribution.get(&shard).unwrap_or(&0);
                let pct = (count as f64 / records.len() as f64) * 100.0;
                let bar_len = (pct / 2.0) as usize;
                println!("    Shard {}: {:>4} ({:>5.2}%) {}",
                         shard, count, pct, "#".repeat(bar_len));
            }
        }
        println!();
    }

    // Demonstrate individual type encoding
    println!("=== Individual Type Encoding Examples ===\n");

    // Test each type individually
    println!("--- Primitive Types ---\n");
    demonstrate_int_encoding();
    demonstrate_long_encoding();
    demonstrate_float_encoding();
    demonstrate_double_encoding();
    demonstrate_boolean_encoding();
    demonstrate_int8_encoding();
    demonstrate_int16_encoding();

    println!("--- String Types ---\n");
    demonstrate_string_encoding();
    demonstrate_char_encoding();

    println!("--- Date/Time Types ---\n");
    demonstrate_date_encoding();
    demonstrate_datetime_encoding();
    demonstrate_time_encoding();
    demonstrate_timestamp_encoding();

    println!("--- Special Types ---\n");
    demonstrate_ipv4_encoding();
    demonstrate_uuid_encoding();
    demonstrate_ulong_encoding();
    demonstrate_decimal_encoding();

    println!("=== Summary ===\n");
    println!("All shard key bytes are concatenated in column order and then");
    println!("hashed using murmur3_x64_128 (seed=10) to produce the routing hash.");
    println!("The routing hash is used to determine which shard receives the record:");
    println!("  shard_index = routing_table[(hash % table_len).abs()] - 1");
}

fn demonstrate_int_encoding() {
    println!("INT encoding (4 bytes, little-endian):");
    let mut key = RecordKey::new(4);
    key.push_i32(12345);
    key.compute_hashes();
    println!("  Value: 12345");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_long_encoding() {
    println!("LONG encoding (8 bytes, little-endian):");
    let mut key = RecordKey::new(8);
    key.push_i64(1234567890123);
    key.compute_hashes();
    println!("  Value: 1234567890123");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_float_encoding() {
    println!("FLOAT encoding (4 bytes, IEEE 754 little-endian):");
    let mut key = RecordKey::new(4);
    let pi_f32 = std::f32::consts::PI;
    key.push_f32(pi_f32);
    key.compute_hashes();
    println!("  Value: {pi_f32}");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_double_encoding() {
    println!("DOUBLE encoding (8 bytes, IEEE 754 little-endian):");
    let mut key = RecordKey::new(8);
    let pi_f64 = std::f64::consts::PI;
    key.push_f64(pi_f64);
    key.compute_hashes();
    println!("  Value: {pi_f64}");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_string_encoding() {
    println!("STRING encoding (8 bytes, murmur3 hash of UTF-8):");
    let mut key = RecordKey::new(8);
    key.push_string("hello world");
    key.compute_hashes();
    println!("  Value: \"hello world\"");
    println!("  Bytes: {:02X?} (murmur3 hash of string)", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_char_encoding() {
    println!("CHAR(16) encoding (16 bytes, reversed packing):");
    let mut key = RecordKey::new(16);
    key.push_char("ABCDEF", 16);
    key.compute_hashes();
    println!("  Value: \"ABCDEF\" (padded to 16 chars)");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Note:  Bytes are reversed - 'F'(46) 'E'(45) 'D'(44) 'C'(43) 'B'(42) 'A'(41)");
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_date_encoding() {
    println!("DATE encoding (4 bytes, packed):");
    let mut key = RecordKey::new(4);
    key.push_date("2024-06-15");
    key.compute_hashes();
    println!("  Value: \"2024-06-15\"");
    println!("  Packed: (year-1900)<<21 | month<<17 | day<<12 | ordinal<<3 | weekday");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_ipv4_encoding() {
    println!("IPV4 encoding (4 bytes, packed octets):");
    let mut key = RecordKey::new(4);
    key.push_ipv4("192.168.1.100");
    key.compute_hashes();
    println!("  Value: \"192.168.1.100\"");
    println!("  Packed: oct1<<24 | oct2<<16 | oct3<<8 | oct4");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_boolean_encoding() {
    println!("BOOLEAN encoding (1 byte):");
    let mut key_true = RecordKey::new(1);
    key_true.push_bool(true);
    key_true.compute_hashes();
    println!("  Value: true");
    println!("  Bytes: {:02X?}", key_true.bytes());
    println!("  Hash:  0x{:016X}", key_true.routing_hash());

    let mut key_false = RecordKey::new(1);
    key_false.push_bool(false);
    key_false.compute_hashes();
    println!("  Value: false");
    println!("  Bytes: {:02X?}", key_false.bytes());
    println!("  Hash:  0x{:016X}\n", key_false.routing_hash());
}

fn demonstrate_int8_encoding() {
    println!("INT8 encoding (1 byte, signed):");
    let mut key = RecordKey::new(1);
    key.push_i8(-42);
    key.compute_hashes();
    println!("  Value: -42");
    println!("  Bytes: {:02X?} (two's complement)", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_int16_encoding() {
    println!("INT16 encoding (2 bytes, little-endian):");
    let mut key = RecordKey::new(2);
    key.push_i16(12345);
    key.compute_hashes();
    println!("  Value: 12345");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_datetime_encoding() {
    println!("DATETIME encoding (8 bytes, packed):");
    let mut key = RecordKey::new(8);
    key.push_datetime("2024-06-15 14:30:45.123");
    key.compute_hashes();
    println!("  Value: \"2024-06-15 14:30:45.123\"");
    println!("  Packed: (y-1900)<<53|m<<49|d<<44|h<<39|min<<33|s<<27|ms<<17|ord<<8|dow<<5");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_time_encoding() {
    println!("TIME encoding (4 bytes, packed):");
    let mut key = RecordKey::new(4);
    key.push_time("14:30:45.123");
    key.compute_hashes();
    println!("  Value: \"14:30:45.123\"");
    println!("  Packed: hour<<26 | min<<20 | sec<<14 | ms<<4");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_timestamp_encoding() {
    println!("TIMESTAMP encoding (8 bytes, packed from millis):");
    let mut key = RecordKey::new(8);
    key.push_timestamp(1718456645123);  // 2024-06-15 14:30:45.123 UTC
    key.compute_hashes();
    println!("  Value: 1718456645123 (ms since epoch)");
    println!("  Packed: Same as datetime encoding");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_uuid_encoding() {
    println!("UUID encoding (16 bytes, special little-endian packing):");
    let mut key = RecordKey::new(16);
    key.push_uuid("550e8400-e29b-41d4-a716-446655440000");
    key.compute_hashes();
    println!("  Value: \"550e8400-e29b-41d4-a716-446655440000\"");
    println!("  Bytes 0-3:   last 8 hex of group5 (reversed)");
    println!("  Bytes 4-5:   first 4 hex of group5 (reversed)");
    println!("  Bytes 6-7:   group4 (reversed)");
    println!("  Bytes 8-9:   group3 (reversed)");
    println!("  Bytes 10-11: group2 (reversed)");
    println!("  Bytes 12-15: group1 (reversed)");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_ulong_encoding() {
    println!("ULONG encoding (8 bytes, little-endian):");
    let mut key = RecordKey::new(8);
    key.push_ulong("9223372036854775808").expect("a valid unsigned long"); // 2^63
    key.compute_hashes();
    println!("  Value: \"9223372036854775808\" (2^63)");
    println!("  Bytes: {:02X?} (little-endian, like every number in the key)", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}

fn demonstrate_decimal_encoding() {
    println!("DECIMAL(18,4) encoding (8 bytes, scaled i64):");
    let mut key = RecordKey::new(8);
    key.push_decimal("12345.6789", 18, 4);
    key.compute_hashes();
    println!("  Value: \"12345.6789\"");
    println!("  Scaled: 12345.6789 * 10^4 = 123456789 as i64");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());

    println!("DECIMAL(38,10) encoding (12 bytes, scaled BigInt):");
    let mut key = RecordKey::new(12);
    key.push_decimal("12345678901234567890.1234567890", 38, 10);
    key.compute_hashes();
    println!("  Value: \"12345678901234567890.1234567890\"");
    println!("  Scaled: value * 10^10 as BigInt (little-endian)");
    println!("  Bytes: {:02X?}", key.bytes());
    println!("  Hash:  0x{:016X}\n", key.routing_hash());
}
