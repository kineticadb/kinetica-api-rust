//! Demonstrates the AvroSchemaBuilder API for defining Kinetica record schemas
//!
//! This example shows how to use the builder pattern instead of manually writing
//! JSON schema strings. The builder approach is:
//! - Type-safe: Compiler catches typos in field types
//! - Easier to read and maintain
//! - Less error-prone than hand-writing JSON
//!
//! # Usage
//!
//! ```bash
//! cargo run -p gpudb-examples --bin schema_builder_demo
//! ```

use gpudb_api::{
    avro_schema, AvroSchemaBuilder, AvroType, KineticaColumnProperties, KineticaRecord,
    TypePropertiesBuilder,
};
use std::collections::HashMap;

// ============================================================================
// Example 1: Simple record using the builder pattern
// ============================================================================

#[derive(serde::Serialize, Clone, Default)]
struct SimpleRecord {
    id: i32,
    name: String,
    value: f64,
}

impl KineticaRecord for SimpleRecord {
    fn avro_schema(&self) -> &'static str {
        // Using the avro_schema! macro for efficient static allocation
        avro_schema!(
            AvroSchemaBuilder::new("SimpleRecord")
                .int_field("id")
                .string_field("name")
                .double_field("value")
        )
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .primary_key("id")
            .data("name")
            .data("value")
            .build()
    }
}

// ============================================================================
// Example 2: Complex record with nullable fields and various types
// ============================================================================

#[derive(serde::Serialize, Clone, Default)]
struct SensorReading {
    sensor_id: String,
    timestamp_ms: i64,
    temperature: Option<f64>,
    humidity: Option<f64>,
    #[serde(with = "serde_bytes")]
    raw_data: Vec<u8>,
    // A Kinetica boolean is an int: written as 1 or 0
    #[serde(with = "gpudb_api::avro_schema_builder::bool_as_int")]
    is_valid: bool,
}

impl KineticaRecord for SensorReading {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("SensorReading")
                .namespace("com.example.sensors")
                .doc("Sensor telemetry data")
                .string_field("sensor_id")
                .timestamp_field("timestamp_ms")
                .nullable_double_field("temperature")
                .nullable_double_field("humidity")
                .bytes_field("raw_data")
                .bool_field("is_valid")
        )
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .column(
                "sensor_id",
                KineticaColumnProperties::new().primary_key().char_n(64),
            )
            .timestamp("timestamp_ms")
            .data("temperature")
            .data("humidity")
            .data("raw_data")
            .column("is_valid", KineticaColumnProperties::new().boolean())
            .build()
    }
}

// ============================================================================
// Example 3: Using AvroType directly for custom type combinations
// ============================================================================

#[derive(serde::Serialize, Clone, Default)]
struct LogEntry {
    timestamp: i64,
    level: i32,
    message: String,
    context: Option<String>,
}

impl KineticaRecord for LogEntry {
    fn avro_schema(&self) -> &'static str {
        avro_schema!(
            AvroSchemaBuilder::new("LogEntry")
                .field("timestamp", AvroType::Long)
                .field("level", AvroType::Int)
                .field("message", AvroType::String)
                .field("context", AvroType::String.nullable()) // Using nullable() method
        )
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        TypePropertiesBuilder::new()
            .timestamp("timestamp")
            .column("level", KineticaColumnProperties::new().int8())
            .data("message")
            .column("context", KineticaColumnProperties::new().nullable())
            .build()
    }
}

// ============================================================================
// Main: Demonstrate schema generation
// ============================================================================

fn main() {
    println!("=== AvroSchemaBuilder Demo ===\n");

    // Example 1: Simple record
    println!("1. SimpleRecord schema:");
    let simple = SimpleRecord::default();
    println!("{}\n", simple.avro_schema());
    println!("   Type properties: {:?}\n", SimpleRecord::type_properties());

    // Example 2: Complex record with nullable fields
    println!("2. SensorReading schema:");
    let sensor = SensorReading::default();
    println!("{}\n", sensor.avro_schema());
    println!("   Type properties: {:?}\n", SensorReading::type_properties());

    // Example 3: LogEntry with custom AvroType
    println!("3. LogEntry schema:");
    let log = LogEntry::default();
    println!("{}\n", log.avro_schema());
    println!("   Type properties: {:?}\n", LogEntry::type_properties());

    // Example 4: Building schemas dynamically (not for KineticaRecord, but for inspection)
    println!("4. Dynamic schema building (for CreateTypeRequest):");
    let dynamic_schema = AvroSchemaBuilder::new("DynamicRecord")
        .long_field("id")
        .string_field("category")
        .nullable_bytes_field("payload")
        // An array column is a string with the array(...) property
        .string_field("tags")
        .build();
    println!("{dynamic_schema}\n");

    // Compare with hand-written JSON (for IngestRecord from load_generator.rs)
    println!("5. Comparison - Builder vs Hand-written:");
    println!("\n   Builder:");
    let builder_schema = AvroSchemaBuilder::new("IngestRecord")
        .long_field("timestampMillis")
        .int_field("source")
        .bytes_field("payload")
        .build();
    println!("   {builder_schema}");

    println!("\n   Hand-written:");
    let handwritten = r#"{"type":"record","name":"IngestRecord","fields":[{"name":"timestampMillis","type":"long"},{"name":"source","type":"int"},{"name":"payload","type":"bytes"}]}"#;
    println!("   {handwritten}");

    println!("\n   Match: {}", builder_schema == handwritten);

    println!("\n=== Demo Complete ===");
}
