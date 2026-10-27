//! Cleanup utility to drop all schemas and tables used by tests and examples.
//!
//! Run with: cargo run -p gpudb-examples --bin cleanup_all

use gpudb_api::{Kinetica, ExecuteSqlRequest, KineticaError};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), KineticaError> {
    let (urls, options) = gpudb_examples::common::connection_config();
    let url = urls.first().cloned().unwrap_or_default();
    let kinetica = Arc::new(Kinetica::new(urls, options).await?);

    println!("Cleaning up all test/example schemas and tables from {url}...");
    println!("{}", "=".repeat(60));
    
    // All schemas to drop (CASCADE will drop all contained tables)
    let schemas = vec![
        // Example schemas
        "complete_workflow",
        "bulk_insert_mt_fast", 
        "bulk_insert_mt_json",
        "bulk_insert_mt_bin",
        "insert_and_get_records_json",
        "insert_and_get_records_bin",
        "all_types_test",
        "sharded_test",
        "create_table",
        "avro_benchmark",
        "backpressure_test",
        "bp_unbounded",
        "bp_bounded",
        // Test schemas
        "test_has_type",
        "test_has_table",
        "test_create_clear",
        "test_isolation_1",
        "test_isolation_2",
        "test_insert_json",
        "test_insert_update_pk",
        "test_insert_binary",
        "test_insert_large",
        "test_insert_empty",
        "test_get_all",
        "test_get_pagn",
        "test_get_filter",
        "test_get_sort",
        "test_get_limit0",
        "test_get_binary",
        "test_get_empty",
        "test_get_offset",
        "test_table_basic",
        "test_table_opts",
        "test_has_table_y",
        "test_has_table_n",
        "test_clear_table",
        "test_multi_tables",
        "test_agg_unique_basic",
        "test_agg_unique_num",
        "test_sql_create",
        "test_sql_select",
        "test_sql_filter",
        "test_sql_count",
        "test_sql_drop",
        "test_sql_limit",
        "test_type_basic",
        "test_type_complex",
        "test_type_string",
        "test_type_pk",
        "test_type_multi",
        "test_type_exists",
        "test_type_not",
        "test_agg_hist_basic",
        "test_agg_hist_empty",
        "test_agg_hist_single",
        "test_agg_hist_uneven",
        "test_agg_minmax",
        "test_agg_minmax_empty",
        "test_agg_minmax_single",
        "test_agg_minmax_double",
        "test_agg_stats",
        "test_agg_stats_all",
        "test_agg_stats_empty",
        "test_agg_groupby",
        "test_agg_groupby_empty",
        "test_agg_groupby_multi",
        "test_agg_groupby_aggs",
        "test_agg_groupby_limit",
        "test_context",
    ];
    
    // Standalone tables (not in a schema)
    let tables = vec![
        "employees",
        "employee_data",
        "load_gen_test",
    ];
    
    let mut dropped_schemas = 0;
    let mut dropped_tables = 0;
    
    // Drop schemas
    for schema in &schemas {
        let sql = format!("DROP SCHEMA IF EXISTS {schema} CASCADE");
        match kinetica.execute_sql_raw(&ExecuteSqlRequest {
            statement: sql,
            offset: 0,
            limit: -9999,
            encoding: "json".to_string(),
            ..Default::default()
        }).await {
            Ok(_) => {
                println!("  Dropped schema: {schema}");
                dropped_schemas += 1;
            }
            Err(e) => {
                // Ignore "does not exist" errors
                let err_str = format!("{e:?}");
                if !err_str.contains("does not exist") {
                    println!("  Warning dropping {schema}: {e:?}");
                }
            }
        }
    }
    
    // Drop standalone tables
    for table in &tables {
        let sql = format!("DROP TABLE IF EXISTS {table}");
        match kinetica.execute_sql_raw(&ExecuteSqlRequest {
            statement: sql,
            offset: 0,
            limit: -9999,
            encoding: "json".to_string(),
            ..Default::default()
        }).await {
            Ok(_) => {
                println!("  Dropped table: {table}");
                dropped_tables += 1;
            }
            Err(e) => {
                let err_str = format!("{e:?}");
                if !err_str.contains("does not exist") {
                    println!("  Warning dropping {table}: {e:?}");
                }
            }
        }
    }
    
    println!("{}", "=".repeat(60));
    println!("Cleanup complete: {dropped_schemas} schemas, {dropped_tables} tables processed");
    
    Ok(())
}
