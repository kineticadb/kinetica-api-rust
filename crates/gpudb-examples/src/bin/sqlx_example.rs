//! SQLx Example
//!
//! Demonstrates using SQLx with Kinetica's PostgreSQL wire protocol.
//!
//! Run with:
//! ```bash
//! # Requires the sqlx feature
//! KINETICA_HOST="localhost" KINETICA_USER="admin" KINETICA_PASSWORD="password" \
//!     cargo run --bin sqlx_example --features sqlx
//! ```
//!
//! Prerequisites:
//! - Kinetica server running with PostgreSQL wire protocol enabled (port 5432)
//!
//! Kinetica reads every parameter after a statement's first as text, so each
//! parameter here is bound through `KineticaText`, or another of the wrappers
//! for the Kinetica types the protocol does not carry as PostgreSQL does. See
//! `crates/gpudb-api/SQLX_API.md`.

#[cfg(feature = "sqlx")]
mod example {
    use gpudb_api::sqlx_support::{
        KineticaArray, KineticaPoolExt, KineticaPoolOptions, KineticaText, KineticaTimestamp, KineticaVector,
    };
    use sqlx::{FromRow, Row};
    use std::time::Duration;

    /// Product struct for type-safe queries. Kinetica sends INT columns as
    /// PostgreSQL's INT4, so they read as `i32`.
    #[derive(FromRow, Debug)]
    pub struct Product {
        pub id: i32,
        pub name: String,
        pub category: String,
        pub price: f64,
        pub stock: i32,
    }

    /// Category summary for aggregations
    #[derive(FromRow, Debug)]
    pub struct CategorySummary {
        pub category: String,
        pub product_count: i64,
        pub total_stock: i64,
        pub avg_price: f64,
    }

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        println!("=== SQLx Example ===\n");

        // Get connection info from environment
        let host = std::env::var("KINETICA_HOST").unwrap_or_else(|_| "localhost".to_string());
        let username = std::env::var("KINETICA_USER").unwrap_or_else(|_| "admin".to_string());
        let password = std::env::var("KINETICA_PASSWORD").unwrap_or_else(|_| "password".to_string());

        // Create connection pool with options
        println!("Connecting to Kinetica via PostgreSQL wire protocol...");
        let pool = KineticaPoolOptions::new()
            .host(&host)
            .port(5432)
            .username(&username)
            .password(&password)
            .database("postgres")
            .max_connections(10)
            .min_connections(2)
            .acquire_timeout(Duration::from_secs(30))
            .connect()
            .await?;

        println!("Connected successfully!\n");

        // Unique schema for this example
        let schema = "sqlx_example";
        let table = format!("{}.products", schema);

        // =====================================================================
        // Setup
        // =====================================================================
        println!("Setting up test data...");

        // Drop schema if exists
        let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", schema))
            .execute(&pool)
            .await;

        // Create schema
        sqlx::query(&format!("CREATE SCHEMA {}", schema))
            .execute(&pool)
            .await?;

        // Create table
        sqlx::query(&format!(
            "CREATE TABLE {} (
                id INT NOT NULL,
                name VARCHAR(100),
                category VARCHAR(50),
                price DOUBLE,
                stock INT,
                PRIMARY KEY (id)
            )",
            table
        ))
        .execute(&pool)
        .await?;

        // =====================================================================
        // INSERT
        // =====================================================================
        println!("\n--- INSERT ---\n");

        let products = vec![
            (1, "Laptop Pro", "Electronics", 1299.99, 25),
            (2, "Wireless Mouse", "Electronics", 49.99, 150),
            (3, "USB-C Hub", "Electronics", 79.99, 75),
            (4, "Office Desk", "Furniture", 399.99, 20),
            (5, "Ergonomic Chair", "Furniture", 599.99, 15),
            (6, "Notebook Set", "Office Supplies", 12.99, 500),
            (7, "Pen Pack", "Office Supplies", 8.99, 1000),
        ];

        for (id, name, category, price, stock) in &products {
            sqlx::query(&format!(
                "INSERT INTO {} (id, name, category, price, stock) VALUES ($1, $2, $3, $4, $5)",
                table
            ))
            .bind(KineticaText(*id))
            .bind(KineticaText(*name))
            .bind(KineticaText(*category))
            .bind(KineticaText(*price))
            .bind(KineticaText(*stock))
            .execute(&pool)
            .await?;
        }
        println!("Inserted {} products", products.len());

        // =====================================================================
        // SELECT with FromRow
        // =====================================================================
        println!("\n--- SELECT with Type-Safe Structs ---\n");

        // Fetch all products as typed structs
        let all_products: Vec<Product> = sqlx::query_as(&format!(
            "SELECT id, name, category, price, stock FROM {} ORDER BY id",
            table
        ))
        .fetch_all(&pool)
        .await?;

        println!("All products:");
        for p in &all_products {
            println!("  #{}: {} ({}) - ${:.2}, stock: {}",
                p.id, p.name, p.category, p.price, p.stock);
        }

        // =====================================================================
        // Parameterized Queries
        // =====================================================================
        println!("\n--- Parameterized Queries ---\n");

        // Find products by category
        let category = "Electronics";
        let electronics: Vec<Product> = sqlx::query_as(&format!(
            "SELECT id, name, category, price, stock FROM {} WHERE category = $1 ORDER BY price DESC",
            table
        ))
        .bind(KineticaText(category))
        .fetch_all(&pool)
        .await?;

        println!("Electronics (sorted by price DESC):");
        for p in &electronics {
            println!("  {} - ${:.2}", p.name, p.price);
        }

        // Find products in price range
        let min_price = 50.0;
        let max_price = 500.0;
        let in_range: Vec<Product> = sqlx::query_as(&format!(
            "SELECT id, name, category, price, stock FROM {} WHERE price BETWEEN $1 AND $2",
            table
        ))
        .bind(KineticaText(min_price))
        .bind(KineticaText(max_price))
        .fetch_all(&pool)
        .await?;

        println!("\nProducts between ${:.2} and ${:.2}:", min_price, max_price);
        for p in &in_range {
            println!("  {} - ${:.2}", p.name, p.price);
        }

        // =====================================================================
        // Aggregations
        // =====================================================================
        println!("\n--- Aggregations ---\n");

        // Simple count
        let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", table))
            .fetch_one(&pool)
            .await?;
        println!("Total products: {}", count.0);

        // Sum and average
        let stats: (f64, f64, f64) = sqlx::query_as(&format!(
            "SELECT SUM(price), AVG(price), MAX(price) FROM {}",
            table
        ))
        .fetch_one(&pool)
        .await?;
        println!("Price stats: sum=${:.2}, avg=${:.2}, max=${:.2}", stats.0, stats.1, stats.2);

        // Group by with custom struct
        let summaries: Vec<CategorySummary> = sqlx::query_as(&format!(
            "SELECT category,
                    COUNT(*) as product_count,
                    SUM(stock) as total_stock,
                    AVG(price) as avg_price
             FROM {}
             GROUP BY category
             ORDER BY product_count DESC",
            table
        ))
        .fetch_all(&pool)
        .await?;

        println!("\nCategory summaries:");
        for s in &summaries {
            println!("  {}: {} products, {} units, avg ${:.2}",
                s.category, s.product_count, s.total_stock, s.avg_price);
        }

        // =====================================================================
        // UPDATE
        // =====================================================================
        println!("\n--- UPDATE ---\n");

        // Apply discount to Office Supplies
        let result = sqlx::query(&format!(
            "UPDATE {} SET price = price * 0.9 WHERE category = $1",
            table
        ))
        .bind(KineticaText("Office Supplies"))
        .execute(&pool)
        .await?;
        println!("Applied 10% discount to {} Office Supplies", result.rows_affected());

        // Restock low-stock items
        let result = sqlx::query(&format!(
            "UPDATE {} SET stock = stock + $1 WHERE stock < $2",
            table
        ))
        .bind(KineticaText(50))
        .bind(KineticaText(30))
        .execute(&pool)
        .await?;
        println!("Restocked {} low-stock items", result.rows_affected());

        // =====================================================================
        // Row-by-Row Processing
        // =====================================================================
        println!("\n--- Row-by-Row Processing ---\n");

        // Using fetch() for streaming large results
        use futures::TryStreamExt;

        let query_str = format!(
            "SELECT id, name, price FROM {} ORDER BY id",
            table
        );
        let mut rows = sqlx::query(&query_str).fetch(&pool);

        println!("Streaming results:");
        while let Some(row) = rows.try_next().await? {
            let id: i32 = row.get("id");
            let name: String = row.get("name");
            let price: f64 = row.get("price");
            println!("  #{}: {} - ${:.2}", id, name, price);
        }

        // =====================================================================
        // Extension Trait Methods
        // =====================================================================
        println!("\n--- Extension Trait Methods ---\n");

        // Count with condition
        let furniture_count = pool.count_rows(&table, Some("category = 'Furniture'")).await?;
        println!("Furniture products: {}", furniture_count);

        // Execute raw SQL
        let rows_affected = pool.execute_sql(&format!(
            "UPDATE {} SET stock = stock - 1 WHERE id = 1",
            table
        )).await?;
        println!("Decremented stock: {} row affected", rows_affected);

        // =====================================================================
        // Transactions
        // =====================================================================
        println!("\n--- Transactions ---\n");

        // Kinetica's PostgreSQL protocol has no transactions: each statement
        // applies when it runs
        match pool.begin().await {
            Ok(_) => println!("A transaction began"),
            Err(e) => println!("No transactions over this protocol: {e}"),
        }

        // =====================================================================
        // Kinetica Types
        // =====================================================================
        println!("\n--- Kinetica Types ---\n");

        let events = format!("{schema}.events");
        sqlx::query(&format!(
            "CREATE TABLE {events} (id INT, at TIMESTAMP, tags VARCHAR[], embedding VECTOR(3))"
        ))
        .execute(&pool)
        .await?;

        let at = KineticaTimestamp(chrono::Utc::now());
        sqlx::query(&format!("INSERT INTO {events} VALUES ($1, $2, $3, $4)"))
            .bind(KineticaText(1))
            .bind(at)
            .bind(KineticaArray(vec![Some("new".to_string()), None]))
            .bind(KineticaVector(vec![0.5, 1.5, 2.5]))
            .execute(&pool)
            .await?;

        let row = sqlx::query(&format!("SELECT at, tags, embedding FROM {events} WHERE id = $1"))
            .bind(KineticaText(1))
            .fetch_one(&pool)
            .await?;
        let read_at: KineticaTimestamp = row.get("at");
        let tags: KineticaArray<Option<String>> = row.get("tags");
        let embedding: KineticaVector = row.get("embedding");
        println!("Timestamp: {} (as written: {})", read_at.0, read_at.millis() == at.millis());
        println!("Tags: {:?}", tags.0);
        println!("Embedding: {:?}", embedding.0);

        // =====================================================================
        // DELETE
        // =====================================================================
        println!("\n--- DELETE ---\n");

        // Delete products with high stock
        let result = sqlx::query(&format!(
            "DELETE FROM {} WHERE stock > $1",
            table
        ))
        .bind(KineticaText(500))
        .execute(&pool)
        .await?;
        println!("Deleted {} products with stock > 500", result.rows_affected());

        // Final count
        let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", table))
            .fetch_one(&pool)
            .await?;
        println!("Remaining products: {}", count.0);

        // =====================================================================
        // Cleanup
        // =====================================================================
        println!("\n--- Cleanup ---\n");

        sqlx::query(&format!("DROP SCHEMA {} CASCADE", schema))
            .execute(&pool)
            .await?;
        println!("Dropped test schema");

        // Close pool gracefully
        pool.close().await;
        println!("Connection pool closed");

        println!("\n=== Example Complete ===");
        Ok(())
    }
}

#[cfg(feature = "sqlx")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    example::run().await
}

#[cfg(not(feature = "sqlx"))]
fn main() {
    eprintln!("This example requires the 'sqlx' feature.");
    eprintln!("Run with: cargo run --bin sqlx_example --features sqlx");
    std::process::exit(1);
}
