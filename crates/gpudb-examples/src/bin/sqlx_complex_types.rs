//! SQLx with JSON, Arrays and Vectors
//!
//! Inserts, selects, updates and deletes rows holding a `JSON` document,
//! `VARCHAR[]` and `DOUBLE[]` arrays and a `VECTOR`, over Kinetica's
//! PostgreSQL wire protocol.
//!
//! Run with:
//! ```bash
//! KINETICA_HOST="localhost" KINETICA_USER="admin" KINETICA_PASSWORD="password" \
//!     cargo run -p gpudb-examples --bin sqlx_complex_types --features sqlx
//! ```
//!
//! Each type is bound through its wrapper:
//! - `KineticaJson` binds a document as `JSON`. SQLx's own JSON types bind
//!   `JSONB`, which Kinetica refuses.
//! - `KineticaArray` binds and reads arrays.
//! - `KineticaVector` binds a vector in a plain `INSERT`. Everywhere else a
//!   vector parameter is text: `KineticaText(vector.as_slice())`.
//!
//! On a server with text search enabled (`enable_text_search = true` in
//! gpudb.conf), it also searches a `JSON(text_search)` column by key.
//!
//! See `crates/gpudb-api/SQLX_API.md`.

#[cfg(feature = "sqlx")]
mod example {
    use gpudb_api::sqlx_support::{
        KineticaArray, KineticaJson, KineticaPool, KineticaPoolOptions, KineticaText, KineticaVector,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sqlx::Row;

    type Error = Box<dyn std::error::Error>;

    const SCHEMA: &str = "sqlx_complex_types_example";
    const TABLE: &str = "sqlx_complex_types_example.articles";

    /// An article's metadata, held in a `JSON` column.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Meta {
        author: String,
        lang: String,
        year: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        draft: Option<bool>,
    }

    struct Article {
        id: i32,
        title: &'static str,
        meta: Meta,
        tags: Vec<&'static str>,
        ratings: Vec<f64>,
        embedding: [f32; 3],
    }

    fn meta(author: &str, lang: &str, year: i32, draft: Option<bool>) -> Meta {
        Meta { author: author.into(), lang: lang.into(), year, draft }
    }

    fn articles() -> Vec<Article> {
        vec![
            Article {
                id: 1,
                title: "Ownership in Rust",
                meta: meta("ana", "en", 2024, None),
                tags: vec!["rust", "memory"],
                ratings: vec![4.5, 5.0],
                embedding: [0.9, 0.1, 0.0],
            },
            Article {
                id: 2,
                title: "Async Rust",
                meta: meta("ben", "en", 2025, None),
                tags: vec!["rust", "async"],
                ratings: vec![4.0],
                embedding: [0.8, 0.3, 0.1],
            },
            Article {
                id: 3,
                title: "Rust en español",
                meta: meta("caro", "es", 2025, None),
                tags: vec!["rust"],
                ratings: vec![],
                embedding: [0.85, 0.2, 0.0],
            },
            Article {
                id: 4,
                title: "GPU joins",
                meta: meta("ana", "en", 2023, None),
                tags: vec!["gpu", "sql"],
                ratings: vec![3.5, 4.0, 4.5],
                embedding: [0.0, 0.1, 0.9],
            },
            Article {
                id: 5,
                title: "Vector search",
                meta: meta("dev", "en", 2025, Some(true)),
                tags: vec!["rust", "vectors"],
                ratings: vec![5.0],
                embedding: [0.6, 0.6, 0.2],
            },
        ]
    }

    pub async fn run() -> Result<(), Error> {
        println!("=== SQLx with JSON, Arrays and Vectors ===\n");

        let host = std::env::var("KINETICA_HOST").unwrap_or_else(|_| "localhost".to_string());
        let username = std::env::var("KINETICA_USER").unwrap_or_else(|_| "admin".to_string());
        let password = std::env::var("KINETICA_PASSWORD").unwrap_or_else(|_| "password".to_string());

        // One connection is all a sequence of statements needs
        let pool = KineticaPoolOptions::new()
            .host(&host)
            .username(&username)
            .password(&password)
            .max_connections(1)
            .connect()
            .await?;

        // Clean up and close the pool whatever happens: the server keeps the
        // session of a connection that is not closed for minutes.
        let outcome = demonstrate(&pool).await;
        let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")).execute(&pool).await;
        pool.close().await;
        println!("\nDropped the schema and closed the pool");
        outcome
    }

    async fn demonstrate(pool: &KineticaPool) -> Result<(), Error> {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")).execute(pool).await?;
        sqlx::query(&format!("CREATE SCHEMA {SCHEMA}")).execute(pool).await?;
        sqlx::query(&format!(
            "CREATE TABLE {TABLE} (
                id INT,
                title VARCHAR(64),
                meta JSON,
                tags VARCHAR[],
                ratings DOUBLE[],
                embedding VECTOR(3)
            )"
        ))
        .execute(pool)
        .await?;

        insert(pool).await?;
        select(pool).await?;
        update(pool).await?;
        delete(pool).await?;
        search(pool).await?;
        Ok(())
    }

    // =========================================================================
    // INSERT
    // =========================================================================

    async fn insert(pool: &KineticaPool) -> Result<(), Error> {
        println!("--- INSERT ---");
        for article in articles() {
            sqlx::query(&format!("INSERT INTO {TABLE} VALUES ($1, $2, $3, $4, $5, $6)"))
                .bind(KineticaText(article.id))
                .bind(KineticaText(article.title))
                // Any Serialize value, bound as JSON
                .bind(KineticaJson(&article.meta))
                .bind(KineticaArray(article.tags.iter().map(|t| t.to_string()).collect()))
                .bind(KineticaArray(article.ratings.clone()))
                // A plain INSERT takes the vector's floats themselves
                .bind(KineticaVector(article.embedding.to_vec()))
                .execute(pool)
                .await?;
        }
        println!("Inserted {} articles", articles().len());

        // A vector may also be inserted as text, through VECTOR($n, dims), and
        // a SQL NULL stands for a missing document, array or vector.
        sqlx::query(&format!("INSERT INTO {TABLE} VALUES ($1, $2, $3, $4, $5, VECTOR($6, 3))"))
            .bind(KineticaText(6))
            .bind(KineticaText("Untitled"))
            .bind(None::<KineticaJson<Meta>>)
            .bind(None::<KineticaArray<String>>)
            .bind(None::<KineticaArray<f64>>)
            .bind(KineticaText([0.0f32, 0.0, 1.0].as_slice()))
            .execute(pool)
            .await?;
        println!("Inserted article 6: no metadata, no tags, a vector written as text\n");
        Ok(())
    }

    // =========================================================================
    // SELECT
    // =========================================================================

    async fn select(pool: &KineticaPool) -> Result<(), Error> {
        println!("--- SELECT ---");

        // Every column reads back through its wrapper
        let row = sqlx::query(&format!("SELECT title, meta, tags, ratings, embedding FROM {TABLE} WHERE id = $1"))
            .bind(KineticaText(5))
            .fetch_one(pool)
            .await?;
        let title: String = row.get("title");
        let meta: KineticaJson<Meta> = row.get("meta");
        let tags: KineticaArray<String> = row.get("tags");
        let ratings: KineticaArray<f64> = row.get("ratings");
        let embedding: KineticaVector = row.get("embedding");
        println!("Article 5: {title:?}, {:?}, tags {:?}, ratings {:?}, embedding {:?}", meta.0, tags.0, ratings.0, embedding.0);

        // Filter on a document's fields: the JSON path may be a parameter
        let rows = sqlx::query(&format!("SELECT id FROM {TABLE} WHERE JSON_VALUE(meta, $1) = $2 ORDER BY id"))
            .bind(KineticaText("$.author"))
            .bind(KineticaText("ana"))
            .fetch_all(pool)
            .await?;
        println!("By ana: {:?}", ids(&rows));

        // JSON functions
        let row = sqlx::query(&format!(
            "SELECT JSON_VALUE(meta, '$.lang') AS lang, JSON_LENGTH(meta) AS fields, JSON_EXISTS(meta, '$.draft') AS has_draft
             FROM {TABLE} WHERE id = $1"
        ))
        .bind(KineticaText(5))
        .fetch_one(pool)
        .await?;
        println!(
            "Article 5's metadata: lang {:?}, {} fields, draft field present: {}",
            row.get::<String, _>("lang"),
            row.get::<i32, _>("fields"),
            row.get::<bool, _>("has_draft"),
        );

        // Filter on arrays. ARRAY_CONTAINS takes a string parameter; for
        // numbers, bind a one-element array to ARRAY_CONTAINS_ANY.
        let rows = sqlx::query(&format!("SELECT id FROM {TABLE} WHERE ARRAY_CONTAINS(tags, $1) ORDER BY id"))
            .bind(KineticaText("rust"))
            .fetch_all(pool)
            .await?;
        println!("Tagged rust: {:?}", ids(&rows));
        let rows = sqlx::query(&format!("SELECT id FROM {TABLE} WHERE ARRAY_CONTAINS_ANY(ratings, $1) ORDER BY id"))
            .bind(KineticaArray(vec![5.0]))
            .fetch_all(pool)
            .await?;
        println!("Rated 5.0 at least once: {:?}", ids(&rows));

        // Array functions; the arrays they compute arrive as JSON text,
        // which KineticaArray reads too
        let row = sqlx::query(&format!(
            "SELECT ARRAY_LENGTH(ratings) AS n, ARRAY_TO_STRING(tags, ', ') AS joined, ARRAY_APPEND(tags, 'new') AS more
             FROM {TABLE} WHERE id = $1"
        ))
        .bind(KineticaText(4))
        .fetch_one(pool)
        .await?;
        println!(
            "Article 4: {} ratings, tags {:?}, with one more {:?}",
            row.get::<i32, _>("n"),
            row.get::<String, _>("joined"),
            row.get::<KineticaArray<String>, _>("more").0,
        );

        // Similarity search: a vector parameter outside a plain INSERT is text
        let probe = [1.0f32, 0.2, 0.0];
        let rows = sqlx::query(&format!(
            "SELECT id, title, L2_DISTANCE(embedding, $1) AS distance
             FROM {TABLE} WHERE JSON_VALUE(meta, '$.lang') = $2
             ORDER BY distance LIMIT 3"
        ))
        .bind(KineticaText(probe.as_slice()))
        .bind(KineticaText("en"))
        .fetch_all(pool)
        .await?;
        println!("Nearest English articles to {probe:?}:");
        for row in &rows {
            println!(
                "  {} {:?} at {:.3}",
                row.get::<i32, _>("id"),
                row.get::<String, _>("title"),
                row.get::<f32, _>("distance"),
            );
        }

        // Nulls read as None
        let row = sqlx::query(&format!("SELECT meta, tags FROM {TABLE} WHERE id = $1"))
            .bind(KineticaText(6))
            .fetch_one(pool)
            .await?;
        let meta: Option<KineticaJson<Meta>> = row.get("meta");
        let tags: Option<KineticaArray<String>> = row.get("tags");
        println!("Article 6: metadata {meta:?}, tags {tags:?}\n");
        Ok(())
    }

    // =========================================================================
    // UPDATE
    // =========================================================================

    async fn update(pool: &KineticaPool) -> Result<(), Error> {
        println!("--- UPDATE ---");

        // Publish the draft: a new document, one more tag, and a new
        // embedding, which an UPDATE takes as text
        let done = sqlx::query(&format!(
            "UPDATE {TABLE} SET meta = $1, tags = ARRAY_APPEND(tags, $2), embedding = $3
             WHERE JSON_EXISTS(meta, '$.draft') AND id = $4"
        ))
        .bind(KineticaJson(meta("dev", "en", 2026, None)))
        .bind(KineticaText("published"))
        .bind(KineticaText([0.7f32, 0.5, 0.1].as_slice()))
        .bind(KineticaText(5))
        .execute(pool)
        .await?;
        println!("Published {} draft", done.rows_affected());

        // Replace whole arrays
        let done = sqlx::query(&format!("UPDATE {TABLE} SET tags = $1, ratings = $2 WHERE id = $3"))
            .bind(KineticaArray(vec!["untitled".to_string()]))
            .bind(KineticaArray(vec![1.0, 2.0]))
            .bind(KineticaText(6))
            .execute(pool)
            .await?;
        println!("Gave article 6 tags and ratings: {} row", done.rows_affected());

        let row = sqlx::query(&format!("SELECT meta, tags, embedding FROM {TABLE} WHERE id = $1"))
            .bind(KineticaText(5))
            .fetch_one(pool)
            .await?;
        println!(
            "Article 5 now: {:?}, tags {:?}, embedding {:?}\n",
            row.get::<KineticaJson<Meta>, _>("meta").0,
            row.get::<KineticaArray<String>, _>("tags").0,
            row.get::<KineticaVector, _>("embedding").0,
        );
        Ok(())
    }

    // =========================================================================
    // DELETE
    // =========================================================================

    async fn delete(pool: &KineticaPool) -> Result<(), Error> {
        println!("--- DELETE ---");

        // By a document's field
        let done = sqlx::query(&format!("DELETE FROM {TABLE} WHERE JSON_VALUE(meta, '$.year') < $1"))
            .bind(KineticaText("2024"))
            .execute(pool)
            .await?;
        println!("Deleted {} article(s) from before 2024", done.rows_affected());

        // By an array's contents
        let done = sqlx::query(&format!("DELETE FROM {TABLE} WHERE ARRAY_CONTAINS(tags, $1)"))
            .bind(KineticaText("untitled"))
            .execute(pool)
            .await?;
        println!("Deleted {} untitled article(s)", done.rows_affected());

        // By distance from a vector
        let done = sqlx::query(&format!("DELETE FROM {TABLE} WHERE COSINE_DISTANCE(embedding, $1) > $2"))
            .bind(KineticaText([1.0f32, 0.2, 0.0].as_slice()))
            .bind(KineticaText(0.05))
            .execute(pool)
            .await?;
        println!("Deleted {} article(s) far from the probe", done.rows_affected());

        let rows = sqlx::query(&format!("SELECT id FROM {TABLE} ORDER BY id")).fetch_all(pool).await?;
        println!("Left: {:?}", ids(&rows));
        Ok(())
    }

    // =========================================================================
    // Full-text search of JSON
    // =========================================================================

    async fn search(pool: &KineticaPool) -> Result<(), Error> {
        println!("\n--- Full-text search ---");
        let table = format!("{SCHEMA}.searchable");
        let created = sqlx::query(&format!(
            "CREATE TABLE {table} (id INT NOT NULL, title VARCHAR(64, text_search), meta JSON(text_search), SHARD KEY (id))"
        ))
        .execute(pool)
        .await;
        if let Err(e) = created {
            if e.as_database_error().is_some_and(|d| d.message().contains("text search is disabled")) {
                println!("Text search is off on this server (enable_text_search); skipped");
                return Ok(());
            }
            return Err(e.into());
        }
        for article in articles() {
            sqlx::query(&format!("INSERT INTO {table} VALUES ($1, $2, $3)"))
                .bind(KineticaText(article.id))
                .bind(KineticaText(article.title))
                .bind(KineticaJson(json!({"author": article.meta.author, "lang": article.meta.lang, "tags": article.tags})))
                .execute(pool)
                .await?;
        }

        // A key of the document, an element of its array, and a combination
        for query in ["author:ana", "tags:rust", "tags:rust AND lang:en -author:dev"] {
            let rows = sqlx::query(&format!("SELECT id FROM {table} WHERE text_match(meta, $1) ORDER BY id"))
                .bind(KineticaText(query))
                .fetch_all(pool)
                .await?;
            println!("{query:<36} -> {:?}", ids(&rows));
        }

        // Ranked by relevance across the whole sharded table
        let rows = sqlx::query(&format!(
            "SELECT id, title, text_match_bm25_global(meta, $1) AS score FROM {table}
             WHERE text_match_bm25_global(meta, $1) > 0 ORDER BY score DESC, id"
        ))
        .bind(KineticaText("tags:rust OR tags:vectors"))
        .fetch_all(pool)
        .await?;
        println!("Ranked for tags rust or vectors:");
        for row in &rows {
            println!("  {} {:?} scores {:.4}", row.get::<i32, _>("id"), row.get::<String, _>("title"), row.get::<f64, _>("score"));
        }

        // Kinetica 7.2.3.21 crashes on an UPDATE of a JSON(text_search)
        // column, so a document is changed by deleting its row and inserting
        // it again
        let done = sqlx::query(&format!("DELETE FROM {table} WHERE text_match(meta, $1)"))
            .bind(KineticaText("author:dev"))
            .execute(pool)
            .await?;
        sqlx::query(&format!("INSERT INTO {table} VALUES ($1, $2, $3)"))
            .bind(KineticaText(5))
            .bind(KineticaText("Vector search"))
            .bind(KineticaJson(json!({"author": "eve", "lang": "en", "tags": ["vectors"]})))
            .execute(pool)
            .await?;
        let rows = sqlx::query(&format!("SELECT id FROM {table} WHERE text_match(meta, $1) ORDER BY id"))
            .bind(KineticaText("author:eve"))
            .fetch_all(pool)
            .await?;
        println!("Replaced {} document; by eve now: {:?}", done.rows_affected(), ids(&rows));
        Ok(())
    }

    fn ids(rows: &[sqlx::postgres::PgRow]) -> Vec<i32> {
        rows.iter().map(|row| row.get(0)).collect()
    }
}

#[cfg(feature = "sqlx")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    example::run().await
}

#[cfg(not(feature = "sqlx"))]
fn main() {
    eprintln!("This example requires the 'sqlx' feature.");
    eprintln!("Run with: cargo run -p gpudb-examples --bin sqlx_complex_types --features sqlx");
    std::process::exit(1);
}
