# Kinetica Rust API Changelog

## Version 7.2

### Version 7.2.3.0 - 2026-10-26

-   Initial release.  An async (Tokio) client covering every endpoint, with a
    shard-aware `BulkInserter` and `RecordRetriever`, HA failover, the KiFS
    file handler, and optional SQLx support (the `sqlx` feature).
