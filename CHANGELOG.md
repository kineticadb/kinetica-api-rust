# Kinetica Rust API Changelog

## Version 7.2

### Version 7.2.3 - 2026-11-20

#### Added
-   `RecordRetriever::get_columns_by_key_raw()` and
    `get_columns_by_key_raw_with_offset()`, which return a by-column lookup's
    response as the server sent it, in the encoding chosen with the new
    `RecordEncoding`: `Binary` (Avro, the default), `Json`, or `Arrow` (an
    Arrow IPC stream).  A caller that reads JSON or Arrow itself skips the
    Avro decode.  `Arrow` needs Kinetica 7.2.3.24 or later.

#### Changed
-   The crate's version is plain SemVer:  `MAJOR.MINOR` is the Kinetica
    version it implements, and the patch counts this client's releases for it,
    from 7.2.3.  The release before was the pre-release 7.2.3-0.  Depend on the
    crate as `"~7.2"`; a requirement of `"7.2.3-0"` also accepts 7.2.3, and one
    pinned with `=` does not.
-   `Kinetica::api_version()` returns that version alone (`7.2.3`), where it
    returned the four-part version and the build number
    (`7.2.3.0-20261026120000`), and the `User-Agent` header carries
    `kinetica-api-rust/7.2.3` where it carried `7.2.3.0`.  Code that split the
    build number off the version no longer finds one.
-   Upgraded `serde_avro_fast` to 2.1.2 and `serde_serializer_quick_unsupported`
    to 1.0.1 for the MPL-2.0 licensing for both.

#### Removed
-   `SqlHints::distributed_joins()`, for a server option Kinetica deprecates;
    use `SqlHints::distributed_operations()` instead.
-   Removed `flume` dependence on `nanorand` to avoid the Zlib license

#### Fixed
-   Requests requiring HA sync are now retried, with a back-off delay, when the
    server's HA queues are initializing after startup, instead of repeatedly
    failing over and failing back for several seconds.
-   Only the cluster whose head rank is unreachable at its advertised addresses
    operates in degraded mode; HA discovery still continues on.

#### Notes
-   Check CHANGELOG-FUNCTIONS.md for endpoint related changes.


### Version 7.2.3-0 - 2026-10-26

Initial release of the Kinetica Rust API, an async client for Kinetica built on
Tokio.

#### Features
-   Every Kinetica endpoint, as a method on `Kinetica` with typed request,
    response and options structs.
-   `BulkInserter`, which sends each record straight to the worker rank that
    owns its shard, with batching, backpressure, timed flushes and retries.
-   `RecordRetriever`, which looks records up by shard key on the rank that
    holds them.
-   Records as your own `serde` structs, described with `KineticaRecord` and
    `avro_schema!`, or as `GenericRecord` for a table known only at run time.
-   High availability: fail-over across an HA ring of clusters, and fail-back
    to the primary.
-   TLS through rustls, trusting the system's roots or a trust store (PKCS#12,
    JKS or PEM).
-   SQL statements and paged queries, JSON records, and KiFS file upload,
    download and ingest.
-   SQLx support over Kinetica's PostgreSQL wire protocol, as the optional
    `sqlx` feature.
