# Kinetica Rust API Changelog

## Version 7.2

### Version 7.2.4 - 2026-11-24

#### Added
-   `RecordRetriever::lookup()` returns a `Lookup` of the records a lookup
    matches, read from the server a page at a time with `next()`, and
    `size()` for the count.  On a replicated table every page comes from one
    worker rank, drawn at random for that lookup, so a lookup's pages neither
    repeat nor skip records.  A lookup is read once, and fails rather than
    continue from another rank if the cluster or shard mapping changes during
    it.
-   `ErrorHandlingMode::Skip`, `FileType::Avro`, `FileType::Gdb` and
    `PartitionType::Series`.
-   `get_default()` on `ErrorHandlingMode`, `FileType` and
    `TypeInferenceMode`, returning the server's defaults (`Abort`,
    `DelimitedText` and `Accuracy`), and `Default` for each, the same.

#### Changed
-   A request is re-sent to the same server only where a second copy cannot
    be applied twice; otherwise it fails over, or fails with a single cluster:
    -   A request whose response timed out, or whose connection closed or
        reset before a response arrived, after it was sent in full is no
        longer re-sent, since the server may have applied it; a second copy
        could store an insert's records twice.  Under heavy load this surfaces
        timeouts that re-sends used to absorb; raise
        `KineticaOptions::set_timeout()`, or re-send a request that is safe to
        repeat yourself.  The client's own reads of table, type, shard and
        server metadata, and listings of KiFS files and directories, are still
        re-sent.
    -   What the server says decides before the status: a reply saying the
        server is shutting down, exiting, offline, starting up, restricted to
        admin users, draining its HA queue, migrating, or has a rank down
        fails over at once instead of being re-sent.
    -   A reply saying a rank failed while handling the request fails over
        instead of being reported.
    -   A gateway's error page with a status below 500, and a request sent to
        a host manager other than the head's, are reported at once, as
        `KineticaError::Api`, instead of failing over.  So is a server error
        saying a connection was refused or reset, which no longer fails over.
    -   A 504 is now re-sent, as a 502 or 503 is.
    -   `KineticaOptions::max_retries` now defaults to 3, down from 5.
-   A `BulkInserter` batch is re-sent to the same cluster up to the inserter's
    own `BulkInserterOptions::max_retries` times, now defaulting to 7, so it is
    sent at most 8 times rather than 24; the connection's `max_retries` no
    longer adds to it.  `BatchInsertionResult::attempt` counts those re-sends.
    A batch sent in full and then lost, as above, fails over rather than
    failing.  If the table has a primary key, the batch is re-sent instead,
    since the server skips a record whose key is already stored; records the
    second copy finds already stored are reported by `drain_warnings()` as
    records that may already have been stored by an earlier copy, not as
    errors.
-   `KifsFileHandler::upload()`, `upload_multiple()` and `ingest()` now fail
    when a file pattern, or a directory, matches no local file, instead of
    uploading nothing; and when a directory cannot be read while the files
    are being resolved, instead of logging it and uploading the rest.  Every
    such name in the request is named in the one error, and nothing is
    uploaded.
-   `KifsFileHandler::upload()`, `upload_multiple()` and `ingest()` now fail,
    before uploading anything, when two of their files would be uploaded to
    the same path (two files of the same name given from different
    directories), instead of uploading one over the other, or ingesting one
    of them twice.  The same file given twice is uploaded once.
-   `TableCreationOptions::set_partition_type()` now sends the partition type
    in uppercase, as the server documents it.
-   `ErrorHandlingMode`, `FileType`, `PartitionType` and `TypeInferenceMode`
    are `#[non_exhaustive]`, as the server may take more values for each: a
    `match` over one of them needs a wildcard arm (`_ => ...`), and later
    values will not break it again.
-   The specific cause for multi-head operations not being available on a
    cluster is now logged: worker addresses not reachable from this client,
    none advertised though multi-head is on at the server, or multi-head off
    at the server.
-   Switched `User-Agent` header to `X-Kinetica-Client` instead.

#### Deprecated
-   `Kinetica::user_agent()`, which names a header the API no longer sets; use
    `Kinetica::client_header()`.  The deprecated method returns the
    `X-Kinetica-Client` value until it is removed.
-   `ErrorHandlingMode::default_mode()`, which returns `permissive` rather
    than the server's default, and the misnamed `default_partition_type()` of
    `FileType` and `TypeInferenceMode`; use `get_default()`.

#### Fixed
-   `RecordRetriever::get_by_key_with_offset()` and
    `get_columns_by_key_with_offset()`, when they tried a lookup again after a
    fail-over or a shard-map update, returned the first page instead of the
    one asked for.
-   A `RecordRetriever` on a replicated table could take the pages of a lookup
    from different worker ranks when another lookup ran between them, which
    could repeat or skip records.  The rank is now chosen by the lookup's
    expression, so one retriever keeps an expression on one rank; different
    retrievers spread the same expression over the ranks.
-   `IngestOptions::set_columns_to_load()` and `set_columns_to_skip()` sent the
    column names as a JSON array: the server refused the columns to load, and
    ignored the columns to skip, loading them all the same.  They are now sent
    as a comma-separated list, in the order given.
-   `TableCreationOptions::set_foreign_keys()` joined the foreign keys with
    commas, which the server refused for more than one key; they are now
    separated by semicolons, as the server expects.
-   `KifsFileHandler::upload()`, `upload_multiple()` and `ingest()` read a local
    name holding a `[` or `{` as a pattern even when a file of that name
    existed, so `q[1].csv` selected `q1.csv`, or nothing where there was no
    such file.  A file or directory of the literal name is
    now taken first, and a name is read as a pattern only when nothing exists
    under it.
-   On Windows, a local path written with backslashes was read as a pattern,
    the backslash taken for the glob escape it is on other platforms.
-   A server reporting its version with four components, without a build
    number, could not be connected to: the version failed to parse.
    `KineticaVersion::build()` is 0 for such a version.
-   `Kinetica::insert_records_from_json()` returned an `ERROR` reply, and a
    JSON `BulkInserter` failed the batch, rather than failing over, when the
    server was draining its HA queue or its query planner was not running.
    Every request now fails over in each of the server states above, where
    some came back as `KineticaError::Api`.


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
