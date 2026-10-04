# Native catalog

The Spark catalog delegates table identities, schemas and snapshots to Cobble's
native `FileCatalog`. It does not maintain a separate Spark schema or table
properties file. A table created through another connector using the same native
catalog is discoverable through Spark.

## Configuration

Configure a catalog before creating the Spark session:

```properties
spark.sql.catalog.cobble=io.cobble.spark.SparkCatalog
spark.sql.catalog.cobble.path=/shared/cobble-warehouse
spark.sql.catalog.cobble.storage-id=shared
spark.sql.catalog.cobble.bucket=32
spark.sql.catalog.cobble.write.tasks=4
spark.sql.catalog.cobble.write.buffer-memory=32mb
spark.sql.catalog.cobble.data.file-type=parquet
```

`warehouse` is an alias for `path`. Local paths must refer to the same storage on
every driver and executor. Native S3 and Hadoop filesystem storage are also supported;
see [remote storage](remote-storage.md). `storage-id` selects the native catalog
storage namespace; its Spark default is `cobble`.

Runtime settings belong on the catalog, not in persistent table properties.
The configured bucket count applies to the first write; later writes use the
committed table's bucket count. Each bucket has its own Table/Db instance under
`bucket-<id>`, which is also its stable database identity; there is no UUID
directory beneath it. One Spark task manages multiple such instances. Changing writer
parallelism only reassigns buckets to tasks, without merging or splitting Db
contents or changing the primary-key hash protocol.

Spark opens each bucket through the Table writer builder, which requires an
explicit bucket ID and has no generic Db-ID or bucket-range mode. The Table layer owns
bucket directory scoping, database creation/restoration, schema binding, and
the underlying database lifetime. Spark only assigns buckets to tasks and
supplies each bucket's runtime budget and committed snapshot boundary.

`write.buffer-memory` is the budget for each Spark writer task. It is divided
equally among that task's assigned buckets (rounded down to whole bytes), with
one vector memtable buffer per Db and WAL disabled. Memtable binary snapshots
are disabled: every snapshot flushes buffered writes into data files. Data files
default to Parquet; set `data.file-type=sst` to select Cobble's SST format.
These are Cobble-managed storage files, not a standalone Spark Parquet dataset.
Native filenames can still end in `.sst`; the manifest records the actual format.
This is a memtable budget, not a bound on all JVM/native memory.
Each write job resumes the same database at its assigned committed snapshot.
Speculative writes and automatic writer task/stage retries are rejected because
bucket state is updated in place. Restart a failed job only after its original
tasks have stopped. An empty baseline is retained for restarted initial writes
and overwrite. Historical snapshots remain readable, and subsequent file and
snapshot identifiers do not rewind.

Unknown runtime options are rejected. All Spark writes currently require
`snapshot.retention=0` (the default), including standalone path-mode tables.
Global snapshots reference bucket snapshots, including historical versions;
the empty baseline must also remain available. Per-database snapshot pruning is
not a safe table-level retention policy.
Automatic expiration requires reference-aware table garbage collection.

```sql
CREATE NAMESPACE cobble.app;

CREATE TABLE cobble.app.orders (
    id BIGINT NOT NULL,
    amount BIGINT,
    description STRING
) USING cobble
TBLPROPERTIES ('primary-key' = 'id');

INSERT INTO cobble.app.orders VALUES (1, 100, 'first');
INSERT INTO cobble.app.orders VALUES (1, 200, 'updated'), (2, 300, NULL);
SELECT * FROM cobble.app.orders;

ALTER TABLE cobble.app.orders RENAME TO cobble.app.purchases;
```

Writes are primary-key upserts. A successful batch publishes a complete global
snapshot, including unchanged keys restored from the previous snapshot. A later
Spark process can append to that snapshot with a different writer parallelism.
Conflicting values for one key within a single distributed input have no defined
ordering; order-dependent updates must be separated into committed batches.

## Schema evolution and historical reads

The catalog supports appending nullable top-level columns, dropping non-key
columns, renaming columns, native lossless type widening, and relaxing non-key
`NOT NULL` constraints. Nested edits, column ordering and comments are not
supported. Each ALTER batch is validated and applied atomically by the native
catalog.

Latest reads use the catalog schema and map snapshot fields by stable field ID:
new fields read as `NULL`, renamed fields retain their values, and dropped then
re-added names do not recover the deleted field's values. Writes use the latest
catalog schema. Existing snapshots retain their original schemas and data.
Numeric snapshot versions through Spark's versioned catalog API and the
`snapshot-id` read option use the snapshot's own schema and are read-only;
timestamp-based version lookup is not implemented.

For catalog reads, use `spark.read.option("snapshot-id", "0").table("cobble.db.t")`.
The connector automatically registers a Spark analysis rule that binds the
historical schema before this call returns, so subsequent projections, filters,
joins and temporary views use that schema. Load the connector jar before creating
`SparkSession`. The rule only binds this standalone reader relation; it does not
rewrite snapshot options on relations embedded in arbitrary logical plans.
An empty selector or `latest` retains the latest schema and data; invalid or
missing numeric snapshots fail instead of falling back to latest.

Timestamp interoperability currently supports Cobble's local-time-zone timestamp
with precision up to microseconds. No-time-zone timestamps and higher precision
are rejected rather than implicitly converted or truncated.

## Sharing with Flink

Use the same warehouse, `storage-id` and bucket count with Cobble Flink's native
catalog. The corresponding Flink configuration uses `buckets`, not Spark's
`bucket`:

```sql
CREATE CATALOG cobble WITH (
    'type' = 'cobble',
    'path' = 'file:///shared/cobble-warehouse',
    'storage-id' = 'shared',
    'buckets' = '32'
);
```

Both connectors use Cobble's table schema, key/value codecs and bucket protocol.
No Spark-specific schema sidecar is required. Consumer modes and refresh behavior
depend on the connector: this Spark implementation provides fixed batch scans,
not a Structured Streaming source or sink. The current Flink native catalog
source also supports batch reads only; it rejects `scan.mode=streaming`.

Configure retention on every writer. Spark requires `snapshot.retention=0`, but
the current Flink sink requires a positive value and defaults to retaining one
global snapshot. Spark's setting does not prevent Flink from expiring historical
global snapshots. Choose a Flink retention large enough for the history your
readers need; this is not an indefinite-retention or snapshot-lease guarantee.

With the single-bucket Table writer integration in both connectors, Spark and
Flink can take turns updating the same table with different writer parallelism.
Set Flink's `sink.parallelism` explicitly; changing only the environment default
does not configure the catalog sink. For example:

```sql
INSERT INTO cobble.app.purchases
/*+ OPTIONS('sink.parallelism'='5', 'snapshot.retention'='32') */
SELECT id, amount, description FROM incoming_orders;
```

Use an absolute `file:` URI for the current Flink catalog warehouse: a bare
local path can create catalog metadata but fails when the sink opens a writer.
Flink currently writes SST by default, whereas Spark defaults to Parquet;
both connectors can read snapshots containing both file formats.

## Operational boundaries

- Local storage uses OS file locks. Remote storage uses process-level coordination
  only; object-store distributed fencing is not implemented by this writer mode.
- Use one active writer across engines. Spark's write lock, commit lock and expected-base
  check coordinate Spark writes, but do not constitute a shared Spark/Flink
  transaction protocol. Perform schema changes, renames and drops while writers
  are stopped; stale-plan checks are not an atomic catalog-and-data transaction.
- A scan is pinned to its planned snapshot. Appending after planning does not
  change the data returned by that scan.
- Namespaces are single-level. Rename stays within a namespace and preserves the
  native table identity. Namespace metadata, table comments, custom locations,
  partition transforms, `PURGE` and namespace `CASCADE` are not supported.
- `DROP TABLE` unregisters a table; it does not recursively delete its data or
  historical snapshots. Recreating the name creates a different table identity.
  Storage reclamation must be managed separately.
- This is a pre-release catalog layout change. Legacy Spark-only
  `cobble-table.properties` directories are not imported or silently interpreted
  as native catalog tables. Standalone `format("cobble").load(path)` remains a
  separate non-catalog access mode.
- Append rejects old snapshots with UUID database identities or multi-bucket shards. There is no
  automatic migration or cross-shard bucket reassembly; use an explicit export
  and rewrite when migrating existing data to the single-bucket layout.
