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
```

`warehouse` is an alias for `path`. The current connector supports local
filesystem paths, which must refer to the same shared storage on every driver
and executor. `storage-id` selects the native catalog storage namespace; its
Spark default is `cobble`.

Runtime settings belong on the catalog, not in persistent table properties.
The configured bucket count applies to the first write; later writes use the
committed table's bucket count. Changing writer parallelism redistributes bucket
ownership, not the primary-key hash protocol.

Unknown runtime options are rejected. Catalog tables currently require
`snapshot.retention=0` (the default): Spark does not run its standalone-path
snapshot pruning against native catalog storage.

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

## Sharing with Flink

Use the same warehouse, `storage-id` and bucket count with Cobble Flink's native
catalog. The corresponding Flink configuration uses `buckets`, not Spark's
`bucket`:

```sql
CREATE CATALOG cobble WITH (
    'type' = 'cobble',
    'path' = '/shared/cobble-warehouse',
    'storage-id' = 'shared',
    'buckets' = '32'
);
```

Both connectors use Cobble's table schema, key/value codecs and bucket protocol.
No Spark-specific schema sidecar is required. Consumer modes and refresh behavior
depend on the connector: this Spark implementation provides fixed batch scans,
not a Structured Streaming source or sink.

The current Flink catalog sink requires existing shard boundaries to match its
configured `sink.parallelism`; changing the Flink environment's default
parallelism alone does not set this connector option. When taking over a table
last written by five Spark writers, use five Flink sink writers as well:

```sql
INSERT INTO cobble.app.purchases /*+ OPTIONS('sink.parallelism'='5') */
SELECT id, amount, description FROM incoming_orders;
```

## Operational boundaries

- Use one active writer across engines. Spark's commit lock and expected-base
  check coordinate Spark commits, but do not constitute a shared Spark/Flink
  transaction protocol. Perform schema changes, renames and drops while writers
  are stopped; stale-plan checks are not an atomic catalog-and-data transaction.
- A scan is pinned to its planned snapshot. Appending after planning does not
  change the data returned by that scan.
- Namespaces are single-level. Rename stays within a namespace and preserves the
  native table identity.
- `DROP TABLE` unregisters a table; it does not recursively delete its data or
  historical snapshots. Recreating the name creates a different table identity.
  Storage reclamation must be managed separately.
- This is a pre-release catalog layout change. Legacy Spark-only
  `cobble-table.properties` directories are not imported or silently interpreted
  as native catalog tables. Standalone `format("cobble").load(path)` remains a
  separate non-catalog access mode.
