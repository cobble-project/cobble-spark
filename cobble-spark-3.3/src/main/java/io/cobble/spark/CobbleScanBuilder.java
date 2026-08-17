package io.cobble.spark;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

/** Scan builder resolving the scan snapshot and applying column pruning. */
public final class CobbleScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private StructType requiredSchema;

    public CobbleScanBuilder(CobbleOptions.CobbleTableConfig config, CobbleTableSchema schema) {
        this.config = config;
        this.schema = schema;
        this.requiredSchema = schema.toStructType();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        if (requiredSchema.fields().length == 0) {
            return;
        }
        for (org.apache.spark.sql.types.StructField field : requiredSchema.fields()) {
            try {
                schema.ordinalOf(field.name());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Column '"
                                + field.name()
                                + "' does not exist in Cobble table "
                                + name()
                                + ".",
                        e);
            }
        }
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Scan build() {
        return new CobbleScan(config, schema, requiredSchema, resolveSnapshot());
    }

    private String name() {
        return config.pathUri();
    }

    private GlobalSnapshot resolveSnapshot() {
        CobbleLoader.ensureCobbleLoaded();
        Integer expectedBuckets =
                schema.totalBuckets > 0 ? Integer.valueOf(schema.totalBuckets) : null;
        try (DbCoordinator coordinator =
                DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, expectedBuckets))) {
            GlobalSnapshot snapshot =
                    config.hasSnapshotId()
                            ? coordinator.getGlobalSnapshot(config.snapshotId())
                            : coordinator.loadCurrentGlobalSnapshot();
            if (snapshot == null) {
                if (!config.hasSnapshotId() && schema.totalBuckets > 0) {
                    // The table exists (schema sidecar) but nothing has been committed yet: a
                    // freshly created empty table reads as zero rows.
                    return emptySnapshot(schema.totalBuckets);
                }
                throw new IllegalArgumentException(
                        "Cobble table "
                                + name()
                                + (config.hasSnapshotId()
                                        ? " has no snapshot " + config.snapshotId() + "."
                                        : " has no committed snapshot yet."));
            }
            if (snapshot.totalBuckets <= 0) {
                throw new IllegalArgumentException(
                        "Cobble snapshot " + snapshot.id + " has invalid totalBuckets.");
            }
            if (expectedBuckets != null && snapshot.totalBuckets != expectedBuckets.intValue()) {
                throw new IllegalArgumentException(
                        "Cobble snapshot "
                                + snapshot.id
                                + " has "
                                + snapshot.totalBuckets
                                + " buckets but the stored schema expects "
                                + expectedBuckets
                                + ".");
            }
            return snapshot;
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Failed to resolve the Cobble snapshot for " + name(), e);
        }
    }

    /** An empty snapshot: no shards, so the scan plans zero partitions and returns zero rows. */
    private static GlobalSnapshot emptySnapshot(int totalBuckets) {
        GlobalSnapshot empty = new GlobalSnapshot();
        empty.id = 0L;
        empty.totalBuckets = totalBuckets;
        empty.shardSnapshots = java.util.Collections.emptyList();
        return empty;
    }
}
