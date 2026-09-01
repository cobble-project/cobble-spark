package io.cobble.spark;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOnlyDb;
import io.cobble.ShardSnapshot;
import io.cobble.table.ReadOnlyTable;

/** Loads snapshots and the authoritative native table schema embedded in each shard. */
public final class CobbleTableRuntime {
    public static final String TABLE_NAME = "data";

    private CobbleTableRuntime() {}

    public static GlobalSnapshot loadSnapshot(CobbleOptions.CobbleTableConfig config) {
        CobbleLoader.ensureCobbleLoaded();
        try (DbCoordinator coordinator =
                DbCoordinator.open(
                        CobblePaths.createCoordinatorConfig(
                                config,
                                config.hasBucketCount()
                                        ? Integer.valueOf(config.bucketCount())
                                        : null))) {
            return config.hasSnapshotId()
                    ? coordinator.getGlobalSnapshot(config.snapshotId())
                    : coordinator.loadCurrentGlobalSnapshot();
        }
    }

    public static boolean tableExists(CobbleOptions.CobbleTableConfig config) {
        return loadSnapshot(config) != null;
    }

    public static CobbleTableSchema loadSchema(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot snapshot) {
        if (snapshot == null
                || snapshot.shardSnapshots == null
                || snapshot.shardSnapshots.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cobble table " + config.pathUri() + " has no committed shard schema.");
        }
        ShardSnapshot shard = null;
        for (ShardSnapshot candidate : snapshot.shardSnapshots) {
            if (candidate != null) {
                shard = candidate;
                break;
            }
        }
        if (shard == null) {
            throw new IllegalArgumentException(
                    "Cobble snapshot " + snapshot.id + " has no readable shard schema.");
        }
        try (ReadOnlyDb db =
                        ReadOnlyDb.open(
                                CobblePaths.createScanConfig(config, snapshot.totalBuckets, 1),
                                shard.snapshotId,
                                shard.dbId);
                ReadOnlyTable table = ReadOnlyTable.open(db, TABLE_NAME)) {
            return CobbleTableSchema.fromTableSchema(table.schema(), snapshot.totalBuckets);
        }
    }
}
