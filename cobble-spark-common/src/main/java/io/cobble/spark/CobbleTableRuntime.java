package io.cobble.spark;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.table.TableScanPlan;

/** Loads committed snapshots and table-aware fixed scan plans. */
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
        TableScanPlan plan = loadScanPlan(config, snapshot);
        return CobbleTableSchema.fromTableSchema(plan.schema(), plan.totalBuckets());
    }

    public static TableScanPlan loadScanPlan(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Cobble table " + config.pathUri() + " has no committed snapshot.");
        }
        CobbleLoader.ensureCobbleLoaded();
        return TableScanPlan.forSnapshot(
                CobblePaths.createScanConfig(config, snapshot.totalBuckets, 1),
                TABLE_NAME,
                snapshot.id);
    }
}
