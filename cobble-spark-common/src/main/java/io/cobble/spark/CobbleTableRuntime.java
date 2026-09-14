package io.cobble.spark;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.TableReader;
import io.cobble.table.TableScanPlan;

/** Loads committed snapshots and table-aware fixed scan plans. */
public final class CobbleTableRuntime {
    public static final String TABLE_NAME = "data";

    private CobbleTableRuntime() {}

    public static GlobalSnapshot loadSnapshot(CobbleOptions.CobbleTableConfig config) {
        CobbleLoader.ensureCobbleLoaded();
        if (config.isCatalogTable()) {
            CobbleCatalogReference reference = config.catalogReference();
            try (FileCatalog catalog =
                            FileCatalog.open(catalogConfig(reference), reference.storageId());
                    CatalogTable table = catalog.loadTable(reference.identifier());
                    DbCoordinator coordinator = table.coordinator(runtimeConfig(config, null))) {
                if (config.hasSnapshotId()) {
                    reference.validateTable(table);
                } else {
                    reference.validate(table);
                }
                return config.hasSnapshotId()
                        ? coordinator.getGlobalSnapshot(config.snapshotId())
                        : coordinator.loadCurrentGlobalSnapshot();
            }
        }
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
        if (config.isCatalogTable()) {
            CobbleCatalogReference reference = config.catalogReference();
            try (FileCatalog catalog =
                            FileCatalog.open(catalogConfig(reference), reference.storageId());
                    CatalogTable table = catalog.loadTable(reference.identifier());
                    TableReader reader =
                            table.readerBuilder(runtimeConfig(config, snapshot.totalBuckets))
                                    .globalSnapshot(snapshot.id)
                                    .open()) {
                if (config.hasSnapshotId()) {
                    reference.validateTable(table);
                } else {
                    reference.validate(table);
                }
                return reader.scanPlan();
            }
        }
        return TableScanPlan.forSnapshot(
                CobblePaths.createScanConfig(config, snapshot.totalBuckets, 1),
                TABLE_NAME,
                snapshot.id);
    }

    private static Config catalogConfig(CobbleCatalogReference reference) {
        return new Config().addVolume(reference.warehouse());
    }

    private static Config runtimeConfig(
            CobbleOptions.CobbleTableConfig config, Integer totalBuckets) {
        int buckets =
                totalBuckets != null
                        ? totalBuckets.intValue()
                        : (config.hasBucketCount()
                                ? config.bucketCount()
                                : CobbleOptions.DEFAULT_BUCKET);
        return CobblePaths.createScanConfig(config, buckets, 1);
    }
}
