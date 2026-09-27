package io.cobble.spark;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.TablePathRequest;
import io.cobble.table.TableReader;
import io.cobble.table.TableScanPlan;
import io.cobble.table.TableWritePlan;

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

    /** Captures the validated catalog definition on the driver for portable bucket writers. */
    public static TableWritePlan buildWritePlan(
            CobbleOptions.CobbleTableConfig config, int totalBuckets) {
        if (!config.isCatalogTable()) {
            throw new IllegalArgumentException("A catalog table is required for a write plan.");
        }
        CobbleLoader.ensureCobbleLoaded();
        CobbleCatalogReference reference = config.catalogReference();
        try (FileCatalog catalog =
                        FileCatalog.open(catalogConfig(reference), reference.storageId());
                CatalogTable table = catalog.loadTable(reference.identifier())) {
            reference.validate(table);
            return table.newWriteBuilder().totalBuckets(totalBuckets).build();
        }
    }

    public static CobbleTableSchema loadSchema(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot snapshot) {
        TableScanPlan readPlan = loadReadPlan(config, snapshot);
        if ("cobble-table".equals(readPlan.formatId())) {
            return CobbleTableSchema.fromTableSchema(readPlan.schema(), readPlan.totalBuckets());
        }
        return CobbleTableSchema.fromReadSchema(readPlan.readSchema(), snapshot.totalBuckets);
    }

    /** Resolves a generic path before assuming that it has a native global manifest. */
    public static TableScanPlan resolveReadPlan(CobbleOptions.CobbleTableConfig config) {
        CobbleLoader.ensureCobbleLoaded();
        if (config.isCatalogTable()) {
            GlobalSnapshot snapshot = loadSnapshot(config);
            if (snapshot == null) {
                throw new IllegalArgumentException(
                        "Cobble catalog table has no committed snapshot: " + config.pathUri());
            }
            return loadReadPlan(config, snapshot);
        }
        int buckets = config.hasBucketCount() ? config.bucketCount() : CobbleOptions.DEFAULT_BUCKET;
        Config readConfig = runtimeConfig(config, Integer.valueOf(buckets));
        try (TableReader reader =
                TableReader.open(
                        readConfig,
                        new TablePathRequest(
                                config.pathUri(),
                                selectedTableName(config),
                                config.hasSnapshotId() ? Long.valueOf(config.snapshotId()) : null,
                                config.resolverOptions()))) {
            return reader.scanPlan();
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Failed to open fixed Cobble table reader.", error);
        }
    }

    /** Resolves the captured format id and creates a generic fixed-snapshot read plan. */
    public static TableScanPlan loadReadPlan(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Cobble table " + config.pathUri() + " has no committed snapshot.");
        }
        CobbleLoader.ensureCobbleLoaded();
        if (config.isCatalogTable()) validateCatalog(config, snapshot);
        Config readConfig = runtimeConfig(config, snapshot.totalBuckets);
        try {
            try (TableReader reader =
                    TableReader.open(readConfig, selectedTableName(config), snapshot.id)) {
                return reader.scanPlan();
            }
        } catch (RuntimeException error) {
            throw new IllegalStateException("Failed to plan fixed Cobble snapshot read.", error);
        }
    }

    public static boolean isNativeFormat(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot snapshot) {
        return snapshot == null || "cobble-table".equals(loadReadPlan(config, snapshot).formatId());
    }

    private static void validateCatalog(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot snapshot) {
        CobbleCatalogReference reference = config.catalogReference();
        try (FileCatalog catalog =
                        FileCatalog.open(catalogConfig(reference), reference.storageId());
                CatalogTable table = catalog.loadTable(reference.identifier())) {
            if (config.hasSnapshotId()) reference.validateTable(table);
            else reference.validate(table);
        }
    }

    private static Config catalogConfig(CobbleCatalogReference reference) {
        return new Config().addVolume(reference.warehouse());
    }

    private static String selectedTableName(CobbleOptions.CobbleTableConfig config) {
        return config.isCatalogTable()
                ? "t" + config.catalogReference().tableId()
                : config.tableName();
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
