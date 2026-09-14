package io.cobble.spark.write;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.spark.CobbleCommitLock;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobblePaths;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.TableSnapshotCommitter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Driver-side commit of one write job: validates shard coverage, materializes the next global
 * snapshot through the native table committer and enforces snapshot retention.
 */
public final class CobbleTableCommitter {

    private CobbleTableCommitter() {}

    /**
     * Commits all writer results and returns the materialized global snapshot.
     *
     * <p>The commit is serialized per table via {@link CobbleCommitLock}. For appends, the writer
     * tasks restore from {@code baseSnapshot} (pinned by the driver before they run); inside the
     * lock the current snapshot is checked against that base, so a job that started from an
     * already-superseded snapshot aborts instead of silently overwriting the newer commit.
     */
    public static GlobalSnapshot commit(
            CobbleOptions.CobbleTableConfig config,
            List<CobbleShardResult> results,
            GlobalSnapshot baseSnapshot,
            boolean overwrite)
            throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        if (results.isEmpty()) {
            throw new IOException("Cobble write produced no writer results.");
        }
        int totalBuckets = results.get(0).totalBuckets();
        for (CobbleShardResult result : results) {
            if (result.totalBuckets() != totalBuckets) {
                throw new IOException(
                        "Mismatched bucket count across Cobble writers: "
                                + totalBuckets
                                + " vs "
                                + result.totalBuckets()
                                + ".");
            }
        }
        List<ShardSnapshot> shardSnapshots = new ArrayList<>(results.size());
        for (CobbleShardResult result : results) {
            shardSnapshots.add(result.shardSnapshot());
        }
        String lockScope = config.isCatalogTable() ? catalogLockScope(config) : config.pathUri();
        try (CobbleCommitLock ignored = CobbleCommitLock.acquire(lockScope)) {
            GlobalSnapshot latest;
            if (config.isCatalogTable()) {
                latest = catalogCurrent(config, totalBuckets);
            } else
                try (DbCoordinator coordinator =
                        DbCoordinator.open(
                                CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                    latest = coordinator.loadCurrentGlobalSnapshot();
                }
            // Check the expected base before publishing any metadata in either storage mode.
            if (!overwrite && !matchesBase(latest, baseSnapshot)) {
                throw new IOException(
                        "Cobble table "
                                + config.pathUri()
                                + " was modified concurrently: expected base snapshot "
                                + describeSnapshot(baseSnapshot)
                                + " but the current snapshot is "
                                + describeSnapshot(latest)
                                + ". Retry the write.");
            }

            Map<String, String> writerPathByDbId = null;
            if (!config.isCatalogTable()) {
                String defaultWriterPath = CobblePaths.tableRoot(config).getAbsolutePath();
                writerPathByDbId = CobblePaths.loadWriterPathIndex(config);
                for (CobbleShardResult result : results) {
                    String writerPath = result.writerPath();
                    if (writerPath == null || writerPath.isEmpty()) writerPath = defaultWriterPath;
                    writerPathByDbId.put(result.shardSnapshot().dbId, writerPath);
                }
                // Preserve the raw path-mode failure boundary: failure to record the durable
                // writer location must occur before the native global snapshot is published.
                CobblePaths.storeWriterPathIndex(config, writerPathByDbId);
            }

            long commitId = latest == null ? 0L : latest.id + 1L;
            GlobalSnapshot materialized;
            if (config.isCatalogTable()) {
                materialized = catalogCommit(config, totalBuckets, commitId, shardSnapshots);
            } else
                try (TableSnapshotCommitter committer =
                        TableSnapshotCommitter.open(
                                CobblePaths.createCoordinatorConfig(config, totalBuckets),
                                totalBuckets,
                                1)) {
                    materialized = committer.commitBatch(commitId, shardSnapshots);
                }
            if (materialized == null) {
                throw new IOException("Cobble table commit was unexpectedly superseded.");
            }

            if (config.isCatalogTable()) {
                return materialized;
            }

            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                expireOlderSnapshots(
                        config, totalBuckets, coordinator, materialized.id, writerPathByDbId);
            }
            return materialized;
        }
    }

    private static String catalogLockScope(CobbleOptions.CobbleTableConfig config)
            throws IOException {
        io.cobble.spark.CobbleCatalogReference reference = config.catalogReference();
        File root = new File(URI.create(reference.warehouse()));
        File lockDir =
                new File(
                        new File(root, ".spark-cobble-locks"),
                        reference.storageId() + "-TABLE-" + reference.tableId());
        if (!lockDir.isDirectory() && !lockDir.mkdirs()) {
            throw new IOException("Failed to create Spark catalog lock directory " + lockDir);
        }
        return lockDir.toURI().toString();
    }

    private static GlobalSnapshot catalogCurrent(
            CobbleOptions.CobbleTableConfig config, int totalBuckets) {
        io.cobble.spark.CobbleCatalogReference reference = config.catalogReference();
        try (FileCatalog catalog =
                        FileCatalog.open(
                                new io.cobble.Config().addVolume(reference.warehouse()),
                                reference.storageId());
                CatalogTable table = catalog.loadTable(reference.identifier());
                DbCoordinator coordinator =
                        table.coordinator(
                                CobblePaths.createWriterConfig(config, totalBuckets, 0, 1))) {
            reference.validate(table);
            return coordinator.loadCurrentGlobalSnapshot();
        }
    }

    private static GlobalSnapshot catalogCommit(
            CobbleOptions.CobbleTableConfig config,
            int totalBuckets,
            long commitId,
            List<ShardSnapshot> snapshots) {
        io.cobble.spark.CobbleCatalogReference reference = config.catalogReference();
        try (FileCatalog catalog =
                        FileCatalog.open(
                                new io.cobble.Config().addVolume(reference.warehouse()),
                                reference.storageId());
                CatalogTable table = catalog.loadTable(reference.identifier());
                TableSnapshotCommitter committer =
                        table.snapshotCommitter(
                                CobblePaths.createWriterConfig(config, totalBuckets, 0, 1), 1)) {
            reference.validate(table);
            return committer.commitBatch(commitId, snapshots);
        }
    }

    /** True when the current snapshot is still the base this append job started from. */
    private static boolean matchesBase(GlobalSnapshot current, GlobalSnapshot expected) {
        if (expected == null) {
            return current == null;
        }
        return current != null && current.id == expected.id;
    }

    private static String describeSnapshot(GlobalSnapshot snapshot) {
        return snapshot == null ? "none" : Long.toString(snapshot.id);
    }

    private static void expireOlderSnapshots(
            CobbleOptions.CobbleTableConfig config,
            int totalBuckets,
            DbCoordinator coordinator,
            long retainedSnapshotId,
            Map<String, String> writerPathByDbId)
            throws IOException {
        if (config.snapshotRetention() <= 0) {
            return;
        }
        List<GlobalSnapshot> snapshots = coordinator.listGlobalSnapshots();
        Collections.sort(snapshots, Comparator.comparingLong(snapshot -> snapshot.id));

        int toExpire = snapshots.size() - config.snapshotRetention();
        for (GlobalSnapshot snapshot : snapshots) {
            if (toExpire <= 0) {
                break;
            }
            if (snapshot.id == retainedSnapshotId) {
                continue;
            }
            for (ShardSnapshot shardSnapshot : snapshot.shardSnapshots) {
                String writerPath = writerPathByDbId.get(shardSnapshot.dbId);
                if (writerPath == null) {
                    writerPath = CobblePaths.tableRoot(config).getAbsolutePath();
                }
                pruneWriterSnapshot(config, totalBuckets, shardSnapshot, writerPath);
            }
            coordinator.expireSnapshot(snapshot.id);
            toExpire--;
        }
    }

    private static void pruneWriterSnapshot(
            CobbleOptions.CobbleTableConfig config,
            int totalBuckets,
            ShardSnapshot shardSnapshot,
            String writerPath)
            throws IOException {
        try {
            SnapshotTools.pruneShardSnapshot(
                    CobblePaths.createWriterConfigForPath(config, totalBuckets, writerPath),
                    shardSnapshot.dbId,
                    shardSnapshot.snapshotId);
        } catch (RuntimeException e) {
            throw new IOException("Failed to prune Cobble shard snapshot", e);
        }
    }
}
