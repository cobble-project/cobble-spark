package io.cobble.spark.write;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.spark.CobbleCommitLock;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobblePaths;
import io.cobble.table.TableSnapshotCommitter;

import java.io.IOException;
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
        try (CobbleCommitLock ignored = CobbleCommitLock.acquire(config.pathUri())) {
            GlobalSnapshot latest;
            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                // Check the expected base before touching any metadata: a stale base rejects the
                // commit without publishing a snapshot or mutating the writer-path index.
                latest = coordinator.loadCurrentGlobalSnapshot();
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
            }

            long commitId = latest == null ? 0L : latest.id + 1L;
            GlobalSnapshot materialized;
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

            String defaultWriterPath = CobblePaths.tableRoot(config).getAbsolutePath();
            Map<String, String> writerPathByDbId = CobblePaths.loadWriterPathIndex(config);
            for (CobbleShardResult result : results) {
                String writerPath = result.writerPath();
                if (writerPath == null || writerPath.isEmpty()) writerPath = defaultWriterPath;
                writerPathByDbId.put(result.shardSnapshot().dbId, writerPath);
            }
            CobblePaths.storeWriterPathIndex(config, writerPathByDbId);

            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                expireOlderSnapshots(
                        config, totalBuckets, coordinator, materialized.id, writerPathByDbId);
            }
            return materialized;
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
