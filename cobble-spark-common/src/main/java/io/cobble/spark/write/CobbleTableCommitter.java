package io.cobble.spark.write;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;
import io.cobble.spark.CobbleCommitLock;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobblePaths;
import io.cobble.spark.CobbleTableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Driver-side commit of one write job: validates shard coverage, materializes the next global
 * snapshot, persists the schema sidecar and enforces snapshot retention.
 */
public final class CobbleTableCommitter {

    private CobbleTableCommitter() {}

    /**
     * Commits all writer results and returns the materialized global snapshot.
     *
     * <p>The commit is serialized per table via {@link CobbleCommitLock}: the current snapshot id
     * is re-read inside the lock so concurrent drivers cannot materialize the same id.
     */
    public static GlobalSnapshot commit(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            List<CobbleShardResult> results)
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
        validateCompleteCoverage(shardSnapshots, totalBuckets);

        try (CobbleCommitLock ignored = CobbleCommitLock.acquire(config.pathUri())) {
            String defaultWriterPath = CobblePaths.tableRoot(config).getAbsolutePath();
            Map<String, String> writerPathByDbId = CobblePaths.loadWriterPathIndex(config);
            for (CobbleShardResult result : results) {
                String writerPath = result.writerPath();
                if (writerPath == null || writerPath.isEmpty()) {
                    writerPath = defaultWriterPath;
                }
                writerPathByDbId.put(result.shardSnapshot().dbId, writerPath);
            }
            CobblePaths.storeWriterPathIndex(config, writerPathByDbId);

            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                GlobalSnapshot latest = coordinator.loadCurrentGlobalSnapshot();
                long globalSnapshotId = latest == null ? 1L : latest.id + 1L;
                GlobalSnapshot materialized =
                        coordinator.materializeGlobalSnapshot(
                                totalBuckets, globalSnapshotId, shardSnapshots);
                CobbleTableSchema.store(config.pathUri(), globalSnapshotId, schema);
                expireOlderSnapshots(
                        config,
                        schema,
                        totalBuckets,
                        coordinator,
                        globalSnapshotId,
                        writerPathByDbId);
                return materialized;
            }
        }
    }

    private static void validateCompleteCoverage(List<ShardSnapshot> snapshots, int totalBuckets)
            throws IOException {
        boolean[] covered = new boolean[totalBuckets];
        for (ShardSnapshot snapshot : snapshots) {
            if (snapshot == null || snapshot.ranges == null) {
                continue;
            }
            for (ShardSnapshot.Range range : snapshot.ranges) {
                if (range == null
                        || range.start < 0
                        || range.end < range.start
                        || range.end >= totalBuckets) {
                    throw new IOException("Invalid shard range in Cobble writer results.");
                }
                for (int bucket = range.start; bucket <= range.end; bucket++) {
                    if (covered[bucket]) {
                        throw new IOException(
                                "Duplicate shard coverage for bucket " + bucket + ".");
                    }
                    covered[bucket] = true;
                }
            }
        }
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            if (!covered[bucket]) {
                throw new IOException("Missing shard coverage for bucket " + bucket + ".");
            }
        }
    }

    private static void expireOlderSnapshots(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
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
                pruneWriterSnapshot(config, schema, totalBuckets, shardSnapshot, writerPath);
            }
            coordinator.expireSnapshot(snapshot.id);
            toExpire--;
        }
    }

    private static void pruneWriterSnapshot(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            int totalBuckets,
            ShardSnapshot shardSnapshot,
            String writerPath)
            throws IOException {
        try {
            SnapshotTools.pruneShardSnapshot(
                    CobblePaths.createWriterConfigForPath(config, schema, totalBuckets, writerPath),
                    shardSnapshot.dbId,
                    shardSnapshot.snapshotId);
        } catch (RuntimeException e) {
            throw new IOException("Failed to prune Cobble shard snapshot", e);
        }
    }
}
