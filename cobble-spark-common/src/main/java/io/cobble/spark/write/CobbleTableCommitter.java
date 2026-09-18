package io.cobble.spark.write;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Driver-side commit of one write job: validates shard coverage, materializes the next global
 * snapshot through the native table committer.
 */
public final class CobbleTableCommitter {

    private CobbleTableCommitter() {}

    /**
     * Commits all writer results and returns the materialized global snapshot.
     *
     * <p>The commit is serialized per table via {@link CobbleCommitLock}. Writer tasks use the
     * {@code baseSnapshot} pinned by the driver before they run; inside the lock the current
     * snapshot is checked against that base, so a job that started from an already-superseded
     * snapshot aborts instead of silently overwriting the newer commit.
     */
    public static GlobalSnapshot commit(
            CobbleOptions.CobbleTableConfig config,
            List<CobbleShardResult> results,
            GlobalSnapshot baseSnapshot)
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
        validateBucketResults(results, totalBuckets);
        List<ShardSnapshot> shardSnapshots = new ArrayList<>(results.size());
        for (CobbleShardResult result : results) {
            shardSnapshots.add(result.shardSnapshot());
        }
        String lockScope = lockScope(config);
        try (CobbleCommitLock ignored = CobbleCommitLock.acquire(lockScope)) {
            if (config.isCatalogTable()) {
                return commitCatalog(config, totalBuckets, shardSnapshots, baseSnapshot);
            }
            GlobalSnapshot latest;
            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                latest = coordinator.loadCurrentGlobalSnapshot();
            }
            requireBase(config, latest, baseSnapshot);

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

            return materialized;
        }
    }

    static String lockScope(CobbleOptions.CobbleTableConfig config) throws IOException {
        return config.isCatalogTable() ? catalogLockScope(config) : config.pathUri();
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

    /** Enforces the physical one-database-per-bucket contract before publishing any metadata. */
    private static void validateBucketResults(List<CobbleShardResult> results, int totalBuckets)
            throws IOException {
        if (results.size() != totalBuckets) {
            throw new IOException(
                    "Cobble write must produce exactly one result for each of "
                            + totalBuckets
                            + " buckets, but produced "
                            + results.size()
                            + ".");
        }
        Set<Integer> buckets = new HashSet<Integer>();
        for (CobbleShardResult result : results) {
            int bucket = result.bucketId();
            ShardSnapshot snapshot = result.shardSnapshot();
            if (bucket < 0
                    || bucket >= totalBuckets
                    || snapshot == null
                    || snapshot.ranges == null
                    || snapshot.ranges.size() != 1) {
                throw new IOException("Cobble write result is not a valid single-bucket shard.");
            }
            ShardSnapshot.Range range = snapshot.ranges.get(0);
            if (range == null || range.start != bucket || range.end != bucket) {
                throw new IOException(
                        "Cobble write result bucket "
                                + bucket
                                + " does not match its shard coverage.");
            }
            if (!("bucket-" + bucket).equals(snapshot.dbId)) {
                throw new IOException(
                        "Cobble write result bucket "
                                + bucket
                                + " must use stable database identity bucket-"
                                + bucket
                                + ".");
            }
            if (!buckets.add(Integer.valueOf(bucket))) {
                throw new IOException(
                        "Cobble write produced duplicate result for bucket " + bucket + ".");
            }
        }
    }

    /** Loads and validates the catalog table once for both expected-base and publish operations. */
    private static GlobalSnapshot commitCatalog(
            CobbleOptions.CobbleTableConfig config,
            int totalBuckets,
            List<ShardSnapshot> snapshots,
            GlobalSnapshot baseSnapshot)
            throws IOException {
        io.cobble.spark.CobbleCatalogReference reference = config.catalogReference();
        try (FileCatalog catalog =
                        FileCatalog.open(
                                new io.cobble.Config().addVolume(reference.warehouse()),
                                reference.storageId());
                CatalogTable table = catalog.loadTable(reference.identifier())) {
            reference.validate(table);
            GlobalSnapshot latest;
            try (DbCoordinator coordinator =
                    table.coordinator(CobblePaths.createCoordinatorConfig(config, totalBuckets))) {
                latest = coordinator.loadCurrentGlobalSnapshot();
            }
            requireBase(config, latest, baseSnapshot);
            long commitId = latest == null ? 0L : latest.id + 1L;
            try (TableSnapshotCommitter committer =
                    table.snapshotCommitter(
                            CobblePaths.createCoordinatorConfig(config, totalBuckets), 1)) {
                GlobalSnapshot materialized = committer.commitBatch(commitId, snapshots);
                if (materialized == null) {
                    throw new IOException("Cobble table commit was unexpectedly superseded.");
                }
                return materialized;
            }
        }
    }

    /** Rejects a write that was planned against a snapshot no longer current. */
    private static void requireBase(
            CobbleOptions.CobbleTableConfig config,
            GlobalSnapshot latest,
            GlobalSnapshot baseSnapshot)
            throws IOException {
        if (!matchesBase(latest, baseSnapshot)) {
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

    /** True when the current snapshot is still the base this write job started from. */
    private static boolean matchesBase(GlobalSnapshot current, GlobalSnapshot expected) {
        if (expected == null) {
            return current == null;
        }
        return current != null && current.id == expected.id;
    }

    private static String describeSnapshot(GlobalSnapshot snapshot) {
        return snapshot == null ? "none" : Long.toString(snapshot.id);
    }
}
