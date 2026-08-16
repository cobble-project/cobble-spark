package io.cobble.spark.write;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.PendingSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.spark.CobbleBucketMath;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobblePaths;
import io.cobble.spark.CobbleRowEncoder;
import io.cobble.spark.CobbleTableSchema;
import io.cobble.structured.Db;
import io.cobble.structured.ExpandStorageMode;

import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes one Spark partition (one writer shard of the table) into a Cobble shard {@link Db} and
 * produces the shard snapshot for the driver-side commit.
 *
 * <p>Rows must already be partitioned so that every row's bucket falls into this writer's range.
 * When appending to an existing table the writer restores the covering shard snapshots first
 * (restore base manifest, shrink to the owned range, adopt overlapping ranges from other shards),
 * which also supports a changed writer count between jobs.
 */
public final class CobbleShardWriteTask {

    private CobbleShardWriteTask() {}

    /** Writes all rows of one partition and returns a single {@link CobbleShardResult}. */
    public static Iterator<CobbleShardResult> writeShard(
            int writerIndex, Iterator<Row> rows, CobbleWriteContext context) throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        CobbleOptions.CobbleTableConfig config = context.config();
        CobbleTableSchema schema = context.schema();
        int totalBuckets = context.totalBuckets();
        int writerCount = context.writerCount();
        int rangeStart = CobbleBucketMath.writerRangeStart(writerIndex, totalBuckets, writerCount);
        int rangeEnd = CobbleBucketMath.writerRangeEnd(writerIndex, totalBuckets, writerCount);

        CobbleRowEncoder encoder = new CobbleRowEncoder(schema);
        String[] fieldNames = schema.fieldNames().toArray(new String[0]);
        int[] keyOrdinals = encoder.keyOrdinals(fieldNames);
        int[] valueOrdinals = encoder.valueOrdinals(fieldNames);

        Db db = openWriter(context, writerIndex, rangeStart, rangeEnd);
        try {
            while (rows.hasNext()) {
                Row row = rows.next();
                byte[] key = encoder.encodeKey(row, keyOrdinals);
                int bucket = CobbleBucketMath.hashBucket(key, totalBuckets);
                if (bucket < rangeStart || bucket > rangeEnd) {
                    throw new IOException(
                            "Record bucket "
                                    + bucket
                                    + " is outside writer-owned range ["
                                    + rangeStart
                                    + ", "
                                    + rangeEnd
                                    + "] for writer "
                                    + writerIndex
                                    + ".");
                }
                for (int valueIndex = 0; valueIndex < valueOrdinals.length; valueIndex++) {
                    byte[] value = encoder.encodeValue(row, valueOrdinals[valueIndex], valueIndex);
                    if (value == null) {
                        db.delete(bucket, key, valueIndex);
                    } else {
                        db.put(bucket, key, valueIndex, value);
                    }
                }
            }
            PendingSnapshot<ShardSnapshot> pending = db.startAsyncSnapshot();
            ShardSnapshot shardSnapshot;
            try {
                shardSnapshot = pending.future().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while snapshotting Cobble shard", e);
            } catch (java.util.concurrent.ExecutionException e) {
                throw new IOException("Failed to snapshot Cobble shard", e.getCause());
            }
            String writerPath = CobblePaths.tableRoot(config).getAbsolutePath();
            return Collections.singleton(
                            new CobbleShardResult(
                                    totalBuckets, writerIndex, writerPath, shardSnapshot))
                    .iterator();
        } finally {
            db.close();
        }
    }

    private static Db openWriter(
            CobbleWriteContext context, int writerIndex, int rangeStart, int rangeEnd)
            throws IOException {
        CobbleOptions.CobbleTableConfig config = context.config();
        if (context.overwrite()) {
            // Overwrite publishes a fresh snapshot chain and never restores prior state.
            return Db.open(
                    CobblePaths.createWriterConfig(
                            config,
                            context.schema(),
                            context.totalBuckets(),
                            writerIndex,
                            context.writerCount()),
                    rangeStart,
                    rangeEnd);
        }
        GlobalSnapshot globalSnapshot = loadCurrentGlobalSnapshot(context);
        if (globalSnapshot == null) {
            return Db.open(
                    CobblePaths.createWriterConfig(
                            config,
                            context.schema(),
                            context.totalBuckets(),
                            writerIndex,
                            context.writerCount()),
                    rangeStart,
                    rangeEnd);
        }
        return restoreRescaledDb(context, writerIndex, globalSnapshot, rangeStart, rangeEnd);
    }

    private static GlobalSnapshot loadCurrentGlobalSnapshot(CobbleWriteContext context)
            throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        try (DbCoordinator coordinator =
                DbCoordinator.open(
                        CobblePaths.createCoordinatorConfig(
                                context.config(), context.totalBuckets()))) {
            return coordinator.loadCurrentGlobalSnapshot();
        }
    }

    private static Db restoreRescaledDb(
            CobbleWriteContext context,
            int writerIndex,
            GlobalSnapshot globalSnapshot,
            int targetRangeStart,
            int targetRangeEnd)
            throws IOException {
        List<RestoreSource> relevantSources =
                collectRelevantSources(globalSnapshot, targetRangeStart, targetRangeEnd);
        if (relevantSources.isEmpty()) {
            throw new IOException(
                    "Cobble writer "
                            + writerIndex
                            + " could not find any shard snapshot covering range ["
                            + targetRangeStart
                            + ", "
                            + targetRangeEnd
                            + "].");
        }
        ensureRestoreCoverage(relevantSources, targetRangeStart, targetRangeEnd);

        RestoreSource baseSource = selectBaseSource(relevantSources);
        Db db =
                Db.restoreWithManifest(
                        CobblePaths.createWriterConfig(
                                context.config(),
                                context.schema(),
                                context.totalBuckets(),
                                writerIndex,
                                context.writerCount()),
                        baseSource.shardSnapshot.manifestPath);
        boolean success = false;
        try {
            shrinkBaseSourceToTargetRange(db, baseSource, targetRangeStart, targetRangeEnd);
            for (RestoreSource source : relevantSources) {
                if (source == baseSource) {
                    continue;
                }
                materializeSourceSnapshotLocally(context, writerIndex, source.shardSnapshot);
                int[] starts = new int[source.intersections.size()];
                int[] ends = new int[source.intersections.size()];
                for (int i = 0; i < source.intersections.size(); i++) {
                    starts[i] = source.intersections.get(i).start;
                    ends[i] = source.intersections.get(i).end;
                }
                db.expandBucket(
                        source.shardSnapshot.dbId,
                        source.shardSnapshot.snapshotId,
                        starts,
                        ends,
                        ExpandStorageMode.ADOPT_ASYNC);
            }
            success = true;
            return db;
        } finally {
            if (!success) {
                db.close();
            }
        }
    }

    private static void materializeSourceSnapshotLocally(
            CobbleWriteContext context, int writerIndex, ShardSnapshot sourceSnapshot)
            throws IOException {
        File localManifest =
                new File(
                        new File(
                                new File(
                                        CobblePaths.tableRoot(context.config()),
                                        sourceSnapshot.dbId),
                                "snapshot"),
                        "SNAPSHOT-" + sourceSnapshot.snapshotId);
        if (localManifest.exists()) {
            return;
        }
        try (Db ignored =
                Db.restoreWithManifest(
                        CobblePaths.createWriterConfig(
                                context.config(),
                                context.schema(),
                                context.totalBuckets(),
                                writerIndex,
                                context.writerCount()),
                        sourceSnapshot.manifestPath)) {
            // Materialize the source shard snapshot into this writer scope for expandBucket lookup.
        }
    }

    private static List<RestoreSource> collectRelevantSources(
            GlobalSnapshot globalSnapshot, int targetRangeStart, int targetRangeEnd) {
        if (globalSnapshot == null || globalSnapshot.shardSnapshots == null) {
            return Collections.emptyList();
        }
        Map<String, RestoreSource> byIdentity = new LinkedHashMap<>();
        for (ShardSnapshot shardSnapshot : globalSnapshot.shardSnapshots) {
            if (shardSnapshot == null
                    || shardSnapshot.ranges == null
                    || shardSnapshot.ranges.isEmpty()) {
                continue;
            }
            String identity = snapshotIdentity(shardSnapshot);
            for (ShardSnapshot.Range range : shardSnapshot.ranges) {
                if (range == null) {
                    continue;
                }
                int start = Math.max(range.start, targetRangeStart);
                int end = Math.min(range.end, targetRangeEnd);
                if (start > end) {
                    continue;
                }
                RestoreSource source =
                        byIdentity.computeIfAbsent(
                                identity, ignored -> new RestoreSource(shardSnapshot));
                source.intersections.add(new BucketRange(start, end));
            }
        }
        List<RestoreSource> relevantSources = new ArrayList<>(byIdentity.values());
        for (RestoreSource source : relevantSources) {
            source.intersections.sort(Comparator.comparingInt(range -> range.start));
        }
        return relevantSources;
    }

    private static RestoreSource selectBaseSource(List<RestoreSource> restoreSources) {
        RestoreSource baseSource = null;
        int bestOverlap = -1;
        for (RestoreSource source : restoreSources) {
            int overlapSize = source.intersectionSize();
            if (overlapSize > bestOverlap) {
                bestOverlap = overlapSize;
                baseSource = source;
            }
        }
        return baseSource;
    }

    private static void shrinkBaseSourceToTargetRange(
            Db db, RestoreSource baseSource, int targetRangeStart, int targetRangeEnd) {
        List<Integer> starts = new ArrayList<>();
        List<Integer> ends = new ArrayList<>();
        for (ShardSnapshot.Range sourceRange : baseSource.shardSnapshot.ranges) {
            if (sourceRange == null) {
                continue;
            }
            if (sourceRange.start < targetRangeStart) {
                int leftEnd = Math.min(sourceRange.end, targetRangeStart - 1);
                if (sourceRange.start <= leftEnd) {
                    starts.add(sourceRange.start);
                    ends.add(leftEnd);
                }
            }
            if (sourceRange.end > targetRangeEnd) {
                int rightStart = Math.max(sourceRange.start, targetRangeEnd + 1);
                if (rightStart <= sourceRange.end) {
                    starts.add(rightStart);
                    ends.add(sourceRange.end);
                }
            }
        }
        if (starts.isEmpty()) {
            return;
        }
        db.shrinkBucket(
                starts.stream().mapToInt(Integer::intValue).toArray(),
                ends.stream().mapToInt(Integer::intValue).toArray());
    }

    private static void ensureRestoreCoverage(
            List<RestoreSource> restoreSources, int targetRangeStart, int targetRangeEnd)
            throws IOException {
        List<BucketRange> ranges = new ArrayList<>();
        for (RestoreSource source : restoreSources) {
            ranges.addAll(source.intersections);
        }
        ranges.sort(
                Comparator.comparingInt((BucketRange range) -> range.start)
                        .thenComparingInt(range -> range.end));

        int nextExpected = targetRangeStart;
        for (BucketRange range : ranges) {
            if (range.start > nextExpected) {
                throw new IOException(
                        "Cobble writer restore is missing shard coverage for bucket range ["
                                + nextExpected
                                + ", "
                                + (range.start - 1)
                                + "].");
            }
            nextExpected = Math.max(nextExpected, range.end + 1);
            if (nextExpected > targetRangeEnd) {
                return;
            }
        }
        throw new IOException(
                "Cobble writer restore is missing shard coverage for bucket range ["
                        + nextExpected
                        + ", "
                        + targetRangeEnd
                        + "].");
    }

    static String snapshotIdentity(ShardSnapshot shardSnapshot) {
        return shardSnapshot.dbId
                + "#"
                + shardSnapshot.snapshotId
                + "#"
                + shardSnapshot.manifestPath;
    }

    private static final class RestoreSource {
        private final ShardSnapshot shardSnapshot;
        private final List<BucketRange> intersections = new ArrayList<>();

        private RestoreSource(ShardSnapshot shardSnapshot) {
            this.shardSnapshot = shardSnapshot;
        }

        private int intersectionSize() {
            int total = 0;
            for (BucketRange range : intersections) {
                total += range.end - range.start + 1;
            }
            return total;
        }
    }

    static final class BucketRange {
        final int start;
        final int end;

        BucketRange(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "[" + start + "-" + end + "]";
        }
    }
}
