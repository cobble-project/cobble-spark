package io.cobble.spark.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

class CobbleShardWriteTaskTest {

    @Test
    void retryAndStageRetryFailBeforeReadingContextOrOpeningAnyWriter() {
        assertThrows(
                IOException.class,
                () -> CobbleShardWriteTask.writeBucketsAtAttempt(0, null, null, 1, 0));
        assertThrows(
                IOException.class,
                () -> CobbleShardWriteTask.writeBucketsAtAttempt(0, null, null, 0, 1));
        assertThrows(
                IOException.class,
                () -> CobbleShardWriteTask.writeBucketsAtAttempt(0, null, null, 2, 3));
    }

    @Test
    void baseRequiresExactlyOneSingleBucketShardPerBucket() throws Exception {
        GlobalSnapshot snapshot = new GlobalSnapshot();
        snapshot.totalBuckets = 8;
        for (int bucket = 0; bucket < 8; bucket++)
            snapshot.shardSnapshots.add(shard(bucket, bucket));

        Map<Integer, ShardSnapshot> sources = CobbleShardWriteTask.baseByBucket(snapshot, 8);
        assertEquals(8, sources.size());
        assertEquals("bucket-3", sources.get(Integer.valueOf(3)).dbId);
    }

    @Test
    void baseRejectsLegacyMultiBucketAndMissingCoverage() {
        GlobalSnapshot legacy = new GlobalSnapshot();
        legacy.totalBuckets = 8;
        legacy.shardSnapshots.add(shard(0, 7));
        assertThrows(IOException.class, () -> CobbleShardWriteTask.baseByBucket(legacy, 8));

        GlobalSnapshot incomplete = new GlobalSnapshot();
        incomplete.totalBuckets = 8;
        for (int bucket = 0; bucket < 7; bucket++)
            incomplete.shardSnapshots.add(shard(bucket, bucket));
        assertThrows(IOException.class, () -> CobbleShardWriteTask.baseByBucket(incomplete, 8));

        GlobalSnapshot overlap = new GlobalSnapshot();
        overlap.totalBuckets = 8;
        for (int bucket = 0; bucket < 8; bucket++)
            overlap.shardSnapshots.add(shard(bucket, bucket));
        overlap.shardSnapshots.set(7, shard(0, 0));
        assertThrows(IOException.class, () -> CobbleShardWriteTask.baseByBucket(overlap, 8));
    }

    private static ShardSnapshot shard(int start, int end) {
        ShardSnapshot shard = new ShardSnapshot();
        shard.dbId = "bucket-" + start;
        shard.manifestPath = "manifest-" + start;
        shard.ranges = new ArrayList<ShardSnapshot.Range>();
        ShardSnapshot.Range range = new ShardSnapshot.Range();
        range.start = start;
        range.end = end;
        shard.ranges.add(range);
        return shard;
    }
}
