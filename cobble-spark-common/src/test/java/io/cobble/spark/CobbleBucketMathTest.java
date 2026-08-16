package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for bucket hashing and writer range math. */
public class CobbleBucketMathTest {

    @Test
    public void hashBucketIsStableAndBounded() {
        byte[] key = new byte[] {1, 2, 3};
        assertEquals(CobbleBucketMath.hashBucket(key, 16), CobbleBucketMath.hashBucket(key, 16));
        for (int i = 0; i < 100; i++) {
            byte[] probe = new byte[] {(byte) i, (byte) (i * 31), (byte) (i * 7)};
            int bucket = CobbleBucketMath.hashBucket(probe, 16);
            assertTrue(bucket >= 0 && bucket < 16, "bucket out of range: " + bucket);
        }
    }

    @Test
    public void writerRangesPartitionAllBuckets() {
        int[][] cases = {{16, 1}, {16, 4}, {16, 16}, {17, 5}, {100, 7}, {65536, 200}};
        for (int[] caseSpec : cases) {
            int totalBuckets = caseSpec[0];
            int writerCount = caseSpec[1];
            boolean[] covered = new boolean[totalBuckets];
            for (int writer = 0; writer < writerCount; writer++) {
                int start = CobbleBucketMath.writerRangeStart(writer, totalBuckets, writerCount);
                int end = CobbleBucketMath.writerRangeEnd(writer, totalBuckets, writerCount);
                assertTrue(start <= end, "empty range for writer " + writer);
                for (int bucket = start; bucket <= end; bucket++) {
                    assertTrue(!covered[bucket], "bucket " + bucket + " covered twice");
                    covered[bucket] = true;
                }
            }
            for (int bucket = 0; bucket < totalBuckets; bucket++) {
                assertTrue(covered[bucket], "bucket " + bucket + " not covered");
            }
        }
    }

    @Test
    public void writerIndexForBucketInvertsRangeFormulas() {
        int[][] cases = {{16, 4}, {16, 16}, {17, 5}, {100, 7}, {65536, 200}};
        for (int[] caseSpec : cases) {
            int totalBuckets = caseSpec[0];
            int writerCount = caseSpec[1];
            for (int writer = 0; writer < writerCount; writer++) {
                int start = CobbleBucketMath.writerRangeStart(writer, totalBuckets, writerCount);
                int end = CobbleBucketMath.writerRangeEnd(writer, totalBuckets, writerCount);
                for (int bucket = start; bucket <= end; bucket++) {
                    assertEquals(
                            writer,
                            CobbleBucketMath.writerIndexForBucket(
                                    bucket, totalBuckets, writerCount),
                            "bucket " + bucket + " of range writer " + writer);
                }
            }
        }
    }

    @Test
    public void rejectsInvalidArguments() {
        assertThrows(
                IllegalArgumentException.class, () -> CobbleBucketMath.hashBucket(new byte[1], 0));
        assertThrows(
                IllegalArgumentException.class, () -> CobbleBucketMath.writerRangeStart(0, 4, 8));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleBucketMath.writerIndexForBucket(16, 16, 4));
    }
}
