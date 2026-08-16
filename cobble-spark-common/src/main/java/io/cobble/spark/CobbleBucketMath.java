package io.cobble.spark;

import java.util.Arrays;

/**
 * Bucket assignment and writer range math shared by the read and write paths.
 *
 * <p>A table has {@code totalBuckets} hash buckets. A write job with {@code writerCount} parallel
 * writers assigns writer {@code s} the inclusive bucket range {@code [floor(s*B/P),
 * floor((s+1)*B/P)-1]}; every bucket is owned by exactly one writer whenever {@code P <= B}.
 */
public final class CobbleBucketMath {

    private CobbleBucketMath() {}

    /** Hashes an encoded key into {@code [0, totalBuckets)}. */
    public static int hashBucket(byte[] encodedKey, int totalBuckets) {
        if (totalBuckets <= 0) {
            throw new IllegalArgumentException("totalBuckets must be > 0");
        }
        return Math.floorMod(Arrays.hashCode(encodedKey), totalBuckets);
    }

    /** Inclusive start bucket owned by writer {@code writerIndex}. */
    public static int writerRangeStart(int writerIndex, int totalBuckets, int writerCount) {
        checkRanges(writerIndex, totalBuckets, writerCount);
        return (int) (((long) writerIndex * (long) totalBuckets) / (long) writerCount);
    }

    /** Inclusive end bucket owned by writer {@code writerIndex}. */
    public static int writerRangeEnd(int writerIndex, int totalBuckets, int writerCount) {
        checkRanges(writerIndex, totalBuckets, writerCount);
        int nextStart =
                (int) ((((long) writerIndex + 1L) * (long) totalBuckets) / (long) writerCount);
        return nextStart - 1;
    }

    /** Index of the writer owning {@code bucket}; inverse of the range formulas above. */
    public static int writerIndexForBucket(int bucket, int totalBuckets, int writerCount) {
        if (writerCount <= 0) {
            throw new IllegalArgumentException("writerCount must be > 0");
        }
        if (bucket < 0 || bucket >= totalBuckets) {
            throw new IllegalArgumentException(
                    "bucket must be in [0, " + totalBuckets + "), but was " + bucket);
        }
        return (int) ((((long) bucket + 1L) * (long) writerCount - 1L) / (long) totalBuckets);
    }

    private static void checkRanges(int writerIndex, int totalBuckets, int writerCount) {
        if (writerCount <= 0) {
            throw new IllegalArgumentException("writerCount must be > 0");
        }
        if (writerIndex < 0 || writerIndex >= writerCount) {
            throw new IllegalArgumentException(
                    "writerIndex must be in [0, " + writerCount + "), but was " + writerIndex);
        }
        if (writerCount > totalBuckets) {
            throw new IllegalArgumentException(
                    "writerCount ("
                            + writerCount
                            + ") must not exceed totalBuckets ("
                            + totalBuckets
                            + ").");
        }
    }
}
