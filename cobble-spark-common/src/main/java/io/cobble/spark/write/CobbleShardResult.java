package io.cobble.spark.write;

import io.cobble.ShardSnapshot;

import java.io.Serializable;

/** Serializable result of one writer task: the shard snapshot it produced for the commit. */
public final class CobbleShardResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int totalBuckets;
    private final int writerIndex;
    private final String writerPath;
    private final ShardSnapshot shardSnapshot;

    public CobbleShardResult(
            int totalBuckets, int writerIndex, String writerPath, ShardSnapshot shardSnapshot) {
        this.totalBuckets = totalBuckets;
        this.writerIndex = writerIndex;
        this.writerPath = writerPath;
        this.shardSnapshot = shardSnapshot;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public int writerIndex() {
        return writerIndex;
    }

    public String writerPath() {
        return writerPath;
    }

    public ShardSnapshot shardSnapshot() {
        return shardSnapshot;
    }

    @Override
    public String toString() {
        return "CobbleShardResult{writerIndex="
                + writerIndex
                + ", totalBuckets="
                + totalBuckets
                + ", snapshotId="
                + (shardSnapshot == null ? -1L : shardSnapshot.snapshotId)
                + ", dbId="
                + (shardSnapshot == null ? null : shardSnapshot.dbId)
                + "}";
    }
}
