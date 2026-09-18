package io.cobble.spark.write;

import io.cobble.ShardSnapshot;

import java.io.Serializable;

/** Serializable result of one logical bucket: the shard snapshot it produced for the commit. */
public final class CobbleShardResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int totalBuckets;
    private final int bucketId;
    private final ShardSnapshot shardSnapshot;

    public CobbleShardResult(int totalBuckets, int bucketId, ShardSnapshot shardSnapshot) {
        this.totalBuckets = totalBuckets;
        this.bucketId = bucketId;
        this.shardSnapshot = shardSnapshot;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public int bucketId() {
        return bucketId;
    }

    public ShardSnapshot shardSnapshot() {
        return shardSnapshot;
    }

    @Override
    public String toString() {
        return "CobbleShardResult{bucketId="
                + bucketId
                + ", totalBuckets="
                + totalBuckets
                + ", snapshotId="
                + (shardSnapshot == null ? -1L : shardSnapshot.snapshotId)
                + ", dbId="
                + (shardSnapshot == null ? null : shardSnapshot.dbId)
                + "}";
    }
}
