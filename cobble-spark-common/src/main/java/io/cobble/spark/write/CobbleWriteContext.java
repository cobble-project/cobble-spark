package io.cobble.spark.write;

import io.cobble.GlobalSnapshot;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobbleTableSchema;

import java.io.Serializable;

/**
 * Serializable context shared by all writer tasks of one write job.
 *
 * <p>{@code baseSnapshot} is the committed snapshot the job started from: it is pinned by the
 * driver before the tasks run, every writer restores from it, and the driver-side commit validates
 * (under the commit lock) that the table still points at this base. This prevents two jobs that
 * started from the same snapshot from overwriting each other's commit.
 */
public final class CobbleWriteContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final int totalBuckets;
    private final int writerCount;
    private final boolean overwrite;
    private final GlobalSnapshot baseSnapshot;

    public CobbleWriteContext(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            int totalBuckets,
            int writerCount,
            boolean overwrite,
            GlobalSnapshot baseSnapshot) {
        this.config = config;
        this.schema = schema;
        this.totalBuckets = totalBuckets;
        this.writerCount = writerCount;
        this.overwrite = overwrite;
        this.baseSnapshot = baseSnapshot;
    }

    public CobbleOptions.CobbleTableConfig config() {
        return config;
    }

    public CobbleTableSchema schema() {
        return schema;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public int writerCount() {
        return writerCount;
    }

    public boolean overwrite() {
        return overwrite;
    }

    /**
     * The committed snapshot this job appends to, or {@code null} for a brand new/overwritten
     * table.
     */
    public GlobalSnapshot baseSnapshot() {
        return baseSnapshot;
    }
}
