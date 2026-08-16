package io.cobble.spark.write;

import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobbleTableSchema;

import java.io.Serializable;

/** Serializable context shared by all writer tasks of one write job. */
public final class CobbleWriteContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final int totalBuckets;
    private final int writerCount;
    private final boolean overwrite;

    public CobbleWriteContext(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            int totalBuckets,
            int writerCount,
            boolean overwrite) {
        this.config = config;
        this.schema = schema;
        this.totalBuckets = totalBuckets;
        this.writerCount = writerCount;
        this.overwrite = overwrite;
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
}
