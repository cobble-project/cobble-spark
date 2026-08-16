package io.cobble.spark;

import io.cobble.ScanSplit;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

/**
 * Serializable input partition carrying the planned scan split; the executor re-opens the scan
 * cursor from the split and the scan config.
 */
public final class CobbleInputPartition implements InputPartition {

    private static final long serialVersionUID = 1L;

    private final ScanSplit split;
    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final StructType requiredSchema;

    public CobbleInputPartition(
            ScanSplit split,
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            StructType requiredSchema) {
        this.split = split;
        this.config = config;
        this.schema = schema;
        this.requiredSchema = requiredSchema;
    }

    public ScanSplit split() {
        return split;
    }

    public CobbleOptions.CobbleTableConfig config() {
        return config;
    }

    public CobbleTableSchema schema() {
        return schema;
    }

    public StructType requiredSchema() {
        return requiredSchema;
    }
}
