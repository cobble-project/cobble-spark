package io.cobble.spark;

import io.cobble.table.TableScanSplit;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

/**
 * Serializable input partition carrying the planned scan split; the executor re-opens the scan
 * cursor from the split and the scan config.
 */
public final class CobbleInputPartition implements InputPartition {

    private static final long serialVersionUID = 1L;

    private final TableScanSplit split;
    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema sourceSchema;
    private final CobbleTableSchema targetSchema;
    private final StructType requiredSchema;

    public CobbleInputPartition(
            TableScanSplit split,
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema sourceSchema,
            CobbleTableSchema targetSchema,
            StructType requiredSchema) {
        this.split = split;
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.requiredSchema = requiredSchema;
    }

    public TableScanSplit split() {
        return split;
    }

    public CobbleOptions.CobbleTableConfig config() {
        return config;
    }

    public CobbleTableSchema sourceSchema() {
        return sourceSchema;
    }

    public CobbleTableSchema targetSchema() {
        return targetSchema;
    }

    public StructType requiredSchema() {
        return requiredSchema;
    }
}
