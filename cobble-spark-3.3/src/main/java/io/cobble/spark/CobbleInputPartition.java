package io.cobble.spark;

import io.cobble.table.TableScanPlan;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

/**
 * Serializable input partition carrying the planned scan split; the executor re-opens the scan
 * cursor from the split and the scan config.
 */
public final class CobbleInputPartition implements InputPartition {

    private static final long serialVersionUID = 1L;

    private final TableScanPlan plan;
    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema sourceSchema;
    private final CobbleTableSchema targetSchema;
    private final StructType requiredSchema;

    public CobbleInputPartition(
            TableScanPlan plan,
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema sourceSchema,
            CobbleTableSchema targetSchema,
            StructType requiredSchema) {
        this.plan = plan;
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.requiredSchema = requiredSchema;
    }

    public TableScanPlan plan() {
        return plan;
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
