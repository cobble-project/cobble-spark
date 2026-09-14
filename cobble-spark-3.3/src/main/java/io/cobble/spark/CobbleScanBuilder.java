package io.cobble.spark;

import io.cobble.table.TableScanPlan;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

/** Scan builder resolving the scan snapshot and applying column pruning. */
public final class CobbleScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final TableScanPlan scanPlan;
    private StructType requiredSchema;

    public CobbleScanBuilder(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            TableScanPlan scanPlan) {
        this.config = config;
        this.schema = schema;
        this.scanPlan = scanPlan;
        this.requiredSchema = schema.toStructType();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        for (org.apache.spark.sql.types.StructField field : requiredSchema.fields()) {
            try {
                schema.ordinalOf(field.name());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Column '"
                                + field.name()
                                + "' does not exist in Cobble table "
                                + name()
                                + ".",
                        e);
            }
        }
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Scan build() {
        return new CobbleScan(config, schema, requiredSchema, scanPlan);
    }

    private String name() {
        return config.pathUri();
    }
}
