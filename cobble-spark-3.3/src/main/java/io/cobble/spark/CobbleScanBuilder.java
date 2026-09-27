package io.cobble.spark;

import io.cobble.table.TableScanPlan;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

/** Scan builder resolving the scan snapshot and applying column pruning. */
public final class CobbleScanBuilder implements ScanBuilder, SupportsPushDownRequiredColumns {

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema sourceSchema;
    private final CobbleTableSchema targetSchema;
    private final TableScanPlan scanPlan;
    private StructType requiredSchema;

    public CobbleScanBuilder(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema sourceSchema,
            CobbleTableSchema targetSchema,
            TableScanPlan scanPlan) {
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.scanPlan = scanPlan;
        this.requiredSchema = targetSchema.toStructType();
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        for (org.apache.spark.sql.types.StructField field : requiredSchema.fields()) {
            try {
                targetSchema.ordinalOf(field.name());
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
        if (scanPlan == null) {
            return new CobbleScan(config, sourceSchema, targetSchema, requiredSchema, null);
        }
        java.util.List<String> fields = new java.util.ArrayList<String>();
        for (org.apache.spark.sql.types.StructField field : requiredSchema.fields()) {
            int sourceOrdinal =
                    sourceSchema.ordinalForFieldId(
                            targetSchema.fieldId(targetSchema.ordinalOf(field.name())));
            if (sourceOrdinal >= 0) {
                fields.add(sourceSchema.toStructType().fields()[sourceOrdinal].name());
            }
        }
        TableScanPlan projected = scanPlan.project(fields);
        return new CobbleScan(
                config,
                CobbleTableSchema.fromReadSchema(
                        projected.readSchema(), sourceSchema.totalBuckets()),
                targetSchema,
                requiredSchema,
                projected);
    }

    private String name() {
        return config.pathUri();
    }
}
