package io.cobble.spark;

import io.cobble.table.TableScanPlan;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
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
        StructField[] requested = requiredSchema.fields();
        StructField[] targetFields = targetSchema.toStructType().fields();
        StructField[] readFields = new StructField[requested.length];
        for (int i = 0; i < requested.length; i++) {
            StructField field = requested[i];
            try {
                readFields[i] = targetFields[targetSchema.ordinalOf(field.name())];
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
        // Native projection selects top-level fields. Report their complete types so Spark
        // extracts nested fields itself, rather than treating pruning as a schema cast.
        this.requiredSchema = new StructType(readFields);
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
