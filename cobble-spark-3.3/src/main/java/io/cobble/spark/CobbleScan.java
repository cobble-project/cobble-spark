package io.cobble.spark;

import io.cobble.table.TableScanPlan;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;

import java.util.OptionalLong;

/** Batch scan over one committed Cobble global snapshot. */
public final class CobbleScan implements Scan, SupportsReportStatistics {

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema sourceSchema;
    private final CobbleTableSchema targetSchema;
    private final StructType requiredSchema;
    private final TableScanPlan scanPlan;

    public CobbleScan(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema sourceSchema,
            CobbleTableSchema targetSchema,
            StructType requiredSchema,
            TableScanPlan scanPlan) {
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.requiredSchema = requiredSchema;
        this.scanPlan = scanPlan;
    }

    @Override
    public StructType readSchema() {
        return requiredSchema;
    }

    @Override
    public Batch toBatch() {
        return new CobbleBatch(config, sourceSchema, targetSchema, requiredSchema, scanPlan);
    }

    @Override
    public Statistics estimateStatistics() {
        long sizeBytes = scanPlan == null ? 0L : scanPlan.dataSizeBytes();
        final long sizeInBytes = Math.max(sizeBytes, 0L);
        return new Statistics() {
            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }

            @Override
            public OptionalLong numRows() {
                return OptionalLong.empty();
            }
        };
    }

    @Override
    public String description() {
        return "Cobble scan snapshot=" + (scanPlan == null ? 0L : scanPlan.snapshotId());
    }
}
