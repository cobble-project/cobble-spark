package io.cobble.spark;

import io.cobble.GlobalSnapshot;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;

import java.util.OptionalLong;

/** Batch scan over one committed Cobble global snapshot. */
public final class CobbleScan implements Scan, SupportsReportStatistics {

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final StructType requiredSchema;
    private final GlobalSnapshot snapshot;

    public CobbleScan(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            StructType requiredSchema,
            GlobalSnapshot snapshot) {
        this.config = config;
        this.schema = schema;
        this.requiredSchema = requiredSchema;
        this.snapshot = snapshot;
    }

    @Override
    public StructType readSchema() {
        return requiredSchema;
    }

    @Override
    public Batch toBatch() {
        return new CobbleBatch(config, schema, requiredSchema, snapshot);
    }

    @Override
    public Statistics estimateStatistics() {
        long sizeBytes = 0L;
        if (snapshot.shardSnapshots != null) {
            for (io.cobble.ShardSnapshot shard : snapshot.shardSnapshots) {
                if (shard != null) {
                    sizeBytes += shard.dataSizeBytes;
                }
            }
        }
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
        return "Cobble scan snapshot=" + snapshot.id;
    }
}
