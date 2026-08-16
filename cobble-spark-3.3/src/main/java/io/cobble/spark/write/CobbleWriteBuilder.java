package io.cobble.spark.write;

import io.cobble.spark.CobbleOptions;

import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Write builder for the V1 batch write path (Spark 3.3): the returned {@link V1Write} delegates to
 * an insertable relation that shuffles rows by bucket owner and commits shard snapshots.
 */
public final class CobbleWriteBuilder implements SupportsOverwrite {

    private final CobbleOptions.CobbleTableConfig config;
    private final StructType writeSchema;
    private final Map<String, String> rawOptions;
    private boolean overwrite;

    public CobbleWriteBuilder(
            CobbleOptions.CobbleTableConfig config,
            StructType writeSchema,
            Map<String, String> rawOptions) {
        this.config = config;
        this.writeSchema = writeSchema;
        this.rawOptions = rawOptions;
    }

    @Override
    public V1Write build() {
        final boolean overwriteAll = overwrite;
        final CobbleOptions.CobbleTableConfig finalConfig = config;
        final StructType schema = writeSchema;
        final Map<String, String> options = rawOptions;
        return new V1Write() {
            @Override
            public InsertableRelation toInsertableRelation() {
                return new CobbleInsertableRelation(finalConfig, schema, overwriteAll, options);
            }
        };
    }

    @Override
    public CobbleWriteBuilder overwrite(Filter[] filters) {
        if (filters != null && filters.length > 0) {
            throw new UnsupportedOperationException(
                    "The Cobble Spark connector only supports unconditional overwrite.");
        }
        this.overwrite = true;
        return this;
    }

    @Override
    public CobbleWriteBuilder truncate() {
        this.overwrite = true;
        return this;
    }
}
