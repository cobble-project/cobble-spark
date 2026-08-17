package io.cobble.spark;

import io.cobble.spark.write.CobbleWriteBuilder;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Cobble table exposed to Spark for batch reads and V1 batch writes.
 *
 * <p>A table is either path based (created from {@code format("cobble")} with a {@code path}
 * option) or loaded from a {@link SparkCatalog}, in which case the full table-level properties
 * (path, primary key, bucket count, ...) are retained here and merged with the per-operation
 * options on every scan and write.
 */
public final class CobbleTable implements SupportsRead, SupportsWrite {

    private final CobbleOptions.CobbleTableConfig config;
    private final StructType providedSchema;
    private final Map<String, String> tableProperties;

    public CobbleTable(
            CobbleOptions.CobbleTableConfig config,
            StructType providedSchema,
            Map<String, String> tableProperties) {
        this.config = config;
        this.providedSchema = providedSchema;
        this.tableProperties = tableProperties;
    }

    @Override
    public String name() {
        return config.pathUri();
    }

    @Override
    public StructType schema() {
        if (CobbleTableSchema.sidecarExists(config.pathUri())) {
            return loadSchema(null).toStructType();
        }
        if (providedSchema != null) {
            return providedSchema;
        }
        throw new IllegalArgumentException(
                "Cobble table "
                        + config.pathUri()
                        + " does not exist yet; write it first or pass a schema when creating it.");
    }

    @Override
    public Set<TableCapability> capabilities() {
        // Writes always go through V1Write (bucket shuffle in the insertable relation), never
        // through the native V2 BatchWrite path, so only V1_BATCH_WRITE is advertised. TRUNCATE
        // enables unconditional overwrite.
        return EnumSet.of(
                TableCapability.BATCH_READ,
                TableCapability.V1_BATCH_WRITE,
                TableCapability.TRUNCATE);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        CobbleOptions.CobbleTableConfig scanConfig = operationConfig(options.asCaseSensitiveMap());
        Long snapshotId = scanConfig.hasSnapshotId() ? Long.valueOf(scanConfig.snapshotId()) : null;
        CobbleTableSchema schema = loadSchema(snapshotId);
        return new CobbleScanBuilder(scanConfig, schema);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        CobbleOptions.CobbleTableConfig writeConfig =
                operationConfig(info.options().asCaseSensitiveMap());
        Map<String, String> merged = operationOptions(info.options().asCaseSensitiveMap());
        return new CobbleWriteBuilder(
                writeConfig, providedSchema != null ? providedSchema : info.schema(), merged);
    }

    /** Resolves the config for one operation by merging table-level properties with op options. */
    private CobbleOptions.CobbleTableConfig operationConfig(Map<String, String> operationOptions) {
        return CobbleOptions.parse(operationOptions(operationOptions));
    }

    private Map<String, String> operationOptions(Map<String, String> operationOptions) {
        return CobbleOptions.mergeTableOptions(tableProperties, operationOptions);
    }

    CobbleTableSchema loadSchema(Long snapshotId) {
        try {
            CobbleTableSchema schema = CobbleTableSchema.load(config.pathUri(), snapshotId);
            if (snapshotId == null && config.hasBucketCount() && schema.totalBuckets > 0) {
                if (schema.totalBuckets != config.bucketCount()) {
                    throw new IllegalArgumentException(
                            "Configured "
                                    + CobbleOptions.BUCKET
                                    + "="
                                    + config.bucketCount()
                                    + " does not match the stored bucket count "
                                    + schema.totalBuckets
                                    + " of table "
                                    + config.pathUri()
                                    + ".");
                }
            }
            return schema;
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load the Cobble schema sidecar for " + config.pathUri(), e);
        }
    }
}
