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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Path-based Cobble table exposed to Spark for batch reads and V1 batch writes. */
public final class CobbleTable implements SupportsRead, SupportsWrite {

    private final CobbleOptions.CobbleTableConfig config;
    private final StructType providedSchema;

    public CobbleTable(CobbleOptions.CobbleTableConfig config, StructType providedSchema) {
        this.config = config;
        this.providedSchema = providedSchema;
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
        // BATCH_WRITE keeps DataFrameWriter on the V2 API; V1_BATCH_WRITE routes the physical
        // plan to V1Write, whose InsertableRelation owns the bucket shuffle for writes. TRUNCATE
        // enables unconditional overwrite (mode Overwrite).
        return EnumSet.of(
                TableCapability.BATCH_READ,
                TableCapability.BATCH_WRITE,
                TableCapability.V1_BATCH_WRITE,
                TableCapability.TRUNCATE);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        Map<String, String> scanOptions = new HashMap<>(options.asCaseSensitiveMap());
        CobbleOptions.CobbleTableConfig scanConfig = CobbleOptions.parse(scanOptions);
        Long snapshotId = scanConfig.hasSnapshotId() ? Long.valueOf(scanConfig.snapshotId()) : null;
        CobbleTableSchema schema = loadSchema(snapshotId);
        return new CobbleScanBuilder(scanConfig, schema);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        Map<String, String> writeOptions = new HashMap<>(info.options().asCaseSensitiveMap());
        CobbleOptions.CobbleTableConfig writeConfig = CobbleOptions.parse(writeOptions);
        return new CobbleWriteBuilder(
                writeConfig, providedSchema != null ? providedSchema : info.schema(), writeOptions);
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
