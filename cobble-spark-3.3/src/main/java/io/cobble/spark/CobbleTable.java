package io.cobble.spark;

import io.cobble.spark.write.CobbleWriteBuilder;
import io.cobble.table.TableScanPlan;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.EnumSet;
import java.util.List;
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
        return config.isCatalogTable()
                ? config.catalogReference().qualifiedName()
                : config.pathUri();
    }

    @Override
    public StructType schema() {
        io.cobble.GlobalSnapshot snapshot = CobbleTableRuntime.loadSnapshot(config);
        if (snapshot != null) return CobbleTableRuntime.loadSchema(config, snapshot).toStructType();
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
        io.cobble.GlobalSnapshot snapshot = CobbleTableRuntime.loadSnapshot(scanConfig);
        CobbleTableSchema schema;
        TableScanPlan scanPlan = null;
        if (snapshot != null) {
            scanPlan = CobbleTableRuntime.loadScanPlan(scanConfig, snapshot);
            schema = CobbleTableSchema.fromTableSchema(scanPlan.schema(), scanPlan.totalBuckets());
        } else if (scanConfig.isCatalogTable()) {
            schema =
                    CobbleTableSchema.fromTableSchema(
                            scanConfig.catalogReference().schema(),
                            scanConfig.hasBucketCount()
                                    ? scanConfig.bucketCount()
                                    : CobbleOptions.DEFAULT_BUCKET);
        } else if (providedSchema != null && !scanConfig.hasSnapshotId()) {
            List<String> primaryKeys =
                    CobbleTableSchema.parsePrimaryKeyOption(
                            operationOptions(options.asCaseSensitiveMap())
                                    .get(CobbleOptions.PRIMARY_KEY));
            int totalBuckets =
                    scanConfig.hasBucketCount()
                            ? scanConfig.bucketCount()
                            : CobbleOptions.DEFAULT_BUCKET;
            schema = CobbleTableSchema.fromStructType(providedSchema, primaryKeys, totalBuckets);
        } else {
            throw new IllegalArgumentException(
                    "Cobble table " + scanConfig.pathUri() + " has no committed snapshot.");
        }
        return new CobbleScanBuilder(scanConfig, schema, scanPlan);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        CobbleOptions.CobbleTableConfig writeConfig =
                operationConfig(info.options().asCaseSensitiveMap());
        if (writeConfig.isCatalogTable() && writeConfig.hasSnapshotId()) {
            throw new UnsupportedOperationException(
                    "Writing a catalog table with snapshot-id is not supported.");
        }
        if (writeConfig.isCatalogTable()) {
            // Fail before Spark launches tasks if a captured table was renamed, dropped, or
            // recreated after this relation was planned.
            CobbleTableRuntime.loadSnapshot(writeConfig);
        }
        Map<String, String> merged = operationOptions(info.options().asCaseSensitiveMap());
        return new CobbleWriteBuilder(
                writeConfig, providedSchema != null ? providedSchema : info.schema(), merged);
    }

    /** Resolves the config for one operation by merging table-level properties with op options. */
    private CobbleOptions.CobbleTableConfig operationConfig(Map<String, String> operationOptions) {
        CobbleOptions.CobbleTableConfig operation =
                CobbleOptions.parse(operationOptions(operationOptions));
        if (!config.isCatalogTable()) {
            return operation;
        }
        if (operation.snapshotRetention() > 0) {
            throw new UnsupportedOperationException(
                    "snapshot.retention is not supported for catalog tables.");
        }
        return operation.withCatalogReference(config.catalogReference());
    }

    private Map<String, String> operationOptions(Map<String, String> operationOptions) {
        return CobbleOptions.mergeTableOptions(tableProperties, operationOptions);
    }
}
