package io.cobble.spark;

import io.cobble.Config;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.TableIdentifier;

import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Native Cobble-backed Spark catalog.
 *
 * <p>Register with {@code spark.sql.catalog.<name>=io.cobble.spark.SparkCatalog} and {@code
 * spark.sql.catalog.<name>.path=<warehouse>}. Namespace and table registration is stored by
 * Cobble's {@link FileCatalog}; Spark does not create sidecar metadata files.
 *
 * <p>Names are validated before invoking the native catalog. Table locations and arbitrary table
 * properties are intentionally unsupported because they are not durable catalog metadata.
 */
public final class SparkCatalog implements TableCatalog, SupportsNamespaces {

    private static final String PROVIDER = "cobble";

    private String name;
    private Path warehouse;
    private String storageId;
    private int defaultBuckets;
    private Map<String, String> runtimeDefaults;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.name = name;
        String warehouseValue = options.get("path");
        if (warehouseValue == null || warehouseValue.trim().isEmpty()) {
            warehouseValue = options.get("warehouse");
        }
        if (warehouseValue == null || warehouseValue.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Cobble catalog '"
                            + name
                            + "' requires a warehouse path option: spark.sql.catalog."
                            + name
                            + ".path=<warehouse>");
        }
        this.warehouse =
                Paths.get(java.net.URI.create(CobbleOptions.normalizePathUri(warehouseValue)))
                        .normalize();
        validateCatalogOptions(options.asCaseSensitiveMap());
        this.storageId = options.get("storage-id") == null ? "cobble" : options.get("storage-id");
        if (storageId.trim().isEmpty()) {
            throw new IllegalArgumentException("Cobble catalog storage-id must not be empty.");
        }
        this.runtimeDefaults = new HashMap<>(options.asCaseSensitiveMap());
        this.runtimeDefaults.put(CobbleOptions.PATH, warehouse.toUri().toString());
        CobbleOptions.CobbleTableConfig runtimeConfig = CobbleOptions.parse(runtimeDefaults);
        if (runtimeConfig.hasSnapshotId()) {
            throw new IllegalArgumentException(
                    "snapshot-id is a table operation option, not a catalog runtime option.");
        }
        validateRuntimeMemoryBounds(runtimeConfig);
        this.defaultBuckets =
                runtimeConfig.hasBucketCount()
                        ? runtimeConfig.bucketCount()
                        : CobbleOptions.DEFAULT_BUCKET;
        try {
            Files.createDirectories(warehouse);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to create Cobble catalog warehouse " + warehouse, e);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String[] defaultNamespace() {
        return new String[0];
    }

    @Override
    public String[][] listNamespaces() {
        try (FileCatalog catalog = openNativeCatalog()) {
            List<List<String>> namespaces = catalog.listNamespaces();
            String[][] result = new String[namespaces.size()][];
            for (int index = 0; index < namespaces.size(); index++) {
                result[index] = namespaces.get(index).toArray(new String[0]);
            }
            return result;
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        requireNoSuchNamespace(namespace);
        // Only single-level namespaces are supported.
        return new String[0][];
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        requireNoSuchNamespace(namespace);
        return java.util.Collections.emptyMap();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        requireNamespaceShape(namespace);
        validateNamespaceMetadata(metadata);
        try (FileCatalog catalog = openNativeCatalog()) {
            if (catalog.listNamespaces().contains(java.util.Arrays.asList(namespace))) {
                throw new NamespaceAlreadyExistsException(namespace);
            }
            catalog.createNamespace(java.util.Arrays.asList(namespace));
        }
    }

    @Override
    public void alterNamespace(
            String[] namespace, org.apache.spark.sql.connector.catalog.NamespaceChange... changes)
            throws NoSuchNamespaceException {
        requireNoSuchNamespace(namespace);
        throw new UnsupportedOperationException(
                "ALTER NAMESPACE is not supported for the Cobble catalog.");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException,
                    org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException {
        requireNoSuchNamespace(namespace);
        if (cascade) {
            throw new UnsupportedOperationException(
                    "CASCADE is not supported for the Cobble catalog; drop registered tables first.");
        }
        try (FileCatalog catalog = openNativeCatalog()) {
            List<TableIdentifier> tables = catalog.listTables(java.util.Arrays.asList(namespace));
            if (!tables.isEmpty()) {
                throw new org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException(
                        namespace);
            }
            catalog.dropNamespace(java.util.Arrays.asList(namespace));
            return true;
        }
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        try {
            requireNamespaceShape(namespace);
            try (FileCatalog catalog = openNativeCatalog()) {
                return catalog.listNamespaces().contains(java.util.Arrays.asList(namespace));
            }
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        requireNoSuchNamespace(namespace);
        try (FileCatalog catalog = openNativeCatalog()) {
            List<TableIdentifier> tables = catalog.listTables(java.util.Arrays.asList(namespace));
            Identifier[] result = new Identifier[tables.size()];
            for (int index = 0; index < tables.size(); index++) {
                result[index] = Identifier.of(namespace, tables.get(index).name());
            }
            return result;
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        TableIdentifier identifier = nativeIdentifier(ident);
        try (FileCatalog catalog = openNativeCatalog()) {
            if (!catalog.tableExists(identifier)) {
                throw new NoSuchTableException(ident);
            }
            try (CatalogTable table = catalog.loadTable(identifier)) {
                return catalogTable(table);
            }
        }
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        final long snapshotId;
        try {
            snapshotId = Long.parseLong(version);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cobble snapshot version must be a numeric snapshot id, got '" + version + "'.",
                    e);
        }
        if (snapshotId < 0L) {
            throw new IllegalArgumentException("Cobble snapshot version must not be negative.");
        }
        TableIdentifier identifier = nativeIdentifier(ident);
        try (FileCatalog catalog = openNativeCatalog()) {
            if (!catalog.tableExists(identifier)) {
                throw new NoSuchTableException(ident);
            }
            try (CatalogTable table = catalog.loadTable(identifier)) {
                return catalogTable(table, Long.valueOf(snapshotId));
            }
        }
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            try (FileCatalog catalog = openNativeCatalog()) {
                return catalog.tableExists(nativeIdentifier(ident));
            }
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        TableIdentifier nativeIdentifier = nativeIdentifier(ident);
        validateCreateOptions(partitions, properties);

        Map<String, String> tableProperties = new HashMap<>();
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String normalizedKey =
                        (key.regionMatches(true, 0, "option.", 0, "option.".length())
                                        ? key.substring("option.".length())
                                        : key)
                                .toLowerCase(java.util.Locale.ROOT);
                String previous = tableProperties.put(normalizedKey, entry.getValue());
                if (previous != null && !previous.equals(entry.getValue())) {
                    throw new IllegalArgumentException(
                            "Conflicting CREATE TABLE options for '" + normalizedKey + "'.");
                }
            }
        }
        tableProperties.put(TableCatalog.PROP_PROVIDER, PROVIDER);
        // Catalog-scoped writers use the warehouse as runtime storage; table identity remains
        // exclusively in native catalog metadata.
        tableProperties.put(CobbleOptions.PATH, warehouse.toUri().toString());

        // Validate everything before creating any directory, so a failed CREATE leaves nothing:
        // schema, primary key, and the full option set (bucket range, retention, write.tasks,
        // memory sizes, snapshot id).
        String rawPrimaryKey = tableProperties.get(CobbleOptions.PRIMARY_KEY);
        List<String> primaryKeys =
                CobbleTableSchema.parsePrimaryKeyOption(rawPrimaryKey == null ? "" : rawPrimaryKey);
        // Spark SQL DDL columns default to nullable; primary key columns are implicitly NOT NULL.
        StructType effectiveSchema = forcePrimaryKeysNotNull(schema, primaryKeys);
        CobbleOptions.CobbleTableConfig config;
        try {
            config = CobbleOptions.parse(tableProperties);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Failed to create Cobble table " + ident.toString() + ": " + e.getMessage(), e);
        }
        CobbleTableSchema tableSchema;
        try {
            tableSchema =
                    CobbleTableSchema.fromStructType(
                            effectiveSchema,
                            primaryKeys,
                            config.hasBucketCount() ? config.bucketCount() : defaultBuckets);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Failed to create Cobble table " + ident.toString() + ": " + e.getMessage(), e);
        }
        try (FileCatalog catalog = openNativeCatalog()) {
            if (!catalog.listNamespaces().contains(nativeIdentifier.namespace())) {
                throw new NoSuchNamespaceException(
                        nativeIdentifier.namespace().toArray(new String[0]));
            }
            if (catalog.tableExists(nativeIdentifier)) {
                throw new TableAlreadyExistsException(ident);
            }
            try (CatalogTable table =
                    catalog.createTable(nativeIdentifier, tableSchema.toTableSchema())) {
                return catalogTable(table);
            }
        }
    }

    private static StructType forcePrimaryKeysNotNull(StructType schema, List<String> primaryKeys) {
        if (primaryKeys.isEmpty()) {
            return schema;
        }
        org.apache.spark.sql.types.StructField[] fields = schema.fields();
        List<org.apache.spark.sql.types.StructField> updated = new ArrayList<>(fields.length);
        for (org.apache.spark.sql.types.StructField field : fields) {
            boolean isKey = false;
            for (String key : primaryKeys) {
                if (key.equals(field.name())) {
                    isKey = true;
                    break;
                }
            }
            updated.add(
                    isKey
                            ? org.apache.spark.sql.types.DataTypes.createStructField(
                                    field.name(), field.dataType(), false)
                            : field);
        }
        return org.apache.spark.sql.types.DataTypes.createStructType(updated);
    }

    private FileCatalog openNativeCatalog() {
        return FileCatalog.open(new Config().addVolume(warehouse.toUri().toString()), storageId);
    }

    private static void validateCatalogOptions(Map<String, String> options) {
        for (String rawKey : options.keySet()) {
            String key = rawKey.toLowerCase(java.util.Locale.ROOT);
            if (!(CobbleOptions.PATH.equals(key)
                    || "warehouse".equals(key)
                    || "storage-id".equals(key)
                    || CobbleOptions.BUCKET.equals(key)
                    || CobbleOptions.WRITE_TASKS.equals(key)
                    || CobbleOptions.WRITE_BUFFER_MEMORY.equals(key)
                    || CobbleOptions.DATA_FILE_TYPE.equals(key)
                    || CobbleOptions.READ_BLOCK_CACHE_MEMORY.equals(key)
                    || CobbleOptions.SNAPSHOT_RETENTION.equals(key))) {
                throw new UnsupportedOperationException(
                        "Unsupported Cobble catalog runtime option '" + rawKey + "'.");
            }
        }
    }

    private static void validateRuntimeMemoryBounds(CobbleOptions.CobbleTableConfig config) {
        if (config.writeBufferMemoryBytes() <= 0L) {
            throw new IllegalArgumentException(
                    CobbleOptions.WRITE_BUFFER_MEMORY
                            + " must be > 0; each task's per-bucket capacity is validated before"
                            + " it opens native databases.");
        }
        if (config.readBlockCacheBytes() < 0L || config.readBlockCacheBytes() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    CobbleOptions.READ_BLOCK_CACHE_MEMORY
                            + " must be in [0, "
                            + Integer.MAX_VALUE
                            + "] for the Cobble runtime.");
        }
    }

    private TableIdentifier nativeIdentifier(Identifier ident) {
        String[] namespace = effectiveNamespace(ident);
        return new TableIdentifier(java.util.Collections.singletonList(namespace[0]), ident.name());
    }

    private CobbleTable catalogTable(CatalogTable table) {
        return catalogTable(table, null);
    }

    private CobbleTable catalogTable(CatalogTable table, Long snapshotId) {
        CobbleCatalogReference reference =
                new CobbleCatalogReference(
                        warehouse.toUri().toString(),
                        storageId,
                        table.identifier().namespace(),
                        table.identifier().name(),
                        table.tableId(),
                        table.catalogSchemaId(),
                        table.schema());
        Map<String, String> properties = new HashMap<>();
        properties.putAll(runtimeDefaults);
        properties.put(CobbleOptions.PATH, warehouse.toUri().toString());
        properties.put(TableCatalog.PROP_PROVIDER, PROVIDER);
        properties.put(CobbleOptions.BUCKET, Integer.toString(defaultBuckets));
        if (snapshotId != null) {
            properties.put(CobbleOptions.SNAPSHOT_ID, Long.toString(snapshotId.longValue()));
        }
        return new CobbleTable(
                CobbleOptions.parse(properties).withCatalogReference(reference),
                CobbleTableSchema.fromTableSchema(table.schema(), defaultBuckets).toStructType(),
                properties);
    }

    private void validateCreateOptions(Transform[] partitions, Map<String, String> properties) {
        if (partitions != null && partitions.length > 0) {
            throw new UnsupportedOperationException(
                    "Cobble catalog does not support partition transforms.");
        }
        if (properties == null) return;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase(java.util.Locale.ROOT);
            if (key.startsWith("option.")) key = key.substring("option.".length());
            if (TableCatalog.PROP_LOCATION.equalsIgnoreCase(key)
                    || CobbleOptions.PATH.equalsIgnoreCase(key)) {
                throw new UnsupportedOperationException(
                        "Cobble catalog stores table locations natively; LOCATION and path are unsupported.");
            }
            if (CobbleOptions.BUCKET.equalsIgnoreCase(key)
                    && Integer.parseInt(entry.getValue()) != defaultBuckets) {
                throw new IllegalArgumentException(
                        "Catalog table bucket must equal the catalog default "
                                + defaultBuckets
                                + ".");
            }
            if (TableCatalog.PROP_PROVIDER.equalsIgnoreCase(key)
                    && !PROVIDER.equalsIgnoreCase(entry.getValue())) {
                throw new UnsupportedOperationException(
                        "Cobble catalog only supports provider '" + PROVIDER + "'.");
            }
            if (!(CobbleOptions.PRIMARY_KEY.equalsIgnoreCase(key)
                    || CobbleOptions.BUCKET.equalsIgnoreCase(key)
                    || TableCatalog.PROP_PROVIDER.equalsIgnoreCase(key)
                    || TableCatalog.PROP_OWNER.equalsIgnoreCase(key))) {
                throw new UnsupportedOperationException(
                        "Cobble catalog does not persist table property '" + entry.getKey() + "'.");
            }
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        TableIdentifier identifier = nativeIdentifier(ident);
        try (FileCatalog catalog = openNativeCatalog()) {
            if (!catalog.tableExists(identifier)) {
                throw new NoSuchTableException(ident);
            }
            try (CatalogTable current = catalog.loadTable(identifier)) {
                List<io.cobble.table.TableSchemaChange> nativeChanges =
                        CobbleSchemaChanges.toNativeChanges(current.schema(), changes);
                try (CatalogTable evolved = catalog.evolveSchema(identifier, nativeChanges)) {
                    return catalogTable(evolved);
                }
            }
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        try (FileCatalog catalog = openNativeCatalog()) {
            TableIdentifier identifier = nativeIdentifier(ident);
            if (!catalog.tableExists(identifier)) {
                return false;
            }
            catalog.dropTable(identifier);
            return true;
        }
    }

    @Override
    public boolean purgeTable(Identifier ident) {
        throw new UnsupportedOperationException(
                "PURGE is not supported for the Cobble catalog; DROP only unregisters the table.");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        TableIdentifier oldIdentifier = nativeIdentifier(oldIdent);
        TableIdentifier newIdentifier = nativeIdentifier(newIdent);
        if (!oldIdentifier.namespace().equals(newIdentifier.namespace())) {
            throw new UnsupportedOperationException(
                    "Cobble catalog rename must stay in one namespace.");
        }
        try (FileCatalog catalog = openNativeCatalog()) {
            if (!catalog.tableExists(oldIdentifier)) throw new NoSuchTableException(oldIdent);
            if (catalog.tableExists(newIdentifier)) throw new TableAlreadyExistsException(newIdent);
            try (CatalogTable ignored = catalog.renameTable(oldIdentifier, newIdentifier.name())) {
                // The catalog owns the renamed table handle; Spark only needs the durable rename.
            }
        }
    }

    private static void requireNamespaceShape(String[] namespace) {
        if (namespace == null || namespace.length != 1) {
            throw new IllegalArgumentException("Cobble catalog supports one namespace level only.");
        }
        requireValidName(namespace[0], "database");
    }

    /** Strips an optional catalog-name namespace prefix and validates a single-level namespace. */
    private String[] effectiveNamespace(Identifier ident) {
        if (ident == null) {
            throw new IllegalArgumentException("Cobble catalog identifier must not be null.");
        }
        String[] namespace = ident.namespace();
        if (namespace.length > 1 && namespace[0].equals(name)) {
            String[] stripped = new String[namespace.length - 1];
            System.arraycopy(namespace, 1, stripped, 0, stripped.length);
            namespace = stripped;
        }
        if (namespace.length != 1) {
            throw new IllegalArgumentException(
                    "Cobble catalog tables require a single database namespace, got "
                            + ident.toString());
        }
        requireValidName(namespace[0], "database");
        requireValidName(ident.name(), "table");
        return namespace;
    }

    /** Rejects names that could escape the warehouse via path resolution. */
    private static void requireValidName(String part, String what) {
        if (part == null || part.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cobble catalog " + what + " name must not be empty.");
        }
        if (part.equals(".") || part.equals("..")) {
            throw new IllegalArgumentException(
                    "Invalid Cobble catalog " + what + " name: '" + part + "'.");
        }
        if (part.indexOf('/') >= 0 || part.indexOf('\\') >= 0 || part.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "Invalid Cobble catalog " + what + " name: '" + part + "'.");
        }
    }

    private void requireNoSuchNamespace(String[] namespace) throws NoSuchNamespaceException {
        try {
            requireNamespaceShape(namespace);
        } catch (IllegalArgumentException e) {
            throw new NoSuchNamespaceException(namespace == null ? new String[0] : namespace);
        }
        try (FileCatalog catalog = openNativeCatalog()) {
            if (!catalog.listNamespaces().contains(java.util.Arrays.asList(namespace))) {
                throw new NoSuchNamespaceException(namespace);
            }
        }
    }

    private void requireTableExists(Identifier ident) throws NoSuchTableException {
        if (!tableExists(ident)) {
            throw new NoSuchTableException(ident);
        }
    }

    private static void validateNamespaceMetadata(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return;
        }
        for (String key : metadata.keySet()) {
            if (!TableCatalog.PROP_OWNER.equalsIgnoreCase(key)) {
                throw new UnsupportedOperationException(
                        "Cobble catalog does not persist namespace metadata: " + key);
            }
        }
    }
}
