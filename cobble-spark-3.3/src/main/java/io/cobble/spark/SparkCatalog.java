package io.cobble.spark;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Filesystem-backed Spark catalog for Cobble tables.
 *
 * <p>Register with {@code spark.sql.catalog.<name>=io.cobble.spark.SparkCatalog} and {@code
 * spark.sql.catalog.<name>.path=<warehouse>}. Each namespace is one directory and each table lives
 * under {@code <warehouse>/<database>/<table>}. A table directory contains the full table-level
 * properties ({@code cobble-table.properties}) plus the schema sidecar, so {@code CREATE TABLE},
 * {@code INSERT INTO} and {@code SELECT} work without repeating the {@code path} option.
 *
 * <p>Namespace and table names are validated and every resolved path is normalized and checked to
 * stay under the warehouse, so hostile identifiers cannot escape it. Tables are managed: a
 * user-supplied {@code path} option is overridden with the table directory, and {@code DROP TABLE}
 * removes that directory.
 */
public final class SparkCatalog implements TableCatalog, SupportsNamespaces {

    private static final String TABLE_PROPERTIES_FILE = "cobble-table.properties";
    private static final String PROVIDER = "cobble";

    private String name;
    private Path warehouse;

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
        File[] dirs = warehouse.toFile().listFiles(File::isDirectory);
        if (dirs == null) {
            return new String[0][];
        }
        String[][] namespaces = new String[dirs.length][];
        for (int i = 0; i < dirs.length; i++) {
            namespaces[i] = new String[] {dirs[i].getName()};
        }
        return namespaces;
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
        Map<String, String> metadata = new HashMap<>();
        metadata.put(TableCatalog.PROP_LOCATION, namespaceDir(namespace).toUri().toString());
        metadata.put(TableCatalog.PROP_COMMENT, "Cobble namespace");
        return metadata;
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws NamespaceAlreadyExistsException {
        Path dir = namespaceDir(namespace);
        if (Files.exists(dir)) {
            throw new NamespaceAlreadyExistsException(namespace);
        }
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to create namespace " + String.join(".", namespace), e);
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
        File dir = namespaceDir(namespace).toFile();
        File[] files = dir.listFiles();
        if (files != null && files.length > 0) {
            if (cascade) {
                try {
                    deleteRecursively(dir.toPath());
                    return true;
                } catch (IOException e) {
                    throw new IllegalStateException(
                            "Failed to drop namespace " + String.join(".", namespace), e);
                }
            }
            throw new org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException(namespace);
        }
        return dir.delete();
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        try {
            return Files.isDirectory(namespaceDir(namespace));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        requireNoSuchNamespace(namespace);
        File dir = namespaceDir(namespace).toFile();
        File[] tableDirs = dir.listFiles(File::isDirectory);
        if (tableDirs == null) {
            return new Identifier[0];
        }
        List<Identifier> identifiers = new ArrayList<>();
        for (File tableDir : tableDirs) {
            if (new File(tableDir, TABLE_PROPERTIES_FILE).isFile()) {
                identifiers.add(Identifier.of(namespace, tableDir.getName()));
            }
        }
        return identifiers.toArray(new Identifier[0]);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        Path tableDir = tableDir(ident);
        if (!Files.isDirectory(tableDir)) {
            throw new NoSuchTableException(ident);
        }
        Map<String, String> properties = loadTableProperties(ident);
        CobbleOptions.CobbleTableConfig config = CobbleOptions.parse(properties);
        return new CobbleTable(config, null, properties);
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            return Files.isRegularFile(tableDir(ident).resolve(TABLE_PROPERTIES_FILE));
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
        Path tableDir = tableDir(ident);
        if (Files.exists(tableDir)) {
            throw new TableAlreadyExistsException(ident);
        }
        String db = effectiveNamespace(ident)[0];

        Map<String, String> tableProperties = new HashMap<>();
        if (properties != null) {
            tableProperties.putAll(properties);
        }
        tableProperties.put(TableCatalog.PROP_PROVIDER, PROVIDER);
        // Managed table: the path always points at the table directory, and DROP removes it.
        tableProperties.put(TableCatalog.PROP_LOCATION, tableDir.toUri().toString());
        tableProperties.put(CobbleOptions.PATH, tableDir.toUri().toString());

        // Validate everything before creating any directory, so a failed CREATE leaves nothing:
        // schema, primary key, and the full option set (bucket range, retention, write.tasks,
        // memory sizes, snapshot id).
        String rawPrimaryKey = tableProperties.get(CobbleOptions.PRIMARY_KEY);
        List<String> primaryKeys =
                CobbleTableSchema.parsePrimaryKeyOption(rawPrimaryKey == null ? "" : rawPrimaryKey);
        // Spark SQL DDL columns default to nullable; primary key columns are implicitly NOT NULL.
        StructType effectiveSchema = forcePrimaryKeysNotNull(schema, primaryKeys);
        CobbleTableSchema stored;
        try {
            stored = CobbleTableSchema.fromStructType(effectiveSchema, primaryKeys);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Failed to create Cobble table " + ident.toString() + ": " + e.getMessage(), e);
        }
        CobbleOptions.CobbleTableConfig config;
        try {
            config = CobbleOptions.parse(tableProperties);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Failed to create Cobble table " + ident.toString() + ": " + e.getMessage(), e);
        }
        stored.totalBuckets =
                config.hasBucketCount() ? config.bucketCount() : CobbleOptions.DEFAULT_BUCKET;

        try {
            Files.createDirectories(tableDir);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create table " + ident.name(), e);
        }
        try {
            storeTableProperties(tableDir, tableProperties);
            // Publish the schema sidecar now so the empty table is introspectable.
            CobbleTableSchema.store(tableDir.toUri().toString(), 0L, stored);
        } catch (IOException e) {
            try {
                deleteRecursively(tableDir);
            } catch (IOException cleanup) {
                // Preserve the original failure.
            }
            throw new IllegalArgumentException(
                    "Failed to create Cobble table "
                            + db
                            + "."
                            + ident.name()
                            + ": "
                            + e.getMessage(),
                    e);
        }
        return new CobbleTable(config, effectiveSchema, tableProperties);
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

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        requireTableExists(ident);
        throw new UnsupportedOperationException(
                "ALTER TABLE is not supported for the Cobble catalog yet.");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        Path tableDir = tableDir(ident);
        if (!Files.exists(tableDir)) {
            return false;
        }
        try {
            deleteRecursively(tableDir);
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to drop table " + ident.name(), e);
        }
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws NoSuchTableException, TableAlreadyExistsException {
        requireTableExists(oldIdent);
        // Moving the table directory would break the absolute data file paths recorded inside the
        // committed snapshots, so renames are not supported yet.
        throw new UnsupportedOperationException(
                "ALTER TABLE ... RENAME is not supported for the Cobble catalog yet.");
    }

    private Path namespaceDir(String[] namespace) {
        if (namespace == null || namespace.length != 1) {
            throw new IllegalArgumentException(
                    "Cobble catalog supports single-level namespaces only, got "
                            + (namespace == null ? "null" : String.join(".", namespace)));
        }
        requireValidName(namespace[0], "database");
        return resolveUnderWarehouse(warehouse.resolve(namespace[0]));
    }

    private Path tableDir(Identifier ident) {
        String[] namespace = effectiveNamespace(ident);
        return resolveUnderWarehouse(warehouse.resolve(namespace[0]).resolve(ident.name()));
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

    /** Normalizes and confirms the resolved path stays inside the warehouse. */
    private Path resolveUnderWarehouse(Path resolved) {
        Path normalized = resolved.normalize();
        if (!normalized.startsWith(warehouse)) {
            throw new IllegalArgumentException(
                    "Cobble catalog path escapes the warehouse: " + resolved);
        }
        return normalized;
    }

    private void requireNoSuchNamespace(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null || namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace == null ? new String[0] : namespace);
        }
        if (!Files.isDirectory(namespaceDir(namespace))) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    private void requireTableExists(Identifier ident) throws NoSuchTableException {
        if (!Files.isDirectory(tableDir(ident))) {
            throw new NoSuchTableException(ident);
        }
    }

    private Map<String, String> loadTableProperties(Identifier ident) {
        File propertiesFile = tableDir(ident).resolve(TABLE_PROPERTIES_FILE).toFile();
        if (!propertiesFile.isFile()) {
            throw new IllegalStateException(
                    "Missing " + TABLE_PROPERTIES_FILE + " in table " + ident.toString());
        }
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(propertiesFile)) {
            properties.load(input);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + propertiesFile, e);
        }
        Map<String, String> result = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            result.put(key, properties.getProperty(key));
        }
        return result;
    }

    private static void storeTableProperties(Path tableDir, Map<String, String> properties)
            throws IOException {
        Properties stored = new Properties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            stored.setProperty(entry.getKey(), entry.getValue());
        }
        File propertiesFile = tableDir.resolve(TABLE_PROPERTIES_FILE).toFile();
        try (FileOutputStream output = new FileOutputStream(propertiesFile)) {
            stored.store(output, "Cobble table properties");
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(
                            path -> {
                                try {
                                    Files.delete(path);
                                } catch (IOException e) {
                                    throw new IllegalStateException("Failed to delete " + path, e);
                                }
                            });
        }
    }
}
