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
                Paths.get(java.net.URI.create(CobbleOptions.normalizePathUri(warehouseValue)));
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
        if (namespace == null || namespace.length != 1) {
            throw new IllegalArgumentException(
                    "Cobble catalog supports single-level namespaces only, got "
                            + (namespace == null ? "null" : String.join(".", namespace)));
        }
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
        if (namespace == null || namespace.length != 1) {
            return false;
        }
        return Files.isDirectory(namespaceDir(namespace));
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
        requireNamespace(ident);
        File tableDir = tableDir(ident).toFile();
        if (!tableDir.isDirectory()) {
            throw new NoSuchTableException(ident);
        }
        Map<String, String> properties = loadTableProperties(ident);
        CobbleOptions.CobbleTableConfig config = CobbleOptions.parse(properties);
        return new CobbleTable(config, null, properties);
    }

    @Override
    public boolean tableExists(Identifier ident) {
        if (ident.namespace().length != 1) {
            return false;
        }
        return Files.isDirectory(tableDir(ident));
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        requireNamespace(ident);
        Path tableDir = tableDir(ident);
        if (Files.exists(tableDir)) {
            throw new TableAlreadyExistsException(ident);
        }
        String db = ident.namespace()[0];
        try {
            Files.createDirectories(tableDir);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create table " + ident.name(), e);
        }

        Map<String, String> tableProperties = new HashMap<>();
        if (properties != null) {
            tableProperties.putAll(properties);
        }
        tableProperties.put(TableCatalog.PROP_PROVIDER, PROVIDER);
        tableProperties.put(TableCatalog.PROP_LOCATION, tableDir.toUri().toString());
        if (!tableProperties.containsKey(CobbleOptions.PATH)) {
            tableProperties.put(CobbleOptions.PATH, tableDir.toUri().toString());
        }
        String rawPrimaryKey = tableProperties.get(CobbleOptions.PRIMARY_KEY);
        List<String> primaryKeys =
                CobbleTableSchema.parsePrimaryKeyOption(rawPrimaryKey == null ? "" : rawPrimaryKey);
        // Spark SQL DDL columns default to nullable; primary key columns are implicitly NOT NULL.
        StructType effectiveSchema = forcePrimaryKeysNotNull(schema, primaryKeys);
        try {
            CobbleTableSchema stored =
                    CobbleTableSchema.fromStructType(effectiveSchema, primaryKeys);
            int buckets =
                    tableProperties.containsKey(CobbleOptions.BUCKET)
                            ? Integer.parseInt(tableProperties.get(CobbleOptions.BUCKET))
                            : CobbleOptions.DEFAULT_BUCKET;
            stored.totalBuckets = buckets;
            storeTableProperties(tableDir, tableProperties);
            // Publish the schema sidecar now so the empty table is introspectable.
            CobbleTableSchema.store(tableDir.toUri().toString(), 0L, stored);
        } catch (IOException | NumberFormatException e) {
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
        CobbleOptions.CobbleTableConfig config = CobbleOptions.parse(tableProperties);
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
        if (ident.namespace().length != 1) {
            return false;
        }
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
        requireTableExists(stripCatalogPrefix(oldIdent));
        // Moving the table directory would break the absolute data file paths recorded inside the
        // committed snapshots, so renames are not supported yet.
        throw new UnsupportedOperationException(
                "ALTER TABLE ... RENAME is not supported for the Cobble catalog yet.");
    }

    private Identifier stripCatalogPrefix(Identifier ident) {
        String[] namespace = ident.namespace();
        if (namespace.length > 1 && namespace[0].equals(name)) {
            String[] stripped = new String[namespace.length - 1];
            System.arraycopy(namespace, 1, stripped, 0, stripped.length);
            return Identifier.of(stripped, ident.name());
        }
        return ident;
    }

    private Path namespaceDir(String[] namespace) {
        return warehouse.resolve(namespace[0]);
    }

    private Path tableDir(Identifier ident) {
        return warehouse.resolve(ident.namespace()[0]).resolve(ident.name());
    }

    private void requireNamespace(Identifier ident) {
        if (ident == null || ident.namespace().length != 1) {
            throw new IllegalArgumentException(
                    "Cobble catalog tables require a single database namespace, got "
                            + (ident == null ? "null" : ident.toString()));
        }
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
