package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Security tests for {@link SparkCatalog} path handling (no Spark session required). */
public class SparkCatalogSecurityTest {

    @TempDir Path warehouse;

    private SparkCatalog newCatalog() {
        SparkCatalog catalog = new SparkCatalog();
        Map<String, String> options = new HashMap<>();
        options.put("path", warehouse.toUri().toString());
        catalog.initialize("cobble", new CaseInsensitiveStringMap(options));
        return catalog;
    }

    private static StructType schema() {
        return DataTypes.createStructType(
                new org.apache.spark.sql.types.StructField[] {
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("v", DataTypes.StringType, true)
                });
    }

    private static Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CobbleOptions.PRIMARY_KEY, "id");
        return properties;
    }

    @Test
    public void loadTableRejectsEscapingNames() {
        SparkCatalog catalog = newCatalog();
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.loadTable(Identifier.of(new String[] {".."}, "x")));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.loadTable(Identifier.of(new String[] {"db"}, "..")));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.loadTable(Identifier.of(new String[] {"db/x"}, "t")));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.loadTable(Identifier.of(new String[] {"db"}, "t/evil")));
    }

    @Test
    public void dropTableRejectsEscapingNamesAndKeepsWarehouse() throws Exception {
        SparkCatalog catalog = newCatalog();
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.dropTable(Identifier.of(new String[] {".."}, "x")));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.dropTable(Identifier.of(new String[] {"db"}, "..")));
        assertTrue(Files.isDirectory(warehouse));
    }

    @Test
    public void createTableRejectsEscapingNames() {
        SparkCatalog catalog = newCatalog();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {".."}, "x"),
                                schema(),
                                new Transform[0],
                                properties()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "../x"),
                                schema(),
                                new Transform[0],
                                properties()));
    }

    @Test
    public void createNamespaceRejectsEscaping() {
        SparkCatalog catalog = newCatalog();
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.createNamespace(new String[] {".."}, Collections.emptyMap()));
        assertThrows(
                IllegalArgumentException.class,
                () -> catalog.createNamespace(new String[] {"a/b"}, Collections.emptyMap()));
    }

    @Test
    public void namespaceExistsReturnsFalseForEscapingName() {
        SparkCatalog catalog = newCatalog();
        assertFalse(catalog.namespaceExists(new String[] {".."}));
        assertFalse(catalog.namespaceExists(new String[] {"a/b"}));
    }

    @Test
    public void externalPathOptionIsOverridden() throws Exception {
        SparkCatalog catalog = newCatalog();
        Path external = Files.createTempDirectory("cobble-outside");
        Map<String, String> properties = new HashMap<>();
        properties.put(CobbleOptions.PRIMARY_KEY, "id");
        properties.put(CobbleOptions.BUCKET, "2");
        properties.put(CobbleOptions.PATH, external.toUri().toString());

        catalog.createTable(
                Identifier.of(new String[] {"db"}, "t"), schema(), new Transform[0], properties);

        // No metadata or data is written to the user-supplied path.
        assertFalse(Files.exists(external.resolve("schema")));
        assertFalse(Files.exists(external.resolve("cobble-table.properties")));
        // Everything lives under the warehouse-managed table directory.
        assertTrue(Files.isRegularFile(warehouse.resolve("db/t/cobble-table.properties")));
        assertTrue(catalog.tableExists(Identifier.of(new String[] {"db"}, "t")));
    }

    @Test
    public void failedCreateLeavesNoTableDirectory() {
        SparkCatalog catalog = newCatalog();
        // Missing primary key fails validation before any directory is created.
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "ghost"),
                                schema(),
                                new Transform[0],
                                Collections.emptyMap()));
        assertFalse(catalog.tableExists(Identifier.of(new String[] {"db"}, "ghost")));
        assertFalse(Files.exists(warehouse.resolve("db/ghost")));
        // The namespace itself was never created by the failed table creation.
        assertFalse(catalog.namespaceExists(new String[] {"db"}));
    }

    @Test
    public void failedCreateOnInvalidBucketLeavesNothing() {
        SparkCatalog catalog = newCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put(CobbleOptions.PRIMARY_KEY, "id");
        properties.put(CobbleOptions.BUCKET, "0");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "t"),
                                schema(),
                                new Transform[0],
                                properties));
        assertFalse(catalog.tableExists(Identifier.of(new String[] {"db"}, "t")));
        assertFalse(Files.exists(warehouse.resolve("db/t")));
        assertFalse(catalog.namespaceExists(new String[] {"db"}));
    }

    @Test
    public void failedCreateOnInvalidRetentionLeavesNothing() {
        SparkCatalog catalog = newCatalog();
        Map<String, String> properties = new HashMap<>();
        properties.put(CobbleOptions.PRIMARY_KEY, "id");
        properties.put(CobbleOptions.SNAPSHOT_RETENTION, "-1");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "t"),
                                schema(),
                                new Transform[0],
                                properties));
        assertFalse(catalog.tableExists(Identifier.of(new String[] {"db"}, "t")));
        assertFalse(Files.exists(warehouse.resolve("db/t")));
        assertFalse(catalog.namespaceExists(new String[] {"db"}));
    }
}
