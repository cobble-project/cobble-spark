package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.table.LogicalTypes;
import io.cobble.table.TimestampKind;

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
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
        options.put("bucket", "2");
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
    public void externalPathOptionIsRejected() throws Exception {
        SparkCatalog catalog = newCatalog();
        Path external = Files.createTempDirectory("cobble-outside");
        Map<String, String> properties = new HashMap<>();
        properties.put(CobbleOptions.PRIMARY_KEY, "id");
        properties.put(CobbleOptions.BUCKET, "2");
        properties.put(CobbleOptions.PATH, external.toUri().toString());

        catalog.createNamespace(new String[] {"db"}, Collections.emptyMap());

        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "t"),
                                schema(),
                                new Transform[0],
                                properties));

        // No metadata or data is written to the user-supplied path.
        assertFalse(Files.exists(external.resolve("schema")));
        assertFalse(Files.exists(external.resolve("cobble-table.properties")));
        // Native FileCatalog owns the registration; no Spark sidecar properties are created.
        assertFalse(Files.exists(warehouse.resolve("db/t/cobble-table.properties")));
        assertFalse(catalog.tableExists(Identifier.of(new String[] {"db"}, "t")));
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
                UnsupportedOperationException.class,
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
    public void namespaceMetadataAndPurgeAreExplicitlyUnsupported() throws Exception {
        SparkCatalog catalog = newCatalog();
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        catalog.createNamespace(
                                new String[] {"db"},
                                Collections.singletonMap(
                                        TableCatalog.PROP_COMMENT, "not persisted")));

        catalog.createNamespace(
                new String[] {"db"}, Collections.singletonMap(TableCatalog.PROP_OWNER, "spark"));
        assertTrue(catalog.loadNamespaceMetadata(new String[] {"db"}).isEmpty());

        Map<String, String> commentProperties = properties();
        commentProperties.put(TableCatalog.PROP_COMMENT, "not persisted");
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "commented"),
                                schema(),
                                new Transform[0],
                                commentProperties));

        Identifier table = Identifier.of(new String[] {"db"}, "t");
        catalog.createTable(table, schema(), new Transform[0], properties());
        assertThrows(UnsupportedOperationException.class, () -> catalog.purgeTable(table));
    }

    @Test
    public void duplicateCreateMapsToSparkException() throws Exception {
        SparkCatalog catalog = newCatalog();
        catalog.createNamespace(new String[] {"db"}, Collections.emptyMap());
        Identifier table = Identifier.of(new String[] {"db"}, "t");
        catalog.createTable(table, schema(), new Transform[0], properties());

        assertThrows(
                TableAlreadyExistsException.class,
                () -> catalog.createTable(table, schema(), new Transform[0], properties()));
    }

    @Test
    public void catalogRuntimeAndOperationOptionsAreValidated() throws Exception {
        Map<String, String> unknown = new HashMap<>();
        unknown.put(CobbleOptions.PATH, warehouse.toUri().toString());
        unknown.put("write-task", "2");
        assertThrows(UnsupportedOperationException.class, () -> initializeCatalog(unknown));

        Map<String, String> retained = new HashMap<>();
        retained.put(CobbleOptions.PATH, warehouse.toUri().toString());
        retained.put(CobbleOptions.SNAPSHOT_RETENTION, "1");
        assertThrows(UnsupportedOperationException.class, () -> initializeCatalog(retained));

        Map<String, String> oversizedBuffer = new HashMap<>();
        oversizedBuffer.put(CobbleOptions.PATH, warehouse.toUri().toString());
        oversizedBuffer.put(CobbleOptions.WRITE_BUFFER_MEMORY, "3g");
        assertNotNull(initializeCatalog(oversizedBuffer));

        SparkCatalog catalog = newCatalog();
        catalog.createNamespace(new String[] {"db"}, Collections.emptyMap());
        CobbleTable table =
                (CobbleTable)
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "t"),
                                schema(),
                                new Transform[0],
                                properties());

        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        table.newScanBuilder(
                                new CaseInsensitiveStringMap(
                                        Collections.singletonMap(
                                                CobbleOptions.SNAPSHOT_RETENTION, "1"))));
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        table.newWriteBuilder(
                                        new LogicalWriteInfo() {
                                            @Override
                                            public CaseInsensitiveStringMap options() {
                                                return new CaseInsensitiveStringMap(
                                                        Collections.singletonMap(
                                                                CobbleOptions.SNAPSHOT_ID, "0"));
                                            }

                                            @Override
                                            public String queryId() {
                                                return "snapshot-write";
                                            }

                                            @Override
                                            public StructType schema() {
                                                return schema();
                                            }
                                        })
                                .build());

        Map<String, String> foreignProvider = properties();
        foreignProvider.put(TableCatalog.PROP_PROVIDER, "parquet");
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "foreign"),
                                schema(),
                                new Transform[0],
                                foreignProvider));
    }

    @Test
    public void createOptionsNormalizeCaseAndRejectConflicts() throws Exception {
        SparkCatalog catalog = newCatalog();
        catalog.createNamespace(new String[] {"db"}, Collections.emptyMap());
        Map<String, String> upperCaseKey = new HashMap<>();
        upperCaseKey.put("option.PRIMARY-KEY", "id");
        catalog.createTable(
                Identifier.of(new String[] {"db"}, "case_key"),
                schema(),
                new Transform[0],
                upperCaseKey);
        assertTrue(catalog.tableExists(Identifier.of(new String[] {"db"}, "case_key")));

        Map<String, String> conflictingKeys = new HashMap<>();
        conflictingKeys.put("PRIMARY-KEY", "id");
        conflictingKeys.put("option.primary-key", "other");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        catalog.createTable(
                                Identifier.of(new String[] {"db"}, "conflicting"),
                                schema(),
                                new Transform[0],
                                conflictingKeys));
    }

    @Test
    public void unsupportedNativeTimestampShapesAreRejectedRatherThanTruncated() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleSparkTypes.toSparkType(
                                LogicalTypes.timestamp(7, TimestampKind.WITH_LOCAL_TIME_ZONE)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleSparkTypes.toSparkType(
                                LogicalTypes.timestamp(6, TimestampKind.WITHOUT_TIME_ZONE)));
    }

    private SparkCatalog initializeCatalog(Map<String, String> options) {
        SparkCatalog catalog = new SparkCatalog();
        catalog.initialize("cobble", new CaseInsensitiveStringMap(options));
        return catalog;
    }
}
