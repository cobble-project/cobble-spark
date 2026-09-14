package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.spark.write.CobbleShardResult;
import io.cobble.spark.write.CobbleTableCommitter;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.Table;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableSchema;
import io.cobble.table.TableWritePlan;
import io.cobble.table.Value;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** End-to-end tests for the filesystem-backed {@link SparkCatalog}. */
public class SparkCatalogTest {

    private static SparkSession spark;

    @TempDir static Path warehouse;

    @BeforeAll
    public static void setUp() {
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("cobble-spark-catalog-it")
                        .config("spark.sql.shuffle.partitions", 2)
                        .config("spark.ui.enabled", false)
                        .config("spark.driver.bindAddress", "127.0.0.1")
                        .config("spark.driver.host", "127.0.0.1")
                        .config("spark.sql.catalog.cobble", "io.cobble.spark.SparkCatalog")
                        .config("spark.sql.catalog.cobble.path", warehouse.toUri().toString())
                        .config("spark.sql.catalog.cobble.bucket", 4)
                        .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void createInsertSelectRoundTrip() {
        spark.sql("CREATE DATABASE cobble.db1");
        spark.sql(
                "CREATE TABLE cobble.db1.t1 (id INT, name STRING, amount DECIMAL(10,2))"
                        + " USING cobble OPTIONS ('primary-key'='id', 'bucket'='4')");
        spark.sql("INSERT INTO cobble.db1.t1 VALUES (1, 'alice', 10.50), (2, 'bob', 20.25)");

        List<Row> rows =
                spark.sql("SELECT id, name, amount FROM cobble.db1.t1 ORDER BY id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals("alice", rows.get(0).getString(1));
        assertEquals(0, new BigDecimal("10.50").compareTo(rows.get(0).getDecimal(2)));
        assertEquals("bob", rows.get(1).getString(1));
    }

    @Test
    public void catalogTableResolvesPathFromTableProperties() {
        spark.sql("CREATE DATABASE cobble.db2");
        spark.sql(
                "CREATE TABLE cobble.db2.t2 (id INT, name STRING)"
                        + " USING cobble OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.db2.t2 VALUES (7, 'seven')");

        // No path option is needed: the table properties carry it.
        List<Row> rows = spark.sql("SELECT name FROM cobble.db2.t2").collectAsList();
        assertEquals(1, rows.size());
        assertEquals("seven", rows.get(0).getString(0));
        assertEquals("seven", spark.read().table("cobble.db2.t2").first().getString(1));
    }

    @Test
    public void createTableAsSelect() {
        spark.sql("CREATE DATABASE cobble.db3");
        spark.sql(
                "CREATE TABLE cobble.db3.src (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.db3.src VALUES (1, 'a'), (2, 'b')");

        spark.sql(
                "CREATE TABLE cobble.db3.copy USING cobble"
                        + " OPTIONS ('primary-key'='id') AS SELECT * FROM cobble.db3.src");
        List<Row> rows = spark.sql("SELECT v FROM cobble.db3.copy ORDER BY id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).getString(0));
        assertEquals("b", rows.get(1).getString(0));
    }

    @Test
    public void upsertThroughCatalog() {
        spark.sql("CREATE DATABASE cobble.db4");
        spark.sql(
                "CREATE TABLE cobble.db4.t4 (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.db4.t4 VALUES (1, 'first')");
        spark.sql("INSERT INTO cobble.db4.t4 VALUES (1, 'second'), (2, 'two')");

        List<Row> rows = spark.sql("SELECT v FROM cobble.db4.t4 ORDER BY id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals("second", rows.get(0).getString(0));
        assertEquals("two", rows.get(1).getString(0));
    }

    @Test
    public void overwriteThroughInsertOverwrite() {
        spark.sql("CREATE DATABASE cobble.db5");
        spark.sql(
                "CREATE TABLE cobble.db5.t5 (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.db5.t5 VALUES (1, 'a'), (2, 'b')");

        spark.sql("INSERT OVERWRITE cobble.db5.t5 VALUES (9, 'z')");

        List<Row> rows = spark.sql("SELECT * FROM cobble.db5.t5").collectAsList();
        assertEquals(1, rows.size());
        assertEquals(9, rows.get(0).getInt(0));
        assertEquals("z", rows.get(0).getString(1));
    }

    @Test
    public void renameAndDropTable() {
        spark.sql("CREATE DATABASE cobble.db6");
        spark.sql(
                "CREATE TABLE cobble.db6.t6 (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.db6.t6 VALUES (1, 'a')");

        spark.sql("ALTER TABLE cobble.db6.t6 RENAME TO cobble.db6.t6b");
        List<Row> rows = spark.sql("SELECT v FROM cobble.db6.t6b").collectAsList();
        assertEquals(1, rows.size());

        spark.sql("DROP TABLE cobble.db6.t6b");
        assertThrows(Exception.class, () -> spark.sql("SELECT * FROM cobble.db6.t6"));
    }

    @Test
    public void dropNamespace() {
        spark.sql("CREATE DATABASE cobble.db7");
        spark.sql(
                "CREATE TABLE cobble.db7.t7 (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        // Non-empty namespace without cascade is rejected.
        assertThrows(Exception.class, () -> spark.sql("DROP DATABASE cobble.db7"));
        spark.sql("DROP TABLE cobble.db7.t7");
        spark.sql("DROP DATABASE cobble.db7");
        assertFalse(spark.catalog().databaseExists("cobble.db7"));
    }

    @Test
    public void createTableFailsWithoutPrimaryKey() {
        spark.sql("CREATE DATABASE cobble.db8");
        assertThrows(
                Exception.class,
                () -> spark.sql("CREATE TABLE cobble.db8.t8 (id INT, v STRING)" + " USING cobble"));
    }

    @Test
    public void emptyTableReadsZeroRows() {
        spark.sql("CREATE DATABASE cobble.dbEmpty");
        spark.sql(
                "CREATE TABLE cobble.dbEmpty.t (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        // No committed snapshot yet: a freshly created table reads as zero rows.
        assertEquals(0, spark.sql("SELECT * FROM cobble.dbEmpty.t").count());
        assertEquals(0, spark.read().table("cobble.dbEmpty.t").count());
    }

    @Test
    public void nativeCreatedTableAcceptsSparkWriteAndRead() throws Exception {
        TableIdentifier identifier = nativeIdentifier("native_created", "scores");
        try (FileCatalog catalog = openNativeCatalog()) {
            catalog.createNamespace(identifier.namespace());
            try (CatalogTable table = catalog.createTable(identifier, nativeSchema())) {
                assertTrue(table.tableId() > 0L);
            }
        }

        spark.sql("INSERT INTO cobble.native_created.scores VALUES (1, 'one')");
        assertEquals(
                "one",
                spark.sql("SELECT v FROM cobble.native_created.scores WHERE id = 1")
                        .first()
                        .getString(0));

        try (FileCatalog catalog = openNativeCatalog();
                CatalogTable table = catalog.loadTable(identifier);
                DbCoordinator coordinator = table.coordinator(nativeRuntime())) {
            assertNotNull(coordinator.loadCurrentGlobalSnapshot());
        }
    }

    @Test
    public void sparkCreatedTableIsVisibleToNativeCatalogAndIsolated() throws Exception {
        spark.sql("CREATE DATABASE cobble.spark_native");
        spark.sql(
                "CREATE TABLE cobble.spark_native.left (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql(
                "CREATE TABLE cobble.spark_native.right (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.spark_native.left VALUES (1, 'left')");
        spark.sql("INSERT INTO cobble.spark_native.right VALUES (1, 'right')");

        assertEquals(
                "left",
                spark.sql("SELECT v FROM cobble.spark_native.left WHERE id = 1")
                        .first()
                        .getString(0));
        assertEquals(
                "right",
                spark.sql("SELECT v FROM cobble.spark_native.right WHERE id = 1")
                        .first()
                        .getString(0));

        try (FileCatalog catalog = openNativeCatalog();
                CatalogTable left = catalog.loadTable(nativeIdentifier("spark_native", "left"));
                CatalogTable right = catalog.loadTable(nativeIdentifier("spark_native", "right"))) {
            assertNotEquals(left.tableId(), right.tableId());
            assertEquals(2, left.schema().fields().size());
            assertEquals(2, right.schema().fields().size());
        }
    }

    @Test
    public void renameKeepsIdentityAndDropRecreateRejectsStaleCobbleTable() throws Exception {
        SparkCatalog catalog = directCatalog();
        String[] namespace = new String[] {"identity"};
        catalog.createNamespace(namespace, Collections.emptyMap());
        Identifier original = Identifier.of(namespace, "scores");
        Identifier renamed = Identifier.of(namespace, "renamed_scores");
        CobbleTable stale =
                (CobbleTable)
                        catalog.createTable(
                                original,
                                sparkSchema(),
                                new org.apache.spark.sql.connector.expressions.Transform[0],
                                primaryKeyProperties());
        assertEquals("identity.scores", stale.name());

        long originalId = nativeTableId("identity", "scores");
        catalog.renameTable(original, renamed);
        assertEquals(originalId, nativeTableId("identity", "renamed_scores"));
        catalog.dropTable(renamed);
        catalog.createTable(
                original,
                sparkSchema(),
                new org.apache.spark.sql.connector.expressions.Transform[0],
                primaryKeyProperties());
        assertNotEquals(originalId, nativeTableId("identity", "scores"));

        assertThrows(IllegalStateException.class, stale::schema);
        assertThrows(
                IllegalStateException.class,
                () -> stale.newWriteBuilder(logicalWriteInfo()).build());
    }

    @Test
    public void catalogStaleBaseCommitIsRejectedWithoutChangingCurrentSnapshot() throws Exception {
        TableIdentifier identifier = nativeIdentifier("stale_commit", "scores");
        CobbleOptions.CobbleTableConfig config;
        TableWritePlan plan;
        GlobalSnapshot base;
        try (FileCatalog catalog = openNativeCatalog()) {
            catalog.createNamespace(identifier.namespace());
            try (CatalogTable table = catalog.createTable(identifier, nativeSchema())) {
                config = catalogConfig(table);
                plan = table.newWriteBuilder().totalBuckets(4).build();
                ShardSnapshot initial = writeShard(plan, null, 1, "base");
                try (io.cobble.table.TableSnapshotCommitter committer =
                        table.snapshotCommitter(nativeRuntime(), 1)) {
                    base = committer.commitBatch(1L, Collections.singletonList(initial));
                }
            }
        }

        ShardSnapshot advanced = writeShard(plan, base, 2, "advanced");
        CobbleTableCommitter.commit(
                config,
                Collections.singletonList(new CobbleShardResult(4, 0, null, advanced)),
                base,
                false);
        GlobalSnapshot current = CobbleTableRuntime.loadSnapshot(config);
        assertTrue(current.id > base.id);

        ShardSnapshot stale = writeShard(plan, base, 3, "stale");
        assertThrows(
                java.io.IOException.class,
                () ->
                        CobbleTableCommitter.commit(
                                config,
                                Collections.singletonList(new CobbleShardResult(4, 0, null, stale)),
                                base,
                                false));
        assertEquals(current.id, CobbleTableRuntime.loadSnapshot(config).id);
    }

    private static Config nativeRuntime() {
        return new Config().addVolume(warehouse.toUri().toString()).totalBuckets(4);
    }

    private static FileCatalog openNativeCatalog() {
        return FileCatalog.open(nativeRuntime(), "cobble");
    }

    private static TableIdentifier nativeIdentifier(String namespace, String table) {
        return new TableIdentifier(Collections.singletonList(namespace), table);
    }

    private static TableSchema nativeSchema() {
        return CobbleTableSchema.fromStructType(sparkSchema(), Collections.singletonList("id"), 4)
                .toTableSchema();
    }

    private static StructType sparkSchema() {
        return DataTypes.createStructType(
                new org.apache.spark.sql.types.StructField[] {
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("v", DataTypes.StringType, true)
                });
    }

    private static Map<String, String> primaryKeyProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CobbleOptions.PRIMARY_KEY, "id");
        return properties;
    }

    private static SparkCatalog directCatalog() {
        SparkCatalog catalog = new SparkCatalog();
        Map<String, String> options = new HashMap<>();
        options.put(CobbleOptions.PATH, warehouse.toUri().toString());
        options.put(CobbleOptions.BUCKET, "4");
        catalog.initialize("cobble", new CaseInsensitiveStringMap(options));
        return catalog;
    }

    private static long nativeTableId(String namespace, String table) {
        try (FileCatalog catalog = openNativeCatalog();
                CatalogTable nativeTable = catalog.loadTable(nativeIdentifier(namespace, table))) {
            return nativeTable.tableId();
        }
    }

    private static LogicalWriteInfo logicalWriteInfo() {
        return new LogicalWriteInfo() {
            @Override
            public CaseInsensitiveStringMap options() {
                return new CaseInsensitiveStringMap(Collections.emptyMap());
            }

            @Override
            public String queryId() {
                return "stale-catalog-table";
            }

            @Override
            public StructType schema() {
                return sparkSchema();
            }
        };
    }

    private static CobbleOptions.CobbleTableConfig catalogConfig(CatalogTable table) {
        CobbleCatalogReference reference =
                new CobbleCatalogReference(
                        warehouse.toUri().toString(),
                        "cobble",
                        table.identifier().namespace(),
                        table.identifier().name(),
                        table.tableId(),
                        table.catalogSchemaId(),
                        table.schema());
        Map<String, String> options = primaryKeyProperties();
        options.put(CobbleOptions.PATH, warehouse.toUri().toString());
        options.put(CobbleOptions.BUCKET, "4");
        return CobbleOptions.parse(options).withCatalogReference(reference);
    }

    private static ShardSnapshot writeShard(
            TableWritePlan plan, GlobalSnapshot base, int id, String value) {
        try (Table table =
                base == null
                        ? plan.writerBuilder(nativeRuntime())
                                .bucketRanges(new int[] {0}, new int[] {3})
                                .open()
                        : plan.writerBuilder(nativeRuntime())
                                .bucketRanges(new int[] {0}, new int[] {3})
                                .openNewFromGlobalSnapshot(base)) {
            table.put(Arrays.asList(Value.int32(id), Value.string(value)));
            return table.snapshot();
        }
    }
}
