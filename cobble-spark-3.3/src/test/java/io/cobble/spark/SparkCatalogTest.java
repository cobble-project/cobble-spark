package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.spark.write.CobbleShardResult;
import io.cobble.spark.write.CobbleShardWriteTask;
import io.cobble.spark.write.CobbleTableCommitter;
import io.cobble.spark.write.CobbleWriteContext;
import io.cobble.table.CatalogTable;
import io.cobble.table.FileCatalog;
import io.cobble.table.Table;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableSchema;
import io.cobble.table.TableWritePlan;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
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
        GlobalSnapshot base;
        try (FileCatalog catalog = openNativeCatalog()) {
            catalog.createNamespace(identifier.namespace());
            try (CatalogTable table = catalog.createTable(identifier, nativeSchema())) {
                config = catalogConfig(table);
                base =
                        CobbleTableCommitter.commit(
                                config, writeCatalogShards(config, null, 1, "base"), null);
            }
        }

        CobbleTableCommitter.commit(config, writeCatalogShards(config, base, 2, "advanced"), base);
        GlobalSnapshot current = CobbleTableRuntime.loadSnapshot(config);
        assertTrue(current.id > base.id);

        assertThrows(
                java.io.IOException.class,
                () ->
                        CobbleTableCommitter.commit(
                                config, writeCatalogShards(config, base, 3, "stale"), base));
        assertEquals(current.id, CobbleTableRuntime.loadSnapshot(config).id);
    }

    @Test
    public void serializedWritePlanOpensBucketsAfterCatalogRename() throws Exception {
        TableIdentifier original = nativeIdentifier("portable_write", "before");
        TableIdentifier renamed = nativeIdentifier("portable_write", "after");
        CobbleOptions.CobbleTableConfig config;
        try (FileCatalog catalog = openNativeCatalog()) {
            catalog.createNamespace(original.namespace());
            try (CatalogTable table = catalog.createTable(original, nativeSchema())) {
                config = catalogConfig(table);
            }
        }

        TableWritePlan plan = CobbleTableRuntime.buildWritePlan(config, 4);
        CobbleWriteContext context =
                new CobbleWriteContext(
                        config,
                        CobbleTableSchema.fromTableSchema(config.catalogReference().schema(), 4),
                        4,
                        2,
                        false,
                        null,
                        plan);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(context);
        }
        try (FileCatalog catalog = openNativeCatalog()) {
            try (CatalogTable ignored = catalog.renameTable(original, renamed.name())) {
                // The worker must use the definition captured under the original name.
            }
        }
        assertThrows(
                IllegalStateException.class, () -> CobbleTableRuntime.buildWritePlan(config, 4));

        CobbleWriteContext workerContext;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            workerContext = (CobbleWriteContext) input.readObject();
        }
        List<CobbleShardResult> written = new ArrayList<CobbleShardResult>();
        CobbleShardWriteTask.writeBuckets(
                        0,
                        Collections.<scala.Tuple2<Integer, Row>>emptyList().iterator(),
                        workerContext)
                .forEachRemaining(written::add);
        assertEquals(2, written.size());
        assertEquals(0, written.get(0).bucketId());
        assertEquals(1, written.get(1).bucketId());
    }

    @Test
    public void schemaEvolutionReadsPinnedSourceWithLatestFieldIdsAndHistoricalVersion()
            throws Exception {
        spark.sql("CREATE DATABASE cobble.evolve_read");
        spark.sql(
                "CREATE TABLE cobble.evolve_read.scores (id INT, old_name STRING, score INT) "
                        + "USING cobble OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.evolve_read.scores VALUES (1, 'before', 7)");
        long oldSnapshotId = currentSnapshotId("evolve_read", "scores");
        SparkCatalog catalog = directCatalog();
        CobbleTable historical =
                (CobbleTable)
                        catalog.loadTable(
                                Identifier.of(new String[] {"evolve_read"}, "scores"),
                                Long.toString(oldSnapshotId));
        assertEquals(
                Collections.singleton(
                        org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ),
                historical.capabilities());
        assertThrows(
                UnsupportedOperationException.class,
                () -> historical.newWriteBuilder(logicalWriteInfo()).build());

        // These catalog edits do not rewrite the snapshot. The reader must decode it using its
        // source schema, then map field ids to the latest schema.
        spark.sql("ALTER TABLE cobble.evolve_read.scores ADD COLUMNS (added STRING)");
        // No write has occurred after ADD: the fixed old snapshot already exposes the new nullable
        // field as null.
        Row afterAdd =
                spark.sql("SELECT old_name, added FROM cobble.evolve_read.scores WHERE id = 1")
                        .first();
        assertEquals("before", afterAdd.getString(0));
        assertNull(afterAdd.get(1));
        // Only the new key receives the added field. The old physical row must remain null.
        spark.sql("INSERT INTO cobble.evolve_read.scores VALUES (2, 'before-two', 8, 'present')");
        assertEquals(
                "present",
                spark.sql("SELECT added FROM cobble.evolve_read.scores WHERE id = 2")
                        .first()
                        .getString(0));

        spark.sql("ALTER TABLE cobble.evolve_read.scores RENAME COLUMN old_name TO renamed");
        assertEquals(
                "before",
                spark.sql("SELECT renamed FROM cobble.evolve_read.scores WHERE id = 1")
                        .first()
                        .getString(0));

        spark.sql("ALTER TABLE cobble.evolve_read.scores ALTER COLUMN score TYPE BIGINT");
        assertEquals(
                7L,
                spark.sql("SELECT score FROM cobble.evolve_read.scores WHERE id = 1")
                        .first()
                        .getLong(0));

        // A retired id must not leak into a later column that reuses its name.
        spark.sql("ALTER TABLE cobble.evolve_read.scores DROP COLUMN renamed");
        spark.sql("ALTER TABLE cobble.evolve_read.scores ADD COLUMNS (renamed STRING)");
        assertNull(
                spark.sql("SELECT renamed FROM cobble.evolve_read.scores WHERE id = 1")
                        .first()
                        .get(0));

        // Exercise the native 2 -> 1 -> 3 writer recovery chain against the evolved catalog
        // schema. Each catalog alias creates its own writer task count over the same table.
        configureCatalogAlias("cobble_one", 1);
        spark.sql("INSERT INTO cobble_one.evolve_read.scores VALUES (3, 9L, 'three', 'new-three')");
        configureCatalogAlias("cobble_three", 3);
        spark.sql(
                "INSERT INTO cobble_three.evolve_read.scores VALUES (4, 10L, 'four', 'new-four')");
        List<Row> latest =
                spark.sql(
                                "SELECT id, score, added, renamed FROM cobble.evolve_read.scores "
                                        + "ORDER BY id")
                        .collectAsList();
        assertEquals(4, latest.size());
        assertEquals(7L, latest.get(0).getLong(1));
        assertNull(latest.get(0).get(2));
        assertNull(latest.get(0).get(3));
        assertEquals("present", latest.get(1).getString(2));
        assertNull(latest.get(1).get(3));
        assertEquals("new-three", latest.get(2).getString(3));
        assertEquals("new-four", latest.get(3).getString(3));

        assertEquals(
                Arrays.asList("id", "old_name", "score"),
                Arrays.asList(historical.schema().fieldNames()));
        assertEquals("before", firstString(historical, 1));
    }

    @Test
    public void unsupportedAlterBatchDoesNotPartiallyUpdateNativeSchema() throws Exception {
        SparkCatalog catalog = directCatalog();
        String[] namespace = new String[] {"evolve_atomic"};
        catalog.createNamespace(namespace, Collections.emptyMap());
        Identifier identifier = Identifier.of(namespace, "scores");
        catalog.createTable(
                identifier,
                sparkSchema(),
                new org.apache.spark.sql.connector.expressions.Transform[0],
                primaryKeyProperties());
        long schemaId = nativeCatalogSchemaId("evolve_atomic", "scores");

        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        catalog.alterTable(
                                identifier,
                                TableChange.addColumn(
                                        new String[] {"would_have_been_added"},
                                        DataTypes.StringType,
                                        true),
                                TableChange.addColumn(
                                        new String[] {"nested", "child"},
                                        DataTypes.StringType,
                                        true)));
        assertEquals(schemaId, nativeCatalogSchemaId("evolve_atomic", "scores"));
        try (FileCatalog nativeCatalog = openNativeCatalog();
                CatalogTable table =
                        nativeCatalog.loadTable(nativeIdentifier("evolve_atomic", "scores"))) {
            assertEquals(
                    Arrays.asList("id", "v"),
                    table.schema().fields().stream()
                            .map(io.cobble.table.DataField::name)
                            .collect(java.util.stream.Collectors.toList()));
        }
    }

    @Test
    public void catalogAlterBatchWithNarrowingLeavesNativeSchemaUntouched() throws Exception {
        String[] namespace = new String[] {"evolve_native_atomic"};
        Identifier identifier = Identifier.of(namespace, "scores");
        StructType schema =
                DataTypes.createStructType(
                        new org.apache.spark.sql.types.StructField[] {
                            DataTypes.createStructField("id", DataTypes.IntegerType, false),
                            DataTypes.createStructField("score", DataTypes.IntegerType, true)
                        });
        SparkCatalog catalog = directCatalog();
        catalog.createNamespace(namespace, Collections.emptyMap());
        catalog.createTable(
                identifier,
                schema,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                primaryKeyProperties());
        long schemaId = nativeCatalogSchemaId("evolve_native_atomic", "scores");
        assertThrows(
                RuntimeException.class,
                () ->
                        catalog.alterTable(
                                identifier,
                                TableChange.addColumn(
                                        new String[] {"valid_prefix"}, DataTypes.StringType, true),
                                TableChange.updateColumnType(
                                        new String[] {"score"}, DataTypes.ByteType)));
        try (FileCatalog nativeCatalog = openNativeCatalog();
                CatalogTable unchanged =
                        nativeCatalog.loadTable(
                                nativeIdentifier("evolve_native_atomic", "scores"))) {
            assertEquals(schemaId, unchanged.catalogSchemaId());
            assertEquals(
                    Arrays.asList("id", "score"),
                    unchanged.schema().fields().stream()
                            .map(io.cobble.table.DataField::name)
                            .collect(java.util.stream.Collectors.toList()));
        }
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

    private static long nativeCatalogSchemaId(String namespace, String table) {
        try (FileCatalog catalog = openNativeCatalog();
                CatalogTable nativeTable = catalog.loadTable(nativeIdentifier(namespace, table))) {
            return nativeTable.catalogSchemaId();
        }
    }

    private static long currentSnapshotId(String namespace, String table) {
        try (FileCatalog catalog = openNativeCatalog();
                CatalogTable nativeTable = catalog.loadTable(nativeIdentifier(namespace, table));
                DbCoordinator coordinator = nativeTable.coordinator(nativeRuntime())) {
            return coordinator.loadCurrentGlobalSnapshot().id;
        }
    }

    private static void configureCatalogAlias(String alias, int writerCount) {
        spark.conf().set("spark.sql.catalog." + alias, SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog." + alias + ".path", warehouse.toUri().toString());
        spark.conf().set("spark.sql.catalog." + alias + ".bucket", "4");
        spark.conf()
                .set("spark.sql.catalog." + alias + ".write.tasks", Integer.toString(writerCount));
    }

    private static String firstString(CobbleTable table, int ordinal) throws Exception {
        org.apache.spark.sql.connector.read.Batch batch =
                table.newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap()))
                        .build()
                        .toBatch();
        org.apache.spark.sql.connector.read.PartitionReaderFactory factory =
                batch.createReaderFactory();
        for (org.apache.spark.sql.connector.read.InputPartition partition :
                batch.planInputPartitions()) {
            try (org.apache.spark.sql.connector.read.PartitionReader<
                            org.apache.spark.sql.catalyst.InternalRow>
                    reader = factory.createReader(partition)) {
                if (reader.next()) {
                    return reader.get().getUTF8String(ordinal).toString();
                }
            }
        }
        throw new AssertionError("Historical Cobble table had no rows.");
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

    /** Produces one fresh, isolated database snapshot for every physical catalog bucket. */
    private static List<CobbleShardResult> writeCatalogShards(
            CobbleOptions.CobbleTableConfig config, GlobalSnapshot base, int id, String value) {
        CobbleCatalogReference reference = config.catalogReference();
        CobbleTableSchema schema = CobbleTableSchema.fromTableSchema(reference.schema(), 4);
        Row row = RowFactory.create(id, value);
        CobbleSparkRowConverter converter = new CobbleSparkRowConverter(schema);
        int valueBucket = converter.bucket(row);
        List<CobbleShardResult> results = new ArrayList<CobbleShardResult>(4);
        try (FileCatalog catalog =
                        FileCatalog.open(
                                new Config().addVolume(reference.warehouse()),
                                reference.storageId());
                CatalogTable catalogTable = catalog.loadTable(reference.identifier())) {
            reference.validate(catalogTable);
            for (int bucket = 0; bucket < 4; bucket++) {
                Config runtime = CobblePaths.createWriterRuntimeConfig(4, 1);
                runtime.dataFileType = config.dataFileType();
                Config.VolumeDescriptor primary = new Config.VolumeDescriptor();
                primary.baseDir = reference.warehouse();
                primary.kinds =
                        Collections.singletonList(
                                Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
                runtime.addVolume(primary);
                ShardSnapshot source = base == null ? null : sourceForBucket(base, bucket);
                try (Table table =
                        source == null
                                ? catalogTable.writerBuilder(runtime).bucket(bucket).open()
                                : catalogTable
                                        .writerBuilder(runtime)
                                        .bucket(bucket)
                                        .resumeFromSnapshot(source.snapshotId)) {
                    if (bucket == valueBucket) table.put(converter.toValues(row));
                    results.add(new CobbleShardResult(4, bucket, table.snapshot()));
                }
            }
        }
        return results;
    }

    private static ShardSnapshot sourceForBucket(GlobalSnapshot snapshot, int bucket) {
        for (ShardSnapshot shard : snapshot.shardSnapshots) {
            if (shard.ranges.size() == 1
                    && shard.ranges.get(0).start == bucket
                    && shard.ranges.get(0).end == bucket) return shard;
        }
        throw new AssertionError("Missing single-bucket source shard " + bucket);
    }
}
