package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOnlyDb;
import io.cobble.ShardSnapshot;
import io.cobble.spark.write.CobbleShardResult;
import io.cobble.spark.write.CobbleTableCommitter;
import io.cobble.table.ReadOnlyTable;
import io.cobble.table.Table;
import io.cobble.table.TableKey;
import io.cobble.table.TableScanPlan;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** End-to-end write/read tests against the real Cobble native engine on a local Spark session. */
public class CobbleSparkReadWriteTest {

    private static SparkSession spark;
    private static StructType schema;

    @TempDir Path tableDir;

    @BeforeAll
    public static void setUp() {
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("cobble-spark-it")
                        .config("spark.sql.shuffle.partitions", 2)
                        .config("spark.ui.enabled", false)
                        .config("spark.driver.bindAddress", "127.0.0.1")
                        .config("spark.driver.host", "127.0.0.1")
                        .getOrCreate();
        schema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField("name", DataTypes.StringType, true),
                                DataTypes.createStructField(
                                        "amount",
                                        new org.apache.spark.sql.types.DecimalType(10, 2),
                                        true),
                                DataTypes.createStructField("created", DataTypes.DateType, true),
                                DataTypes.createStructField(
                                        "updated", DataTypes.TimestampType, true),
                                DataTypes.createStructField("score", DataTypes.DoubleType, true)));
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private Row row(
            int id, String name, String amount, String created, String updated, double score) {
        return RowFactory.create(
                id,
                name,
                amount == null ? null : new BigDecimal(amount),
                created == null ? null : Date.valueOf(created),
                updated == null ? null : Timestamp.valueOf(updated),
                score);
    }

    private Dataset<Row> writeAndRead(List<Row> rows, int buckets, int tasks) {
        Dataset<Row> data = spark.createDataFrame(rows, schema);
        data.write()
                .format("cobble")
                .mode("append")
                .option("path", tableDir.toUri().toString())
                .option("primary-key", "id")
                .option("bucket", Integer.toString(buckets))
                .option("write.tasks", Integer.toString(tasks))
                .option("snapshot.retention", "10")
                .save();
        return spark.read().format("cobble").load(tableDir.toUri().toString());
    }

    @Test
    public void writeAndReadRoundTrip() {
        List<Row> rows =
                Arrays.asList(
                        row(1, "alice", "10.50", "2026-01-01", "2026-01-02 03:04:05.123456", 1.5d),
                        row(2, "bob", "-20.25", "2026-02-01", "2026-02-02 03:04:05.654321", -2.5d),
                        row(3, null, null, null, null, 0d),
                        row(4, "dave", "0.00", "2026-04-01", "2026-04-02 03:04:05", 100.125d));
        Dataset<Row> read = writeAndRead(rows, 4, 2);
        assertEquals(4, read.count());

        List<Row> sorted = read.orderBy(org.apache.spark.sql.functions.col("id")).collectAsList();
        assertEquals("alice", sorted.get(0).getString(1));
        assertEquals(0, new BigDecimal("10.50").compareTo(sorted.get(0).getDecimal(2)));
        assertEquals(Date.valueOf("2026-01-01"), sorted.get(0).getDate(3));
        assertEquals(
                Timestamp.valueOf("2026-01-02 03:04:05.123456"), sorted.get(0).getTimestamp(4));
        assertEquals(1.5d, sorted.get(0).getDouble(5), 0d);
        assertTrue(sorted.get(2).isNullAt(1));
        assertTrue(sorted.get(2).isNullAt(2));
        assertTrue(sorted.get(2).isNullAt(3));
        assertTrue(sorted.get(2).isNullAt(4));
        assertEquals("bob", sorted.get(1).getString(1));
        assertEquals("dave", sorted.get(3).getString(1));
    }

    @Test
    public void appendUpsertsByPrimaryKey() {
        List<Row> first =
                Arrays.asList(
                        row(1, "alice", "10.50", null, null, 0d),
                        row(2, "bob", "20.00", null, null, 0d));
        writeAndRead(first, 4, 2);

        // Same primary key 1 is updated; key 3 is appended.
        List<Row> second =
                Arrays.asList(
                        row(1, "alice-2", "99.99", null, null, 0d),
                        row(3, "carol", "30.00", null, null, 0d));
        Dataset<Row> read = writeAndRead(second, 4, 2);

        assertEquals(3, read.count());
        List<Row> sorted = read.orderBy(org.apache.spark.sql.functions.col("id")).collectAsList();
        assertEquals("alice-2", sorted.get(0).getString(1));
        assertEquals(0, new BigDecimal("99.99").compareTo(sorted.get(0).getDecimal(2)));
        assertEquals("bob", sorted.get(1).getString(1));
        assertEquals("carol", sorted.get(2).getString(1));
    }

    @Test
    public void timeTravelBySnapshotId() {
        List<Row> first = Collections.singletonList(row(1, "v1", "1.00", null, null, 0d));
        writeAndRead(first, 2, 1);
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(
                        Collections.singletonMap(CobbleOptions.PATH, tableDir.toUri().toString()));
        long firstSnapshotId = loadCurrentSnapshot(config).id;
        List<Row> second = Collections.singletonList(row(1, "v2", "2.00", null, null, 0d));
        writeAndRead(second, 2, 1);

        List<Row> latest =
                spark.read()
                        .format("cobble")
                        .option("path", tableDir.toUri().toString())
                        .load()
                        .orderBy(org.apache.spark.sql.functions.col("id"))
                        .collectAsList();
        assertEquals("v2", latest.get(0).getString(1));

        List<Row> historical =
                spark.read()
                        .format("cobble")
                        .option("path", tableDir.toUri().toString())
                        .option("snapshot-id", Long.toString(firstSnapshotId))
                        .load()
                        .orderBy(org.apache.spark.sql.functions.col("id"))
                        .collectAsList();
        assertEquals("v1", historical.get(0).getString(1));
    }

    @Test
    public void overwriteReplacesTableContent() {
        writeAndRead(
                Arrays.asList(row(1, "a", null, null, null, 0d), row(2, "b", null, null, null, 0d)),
                4,
                2);

        Dataset<Row> replacement =
                spark.createDataFrame(
                        Collections.singletonList(row(9, "z", null, null, null, 0d)), schema);
        replacement
                .write()
                .format("cobble")
                .mode("overwrite")
                .option("path", tableDir.toUri().toString())
                .option("bucket", "4")
                .option("write.tasks", "2")
                .save();

        Dataset<Row> read = spark.read().format("cobble").load(tableDir.toUri().toString());
        List<Row> rows = read.collectAsList();
        assertEquals(1, rows.size());
        assertEquals(9, rows.get(0).getInt(0));
        assertEquals("z", rows.get(0).getString(1));
    }

    @Test
    public void columnPruningOnKeyAndValueColumns() {
        writeAndRead(
                Arrays.asList(
                        row(1, "alice", "10.50", null, null, 1d),
                        row(2, "bob", "20.00", null, null, 2d)),
                4,
                2);

        Dataset<Row> read = spark.read().format("cobble").load(tableDir.toUri().toString());
        assertEquals(
                Arrays.asList(1, 2),
                read.select("id").orderBy("id").collectAsList().stream()
                        .map(r -> r.getInt(0))
                        .collect(java.util.stream.Collectors.toList()));
        assertEquals(
                Arrays.asList("alice", "bob"),
                read.select("name").orderBy("id").collectAsList().stream()
                        .map(r -> r.getString(0))
                        .collect(java.util.stream.Collectors.toList()));
        assertEquals(
                Arrays.asList("alice", "bob"),
                read.select("name", "id").orderBy("id").collectAsList().stream()
                        .map(r -> r.getString(0))
                        .collect(java.util.stream.Collectors.toList()));
        // Spark pushes an empty required schema for count(*). The reader still needs a hidden
        // existence column, but must not decode or expose a user column.
        assertEquals(2L, read.selectExpr("count(*)").collectAsList().get(0).getLong(0));
    }

    @Test
    public void keyOnlyTableSupportsWriteReadAndCount() {
        StructType keyOnlySchema =
                DataTypes.createStructType(
                        Collections.singletonList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false)));
        spark.createDataFrame(
                        Arrays.asList(
                                RowFactory.create(3), RowFactory.create(1), RowFactory.create(2)),
                        keyOnlySchema)
                .write()
                .format("cobble")
                .option("path", tableDir.toUri().toString())
                .option("primary-key", "id")
                .option("bucket", "2")
                .option("write.tasks", "1")
                .save();

        Dataset<Row> read = spark.read().format("cobble").load(tableDir.toUri().toString());
        assertEquals(
                Arrays.asList(1, 2, 3),
                read.orderBy("id").collectAsList().stream()
                        .map(row -> row.getInt(0))
                        .collect(java.util.stream.Collectors.toList()));
        assertEquals(3L, read.selectExpr("count(*)").first().getLong(0));
    }

    @Test
    public void directScanCopiesLargeBinaryAndNestedBinaryBeforeCursorAdvances() {
        int largeBytes = 3 * 1024 * 1024;
        String firstKey = "key-a-" + repeated('a', largeBytes);
        String secondKey = "key-b-" + repeated('b', largeBytes);
        byte[] firstPayload = new byte[largeBytes];
        byte[] firstNestedPayload = new byte[largeBytes];
        byte[] secondPayload = new byte[largeBytes];
        byte[] secondNestedPayload = new byte[largeBytes];
        for (int i = 0; i < largeBytes; i++) {
            firstPayload[i] = (byte) (i * 31);
            firstNestedPayload[i] = (byte) (i * 17);
            secondPayload[i] = (byte) (i * 7);
            secondNestedPayload[i] = (byte) (i * 13);
        }
        StructType nestedType =
                DataTypes.createStructType(
                        Collections.singletonList(
                                DataTypes.createStructField("blob", DataTypes.BinaryType, true)));
        StructType largeSchema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.StringType, false),
                                DataTypes.createStructField("payload", DataTypes.BinaryType, false),
                                DataTypes.createStructField("nested", nestedType, false)));
        spark.createDataFrame(
                        Arrays.asList(
                                RowFactory.create(
                                        firstKey,
                                        firstPayload,
                                        RowFactory.create(firstNestedPayload)),
                                RowFactory.create(
                                        secondKey,
                                        secondPayload,
                                        RowFactory.create(secondNestedPayload))),
                        largeSchema)
                .write()
                .format("cobble")
                .option("path", tableDir.toUri().toString())
                .option("primary-key", "id")
                .option("bucket", "2")
                .option("write.tasks", "1")
                .save();

        // One scan cursor advances across both rows before collectAsList returns, then closes.
        // These assertions prove root and nested binary leaves were copied out of its reusable
        // native I/O buffer rather than retaining dangling direct-buffer views.
        List<Row> read =
                spark.read().format("cobble").load(tableDir.toUri().toString()).collectAsList();
        assertEquals(2, read.size());
        for (Row row : read) {
            if (firstKey.equals(row.getString(0))) {
                assertArrayEquals(firstPayload, row.<byte[]>getAs(1));
                assertArrayEquals(firstNestedPayload, row.getStruct(2).<byte[]>getAs(0));
            } else if (secondKey.equals(row.getString(0))) {
                assertArrayEquals(secondPayload, row.<byte[]>getAs(1));
                assertArrayEquals(secondNestedPayload, row.getStruct(2).<byte[]>getAs(0));
            } else {
                throw new AssertionError("unexpected large binary row key");
            }
        }
    }

    private static String repeated(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    @Test
    public void tableScanPlanRemainsPinnedWhenDataIsAppended() throws Exception {
        writeAndRead(Collections.singletonList(row(1, "before", "1.00", null, null, 0d)), 2, 1);
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(
                        Collections.singletonMap(CobbleOptions.PATH, tableDir.toUri().toString()));
        GlobalSnapshot firstSnapshot = loadCurrentSnapshot(config);
        TableScanPlan plan = CobbleTableRuntime.loadScanPlan(config, firstSnapshot);
        CobbleTableSchema scanSchema =
                CobbleTableSchema.fromTableSchema(plan.schema(), plan.totalBuckets());

        writeAndRead(
                Arrays.asList(
                        row(1, "after", "2.00", null, null, 0d),
                        row(2, "new", "3.00", null, null, 0d)),
                2,
                1);
        assertTrue(loadCurrentSnapshot(config).id > plan.snapshotId());

        CobbleBatch batch =
                new CobbleBatch(config, scanSchema, scanSchema, scanSchema.toStructType(), plan);
        PartitionReaderFactory factory = batch.createReaderFactory();
        List<Integer> ids = new java.util.ArrayList<Integer>();
        List<String> names = new java.util.ArrayList<String>();
        for (InputPartition partition : batch.planInputPartitions()) {
            try (PartitionReader<InternalRow> reader = factory.createReader(partition)) {
                while (reader.next()) {
                    InternalRow row = reader.get();
                    ids.add(Integer.valueOf(row.getInt(0)));
                    names.add(row.getUTF8String(1).toString());
                }
            }
        }
        assertEquals(firstSnapshot.id, plan.snapshotId());
        assertEquals(Collections.singletonList(Integer.valueOf(1)), ids);
        assertEquals(Collections.singletonList("before"), names);
    }

    @Test
    public void sqlOverTempView() {
        writeAndRead(
                Arrays.asList(
                        row(1, "alice", "10.50", null, null, 1d),
                        row(2, "bob", "20.00", null, null, 2d),
                        row(3, "carol", "30.00", null, null, 3d)),
                4,
                2);

        spark.read()
                .format("cobble")
                .load(tableDir.toUri().toString())
                .createOrReplaceTempView("cobble_table");
        List<Row> aggregated =
                spark.sql(
                                "SELECT count(*) AS cnt, sum(amount) AS total FROM cobble_table"
                                        + " WHERE id >= 2")
                        .collectAsList();
        assertEquals(2, aggregated.get(0).getLong(0));
        assertEquals(0, new BigDecimal("50.00").compareTo(aggregated.get(0).getDecimal(1)));
    }

    @Test
    public void rejectsSchemaMismatchOnAppend() {
        writeAndRead(Collections.singletonList(row(1, "x", null, null, null, 0d)), 2, 1);

        StructType different =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField("label", DataTypes.StringType, true)));
        Dataset<Row> data =
                spark.createDataFrame(
                        Collections.singletonList(RowFactory.create(1, "y")), different);
        assertThrows(
                Exception.class,
                () ->
                        data.write()
                                .format("cobble")
                                .mode("append")
                                .option("path", tableDir.toUri().toString())
                                .save());
    }

    @Test
    public void rejectsPrimaryKeyMismatchOnAppend() {
        writeAndRead(Collections.singletonList(row(1, "x", null, null, null, 0d)), 2, 1);

        Dataset<Row> data =
                spark.createDataFrame(
                        Collections.singletonList(row(2, "y", null, null, null, 0d)), schema);
        assertThrows(
                Exception.class,
                () ->
                        data.write()
                                .format("cobble")
                                .mode("append")
                                .option("path", tableDir.toUri().toString())
                                .option("primary-key", "name")
                                .save());
    }

    @Test
    public void rejectsMissingPrimaryKeyOnCreate() {
        Dataset<Row> data =
                spark.createDataFrame(
                        Collections.singletonList(row(1, "x", null, null, null, 0d)), schema);
        assertThrows(
                Exception.class,
                () ->
                        data.write()
                                .format("cobble")
                                .option("path", tableDir.toUri().toString())
                                .save());
    }

    @Test
    public void nativeTableWrittenLikeFlinkIsReadableBySpark() {
        String pathUri = tableDir.toUri().toString();
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(Collections.singletonMap(CobbleOptions.PATH, pathUri));
        CobbleTableSchema nativeSchema =
                CobbleTableSchema.fromStructType(schema, Collections.singletonList("id"), 4);
        ShardSnapshot shard;
        try (io.cobble.Db db =
                io.cobble.Db.open(CobblePaths.createWriterConfig(config, 4, 0, 1), 0, 3)) {
            try (Table table =
                    Table.create(db, CobbleTableRuntime.TABLE_NAME, nativeSchema.toTableSchema())) {
                table.put(
                        new CobbleSparkRowConverter(nativeSchema)
                                .toValues(
                                        row(
                                                7,
                                                "from-flink-format",
                                                "12.34",
                                                "2026-08-27",
                                                "2026-08-27 12:34:56.123456",
                                                9.5d)));
            }
            shard = db.snapshot();
        }
        try (TableSnapshotCommitter committer =
                TableSnapshotCommitter.open(CobblePaths.createCoordinatorConfig(config, 4), 4, 1)) {
            assertTrue(committer.commitBatch(0L, Collections.singletonList(shard)) != null);
        }

        Row read = spark.read().format("cobble").load(pathUri).collectAsList().get(0);
        assertEquals(7, read.getInt(0));
        assertEquals("from-flink-format", read.getString(1));
        assertEquals(0, new BigDecimal("12.34").compareTo(read.getDecimal(2)));
    }

    @Test
    public void sparkTableIsReadableThroughNativeTableApiUsedByFlink() {
        writeAndRead(
                Collections.singletonList(row(11, "from-spark", "88.00", null, null, 3.25d)), 4, 1);
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(
                        Collections.singletonMap(CobbleOptions.PATH, tableDir.toUri().toString()));
        GlobalSnapshot snapshot = loadCurrentSnapshot(config);
        ShardSnapshot shard = snapshot.shardSnapshots.get(0);
        try (ReadOnlyDb db =
                        ReadOnlyDb.open(
                                CobblePaths.createScanConfig(config, 4, 5),
                                shard.snapshotId,
                                shard.dbId);
                ReadOnlyTable table = ReadOnlyTable.open(db, CobbleTableRuntime.TABLE_NAME)) {
            TableKey key = table.keyBuilder().push(Value.int32(11)).build();
            List<Value> values = table.get(key);
            assertEquals(Value.int32(11), values.get(0));
            assertEquals(Value.string("from-spark"), values.get(1));
        }
    }

    @Test
    public void nestedTypesRoundTripThroughNativeCodecs() {
        StructType nestedSchema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField(
                                        "items",
                                        DataTypes.createArrayType(DataTypes.IntegerType, true),
                                        true),
                                DataTypes.createStructField(
                                        "attrs",
                                        DataTypes.createMapType(
                                                DataTypes.StringType, DataTypes.LongType, true),
                                        true),
                                DataTypes.createStructField(
                                        "profile",
                                        DataTypes.createStructType(
                                                Arrays.asList(
                                                        DataTypes.createStructField(
                                                                "label",
                                                                DataTypes.StringType,
                                                                true),
                                                        DataTypes.createStructField(
                                                                "scores",
                                                                DataTypes.createArrayType(
                                                                        DataTypes.DoubleType,
                                                                        false),
                                                                false))),
                                        true)));
        Map<String, Long> attrs = new LinkedHashMap<String, Long>();
        attrs.put("a", 1L);
        attrs.put("nullable", null);
        Dataset<Row> data =
                spark.createDataFrame(
                        Collections.singletonList(
                                RowFactory.create(
                                        1,
                                        Arrays.asList(3, null, 5),
                                        attrs,
                                        RowFactory.create("nested", Arrays.asList(1.5d, 2.5d)))),
                        nestedSchema);
        data.write()
                .format("cobble")
                .option("path", tableDir.toUri().toString())
                .option("primary-key", "id")
                .option("bucket", "4")
                .save();

        Row read = spark.read().format("cobble").load(tableDir.toUri().toString()).first();
        assertEquals(Arrays.asList(3, null, 5), read.getList(1));
        assertEquals(Long.valueOf(1L), read.<String, Long>getJavaMap(2).get("a"));
        assertTrue(read.<String, Long>getJavaMap(2).containsKey("nullable"));
        assertEquals("nested", read.getStruct(3).getString(0));
        assertEquals(Arrays.asList(1.5d, 2.5d), read.getStruct(3).getList(1));
    }

    /**
     * Deterministic regression for the expected-base check: a job that started from an old snapshot
     * must be rejected at commit time without publishing a snapshot or touching the writer-path
     * index.
     */
    @Test
    public void staleBaseCommitIsRejectedAndLeavesNoMetadata() throws Exception {
        writeAndRead(Collections.singletonList(row(1, "v1", "1.00", null, null, 0d)), 4, 2);
        String pathUri = tableDir.toUri().toString();
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(Collections.singletonMap(CobbleOptions.PATH, pathUri));
        GlobalSnapshot staleBase = loadCurrentSnapshot(config);
        writeAndRead(Collections.singletonList(row(2, "v2", "2.00", null, null, 0d)), 4, 2);

        GlobalSnapshot current = loadCurrentSnapshot(config);
        CobbleTableSchema storedSchema = CobbleTableRuntime.loadSchema(config, current);
        assertTrue(current.id > staleBase.id);

        // The commit below claims to have been built on the already superseded first snapshot.
        CobbleShardResult staleResult = produceShardResult(config, storedSchema, 4);

        assertThrows(
                IOException.class,
                () ->
                        CobbleTableCommitter.commit(
                                config, Collections.singletonList(staleResult), staleBase, false));

        // The current snapshot is unchanged and the rejected shard left no writer-path index entry.
        assertEquals(current.id, loadCurrentSnapshot(config).id);
        Map<String, String> index = CobblePaths.loadWriterPathIndex(config);
        assertFalse(index.containsKey(staleResult.shardSnapshot().dbId));
    }

    private static GlobalSnapshot loadCurrentSnapshot(CobbleOptions.CobbleTableConfig config) {
        try (DbCoordinator coordinator =
                DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, 4))) {
            return coordinator.loadCurrentGlobalSnapshot();
        }
    }

    /** Opens a writer covering every bucket and returns its fresh shard snapshot as a result. */
    private static CobbleShardResult produceShardResult(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema tableSchema,
            int totalBuckets) {
        io.cobble.Db db =
                io.cobble.Db.open(
                        CobblePaths.createWriterConfig(config, totalBuckets, 0, 1),
                        0,
                        totalBuckets - 1);
        try {
            try (Table table =
                    Table.create(db, CobbleTableRuntime.TABLE_NAME, tableSchema.toTableSchema())) {
                table.put(
                        Arrays.asList(
                                Value.int32(999),
                                Value.nullValue(),
                                Value.nullValue(),
                                Value.nullValue(),
                                Value.nullValue(),
                                Value.float64(0d)));
            }
            ShardSnapshot shard = db.snapshot();
            return new CobbleShardResult(
                    totalBuckets, 0, CobblePaths.tableRoot(config).getAbsolutePath(), shard);
        } finally {
            db.close();
        }
    }
}
