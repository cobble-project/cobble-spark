package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.spark.write.CobbleShardResult;
import io.cobble.spark.write.CobbleTableCommitter;
import io.cobble.table.Table;
import io.cobble.table.TableScanCursor;
import io.cobble.table.TableScanPlan;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** End-to-end write/read tests against the real Cobble native engine on a local Spark session. */
public class CobbleSparkReadWriteTest {
    private static final ObjectMapper JSON = new ObjectMapper();

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
        return writeAndRead(rows, buckets, tasks, 0);
    }

    private Dataset<Row> writeAndRead(List<Row> rows, int buckets, int tasks, int retention) {
        Dataset<Row> data = spark.createDataFrame(rows, schema);
        data.write()
                .format("cobble")
                .mode("append")
                .option("path", tableDir.toUri().toString())
                .option("primary-key", "id")
                .option("bucket", Integer.toString(buckets))
                .option("write.tasks", Integer.toString(tasks))
                .option("snapshot.retention", Integer.toString(retention))
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
    public void writesOneDatabasePerBucketAcrossWriterCountsAndKeepsHistoricalData()
            throws Exception {
        List<Row> initial = rowsCoveringEveryBucket(8, "p8");
        writeAndRead(initial, 8, 8);
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(
                        Collections.singletonMap(CobbleOptions.PATH, tableDir.toUri().toString()));
        GlobalSnapshot first = loadCurrentSnapshot(config);
        assertSingleBucketSnapshot(first, 8, "parquet");

        List<Row> p4 = rowsWithNames(initial, "p4");
        writeAndRead(p4, 8, 4);
        GlobalSnapshot second = loadCurrentSnapshot(config);
        assertTrue(second.id > first.id);
        assertSingleBucketSnapshot(second, 8, "parquet");

        Row onlyUpdate = p4.get(0);
        writeAndRead(Collections.singletonList(namedRow(onlyUpdate.getInt(0), "p3")), 8, 3);
        GlobalSnapshot current = loadCurrentSnapshot(config);
        assertTrue(current.id > second.id);
        assertSingleBucketSnapshot(current, 8, "parquet");

        List<Row> latest =
                spark.read()
                        .format("cobble")
                        .load(tableDir.toUri().toString())
                        .orderBy("id")
                        .collectAsList();
        assertEquals(8, latest.size());
        assertEquals(
                "p3",
                latest.stream()
                        .filter(r -> r.getInt(0) == onlyUpdate.getInt(0))
                        .findFirst()
                        .get()
                        .getString(1));
        assertTrue(
                latest.stream()
                        .filter(r -> r.getInt(0) != onlyUpdate.getInt(0))
                        .allMatch(r -> "p4".equals(r.getString(1))));

        List<Row> historical =
                spark.read()
                        .format("cobble")
                        .option("path", tableDir.toUri().toString())
                        .option(CobbleOptions.SNAPSHOT_ID, Long.toString(first.id))
                        .load()
                        .collectAsList();
        assertEquals(8, historical.size());
        assertTrue(historical.stream().allMatch(r -> "p8".equals(r.getString(1))));
    }

    @Test
    public void sstDataFileOptionIsRecordedInEveryWrittenShardManifest() throws Exception {
        Path sstTable = tableDir.resolve("sst-table");
        spark.createDataFrame(Collections.singletonList(namedRow(1, "sst")), schema)
                .write()
                .format("cobble")
                .option("path", sstTable.toUri().toString())
                .option("primary-key", "id")
                .option("bucket", "2")
                .option(CobbleOptions.DATA_FILE_TYPE, "sst")
                .save();

        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(
                        Collections.singletonMap(CobbleOptions.PATH, sstTable.toUri().toString()));
        assertSingleBucketSnapshot(loadCurrentSnapshot(config), 2, "sst", sstTable);
    }

    @Test
    public void positiveSnapshotRetentionIsRejectedBeforeAnyBucketIsOpened() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> writeAndRead(Collections.singletonList(namedRow(1, "rejected")), 4, 2, 1));
        assertFalse(Files.exists(tableDir.resolve("bucket-0")));
    }

    @Test
    public void emptyAppendSnapshotsEveryBucketAndPreservesUntouchedRows() throws Exception {
        List<Row> initial = rowsCoveringEveryBucket(4, "kept");
        writeAndRead(initial, 4, 2);
        CobbleOptions.CobbleTableConfig config =
                CobbleOptions.parse(
                        Collections.singletonMap(CobbleOptions.PATH, tableDir.toUri().toString()));
        GlobalSnapshot before = loadCurrentSnapshot(config);

        writeAndRead(Collections.<Row>emptyList(), 4, 2);

        GlobalSnapshot after = loadCurrentSnapshot(config);
        assertTrue(after.id > before.id);
        assertSingleBucketSnapshot(after, 4, "parquet");
        List<Row> rows =
                spark.read().format("cobble").load(tableDir.toUri().toString()).collectAsList();
        assertEquals(4, rows.size());
        assertTrue(rows.stream().allMatch(row -> "kept".equals(row.getString(1))));
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
        Row nativeRow =
                row(
                        7,
                        "from-flink-format",
                        "12.34",
                        "2026-08-27",
                        "2026-08-27 12:34:56.123456",
                        9.5d);
        CobbleSparkRowConverter converter = new CobbleSparkRowConverter(nativeSchema);
        int rowBucket = converter.bucket(nativeRow);
        List<ShardSnapshot> shards = new ArrayList<ShardSnapshot>();
        for (int bucket = 0; bucket < 4; bucket++) {
            try (Table table =
                    Table.writerBuilder(CobblePaths.createPathWriterRuntimeConfig(config, 4, 1))
                            .tableName(CobbleTableRuntime.TABLE_NAME)
                            .bucket(bucket)
                            .create(nativeSchema.toTableSchema())) {
                if (bucket == rowBucket) table.put(converter.toValues(nativeRow));
                shards.add(table.snapshot());
            }
        }
        try (TableSnapshotCommitter committer =
                TableSnapshotCommitter.open(CobblePaths.createCoordinatorConfig(config, 4), 4, 1)) {
            assertTrue(committer.commitBatch(0L, shards) != null);
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
        TableScanPlan plan = CobbleTableRuntime.loadScanPlan(config, snapshot);
        List<Value> matched = null;
        for (io.cobble.table.TableScanSplit split : plan.splits()) {
            try (TableScanCursor cursor =
                    split.openTypedScanner(CobblePaths.createScanConfig(config, 4, 5), 4096)) {
                List<Value> values;
                while ((values = cursor.nextRow()) != null) {
                    if (Value.int32(11).equals(values.get(0))) {
                        matched = values;
                    }
                }
            }
        }
        assertTrue(matched != null);
        assertEquals(Value.int32(11), matched.get(0));
        assertEquals(Value.string("from-spark"), matched.get(1));
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
     * must be rejected at commit time without publishing a snapshot.
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
        List<CobbleShardResult> staleResults = produceShardResults(config, storedSchema, 4);

        assertThrows(
                IOException.class,
                () -> CobbleTableCommitter.commit(config, staleResults, staleBase));

        // The current snapshot is unchanged after the rejected commit.
        assertEquals(current.id, loadCurrentSnapshot(config).id);
    }

    private static GlobalSnapshot loadCurrentSnapshot(CobbleOptions.CobbleTableConfig config) {
        try (DbCoordinator coordinator =
                DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, 4))) {
            return coordinator.loadCurrentGlobalSnapshot();
        }
    }

    private List<Row> rowsCoveringEveryBucket(int totalBuckets, String namePrefix) {
        CobbleTableSchema nativeSchema =
                CobbleTableSchema.fromStructType(
                        schema, Collections.singletonList("id"), totalBuckets);
        CobbleSparkRowConverter converter = new CobbleSparkRowConverter(nativeSchema);
        Map<Integer, Row> byBucket = new LinkedHashMap<Integer, Row>();
        for (int id = 0; byBucket.size() < totalBuckets; id++) {
            Row candidate = namedRow(id, namePrefix);
            byBucket.putIfAbsent(Integer.valueOf(converter.bucket(candidate)), candidate);
        }
        return new ArrayList<Row>(byBucket.values());
    }

    private List<Row> rowsWithNames(List<Row> rows, String name) {
        List<Row> updated = new ArrayList<Row>(rows.size());
        for (Row value : rows) updated.add(namedRow(value.getInt(0), name));
        return updated;
    }

    private Row namedRow(int id, String name) {
        return row(id, name, null, null, null, 0d);
    }

    private void assertSingleBucketSnapshot(
            GlobalSnapshot snapshot, int totalBuckets, String expectedDataFileType)
            throws IOException {
        assertSingleBucketSnapshot(snapshot, totalBuckets, expectedDataFileType, tableDir);
    }

    private static void assertSingleBucketSnapshot(
            GlobalSnapshot snapshot, int totalBuckets, String expectedDataFileType, Path root)
            throws IOException {
        assertEquals(totalBuckets, snapshot.totalBuckets);
        assertEquals(totalBuckets, snapshot.shardSnapshots.size());
        boolean[] seen = new boolean[totalBuckets];
        boolean sawExpectedDataFile = false;
        for (ShardSnapshot shard : snapshot.shardSnapshots) {
            assertEquals(1, shard.ranges.size());
            int bucket = shard.ranges.get(0).start;
            assertEquals(bucket, shard.ranges.get(0).end);
            assertFalse(seen[bucket]);
            seen[bucket] = true;
            Path bucketRoot = root.resolve("bucket-" + bucket).toAbsolutePath();
            assertEquals("bucket-" + bucket, shard.dbId);
            assertTrue(Files.isDirectory(bucketRoot));
            Path manifest = nativePath(shard.manifestPath);
            assertTrue(manifest.startsWith(bucketRoot));
            assertTrue(Files.isRegularFile(manifest));
            assertEquals(bucketRoot.resolve("snapshot"), manifest.getParent());
            JsonNode manifestJson =
                    JSON.readTree(new String(Files.readAllBytes(manifest), StandardCharsets.UTF_8));
            assertTrue(manifestJson.path("active_memtable_data").isArray());
            assertEquals(0, manifestJson.path("active_memtable_data").size());
            assertTrue(manifestJson.path("wal_volume").isNull());
            JsonNode baseSnapshotId = manifestJson.get("base_snapshot_id");
            assertTrue(baseSnapshotId == null || baseSnapshotId.isNull());
            for (JsonNode fileType : manifestJson.findValues("file_type")) {
                sawExpectedDataFile |= expectedDataFileType.equals(fileType.asText());
            }
        }
        for (boolean bucketSeen : seen) assertTrue(bucketSeen);
        assertTrue(sawExpectedDataFile);
    }

    private static Path nativePath(String value) {
        URI uri = URI.create(value);
        return uri.getScheme() == null ? Paths.get(value).toAbsolutePath() : Paths.get(uri);
    }

    /** Opens one independent writer for every bucket and returns its complete result set. */
    private List<CobbleShardResult> produceShardResults(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema tableSchema,
            int totalBuckets) {
        Row source = row(999, null, null, null, null, 0d);
        CobbleSparkRowConverter converter = new CobbleSparkRowConverter(tableSchema);
        int sourceBucket = converter.bucket(source);
        List<CobbleShardResult> results = new ArrayList<CobbleShardResult>(totalBuckets);
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            try (Table table =
                    Table.writerBuilder(
                                    CobblePaths.createPathWriterRuntimeConfig(
                                            config, totalBuckets, 1))
                            .tableName(CobbleTableRuntime.TABLE_NAME)
                            .bucket(bucket)
                            .create(tableSchema.toTableSchema())) {
                if (bucket == sourceBucket) table.put(converter.toValues(source));
                results.add(new CobbleShardResult(totalBuckets, bucket, table.snapshot()));
            }
        }
        return results;
    }
}
