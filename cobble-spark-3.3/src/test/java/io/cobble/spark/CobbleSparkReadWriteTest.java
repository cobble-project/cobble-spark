package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
                        .option("snapshot-id", "1")
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
}
