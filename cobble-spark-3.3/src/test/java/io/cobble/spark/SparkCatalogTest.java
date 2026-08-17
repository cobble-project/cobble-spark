package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

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
    public void renameRejectedAndDropTable() {
        spark.sql("CREATE DATABASE cobble.db6");
        spark.sql(
                "CREATE TABLE cobble.db6.t6 (id INT, v STRING) USING cobble"
                        + " OPTIONS ('primary-key'='id')");
        spark.sql("INSERT INTO cobble.db6.t6 VALUES (1, 'a')");

        // Rename is not supported yet: moving the directory would break snapshot data paths.
        assertThrows(
                Exception.class,
                () -> spark.sql("ALTER TABLE cobble.db6.t6 RENAME TO cobble.db6.t6b"));

        // Original table is still intact and readable.
        List<Row> rows = spark.sql("SELECT v FROM cobble.db6.t6").collectAsList();
        assertEquals(1, rows.size());

        spark.sql("DROP TABLE cobble.db6.t6");
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
}
