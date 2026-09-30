package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

class CobbleWriteSafetyTest {
    @TempDir Path root;

    @Test
    void speculativeWriteFailsBeforeCreatingStorageWithoutChangingSparkConfiguration() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("cobble-write-safety")
                        .config("spark.ui.enabled", false)
                        .config("spark.driver.host", "127.0.0.1")
                        .config("spark.speculation", true)
                        .getOrCreate();
        try {
            Path table = root.resolve("untouched");
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            spark.createDataFrame(
                                            Collections.singletonList(RowFactory.create(1)),
                                            DataTypes.createStructType(
                                                    Collections.singletonList(
                                                            DataTypes.createStructField(
                                                                    "id",
                                                                    DataTypes.IntegerType,
                                                                    false))))
                                    .write()
                                    .format("cobble")
                                    .option("primary-key", "id")
                                    .save(table.toString()));
            assertFalse(Files.exists(table));
            assertTrue(spark.sparkContext().getConf().getBoolean("spark.speculation", false));
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }
}
