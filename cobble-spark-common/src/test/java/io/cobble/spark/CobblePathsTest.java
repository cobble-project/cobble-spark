package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.Config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class CobblePathsTest {

    @TempDir Path tableRoot;

    @Test
    void pathWriterRuntimeLeavesBucketScopingToTheTableBuilder() {
        CobbleOptions.CobbleTableConfig options = options(80L, null);

        Config config = CobblePaths.createPathWriterRuntimeConfig(options, 8, 10);

        assertEquals(Config.DataFileType.PARQUET, config.dataFileType);
        assertEquals(Config.MemtableType.VEC, config.memtableType);
        assertEquals(Boolean.FALSE, config.walEnabled);
        assertEquals(Boolean.FALSE, config.snapshotOnFlush);
        assertEquals(Double.valueOf(0.0d), config.activeMemtableIncrementalSnapshotRatio);
        assertEquals(Integer.valueOf(10), config.memtableCapacity);
        assertEquals(Integer.valueOf(1), config.memtableBufferCount);
        assertEquals(2, config.volumes.size());
        String root = CobblePaths.tableRoot(options).getAbsolutePath();
        assertEquals(root, config.volumes.get(0).baseDir);
        assertEquals(root, config.volumes.get(1).baseDir);
        assertTrue(CobblePaths.tableRoot(options).isDirectory());
        assertFalse(Files.exists(tableRoot.resolve("bucket-3")));
    }

    @Test
    void dataFileTypeIsParquetByDefaultAndSstOnlyWhenExplicitlyRequested() {
        assertEquals(Config.DataFileType.PARQUET, options(32L, null).dataFileType());
        assertEquals(Config.DataFileType.SST, options(32L, "SST").dataFileType());
        assertThrows(IllegalArgumentException.class, () -> options(32L, "orc"));
    }

    @Test
    void positiveSnapshotRetentionIsExplicitlyUnsupported() {
        Map<String, String> values = new HashMap<String, String>();
        values.put(CobbleOptions.PATH, tableRoot.toUri().toString());
        values.put(CobbleOptions.SNAPSHOT_RETENTION, "1");

        assertThrows(UnsupportedOperationException.class, () -> CobbleOptions.parse(values));
    }

    @Test
    void writeBufferIsDividedPerOwnedBucketWithoutExceedingTaskBudget() {
        CobbleOptions.CobbleTableConfig options = options(101L, null);

        int perBucket = CobblePaths.perBucketWriteBuffer(options, 8);

        assertEquals(12, perBucket);
        assertTrue((long) perBucket * 8L <= options.writeBufferMemoryBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> CobblePaths.perBucketWriteBuffer(options(3L, null), 4));
    }

    private CobbleOptions.CobbleTableConfig options(long writeBuffer, String dataFileType) {
        Map<String, String> values = new HashMap<String, String>();
        values.put(CobbleOptions.PATH, tableRoot.toUri().toString());
        values.put(CobbleOptions.WRITE_BUFFER_MEMORY, Long.toString(writeBuffer));
        if (dataFileType != null) values.put("DATA.FILE-TYPE", dataFileType);
        return CobbleOptions.parse(values);
    }
}
