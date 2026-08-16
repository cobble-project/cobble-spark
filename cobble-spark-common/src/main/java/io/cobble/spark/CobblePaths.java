package io.cobble.spark;

import io.cobble.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Builds the Cobble {@link Config}s used by writers, the coordinator and scan readers, and
 * maintains the writer-path index under the table root.
 *
 * <p>All configs target local tables: writer primary data, manifests and snapshots live under the
 * table root, with writers isolated by disjoint bucket ranges.
 */
public final class CobblePaths {

    private CobblePaths() {}

    /** Local directory of the table root. */
    public static File tableRoot(CobbleOptions.CobbleTableConfig config) {
        URI uri = URI.create(config.pathUri());
        if (uri.getScheme() != null && !"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(
                    "The Cobble Spark connector currently only supports local tables, but the table"
                            + " root is "
                            + config.pathUri());
        }
        return new File(uri);
    }

    /**
     * Writer config for {@code writerIndex} owning an inclusive bucket range. Mirrors the proven
     * Flink sink writer settings: WAL disabled because commits are snapshot based, snapshot chains
     * tracked only for the latest snapshot, no block cache, vectorized memtable.
     */
    public static Config createWriterConfig(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            int totalBuckets,
            int writerIndex,
            int writerCount) {
        File localDir = tableRoot(config);
        mkdirs(localDir);

        Config dbConfig =
                new Config().numColumns(schema.valueColumnCount()).totalBuckets(totalBuckets);
        dbConfig.walEnabled = false;
        dbConfig.snapshotRetention = null;
        dbConfig.snapshotOnlyTrack = true;
        dbConfig.snapshotDisableIncrementalBaseLink = true;
        dbConfig.memtableType = Config.MemtableType.VEC;
        dbConfig.governanceMode = Config.GovernanceMode.NOOP;
        dbConfig.logConsole = false;
        dbConfig.logPath =
                new File(localDir, "cobble-writer-" + writerIndex + ".log").getAbsolutePath();
        dbConfig.blockCacheSize = 0;
        dbConfig.blockCacheHybridEnabled = false;
        dbConfig.blockCacheHybridDiskSize = 0;
        dbConfig.memtableCapacity =
                positiveInt(config.writeBufferMemoryBytes(), CobbleOptions.WRITE_BUFFER_MEMORY);
        dbConfig.memtableBufferCount = 1;

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = localDir.getAbsolutePath();
        localVolume.kinds =
                Collections.singletonList(Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        dbConfig.addVolume(localVolume);

        // Snapshot manifests are metadata; keeping META with SNAPSHOT keeps every shard snapshot
        // complete under the table root.
        Config.VolumeDescriptor tableVolume = new Config.VolumeDescriptor();
        tableVolume.baseDir = config.pathUri();
        tableVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        dbConfig.addVolume(tableVolume);
        return dbConfig;
    }

    /**
     * Coordinator config used for global snapshot lookup and materialization. {@code
     * totalBucketsOrNull} pins the expected bucket count when known; the coordinator derives it
     * from stored snapshots otherwise.
     */
    public static Config createCoordinatorConfig(
            CobbleOptions.CobbleTableConfig config, Integer totalBucketsOrNull) {
        File localDir = tableRoot(config);
        mkdirs(localDir);

        Config coordinatorConfig = new Config();
        if (totalBucketsOrNull != null) {
            coordinatorConfig.totalBuckets(totalBucketsOrNull.intValue());
        }
        coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordinatorConfig.logConsole = false;
        coordinatorConfig.logPath = new File(localDir, "cobble-coordinator.log").getAbsolutePath();

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = config.pathUri();
        volume.kinds = Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        coordinatorConfig.addVolume(volume);
        return coordinatorConfig;
    }

    /**
     * Scan config for executors: full column width, minimal memtable, bounded block cache, one
     * volume carrying data plus metadata.
     */
    public static Config createScanConfig(
            CobbleOptions.CobbleTableConfig config, int totalBuckets, int scanColumnCount) {
        Config scanConfig = new Config().numColumns(scanColumnCount).totalBuckets(totalBuckets);
        scanConfig.memtableCapacity = 1;
        scanConfig.memtableBufferCount = 1;
        scanConfig.blockCacheSize =
                positiveInt(config.readBlockCacheBytes(), CobbleOptions.READ_BLOCK_CACHE_MEMORY);
        scanConfig.blockCacheHybridEnabled = false;
        scanConfig.blockCacheHybridDiskSize = 0;
        scanConfig.governanceMode = Config.GovernanceMode.NOOP;
        scanConfig.logConsole = false;

        Config.VolumeDescriptor volume = new Config.VolumeDescriptor();
        volume.baseDir = config.pathUri();
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        scanConfig.addVolume(volume);
        return scanConfig;
    }

    /** Writer config scoped to an explicit writer path, used for snapshot pruning. */
    public static Config createWriterConfigForPath(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            int totalBuckets,
            String writerPath) {
        Config dbConfig =
                new Config().numColumns(schema.valueColumnCount()).totalBuckets(totalBuckets);
        dbConfig.governanceMode = Config.GovernanceMode.NOOP;
        dbConfig.logConsole = false;

        Config.VolumeDescriptor localVolume = new Config.VolumeDescriptor();
        localVolume.baseDir = writerPath;
        localVolume.kinds =
                Collections.singletonList(Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        dbConfig.addVolume(localVolume);

        Config.VolumeDescriptor tableVolume = new Config.VolumeDescriptor();
        tableVolume.baseDir = config.pathUri();
        tableVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        dbConfig.addVolume(tableVolume);
        return dbConfig;
    }

    private static int positiveInt(long value, String optionKey) {
        if (value <= 0L || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    optionKey + " must be in (0, " + Integer.MAX_VALUE + "].");
        }
        return (int) value;
    }

    private static void mkdirs(File dir) {
        try {
            Files.createDirectories(dir.toPath());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create Cobble directory " + dir, e);
        }
    }

    /** Serializable index mapping shard dbIds to writer paths for snapshot pruning. */
    public static Map<String, String> loadWriterPathIndex(CobbleOptions.CobbleTableConfig config) {
        File indexFile = new File(tableRoot(config), "writer-paths.properties");
        Map<String, String> result = new HashMap<>();
        if (!indexFile.exists()) {
            return result;
        }
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(indexFile)) {
            properties.load(input);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + indexFile, e);
        }
        for (String name : properties.stringPropertyNames()) {
            result.put(name, properties.getProperty(name));
        }
        return result;
    }

    public static void storeWriterPathIndex(
            CobbleOptions.CobbleTableConfig config, Map<String, String> writerPathByDbId) {
        File indexFile = new File(tableRoot(config), "writer-paths.properties");
        File parent = indexFile.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : writerPathByDbId.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        try (FileOutputStream output = new FileOutputStream(indexFile)) {
            properties.store(output, "Cobble writer path index by dbId");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write " + indexFile, e);
        }
    }
}
