package io.cobble.spark;

import io.cobble.Config;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

/**
 * Builds the Cobble {@link Config}s used by writers, the coordinator, and scan readers.
 *
 * <p>Table writers scope their own physical bucket directories; table-root metadata is reserved for
 * global coordination.
 */
public final class CobblePaths {

    private CobblePaths() {}

    public static boolean isLocal(String pathUri) {
        String scheme = URI.create(pathUri).getScheme();
        return scheme == null || "file".equalsIgnoreCase(scheme);
    }

    public static Config.VolumeDescriptor volume(
            CobbleOptions.CobbleTableConfig config, String uri) {
        Config.VolumeDescriptor volume = Config.VolumeDescriptor.singleVolume(uri);
        config.storageOptions().applyTo(volume);
        return volume;
    }

    public static Config createCatalogConfig(CobbleOptions.CobbleTableConfig config) {
        return new Config()
                .addVolume(
                        volume(
                                config,
                                config.isCatalogTable()
                                        ? config.catalogReference().warehouse()
                                        : config.pathUri()));
    }

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

    /** Runtime configuration for path-mode writers before the table builder scopes a bucket. */
    public static Config createPathWriterRuntimeConfig(
            CobbleOptions.CobbleTableConfig config, int totalBuckets, int memtableCapacity) {
        if (memtableCapacity <= 0) {
            throw new IllegalArgumentException("memtableCapacity must be positive");
        }
        Config dbConfig = createWriterRuntimeConfig(totalBuckets, memtableCapacity);
        dbConfig.dataFileType = config.dataFileType();
        localLog(config, dbConfig, "cobble-writer.log");

        Config.VolumeDescriptor localVolume = volume(config, config.pathUri());
        localVolume.kinds =
                Collections.singletonList(Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        dbConfig.addVolume(localVolume);

        Config.VolumeDescriptor tableVolume = volume(config, config.pathUri());
        tableVolume.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        dbConfig.addVolume(tableVolume);
        return dbConfig;
    }

    /** Runtime settings for a writer before native code scopes its persistent volumes. */
    public static Config createWriterRuntimeConfig(int totalBuckets, int memtableCapacity) {
        if (memtableCapacity <= 0) {
            throw new IllegalArgumentException("memtableCapacity must be positive");
        }
        Config config = new Config().totalBuckets(totalBuckets);
        config.walEnabled = false;
        config.snapshotRetention = null;
        config.snapshotOnlyTrack = true;
        config.snapshotDisableIncrementalBaseLink = true;
        config.memtableType = Config.MemtableType.VEC;
        config.governanceMode = Config.GovernanceMode.NOOP;
        config.logConsole = false;
        config.blockCacheSize = 0;
        config.blockCacheHybridEnabled = false;
        config.blockCacheHybridDiskSize = 0;
        config.memtableCapacity = memtableCapacity;
        config.memtableBufferCount = 1;
        config.snapshotOnFlush = false;
        config.activeMemtableIncrementalSnapshotRatio = 0.0d;
        config.dataFileType = Config.DataFileType.PARQUET;
        return config;
    }

    /** Divides one Spark task's write-buffer budget across its independently opened buckets. */
    public static int perBucketWriteBuffer(
            CobbleOptions.CobbleTableConfig config, int ownedBucketCount) {
        if (ownedBucketCount <= 0) {
            throw new IllegalArgumentException("ownedBucketCount must be positive");
        }
        long perBucket = config.writeBufferMemoryBytes() / ownedBucketCount;
        if (perBucket < 1L || perBucket > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Configured "
                            + CobbleOptions.WRITE_BUFFER_MEMORY
                            + "="
                            + config.writeBufferMemoryBytes()
                            + " cannot provide a Java memtable capacity for each of "
                            + ownedBucketCount
                            + " owned buckets.");
        }
        return (int) perBucket;
    }

    /**
     * Coordinator config used for global snapshot lookup and materialization. {@code
     * totalBucketsOrNull} pins the expected bucket count when known; the coordinator derives it
     * from stored snapshots otherwise.
     */
    public static Config createCoordinatorConfig(
            CobbleOptions.CobbleTableConfig config, Integer totalBucketsOrNull) {
        Config coordinatorConfig = new Config();
        if (totalBucketsOrNull != null) {
            coordinatorConfig.totalBuckets(totalBucketsOrNull.intValue());
        }
        coordinatorConfig.governanceMode = Config.GovernanceMode.NOOP;
        coordinatorConfig.logConsole = false;
        localLog(config, coordinatorConfig, "cobble-coordinator.log");

        Config.VolumeDescriptor volume = volume(config, config.pathUri());
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
        // Native scan configuration always needs one physical column, even though a key-only
        // table has zero semantic value columns. TableScanSplit retains that hidden existence
        // column and connector readers deliberately ignore it for key-only/COUNT projections.
        Config scanConfig =
                new Config().numColumns(Math.max(1, scanColumnCount)).totalBuckets(totalBuckets);
        scanConfig.memtableCapacity = 1;
        scanConfig.memtableBufferCount = 1;
        scanConfig.blockCacheSize =
                nonNegativeInt(config.readBlockCacheBytes(), CobbleOptions.READ_BLOCK_CACHE_MEMORY);
        scanConfig.blockCacheHybridEnabled = false;
        scanConfig.blockCacheHybridDiskSize = 0;
        scanConfig.governanceMode = Config.GovernanceMode.NOOP;
        scanConfig.logConsole = false;

        Config.VolumeDescriptor volume = volume(config, config.pathUri());
        volume.kinds =
                Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        scanConfig.addVolume(volume);
        return scanConfig;
    }

    private static int nonNegativeInt(long value, String optionKey) {
        if (value < 0L || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    optionKey + " must be in [0, " + Integer.MAX_VALUE + "].");
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

    private static void localLog(
            CobbleOptions.CobbleTableConfig options, Config config, String name) {
        if (isLocal(options.pathUri())) {
            File root = tableRoot(options);
            mkdirs(root);
            config.logPath = new File(root, name).getAbsolutePath();
        } else config.logPath = null;
    }
}
