package io.cobble.spark;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.TreeMap;

/** Option keys and the parsed, serializable table configuration for the Cobble Spark connector. */
public final class CobbleOptions {

    /** Root URI of the Cobble table. Required for both reads and writes. */
    public static final String PATH = "path";

    /**
     * Total bucket count of the table. Only consulted when the table is created; later writes must
     * reuse the bucket count stored with the table.
     */
    public static final String BUCKET = "bucket";

    public static final int DEFAULT_BUCKET = 16;

    /** Primary key column names, comma separated. Required when creating a new table. */
    public static final String PRIMARY_KEY = "primary-key";

    /** Snapshot id to read, or "latest" (default) for the latest committed global snapshot. */
    public static final String SNAPSHOT_ID = "snapshot-id";

    public static final String LATEST_SNAPSHOT = "latest";

    /** Number of retained global snapshots after each write commit. */
    public static final String SNAPSHOT_RETENTION = "snapshot.retention";

    /**
     * Snapshot cleanup is disabled by default: scans resolve a snapshot on the driver and release
     * the coordinator before executors open cursors, so an aggressive retention could delete files
     * a running query still needs. Enable with an explicit {@code snapshot.retention > 0} once
     * snapshots are leased.
     */
    public static final int DEFAULT_SNAPSHOT_RETENTION = 0;

    /**
     * Number of writer tasks for a write job. Defaults to the Spark default parallelism, capped by
     * the bucket count.
     */
    public static final String WRITE_TASKS = "write.tasks";

    /** Per-writer memtable capacity in bytes. */
    public static final String WRITE_BUFFER_MEMORY = "write.buffer-memory";

    public static final long DEFAULT_WRITE_BUFFER_MEMORY = 256L * 1024L * 1024L;

    /** Block cache bytes used by scan readers on executors. */
    public static final String READ_BLOCK_CACHE_MEMORY = "read.block-cache-memory";

    public static final long DEFAULT_READ_BLOCK_CACHE_MEMORY = 8L * 1024L * 1024L;

    private CobbleOptions() {}

    /** Parsed connector options for one table operation. Serializable for executors. */
    public static final class CobbleTableConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String pathUri;
        private final Integer bucketCount;
        private final Long snapshotId;
        private final int snapshotRetention;
        private final int writeTasks;
        private final long writeBufferMemoryBytes;
        private final long readBlockCacheBytes;

        private CobbleTableConfig(
                String pathUri,
                Integer bucketCount,
                Long snapshotId,
                int snapshotRetention,
                int writeTasks,
                long writeBufferMemoryBytes,
                long readBlockCacheBytes) {
            this.pathUri = pathUri;
            this.bucketCount = bucketCount;
            this.snapshotId = snapshotId;
            this.snapshotRetention = snapshotRetention;
            this.writeTasks = writeTasks;
            this.writeBufferMemoryBytes = writeBufferMemoryBytes;
            this.readBlockCacheBytes = readBlockCacheBytes;
        }

        public String pathUri() {
            return pathUri;
        }

        public boolean hasBucketCount() {
            return bucketCount != null;
        }

        public int bucketCount() {
            if (bucketCount == null) {
                throw new IllegalStateException("bucket count is not configured");
            }
            return bucketCount.intValue();
        }

        public boolean hasSnapshotId() {
            return snapshotId != null;
        }

        public long snapshotId() {
            if (snapshotId == null) {
                throw new IllegalStateException("snapshot id is not configured");
            }
            return snapshotId.longValue();
        }

        public int snapshotRetention() {
            return snapshotRetention;
        }

        public int writeTasks() {
            return writeTasks;
        }

        public long writeBufferMemoryBytes() {
            return writeBufferMemoryBytes;
        }

        public long readBlockCacheBytes() {
            return readBlockCacheBytes;
        }

        @Override
        public String toString() {
            return "CobbleTableConfig{pathUri="
                    + pathUri
                    + ", bucketCount="
                    + bucketCount
                    + ", snapshotId="
                    + snapshotId
                    + ", snapshotRetention="
                    + snapshotRetention
                    + ", writeTasks="
                    + writeTasks
                    + "}";
        }
    }

    /**
     * Merges table-level properties (for example loaded from a catalog) with per-operation options
     * (for example a scan's {@code snapshot-id}); operation options take precedence. The result is
     * case-insensitive.
     */
    public static Map<String, String> mergeTableOptions(
            Map<String, String> tableProperties, Map<String, String> operationOptions) {
        Map<String, String> merged = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (tableProperties != null) {
            merged.putAll(tableProperties);
        }
        if (operationOptions != null) {
            merged.putAll(operationOptions);
        }
        return merged;
    }

    /** Parses raw option map into {@link CobbleTableConfig}, validating values. */
    public static CobbleTableConfig parse(Map<String, String> options) {
        Map<String, String> normalized = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        normalized.putAll(options);

        String rawPath = normalized.get(PATH);
        if (rawPath == null || rawPath.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Cobble table requires a '" + PATH + "' option pointing at the table root.");
        }
        String pathUri = normalizePathUri(rawPath.trim());

        Integer bucketCount = null;
        String rawBucket = normalized.get(BUCKET);
        if (rawBucket != null && !rawBucket.trim().isEmpty()) {
            bucketCount = Integer.valueOf(parseIntOption(BUCKET, rawBucket));
            if (bucketCount.intValue() <= 0 || bucketCount.intValue() > 65536) {
                throw new IllegalArgumentException(
                        BUCKET + " must be in [1, 65536], but was " + bucketCount + ".");
            }
        }

        Long snapshotId = null;
        String rawSnapshot = normalized.get(SNAPSHOT_ID);
        if (rawSnapshot != null && !rawSnapshot.trim().isEmpty()) {
            String value = rawSnapshot.trim();
            if (!LATEST_SNAPSHOT.equalsIgnoreCase(value)) {
                snapshotId = Long.valueOf(parseLongOption(SNAPSHOT_ID, value));
                if (snapshotId.longValue() <= 0L) {
                    throw new IllegalArgumentException(
                            SNAPSHOT_ID
                                    + " must be a positive snapshot id or '"
                                    + LATEST_SNAPSHOT
                                    + "'.");
                }
            }
        }

        int retention = DEFAULT_SNAPSHOT_RETENTION;
        String rawRetention = normalized.get(SNAPSHOT_RETENTION);
        if (rawRetention != null && !rawRetention.trim().isEmpty()) {
            retention = parseIntOption(SNAPSHOT_RETENTION, rawRetention);
            if (retention < 0) {
                throw new IllegalArgumentException(
                        SNAPSHOT_RETENTION + " must be >= 0, but was " + retention + ".");
            }
        }

        int writeTasks = 0;
        String rawTasks = normalized.get(WRITE_TASKS);
        if (rawTasks != null && !rawTasks.trim().isEmpty()) {
            writeTasks = parseIntOption(WRITE_TASKS, rawTasks);
            if (writeTasks <= 0) {
                throw new IllegalArgumentException(
                        WRITE_TASKS + " must be > 0, but was " + writeTasks + ".");
            }
        }

        long writeBuffer =
                parseMemoryOption(
                        normalized.get(WRITE_BUFFER_MEMORY),
                        DEFAULT_WRITE_BUFFER_MEMORY,
                        WRITE_BUFFER_MEMORY);
        long readCache =
                parseMemoryOption(
                        normalized.get(READ_BLOCK_CACHE_MEMORY),
                        DEFAULT_READ_BLOCK_CACHE_MEMORY,
                        READ_BLOCK_CACHE_MEMORY);

        return new CobbleTableConfig(
                pathUri, bucketCount, snapshotId, retention, writeTasks, writeBuffer, readCache);
    }

    /**
     * Normalizes a user supplied path to an absolute URI string. Only local filesystem paths are
     * supported by this connector version.
     */
    public static String normalizePathUri(String path) {
        URI uri;
        try {
            uri = new URI(path);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid Cobble table path: " + path, e);
        }
        String scheme = uri.getScheme();
        if (scheme == null) {
            return java.nio.file.Paths.get(path).toUri().toString();
        }
        if (!"file".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException(
                    "The Cobble Spark connector currently only supports local 'file' paths, but"
                            + " got: "
                            + path);
        }
        return uri.toString();
    }

    private static int parseIntOption(String key, String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, but was: " + value, e);
        }
    }

    private static long parseLongOption(String key, String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a long, but was: " + value, e);
        }
    }

    private static long parseMemoryOption(String raw, long defaultValue, String key) {
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        String value = raw.trim().toLowerCase();
        long multiplier = 1L;
        if (value.endsWith("k") || value.endsWith("kb")) {
            multiplier = 1024L;
            value = stripSuffix(value);
        } else if (value.endsWith("m") || value.endsWith("mb")) {
            multiplier = 1024L * 1024L;
            value = stripSuffix(value);
        } else if (value.endsWith("g") || value.endsWith("gb")) {
            multiplier = 1024L * 1024L * 1024L;
            value = stripSuffix(value);
        } else if (value.endsWith("b")) {
            value = value.substring(0, value.length() - 1);
        }
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    key + " must be a byte size like '256mb', but was: " + raw, e);
        }
        if (parsed < 0L) {
            throw new IllegalArgumentException(key + " must be >= 0, but was: " + raw);
        }
        if (parsed != 0L && multiplier > Long.MAX_VALUE / parsed) {
            throw new IllegalArgumentException(key + " overflows a long: " + raw);
        }
        return parsed * multiplier;
    }

    private static String stripSuffix(String value) {
        if (value.endsWith("kb") || value.endsWith("mb") || value.endsWith("gb")) {
            return value.substring(0, value.length() - 2);
        }
        return value.substring(0, value.length() - 1);
    }
}
