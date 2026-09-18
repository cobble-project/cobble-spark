package io.cobble.spark.write;

import io.cobble.GlobalSnapshot;
import io.cobble.spark.CobbleBucketMath;
import io.cobble.spark.CobbleCommitLock;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobblePaths;
import io.cobble.spark.CobbleSparkRowConverter;
import io.cobble.spark.CobbleTableRuntime;
import io.cobble.spark.CobbleTableSchema;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Executes one Cobble write: shuffles rows so every writer task owns a contiguous bucket range,
 * writes each shard, then materializes the next global snapshot on the driver.
 */
public final class CobbleInsertableRelation implements InsertableRelation {

    private final CobbleOptions.CobbleTableConfig config;
    private final StructType writeSchema;
    private final boolean overwrite;
    private final Map<String, String> rawOptions;

    public CobbleInsertableRelation(
            CobbleOptions.CobbleTableConfig config,
            StructType writeSchema,
            boolean overwrite,
            Map<String, String> rawOptions) {
        this.config = config;
        this.writeSchema = writeSchema;
        this.overwrite = overwrite;
        this.rawOptions = rawOptions;
    }

    @Override
    public void insert(Dataset<Row> data, boolean overwrite) {
        CobbleLoader.ensureCobbleLoaded();
        boolean overwriteAll = overwrite || this.overwrite;

        try (CobbleCommitLock ignored =
                CobbleCommitLock.acquireWrite(CobbleTableCommitter.lockScope(config))) {

            GlobalSnapshot currentSnapshot = loadCurrentGlobalSnapshot(config);
            CobbleTableSchema schema;
            int totalBuckets;
            if (config.isCatalogTable()) {
                // A catalog relation captures the latest catalog schema on the driver. Its current
                // data snapshot can legitimately embed an older schema, so never derive write
                // encoding from that snapshot.
                schema =
                        CobbleTableSchema.fromTableSchema(
                                config.catalogReference().schema(),
                                currentSnapshot != null
                                        ? currentSnapshot.totalBuckets
                                        : (config.hasBucketCount()
                                                ? config.bucketCount()
                                                : CobbleOptions.DEFAULT_BUCKET));
                schema.validateWriteSchema(data.schema());
                validatePrimaryKeyOption(schema);
                totalBuckets = schema.totalBuckets();
            } else if (currentSnapshot != null) {
                schema = CobbleTableRuntime.loadSchema(config, currentSnapshot);
                schema.validateWriteSchema(data.schema());
                validatePrimaryKeyOption(schema);
                totalBuckets = schema.totalBuckets();
            } else {
                if (writeSchema == null) {
                    throw new IllegalArgumentException(
                            "Creating a Cobble table requires a write schema.");
                }
                String rawPrimaryKey = rawOptions.get(CobbleOptions.PRIMARY_KEY);
                List<String> primaryKeys =
                        CobbleTableSchema.parsePrimaryKeyOption(
                                rawPrimaryKey == null ? "" : rawPrimaryKey);
                totalBuckets =
                        config.hasBucketCount()
                                ? config.bucketCount()
                                : CobbleOptions.DEFAULT_BUCKET;
                schema = CobbleTableSchema.fromStructType(data.schema(), primaryKeys, totalBuckets);
            }

            int writerCount =
                    Math.min(
                            config.writeTasks() > 0
                                    ? config.writeTasks()
                                    : data.sparkSession().sparkContext().defaultParallelism(),
                            totalBuckets);
            if (writerCount <= 0) {
                throw new IllegalStateException("Cobble writer count must be > 0.");
            }
            for (int writer = 0; writer < writerCount; writer++) {
                int start = CobbleBucketMath.writerRangeStart(writer, totalBuckets, writerCount);
                int end = CobbleBucketMath.writerRangeEnd(writer, totalBuckets, writerCount);
                CobblePaths.perBucketWriteBuffer(config, end - start + 1);
            }

            // Pin the base snapshot on the driver before any task runs: every writer restores from
            // it
            // and the commit verifies (under the lock) that the table still points at it, so two
            // jobs
            // starting from the same snapshot cannot overwrite each other's commit.
            GlobalSnapshot baseSnapshot = currentSnapshot;
            if (baseSnapshot != null) {
                try {
                    CobbleShardWriteTask.baseByBucket(baseSnapshot, totalBuckets);
                } catch (IOException invalidLayout) {
                    throw new IllegalStateException(
                            "Cobble table cannot append from a legacy or incomplete bucket snapshot.",
                            invalidLayout);
                }
            }

            final CobbleWriteContext context =
                    new CobbleWriteContext(
                            config, schema, totalBuckets, writerCount, overwriteAll, baseSnapshot);
            final CobbleSparkRowConverter converter = new CobbleSparkRowConverter(schema);
            final int buckets = totalBuckets;
            final int writers = writerCount;

            JavaRDD<Row> rows = data.javaRDD();
            JavaPairRDD<Integer, Row> byBucket =
                    rows.mapToPair(
                            (PairFunction<Row, Integer, Row>)
                                    row -> {
                                        int bucket = converter.bucket(row);
                                        return new Tuple2<>(Integer.valueOf(bucket), row);
                                    });
            JavaRDD<CobbleShardResult> results =
                    byBucket.partitionBy(new BucketTaskPartitioner(buckets, writers))
                            .mapPartitionsWithIndex(
                                    (Function2<
                                                    Integer,
                                                    Iterator<Tuple2<Integer, Row>>,
                                                    Iterator<CobbleShardResult>>)
                                            (index, partition) ->
                                                    CobbleShardWriteTask.writeBuckets(
                                                            index, partition, context),
                                    true);

            List<CobbleShardResult> shardResults = results.collect();
            try {
                CobbleTableCommitter.commit(config, shardResults, baseSnapshot);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to commit the Cobble write.", e);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to acquire the Cobble table write lock.", e);
        }
    }

    private static GlobalSnapshot loadCurrentGlobalSnapshot(
            CobbleOptions.CobbleTableConfig config) {
        if (config.isCatalogTable()) {
            return CobbleTableRuntime.loadSnapshot(config);
        }
        io.cobble.DbCoordinator coordinator = null;
        try {
            coordinator =
                    io.cobble.DbCoordinator.open(
                            CobblePaths.createCoordinatorConfig(
                                    config,
                                    config.hasBucketCount()
                                            ? Integer.valueOf(config.bucketCount())
                                            : null));
            return coordinator.loadCurrentGlobalSnapshot();
        } finally {
            if (coordinator != null) {
                coordinator.close();
            }
        }
    }

    /** Rejects a supplied primary key option that disagrees with the stored table schema. */
    private void validatePrimaryKeyOption(CobbleTableSchema schema) {
        String rawPrimaryKey = rawOptions.get(CobbleOptions.PRIMARY_KEY);
        if (rawPrimaryKey == null || rawPrimaryKey.trim().isEmpty()) {
            return;
        }
        List<String> provided = CobbleTableSchema.parsePrimaryKeyOption(rawPrimaryKey);
        if (!provided.equals(schema.primaryKeys())) {
            throw new IllegalArgumentException(
                    "Configured "
                            + CobbleOptions.PRIMARY_KEY
                            + "="
                            + rawPrimaryKey
                            + " does not match the stored primary key "
                            + schema.primaryKeys()
                            + " of table "
                            + config.pathUri()
                            + ".");
        }
    }

    /** Routes each logical bucket to its task's contiguous bucket assignment. */
    static final class BucketTaskPartitioner extends Partitioner {

        private static final long serialVersionUID = 1L;

        private final int totalBuckets;
        private final int writers;

        BucketTaskPartitioner(int totalBuckets, int writers) {
            this.totalBuckets = totalBuckets;
            this.writers = writers;
        }

        @Override
        public int numPartitions() {
            return writers;
        }

        @Override
        public int getPartition(Object key) {
            return CobbleBucketMath.writerIndexForBucket(
                    ((Integer) key).intValue(), totalBuckets, writers);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof BucketTaskPartitioner
                    && ((BucketTaskPartitioner) other).totalBuckets == totalBuckets
                    && ((BucketTaskPartitioner) other).writers == writers;
        }

        @Override
        public int hashCode() {
            return 31 * totalBuckets + writers;
        }
    }
}
