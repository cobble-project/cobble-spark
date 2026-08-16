package io.cobble.spark.write;

import io.cobble.spark.CobbleBucketMath;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobbleRowEncoder;
import io.cobble.spark.CobbleTableSchema;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructField;
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

        CobbleTableSchema schema;
        int totalBuckets;
        if (CobbleTableSchema.sidecarExists(config.pathUri())) {
            schema = loadExistingSchema();
            schema.validateWriteSchema(data.schema());
            validatePrimaryKeyOption(schema);
            totalBuckets = schema.totalBuckets;
        } else {
            if (writeSchema == null) {
                throw new IllegalArgumentException(
                        "Creating a Cobble table requires a write schema.");
            }
            String rawPrimaryKey = rawOptions.get(CobbleOptions.PRIMARY_KEY);
            List<String> primaryKeys =
                    CobbleTableSchema.parsePrimaryKeyOption(
                            rawPrimaryKey == null ? "" : rawPrimaryKey);
            schema = CobbleTableSchema.fromStructType(data.schema(), primaryKeys);
            totalBuckets =
                    config.hasBucketCount() ? config.bucketCount() : CobbleOptions.DEFAULT_BUCKET;
            schema.totalBuckets = totalBuckets;
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

        final CobbleWriteContext context =
                new CobbleWriteContext(config, schema, totalBuckets, writerCount, overwriteAll);
        final CobbleRowEncoder encoder = new CobbleRowEncoder(schema);
        final String[] fieldNames = fieldNames(schema.toStructType());
        final int[] keyOrdinals = encoder.keyOrdinals(fieldNames);
        final int buckets = totalBuckets;
        final int writers = writerCount;

        JavaRDD<Row> rows = data.javaRDD();
        JavaPairRDD<Integer, Row> byWriter =
                rows.mapToPair(
                        (PairFunction<Row, Integer, Row>)
                                row -> {
                                    byte[] key = encoder.encodeKey(row, keyOrdinals);
                                    int bucket = CobbleBucketMath.hashBucket(key, buckets);
                                    int writerIndex =
                                            CobbleBucketMath.writerIndexForBucket(
                                                    bucket, buckets, writers);
                                    return new Tuple2<>(Integer.valueOf(writerIndex), row);
                                });
        JavaRDD<CobbleShardResult> results =
                byWriter.partitionBy(new IdentityPartitioner(writerCount))
                        .mapPartitionsWithIndex(
                                (Function2<
                                                Integer,
                                                Iterator<Tuple2<Integer, Row>>,
                                                Iterator<CobbleShardResult>>)
                                        (index, partition) ->
                                                CobbleShardWriteTask.writeShard(
                                                        index, toRowIterator(partition), context),
                                true);

        List<CobbleShardResult> shardResults = results.collect();
        try {
            CobbleTableCommitter.commit(config, schema, shardResults);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to commit the Cobble write.", e);
        }
    }

    private CobbleTableSchema loadExistingSchema() {
        try {
            CobbleTableSchema schema = CobbleTableSchema.load(config.pathUri(), null);
            if (config.hasBucketCount() && schema.totalBuckets != config.bucketCount()) {
                throw new IllegalArgumentException(
                        "Configured "
                                + CobbleOptions.BUCKET
                                + "="
                                + config.bucketCount()
                                + " does not match the stored bucket count "
                                + schema.totalBuckets
                                + " of table "
                                + config.pathUri()
                                + ".");
            }
            return schema;
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load the Cobble schema sidecar for " + config.pathUri(), e);
        }
    }

    /** Rejects a supplied primary key option that disagrees with the stored table schema. */
    private void validatePrimaryKeyOption(CobbleTableSchema schema) {
        String rawPrimaryKey = rawOptions.get(CobbleOptions.PRIMARY_KEY);
        if (rawPrimaryKey == null || rawPrimaryKey.trim().isEmpty()) {
            return;
        }
        List<String> provided = CobbleTableSchema.parsePrimaryKeyOption(rawPrimaryKey);
        if (!provided.equals(schema.primaryKeys)) {
            throw new IllegalArgumentException(
                    "Configured "
                            + CobbleOptions.PRIMARY_KEY
                            + "="
                            + rawPrimaryKey
                            + " does not match the stored primary key "
                            + schema.primaryKeys
                            + " of table "
                            + config.pathUri()
                            + ".");
        }
    }

    private static String[] fieldNames(StructType schema) {
        StructField[] fields = schema.fields();
        String[] names = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            names[i] = fields[i].name();
        }
        return names;
    }

    private static Iterator<Row> toRowIterator(Iterator<Tuple2<Integer, Row>> partition) {
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return partition.hasNext();
            }

            @Override
            public Row next() {
                return partition.next()._2();
            }
        };
    }

    /** Routes each pre-computed writer index to itself, keeping bucket ranges contiguous. */
    static final class IdentityPartitioner extends Partitioner {

        private static final long serialVersionUID = 1L;

        private final int partitions;

        IdentityPartitioner(int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int numPartitions() {
            return partitions;
        }

        @Override
        public int getPartition(Object key) {
            return ((Integer) key).intValue();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof IdentityPartitioner
                    && ((IdentityPartitioner) other).partitions == partitions;
        }

        @Override
        public int hashCode() {
            return partitions;
        }
    }
}
