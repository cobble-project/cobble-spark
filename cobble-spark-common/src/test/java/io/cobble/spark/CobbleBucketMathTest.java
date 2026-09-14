package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.cobble.table.BucketHash;
import io.cobble.table.KeyCodec;
import io.cobble.table.Value;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Tests for bucket hashing and writer range math. */
public class CobbleBucketMathTest {

    @Test
    public void hashBucketIsStableAndBounded() {
        byte[] key = new byte[] {1, 2, 3};
        BucketHash hash = new BucketHash(16);
        assertEquals(hash.bucket(key), hash.bucket(key));
        for (int i = 0; i < 100; i++) {
            byte[] probe = new byte[] {(byte) i, (byte) (i * 31), (byte) (i * 7)};
            int bucket = hash.bucket(probe);
            assertTrue(bucket >= 0 && bucket < 16, "bucket out of range: " + bucket);
        }
    }

    @Test
    public void writerRangesPartitionAllBuckets() {
        int[][] cases = {{16, 1}, {16, 4}, {16, 16}, {17, 5}, {100, 7}, {65536, 200}};
        for (int[] caseSpec : cases) {
            int totalBuckets = caseSpec[0];
            int writerCount = caseSpec[1];
            boolean[] covered = new boolean[totalBuckets];
            for (int writer = 0; writer < writerCount; writer++) {
                int start = CobbleBucketMath.writerRangeStart(writer, totalBuckets, writerCount);
                int end = CobbleBucketMath.writerRangeEnd(writer, totalBuckets, writerCount);
                assertTrue(start <= end, "empty range for writer " + writer);
                for (int bucket = start; bucket <= end; bucket++) {
                    assertTrue(!covered[bucket], "bucket " + bucket + " covered twice");
                    covered[bucket] = true;
                }
            }
            for (int bucket = 0; bucket < totalBuckets; bucket++) {
                assertTrue(covered[bucket], "bucket " + bucket + " not covered");
            }
        }
    }

    @Test
    public void writerIndexForBucketInvertsRangeFormulas() {
        int[][] cases = {{16, 4}, {16, 16}, {17, 5}, {100, 7}, {65536, 200}};
        for (int[] caseSpec : cases) {
            int totalBuckets = caseSpec[0];
            int writerCount = caseSpec[1];
            for (int writer = 0; writer < writerCount; writer++) {
                int start = CobbleBucketMath.writerRangeStart(writer, totalBuckets, writerCount);
                int end = CobbleBucketMath.writerRangeEnd(writer, totalBuckets, writerCount);
                for (int bucket = start; bucket <= end; bucket++) {
                    assertEquals(
                            writer,
                            CobbleBucketMath.writerIndexForBucket(
                                    bucket, totalBuckets, writerCount),
                            "bucket " + bucket + " of range writer " + writer);
                }
            }
        }
    }

    @Test
    public void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new BucketHash(0));
        assertThrows(
                IllegalArgumentException.class, () -> CobbleBucketMath.writerRangeStart(0, 4, 8));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleBucketMath.writerIndexForBucket(16, 16, 4));
    }

    @Test
    public void converterBucketCacheSurvivesSerializationWithoutChangingProtocol()
            throws Exception {
        StructType schema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField("text", DataTypes.StringType, false),
                                DataTypes.createStructField("bytes", DataTypes.BinaryType, false)));
        CobbleTableSchema tableSchema =
                CobbleTableSchema.fromStructType(schema, Arrays.asList("id", "text", "bytes"), 17);
        byte[] largeBytes = new byte[64 * 1024];
        for (int i = 0; i < largeBytes.length; i++) largeBytes[i] = (byte) (i * 19);
        List<Row> rows =
                Arrays.asList(
                        RowFactory.create(
                                -7, "\u4f60\u597d\u0000snowman-\u2603", new byte[] {0, 1, -1}),
                        RowFactory.create(
                                Integer.MIN_VALUE, "\u00df\u0000emoji-\ud83d\ude80", largeBytes));

        CobbleSparkRowConverter converter = new CobbleSparkRowConverter(tableSchema);
        for (Row row : rows) assertProtocolBucket(tableSchema, converter, row);

        CobbleSparkRowConverter restored = roundTrip(converter);
        for (Row row : rows) assertProtocolBucket(tableSchema, restored, row);
    }

    private static void assertProtocolBucket(
            CobbleTableSchema schema, CobbleSparkRowConverter converter, Row row) {
        List<Value> values = converter.toValues(row);
        int[] ordinals = schema.bucketKeyOrdinals();
        List<Value> bucketValues = new ArrayList<Value>(ordinals.length);
        for (int ordinal : ordinals) bucketValues.add(values.get(ordinal));
        int expected =
                new BucketHash(schema.totalBuckets())
                        .bucket(KeyCodec.encode(schema.bucketKeyTypes(), bucketValues));
        assertEquals(expected, converter.bucket(row));
    }

    private static CobbleSparkRowConverter roundTrip(CobbleSparkRowConverter converter)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(converter);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (CobbleSparkRowConverter) input.readObject();
        }
    }
}
