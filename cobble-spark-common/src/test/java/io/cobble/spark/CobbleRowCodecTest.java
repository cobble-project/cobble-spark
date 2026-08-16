package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;

/** Round-trip tests for the row encoder and decoder over every supported type. */
public class CobbleRowCodecTest {

    private StructType schema() {
        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("id", DataTypes.LongType, false),
                        DataTypes.createStructField("flag", DataTypes.BooleanType, true),
                        DataTypes.createStructField("b", DataTypes.ByteType, true),
                        DataTypes.createStructField("s", DataTypes.ShortType, true),
                        DataTypes.createStructField("i", DataTypes.IntegerType, true),
                        DataTypes.createStructField("f", DataTypes.FloatType, true),
                        DataTypes.createStructField("d", DataTypes.DoubleType, true),
                        DataTypes.createStructField("str", DataTypes.StringType, true),
                        DataTypes.createStructField("bin", DataTypes.BinaryType, true),
                        DataTypes.createStructField("date", DataTypes.DateType, true),
                        DataTypes.createStructField("ts", DataTypes.TimestampType, true),
                        DataTypes.createStructField("dec", new DecimalType(10, 2), true)));
    }

    private CobbleTableSchema tableSchema() {
        CobbleTableSchema table =
                CobbleTableSchema.fromStructType(schema(), Collections.singletonList("id"));
        table.totalBuckets = 4;
        return table;
    }

    @Test
    public void roundTripsAllSupportedTypes() {
        CobbleTableSchema table = tableSchema();
        CobbleRowEncoder encoder = new CobbleRowEncoder(table);
        CobbleRowDecoder decoder = new CobbleRowDecoder(table);

        Timestamp ts = Timestamp.valueOf("2026-08-16 12:34:56.789123");
        Date date = Date.valueOf("2026-08-16");
        Row row =
                RowFactory.create(
                        42L,
                        true,
                        (byte) -5,
                        (short) 1500,
                        -123456,
                        1.5f,
                        -2.25d,
                        "héllo cobble",
                        new byte[] {1, 2, 3},
                        date,
                        ts,
                        new BigDecimal("12345.67"));

        String[] names = table.fieldNames().toArray(new String[0]);
        int[] keyOrdinals = encoder.keyOrdinals(names);
        int[] valueOrdinals = encoder.valueOrdinals(names);

        byte[] key = encoder.encodeKey(row, keyOrdinals);
        byte[][] keyParts = decoder.splitKey(key);
        assertEquals(1, keyParts.length);
        assertEquals(Long.valueOf(42L), decoder.decodeKeyField(keyParts, 0));

        Object[] decoded = new Object[valueOrdinals.length];
        for (int v = 0; v < valueOrdinals.length; v++) {
            byte[] encoded = encoder.encodeValue(row, valueOrdinals[v], v);
            decoded[v] = decoder.decodeValue(encoded, v);
        }

        assertEquals(Boolean.TRUE, decoded[0]);
        assertEquals(Byte.valueOf((byte) -5), decoded[1]);
        assertEquals(Short.valueOf((short) 1500), decoded[2]);
        assertEquals(Integer.valueOf(-123456), decoded[3]);
        assertEquals(Float.valueOf(1.5f), decoded[4]);
        assertEquals(Double.valueOf(-2.25d), decoded[5]);
        assertEquals(UTF8String.fromString("héllo cobble"), decoded[6]);
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) decoded[7]);
        assertEquals(
                Integer.valueOf(
                        org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(date)),
                decoded[8]);
        assertEquals(
                Long.valueOf(
                        org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp(ts)),
                decoded[9]);
        Decimal decimal = (Decimal) decoded[10];
        assertEquals(0, decimal.toJavaBigDecimal().compareTo(new BigDecimal("12345.67")));
        assertEquals(2, decimal.toJavaBigDecimal().scale());
    }

    @Test
    public void nullValuesEncodeToNullAndDecodeToNull() {
        CobbleTableSchema table = tableSchema();
        CobbleRowEncoder encoder = new CobbleRowEncoder(table);
        Row row =
                RowFactory.create(
                        1L, null, null, null, null, null, null, null, null, null, null, null);
        String[] names = table.fieldNames().toArray(new String[0]);
        int[] valueOrdinals = encoder.valueOrdinals(names);
        CobbleRowDecoder decoder = new CobbleRowDecoder(table);
        for (int v = 0; v < valueOrdinals.length; v++) {
            assertNull(encoder.encodeValue(row, valueOrdinals[v], v));
            assertNull(decoder.decodeValue(null, v));
        }
    }

    @Test
    public void multiColumnKeysAreLengthFramed() {
        StructType schema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("region", DataTypes.StringType, false),
                                DataTypes.createStructField("uid", DataTypes.IntegerType, false),
                                DataTypes.createStructField("payload", DataTypes.LongType, true)));
        CobbleTableSchema table =
                CobbleTableSchema.fromStructType(schema, Arrays.asList("region", "uid"));
        CobbleRowEncoder encoder = new CobbleRowEncoder(table);
        CobbleRowDecoder decoder = new CobbleRowDecoder(table);

        Row row = RowFactory.create("eu", 7, 100L);
        String[] names = table.fieldNames().toArray(new String[0]);
        byte[] key = encoder.encodeKey(row, encoder.keyOrdinals(names));

        byte[][] parts = decoder.splitKey(key);
        assertEquals(2, parts.length);
        assertEquals(UTF8String.fromString("eu"), decoder.decodeKeyField(parts, 0));
        assertEquals(Integer.valueOf(7), decoder.decodeKeyField(parts, 1));

        // Independent keys stay distinguishable despite variable length fields.
        Row other = RowFactory.create("e", 17, 200L);
        byte[] otherKey = encoder.encodeKey(other, encoder.keyOrdinals(names));
        byte[][] otherParts = decoder.splitKey(otherKey);
        assertEquals(UTF8String.fromString("e"), decoder.decodeKeyField(otherParts, 0));
        assertEquals(Integer.valueOf(17), decoder.decodeKeyField(otherParts, 1));
    }

    @Test
    public void rejectsNullPrimaryKey() {
        CobbleTableSchema table = tableSchema();
        CobbleRowEncoder encoder = new CobbleRowEncoder(table);
        Row row =
                RowFactory.create(
                        null, null, null, null, null, null, null, null, null, null, null, null);
        String[] names = table.fieldNames().toArray(new String[0]);
        assertThrows(
                IllegalArgumentException.class,
                () -> encoder.encodeKey(row, encoder.keyOrdinals(names)));
    }

    @Test
    public void keyOrdinalsResolveAgainstRowSchema() {
        CobbleTableSchema table = tableSchema();
        CobbleRowEncoder encoder = new CobbleRowEncoder(table);
        String[] names = table.fieldNames().toArray(new String[0]);
        assertArrayEquals(new int[] {0}, encoder.keyOrdinals(names));
        assertEquals(11, encoder.valueOrdinals(names).length);
        assertEquals(11, encoder.valueColumnCount());
    }

    @Test
    public void corruptKeyFramingIsRejected() {
        CobbleTableSchema table = tableSchema();
        CobbleRowDecoder decoder = new CobbleRowDecoder(table);
        // Truncated framing.
        assertThrows(IllegalArgumentException.class, () -> decoder.splitKey(new byte[] {0, 0}));
        // Length prefix beyond the key bytes.
        assertThrows(
                IllegalArgumentException.class, () -> decoder.splitKey(new byte[] {0, 0, 0, 8, 1}));
        // Negative length prefix.
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        decoder.splitKey(
                                new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
        // Trailing bytes after the single framed field.
        assertThrows(
                IllegalArgumentException.class,
                () -> decoder.splitKey(new byte[] {0, 0, 0, 1, 1, 9}));
    }
}
