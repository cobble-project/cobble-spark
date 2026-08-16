package io.cobble.spark;

import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * Decodes Cobble scan entries (row key + projected value columns) into Catalyst internal values.
 *
 * <p>Key fields are framed by the 4-byte big-endian length prefixes written by {@link
 * CobbleRowEncoder}; value columns use the same per-type encodings.
 */
public final class CobbleRowDecoder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataType[] keyTypes;
    private final DataType[] valueTypes;

    public CobbleRowDecoder(CobbleTableSchema schema) {
        this.keyTypes = new DataType[schema.primaryKeys.size()];
        this.valueTypes = new DataType[schema.valueColumnCount()];
        for (CobbleTableSchema.Column column : schema.fields) {
            if (column.columnRole() == CobbleTableSchema.ColumnRole.KEY) {
                for (int i = 0; i < schema.primaryKeys.size(); i++) {
                    if (schema.primaryKeys.get(i).equals(column.name)) {
                        keyTypes[i] = column.dataType();
                    }
                }
            } else {
                valueTypes[column.valueIndex.intValue()] = column.dataType();
            }
        }
    }

    /** Splits a Cobble row key into its per-column framed bytes. */
    public byte[][] splitKey(byte[] key) {
        return CobbleBytes.splitKeyParts(key, keyTypes.length);
    }

    /** Decodes key field {@code keyIndex} from framed key bytes into an internal value. */
    public Object decodeKeyField(byte[][] keyParts, int keyIndex) {
        return decodeValue(keyParts[keyIndex], keyTypes[keyIndex]);
    }

    /** Decodes a value column into a Catalyst internal value; {@code null} stays SQL NULL. */
    public Object decodeValue(byte[] bytes, int valueIndex) {
        return decodeValue(bytes, valueTypes[valueIndex]);
    }

    private Object decodeValue(byte[] bytes, DataType type) {
        if (bytes == null) {
            return null;
        }
        if (type instanceof BooleanType) {
            return Boolean.valueOf(bytes[0] != 0);
        }
        if (type instanceof ByteType) {
            return Byte.valueOf(bytes[0]);
        }
        if (type instanceof ShortType) {
            return Short.valueOf(CobbleBytes.getShort(bytes, 0));
        }
        if (type instanceof IntegerType || type instanceof DateType) {
            return Integer.valueOf(CobbleBytes.getInt(bytes, 0));
        }
        if (type instanceof LongType || type instanceof TimestampType) {
            return Long.valueOf(CobbleBytes.getLong(bytes, 0));
        }
        if (type instanceof FloatType) {
            return Float.valueOf(Float.intBitsToFloat(CobbleBytes.getInt(bytes, 0)));
        }
        if (type instanceof DoubleType) {
            return Double.valueOf(Double.longBitsToDouble(CobbleBytes.getLong(bytes, 0)));
        }
        if (type instanceof StringType) {
            return UTF8String.fromBytes(bytes);
        }
        if (type instanceof BinaryType) {
            return bytes;
        }
        if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return Decimal.apply(CobbleBytes.decimal(new BigInteger(bytes), decimal.scale()));
        }
        throw new IllegalArgumentException(
                "Unsupported Cobble column type " + type.catalogString() + ".");
    }
}
