package io.cobble.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Encodes Spark {@link Row} fields into the Cobble physical layout.
 *
 * <p>The row key is the length prefixed concatenation of the encoded primary key fields (4-byte big
 * endian length + field bytes per key column). Each value column is stored in its own Cobble column
 * with a fixed-width little-endian encoding for numerics and raw bytes for strings and binaries.
 */
public final class CobbleRowEncoder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String[] keyNames;
    private final DataType[] keyTypes;
    private final String[] valueNames;
    private final DataType[] valueTypes;

    public CobbleRowEncoder(CobbleTableSchema schema) {
        this.keyNames = schema.primaryKeys.toArray(new String[0]);
        this.valueNames = new String[schema.valueColumnCount()];
        this.keyTypes = new DataType[keyNames.length];
        this.valueTypes = new DataType[valueNames.length];
        for (CobbleTableSchema.Column column : schema.fields) {
            if (column.columnRole() == CobbleTableSchema.ColumnRole.KEY) {
                for (int i = 0; i < keyNames.length; i++) {
                    if (keyNames[i].equals(column.name)) {
                        keyTypes[i] = column.dataType();
                    }
                }
            } else {
                int index = column.valueIndex.intValue();
                valueNames[index] = column.name;
                valueTypes[index] = column.dataType();
            }
        }
        for (int i = 0; i < valueTypes.length; i++) {
            if (valueTypes[i] == null) {
                throw new IllegalArgumentException(
                        "Cobble value column index " + i + " is missing.");
            }
        }
        for (int i = 0; i < keyTypes.length; i++) {
            if (keyTypes[i] == null) {
                throw new IllegalArgumentException(
                        "Cobble key column '" + keyNames[i] + "' is missing.");
            }
        }
    }

    /** Number of Cobble value columns. */
    public int valueColumnCount() {
        return valueNames.length;
    }

    /** Ordinals of the value columns in the row schema, indexed by Cobble column index. */
    public int[] valueOrdinals(String[] rowFieldNames) {
        return ordinals(valueNames, rowFieldNames, "value");
    }

    /** Ordinals of the key columns in the row schema, in primary key order. */
    public int[] keyOrdinals(String[] rowFieldNames) {
        return ordinals(keyNames, rowFieldNames, "key");
    }

    private static int[] ordinals(String[] names, String[] rowFieldNames, String role) {
        int[] ordinals = new int[names.length];
        for (int i = 0; i < names.length; i++) {
            boolean found = false;
            for (int ordinal = 0; ordinal < rowFieldNames.length; ordinal++) {
                if (names[i].equals(rowFieldNames[ordinal])) {
                    ordinals[i] = ordinal;
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException(
                        "Cobble "
                                + role
                                + " column '"
                                + names[i]
                                + "' is missing from the write schema.");
            }
        }
        return ordinals;
    }

    /** Encodes the Cobble row key from the primary key fields of {@code row}. */
    public byte[] encodeKey(Row row, int[] keyOrdinals) {
        byte[][] parts = new byte[keyOrdinals.length][];
        for (int i = 0; i < keyOrdinals.length; i++) {
            parts[i] = encodeField(row, keyOrdinals[i], keyTypes[i]);
            if (parts[i] == null) {
                throw new IllegalArgumentException(
                        "Primary key column '" + keyNames[i] + "' must not be null.");
            }
        }
        return CobbleBytes.frameKeyParts(parts);
    }

    /**
     * Encodes one value column; returns {@code null} when the field is null so the caller issues a
     * per-column delete.
     */
    public byte[] encodeValue(Row row, int ordinal, int valueIndex) {
        return encodeField(row, ordinal, valueTypes[valueIndex]);
    }

    private byte[] encodeField(Row row, int ordinal, DataType type) {
        if (row.isNullAt(ordinal)) {
            return null;
        }
        if (type instanceof BooleanType) {
            return new byte[] {(byte) (row.getBoolean(ordinal) ? 1 : 0)};
        }
        if (type instanceof ByteType) {
            return new byte[] {row.getByte(ordinal)};
        }
        if (type instanceof ShortType) {
            return CobbleBytes.putShort(new byte[2], row.getShort(ordinal));
        }
        if (type instanceof IntegerType) {
            return CobbleBytes.putInt(new byte[4], row.getInt(ordinal));
        }
        if (type instanceof LongType) {
            return CobbleBytes.putLong(new byte[8], row.getLong(ordinal));
        }
        if (type instanceof FloatType) {
            return CobbleBytes.putInt(new byte[4], Float.floatToRawIntBits(row.getFloat(ordinal)));
        }
        if (type instanceof DoubleType) {
            return CobbleBytes.putLong(
                    new byte[8], Double.doubleToRawLongBits(row.getDouble(ordinal)));
        }
        if (type instanceof StringType) {
            return row.getString(ordinal).getBytes(StandardCharsets.UTF_8);
        }
        if (type instanceof BinaryType) {
            byte[] value = (byte[]) row.get(ordinal);
            byte[] copy = new byte[value.length];
            System.arraycopy(value, 0, copy, 0, value.length);
            return copy;
        }
        if (type instanceof DateType) {
            return CobbleBytes.putInt(
                    new byte[4], DateTimeUtils.fromJavaDate(row.getDate(ordinal)));
        }
        if (type instanceof TimestampType) {
            return CobbleBytes.putLong(
                    new byte[8], DateTimeUtils.fromJavaTimestamp(row.getTimestamp(ordinal)));
        }
        if (type instanceof DecimalType) {
            java.math.BigDecimal decimal = row.getDecimal(ordinal);
            return decimal.unscaledValue().toByteArray();
        }
        throw new IllegalArgumentException(
                "Unsupported Cobble column type " + type.catalogString() + ".");
    }
}
