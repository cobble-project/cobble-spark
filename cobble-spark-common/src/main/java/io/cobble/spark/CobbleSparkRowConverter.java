package io.cobble.spark;

import io.cobble.table.BucketHash;
import io.cobble.table.KeyCodec;
import io.cobble.table.LogicalType;
import io.cobble.table.Value;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Converts Spark rows to the same native values and codecs used by the Flink connector. */
public final class CobbleSparkRowConverter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final CobbleTableSchema schema;
    private final int[] bucketOrdinals;
    private transient BucketHash bucketHash;

    public CobbleSparkRowConverter(CobbleTableSchema schema) {
        this.schema = schema;
        this.bucketOrdinals = schema.bucketKeyOrdinals();
        this.bucketHash = new BucketHash(schema.totalBuckets());
    }

    public List<Value> toValues(Row row) {
        List<io.cobble.table.DataField> fields = schema.toTableSchema().fields();
        if (row.size() != fields.size()) {
            throw new IllegalArgumentException("Spark row does not match the Cobble table arity.");
        }
        List<Value> values = new ArrayList<Value>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            values.add(toValue(fields.get(i).logicalType(), row.isNullAt(i) ? null : row.get(i)));
        }
        return values;
    }

    /** Computes the bucket while converting only bucket-key fields, not the complete row. */
    public int bucket(Row row) {
        List<Value> bucketValues = new ArrayList<Value>(bucketOrdinals.length);
        for (int ordinal : bucketOrdinals) {
            bucketValues.add(
                    toValue(
                            schema.logicalType(ordinal),
                            row.isNullAt(ordinal) ? null : row.get(ordinal)));
        }
        byte[] encoded = KeyCodec.encode(schema.bucketKeyTypes(), bucketValues);
        BucketHash hash = bucketHash;
        if (hash == null) {
            hash = new BucketHash(schema.totalBuckets());
            bucketHash = hash;
        }
        return hash.bucket(encoded);
    }

    public static Object toSparkInternal(LogicalType type, Value value) {
        if (value.kind() == Value.Kind.NULL) return null;
        switch (type.kind()) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case DATE:
                return value.raw();
            case DECIMAL:
                Value.Decimal decimal = (Value.Decimal) value.raw();
                return Decimal.apply(new BigDecimal(decimal.unscaled, decimal.scale));
            case TIMESTAMP:
                Value.Timestamp timestamp = (Value.Timestamp) value.raw();
                return Long.valueOf(
                        Math.addExact(
                                Math.multiplyExact(timestamp.seconds, 1_000_000L),
                                timestamp.nanos / 1_000L));
            case STRING:
                return UTF8String.fromString((String) value.raw());
            case BINARY:
                return copyBytes((ByteBuffer) value.raw());
            case LIST:
                return toInternalArray((io.cobble.table.ListType) type, value);
            case MAP:
                return toInternalMap((io.cobble.table.MapType) type, value);
            case STRUCT:
                return toInternalRow((io.cobble.table.StructType) type, value);
            case TIME:
                throw new IllegalArgumentException("Spark 3.3 cannot represent Cobble TIME.");
            case EXTENSION:
                throw new IllegalArgumentException(
                        "No Spark mapping is registered for Cobble extension types.");
            default:
                throw new IllegalArgumentException("Unsupported Cobble value type: " + type.kind());
        }
    }

    private static Value toValue(LogicalType type, Object value) {
        if (value == null) return Value.nullValue();
        switch (type.kind()) {
            case BOOLEAN:
                return Value.bool((Boolean) value);
            case INT8:
                return Value.int8(((Number) value).byteValue());
            case INT16:
                return Value.int16(((Number) value).shortValue());
            case INT32:
                return Value.int32(((Number) value).intValue());
            case INT64:
                return Value.int64(((Number) value).longValue());
            case FLOAT32:
                return Value.float32(((Number) value).floatValue());
            case FLOAT64:
                return Value.float64(((Number) value).doubleValue());
            case DECIMAL:
                io.cobble.table.DecimalType decimalType = (io.cobble.table.DecimalType) type;
                BigDecimal decimal =
                        value instanceof Decimal
                                ? ((Decimal) value).toJavaBigDecimal()
                                : (BigDecimal) value;
                return Value.decimal(
                        decimalType.precision(), decimalType.scale(), decimal.unscaledValue());
            case DATE:
                if (value instanceof Date) {
                    return Value.date(
                            org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(
                                    (Date) value));
                }
                return Value.date(((Number) value).intValue());
            case TIMESTAMP:
                io.cobble.table.TimestampType timestampType = (io.cobble.table.TimestampType) type;
                long micros =
                        value instanceof Timestamp
                                ? org.apache.spark.sql.catalyst.util.DateTimeUtils
                                        .fromJavaTimestamp((Timestamp) value)
                                : ((Number) value).longValue();
                long seconds = Math.floorDiv(micros, 1_000_000L);
                int nanos = (int) Math.floorMod(micros, 1_000_000L) * 1_000;
                return Value.timestamp(
                        timestampType.precision(), timestampType.timestampKind(), seconds, nanos);
            case STRING:
                return Value.string(value.toString());
            case BINARY:
                return Value.binary(ByteBuffer.wrap((byte[]) value));
            case LIST:
                return listValue((io.cobble.table.ListType) type, value);
            case MAP:
                return mapValue((io.cobble.table.MapType) type, value);
            case STRUCT:
                return structValue((io.cobble.table.StructType) type, (Row) value);
            case TIME:
                throw new IllegalArgumentException("Spark 3.3 cannot write Cobble TIME.");
            case EXTENSION:
                throw new IllegalArgumentException(
                        "No Spark mapping is registered for Cobble extension types.");
            default:
                throw new IllegalArgumentException("Unsupported Cobble value type: " + type.kind());
        }
    }

    private static Value listValue(io.cobble.table.ListType type, Object value) {
        List<?> elements = asJavaList(value);
        List<Value> converted = new ArrayList<Value>(elements.size());
        for (Object element : elements) converted.add(toValue(type.elementType(), element));
        return Value.list(converted);
    }

    private static Value mapValue(io.cobble.table.MapType type, Object value) {
        Map<?, ?> map = asJavaMap(value);
        List<Map.Entry<Value, Value>> converted =
                new ArrayList<Map.Entry<Value, Value>>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            converted.add(
                    new AbstractMap.SimpleImmutableEntry<Value, Value>(
                            toValue(type.keyType(), entry.getKey()),
                            toValue(type.valueType(), entry.getValue())));
        }
        return Value.map(converted);
    }

    private static Value structValue(io.cobble.table.StructType type, Row row) {
        List<io.cobble.table.DataField> fields = type.recordType().fields();
        List<Value> converted = new ArrayList<Value>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            converted.add(
                    toValue(fields.get(i).logicalType(), row.isNullAt(i) ? null : row.get(i)));
        }
        return Value.struct(converted);
    }

    @SuppressWarnings("unchecked")
    private static List<?> asJavaList(Object value) {
        if (value instanceof List) return (List<?>) value;
        if (value instanceof scala.collection.Seq) {
            return scala.collection.JavaConverters.seqAsJavaListConverter(
                            (scala.collection.Seq<Object>) value)
                    .asJava();
        }
        if (value instanceof Object[]) {
            return java.util.Arrays.asList((Object[]) value);
        }
        throw new IllegalArgumentException("Expected a Spark ARRAY value but found " + value);
    }

    @SuppressWarnings("unchecked")
    private static Map<?, ?> asJavaMap(Object value) {
        if (value instanceof Map) return (Map<?, ?>) value;
        if (value instanceof scala.collection.Map) {
            return scala.collection.JavaConverters.mapAsJavaMapConverter(
                            (scala.collection.Map<Object, Object>) value)
                    .asJava();
        }
        throw new IllegalArgumentException("Expected a Spark MAP value but found " + value);
    }

    @SuppressWarnings("unchecked")
    private static GenericArrayData toInternalArray(io.cobble.table.ListType type, Value value) {
        List<Value> values = (List<Value>) value.raw();
        Object[] converted = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            converted[i] = toSparkInternal(type.elementType(), values.get(i));
        }
        return new GenericArrayData(converted);
    }

    @SuppressWarnings("unchecked")
    private static ArrayBasedMapData toInternalMap(io.cobble.table.MapType type, Value value) {
        List<Map.Entry<Value, Value>> entries = (List<Map.Entry<Value, Value>>) value.raw();
        Object[] keys = new Object[entries.size()];
        Object[] values = new Object[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            keys[i] = toSparkInternal(type.keyType(), entries.get(i).getKey());
            values[i] = toSparkInternal(type.valueType(), entries.get(i).getValue());
        }
        return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
    }

    @SuppressWarnings("unchecked")
    private static InternalRow toInternalRow(io.cobble.table.StructType type, Value value) {
        List<Value> values = (List<Value>) value.raw();
        List<io.cobble.table.DataField> fields = type.recordType().fields();
        GenericInternalRow row = new GenericInternalRow(values.size());
        for (int i = 0; i < values.size(); i++) {
            row.update(i, toSparkInternal(fields.get(i).logicalType(), values.get(i)));
        }
        return row;
    }

    private static byte[] copyBytes(ByteBuffer value) {
        ByteBuffer bytes = value.duplicate();
        byte[] result = new byte[bytes.remaining()];
        bytes.get(result);
        return result;
    }
}
