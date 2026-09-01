package io.cobble.spark;

import io.cobble.table.DataField;
import io.cobble.table.DecimalType;
import io.cobble.table.ListType;
import io.cobble.table.LogicalType;
import io.cobble.table.LogicalTypes;
import io.cobble.table.RecordType;
import io.cobble.table.TimestampKind;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/** Exact mapping between Spark SQL types and Cobble's cross-engine logical types. */
public final class CobbleSparkTypes {
    private CobbleSparkTypes() {}

    static LogicalType toCobbleType(DataType type, boolean nullable, long[] nextNestedId) {
        LogicalType converted;
        if (type.sameType(DataTypes.BooleanType)) converted = LogicalTypes.bool();
        else if (type.sameType(DataTypes.ByteType)) converted = LogicalTypes.int8();
        else if (type.sameType(DataTypes.ShortType)) converted = LogicalTypes.int16();
        else if (type.sameType(DataTypes.IntegerType)) converted = LogicalTypes.int32();
        else if (type.sameType(DataTypes.LongType)) converted = LogicalTypes.int64();
        else if (type.sameType(DataTypes.FloatType)) converted = LogicalTypes.float32();
        else if (type.sameType(DataTypes.DoubleType)) converted = LogicalTypes.float64();
        else if (type instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType decimal =
                    (org.apache.spark.sql.types.DecimalType) type;
            if (decimal.precision() < 1 || decimal.precision() > 38 || decimal.scale() < 0) {
                throw unsupported(type);
            }
            converted = LogicalTypes.decimal(decimal.precision(), decimal.scale());
        } else if (type.sameType(DataTypes.DateType)) converted = LogicalTypes.date();
        else if (type.sameType(DataTypes.TimestampType)) {
            // Spark 3.3 TIMESTAMP is an instant represented in microseconds.
            converted = LogicalTypes.timestamp(6, TimestampKind.WITH_LOCAL_TIME_ZONE);
        } else if (type.sameType(DataTypes.StringType)) converted = LogicalTypes.string();
        else if (type.sameType(DataTypes.BinaryType)) converted = LogicalTypes.binary();
        else if (type instanceof ArrayType) {
            ArrayType array = (ArrayType) type;
            converted =
                    LogicalTypes.list(
                            toCobbleType(array.elementType(), array.containsNull(), nextNestedId));
        } else if (type instanceof MapType) {
            MapType map = (MapType) type;
            converted =
                    LogicalTypes.map(
                            toCobbleType(map.keyType(), false, nextNestedId),
                            toCobbleType(map.valueType(), map.valueContainsNull(), nextNestedId));
        } else if (type instanceof StructType) {
            StructField[] fields = ((StructType) type).fields();
            List<DataField> nested = new ArrayList<DataField>(fields.length);
            for (StructField field : fields) {
                nested.add(
                        new DataField(
                                nextNestedId[0]++,
                                field.name(),
                                toCobbleType(field.dataType(), field.nullable(), nextNestedId)));
            }
            converted = LogicalTypes.struct(new RecordType(nested));
        } else {
            throw unsupported(type);
        }
        return nullable ? converted.nullable() : converted.notNull();
    }

    static StructField toSparkField(DataField field) {
        return new StructField(
                field.name(),
                toSparkType(field.logicalType()),
                field.logicalType().isNullable(),
                Metadata.empty());
    }

    static DataType toSparkType(LogicalType type) {
        switch (type.kind()) {
            case BOOLEAN:
                return DataTypes.BooleanType;
            case INT8:
                return DataTypes.ByteType;
            case INT16:
                return DataTypes.ShortType;
            case INT32:
                return DataTypes.IntegerType;
            case INT64:
                return DataTypes.LongType;
            case FLOAT32:
                return DataTypes.FloatType;
            case FLOAT64:
                return DataTypes.DoubleType;
            case DECIMAL:
                DecimalType decimal = (DecimalType) type;
                return DataTypes.createDecimalType(decimal.precision(), decimal.scale());
            case DATE:
                return DataTypes.DateType;
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case STRING:
                return DataTypes.StringType;
            case BINARY:
                return DataTypes.BinaryType;
            case LIST:
                ListType list = (ListType) type;
                return DataTypes.createArrayType(
                        toSparkType(list.elementType()), list.elementType().isNullable());
            case MAP:
                io.cobble.table.MapType map = (io.cobble.table.MapType) type;
                return DataTypes.createMapType(
                        toSparkType(map.keyType()),
                        toSparkType(map.valueType()),
                        map.valueType().isNullable());
            case STRUCT:
                io.cobble.table.StructType struct = (io.cobble.table.StructType) type;
                List<StructField> fields =
                        new ArrayList<StructField>(struct.recordType().fields().size());
                for (DataField field : struct.recordType().fields())
                    fields.add(toSparkField(field));
                return DataTypes.createStructType(fields);
            case TIME:
                throw new IllegalArgumentException(
                        "Cobble TIME cannot be represented by Spark 3.3 SQL types.");
            case EXTENSION:
                throw new IllegalArgumentException(
                        "Cobble extension types require an explicitly registered Spark mapping.");
            default:
                throw new IllegalArgumentException(
                        "Unsupported Cobble logical type: " + type.kind());
        }
    }

    private static IllegalArgumentException unsupported(DataType type) {
        return new IllegalArgumentException(
                "Unsupported Spark type "
                        + type.catalogString()
                        + ". Supported types are primitive SQL types, DECIMAL(p<=38, scale>=0),"
                        + " ARRAY, MAP, and STRUCT; Spark 3.3 has no TIME mapping.");
    }
}
