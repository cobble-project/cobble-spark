package io.cobble.spark;

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

/** Validation of the Spark types supported by the Cobble connector. */
public final class CobbleSparkTypes {

    private CobbleSparkTypes() {}

    /** Returns true when the type can be stored in a Cobble column by this connector. */
    public static boolean isSupported(DataType type) {
        if (type instanceof BooleanType
                || type instanceof ByteType
                || type instanceof ShortType
                || type instanceof IntegerType
                || type instanceof LongType
                || type instanceof FloatType
                || type instanceof DoubleType
                || type instanceof StringType
                || type instanceof BinaryType
                || type instanceof DateType
                || type instanceof TimestampType) {
            return true;
        }
        if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return decimal.precision() > 0 && decimal.precision() <= 38;
        }
        return false;
    }

    /** Throws with a descriptive message when {@code type} cannot be stored. */
    public static void requireSupported(String fieldName, DataType type) {
        if (!isSupported(type)) {
            throw new IllegalArgumentException(
                    "Column '"
                            + fieldName
                            + "' has unsupported type "
                            + type.catalogString()
                            + ". Supported types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT,"
                            + " DOUBLE, STRING, BINARY, DATE, TIMESTAMP, DECIMAL(p<=38, s).");
        }
    }
}
