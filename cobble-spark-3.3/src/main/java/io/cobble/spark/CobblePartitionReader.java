package io.cobble.spark;

import io.cobble.ScanCursor;
import io.cobble.ScanOptions;
import io.cobble.ScanSplit;
import io.cobble.table.KeyCodec;
import io.cobble.table.Value;
import io.cobble.table.ValueCodec;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeSet;

/**
 * Reads one scan split: opens the scan cursor on the executor, decodes the length framed key and
 * the projected value columns into a reused internal row.
 */
public final class CobblePartitionReader implements PartitionReader<InternalRow> {

    private static final int READ_AHEAD_BYTES = 1 << 20;

    private final ScanSplit split;
    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final StructType requiredSchema;
    // Projection mapping per required field: primary key slot (>= 0) or value column index (>= 0).
    private final io.cobble.table.LogicalType[] fieldTypes;
    private final int[] fieldKeySlot;
    private final int[] fieldValueIndex;
    // Distinct value columns requested for the scan, ascending, and their entry positions.
    private final int[] requestedValueColumns;
    private final int[] positionByValueIndex;
    private final boolean needsKey;

    private GenericInternalRow row;
    private ScanCursor cursor;
    private ScanOptions scanOptions;

    public CobblePartitionReader(
            ScanSplit split,
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            StructType requiredSchema) {
        this.split = split;
        this.config = config;
        this.schema = schema;
        this.requiredSchema = requiredSchema;

        StructField[] required = requiredSchema.fields();
        this.fieldTypes = new io.cobble.table.LogicalType[required.length];
        this.fieldKeySlot = new int[required.length];
        this.fieldValueIndex = new int[required.length];
        TreeSet<Integer> requested = new TreeSet<>();
        boolean projectedKey = false;
        for (int i = 0; i < required.length; i++) {
            int ordinal = schema.ordinalOf(required[i].name());
            fieldTypes[i] = schema.logicalType(ordinal);
            int keySlot = schema.keySlotForOrdinal(ordinal);
            if (keySlot >= 0) {
                projectedKey = true;
                fieldKeySlot[i] = keySlot;
                fieldValueIndex[i] = -1;
            } else {
                fieldKeySlot[i] = -1;
                int valueIndex = schema.valueIndexForOrdinal(ordinal);
                fieldValueIndex[i] = valueIndex;
                requested.add(Integer.valueOf(valueIndex));
            }
        }
        this.needsKey = projectedKey;
        if (requested.isEmpty()) {
            // The scan API requires at least one column; request column 0 and ignore it.
            requested.add(Integer.valueOf(0));
        }
        this.requestedValueColumns = new int[requested.size()];
        this.positionByValueIndex = new int[schema.valueColumnCount()];
        java.util.Arrays.fill(positionByValueIndex, -1);
        int position = 0;
        for (Integer valueIndex : requested) {
            requestedValueColumns[position] = valueIndex.intValue();
            if (valueIndex.intValue() < positionByValueIndex.length) {
                positionByValueIndex[valueIndex.intValue()] = position;
            }
            position++;
        }
    }

    @Override
    public boolean next() throws IOException {
        if (cursor == null) {
            openCursor();
        }
        ScanCursor.Entry entry = cursor.nextEntry();
        if (entry == null) {
            return false;
        }
        if (row == null) {
            row = new GenericInternalRow(requiredSchema.fields().length);
        }
        List<Value> keyValues =
                needsKey
                        ? KeyCodec.decode(schema.primaryKeyTypes(), ByteBuffer.wrap(entry.key))
                        : java.util.Collections.emptyList();
        for (int i = 0; i < fieldKeySlot.length; i++) {
            Object value;
            if (fieldKeySlot[i] >= 0) {
                value =
                        CobbleSparkRowConverter.toSparkInternal(
                                fieldTypes[i], keyValues.get(fieldKeySlot[i]));
            } else {
                int valueIndex = fieldValueIndex[i];
                int position = positionByValueIndex[valueIndex];
                if (position < 0) {
                    throw new IOException(
                            "Cobble value column "
                                    + valueIndex
                                    + " was not requested in the scan.");
                }
                Value decoded =
                        ValueCodec.decodeOwned(
                                fieldTypes[i], ByteBuffer.wrap(entry.columns[position]));
                value = CobbleSparkRowConverter.toSparkInternal(fieldTypes[i], decoded);
            }
            row.update(i, value);
        }
        return true;
    }

    private void openCursor() throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        int totalBuckets = schema.totalBuckets();
        if (totalBuckets <= 0) {
            throw new IOException("Cobble native table schema has no bucket count.");
        }
        scanOptions =
                ScanOptions.forColumns(requestedValueColumns)
                        .columnFamily(CobbleTableRuntime.TABLE_NAME)
                        .readAheadBytes(READ_AHEAD_BYTES);
        try {
            cursor =
                    split.openScannerWithOptions(
                            CobblePaths.createScanConfig(
                                    config, totalBuckets, schema.valueColumnCount()),
                            scanOptions);
        } catch (RuntimeException e) {
            closeQuietly();
            throw new IOException(
                    "Failed to open Cobble scan cursor for split: " + e.getMessage(), e);
        }
    }

    @Override
    public InternalRow get() {
        if (row == null) {
            throw new IllegalStateException("Cobble reader has no current row.");
        }
        return row;
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        if (cursor != null) {
            try {
                cursor.close();
            } catch (RuntimeException e) {
                failure = new IOException("Failed to close Cobble scan cursor.", e);
            } finally {
                cursor = null;
            }
        }
        if (scanOptions != null) {
            try {
                scanOptions.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = new IOException("Failed to close Cobble scan options.", e);
                }
            } finally {
                scanOptions = null;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // Best effort cleanup on the failure path.
        }
    }
}
