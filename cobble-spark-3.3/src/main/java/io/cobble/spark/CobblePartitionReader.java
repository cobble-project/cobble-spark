package io.cobble.spark;

import io.cobble.DirectScanCursor;
import io.cobble.DirectScanEntry;
import io.cobble.table.KeyCodec;
import io.cobble.table.TableScanSplit;
import io.cobble.table.Value;
import io.cobble.table.ValueCodec;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads one scan split: opens the direct scan cursor on the executor, decodes Cobble's ordered
 * composite key and projected value columns into a reused internal row.
 */
public final class CobblePartitionReader implements PartitionReader<InternalRow> {

    private static final int READ_AHEAD_BYTES = 1 << 20;

    private final TableScanSplit split;
    private final CobbleOptions.CobbleTableConfig config;
    /** Snapshot schema used to decode physical keys and values. */
    private final CobbleTableSchema sourceSchema;

    private final StructType requiredSchema;
    // Projection mapping per required field: primary key slot (>= 0) or value column index (>= 0).
    private final io.cobble.table.LogicalType[] fieldTypes;
    private final List<io.cobble.table.LogicalType> primaryKeyTypes;
    private final int[] fieldKeySlot;
    private final boolean[] fieldMissingFromSource;
    // Entry-column positions for non-key fields, derived once from the projected semantic fields.
    private final int[] fieldValuePosition;
    private final List<String> projectedFields;
    private final boolean needsKey;
    private final Cast[] casts;

    private GenericInternalRow row;
    private GenericInternalRow castInput;
    private DirectScanCursor cursor;

    public CobblePartitionReader(
            TableScanSplit split,
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema sourceSchema,
            CobbleTableSchema targetSchema,
            StructType requiredSchema) {
        this.split = split;
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.requiredSchema = requiredSchema;

        StructField[] required = requiredSchema.fields();
        this.fieldTypes = new io.cobble.table.LogicalType[required.length];
        this.primaryKeyTypes = sourceSchema.primaryKeyTypes();
        this.fieldKeySlot = new int[required.length];
        this.fieldMissingFromSource = new boolean[required.length];
        this.fieldValuePosition = new int[required.length];
        this.casts = new Cast[required.length];
        String[] projectedNamesByValueIndex = new String[sourceSchema.valueColumnCount()];
        int[] valueIndexByField = new int[required.length];
        java.util.Arrays.fill(valueIndexByField, -1);
        boolean projectedKey = false;
        for (int i = 0; i < required.length; i++) {
            int targetOrdinal = targetSchema.ordinalOf(required[i].name());
            int sourceOrdinal = sourceSchema.ordinalForFieldId(targetSchema.fieldId(targetOrdinal));
            if (sourceOrdinal < 0) {
                fieldMissingFromSource[i] = true;
                fieldKeySlot[i] = -1;
                fieldValuePosition[i] = -1;
                continue;
            }
            fieldTypes[i] = sourceSchema.logicalType(sourceOrdinal);
            int keySlot = sourceSchema.keySlotForOrdinal(sourceOrdinal);
            if (keySlot >= 0) {
                projectedKey = true;
                fieldKeySlot[i] = keySlot;
                fieldValuePosition[i] = -1;
            } else {
                fieldKeySlot[i] = -1;
                int valueIndex = sourceSchema.valueIndexForOrdinal(sourceOrdinal);
                valueIndexByField[i] = valueIndex;
                projectedNamesByValueIndex[valueIndex] =
                        sourceSchema.toStructType().fields()[sourceOrdinal].name();
            }
            if (!sourceSchema
                    .toStructType()
                    .fields()[sourceOrdinal]
                    .dataType()
                    .sameType(required[i].dataType())) {
                casts[i] =
                        new Cast(
                                new BoundReference(
                                        0,
                                        sourceSchema
                                                .toStructType()
                                                .fields()[sourceOrdinal]
                                                .dataType(),
                                        true),
                                required[i].dataType(),
                                scala.Option.empty());
            }
        }
        this.needsKey = projectedKey;
        List<String> names = new ArrayList<String>(projectedNamesByValueIndex.length);
        int[] positionByValueIndex = new int[projectedNamesByValueIndex.length];
        java.util.Arrays.fill(positionByValueIndex, -1);
        for (int valueIndex = 0; valueIndex < projectedNamesByValueIndex.length; valueIndex++) {
            String name = projectedNamesByValueIndex[valueIndex];
            if (name != null) {
                positionByValueIndex[valueIndex] = names.size();
                names.add(name);
            }
        }
        if (names.isEmpty()) {
            // The table scan retains one internal value column for row existence on key-only and
            // zero-column projections. It is intentionally not decoded by this reader.
            projectedFields = Collections.singletonList(sourceSchema.primaryKeys().get(0));
        } else {
            projectedFields = Collections.unmodifiableList(names);
        }
        for (int i = 0; i < valueIndexByField.length; i++) {
            int valueIndex = valueIndexByField[i];
            if (valueIndex >= 0) {
                fieldValuePosition[i] = positionByValueIndex[valueIndex];
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        if (cursor == null) {
            openCursor();
        }
        DirectScanEntry entry = cursor.nextEntry();
        if (entry == null) {
            return false;
        }
        if (row == null) {
            row = new GenericInternalRow(requiredSchema.fields().length);
        }
        List<Value> keyValues =
                needsKey
                        ? KeyCodec.decode(primaryKeyTypes, entry.getKey())
                        : Collections.emptyList();
        io.cobble.DirectColumns columns = null;
        for (int i = 0; i < fieldKeySlot.length; i++) {
            Object value;
            if (fieldMissingFromSource[i]) {
                value = null;
            } else if (fieldKeySlot[i] >= 0) {
                value =
                        CobbleSparkRowConverter.toSparkInternal(
                                fieldTypes[i], keyValues.get(fieldKeySlot[i]));
            } else {
                int position = fieldValuePosition[i];
                if (position < 0) {
                    throw new IOException(
                            "Cobble value field at output position "
                                    + i
                                    + " was not requested in the scan.");
                }
                if (columns == null) {
                    columns = entry.columnsView();
                }
                ByteBuffer encoded = columns.get(position);
                if (encoded == null) {
                    // An added nullable field can be absent from an older physical SST after a
                    // later snapshot is materialized. Native represents that as no column bytes.
                    value = null;
                } else {
                    // Direct columns borrow the cursor's reusable native buffer. Convert to a
                    // Spark-owned value before advancing the cursor; binary leaves are copied by
                    // toSparkInternal exactly once.
                    Value decoded = ValueCodec.decode(fieldTypes[i], encoded);
                    value = CobbleSparkRowConverter.toSparkInternal(fieldTypes[i], decoded);
                }
            }
            if (value != null && casts[i] != null) {
                if (castInput == null) {
                    castInput = new GenericInternalRow(1);
                }
                castInput.update(0, value);
                value = casts[i].eval(castInput);
            }
            row.update(i, value);
        }
        return true;
    }

    private void openCursor() throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        int totalBuckets = sourceSchema.totalBuckets();
        if (totalBuckets <= 0) {
            throw new IOException("Cobble native table schema has no bucket count.");
        }
        try {
            cursor =
                    split.openDirectScanner(
                            CobblePaths.createScanConfig(
                                    config, totalBuckets, sourceSchema.valueColumnCount()),
                            projectedFields,
                            READ_AHEAD_BYTES);
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
