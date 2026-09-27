package io.cobble.spark;

import io.cobble.table.TableReadCursor;
import io.cobble.table.TableReadEntry;
import io.cobble.table.TableReadProvider;
import io.cobble.table.TableReadRange;
import io.cobble.table.TableReadSession;
import io.cobble.table.TableScanPlan;
import io.cobble.table.Value;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

/**
 * Reads one fixed scan split through its snapshot-selected format provider and converts typed
 * Cobble values into a reused Spark internal row.
 */
public final class CobblePartitionReader implements PartitionReader<InternalRow> {

    private final TableScanPlan plan;
    private final CobbleOptions.CobbleTableConfig config;
    /** Snapshot schema used to decode physical keys and values. */
    private final CobbleTableSchema sourceSchema;

    private final StructType requiredSchema;
    private final io.cobble.table.LogicalType[] fieldTypes;
    private final boolean[] fieldMissingFromSource;
    private final int[] fieldValuePosition;
    private final Cast[] casts;

    private GenericInternalRow row;
    private GenericInternalRow castInput;
    private TableReadProvider<List<Value>, ?> provider;
    private TableReadSession<List<Value>, ?> session;
    private TableReadCursor<List<Value>> cursor;

    public CobblePartitionReader(
            TableScanPlan plan,
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema sourceSchema,
            CobbleTableSchema targetSchema,
            StructType requiredSchema) {
        this.plan = plan;
        this.config = config;
        this.sourceSchema = sourceSchema;
        this.requiredSchema = requiredSchema;

        StructField[] required = requiredSchema.fields();
        this.fieldTypes = new io.cobble.table.LogicalType[required.length];
        this.fieldMissingFromSource = new boolean[required.length];
        this.fieldValuePosition = new int[required.length];
        this.casts = new Cast[required.length];
        for (int i = 0; i < required.length; i++) {
            int targetOrdinal = targetSchema.ordinalOf(required[i].name());
            int sourceOrdinal = sourceSchema.ordinalForFieldId(targetSchema.fieldId(targetOrdinal));
            if (sourceOrdinal < 0) {
                fieldMissingFromSource[i] = true;
                fieldValuePosition[i] = -1;
                continue;
            }
            fieldTypes[i] = sourceSchema.logicalType(sourceOrdinal);
            fieldValuePosition[i] = readValuePosition(sourceOrdinal);
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
    }

    private int readValuePosition(int sourceOrdinal) {
        String sourceName = sourceSchema.toStructType().fields()[sourceOrdinal].name();
        List<io.cobble.table.DataField> fields = plan.readSchema().fields();
        for (int i = 0; i < fields.size(); i++) {
            if (sourceName.equals(fields.get(i).name())) return i;
        }
        throw new IllegalArgumentException(
                "Cobble scan projection is missing source field '" + sourceName + "'.");
    }

    @Override
    public boolean next() throws IOException {
        if (cursor == null) {
            openCursor();
        }
        TableReadEntry<List<Value>> entry;
        try {
            entry = cursor.next();
        } catch (Exception error) {
            throw new IOException("Failed to read Cobble table row", error);
        }
        if (entry == null) {
            return false;
        }
        if (row == null) {
            row = new GenericInternalRow(requiredSchema.fields().length);
        }
        for (int i = 0; i < fieldValuePosition.length; i++) {
            Object value;
            if (fieldMissingFromSource[i]) {
                value = null;
            } else {
                int position = fieldValuePosition[i];
                if (position < 0) {
                    throw new IOException(
                            "Cobble value field at output position "
                                    + i
                                    + " was not requested in the scan.");
                }
                value =
                        CobbleSparkRowConverter.toSparkInternal(
                                fieldTypes[i], entry.value().get(position));
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
        try {
            provider =
                    plan.open(
                            CobblePaths.createScanConfig(config, sourceSchema.totalBuckets(), 1),
                            plan.splits().get(0));
            session = provider.open();
            cursor = session.scan(new TableReadRange(0, Integer.MAX_VALUE), null);
        } catch (Exception e) {
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
        if (session != null) {
            try {
                session.close();
            } catch (RuntimeException error) {
                if (failure == null)
                    failure = new IOException("Failed to close Cobble read session.", error);
            } finally {
                session = null;
            }
        }
        if (provider != null) {
            try {
                provider.close();
            } catch (RuntimeException error) {
                if (failure == null)
                    failure = new IOException("Failed to close Cobble read provider.", error);
            } finally {
                provider = null;
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
