package io.cobble.spark;

import io.cobble.table.DataField;
import io.cobble.table.LogicalType;
import io.cobble.table.TableSchema;
import io.cobble.table.TableSchemaChange;

import org.apache.spark.sql.connector.catalog.TableChange;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts one complete Spark ALTER batch into the native catalog's atomic schema edit list. */
final class CobbleSchemaChanges {

    private CobbleSchemaChanges() {}

    static List<TableSchemaChange> toNativeChanges(TableSchema schema, TableChange... changes) {
        if (changes == null || changes.length == 0) {
            throw new UnsupportedOperationException(
                    "Cobble ALTER TABLE requires at least one change.");
        }
        Map<String, MutableField> fields = new LinkedHashMap<String, MutableField>();
        Set<Long> primaryKeyIds = new LinkedHashSet<Long>(schema.primaryKey());
        for (DataField field : schema.fields()) {
            fields.put(field.name(), new MutableField(field.id(), field.logicalType()));
        }
        List<TableSchemaChange> result = new ArrayList<TableSchemaChange>(changes.length);
        for (TableChange change : changes) {
            if (change instanceof TableChange.AddColumn) {
                TableChange.AddColumn add = (TableChange.AddColumn) change;
                String name = topLevelColumn(add.fieldNames(), "ADD COLUMN");
                if (!add.isNullable()) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE only supports adding nullable columns.");
                }
                if (add.comment() != null) {
                    throw new UnsupportedOperationException(
                            "Cobble catalog does not persist column comments.");
                }
                if (add.position() != null) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE only supports appending columns.");
                }
                if (fields.containsKey(name)) {
                    throw new IllegalArgumentException(
                            "Cobble column already exists: '" + name + "'.");
                }
                LogicalType type = toCobbleType(add.dataType(), true);
                fields.put(name, new MutableField(-1L, type));
                result.add(TableSchemaChange.addField(name, type));
            } else if (change instanceof TableChange.DeleteColumn) {
                TableChange.DeleteColumn delete = (TableChange.DeleteColumn) change;
                if (Boolean.TRUE.equals(delete.ifExists())) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE does not support DROP COLUMN IF EXISTS.");
                }
                String name = topLevelColumn(delete.fieldNames(), "DROP COLUMN");
                MutableField field = requireField(fields, name);
                if (primaryKeyIds.contains(Long.valueOf(field.id))) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE cannot drop primary key column '" + name + "'.");
                }
                fields.remove(name);
                result.add(TableSchemaChange.dropField(name));
            } else if (change instanceof TableChange.RenameColumn) {
                TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
                String name = topLevelColumn(rename.fieldNames(), "RENAME COLUMN");
                MutableField field = requireField(fields, name);
                String newName = rename.newName();
                if (newName == null || newName.trim().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Cobble renamed column name must not be empty.");
                }
                if (!newName.equals(name) && fields.containsKey(newName)) {
                    throw new IllegalArgumentException(
                            "Cobble column already exists: '" + newName + "'.");
                }
                fields.remove(name);
                fields.put(newName, field);
                result.add(TableSchemaChange.renameField(name, newName));
            } else if (change instanceof TableChange.UpdateColumnType) {
                TableChange.UpdateColumnType update = (TableChange.UpdateColumnType) change;
                String name = topLevelColumn(update.fieldNames(), "ALTER COLUMN TYPE");
                MutableField field = requireField(fields, name);
                if (primaryKeyIds.contains(Long.valueOf(field.id))) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE cannot change primary key column '" + name + "'.");
                }
                LogicalType type =
                        toCobbleType(update.newDataType(), field.logicalType.isNullable());
                field.logicalType = type;
                result.add(TableSchemaChange.alterFieldType(name, type));
            } else if (change instanceof TableChange.UpdateColumnNullability) {
                TableChange.UpdateColumnNullability update =
                        (TableChange.UpdateColumnNullability) change;
                String name = topLevelColumn(update.fieldNames(), "ALTER COLUMN NULLABILITY");
                MutableField field = requireField(fields, name);
                if (!update.nullable() || field.logicalType.isNullable()) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE only supports widening NOT NULL columns to nullable.");
                }
                if (primaryKeyIds.contains(Long.valueOf(field.id))) {
                    throw new UnsupportedOperationException(
                            "Cobble ALTER TABLE cannot change primary key column '" + name + "'.");
                }
                field.logicalType = field.logicalType.nullable();
                result.add(TableSchemaChange.alterFieldType(name, field.logicalType));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported Cobble ALTER TABLE change: "
                                + change.getClass().getSimpleName());
            }
        }
        return result;
    }

    private static LogicalType toCobbleType(
            org.apache.spark.sql.types.DataType type, boolean nullable) {
        return CobbleSparkTypes.toCobbleType(type, nullable, new long[] {0L});
    }

    private static String topLevelColumn(String[] names, String operation) {
        if (names == null || names.length != 1) {
            throw new UnsupportedOperationException(
                    "Cobble " + operation + " only supports top-level columns.");
        }
        return names[0];
    }

    private static MutableField requireField(Map<String, MutableField> fields, String name) {
        MutableField field = fields.get(name);
        if (field == null) {
            throw new IllegalArgumentException("Unknown Cobble column '" + name + "'.");
        }
        return field;
    }

    private static final class MutableField {
        private final long id;
        private LogicalType logicalType;

        private MutableField(long id, LogicalType logicalType) {
            this.id = id;
            this.logicalType = logicalType;
        }
    }
}
