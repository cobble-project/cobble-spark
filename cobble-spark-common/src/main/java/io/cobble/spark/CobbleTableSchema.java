package io.cobble.spark;

import io.cobble.table.DataField;
import io.cobble.table.LogicalType;
import io.cobble.table.TableReadSchema;
import io.cobble.table.TableSchema;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Serializable Spark view of the semantic schema stored by Cobble's native table layer. */
public final class CobbleTableSchema implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TableSchema tableSchema;
    private final StructType sparkSchema;
    private final List<DataField> fields;
    private final List<String> primaryKeys;
    private final int totalBuckets;
    private transient volatile Map<Long, Integer> positionsById;

    private CobbleTableSchema(
            TableSchema tableSchema,
            StructType sparkSchema,
            List<String> primaryKeys,
            int totalBuckets) {
        this.tableSchema = tableSchema;
        this.sparkSchema = sparkSchema;
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<DataField>(
                                tableSchema == null
                                        ? Collections.<DataField>emptyList()
                                        : tableSchema.fields()));
        this.primaryKeys = Collections.unmodifiableList(new ArrayList<String>(primaryKeys));
        this.totalBuckets = totalBuckets;
    }

    /** Builds a native table schema from Spark SQL fields and primary keys. */
    public static CobbleTableSchema fromStructType(
            StructType schema, List<String> primaryKeys, int totalBuckets) {
        if (schema == null || schema.fields().length == 0) {
            throw new IllegalArgumentException(
                    "Cobble table schema must contain at least one column.");
        }
        if (totalBuckets < 1 || totalBuckets > 65536) {
            throw new IllegalArgumentException("Cobble bucket count must be in [1, 65536].");
        }
        List<String> keys = normalizePrimaryKeys(primaryKeys);
        if (keys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Creating a Cobble table requires the '"
                            + CobbleOptions.PRIMARY_KEY
                            + "' option listing the primary key columns.");
        }

        Set<String> keyNames = new LinkedHashSet<String>(keys);
        long[] nextNestedId = new long[] {schema.fields().length};
        List<DataField> fields = new ArrayList<DataField>(schema.fields().length);
        Map<String, Long> idsByName = new LinkedHashMap<String, Long>();
        StructField[] sparkFields = schema.fields();
        for (int i = 0; i < sparkFields.length; i++) {
            StructField field = sparkFields[i];
            boolean key = keyNames.contains(field.name());
            if (key && field.nullable()) {
                throw new IllegalArgumentException(
                        "Primary key column '" + field.name() + "' must not be nullable.");
            }
            LogicalType logicalType =
                    CobbleSparkTypes.toCobbleType(
                            field.dataType(), key ? false : field.nullable(), nextNestedId);
            fields.add(new DataField(i, field.name(), logicalType));
            idsByName.put(field.name(), Long.valueOf(i));
        }
        List<Long> keyIds = new ArrayList<Long>(keys.size());
        for (String key : keys) {
            Long id = idsByName.get(key);
            if (id == null) {
                throw new IllegalArgumentException(
                        "Primary key column '" + key + "' is not present in the schema.");
            }
            keyIds.add(id);
        }
        TableSchema tableSchema = new TableSchema(fields, keyIds, keyIds);
        return new CobbleTableSchema(tableSchema, schema, keys, totalBuckets);
    }

    /** Creates a Spark view from schema metadata embedded in a native shard snapshot. */
    public static CobbleTableSchema fromTableSchema(TableSchema schema, int totalBuckets) {
        Map<Long, DataField> fieldsById = new LinkedHashMap<Long, DataField>();
        List<StructField> sparkFields = new ArrayList<StructField>(schema.fields().size());
        for (DataField field : schema.fields()) {
            fieldsById.put(Long.valueOf(field.id()), field);
            sparkFields.add(CobbleSparkTypes.toSparkField(field));
        }
        List<String> keys = new ArrayList<String>(schema.primaryKey().size());
        for (Long id : schema.primaryKey()) {
            DataField field = fieldsById.get(id);
            if (field == null) {
                throw new IllegalArgumentException("Cobble primary key field is missing: " + id);
            }
            keys.add(field.name());
        }
        return new CobbleTableSchema(
                schema,
                new StructType(sparkFields.toArray(new StructField[0])),
                keys,
                totalBuckets);
    }

    /** Builds a read-only Spark schema from a generic table-read schema, without requiring keys. */
    public static CobbleTableSchema fromReadSchema(TableReadSchema schema, int totalBuckets) {
        List<StructField> sparkFields = new ArrayList<StructField>(schema.fields().size());
        for (DataField field : schema.fields())
            sparkFields.add(CobbleSparkTypes.toSparkField(field));
        return new CobbleTableSchema(
                null,
                new StructType(sparkFields.toArray(new StructField[0])),
                Collections.<String>emptyList(),
                totalBuckets,
                schema.fields());
    }

    private CobbleTableSchema(
            TableSchema tableSchema,
            StructType sparkSchema,
            List<String> primaryKeys,
            int totalBuckets,
            List<DataField> fields) {
        this.tableSchema = tableSchema;
        this.sparkSchema = sparkSchema;
        this.fields = Collections.unmodifiableList(new ArrayList<DataField>(fields));
        this.primaryKeys = Collections.unmodifiableList(new ArrayList<String>(primaryKeys));
        this.totalBuckets = totalBuckets;
    }

    public TableSchema toTableSchema() {
        if (tableSchema == null)
            throw new UnsupportedOperationException("read-only format has no write schema");
        return tableSchema;
    }

    public StructType toStructType() {
        return sparkSchema;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public int ordinalOf(String columnName) {
        StructField[] fields = sparkSchema.fields();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].name().equals(columnName)) return i;
        }
        throw new IllegalArgumentException("Unknown Cobble column '" + columnName + "'.");
    }

    /** Returns the stable native field id for a Spark-schema ordinal. */
    public long fieldId(int ordinal) {
        return fields.get(ordinal).id();
    }

    /** Returns the active schema ordinal for a stable native field id, or {@code -1} if retired. */
    public int ordinalForFieldId(long fieldId) {
        Integer ordinal = positionsById().get(Long.valueOf(fieldId));
        return ordinal == null ? -1 : ordinal.intValue();
    }

    public int[] keyOrdinals() {
        int[] ordinals = new int[primaryKeys.size()];
        for (int i = 0; i < ordinals.length; i++) ordinals[i] = ordinalOf(primaryKeys.get(i));
        return ordinals;
    }

    public List<LogicalType> primaryKeyTypes() {
        return fieldTypes(toTableSchema().primaryKey());
    }

    public List<LogicalType> bucketKeyTypes() {
        return fieldTypes(toTableSchema().bucketKey());
    }

    public int[] bucketKeyOrdinals() {
        TableSchema schema = toTableSchema();
        int[] ordinals = new int[schema.bucketKey().size()];
        Map<Long, Integer> positions = positionsById();
        for (int i = 0; i < ordinals.length; i++) {
            Integer position = positions.get(schema.bucketKey().get(i));
            if (position == null) throw new IllegalStateException("Bucket key field is missing.");
            ordinals[i] = position.intValue();
        }
        return ordinals;
    }

    public int valueColumnCount() {
        return toTableSchema().fields().size() - toTableSchema().primaryKey().size();
    }

    public int valueIndexForOrdinal(int ordinal) {
        TableSchema schema = toTableSchema();
        long fieldId = schema.fields().get(ordinal).id();
        if (schema.primaryKey().contains(Long.valueOf(fieldId))) return -1;
        int valueIndex = 0;
        for (DataField field : schema.fields()) {
            if (!schema.primaryKey().contains(Long.valueOf(field.id()))) {
                if (field.id() == fieldId) return valueIndex;
                valueIndex++;
            }
        }
        throw new IllegalStateException("Cobble value field is missing.");
    }

    public int keySlotForOrdinal(int ordinal) {
        long fieldId = toTableSchema().fields().get(ordinal).id();
        return toTableSchema().primaryKey().indexOf(Long.valueOf(fieldId));
    }

    public LogicalType logicalType(int ordinal) {
        return fields.get(ordinal).logicalType();
    }

    public void validateWriteSchema(StructType provided) {
        StructField[] expected = sparkSchema.fields();
        StructField[] actual = provided.fields();
        if (expected.length != actual.length) {
            throw new IllegalArgumentException(
                    "Cobble table expects "
                            + expected.length
                            + " columns but the write contains "
                            + actual.length
                            + ".");
        }
        for (int i = 0; i < expected.length; i++) {
            if (!expected[i].name().equals(actual[i].name())) {
                throw new IllegalArgumentException(
                        "Cobble column order mismatch at position "
                                + i
                                + ": expected '"
                                + expected[i].name()
                                + "' but found '"
                                + actual[i].name()
                                + "'.");
            }
            if (!expected[i].dataType().sameType(actual[i].dataType())) {
                throw new IllegalArgumentException(
                        "Cobble column '"
                                + expected[i].name()
                                + "' has type "
                                + expected[i].dataType().catalogString()
                                + " but the write supplies "
                                + actual[i].dataType().catalogString()
                                + ".");
            }
        }
    }

    public static List<String> parsePrimaryKeyOption(String value) {
        if (value == null || value.trim().isEmpty()) return Collections.emptyList();
        List<String> keys = new ArrayList<String>();
        for (String part : value.split(",")) {
            String key = part.trim();
            if (!key.isEmpty()) keys.add(key);
        }
        return normalizePrimaryKeys(keys);
    }

    private List<LogicalType> fieldTypes(List<Long> ids) {
        List<LogicalType> types = new ArrayList<LogicalType>(ids.size());
        Map<Long, Integer> positions = positionsById();
        for (Long id : ids) {
            Integer position = positions.get(id);
            if (position == null) throw new IllegalStateException("Cobble key field is missing.");
            types.add(toTableSchema().fields().get(position.intValue()).logicalType());
        }
        return types;
    }

    private Map<Long, Integer> positionsById() {
        Map<Long, Integer> cached = positionsById;
        if (cached == null) {
            cached = new LinkedHashMap<Long, Integer>();
            List<DataField> fields = this.fields;
            for (int i = 0; i < fields.size(); i++) cached.put(fields.get(i).id(), i);
            positionsById = cached;
        }
        return cached;
    }

    private static List<String> normalizePrimaryKeys(List<String> primaryKeys) {
        if (primaryKeys == null) return Collections.emptyList();
        List<String> result = new ArrayList<String>();
        Set<String> seen = new LinkedHashSet<String>();
        for (String raw : primaryKeys) {
            if (raw == null || raw.trim().isEmpty()) continue;
            String key = raw.trim();
            if (!seen.add(key)) {
                throw new IllegalArgumentException("Duplicate primary key column '" + key + "'.");
            }
            result.add(key);
        }
        return result;
    }
}
