package io.cobble.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Connector table schema persisted as a sidecar JSON file under the table root.
 *
 * <p>The schema assigns each column a role: key columns are encoded into the Cobble row key, value
 * columns are stored in their own Cobble column indexed by {@code valueIndex}. The sidecar is
 * written once per committed global snapshot as {@code schema/schema-<snapshotId>.json} and reads
 * resolve the newest schema whose snapshot id does not exceed the scanned snapshot.
 */
public final class CobbleTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    static final int FORMAT_VERSION = 1;

    static final String SCHEMA_DIR = "schema";

    private static final Pattern SCHEMA_FILE_PATTERN = Pattern.compile("schema-(\\d+)\\.json");

    /** Column role in the physical layout. */
    public enum ColumnRole {
        KEY,
        VALUE
    }

    /** One persisted column definition. */
    public static final class Column implements Serializable {
        private static final long serialVersionUID = 1L;

        public String name;
        public String typeJson;
        public boolean nullable;
        public String role;
        public Integer valueIndex;

        public Column() {}

        Column(
                String name,
                String typeJson,
                boolean nullable,
                ColumnRole role,
                Integer valueIndex) {
            this.name = name;
            this.typeJson = typeJson;
            this.nullable = nullable;
            this.role = role.name();
            this.valueIndex = valueIndex;
        }

        public ColumnRole columnRole() {
            return ColumnRole.valueOf(role);
        }

        public DataType dataType() {
            return DataType.fromJson(typeJson);
        }
    }

    public int formatVersion = FORMAT_VERSION;
    public int totalBuckets;
    public List<String> primaryKeys = new ArrayList<>();
    public List<Column> fields = new ArrayList<>();

    private transient volatile StructType structType;

    /** Builds a schema for a new table from the Spark schema and primary key names. */
    public static CobbleTableSchema fromStructType(StructType schema, List<String> primaryKeys) {
        if (schema == null || schema.fields().length == 0) {
            throw new IllegalArgumentException(
                    "Cobble table schema must contain at least one column.");
        }
        Set<String> pkSet = new LinkedHashSet<>();
        if (primaryKeys != null) {
            for (String name : primaryKeys) {
                if (name != null && !name.trim().isEmpty()) {
                    pkSet.add(name.trim());
                }
            }
        }
        if (pkSet.isEmpty()) {
            throw new IllegalArgumentException(
                    "Creating a Cobble table requires the '"
                            + CobbleOptions.PRIMARY_KEY
                            + "' option listing the primary key columns.");
        }

        CobbleTableSchema result = new CobbleTableSchema();
        int valueIndex = 0;
        for (StructField field : schema.fields()) {
            CobbleSparkTypes.requireSupported(field.name(), field.dataType());
            if (pkSet.contains(field.name())) {
                if (field.nullable()) {
                    throw new IllegalArgumentException(
                            "Primary key column '" + field.name() + "' must not be nullable.");
                }
                result.fields.add(
                        new Column(
                                field.name(),
                                field.dataType().json(),
                                false,
                                ColumnRole.KEY,
                                null));
            } else {
                result.fields.add(
                        new Column(
                                field.name(),
                                field.dataType().json(),
                                field.nullable(),
                                ColumnRole.VALUE,
                                Integer.valueOf(valueIndex)));
                valueIndex++;
            }
        }
        for (String pk : pkSet) {
            if (!containsColumn(result.fields, pk)) {
                throw new IllegalArgumentException(
                        "Primary key column '" + pk + "' is not present in the schema.");
            }
            result.primaryKeys.add(pk);
        }
        if (valueIndex == 0) {
            throw new IllegalArgumentException(
                    "Cobble table schema must contain at least one non-primary-key column.");
        }
        return result;
    }

    /** Full row schema in physical (ordinal) order. */
    public StructType toStructType() {
        StructType cached = structType;
        if (cached == null) {
            synchronized (this) {
                cached = structType;
                if (cached == null) {
                    List<StructField> sparkFields = new ArrayList<>(fields.size());
                    for (Column column : fields) {
                        sparkFields.add(
                                new StructField(
                                        column.name,
                                        column.dataType(),
                                        column.nullable,
                                        org.apache.spark.sql.types.Metadata.empty()));
                    }
                    cached = new StructType(sparkFields.toArray(new StructField[0]));
                    structType = cached;
                }
            }
        }
        return cached;
    }

    /** Ordinals of the primary key columns, in primary-key order. */
    public int[] keyOrdinals() {
        int[] ordinals = new int[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            ordinals[i] = ordinalOf(primaryKeys.get(i));
        }
        return ordinals;
    }

    /** Ordinal of a column by name, case sensitive. */
    public int ordinalOf(String columnName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name.equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown Cobble column '" + columnName + "'.");
    }

    /** Number of value (non-key) columns; equals the Cobble column family width. */
    public int valueColumnCount() {
        int count = 0;
        for (Column column : fields) {
            if (column.columnRole() == ColumnRole.VALUE) {
                count++;
            }
        }
        return count;
    }

    /**
     * Validates that a write schema is compatible with this stored schema: same columns, same
     * order, same types and same primary keys.
     */
    public void validateWriteSchema(StructType provided) {
        StructType stored = toStructType();
        StructField[] providedFields = provided.fields();
        if (providedFields.length != stored.fields().length) {
            throw new IllegalArgumentException(
                    "Cobble table expects "
                            + stored.fields().length
                            + " columns ["
                            + stored.catalogString()
                            + "] but the write contains "
                            + providedFields.length
                            + " columns ["
                            + provided.catalogString()
                            + "]. Schema evolution is not supported yet.");
        }
        for (int i = 0; i < providedFields.length; i++) {
            StructField expected = stored.fields()[i];
            StructField actual = providedFields[i];
            if (!expected.name().equals(actual.name())) {
                throw new IllegalArgumentException(
                        "Cobble column order mismatch at position "
                                + i
                                + ": expected '"
                                + expected.name()
                                + "' but found '"
                                + actual.name()
                                + "'.");
            }
            if (!expected.dataType().json().equals(actual.dataType().json())) {
                throw new IllegalArgumentException(
                        "Cobble column '"
                                + expected.name()
                                + "' has stored type "
                                + expected.dataType().catalogString()
                                + " but the write supplies "
                                + actual.dataType().catalogString()
                                + ".");
            }
        }
    }

    /** Returns true when the schema sidecar directory exists for the table at {@code pathUri}. */
    public static boolean sidecarExists(String pathUri) {
        return listSchemaSnapshotIds(tableRoot(pathUri)).length > 0;
    }

    /**
     * Loads the stored schema visible for {@code snapshotId}: the newest schema file whose snapshot
     * id is {@code <= snapshotId}, or the oldest schema file when every file is newer.
     */
    public static CobbleTableSchema load(String pathUri, Long snapshotId) throws IOException {
        long[] ids = listSchemaSnapshotIds(tableRoot(pathUri));
        if (ids.length == 0) {
            throw new IOException(
                    "No Cobble schema sidecar found under " + pathUri + ". Write the table first.");
        }
        long selected = ids[ids.length - 1];
        for (long id : ids) {
            if (snapshotId == null || id <= snapshotId.longValue()) {
                selected = id;
                break;
            }
        }
        Path file = schemaFile(tableRoot(pathUri), selected);
        String json = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        CobbleTableSchema schema = GSON.fromJson(json, CobbleTableSchema.class);
        if (schema == null || schema.fields == null || schema.fields.isEmpty()) {
            throw new IOException("Corrupt Cobble schema sidecar: " + file);
        }
        if (schema.formatVersion > FORMAT_VERSION) {
            throw new IOException(
                    "Cobble schema sidecar "
                            + file
                            + " has unsupported format version "
                            + schema.formatVersion
                            + " (supported: "
                            + FORMAT_VERSION
                            + ").");
        }
        return schema;
    }

    /** Atomically writes the schema sidecar for {@code snapshotId} under the table root. */
    public static void store(String pathUri, long snapshotId, CobbleTableSchema schema)
            throws IOException {
        File root = tableRoot(pathUri);
        File dir = new File(root, SCHEMA_DIR);
        Files.createDirectories(dir.toPath());
        Path target = schemaFile(root, snapshotId);
        Path temp =
                dir.toPath()
                        .resolve(
                                "schema-"
                                        + snapshotId
                                        + ".json."
                                        + Long.toHexString(System.nanoTime())
                                        + ".tmp");
        String json = GSON.toJson(schema);
        Files.write(temp, json.getBytes(StandardCharsets.UTF_8));
        try {
            Files.move(
                    temp,
                    target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (java.nio.file.AtomicMoveNotSupportedException e) {
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static File tableRoot(String pathUri) {
        return Paths.get(java.net.URI.create(pathUri)).toFile();
    }

    private static Path schemaFile(File root, long snapshotId) {
        return new File(new File(root, SCHEMA_DIR), "schema-" + snapshotId + ".json").toPath();
    }

    private static long[] listSchemaSnapshotIds(File root) {
        File dir = new File(root, SCHEMA_DIR);
        File[] files = dir.listFiles();
        if (files == null) {
            return new long[0];
        }
        List<Long> ids = new ArrayList<>();
        for (File file : files) {
            Matcher matcher = SCHEMA_FILE_PATTERN.matcher(file.getName());
            if (file.isFile() && matcher.matches()) {
                ids.add(Long.valueOf(Long.parseLong(matcher.group(1))));
            }
        }
        Collections.sort(ids, Comparator.reverseOrder());
        long[] result = new long[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            result[i] = ids.get(i).longValue();
        }
        return result;
    }

    private static boolean containsColumn(List<Column> columns, String name) {
        for (Column column : columns) {
            if (column.name.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /** Human readable summary used in error messages. */
    public String summary() {
        List<String> names = new ArrayList<>();
        for (Column column : fields) {
            names.add(column.name.toLowerCase(Locale.ROOT));
        }
        return "fields="
                + names
                + ", primaryKeys="
                + primaryKeys
                + ", totalBuckets="
                + totalBuckets;
    }

    @Override
    public String toString() {
        return "CobbleTableSchema{" + summary() + "}";
    }

    /** Lists the primary keys split from an option value, validating basic shape. */
    public static List<String> parsePrimaryKeyOption(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> keys = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                keys.add(trimmed);
            }
        }
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> seen = new LinkedHashSet<>(keys);
        if (seen.size() != keys.size()) {
            throw new IllegalArgumentException("Duplicate primary key column in '" + value + "'.");
        }
        return keys;
    }

    /** Convenience for tests: schema field names in ordinal order. */
    public List<String> fieldNames() {
        List<String> names = new ArrayList<>(fields.size());
        for (Column column : fields) {
            names.add(column.name);
        }
        return names;
    }
}
