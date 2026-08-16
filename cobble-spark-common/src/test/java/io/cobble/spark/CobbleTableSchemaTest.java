package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

/** Tests for schema construction, validation and sidecar persistence. */
public class CobbleTableSchemaTest {

    private StructType sampleSchema() {
        return DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField(
                                "amount",
                                new org.apache.spark.sql.types.DecimalType(10, 2),
                                true)));
    }

    @Test
    public void buildsSchemaWithKeyAndValueRoles() {
        CobbleTableSchema schema =
                CobbleTableSchema.fromStructType(sampleSchema(), Collections.singletonList("id"));
        assertEquals(Arrays.asList("id", "name", "amount"), schema.fieldNames());
        assertEquals(Collections.singletonList("id"), schema.primaryKeys);
        assertEquals(2, schema.valueColumnCount());
        assertEquals(0, schema.ordinalOf("id"));
        assertEquals(1, schema.ordinalOf("name"));
        assertEquals(2, schema.ordinalOf("amount"));
        CobbleTableSchema.Column id = schema.fields.get(0);
        assertEquals(CobbleTableSchema.ColumnRole.KEY, id.columnRole());
        assertEquals(CobbleTableSchema.ColumnRole.VALUE, schema.fields.get(1).columnRole());
        assertEquals(0, schema.fields.get(1).valueIndex.intValue());
        assertEquals(1, schema.fields.get(2).valueIndex.intValue());
        StructType structType = schema.toStructType();
        assertEquals(3, structType.fields().length);
        assertEquals("id", structType.fields()[0].name());
    }

    @Test
    public void rejectsMissingPrimaryKey() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleTableSchema.fromStructType(sampleSchema(), Collections.emptyList()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        CobbleTableSchema.fromStructType(
                                sampleSchema(), Collections.singletonList("missing")));
    }

    @Test
    public void rejectsNullablePrimaryKey() {
        StructType schema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                                DataTypes.createStructField("v", DataTypes.StringType, true)));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleTableSchema.fromStructType(schema, Collections.singletonList("id")));
    }

    @Test
    public void rejectsUnsupportedTypes() {
        StructType schema =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField(
                                        "arr",
                                        DataTypes.createArrayType(DataTypes.IntegerType),
                                        true)));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleTableSchema.fromStructType(schema, Collections.singletonList("id")));
    }

    @Test
    public void rejectsAllKeyColumns() {
        StructType schema =
                DataTypes.createStructType(
                        Collections.singletonList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false)));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleTableSchema.fromStructType(schema, Collections.singletonList("id")));
    }

    @Test
    public void sidecarRoundTripAndResolve(@TempDir Path tempDir) throws Exception {
        String pathUri = tempDir.toUri().toString();
        CobbleTableSchema schema =
                CobbleTableSchema.fromStructType(sampleSchema(), Collections.singletonList("id"));
        assertFalse(CobbleTableSchema.sidecarExists(pathUri));

        schema.totalBuckets = 8;
        CobbleTableSchema.store(pathUri, 1L, schema);
        schema.totalBuckets = 9;
        CobbleTableSchema.store(pathUri, 3L, schema);
        schema.totalBuckets = 10;
        CobbleTableSchema.store(pathUri, 5L, schema);
        assertTrue(CobbleTableSchema.sidecarExists(pathUri));

        assertEquals(10, CobbleTableSchema.load(pathUri, null).totalBuckets);
        assertEquals(9, CobbleTableSchema.load(pathUri, 4L).totalBuckets);
        CobbleTableSchema loaded = CobbleTableSchema.load(pathUri, 2L);
        assertEquals(8, loaded.totalBuckets);
        assertEquals(Arrays.asList("id", "name", "amount"), loaded.fieldNames());
        assertEquals(Collections.singletonList("id"), loaded.primaryKeys);
        assertEquals(sampleSchema().json(), loaded.toStructType().json());
    }

    @Test
    public void loadFailsWithoutSidecar(@TempDir Path tempDir) throws Exception {
        assertThrows(
                java.io.IOException.class,
                () -> CobbleTableSchema.load(tempDir.toUri().toString(), null));
    }

    @Test
    public void validateWriteSchemaRejectsMismatches() {
        CobbleTableSchema schema =
                CobbleTableSchema.fromStructType(sampleSchema(), Collections.singletonList("id"));
        schema.validateWriteSchema(sampleSchema());

        StructType wrongOrder =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("name", DataTypes.StringType, true),
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField(
                                        "amount",
                                        new org.apache.spark.sql.types.DecimalType(10, 2),
                                        true)));
        assertThrows(IllegalArgumentException.class, () -> schema.validateWriteSchema(wrongOrder));

        StructType wrongType =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField("name", DataTypes.StringType, true),
                                DataTypes.createStructField("amount", DataTypes.DoubleType, true)));
        assertThrows(IllegalArgumentException.class, () -> schema.validateWriteSchema(wrongType));

        StructType missing =
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                                DataTypes.createStructField("name", DataTypes.StringType, true)));
        assertThrows(IllegalArgumentException.class, () -> schema.validateWriteSchema(missing));
    }

    @Test
    public void parsesPrimaryKeyOption() {
        assertEquals(Arrays.asList("a", "b"), CobbleTableSchema.parsePrimaryKeyOption("a, b"));
        assertEquals(Collections.emptyList(), CobbleTableSchema.parsePrimaryKeyOption(null));
        assertEquals(Collections.emptyList(), CobbleTableSchema.parsePrimaryKeyOption(" "));
        assertThrows(
                IllegalArgumentException.class,
                () -> CobbleTableSchema.parsePrimaryKeyOption("a,a"));
    }
}
