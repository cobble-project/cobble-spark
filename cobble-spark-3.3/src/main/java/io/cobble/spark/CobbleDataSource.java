package io.cobble.spark;

import io.cobble.spark.write.CobbleInsertableRelation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Entry point of the Cobble Spark connector.
 *
 * <p>Path-based tables: {@code spark.read.format("cobble").load(path)} reads through the Data
 * Source V2 API, and {@code df.write.format("cobble").option("path", path)...save()} writes through
 * the V1 {@link CreatableRelationProvider} path. Tables can also be managed by {@link
 * SparkCatalog}, which carries the table-level properties (including the path) and reuses this
 * class' write path via {@link CobbleInsertableRelation}.
 */
public final class CobbleDataSource
        implements TableProvider, DataSourceRegister, CreatableRelationProvider {

    @Override
    public String shortName() {
        return "cobble";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        CobbleOptions.CobbleTableConfig config = CobbleOptions.parse(options.asCaseSensitiveMap());
        if (!CobbleTableSchema.sidecarExists(config.pathUri())) {
            return null;
        }
        try {
            return CobbleTableSchema.load(config.pathUri(), null).toStructType();
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException(
                    "Failed to read the Cobble schema sidecar under " + config.pathUri(), e);
        }
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> properties) {
        Map<String, String> options = new HashMap<>();
        if (properties != null) {
            options.putAll(properties);
        }
        CobbleOptions.CobbleTableConfig config = CobbleOptions.parse(options);
        return new CobbleTable(config, schema, options);
    }

    @Override
    public boolean supportsExternalMetadata() {
        // Writing a not yet existing path table supplies the DataFrame schema here; the sidecar
        // only exists after the first commit.
        return true;
    }

    @Override
    public BaseRelation createRelation(
            SQLContext sqlContext,
            SaveMode mode,
            scala.collection.immutable.Map<String, String> parameters,
            Dataset<Row> data) {
        Map<String, String> options =
                new HashMap<>(scala.collection.JavaConverters.mapAsJavaMap(parameters));
        CobbleOptions.CobbleTableConfig config = CobbleOptions.parse(options);

        if (mode.equals(SaveMode.ErrorIfExists)
                && CobbleTableSchema.sidecarExists(config.pathUri())) {
            throw new IllegalArgumentException(
                    "Cobble table "
                            + config.pathUri()
                            + " already exists; use append or overwrite.");
        }
        if (mode.equals(SaveMode.Ignore) && CobbleTableSchema.sidecarExists(config.pathUri())) {
            return emptyRelation(sqlContext, data);
        }

        boolean overwrite = mode.equals(SaveMode.Overwrite);
        new CobbleInsertableRelation(config, data.schema(), overwrite, options)
                .insert(data, overwrite);
        return emptyRelation(sqlContext, data);
    }

    private static BaseRelation emptyRelation(SQLContext sqlContext, Dataset<Row> data) {
        return new BaseRelation() {
            @Override
            public SQLContext sqlContext() {
                return sqlContext;
            }

            @Override
            public StructType schema() {
                return data.schema();
            }
        };
    }
}
