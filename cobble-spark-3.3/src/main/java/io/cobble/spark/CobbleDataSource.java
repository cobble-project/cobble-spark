package io.cobble.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Entry point of the Cobble Spark connector (DataSource V2).
 *
 * <p>Tables are path based: {@code spark.read.format("cobble").load(path)} and {@code
 * df.write.format("cobble").option("path", path)...save()}. The table schema is inferred from the
 * schema sidecar written by the last commit.
 */
public final class CobbleDataSource implements TableProvider, DataSourceRegister {

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
        return new CobbleTable(CobbleOptions.parse(options), schema);
    }

    @Override
    public boolean supportsExternalMetadata() {
        // Writing a not yet existing path table supplies the DataFrame schema here; the sidecar
        // only exists after the first commit.
        return true;
    }
}
