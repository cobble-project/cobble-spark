package io.cobble.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/** Creates one {@link CobblePartitionReader} per input partition on executors. */
public final class CobblePartitionReaderFactory implements PartitionReaderFactory {

    private static final long serialVersionUID = 1L;

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final StructType requiredSchema;

    public CobblePartitionReaderFactory(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            StructType requiredSchema) {
        this.config = config;
        this.schema = schema;
        this.requiredSchema = requiredSchema;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        if (!(partition instanceof CobbleInputPartition)) {
            throw new IllegalArgumentException(
                    "Unsupported input partition type: " + partition.getClass().getName());
        }
        CobbleInputPartition cobblePartition = (CobbleInputPartition) partition;
        return new CobblePartitionReader(cobblePartition.split(), config, schema, requiredSchema);
    }
}
