package io.cobble.spark;

import io.cobble.table.TableScanPlan;
import io.cobble.table.TableScanSplit;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/** Plans one {@link InputPartition} per scan split of the snapshot. */
public final class CobbleBatch implements Batch {

    private final CobbleOptions.CobbleTableConfig config;
    private final CobbleTableSchema schema;
    private final StructType requiredSchema;
    private final TableScanPlan scanPlan;

    public CobbleBatch(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            StructType requiredSchema,
            TableScanPlan scanPlan) {
        this.config = config;
        this.schema = schema;
        this.requiredSchema = requiredSchema;
        this.scanPlan = scanPlan;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        if (scanPlan == null) {
            return new InputPartition[0];
        }
        List<TableScanSplit> splits = scanPlan.splits();
        List<InputPartition> partitions = new ArrayList<>(splits.size());
        for (TableScanSplit split : splits) {
            partitions.add(new CobbleInputPartition(split, config, schema, requiredSchema));
        }
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CobblePartitionReaderFactory(config, schema, requiredSchema);
    }
}
