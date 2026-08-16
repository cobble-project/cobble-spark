package io.cobble.spark;

import io.cobble.GlobalSnapshot;
import io.cobble.ScanPlan;
import io.cobble.ScanSplit;

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
    private final GlobalSnapshot snapshot;

    public CobbleBatch(
            CobbleOptions.CobbleTableConfig config,
            CobbleTableSchema schema,
            StructType requiredSchema,
            GlobalSnapshot snapshot) {
        this.config = config;
        this.schema = schema;
        this.requiredSchema = requiredSchema;
        this.snapshot = snapshot;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<ScanSplit> splits = ScanPlan.fromGlobalSnapshot(snapshot).splits();
        List<InputPartition> partitions = new ArrayList<>(splits.size());
        for (ScanSplit split : splits) {
            if (split == null || split.shard == null || split.shard.ranges == null) {
                continue;
            }
            partitions.add(new CobbleInputPartition(split, config, schema, requiredSchema));
        }
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new CobblePartitionReaderFactory(config, schema, requiredSchema);
    }
}
