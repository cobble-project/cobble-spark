package io.cobble.spark.write;

import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.spark.CobbleBucketMath;
import io.cobble.spark.CobbleLoader;
import io.cobble.spark.CobbleOptions;
import io.cobble.spark.CobblePaths;
import io.cobble.spark.CobbleSparkRowConverter;
import io.cobble.spark.CobbleTableRuntime;
import io.cobble.table.Table;
import io.cobble.table.Value;

import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/** Writes each task's contiguous assignment as independent one-bucket databases. */
public final class CobbleShardWriteTask {
    private static final int INITIAL_KEY_BUFFER_BYTES = 4 * 1024;
    private static final int INITIAL_ROW_BUFFER_BYTES = 64 * 1024;
    private static final int MAX_BUFFER_CAPACITY = Integer.MAX_VALUE - 8;

    private CobbleShardWriteTask() {}

    public static Iterator<CobbleShardResult> writeBuckets(
            int taskIndex, Iterator<Tuple2<Integer, Row>> rows, CobbleWriteContext context)
            throws IOException {
        CobbleLoader.ensureCobbleLoaded();
        int start =
                CobbleBucketMath.writerRangeStart(
                        taskIndex, context.totalBuckets(), context.writerCount());
        int end =
                CobbleBucketMath.writerRangeEnd(
                        taskIndex, context.totalBuckets(), context.writerCount());
        int count = end - start + 1;
        int perBucketBuffer = CobblePaths.perBucketWriteBuffer(context.config(), count);
        Map<Integer, ShardSnapshot> base =
                baseByBucket(context.baseSnapshot(), context.totalBuckets());
        CobbleSparkRowConverter converter = new CobbleSparkRowConverter(context.schema());
        DirectWriteBuffers buffers = new DirectWriteBuffers();
        if (context.config().isCatalogTable()) {
            return writeOpenedBuckets(
                    rows,
                    context,
                    start,
                    end,
                    bucket ->
                            openCatalogBucket(
                                    context,
                                    bucket,
                                    perBucketBuffer,
                                    base.get(Integer.valueOf(bucket))),
                    converter,
                    buffers);
        }
        return writeOpenedBuckets(
                rows,
                context,
                start,
                end,
                bucket ->
                        openPathBucket(
                                context,
                                bucket,
                                perBucketBuffer,
                                base.get(Integer.valueOf(bucket))),
                converter,
                buffers);
    }

    /** Runs the shared row routing/snapshot lifecycle after a mode-specific bucket opener. */
    private static Iterator<CobbleShardResult> writeOpenedBuckets(
            Iterator<Tuple2<Integer, Row>> rows,
            CobbleWriteContext context,
            int start,
            int end,
            BucketOpener opener,
            CobbleSparkRowConverter converter,
            DirectWriteBuffers buffers)
            throws IOException {
        int count = end - start + 1;
        Table[] writers = new Table[count];
        Throwable primaryFailure = null;
        try {
            for (int bucket = start; bucket <= end; bucket++) {
                writers[bucket - start] = opener.open(bucket);
            }
            while (rows.hasNext()) {
                Tuple2<Integer, Row> pair = rows.next();
                int bucket = pair._1().intValue();
                int writerIndex = bucket - start;
                if (writerIndex < 0 || writerIndex >= writers.length) {
                    throw new IOException("Spark task received an unowned Cobble bucket " + bucket);
                }
                Table writer = writers[writerIndex];
                put(writer, converter.toValues(pair._2()), buffers);
            }
            List<CobbleShardResult> results = new ArrayList<CobbleShardResult>(count);
            for (int bucket = start; bucket <= end; bucket++) {
                Table writer = writers[bucket - start];
                results.add(
                        new CobbleShardResult(context.totalBuckets(), bucket, writer.snapshot()));
            }
            return results.iterator();
        } catch (IOException | RuntimeException | Error error) {
            primaryFailure = error;
            throw error;
        } finally {
            closeWriters(writers, primaryFailure);
        }
    }

    private interface BucketOpener {
        Table open(int bucket);
    }

    private static Table openPathBucket(
            CobbleWriteContext context, int bucket, int perBucketBuffer, ShardSnapshot source) {
        CobbleOptions.CobbleTableConfig options = context.config();
        io.cobble.Config runtime =
                CobblePaths.createPathWriterRuntimeConfig(
                        options, context.totalBuckets(), perBucketBuffer);
        io.cobble.table.TableWriterBuilder builder =
                Table.writerBuilder(runtime)
                        .tableName(CobbleTableRuntime.TABLE_NAME)
                        .bucket(bucket);
        Table table =
                context.overwrite() || context.baseSnapshot() == null
                        ? builder.create(context.schema().toTableSchema())
                        : builder.resumeFromSnapshot(source.snapshotId);
        if (!table.schema().equals(context.schema().toTableSchema())) {
            table.close();
            throw new IllegalStateException(
                    "Cobble base shard schema does not match the write schema.");
        }
        return table;
    }

    private static Table openCatalogBucket(
            CobbleWriteContext context, int bucket, int perBucketBuffer, ShardSnapshot source) {
        io.cobble.Config runtime =
                CobblePaths.createWriterRuntimeConfig(context.totalBuckets(), perBucketBuffer);
        runtime.dataFileType = context.config().dataFileType();
        io.cobble.Config.VolumeDescriptor primary = new io.cobble.Config.VolumeDescriptor();
        primary.baseDir = context.config().catalogReference().warehouse();
        primary.kinds =
                Collections.singletonList(
                        io.cobble.Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        runtime.addVolume(primary);
        io.cobble.table.TableWriterBuilder builder =
                context.catalogWritePlan().writerBuilder(runtime).bucket(bucket);
        return context.overwrite() || context.baseSnapshot() == null
                ? builder.open()
                : builder.resumeFromSnapshot(source.snapshotId);
    }

    /** Preserves a write/snapshot failure and adds close failures only as suppressed context. */
    private static void closeWriters(Table[] writers, Throwable primaryFailure) throws IOException {
        IOException failure = null;
        for (Table writer : writers) {
            if (writer == null) continue;
            try {
                writer.close();
            } catch (RuntimeException error) {
                if (failure == null)
                    failure = new IOException("Failed to close Cobble bucket", error);
                else failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            if (primaryFailure != null) primaryFailure.addSuppressed(failure);
            else throw failure;
        }
    }

    /** Rejects legacy multi-bucket snapshots instead of reassembling or shrinking them. */
    static Map<Integer, ShardSnapshot> baseByBucket(GlobalSnapshot snapshot, int totalBuckets)
            throws IOException {
        Map<Integer, ShardSnapshot> result = new HashMap<Integer, ShardSnapshot>();
        if (snapshot == null) return result;
        if (snapshot.totalBuckets != totalBuckets || snapshot.shardSnapshots == null) {
            throw new IOException("Cobble base snapshot has an incompatible bucket layout.");
        }
        for (ShardSnapshot shard : snapshot.shardSnapshots) {
            if (shard == null || shard.ranges == null || shard.ranges.size() != 1) {
                throw new IOException("Cobble base snapshot must contain one shard per bucket.");
            }
            ShardSnapshot.Range range = shard.ranges.get(0);
            if (range == null
                    || range.start != range.end
                    || range.start < 0
                    || range.start >= totalBuckets) {
                throw new IOException("Cobble base snapshot contains a legacy multi-bucket shard.");
            }
            if (!("bucket-" + range.start).equals(shard.dbId)) {
                throw new IOException(
                        "Cobble base snapshot shard has an unexpected database identity.");
            }
            if (result.put(Integer.valueOf(range.start), shard) != null) {
                throw new IOException("Cobble base snapshot contains overlapping bucket shards.");
            }
        }
        if (result.size() != totalBuckets) {
            throw new IOException("Cobble base snapshot does not cover every bucket exactly once.");
        }
        return result;
    }

    private static void put(Table table, List<Value> values, DirectWriteBuffers buffers) {
        for (; ; ) {
            try {
                table.putDirect(values, buffers.keyBuffer(), buffers.rowBuffer());
                return;
            } catch (BufferOverflowException overflow) {
                if (buffers.keyBuffer().position() == 0) buffers.growKey(overflow);
                else buffers.growRow(overflow);
            }
        }
    }

    private static final class DirectWriteBuffers {
        private ByteBuffer keyBuffer;
        private ByteBuffer rowBuffer;

        private ByteBuffer keyBuffer() {
            if (keyBuffer == null) keyBuffer = ByteBuffer.allocateDirect(INITIAL_KEY_BUFFER_BYTES);
            return keyBuffer;
        }

        private ByteBuffer rowBuffer() {
            if (rowBuffer == null) rowBuffer = ByteBuffer.allocateDirect(INITIAL_ROW_BUFFER_BYTES);
            return rowBuffer;
        }

        private void growKey(BufferOverflowException error) {
            keyBuffer = grow(keyBuffer(), "primary key", error);
        }

        private void growRow(BufferOverflowException error) {
            rowBuffer = grow(rowBuffer(), "row values", error);
        }

        private static ByteBuffer grow(
                ByteBuffer current, String contents, BufferOverflowException error) {
            int capacity = current.capacity();
            if (capacity >= MAX_BUFFER_CAPACITY) {
                throw new IllegalArgumentException(
                        "Cobble " + contents + " cannot fit in a Java direct ByteBuffer.", error);
            }
            int next =
                    (int)
                            Math.min(
                                    MAX_BUFFER_CAPACITY,
                                    Math.max((long) capacity + 1L, (long) capacity * 2L));
            return ByteBuffer.allocateDirect(next);
        }
    }
}
