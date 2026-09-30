package io.cobble.spark;

import static org.junit.jupiter.api.Assertions.*;

import io.cobble.CustomFileSystem;
import io.cobble.CustomRandomAccessFile;
import io.cobble.CustomSequentialWriteFile;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ProcessFileSystemRequest;
import io.cobble.ShardSnapshot;
import io.cobble.spark.write.CobbleShardResult;
import io.cobble.spark.write.CobbleTableCommitter;
import io.cobble.table.DataField;
import io.cobble.table.LogicalTypes;
import io.cobble.table.Table;
import io.cobble.table.TableSchema;
import io.cobble.table.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class CobbleHadoopFileSystemsTest {
    @TempDir Path root;

    @Test
    void realNativeFallbackWritesAndPublishesRepeatedSnapshotsWithoutPersistingHadoopSecrets()
            throws Exception {
        CobbleOptions.CobbleTableConfig config = config(uri(root), "native-test");
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1L, "id", LogicalTypes.int32()),
                                new DataField(2L, "value", LogicalTypes.string())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        ShardSnapshot first, second;
        int opened = MockFileSystem.opened.get(), closed = MockFileSystem.closed.get();
        try (CobbleHadoopFileSystems.Lease ignored = CobbleHadoopFileSystems.acquire(config)) {
            try (Table writer =
                    Table.writerBuilder(CobblePaths.createPathWriterRuntimeConfig(config, 1, 4096))
                            .tableName("data")
                            .bucket(0)
                            .create(schema)) {
                writer.put(Arrays.asList(Value.int32(1), Value.string("first")));
                first = writer.snapshot();
            }
            GlobalSnapshot initial =
                    CobbleTableCommitter.commit(
                            config,
                            Collections.singletonList(new CobbleShardResult(1, 0, first)),
                            null);
            try (Table writer =
                    Table.writerBuilder(CobblePaths.createPathWriterRuntimeConfig(config, 1, 4096))
                            .tableName("data")
                            .bucket(0)
                            .resumeFromSnapshot(first.snapshotId)) {
                writer.put(Arrays.asList(Value.int32(1), Value.string("second")));
                second = writer.snapshot();
            }
            GlobalSnapshot updated =
                    CobbleTableCommitter.commit(
                            config,
                            Collections.singletonList(new CobbleShardResult(1, 0, second)),
                            initial);
            try (DbCoordinator coordinator =
                    DbCoordinator.open(CobblePaths.createCoordinatorConfig(config, 1))) {
                assertEquals(updated.id, coordinator.loadCurrentGlobalSnapshot().id);
                assertNotNull(coordinator.getGlobalSnapshot(initial.id));
            }
        }
        assertTrue(MockFileSystem.opened.get() > opened);
        assertEquals(MockFileSystem.opened.get() - opened, MockFileSystem.closed.get() - closed);
        assertNull(CobbleHadoopFileSystems.tryResolve(request(uri(root))));
        try (Stream<Path> files = Files.walk(root)) {
            for (Path path : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
                String content = new String(Files.readAllBytes(path), StandardCharsets.ISO_8859_1);
                assertFalse(content.contains("HADOOP-PRIVATE-SECRET"));
                assertFalse(content.contains("mock.private.secret"));
            }
        }
    }

    @Test
    void contextsAreSerializedImmutableAndScopedByCanonicalUriBoundaries() throws Exception {
        String uri = uri(root.resolve("table"));
        CobbleOptions.CobbleTableConfig first = config(uri, "one");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(first);
        }
        CobbleOptions.CobbleTableConfig restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (CobbleOptions.CobbleTableConfig) input.readObject();
        }
        try (CobbleHadoopFileSystems.Lease ignored = CobbleHadoopFileSystems.acquire(restored)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> CobbleHadoopFileSystems.acquire(config(uri + "/", "two")));
            assertThrows(
                    IllegalStateException.class,
                    () -> CobbleHadoopFileSystems.acquire(config(uri + "/bucket-0", "two")));
            assertNull(CobbleHadoopFileSystems.tryResolve(request(uri + "2")));
            try (CustomFileSystem fs =
                    CobbleHadoopFileSystems.tryResolve(request(uri + "/bucket-0"))) {
                assertNotNull(fs);
                fs.createDir("");
            }
        }
        assertEquals(
                CobbleHadoopFileSystems.canonicalRoot("file:/tmp/a/"),
                CobbleHadoopFileSystems.canonicalRoot("file:///tmp/a"));
        assertNull(CobbleHadoopFileSystems.tryResolve(request(uri)));
    }

    @Test
    void bridgePreservesNativeBufferPositionsAndNeverClosesSharedCachedFileSystem()
            throws Exception {
        CobbleOptions.CobbleTableConfig config = config(uri(root), "streams");
        Configuration hadoop = configuration("streams");
        MockFileSystem cached = (MockFileSystem) FileSystem.get(URI.create(uri(root)), hadoop);
        try (CobbleHadoopFileSystems.Lease ignored = CobbleHadoopFileSystems.acquire(config);
                CustomFileSystem fs =
                        CobbleHadoopFileSystems.tryResolve(
                                new ProcessFileSystemRequest(
                                        uri(root),
                                        uri(root),
                                        "native-key",
                                        "native-secret",
                                        Collections.singletonMap(
                                                "mock.private.secret", "native-provider-setting"),
                                        "unknown scheme"))) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(4).put(new byte[] {1, 2, 3, 4});
            buffer.flip();
            try (CustomSequentialWriteFile output = fs.openWrite("data")) {
                output.writeDirect(buffer, 4);
                output.writeDirect(buffer, 4);
                assertEquals(0, buffer.position());
                assertEquals(4, buffer.limit());
                assertEquals(8L, output.size());
            }
            try (CustomRandomAccessFile input = fs.openRead("data")) {
                byte[] first = input.readAt(0, 4), second = input.readAt(4, 4);
                assertNotSame(first, second);
                assertArrayEquals(first, second);
                assertFalse(input.supportDirect());
            }
            assertThrows(IllegalStateException.class, () -> fs.openRead("missing"));
            assertNull(fs.fileSize("missing"));
        }
        assertFalse(cached.wasClosed);
        cached.close();
    }

    @Test
    void failedReplacementLeavesExistingTargetAndReleasesContext() throws Exception {
        CobbleOptions.CobbleTableConfig config = config(uri(root), "failure");
        try (CobbleHadoopFileSystems.Lease ignored = CobbleHadoopFileSystems.acquire(config);
                CustomFileSystem fs = CobbleHadoopFileSystems.tryResolve(request(uri(root)))) {
            try (CustomSequentialWriteFile output = fs.openWrite("CURRENT")) {
                output.write(new byte[] {1});
            }
            try (CustomSequentialWriteFile output = fs.openWrite("new")) {
                output.write(new byte[] {2});
            }
            MockFileSystem.failRename = true;
            try {
                assertThrows(IllegalStateException.class, () -> fs.rename("new", "CURRENT"));
            } finally {
                MockFileSystem.failRename = false;
            }
            try (CustomRandomAccessFile input = fs.openRead("CURRENT")) {
                assertArrayEquals(new byte[] {1}, input.readAt(0, 1));
            }
        }
        assertNull(CobbleHadoopFileSystems.tryResolve(request(uri(root))));
    }

    @Test
    void abortableObjectReplacementPreservesOldObjectOnReadAndWriteFailure() throws Exception {
        for (boolean readFailure : new boolean[] {true, false}) {
            UploadFileSystem fs = new UploadFileSystem(readFailure, !readFailure, true);
            assertThrows(
                    IOException.class,
                    () ->
                            CobbleHadoopFileSystems.copyObject(
                                    fs,
                                    new org.apache.hadoop.fs.Path("/source"),
                                    new org.apache.hadoop.fs.Path("/CURRENT"),
                                    new byte[1]));
            assertArrayEquals(new byte[] {9}, fs.visible);
            assertTrue(fs.aborted);
            assertFalse(fs.published);
            assertTrue(fs.inputClosed);
        }
        UploadFileSystem fs = new UploadFileSystem(false, false, true);
        CobbleHadoopFileSystems.copyObject(
                fs,
                new org.apache.hadoop.fs.Path("/source"),
                new org.apache.hadoop.fs.Path("/CURRENT"),
                new byte[1]);
        assertArrayEquals(new byte[] {1, 2, 3}, fs.visible);
        assertTrue(fs.published);
        UploadFileSystem unsupported = new UploadFileSystem(false, false, false);
        assertThrows(
                IOException.class,
                () ->
                        CobbleHadoopFileSystems.copyObject(
                                unsupported,
                                new org.apache.hadoop.fs.Path("/source"),
                                new org.apache.hadoop.fs.Path("/CURRENT"),
                                new byte[1]));
        assertEquals(0, unsupported.created);
        assertArrayEquals(new byte[] {9}, unsupported.visible);
    }

    private CobbleOptions.CobbleTableConfig config(String uri, String marker) {
        Map<String, String> options = new HashMap<>();
        options.put("path", uri);
        return CobbleOptions.parse(options)
                .withHadoopContext(CobbleHadoopContext.capture(configuration(marker)));
    }

    private static Configuration configuration(String marker) {
        Configuration config = new Configuration();
        config.set("fs.mock.impl", MockFileSystem.class.getName());
        config.set("mock.marker", marker);
        config.set("mock.private.secret", "HADOOP-PRIVATE-SECRET");
        return config;
    }

    private static String uri(Path path) {
        return "mock://" + path.toUri().getPath();
    }

    private static ProcessFileSystemRequest request(String uri) {
        return new ProcessFileSystemRequest(
                uri, uri, null, null, Collections.emptyMap(), "unknown scheme");
    }

    public static class MockFileSystem extends RawLocalFileSystem
            implements CobbleHadoopFileSystems.AtomicReplaceFileSystem {
        static final AtomicInteger opened = new AtomicInteger(), closed = new AtomicInteger();
        static volatile boolean failRename;
        boolean wasClosed;
        private URI uri;

        @Override
        public void initialize(URI uri, Configuration config) throws IOException {
            super.initialize(uri, config);
            this.uri = URI.create("mock:/");
            if (config.get("mock.marker") == null)
                throw new IOException("Missing captured configuration.");
            assertEquals("HADOOP-PRIVATE-SECRET", config.get("mock.private.secret"));
            assertNotNull(UserGroupInformation.getCurrentUser().getUserName());
            opened.incrementAndGet();
        }

        @Override
        public URI getUri() {
            return uri == null ? URI.create("mock:/") : uri;
        }

        @Override
        public File pathToFile(org.apache.hadoop.fs.Path path) {
            return new File(path.toUri().getPath());
        }

        @Override
        public void renameOverwrite(
                org.apache.hadoop.fs.Path source, org.apache.hadoop.fs.Path target)
                throws IOException {
            if (failRename) throw new IOException("injected publication failure");
            Files.move(
                    pathToFile(source).toPath(),
                    pathToFile(target).toPath(),
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }

        @Override
        public void close() throws IOException {
            if (!wasClosed) {
                wasClosed = true;
                closed.incrementAndGet();
            }
            super.close();
        }
    }

    private static final class UploadFileSystem extends RawLocalFileSystem {
        final boolean readFailure, writeFailure, capable;
        byte[] visible = new byte[] {9};
        boolean aborted, published, inputClosed;
        int created;

        UploadFileSystem(boolean readFailure, boolean writeFailure, boolean capable) {
            setConf(new Configuration());
            this.readFailure = readFailure;
            this.writeFailure = writeFailure;
            this.capable = capable;
        }

        @Override
        public boolean hasPathCapability(org.apache.hadoop.fs.Path path, String capability) {
            return capable && CommonPathCapabilities.ABORTABLE_STREAM.equals(capability);
        }

        @Override
        public FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize) {
            return new FSDataInputStream(
                    new FSInputStream() {
                        int offset;

                        @Override
                        public void seek(long value) {
                            offset = (int) value;
                        }

                        @Override
                        public long getPos() {
                            return offset;
                        }

                        @Override
                        public boolean seekToNewSource(long offset) {
                            return false;
                        }

                        @Override
                        public int read() throws IOException {
                            if (readFailure && offset == 1)
                                throw new IOException("injected read failure");
                            return offset == 3 ? -1 : ++offset;
                        }

                        @Override
                        public void close() {
                            inputClosed = true;
                        }
                    });
        }

        @Override
        public FSDataOutputStream create(
                org.apache.hadoop.fs.Path path,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                org.apache.hadoop.util.Progressable progress) {
            created++;
            return new FSDataOutputStream(new Upload(), null);
        }

        final class Upload extends OutputStream implements Abortable {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            @Override
            public void write(int value) throws IOException {
                if (writeFailure && bytes.size() == 1)
                    throw new IOException("injected write failure");
                bytes.write(value);
            }

            @Override
            public void close() {
                if (!aborted) {
                    visible = bytes.toByteArray();
                    published = true;
                }
            }

            @Override
            public AbortableResult abort() {
                aborted = true;
                return new AbortableResult() {
                    @Override
                    public boolean alreadyClosed() {
                        return false;
                    }

                    @Override
                    public IOException anyCleanupException() {
                        return null;
                    }
                };
            }
        }
    }
}
