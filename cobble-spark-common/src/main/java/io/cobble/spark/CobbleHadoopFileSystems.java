package io.cobble.spark;

import io.cobble.CustomFileSystem;
import io.cobble.CustomRandomAccessFile;
import io.cobble.CustomSequentialWriteFile;
import io.cobble.ProcessFileSystemRequest;
import io.cobble.ProcessFileSystems;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Abortable;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/** Native-first Hadoop fallback, with immutable contexts leased for native handle lifetimes. */
public final class CobbleHadoopFileSystems {
    private static final Map<String, ContextLease> CONTEXTS = new HashMap<>();
    private static boolean registered;

    private CobbleHadoopFileSystems() {}

    /** Optional plugin contract: atomically replace an existing file, preserving it on failure. */
    public interface AtomicReplaceFileSystem {
        void renameOverwrite(Path source, Path target) throws IOException;
    }

    public static synchronized Lease acquire(CobbleOptions.CobbleTableConfig config) {
        if (!registered) {
            ProcessFileSystems.registerCustomRegistry(CobbleHadoopFileSystems::tryResolve);
            registered = true;
        }
        String root =
                canonicalRoot(
                        config.isCatalogTable()
                                ? config.catalogReference().warehouse()
                                : config.pathUri());
        for (Map.Entry<String, ContextLease> entry : CONTEXTS.entrySet()) {
            URI existing = URI.create(entry.getKey()), requested = URI.create(root);
            if ((contains(existing, requested) || contains(requested, existing))
                    && !entry.getValue().context.equals(config.hadoopContext())) {
                throw new IllegalStateException(
                        "Overlapping Cobble storage roots require identical Hadoop configuration and identity.");
            }
        }
        ContextLease lease = CONTEXTS.get(root);
        if (lease == null) {
            lease = new ContextLease(config.hadoopContext());
            CONTEXTS.put(root, lease);
        }
        lease.references++;
        return new Lease(root);
    }

    public static final class Lease implements AutoCloseable {
        private final String root;
        private boolean closed;

        private Lease(String root) {
            this.root = root;
        }

        @Override
        public void close() {
            synchronized (CobbleHadoopFileSystems.class) {
                if (closed) return;
                closed = true;
                ContextLease context = CONTEXTS.get(root);
                if (--context.references == 0) CONTEXTS.remove(root);
            }
        }
    }

    private static final class ContextLease {
        private final CobbleHadoopContext context;
        private int references;

        private ContextLease(CobbleHadoopContext context) {
            this.context = context;
        }
    }

    static CustomFileSystem tryResolve(ProcessFileSystemRequest request) {
        String base = request.normalizedBaseDir();
        if (base == null) base = request.baseDir();
        if (base == null) return null;
        URI uri = URI.create(canonicalRoot(base));
        CobbleHadoopContext context = null;
        int best = -1;
        synchronized (CobbleHadoopFileSystems.class) {
            for (Map.Entry<String, ContextLease> entry : CONTEXTS.entrySet()) {
                URI root = URI.create(entry.getKey());
                if (contains(root, uri) && root.getPath().length() > best) {
                    context = entry.getValue().context;
                    best = root.getPath().length();
                }
            }
        }
        if (context == null) return null;
        Configuration configuration = context.configuration();
        try {
            UserGroupInformation user = context.identity();
            FileSystem fileSystem =
                    user.doAs(
                            (PrivilegedExceptionAction<FileSystem>)
                                    () -> FileSystem.newInstance(uri, configuration));
            return new HadoopFileSystem(fileSystem, new Path(uri), user);
        } catch (IOException error) {
            throw new IllegalStateException("Failed to resolve Cobble Hadoop filesystem.", error);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while resolving Cobble Hadoop filesystem.", error);
        }
    }

    private static boolean contains(URI root, URI path) {
        if (!java.util.Objects.equals(root.getScheme(), path.getScheme())
                || !java.util.Objects.equals(root.getAuthority(), path.getAuthority()))
            return false;
        String prefix = root.getPath();
        String target = path.getPath();
        return target != null
                && prefix != null
                && (target.equals(prefix)
                        || target.startsWith(prefix.endsWith("/") ? prefix : prefix + "/"));
    }

    static String canonicalRoot(String value) {
        URI uri = URI.create(value).normalize();
        String path = uri.getPath();
        if (path == null || path.isEmpty()) path = "/";
        while (path.length() > 1 && path.endsWith("/")) path = path.substring(0, path.length() - 1);
        String scheme = uri.getScheme();
        String authority = uri.getAuthority();
        try {
            return new URI(
                            scheme == null ? "file" : scheme.toLowerCase(java.util.Locale.ROOT),
                            authority == null || authority.isEmpty()
                                    ? null
                                    : authority.toLowerCase(java.util.Locale.ROOT),
                            path,
                            null,
                            null)
                    .toString();
        } catch (java.net.URISyntaxException error) {
            throw new IllegalArgumentException("Invalid storage root.");
        }
    }

    private interface Operation<T> {
        T run() throws IOException;
    }

    static final class HadoopFileSystem implements CustomFileSystem {
        private final FileSystem fs;
        private final Path root;
        private final UserGroupInformation user;
        private boolean closed;

        HadoopFileSystem(FileSystem fs, Path root, UserGroupInformation user) {
            this.fs = fs;
            this.root = root;
            this.user = user;
        }

        private <T> T call(Operation<T> operation) {
            try {
                return user.doAs((PrivilegedExceptionAction<T>) operation::run);
            } catch (IOException error) {
                throw new IllegalStateException(
                        "Cobble Hadoop filesystem operation failed.", error);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Cobble Hadoop filesystem operation interrupted.", error);
            }
        }

        private Path path(String value) {
            if (value == null || value.isEmpty() || "/".equals(value)) return root;
            Path path = new Path(value);
            return path.isAbsolute() || path.toUri().getScheme() != null
                    ? path
                    : new Path(root, path);
        }

        @Override
        public void createDir(String value) {
            call(
                    () -> {
                        Path path = path(value);
                        if (!fs.mkdirs(path) && !fs.isDirectory(path))
                            throw new IOException("Hadoop mkdirs returned false.");
                        return null;
                    });
        }

        @Override
        public boolean exists(String value) {
            return call(() -> fs.exists(path(value)));
        }

        @Override
        public Long fileSize(String value) {
            return call(
                    () -> {
                        try {
                            FileStatus status = fs.getFileStatus(path(value));
                            return status.isFile() ? status.getLen() : null;
                        } catch (FileNotFoundException absent) {
                            return null;
                        }
                    });
        }

        @Override
        public Long lastModified(String value) {
            return call(
                    () -> {
                        try {
                            return fs.getFileStatus(path(value)).getModificationTime() / 1000L;
                        } catch (FileNotFoundException absent) {
                            return null;
                        }
                    });
        }

        @Override
        public void delete(String value) {
            call(
                    () -> {
                        Path path = path(value);
                        if (!fs.delete(path, true) && fs.exists(path))
                            throw new IOException("Hadoop delete returned false.");
                        return null;
                    });
        }

        @Override
        public void deleteAsync(String value) {
            delete(value);
        }

        @Override
        public void rename(String from, String to) {
            call(
                    () -> {
                        Path source = path(from), target = path(to);
                        if (isS3A(fs)) {
                            // S3A closes an upload before making the replacement object visible.
                            // Its
                            // ordinary Hadoop rename deletes an existing destination before
                            // copying.
                            byte[] buffer = new byte[64 * 1024];
                            copyObject(fs, source, target, buffer);
                            if (!fs.delete(source, false) && fs.exists(source))
                                throw new IOException(
                                        "Published replacement but failed to remove rename source.");
                        } else if (fs instanceof AtomicReplaceFileSystem) {
                            ((AtomicReplaceFileSystem) fs).renameOverwrite(source, target);
                        } else if (fs instanceof DistributedFileSystem) {
                            ((DistributedFileSystem) fs)
                                    .rename(source, target, Options.Rename.OVERWRITE);
                        } else {
                            if (fs.exists(target))
                                throw new IOException(
                                        "Hadoop plugin does not support safe file replacement; implement AtomicReplaceFileSystem.");
                            if (!fs.rename(source, target))
                                throw new IOException("Hadoop rename returned false.");
                        }
                        return null;
                    });
        }

        @Override
        public String[] list(String value) {
            return call(
                    () -> {
                        FileStatus[] statuses;
                        try {
                            statuses = fs.listStatus(path(value));
                        } catch (FileNotFoundException absent) {
                            return new String[0];
                        }
                        String[] result = new String[statuses.length];
                        for (int i = 0; i < statuses.length; i++)
                            result[i] = statuses[i].getPath().getName();
                        return result;
                    });
        }

        @Override
        public CustomRandomAccessFile openRead(String value) {
            return call(
                    () -> {
                        Path path = path(value);
                        long size = fs.getFileStatus(path).getLen();
                        return new HadoopReadFile(fs.open(path), size, this);
                    });
        }

        @Override
        public CustomSequentialWriteFile openWrite(String value) {
            return call(() -> new HadoopWriteFile(fs.create(path(value), true), this));
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                call(
                        () -> {
                            fs.close();
                            return null;
                        });
            }
        }
    }

    private static boolean isS3A(FileSystem fileSystem) {
        for (Class<?> type = fileSystem.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().equals("org.apache.hadoop.fs.s3a.S3AFileSystem")) return true;
        }
        return false;
    }

    static void copyObject(FileSystem fs, Path source, Path target, byte[] buffer)
            throws IOException {
        if (!fs.hasPathCapability(target, CommonPathCapabilities.ABORTABLE_STREAM)) {
            throw new IOException("Hadoop object replacement requires an abortable upload.");
        }
        try (FSDataInputStream input = fs.open(source)) {
            FSDataOutputStream output = fs.create(target, true);
            try {
                int count;
                while ((count = input.read(buffer)) != -1) output.write(buffer, 0, count);
                output.close();
            } catch (IOException | RuntimeException error) {
                try {
                    Abortable.AbortableResult result = output.abort();
                    if (result.anyCleanupException() != null)
                        error.addSuppressed(result.anyCleanupException());
                } catch (RuntimeException abortError) {
                    error.addSuppressed(abortError);
                }
                // Abort owns cleanup. Closing after an abort failure could publish partial data.
                throw error;
            }
        }
    }

    static final class HadoopReadFile implements CustomRandomAccessFile {
        private final FSDataInputStream input;
        private final long size;
        private final HadoopFileSystem owner;
        private boolean closed;

        HadoopReadFile(FSDataInputStream input, long size, HadoopFileSystem owner) {
            this.input = input;
            this.size = size;
            this.owner = owner;
        }

        @Override
        public synchronized byte[] readAt(long offset, int length) {
            return owner.call(
                    () -> {
                        if (offset < 0L || length < 0) throw new IOException("Invalid read range.");
                        int requested = (int) Math.min(length, Math.max(0L, size - offset));
                        byte[] bytes = new byte[requested];
                        input.readFully(offset, bytes);
                        return bytes;
                    });
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                owner.call(
                        () -> {
                            input.close();
                            return null;
                        });
            }
        }
    }

    static final class HadoopWriteFile implements CustomSequentialWriteFile {
        private final FSDataOutputStream output;
        private final HadoopFileSystem owner;
        private final byte[] scratch = new byte[64 * 1024];
        private long size;
        private boolean closed;

        HadoopWriteFile(FSDataOutputStream output, HadoopFileSystem owner) {
            this.output = output;
            this.owner = owner;
        }

        @Override
        public synchronized int write(byte[] data) {
            return owner.call(
                    () -> {
                        output.write(data);
                        size += data.length;
                        return data.length;
                    });
        }

        @Override
        public boolean supportDirect() {
            return true;
        }

        @Override
        public synchronized int writeDirect(ByteBuffer data, int length) {
            return owner.call(
                    () -> {
                        if (length < 0 || length > data.remaining())
                            throw new IOException("Invalid direct write length.");
                        ByteBuffer source = data.duplicate();
                        int remaining = length;
                        while (remaining > 0) {
                            int count = Math.min(remaining, scratch.length);
                            source.get(scratch, 0, count);
                            output.write(scratch, 0, count);
                            remaining -= count;
                        }
                        size += length;
                        return length;
                    });
        }

        @Override
        public synchronized long size() {
            return size;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                owner.call(
                        () -> {
                            output.close();
                            return null;
                        });
            }
        }
    }
}
