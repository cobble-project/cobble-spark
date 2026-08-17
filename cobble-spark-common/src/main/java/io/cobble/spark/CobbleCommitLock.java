package io.cobble.spark;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Serializes Cobble snapshot commits per table, both within this JVM and across JVMs.
 *
 * <p>The driver reads the current snapshot id and materializes {@code id + 1} while holding the
 * lock, so concurrent commit attempts cannot pick the same snapshot id. The JVM-level lock prevents
 * {@link OverlappingFileLockException} between threads of the same process; the file lock guards
 * against other processes on the same table root.
 */
public final class CobbleCommitLock implements AutoCloseable {

    private static final String LOCK_FILE_NAME = ".commit-lock";

    private static final ConcurrentMap<String, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private final String pathUri;
    private final ReentrantLock jvmLock;
    private final FileChannel channel;
    private final FileLock fileLock;

    private CobbleCommitLock(
            String pathUri, ReentrantLock jvmLock, FileChannel channel, FileLock fileLock) {
        this.pathUri = pathUri;
        this.jvmLock = jvmLock;
        this.channel = channel;
        this.fileLock = fileLock;
    }

    /**
     * Acquires the commit lock for the table rooted at {@code pathUri}, blocking other commits in
     * this JVM and failing fast when another process is mid-commit.
     */
    public static CobbleCommitLock acquire(String pathUri) throws IOException {
        Path lockFile = Paths.get(URI.create(pathUri)).resolve(LOCK_FILE_NAME);
        ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(pathUri, ignored -> new ReentrantLock());
        jvmLock.lock();
        FileChannel channel = null;
        try {
            channel =
                    FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock fileLock;
            try {
                fileLock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                fileLock = null;
            }
            if (fileLock == null) {
                throw new IOException(
                        "Cobble table " + pathUri + " is being committed by another process.");
            }
            return new CobbleCommitLock(pathUri, jvmLock, channel, fileLock);
        } catch (IOException | RuntimeException e) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // Preserve the original failure.
                }
            }
            jvmLock.unlock();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            fileLock.release();
        } finally {
            try {
                channel.close();
            } finally {
                jvmLock.unlock();
            }
        }
    }
}
