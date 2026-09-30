package io.cobble.spark;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Serializes Cobble writes and commits per table within this JVM, plus OS file locks for local
 * storage.
 *
 * <p>The driver reads the current snapshot id and materializes {@code id + 1} while holding the
 * lock, so concurrent commit attempts cannot pick the same snapshot id. The JVM-level lock prevents
 * {@link OverlappingFileLockException} between threads of the same process; the file lock guards
 * against other processes on the same local table root. Remote roots use only the JVM lock; callers
 * must ensure one active writer job across processes and engines.
 */
public final class CobbleCommitLock implements AutoCloseable {

    private static final String LOCK_FILE_NAME = ".commit-lock";
    private static final String WRITE_LOCK_FILE_NAME = ".write-lock";

    private static final ConcurrentMap<String, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

    private final ReentrantLock jvmLock;
    private final FileChannel channel;
    private final FileLock fileLock;

    private CobbleCommitLock(ReentrantLock jvmLock, FileChannel channel, FileLock fileLock) {
        this.jvmLock = jvmLock;
        this.channel = channel;
        this.fileLock = fileLock;
    }

    /**
     * Acquires the commit lock for the table rooted at {@code pathUri}, blocking other commits in
     * this JVM and failing fast when another process is mid-commit.
     */
    public static CobbleCommitLock acquire(String pathUri) throws IOException {
        return acquire(pathUri, LOCK_FILE_NAME);
    }

    /** Holds a table-wide write job from planning through task execution and commit. */
    public static CobbleCommitLock acquireWrite(String pathUri) throws IOException {
        return acquire(pathUri, WRITE_LOCK_FILE_NAME);
    }

    private static CobbleCommitLock acquire(String pathUri, String lockFileName)
            throws IOException {
        ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(pathUri, ignored -> new ReentrantLock());
        jvmLock.lock();
        FileChannel channel = null;
        try {
            if (!CobblePaths.isLocal(pathUri)) return new CobbleCommitLock(jvmLock, null, null);
            Path lockFile = Paths.get(URI.create(pathUri)).resolve(lockFileName);
            Files.createDirectories(lockFile.getParent());
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
            return new CobbleCommitLock(jvmLock, channel, fileLock);
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
            if (fileLock != null) fileLock.release();
        } finally {
            try {
                if (channel != null) channel.close();
            } finally {
                jvmLock.unlock();
            }
        }
    }
}
