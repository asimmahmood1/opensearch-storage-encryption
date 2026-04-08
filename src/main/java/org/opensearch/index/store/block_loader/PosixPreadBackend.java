/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.iouring.PosixFD;
import org.opensearch.index.store.iouring.PosixFDCache;
import org.opensearch.index.store.iouring.RefCountedFD;

/**
 * I/O backend that uses native {@code pread} syscall via Panama FFI with O_DIRECT.
 *
 * <p>{@code pread} bypasses JDK {@link java.nio.channels.FileChannel} alignment
 * enforcement — the kernel only requires page-aligned buffers and offsets for
 * O_DIRECT. This avoids the read amplification that {@code FileChannel} suffers
 * on NFS/EFS where {@code f_bsize} (1MB) >> page size (4KB).
 *
 * <p>When constructed with a {@link PosixFDCache}, file descriptors are pooled
 * and reused across reads, amortizing the open/close syscall overhead.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO via PosixFD")
public class PosixPreadBackend implements IOBackendStrategy {

    private final PosixFDCache fdCache;

    /** Construct with FD caching disabled (open/close every read). */
    public PosixPreadBackend() {
        this(null);
    }

    /** Construct with an optional FD cache for pooling file descriptors. */
    public PosixPreadBackend(PosixFDCache fdCache) {
        this.fdCache = fdCache;
    }

    @Override
    public MemorySegment read(Path filePath, long offset, long length,
                              Arena arena, int blockSize) throws IOException {
        int mmuPageSize = PosixFD.pageSize();
        if (fdCache != null) {
            return readCached(filePath, offset, length, arena, blockSize, mmuPageSize);
        }
        return readUncached(filePath, offset, length, arena, blockSize, mmuPageSize);
    }

    private MemorySegment readCached(Path filePath, long offset, long length,
                                     Arena arena, int blockSize, int mmuPageSize) throws IOException {
        try (RefCountedFD ref = fdCache.acquire(filePath)) {
            int fd = ref.fd();
            if (blockSize > mmuPageSize) {
                return readUnaligned(fd, offset, length, arena, mmuPageSize);
            }
            return readAligned(fd, offset, length, arena, blockSize, mmuPageSize);
        }
    }

    private MemorySegment readUncached(Path filePath, long offset, long length,
                                       Arena arena, int blockSize, int mmuPageSize) throws IOException {
        int fd = PosixFD.openDirect(filePath);
        try {
            if (blockSize > mmuPageSize) {
                return readUnaligned(fd, offset, length, arena, mmuPageSize);
            }
            return readAligned(fd, offset, length, arena, blockSize, mmuPageSize);
        } finally {
            PosixFD.close(fd);
        }
    }

    /** NFS/EFS path: no alignment padding, read exactly [offset, offset+length). */
    private MemorySegment readUnaligned(int fd, long offset, long length,
                                        Arena arena, int pageSize) throws IOException {
        MemorySegment buffer = arena.allocate(length, pageSize);
        long bytesRead = PosixFD.pread(fd, buffer, length, offset);
        if (bytesRead < 0) {
            return arena.allocate(0);
        }
        return buffer.asSlice(0, Math.min(length, bytesRead));
    }

    /** Local filesystem path: align offset/length to kernel requirements. */
    private MemorySegment readAligned(int fd, long offset, long length,
                                      Arena arena, int blockSize, int pageSize) throws IOException {
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForKernel(offset, length, blockSize, pageSize);
        MemorySegment buffer = arena.allocate(ar.alignedLength(), (int) Math.min(ar.alignedLength(), pageSize));

        long bytesRead = PosixFD.pread(fd, buffer, ar.alignedLength(), ar.alignedOffset());
        if (bytesRead < 0) {
            return arena.allocate(0);
        }
        int available = Math.max(0, (int) bytesRead - (int) ar.offsetDelta());
        int toCopy = (int) Math.min(length, available);
        return buffer.asSlice(ar.offsetDelta(), toCopy);
    }

    @Override
    public void close() throws IOException {
        if (fdCache != null) {
            fdCache.close();
        }
    }
}
