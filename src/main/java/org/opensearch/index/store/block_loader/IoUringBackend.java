/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.PanamaNativeAccess;
import org.opensearch.index.store.iouring.IoUringChannelCache;
import org.opensearch.index.store.iouring.RefCountedChannel;
import org.opensearch.index.store.iouring.api.IoUringFileChannel;

/**
 * I/O backend that uses {@link IoUringFileChannel} for async io_uring-based I/O
 * with O_DIRECT.
 *
 * <p>io_uring submits directly to the kernel ring buffer, bypassing JDK
 * {@link java.nio.channels.FileChannel} alignment enforcement — the kernel only
 * requires page-aligned buffers and offsets for O_DIRECT. This avoids the read
 * amplification that {@code FileChannel} suffers on NFS/EFS where
 * {@code f_bsize} (1MB) >> page size (4KB).
 *
 * <p>When constructed with an {@link IoUringChannelCache}, channels are pooled
 * and reused across reads, amortizing the open/close syscall overhead.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO via IoUringFileChannel")
public class IoUringBackend implements IOBackendStrategy {

    private final IoUringChannelCache channelCache;

    /** Construct with channel caching disabled (open/close every read). */
    public IoUringBackend() {
        this(null);
    }

    /** Construct with an optional channel cache for pooling IoUringFileChannels. */
    public IoUringBackend(IoUringChannelCache channelCache) {
        this.channelCache = channelCache;
    }

    @Override
    public MemorySegment read(Path filePath, long offset, long length,
                              Arena arena, int blockSize) throws IOException {
        int mmuPageSize = PanamaNativeAccess.getPageSize();
        if (channelCache != null) {
            return readCached(filePath, offset, length, arena, blockSize, mmuPageSize);
        }
        return readUncached(filePath, offset, length, arena, blockSize, mmuPageSize);
    }

    private MemorySegment readCached(Path filePath, long offset, long length,
                                     Arena arena, int blockSize, int mmuPageSize) throws IOException {
        try (RefCountedChannel ref = channelCache.acquire(filePath)) {
            IoUringFileChannel channel = ref.channel();
            if (blockSize > mmuPageSize) {
                return readUnaligned(channel, offset, length, arena, mmuPageSize);
            }
            return readAligned(channel, offset, length, arena, blockSize, mmuPageSize);
        }
    }

    private MemorySegment readUncached(Path filePath, long offset, long length,
                                       Arena arena, int blockSize, int mmuPageSize) throws IOException {
        try (IoUringFileChannel channel = IoUringFileChannel.open(
                filePath, StandardOpenOption.READ,
                DirectIOReaderUtil.getDirectOpenOption())) {
            // On NFS/EFS (blockSize > pageSize), the kernel NFS client handles
            // DMA alignment internally — io_uring bypasses JDK so no f_bsize
            // alignment is needed. Read exactly the requested range.
            if (blockSize > mmuPageSize) {
                return readUnaligned(channel, offset, length, arena, mmuPageSize);
            }
            return readAligned(channel, offset, length, arena, blockSize, mmuPageSize);
        }
    }

    /** NFS/EFS path: no alignment padding, read exactly [offset, offset+length). */
    private MemorySegment readUnaligned(IoUringFileChannel channel, long offset, long length,
                                        Arena arena, int pageSize) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException("Read size too large: " + length);
        }
        MemorySegment buffer = arena.allocate(length, pageSize);
        ByteBuffer directBuffer = buffer.asByteBuffer();

        int bytesRead = channel.read(directBuffer, offset);
        if (bytesRead < 0) {
            return arena.allocate(0);
        }
        return buffer.asSlice(0, Math.min(length, bytesRead));
    }

    /** Local filesystem path: align offset/length to kernel requirements. */
    private MemorySegment readAligned(IoUringFileChannel channel, long offset, long length,
                                      Arena arena, int blockSize, int pageSize) throws IOException {
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForKernel(offset, length, blockSize, pageSize);

        if (ar.alignedLength() > Integer.MAX_VALUE) {
            throw new IOException("Aligned read size too large: " + ar.alignedLength());
        }

        MemorySegment alignedSegment = arena.allocate(ar.alignedLength(), Math.min((int) ar.alignedLength(), pageSize));
        ByteBuffer directBuffer = alignedSegment.asByteBuffer();

        int bytesRead = channel.read(directBuffer, ar.alignedOffset());
        if (bytesRead < 0) {
            return arena.allocate(0);
        }
        int available = Math.max(0, bytesRead - (int) ar.offsetDelta());
        int toCopy = (int) Math.min(length, available);
        return alignedSegment.asSlice(ar.offsetDelta(), toCopy);
    }

    @Override
    public void close() throws IOException {
        if (channelCache != null) {
            channelCache.close();
        }
    }
}
