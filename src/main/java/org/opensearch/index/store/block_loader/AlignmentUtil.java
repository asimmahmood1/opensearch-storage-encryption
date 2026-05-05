/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.io.IOException;

/**
 * Shared alignment computation for Direct I/O backends.
 *
 * <p>Direct I/O requires buffer addresses, read offsets, and read lengths to be
 * aligned to a boundary that depends on the I/O path:
 * <ul>
 *   <li><b>FileChannel</b>: JDK enforces {@code f_bsize} alignment → use {@code max(blockSize, pageSize)}</li>
 *   <li><b>pread / io_uring</b>: bypass JDK, kernel only needs page alignment → use {@code max(512, min(blockSize, pageSize))}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class AlignmentUtil {

    private AlignmentUtil() {}

    /** Minimum sector alignment (512 bytes). */
    private static final int MIN_SECTOR_ALIGNMENT = 512;

    /**
     * Result of an alignment computation: the aligned offset, the delta from the
     * original offset, and the aligned length.
     */
    public record AlignedRead(long alignedOffset, long offsetDelta, long alignedLength) {}

    /**
     * Computes alignment for backends that bypass JDK FileChannel (pread, io_uring).
     * Uses {@code max(512, min(blockSize, pageSize))} to minimize read amplification.
     *
     * @param offset    logical byte offset in the file
     * @param length    number of bytes to read
     * @param blockSize filesystem block size (from FileStore.getBlockSize())
     * @param pageSize  system MMU page size
     * @return the aligned read parameters
     */
    public static AlignedRead alignForKernel(long offset, long length, int blockSize, int pageSize) {
        int alignment = Math.max(MIN_SECTOR_ALIGNMENT, Math.min(blockSize, pageSize));
        return align(offset, length, alignment);
    }

    /**
     * Computes alignment for the JDK FileChannel backend.
     * Uses {@code max(blockSize, pageSize)} because JDK enforces {@code f_bsize} alignment.
     *
     * @param offset    logical byte offset in the file
     * @param length    number of bytes to read
     * @param blockSize filesystem block size (from FileStore.getBlockSize())
     * @param pageSize  system MMU page size
     * @return the aligned read parameters
     */
    public static AlignedRead alignForFileChannel(long offset, long length, int blockSize, int pageSize) {
        int alignment = Math.max(blockSize, pageSize);
        return align(offset, length, alignment);
    }

    /**
     * Core alignment logic shared by all backends.
     *
     * <p>Note: {@code alignment=0} is explicitly rejected by {@link #align} because
     * {@code (0 & -1) == 0} would pass the power-of-2 bit-trick, producing
     * {@code mask=-1}, {@code alignedOffset=0}, and {@code alignedLength=0} (integer wrap).
     *
     * @param offset    logical byte offset
     * @param length    number of bytes to read
     * @param alignment alignment boundary (must be a power of 2)
     * @return the aligned read parameters
     * @throws IllegalArgumentException if alignment is not a power of 2
     */
    public static AlignedRead align(long offset, long length, int alignment) {
        if (alignment <= 0 || (alignment & (alignment - 1)) != 0) {
            throw new IllegalArgumentException("Alignment must be a positive power of 2: " + alignment);
        }
        long mask = (long) alignment - 1;
        long alignedOffset = offset & ~mask;
        long offsetDelta = offset - alignedOffset;
        long alignedLength = (offsetDelta + length + mask) & ~mask;
        return new AlignedRead(alignedOffset, offsetDelta, alignedLength);
    }
}
