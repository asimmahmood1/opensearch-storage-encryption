/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

/**
 * Strategy interface that abstracts the open-read-close lifecycle for a single
 * I/O backend, allowing the block loader to delegate disk reads without
 * knowledge of the underlying mechanism.
 */
@SuppressWarnings("preview")
public interface IOBackendStrategy {

    /**
     * Reads aligned data from a file, returning a MemorySegment containing
     * exactly {@code length} bytes starting at {@code offset}.
     *
     * The implementation handles O_DIRECT alignment internally:
     * - Aligns offset down to max(blockSize, pageSize) boundary
     * - Aligns read length up to the same boundary
     * - Returns a slice of the aligned buffer corresponding to the
     *   requested [offset, offset+length) range
     *
     * @param filePath   path to the file to read
     * @param offset     logical byte offset in the file
     * @param length     number of bytes to read
     * @param arena      Arena for allocating the result MemorySegment
     * @param blockSize  filesystem block size (from FileStore.getBlockSize())
     * @return MemorySegment containing the read data
     * @throws IOException on I/O failure
     */
    MemorySegment read(Path filePath, long offset, long length,
                       Arena arena, int blockSize) throws IOException;

    /**
     * Releases any resources held by this backend (e.g., shared rings,
     * cached file descriptors). Called when the BlockLoader is no longer needed.
     * Default implementation is a no-op for stateless backends.
     */
    default void close() throws IOException {}
}
