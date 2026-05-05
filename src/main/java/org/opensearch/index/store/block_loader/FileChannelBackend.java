/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.store.block_loader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.opensearch.common.SuppressForbidden;

/**
 * I/O backend that uses Java NIO {@link FileChannel} with Direct I/O.
 *
 * <p>This is a direct extraction of the I/O path from
 * {@code CryptoDirectIOBlockLoader.load()} and preserves byte-identical
 * behavior with the original implementation.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "uses custom DirectIO")
public class FileChannelBackend implements IOBackendStrategy {

    @Override
    public MemorySegment read(Path filePath, long offset, long length,
                              Arena arena, int blockSize) throws IOException {
        try (FileChannel channel = FileChannel.open(
                filePath, StandardOpenOption.READ,
                DirectIOReaderUtil.getDirectOpenOption())) {
            return DirectIOReaderUtil.directIOReadAligned(
                channel, offset, length, arena, blockSize);
        }
    }
}
