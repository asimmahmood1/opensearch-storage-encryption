/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.opensearch.index.store.block_cache.BlockCacheValue;

/**
 * A GC-managed wrapper around a direct {@link ByteBuffer} that implements {@link BlockCacheValue}.
 *
 * <p>All lifecycle methods are no-ops. The JVM's GC frees the backing
 * DirectByteBuffer when this object becomes unreachable. No ref counting,
 * no generation tracking, no close flag.
 *
 * @opensearch.internal
 */
public final class RefCountedByteBuffer implements BlockCacheValue<RefCountedByteBuffer> {

    private final ByteBuffer buffer;
    private final int length;
    private final MemorySegment segment;

    public RefCountedByteBuffer(ByteBuffer buffer, int length) {
        this.buffer = buffer;
        this.length = length;
        this.segment = MemorySegment.ofBuffer(buffer);
    }

    public ByteBuffer buffer() { return buffer; }

    public MemorySegment segment() { return segment; }

    @Override public RefCountedByteBuffer value() { return this; }

    @Override public int length() { return length; }

    @Override public int getGeneration() { return 0; }

    @Override public boolean tryPin() { return true; }

    @Override public void unpin() {}

    @Override public void decRef() {}

    @Override public void close() {}
}
