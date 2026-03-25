/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;

/**
 * L1 block cache backed by {@link RadixBlockTable}.
 * Two plain array loads on the read path — no fences, no synchronization.
 * Falls through to the Caffeine L2 cache on miss.
 */
public class RadixL1BlockCache implements L1BlockCache {

    private final RadixBlockTable<BlockCacheValue<RefCountedMemorySegment>> table;
    private final BlockCache<RefCountedMemorySegment> cache;
    private final Path path;

    private final LongAdder l1Hits = new LongAdder();
    private final LongAdder l2Hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public RadixL1BlockCache(BlockCache<RefCountedMemorySegment> cache, Path path, long fileLength) {
        this.cache = cache;
        this.path = path;
        int outerSlots = (int) ((fileLength >>> CACHE_BLOCK_SIZE_POWER >>> RadixBlockTable.PAGE_SHIFT) + 1);
        this.table = new RadixBlockTable<>(Math.max(outerSlots, RadixBlockTable.DEFAULT_OUTER_SLOTS));
    }

    @Override
    public BlockCacheValue<RefCountedMemorySegment> acquireRefCountedValue(long blockOff, BlockSlotTinyCache.CacheHitHolder hitHolder)
        throws IOException {
        final long blockIdx = blockOff >>> CACHE_BLOCK_SIZE_POWER;

        // L1 lookup — two plain array loads
        BlockCacheValue<RefCountedMemorySegment> v = table.get(blockIdx);
        if (v != null && v.tryPin()) {
            if (hitHolder != null) hitHolder.setWasCacheHit(true);
            l1Hits.increment();
            return v;
        }

        // L2 hit
        FileBlockCacheKey key = new FileBlockCacheKey(path, blockOff);
        v = cache.get(key);
        if (v != null && v.tryPin()) {
            table.put(blockIdx, v);
            if (hitHolder != null) hitHolder.setWasCacheHit(true);
            l2Hits.increment();
            return v;
        }

        // L2 load
        v = cache.getOrLoad(key);
        if (v != null && v.tryPin()) {
            table.put(blockIdx, v);
            if (hitHolder != null) hitHolder.setWasCacheHit(false);
            misses.increment();
            return v;
        }

        throw new IOException("Unable to acquire block at offset " + blockOff);
    }

    @Override
    public void clear() {
        table.clear();
    }

    @Override
    public String stats() {
        long l1 = l1Hits.sum(), l2 = l2Hits.sum(), m = misses.sum();
        long total = l1 + l2 + m;
        return String.format(
            "RadixL1[l1Hits=%d, l2Hits=%d, misses=%d, total=%d, l1Rate=%.2f%%, l2Rate=%.2f%%]",
            l1, l2, m, total,
            total == 0 ? 0 : l1 * 100.0 / total,
            total == 0 ? 0 : l2 * 100.0 / total
        );
    }

    @Override
    public void resetStats() {
        l1Hits.reset();
        l2Hits.reset();
        misses.reset();
    }
}
