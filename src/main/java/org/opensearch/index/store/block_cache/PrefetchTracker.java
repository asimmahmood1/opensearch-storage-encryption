/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks prefetch deduplication state and statistics.
 * Encapsulates the in-flight dedup map, counters, and async executor for prefetch operations.
 *
 * @opensearch.internal
 */
public class PrefetchTracker {

    private final ConcurrentHashMap<BlockCacheKey, Boolean> inflight = new ConcurrentHashMap<>();
    private final Executor executor;

    private final LongAdder loadMissingBlocksCalls = new LongAdder();
    private final LongAdder blocksRequested = new LongAdder();
    private final LongAdder blocksLoaded = new LongAdder();
    private final LongAdder blocksDeduped = new LongAdder();
    private final LongAdder blocksCacheHit = new LongAdder();
    private final LongAdder executeRejections = new LongAdder();

    /**
     * Creates a prefetch tracker with the given async executor.
     *
     * @param executor the executor for async prefetch operations (must not be null)
     */
    public PrefetchTracker(Executor executor) {
        this.executor = executor;
    }

    /**
     * Submits a task for async prefetch execution.
     *
     * @param task the runnable to execute
     */
    public void execute(Runnable task) {
        try {
            executor.execute(task);
        } catch (Exception e) {
            executeRejections.increment();
        }
    }

    /**
     * Attempts to mark a block as in-flight for prefetch.
     *
     * @param key the block cache key
     * @return true if newly added (should be loaded), false if already in-flight (deduped)
     */
    public boolean putIfAbsent(BlockCacheKey key) {
        if (inflight.putIfAbsent(key, Boolean.TRUE) == null) {
            return true;
        }
        blocksDeduped.increment();
        return false;
    }

    public void remove(BlockCacheKey key) {
        inflight.remove(key);
    }

    public int size() {
        return inflight.size();
    }

    public boolean isEmpty() {
        return inflight.isEmpty();
    }

    public void clear() {
        inflight.clear();
    }

    public void recordLoadMissingBlocksCall(long blockCount) {
        loadMissingBlocksCalls.increment();
        blocksRequested.add(blockCount);
    }

    public void recordBlocksLoaded(long count) {
        blocksLoaded.add(count);
    }

    public void recordCacheHits(long count) {
        blocksCacheHit.add(count);
    }

    public String stats() {
        long calls = loadMissingBlocksCalls.sum();
        long requested = blocksRequested.sum();
        long loaded = blocksLoaded.sum();
        long deduped = blocksDeduped.sum();
        long cacheHit = blocksCacheHit.sum();
        long rejections = executeRejections.sum();
        double cacheHitRate = requested > 0 ? (100.0 * cacheHit / requested): 0;
        double loadRatio = requested > 0 ? (100.0 * loaded / requested) : 0;
        return String
            .format(
                "Prefetch[calls=%d, requested=%d, loaded=%d, deduped=%d, cacheHit=%d, hitRatio=%.2f%%, loadRatio=%.2f%%, inflight=%d, rejections=%d]",
                calls,
                requested,
                loaded,
                deduped,
                cacheHit,
                cacheHitRate,
                loadRatio,
                inflight.size(),
                rejections
            );
    }

    public long getCalls() {
        return loadMissingBlocksCalls.sum();
    }

    public long getBlocksRequested() {
        return blocksRequested.sum();
    }

    public long getBlocksLoaded() {
        return blocksLoaded.sum();
    }

    public long getBlocksDeduped() {
        return blocksDeduped.sum();
    }

    public long getBlocksCacheHit() {
        return blocksCacheHit.sum();
    }

    public long getExecuteRejections() {
        return executeRejections.sum();
    }

    // Testing and benchmarking
    public void resetStats() {
        loadMissingBlocksCalls.reset();
        blocksRequested.reset();
        blocksLoaded.reset();
        blocksDeduped.reset();
        blocksCacheHit.reset();
        executeRejections.reset();
    }
}
