/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks prefetch deduplication state and statistics.
 * Encapsulates the in-flight dedup map, counters, and async executor for prefetch operations.
 *
 * @opensearch.internal
 */
public class PrefetchTracker {

    private final ConcurrentHashMap<BlockCacheKey, Boolean> inflight = new ConcurrentHashMap<>();
    private final AtomicInteger inflightCount = new AtomicInteger();
    private final Executor executor;
    private static volatile boolean enabled = true;

    private final AtomicLong prefetchCalls = new AtomicLong();
    private final AtomicLong blocksRequested = new AtomicLong();
    private final AtomicLong blocksLoaded = new AtomicLong();
    private final AtomicLong blocksDeduped = new AtomicLong();
    private final AtomicLong blocksCacheHit = new AtomicLong();
    private final AtomicLong prefetchTimeNs = new AtomicLong();
    private final AtomicLong executeRejections = new AtomicLong();
    private final AtomicLong l1Hits = new AtomicLong();
    private final AtomicLong l1Misses = new AtomicLong();

    private final int maxInflight;

    /**
     * Creates a prefetch tracker with the given async executor.
     *
     * @param executor the executor for async prefetch operations (must not be null)
     * @param maxInflight maximum in-flight prefetch tasks before dropping new submissions
     */
    public PrefetchTracker(Executor executor, int maxInflight) {
        this.executor = executor;
        this.maxInflight = maxInflight;
    }

    public PrefetchTracker(Executor executor) {
        this(executor, 10_000);
    }

    /**
     * Submits a task for async prefetch execution.
     * Drops the task if prefetch is disabled or too many operations are already in-flight.
     *
     * @param task the runnable to execute
     */
    public void execute(Runnable task) {
        if (!enabled) {
            return;
        }
        if (inflightCount.get() > maxInflight) {
            executeRejections.incrementAndGet();
            return;
        }
        executor.execute(task);
    }

    /**
     * Attempts to mark a block as in-flight for prefetch.
     *
     * @param key the block cache key
     * @return true if newly added (should be loaded), false if already in-flight (deduped)
     */
    public boolean putIfAbsent(BlockCacheKey key) {
        if (inflight.putIfAbsent(key, Boolean.TRUE) == null) {
            inflightCount.incrementAndGet();
            return true;
        }
        blocksDeduped.incrementAndGet();
        return false;
    }

    public void remove(BlockCacheKey key) {
        if (inflight.remove(key) != null) {
            inflightCount.decrementAndGet();
        }
    }

    public int size() {
        return inflightCount.get();
    }

    public boolean isEmpty() {
        return inflightCount.get() == 0;
    }

    public void clear() {
        inflight.clear();
        inflightCount.set(0);
    }

    public void recordPrefetchCall(long blockCount) {
        prefetchCalls.incrementAndGet();
        blocksRequested.addAndGet(blockCount);
    }

    public void recordPrefetchTimeNs(long nanos) {
        prefetchTimeNs.addAndGet(nanos);
    }

    public void recordBlocksLoaded(long count) {
        blocksLoaded.addAndGet(count);
    }

    public void recordCacheHits(long count) {
        blocksCacheHit.addAndGet(count);
    }

    public String stats() {
        long calls = prefetchCalls.get();
        long requested = blocksRequested.get();
        long loaded = blocksLoaded.get();
        long deduped = blocksDeduped.get();
        long cacheHit = blocksCacheHit.get();
        long timeMs = prefetchTimeNs.get() / 1_000_000;
        long rejections = executeRejections.get();
        double loadRatio = requested > 0 ? (100.0 * loaded / requested) : 0;
        return String
            .format(
                "Prefetch[calls=%d, requested=%d, loaded=%d, deduped=%d, cacheHit=%d, loadRatio=%.2f%%, timeMs=%d, inflight=%d, rejections=%d]",
                calls,
                requested,
                loaded,
                deduped,
                cacheHit,
                loadRatio,
                timeMs,
                inflight.size(),
                rejections
            );
    }

    public long getCalls() {
        return prefetchCalls.get();
    }

    public long getBlocksRequested() {
        return blocksRequested.get();
    }

    public long getBlocksLoaded() {
        return blocksLoaded.get();
    }

    public long getBlocksDeduped() {
        return blocksDeduped.get();
    }

    public long getBlocksCacheHit() {
        return blocksCacheHit.get();
    }

    public long getPrefetchTimeNs() {
        return prefetchTimeNs.get();
    }

    public long getExecuteRejections() {
        return executeRejections.get();
    }

    public void recordL1Hits(long count) {
        l1Hits.addAndGet(count);
    }

    public void recordL1Misses(long count) {
        l1Misses.addAndGet(count);
    }

    public long getL1Hits() {
        return l1Hits.get();
    }

    public long getL1Misses() {
        return l1Misses.get();
    }

    // Testing only
    void resetStats() {
        prefetchCalls.set(0);
        blocksRequested.set(0);
        blocksLoaded.set(0);
        blocksDeduped.set(0);
        blocksCacheHit.set(0);
        prefetchTimeNs.set(0);
        executeRejections.set(0);
        l1Hits.set(0);
        l1Misses.set(0);
    }
}
