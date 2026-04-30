/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


/**
 * Tracks prefetch deduplication state and statistics.
 * Encapsulates the in-flight dedup map, counters, and async executor for prefetch operations.
 *
 * @opensearch.internal
 */
public class PrefetchTracker {

    private static volatile boolean enabled = true;

    private final ConcurrentHashMap<BlockCacheKey, Boolean> inflight = new ConcurrentHashMap<>();
    private final AtomicInteger inflightCount = new AtomicInteger();
    private final Executor executor;

    private final LongAdder prefetchCalls = new LongAdder();
    private final LongAdder blocksRequested = new LongAdder();
    private final LongAdder blocksLoaded = new LongAdder();
    private final LongAdder blocksDeduped = new LongAdder();
    private final LongAdder blocksCacheHit = new LongAdder();
    private final LongAdder prefetchTimeNs = new LongAdder();
    private final LongAdder executeRejections = new LongAdder();
    private final LongAdder l1Hits = new LongAdder();
    private final LongAdder l1Misses = new LongAdder();
    private final LongAdder l1Promotions = new LongAdder();
    private final LongAdder leadHits = new LongAdder();
    private final LongAdder leadMisses = new LongAdder();
    private final ConcurrentHashMap<BlockCacheKey, Boolean> completed = new ConcurrentHashMap<>();

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
            executeRejections.increment();
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
        blocksDeduped.increment();
        return false;
    }

    public void remove(BlockCacheKey key) {
        if (inflight.remove(key) != null) {
            inflightCount.decrementAndGet();
        }
    }

    /** Number of completed entries awaiting consumption by checkLeadHit. */
    public int completedSize() {
        return completed.size();
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
        prefetchCalls.increment();
        blocksRequested.add(blockCount);
    }

    public void recordPrefetchTimeNs(long nanos) {
        prefetchTimeNs.add(nanos);
    }

    public void recordBlocksLoaded(long count) {
        blocksLoaded.add(count);
    }

    public void recordL1Hits(long count) {
        l1Hits.add(count);
    }

    public void recordL1Misses(long count) {
        l1Misses.add(count);
    }

    public void recordL1Promotion() {
        l1Promotions.increment();
    }

    public void recordCacheHits(long count) {
        blocksCacheHit.add(count);
    }

    public String stats() {
        long calls = prefetchCalls.sum();
        long requested = blocksRequested.sum();
        long loaded = blocksLoaded.sum();
        long deduped = blocksDeduped.sum();
        long cacheHit = blocksCacheHit.sum();
        long timeMs = prefetchTimeNs.sum() / 1_000_000;
        long rejections = executeRejections.sum();
        double loadRatio = requested > 0 ? (100.0 * loaded / requested) : 0;
        return String
            .format(
                "Prefetch[calls=%d, requested=%d, loaded=%d, deduped=%d, cacheHit=%d, loadRatio=%.2f%%, timeMs=%d, inflight=%d, rejections=%d, leadHits=%d, leadMisses=%d]",
                calls,
                requested,
                loaded,
                deduped,
                cacheHit,
                loadRatio,
                timeMs,
                inflight.size(),
                rejections,
                leadHits.sum(),
                leadMisses.sum()
            );
    }

    public long getCalls() {
        return prefetchCalls.sum();
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

    public long getPrefetchTimeNs() {
        return prefetchTimeNs.sum();
    }

    public long getExecuteRejections() {
        return executeRejections.sum();
    }

    public long getL1Hits() {
        return l1Hits.sum();
    }

    public long getL1Misses() {
        return l1Misses.sum();
    }

    public long getL1Promotions() {
        return l1Promotions.sum();
    }

    /** Mark a block as successfully loaded by prefetch. */
    public void markCompleted(BlockCacheKey key) {
        completed.put(key, Boolean.TRUE);
    }

    /** Check and consume a lead hit: acquireBlock found block in L1 that prefetch loaded. */
    public boolean checkLeadHit(BlockCacheKey key) {
        if (completed.remove(key) != null) {
            leadHits.increment();
            return true;
        }
        return false;
    }

    /** Check if a block is currently being prefetched (in-flight). */
    public boolean isInflight(BlockCacheKey key) {
        return inflight.containsKey(key);
    }

    /** Record a lead miss: acquireBlock needed a block that prefetch hadn't finished loading. */
    public void recordLeadMiss() {
        leadMisses.increment();
    }

    public long getLeadHits() { return leadHits.sum(); }
    public long getLeadMisses() { return leadMisses.sum(); }

    // Testing only
    void resetStats() {
        prefetchCalls.reset();
        blocksRequested.reset();
        blocksLoaded.reset();
        blocksDeduped.reset();
        blocksCacheHit.reset();
        prefetchTimeNs.reset();
        executeRejections.reset();
        l1Hits.reset();
        l1Misses.reset();
        l1Promotions.reset();
        leadHits.reset();
        leadMisses.reset();
        completed.clear();
    }
}
