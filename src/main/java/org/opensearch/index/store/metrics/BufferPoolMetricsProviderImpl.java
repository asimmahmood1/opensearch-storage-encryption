/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.metrics;

import com.amazonaws.juno.metric.bufferpool.BufferPoolMetricsProvider;
import org.opensearch.index.store.block_cache.PrefetchTracker;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.opensearch.index.store.CryptoDirectoryFactory;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.hll.WorkingSetEstimator;
import org.opensearch.index.store.hll.WorkingSetEstimatorScheduler;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;

/**
 * Implementation of BufferPoolMetricsProvider that exposes HLL metrics
 * to JunoSearchWorker's NodeStatsCollector and accepts dynamic configuration.
 */
public class BufferPoolMetricsProviderImpl implements BufferPoolMetricsProvider {

    private final WorkingSetEstimatorScheduler scheduler;

    public BufferPoolMetricsProviderImpl(WorkingSetEstimatorScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public long getWorkingSet(long windowSeconds) {
        return WorkingSetEstimator.getInstance().estimateCardinality(windowSeconds);
    }

    @Override
    public long getCacheCapacity() {
        return WorkingSetEstimator.getInstance().getCacheCapacity();
    }

    @Override
    public long getReadAheadBlocksLoaded() {
        return WorkingSetEstimator.getInstance().getReadAheadBlocksLoaded();
    }

    @Override
    public long getReadAheadBlocksWasted() {
        return WorkingSetEstimator.getInstance().getReadAheadBlocksWasted();
    }

    @Override
    public void setEnabled(boolean enabled) {
        WorkingSetEstimator.getInstance().setEnabled(enabled);
        if (enabled) {
            scheduler.start();
        } else {
            scheduler.stop();
        }
    }

    @Override
    public void setPollIntervalSeconds(int intervalSeconds) {
        scheduler.updateInterval(intervalSeconds);
    }

    @Override
    public void setWindowRecentSeconds(int windowSeconds) {
        scheduler.setWindowRecentSeconds(windowSeconds);
    }

    @Override
    public void setWindowExtendedSeconds(int windowSeconds) {
        scheduler.setWindowExtendedSeconds(windowSeconds);
    }

    private PrefetchTracker prefetchTracker() {
        return CryptoDirectoryFactory.getSharedPrefetchTracker();
    }

    @Override
    public long getPrefetchCalls() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getCalls() : 0;
    }

    @Override
    public long getPrefetchBlocksRequested() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getBlocksRequested() : 0;
    }

    @Override
    public long getPrefetchBlocksLoaded() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getBlocksLoaded() : 0;
    }

    @Override
    public long getPrefetchBlocksDeduped() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getBlocksDeduped() : 0;
    }

    @Override
    public long getPrefetchBlocksCacheHit() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getBlocksCacheHit() : 0;
    }

    @Override
    public long getPrefetchTimeNs() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getPrefetchTimeNs() : 0;
    }

    @Override
    public long getPrefetchExecuteRejections() {
        PrefetchTracker pt = prefetchTracker();
        return pt != null ? pt.getExecuteRejections() : 0;
    }

    // ---- Pool metrics ----

    private MemorySegmentPool pool() {
        Pool<?> p = CryptoDirectoryFactory.getSharedPool();
        return p instanceof MemorySegmentPool ? (MemorySegmentPool) p : null;
    }

    @Override
    public int getPoolMaxSegments() {
        MemorySegmentPool p = pool();
        return p != null ? (int)(p.totalMemory() / p.pooledSegmentSize()) : 0;
    }

    @Override
    public int getPoolBuffersInUse() {
        MemorySegmentPool p = pool();
        return p != null ? p.getBuffersInUse() : 0;
    }

    @Override
    public long getPoolAllocatedBytes() {
        MemorySegmentPool p = pool();
        return p != null ? p.getAllocatedBytes() : 0;
    }

    @Override
    public long getPoolNativeUsedBytes() {
        MemorySegmentPool p = pool();
        return p != null ? p.getDirectMemoryUsed() : -1;
    }

    @Override
    public long getPoolZombieBytes() {
        long native_ = getPoolNativeUsedBytes();
        long allocated = getPoolAllocatedBytes();
        return native_ >= 0 ? Math.max(0, native_ - allocated) : 0;
    }

    @Override
    public long getPoolStallCount() {
        MemorySegmentPool p = pool();
        return p != null ? p.getStallCount() : 0;
    }

    @Override
    public long getPoolGcTriggerCount() {
        MemorySegmentPool p = pool();
        return p != null ? p.getGcTriggerCount() : 0;
    }

    // ---- Cache metrics ----

    @SuppressWarnings("rawtypes")
    private CaffeineBlockCache caffeineCache() {
        BlockCache<?> c = CryptoDirectoryFactory.getSharedBlockCache();
        return c instanceof CaffeineBlockCache ? (CaffeineBlockCache) c : null;
    }

    /** Snapshot all cache stats once per collection to ensure consistency. */
    private volatile CacheStats cachedStats;
    private volatile long cachedStatsTimestamp;

    @SuppressWarnings("rawtypes")
    private CacheStats cacheStats() {
        long now = System.nanoTime();
        // Reuse snapshot for 5 second — covers a full collection cycle
        if (cachedStats != null && (now - cachedStatsTimestamp) < 5_000_000_000L) {
            return cachedStats;
        }
        CaffeineBlockCache c = caffeineCache();
        cachedStats = c != null ? c.getCache().stats() : null;
        cachedStatsTimestamp = now;
        return cachedStats;
    }

    @Override
    public long getCacheSize() {
        CaffeineBlockCache c = caffeineCache();
        return c != null ? c.getCache().estimatedSize() : 0;
    }

    @Override
    public long getCacheMaxSize() {
        CaffeineBlockCache c = caffeineCache();
        return c != null ? c.getMaxSize() : 0L;
    }

    @Override
    public long getCacheHits() {
        var s = cacheStats();
        return s != null ? s.hitCount() : 0;
    }

    @Override
    public long getCacheMisses() {
        var s = cacheStats();
        return s != null ? s.missCount() : 0;
    }

    @Override
    public double getCacheHitRate() {
        var s = cacheStats();
        return s != null ? s.hitRate() : 0;
    }

    @Override
    public long getCacheLoads() {
        var s = cacheStats();
        return s != null ? s.loadCount() : 0;
    }

    @Override
    public long getCacheEvictions() {
        var s = cacheStats();
        return s != null ? s.evictionCount() : 0;
    }

    @Override
    public double getCacheAvgLoadNanos() {
        var s = cacheStats();
        return s != null ? s.averageLoadPenalty() : 0;
    }
}
