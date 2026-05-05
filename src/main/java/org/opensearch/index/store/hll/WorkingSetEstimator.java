/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hll;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Singleton wrapper for the Sliding Window HLL estimator.
 * Provides a global instance for tracking working set cardinality across the buffer pool.
 * Also tracks read-ahead statistics separately.
 */
public class WorkingSetEstimator {
    private static final Logger logger = LogManager.getLogger(WorkingSetEstimator.class);

    /** 9 register bits = 512 buckets, ~4.59% standard error. Matches Aurora's configuration. */
    private static final int DEFAULT_REGISTER_BITS = 9;

    /** Bytes per block in the buffer pool */
    static final long BLOCK_SIZE_BYTES = 8192L;

    /** Bytes per megabyte */
    static final long BYTES_PER_MB = 1024L * 1024L;

    private static final WorkingSetEstimator INSTANCE = new WorkingSetEstimator();

    private volatile SlidingWindowHLL hll;

    /** Controls whether HLL tracking is active */
    private volatile boolean enabled = false;

    // Track read-ahead statistics separately
    private final AtomicLong readAheadBlocksLoaded = new AtomicLong(0);
    private final AtomicLong readAheadBlocksAccessed = new AtomicLong(0);

    // Cache capacity (set when cache is initialized)
    private volatile long cacheCapacityBlocks = 0;

    private WorkingSetEstimator() {
        this.hll = new SlidingWindowHLL(DEFAULT_REGISTER_BITS);
        logger.info("WorkingSetEstimator initialized: {}", hll);
    }

    /**
     * Gets the singleton instance.
     */
    public static WorkingSetEstimator getInstance() {
        return INSTANCE;
    }

    /**
     * Updates the estimator with a block access.
     * Uses Murmur3 hash of (pathHash, blockNumber) for uniform register distribution.
     *
     * @param pathHashCode hashCode of the file path
     * @param fileOffset block-aligned byte offset in the file
     */
    public void update(int pathHashCode, long fileOffset) {
        if (!enabled) return;
        // Combine path identity (upper 32 bits) with block number (lower 32 bits)
        // fileOffset >>> 13 converts byte offset to block number (8192 = 2^13)
        long combined = ((long) pathHashCode << 32) | (fileOffset >>> 13);
        int hllHash = (int) org.opensearch.common.hash.MurmurHash3.murmur64(combined);
        hll.update(hllHash, System.currentTimeMillis() / 1000L);
    }

    /**
     * Enables or disables HLL tracking.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        logger.info("WorkingSetEstimator enabled={}", enabled);
    }

    /**
     * Returns whether HLL tracking is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Records the number of blocks were loaded via read-ahead.
     */
    public void recordReadAheadLoad(long loaded) {
        readAheadBlocksLoaded.addAndGet(loaded);
    }

    /**
     * Records that a read-ahead block was accessed by a query.
     */
    public void recordReadAheadAccess() {
        readAheadBlocksAccessed.incrementAndGet();
    }

    /**
     * Gets the total number of blocks loaded via read-ahead.
     */
    public long getReadAheadBlocksLoaded() {
        return readAheadBlocksLoaded.get();
    }

    /**
     * Gets the number of read-ahead blocks that were actually accessed.
     */
    public long getReadAheadBlocksAccessed() {
        return readAheadBlocksAccessed.get();
    }

    /**
     * Gets the number of read-ahead blocks that were never accessed (wasted prefetch).
     * Bounded to 0 to handle race conditions where accessed > loaded temporarily.
     */
    public long getReadAheadBlocksWasted() {
        return Math.max(0, readAheadBlocksLoaded.get() - readAheadBlocksAccessed.get());
    }

    /**
     * Gets the read-ahead hit rate (percentage of prefetched blocks that were used).
     */
    public double getReadAheadHitRate() {
        long loaded = readAheadBlocksLoaded.get();
        if (loaded == 0) return 0.0;
        return Math.min(100.0, (double) readAheadBlocksAccessed.get() / loaded * 100.0);
    }

    /**
     * Sets the cache capacity (called when cache is initialized).
     */
    public void setCacheCapacity(long capacityBlocks) {
        cacheCapacityBlocks = capacityBlocks;
        logger.info("Cache capacity set: {} blocks ({} MB)", capacityBlocks,
            blocksToMB(capacityBlocks));
    }

    /**
     * Gets the cache capacity in blocks.
     */
    public long getCacheCapacity() {
        return cacheCapacityBlocks;
    }

    /**
     * Estimates the number of unique blocks accessed in the time window.
     *
     * @param windowSizeSeconds Size of the sliding window in seconds
     * @return Estimated number of unique blocks (always >= 0)
     */
    public long estimateCardinality(long windowSizeSeconds) {
        return hll.estimateCardinality(windowSizeSeconds);
    }

    /**
     * Converts blocks to megabytes.
     */
    public static long blocksToMB(long blocks) {
        return (blocks * BLOCK_SIZE_BYTES) / BYTES_PER_MB;
    }

    /**
     * Resets the HLL state. For testing only.
     */
    public void resetForTesting() {
        this.hll = new SlidingWindowHLL(DEFAULT_REGISTER_BITS);
        readAheadBlocksLoaded.set(0);
        readAheadBlocksAccessed.set(0);
        cacheCapacityBlocks = 0;
        logger.info("WorkingSetEstimator reset for testing");
    }
}
