/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.store.CryptoDirectoryPlugin;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheBuilder;
import org.opensearch.index.store.block_cache.PrefetchTracker;
import org.opensearch.index.store.hll.WorkingSetEstimator;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;
import org.opensearch.index.store.read_ahead.impl.ReadAheadSizingPolicy;
import org.opensearch.threadpool.ThreadPool;

/**
 * Builder for creating shared pool and cache resources with proper lifecycle management.
 * This class handles initialization of node-level shared resources used across all
 * encrypted directories.
 */
public final class PoolBuilder {

    private static final Logger LOGGER = LogManager.getLogger(PoolBuilder.class);

    /** 
    * Initial size for cache data structures (64K entries).
    */
    public static final int CACHE_INITIAL_SIZE = 65536;

    private PoolBuilder() {}

    /**
     * Container for shared pool resources with lifecycle management.
     * This class holds references to the shared memory segment pool, block cache,
     * telemetry thread, cache removal executor, and read-ahead executor service,
     * providing proper cleanup when closed.
     */
    public static class PoolResources implements Closeable {
        private final Pool<RefCountedByteBuffer> segmentPool;
        private final BlockCache<RefCountedByteBuffer> blockCache;
        private final long maxCacheBlocks;
        private final int readAheadQueueSize;
        private final Worker sharedReadaheadWorker;
        private final TelemetryThread telemetry;
        private final java.util.concurrent.ThreadPoolExecutor removalExecutor;
        private final ExecutorService readAheadExecutor;
        private final PrefetchTracker prefetchTracker;
        private final ExecutorService prefetchExecutor;

        PoolResources(
            Pool<RefCountedByteBuffer> segmentPool,
            BlockCache<RefCountedByteBuffer> blockCache,
            long maxCacheBlocks,
            int readAheadQueueSize,
            Worker sharedReadaheadWorker,
            TelemetryThread telemetry,
            java.util.concurrent.ThreadPoolExecutor removalExecutor,
            ExecutorService readAheadExecutor,
            PrefetchTracker prefetchTracker,
            ExecutorService prefetchExecutor
        ) {
            this.segmentPool = segmentPool;
            this.blockCache = blockCache;
            this.maxCacheBlocks = maxCacheBlocks;
            this.readAheadQueueSize = readAheadQueueSize;
            this.sharedReadaheadWorker = sharedReadaheadWorker;
            this.telemetry = telemetry;
            this.removalExecutor = removalExecutor;
            this.readAheadExecutor = readAheadExecutor;
            this.prefetchTracker = prefetchTracker;
            this.prefetchExecutor = prefetchExecutor;
        }

        /**
         * Returns the shared memory segment pool.
         *
         * @return the segment pool
         */
        public Pool<RefCountedByteBuffer> getSegmentPool() {
            return segmentPool;
        }

        /**
         * Returns the shared block cache.
         *
         * @return the block cache
         */
        public BlockCache<RefCountedByteBuffer> getBlockCache() {
            return blockCache;
        }

        /**
         * Returns the maximum number of blocks that can be cached.
         *
         * @return the maximum cache blocks
         */
        public long getMaxCacheBlocks() {
            return maxCacheBlocks;
        }

        /**
         * Returns the calculated read-ahead queue size.
         *
         * @return the read-ahead queue size
         */
        public int getReadAheadQueueSize() {
            return readAheadQueueSize;
        }

        /**
         * Returns the shared read-ahead worker.
         * This worker is shared across all shards/directories with a single queue and executor pool.
         *
         * @return the shared read-ahead worker
         */
        public Worker getSharedReadaheadWorker() {
            return sharedReadaheadWorker;
        }

        /**
         * Returns the shared read-ahead executor service.
         * This executor is shared across all per-shard workers for thread reuse while maintaining queue isolation.
         *
         * @return the read-ahead executor service
         */
        public ExecutorService getReadAheadExecutor() {
            return readAheadExecutor;
        }

        /**
         * Returns the shared prefetch tracker for deduplication and stats.
         *
         * @return the prefetch tracker
         */
        public PrefetchTracker getPrefetchTracker() {
            return prefetchTracker;
        }

        /**
         * Closes the shared pool resources, stops the telemetry thread, and shuts down executors.
         */
        @Override
        public void close() {
            if (telemetry != null) {
                telemetry.close();
            }
            if (segmentPool instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) segmentPool).close();
                } catch (Exception e) {
                    LOGGER.warn("Error closing pool", e);
                }
            }
            if (sharedReadaheadWorker != null) {
                try {
                    sharedReadaheadWorker.close();
                } catch (Exception e) {
                    LOGGER.warn("Error closing shared readahead worker", e);
                }
            }
            if (removalExecutor != null) {
                removalExecutor.shutdown();
                try {
                    if (!removalExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        removalExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    removalExecutor.shutdownNow();
                }
            }
            if (readAheadExecutor != null) {
                readAheadExecutor.shutdown();
                try {
                    if (!readAheadExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        readAheadExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    readAheadExecutor.shutdownNow();
                }
            }
            if (prefetchExecutor != null) {
                prefetchExecutor.shutdown();
                try {
                    if (!prefetchExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        prefetchExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    prefetchExecutor.shutdownNow();
                }
            }
        }
    }

    /**
     * Autocloseable telemetry thread for periodic pool statistics logging.
     */
    private static class TelemetryThread implements Closeable {
        private final Thread thread;
        private final Pool<RefCountedByteBuffer> pool;
        private final BlockCache<RefCountedByteBuffer> blockCache;

        TelemetryThread(Pool<RefCountedByteBuffer> pool, BlockCache<RefCountedByteBuffer> blockCache) {
            this.pool = pool;
            this.blockCache = blockCache;
            this.thread = new Thread(this::run);
            this.thread.setDaemon(true);
            this.thread.setName("DirectIOBufferPoolStatsLogger");
            this.thread.start();
        }

        private void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(Duration.ofMinutes(5));
                    publishStats();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    LOGGER.warn("Panic in telemetry buffer stats logger", t);
                }
            }
        }

        private void publishStats() {
            try {
                pool.recordStats();
                blockCache.recordStats();

            } catch (Exception e) {
                LOGGER.warn("Failed to log cache/pool stats", e);
            }
        }

        @Override
        public void close() {
            thread.interrupt();
            try {
                thread.join(5000); // Wait up to 5 seconds for graceful shutdown
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Initialized the MemorySegmentPool and BlockCache.
     *
     * @param settings   the node settings for configuration
     * @param threadPool
     * @return SharedPoolResources containing the initialized pool and cache
     */
    public static PoolResources build(Settings settings, ThreadPool threadPool) {
        long reservedPoolSizeInBytes = PoolSizeCalculator.calculatePoolSize(settings);

        reservedPoolSizeInBytes = (reservedPoolSizeInBytes / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        long maxBlocks = reservedPoolSizeInBytes / CACHE_BLOCK_SIZE;

        // Calculate off-heap memory for tiered cache ratio and warmup
        long maxHeap = Runtime.getRuntime().maxMemory();
        long totalPhysical = org.opensearch.monitor.os.OsProbe.getInstance().getTotalPhysicalMemorySize();
        if (totalPhysical <= 0) {
            throw new IllegalStateException("Failed to calculate instance's physical memory, bailing out...: " + totalPhysical);
        }
        long offHeap = Math.max(0, totalPhysical - maxHeap);

        double cacheToPoolRatio = PoolSizeCalculator.calculateCacheToPoolRatio(offHeap, settings);
        double warmupPercentage = PoolSizeCalculator.calculateWarmupPercentage(offHeap, settings);

        Pool<RefCountedByteBuffer> segmentPool = new MemorySegmentPool(reservedPoolSizeInBytes, CACHE_BLOCK_SIZE);
        LOGGER
            .info(
                "Creating shared pool with sizeBytes={}, segmentSize={}, totalSegments={}",
                reservedPoolSizeInBytes,
                CACHE_BLOCK_SIZE,
                maxBlocks
            );

        // Calculate cache size: cache = pool * ratio
        long maxCacheBlocks = (long) (maxBlocks * cacheToPoolRatio);
        long warmupBlocks = (long) (maxCacheBlocks * warmupPercentage);
        segmentPool.warmUp(warmupBlocks);
        LOGGER.info("Warmed up {} blocks ({}% of {} cache blocks)", warmupBlocks, warmupPercentage * 100, maxCacheBlocks);

        // Calculate read-ahead queue size based on cache capacity
        // Pool constraint not needed since cache evictions automatically release pool memory
        int readAheadQueueSize = ReadAheadSizingPolicy.calculateQueueSize(maxCacheBlocks);
        LOGGER.info("Calculated read-ahead queue size={} (cache={} blocks)", readAheadQueueSize, maxCacheBlocks);

        int prefetchThreads = com.amazonaws.juno.settings.JunoSettings.STORAGE_PREFETCH_THREAD_COUNT_SETTING.get();
        if (prefetchThreads == -1) {
            // his accounts for approx processors x 1.5 search threads and processors x 2 index_searcher threads
            prefetchThreads = OpenSearchExecutors.allocatedProcessors(settings) * 4;
        }
        int prefetchQueueSize = com.amazonaws.juno.settings.JunoSettings.STORAGE_PREFETCH_QUEUE_SIZE_SETTING.get();
        if (prefetchQueueSize == -1) {
            prefetchQueueSize = prefetchThreads * 1000;
        }
        LOGGER.info("Prefetch ForkJoinPool: threads={}, maxInflight={}, allocatedProcessors={}", prefetchThreads, prefetchQueueSize, OpenSearchExecutors.allocatedProcessors(settings));
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
            java.util.concurrent.ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            t.setName("prefetch-worker-" + t.getPoolIndex());
            t.setDaemon(true);
            return t;
        };
        // ForkJoin is much faster queue based executors: https://github.com/opensearch-project/opensearch-storage-encryption/pull/149#issuecomment-4151066212
        ExecutorService prefetchExecutor = new ForkJoinPool(prefetchThreads, factory, null, false);
        PrefetchTracker prefetchTracker = new PrefetchTracker(prefetchExecutor, prefetchQueueSize);

        // Initialize shared cache with removal listener and get its executor
        BlockCacheBuilder.CacheWithExecutor<RefCountedByteBuffer, RefCountedByteBuffer> cacheWithExecutor = BlockCacheBuilder
            .build(CACHE_INITIAL_SIZE, maxCacheBlocks, prefetchTracker);
        BlockCache<RefCountedByteBuffer> blockCache = cacheWithExecutor.getCache();
        java.util.concurrent.ThreadPoolExecutor removalExecutor = cacheWithExecutor.getExecutor();
        LOGGER.info("Creating shared block cache with blocks={}", maxCacheBlocks);

        // Wire cache size into pool's GC debt monitor
        ((MemorySegmentPool) segmentPool).setCacheEntriesSupplier(blockCache::getCacheSize);

         // Set cache capacity in WorkingSetEstimator for percentage calculations
        WorkingSetEstimator.getInstance().setCacheCapacity(maxCacheBlocks);

        // Calculate worker threads using principled drain-time approach
        int threads = ReadAheadSizingPolicy.calculateWorkerThreads(readAheadQueueSize);

        AtomicInteger threadId = new AtomicInteger();
        // TODO: why not use OS's threadpool
        ExecutorService readAheadExecutor = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "readahead-worker-" + threadId.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        LOGGER.info("Creating shared read-ahead executor with threads={} (queue={})", threads, readAheadQueueSize);

        // Create shared read-ahead worker (node-wide, single queue)
        // Executor thread pool naturally limits concurrency - no need for separate maxRunners cap
        // BlockCache is passed per-request to support directory-specific loaders
        Worker sharedReadaheadWorker = new QueuingWorker(readAheadQueueSize, readAheadExecutor);
        LOGGER.info("Created shared read-ahead worker: queueSize={} executorThreads={}", readAheadQueueSize, threads);

        // Start telemetry
        TelemetryThread telemetry = new TelemetryThread(segmentPool, blockCache);

        return new PoolResources(
            segmentPool,
            blockCache,
            maxCacheBlocks,
            readAheadQueueSize,
            sharedReadaheadWorker,
            telemetry,
            removalExecutor,
            readAheadExecutor,
            cacheWithExecutor.getPrefetchTracker(),
            prefetchExecutor
        );
    }
}
