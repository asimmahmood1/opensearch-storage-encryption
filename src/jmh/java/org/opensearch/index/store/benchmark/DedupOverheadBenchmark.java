/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.PrefetchTracker;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Benchmark measuring PrefetchTracker ConcurrentHashMap dedup overhead.
 *
 * Compares throughput with and without dedup checks at varying duplicate rates.
 * Load calls are mocked with a 5ms sleep to simulate I/O latency.
 *
 * Parameters:
 * - duplicatePercent: 0 (pure overhead), 50, 90 (dedup benefit)
 * - useTracker: true (real ConcurrentHashMap dedup) vs false (no-op, always loads)
 * - Thread counts: 4, 16, 32
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class DedupOverheadBenchmark {

    private static final int BLOCK_SIZE = 8192;
    private static final int BLOCKS_PER_REQUEST = 8;
    private static final long MAX_OFFSET = 100L * 1024 * 1024;
    private static final int SHARED_OFFSET_COUNT = 100;

    @Param({ "0", "50", "90" })
    private int duplicatePercent;

    @Param({ "true", "false" })
    private boolean useTracker;

    private Path tempDir;
    private Path testFile;
    private Pool<RefCountedMemorySegment> pool;
    private ExecutorService executor;
    private PrefetchTracker prefetchTracker;
    private CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache;
    private long[] sharedOffsets;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("dedup-benchmark");
        testFile = tempDir.resolve("test.dat");
        Files.createFile(testFile);

        pool = new MemorySegmentPool(50L * 1024 * 1024, BLOCK_SIZE);

        executor = Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "prefetch-worker");
            t.setDaemon(true);
            return t;
        });

        prefetchTracker = useTracker ? new PrefetchTracker(executor) : new NoOpPrefetchTracker(executor);

        Cache<BlockCacheKey, org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1_000)
            .removalListener(
                (
                    BlockCacheKey key,
                    org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment> value,
                    com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                    if (value != null) {
                        try {
                            value.close();
                        } catch (Exception e) { /* ignore */ }
                    }
                }
            )
            .build();

        blockCache = new CaffeineBlockCache<>(caffeineCache, new SleepingBlockLoader(pool), 1_000, prefetchTracker);

        sharedOffsets = new long[SHARED_OFFSET_COUNT];
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for (int i = 0; i < SHARED_OFFSET_COUNT; i++) {
            sharedOffsets[i] = (rng.nextLong(MAX_OFFSET / BLOCK_SIZE)) * BLOCK_SIZE;
        }
    }

    @Setup(Level.Iteration)
    public void resetIteration() {
        blockCache.clear();
        prefetchTracker.clear();
        prefetchTracker.resetStats();
    }

    @TearDown(Level.Iteration)
    public void reportIteration() {
        System.out.println(prefetchTracker.stats());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (pool != null) {
            pool.close();
        }
        if (tempDir != null) {
            Files.walk(tempDir).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) { /* ignore */ }
            });
        }
    }

    @Benchmark
    @Threads(4)
    public void dedup_4Threads(Blackhole bh) throws IOException {
        runPrefetch(bh);
    }

    @Benchmark
    @Threads(16)
    public void dedup_16Threads(Blackhole bh) throws IOException {
        runPrefetch(bh);
    }

    @Benchmark
    @Threads(32)
    public void dedup_32Threads(Blackhole bh) throws IOException {
        runPrefetch(bh);
    }

    private void runPrefetch(Blackhole bh) throws IOException {
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        long offset;
        if (rng.nextInt(100) < duplicatePercent) {
            offset = sharedOffsets[rng.nextInt(SHARED_OFFSET_COUNT)];
        } else {
            offset = (rng.nextLong(MAX_OFFSET / BLOCK_SIZE)) * BLOCK_SIZE;
        }

        blockCache.loadMissingBlocks(testFile, offset, BLOCKS_PER_REQUEST);
        bh.consume(prefetchTracker.getBlocksLoaded());
    }

    /**
     * PrefetchTracker that bypasses ConcurrentHashMap - always allows load, never deduplicates.
     */
    static class NoOpPrefetchTracker extends PrefetchTracker {
        NoOpPrefetchTracker(java.util.concurrent.Executor executor) {
            super(executor);
        }

        @Override
        public boolean putIfAbsent(BlockCacheKey key) {
            return true;
        }

        @Override
        public void remove(BlockCacheKey key) {
            // no-op
        }
    }

    /**
     * BlockLoader that simulates 5ms I/O latency, then returns pool-allocated segments.
     */
    static class SleepingBlockLoader implements BlockLoader<RefCountedMemorySegment> {
        private final Pool<RefCountedMemorySegment> pool;

        SleepingBlockLoader(Pool<RefCountedMemorySegment> pool) {
            this.pool = pool;
        }

        @Override
        public RefCountedMemorySegment[] load(Path filePath, long startOffset, long blockCount, long poolTimeoutMs) throws Exception {
            Thread.sleep(5);
            RefCountedMemorySegment[] result = new RefCountedMemorySegment[(int) blockCount];
            for (int i = 0; i < blockCount; i++) {
                result[i] = pool.tryAcquire(poolTimeoutMs, TimeUnit.MILLISECONDS);
            }
            return result;
        }
    }
}
