/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
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
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_cache.PrefetchTracker;

/**
 * Measures PrefetchTracker ConcurrentHashMap dedup contention.
 * JMH threads submit work via prefetchTracker.execute(); worker threads
 * contend on putIfAbsent/remove with a 5ms simulated load.
 * Duplicates always hit the same fixed offset for maximum contention.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class DedupOverheadBenchmark {

    private static final int BLOCK_SIZE = 8192;
    private static final int BLOCKS_PER_REQUEST = 2;
    private static final long MAX_OFFSET = 100L * 1024 * 1024;
    private static final long SHARED_OFFSET = 0L;

    @Param({ "0", "50", "90" })
    private int duplicatePercent;

    @Param({ "true", "false" })
    private boolean useTracker;

    @Param({ "4", "16", "32" })
    private int workerThreads;

    private Path testFile;
    private PrefetchTracker prefetchTracker;
    private ExecutorService executor;

    @Setup(Level.Trial)
    public void setup() {
        testFile = Path.of("/tmp/dedup-benchmark-dummy.dat");
        executor = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r, "prefetch-worker");
            t.setDaemon(true);
            return t;
        });
        prefetchTracker = useTracker ? new PrefetchTracker(executor) : new NoOpPrefetchTracker(executor);
    }

    @Setup(Level.Iteration)
    public void resetIteration() {
        prefetchTracker.clear();
        prefetchTracker.resetStats();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Benchmark
    @Threads(32)
    public void dedup(Blackhole bh) throws InterruptedException {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        boolean isDuplicate = rng.nextInt(100) < duplicatePercent;
        long offset = isDuplicate ? SHARED_OFFSET : (rng.nextLong(MAX_OFFSET / BLOCK_SIZE)) * BLOCK_SIZE;

        CountDownLatch latch = new CountDownLatch(1);
        long off = offset;
        prefetchTracker.execute(() -> {
            try {
                for (int i = 0; i < BLOCKS_PER_REQUEST; i++) {
                    BlockCacheKey key = new FileBlockCacheKey(testFile, off + (long) i * BLOCK_SIZE);
                    if (prefetchTracker.putIfAbsent(key)) {
                        Thread.sleep(5);
                        prefetchTracker.remove(key);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });
        latch.await();
        bh.consume(offset);
    }

    static class NoOpPrefetchTracker extends PrefetchTracker {
        NoOpPrefetchTracker(java.util.concurrent.Executor executor) {
            super(executor);
        }

        @Override
        public boolean putIfAbsent(BlockCacheKey key) {
            return true;
        }

        @Override
        public void remove(BlockCacheKey key) {}
    }
}
