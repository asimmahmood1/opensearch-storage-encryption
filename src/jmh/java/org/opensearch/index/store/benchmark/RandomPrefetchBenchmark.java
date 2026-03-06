/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for random prefetch access pattern.
 * Simulates scattered document access - tests cache pressure.
 */
@State(Scope.Benchmark)
public class RandomPrefetchBenchmark extends PrefetchBenchmarkBase {

    @Param({ "1000", "10000" })
    private int cacheBlocks;

    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        super.setup();
        setupBlockCache(cacheBlocks);
    }

    @Setup(Level.Iteration)
    public void resetIterationMetrics() {
        resetMetrics();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(1)
    public void randomPrefetch_1Thread(Blackhole bh) throws IOException {
        prefetchRandom(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(4)
    public void randomPrefetch_4Threads(Blackhole bh) throws IOException {
        prefetchRandom(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(8)
    public void randomPrefetch_8Threads(Blackhole bh) throws IOException {
        prefetchRandom(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(16)
    public void randomPrefetch_16Threads(Blackhole bh) throws IOException {
        prefetchRandom(bh);
    }

    private void prefetchRandom(Blackhole bh) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // Random offset
        long offset = random.nextLong(FILE_SIZE - 64 * 1024);
        offset = getBlockAlignedOffset(offset);

        // Prefetch 8 blocks (64KB)
        long blockCount = 8;
        totalPrefetchRequests.addAndGet(blockCount);

        blockCache.loadMissingBlocks(testFile, offset, blockCount);
        bh.consume(prefetchTracker.getBlocksLoaded());
    }
}
