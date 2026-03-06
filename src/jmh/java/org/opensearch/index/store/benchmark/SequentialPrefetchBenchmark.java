/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;

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
 * Benchmark for sequential prefetch access pattern.
 * Simulates query execution with sequential file reads.
 */
@State(Scope.Benchmark)
public class SequentialPrefetchBenchmark extends PrefetchBenchmarkBase {

    @Param({ "1000", "10000" })
    private int cacheBlocks;

    private long currentOffset = 0;

    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        super.setup();
        setupBlockCache(cacheBlocks);
    }

    @Setup(Level.Iteration)
    public void resetOffset() {
        currentOffset = 0;
        resetMetrics();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(1)
    public void sequentialPrefetch_1Thread(Blackhole bh) throws IOException {
        prefetchSequential(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(4)
    public void sequentialPrefetch_4Threads(Blackhole bh) throws IOException {
        prefetchSequential(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(8)
    public void sequentialPrefetch_8Threads(Blackhole bh) throws IOException {
        prefetchSequential(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(16)
    public void sequentialPrefetch_16Threads(Blackhole bh) throws IOException {
        prefetchSequential(bh);
    }

    private void prefetchSequential(Blackhole bh) throws IOException {
        // Each thread gets its own sequential range
        long threadId = Thread.currentThread().getId();
        long offset = (threadId * 1024 * 1024) % (FILE_SIZE - 64 * 1024);
        offset = getBlockAlignedOffset(offset);

        // Prefetch 8 blocks (64KB)
        long blockCount = 8;
        totalPrefetchRequests.addAndGet(blockCount);

        blockCache.loadMissingBlocks(testFile, offset, blockCount);
        bh.consume(prefetchTracker.getBlocksLoaded());
    }
}
