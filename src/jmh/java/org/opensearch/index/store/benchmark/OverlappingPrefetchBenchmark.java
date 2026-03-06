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
 * Benchmark for overlapping prefetch access pattern.
 * Simulates multiple queries hitting same segments - tests deduplication.
 */
@State(Scope.Benchmark)
public class OverlappingPrefetchBenchmark extends PrefetchBenchmarkBase {
    
    @Param({"50", "75", "100"})
    private int overlapPercent;
    
    private static final long[] SHARED_OFFSETS = new long[100];
    
    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        super.setup();
        
        // Pre-generate shared offsets for overlap
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < SHARED_OFFSETS.length; i++) {
            long offset = random.nextLong(FILE_SIZE - 64 * 1024);
            SHARED_OFFSETS[i] = getBlockAlignedOffset(offset);
        }
    }
    
    @Setup(Level.Iteration)
    public void resetIterationMetrics() {
        resetMetrics();
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(4)
    public void overlappingPrefetch_4Threads(Blackhole bh) throws IOException {
        prefetchOverlapping(bh);
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(8)
    public void overlappingPrefetch_8Threads(Blackhole bh) throws IOException {
        prefetchOverlapping(bh);
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(16)
    public void overlappingPrefetch_16Threads(Blackhole bh) throws IOException {
        prefetchOverlapping(bh);
    }
    
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(32)
    public void overlappingPrefetch_32Threads(Blackhole bh) throws IOException {
        prefetchOverlapping(bh);
    }
    
    private void prefetchOverlapping(Blackhole bh) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        
        long offset;
        if (random.nextInt(100) < overlapPercent) {
            // Use shared offset (overlap)
            offset = SHARED_OFFSETS[random.nextInt(SHARED_OFFSETS.length)];
        } else {
            // Use unique offset
            offset = random.nextLong(FILE_SIZE - 64 * 1024);
            offset = getBlockAlignedOffset(offset);
        }
        
        // Prefetch 8 blocks (64KB)
        long blockCount = 8;
        totalPrefetchRequests.addAndGet(blockCount);
        
        long loaded = blockCache.loadMissingBlocks(testFile, offset, blockCount);
        bh.consume(loaded);
    }
}
