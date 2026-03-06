/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.lucene.store.IndexInput;
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
 * Benchmark for mixed workload (prefetch + regular reads).
 * Simulates realistic cluster with search + indexing.
 */
@State(Scope.Benchmark)
public class MixedWorkloadBenchmark extends PrefetchBenchmarkBase {

    @Param({ "0.2", "0.5", "0.8" })
    private double prefetchRatio;

    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        super.setup();
    }

    @Setup(Level.Iteration)
    public void resetIterationMetrics() {
        resetMetrics();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(4)
    public void mixedWorkload_4Threads(Blackhole bh) throws IOException {
        mixedOperation(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(8)
    public void mixedWorkload_8Threads(Blackhole bh) throws IOException {
        mixedOperation(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(16)
    public void mixedWorkload_16Threads(Blackhole bh) throws IOException {
        mixedOperation(bh);
    }

    private void mixedOperation(Blackhole bh) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        if (random.nextDouble() < prefetchRatio) {
            // Prefetch operation
            long offset;
            if (random.nextDouble() < 0.8) {
                // 80% sequential
                long threadId = Thread.currentThread().getId();
                offset = (threadId * 1024 * 1024) % (FILE_SIZE - 64 * 1024);
            } else {
                // 20% random
                offset = random.nextLong(FILE_SIZE - 64 * 1024);
            }
            offset = getBlockAlignedOffset(offset);

            long blockCount = 8;
            totalPrefetchRequests.addAndGet(blockCount);
            blockCache.loadMissingBlocks(testFile, offset, blockCount);
            bh.consume(prefetchTracker.getBlocksLoaded());
        } else {
            // Regular read operation
            try (IndexInput input = directory.openInput("test.dat", org.apache.lucene.store.IOContext.DEFAULT)) {
                long offset = random.nextLong(FILE_SIZE - 1024);
                input.seek(offset);

                // Read some data
                byte b = input.readByte();
                int i = input.readInt();
                long l = input.readLong();

                bh.consume(b);
                bh.consume(i);
                bh.consume(l);
            }
        }
    }
}
