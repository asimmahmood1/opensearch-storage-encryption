/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
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
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BufferPoolDirectory;
import org.opensearch.index.store.bufferpoolfs.CachedMemorySegmentIndexInput;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * Compares sequential prefetch throughput: BufferPoolDirectory (block cache + decrypt)
 * vs MMapDirectory (mmap + madvise) on the same encrypted file.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class PrefetchBufferpoolVsMMapBenchmark {

    private static final int BLOCK_SIZE = 8192;
    private static final long FILE_SIZE = 100L * 1024 * 1024; // 100MB
    private static final int PREFETCH_AHEAD = 1; // prefetch 8 blocks ahead
    private static final int READS_PER_BLOCK = BLOCK_SIZE / 8; //longs
    private static final long TOTAL_MEMORY_POOL = 256L * 1024 * 1024; // 256MB
    private static final int MAX_BLOCKS_CACHE = 15_000;

    @Param({ "bufferpool"/*, "mmap"*/ })
    private String mode;

    @Param({ "true", "false" })
    private boolean prefetchEnabled;

    @Param({ "true", "false" })
    private boolean cacheWarm;

    private Path tempDir;
    private Pool<RefCountedMemorySegment> pool;
    private ExecutorService executor;
    private PrefetchTracker prefetchTracker;
    private Worker readaheadWorker;
    private BufferPoolDirectory bufferPoolDir;
    private MMapDirectory mmapDir;
    private IndexInput sharedInput;
    private CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache;
    private CacheStats cacheStatsBaseline;
    private final AtomicInteger totalPasses = new AtomicInteger();
    private long fileLength;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        initMetrics();

        tempDir = Files.createTempDirectory("prefetch-comparison");
        Provider provider = Security.getProvider("SunJCE");
        EncryptionMetadataCache encMetaCache = new EncryptionMetadataCache();

        byte[] rawKey = new byte[32];
        new Random(42).nextBytes(rawKey);
        SecretKeySpec aesKey = new SecretKeySpec(rawKey, "AES");
        KeyResolver keyResolver = () -> aesKey;

        // Setup BufferPoolDirectory components
        pool = new MemorySegmentPool(TOTAL_MEMORY_POOL, BLOCK_SIZE);
        executor = Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "prefetch-worker");
            t.setDaemon(true);
            return t;
        });
        prefetchTracker = new PrefetchTracker(executor);

        Cache<BlockCacheKey, org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(MAX_BLOCKS_CACHE) // larger than needed, to compare mmap
            .recordStats()
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

        CryptoDirectIOBlockLoader loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encMetaCache);
        blockCache = new CaffeineBlockCache<>(caffeineCache, loader, MAX_BLOCKS_CACHE, prefetchTracker);

        readaheadWorker = new QueuingWorker(64, executor);
        bufferPoolDir = new BufferPoolDirectory(
            tempDir,
            FSLockFactory.getDefault(),
            provider,
            keyResolver,
            pool,
            blockCache,
            loader,
            readaheadWorker,
            encMetaCache
        );

        // Write encrypted file via BufferPoolDirectory
        try (IndexOutput out = bufferPoolDir.createOutput("test.dat", IOContext.DEFAULT)) {
            Random rng = new Random(42);
            byte[] buf = new byte[BLOCK_SIZE];
            long written = 0;
            while (written < FILE_SIZE) {
                rng.nextBytes(buf);
                out.writeBytes(buf, 0, buf.length);
                written += buf.length;
            }
        }

        // Open a shared IndexInput for cloning in per-thread setup.
        // For mmap: IOContext.DEFAULT gives a shared arena (not confined), allowing cross-thread clone.
        // For bufferpool: CachedMemorySegmentIndexInput supports cross-thread clone natively.
        if ("bufferpool".equals(mode)) {
            sharedInput = bufferPoolDir.openInput("test.dat", IOContext.DEFAULT);
        } else {
            mmapDir = new MMapDirectory(tempDir);
            sharedInput = mmapDir.openInput("test.dat", IOContext.DEFAULT);
        }
        fileLength = sharedInput.length();
    }

    private void initMetrics() {
        CryptoMetricsService.initialize(new org.opensearch.telemetry.metrics.MetricsRegistry() {
            @Override
            public org.opensearch.telemetry.metrics.Counter createCounter(String n, String d, String u) {
                return null;
            }

            @Override
            public org.opensearch.telemetry.metrics.Counter createUpDownCounter(String n, String d, String u) {
                return null;
            }

            @Override
            public org.opensearch.telemetry.metrics.Histogram createHistogram(String n, String d, String u) {
                return null;
            }

            @Override
            public java.io.Closeable createGauge(
                String n,
                String d,
                String u,
                java.util.function.Supplier s,
                org.opensearch.telemetry.metrics.tags.Tags t
            ) {
                return null;
            }

            @Override
            public java.io.Closeable createGauge(String n, String d, String u, java.util.function.Supplier s) {
                return null;
            }

            @Override
            public void close() {}
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        // 1. Stop the readahead worker from accepting/scheduling new tasks
        if (readaheadWorker != null)
            readaheadWorker.close();

        // 2. Clear prefetch tracker to prevent any pending dedup entries from
        // triggering new loads
        if (prefetchTracker != null)
            prefetchTracker.clear();

        // 3. Shut down the executor and drain all in-flight async prefetch I/O.
        // Both QueuingWorker.drainLoop and PrefetchTracker.execute run on this
        // executor, so awaiting termination covers both paths.
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        // 4. Now safe to close shared input and directories
        if (sharedInput != null)
            sharedInput.close();
        if (bufferPoolDir != null)
            bufferPoolDir.close();
        if (mmapDir != null)
            mmapDir.close();
        // Temp files cleaned up on JVM exit; deleting here races with
        // async prefetch I/O that may still be in-flight.
        if (tempDir != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.walk(tempDir).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) { /* ignore */ }
                    });
                } catch (IOException e) { /* ignore */ }
            }));
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        long offset = 0;
        int passCount = 0;
        IndexInput threadInput;

        @Setup(Level.Trial)
        public void setupThread(PrefetchBufferpoolVsMMapBenchmark bench) {
            threadInput = bench.sharedInput.clone();
        }

        @TearDown(Level.Trial)
        public void tearDownThread() throws IOException {
            if (threadInput != null)
                threadInput.close();
        }
    }



    @TearDown(Level.Iteration)
    public void logStats() {
        System.out.println();
        System.out.println("[STATS] passes=" + totalPasses.getAndSet(0));
        if (blockCache != null) {
            CacheStats delta = blockCache.getCache().stats();
            if (cacheStatsBaseline != null) {
                delta = blockCache.getCache().stats().minus(cacheStatsBaseline);

            }
            System.out.println("[STATS] CaffineCache[size =" + blockCache.getCache().estimatedSize() + ",hits=" + delta.hitCount() + ", misses=" + delta.missCount()
                    + ", hitRate=" + String.format("%.2f%%", delta.hitRate() * 100)
                    + ", loads=" + delta.loadCount()
                    + ", evictions=" + delta.evictionCount()
                    + ", avgLoadTime=" + String.format("%.2fms", delta.averageLoadPenalty() / 1_000_000.0) + "]");
            System.out.println("[STATS] " + blockCache.prefetchStats());
            System.out.println("[STATS] " + pool.poolStats());

            //reset stats
            prefetchTracker.resetStats();
            cacheStatsBaseline = blockCache.getCache().stats();

        }
        if (sharedInput instanceof CachedMemorySegmentIndexInput cmsi) {
            System.out.println("[STATS] " + cmsi.getBlockSlotTinyCache().stats());
            cmsi.getBlockSlotTinyCache().clear();
            cmsi.getBlockSlotTinyCache().resetStats();
        }
    }


    @Benchmark
    @Threads(4)
    public void read_4Threads(ThreadState ts, Blackhole bh) throws IOException, InterruptedException {
        doRead(ts, bh);
    }

    private void doRead(ThreadState ts, Blackhole bh) throws IOException, InterruptedException {




        // Read current block
        ts.threadInput.seek(ts.offset);
        for (int i = 0; i < READS_PER_BLOCK; i++) {
            bh.consume(ts.threadInput.readLong());
        }

        // Advance to next block
        ts.offset += BLOCK_SIZE;
        if (ts.offset + BLOCK_SIZE > fileLength) {
            ts.offset = 0;
            ts.passCount++;
            totalPasses.incrementAndGet();
            if (!cacheWarm) {
                blockCache.clear();
            }
        }
        // Prefetch N blocks ahead — gives the async threadpool enough
        // lead time to load before the read catches up
        if (prefetchEnabled) {
            long prefetchOffset = ts.offset + (long) PREFETCH_AHEAD * BLOCK_SIZE;
            if (prefetchOffset < fileLength) {
                ts.threadInput.prefetch(prefetchOffset, BLOCK_SIZE);
            }
        }

    }
}
