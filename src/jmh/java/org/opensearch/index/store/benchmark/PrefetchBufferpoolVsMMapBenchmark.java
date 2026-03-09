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
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.opensearch.index.store.read_ahead.Worker;
import org.opensearch.index.store.read_ahead.impl.QueuingWorker;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Compares sequential prefetch throughput: BufferPoolDirectory (block cache + decrypt)
 * vs MMapDirectory (mmap + madvise) on the same encrypted file.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
public class PrefetchBufferpoolVsMMapBenchmark {

    private static final int BLOCK_SIZE = 8192;
    private static final long FILE_SIZE = 10L * 1024 * 1024;
    private static final long PREFETCH_SIZE = BLOCK_SIZE;
    private static final int READS_PER_BLOCK = 1024;

    @Param({ "bufferpool", "mmap" })
    private String mode;

    @Param({ "true", "false" })
    private boolean prefetchEnabled;

    private Path tempDir;
    private Pool<RefCountedMemorySegment> pool;
    private ExecutorService executor;
    private PrefetchTracker prefetchTracker;
    private Worker readaheadWorker;
    private BufferPoolDirectory bufferPoolDir;
    private MMapDirectory mmapDir;
    private IndexInput sharedInput;
    private CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache;
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
        pool = new MemorySegmentPool(10L * 1024 * 1024, BLOCK_SIZE);
        executor = Executors.newFixedThreadPool(8, r -> {
            Thread t = new Thread(r, "prefetch-worker");
            t.setDaemon(true);
            return t;
        });
        prefetchTracker = new PrefetchTracker(executor);

        Cache<BlockCacheKey, org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine
            .newBuilder()
            .maximumSize(1_000)
            .removalListener(
                (
                    BlockCacheKey key,
                    org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment> value,
                    com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                    if (value != null) {
                        try { value.close(); } catch (Exception e) { /* ignore */ }
                    }
                }
            )
            .build();

        CryptoDirectIOBlockLoader loader = new CryptoDirectIOBlockLoader(pool, keyResolver, encMetaCache);
        blockCache = new CaffeineBlockCache<>(caffeineCache, loader, 1_000, prefetchTracker);

        readaheadWorker = new QueuingWorker(64, executor);
        bufferPoolDir = new BufferPoolDirectory(
            tempDir, FSLockFactory.getDefault(), provider, keyResolver,
            pool, blockCache, loader, readaheadWorker, encMetaCache
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
            public org.opensearch.telemetry.metrics.Counter createCounter(String n, String d, String u) { return null; }

            @Override
            public org.opensearch.telemetry.metrics.Counter createUpDownCounter(String n, String d, String u) { return null; }

            @Override
            public org.opensearch.telemetry.metrics.Histogram createHistogram(String n, String d, String u) { return null; }

            @Override
            public java.io.Closeable createGauge(String n, String d, String u, java.util.function.Supplier s,
                org.opensearch.telemetry.metrics.tags.Tags t) { return null; }

            @Override
            public java.io.Closeable createGauge(String n, String d, String u, java.util.function.Supplier s) { return null; }

            @Override
            public void close() {}
        });
    }

    @TearDown(Level.Iteration)
    public void logStats() {
        if (blockCache != null) {
            System.out.println("[STATS] " + blockCache.cacheStats());
            System.out.println("[STATS] " + blockCache.prefetchStats());
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        // 1. Stop the readahead worker from accepting/scheduling new tasks
        if (readaheadWorker != null) readaheadWorker.close();

        // 2. Clear prefetch tracker to prevent any pending dedup entries from
        //    triggering new loads
        if (prefetchTracker != null) prefetchTracker.clear();

        // 3. Shut down the executor and drain all in-flight async prefetch I/O.
        //    Both QueuingWorker.drainLoop and PrefetchTracker.execute run on this
        //    executor, so awaiting termination covers both paths.
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        // 4. Now safe to close shared input and directories
        if (sharedInput != null) sharedInput.close();
        if (bufferPoolDir != null) bufferPoolDir.close();
        if (mmapDir != null) mmapDir.close();
        // Temp files cleaned up on JVM exit; deleting here races with
        // async prefetch I/O that may still be in-flight.
        if (tempDir != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.walk(tempDir).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException e) { /* ignore */ }
                    });
                } catch (IOException e) { /* ignore */ }
            }));
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        long offset = 0;
        IndexInput threadInput;

        @Setup(Level.Trial)
        public void setupThread(PrefetchBufferpoolVsMMapBenchmark bench) {
            threadInput = bench.sharedInput.clone();
        }

        @TearDown(Level.Trial)
        public void tearDownThread() throws IOException {
            if (threadInput != null) threadInput.close();
        }
    }

    @Benchmark
    @Threads(1)
    public void read_1Thread(ThreadState ts, Blackhole bh) throws IOException {
        doRead(ts, bh);
    }

    @Benchmark
    @Threads(4)
    public void read_4Threads(ThreadState ts, Blackhole bh) throws IOException {
        doRead(ts, bh);
    }

    private void doRead(ThreadState ts, Blackhole bh) throws IOException {
        if (prefetchEnabled) {
            ts.threadInput.prefetch(ts.offset, PREFETCH_SIZE);
        }
        ts.threadInput.seek(ts.offset);
        for (int i = 0; i < READS_PER_BLOCK; i++) {
            bh.consume(ts.threadInput.readLong());
        }
        ts.offset += PREFETCH_SIZE;
        if (ts.offset + PREFETCH_SIZE > fileLength) {
            ts.offset = 0;
        }
    }
}
