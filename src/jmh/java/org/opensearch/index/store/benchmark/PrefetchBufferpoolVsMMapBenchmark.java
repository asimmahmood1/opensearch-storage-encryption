/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.benchmark;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.DataAccessHint;
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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
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
    private static final int PREFETCH_AHEAD = 16; // prefetch 4 non-contiguous blocks
    private static final int STRIDE_BLOCKS = 16; // blocks between each prefetched block
    private static final int READS_PER_BLOCK = 64 / 8; // 8 longs = 64 bytes per block
    private static final long TOTAL_MEMORY_POOL = 256L * 1024 * 1024; // 256MB
    private static final int MAX_BLOCKS_CACHE = 15_000;

    @Param({ "bufferpool","mmap" })
    private String mode;

    /**
     * Prefetch mode:
     * - "off": no prefetch
     * - "async": prefetch via executor (original path)
     * - "inline_check": check cache inline, skip executor if all cached
     */
    @Param({ "async", /*"inline_check", "inline_load",*/ "off" })
    private String prefetchMode;

    /**
     * Executor type for async prefetch:
     * - "opensearch": OpenSearchThreadPoolExecutor (ThreadContext wrapping overhead)
     * - "jdk": plain Executors.newFixedThreadPool (no wrapping)
     */
    @Param({ "opensearch" /*, "jdk" */})
    private String executorType;

    @Param({ "true"  , "false" })
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
    private final LongAdder totalPasses = new LongAdder();
    private final LongAdder prefetchTimeNs = new LongAdder();
    private final LongAdder prefetchCalls = new LongAdder();
    private final AtomicInteger threadIndex = new AtomicInteger();
    private long fileLength;
    private int threadCount;
    private Path testFilePath;
    private java.lang.reflect.Field mmapPrefetchField; // Lucene's backoff counter field

    // posix_fadvise via Panama FFI for dropping page cache
    private static final int POSIX_FADV_DONTNEED = 4;
    private static final MethodHandle OPEN_MH;
    private static final MethodHandle CLOSE_MH;
    private static final MethodHandle FADVISE_MH;

    static {
        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = linker.defaultLookup();
        OPEN_MH = linker.downcallHandle(
            lookup.find("open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
        );
        CLOSE_MH = linker.downcallHandle(
            lookup.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
        );
        FADVISE_MH = linker.downcallHandle(
            lookup.find("posix_fadvise").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT)
        );
    }

    private static void dropPageCache(Path file) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathStr = arena.allocateFrom(file.toAbsolutePath().toString());
            int fd = (int) OPEN_MH.invokeExact(pathStr, 0 /* O_RDONLY */);
            if (fd >= 0) {
                try {
                    int rc = (int) FADVISE_MH.invokeExact(fd, 0L, 0L, POSIX_FADV_DONTNEED);
                } finally {
                    int rc = (int) CLOSE_MH.invokeExact(fd);
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to drop page cache", e);
        }
    }

    @Setup(Level.Trial)
    public void setup(org.openjdk.jmh.infra.BenchmarkParams params) throws Exception {
        threadCount = params.getThreads();
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
        if ("jdk".equals(executorType)) {
            executor = Executors.newFixedThreadPool(32);
        } else {
            executor = OpenSearchExecutors.newFixed(
                "prefetch-worker",
                32,
                10000,
                OpenSearchExecutors.daemonThreadFactory("prefetch-worker"),
                new ThreadContext(Settings.EMPTY)
            );
        }
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
            blockCache.clear();

            sharedInput = bufferPoolDir.openInput("test.dat", IOContext.DEFAULT);
        } else {
            mmapDir = new MMapDirectory(tempDir);
            sharedInput = mmapDir.openInput("test.dat", IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
            // Resolve Lucene's prefetch backoff field for resetting in benchmark
            try {
                Class<?> clazz = sharedInput.getClass();
                while (clazz != null) {
                    try {
                        mmapPrefetchField = clazz.getDeclaredField("consecutivePrefetchHitCount");
                        mmapPrefetchField.setAccessible(true);
                        break;
                    } catch (NoSuchFieldException e) {
                        clazz = clazz.getSuperclass();
                    }
                }
            } catch (Exception e) {
                System.err.println("[WARN] Could not resolve prefetch backoff field: " + e);
            }
        }
        fileLength = sharedInput.length();
        testFilePath = tempDir.resolve("test.dat").toAbsolutePath().normalize();
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
        long rangeStart;
        long rangeEnd;
        int passCount = 0;
        IndexInput threadInput;
        Path filePath; // cached normalized path for inline cache check

        @Setup(Level.Trial)
        public void setupThread(PrefetchBufferpoolVsMMapBenchmark bench) {
            threadInput = bench.sharedInput.clone();
            filePath = bench.tempDir.resolve("test.dat").toAbsolutePath().normalize();
            int idx = bench.threadIndex.getAndIncrement();
            int threads = bench.threadCount;
            long totalBlocks = bench.fileLength / BLOCK_SIZE;
            long blocksPerPartition = totalBlocks / threads;
            rangeStart = (idx % threads) * blocksPerPartition * BLOCK_SIZE;
            rangeEnd = rangeStart + blocksPerPartition * BLOCK_SIZE;
            offset = rangeStart;
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
        System.out.println("[STATS] passes=" + totalPasses.sumThenReset());
        long calls = prefetchCalls.sumThenReset();
        long timeNs = prefetchTimeNs.sumThenReset();
        if (calls > 0) {
            System.out.println("[STATS] prefetch[calls=" + calls + ", totalMs=" + String.format("%.2f", timeNs / 1_000_000.0)
                + ", avgUs=" + String.format("%.2f", timeNs / 1_000.0 / calls) + "]");
        }
        if (blockCache != null) {
            System.out.println("[STATS] " + blockCache.prefetchStats());
            CacheStats delta = blockCache.getCache().stats();
            if (cacheStatsBaseline != null) {
                delta = blockCache.getCache().stats().minus(cacheStatsBaseline);

            }
            System.out.println("[STATS] CaffineCache[size =" + blockCache.getCache().estimatedSize() + ",hits=" + delta.hitCount() + ", misses=" + delta.missCount()
                    + ", hitRate=" + String.format("%.2f%%", delta.hitRate() * 100)
                    + ", loads=" + delta.loadCount()
                    + ", evictions=" + delta.evictionCount()
                    + ", avgLoadTime=" + String.format("%.2fms", delta.averageLoadPenalty() / 1_000_000.0) + "]");
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
/*
    @Benchmark
    @Threads(1)
    public void read_1Threads(ThreadState ts, Blackhole bh) throws IOException, InterruptedException {
        doRead(ts, bh);
    }
*/
    @Benchmark
    @Threads(4)
    public void read_4Threads(ThreadState ts, Blackhole bh) throws IOException, InterruptedException {
        doRead(ts, bh);
    }

    private void doRead(ThreadState ts, Blackhole bh) throws IOException, InterruptedException {
        long strideBytes = (long) STRIDE_BLOCKS * BLOCK_SIZE;

        if (!cacheWarm) {
            if ("mmap".equals(mode)) {
                dropPageCache(testFilePath);
            } else {
                for (int i = 0; i < PREFETCH_AHEAD; i++) {
                    long prefetchOffset = ts.offset + i * strideBytes;
                    if (prefetchOffset + BLOCK_SIZE <= ts.rangeEnd) {
                        long t0 = System.nanoTime();
                        FileBlockCacheKey key = new FileBlockCacheKey(ts.filePath, prefetchOffset);
                        blockCache.invalidate(key);
                    }
                }
            }
        }



            if ("async".equals(prefetchMode)) {
                if ("mmap".equals(mode)) {
                    // Disable Lucene's madvise backoff so every prefetch actually calls madvise
                    if (mmapPrefetchField != null) {
                        try {
                            mmapPrefetchField.setInt(ts.threadInput, 0);
                        } catch (Exception ignored) {
                            System.out.println("Benchmark: unable to disable mmap prefetch backup");
                        }
                    }
                }
                // Original path: submit to executor (tests executor overhead)
                for (int i = 0; i < PREFETCH_AHEAD; i++) {
                    long prefetchOffset = ts.offset + i * strideBytes;
                    if (prefetchOffset + BLOCK_SIZE <= ts.rangeEnd) {
                        long t0 = System.nanoTime();
                        ts.threadInput.prefetch(prefetchOffset, BLOCK_SIZE);
                        prefetchTimeNs.add(System.nanoTime() - t0);
                        prefetchCalls.increment();
                    }
                }
            } else if ("inline_check".equals(prefetchMode) && "bufferpool".equals(mode)) {
                // Inline cache check: skip executor submission if block is already cached
                for (int i = 0; i < PREFETCH_AHEAD; i++) {
                    long prefetchOffset = ts.offset + i * strideBytes;
                    if (prefetchOffset + BLOCK_SIZE <= ts.rangeEnd) {
                        long t0 = System.nanoTime();
                        FileBlockCacheKey key = new FileBlockCacheKey(ts.filePath, prefetchOffset);
                        if (blockCache.getCache().getIfPresent(key) == null) {
                            // Only submit to executor if not cached
                            ts.threadInput.prefetch(prefetchOffset, BLOCK_SIZE);
                        }
                        prefetchTimeNs.add(System.nanoTime() - t0);
                        prefetchCalls.increment();
                    }
                }
            } else if ("inline_load".equals(prefetchMode) && "bufferpool".equals(mode)) {
                // Inline load: call getOrLoad on calling thread, no executor submission
                for (int i = 0; i < PREFETCH_AHEAD; i++) {
                    long prefetchOffset = ts.offset + i * strideBytes;
                    if (prefetchOffset + BLOCK_SIZE <= ts.rangeEnd) {
                        long t0 = System.nanoTime();
                        FileBlockCacheKey key = new FileBlockCacheKey(ts.filePath, prefetchOffset);
                        blockCache.getOrLoad(key);
                        prefetchTimeNs.add(System.nanoTime() - t0);
                        prefetchCalls.increment();
                    }
                }
            }
        // "off" mode: no prefetch at all

        // Simulate query processing work (scoring, merging, collecting)
        //Blackhole.consumeCPU(500);

        // Read full block from each strided position
        for (int i = 0; i < PREFETCH_AHEAD; i++) {
            long blockOffset = ts.offset + i * strideBytes;
            if (blockOffset + BLOCK_SIZE <= ts.rangeEnd) {
                ts.threadInput.seek(blockOffset);
                for (int j = 0; j < BLOCK_SIZE / Long.BYTES; j++) {
                    bh.consume(ts.threadInput.readLong());
                }
            }
        }

        // Advance past all strided blocks
        ts.offset += PREFETCH_AHEAD * strideBytes;
        if (ts.offset + BLOCK_SIZE > ts.rangeEnd) {
            ts.offset = ts.rangeStart;
            ts.passCount++;
            totalPasses.increment();

        }
    }
}
