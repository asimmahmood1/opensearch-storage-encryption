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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.block_cache.PrefetchTracker;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.bufferpoolfs.BlockSlotTinyCache;
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
 * Measures cold-path prefetch latency: time-to-first-IO for bufferpool vs mmap.
 *
 * For bufferpool, instruments right before the actual pread syscall to isolate
 * application overhead (executor submit, thread scheduling, dedup, Caffeine loader,
 * pool acquire, FileChannel.open) from actual IO time.
 *
 * For mmap, measures the madvise(MADV_WILLNEED) syscall duration directly.
 *
 * Sweeps simulated IO delay to show how overhead/IO ratio changes with slower storage.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 50)
@Measurement(iterations = 200)
@Fork(1)
@Threads(1)
public class ColdPrefetchLatencyBenchmark {

    private static final int BLOCK_SIZE = 8192;
    private static final long FILE_SIZE = 1L * 1024 * 1024; // 1MB — small, we only need a few blocks
    private static final int MAX_BLOCKS_CACHE = 1000;
    private static final long POOL_SIZE = 32L * 1024 * 1024;

    @Param({"bufferpool", "mmap"})
    private String mode;

    @Param({"0", "250", "500", "1000", "2000", "4000", "8000"})
    private long simulatedIoDelayUs;

    // --- Infrastructure ---
    private Path tempDir;
    private Path testFilePath;
    private Pool<RefCountedMemorySegment> pool;
    private ExecutorService executor;
    private PrefetchTracker prefetchTracker;
    private Worker readaheadWorker;
    private BufferPoolDirectory bufferPoolDir;
    private MMapDirectory mmapDir;
    private IndexInput sharedInput;
    private CaffeineBlockCache<RefCountedMemorySegment, RefCountedMemorySegment> blockCache;
    private TimestampingBlockLoader timestampingLoader;
    private java.lang.reflect.Field mmapPrefetchField;

    // --- Per-invocation state ---
    private long currentOffset;
    private long maxOffset;

    // --- Stats collection ---
    private final List<Long> overheadSamples = new ArrayList<>();
    private final List<Long> ioTimeSamples = new ArrayList<>();
    private final List<Long> totalTimeSamples = new ArrayList<>();
    private final List<Long> readByteSamples = new ArrayList<>();

    // --- posix_fadvise via Panama FFI ---
    private static final int POSIX_FADV_DONTNEED = 4;
    private static final MethodHandle OPEN_MH;
    private static final MethodHandle CLOSE_MH;
    private static final MethodHandle FADVISE_MH;

    static {
        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = linker.defaultLookup();
        OPEN_MH = linker.downcallHandle(
            lookup.find("open").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
        CLOSE_MH = linker.downcallHandle(
            lookup.find("close").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
        FADVISE_MH = linker.downcallHandle(
            lookup.find("posix_fadvise").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT,
                ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
    }

    private static void dropPageCache(Path file) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathStr = arena.allocateUtf8String(file.toAbsolutePath().toString());
            int fd = (int) OPEN_MH.invokeExact(pathStr, 0);
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

    /**
     * BlockLoader wrapper that timestamps right before and after the actual IO syscall.
     */
    static class TimestampingBlockLoader implements BlockLoader<RefCountedMemorySegment> {
        private final BlockLoader<RefCountedMemorySegment> delegate;
        private final long simulatedDelayNs;
        volatile long preSyscallNanos;
        volatile long postSyscallNanos;

        TimestampingBlockLoader(BlockLoader<RefCountedMemorySegment> delegate, long simulatedDelayUs) {
            this.delegate = delegate;
            this.simulatedDelayNs = simulatedDelayUs * 1000L;
        }

        @Override
        public RefCountedMemorySegment[] load(Path filePath, long startOffset, long blockCount, long poolTimeoutMs) throws Exception {
            preSyscallNanos = System.nanoTime();
            RefCountedMemorySegment[] result = delegate.load(filePath, startOffset, blockCount, poolTimeoutMs);
            if (simulatedDelayNs > 0) {
                LockSupport.parkNanos(simulatedDelayNs * blockCount);
            }
            postSyscallNanos = System.nanoTime();
            return result;
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        initMetrics();
        tempDir = Files.createTempDirectory("cold-prefetch-bench");
        Provider provider = Security.getProvider("SunJCE");
        EncryptionMetadataCache encMetaCache = new EncryptionMetadataCache();

        byte[] rawKey = new byte[32];
        new Random(42).nextBytes(rawKey);
        SecretKeySpec aesKey = new SecretKeySpec(rawKey, "AES");
        KeyResolver keyResolver = () -> aesKey;

        pool = new MemorySegmentPool(POOL_SIZE, BLOCK_SIZE);
        executor = new java.util.concurrent.ForkJoinPool(4);
        prefetchTracker = new PrefetchTracker(executor);

        Cache<BlockCacheKey, BlockCacheValue<RefCountedMemorySegment>> caffeineCache = Caffeine.newBuilder()
            .maximumSize(MAX_BLOCKS_CACHE).recordStats()
            .removalListener((BlockCacheKey k, BlockCacheValue<RefCountedMemorySegment> v,
                              com.github.benmanes.caffeine.cache.RemovalCause c) -> {
                if (v != null) try { v.close(); } catch (Exception ignored) {}
            }).build();

        CryptoDirectIOBlockLoader realLoader = new CryptoDirectIOBlockLoader(pool, keyResolver, encMetaCache);
        timestampingLoader = new TimestampingBlockLoader(realLoader, simulatedIoDelayUs);
        blockCache = new CaffeineBlockCache<>(caffeineCache, timestampingLoader, MAX_BLOCKS_CACHE, prefetchTracker);

        readaheadWorker = new QueuingWorker(64, executor);
        bufferPoolDir = new BufferPoolDirectory(
            tempDir, FSLockFactory.getDefault(), provider, keyResolver,
            pool, blockCache, timestampingLoader, readaheadWorker, encMetaCache);

        // Write test file
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

        if ("bufferpool".equals(mode)) {
            blockCache.clear();
            sharedInput = bufferPoolDir.openInput("test.dat", IOContext.DEFAULT);
        } else {
            mmapDir = new MMapDirectory(tempDir);
            sharedInput = mmapDir.openInput("test.dat", IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
            // Resolve Lucene's prefetch backoff counter for resetting
            try {
                Class<?> clazz = sharedInput.getClass();
                while (clazz != null) {
                    try {
                        mmapPrefetchField = clazz.getDeclaredField("consecutivePrefetchHitCount");
                        mmapPrefetchField.setAccessible(true);
                        break;
                    } catch (NoSuchFieldException e) { clazz = clazz.getSuperclass(); }
                }
            } catch (Exception e) {
                System.err.println("[WARN] Could not resolve mmap prefetch backoff field: " + e);
            }
        }

        testFilePath = tempDir.resolve("test.dat").toAbsolutePath().normalize();
        maxOffset = sharedInput.length() - BLOCK_SIZE;
        currentOffset = 0;

        // Prewarm: exercise the full prefetch path so ForkJoinPool threads are
        // started, JIT compiles hot methods, and Caffeine internals are initialized.
        if ("bufferpool".equals(mode)) {
            for (int i = 0; i < 20; i++) {
                long off = (long) i * BLOCK_SIZE;
                if (off + BLOCK_SIZE > sharedInput.length()) break;
                sharedInput.prefetch(off, BLOCK_SIZE);
                FileBlockCacheKey key = new FileBlockCacheKey(testFilePath, off);
                while (blockCache.get(key) == null) Thread.onSpinWait();
                blockCache.invalidate(key);
            }
            prefetchTracker.resetStats();
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        // Ensure cold cache for every invocation
        if ("bufferpool".equals(mode)) {
            FileBlockCacheKey key = new FileBlockCacheKey(testFilePath, currentOffset);
            blockCache.invalidate(key);
        } else {
            dropPageCache(testFilePath);
            if (mmapPrefetchField != null) {
                try { mmapPrefetchField.setInt(sharedInput, 0); } catch (Exception ignored) {}
            }
        }
    }

    @Benchmark
    public long coldPrefetchLatency(Blackhole bh) throws Exception {
        long t0 = System.nanoTime();

        if ("bufferpool".equals(mode)) {
            // Prefetch 1 block async
            sharedInput.prefetch(currentOffset, BLOCK_SIZE);

            // Poll until block appears in cache (prefetch complete)
            FileBlockCacheKey key = new FileBlockCacheKey(testFilePath, currentOffset);
            while (blockCache.get(key) == null) {
                Thread.onSpinWait();
            }
            long tDone = System.nanoTime();

            long overhead = timestampingLoader.preSyscallNanos - t0;
            long ioTime = timestampingLoader.postSyscallNanos - timestampingLoader.preSyscallNanos;
            long total = tDone - t0;

            // Measure first byte read latency (block is now warm in cache)
            sharedInput.seek(currentOffset);
            long tRead0 = System.nanoTime();
            bh.consume(sharedInput.readByte());
            long readByteNs = System.nanoTime() - tRead0;

            overheadSamples.add(overhead);
            ioTimeSamples.add(ioTime);
            totalTimeSamples.add(total);
            readByteSamples.add(readByteNs);

            bh.consume(overhead);
            bh.consume(ioTime);

            advanceOffset();
            return total;
        } else {
            // mmap: madvise is the IO trigger, measure its duration
            sharedInput.prefetch(currentOffset, BLOCK_SIZE);
            long tDone = System.nanoTime();
            long madviseTime = tDone - t0;

            // Measure first byte read latency (may page fault if madvise hasn't completed)
            sharedInput.seek(currentOffset);
            long tRead0 = System.nanoTime();
            bh.consume(sharedInput.readByte());
            long readByteNs = System.nanoTime() - tRead0;

            overheadSamples.add(madviseTime);
            ioTimeSamples.add(0L);
            totalTimeSamples.add(madviseTime);
            readByteSamples.add(readByteNs);

            bh.consume(madviseTime);

            advanceOffset();
            return madviseTime;
        }
    }

    private void advanceOffset() {
        currentOffset += BLOCK_SIZE;
        if (currentOffset > maxOffset) currentOffset = 0;
    }

    @TearDown(Level.Trial)
    public void reportStats() {
        if (overheadSamples.isEmpty()) return;

        long sumOverhead = 0, sumIo = 0, sumTotal = 0, sumReadByte = 0;
        for (int i = 0; i < overheadSamples.size(); i++) {
            sumOverhead += overheadSamples.get(i);
            sumIo += ioTimeSamples.get(i);
            sumTotal += totalTimeSamples.get(i);
            sumReadByte += readByteSamples.get(i);
        }
        int n = overheadSamples.size();
        double avgOverheadUs = sumOverhead / 1000.0 / n;
        double avgIoUs = sumIo / 1000.0 / n;
        double avgTotalUs = sumTotal / 1000.0 / n;
        double avgReadByteUs = sumReadByte / 1000.0 / n;
        double ratio = avgIoUs > 0 ? avgOverheadUs / avgIoUs : -1;

        // Sort for p50/p99
        List<Long> sorted = new ArrayList<>(totalTimeSamples);
        sorted.sort(Long::compareTo);
        double p50Us = sorted.get(n / 2) / 1000.0;
        double p99Us = sorted.get((int) (n * 0.99)) / 1000.0;

        System.out.println();
        System.out.printf("[COLD-PREFETCH] mode=%s simulatedDelayUs=%d samples=%d%n", mode, simulatedIoDelayUs, n);
        if ("bufferpool".equals(mode)) {
            System.out.printf("[COLD-PREFETCH] avgOverheadUs=%.1f avgIoUs=%.1f avgTotalUs=%.1f overhead/io=%.2f%n",
                avgOverheadUs, avgIoUs, avgTotalUs, ratio);
        } else {
            System.out.printf("[COLD-PREFETCH] avgMadviseUs=%.1f%n", avgOverheadUs);
        }
        System.out.printf("[COLD-PREFETCH] p50Us=%.1f p99Us=%.1f%n", p50Us, p99Us);
        System.out.printf("[COLD-PREFETCH] avgReadByteUs=%.3f%n", avgReadByteUs);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (readaheadWorker != null) readaheadWorker.close();
        if (prefetchTracker != null) prefetchTracker.clear();
        if (executor != null) { executor.shutdown(); executor.awaitTermination(10, TimeUnit.SECONDS); }
        if (sharedInput != null) sharedInput.close();
        if (bufferPoolDir != null) bufferPoolDir.close();
        if (mmapDir != null) mmapDir.close();
        if (tempDir != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.walk(tempDir).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
                } catch (IOException ignored) {}
            }));
        }
    }

    private void initMetrics() {
        CryptoMetricsService.initialize(new org.opensearch.telemetry.metrics.MetricsRegistry() {
            @Override public org.opensearch.telemetry.metrics.Counter createCounter(String n, String d, String u) { return null; }
            @Override public org.opensearch.telemetry.metrics.Counter createUpDownCounter(String n, String d, String u) { return null; }
            @Override public org.opensearch.telemetry.metrics.Histogram createHistogram(String n, String d, String u) { return null; }
            @Override public java.io.Closeable createGauge(String n, String d, String u, java.util.function.Supplier s, org.opensearch.telemetry.metrics.tags.Tags t) { return null; }
            @Override public java.io.Closeable createGauge(String n, String d, String u, java.util.function.Supplier s) { return null; }
            @Override public void close() {}
        });
    }
}
