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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.block.RefCountedMemorySegment;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheKey;
import org.opensearch.index.store.block_cache.CaffeineBlockCache;
import org.opensearch.index.store.block_loader.BlockLoader;
import org.opensearch.index.store.block_loader.CryptoDirectIOBlockLoader;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;
import org.opensearch.index.store.metrics.CryptoMetricsService;
import org.opensearch.index.store.niofs.CryptoNIOFSDirectory;
import org.opensearch.index.store.pool.MemorySegmentPool;
import org.opensearch.index.store.pool.Pool;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Base class for prefetch benchmarks with real file I/O and encryption.
 */
@State(Scope.Benchmark)
public class PrefetchBenchmarkBase {
    protected static final int BLOCK_SIZE = 8192;
    protected static final long FILE_SIZE_MB = 100;
    protected static final long FILE_SIZE = FILE_SIZE_MB * 1024 * 1024;
    
    protected Path tempDir;
    protected Path testFile;
    protected CryptoNIOFSDirectory directory;
    protected EncryptionMetadataCache encryptionMetadataCache;
    protected Pool<RefCountedMemorySegment> pool;
    protected BlockCache<RefCountedMemorySegment> blockCache;
    protected ConcurrentHashMap<BlockCacheKey, Boolean> prefetchCache;
    protected ExecutorService prefetchExecutor;
    protected InstrumentedBlockLoader instrumentedLoader;
    
    // Metrics
    protected AtomicLong totalPrefetchRequests = new AtomicLong();
    protected AtomicLong actualFileReads = new AtomicLong();
    protected AtomicLong cacheHits = new AtomicLong();
    
    @Setup(Level.Trial)
    public void setup() throws Exception {
        // Initialize metrics with no-op registry
        CryptoMetricsService.initialize(new org.opensearch.telemetry.metrics.MetricsRegistry() {
            @Override public org.opensearch.telemetry.metrics.Counter createCounter(String name, String description, String unit) { return null; }
            @Override public org.opensearch.telemetry.metrics.Counter createUpDownCounter(String name, String description, String unit) { return null; }
            @Override public org.opensearch.telemetry.metrics.Histogram createHistogram(String name, String description, String unit) { return null; }
            @Override public java.io.Closeable createGauge(String name, String description, String unit, java.util.function.Supplier supplier, org.opensearch.telemetry.metrics.tags.Tags tags) { return null; }
            @Override public java.io.Closeable createGauge(String name, String description, String unit, java.util.function.Supplier supplier) { return null; }
            @Override public void close() {}
        });

        // Create temp directory
        tempDir = Files.createTempDirectory("prefetch-benchmark");
        testFile = tempDir.resolve("test.dat");
        
        // Setup encryption with a simple static key (no KMS/NodeLevelKeyCache needed)
        Provider provider = Security.getProvider("SunJCE");
        encryptionMetadataCache = new EncryptionMetadataCache();

        byte[] rawKey = new byte[32];
        new Random(42).nextBytes(rawKey);
        javax.crypto.spec.SecretKeySpec aesKey = new javax.crypto.spec.SecretKeySpec(rawKey, "AES");
        KeyResolver keyResolver = () -> aesKey;
        
        // Create encrypted test file
        directory = new CryptoNIOFSDirectory(FSLockFactory.getDefault(), tempDir, provider, keyResolver, encryptionMetadataCache);
        createTestFile();
        
        // Setup memory pool (10MB pool)
        long poolSize = 10 * 1024 * 1024;
        pool = new MemorySegmentPool(poolSize, BLOCK_SIZE);
        
        // Setup prefetch cache
        prefetchCache = new ConcurrentHashMap<>();
        
        // Setup prefetch executor (4 threads)
        prefetchExecutor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "prefetch-worker");
            t.setDaemon(true);
            return t;
        });
        
        // Setup block cache with instrumentation
        setupBlockCache(1000, true);
    }
    
    protected void setupBlockCache(int maxBlocks, boolean withAsync) throws Exception {
        Cache<BlockCacheKey, org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment>> caffeineCache = 
            Caffeine.newBuilder()
                .maximumSize(maxBlocks)
                .recordStats()
                .removalListener((BlockCacheKey key, org.opensearch.index.store.block_cache.BlockCacheValue<RefCountedMemorySegment> value, com.github.benmanes.caffeine.cache.RemovalCause cause) -> {
                    if (value != null) {
                        try { value.close(); } catch (Exception e) { /* ignore */ }
                    }
                })
                .build();
        
        // Create instrumented loader
        CryptoDirectIOBlockLoader baseLoader = new CryptoDirectIOBlockLoader(
            pool,
            directory.keyResolver,
            encryptionMetadataCache
        );
        instrumentedLoader = new InstrumentedBlockLoader(baseLoader, actualFileReads);
        
        // Create block cache
        blockCache = new CaffeineBlockCache<>(
            caffeineCache,
            instrumentedLoader,
            maxBlocks,
            prefetchCache,
            withAsync ? prefetchExecutor : null
        );
    }
    
    protected void createTestFile() throws IOException {
        try (IndexOutput output = directory.createOutput("test.dat", org.apache.lucene.store.IOContext.DEFAULT)) {
            Random random = new Random(42);
            byte[] buffer = new byte[BLOCK_SIZE];
            long written = 0;
            while (written < FILE_SIZE) {
                random.nextBytes(buffer);
                output.writeBytes(buffer, 0, buffer.length);
                written += buffer.length;
            }
        }
    }
    
    protected long getBlockAlignedOffset(long offset) {
        return (offset / BLOCK_SIZE) * BLOCK_SIZE;
    }
    
    protected void resetMetrics() {
        totalPrefetchRequests.set(0);
        actualFileReads.set(0);
        cacheHits.set(0);
        prefetchCache.clear();
    }
    
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (prefetchExecutor != null) {
            prefetchExecutor.shutdown();
            prefetchExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (pool != null) {
            pool.close();
        }
        if (directory != null) {
            directory.close();
        }
        if (tempDir != null) {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
    
    @TearDown(Level.Iteration)
    public void reportMetrics() {
        long requests = totalPrefetchRequests.get();
        long reads = actualFileReads.get();
        long hits = cacheHits.get();
        
        if (requests > 0) {
            double dedupRate = 100.0 * (requests - reads) / requests;
            double hitRate = 100.0 * hits / requests;
            System.out.printf("Metrics: requests=%d, actual_reads=%d, cache_hits=%d, dedup_rate=%.2f%%, hit_rate=%.2f%%%n",
                requests, reads, hits, dedupRate, hitRate);
        }
    }
    
    /**
     * Instrumented block loader that tracks actual file reads.
     */
    protected static class InstrumentedBlockLoader implements BlockLoader<RefCountedMemorySegment> {
        private final BlockLoader<RefCountedMemorySegment> delegate;
        private final AtomicLong readCounter;
        
        public InstrumentedBlockLoader(
            BlockLoader<RefCountedMemorySegment> delegate,
            AtomicLong readCounter
        ) {
            this.delegate = delegate;
            this.readCounter = readCounter;
        }
        
        @Override
        public RefCountedMemorySegment load(BlockCacheKey key) throws Exception {
            readCounter.incrementAndGet();
            return delegate.load(key);
        }
        
        @Override
        public RefCountedMemorySegment[] load(
            Path filePath,
            long startOffset,
            long blockCount,
            long timeoutMs
        ) throws Exception {
            readCounter.addAndGet(blockCount);
            return delegate.load(filePath, startOffset, blockCount, timeoutMs);
        }
    }
}
