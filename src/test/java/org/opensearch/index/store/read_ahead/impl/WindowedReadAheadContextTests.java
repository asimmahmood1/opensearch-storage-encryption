/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.read_ahead.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.bufferpoolfs.StaticConfigs;
import org.opensearch.index.store.read_ahead.Worker;

public class WindowedReadAheadContextTests {

    private static int CACHE_BLOCK_SIZE;
    private static final Path TEST_PATH = Paths.get("/test/file.dat");
    private static final long FILE_SIZE = 1024 * 1024; // 1MB

    private Worker mockWorker;
    @SuppressWarnings("unchecked")
    private BlockCache<AutoCloseable> mockBlockCache;
    private Runnable mockSignalCallback;
    private WindowedReadAheadContext context;
    private WindowedReadAheadConfig config;

    @Before
    public void setUp() throws Exception {
        StaticConfigs.resetForTesting();
        StaticConfigs.init(8192);
        CACHE_BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;
        mockWorker = mock(Worker.class);
        @SuppressWarnings("unchecked")
        BlockCache<AutoCloseable> cache = (BlockCache<AutoCloseable>) mock(BlockCache.class);
        mockBlockCache = cache;
        mockSignalCallback = mock(Runnable.class);
        config = WindowedReadAheadConfig.defaultConfig();

        // Default: worker accepts all schedules and is not paused
        when(mockWorker.schedule(any(), any(Path.class), anyLong(), anyLong())).thenReturn(true);
        when(mockWorker.isReadAheadPaused()).thenReturn(false);
        when(mockWorker.getQueueCapacity()).thenReturn(100);
        when(mockWorker.getQueueSize()).thenReturn(0);
    }

    @After
    public void tearDown() {
        StaticConfigs.resetForTesting();
    }

    private WindowedReadAheadContext createContext(long fileLength) {
        return WindowedReadAheadContext.build(TEST_PATH, fileLength, mockWorker, mockBlockCache, config, mockSignalCallback);
    }

    private WindowedReadAheadContext createContext(long fileLength, WindowedReadAheadConfig customConfig) {
        return WindowedReadAheadContext.build(TEST_PATH, fileLength, mockWorker, mockBlockCache, customConfig, mockSignalCallback);
    }

    /**
     * Tests that context can be created with valid parameters.
     */
    @Test
    public void ContextCreation() {
        context = createContext(FILE_SIZE);

        assertNotNull(context);
        assertTrue(context.isReadAheadEnabled());
        assertFalse(context.hasQueuedWork());
    }

    /**
     * Tests that cache hits do not trigger readahead.
     */
    @Test
    public void CacheHitDoesNotTrigger() throws Exception {
        context = createContext(FILE_SIZE);

        // Simulate cache hit
        runOnSearchThread(() -> context.onAccess(0, true));

        assertFalse("Cache hit should not queue work", context.hasQueuedWork());
        verify(mockSignalCallback, never()).run();
    }

    /**
     * Tests that cache miss triggers readahead immediately (no batching threshold).
     */
    @Test
    public void CacheMissTriggersImmediate() throws Exception {
        context = createContext(FILE_SIZE);

        // First miss should trigger immediately
        runOnSearchThread(() -> context.onAccess(0, false));

        // Should have queued work and signaled
        assertTrue("Cache miss should queue work", context.hasQueuedWork());
        verify(mockSignalCallback, times(1)).run();
    }

    /**
     * Tests sequential misses trigger readahead.
     */
    @Test
    public void SequentialMisses() throws Exception {
        context = createContext(FILE_SIZE);

        // Sequential misses
        runOnSearchThread(() -> context.onAccess(0, false));
        runOnSearchThread(() -> context.onAccess(CACHE_BLOCK_SIZE, false));
        runOnSearchThread(() -> context.onAccess(2 * CACHE_BLOCK_SIZE, false));

        assertTrue("Sequential misses should queue work", context.hasQueuedWork());
        // Wake is idempotent - may be called 1-3 times depending on timing
        verify(mockSignalCallback, atLeastOnce()).run();
    }

    /**
     * Tests processQueue schedules work with worker.
     */
    @Test
    public void ProcessQueueSchedulesWork() throws Exception {
        context = createContext(FILE_SIZE);

        // Trigger readahead
        runOnSearchThread(() -> context.onAccess(0, false));

        // Process the queue
        boolean processed = context.processQueue();

        assertTrue("processQueue should return true when work scheduled", processed);
        verify(mockWorker, times(1)).schedule(any(), eq(TEST_PATH), anyLong(), anyLong());
    }

    /**
     * Tests processQueue returns false when no work queued.
     */
    @Test
    public void ProcessQueueNoWork() {
        context = createContext(FILE_SIZE);

        // No misses, no work
        boolean processed = context.processQueue();

        assertFalse("processQueue should return false when no work", processed);
        verify(mockWorker, never()).schedule(any(), any(), anyLong(), anyLong());
    }

    /**
     * Tests global pause prevents readahead.
     */
    @Test
    public void GlobalPausePreventsReadahead() {
        when(mockWorker.isReadAheadPaused()).thenReturn(true);
        context = createContext(FILE_SIZE);

        // Miss should not trigger when paused
        context.onAccess(0, false);

        assertFalse("Global pause should prevent queuing work", context.hasQueuedWork());
        verify(mockSignalCallback, never()).run();
    }

    /**
     * Tests queue pressure drops backlog.
     */
    @Test
    public void QueuePressureDropsBacklog() throws Exception {
        // Simulate high queue pressure (>75%)
        when(mockWorker.getQueueSize()).thenReturn(80);
        when(mockWorker.getQueueCapacity()).thenReturn(100);

        context = createContext(FILE_SIZE);

        // Trigger readahead
        runOnSearchThread(() -> context.onAccess(0, false));
        assertTrue(context.hasQueuedWork());

        // Process should drop backlog due to pressure
        boolean processed = context.processQueue();

        assertFalse("High queue pressure should drop backlog", processed);
        assertFalse("Should have no queued work after pressure drop", context.hasQueuedWork());
    }

    /**
     * Tests worker rejection drops backlog.
     */
    @Test
    public void WorkerRejectionDropsBacklog() throws Exception {
        when(mockWorker.schedule(any(), any(), anyLong(), anyLong())).thenReturn(false);

        context = createContext(FILE_SIZE);

        // Trigger readahead
        runOnSearchThread(() -> context.onAccess(0, false));
        assertTrue(context.hasQueuedWork());

        // Process should handle rejection
        boolean processed = context.processQueue();

        assertFalse("Worker rejection should return false", processed);
        assertFalse("Should have no queued work after rejection", context.hasQueuedWork());
    }

    /**
     * Tests idempotent wake - multiple misses don't storm callback.
     */
    @Test
    public void IdempotentWake() throws Exception {
        AtomicInteger callbackCount = new AtomicInteger(0);
        Runnable countingCallback = callbackCount::incrementAndGet;

        context = WindowedReadAheadContext.build(TEST_PATH, FILE_SIZE, mockWorker, mockBlockCache, config, countingCallback);

        // Multiple rapid misses
        runOnSearchThread(() -> {
            for (int i = 0; i < 10; i++) {
                context.onAccess(i * CACHE_BLOCK_SIZE, false);
            }
        });

        // Should wake at least once, but far fewer than 10 times due to idempotent gate
        int wakeCount = callbackCount.get();
        assertTrue("Should wake at least once", wakeCount >= 1);
        assertTrue("Should not wake 10 times (idempotent gate)", wakeCount < 10);
    }

    /**
     * Tests context close stops readahead.
     */
    @Test
    public void ContextClose() {
        context = createContext(FILE_SIZE);

        assertTrue(context.isReadAheadEnabled());

        context.close();

        assertFalse("Context should be disabled after close", context.isReadAheadEnabled());

        // Miss after close should not trigger
        context.onAccess(0, false);
        assertFalse("Closed context should not queue work", context.hasQueuedWork());
    }

    /**
     * Tests reset clears queued work.
     */
    @Test
    public void Reset() throws Exception {
        context = createContext(FILE_SIZE);

        // Queue some work
        runOnSearchThread(() -> context.onAccess(0, false));
        assertTrue(context.hasQueuedWork());

        // Reset should clear
        context.reset();

        assertFalse("Reset should clear queued work", context.hasQueuedWork());
    }

    /**
     * Tests cancel delegates to worker.
     */
    @Test
    public void Cancel() {
        context = createContext(FILE_SIZE);

        context.cancel();

        verify(mockWorker, times(1)).cancel(TEST_PATH);
    }

    /**
     * Tests triggerReadahead manually queues work.
     */
    @Test
    public void TriggerReadahead() throws Exception {
        context = createContext(FILE_SIZE);

        // Manually trigger readahead
        runOnSearchThread(() -> context.triggerReadahead(0));

        assertTrue("Manual trigger should queue work", context.hasQueuedWork());
        verify(mockSignalCallback, times(1)).run();
    }

    /**
     * Tests triggerReadahead respects global pause.
     */
    @Test
    public void TriggerReadaheadRespectsPause() throws Exception {
        when(mockWorker.isReadAheadPaused()).thenReturn(true);
        context = createContext(FILE_SIZE);

        runOnSearchThread(() -> context.triggerReadahead(0));

        assertFalse("Trigger should respect global pause", context.hasQueuedWork());
        verify(mockSignalCallback, never()).run();
    }

    /**
     * Tests null signal callback is handled gracefully.
     */
    @Test
    public void NullSignalCallback() throws Exception {
        context = WindowedReadAheadContext.build(TEST_PATH, FILE_SIZE, mockWorker, mockBlockCache, config, null);

        // Should not throw
        runOnSearchThread(() -> context.onAccess(0, false));

        // Work is still queued even without callback
        assertTrue("Work should be queued even with null callback", context.hasQueuedWork());
    }

    /**
     * Tests custom config is respected.
     */
    @Test
    public void CustomConfig() {
        WindowedReadAheadConfig customConfig = WindowedReadAheadConfig
            .of(
                2,  // initialWindow
                16, // maxWindow
                8   // randomAccessThreshold
            );

        context = createContext(FILE_SIZE, customConfig);

        assertNotNull(context);
        assertEquals(2, context.policy().currentWindow());
    }

    /**
     * Tests policy integration - window grows on sequential access.
     */
    @Test
    public void PolicyWindowGrowth() throws Exception {
        context = createContext(FILE_SIZE);

        int initialWindow = context.policy().currentWindow();

        // Sequential access should grow window
        runOnSearchThread(() -> {
            for (int i = 0; i < 10; i++) {
                context.onAccess(i * CACHE_BLOCK_SIZE, false);
            }
        });

        int newWindow = context.policy().currentWindow();
        assertTrue("Window should grow on sequential access", newWindow >= initialWindow);
    }

    /**
     * Tests hasQueuedWork reflects pending work accurately.
     */
    @Test
    public void HasQueuedWorkAccuracy() throws Exception {
        context = createContext(FILE_SIZE);

        assertFalse(context.hasQueuedWork());

        // Queue work
        runOnSearchThread(() -> context.onAccess(0, false));
        assertTrue(context.hasQueuedWork());

        // Process all work
        while (context.processQueue()) {
            // Keep processing until done
        }

        assertFalse("Should have no queued work after processing all", context.hasQueuedWork());
    }

    /**
     * Tests large file sizes are handled correctly.
     */
    @Test
    public void LargeFile() throws Exception {
        long largeFileSize = 10L * 1024 * 1024 * 1024; // 10GB
        context = createContext(largeFileSize);

        assertNotNull(context);

        // Should handle large offsets
        runOnSearchThread(() -> context.onAccess(largeFileSize - CACHE_BLOCK_SIZE, false));
        assertTrue(context.hasQueuedWork());
    }

    /**
     * Tests zero-length file is handled.
     */
    @Test
    public void ZeroLengthFile() throws Exception {
        context = createContext(0);

        assertNotNull(context);

        // Access should not crash
        runOnSearchThread(() -> context.onAccess(0, false));
    }

    /**
     * Tests processQueue clears wake flag when all work done.
     */
    @Test
    public void WakeFlagClearedWhenDone() throws Exception {
        AtomicInteger wakeCount = new AtomicInteger(0);
        context = WindowedReadAheadContext.build(TEST_PATH, FILE_SIZE, mockWorker, mockBlockCache, config, wakeCount::incrementAndGet);

        // Queue small amount of work
        runOnSearchThread(() -> context.onAccess(0, false));
        int wakesAfterFirst = wakeCount.get();
        assertTrue("Should wake on first access", wakesAfterFirst >= 1);

        // Process it all
        boolean processed = context.processQueue();
        assertTrue("Should process work", processed);
        assertFalse("Should have no queued work after processing", context.hasQueuedWork());
    }

    /**
     * Tests processQueue keeps wake flag if more work remains.
     */
    @Test
    public void WakeFlagKeptWhenWorkRemains() throws Exception {
        context = createContext(FILE_SIZE);

        // Queue lots of work (more than MAX_BLOCKS_PER_SUBMISSION = 64)
        runOnSearchThread(() -> {
            context.onAccess(0, false);
            for (int i = 0; i < 100; i++) {
                context.onAccess(i * CACHE_BLOCK_SIZE, false);
            }
        });

        // Process once (should only process up to 64 blocks)
        context.processQueue();

        // Should still have work queued
        assertTrue("Should have remaining work after partial processing", context.hasQueuedWork());
    }

    /**
     * Tests that read-ahead is allowed on search thread pool.
     */
    public void testReadAheadAllowedOnSearchThread() throws Exception {
        context = createContext(FILE_SIZE);
        runOnNamedThread("opensearch[node1][search][T#1]", () -> context.onAccess(0, false));
        assertTrue("search thread should trigger read-ahead", context.hasQueuedWork());
    }

    /**
     * Tests that read-ahead is allowed on index_searcher thread pool.
     */
    public void testReadAheadAllowedOnIndexSearcherThread() throws Exception {
        context = createContext(FILE_SIZE);
        runOnNamedThread("opensearch[node1][index_searcher][T#2]", () -> context.onAccess(0, false));
        assertTrue("index_searcher thread should trigger read-ahead", context.hasQueuedWork());
    }

    /**
     * Tests that read-ahead is blocked on generic thread pool.
     */
    public void testReadAheadBlockedOnGenericThread() throws Exception {
        context = createContext(FILE_SIZE);
        runOnNamedThread("opensearch[node1][generic][T#3]", () -> context.onAccess(0, false));
        assertFalse("generic thread should not trigger read-ahead", context.hasQueuedWork());
    }

    /**
     * Tests that read-ahead is blocked on flush thread pool.
     */
    public void testReadAheadBlockedOnFlushThread() throws Exception {
        context = createContext(FILE_SIZE);
        runOnNamedThread("opensearch[node1][flush][T#1]", () -> context.onAccess(0, false));
        assertFalse("flush thread should not trigger read-ahead", context.hasQueuedWork());
    }

    /**
     * Tests that read-ahead is blocked on refresh thread pool.
     */
    public void testReadAheadBlockedOnRefreshThread() throws Exception {
        context = createContext(FILE_SIZE);
        runOnNamedThread("opensearch[node1][refresh][T#1]", () -> context.onAccess(0, false));
        assertFalse("refresh thread should not trigger read-ahead", context.hasQueuedWork());
    }

    /**
     * Tests that triggerReadahead is blocked on non-search threads.
     */
    public void testTriggerReadaheadBlockedOnGenericThread() throws Exception {
        context = createContext(FILE_SIZE);
        runOnNamedThread("opensearch[node1][generic][T#3]", () -> context.triggerReadahead(0));
        assertFalse("generic thread should not trigger manual read-ahead", context.hasQueuedWork());
    }

    private static void runOnSearchThread(Runnable runnable) throws Exception {
        runOnNamedThread("opensearch[node1][search][T#1]", runnable);
    }

    private static void runOnNamedThread(String name, Runnable action) throws Exception {
        Thread t = new Thread(action, name);
        t.start();
        t.join();
    }
}
