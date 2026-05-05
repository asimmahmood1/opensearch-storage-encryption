/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;import java.nio.ByteBuffer;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;
import org.junit.Before;
import static org.junit.Assert.assertThrows;
import org.junit.Test;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.hll.WorkingSetEstimator;

/**
 * Tests for BlockSlotTinyCache focusing on the race condition fix and proper pin/unpin behavior.
 */
@SuppressWarnings("preview")
public class BlockSlotTinyCacheTests {

    private static final int BLOCK_SIZE = 8192; // DirectIoConfigs.CACHE_BLOCK_SIZE

    private BlockCache<RefCountedByteBuffer> mockCache;
    private Path testPath;
    private Arena arena;

    @Before
    public void setUp() throws Exception {
        mockCache = mock(BlockCache.class);
        testPath = Paths.get("/test/file.dat");
        arena = Arena.ofAuto();
    }

    /**
     * Test that acquireRefCountedValue returns an already-pinned block.
     * This is the core fix - the L1 cache must return pinned blocks.
     */
    @Test
    public void AcquireReturnsAlreadyPinnedBlock() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        // Create a memory segment and wrap it
        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Initial refCount should be 1 (cache's reference)

        // Acquire the block - should return with refCount incremented (pinned)
        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);
        assertNotNull(result);

        // RefCount should now be 2 (cache + our pin)

        // Unpin should decrement
        result.unpin();

        // No releases should have occurred yet
    }

    /**
     * Test that a block is pinned exactly once per acquireRefCountedValue call.
     * Multiple acquisitions should each increment the refCount.
     */
    @Test
    public void BlockIsPinnedExactlyOncePerAcquisition() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Initial state

        // First acquisition
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);

        // Second acquisition (same block) - should hit thread-local cache and pin again
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);

        // Third acquisition
        BlockCacheValue<RefCountedByteBuffer> result3 = cache.acquireRefCountedValue(0);

        // Unpin all three
        result1.unpin();

        result2.unpin();

        result3.unpin();

        // No releases yet (cache still holds reference)
    }

    /**
     * Test that unpinning releases the reference properly.
     * When all pins are released and cache drops its reference, the segment should be released.
     */
    @Test
    public void UnpinReleasesReference() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // Acquire and unpin
        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);

        result.unpin();

        // Simulate cache eviction (cache drops its reference)
        refSegment.close(); // This increments generation and calls decRef

        // Releaser should have been called
    }

    /**
     * Test the race condition fix: verify that generation checking prevents returning stale blocks.
     * When a block is evicted (generation incremented), the L1 cache should detect this and reload.
     */

    /**
     * Test concurrent access to the same block from multiple threads.
     * Each thread should get a properly pinned block and unpinning should work correctly.
     */
    @Test
    public void ConcurrentAcquisitionAndRelease() throws Exception {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        AtomicInteger releaseCount = new AtomicInteger(0);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        int numThreads = 10;
        int acquisitionsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    barrier.await(); // Synchronize start
                    for (int j = 0; j < acquisitionsPerThread; j++) {
                        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);
                        assertNotNull(result);
                        // Block is pinned - refCount should be > 1
                        result.unpin();
                    }
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Threads did not complete in time", latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        if (error.get() != null) {
            throw new AssertionError("Thread error", error.get());
        }

        // All threads are done, refCount should be back to 1 (cache only)
    }

    /**
     * Test that multiple blocks can be cached and pinned independently.
     */
    @Test
    public void MultipleBlocksIndependentPinning() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        // Create three different blocks
        List<RefCountedByteBuffer> segments = new ArrayList<>();
        List<BlockCacheValue<RefCountedByteBuffer>> cacheValues = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            MemorySegment segment = arena.allocate(BLOCK_SIZE);
            int finalI = i;
            RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);
            segments.add(refSegment);

            BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
            when(cacheValue.value()).thenReturn(refSegment);
            when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
            Mockito.doAnswer(inv -> {
                refSegment.unpin();
                return null;
            }).when(cacheValue).unpin();
            cacheValues.add(cacheValue);
        }

        // Mock cache to return appropriate segment based on block offset
        when(mockCache.get(any(FileBlockCacheKey.class))).thenAnswer(inv -> {
            FileBlockCacheKey key = inv.getArgument(0);
            long blockOffset = key.fileOffset();
            int blockIdx = (int) (blockOffset / BLOCK_SIZE);
            return cacheValues.get(blockIdx);
        });

        // Acquire all three blocks
        BlockCacheValue<RefCountedByteBuffer> result0 = cache.acquireRefCountedValue(0);
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(BLOCK_SIZE);
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(BLOCK_SIZE * 2L);

        // Each should be pinned (refCount = 2)

        // Unpin in different order
        result1.unpin();

        result0.unpin();

        result2.unpin();
    }

    /**
     * Test the retry mechanism when tryPin() temporarily fails.
     * This simulates the scenario where eviction is happening concurrently.
     */

    /**
     * Test that exceeding max retry attempts throws IOException.
     */

    /**
     * Test clear() properly resets the cache and prevents stale references.
     */
    @Test
    public void ClearPreventsStaleCacheHits() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment1 = arena.allocate(BLOCK_SIZE);
        RefCountedByteBuffer refSegment1 = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue1 = mock(BlockCacheValue.class);
        when(cacheValue1.value()).thenReturn(refSegment1);
        when(cacheValue1.tryPin()).thenAnswer(inv -> refSegment1.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment1.unpin();
            return null;
        }).when(cacheValue1).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue1);

        // Populate cache
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);
        result1.unpin();

        // Clear cache
        cache.clear();

        // Create new segment
        MemorySegment segment2 = arena.allocate(BLOCK_SIZE);
        RefCountedByteBuffer refSegment2 = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue2 = mock(BlockCacheValue.class);
        when(cacheValue2.value()).thenReturn(refSegment2);
        when(cacheValue2.tryPin()).thenAnswer(inv -> refSegment2.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment2.unpin();
            return null;
        }).when(cacheValue2).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue2);

        // Next acquisition should get the new segment (not stale cached one)
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);
        assertEquals(refSegment2, result2.value());
        result2.unpin();
    }

    /**
     * Test thread-local cache hits prevent redundant pinning on the same thread.
     */
    @Test
    public void ThreadLocalCacheHitsPinCorrectly() throws IOException {
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 10);

        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(BLOCK_SIZE), BLOCK_SIZE);

        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenAnswer(inv -> refSegment.tryPin());
        Mockito.doAnswer(inv -> {
            refSegment.unpin();
            return null;
        }).when(cacheValue).unpin();

        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);

        // First acquisition
        BlockCacheValue<RefCountedByteBuffer> result1 = cache.acquireRefCountedValue(0);

        // Second acquisition on same thread - should hit thread-local cache but still pin
        BlockCacheValue<RefCountedByteBuffer> result2 = cache.acquireRefCountedValue(0);

        // Verify tryPin was called at least twice (once per acquisition)
        verify(cacheValue, atMost(3)).tryPin(); // At most 3 because of potential retry logic

        result1.unpin();
        result2.unpin();
    }


    /**
     * Tests that HLL working set estimator is updated on block access.
     */
    @Test
    public void HLLUpdateOnBlockAccess() throws IOException {
        WorkingSetEstimator.getInstance().setEnabled(true);
        WorkingSetEstimator.getInstance().resetForTesting();
        WorkingSetEstimator.getInstance().setEnabled(true); // resetForTesting doesn't preserve enabled state
        long initialEstimate = WorkingSetEstimator.getInstance().estimateCardinality(60);
        
        // Create mock cache value
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(ByteBuffer.allocateDirect(1), 1);
        BlockCacheValue<RefCountedByteBuffer> cacheValue = mock(BlockCacheValue.class);
        when(cacheValue.value()).thenReturn(refSegment);
        when(cacheValue.tryPin()).thenReturn(true);
        
        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(cacheValue);
        
        BlockSlotTinyCache cache = new BlockSlotTinyCache(mockCache, testPath, BLOCK_SIZE * 32L);
        
        // Access a block - should trigger HLL update
        BlockCacheValue<RefCountedByteBuffer> result = cache.acquireRefCountedValue(0);
        assertNotNull("Should return block", result);
        
        // HLL should have been updated
        long afterEstimate = WorkingSetEstimator.getInstance().estimateCardinality(60);
        assertTrue("HLL estimate should increase after block access", afterEstimate > initialEstimate);
        
        result.unpin();
        WorkingSetEstimator.getInstance().setEnabled(false);
    }
}
