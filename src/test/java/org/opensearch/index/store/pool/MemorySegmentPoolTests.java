/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.index.store.block.RefCountedByteBuffer;

/**
 * Unit tests for {@link MemorySegmentPool} — GC-managed ByteBuffer pool.
 */
@SuppressWarnings("preview")
public class MemorySegmentPoolTests {

    private MemorySegmentPool pool;

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
        if (pool != null && !pool.isClosed())
            pool.close();
    }

    // ---- Construction ----

    @Test
    public void testPoolCreation() {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(1024, pool.pooledSegmentSize());
        assertEquals(4096L, pool.totalMemory());
        assertFalse(pool.isClosed());
        assertEquals(0, pool.getBuffersInUse());
        assertEquals(0, pool.getAllocatedBytes());
    }

    @Test
    public void testInvalidConfigurationThrows() {
        try {
            pool = new MemorySegmentPool(4097, 1024);
            fail("Should throw for non-aligned totalMemory");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("multiple"));
        }
    }

    @Test
    public void testGcHeadroomFractionConstructor() {
        pool = new MemorySegmentPool(4096, 1024, 0.25);
        assertNotNull(pool);
        assertEquals(1024, pool.pooledSegmentSize());
    }

    // ---- acquire() ----

    @Test
    public void testAcquireReturnsDirectByteBuffer() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer buf = pool.acquire();
        assertNotNull(buf);
        assertNotNull(buf.buffer());
        assertTrue(buf.buffer().isDirect());
        assertEquals(1024, buf.buffer().capacity());
        assertEquals(ByteOrder.LITTLE_ENDIAN, buf.buffer().order());
    }

    @Test
    public void testAcquireIncrementsBuffersInUse() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(0, pool.getBuffersInUse());
        pool.acquire();
        assertEquals(1, pool.getBuffersInUse());
        pool.acquire();
        assertEquals(2, pool.getBuffersInUse());
        pool.acquire();
        assertEquals(3, pool.getBuffersInUse());
    }

    @Test
    public void testAcquireOverCapacitySucceeds() throws Exception {
        pool = new MemorySegmentPool(2048, 1024); // max=2
        pool.acquire();
        pool.acquire();
        // Over capacity — should still succeed (read path must not fail)
        RefCountedByteBuffer buf = pool.acquire();
        assertNotNull(buf);
        assertEquals(3, pool.getBuffersInUse());
    }

    @Test
    public void testAcquireFromClosedPoolThrows() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        pool.close();
        try {
            pool.acquire();
            fail("Should throw on closed pool");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    // ---- tryAcquire() ----

    @Test
    public void testTryAcquireUnderCapacitySucceeds() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer buf = pool.tryAcquire(100, TimeUnit.MILLISECONDS);
        assertNotNull(buf);
        assertEquals(1, pool.getBuffersInUse());
    }

    @Test
    public void testTryAcquireFromClosedPoolThrows() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        pool.close();
        try {
            pool.tryAcquire(100, TimeUnit.MILLISECONDS);
            fail("Should throw on closed pool");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("closed"));
        }
    }

    // ---- MemorySegment view ----

    @Test
    public void testSegmentViewSharesMemory() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        RefCountedByteBuffer buf = pool.acquire();
        // Write via ByteBuffer, read via MemorySegment
        buf.buffer().put(0, (byte) 42);
        assertEquals((byte) 42, buf.segment().get(ValueLayout.JAVA_BYTE, 0));
        // Write via MemorySegment, read via ByteBuffer
        buf.segment().set(ValueLayout.JAVA_BYTE, 100, (byte) 99);
        assertEquals((byte) 99, buf.buffer().get(100));
    }

    @Test
    public void testSegmentReadWriteFullBlock() throws Exception {
        pool = new MemorySegmentPool(2048, 1024);
        RefCountedByteBuffer buf = pool.acquire();
        buf.segment().fill((byte) 0xFF);
        assertEquals((byte) 0xFF, buf.segment().get(ValueLayout.JAVA_BYTE, 0));
        assertEquals((byte) 0xFF, buf.segment().get(ValueLayout.JAVA_BYTE, 1023));
    }

    // ---- release() no-op ----

    @Test
    public void testReleaseIsNoOp() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        int before = pool.getBuffersInUse();
        pool.release(seg);
        assertEquals(before, pool.getBuffersInUse());
    }

    @Test
    public void testReleaseAllIsNoOp() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer s1 = pool.acquire();
        RefCountedByteBuffer s2 = pool.acquire();
        int before = pool.getBuffersInUse();
        pool.releaseAll(s1, s2);
        assertEquals(before, pool.getBuffersInUse());
    }

    // ---- availableMemory ----

    @Test
    public void testAvailableMemoryDecreases() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(4096L, pool.availableMemory());
        pool.acquire();
        assertEquals(3072L, pool.availableMemory());
        pool.acquire();
        assertEquals(2048L, pool.availableMemory());
    }

    @Test
    public void testAvailableMemoryNeverNegative() throws Exception {
        pool = new MemorySegmentPool(2048, 1024); // max=2
        pool.acquire();
        pool.acquire();
        pool.acquire(); // over capacity
        assertTrue(pool.availableMemory() >= 0);
    }

    // ---- allocatedBytes ----

    @Test
    public void testAllocatedBytesTracking() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        assertEquals(0, pool.getAllocatedBytes());
        pool.acquire();
        assertEquals(1024, pool.getAllocatedBytes());
        pool.acquire();
        assertEquals(2048, pool.getAllocatedBytes());
    }

    // ---- isUnderPressure ----

    @Test
    public void testNotUnderPressureInitially() {
        pool = new MemorySegmentPool(10240, 1024); // max=10
        assertFalse(pool.isUnderPressure());
    }

    @Test
    public void testNotUnderPressureAt90Percent() throws Exception {
        pool = new MemorySegmentPool(20480, 1024); // max=20
        for (int i = 0; i < 18; i++) pool.acquire(); // 90%
        assertFalse(pool.isUnderPressure()); // threshold is 95%
    }

    @Test
    public void testUnderPressureAt95Percent() throws Exception {
        pool = new MemorySegmentPool(20480, 1024); // max=20
        for (int i = 0; i < 19; i++) pool.acquire(); // 95%
        assertTrue(pool.isUnderPressure());
    }

    @Test
    public void testUnderPressureAtFull() throws Exception {
        pool = new MemorySegmentPool(4096, 1024); // max=4
        for (int i = 0; i < 4; i++) pool.acquire();
        assertTrue(pool.isUnderPressure());
    }

    // ---- warmUp ----

    @Test
    public void testWarmUpIsNoOp() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.warmUp(4);
        assertEquals(0, pool.getBuffersInUse());
    }

    // ---- close ----

    @Test
    public void testCloseMarksPoolClosed() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
        pool.close();
        assertTrue(pool.isClosed());
    }

    @Test
    public void testDoubleCloseIsSafe() {
        pool = new MemorySegmentPool(2048, 1024);
        pool.close();
        pool.close();
        assertTrue(pool.isClosed());
    }

    @Test
    public void testCloseStopsGcDebtMonitor() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.close();
        // Give thread time to stop
        Thread.sleep(100);
        // No assertion needed — if join(5000) hangs, test framework will timeout
    }

    // ---- poolStats ----

    @Test
    public void testPoolStatsContainsAllFields() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        pool.acquire();
        String stats = pool.poolStats();
        assertNotNull(stats);
        assertTrue(stats.contains("PoolStats"));
        assertTrue(stats.contains("max=4"));
        assertTrue(stats.contains("inUse=1"));
        assertTrue(stats.contains("utilization="));
        assertTrue(stats.contains("stalls="));
        assertTrue(stats.contains("tracked="));
        assertTrue(stats.contains("native="));
        assertTrue(stats.contains("zombie="));
    }

    @Test
    public void testPoolStatsShowsStallsAfterOverCapacity() throws Exception {
        pool = new MemorySegmentPool(2048, 1024); // max=2
        pool.acquire();
        pool.acquire();
        pool.acquire(); // over capacity → stall
        String stats = pool.poolStats();
        assertFalse(stats.contains("stalls=0"));
    }

    // ---- Cleaner decrements buffersInUse ----

    @Test
    public void testCleanerDecrementsBuffersInUse() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer seg = pool.acquire();
        assertEquals(1, pool.getBuffersInUse());

        seg = null; // drop reference
        for (int i = 0; i < 20; i++) {
            System.gc();
            Thread.sleep(50);
            if (pool.getBuffersInUse() == 0) break;
        }
        // Best-effort: Cleaner may or may not have run
        if (pool.getBuffersInUse() == 0) {
            assertEquals(0L, pool.getAllocatedBytes());
        }
    }

    // ---- GC debt monitor ----

    @Test
    public void testCacheEntriesSupplierWiring() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        AtomicInteger cacheSize = new AtomicInteger(0);
        pool.setCacheEntriesSupplier(cacheSize::get);
        pool.acquire();
        cacheSize.set(1);
        // No assertion — just verify it doesn't throw
    }

    // ---- Concurrent acquire ----

    @Test
    public void testConcurrentAcquire() throws Exception {
        pool = new MemorySegmentPool(81920, 1024); // max=80
        int threads = 8;
        int acquiresPerThread = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> error = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < acquiresPerThread; i++) {
                        RefCountedByteBuffer buf = pool.acquire();
                        assertNotNull(buf);
                        // Write and read to verify buffer is usable
                        buf.buffer().putInt(0, 42);
                        assertEquals(42, buf.buffer().getInt(0));
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        start.countDown();
        assertTrue("Threads should complete", done.await(30, TimeUnit.SECONDS));
        if (error.get() != null) throw new AssertionError("Thread failed", error.get());
        assertEquals(threads * acquiresPerThread, pool.getBuffersInUse());
    }

    // ---- Buffer independence ----

    @Test
    public void testEachAcquireReturnsIndependentBuffer() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer buf1 = pool.acquire();
        RefCountedByteBuffer buf2 = pool.acquire();

        buf1.buffer().putInt(0, 111);
        buf2.buffer().putInt(0, 222);

        assertEquals(111, buf1.buffer().getInt(0));
        assertEquals(222, buf2.buffer().getInt(0));
    }

    // ---- Length ----

    @Test
    public void testAcquiredBufferLength() throws Exception {
        pool = new MemorySegmentPool(8192, 8192);
        RefCountedByteBuffer buf = pool.acquire();
        assertEquals(8192, buf.length());
        assertEquals(8192, buf.buffer().capacity());
    }

    // ---- tryPin/unpin/close are no-ops on RefCountedByteBuffer ----

    @Test
    public void testRefCountedByteBufferLifecycleNoOps() throws Exception {
        pool = new MemorySegmentPool(4096, 1024);
        RefCountedByteBuffer buf = pool.acquire();
        assertTrue(buf.tryPin());
        buf.unpin();
        buf.close();
        buf.decRef();
        assertTrue(buf.tryPin()); // still true — all no-ops
        assertEquals(0, buf.getGeneration()); // always 0
    }
}
