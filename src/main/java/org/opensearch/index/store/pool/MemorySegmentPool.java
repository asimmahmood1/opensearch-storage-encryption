/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.pool;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.metrics.CryptoMetricsService;

/**
 * GC-managed pool for off-heap memory backed by direct ByteBuffers.
 *
 * <p>Every {@link #acquire()} call allocates a fresh {@link ByteBuffer#allocateDirect(int)}.
 * The JVM's internal {@code Bits.reserveMemory()} handles GC pressure and retry when
 * direct memory is exhausted.
 *
 * <p>A periodic GC debt monitor runs every second. When zombie buffers (evicted from
 * cache but not yet GC'd) accumulate and remaining capacity drops below 10%, it
 * triggers {@code System.gc()} to help reclaim phantom-reachable DirectByteBuffers.
 *
 * @opensearch.internal
 */
@SuppressWarnings("preview")
@SuppressForbidden(reason = "Uses ByteBuffer.allocateDirect for native memory allocation")
public class MemorySegmentPool implements Pool<RefCountedByteBuffer>, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MemorySegmentPool.class);
    private static final Cleaner CLEANER = Cleaner.create();

    private final int segmentSize;
    private final int maxSegments;
    private final long totalMemory;
    private final AtomicInteger buffersInUse = new AtomicInteger(0);
    private final LongAdder stallCount = new LongAdder();
    private final LongAdder gcTriggerCount = new LongAdder();

    private final Thread gcDebtMonitor;
    private final Runnable cleanerAction = buffersInUse::decrementAndGet;
    private final BufferPoolMXBean directMemoryMxBean;
    private volatile LongSupplier cacheEntriesSupplier = () -> 0;

    private volatile boolean closed = false;

    /** Register cache size supplier for GC debt monitoring. */
    public void setCacheEntriesSupplier(LongSupplier supplier) {
        this.cacheEntriesSupplier = supplier;
    }

    public MemorySegmentPool(long totalMemory, int segmentSize) {
        if (totalMemory % segmentSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of segment size");
        }
        this.directMemoryMxBean = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(p -> "direct".equals(p.getName()))
                .findFirst()
                .orElse(null);
        this.totalMemory = totalMemory;
        this.segmentSize = segmentSize;
        this.maxSegments = (int) (totalMemory / segmentSize);
        this.gcDebtMonitor = new Thread(this::gcDebtMonitorLoop, "pool-gc-debt-monitor");
        gcDebtMonitor.setDaemon(true);
        gcDebtMonitor.start();
    }

    public MemorySegmentPool(long totalMemory, int segmentSize, boolean requiresZeroing) {
        this(totalMemory, segmentSize);
    }

    @Override
    public RefCountedByteBuffer acquire() throws InterruptedException {
        if (closed) throw new IllegalStateException("Pool is closed");
        int current = buffersInUse.incrementAndGet();

        if (current <= maxSegments) {
            ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
            return wrapAndRegister(buf);
        }

        long t0 = System.nanoTime();
        stallCount.increment();
        ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
        LOGGER.info("Over capacity allocateDirect took {}ms (inUse={}, max={})",
                (System.nanoTime() - t0) / 1_000_000, current, maxSegments);
        return wrapAndRegister(buf);
    }

    @Override
    public RefCountedByteBuffer tryAcquire(long timeout, TimeUnit unit) throws Exception {
        if (closed) throw new IllegalStateException("Pool is closed");

        // Under 150% of max — allocate directly (50% headroom for GC lag)
        if (buffersInUse.get() < maxSegments + (maxSegments / 2)) {
            buffersInUse.incrementAndGet();
            ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
            return wrapAndRegister(buf);
        }

        // Over 150% — wait for GC to reclaim buffers, with timeout
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        stallCount.increment();
        while (true) {
            if (closed) throw new IllegalStateException("Pool is closed");

            if (buffersInUse.get() < maxSegments + (maxSegments / 2)) {
                buffersInUse.incrementAndGet();
                ByteBuffer buf = ByteBuffer.allocateDirect(segmentSize).order(ByteOrder.LITTLE_ENDIAN);
                return wrapAndRegister(buf);
            }

            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new IOException(
                    "Pool acquisition timed out after " + unit.toMillis(timeout) + "ms"
                        + " (inUse=" + buffersInUse.get() + ", max=" + maxSegments + ", limit=" + (maxSegments + maxSegments / 2) + ")"
                );
            }

            // Brief sleep and let GC/Cleaner reclaim buffers
            Thread.sleep(Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNanos), 10));
        }
    }

    private RefCountedByteBuffer wrapAndRegister(ByteBuffer direct) {
        RefCountedByteBuffer wrapper = new RefCountedByteBuffer(direct, segmentSize);
        CLEANER.register(wrapper, cleanerAction);
        return wrapper;
    }

    private void gcDebtMonitorLoop() {
        while (!closed) {
            try {
                Thread.sleep(1000);
                checkGcDebt();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOGGER.warn("Error in GC debt monitor", t);
            }
        }
    }

    private void checkGcDebt() {
        if (closed) return;
        int inUse = buffersInUse.get();
        long cacheEntries = cacheEntriesSupplier.getAsLong();
        long zombies = inUse - cacheEntries;
        int remaining = maxSegments - inUse;

        if (zombies > 0 && remaining < maxSegments / 10) {
            LOGGER.info("GC debt: inUse={}, cacheEntries={}, zombies={}, remaining={}/{} — triggering GC",
                inUse, cacheEntries, zombies, remaining, maxSegments);
            System.gc();
            gcTriggerCount.increment();
        }
    }

    @Override
    public void release(RefCountedByteBuffer refSegment) {
        // No-op: Cleaner handles lifecycle when wrapper is GC'd.
    }

    public void releaseAll(RefCountedByteBuffer... segments) {
        // No-op
    }

    @Override
    public long totalMemory() { return totalMemory; }

    @Override
    public long availableMemory() {
        return (long) Math.max(0, maxSegments - buffersInUse.get()) * segmentSize;
    }

    @Override
    public int pooledSegmentSize() { return segmentSize; }

    public int getBuffersInUse() { return buffersInUse.get(); }

    public long getAllocatedBytes() { return (long) buffersInUse.get() * segmentSize; }

    @Override
    public boolean isUnderPressure() {
        return buffersInUse.get() >= (int) (maxSegments * 0.95);
    }

    @Override
    public void warmUp(long targetSegments) {
        LOGGER.info("warmUp is no-op with byte buffers");
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        gcDebtMonitor.interrupt();
        try {
            gcDebtMonitor.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("MemorySegmentPool closed");
    }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public String poolStats() {
        int inUse = getBuffersInUse();
        long trackedBytes = getAllocatedBytes();
        long nativeUsed = getDirectMemoryUsedInternal();
        long zombieBytes = nativeUsed >= 0 ? nativeUsed - trackedBytes : -1;
        return String.format(
            "PoolStats[max=%d, inUse=%d, utilization=%.1f%%, stalls=%d, tracked=%dMB, native=%dMB, zombie=%dMB]",
            maxSegments, inUse, maxSegments > 0 ? (double) inUse / maxSegments * 100 : 0,
            stallCount.sum(), trackedBytes / (1024 * 1024),
            nativeUsed / (1024 * 1024), zombieBytes / (1024 * 1024));
    }

    @Override
    public void recordStats() {
        int inUse = buffersInUse.get();
        long allocatedBytes = (long) inUse * segmentSize;
        long nativeUsed = getDirectMemoryUsedInternal();
        long zombieBytes = nativeUsed >= 0 ? Math.max(0, nativeUsed - allocatedBytes) : 0;
        double pct = maxSegments > 0 ? (double) inUse / maxSegments : 0;
        long stalls = stallCount.sum();
        long gcTriggers = gcTriggerCount.sum();
        LOGGER.debug("PoolMetrics[max={}, inUse={}, allocated={}MB, native={}MB, zombie={}MB, stalls={}, gcTriggers={}]",
            maxSegments, inUse,
            allocatedBytes / (1024 * 1024), nativeUsed / (1024 * 1024), zombieBytes / (1024 * 1024),
            stalls, gcTriggers);
        CryptoMetricsService.getInstance().recordPoolStats(
            SegmentType.PRIMARY, maxSegments, inUse, allocatedBytes, zombieBytes, pct, stalls, gcTriggers
        );
    }

    private long getDirectMemoryUsedInternal() {
        return directMemoryMxBean != null ? directMemoryMxBean.getMemoryUsed() : -1;
    }

    public long getDirectMemoryUsed() {
        return getDirectMemoryUsedInternal();
    }

    public long getStallCount() {
        return stallCount.sum();
    }

    public long getGcTriggerCount() {
        return gcTriggerCount.sum();
    }
}
