/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hll;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Scheduler that periodically estimates working set cardinality and logs the results.
 * Supports dynamic configuration of poll interval and window sizes.
 */
public class WorkingSetEstimatorScheduler implements Closeable {
    private static final Logger logger = LogManager.getLogger(WorkingSetEstimatorScheduler.class);

    private static final int DEFAULT_POLL_INTERVAL_SECONDS = 10;
    private static final int DEFAULT_WINDOW_RECENT_SECONDS = 300;
    private static final int DEFAULT_WINDOW_EXTENDED_SECONDS = 900;

    /** Counter for estimation failures - can be used for alarming */
    private final AtomicLong estimationFailureCount = new AtomicLong(0);

    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledTask;
    private volatile int currentIntervalSeconds = DEFAULT_POLL_INTERVAL_SECONDS;
    private volatile int windowRecentSeconds = DEFAULT_WINDOW_RECENT_SECONDS;
    private volatile int windowExtendedSeconds = DEFAULT_WINDOW_EXTENDED_SECONDS;

    public WorkingSetEstimatorScheduler(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Starts the periodic estimation task.
     */
    public synchronized void start() {
        if (scheduledTask != null) {
            logger.warn("WorkingSetEstimatorScheduler already started");
            return;
        }

        logger.info("Starting WorkingSetEstimatorScheduler with interval: {}s", currentIntervalSeconds);
        scheduledTask = threadPool.scheduleWithFixedDelay(
            this::estimateAndLog,
            TimeValue.timeValueSeconds(currentIntervalSeconds),
            ThreadPool.Names.GENERIC
        );
    }

    /**
     * Stops the periodic estimation task.
     */
    public synchronized void stop() {
        if (scheduledTask != null) {
            scheduledTask.cancel();
            scheduledTask = null;
            logger.info("Stopped WorkingSetEstimatorScheduler");
        }
    }

    /**
     * Updates the poll interval. Restarts the scheduler if interval changed.
     */
    public synchronized void updateInterval(int intervalSeconds) {
        if (intervalSeconds == currentIntervalSeconds) return;
        logger.info("Updating poll interval from {}s to {}s", currentIntervalSeconds, intervalSeconds);
        currentIntervalSeconds = intervalSeconds;
        if (scheduledTask != null) {
            stop();
            start();
        }
    }

    /**
     * Sets the short sliding window length.
     */
    public void setWindowRecentSeconds(int windowSeconds) {
        if (windowSeconds != windowRecentSeconds) {
            logger.info("Updating short window from {}s to {}s", windowRecentSeconds, windowSeconds);
            windowRecentSeconds = windowSeconds;
        }
    }

    /**
     * Sets the long sliding window length.
     */
    public void setWindowExtendedSeconds(int windowSeconds) {
        if (windowSeconds != windowExtendedSeconds) {
            logger.info("Updating long window from {}s to {}s", windowExtendedSeconds, windowSeconds);
            windowExtendedSeconds = windowSeconds;
        }
    }

    /**
     * Gets the number of estimation failures (for alarming).
     */
    public long getEstimationFailureCount() {
        return estimationFailureCount.get();
    }

    private void estimateAndLog() {
        try {
            WorkingSetEstimator wse = WorkingSetEstimator.getInstance();
            if (!wse.isEnabled()) return;

            long estimateShort = wse.estimateCardinality(windowRecentSeconds);
            long estimateLong = wse.estimateCardinality(windowExtendedSeconds);

            long cacheCapacityBlocks = wse.getCacheCapacity();
            long cacheCapacityMB = WorkingSetEstimator.blocksToMB(cacheCapacityBlocks);

            double memoryRequirementPercent = cacheCapacityBlocks > 0
                ? (double) estimateShort / cacheCapacityBlocks * 100.0
                : 0.0;

            long raLoaded = wse.getReadAheadBlocksLoaded();
            long raWasted = wse.getReadAheadBlocksWasted();
            double raHitRate = wse.getReadAheadHitRate();

            String scalingRecommendation = "";
            if (memoryRequirementPercent > 100.0) {
                scalingRecommendation = " | SCALE UP: Memory demand exceeds cache capacity";
            } else if (memoryRequirementPercent < 50.0 && estimateShort > 0) {
                scalingRecommendation = " | SCALE DOWN: Memory demand is low";
            }

            logger.debug(
                "Working Set Estimates: recent[{}s]=[blocks={}, sizeMB={}, {}% of cache], "
                    + "extended[{}s]=[blocks={}, sizeMB={}] | "
                    + "Cache: capacityMB={} | ReadAhead: loaded={}, wasted={}, hitRate={}%{}",
                windowRecentSeconds, estimateShort, WorkingSetEstimator.blocksToMB(estimateShort),
                String.format("%.1f", memoryRequirementPercent),
                windowExtendedSeconds, estimateLong, WorkingSetEstimator.blocksToMB(estimateLong),
                cacheCapacityMB,
                raLoaded, raWasted, String.format("%.2f", raHitRate),
                scalingRecommendation
            );
        } catch (Exception e) {
            estimationFailureCount.incrementAndGet();
            logger.error("Failed to estimate working set cardinality (failureCount={})",
                estimationFailureCount.get(), e);
        }
    }

    @Override
    public void close() {
        stop();
    }
}
