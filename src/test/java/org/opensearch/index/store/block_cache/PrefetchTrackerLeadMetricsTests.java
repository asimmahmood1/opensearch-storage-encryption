/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

public class PrefetchTrackerLeadMetricsTests {

    private PrefetchTracker tracker;
    private static final Path TEST_PATH = Paths.get("/test/file.dat");

    @Before
    public void setUp() {
        tracker = new PrefetchTracker(Runnable::run);
    }

    @Test
    public void testMarkCompletedAndCheckLeadHit() {
        BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, 0L);
        tracker.markCompleted(key);

        assertTrue("Should return true for completed block", tracker.checkLeadHit(key));
        assertEquals(1, tracker.getLeadHits());
        assertEquals(0, tracker.getLeadMisses());
    }

    @Test
    public void testCheckLeadHitConsumesEntry() {
        BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, 0L);
        tracker.markCompleted(key);

        assertTrue(tracker.checkLeadHit(key));
        assertFalse("Second call should return false — entry consumed", tracker.checkLeadHit(key));
        assertEquals("Should only count one lead hit", 1, tracker.getLeadHits());
    }

    @Test
    public void testCheckLeadHitReturnsFalseForUnknownBlock() {
        BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, 0L);
        assertFalse(tracker.checkLeadHit(key));
        assertEquals(0, tracker.getLeadHits());
    }

    @Test
    public void testIsInflight() {
        BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, 0L);
        assertFalse(tracker.isInflight(key));

        tracker.putIfAbsent(key);
        assertTrue(tracker.isInflight(key));

        tracker.remove(key);
        assertFalse(tracker.isInflight(key));
    }

    @Test
    public void testRecordLeadMiss() {
        tracker.recordLeadMiss();
        tracker.recordLeadMiss();
        assertEquals(2, tracker.getLeadMisses());
    }

    @Test
    public void testLeadMissWhenBlockIsInflight() {
        BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, 0L);
        tracker.putIfAbsent(key);

        assertTrue("Block should be in-flight", tracker.isInflight(key));
        tracker.recordLeadMiss();
        assertEquals(1, tracker.getLeadMisses());
    }

    @Test
    public void testCompletedSizeTracking() {
        BlockCacheKey key1 = new FileBlockCacheKey(TEST_PATH, 0L);
        BlockCacheKey key2 = new FileBlockCacheKey(TEST_PATH, 8192L);

        assertEquals(0, tracker.completedSize());
        tracker.markCompleted(key1);
        tracker.markCompleted(key2);
        assertEquals(2, tracker.completedSize());

        tracker.checkLeadHit(key1);
        assertEquals(1, tracker.completedSize());
    }

    @Test
    public void testResetStatsClearsLeadMetrics() {
        BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, 0L);
        tracker.markCompleted(key);
        tracker.checkLeadHit(key);
        tracker.recordLeadMiss();

        assertEquals(1, tracker.getLeadHits());
        assertEquals(1, tracker.getLeadMisses());

        tracker.resetStats();

        assertEquals(0, tracker.getLeadHits());
        assertEquals(0, tracker.getLeadMisses());
        assertEquals(0, tracker.completedSize());
    }

    @Test
    public void testStatsStringIncludesLeadMetrics() {
        tracker.markCompleted(new FileBlockCacheKey(TEST_PATH, 0L));
        tracker.checkLeadHit(new FileBlockCacheKey(TEST_PATH, 0L));
        tracker.recordLeadMiss();

        String stats = tracker.stats();
        assertTrue("Stats should contain leadHits", stats.contains("leadHits=1"));
        assertTrue("Stats should contain leadMisses", stats.contains("leadMisses=1"));
    }

    @Test
    public void testMultipleBlocksLeadHitTracking() {
        for (int i = 0; i < 10; i++) {
            BlockCacheKey key = new FileBlockCacheKey(TEST_PATH, i * 8192L);
            tracker.markCompleted(key);
        }
        assertEquals(10, tracker.completedSize());

        // Consume 7 of them
        for (int i = 0; i < 7; i++) {
            assertTrue(tracker.checkLeadHit(new FileBlockCacheKey(TEST_PATH, i * 8192L)));
        }
        assertEquals(7, tracker.getLeadHits());
        assertEquals(3, tracker.completedSize());
    }
}
