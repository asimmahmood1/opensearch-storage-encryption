/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hll;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class WorkingSetEstimatorTests {

    @Before
    public void setUp() {
        WorkingSetEstimator.getInstance().resetForTesting();
        WorkingSetEstimator.getInstance().setEnabled(true);
    }

    @Test
    public void testSingletonInstance() {
        assertSame(WorkingSetEstimator.getInstance(), WorkingSetEstimator.getInstance());
    }

    @Test
    public void testUpdate() {
        long now = System.currentTimeMillis() / 1000L;
        WorkingSetEstimator.getInstance().update(12345, 0L);
        
        long estimate = WorkingSetEstimator.getInstance().estimateCardinality(60);
        assertTrue("Should have non-zero estimate", estimate > 0);
    }

    @Test
    public void testCacheCapacity() {
        assertEquals(0, WorkingSetEstimator.getInstance().getCacheCapacity());
        
        WorkingSetEstimator.getInstance().setCacheCapacity(1000);
        assertEquals(1000, WorkingSetEstimator.getInstance().getCacheCapacity());
    }

    @Test
    public void testReadAheadStatistics() {
        WorkingSetEstimator wse = WorkingSetEstimator.getInstance();
        
        assertEquals(0, wse.getReadAheadBlocksLoaded());
        assertEquals(0, wse.getReadAheadBlocksWasted());
        
        wse.recordReadAheadLoad(3);

        assertEquals(3, wse.getReadAheadBlocksLoaded());
        assertEquals(3, wse.getReadAheadBlocksWasted());
        
        wse.recordReadAheadAccess();
        assertEquals(2, wse.getReadAheadBlocksWasted());
        assertEquals(33.33, wse.getReadAheadHitRate(), 0.1);
    }

    @Test
    public void testReadAheadWastedBounded() {
        WorkingSetEstimator wse = WorkingSetEstimator.getInstance();
        
        // Simulate race: accessed > loaded
        wse.recordReadAheadAccess();
        assertTrue("Wasted should be >= 0", wse.getReadAheadBlocksWasted() >= 0);
    }

    @Test
    public void testBlocksToMB() {
        assertEquals(0, WorkingSetEstimator.blocksToMB(0));
        assertEquals(1, WorkingSetEstimator.blocksToMB(128)); // 128 * 8192 = 1MB
    }

    @Test
    public void testReset() {
        long now = System.currentTimeMillis() / 1000L;
        WorkingSetEstimator.getInstance().update(12345, 0L);
        
        WorkingSetEstimator.getInstance().resetForTesting();
        
        assertEquals(0, WorkingSetEstimator.getInstance().estimateCardinality(60));
    }
}
