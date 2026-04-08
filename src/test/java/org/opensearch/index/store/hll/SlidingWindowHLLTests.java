/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hll;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import org.junit.Test;

public class SlidingWindowHLLTests {

    @Test
    public void testInitialization() {
        SlidingWindowHLL hll = new SlidingWindowHLL(9);
        assertEquals(96 * 1024, hll.getMatrixSizeBytes());
        assertTrue(hll.toString().contains("registerBits=9"));
    }

    @Test
    public void testUpdateAndEstimate() {
        SlidingWindowHLL hll = new SlidingWindowHLL(9);
        Random random = new Random(42);
        long now = System.currentTimeMillis() / 1000L;
        
        for (int i = 0; i < 100; i++) {
            hll.update(random.nextInt(), now);
        }
        
        long estimate = hll.estimateCardinality(60);
        assertTrue("Estimate should be 70-130, got " + estimate, estimate >= 70 && estimate <= 130);
    }

    @Test
    public void testZeroCardinality() {
        SlidingWindowHLL hll = new SlidingWindowHLL(9);
        assertEquals("Empty HLL should estimate 0", 0, hll.estimateCardinality(60));
    }

    @Test
    public void testDuplicateBlocks() {
        SlidingWindowHLL hll = new SlidingWindowHLL(9);
        long now = System.currentTimeMillis() / 1000L;
        
        for (int i = 0; i < 100; i++) {
            hll.update(12345, now);
        }
        
        long estimate = hll.estimateCardinality(60);
        assertTrue("Should estimate 1-5 blocks, got " + estimate, estimate >= 1 && estimate <= 5);
    }

    @Test
    public void testInvalidRegisterBits() {
        try {
            new SlidingWindowHLL(0);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("registerBits"));
        }
    }
}
