/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StaticConfigsTests {

    @Before
    public void setUp() {
        StaticConfigs.resetForTesting();
    }

    @After
    public void tearDown() {
        StaticConfigs.resetForTesting();
    }

    @Test
    public void testInitWithValidPowerOfTwo() {
        StaticConfigs.init(4096);
        assertEquals(4096, StaticConfigs.CACHE_BLOCK_SIZE);
        assertEquals(12, StaticConfigs.CACHE_BLOCK_SIZE_POWER);
        assertEquals(4095L, StaticConfigs.CACHE_BLOCK_MASK);
    }

    @Test
    public void testInitWithNullDefaultsTo1MB() {
        StaticConfigs.init(null);
        assertEquals(1 << 20, StaticConfigs.CACHE_BLOCK_SIZE);
        assertEquals(20, StaticConfigs.CACHE_BLOCK_SIZE_POWER);
        assertEquals((1 << 20) - 1L, StaticConfigs.CACHE_BLOCK_MASK);
    }

    @Test
    public void testInitRejectsNonPowerOfTwo() {
        try {
            StaticConfigs.init(3000);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must be a power of 2"));
        }
    }

    @Test
    public void testInitRejectsZero() {
        try {
            StaticConfigs.init(0);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testInitCalledTwiceThrows() {
        StaticConfigs.init(4096);
        try {
            StaticConfigs.init(8192);
            throw new AssertionError("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("already been called"));
        }
    }

    @Test
    public void testInitWithMinBlockSize() {
        StaticConfigs.init(512);
        assertEquals(512, StaticConfigs.CACHE_BLOCK_SIZE);
        assertEquals(9, StaticConfigs.CACHE_BLOCK_SIZE_POWER);
        assertEquals(511L, StaticConfigs.CACHE_BLOCK_MASK);
    }
}
