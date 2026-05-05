/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.store.PanamaNativeAccess;

/**
 * Static configuration constants for the encrypted storage buffer pool and Direct I/O operations.
 *
 * <p>Cache block size is configurable via {@code juno.storage_encryption.block_size} DynamicConfig
 * (defined in JunoSettings). Call {@link #init(Integer)} from createComponents to apply the configured value.
 * If not explicitly set, defaults to 1MB.
 */
public class StaticConfigs {

    // Prevent instantiation
    private StaticConfigs() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    private static final Logger LOGGER = LogManager.getLogger(StaticConfigs.class);

    public static final int MIN_CACHE_BLOCK_SIZE = 512;

    private static final int DEFAULT_CACHE_BLOCK_SIZE_POWER = 20; // 1MB default

    /** 
     * Alignment requirement for Direct I/O operations in bytes.
     * Must be at least 512 bytes or the system page size, whichever is larger.
     */
    public static final int DIRECT_IO_ALIGNMENT = Math.max(512, getPageSizeSafe());

    /** 
     * Power of 2 for Direct I/O write buffer size (2^18 = 256KB).
     */
    public static final int DIRECT_IO_WRITE_BUFFER_SIZE_POWER = 18;

    /** 
     * Power of 2 for cache block size. Computed from CACHE_BLOCK_SIZE.
     */
    public static volatile int CACHE_BLOCK_SIZE_POWER = DEFAULT_CACHE_BLOCK_SIZE_POWER;

    /** 
     * Size of each cache block in bytes.
     */
    public static volatile int CACHE_BLOCK_SIZE = 1 << DEFAULT_CACHE_BLOCK_SIZE_POWER;

    /**
     * Bit mask for cache block alignment (block_size - 1).
     */
    public static volatile long CACHE_BLOCK_MASK = CACHE_BLOCK_SIZE - 1;

    /**
     * Whether to populate the block cache during writes.
     * Disable to reduce direct memory pressure during heavy indexing.
     */
    public static volatile boolean CACHE_ON_WRITE = false;

    private static final int DEFAULT_BLOCK_SIZE = 1 << DEFAULT_CACHE_BLOCK_SIZE_POWER; // 1MB

    private static volatile boolean initialized = false;

    /**
     * Initializes block size from the resolved DynamicConfig value.
     * Must be called exactly once from {@code CryptoDirectoryPlugin.createComponents}.
     * Falls back to 1MB default if dynamicConfigValue is null.
     *
     * @throws IllegalStateException if called more than once
     */
    public static void init(Integer dynamicConfigValue) {
        if (initialized) {
            throw new IllegalStateException("StaticConfigs.init() has already been called; block size cannot be re-initialized");
        }
        initialized = true;
        int blockSize;
        if (dynamicConfigValue != null) {
            blockSize = dynamicConfigValue;
            LOGGER.info("Storage encryption block size: source=DynamicConfig, value={}", blockSize);
        } else {
            blockSize = DEFAULT_BLOCK_SIZE;
            LOGGER.info("Storage encryption block size: source=local default, value={}", blockSize);
        }
        if (Integer.bitCount(blockSize) != 1) {
            throw new IllegalArgumentException("juno.storage_encryption.block_size must be a power of 2, got: " + blockSize);
        }
        CACHE_BLOCK_SIZE = blockSize;
        CACHE_BLOCK_SIZE_POWER = Integer.numberOfTrailingZeros(blockSize);
        CACHE_BLOCK_MASK = blockSize - 1L;
        LOGGER.info("Storage encryption block size set to {} bytes (2^{})", CACHE_BLOCK_SIZE, CACHE_BLOCK_SIZE_POWER);
    }

    /** For testing only — resets initialization state so {@link #init(Integer)} can be called again. */
    public static void resetForTesting() {
        initialized = false;
    }

    private static int getPageSizeSafe() {
        try {
            return PanamaNativeAccess.getPageSize();
        } catch (Throwable e) {
            // Native access not available (class initialization failed, native library not found, etc.)
            // Fall back to common page size
            return 4096;
        }
    }
}
