/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hll;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Sliding Window HyperLogLog implementation for estimating cardinality of unique blocks
 * accessed within a time window.
 *
 * Based on the paper: "Sliding HyperLogLog" (hal-00465313).
 * Reference: Aurora's C++ implementation in GroverStorageCommon/grover/util/sliding-window-hll.h
 *
 * Instead of storing simple bits in registers (standard HLL), this implementation stores
 * timestamps. When estimating cardinality for a time window, only registers with timestamps
 * within that window are counted.
 *
 * Thread-safe: Matrix updates are lock-free (last write wins). 64-bit long writes are
 * atomic on 64-bit JVMs, so no synchronization is needed.
 */
public class SlidingWindowHLL {
    private static final Logger logger = LogManager.getLogger(SlidingWindowHLL.class);

    /** Number of bits in the hash value (Java int = 32 bits) */
    private static final int HASH_LENGTH = 32;

    /**
     * Small range correction threshold multiplier.
     * When estimate <= 2.5 * numRegisters, HLL overestimates because many registers are empty.
     * Linear counting (based on empty register count) is more accurate in this range.
     * Reference: Flajolet et al., "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"
     */
    private static final double SMALL_RANGE_THRESHOLD_MULTIPLIER = 2.5;

    /**
     * Large range correction threshold divisor.
     * When estimate > 2^32 / 30, hash collisions in the 32-bit space cause underestimation.
     * Correction uses the coupon collector formula to adjust for collision probability.
     */
    private static final double LARGE_RANGE_THRESHOLD_DIVISOR = 30.0;

    /** Maximum value of 2^32 (full 32-bit hash space) */
    private static final long POW_2_32 = 0xFFFFFFFFL;

    /**
     * Alpha constant numerator for bias correction.
     * Derived from empirical analysis of HLL harmonic mean bias.
     * For m >= 128 registers: alpha ≈ 0.7213 / (1 + 1.079/m)
     */
    private static final double ALPHA_NUMERATOR = 0.7213;
    private static final double ALPHA_DENOMINATOR_FACTOR = 1.079;

    // Configuration
    private final int registerBits;
    private final int stochasticRange;
    private final int numRegisters;

    /** Pre-computed bias correction constant: alpha * m^2 */
    private final double alphaMM;

    /**
     * Matrix storing timestamps: registers[registerId * (stochasticRange + 1) + nLeadingZeros] = timestamp.
     * Plain long[] is used instead of AtomicLong[] because:
     * - 64-bit writes are atomic on 64-bit JVMs (JLS §17.7)
     * - Last-write-wins semantics are acceptable (same as Aurora's approach)
     * - Better cache locality and lower memory overhead
     */
    private final long[] registers;

    /**
     * Creates a new Sliding Window HLL estimator.
     *
     * @param registerBits Number of bits for register selection (1-31, recommended 8-13)
     */
    public SlidingWindowHLL(int registerBits) {
        if (registerBits < 1 || registerBits >= HASH_LENGTH) {
            throw new IllegalArgumentException("registerBits must be between 1 and " + (HASH_LENGTH - 1));
        }

        this.registerBits = registerBits;
        this.stochasticRange = HASH_LENGTH - registerBits;
        this.numRegisters = 1 << registerBits;

        double alpha = ALPHA_NUMERATOR / (1.0 + ALPHA_DENOMINATOR_FACTOR / numRegisters);
        this.alphaMM = alpha * numRegisters * numRegisters;

        int matrixSize = numRegisters * (stochasticRange + 1);
        this.registers = new long[matrixSize];

        logger.info(
            "Initialized SlidingWindowHLL: registerBits={}, stochasticRange={}, numRegisters={}, matrixSize={}, memoryBytes={}",
            registerBits, stochasticRange, numRegisters, matrixSize, matrixSize * 8L
        );
    }

    /**
     * Updates the HLL matrix with a new hash value.
     * Called on every block access (L1 hit, L2 hit, or cache miss).
     *
     * @param hashValue 32-bit hash of the accessed block (from FileBlockCacheKey.hashCode())
     * @param nowSeconds Current time in epoch seconds (use cached time for performance)
     */
    public void update(int hashValue, long nowSeconds) {
        // Lower bits select the register (bucket)
        int registerId = hashValue & ((1 << registerBits) - 1);

        // Leading zeros determine the position within the register
        int nLeadingZeros = Math.min(Integer.numberOfLeadingZeros(hashValue), stochasticRange);

        // Store timestamp at [registerId][nLeadingZeros]
        int index = registerId * (stochasticRange + 1) + nLeadingZeros;
        registers[index] = nowSeconds;
    }

    /**
     * Estimates the cardinality of unique elements accessed within the time window.
     *
     * @param windowSizeSeconds Size of the sliding window in seconds
     * @return Estimated number of unique elements (always >= 0)
     */
    public long estimateCardinality(long windowSizeSeconds) {
        long currentTime = System.currentTimeMillis() / 1000L;
        long intervalStart = currentTime - windowSizeSeconds;

        int zeroCount = 0;
        double sum = 0.0;

        // For each register, find the highest bit set within the time window
        // Worst case: 512 registers × 24 positions = 12,288 comparisons
        for (int registerId = 0; registerId < numRegisters; registerId++) {
            int registerValue = getValueForRegister(registerId, intervalStart);
            if (registerValue == 0) {
                zeroCount++;
            }
            sum += 1.0 / (1L << registerValue);
        }

        // Raw HLL estimate
        double result = alphaMM / sum;

        // Small range correction: use linear counting when many registers are empty
        if (result <= (SMALL_RANGE_THRESHOLD_MULTIPLIER * numRegisters) && zeroCount > 0) {
            result = numRegisters * Math.log((double) numRegisters / zeroCount);
        }
        // Large range correction: adjust for hash collision probability near 2^32
        else if (result > (POW_2_32 / LARGE_RANGE_THRESHOLD_DIVISOR)) {
            result = POW_2_32 * Math.log(1.0 / (1.0 - (result / POW_2_32)));
        }

        return Math.max(0, (long) result);
    }

    /**
     * Gets the register value (highest bit position with a recent timestamp) for a register.
     */
    private int getValueForRegister(int registerId, long intervalStart) {
        for (int entry = stochasticRange; entry >= 0; entry--) {
            int index = registerId * (stochasticRange + 1) + entry;
            if (registers[index] > intervalStart) {
                return entry + 1;
            }
        }
        return 0;
    }

    /**
     * Returns the size of the matrix in bytes.
     */
    public long getMatrixSizeBytes() {
        return (long) registers.length * 8L;
    }

    @Override
    public String toString() {
        return String.format(
            "SlidingWindowHLL[registerBits=%d, numRegisters=%d, stochasticRange=%d, memoryKB=%d]",
            registerBits, numRegisters, stochasticRange, getMatrixSizeBytes() / 1024
        );
    }
}
