/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.hll;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.hash.MurmurHash3;

/**
 * Tests for the hashing logic in WorkingSetEstimator.update(pathHashCode, fileOffset).
 *
 * The update method constructs: combined = ((long) pathHashCode << 32) | (fileOffset >>> 13)
 * then hashes with MurmurHash3.murmur64(combined) and feeds the result into the HLL.
 */
public class WorkingSetEstimatorHashingTests {

    private static final long BLOCK_SIZE = 8192L; // 2^13

    private WorkingSetEstimator wse;

    @Before
    public void setUp() {
        wse = WorkingSetEstimator.getInstance();
        wse.resetForTesting();
        wse.setEnabled(true);
    }

    // ---- Combined key construction tests ----

    @Test
    public void testDifferentPathsSameOffsetProduceDistinctKeys() {
        int path1 = "/index/segment_0".hashCode();
        int path2 = "/index/segment_1".hashCode();
        long offset = 8192L;

        long combined1 = ((long) path1 << 32) | (offset >>> 13);
        long combined2 = ((long) path2 << 32) | (offset >>> 13);

        assertTrue("Different paths must produce different combined keys", combined1 != combined2);
    }

    @Test
    public void testSamePathDifferentOffsetsProduceDistinctKeys() {
        int pathHash = "/index/segment_0".hashCode();
        long offset1 = 0L;
        long offset2 = BLOCK_SIZE;

        long combined1 = ((long) pathHash << 32) | (offset1 >>> 13);
        long combined2 = ((long) pathHash << 32) | (offset2 >>> 13);

        assertTrue("Different offsets must produce different combined keys", combined1 != combined2);
    }

    @Test
    public void testOffsetsWithinSameBlockProduceSameBlockNumber() {
        // Offsets 0..8191 are all in block 0
        assertEquals("Offset 0 should be block 0", 0, 0L >>> 13);
        assertEquals("Offset 4096 should be block 0", 0, 4096L >>> 13);
        assertEquals("Offset 8191 should be block 0", 0, 8191L >>> 13);

        // Offset 8192 is block 1
        assertEquals("Offset 8192 should be block 1", 1, 8192L >>> 13);
    }

    @Test
    public void testBlockNumberExtractionForLargeOffsets() {
        // 1 GB offset = 131072 blocks
        long gbOffset = 1073741824L;
        assertEquals(131072, gbOffset >>> 13);

        // 1 TB offset
        long tbOffset = 1L << 40;
        assertEquals(1L << 27, tbOffset >>> 13);
    }

    // #11 fix: end-to-end test for large offsets through the estimator
    @Test
    public void testLargeOffsetsCountedCorrectlyEndToEnd() {
        int pathHash = "/large/file".hashCode();
        // Insert 100 blocks starting at a 1GB offset
        long baseOffset = 1073741824L;
        for (int i = 0; i < 100; i++) {
            wse.update(pathHash, baseOffset + i * BLOCK_SIZE);
        }
        long estimate = wse.estimateCardinality(60);
        assertTrue("Expected ~100 for large-offset blocks, got " + estimate, estimate >= 80 && estimate <= 120);
    }

    // #4 fix: 0xDEADBEEF needs cast to int
    @Test
    public void testCombinedKeyPreservesPathInUpperBits() {
        int pathHash = (int) 0xDEADBEEFL; // -559038737
        long offset = BLOCK_SIZE * 5; // block 5

        long combined = ((long) pathHash << 32) | (offset >>> 13);

        // Upper 32 bits should be the path hash
        assertEquals(pathHash, (int) (combined >>> 32));
        // Lower 32 bits should be the block number
        assertEquals(5L, combined & 0xFFFFFFFFL);
    }

    @Test
    public void testCombinedKeyWithNegativePathHash() {
        int negativePath = -12345;
        long offset = BLOCK_SIZE * 3;

        long combined = ((long) negativePath << 32) | (offset >>> 13);

        assertEquals(negativePath, (int) (combined >>> 32));
        assertEquals(3L, combined & 0xFFFFFFFFL);
    }

    // #10 fix: actually call wse.update() end-to-end for zero inputs
    @Test
    public void testZeroPathHashAndZeroOffset() {
        long combined = ((long) 0 << 32) | (0L >>> 13);
        assertEquals(0L, combined);

        // Murmur hash of 0 should produce a usable value (no exception)
        int hash = (int) MurmurHash3.murmur64(0L);

        // End-to-end: estimator should handle (0, 0) without error
        wse.update(0, 0L);
        long estimate = wse.estimateCardinality(60);
        assertTrue("Zero inputs should still register as 1 block, got " + estimate, estimate >= 1 && estimate <= 3);
    }

    // ---- Murmur hash distribution tests ----

    // #5 fix: test distribution with varying paths AND varying blocks
    @Test
    public void testMurmurHashDistributionAcrossHLLRegisters() {
        int registerBits = 9;
        int numRegisters = 1 << registerBits;
        Set<Integer> hitRegisters = new HashSet<>();

        // Vary both path and block to exercise upper and lower bits of combined key
        String[] paths = {"/data/index", "/seg/0", "/seg/1", "/logs/wal", "/meta/state"};
        for (String path : paths) {
            int pathHash = path.hashCode();
            for (int block = 0; block < 2000; block++) {
                long combined = ((long) pathHash << 32) | (long) block;
                int hllHash = (int) MurmurHash3.murmur64(combined);
                int registerId = hllHash & (numRegisters - 1);
                hitRegisters.add(registerId);
            }
        }

        // 10000 keys into 512 buckets: expect nearly all buckets hit
        double coverage = (double) hitRegisters.size() / numRegisters;
        assertTrue("Expected >95% register coverage, got " + (coverage * 100) + "%", coverage > 0.95);
    }

    @Test
    public void testMurmurHashProducesNonZeroLeadingZeros() {
        int maxLeadingZeros = 0;
        int pathHash = "/test/path".hashCode();

        for (int block = 0; block < 100000; block++) {
            long combined = ((long) pathHash << 32) | (long) block;
            int hllHash = (int) MurmurHash3.murmur64(combined);
            int lz = Integer.numberOfLeadingZeros(hllHash);
            maxLeadingZeros = Math.max(maxLeadingZeros, lz);
        }

        assertTrue("Expected max leading zeros > 5, got " + maxLeadingZeros, maxLeadingZeros > 5);
    }

    // #6 fix: tighten uniqueness threshold to >99.9%
    @Test
    public void testDistinctInputsProduceDistinctHashes() {
        Set<Integer> hashes = new HashSet<>();
        int pathHash = "/shard/segment".hashCode();

        for (int block = 0; block < 10000; block++) {
            long combined = ((long) pathHash << 32) | (long) block;
            int hllHash = (int) MurmurHash3.murmur64(combined);
            hashes.add(hllHash);
        }

        // Birthday paradox: 10000 inputs into 2^32 space ≈ 0.012 expected collisions
        double uniqueRatio = (double) hashes.size() / 10000;
        assertTrue("Expected >99.9% unique hashes, got " + (uniqueRatio * 100) + "%", uniqueRatio > 0.999);
    }

    // #12 fix: verify different (path, offset) pairs produce different Murmur hashes
    @Test
    public void testCrossFileHashUniqueness() {
        Set<Integer> hashes = new HashSet<>();
        String[] files = {"/fileA", "/fileB", "/fileC", "/fileD", "/fileE"};

        for (String file : files) {
            int pathHash = file.hashCode();
            for (int block = 0; block < 2000; block++) {
                long combined = ((long) pathHash << 32) | (long) block;
                int hllHash = (int) MurmurHash3.murmur64(combined);
                hashes.add(hllHash);
            }
        }

        // 10000 total inputs, expect >99.9% unique
        double uniqueRatio = (double) hashes.size() / 10000;
        assertTrue("Expected >99.9% unique cross-file hashes, got " + (uniqueRatio * 100) + "%", uniqueRatio > 0.999);
    }

    // ---- HLL register-level bucketization tests ----
    // These replicate the exact production flow:
    //   BlockSlotTinyCache: pathHash = path.hashCode() [cached at construction]
    //   WorkingSetEstimator.update(pathHash, blockOff):
    //     combined = ((long) pathHashCode << 32) | (fileOffset >>> 13)
    //     hllHash = (int) MurmurHash3.murmur64(combined)
    //   SlidingWindowHLL.update(hllHash, timestamp):
    //     registerId = hllHash & ((1 << 9) - 1)          // lower 9 bits → 0..511
    //     nLeadingZeros = min(nlz(hllHash), 23)           // stochasticRange = 32 - 9
    //     registers[registerId * 24 + nLeadingZeros] = timestamp

    private static final int REGISTER_BITS = 9;
    private static final int NUM_REGISTERS = 1 << REGISTER_BITS;       // 512
    private static final int STOCHASTIC_RANGE = 32 - REGISTER_BITS;    // 23

    /**
     * Replicate the exact hash pipeline from BlockSlotTinyCache → WorkingSetEstimator → SlidingWindowHLL.
     * Returns the (registerId, nLeadingZeros) pair that a block access would write to.
     */
    private static int[] replicateHLLPipeline(int pathHash, long blockOff) {
        // Step 1: WorkingSetEstimator.update() — combine and murmur
        long combined = ((long) pathHash << 32) | (blockOff >>> 13);
        int hllHash = (int) MurmurHash3.murmur64(combined);

        // Step 2: SlidingWindowHLL.update() — extract register and leading zeros
        int registerId = hllHash & (NUM_REGISTERS - 1);
        int nLeadingZeros = Math.min(Integer.numberOfLeadingZeros(hllHash), STOCHASTIC_RANGE);

        return new int[]{registerId, nLeadingZeros};
    }

    /**
     * Encodes (registerId, nLeadingZeros) into a single int for collision tracking.
     * This is the exact matrix index used by SlidingWindowHLL:
     *   index = registerId * (stochasticRange + 1) + nLeadingZeros
     */
    private static int matrixIndex(int[] registerPos) {
        return registerPos[0] * (STOCHASTIC_RANGE + 1) + registerPos[1];
    }

    @Test
    public void testRegisterCollisionRateForSingleFile() {
        // Simulate 10000 sequential block accesses on one file.
        // In HLL, multiple blocks can share the same (register, nlz) cell — this is expected
        // because nLeadingZeros is geometrically distributed (50% have 0, 25% have 1, etc.).
        // What matters is that blocks spread across many *registers*, not many cells.
        // Here we verify that the per-register collision rate is bounded:
        // no single register should absorb a disproportionate share of blocks.
        java.nio.file.Path filePath = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_3.cfs");
        int pathHash = filePath.hashCode();

        int[] registerCounts = new int[NUM_REGISTERS];
        int totalBlocks = 10000;

        for (int block = 0; block < totalBlocks; block++) {
            long blockOff = block * BLOCK_SIZE;
            int[] pos = replicateHLLPipeline(pathHash, blockOff);
            registerCounts[pos[0]]++;
        }

        // Expected ~19.5 blocks per register. No register should have > 3x expected.
        double expected = (double) totalBlocks / NUM_REGISTERS;
        int max = 0;
        for (int count : registerCounts) {
            max = Math.max(max, count);
        }
        assertTrue("Max register count " + max + " too high vs expected " + expected,
            max < expected * 3);
    }

    @Test
    public void testRegisterCollisionRateAcrossMultipleFiles() {
        // Simulate accesses across 10 files, 1000 blocks each = 10000 total distinct blocks.
        // Verify that different files spread across registers evenly and don't cluster.
        String[] files = {
            "/mnt/data/indices/abc123/0/index/_0.cfs",
            "/mnt/data/indices/abc123/0/index/_1.cfs",
            "/mnt/data/indices/abc123/0/index/_2.cfs",
            "/mnt/data/indices/abc123/0/index/_3.cfs",
            "/mnt/data/indices/abc123/0/index/_4.si",
            "/mnt/data/indices/abc123/0/index/_5.si",
            "/mnt/data/indices/def456/0/index/_0.cfs",
            "/mnt/data/indices/def456/0/index/_1.cfs",
            "/mnt/data/indices/def456/1/index/_0.cfs",
            "/mnt/data/indices/ghi789/0/index/_0.cfs"
        };

        int[] registerCounts = new int[NUM_REGISTERS];
        int blocksPerFile = 1000;
        int totalBlocks = files.length * blocksPerFile;

        Set<Integer> usedRegisters = new HashSet<>();
        for (String file : files) {
            int pathHash = java.nio.file.Path.of(file).hashCode();
            for (int block = 0; block < blocksPerFile; block++) {
                long blockOff = block * BLOCK_SIZE;
                int[] pos = replicateHLLPipeline(pathHash, blockOff);
                registerCounts[pos[0]]++;
                usedRegisters.add(pos[0]);
            }
        }

        // All 512 registers should be used
        double utilization = (double) usedRegisters.size() / NUM_REGISTERS;
        assertTrue("Expected >95% register utilization across files, got " + (utilization * 100) + "%",
            utilization > 0.95);

        // No register should have > 3x expected count
        double expected = (double) totalBlocks / NUM_REGISTERS;
        int max = 0;
        for (int count : registerCounts) {
            max = Math.max(max, count);
        }
        assertTrue("Max register count " + max + " too high vs expected " + expected,
            max < expected * 3);
    }

    @Test
    public void testRegisterUtilization() {
        // With 10000 distinct blocks, check how many of the 512 registers are actually used.
        // Poor hashing would cluster into a subset of registers.
        java.nio.file.Path filePath = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_3.cfs");
        int pathHash = filePath.hashCode();

        Set<Integer> usedRegisters = new HashSet<>();
        for (int block = 0; block < 10000; block++) {
            long blockOff = block * BLOCK_SIZE;
            int[] pos = replicateHLLPipeline(pathHash, blockOff);
            usedRegisters.add(pos[0]);
        }

        // 10000 blocks into 512 registers: expect nearly all registers used
        double utilization = (double) usedRegisters.size() / NUM_REGISTERS;
        assertTrue("Expected >95% register utilization, got " + (utilization * 100) + "%",
            utilization > 0.95);
    }

    @Test
    public void testRegisterBalanceIsUniform() {
        // Verify that blocks are roughly evenly distributed across registers.
        // Count how many blocks land in each register and check the max/min ratio.
        java.nio.file.Path filePath = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_3.cfs");
        int pathHash = filePath.hashCode();

        int[] registerCounts = new int[NUM_REGISTERS];
        int totalBlocks = 50000;

        for (int block = 0; block < totalBlocks; block++) {
            long blockOff = block * BLOCK_SIZE;
            int[] pos = replicateHLLPipeline(pathHash, blockOff);
            registerCounts[pos[0]]++;
        }

        // Expected count per register: 50000 / 512 ≈ 97.6
        double expected = (double) totalBlocks / NUM_REGISTERS;
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
        for (int count : registerCounts) {
            min = Math.min(min, count);
            max = Math.max(max, count);
        }

        // Max should be no more than 3x expected (very generous; good hash gives ~1.5x)
        assertTrue("Max register count " + max + " too high vs expected " + expected,
            max < expected * 3);
        // Min should be at least 1 (all registers used)
        assertTrue("Some registers are empty, min count = " + min, min >= 1);
    }

    @Test
    public void testSameBlockNeverInTwoRegisters() {
        // The core invariant: the same (path, offset) must always hash to the exact same
        // (registerId, nLeadingZeros) pair. If it doesn't, the HLL would double-count.
        java.nio.file.Path filePath = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_3.cfs");
        int pathHash = filePath.hashCode();

        for (int block = 0; block < 5000; block++) {
            long blockOff = block * BLOCK_SIZE;

            int[] pos1 = replicateHLLPipeline(pathHash, blockOff);
            int[] pos2 = replicateHLLPipeline(pathHash, blockOff);

            assertEquals("Block " + block + " register mismatch on repeated hash",
                pos1[0], pos2[0]);
            assertEquals("Block " + block + " leading zeros mismatch on repeated hash",
                pos1[1], pos2[1]);
        }
    }

    @Test
    public void testSameOffsetDifferentFilesLandInDifferentCells() {
        // Two files accessing the same block offset should NOT collide in the HLL.
        // Check that for block 0..999, the two files rarely share the same matrix cell.
        java.nio.file.Path fileA = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_0.cfs");
        java.nio.file.Path fileB = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_1.cfs");
        int pathHashA = fileA.hashCode();
        int pathHashB = fileB.hashCode();

        int collisions = 0;
        int totalBlocks = 1000;

        for (int block = 0; block < totalBlocks; block++) {
            long blockOff = block * BLOCK_SIZE;
            int cellA = matrixIndex(replicateHLLPipeline(pathHashA, blockOff));
            int cellB = matrixIndex(replicateHLLPipeline(pathHashB, blockOff));
            if (cellA == cellB) {
                collisions++;
            }
        }

        // With 12288 possible cells, random collision rate ≈ 1/12288 per pair ≈ 0.08%
        // Allow up to 5% to be safe
        double collisionRate = (double) collisions / totalBlocks;
        assertTrue("Cross-file collision rate too high: " + (collisionRate * 100) + "%",
            collisionRate < 0.05);
    }

    @Test
    public void testEndToEndBucketizationMatchesEstimate() {
        // Feed 1000 distinct blocks through the real WorkingSetEstimator,
        // then independently replay the hash pipeline and verify register usage.
        java.nio.file.Path filePath = java.nio.file.Path.of("/mnt/data/indices/abc123/0/index/_3.cfs");
        int pathHash = filePath.hashCode();
        int totalBlocks = 1000;

        // Feed through real estimator
        for (int block = 0; block < totalBlocks; block++) {
            wse.update(pathHash, block * BLOCK_SIZE);
        }
        long estimate = wse.estimateCardinality(60);

        // The estimate should be close to totalBlocks (±15%)
        assertTrue("Estimate " + estimate + " too far from " + totalBlocks,
            estimate >= totalBlocks * 0.85 && estimate <= totalBlocks * 1.15);

        // Independently verify register utilization — with 1000 blocks into 512 registers,
        // nearly all registers should be hit
        Set<Integer> usedRegisters = new HashSet<>();
        for (int block = 0; block < totalBlocks; block++) {
            int[] pos = replicateHLLPipeline(pathHash, block * BLOCK_SIZE);
            usedRegisters.add(pos[0]);
        }

        double utilization = (double) usedRegisters.size() / NUM_REGISTERS;
        assertTrue("Expected >85% register utilization, got " + (utilization * 100) + "%",
            utilization > 0.85);
    }

    // ---- End-to-end cardinality estimation tests ----

    // Verify same (pathHash, offset) always produces the same HLL hash,
    // ensuring a unique block never lands in two different registers.
    @Test
    public void testSameBlockAlwaysMapsToSameRegister() {
        int registerBits = 9;
        int numRegisters = 1 << registerBits;
        int pathHash = "/data/segment_0".hashCode();

        for (long offset = 0; offset < 500 * BLOCK_SIZE; offset += BLOCK_SIZE) {
            long combined = ((long) pathHash << 32) | (offset >>> 13);
            int hash1 = (int) MurmurHash3.murmur64(combined);
            int hash2 = (int) MurmurHash3.murmur64(combined);

            // Same input must always produce the same register and leading-zero count
            assertEquals("Register must be deterministic for offset " + offset,
                hash1 & (numRegisters - 1), hash2 & (numRegisters - 1));
            assertEquals("Leading zeros must be deterministic for offset " + offset,
                Integer.numberOfLeadingZeros(hash1), Integer.numberOfLeadingZeros(hash2));
        }
    }

    // #2 fix: use a known non-zero path hash to avoid degenerate case
    @Test
    public void testSingleBlockAccessEstimatesOne() {
        wse.update("/data/segment_0".hashCode(), 0L);
        long estimate = wse.estimateCardinality(60);
        assertTrue("Single access should estimate ~1, got " + estimate, estimate >= 1 && estimate <= 3);
    }

    // #9 fix: test repeated access with both zero and non-zero offsets
    @Test
    public void testRepeatedSameBlockDoesNotInflateEstimate() {
        int pathHash = "/data/segment_0".hashCode();
        // Repeat block 0
        for (int i = 0; i < 500; i++) {
            wse.update(pathHash, 0L);
        }
        // Repeat block 5
        for (int i = 0; i < 500; i++) {
            wse.update(pathHash, 5 * BLOCK_SIZE);
        }
        long estimate = wse.estimateCardinality(60);
        // Two distinct blocks repeated 500 times each should estimate ~2
        assertTrue("Repeated two blocks should estimate ~2, got " + estimate, estimate >= 1 && estimate <= 4);
    }

    // #7 fix: tighten tolerance to ±20%
    @Test
    public void testKnownCardinalitySmall() {
        int pathHash = "/data/segment_0".hashCode();
        for (int i = 0; i < 100; i++) {
            wse.update(pathHash, i * BLOCK_SIZE);
        }
        long estimate = wse.estimateCardinality(60);
        assertTrue("Expected ~100, got " + estimate, estimate >= 80 && estimate <= 120);
    }

    // #8 fix: consistent tolerance that tightens with scale
    @Test
    public void testKnownCardinalityMedium() {
        String[] files = {"/seg_0", "/seg_1", "/seg_2", "/seg_3", "/seg_4"};
        int blocksPerFile = 200;
        int totalBlocks = files.length * blocksPerFile;

        for (String file : files) {
            int pathHash = file.hashCode();
            for (int i = 0; i < blocksPerFile; i++) {
                wse.update(pathHash, i * BLOCK_SIZE);
            }
        }

        long estimate = wse.estimateCardinality(60);
        // ±15% for 1000 elements
        assertTrue("Expected ~1000, got " + estimate,
            estimate >= totalBlocks * 0.85 && estimate <= totalBlocks * 1.15);
    }

    @Test
    public void testKnownCardinalityLarge() {
        int pathHash = "/large/file".hashCode();
        for (int i = 0; i < 10000; i++) {
            wse.update(pathHash, i * BLOCK_SIZE);
        }
        long estimate = wse.estimateCardinality(60);
        // ±10% for 10000 elements (HLL more accurate at scale)
        assertTrue("Expected ~10000, got " + estimate, estimate >= 9000 && estimate <= 11000);
    }

    @Test
    public void testMultipleFilesCountedSeparately() {
        int pathA = "/fileA".hashCode();
        int pathB = "/fileB".hashCode();

        for (int i = 0; i < 50; i++) {
            wse.update(pathA, i * BLOCK_SIZE);
            wse.update(pathB, i * BLOCK_SIZE);
        }

        long estimate = wse.estimateCardinality(60);
        // Should be ~100, not ~50
        assertTrue("Expected ~100 (two files), got " + estimate, estimate >= 80 && estimate <= 120);
    }

    @Test
    public void testDisabledEstimatorDoesNotCount() {
        wse.setEnabled(false);
        for (int i = 0; i < 100; i++) {
            wse.update("/file".hashCode(), i * BLOCK_SIZE);
        }
        long estimate = wse.estimateCardinality(60);
        assertEquals("Disabled estimator should report 0", 0, estimate);
    }

    // #3 fix: clarify this tests floor-to-block behavior for non-aligned offsets
    @Test
    public void testNonAlignedOffsetsFloorToSameBlock() {
        // Offsets 0, 100, 4000, 8191 all floor to block 0 via >>> 13
        // This is a defensive test — real callers always pass block-aligned offsets
        int pathHash = "/file".hashCode();
        wse.update(pathHash, 0L);
        wse.update(pathHash, 100L);
        wse.update(pathHash, 4000L);
        wse.update(pathHash, 8191L);

        long estimate = wse.estimateCardinality(60);
        assertTrue("Non-aligned offsets in same block should count as 1, got " + estimate,
            estimate >= 1 && estimate <= 3);
    }

    // #1 fix: lower bound is 2, not 1
    @Test
    public void testBlockBoundaryOffsets() {
        // Offset 8191 = block 0, offset 8192 = block 1 — must be 2 unique blocks
        int pathHash = "/file".hashCode();
        wse.update(pathHash, 8191L);
        wse.update(pathHash, 8192L);

        long estimate = wse.estimateCardinality(60);
        assertTrue("Block boundary should yield 2 blocks, got " + estimate, estimate >= 2 && estimate <= 4);
    }

    // #13 fix: concurrent updates should not throw or corrupt the estimator
    @Test
    public void testConcurrentUpdatesDoNotCorrupt() throws Exception {
        int numThreads = 8;
        int updatesPerThread = 5000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicBoolean failed = new AtomicBoolean(false);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < updatesPerThread; i++) {
                        // Each thread writes to its own file's blocks
                        wse.update(("/thread/" + threadId).hashCode(), i * BLOCK_SIZE);
                    }
                } catch (Exception e) {
                    failed.set(true);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // release all threads simultaneously
        assertTrue("Threads should finish within 30s", doneLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertTrue("No thread should have thrown an exception", !failed.get());

        // Total unique blocks = numThreads * updatesPerThread = 40000
        // HLL estimate should be in the right ballpark (allow ±20% for concurrency noise)
        long expected = (long) numThreads * updatesPerThread;
        long estimate = wse.estimateCardinality(60);
        assertTrue("Expected ~" + expected + " under concurrency, got " + estimate,
            estimate >= expected * 0.8 && estimate <= expected * 1.2);
    }
}
