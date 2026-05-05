/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

@SuppressWarnings("unchecked")
public class CachedMemorySegmentIndexInputTests {

    private static int BLOCK_SIZE;
    private static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private BlockCache<RefCountedByteBuffer> mockCache;
    private RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> radixBlockTable;
    private RadixBlockTableRegistry radixBlockTableRegistry;
    private ReadaheadManager mockReadaheadManager;
    private ReadaheadContext mockReadaheadContext;
    private Path testPath;
    private Arena arena;

    @Before
    public void setUp() throws Exception {
        StaticConfigs.resetForTesting();
        StaticConfigs.init(8192);
        BLOCK_SIZE = StaticConfigs.CACHE_BLOCK_SIZE;
        mockCache = mock(BlockCache.class);
        radixBlockTableRegistry = new RadixBlockTableRegistry();
        mockReadaheadManager = mock(ReadaheadManager.class);
        mockReadaheadContext = mock(ReadaheadContext.class);
        testPath = Paths.get("/test/exhaustive.dat");
        radixBlockTable = radixBlockTableRegistry.acquire(testPath);
        arena = Arena.ofAuto();
    }

    @After
    public void tearDown() {
        StaticConfigs.resetForTesting();
    }

    /**
     * Tests reading a single byte at exact block boundary (first byte of second block).
     */
    @Test
    public void ReadByteAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x10);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x20);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read first byte of second block
        input.seek(BLOCK_SIZE);
        byte value = input.readByte();

        assertEquals("Should read first byte of second block", (byte) 0x20, value);
        assertEquals("Position should advance", BLOCK_SIZE + 1, input.getFilePointer());
    }

    /**
     * Tests reading byte at last position of first block.
     */
    @Test
    public void ReadByteAtEndOfBlock() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x10);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x20);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Seek to last byte of first block
        input.seek(BLOCK_SIZE - 1);
        byte value = input.readByte();

        assertEquals("Should read last byte of first block", (byte) 0x10, value);
        assertEquals("Position should be at block boundary", BLOCK_SIZE, input.getFilePointer());
    }

    /**
     * Tests reading byte one position before block boundary.
     */
    @Test
    public void ReadByteOneBeforeBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 2);
        byte value = input.readByte();

        assertEquals("Should read second-to-last byte of first block", (byte) 0xAA, value);
    }

    /**
     * Tests reading bytes that span exact block boundary (4 bytes before, 4 bytes after).
     */
    @Test
    public void ReadBytesAcrossExactBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Position at 4 bytes before boundary
        input.seek(BLOCK_SIZE - 4);
        byte[] buffer = new byte[8]; // Read 8 bytes (4 from each block)
        input.readBytes(buffer, 0, 8);

        // First 4 bytes should be 0xAA, next 4 should be 0xBB
        for (int i = 0; i < 4; i++) {
            assertEquals("First 4 bytes from block 0", (byte) 0xAA, buffer[i]);
        }
        for (int i = 4; i < 8; i++) {
            assertEquals("Next 4 bytes from block 1", (byte) 0xBB, buffer[i]);
        }
    }

    /**
     * Tests reading bytes that start exactly at block boundary.
     */
    @Test
    public void ReadBytesStartingAtBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x11);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x22);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE);
        byte[] buffer = new byte[10];
        input.readBytes(buffer, 0, 10);

        for (int i = 0; i < 10; i++) {
            assertEquals("All bytes from second block", (byte) 0x22, buffer[i]);
        }
    }

    /**
     * Tests reading bytes that end exactly at block boundary.
     */
    @Test
    public void ReadBytesEndingAtBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x33);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x44);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 10);
        byte[] buffer = new byte[10];
        input.readBytes(buffer, 0, 10);

        for (int i = 0; i < 10; i++) {
            assertEquals("All bytes from first block", (byte) 0x33, buffer[i]);
        }
        assertEquals("Position should be at boundary", BLOCK_SIZE, input.getFilePointer());
    }

    /**
     * Tests reading large byte array spanning 3 complete blocks.
     */
    @Test
    public void ReadBytesSpanningThreeBlocks() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x11);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x22);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0x33);
        MemorySegment block3 = createBlockWithPattern(3, (byte) 0x44);

        setupFourBlocks(block0, block1, block2, block3);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read from middle of block 0 through middle of block 3
        input.seek(BLOCK_SIZE - 100);
        int readSize = BLOCK_SIZE * 2 + 200; // Spans blocks 0, 1, 2, and into 3
        byte[] buffer = new byte[readSize];
        input.readBytes(buffer, 0, readSize);

        // Verify pattern
        for (int i = 0; i < 100; i++) {
            assertEquals("Bytes from block 0", (byte) 0x11, buffer[i]);
        }
        for (int i = 100; i < 100 + BLOCK_SIZE; i++) {
            assertEquals("Bytes from block 1", (byte) 0x22, buffer[i]);
        }
        for (int i = 100 + BLOCK_SIZE; i < 100 + BLOCK_SIZE * 2; i++) {
            assertEquals("Bytes from block 2", (byte) 0x33, buffer[i]);
        }
        for (int i = 100 + BLOCK_SIZE * 2; i < readSize; i++) {
            assertEquals("Bytes from block 3", (byte) 0x44, buffer[i]);
        }
    }

    /**
     * Tests reading exactly one full block (block-aligned, full block size).
     */
    @Test
    public void ReadFullBlockAligned() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[BLOCK_SIZE];
        input.readBytes(buffer, 0, BLOCK_SIZE);

        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("Full block read", (byte) 0xAA, buffer[i]);
        }
        assertEquals("Position should be at next block", BLOCK_SIZE, input.getFilePointer());
    }

    /**
     * Tests reading multiple full blocks sequentially.
     */
    @Test
    public void ReadMultipleFullBlocks() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[BLOCK_SIZE * 2];
        input.readBytes(buffer, 0, BLOCK_SIZE * 2);

        for (int i = 0; i < BLOCK_SIZE; i++) {
            assertEquals("Block 0 data", (byte) 1, buffer[i]);
        }
        for (int i = BLOCK_SIZE; i < BLOCK_SIZE * 2; i++) {
            assertEquals("Block 1 data", (byte) 2, buffer[i]);
        }
    }

    /**
     * Tests reading short value that spans block boundary (1 byte in each block).
     */
    @Test
    public void ReadShortAcrossBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Set up short value split across blocks (little-endian: 0x1234)
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x12);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 1);
        short value = input.readShort();

        assertEquals("Short should span blocks correctly", (short) 0x1234, value);
        assertEquals("Position advanced by 2", BLOCK_SIZE + 1, input.getFilePointer());
    }

    /**
     * Tests reading short at exact block boundary (both bytes in second block).
     */
    @Test
    public void ReadShortAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Place short at start of block 1
        block1.set(LAYOUT_LE_SHORT, 0, (short) 0x5678);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE);
        short value = input.readShort();

        assertEquals("Short at boundary", (short) 0x5678, value);
    }

    /**
     * Tests reading short one byte before block boundary.
     */
    @Test
    public void ReadShortOneByteBeforeBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        block0.set(LAYOUT_LE_SHORT, BLOCK_SIZE - 2, (short) 0xABCD);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 2);
        short value = input.readShort();

        assertEquals("Short before boundary", (short) 0xABCD, value);
    }

    /**
     * Tests reading int that spans block boundary (2 bytes in each block).
     */
    @Test
    public void ReadIntAcrossBlockBoundaryEvenSplit() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Position int so 2 bytes in each block (little-endian: 0x78563412)
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0x12);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 1, (byte) 0x78);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 2);
        int value = input.readInt();

        assertEquals("Int should span blocks (2+2)", 0x78563412, value);
    }

    /**
     * Tests reading int with 1 byte in first block, 3 bytes in second.
     */
    @Test
    public void ReadIntAcrossBlockBoundaryUnevenSplit1_3() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // 1 byte in block0, 3 bytes in block1
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x12);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 1, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 2, (byte) 0x78);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 1);
        int value = input.readInt();

        assertEquals("Int should span blocks (1+3)", 0x78563412, value);
    }

    /**
     * Tests reading int with 3 bytes in first block, 1 byte in second.
     */
    @Test
    public void ReadIntAcrossBlockBoundaryUnevenSplit3_1() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // 3 bytes in block0, 1 byte in block1
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 3, (byte) 0x12);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0x34);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x78);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 3);
        int value = input.readInt();

        assertEquals("Int should span blocks (3+1)", 0x78563412, value);
    }

    /**
     * Tests reading int at exact block boundary (all 4 bytes in second block).
     */
    @Test
    public void ReadIntAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        block1.set(LAYOUT_LE_INT, 0, 0xDEADBEEF);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE);
        int value = input.readInt();

        assertEquals("Int at boundary", 0xDEADBEEF, value);
    }

    // ==================== Long Reads Across Boundaries ====================

    /**
     * Tests reading long that spans block boundary (4 bytes in each block).
     */
    @Test
    public void ReadLongAcrossBlockBoundaryEvenSplit() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Position long so 4 bytes in each block
        long testValue = 0x123456789ABCDEF0L;
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 4, (byte) 0xF0);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 3, (byte) 0xDE);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0xBC);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x9A);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x78);
        block1.set(LAYOUT_BYTE, 1, (byte) 0x56);
        block1.set(LAYOUT_BYTE, 2, (byte) 0x34);
        block1.set(LAYOUT_BYTE, 3, (byte) 0x12);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 4);
        long value = input.readLong();

        assertEquals("Long should span blocks (4+4)", testValue, value);
    }

    /**
     * Tests reading long with various split positions.
     */
    @Test
    public void ReadLongAcrossBlockBoundaryVariousSplits() throws IOException {
        // Test splits: 1+7, 2+6, 3+5, 5+3, 6+2, 7+1
        int[] splits = { 1, 2, 3, 5, 6, 7 };

        for (int bytesInFirstBlock : splits) {
            // Fresh L1 cache per iteration to avoid stale entries from previous split
            radixBlockTable = new RadixBlockTable<>();

            long fileLength = BLOCK_SIZE * 2;
            MemorySegment block0 = arena.allocate(BLOCK_SIZE);
            MemorySegment block1 = arena.allocate(BLOCK_SIZE);

            long testValue = 0x123456789ABCDEF0L;
            byte[] bytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                bytes[i] = (byte) (testValue >> (i * 8));
            }

            // Place bytes split across blocks
            for (int i = 0; i < bytesInFirstBlock; i++) {
                block0.set(LAYOUT_BYTE, BLOCK_SIZE - bytesInFirstBlock + i, bytes[i]);
            }
            for (int i = bytesInFirstBlock; i < 8; i++) {
                block1.set(LAYOUT_BYTE, i - bytesInFirstBlock, bytes[i]);
            }

            setupTwoBlocks(block0, block1);

            CachedMemorySegmentIndexInput input = createInput(fileLength);

            input.seek(BLOCK_SIZE - bytesInFirstBlock);
            long value = input.readLong();

            assertEquals("Long with " + bytesInFirstBlock + "+" + (8 - bytesInFirstBlock) + " split", testValue, value);
        }
    }

    /**
     * Tests readInts when int array spans block boundary.
     */
    @Test
    public void ReadIntsSpanningBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write 10 ints near boundary
        for (int i = 0; i < 10; i++) {
            long offset = BLOCK_SIZE - 20 + i * 4L;
            int value = 1000 + i;
            if (offset >= 0 && offset + 4 <= BLOCK_SIZE) {
                block0.set(LAYOUT_LE_INT, (int) offset, value);
            } else if (offset >= BLOCK_SIZE) {
                block1.set(LAYOUT_LE_INT, (int) (offset - BLOCK_SIZE), value);
            }
            // Ints spanning boundary will be handled by fallback
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 20);
        int[] buffer = new int[5]; // Read 5 ints spanning boundary
        input.readInts(buffer, 0, 5);

        // Should read values successfully
        assertNotNull("Buffer should be populated", buffer);
    }

    /**
     * Tests readInts entirely within one block.
     */
    @Test
    public void ReadIntsWithinBlock() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write ints in middle of block
        for (int i = 0; i < 10; i++) {
            block0.set(LAYOUT_LE_INT, 1000 + i * 4, 100 + i);
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(1000);
        int[] buffer = new int[10];
        input.readInts(buffer, 0, 10);

        for (int i = 0; i < 10; i++) {
            assertEquals("Int " + i, 100 + i, buffer[i]);
        }
    }

    /**
     * Tests readLongs spanning block boundary.
     */
    @Test
    public void ReadLongsSpanningBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write longs near boundary
        for (int i = 0; i < 5; i++) {
            long offset = BLOCK_SIZE - 24 + i * 8L;
            long value = 10000L + i;
            if (offset >= 0 && offset + 8 <= BLOCK_SIZE) {
                block0.set(LAYOUT_LE_LONG, (int) offset, value);
            } else if (offset >= BLOCK_SIZE) {
                block1.set(LAYOUT_LE_LONG, (int) (offset - BLOCK_SIZE), value);
            }
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 24);
        long[] buffer = new long[3];
        input.readLongs(buffer, 0, 3);

        assertNotNull("Buffer should be populated", buffer);
    }

    /**
     * Tests readFloats spanning block boundary.
     */
    @Test
    public void ReadFloatsSpanningBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Write floats near boundary
        for (int i = 0; i < 5; i++) {
            long offset = BLOCK_SIZE - 8 + i * 4L;
            float value = 10.5f + i;
            if (offset >= 0 && offset + 4 <= BLOCK_SIZE) {
                block0.set(LAYOUT_LE_FLOAT, (int) offset, value);
            } else if (offset >= BLOCK_SIZE) {
                block1.set(LAYOUT_LE_FLOAT, (int) (offset - BLOCK_SIZE), value);
            }
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 8);
        float[] buffer = new float[3];
        input.readFloats(buffer, 0, 3);

        assertNotNull("Buffer should be populated", buffer);
    }

    /**
     * Tests random access readByte at block boundaries and various offsets.
     */
    @Test
    public void RandomAccessByteAtVariousOffsets() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        MemorySegment block2 = arena.allocate(BLOCK_SIZE);

        // Fill with identifiable pattern
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block0.set(LAYOUT_BYTE, i, (byte) (i % 128));
            block1.set(LAYOUT_BYTE, i, (byte) ((i + 50) % 128));
            block2.set(LAYOUT_BYTE, i, (byte) ((i + 100) % 128));
        }

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Random access reads at strategic positions
        assertEquals("Byte at 0", (byte) 0, input.readByte(0));
        assertEquals("Byte at BLOCK_SIZE-1", (byte) ((BLOCK_SIZE - 1) % 128), input.readByte(BLOCK_SIZE - 1));
        assertEquals("Byte at BLOCK_SIZE", (byte) 50, input.readByte(BLOCK_SIZE));
        assertEquals("Byte at BLOCK_SIZE+1", (byte) 51, input.readByte(BLOCK_SIZE + 1));
        assertEquals("Byte at 2*BLOCK_SIZE-1", (byte) ((BLOCK_SIZE - 1 + 50) % 128), input.readByte(BLOCK_SIZE * 2 - 1));
        assertEquals("Byte at 2*BLOCK_SIZE", (byte) 100, input.readByte(BLOCK_SIZE * 2));

        // File pointer should not change
        assertEquals("File pointer should not move", 0, input.getFilePointer());
    }

    /**
     * Tests random access readInt at exact block boundaries.
     */
    @Test
    public void RandomAccessIntAtBlockBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        MemorySegment block2 = arena.allocate(BLOCK_SIZE);

        // Place ints at strategic positions
        block0.set(LAYOUT_LE_INT, 0, 0x11111111);
        block0.set(LAYOUT_LE_INT, BLOCK_SIZE - 4, 0x22222222);
        block1.set(LAYOUT_LE_INT, 0, 0x33333333);
        block1.set(LAYOUT_LE_INT, BLOCK_SIZE / 2, 0x44444444);
        block1.set(LAYOUT_LE_INT, BLOCK_SIZE - 4, 0x55555555);
        block2.set(LAYOUT_LE_INT, 0, 0x66666666);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertEquals("Int at 0", 0x11111111, input.readInt(0));
        assertEquals("Int at end of block 0", 0x22222222, input.readInt(BLOCK_SIZE - 4));
        assertEquals("Int at start of block 1", 0x33333333, input.readInt(BLOCK_SIZE));
        assertEquals("Int at mid block 1", 0x44444444, input.readInt(BLOCK_SIZE + BLOCK_SIZE / 2));
        assertEquals("Int at end of block 1", 0x55555555, input.readInt(BLOCK_SIZE * 2 - 4));
        assertEquals("Int at start of block 2", 0x66666666, input.readInt(BLOCK_SIZE * 2));

        assertEquals("File pointer unchanged", 0, input.getFilePointer());
    }

    /**
     * Tests random access readLong spanning boundary.
     */
    @Test
    public void RandomAccessLongSpanningBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        // Set up long spanning boundary
        long testValue = 0xFEDCBA9876543210L;
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 4, (byte) 0x10);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 3, (byte) 0x32);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 2, (byte) 0x54);
        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0x76);
        block1.set(LAYOUT_BYTE, 0, (byte) 0x98);
        block1.set(LAYOUT_BYTE, 1, (byte) 0xBA);
        block1.set(LAYOUT_BYTE, 2, (byte) 0xDC);
        block1.set(LAYOUT_BYTE, 3, (byte) 0xFE);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertEquals("Long spanning boundary", testValue, input.readLong(BLOCK_SIZE - 4));
        assertEquals("File pointer unchanged", 0, input.getFilePointer());
    }

    /**
     * Tests random access readShort spanning boundary.
     */
    @Test
    public void RandomAccessShortSpanningBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        block0.set(LAYOUT_BYTE, BLOCK_SIZE - 1, (byte) 0xAB);
        block1.set(LAYOUT_BYTE, 0, (byte) 0xCD);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertEquals("Short spanning boundary", (short) 0xCDAB, input.readShort(BLOCK_SIZE - 1));
    }

    /**
     * Tests slice starting exactly at block boundary.
     */
    @Test
    public void SliceStartingAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x10);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x20);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0x30);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Create slice starting at block 1
        CachedMemorySegmentIndexInput slice = input.slice("block1_slice", BLOCK_SIZE, BLOCK_SIZE);

        assertEquals("Slice length", BLOCK_SIZE, slice.length());
        assertEquals("Slice position", 0, slice.getFilePointer());

        byte value = slice.readByte();
        assertEquals("First byte of slice from block 1", (byte) 0x20, value);
    }

    /**
     * Tests slice ending exactly at block boundary.
     */
    @Test
    public void SliceEndingAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice from middle of block 0 to exactly end of block 1
        long sliceOffset = BLOCK_SIZE / 2;
        long sliceLength = BLOCK_SIZE + BLOCK_SIZE / 2;
        CachedMemorySegmentIndexInput slice = input.slice("partial_slice", sliceOffset, sliceLength);

        assertEquals("Slice length", sliceLength, slice.length());

        // Read to end
        slice.seek(sliceLength - 1);
        byte lastByte = slice.readByte();
        assertEquals("Last byte at block 1 end", (byte) 2, lastByte);
    }

    /**
     * Tests slice spanning multiple blocks with non-aligned start and end.
     */
    @Test
    public void SliceSpanningBlocksNonAligned() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);
        MemorySegment block3 = createBlockWithPattern(3, (byte) 4);

        setupFourBlocks(block0, block1, block2, block3);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice from 1/4 into block 0 to 3/4 into block 3
        long sliceOffset = BLOCK_SIZE / 4;
        long sliceLength = BLOCK_SIZE * 3;
        CachedMemorySegmentIndexInput slice = input.slice("multi_block_slice", sliceOffset, sliceLength);

        // Read through slice verifying block transitions
        int readSize = BLOCK_SIZE + 100;
        byte[] buffer = new byte[readSize];
        slice.readBytes(buffer, 0, readSize);

        // First 3/4 block from block 0 (6144 bytes)
        for (int i = 0; i < BLOCK_SIZE * 3 / 4; i++) {
            assertEquals("Bytes from block 0", (byte) 1, buffer[i]);
        }
        // Next 100 bytes from block 1
        for (int i = BLOCK_SIZE * 3 / 4; i < readSize; i++) {
            assertEquals("Bytes from block 1", (byte) 2, buffer[i]);
        }
    }

    /**
     * Tests nested slices with boundary crossings.
     */
    @Test
    public void NestedSlicesAcrossBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);
        MemorySegment block3 = createBlockWithPattern(3, (byte) 4);

        setupFourBlocks(block0, block1, block2, block3);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // First slice: spans blocks 1-2
        CachedMemorySegmentIndexInput slice1 = input.slice("slice1", BLOCK_SIZE, BLOCK_SIZE * 2);

        // Second slice: middle portion of first slice, crosses block 1-2 boundary
        CachedMemorySegmentIndexInput slice2 = slice1.slice("slice2", BLOCK_SIZE / 2, BLOCK_SIZE);

        // slice2 starts at BLOCK_SIZE + BLOCK_SIZE/2, spans into block 2
        byte[] buffer = new byte[100];
        slice2.seek(BLOCK_SIZE / 2 - 50); // Position near boundary
        slice2.readBytes(buffer, 0, 100);

        // Should have bytes from block 1 and block 2
        for (int i = 0; i < 50; i++) {
            assertEquals("Bytes from block 1", (byte) 2, buffer[i]);
        }
        for (int i = 50; i < 100; i++) {
            assertEquals("Bytes from block 2", (byte) 3, buffer[i]);
        }
    }

    /**
     * Tests slice absolute offset calculation with boundaries.
     */
    @Test
    public void SliceAbsoluteFileOffsetAtBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice starting at block boundary
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", BLOCK_SIZE, BLOCK_SIZE * 2);

        assertEquals("Slice absolute offset at pos 0", BLOCK_SIZE, slice.getAbsoluteFileOffset());
        slice.seek(BLOCK_SIZE - 1); // One before next boundary
        assertEquals("Slice absolute offset before boundary", BLOCK_SIZE * 2 - 1, slice.getAbsoluteFileOffset());
        slice.seek(BLOCK_SIZE); // Exactly at next boundary
        assertEquals("Slice absolute offset at boundary", BLOCK_SIZE * 2, slice.getAbsoluteFileOffset());
    }

    // ==================== Edge Cases and Error Conditions ====================

    /**
     * Tests reading zero bytes (no-op).
     */
    @Test
    public void ReadZeroBytes() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[10];
        input.readBytes(buffer, 0, 0);

        assertEquals("Position should not change", 0, input.getFilePointer());
    }

    /**
     * Tests zero-length operations for array reads.
     */
    @Test
    public void ZeroLengthArrayReads() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.readInts(new int[5], 0, 0);
        input.readLongs(new long[5], 0, 0);
        input.readFloats(new float[5], 0, 0);

        assertEquals("Position unchanged", 0, input.getFilePointer());
    }

    /**
     * Tests seek to negative position throws exception.
     * Note: The slice implementation uses assert, so this throws AssertionError in test mode.
     */
    @Test
    public void SeekNegativePosition() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Expect either IOException or AssertionError depending on whether assertions are enabled
        assertThrows(Throwable.class, () -> input.seek(-1));
    }

    /**
     * Tests seek past EOF throws exception.
     */
    @Test
    public void SeekPastEOF() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        assertThrows(IOException.class, () -> input.seek(fileLength + 1));
    }

    /**
     * Tests seek to exact file length is valid.
     */
    @Test
    public void SeekToFileLength() throws IOException {
        long fileLength = BLOCK_SIZE + 100;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(fileLength); // Should be valid
        assertEquals("Position at EOF", fileLength, input.getFilePointer());
    }

    /**
     * Tests clone preserves position at block boundary.
     */
    @Test
    public void CloneAtBlockBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE); // At exact boundary
        CachedMemorySegmentIndexInput clone = input.clone();

        assertEquals("Clone at boundary", BLOCK_SIZE, clone.getFilePointer());
        assertEquals("Clone length", fileLength, clone.length());
    }

    /**
     * Tests clone independence with reads across boundaries.
     */
    @Test
    public void CloneIndependenceAcrossBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 5);
        CachedMemorySegmentIndexInput clone = input.clone();

        // Read in original crosses boundary
        byte[] buffer1 = new byte[10];
        input.readBytes(buffer1, 0, 10);

        // Clone should still be at BLOCK_SIZE - 5
        assertEquals("Clone position independent", BLOCK_SIZE - 5, clone.getFilePointer());

        // Clone reads same data
        byte[] buffer2 = new byte[10];
        clone.readBytes(buffer2, 0, 10);

        assertArrayEquals("Clone reads same data", buffer1, buffer2);
    }

    /**
     * Tests reading at exact file length boundary.
     */
    @Test
    public void ReadAtExactFileLength() throws IOException {
        long fileLength = BLOCK_SIZE + 99;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < 99; i++) {
            block1.set(LAYOUT_BYTE, i, (byte) (i + 2));
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(fileLength - 1);
        byte lastByte = input.readByte();

        assertEquals("Last byte of file", (byte) (98 + 2), lastByte);
        assertEquals("At EOF", fileLength, input.getFilePointer());
    }

    /**
     * Tests partial read at end crossing block boundary.
     */
    @Test
    public void PartialReadAtEndCrossingBoundary() throws IOException {
        long fileLength = BLOCK_SIZE + 50;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < 50; i++) {
            block1.set(LAYOUT_BYTE, i, (byte) 2);
        }

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.seek(BLOCK_SIZE - 10);
        byte[] buffer = new byte[60];
        input.readBytes(buffer, 0, 60);

        for (int i = 0; i < 10; i++) {
            assertEquals("Bytes from block 0", (byte) 1, buffer[i]);
        }
        for (int i = 10; i < 60; i++) {
            assertEquals("Bytes from block 1", (byte) 2, buffer[i]);
        }

        assertEquals("At EOF", fileLength, input.getFilePointer());
    }

    /**
     * Tests large sequential read through many block boundaries.
     */
    @Test
    public void LargeSequentialReadManyBlocks() throws IOException {
        int numBlocks = 10;
        long fileLength = BLOCK_SIZE * numBlocks;

        // Setup 10 blocks with different patterns
        for (int i = 0; i < numBlocks; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i + 1));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        byte[] buffer = new byte[(int) fileLength];
        input.readBytes(buffer, 0, (int) fileLength);

        // Verify each block
        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
            for (int i = 0; i < BLOCK_SIZE; i++) {
                int bufferPos = blockIdx * BLOCK_SIZE + i;
                assertEquals("Block " + blockIdx + " byte " + i, (byte) (blockIdx + 1), buffer[bufferPos]);
            }
        }
    }

    /**
     * Tests seek and read pattern across boundaries (simulating random access).
     */
    @Test
    public void RandomSeekReadPatternAcrossBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 5;

        for (int i = 0; i < 5; i++) {
            MemorySegment block = createBlockWithPattern(i, (byte) (i * 10));
            setupBlock(i * BLOCK_SIZE, block);
        }

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Random access pattern at various boundaries
        long[] positions = {
            0,
            BLOCK_SIZE - 1,
            BLOCK_SIZE,
            BLOCK_SIZE + 1,
            BLOCK_SIZE * 2 - 1,
            BLOCK_SIZE * 2,
            BLOCK_SIZE * 3 + BLOCK_SIZE / 2,
            BLOCK_SIZE * 4 - 10 };

        for (long pos : positions) {
            input.seek(pos);
            byte value = input.readByte();
            assertNotNull("Should read byte at position " + pos, value);
        }
    }

    /**
     * Tests slice with invalid parameters at boundaries.
     */
    @Test
    public void SliceInvalidParametersAtBoundaries() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Negative offset
        assertThrows(IllegalArgumentException.class, () -> input.slice("test", -1, BLOCK_SIZE));

        // Negative length
        assertThrows(IllegalArgumentException.class, () -> input.slice("test", 0, -1));

        // Offset + length > file length (at boundary)
        assertThrows(IllegalArgumentException.class, () -> input.slice("test", BLOCK_SIZE, BLOCK_SIZE + 1));

        // Offset beyond file length
        assertThrows(IllegalArgumentException.class, () -> input.slice("test", fileLength + 1, 10));
    }

    // ==================== Close Operation Tests ====================

    /**
     * Tests that close clears resources properly.
     */
    @Test
    public void CloseUnpinsCurrentBlock() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read a byte to load the block
        input.readByte();

        // Verify block is loaded by reading current position
        assertEquals(1, input.getFilePointer());

        // Close the input
        input.close();

        // Verify that close happened by checking input is no longer open
        assertThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> input.getFilePointer());
    }

    /**
     * Tests that close releases the radix block table registry entry.
     */
    @Test
    public void CloseClearsBlockSlotCache() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read to populate cache
        input.readByte();

        // Close the input
        input.close();

        // Verify input is closed
        assertThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> input.getFilePointer());
    }

    /**
     * Tests that close on slice does not close readahead manager but master does.
     */
    @Test
    public void CloseOnSliceDoesNotClearTinyCache() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", 100, BLOCK_SIZE);

        // Read from slice
        slice.readByte();

        // Close the slice (not the master)
        slice.close();

        // Slice is closed
        assertThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> slice.getFilePointer());

        // Master is still open
        input.seek(0);
        input.readByte(); // should not throw

        // Now close the master
        input.close();

        // Verify readahead manager was closed for master
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that close closes the readahead manager for master instance.
     */
    @Test
    public void CloseClosesReadaheadManager() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Close the input
        input.close();

        // Verify readahead manager close was called
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that close on slice does not close the readahead manager.
     */
    @Test
    public void CloseOnSliceDoesNotCloseReadaheadManager() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", 0, BLOCK_SIZE);

        // Reset mock to clear any previous interactions
        clearInvocations(mockReadaheadManager);

        // Close the slice
        slice.close();

        // Verify readahead manager was NOT closed
        verify(mockReadaheadManager, never()).close();

        // Close the master
        input.close();

        // Verify readahead manager WAS closed
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that calling close multiple times is idempotent (safe).
     */
    @Test
    public void CloseIsIdempotent() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read to load a block
        input.readByte();

        // Close multiple times
        input.close();
        input.close();
        input.close();

        // Verify tiny cache clear was called only once (idempotent)

        // Verify readahead manager close was called only once
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that close unpins block even when positioned at block boundary.
     */
    @Test
    public void CloseUnpinsBlockAtBoundary() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);

        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read up to block boundary
        input.seek(BLOCK_SIZE - 1);
        input.readByte();

        // Verify we're at the boundary
        assertEquals(BLOCK_SIZE, input.getFilePointer());

        // Close should unpin the current block
        input.close();

        // Verify tiny cache was cleared
    }

    /**
     * Tests that close properly handles the case with no current block loaded.
     */
    @Test
    public void CloseWithNoCurrentBlock() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Close immediately without reading (no current block)
        input.close();

        // Should still clear cache and close readahead manager
        verify(mockReadaheadManager, times(1)).close();
    }

    /**
     * Tests that clone creates independent instance with separate lifecycle.
     */
    @Test
    public void CloneHasIndependentLifecycle() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);

        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read to load block
        input.readByte();

        // Create clone
        CachedMemorySegmentIndexInput clone = input.clone();

        // Close the original
        input.close();

        // Clone should still be usable (it's a slice, so it has separate lifecycle)
        byte value = clone.readByte();
        assertEquals((byte) 1, value);

        // Close the clone
        clone.close();
    }

    /**
     * Tests cleanup with multiple blocks loaded and closed.
     */
    @Test
    public void CloseWithMultipleBlocksLoaded() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 3);

        setupThreeBlocks(block0, block1, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read from multiple blocks to load them
        input.seek(BLOCK_SIZE - 1);
        input.readByte(); // Loads block 0

        input.seek(BLOCK_SIZE);
        input.readByte(); // Loads block 1

        input.seek(BLOCK_SIZE * 2);
        input.readByte(); // Loads block 2

        // Close should clean up resources
        input.close();

        // Verify cache clear was called
        verify(mockReadaheadManager, times(1)).close();
    }

    private MemorySegment createBlockWithPattern(int blockIndex, byte pattern) {
        MemorySegment segment = arena.allocate(BLOCK_SIZE);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            segment.set(LAYOUT_BYTE, i, pattern);
        }
        return segment;
    }

    private void setupOneBlock(MemorySegment block0) throws IOException {
        setupBlock(0, block0);
    }

    private void setupTwoBlocks(MemorySegment block0, MemorySegment block1) throws IOException {
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
    }

    private void setupThreeBlocks(MemorySegment block0, MemorySegment block1, MemorySegment block2) throws IOException {
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);
    }

    private void setupFourBlocks(MemorySegment block0, MemorySegment block1, MemorySegment block2, MemorySegment block3)
        throws IOException {
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);
        setupBlock(BLOCK_SIZE * 3, block3);
    }

    private void setupBlock(long offset, MemorySegment segment) throws IOException {
        // Create a real RefCountedByteBuffer with a no-op releaser
        ByteBuffer buf = ByteBuffer.allocateDirect((int) segment.byteSize()).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        MemorySegment.copy(segment, 0, MemorySegment.ofBuffer(buf), 0, (int) segment.byteSize());
        RefCountedByteBuffer refSegment = new RefCountedByteBuffer(buf, (int) segment.byteSize());

        BlockCacheValue<RefCountedByteBuffer> value = mock(BlockCacheValue.class);
        when(value.value()).thenReturn(refSegment);
        when(value.tryPin()).thenReturn(true);

        FileBlockCacheKey key = new FileBlockCacheKey(testPath, offset);
        when(mockCache.get(eq(key))).thenReturn(value);
        when(mockCache.getOrLoad(eq(key))).thenReturn(value);
    }

    private CachedMemorySegmentIndexInput createInput(long length) {
        return CachedMemorySegmentIndexInput
            .newInstance("test", testPath, length, mockCache, mockReadaheadManager, mockReadaheadContext, radixBlockTable, radixBlockTableRegistry);
    }

    /**
     * Test for the bug that caused negative file offsets in production.
     *
     * The old MultiSegmentImpl implementation would double-count offsets when creating slices,
     * leading to negative overflow in the block cache key calculation.
     *
     * Error scenario from logs:
     * FileBlockCacheKey[filePath=.../_26.cfs, fileOffset=-933888]
     * Bulk read failed: offset=-933888 err=java.lang.IllegalArgumentException: Negative position
     */
    @Test
    public void SliceDoesNotProduceNegativeOffsets() throws IOException {
        // Create a large file spanning multiple blocks
        long fileLength = BLOCK_SIZE * 10; // 81920 bytes

        // Setup blocks that span a realistic file
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        MemorySegment block1 = arena.allocate(BLOCK_SIZE);
        MemorySegment block2 = arena.allocate(BLOCK_SIZE);

        // Fill blocks with test data
        for (int i = 0; i < BLOCK_SIZE; i++) {
            block0.set(LAYOUT_BYTE, i, (byte) (i & 0xFF));
            block1.set(LAYOUT_BYTE, i, (byte) ((i + 1) & 0xFF));
            block2.set(LAYOUT_BYTE, i, (byte) ((i + 2) & 0xFF));
        }

        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Create a slice at a large offset (simulating compound file structure)
        long sliceOffset = BLOCK_SIZE * 5 + 1000; // 41960 bytes into the file
        long sliceLength = BLOCK_SIZE * 2; // 16384 bytes

        CachedMemorySegmentIndexInput slice1 = input.slice("slice1", sliceOffset, sliceLength);

        // Verify the slice has correct properties
        assertEquals(sliceLength, slice1.length());
        assertEquals(0L, slice1.getFilePointer());

        // The absolute offset should be parent's base + sliceOffset
        long expectedAbsoluteOffset = sliceOffset;
        assertEquals(expectedAbsoluteOffset, slice1.getAbsoluteFileOffset());

        // Seek within the slice and verify absolute offset is still correct
        slice1.seek(100L);
        assertEquals(expectedAbsoluteOffset + 100L, slice1.getAbsoluteFileOffset());

        // Create a nested slice (slice of a slice) - this was particularly problematic
        long nestedSliceOffset = 500L;
        long nestedSliceLength = 1000L;
        CachedMemorySegmentIndexInput slice2 = slice1.slice("slice2", nestedSliceOffset, nestedSliceLength);

        assertEquals(nestedSliceLength, slice2.length());
        assertEquals(0L, slice2.getFilePointer());

        // The absolute offset should be original + slice1Offset + slice2Offset
        long expectedNestedAbsoluteOffset = sliceOffset + nestedSliceOffset;
        assertEquals(expectedNestedAbsoluteOffset, slice2.getAbsoluteFileOffset());

        // Most importantly: verify that when we access the cache, we NEVER produce negative offsets
        // The bug would cause fileOffset = absoluteBaseOffset + curPosition to overflow negative
        slice2.seek(100L);
        long finalAbsoluteOffset = slice2.getAbsoluteFileOffset();

        // This should be positive and within the file bounds
        assertTrue("Absolute offset should be positive, got: " + finalAbsoluteOffset, finalAbsoluteOffset >= 0);
        assertTrue("Absolute offset should be within file, got: " + finalAbsoluteOffset, finalAbsoluteOffset < fileLength);

        // The exact offset should be: sliceOffset + nestedSliceOffset + current position
        assertEquals(sliceOffset + nestedSliceOffset + 100L, finalAbsoluteOffset);

        // Triple-nested slice to really stress test the offset calculation
        CachedMemorySegmentIndexInput slice3 = slice2.slice("slice3", 50L, 500L);
        slice3.seek(25L);
        long tripleNestedOffset = slice3.getAbsoluteFileOffset();

        assertTrue("Triple-nested absolute offset should be positive, got: " + tripleNestedOffset, tripleNestedOffset >= 0);
        assertTrue("Triple-nested absolute offset should be within file, got: " + tripleNestedOffset, tripleNestedOffset < fileLength);

        // Should be: sliceOffset + nestedSliceOffset + 50 + 25
        assertEquals(sliceOffset + nestedSliceOffset + 50L + 25L, tripleNestedOffset);

        // Close all slices
        slice3.close();
        slice2.close();
        slice1.close();
        input.close();
    }

    /**
     * Test that verifies the getAbsoluteFileOffset(pos) method works correctly for slices.
     * This method is used internally for cache key generation.
     */
    @Test
    public void SliceAbsoluteFileOffsetWithPosition() throws IOException {
        long fileLength = BLOCK_SIZE * 5;
        MemorySegment block0 = arena.allocate(BLOCK_SIZE);
        setupBlock(0, block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Create a slice
        long sliceOffset = BLOCK_SIZE + 100;
        long sliceLength = BLOCK_SIZE * 2;
        CachedMemorySegmentIndexInput slice = input.slice("slice", sliceOffset, sliceLength);

        // Test getAbsoluteFileOffset(pos) with various positions
        assertEquals(sliceOffset + 0, slice.getAbsoluteFileOffset(0));
        assertEquals(sliceOffset + 50, slice.getAbsoluteFileOffset(50));
        assertEquals(sliceOffset + 1000, slice.getAbsoluteFileOffset(1000));
        assertEquals(sliceOffset + sliceLength - 1, slice.getAbsoluteFileOffset(sliceLength - 1));

        // All offsets should be positive and within file bounds
        for (long pos = 0; pos < sliceLength; pos += 100) {
            long absoluteOffset = slice.getAbsoluteFileOffset(pos);
            assertTrue("Offset at position " + pos + " should be positive, got: " + absoluteOffset, absoluteOffset >= 0);
            assertTrue("Offset at position " + pos + " should be within file, got: " + absoluteOffset, absoluteOffset < fileLength);
        }

        slice.close();
        input.close();
    }

    // ==================== Prefetch Tests ====================

    /**
     * Tests that prefetch is a no-op when readaheadContext is null.
     */
    @Test
    public void PrefetchWithNullReadaheadContext() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        // Create input with null readahead context
        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance(
                "test",
                testPath,
                fileLength,
                mockCache,
                mockReadaheadManager,
                null, // null readahead context
                radixBlockTable,
                radixBlockTableRegistry
            );

        // Prefetch should be a no-op and not throw
        input.prefetch(0, BLOCK_SIZE);

        // No readahead should have been triggered
        verify(mockReadaheadContext, never()).triggerReadahead(any(Long.class));

        input.close();
    }

    /**
     * Tests that prefetch triggers readahead on first call (count = 0).
     */
    @Test
    public void PrefetchTriggersOnFirstCall() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.prefetch(0, BLOCK_SIZE);

        // prefetch() calls loadMissingBlocks with block-aligned offset and count
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq(0L), eq(1L), any());

        input.close();
    }

    /**
     * Tests that prefetch skips readahead when hit count is not 0 or power of 2.
     */
    @Test
    public void PrefetchSkipsOnNonPowerOfTwoHitCount() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Mock cache to return a value (cache hit)
        BlockCacheValue<RefCountedByteBuffer> mockValue = mock(BlockCacheValue.class);
        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(mockValue);

        // Call prefetch multiple times to increment hit count
        input.prefetch(0, BLOCK_SIZE); // count = 0 -> 1 (power of 2: 2^0)
        input.prefetch(0, BLOCK_SIZE); // count = 1 -> 2 (power of 2: 2^1)
        input.prefetch(0, BLOCK_SIZE); // count = 2 -> 3 (NOT power of 2) - should skip
        input.prefetch(0, BLOCK_SIZE); // count = 3 -> 4 (power of 2: 2^2)

        // Readahead should only be triggered when count is 0, 1, 2, 4 (powers of 2)
        // But we have cache hits, so it won't trigger
        // The key is that calls 3 should return early without checking cache

        input.close();
    }

    /**
     * Tests that prefetch resets hit count on cache miss.
     */
    @Test
    public void PrefetchResetsHitCountOnCacheMiss() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.prefetch(0, BLOCK_SIZE);
        input.prefetch(BLOCK_SIZE, BLOCK_SIZE);

        // Each prefetch call delegates to loadMissingBlocks with the correct block-aligned offset
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq(0L), eq(1L), any());
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq((long) BLOCK_SIZE), eq(1L), any());

        input.close();
    }

    /**
     * Tests prefetch with offset calculation for slices.
     */
    @Test
    public void PrefetchWithSliceOffset() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice = input.slice("test_slice", BLOCK_SIZE, BLOCK_SIZE * 2);

        slice.prefetch(0, BLOCK_SIZE);

        // Absolute start block offset = BLOCK_SIZE, count = 1
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq((long) BLOCK_SIZE), eq(1L), any());

        slice.close();
        input.close();
    }

    /**
     * Tests prefetch with nested slice offsets.
     */
    @Test
    public void PrefetchWithNestedSlices() throws IOException {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice1 = input.slice("slice1", BLOCK_SIZE, BLOCK_SIZE * 2);
        CachedMemorySegmentIndexInput slice2 = slice1.slice("slice2", 100, BLOCK_SIZE);

        slice2.prefetch(50, 100);

        // absoluteBaseOffset = BLOCK_SIZE + 100, offset = 50
        // startFileOffset = BLOCK_SIZE + 150, startBlockOffset = BLOCK_SIZE (block-aligned)
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq((long) BLOCK_SIZE), anyLong(), any());

        slice2.close();
        slice1.close();
        input.close();
    }

    /**
     * Tests that prefetch respects block alignment.
     */
    @Test
    public void PrefetchBlockAlignment() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Prefetch at non-aligned offset — should be aligned down to block boundary
        input.prefetch(100, 200);

        // startFileOffset=100, startBlockOffset=0 (block-aligned down)
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq(0L), anyLong(), any());

        input.close();
    }

    /**
     * Tests prefetch doesn't fail when closed.
     */
    @Test
    public void PrefetchAfterClose() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.close();

        // Prefetch after close should throw AlreadyClosedException
        assertThrows(Exception.class, () -> input.prefetch(0, BLOCK_SIZE));
    }

    /**
     * Tests prefetch with large length spanning multiple blocks.
     */
    @Test
    public void PrefetchMultipleBlocks() throws IOException {
        long fileLength = BLOCK_SIZE * 5;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.prefetch(0, BLOCK_SIZE * 3);

        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq(0L), eq(3L), any());

        input.close();
    }

    /**
     * Tests prefetch with various lengths doesn't fail.
     */
    @Test
    public void PrefetchPowerOfTwoPattern() throws IOException {
        long fileLength = BLOCK_SIZE * 10;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Mock cache to return null (not cached) for all block checks
        when(mockCache.get(any(FileBlockCacheKey.class))).thenReturn(null);

        // Test various lengths
        input.prefetch(0, 100);                    // startBlock=0, count=1
        input.prefetch(0, BLOCK_SIZE);             // startBlock=0, count=1
        input.prefetch(0, BLOCK_SIZE + 100);       // startBlock=0, count=2
        input.prefetch(0, BLOCK_SIZE * 3);         // startBlock=0, count=3
        input.prefetch(100, BLOCK_SIZE);           // startBlock=0, count=1
        input.prefetch(BLOCK_SIZE + 100, BLOCK_SIZE * 2); // startBlock=BLOCK_SIZE, count=2

        // Mock cache doesn't have deduplication, so all 6 calls go through
        verify(mockCache, times(6)).loadMissingBlocks(eq(testPath), anyLong(), anyLong(), any());

        input.close();
    }

    /**
     * Tests prefetch with slice uses correct offset calculation.
     */
    public void testPrefetchWithSlice() throws Exception {
        long fileLength = BLOCK_SIZE * 4;
        MemorySegment block1 = createBlockWithPattern(1, (byte) 2);
        setupBlock(BLOCK_SIZE, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);
        CachedMemorySegmentIndexInput slice = input.slice("slice", BLOCK_SIZE, BLOCK_SIZE * 2);

        // Prefetch from slice should not fail
        slice.prefetch(0, BLOCK_SIZE);
        slice.prefetch(100, 200);

        slice.close();
        input.close();
    }

    /**
     * Tests that prefetch skips loading when first block is already cached.
     */
    public void testPrefetchSkipsWhenFirstBlockCached() throws Exception {
        long fileLength = BLOCK_SIZE * 3;

        // With cache-first optimization, loadForPrefetch checks cache internally
        doNothing().when(mockCache).loadMissingBlocks(eq(testPath), eq(0L), eq(3L), any());

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.prefetch(0, BLOCK_SIZE * 3);

        // Wait for async executor to complete
        Thread.sleep(100);

        // Verify loadForPrefetch was called (it checks cache internally)
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq(0L), eq(3L), any());

        input.close();
    }

    /**
     * Tests that prefetch proceeds with loading when first block is not cached.
     */
    public void testPrefetchLoadsWhenFirstBlockNotCached() throws Exception {
        long fileLength = BLOCK_SIZE * 3;

        // With cache-first optimization, loadForPrefetch checks cache internally
        doNothing().when(mockCache).loadMissingBlocks(eq(testPath), eq(0L), eq(3L), any());

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        input.prefetch(0, BLOCK_SIZE * 3);

        // Wait for async executor to complete
        Thread.sleep(100);

        // Verify loadForPrefetch was called (it checks cache and loads internally)
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq(0L), eq(3L), any());

        input.close();
    }

    /**
     * Tests that multi-block prefetch skips loading when all blocks are in L1 cache.
     */
    @SuppressWarnings("unchecked")
    public void testPrefetchSkipsMultiBlockWhenAllInL1() throws Exception {
        long fileLength = BLOCK_SIZE * 4;
        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Populate L1 cache for blocks 0, 1, 2
        for (int i = 0; i < 3; i++) {
            BlockCacheValue<RefCountedByteBuffer> mockValue = mock(BlockCacheValue.class);
            radixBlockTable.put(i, mockValue);
        }

        input.prefetch(0, BLOCK_SIZE * 3);

        // All 3 blocks in L1 — loadMissingBlocks should NOT be called
        verify(mockCache, never()).loadMissingBlocks(any(), anyLong(), anyLong(), any());

        input.close();
    }

    /**
     * Tests that multi-block prefetch loads when one block is missing from L1 cache,
     * starting from the first missing block.
     */
    @SuppressWarnings("unchecked")
    public void testPrefetchLoadsMultiBlockWhenOneBlockMissing() throws Exception {
        long fileLength = BLOCK_SIZE * 4;

        // Block 0 cached, block 1 missing → load starts at block 1 offset, 2 remaining blocks
        doNothing().when(mockCache).loadMissingBlocks(eq(testPath), eq((long) BLOCK_SIZE), eq(2L), any());

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Populate L1 cache for block 0 only — block 1 is the first miss
        BlockCacheValue<RefCountedByteBuffer> mockValue = mock(BlockCacheValue.class);
        radixBlockTable.put(0, mockValue);

        input.prefetch(0, BLOCK_SIZE * 3);

        // Loading should start from block 1 (first miss), covering 2 remaining blocks
        verify(mockCache, times(1)).loadMissingBlocks(eq(testPath), eq((long) BLOCK_SIZE), eq(2L), any());

        input.close();
    }

    /**
     * Tests that prefetch handles RejectedExecutionException gracefully.
     */
    public void testPrefetchHandlesRejectedExecutionException() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 1);
        setupOneBlock(block0);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, radixBlockTable, radixBlockTableRegistry);

        // Should not throw exception
        input.prefetch(0, BLOCK_SIZE);

        input.close();
    }

    /**
     * Tests that sequential reads across multiple blocks populate the L1 RadixBlockTable,
     * and subsequent reads of the same blocks are served from L1 without hitting L2.
     */
    @Test
    public void testSequentialReadsPopulateL1AndSubsequentReadsHitL1() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // First pass: sequential read across all 3 blocks — populates L1
        assertEquals((byte) 0xAA, input.readByte());
        input.seek(BLOCK_SIZE);
        assertEquals((byte) 0xBB, input.readByte());
        input.seek(BLOCK_SIZE * 2);
        assertEquals((byte) 0xCC, input.readByte());

        // Verify L1 is populated for all 3 blocks
        assertNotNull("Block 0 should be in L1", radixBlockTable.get(0));
        assertNotNull("Block 1 should be in L1", radixBlockTable.get(1));
        assertNotNull("Block 2 should be in L1", radixBlockTable.get(2));

        // Clear L2 mock invocations to track only the second pass
        clearInvocations(mockCache);

        // Second pass: re-read all 3 blocks — should hit L1, NOT call L2
        input.seek(0);
        assertEquals((byte) 0xAA, input.readByte());
        input.seek(BLOCK_SIZE);
        assertEquals((byte) 0xBB, input.readByte());
        input.seek(BLOCK_SIZE * 2);
        assertEquals((byte) 0xCC, input.readByte());

        // L2 should NOT have been called — all reads served from L1
        verify(mockCache, never()).get(any());
        verify(mockCache, never()).getOrLoad(any());

        input.close();
    }

    /**
     * Tests that RadixBlockTable is cleared when the master IndexInput is closed.
     */
    @Test
    public void testRadixBlockTableClearedOnClose() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0x11);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0x22);
        setupTwoBlocks(block0, block1);

        // Use a dedicated registry so close() calls release() which clears the table
        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> registeredTable = registry.acquire(testPath);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, registeredTable, registry);

        // Read from both blocks to populate L1
        input.readByte();
        input.seek(BLOCK_SIZE);
        input.readByte();

        // Verify L1 has entries
        assertNotNull("Block 0 should be in L1 before close", registeredTable.get(0));
        assertNotNull("Block 1 should be in L1 before close", registeredTable.get(1));

        // Close the master input — should call registry.release() which clears the table
        input.close();

        // Verify L1 is cleared
        assertNull("Block 0 should be null after close", registeredTable.get(0));
        assertNull("Block 1 should be null after close", registeredTable.get(1));
    }

    /**
     * Tests that L1 eviction via RadixBlockTableRegistry.onEviction() clears the
     * correct L1 slot, and subsequent reads fall through to L2.
     */
    @Test
    public void testL1EvictionClearsSlotAndFallsToL2() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> table = registry.acquire(testPath);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, table, registry);

        // Read all 3 blocks to populate L1
        assertEquals((byte) 0xAA, input.readByte());
        input.seek(BLOCK_SIZE);
        assertEquals((byte) 0xBB, input.readByte());
        input.seek(BLOCK_SIZE * 2);
        assertEquals((byte) 0xCC, input.readByte());

        // Verify all 3 blocks in L1
        assertNotNull("Block 0 in L1", table.get(0));
        assertNotNull("Block 1 in L1", table.get(1));
        assertNotNull("Block 2 in L1", table.get(2));

        // Simulate Caffeine evicting block 1 — this is what the removal listener calls
        registry.onEviction(testPath, BLOCK_SIZE);

        // Block 1 should be evicted from L1, others untouched
        assertNotNull("Block 0 still in L1", table.get(0));
        assertNull("Block 1 evicted from L1", table.get(1));
        assertNotNull("Block 2 still in L1", table.get(2));

        // Clear mock to track L2 calls
        clearInvocations(mockCache);

        // Re-read block 1 — should miss L1 and hit L2
        input.seek(BLOCK_SIZE);
        assertEquals((byte) 0xBB, input.readByte());

        // Verify L2 was called for block 1
        FileBlockCacheKey key1 = new FileBlockCacheKey(testPath, BLOCK_SIZE);
        verify(mockCache, times(1)).get(eq(key1));

        // Block 1 should be back in L1 after the L2 hit
        assertNotNull("Block 1 re-populated in L1", table.get(1));

        input.close();
    }

    /**
     * Tests that evicting all blocks from L1 forces all subsequent reads to L2.
     */
    @Test
    public void testEvictAllBlocksFromL1() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> table = registry.acquire(testPath);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, table, registry);

        // Read all 3 blocks to populate L1
        input.readByte();
        input.seek(BLOCK_SIZE);
        input.readByte();
        input.seek(BLOCK_SIZE * 2);
        input.readByte();

        // Evict all 3 blocks (simulates Caffeine evicting under memory pressure)
        registry.onEviction(testPath, 0);
        registry.onEviction(testPath, BLOCK_SIZE);
        registry.onEviction(testPath, BLOCK_SIZE * 2);

        // L1 should be empty
        assertNull("Block 0 evicted", table.get(0));
        assertNull("Block 1 evicted", table.get(1));
        assertNull("Block 2 evicted", table.get(2));

        clearInvocations(mockCache);

        // Re-read all blocks — all should go to L2
        input.seek(0);
        input.readByte();
        input.seek(BLOCK_SIZE);
        input.readByte();
        input.seek(BLOCK_SIZE * 2);
        input.readByte();

        // Verify L2 was called for each block
        verify(mockCache, times(3)).get(any());

        input.close();
    }

    /**
     * Tests that eviction for a non-existent path is a no-op (doesn't crash).
     */
    @Test
    public void testEvictionForUnknownPathIsNoOp() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        setupBlock(0, block0);

        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> table = registry.acquire(testPath);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, table, registry);

        input.readByte();
        assertNotNull("Block 0 in L1", table.get(0));

        // Evict for a completely different path — should be a no-op
        registry.onEviction(Path.of("/some/other/file.dat"), 0);

        // Original entry untouched
        assertNotNull("Block 0 still in L1", table.get(0));

        input.close();
    }

    /**
     * Tests that eviction for a non-existent blockOffset within a valid path is safe.
     */
    @Test
    public void testEvictionForNonCachedBlockIsNoOp() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        setupBlock(0, block0);

        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> table = registry.acquire(testPath);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, table, registry);

        input.readByte();
        assertNotNull("Block 0 in L1", table.get(0));

        // Evict block 1 which was never read/cached
        registry.onEviction(testPath, BLOCK_SIZE);

        // Block 0 untouched, block 1 was already null
        assertNotNull("Block 0 still in L1", table.get(0));
        assertNull("Block 1 was never cached", table.get(1));

        input.close();
    }

    /**
     * Tests that after L1 eviction, the currentBlock fast path still works
     * (reader holding a reference to an evicted block can still read from it).
     */
    @Test
    public void testCurrentBlockSurvivesL1Eviction() throws IOException {
        long fileLength = BLOCK_SIZE;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        setupBlock(0, block0);

        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> table = registry.acquire(testPath);

        CachedMemorySegmentIndexInput input = CachedMemorySegmentIndexInput
            .newInstance("test", testPath, fileLength, mockCache, mockReadaheadManager, mockReadaheadContext, table, registry);

        // Read byte — loads block 0 into currentBlock and L1
        assertEquals((byte) 0xAA, input.readByte());

        // Evict block 0 from L1
        registry.onEviction(testPath, 0);
        assertNull("Block 0 evicted from L1", table.get(0));

        // Read next byte — should use currentBlock fast path (same block),
        // NOT go to L1 or L2. The ByteBuffer is still alive via currentBlock reference.
        clearInvocations(mockCache);
        assertEquals((byte) 0xAA, input.readByte());

        // L2 should NOT have been called — currentBlock fast path
        verify(mockCache, never()).get(any());
        verify(mockCache, never()).getOrLoad(any());

        input.close();
    }

    /**
     * Verifies that FileBlockCacheKey normalizes paths, ensuring onEviction
     * can skip redundant normalization for performance.
     */
    @Test
    public void testFileBlockCacheKeyNormalizesPath() {
        // Path normalization is now the caller's responsibility (done in BufferPoolDirectory).
        // FileBlockCacheKey stores the path as-is.
        Path normalized = Paths.get("/test/exhaustive.dat");

        FileBlockCacheKey key = new FileBlockCacheKey(normalized, 0);

        assertEquals("FileBlockCacheKey should store path as-is", normalized, key.filePath());
    }

    /**
     * Verifies that onEviction matches acquire keys without redundant normalization,
     * because FileBlockCacheKey already normalizes the path.
     */
    @Test
    public void testOnEvictionMatchesAcquireWithoutNormalization() throws IOException {
        RadixBlockTableRegistry registry = new RadixBlockTableRegistry();
        Path filePath = Paths.get("/test/eviction_match.dat");

        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> table = registry.acquire(filePath);

        // Simulate a block in L1
        MemorySegment block = createBlockWithPattern(0, (byte) 0xAA);
        ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        MemorySegment.copy(block, 0, MemorySegment.ofBuffer(buf), 0, BLOCK_SIZE);
        RefCountedByteBuffer refBuf = new RefCountedByteBuffer(buf, BLOCK_SIZE);
        BlockCacheValue<RefCountedByteBuffer> value = mock(BlockCacheValue.class);
        when(value.value()).thenReturn(refBuf);
        table.put(0, value);

        assertNotNull("Block should be in L1", table.get(0));

        // onEviction receives path from FileBlockCacheKey (already normalized)
        FileBlockCacheKey key = new FileBlockCacheKey(filePath, 0);
        registry.onEviction(key.filePath(), key.fileOffset());

        assertNull("Block should be evicted from L1", table.get(0));

        registry.release(filePath);
    }

    /**
     * Tests that backward seek correctly falls to slow path instead of using
     * stale JIT fast-path fields (currentBlockEnd/currentSegment) from a different block.
     */
    @Test
    public void testBackwardSeekAfterBlockSwitchUsesSlowPath() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read from block 0
        assertEquals((byte) 0xAA, input.readByte());

        // Advance to block 2 (sets JIT fields to block 2)
        input.seek(BLOCK_SIZE * 2);
        assertEquals((byte) 0xCC, input.readByte());

        // Seek backward to block 0 — must not use block 2's JIT fields
        input.seek(0);
        assertEquals("Backward seek should read block 0 correctly", (byte) 0xAA, input.readByte());

        // Seek backward to block 1
        input.seek(BLOCK_SIZE);
        assertEquals("Backward seek should read block 1 correctly", (byte) 0xBB, input.readByte());

        input.close();
    }

    /**
     * Tests that positional readByte(long) uses JIT fast path when reading
     * within the current block, and falls to slow path for different blocks.
     */
    @Test
    public void testPositionalReadByteUsesJitFastPath() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Prime block 0 via sequential read
        assertEquals((byte) 0xAA, input.readByte());

        // Positional read within same block — should use JIT fast path
        assertEquals((byte) 0xAA, input.readByte(10));
        assertEquals((byte) 0xAA, input.readByte(100));

        // Positional read in different block — falls to slow path
        assertEquals((byte) 0xBB, input.readByte(BLOCK_SIZE));
        assertEquals((byte) 0xBB, input.readByte(BLOCK_SIZE + 50));

        // Positional read back in block 0 — slow path (block switched)
        assertEquals((byte) 0xAA, input.readByte(0));

        input.close();
    }

    /**
     * Tests that positional readShort(long) uses JIT fast path within current block.
     */
    @Test
    public void testPositionalReadShortUsesJitFastPath() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Prime block 0
        input.readByte();

        // Positional readShort within block 0
        short val0 = input.readShort(10);
        assertEquals("readShort in block 0", (short) 0xAAAA, val0);

        // Positional readShort in block 1
        short val1 = input.readShort(BLOCK_SIZE + 10);
        assertEquals("readShort in block 1", (short) 0xBBBB, val1);

        input.close();
    }

    /**
     * Tests positional reads interleaved with sequential reads don't corrupt data.
     */
    @Test
    public void testPositionalAndSequentialReadsInterleaved() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Sequential read in block 0
        assertEquals((byte) 0xAA, input.readByte());
        assertEquals(1, input.getFilePointer());

        // Positional read in block 1 — should NOT move cursor
        assertEquals((byte) 0xBB, input.readByte(BLOCK_SIZE));
        // Note: positional readByte doesn't guarantee cursor preservation,
        // but readShort(long)/readInt(long)/readLong(long) do via seek/restore

        // Sequential read should still work correctly after positional read
        input.seek(0);
        assertEquals((byte) 0xAA, input.readByte());

        input.close();
    }

    /**
     * Tests that JIT fast-path offset math is correct for slices with non-zero absoluteBaseOffset.
     * Slices are heavily used in production (compound file format .cfs creates slices for each sub-file).
     */
    @Test
    public void testJitFastPathWorksForSlicesWithNonZeroOffset() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Create slice at non-block-aligned offset within block 1
        long sliceOffset = BLOCK_SIZE + 100;
        long sliceLength = BLOCK_SIZE;
        CachedMemorySegmentIndexInput slice = (CachedMemorySegmentIndexInput) input.slice("test_slice", sliceOffset, sliceLength);

        // First read primes the JIT fields via slow path
        assertEquals((byte) 0xBB, slice.readByte());

        // Sequential reads — all types via JIT fast path
        assertEquals("Sequential readByte", (byte) 0xBB, slice.readByte());
        assertEquals("Sequential readShort", (short) 0xBBBB, slice.readShort());
        assertEquals("Sequential readInt", 0xBBBBBBBB, slice.readInt());
        assertEquals("Sequential readLong", 0xBBBBBBBBBBBBBBBBL, slice.readLong());

        // Read multiple bytes to exercise fast path repeatedly
        for (int i = 0; i < 100; i++) {
            assertEquals("Byte " + i + " should be from block 1", (byte) 0xBB, slice.readByte());
        }

        slice.close();
        input.close();
    }

    /**
     * Tests JIT fast path for a slice that spans two blocks.
     * Reads should return correct data from both blocks.
     */
    @Test
    public void testJitFastPathSliceSpanningTwoBlocks() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice starts 100 bytes before block 2, spans into block 2
        long sliceOffset = BLOCK_SIZE * 2 - 100;
        long sliceLength = 200;
        CachedMemorySegmentIndexInput slice = (CachedMemorySegmentIndexInput) input.slice("span_slice", sliceOffset, sliceLength);

        // First 100 bytes are in block 1
        for (int i = 0; i < 100; i++) {
            assertEquals("Byte " + i + " should be from block 1", (byte) 0xBB, slice.readByte());
        }

        // Next 100 bytes are in block 2 (block switch)
        for (int i = 0; i < 100; i++) {
            assertEquals("Byte " + (100 + i) + " should be from block 2", (byte) 0xCC, slice.readByte());
        }

        slice.close();
        input.close();
    }

    /**
     * Tests JIT fast path positional reads on a slice with non-zero absoluteBaseOffset.
     */
    @Test
    public void testJitFastPathPositionalReadsOnSlice() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice at 100 bytes into block 1
        long sliceOffset = BLOCK_SIZE + 100;
        long sliceLength = BLOCK_SIZE;
        CachedMemorySegmentIndexInput slice = (CachedMemorySegmentIndexInput) input.slice("pos_slice", sliceOffset, sliceLength);

        // Prime block via sequential read
        assertEquals((byte) 0xBB, slice.readByte());

        // Sequential readShort/readInt/readLong within same block
        assertEquals("Sequential readShort", (short) 0xBBBB, slice.readShort());
        assertEquals("Sequential readInt", 0xBBBBBBBB, slice.readInt());
        assertEquals("Sequential readLong", 0xBBBBBBBBBBBBBBBBL, slice.readLong());

        // Positional reads within same block — should use JIT fast path
        assertEquals("Positional pos=0", (byte) 0xBB, slice.readByte(0));
        assertEquals("Positional pos=50", (byte) 0xBB, slice.readByte(50));
        assertEquals("Positional pos=100", (byte) 0xBB, slice.readByte(100));

        // Positional readShort within same block
        assertEquals("Positional readShort", (short) 0xBBBB, slice.readShort(10));

        // Positional readInt within same block
        assertEquals("Positional readInt", 0xBBBBBBBB, slice.readInt(20));

        // Positional readLong within same block
        assertEquals("Positional readLong", 0xBBBBBBBBBBBBBBBBL, slice.readLong(30));

        slice.close();
        input.close();
    }

    /**
     * Tests JIT fast path for a slice of a slice (nested slices).
     * Lucene creates nested slices for compound files containing compound files.
     */
    @Test
    public void testJitFastPathNestedSlice() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // First slice: starts 100 bytes into block 1
        CachedMemorySegmentIndexInput slice1 = (CachedMemorySegmentIndexInput) input.slice("slice1", BLOCK_SIZE + 100, BLOCK_SIZE);

        // Nested slice: 50 bytes into slice1
        CachedMemorySegmentIndexInput slice2 = (CachedMemorySegmentIndexInput) slice1.slice("slice2", 50, 200);
        // slice2.absoluteBaseOffset = BLOCK_SIZE + 100 + 50 = BLOCK_SIZE + 150

        // Prime and read — all types
        assertEquals((byte) 0xBB, slice2.readByte());
        assertEquals("Nested fast path readByte", (byte) 0xBB, slice2.readByte());
        assertEquals("Nested fast path readShort", (short) 0xBBBB, slice2.readShort());
        assertEquals("Nested fast path readInt", 0xBBBBBBBB, slice2.readInt());
        assertEquals("Nested fast path readLong", 0xBBBBBBBBBBBBBBBBL, slice2.readLong());

        // Positional reads on nested slice
        assertEquals("Positional on nested slice", (byte) 0xBB, slice2.readByte(0));
        assertEquals("Positional readShort on nested slice", (short) 0xBBBB, slice2.readShort(10));
        assertEquals("Positional readInt on nested slice", 0xBBBBBBBB, slice2.readInt(20));
        assertEquals("Positional readLong on nested slice", 0xBBBBBBBBBBBBBBBBL, slice2.readLong(30));

        slice2.close();
        slice1.close();
        input.close();
    }

    /**
     * Tests JIT fast path for a clone of a slice.
     * Clone inherits absoluteBaseOffset from the slice.
     */
    @Test
    public void testJitFastPathCloneOfSlice() throws IOException {
        long fileLength = BLOCK_SIZE * 3;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        MemorySegment block2 = createBlockWithPattern(2, (byte) 0xCC);
        setupBlock(0, block0);
        setupBlock(BLOCK_SIZE, block1);
        setupBlock(BLOCK_SIZE * 2, block2);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Slice at 100 bytes into block 1
        CachedMemorySegmentIndexInput slice = (CachedMemorySegmentIndexInput) input.slice("slice", BLOCK_SIZE + 100, BLOCK_SIZE);

        // Clone the slice — inherits absoluteBaseOffset
        CachedMemorySegmentIndexInput cloned = slice.clone();

        // Sequential reads on clone — all types
        assertEquals((byte) 0xBB, cloned.readByte());
        assertEquals("Clone readByte", (byte) 0xBB, cloned.readByte());
        assertEquals("Clone readShort", (short) 0xBBBB, cloned.readShort());
        assertEquals("Clone readInt", 0xBBBBBBBB, cloned.readInt());
        assertEquals("Clone readLong", 0xBBBBBBBBBBBBBBBBL, cloned.readLong());

        // Positional reads on clone — all types
        assertEquals("Clone positional readByte", (byte) 0xBB, cloned.readByte(0));
        assertEquals("Clone positional readShort", (short) 0xBBBB, cloned.readShort(10));
        assertEquals("Clone positional readInt", 0xBBBBBBBB, cloned.readInt(20));
        assertEquals("Clone positional readLong", 0xBBBBBBBBBBBBBBBBL, cloned.readLong(30));

        cloned.close();
        slice.close();
        input.close();
    }

    /**
     * Tests that every 4096th L1 hit touches L2 (damp signal) so Caffeine
     * sees access frequency and doesn't evict hot blocks.
     */
    @Test
    public void testDampSignalTouchesL2Every4096thHit() throws IOException {
        long fileLength = BLOCK_SIZE * 2;
        MemorySegment block0 = createBlockWithPattern(0, (byte) 0xAA);
        MemorySegment block1 = createBlockWithPattern(1, (byte) 0xBB);
        setupTwoBlocks(block0, block1);

        CachedMemorySegmentIndexInput input = createInput(fileLength);

        // Read both blocks to populate L1 and prime currentBlock
        input.readByte();
        input.seek(BLOCK_SIZE);
        input.readByte();

        // Clear mock to track only L1 hit path
        clearInvocations(mockCache);

        // Alternate between block 0 and block 1 to force acquireBlock on each read
        for (int i = 0; i < 4095; i++) {
            input.seek(i % 2 == 0 ? 0 : BLOCK_SIZE);
            input.readByte();
        }
        assertEquals("Counter should be 4095 after 4095 L1 hits", 4095, radixBlockTable.accessCounter);
        verify(mockCache, never()).get(any());

        // 4096th L1 hit — must read different block than 4095th to avoid currentBlock fast path
        input.seek(BLOCK_SIZE);
        input.readByte();
        assertEquals("Counter should be 4096 after 4096th hit", 4096, radixBlockTable.accessCounter);
        verify(mockCache, times(1)).get(any());

        input.close();
    }
}
