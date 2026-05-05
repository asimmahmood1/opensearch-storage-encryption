/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.block_loader;

import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class AlignmentUtilTests {

    // --- alignForKernel: uses min(blockSize, pageSize) ---

    @Test
    public void AlignForKernelLocalFS() {
        // Local FS: blockSize=4096, pageSize=4096 → alignment=4096
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForKernel(8192, 8192, 4096, 4096);
        assertEquals(8192, ar.alignedOffset());
        assertEquals(0, ar.offsetDelta());
        assertEquals(8192, ar.alignedLength());
    }

    @Test
    public void AlignForKernelEFS() {
        // EFS: blockSize=1048576 (1MB), pageSize=4096 → alignment=min=4096
        // This is the key benefit: pread/io_uring avoid 1MB read amplification
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForKernel(8192, 8192, 1048576, 4096);
        assertEquals(8192, ar.alignedOffset());
        assertEquals(0, ar.offsetDelta());
        assertEquals(8192, ar.alignedLength());
    }

    @Test
    public void AlignForKernelUnalignedOffset() {
        // offset=5000 is not 4096-aligned → aligns down to 4096
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForKernel(5000, 100, 4096, 4096);
        assertEquals(4096, ar.alignedOffset());
        assertEquals(904, ar.offsetDelta()); // 5000 - 4096
        assertTrue(ar.alignedLength() >= 904 + 100); // must cover the requested range
        assertEquals(0, ar.alignedLength() % 4096); // must be alignment-multiple
    }

    @Test
    public void AlignForKernelSmallBlockSize() {
        // blockSize=512, pageSize=4096 → alignment=min(512,4096)=512
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForKernel(1000, 100, 512, 4096);
        assertEquals(512, ar.alignedOffset()); // 1000 aligned down to 512
        assertEquals(488, ar.offsetDelta()); // 1000 - 512
        assertEquals(0, ar.alignedLength() % 512);
    }

    // --- alignForFileChannel: uses max(blockSize, pageSize) ---

    @Test
    public void AlignForFileChannelLocalFS() {
        // Local FS: blockSize=4096, pageSize=4096 → alignment=4096
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForFileChannel(8192, 8192, 4096, 4096);
        assertEquals(8192, ar.alignedOffset());
        assertEquals(0, ar.offsetDelta());
        assertEquals(8192, ar.alignedLength());
    }

    @Test
    public void AlignForFileChannelEFS() {
        // EFS: blockSize=1048576 (1MB), pageSize=4096 → alignment=max=1MB
        // FileChannel is forced to read 1MB even for 8KB request
        AlignmentUtil.AlignedRead ar = AlignmentUtil.alignForFileChannel(8192, 8192, 1048576, 4096);
        assertEquals(0, ar.alignedOffset()); // 8192 aligned down to 0 (1MB boundary)
        assertEquals(8192, ar.offsetDelta());
        assertEquals(1048576, ar.alignedLength()); // full 1MB read
    }

    // --- Invariants ---

    @Test
    public void AlignedOffsetNeverExceedsOriginal() {
        for (int i = 0; i < 100; i++) {
            long offset = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2 + 1);
            long length = ThreadLocalRandom.current().nextLong(1, 10_000_000 + 1);
            int blockSize = 1 << ThreadLocalRandom.current().nextInt(9, 21); // 512 to 1MB
            int pageSize = 4096;

            AlignmentUtil.AlignedRead kernel = AlignmentUtil.alignForKernel(offset, length, blockSize, pageSize);
            assertTrue("alignedOffset must be <= offset", kernel.alignedOffset() <= offset);
            assertTrue("aligned range must cover requested range",
                kernel.alignedOffset() + kernel.alignedLength() >= offset + length);

            AlignmentUtil.AlignedRead fc = AlignmentUtil.alignForFileChannel(offset, length, blockSize, pageSize);
            assertTrue("alignedOffset must be <= offset", fc.alignedOffset() <= offset);
            assertTrue("aligned range must cover requested range",
                fc.alignedOffset() + fc.alignedLength() >= offset + length);
        }
    }

    @Test
    public void KernelAlignmentNeverExceedsFileChannelAlignment() {
        for (int i = 0; i < 100; i++) {
            long offset = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE / 2 + 1);
            long length = ThreadLocalRandom.current().nextLong(1, 10_000_000 + 1);
            int blockSize = 1 << ThreadLocalRandom.current().nextInt(9, 21);
            int pageSize = 4096;

            AlignmentUtil.AlignedRead kernel = AlignmentUtil.alignForKernel(offset, length, blockSize, pageSize);
            AlignmentUtil.AlignedRead fc = AlignmentUtil.alignForFileChannel(offset, length, blockSize, pageSize);

            // Kernel alignment should read <= FileChannel alignment (less amplification)
            assertTrue("kernel aligned read should be <= FileChannel aligned read",
                kernel.alignedLength() <= fc.alignedLength());
        }
    }

    // --- Error cases ---

    @Test
    public void NonPowerOf2AlignmentThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> AlignmentUtil.align(0, 100, 3));
    }

    @Test
    public void ZeroAlignmentThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> AlignmentUtil.align(0, 100, 0));
    }
}
