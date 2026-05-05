/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Regression tests for two decryption bugs in MemorySegmentDecryptor:
 *
 * Bug 1: Arena overload of decryptInPlace() uses wrong bitmask in the
 *         alignment-skip condition. The mask is (POWER - 1) = 3 instead
 *         of (1 << POWER) - 1 = 15. File offsets where (offset % 16) is
 *         4, 8, or 12 skip the cipher alignment step, producing garbage.
 *
 * Bug 2: decryptInPlaceFrameBased() slow path (multi-frame) passes the
 *         absolute file offset to decryptInPlace() instead of the offset
 *         within the current frame. This double-applies the offset in IV
 *         computation, producing garbage for cross-frame reads.
 */
package org.opensearch.index.store.cipher;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("preview")
public class MemorySegmentDecryptorBugTests {

    private static final byte[] TEST_KEY = new byte[32];
    private static final byte[] TEST_IV = new byte[16];
    private static final int AES_BLOCK_SIZE = 1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER; // 16

    private Arena arena;

    static {
        Arrays.fill(TEST_KEY, (byte) 0x42);
        Arrays.fill(TEST_IV, (byte) 0x24);
    }

    @Before
    public void setUp() {
        arena = Arena.ofConfined();
    }

    @After
    public void tearDown() {
        if (arena != null) arena.close();
    }

    /**
     * Helper: encrypt data at a given file offset using AES-CTR with proper
     * IV and sub-block alignment, matching what the write path produces.
     */
    private byte[] encryptAtOffset(byte[] plaintext, long fileOffset) throws Exception {
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        byte[] offsetIV = AesCipherFactory.computeOffsetIVForAesGcmEncrypted(TEST_IV, fileOffset);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(offsetIV));

        int skipBytes = (int) (fileOffset & (AES_BLOCK_SIZE - 1));
        if (skipBytes > 0) {
            cipher.update(new byte[skipBytes]);
        }
        return cipher.update(plaintext);
    }

    /**
     * Helper: copy bytes into a new arena-allocated MemorySegment.
     */
    private MemorySegment toSegment(byte[] data) {
        MemorySegment seg = arena.allocate(data.length);
        for (int i = 0; i < data.length; i++) {
            seg.set(ValueLayout.JAVA_BYTE, i, data[i]);
        }
        return seg;
    }

    /**
     * Helper: read bytes back from a MemorySegment.
     */
    private byte[] fromSegment(MemorySegment seg, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = seg.get(ValueLayout.JAVA_BYTE, i);
        }
        return out;
    }

    // =========================================================================
    // Bug 1: Arena overload bitmask — offsets 4, 8, 12 within a 16-byte block
    // =========================================================================

    /**
     * fileOffset=4: (4 & 3) == 0 → skip is NOT applied (BUG)
     * but (4 & 15) == 4 → skip SHOULD be applied.
     * Decryption produces garbage because the CTR counter is not advanced.
     */
    @Test
    public void bug1_arenaOverload_offset4_producesGarbage() throws Exception {
        byte[] plaintext = "ABCDEFGHIJKLMNOP".getBytes(); // 16 bytes
        long fileOffset = 4;

        byte[] encrypted = encryptAtOffset(plaintext, fileOffset);
        MemorySegment seg = toSegment(encrypted);

        MemorySegmentDecryptor.decryptInPlace(arena, seg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        byte[] decrypted = fromSegment(seg, plaintext.length);
        assertArrayEquals(
            "Bug 1: Arena decryptInPlace at offset 4 should produce correct plaintext. "
                + "If this fails, the bitmask condition (POWER-1) vs ((1<<POWER)-1) is wrong.",
            plaintext, decrypted
        );
    }

    /**
     * fileOffset=8: (8 & 3) == 0 → skip NOT applied (BUG)
     */
    @Test
    public void bug1_arenaOverload_offset8_producesGarbage() throws Exception {
        byte[] plaintext = "0123456789abcdef".getBytes();
        long fileOffset = 8;

        byte[] encrypted = encryptAtOffset(plaintext, fileOffset);
        MemorySegment seg = toSegment(encrypted);

        MemorySegmentDecryptor.decryptInPlace(arena, seg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        assertArrayEquals(
            "Bug 1: Arena decryptInPlace at offset 8 should produce correct plaintext.",
            plaintext, fromSegment(seg, plaintext.length)
        );
    }

    /**
     * fileOffset=12: (12 & 3) == 0 → skip NOT applied (BUG)
     */
    @Test
    public void bug1_arenaOverload_offset12_producesGarbage() throws Exception {
        byte[] plaintext = "TestDataOffset12".getBytes();
        long fileOffset = 12;

        byte[] encrypted = encryptAtOffset(plaintext, fileOffset);
        MemorySegment seg = toSegment(encrypted);

        MemorySegmentDecryptor.decryptInPlace(arena, seg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        assertArrayEquals(
            "Bug 1: Arena decryptInPlace at offset 12 should produce correct plaintext.",
            plaintext, fromSegment(seg, plaintext.length)
        );
    }

    /**
     * Verify the non-Arena overload works correctly at offset 4 (it has the right mask).
     * This is the control test — proves the bug is Arena-specific.
     */
    @Test
    public void bug1_control_nonArenaOverload_offset4_works() throws Exception {
        byte[] plaintext = "ABCDEFGHIJKLMNOP".getBytes();
        long fileOffset = 4;

        byte[] encrypted = encryptAtOffset(plaintext, fileOffset);
        MemorySegment seg = toSegment(encrypted);

        MemorySegmentDecryptor.decryptInPlace(seg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

        assertArrayEquals(
            "Control: non-Arena overload at offset 4 should work correctly.",
            plaintext, fromSegment(seg, plaintext.length)
        );
    }

    /**
     * Exhaustive: test all 16 sub-block offsets for the Arena overload.
     * Offsets 0 and 1-3 work. Offsets 4,8,12 fail due to the bug.
     */
    @Test
    public void bug1_arenaOverload_allSubBlockOffsets() throws Exception {
        byte[] plaintext = "TestAllOffsets!!".getBytes(); // 16 bytes

        for (int subOffset = 0; subOffset < AES_BLOCK_SIZE; subOffset++) {
            long fileOffset = 256 + subOffset; // base at block boundary + sub-offset

            byte[] encrypted = encryptAtOffset(plaintext, fileOffset);

            try (Arena testArena = Arena.ofConfined()) {
                MemorySegment seg = testArena.allocate(encrypted.length);
                for (int i = 0; i < encrypted.length; i++) {
                    seg.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
                }

                MemorySegmentDecryptor.decryptInPlace(testArena, seg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

                byte[] decrypted = new byte[plaintext.length];
                for (int i = 0; i < decrypted.length; i++) {
                    decrypted[i] = seg.get(ValueLayout.JAVA_BYTE, i);
                }

                assertArrayEquals(
                    "Bug 1: Arena decryptInPlace fails at sub-block offset " + subOffset
                        + " (fileOffset=" + fileOffset + ")",
                    plaintext, decrypted
                );
            }
        }
    }

    // =========================================================================
    // Bug 2: decryptInPlaceFrameBased slow path — absolute vs frame-relative offset
    // =========================================================================

    /**
     * Read that spans two frames. The slow path passes currentOffset (absolute)
     * to decryptInPlace instead of (currentOffset - frameStart).
     *
     * With a small frame size (e.g., 64 bytes), a read starting at offset 48
     * with length 32 spans frames [0..63] and [64..127].
     */
    @Test
    public void bug2_frameBasedSlowPath_crossFrameRead() throws Exception {
        long frameSize = 64;
        long fileOffset = 48; // 48 bytes into frame 0
        int readLen = 32;     // spans frame 0 (bytes 48-63) and frame 1 (bytes 0-15)
        byte[] plaintext = new byte[readLen];
        Arrays.fill(plaintext, (byte) 0xAB);

        byte[] directoryKey = new byte[32];
        Arrays.fill(directoryKey, (byte) 0xDD);
        byte[] messageId = new byte[16];
        Arrays.fill(messageId, (byte) 0x11);
        String filePath = "test.dvd";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();

        // Derive frame IVs the same way decryptInPlaceFrameBased does
        // Frame 0: offsetWithinFrame=48
        byte[] frame0IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 0, 48, filePath, cache);
        // Frame 1: offsetWithinFrame=0
        byte[] frame1IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 1, 0, filePath, cache);

        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");

        // Encrypt frame 0 portion [48..64) = 16 bytes with frame0 IV
        byte[] plain0 = Arrays.copyOfRange(plaintext, 0, 16);
        Cipher c0 = AesCipherFactory.CIPHER_POOL.get();
        c0.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(frame0IV));
        byte[] enc0 = c0.update(plain0);

        // Encrypt frame 1 portion [0..16) = 16 bytes with frame1 IV
        byte[] plain1 = Arrays.copyOfRange(plaintext, 16, 32);
        Cipher c1 = AesCipherFactory.CIPHER_POOL.get();
        c1.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(frame1IV));
        byte[] enc1 = c1.update(plain1);

        // Concatenate ciphertext
        byte[] ciphertext = new byte[readLen];
        System.arraycopy(enc0, 0, ciphertext, 0, 16);
        System.arraycopy(enc1, 0, ciphertext, 16, 16);

        MemorySegment seg = toSegment(ciphertext);

        // Fresh cache so decryptInPlaceFrameBased re-derives IVs
        EncryptionMetadataCache freshCache = new EncryptionMetadataCache();

        MemorySegmentDecryptor.decryptInPlaceFrameBased(
            seg.address(), readLen, TEST_KEY, directoryKey, messageId,
            frameSize, fileOffset, filePath, freshCache
        );

        byte[] decrypted = fromSegment(seg, readLen);
        assertArrayEquals(
            "Bug 2: Cross-frame decryption should produce correct plaintext. "
                + "If this fails, the slow path passes absolute offset instead of frame-relative.",
            plaintext, decrypted
        );
    }

    /**
     * Control: single-frame read (fast path) should work correctly.
     */
    @Test
    public void bug2_control_singleFrameRead_works() throws Exception {
        long frameSize = 64;
        long fileOffset = 16;
        int readLen = 32;
        byte[] plaintext = new byte[readLen];
        Arrays.fill(plaintext, (byte) 0xCD);

        byte[] directoryKey = new byte[32];
        Arrays.fill(directoryKey, (byte) 0xDD);
        byte[] messageId = new byte[16];
        Arrays.fill(messageId, (byte) 0x11);
        String filePath = "test.dvd";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();

        // Derive frame IV for frame 0, offsetWithinFrame=16
        byte[] frameIV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 0, 16, filePath, cache);

        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(frameIV));
        byte[] encrypted = cipher.update(plaintext);

        MemorySegment seg = toSegment(encrypted);

        // Fresh cache
        EncryptionMetadataCache freshCache = new EncryptionMetadataCache();

        MemorySegmentDecryptor.decryptInPlaceFrameBased(
            seg.address(), readLen, TEST_KEY, directoryKey, messageId,
            frameSize, fileOffset, filePath, freshCache
        );

        assertArrayEquals(
            "Control: single-frame read (fast path) should work.",
            plaintext, fromSegment(seg, readLen)
        );
    }

    // =========================================================================
    // Correctness: Arena vs non-Arena parity
    // =========================================================================

    /**
     * Both overloads must produce identical output for every sub-block offset.
     * This catches any divergence between the two code paths.
     */
    @Test
    public void correctness_arenaAndNonArenaParity_allOffsets() throws Exception {
        byte[] plaintext = "ParityCheckData!".getBytes(); // 16 bytes

        for (int off = 0; off < 256; off++) {
            long fileOffset = off;
            byte[] encrypted = encryptAtOffset(plaintext, fileOffset);

            // Arena overload
            byte[] arenaResult;
            try (Arena a = Arena.ofConfined()) {
                MemorySegment s = a.allocate(encrypted.length);
                for (int i = 0; i < encrypted.length; i++) s.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
                MemorySegmentDecryptor.decryptInPlace(a, s.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);
                arenaResult = new byte[plaintext.length];
                for (int i = 0; i < arenaResult.length; i++) arenaResult[i] = s.get(ValueLayout.JAVA_BYTE, i);
            }

            // Non-Arena overload
            byte[] nonArenaResult;
            try (Arena a = Arena.ofConfined()) {
                MemorySegment s = a.allocate(encrypted.length);
                for (int i = 0; i < encrypted.length; i++) s.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);
                MemorySegmentDecryptor.decryptInPlace(s.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);
                nonArenaResult = new byte[plaintext.length];
                for (int i = 0; i < nonArenaResult.length; i++) nonArenaResult[i] = s.get(ValueLayout.JAVA_BYTE, i);
            }

            assertArrayEquals("Arena and non-Arena must match at offset " + off, arenaResult, nonArenaResult);
            assertArrayEquals("Decryption must recover plaintext at offset " + off, plaintext, arenaResult);
        }
    }

    // =========================================================================
    // Correctness: Multi-block round-trip
    // =========================================================================

    /**
     * Encrypt-then-decrypt large data (multiple AES blocks) at various non-aligned offsets.
     */
    @Test
    public void correctness_multiBlock_variousOffsets() throws Exception {
        byte[] plaintext = new byte[1024]; // 64 AES blocks
        for (int i = 0; i < plaintext.length; i++) plaintext[i] = (byte) (i & 0xFF);

        // Test offsets that are multiples of 4 but not 16 (the buggy offsets)
        long[] offsets = {0, 1, 4, 7, 8, 12, 15, 16, 20, 31, 32, 48, 100, 255, 256, 1023, 4096, 8191};

        for (long fileOffset : offsets) {
            byte[] encrypted = encryptAtOffset(plaintext, fileOffset);

            try (Arena a = Arena.ofConfined()) {
                MemorySegment seg = a.allocate(encrypted.length);
                for (int i = 0; i < encrypted.length; i++) seg.set(ValueLayout.JAVA_BYTE, i, encrypted[i]);

                MemorySegmentDecryptor.decryptInPlace(a, seg.address(), encrypted.length, TEST_KEY, TEST_IV, fileOffset);

                byte[] decrypted = new byte[plaintext.length];
                for (int i = 0; i < decrypted.length; i++) decrypted[i] = seg.get(ValueLayout.JAVA_BYTE, i);

                assertArrayEquals("Multi-block round-trip failed at offset " + fileOffset, plaintext, decrypted);
            }
        }
    }

    /**
     * Single-byte reads at every sub-block offset — simulates Lucene's readByte().
     * This is exactly the pattern that triggers the DocValues skip index crash.
     */
    @Test
    public void correctness_singleByteRead_allSubBlockOffsets() throws Exception {
        for (int subOffset = 0; subOffset < 16; subOffset++) {
            long fileOffset = 1024 + subOffset; // block-aligned base + sub-offset
            byte[] plaintext = {(byte) (0x30 + subOffset)}; // single byte

            byte[] encrypted = encryptAtOffset(plaintext, fileOffset);

            try (Arena a = Arena.ofConfined()) {
                MemorySegment seg = a.allocate(encrypted.length);
                seg.set(ValueLayout.JAVA_BYTE, 0, encrypted[0]);

                MemorySegmentDecryptor.decryptInPlace(a, seg.address(), 1, TEST_KEY, TEST_IV, fileOffset);

                byte result = seg.get(ValueLayout.JAVA_BYTE, 0);
                assertEquals("Single byte at sub-offset " + subOffset + " (fileOffset=" + fileOffset + ")",
                    plaintext[0], result);
            }
        }
    }

    // =========================================================================
    // Correctness: Frame-based with realistic frame sizes
    // =========================================================================

    /**
     * Cross-frame read with 4MB frame size (production default).
     * Bug 2 is masked here because 4MB % 16 == 0, but we verify correctness.
     */
    @Test
    public void correctness_frameBased_4MBFrameSize() throws Exception {
        long frameSize = 4 * 1024 * 1024; // 4MB
        long fileOffset = frameSize - 16;  // last 16 bytes of frame 0
        int readLen = 32;                  // spans into frame 1
        byte[] plaintext = new byte[readLen];
        for (int i = 0; i < readLen; i++) plaintext[i] = (byte) (0xA0 + i);

        byte[] directoryKey = new byte[32];
        Arrays.fill(directoryKey, (byte) 0xDD);
        byte[] messageId = new byte[16];
        Arrays.fill(messageId, (byte) 0x11);
        String filePath = "test_4mb.dvd";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();

        // Frame 0 portion: last 16 bytes
        long frame0OffsetWithin = fileOffset % frameSize; // frameSize - 16
        byte[] frame0IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 0, frame0OffsetWithin, filePath, cache);
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        Cipher c0 = AesCipherFactory.CIPHER_POOL.get();
        c0.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(frame0IV));
        byte[] enc0 = c0.update(Arrays.copyOfRange(plaintext, 0, 16));

        // Frame 1 portion: first 16 bytes
        byte[] frame1IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 1, 0, filePath, cache);
        Cipher c1 = AesCipherFactory.CIPHER_POOL.get();
        c1.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(frame1IV));
        byte[] enc1 = c1.update(Arrays.copyOfRange(plaintext, 16, 32));

        byte[] ciphertext = new byte[readLen];
        System.arraycopy(enc0, 0, ciphertext, 0, 16);
        System.arraycopy(enc1, 0, ciphertext, 16, 16);

        MemorySegment seg = toSegment(ciphertext);
        EncryptionMetadataCache freshCache = new EncryptionMetadataCache();

        MemorySegmentDecryptor.decryptInPlaceFrameBased(
            seg.address(), readLen, TEST_KEY, directoryKey, messageId,
            frameSize, fileOffset, filePath, freshCache
        );

        assertArrayEquals("4MB frame cross-frame read", plaintext, fromSegment(seg, readLen));
    }

    /**
     * Helper: encrypt data for a frame-based read, accounting for sub-block alignment.
     * The frameIV already has the block counter set for offsetWithinFrame,
     * but we still need to advance past sub-block bytes.
     */
    private byte[] encryptForFrame(byte[] plaintext, byte[] frameIV, long offsetWithinFrame) throws Exception {
        Cipher cipher = AesCipherFactory.CIPHER_POOL.get();
        SecretKeySpec keySpec = new SecretKeySpec(TEST_KEY, "AES");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(frameIV));
        int skipBytes = (int) (offsetWithinFrame & (AES_BLOCK_SIZE - 1));
        if (skipBytes > 0) {
            cipher.update(new byte[skipBytes]); // advance past sub-block bytes
        }
        return cipher.update(plaintext);
    }

    /**
     * Cross-frame read with non-aligned frame size (e.g., 48 bytes).
     * This exposes Bug 2 because the frame-relative offset
     * differs from the absolute offset in sub-block alignment.
     */
    @Test
    public void correctness_frameBased_nonAlignedFrameSize() throws Exception {
        long frameSize = 48; // not a power of 2
        long fileOffset = 40; // 40 bytes into frame 0, 8 bytes before frame boundary
        int readLen = 16;     // spans frame 0 (8 bytes) and frame 1 (8 bytes)
        byte[] plaintext = new byte[readLen];
        Arrays.fill(plaintext, (byte) 0xBB);

        byte[] directoryKey = new byte[32];
        Arrays.fill(directoryKey, (byte) 0xEE);
        byte[] messageId = new byte[16];
        Arrays.fill(messageId, (byte) 0x22);
        String filePath = "test_48.dvd";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();

        // Frame 0: offsetWithinFrame=40, 8 bytes
        byte[] f0IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 0, 40, filePath, cache);
        byte[] enc0 = encryptForFrame(Arrays.copyOfRange(plaintext, 0, 8), f0IV, 40);

        // Frame 1: offsetWithinFrame=0, 8 bytes
        byte[] f1IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 1, 0, filePath, cache);
        byte[] enc1 = encryptForFrame(Arrays.copyOfRange(plaintext, 8, 16), f1IV, 0);

        byte[] ciphertext = new byte[readLen];
        System.arraycopy(enc0, 0, ciphertext, 0, 8);
        System.arraycopy(enc1, 0, ciphertext, 8, 8);

        MemorySegment seg = toSegment(ciphertext);
        EncryptionMetadataCache freshCache = new EncryptionMetadataCache();

        MemorySegmentDecryptor.decryptInPlaceFrameBased(
            seg.address(), readLen, TEST_KEY, directoryKey, messageId,
            frameSize, fileOffset, filePath, freshCache
        );

        assertArrayEquals("48-byte frame cross-frame read", plaintext, fromSegment(seg, readLen));
    }

    /**
     * Read spanning 3 frames — exercises the slow path loop for multiple iterations.
     */
    @Test
    public void correctness_frameBased_threeFrameSpan() throws Exception {
        long frameSize = 32;
        long fileOffset = 24; // 24 bytes into frame 0
        int readLen = 48;     // frame 0: 8 bytes, frame 1: 32 bytes, frame 2: 8 bytes
        byte[] plaintext = new byte[readLen];
        for (int i = 0; i < readLen; i++) plaintext[i] = (byte) (i + 1);

        byte[] directoryKey = new byte[32];
        Arrays.fill(directoryKey, (byte) 0xCC);
        byte[] messageId = new byte[16];
        Arrays.fill(messageId, (byte) 0x33);
        String filePath = "test_3frame.dvd";
        EncryptionMetadataCache cache = new EncryptionMetadataCache();

        // Frame 0: offsetWithinFrame=24, 8 bytes
        byte[] f0IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 0, 24, filePath, cache);
        byte[] enc0 = encryptForFrame(Arrays.copyOfRange(plaintext, 0, 8), f0IV, 24);

        // Frame 1: offsetWithinFrame=0, 32 bytes (full frame)
        byte[] f1IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 1, 0, filePath, cache);
        byte[] enc1 = encryptForFrame(Arrays.copyOfRange(plaintext, 8, 40), f1IV, 0);

        // Frame 2: offsetWithinFrame=0, 8 bytes
        byte[] f2IV = AesCipherFactory.computeFrameIV(directoryKey, messageId, 2, 0, filePath, cache);
        byte[] enc2 = encryptForFrame(Arrays.copyOfRange(plaintext, 40, 48), f2IV, 0);

        byte[] ciphertext = new byte[readLen];
        System.arraycopy(enc0, 0, ciphertext, 0, 8);
        System.arraycopy(enc1, 0, ciphertext, 8, 32);
        System.arraycopy(enc2, 0, ciphertext, 40, 8);

        MemorySegment seg = toSegment(ciphertext);
        EncryptionMetadataCache freshCache = new EncryptionMetadataCache();

        MemorySegmentDecryptor.decryptInPlaceFrameBased(
            seg.address(), readLen, TEST_KEY, directoryKey, messageId,
            frameSize, fileOffset, filePath, freshCache
        );

        assertArrayEquals("3-frame spanning read", plaintext, fromSegment(seg, readLen));
    }
}
