/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.store.IndexInput;
import org.junit.Test;

/**
 * Base tests for IndexInput implementations — ported from OSS BaseIndexInputTests
 * (which extends OpenSearchIndexInputTestCase). Plain JUnit to avoid Juno runtime
 * classpath dependencies. Includes randomReadAndSlice from OpenSearchIndexInputTestCase.
 */
public abstract class BaseIndexInputTests {

    protected abstract IndexInput getIndexInput(byte[] bytes) throws IOException;

    private final Random random = new Random();

    @Test
    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeBytes(randomIntBetween(7 * 1024 + 7, 7 * 1024 + 113));
            IndexInput indexInput = getIndexInput(input);
            assertEquals(input.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, (int) indexInput.length());
            assertArrayEquals("Iteration " + i, input, output);
            indexInput.close();
        }
    }

    @Test
    public void testRandomReadsConcurrent() throws IOException {
        int numReaders = 32;
        ExecutorService readers = Executors.newFixedThreadPool(16);
        try {
            for (int i = 0; i < 100; i++) {
                int start = randomIntBetween(7, 7 * 23);
                int end = start + randomIntBetween(7, 7 * 23);
                byte[] input = randomUnicodeBytes(randomIntBetween(1024 + start, 1024 + end));
                IndexInput indexInput = getIndexInput(input);
                long length = indexInput.length();
                assertEquals(input.length, length);
                CountDownLatch latch = new CountDownLatch(numReaders);
                for (int r = 0; r < numReaders; r++) {
                    int sliceOffset = randomIntBetween(0, (int) length - 1);
                    int sliceLength = randomIntBetween(0, (int) length - sliceOffset);
                    IndexInput sliceInput = indexInput.slice("slice-" + r, sliceOffset, sliceLength);
                    readers.submit(() -> {
                        try {
                            byte[] output = randomReadAndSlice(sliceInput, sliceLength);
                            for (int j = 0; j < sliceLength; j++) {
                                assertEquals("Mismatch at " + j, input[sliceOffset + j], output[j]);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                try { latch.await(); } catch (InterruptedException e) { }
                indexInput.close();
            }
        } finally {
            readers.shutdown();
        }
    }

    @Test
    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeBytes(randomIntBetween(1, 1000));
            IndexInput indexInput = getIndexInput(input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            int bytesLeft = input.length - firstReadLen;
            try {
                int secondReadLen = bytesLeft + randomIntBetween(1, 100);
                indexInput.readBytes(new byte[secondReadLen], 0, secondReadLen);
            } catch (IOException ex) {
                // expected
            }
            indexInput.close();
        }
    }

    @Test
    public void testSeekOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeBytes(randomIntBetween(1, 1000));
            IndexInput indexInput = getIndexInput(input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            try {
                switch (randomIntBetween(0, 2)) {
                    case 0: indexInput.seek(Integer.MAX_VALUE + 4L); break;
                    case 1: indexInput.seek(-randomIntBetween(1, 10)); break;
                    case 2: indexInput.seek(input.length + randomIntBetween(1, 100)); break;
                }
                fail("Expected exception");
            } catch (IOException | IllegalArgumentException ex) {
                // expected
            }
            indexInput.close();
        }
    }

    @Test
    public void testReadBytesWithSlice() throws IOException {
        int inputLength = randomIntBetween(1024 * 50, 1024 * 100) + randomIntBetween(3, 7);
        byte[] input = randomUnicodeBytes(inputLength);
        IndexInput indexInput = getIndexInput(input);

        int sliceOffset = randomIntBetween(1, inputLength - 10);
        int sliceLength = randomIntBetween(2, inputLength - sliceOffset);
        IndexInput slice = indexInput.slice("slice", sliceOffset, sliceLength);

        byte b = slice.readByte();
        assertEquals(input[sliceOffset], b);

        // read few more bytes into a byte array
        int bytesToRead = randomIntBetween(1, sliceLength - 1);
        slice.readBytes(new byte[bytesToRead], 0, bytesToRead);

        slice.close();
        indexInput.close();
    }

    /**
     * Ported from OpenSearchIndexInputTestCase.randomReadAndSlice.
     * Reads length bytes from the input using a random mix of readByte, readBytes,
     * seek, clone, and slice operations.
     */
    private byte[] randomReadAndSlice(IndexInput indexInput, int length) throws IOException {
        int readPos = (int) indexInput.getFilePointer();
        byte[] output = new byte[length];
        while (readPos < length) {
            switch (randomIntBetween(0, 4)) {
                case 0: {
                    // Read by one byte at a time
                    output[readPos++] = indexInput.readByte();
                    break;
                }
                case 1: {
                    // Read several bytes into target
                    int len = randomIntBetween(1, length - readPos);
                    indexInput.readBytes(output, readPos, len);
                    readPos += len;
                    break;
                }
                case 2: {
                    // Read several bytes into 0-offset target
                    int len = randomIntBetween(1, length - readPos);
                    byte[] temp = new byte[len];
                    indexInput.readBytes(temp, 0, len);
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    break;
                }
                case 3: {
                    // Read using slice
                    int len = randomIntBetween(1, length - readPos);
                    IndexInput slice = indexInput.slice("slice (" + readPos + ", " + len + ")", readPos, len);
                    byte[] temp = randomReadAndSlice(slice, len);
                    assertEquals(readPos, indexInput.getFilePointer());
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    break;
                }
                case 4: {
                    // Seek at a random position and read a single byte, then seek back
                    final int lastReadPos = readPos;
                    readPos = randomIntBetween(0, length - 1);
                    indexInput.seek(readPos);
                    byte[] temp = randomReadAndSlice(indexInput, readPos + 1);
                    System.arraycopy(temp, readPos, output, readPos, 1);
                    readPos = lastReadPos;
                    indexInput.seek(readPos);
                    break;
                }
            }
        }
        return output;
    }

    private int randomIntBetween(int min, int max) {
        return min + random.nextInt(max - min + 1);
    }

    private byte[] randomUnicodeBytes(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) (32 + random.nextInt(95)));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
