/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.translog;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.apache.lucene.codecs.CodecUtil;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.AesGcmCipherFactory;
import org.opensearch.index.store.key.HkdfKeyDerivation;
import org.opensearch.index.store.key.KeyResolver;

/**
 * Manages 8KB encrypted chunks for translog files using AES-GCM authentication.
 * Handles chunk positioning, encryption, decryption, and I/O operations.
 *
 * This class separates chunking logic from FileChannel delegation, making the code
 * more maintainable and testable.
 *
 * @opensearch.internal
 */
@SuppressForbidden(reason = "Channel operations required for chunk-based encryption")
@SuppressWarnings("preview")
public class TranslogChunkManager {

    // GCM chunk constants
    /** Size of each data chunk in bytes (8KB). */
    public static final int GCM_CHUNK_SIZE = 8192;

    /** Size of GCM authentication tag in bytes (16 bytes). */
    public static final int GCM_TAG_SIZE = AesGcmCipherFactory.GCM_TAG_LENGTH;

    /** Total size of chunk plus authentication tag in bytes (8208 bytes maximum). */
    public static final int CHUNK_WITH_TAG_SIZE = GCM_CHUNK_SIZE + GCM_TAG_SIZE;

    // Thread-local buffer pool for reducing allocations
    private static final ThreadLocal<ByteBuffer> CHUNK_BUFFER_POOL = ThreadLocal
        .withInitial(() -> ByteBuffer.allocate(CHUNK_WITH_TAG_SIZE));
    private static final ThreadLocal<ByteBuffer> TRANSFER_BUFFER_POOL = ThreadLocal.withInitial(() -> ByteBuffer.allocate(GCM_CHUNK_SIZE));
    private static final ThreadLocal<byte[]> TEMP_ARRAY_POOL = ThreadLocal.withInitial(() -> new byte[GCM_CHUNK_SIZE]);

    private final FileChannel delegate;
    private final KeyResolver keyResolver;
    private final Path filePath;
    private final String translogUUID;

    // Header size - calculated exactly using TranslogHeader.headerSizeInBytes()
    private final int actualHeaderSize;

    // Base IV derived using HKDF for deterministic translog encryption
    private final byte[] baseIV;

    // Streaming cipher state for write operations
    private MemorySegment currentCipher;
    private long currentBlockNumber = 0;
    private int currentBlockBytesWritten = 0;
    private static final int BLOCK_SIZE_SHIFT = 13;
    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT; // 8KB blocks
    private long fileWritePosition = 0;

    /**
     * Helper class for chunk position mapping
     */
    public static class ChunkInfo {
        /** The chunk index (0, 1, 2, ...). */
        public final int chunkIndex;

        /** The byte position within the 8KB chunk. */
        public final int offsetInChunk;

        /** The actual file position where the chunk starts on disk. */
        public final long diskPosition;

        /**
         * Constructs a new ChunkInfo with the specified chunk coordinates.
         *
         * @param chunkIndex the chunk index (0, 1, 2, ...)
         * @param offsetInChunk the byte position within the 8KB chunk
         * @param diskPosition the actual file position where the chunk starts on disk
         */
        public ChunkInfo(int chunkIndex, int offsetInChunk, long diskPosition) {
            this.chunkIndex = chunkIndex;
            this.offsetInChunk = offsetInChunk;
            this.diskPosition = diskPosition;
        }
    }

    /**
     * Creates a new TranslogChunkManager for managing encrypted chunks.
     *
     * @param delegate the underlying FileChannel for actual I/O operations
     * @param keyResolver the key resolver for encryption operations
     * @param filePath the file path (used for logging and debugging)
     * @param translogUUID the translog UUID for exact header size calculation
     */
    public TranslogChunkManager(FileChannel delegate, KeyResolver keyResolver, Path filePath, String translogUUID) {
        if (translogUUID == null) {
            throw new IllegalArgumentException("translogUUID is required for exact header size calculation");
        }
        this.delegate = delegate;
        this.keyResolver = keyResolver;
        this.filePath = filePath;
        this.translogUUID = translogUUID;
        // Non-translog files (.ckp) don't need encryption anyway
        this.actualHeaderSize = filePath.getFileName().toString().endsWith(".tlog") ? calculateTranslogHeaderSize(translogUUID) : 0;

        // Derive base IV using HKDF instead of random IV from KeyResolver
        this.baseIV = HkdfKeyDerivation.deriveTranslogBaseIV(keyResolver.getDataKey().getEncoded(), translogUUID);
    }

    /**
     * Determines the exact header size using local calculation to avoid cross-classloader access.
     * This replicates the exact same logic as TranslogHeader.headerSizeInBytes() method.
     *
     * @return the calculated header size in bytes
     */
    public int determineHeaderSize() {
        return actualHeaderSize;
    }

    /**
     * Local implementation of TranslogHeader.headerSizeInBytes() to avoid cross-classloader access issues.
     * This replicates the exact same calculation as the original method.
     *
     * @param translogUUID the translog UUID used for calculating the UUID field size in the header
     * @return the calculated header size in bytes including codec header, UUID field, and version-specific fields
     */
    private static int calculateTranslogHeaderSize(String translogUUID) {
        int uuidLength = translogUUID.getBytes(StandardCharsets.UTF_8).length;

        // Calculate header size using official TranslogHeader constants
        int size = CodecUtil.headerLength(TranslogHeader.TRANSLOG_CODEC); // Lucene codec header
        size += Integer.BYTES + uuidLength; // uuid length field + uuid bytes

        if (TranslogHeader.CURRENT_VERSION >= TranslogHeader.VERSION_PRIMARY_TERM) {
            size += Long.BYTES;    // primary term
            size += Integer.BYTES; // checksum
        }

        return size;
    }

    /**
     * Maps a file position to chunk information including chunk index and offset within chunk.
     *
     * @param filePosition the logical file position to map to chunk coordinates
     * @return chunk information containing index, offset, and disk position
     */
    public ChunkInfo getChunkInfo(long filePosition) {
        long dataPosition = filePosition - determineHeaderSize();
        int chunkIndex = (int) (dataPosition / GCM_CHUNK_SIZE);
        int offsetInChunk = (int) (dataPosition % GCM_CHUNK_SIZE);
        long diskPosition = determineHeaderSize() + ((long) chunkIndex * GCM_CHUNK_SIZE);
        return new ChunkInfo(chunkIndex, offsetInChunk, diskPosition);
    }

    /**
     * Checks if we can read a chunk at the given disk position.
     * Returns false for write-only channels or if chunk doesn't exist.
     *
     * @param diskPosition the disk position where the chunk should be located
     * @return true if the chunk exists and can be read, false otherwise
     */
    public boolean canReadChunk(long diskPosition) {
        try {
            // Check if position is beyond current file size (new chunk)
            if (diskPosition >= delegate.size()) {
                return false;
            }

            // Test if channel is readable by attempting a zero-byte read
            ByteBuffer testBuffer = ByteBuffer.allocate(0);
            delegate.read(testBuffer, diskPosition);
            return true;

        } catch (NonReadableChannelException | IOException e) {
            // Channel is write-only
            return false;
        }
        // Other read errors - assume can't read

    }

    /**
     * Reads and decrypts a complete chunk from disk.
     * Returns empty array if chunk doesn't exist or channel is write-only.
     *
     * @param chunkIndex the index of the chunk to read and decrypt
     * @return the decrypted chunk data, or empty array if chunk doesn't exist
     * @throws IOException if reading or decryption fails
     */
    public byte[] readAndDecryptChunk(int chunkIndex) throws IOException {
        try {
            // Calculate disk position for this chunk
            long diskPosition = determineHeaderSize() + ((long) chunkIndex * GCM_CHUNK_SIZE);

            // Check if position is beyond current file size (new chunk)
            if (diskPosition >= delegate.size()) {
                return new byte[0];
            }

            // Read plaintext chunk from disk using pooled buffer
            ByteBuffer buffer = CHUNK_BUFFER_POOL.get();
            buffer.clear();
            buffer.limit(GCM_CHUNK_SIZE);
            int bytesRead = delegate.read(buffer, diskPosition);

            if (bytesRead <= 0) {
                return new byte[0]; // Empty chunk
            }

            // Extract plaintext data
            byte[] plainData = new byte[bytesRead];
            buffer.flip();
            buffer.get(plainData);

            return plainData;

        } catch (NonReadableChannelException e) {
            // Channel is write-only
            return new byte[0];
        } catch (IOException e) {
            throw new IOException("Failed to read chunk " + chunkIndex, e);
        }
    }

    /**
     * Encrypts and writes a complete chunk to disk.
     *
     * @param chunkIndex the index of the chunk to encrypt and write
     * @param plainData the plain data to encrypt and write to the chunk
     * @throws IOException if encryption or writing fails
     */
    public void encryptAndWriteChunk(int chunkIndex, byte[] plainData) throws IOException {
        try {
            // Write plaintext to disk at chunk position
            long diskPosition = determineHeaderSize() + ((long) chunkIndex * GCM_CHUNK_SIZE);
            ByteBuffer buffer = ByteBuffer.wrap(plainData);
            delegate.write(buffer, diskPosition);

        } catch (IOException e) {
            throw new IOException("Failed to write chunk " + chunkIndex + " in file " + filePath, e);
        }
    }

    /**
     * Reads data from encrypted chunks at the specified position.
     * This method handles chunk boundary crossing and decryption.
     *
     * @param dst the buffer to read data into
     * @param position the file position to read from
     * @return the number of bytes read
     * @throws IOException if reading fails
     */
    public int readFromChunks(ByteBuffer dst, long position) throws IOException {
        if (dst.remaining() == 0) {
            return 0;
        }

        int headerSize = determineHeaderSize();

        // Header reads remain unchanged
        if (position < headerSize) {
            return delegate.read(dst, position);
        }

        // Chunk-based reading for encrypted data
        ChunkInfo chunkInfo = getChunkInfo(position);

        // Read and decrypt the needed chunk
        byte[] decryptedChunk = readAndDecryptChunk(chunkInfo.chunkIndex);

        // Extract requested data from decrypted chunk
        int available = Math.max(0, decryptedChunk.length - chunkInfo.offsetInChunk);
        int toRead = Math.min(dst.remaining(), available);

        if (toRead > 0) {
            dst.put(decryptedChunk, chunkInfo.offsetInChunk, toRead);
        }

        return toRead;
    }

    /**
     * Writes data using streaming cipher with auto block management.
     * Handles block boundaries by finalizing cipher and writing tag inline.
     *
     * @param src the buffer containing data to write
     * @param position the file position to write to
     * @return the number of bytes written
     * @throws IOException if writing fails
     */
    public int writeToChunks(ByteBuffer src, long position) throws IOException {
        if (src.remaining() == 0) {
            return 0;
        }

        int headerSize = determineHeaderSize();

        // Header writes remain unchanged
        if (position < headerSize) {
            return delegate.write(src, position);
        }

        if (fileWritePosition == 0) {
            fileWritePosition = headerSize;
        }

        // Write plaintext data directly
        int written = delegate.write(src, fileWritePosition);
        fileWritePosition += written;

        return written;
    }

    /**
     * Transfers data from encrypted chunks to a target channel.
     * This method decrypts data during transfer.
     *
     * @param position the starting position in the source
     * @param count the number of bytes to transfer
     * @param target the target channel to write to
     * @return the number of bytes transferred
     * @throws IOException if transfer fails
     */
    public long transferFromChunks(long position, long count, WritableByteChannel target) throws IOException {
        long transferred = 0;
        long remaining = count;
        ByteBuffer buffer = TRANSFER_BUFFER_POOL.get();
        buffer.clear();

        while (remaining > 0 && transferred < count) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.remaining(), remaining);
            buffer.limit(toRead);

            int bytesRead = readFromChunks(buffer, position + transferred);
            if (bytesRead <= 0) {
                break;
            }

            buffer.flip();
            int bytesWritten = target.write(buffer);
            transferred += bytesWritten;
            remaining -= bytesWritten;

            if (bytesWritten < bytesRead) {
                break;
            }
        }

        return transferred;
    }

    /**
     * Transfers data from a source channel to encrypted chunks.
     * This method encrypts data during transfer.
     *
     * @param src the source channel to read from
     * @param position the starting position in the target
     * @param count the number of bytes to transfer
     * @return the number of bytes transferred
     * @throws IOException if transfer fails
     */
    public long transferToChunks(ReadableByteChannel src, long position, long count) throws IOException {
        long transferred = 0;
        long remaining = count;
        ByteBuffer buffer = TRANSFER_BUFFER_POOL.get();
        buffer.clear();

        while (remaining > 0 && transferred < count) {
            buffer.clear();
            int toRead = (int) Math.min(buffer.remaining(), remaining);
            buffer.limit(toRead);

            int bytesRead = src.read(buffer);
            if (bytesRead <= 0) {
                break;
            }

            buffer.flip();
            int bytesWritten = writeToChunks(buffer, position + transferred);
            transferred += bytesWritten;
            remaining -= bytesWritten;

            if (bytesWritten < bytesRead) {
                break;
            }
        }

        return transferred;
    }

    /**
     * Initialize GCM cipher for a new block (disabled)
     */
    private void initializeBlockCipher(long blockNumber) throws IOException {
        // Encryption disabled
    }

    /**
     * Finalize current block and write tag inline (disabled)
     */
    private void finalizeCurrentBlock() throws IOException {
        // Encryption disabled
    }

    /**
     * Close and finalize last block (disabled)
     */
    public void close() throws IOException {
        // Encryption disabled
    }
}
