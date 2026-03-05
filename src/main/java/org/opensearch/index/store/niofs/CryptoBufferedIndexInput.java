/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.niofs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import javax.crypto.spec.SecretKeySpec;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.store.cipher.AesCipherFactory;
import org.opensearch.index.store.cipher.EncryptionAlgorithm;
import org.opensearch.index.store.cipher.EncryptionMetadataCache;
import org.opensearch.index.store.key.KeyResolver;

/**
 * An IndexInput implementation that decrypts data for reading
 *
 * @opensearch.internal
 */
final class CryptoBufferedIndexInput extends BufferedIndexInput {
    private static final byte[] ZERO_SKIP = new byte[1 << AesCipherFactory.AES_BLOCK_SIZE_BYTES_IN_POWER];
    private static final ByteBuffer EMPTY_BYTEBUFFER = ByteBuffer.allocate(0);
    private static final int CHUNK_SIZE = 16_384;

    private final FileChannel channel;
    private final boolean isClone;
    private final long off;
    private final long end;
    private final KeyResolver keyResolver;
    private final SecretKeySpec keySpec;
    private final byte[] masterKey;
    private final byte[] messageId;
    private final int footerLength;
    private final long frameSize;
    private final int frameSizePower;
    private final EncryptionAlgorithm algorithm;
    private final EncryptionMetadataCache encryptionMetadataCache;

    private ByteBuffer tmpBuffer = EMPTY_BYTEBUFFER;

    private final String normalizedFilePath;

    public CryptoBufferedIndexInput(
        String resourceDesc,
        FileChannel fc,
        IOContext context,
        KeyResolver keyResolver,
        Path filePath,
        EncryptionMetadataCache encryptionMetadataCache
    )
        throws IOException {
        super(resourceDesc, context);
        this.channel = fc;
        this.off = 0L;
        this.end = fc.size();
        this.keyResolver = keyResolver;
        this.isClone = false;
        this.normalizedFilePath = EncryptionMetadataCache.normalizePath(filePath);
        this.encryptionMetadataCache = encryptionMetadataCache;

        // Encryption disabled - use dummy values
        this.masterKey = new byte[32];
        this.messageId = new byte[16];
        this.frameSize = 0;
        this.frameSizePower = 0;
        this.algorithm = null;
        this.keySpec = null;
        this.footerLength = 0;
    }

    public CryptoBufferedIndexInput(
        String resourceDesc,
        FileChannel fc,
        long off,
        long length,
        int bufferSize,
        KeyResolver keyResolver,
        SecretKeySpec keySpec,
        int footerLength,
        long frameSize,
        int frameSizePower,
        EncryptionAlgorithm algorithm,
        byte[] masterKey,
        byte[] messageId,
        String normalizedFilePath,
        EncryptionMetadataCache encryptionMetadataCache
    )
        throws IOException {
        super(resourceDesc, bufferSize);
        this.channel = fc;
        this.off = off;
        this.end = off + length;
        this.isClone = true;
        this.keyResolver = keyResolver;
        this.keySpec = keySpec;  // Reuse keySpec from main file
        this.footerLength = footerLength;
        this.frameSize = frameSize;
        this.frameSizePower = frameSizePower;
        this.algorithm = algorithm;
        this.masterKey = masterKey;  // Passed from parent
        this.messageId = messageId;  // Passed from parent
        this.normalizedFilePath = normalizedFilePath;
        this.encryptionMetadataCache = encryptionMetadataCache;
    }

    @Override
    public void close() throws IOException {
        if (!isClone) {
            channel.close();
        }
    }

    @Override
    public CryptoBufferedIndexInput clone() {
        CryptoBufferedIndexInput clone = (CryptoBufferedIndexInput) super.clone();
        clone.tmpBuffer = EMPTY_BYTEBUFFER;
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() " + sliceDescription + " out of bounds: offset=" + offset + ", length=" + length + ", fileLength=" + this.length()
            );
        }
        return new CryptoBufferedIndexInput(
            getFullSliceDescription(sliceDescription),
            channel,
            off + offset,
            length,
            getBufferSize(),
            keyResolver,
            keySpec,  // Pass the already-derived keySpec
            footerLength,
            frameSize,
            frameSizePower,
            algorithm,
            masterKey,  // Pass directory key
            messageId,      // Pass message ID
            normalizedFilePath,
            encryptionMetadataCache
        );
    }

    @Override
    public long length() {
        // Return actual file length without footer adjustment
        return end - off;
    }

    @SuppressForbidden(reason = "FileChannel#read is efficient and used intentionally")
    private int read(ByteBuffer dst, long position) throws IOException {
        // Read plaintext data directly without decryption
        int bytesRead = channel.read(dst, position);
        return bytesRead;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        long pos = getFilePointer() + off;
        if (pos + b.remaining() > end) {
            throw new EOFException("read past EOF: pos=" + pos + ", end=" + end);
        }

        // Read plaintext data directly
        while (b.hasRemaining()) {
            final int bytesRead = read(b, pos);
            if (bytesRead < 0) {
                throw new EOFException("Unexpected EOF while reading data at pos=" + pos);
            }
            pos += bytesRead;
        }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("seek past EOF: pos=" + pos + ", length=" + length());
        }
    }
}
