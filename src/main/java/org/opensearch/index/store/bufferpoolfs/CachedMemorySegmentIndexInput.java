/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_MASK;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE;
import static org.opensearch.index.store.bufferpoolfs.StaticConfigs.CACHE_BLOCK_SIZE_POWER;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.GroupVIntUtil;
import org.opensearch.index.store.block.RefCountedByteBuffer;
import org.opensearch.index.store.block_cache.BlockCache;
import org.opensearch.index.store.block_cache.BlockCacheValue;
import org.opensearch.index.store.block_cache.FileBlockCacheKey;
import org.opensearch.index.store.hll.WorkingSetEstimator;
import org.opensearch.index.store.read_ahead.ReadaheadContext;
import org.opensearch.index.store.read_ahead.ReadaheadManager;

/**
 * A high-performance IndexInput implementation that uses memory-mapped segments with block-level caching.
 * 
 * <p>This implementation provides :
 * <ul>
 * <li>Block-aligned cached memory segments for efficient random access</li>
 * <li>Reference counting to manage memory lifecycle</li>
 * <li>Read-ahead support for sequential access patterns</li>
 * <li>Optimized bulk operations for primitive arrays</li>
 * <li>Slice support with offset management</li>
 * </ul>
 * 
 * <p>The class uses a {@link RadixBlockTable} for L1 caching and falls back to
 * the main {@link BlockCache} (Caffeine L2) for cache misses.
 *
 * <p>The L1 cache provides zero-collision, lock-free lookups via two plain array reads.
 * 
 * @opensearch.internal
 */
@SuppressWarnings("preview")
public class CachedMemorySegmentIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger LOGGER = LogManager.getLogger(CachedMemorySegmentIndexInput.class);
    private static final WorkingSetEstimator WORKING_SET_ESTIMATOR = WorkingSetEstimator.getInstance();

    static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
    static final ValueLayout.OfShort LAYOUT_LE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfFloat LAYOUT_LE_FLOAT = ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    final long length;

    final Path path;
    final int pathHash; // cached path.hashCode() for WorkingSetEstimator
    final BlockCache<RefCountedByteBuffer> blockCache;
    final ReadaheadManager readaheadManager;
    final ReadaheadContext readaheadContext;

    final long absoluteBaseOffset; // absolute position in original file where this input starts
    final boolean isSlice; // true for slices, false for main instances

    long curPosition = 0L; // absolute position within this input (0-based)
    volatile boolean isOpen = true;

    // Single block cache for current access
    private long currentBlockOffset = -1;
    private BlockCacheValue<RefCountedByteBuffer> currentBlock = null;

    // Cached offset from last getCacheBlockWithOffset call (avoid BlockAccess allocation)
    private int lastOffsetInBlock;

    // --- JIT-friendly fast-path fields ---
    // Pre-computed boundary: the position (relative to this input) where the current block ends.
    private long currentBlockEnd = 0L;
    // Cached MemorySegment for direct access without getCacheBlockWithOffset
    private MemorySegment currentSegment;
    // Block-aligned file offset of the current block (used to compute offset within segment)
    private long currentBlockStart;
    private long currentBlockStartRelative; // = currentBlockStart - absoluteBaseOffset (input-relative)

    private final RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> radixBlockTable;
    private final RadixBlockTableRegistry radixBlockTableRegistry; // shared by master and slices; only master calls release()

    // Safe because IndexInput instances are not thread-safe per Lucene contract -
    // each thread must use its own clone().
    private boolean lastAccessWasCacheHit;

    /**
     * Creates a new CachedMemorySegmentIndexInput instance.
     * 
     * @param resourceDescription description of the resource for debugging
     * @param path the file path being accessed
     * @param length the length of the file in bytes
     * @param blockCache the main block cache for storing memory segments
     * @param readaheadManager manager for read-ahead operations
     * @param readaheadContext context for read-ahead policy decisions
     * @param radixBlockTable L1 cache for recently accessed blocks
     * @param radixBlockTableRegistry registry for lifecycle management (release on close)
     * @return a new CachedMemorySegmentIndexInput instance
     */
    public static CachedMemorySegmentIndexInput newInstance(
        String resourceDescription,
        Path path,
        long length,
        BlockCache<RefCountedByteBuffer> blockCache,
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> radixBlockTable,
        RadixBlockTableRegistry radixBlockTableRegistry
    ) {
        CachedMemorySegmentIndexInput input = new CachedMemorySegmentIndexInput(
            resourceDescription,
            path,
            0,
            length,
            blockCache,
            readaheadManager,
            readaheadContext,
            false,
            radixBlockTable,
            radixBlockTableRegistry
        );
        try {
            input.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
        return input;
    }

    private CachedMemorySegmentIndexInput(
        String resourceDescription,
        Path path,
        long absoluteBaseOffset,
        long length,
        BlockCache<RefCountedByteBuffer> blockCache,
        ReadaheadManager readaheadManager,
        ReadaheadContext readaheadContext,
        boolean isSlice,
        RadixBlockTable<BlockCacheValue<RefCountedByteBuffer>> radixBlockTable,
        RadixBlockTableRegistry radixBlockTableRegistry
    ) {
        super(resourceDescription);
        this.path = path.toAbsolutePath().normalize();
        this.pathHash = this.path.hashCode();
        this.absoluteBaseOffset = absoluteBaseOffset;
        this.length = length;
        this.blockCache = blockCache;
        this.readaheadManager = readaheadManager;
        this.readaheadContext = readaheadContext;
        this.isSlice = isSlice;
        this.radixBlockTable = radixBlockTable;
        this.radixBlockTableRegistry = radixBlockTableRegistry;
    }

    void ensureOpen() {
        if (!isOpen) {
            throw alreadyClosed(null);
        }
    }

    // the unused parameter is just to silence javac about unused variables
    RuntimeException handlePositionalIOOBE(RuntimeException unused, String action, long pos) throws IOException {
        if (pos < 0L) {
            return new IllegalArgumentException(action + " negative position (pos=" + pos + "): " + this);
        } else {
            throw new EOFException(action + " past EOF (pos=" + pos + "): " + this);
        }
    }

    // the unused parameter is just to silence javac about unused variables
    AlreadyClosedException alreadyClosed(RuntimeException unused) {
        return new AlreadyClosedException("Already closed: " + this);
    }

    /**
    * Optimized method to get both cache block and offset in one operation.
    * Fast path kept small for JIT inlining.
    *
    * @param pos position relative to this input
    * @return MemorySegment for the cache block (offset available in lastOffsetInBlock)
    * @throws IOException if the block cannot be acquired
    */
    private MemorySegment getCacheBlockWithOffset(long pos) throws IOException {
        final long fileOffset = absoluteBaseOffset + pos;
        final long blockOffset = fileOffset & ~CACHE_BLOCK_MASK;
        lastOffsetInBlock = (int) (fileOffset - blockOffset);

        // Fast path: reuse current block if still valid.
        if (blockOffset == currentBlockOffset && currentBlock != null) {
            return currentSegment;
        }
        return acquireCacheBlockOnMiss(blockOffset);
    }

    /**
     * Slow path for cache block acquisition — separated to keep the fast path
     * small enough for JIT inlining.
     */
    private MemorySegment acquireCacheBlockOnMiss(long blockOffset) throws IOException {
        lastAccessWasCacheHit = false;

        final BlockCacheValue<RefCountedByteBuffer> cacheValue = acquireBlock(blockOffset);

        currentBlockOffset = blockOffset;
        currentBlock = cacheValue;

        // Update JIT fast-path fields
        final MemorySegment seg = cacheValue.value().segment();
        currentSegment = seg;
        currentBlockStart = blockOffset;
        currentBlockStartRelative = blockOffset - absoluteBaseOffset;
        currentBlockEnd = currentBlockStartRelative + seg.byteSize();

        // Notify readahead manager of access pattern
        if (readaheadContext != null) {
            readaheadContext.onAccess(blockOffset, lastAccessWasCacheHit);
        }

        return seg;
    }

    /**
     * Acquires a block for the given block offset, checking L1 (RadixBlockTable)
     * first, then falling back to L2 (Caffeine), then loading from disk.
     *
     * <p>L1 lookup is two plain array reads with no synchronization. On L1 miss,
     * falls through to L2 and publishes back to L1.
     *
     * @param blockOffset the block-aligned file offset
     * @return a BlockCacheValue for the block
     * @throws IOException if the block cannot be acquired after max attempts
     */
    private BlockCacheValue<RefCountedByteBuffer> acquireBlock(long blockOffset) throws IOException {
        final long blockId = blockOffset >>> CACHE_BLOCK_SIZE_POWER;

        // Update working set estimator for HLL-based cache sizing
        try {
            WORKING_SET_ESTIMATOR.update(pathHash, blockOffset);
        } catch (Exception e) {
            LOGGER.warn("Failed to update WorkingSetEstimator for block offset {}", blockOffset, e);
        }

        // ---- L1 lookup: two plain array reads, no fences, no CAS ----
        BlockCacheValue<RefCountedByteBuffer> entry = radixBlockTable.get(blockId);
        if (entry != null) {
            lastAccessWasCacheHit = true;
            if (radixBlockTableRegistry != null) radixBlockTableRegistry.recordHit();
            blockCache.checkPrefetchLeadHit(path, blockOffset);
            // Damp signal: every 4096th L1 hit, touch L2 so Caffeine sees access frequency
            if ((++radixBlockTable.accessCounter & RadixBlockTable.SAMPLE_MASK) == 0) {
                blockCache.get(new FileBlockCacheKey(path, blockOffset));
            }
            return entry;
        }
        if (radixBlockTableRegistry != null) radixBlockTableRegistry.recordMiss();
        // Check if prefetch was supposed to have this block ready but hasn't finished
        blockCache.checkPrefetchLeadMiss(path, blockOffset);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[READ] file={} blockId={} blockOffset={}",
                path.getFileName(), blockId, blockOffset);
        }
        // ---- L2 lookup + disk load ----
        final FileBlockCacheKey key = new FileBlockCacheKey(path, blockOffset);
        // Try L2 hit
        BlockCacheValue<RefCountedByteBuffer> v = blockCache.get(key);
        if (v != null) {
            radixBlockTable.put(blockId, v);
            lastAccessWasCacheHit = true;
            if (blockCache.consumeReadAheadHit(path, blockOffset)) {
                WORKING_SET_ESTIMATOR.recordReadAheadAccess();
            }
            return v;
        }
        // L2 miss — load from disk (deduped by Caffeine)
        BlockCacheValue<RefCountedByteBuffer> loaded = blockCache.getOrLoad(key);
        if (loaded != null) {
            radixBlockTable.put(blockId, loaded);
            lastAccessWasCacheHit = false;
            return loaded;
        }
        throw new IOException("Unable to acquire block for offset " + blockOffset);
    }

    /**
     * Slow path for sequential reads — called only at block boundaries (~1 in CACHE_BLOCK_SIZE calls).
     * Kept as a separate method so the JIT can keep readByte/readShort/readInt/readLong tiny.
     */
    private byte readByteSlow(long pos) throws IOException {
        try {
            final MemorySegment seg = getCacheBlockWithOffset(pos);
            final byte v = seg.get(LAYOUT_BYTE, lastOffsetInBlock);
            curPosition = pos + 1;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private short readShortSlow(long pos) throws IOException {
        try {
            final MemorySegment seg = getCacheBlockWithOffset(pos);
            final int off = lastOffsetInBlock;
            if (off + Short.BYTES > seg.byteSize()) {
                return super.readShort();
            }
            final short v = seg.get(LAYOUT_LE_SHORT, off);
            curPosition = pos + Short.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private int readIntSlow(long pos) throws IOException {
        try {
            final MemorySegment seg = getCacheBlockWithOffset(pos);
            final int off = lastOffsetInBlock;
            if (off + Integer.BYTES > seg.byteSize()) {
                return super.readInt();
            }
            final int v = seg.get(LAYOUT_LE_INT, off);
            curPosition = pos + Integer.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    private long readLongSlow(long pos) throws IOException {
        try {
            final MemorySegment seg = getCacheBlockWithOffset(pos);
            final int off = lastOffsetInBlock;
            if (off + Long.BYTES > seg.byteSize()) {
                return super.readLong();
            }
            final long v = seg.get(LAYOUT_LE_LONG, off);
            curPosition = pos + Long.BYTES;
            return v;
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final byte readByte() throws IOException {
        final long pos = curPosition;
        if (pos >= currentBlockStartRelative && pos < currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            final byte v = currentSegment.get(LAYOUT_BYTE, off);
            curPosition = pos + 1;
            return v;
        }
        return readByteSlow(pos);
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0)
            return;

        final long startPos = curPosition; // avoid virtual call
        int remaining = len;
        int bufferOffset = offset;
        long currentPos = startPos;

        try {
            while (remaining > 0) {
                final MemorySegment seg = getCacheBlockWithOffset(currentPos);
                final int offInBlock = lastOffsetInBlock;
                final int avail = (int) (seg.byteSize() - offInBlock);

                // Fast path: full block copy
                if (offInBlock == 0 && remaining >= CACHE_BLOCK_SIZE && seg.byteSize() >= CACHE_BLOCK_SIZE) {
                    MemorySegment.copy(seg, LAYOUT_BYTE, 0L, b, bufferOffset, CACHE_BLOCK_SIZE);
                    remaining -= CACHE_BLOCK_SIZE;
                    bufferOffset += CACHE_BLOCK_SIZE;
                    currentPos += CACHE_BLOCK_SIZE;
                    continue;
                }

                // Partial block
                final int toRead = Math.min(remaining, avail);
                MemorySegment.copy(seg, LAYOUT_BYTE, offInBlock, b, bufferOffset, toRead);

                remaining -= toRead;
                bufferOffset += toRead;
                currentPos += toRead;
            }

            curPosition = startPos + len;

        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Integer.BYTES * (long) length;

        try {
            final MemorySegment segment = getCacheBlockWithOffset(startPos);
            final int offsetInBlock = lastOffsetInBlock;

            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                MemorySegment.copy(segment, LAYOUT_LE_INT, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readInts(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Long.BYTES * (long) length;

        try {
            final MemorySegment segment;
            final int offsetInBlock;

            segment = getCacheBlockWithOffset(startPos);
            offsetInBlock = lastOffsetInBlock;

            // Check if entire read fits in current cache block
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(segment, LAYOUT_LE_LONG, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                // Slow path: spans cache blocks, fall back to super implementation
                super.readLongs(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int length) throws IOException {
        if (length == 0)
            return;

        final long startPos = getFilePointer();
        final long totalBytes = Float.BYTES * (long) length;

        try {
            final MemorySegment segment;
            final int offsetInBlock;

            segment = getCacheBlockWithOffset(startPos);
            offsetInBlock = lastOffsetInBlock;

            // Check if entire read fits in current cache block
            if (offsetInBlock + totalBytes <= segment.byteSize()) {
                // Fast path: entire read fits in one cache block
                MemorySegment.copy(segment, LAYOUT_LE_FLOAT, offsetInBlock, dst, offset, length);
                curPosition += totalBytes;
            } else {
                super.readFloats(dst, offset, length);
            }
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", startPos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final short readShort() throws IOException {
        final long pos = curPosition;
        if (pos >= currentBlockStartRelative && pos + Short.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            final short v = currentSegment.get(LAYOUT_LE_SHORT, off);
            curPosition = pos + Short.BYTES;
            return v;
        }
        return readShortSlow(pos);
    }

    @Override
    public final int readInt() throws IOException {
        final long pos = curPosition;
        if (pos >= currentBlockStartRelative && pos + Integer.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            final int v = currentSegment.get(LAYOUT_LE_INT, off);
            curPosition = pos + Integer.BYTES;
            return v;
        }
        return readIntSlow(pos);
    }

    @Override
    public final long readLong() throws IOException {
        final long pos = curPosition;
        if (pos >= currentBlockStartRelative && pos + Long.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            final long v = currentSegment.get(LAYOUT_LE_LONG, off);
            curPosition = pos + Long.BYTES;
            return v;
        }
        return readLongSlow(pos);
    }

    @Override
    public void readGroupVInt(int[] dst, int offset) throws IOException {
        try {
            final MemorySegment segment;
            final int offsetInBlock;

            segment = getCacheBlockWithOffset(curPosition);
            offsetInBlock = lastOffsetInBlock;

            final int len = GroupVIntUtil
                .readGroupVInt(
                    this,
                    segment.byteSize() - offsetInBlock,
                    p -> segment.get(LAYOUT_LE_INT, p),
                    offsetInBlock,
                    dst,
                    offset
                );
            curPosition += len;
        } catch (IllegalStateException | NullPointerException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final int readVInt() throws IOException {
        // this can make JVM less confused (see LUCENE-10366)
        return super.readVInt();
    }

    @Override
    public final long readVLong() throws IOException {
        // this can make JVM less confused (see LUCENE-10366)
        return super.readVLong();
    }

    @Override
    public long getFilePointer() {
        ensureOpen();
        return curPosition;
    }

    /**
     * Returns the absolute file offset for the current position.
     * This is useful for cache keys, encryption, and other operations that need
     * the actual position in the original file.
     * 
     * @return the absolute byte offset in the original file
     */
    public long getAbsoluteFileOffset() {
        return absoluteBaseOffset + getFilePointer();
    }

    /**
     * Returns the absolute file offset for a given position within this input.
     * This is useful for cache keys, encryption, and other operations that need
     * the actual position in the original file for random access operations.
     * 
     * @param pos position relative to this input (0-based)
     * @return absolute position in the original file
     */
    public long getAbsoluteFileOffset(long pos) {
        return absoluteBaseOffset + pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (pos < 0 || pos > length) {
            throw handlePositionalIOOBE(null, "seek", pos);
        }
        this.curPosition = pos;
    }

    @Override
    public byte readByte(long pos) throws IOException {
        if (pos < 0 || pos >= length) {
            return 0;
        }
        if (pos >= currentBlockStartRelative && pos < currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            return currentSegment.get(LAYOUT_BYTE, off);
        }
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            return segment.get(LAYOUT_BYTE, lastOffsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        if (pos >= currentBlockStartRelative && pos + Short.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            return currentSegment.get(LAYOUT_LE_SHORT, off);
        }
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Short.BYTES > segment.byteSize()) {
                long savedPos = getFilePointer();
                try {
                    seek(pos);
                    return readShort();
                } finally {
                    seek(savedPos);
                }
            }
            return segment.get(LAYOUT_LE_SHORT, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        if (pos >= currentBlockStartRelative && pos + Integer.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            return currentSegment.get(LAYOUT_LE_INT, off);
        }
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Integer.BYTES > segment.byteSize()) {
                long savedPos = getFilePointer();
                try {
                    seek(pos);
                    return readInt();
                } finally {
                    seek(savedPos);
                }
            }
            return segment.get(LAYOUT_LE_INT, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        if (pos >= currentBlockStartRelative && pos + Long.BYTES <= currentBlockEnd) {
            final long off = absoluteBaseOffset + pos - currentBlockStart;
            return currentSegment.get(LAYOUT_LE_LONG, off);
        }
        try {
            final MemorySegment segment = getCacheBlockWithOffset(pos);
            final int offsetInBlock = lastOffsetInBlock;
            if (offsetInBlock + Long.BYTES > segment.byteSize()) {
                long savedPos = getFilePointer();
                try {
                    seek(pos);
                    return readLong();
                } finally {
                    seek(savedPos);
                }
            }
            return segment.get(LAYOUT_LE_LONG, offsetInBlock);
        } catch (IndexOutOfBoundsException ioobe) {
            throw handlePositionalIOOBE(ioobe, "read", pos);
        } catch (NullPointerException | IllegalStateException e) {
            throw alreadyClosed(e);
        }
    }

    @Override
    public final long length() {
        return length;
    }

    @Override
    public final CachedMemorySegmentIndexInput clone() {
        final CachedMemorySegmentIndexInput clone = buildSlice((String) null, 0L, this.length);
        try {
            clone.seek(getFilePointer());
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }

        return clone;
    }

    /**
     * Creates a slice of this index input, with the given description, offset, and length. The slice
     * is seeked to the beginning.
     */
    @Override
    public final CachedMemorySegmentIndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length
                    + ": "
                    + this
            );
        }

        var slice = buildSlice(sliceDescription, offset, length);

        slice.seek(0L);

        return slice;
    }

    /** Builds the actual sliced IndexInput. * */
    CachedMemorySegmentIndexInput buildSlice(String sliceDescription, long sliceOffset, long length) {
        ensureOpen();
        // Calculate absolute base offset for the slice
        final long sliceAbsoluteBaseOffset = this.absoluteBaseOffset + sliceOffset;
        final String newResourceDescription = getFullSliceDescription(sliceDescription);

        CachedMemorySegmentIndexInput slice = new CachedMemorySegmentIndexInput(
            newResourceDescription,
            path,
            sliceAbsoluteBaseOffset,
            length,
            blockCache,
            readaheadManager,
            readaheadContext,
            true,
            radixBlockTable,
            radixBlockTableRegistry // slices share the table and registry for metrics, but don't call release()
        );

        try {
            slice.seek(0L);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }

        return slice;
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        ensureOpen();
        // Guard against corrupt length values from VaryingBPV rankSlice reads
        if (length <= 0 || length > this.length || offset < 0 || offset + length > this.length) {
            return;
        }

        final long startFileOffset = absoluteBaseOffset + offset;
        final long startBlockOffset = startFileOffset & ~CACHE_BLOCK_MASK;
        final long endFileOffset = absoluteBaseOffset + offset + length;
        final long endBlockOffset = (endFileOffset + CACHE_BLOCK_MASK) & ~CACHE_BLOCK_MASK;
        final long blockCount = (endBlockOffset - startBlockOffset) >>> CACHE_BLOCK_SIZE_POWER;
        final long startBlockId = startBlockOffset >>> CACHE_BLOCK_SIZE_POWER;

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[PREFETCH] file={} offset={} len={} blocks=[{}..{}] count={}",
                path.getFileName(), offset, length, startBlockId, startBlockId + blockCount - 1, blockCount);
        }

        if (blockCount == 1) {
            if (radixBlockTable.get(startBlockId) != null) {
                blockCache.recordPrefetchL1Hit(1);
                return;
            }
            blockCache.recordPrefetchL1Miss(1);
        } else {
            // Find the first missing block in L1 cache. Sequential blockId access is
            // branch-predictor and CPU-prefetch friendly — RadixBlockTable.get() is two
            // plain array loads, no locks, no CAS.
            long firstMissing = blockCount; // sentinel: all present
            for (long i = 0; i < blockCount; i++) {
                if (radixBlockTable.get(startBlockId + i) == null) {
                    firstMissing = i;
                    break;
                }
            }
            if (firstMissing == blockCount) {
                blockCache.recordPrefetchL1Hit(blockCount);
                return;
            }
            if (firstMissing > 0) {
                blockCache.recordPrefetchL1Hit(firstMissing);
                blockCache.recordPrefetchL1Miss(blockCount - firstMissing);
                // Skip leading cached blocks — start loading from the first miss
                blockCache.loadMissingBlocks(path,
                    startBlockOffset + (firstMissing << CACHE_BLOCK_SIZE_POWER),
                    blockCount - firstMissing,
                    radixBlockTable::put);
                return;
            }
            blockCache.recordPrefetchL1Miss(blockCount);
        }

        blockCache.loadMissingBlocks(path, startBlockOffset, blockCount, radixBlockTable::put);
    }

    @Override
    @SuppressWarnings("ConvertToTryWithResources")
    public final void close() throws IOException {
        if (!isOpen) {
            return;
        }

        // Mark as closed to ensure all future accesses throw AlreadyClosedException
        isOpen = false;

        // Release current block reference — GC handles cleanup
        currentBlock = null;
        currentBlockOffset = -1;
        currentBlockEnd = 0L;
        currentSegment = null;
        currentBlockStart = 0L;
        currentBlockStartRelative = 0L;

        if (!isSlice) {
            // Master instance cleanup
            if (radixBlockTableRegistry != null) {
                // Release our ref in the registry; when refCount reaches 0
                // the table is cleared and removed from the registry.
                radixBlockTableRegistry.release(path);
            }

            readaheadManager.close();
        }
        // Slices share cache, readahead manager, and radix table, so don't close them
    }
}
