/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.index.store.bufferpoolfs;

/**
 * A two-level radix table mapping blockIds to values of type {@code V}.
 * Designed as a per-file L1 lookup cache, but generic enough for any
 * blockId-to-value mapping.
 *
 * <h2>Structure</h2>
 * <pre>
 *   outer index = blockId >>> PAGE_SHIFT   (which group of 1024)
 *   inner slot  = blockId &amp; PAGE_MASK     (position within group)
 * </pre>
 *
 * The outer level defaults to {@value #DEFAULT_OUTER_SLOTS} entries (2 KB),
 * covering up to 2 GB of file data with 8 KB cache blocks. It can grow
 * lazily on demand for larger files. Each inner array is a fixed-size
 * {@code Object[PAGE_SIZE]} (1024 slots / 8 KB), directly indexed by
 * the inner slot — no popcount, no COW. Inner arrays are reclaimed (nulled)
 * when all their slots become empty, keeping memory overhead proportional
 * to actual cached blocks.
 *
 * <h2>Thread safety</h2>
 * <ul>
 *   <li>Reads are plain array loads — no fences, no synchronization.
 *       JLS §17.7 guarantees reference writes/reads are atomic, so a reader
 *       either sees the old value or the new value, never a torn reference.</li>
 *   <li>Stale reads are benign: a stale {@code null} simply means an L1 miss,
 *       falling through to the Caffeine L2 cache which is the source of truth.</li>
 *   <li>Writers perform plain stores. Concurrent inner-array allocation is guarded by
 *       {@code synchronized(this)} only for the allocation; subsequent slot
 *       writes are plain stores.</li>
 *   <li>Inner-array reclamation on {@link #remove} is guarded by
 *       {@code synchronized(this)} to avoid races with concurrent
 *       {@link #put} that may be allocating the same inner array.</li>
 *   <li>No {@code volatile}, no {@code VarHandle}, no CAS on the read path.</li>
 * </ul>
 *
 * @param <V> the type of values stored in the table
 */
public final class RadixBlockTable<V> {

    /** Each inner array covers 1024 consecutive blockIds. */
    public static final int PAGE_SHIFT = 10;
    public static final int PAGE_SIZE = 1 << PAGE_SHIFT; // 1024
    private static final int PAGE_MASK = PAGE_SIZE - 1;   // 1023

    /**
     * Default outer directory size. 256 x 1024 = 262,144 block IDs.
     * With 8 KB cache blocks this covers 2 GB per file without growth.
     */
    public static final int DEFAULT_OUTER_SLOTS = 256;

    /**
     * Plain int counter for L2 damp signaling. Incremented on every L1 hit.
     * When (accessCounter & SAMPLE_MASK) == 0, the caller touches L2 so Caffeine
     * sees the access frequency and doesn't evict hot blocks.
     *
     * <p>Not volatile — races can only lose increments (fewer L2 touches),
     * never corrupt state. int writes are atomic per JLS §17.7.
     * Overflow is safe — the mask check only looks at low bits.
     */
    int accessCounter;

    /** Touch L2 every 4096 L1 hits. Must be power of 2 minus 1. */
    static final int SAMPLE_MASK = 4095;

    /**
     * Outer directory: {@code directory[outer]} is either null (no inner array)
     * or an {@code Object[PAGE_SIZE]}. Grown on demand under synchronized.
     */
    private Object[][] directory;

    public RadixBlockTable() {
        this.directory = new Object[DEFAULT_OUTER_SLOTS][];
    }

    public RadixBlockTable(int initialOuterSlots) {
        this.directory = new Object[Math.max(initialOuterSlots, 1)][];
    }

    /**
     * Looks up the value for the given blockId.
     * Lock-free, no fences, no synchronization.
     *
     * @return the cached value, or null if not present (L1 miss)
     */
    @SuppressWarnings("unchecked")
    public V get(long blockId) {
        int outer = (int) (blockId >>> PAGE_SHIFT);
        Object[][] dir = directory; // single read of the reference
        if (outer >= dir.length)
            return null;

        Object[] inner = dir[outer]; // plain array load
        if (inner == null)
            return null;

        int slot = (int) (blockId & PAGE_MASK);
        return (V) inner[slot]; // plain array load — JLS §17.7 atomic reference read
    }

    /**
     * Stores a value at the given blockId.
     * Allocates the inner array lazily if needed (synchronized for allocation only).
     * The slot write itself is a plain store.
     */
    public void put(long blockId, V value) {
        int outer = (int) (blockId >>> PAGE_SHIFT);
        int slot = (int) (blockId & PAGE_MASK);

        Object[][] dir = directory;
        if (outer < dir.length) {
            Object[] inner = dir[outer];
            if (inner != null) {
                // Fast path: inner array exists, plain store
                inner[slot] = value;
                return;
            }
        }

        // Slow path: need to allocate inner array or grow directory
        putSlow(outer, slot, value);
    }

    private synchronized void putSlow(int outer, int slot, V value) {
        if (outer >= directory.length) {
            growDirectory(outer);
        }
        Object[] inner = directory[outer];
        if (inner == null) {
            inner = new Object[PAGE_SIZE];
            directory[outer] = inner;
        }
        inner[slot] = value;
    }

    /**
     * Removes (nulls) the entry at the given blockId.
     * After nulling the slot, scans the inner array. If all slots are null,
     * the inner array is reclaimed (set to null) under synchronization.
     *
     * @return the previous value, or null if slot was empty
     */
    @SuppressWarnings("unchecked")
    public V remove(long blockId) {
        int outer = (int) (blockId >>> PAGE_SHIFT);
        Object[][] dir = directory;
        if (outer >= dir.length)
            return null;

        Object[] inner = dir[outer];
        if (inner == null)
            return null;

        int slot = (int) (blockId & PAGE_MASK);
        V prev = (V) inner[slot];
        inner[slot] = null; // plain store — JLS §17.7 atomic reference write

        // Check if inner array is now empty and reclaim if so
        if (prev != null) {
            reclaimIfEmpty(outer, inner);
        }
        return prev;
    }

    private synchronized void reclaimIfEmpty(int outer, Object[] inner) {
        if (directory[outer] != inner) {
            return;
        }
        for (int i = 0; i < PAGE_SIZE; i++) {
            if (inner[i] != null) {
                return;
            }
        }
        directory[outer] = null;
    }

    /**
     * Clears all entries.
     * Atomically swaps to a fresh empty directory of the same length.
     */
    public synchronized void clear() {
        directory = new Object[directory.length][];
    }

    /**
     * Estimates the memory overhead of this table in bytes.
     */
    public long memoryOverheadBytes() {
        Object[][] dir = directory;
        long overhead = 16;
        overhead += 16 + 4 + 4 + (long) dir.length * 8;
        for (Object[] inner : dir) {
            if (inner != null) {
                overhead += 16 + 4 + 4 + (long) PAGE_SIZE * 8;
            }
        }
        return overhead;
    }

    boolean isInnerAllocated(int outer) {
        Object[][] dir = directory;
        return outer < dir.length && dir[outer] != null;
    }

    int directoryLength() {
        return directory.length;
    }

    int countEntries(int outer) {
        Object[][] dir = directory;
        if (outer >= dir.length)
            return -1;
        Object[] inner = dir[outer];
        if (inner == null)
            return -1;
        int count = 0;
        for (Object v : inner) {
            if (v != null)
                count++;
        }
        return count;
    }

    int allocatedInnerCount() {
        Object[][] dir = directory;
        int count = 0;
        for (Object[] inner : dir) {
            if (inner != null)
                count++;
        }
        return count;
    }

    private void growDirectory(int requiredOuter) {
        int newSize = Math.max(requiredOuter + 1, directory.length * 2);
        Object[][] newDir = new Object[newSize][];
        System.arraycopy(directory, 0, newDir, 0, directory.length);
        directory = newDir;
    }
}
