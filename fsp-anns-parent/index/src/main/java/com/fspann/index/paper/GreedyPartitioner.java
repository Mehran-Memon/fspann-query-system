package com.fspann.index.paper;

import java.io.Serializable;
import java.util.*;

public final class GreedyPartitioner {

    private GreedyPartitioner() {}

    // ============================================================
    // Partition (range-aware)
    // ============================================================
    public static final class Partition implements Serializable {
        public final long minKey;      // inclusive (build-time only)
        public final long maxKey;      // inclusive (build-time only)
        public final long centerKey;   // build-time only

        public final BitSet repCode;   // ← NEW: representative binary code
        public final List<String> ids;

        Partition(long minKey,
                  long maxKey,
                  long centerKey,
                  BitSet repCode,
                  List<String> ids) {
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.centerKey = centerKey;
            this.repCode = repCode;
            this.ids = ids;
        }
    }

    // ============================================================
    // Build greedy partitions (sorted by key)
    // ============================================================
    public static List<Partition> build(
            Map<String, BitSet> idToCode,
            int blockSize
    ) {
        if (idToCode == null || idToCode.isEmpty()) return List.of();
        if (blockSize <= 0) throw new IllegalArgumentException("blockSize must be > 0");

        // 1) Convert BitSet -> sortable long key
        List<Map.Entry<String, Long>> ordered = new ArrayList<>(idToCode.size());
        for (var e : idToCode.entrySet()) {
            ordered.add(Map.entry(e.getKey(), computeKey(e.getValue())));
        }

        // 2) Sort by numeric key (ONLY for grouping)
        ordered.sort(Comparator.comparingLong(Map.Entry::getValue));

        // 3) Partition into blocks
        List<Partition> out = new ArrayList<>();
        for (int i = 0; i < ordered.size(); i += blockSize) {
            int end = Math.min(i + blockSize, ordered.size());

            long minK = ordered.get(i).getValue();
            long maxK = ordered.get(end - 1).getValue();
            int mid = i + ((end - i - 1) >>> 1);
            long centerK = ordered.get(mid).getValue();

            List<String> ids = new ArrayList<>(end - i);
            for (int j = i; j < end; j++) {
                ids.add(ordered.get(j).getKey());
            }

            // ← REPRESENTATIVE CODE (CRITICAL)
            String repId = ordered.get(mid).getKey();
            BitSet repCode = (BitSet) idToCode.get(repId).clone();

            out.add(new Partition(minK, maxK, centerK, repCode, ids));
        }

        return out;
    }



    // ============================================================
    // BitSet -> sortable long (MSB-first safe)
    // ============================================================
    /**
     * Maps a BitSet to a sortable long key.
     * Uses MSB-first mapping to ensure prefixes dominate the sort order.
     */
    public static long computeKey(BitSet bs) {
        long v = 0L;
        // We only use up to 63 bits to avoid the sign-bit in signed Longs
        // This provides 2^63 possible partitions, which is more than enough.
        int limit = Math.min(63, bs.length());
        for (int i = 0; i < limit; i++) {
            if (bs.get(i)) {
                // Bit 0 (MSB) moves to position 62
                v |= (1L << (62 - i));
            }
        }
        return v;
    }

    /**
     * Efficient Hamming distance using BitSet cardinality.
     * Note: BitSet doesn't provide a non-destructive XOR, so we must clone.
     */
    public static long hamming(BitSet a, BitSet b) {
        if (a == null || b == null) return Long.MAX_VALUE;
        BitSet copy = (BitSet) a.clone();
        copy.xor(b);
        return copy.cardinality();
    }

    // ============================================================
    // Find nearest partition by key (binary search by range)
    // ============================================================
    /**
     * Finds the index of the partition whose range (min-max) or center
     * is closest to the query key.
     */
    public static int findNearestPartition(List<Partition> parts, long qKey) {
        if (parts == null || parts.isEmpty()) return 0;

        int lo = 0, hi = parts.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            Partition p = parts.get(mid);

            if (qKey < p.minKey) hi = mid - 1;
            else if (qKey > p.maxKey) lo = mid + 1;
            else return mid; // Target key is inside this partition's range
        }

        // Binary search missed: target is between or outside ranges.
        // Clamp and return the closest boundary partition.
        if (lo <= 0) return 0;
        if (lo >= parts.size()) return parts.size() - 1;

        // Check if the previous or current 'lo' is closer
        long distLeft = Math.abs(parts.get(lo - 1).maxKey - qKey);
        long distRight = Math.abs(parts.get(lo).minKey - qKey);

        return (distLeft <= distRight) ? (lo - 1) : lo;
    }

    public static long distanceToRange(long q, long minK, long maxK) {
        if (q < minK) return minK - q;
        if (q > maxK) return q - maxK;
        return 0L;
    }
}
