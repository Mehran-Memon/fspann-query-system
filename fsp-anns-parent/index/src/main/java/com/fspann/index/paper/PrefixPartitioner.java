package com.fspann.index.paper;

import java.io.Serializable;
import java.util.*;

/**
 * PrefixPartitioner — Peng MSANNP Algorithm-2 (BIT PREFIX VERSION)
 *
 * Buckets vectors by BitSet prefixes.
 * Relaxation is implemented by shortening prefix length.
 */
public final class PrefixPartitioner {

    private PrefixPartitioner() {}

    // ============================================================
    // PrefixKey (BitSet + length)
    // ============================================================
    public static final class PrefixKey implements Serializable {
        private final BitSet bits;
        private final int length;

        public PrefixKey(BitSet full, int length) {
            this.length = length;
            this.bits = full.get(0, length); // copy prefix
        }

        @Override
        public int hashCode() {
            return Objects.hash(bits, length);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PrefixKey k)) return false;
            return length == k.length && bits.equals(k.bits);
        }
    }

    // ============================================================
    // Partition
    // ============================================================
    public static final class Partition {
        public final int prefixLen;
        public final Map<PrefixKey, List<String>> buckets;

        public Partition(int prefixLen, Map<PrefixKey, List<String>> buckets) {
            this.prefixLen = prefixLen;
            this.buckets = buckets;
        }
    }

    // ============================================================
    // Build partitions (one division)
    // ============================================================
    public static List<Partition> build(
            Map<String, BitSet> idToCode,
            int fullBits
    ) {
        List<Partition> out = new ArrayList<>();

        // Relax from fullBits → 1
        for (int p = fullBits; p >= 1; p--) {
            Map<PrefixKey, List<String>> map = new HashMap<>();

            for (Map.Entry<String, BitSet> e : idToCode.entrySet()) {
                PrefixKey key = new PrefixKey(e.getValue(), p);
                map.computeIfAbsent(key, k -> new ArrayList<>()).add(e.getKey());
            }

            out.add(new Partition(p, map));
        }

        return out;
    }
}
