package com.fspann.index.paper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * PrefixPartitioner — Peng MSANNP Algorithm-2 (BIT PREFIX VERSION)
 *
 * Buckets vectors by BitSet prefixes.
 * Relaxation is implemented by shortening prefix length.
 */
public final class PrefixPartitioner {

    private static final Logger logger = LoggerFactory.getLogger(PrefixPartitioner.class);

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

        @Override
        public String toString() {
            return "PrefixKey{length=" + length + ", bits=" + toBitString(bits, Math.min(10, length)) + "}";
        }

        private static String toBitString(BitSet bs, int maxBits) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < maxBits; i++) {
                sb.append(bs.get(i) ? '1' : '0');
            }
            return sb.toString();
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

        logger.debug("Building partitions: fullBits={}, numVectors={}", fullBits, idToCode.size());

        // Create partitions in DECREASING order: p=fullBits → p=1
        // partition[0] will have p=fullBits (most specific)
        // partition[fullBits-1] will have p=1 (most relaxed)

        for (int p = fullBits; p >= 1; p--) {
            Map<PrefixKey, List<String>> map = new HashMap<>();

            int sampleCount = 0;
            for (Map.Entry<String, BitSet> e : idToCode.entrySet()) {
                PrefixKey key = new PrefixKey(e.getValue(), p);
                map.computeIfAbsent(key, k -> new ArrayList<>()).add(e.getKey());

                // LOG: First 5 keys for fullBits partition only
                if (p == fullBits && sampleCount < 5) {
                    logger.info("BUILD PARTITION KEY [p={}, listIndex={}]: id={}, key={}",
                            p, out.size(), e.getKey(), key);
                    sampleCount++;
                }
            }

            Partition partition = new Partition(p, map);
            out.add(partition);

            if (p == fullBits) {
                logger.info("BUILD PARTITION SUMMARY [p={}, listIndex={}]: {} unique keys, {} total vectors",
                        p, out.size() - 1, map.size(), idToCode.size());
            }
        }

        logger.debug("Built {} partitions (fullBits={} down to 1)", out.size(), fullBits);

        return out;
    }
}