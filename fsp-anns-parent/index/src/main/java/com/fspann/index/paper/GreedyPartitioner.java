package com.fspann.index.paper;

import com.fspann.common.EncryptedPoint;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.*;

/**
 * GreedyPartitioner (Algorithm-2)
 * --------------------------------
 * Sort items by code C, compute width w = max multiplicity of identical codes,
 * and sweep to form subsets SD_i with bounds ⟨C_low, C_up⟩ and unique tags.
 *
 * P1–P3 constraints:
 *  - Never split equal-code runs across two subsets.
 *  - Bounds independent of any particular item.
 *  - Each code belongs to exactly one subset (single mapping).
 */
public final class GreedyPartitioner {

    private GreedyPartitioner() {}

    /** In-memory item used during building. */
    public static final class Item {
        public final String id;
        public final EncryptedPoint point;
        public final BitSet code;
        public Item(String id, EncryptedPoint point, BitSet code) {
            this.id = Objects.requireNonNull(id);
            this.point = Objects.requireNonNull(point);
            this.code = (BitSet) Objects.requireNonNull(code).clone();
        }
    }

    /** Subset bounds + tag entry for the map index I. */
    public static final class SubsetBounds implements Serializable {
        public final BitSet lower;   // inclusive
        public final BitSet upper;   // inclusive
        public final String tag;     // random tag (opaque)
        public final int codeBits;   // for lexicographic comparisons
        public SubsetBounds(BitSet lower, BitSet upper, String tag, int codeBits) {
            this.lower = (BitSet)lower.clone();
            this.upper = (BitSet)upper.clone();
            this.tag = Objects.requireNonNull(tag);
            this.codeBits = codeBits;
        }
        @Override public String toString() {
            return "SubsetBounds{tag="+tag+",lower="+asBin(lower, codeBits)+",upper="+asBin(upper, codeBits)+"}";
        }
    }

    /** Output of a build pass. */
    public static final class BuildResult {
        public final List<SubsetBounds> indexI;         // sorted by lower
        public final Map<String, List<EncryptedPoint>> tagToSubset; // tag -> points
        public final int w;
        public BuildResult(List<SubsetBounds> indexI, Map<String, List<EncryptedPoint>> tagToSubset, int w) {
            this.indexI = indexI;
            this.tagToSubset = tagToSubset;
            this.w = w;
        }
    }

    // Comparator: lexicographic BitSet comparison using fixed codeBits
    public static final class CodeComparator implements Comparator<BitSet> {
        private final int codeBits;
        public CodeComparator(int codeBits) { this.codeBits = codeBits; }
        @Override public int compare(BitSet a, BitSet b) {
            for (int i = codeBits - 1; i >= 0; i--) { // msb-first
                boolean ai = a.get(i);
                boolean bi = b.get(i);
                if (ai != bi) return ai ? 1 : -1;
            }
            return 0;
        }
    }

    /** Build partitions from items already coded to BitSet C(v). */
    public static BuildResult build(List<Item> items, int codeBits, long tagSeed) {
        if (items == null || items.isEmpty()) {
            return new BuildResult(Collections.emptyList(), Collections.emptyMap(), 0);
        }
        // Group by exact code to compute w and to prevent splitting
        Map<BitSet, List<Item>> byCode = new TreeMap<>(new CodeComparator(codeBits));
        for (Item it : items) {
            BitSet key = (BitSet) it.code.clone();
            byCode.computeIfAbsent(key, k -> new ArrayList<>()).add(it);
        }
        int w = 0;
        for (List<Item> group : byCode.values()) w = Math.max(w, group.size());

        // Sweep groups in lex order, forming chunks of <= w, respecting whole-code groups.
        List<SubsetBounds> indexI = new ArrayList<>();
        Map<String, List<EncryptedPoint>> tagToSubset = new LinkedHashMap<>();

        SecureRandom tagRnd = new SecureRandom(longToBytes(tagSeed));
        CodeComparator cmp = new CodeComparator(codeBits);

        List<Map.Entry<BitSet, List<Item>>> groups = new ArrayList<>(byCode.entrySet());
        int i = 0;
        while (i < groups.size()) {
            int taken = 0;
            BitSet lower = (BitSet) groups.get(i).getKey().clone();
            List<EncryptedPoint> subset = new ArrayList<>();

            int start = i;
            while (i < groups.size()) {
                List<Item> g = groups.get(i).getValue();
                if (taken + g.size() > w && taken > 0) break; // next subset
                subsetAdd(subset, g);
                taken += g.size();
                i++;
            }
            BitSet upper = (BitSet) groups.get(i - 1).getKey().clone();
            String tag = randomTag(tagRnd);

            indexI.add(new SubsetBounds(lower, upper, tag, codeBits));
            tagToSubset.put(tag, subset);

            // Safety: ensure that for any code in [lower,upper] we entirely included that run.
            // (By construction, we only add whole groups, never partial.)
        }

        // Ensure indexI is sorted by lower
        indexI.sort((a, b) -> cmp.compare(a.lower, b.lower));
        return new BuildResult(indexI, tagToSubset, w);
    }

    private static void subsetAdd(List<EncryptedPoint> subset, List<Item> group) {
        for (Item it : group) subset.add(it.point);
    }

    private static String randomTag(SecureRandom rnd) {
        byte[] b = new byte[16];
        rnd.nextBytes(b);
        StringBuilder sb = new StringBuilder(32);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private static String asBin(BitSet bs, int bits) {
        StringBuilder sb = new StringBuilder(bits);
        for (int i = bits - 1; i >= 0; i--) sb.append(bs.get(i) ? '1' : '0');
        return sb.toString();
    }

    private static byte[] longToBytes(long x) {
        return new byte[] {
                (byte)(x >>> 56), (byte)(x >>> 48), (byte)(x >>> 40), (byte)(x >>> 32),
                (byte)(x >>> 24), (byte)(x >>> 16), (byte)(x >>>  8), (byte)(x)
        };
    }
}
