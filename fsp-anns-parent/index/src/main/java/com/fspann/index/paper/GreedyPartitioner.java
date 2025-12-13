package com.fspann.index.paper;

import java.io.Serializable;
import java.util.*;

/**
 * GreedyPartitioner (MSANNP Algorithm-2)
 *
 * Produces:
 *   • Sorted index I = [ [C_low, C_up], tag ]
 *   • tag → list of IDs  (ID-ONLY, no ciphertexts)
 *
 * Guarantees:
 *   • Deterministic build (seeded)
 *   • Stable lexicographic ordering
 *   • All identical codes remain together
 *   • No subset exceeds w = max frequency of identical codes
 *
 * Fully compatible with:
 *   - PartitionedIndexService
 *   - QueryServiceImpl (D1 limiter, prefix refinement)
 *   - Forward-secure rebuild / restore
 */
public final class GreedyPartitioner {

    private GreedyPartitioner() {}

    // ============================================================
    // Item (ID + code only)
    // ============================================================
    public static final class Item {
        public final String id;
        public final BitSet code;

        public Item(String id, BitSet code) {
            this.id = Objects.requireNonNull(id, "id");
            this.code = (BitSet) Objects.requireNonNull(code, "code").clone();
        }
    }

    // ============================================================
    // SubsetBounds (immutable interval)
    // ============================================================
    public static final class SubsetBounds implements Serializable {
        public final BitSet lower;
        public final BitSet upper;
        public final String tag;
        public final int codeBits;

        public SubsetBounds(BitSet lower, BitSet upper, String tag, int codeBits) {
            this.lower = cloneImmutable(lower);
            this.upper = cloneImmutable(upper);
            this.tag = Objects.requireNonNull(tag, "tag");
            this.codeBits = codeBits;
        }

        private static BitSet cloneImmutable(BitSet b) {
            return (BitSet) b.clone(); // defensive immutability
        }

        @Override
        public String toString() {
            return "SubsetBounds{tag=" + tag +
                    ", lower=" + asBin(lower, codeBits) +
                    ", upper=" + asBin(upper, codeBits) +
                    "}";
        }
    }

    // ============================================================
    // Build result
    // ============================================================
    public static final class BuildResult {
        public final List<SubsetBounds> indexI;
        public final Map<String, List<String>> tagToIds;
        public final int w;

        public BuildResult(List<SubsetBounds> indexI,
                           Map<String, List<String>> tagToIds,
                           int w) {
            this.indexI = indexI;
            this.tagToIds = tagToIds;
            this.w = w;
        }
    }

    // ============================================================
    // Lexicographic BitSet comparator (MSB → LSB)
    // ============================================================
    public static final class CodeComparator implements Comparator<BitSet> {
        private final int codeBits;

        public CodeComparator(int codeBits) {
            this.codeBits = codeBits;
        }

        @Override
        public int compare(BitSet a, BitSet b) {
            for (int i = codeBits - 1; i >= 0; i--) {
                boolean ai = a.get(i);
                boolean bi = b.get(i);
                if (ai != bi) return ai ? 1 : -1;
            }
            return 0;
        }
    }

    // ============================================================
    // Algorithm-2 (Peng MSANNP)
    // ============================================================
    public static BuildResult build(List<Item> items, int codeBits, long tagSeed) {

        if (items == null || items.isEmpty()) {
            return new BuildResult(List.of(), Map.of(), 0);
        }

        CodeComparator cmp = new CodeComparator(codeBits);

        // Step 1: group identical codes (deterministic)
        Map<BitSet, List<Item>> grouped = new TreeMap<>(cmp);

        for (Item it : items) {
            BitSet key = (BitSet) it.code.clone();
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(it);
        }

        // Step 2: compute w = max group size
        int w = 0;
        for (List<Item> g : grouped.values()) {
            w = Math.max(w, g.size());
        }

        // Step 3: greedy sweep to form subsets ≤ w
        List<SubsetBounds> I = new ArrayList<>();
        Map<String, List<String>> tagMap = new LinkedHashMap<>();

        List<Map.Entry<BitSet, List<Item>>> G =
                new ArrayList<>(grouped.entrySet());

        // Deterministic PRNG for tags
        Random rnd = new Random(tagSeed);

        int i = 0;
        while (i < G.size()) {

            int taken = 0;
            BitSet lower = (BitSet) G.get(i).getKey().clone();
            List<String> ids = new ArrayList<>();

            while (i < G.size()) {
                List<Item> grp = G.get(i).getValue();

                // Oversized identical-code group → standalone interval
                if (grp.size() > w) {
                    if (taken == 0) {
                        BitSet key = G.get(i).getKey();
                        String tag = deterministicTag(rnd);

                        I.add(new SubsetBounds(key, key, tag, codeBits));

                        List<String> only = new ArrayList<>(grp.size());
                        for (Item it : grp) only.add(it.id);
                        tagMap.put(tag, only);

                        i++;
                    }
                    break;
                }

                if (taken + grp.size() > w && taken > 0) break;

                for (Item it : grp) ids.add(it.id);
                taken += grp.size();
                i++;
            }

            if (!ids.isEmpty()) {
                BitSet upper = (BitSet) G.get(i - 1).getKey().clone();
                String tag = deterministicTag(rnd);

                I.add(new SubsetBounds(lower, upper, tag, codeBits));
                tagMap.put(tag, ids);
            }
        }

        // Final sort by lower bound (defensive, deterministic)
        I.sort((a, b) -> cmp.compare(a.lower, b.lower));

        return new BuildResult(I, tagMap, w);
    }

    // ============================================================
    // Utilities
    // ============================================================
    private static String deterministicTag(Random rnd) {
        byte[] b = new byte[16];
        rnd.nextBytes(b);
        StringBuilder sb = new StringBuilder(32);
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }

    private static String asBin(BitSet bs, int bits) {
        StringBuilder sb = new StringBuilder(bits);
        for (int i = bits - 1; i >= 0; i--) {
            sb.append(bs.get(i) ? '1' : '0');
        }
        return sb.toString();
    }
}
