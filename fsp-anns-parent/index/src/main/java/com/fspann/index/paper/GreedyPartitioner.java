package com.fspann.index.paper;

import java.io.Serializable;
import java.util.*;

/**
 * GreedyPartitioner — MSANNP Algorithm-2 (Integer Interval Version)
 *
 * Builds disjoint integer bucket intervals:
 *
 *     I = { [h_low, h_high] → tag }
 *
 * Guarantees:
 *  • Deterministic (seeded)
 *  • Stable ordering
 *  • All identical hashes stay together
 *  • No interval exceeds w = max identical-hash frequency
 *
 * This version is STRICTLY integer-based.
 * BitSets are NOT supported.
 */
public final class GreedyPartitioner {

    private GreedyPartitioner() {}

    // ============================================================
    // Item (ID + integer hash)
    // ============================================================
    public static final class Item {
        public final String id;
        public final int hash;

        public Item(String id, int hash) {
            this.id = Objects.requireNonNull(id, "id");
            this.hash = hash;
        }
    }

    // ============================================================
    // SubsetBounds (immutable integer interval)
    // ============================================================
    public static final class SubsetBounds implements Serializable {
        public final int lower;
        public final int upper;
        public final String tag;

        public SubsetBounds(int lower, int upper, String tag) {
            if (lower > upper) {
                throw new IllegalArgumentException("lower > upper");
            }
            this.lower = lower;
            this.upper = upper;
            this.tag = Objects.requireNonNull(tag, "tag");
        }

        @Override
        public String toString() {
            return "SubsetBounds{[" + lower + "," + upper + "] → " + tag + "}";
        }
    }

    // ============================================================
    // Build result
    // ============================================================
    public static final class BuildResult {
        public final List<SubsetBounds> indexI;
        public final Map<String, List<String>> tagToIds;
        public final int w;

        public BuildResult(
                List<SubsetBounds> indexI,
                Map<String, List<String>> tagToIds,
                int w
        ) {
            this.indexI = indexI;
            this.tagToIds = tagToIds;
            this.w = w;
        }
    }

    // ============================================================
    // Algorithm-2 (Peng et al., MSANNP)
    // ============================================================
    public static BuildResult build(List<Item> items, long tagSeed) {

        if (items == null || items.isEmpty()) {
            return new BuildResult(List.of(), Map.of(), 0);
        }

        // Step 1: group by identical integer hashes
        Map<Integer, List<Item>> grouped = new TreeMap<>();
        for (Item it : items) {
            grouped.computeIfAbsent(it.hash, k -> new ArrayList<>()).add(it);
        }

        // Step 2: compute w = max identical-hash frequency
        int w = 0;
        for (List<Item> g : grouped.values()) {
            w = Math.max(w, g.size());
        }

        // Step 3: greedy sweep to form ≤ w intervals
        List<SubsetBounds> indexI = new ArrayList<>();
        Map<String, List<String>> tagMap = new LinkedHashMap<>();

        List<Map.Entry<Integer, List<Item>>> G =
                new ArrayList<>(grouped.entrySet());

        Random rnd = new Random(tagSeed);

        int i = 0;
        while (i < G.size()) {

            int taken = 0;
            int lower = G.get(i).getKey();
            List<String> ids = new ArrayList<>();

            while (i < G.size()) {
                List<Item> grp = G.get(i).getValue();

                if (grp.size() > w) {
                    // Standalone interval
                    if (taken == 0) {
                        int h = G.get(i).getKey();
                        String tag = deterministicTag(rnd);

                        indexI.add(new SubsetBounds(h, h, tag));
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
                int upper = G.get(i - 1).getKey();
                String tag = deterministicTag(rnd);

                indexI.add(new SubsetBounds(lower, upper, tag));
                tagMap.put(tag, ids);
            }
        }

        return new BuildResult(indexI, tagMap, w);
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
}
