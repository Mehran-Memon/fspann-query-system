package com.fspann.index.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Single source of truth for bucket assignment (insert) and probing expansions (query).
 * - Per-table primary bucket for inserts
 * - Per-table contiguous expansions for queries (multi-probe cloak)
 * - Helper presets for topK sweeps (1..100 step 20) used by eval
 */
public final class PartitioningPolicy {

    // Default eval sweep as requested
    public static final int[] EVAL_TOPKS =
            parseEvalSweep(System.getProperty("eval.sweep", "1,20,40,60,80,100"));

    // Safety cap, so expansion can't exceed a fraction of buckets (defers to EvenLSH's internal cap too)
    public static final double MAX_RANGE_FRACTION = 0.25;

    private PartitioningPolicy() {}

    /**
     * Compute per-table primary buckets for an insert.
     * @param lsh        the EvenLSH for this dimension
     * @param vector     raw vector (plaintext)
     * @param numTables  number of hash tables
     */
    public static List<Integer> bucketsForInsert(EvenLSH lsh, double[] vector, int numTables) {
        Objects.requireNonNull(lsh, "lsh");
        Objects.requireNonNull(vector, "vector");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");

        List<Integer> out = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            out.add(lsh.getBucketId(vector, t));
        }
        return out;
    }

    /**
     * Per-table contiguous expansions for a query, using the same logic that insert uses for primary buckets.
     * This is the cloak width controller via topK -> range mapping (log-scaled).
     *
     * @param lsh        the EvenLSH for this dimension
     * @param query      query vector
     * @param numTables  number of hash tables to probe
     * @param topK       topK knob (drives contiguous range per table)
     */
    public static List<List<Integer>> expansionsForQuery(EvenLSH lsh, double[] query, int numTables, int topK) {
        Objects.requireNonNull(lsh, "lsh");
        Objects.requireNonNull(query, "query");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        if (topK <= 0) throw new IllegalArgumentException("topK must be > 0");

        List<List<Integer>> perTable = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            perTable.add(lsh.getBuckets(query, topK, t));
        }
        return perTable;
    }

    /**
     * Vectorized variant: different topK per table (rare, but useful for budget tuning).
     * @param perTableTopK must have length == numTables
     */
    public static List<List<Integer>> expansionsForQuery(EvenLSH lsh, double[] query, int numTables, int[] perTableTopK) {
        Objects.requireNonNull(lsh, "lsh");
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(perTableTopK, "perTableTopK");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        if (perTableTopK.length != numTables) {
            throw new IllegalArgumentException("perTableTopK length must equal numTables");
        }

        List<List<Integer>> perTable = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            int k = Math.max(1, perTableTopK[t]);
            perTable.add(lsh.getBuckets(query, k, t));
        }
        return perTable;
    }

    /**
     * Convenience: preset sweep for evaluation {1,20,40,60,80,100}.
     */
    public static List<List<List<Integer>>> expansionsForEvalSweep(EvenLSH lsh, double[] query, int numTables) {
        List<List<List<Integer>>> all = new ArrayList<>(EVAL_TOPKS.length);
        for (int k : EVAL_TOPKS) {
            all.add(expansionsForQuery(lsh, query, numTables, k));
        }
        return all;
    }

    private static int[] parseEvalSweep(String spec) {
        // Accept "1-100" ranges, comma lists, or mixes like "1,5,10-50"
        java.util.LinkedHashSet<Integer> out = new java.util.LinkedHashSet<>();
        for (String tok : spec.replaceAll("\\s+", "").split(",")) {
            if (tok.isEmpty()) continue;
            int dash = tok.indexOf('-');
            if (dash > 0) {
                int a = Integer.parseInt(tok.substring(0, dash));
                int b = Integer.parseInt(tok.substring(dash + 1));
                if (a > b) { int t = a; a = b; b = t; }
                for (int k = a; k <= b; k++) out.add(k);
            } else {
                out.add(Integer.parseInt(tok));
            }
        }
        if (out.isEmpty()) out.add(100);
        return out.stream().mapToInt(Integer::intValue).toArray();
    }

    public static String debugString(List<List<Integer>> perTable) {
        StringBuilder sb = new StringBuilder("[");
        for (int t = 0; t < perTable.size(); t++) {
            sb.append("T").append(t).append("=").append(perTable.get(t));
            if (t + 1 < perTable.size()) sb.append(", ");
        }
        return sb.append("]").toString();
    }
}
