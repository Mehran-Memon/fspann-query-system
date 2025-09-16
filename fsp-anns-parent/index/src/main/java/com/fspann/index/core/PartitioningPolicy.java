package com.fspann.index.core;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * LEGACY: PartitioningPolicy (multi-probe fanout over EvenLSH)
 * ------------------------------------------------------------------
 * This utility maps a plaintext vector to:
 *   - per-table primary buckets (insert path),
 *   - per-table multi-probe expansions (query path).
 *
 * It belongs to the older "multiprobe" mode and is retained ONLY for
 * backward compatibility and A/B experiments:
 *
 *   -Dfspann.mode=multiprobe    -> use EvenLSH + PartitioningPolicy
 *   -Dfspann.mode=partitioned   -> use Coding (Alg-1) + GreedyPartition (Alg-2) + TagQuery (Alg-3)
 *
 * The paper-aligned default path should NOT route through this class.
 * In SANNP/mSANNP, queries are resolved via tag-indexed subsets, not
 * Hamming-ball bucket unions.
 *
 * Thread-safety: stateless; all methods are pure functions.
 */
@Deprecated
public final class PartitioningPolicy {

    /**
     * Default evaluation sweep for topK (kept for historical comparability).
     * Accepts override with: -Deval.sweep="1,5,10-50"
     */
    public static final int[] EVAL_TOPKS =
            parseEvalSweep(System.getProperty("eval.sweep", "1,20,40,60,80,100"));

    /**
     * @deprecated Unused in current implementation. Historically used to bound
     * contiguous range as a fraction of numBuckets. Retained for source compatibility.
     */
    @Deprecated
    public static final double MAX_RANGE_FRACTION = 0.25;

    private PartitioningPolicy() { /* no instances */ }

    // -------------------------------------------------------------------------
    // INSERT PATH (legacy)
    // -------------------------------------------------------------------------

    /**
     * Compute per-table primary buckets for an insert.
     * @param lsh        EvenLSH router (legacy multiprobe mode)
     * @param vector     plaintext vector (dimensionality must match lsh)
     * @param numTables  number of hash tables
     * @return list of primary bucket ids, one per table
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

    // -------------------------------------------------------------------------
    // QUERY PATH (legacy multiprobe)
    // -------------------------------------------------------------------------

    /**
     * Per-table multi-probe expansions for a query. This is the "fanout" driver
     * in the legacy path. For paper-aligned SANNP, use TagQuery over the map index.
     *
     * @param lsh        EvenLSH router
     * @param query      plaintext query vector
     * @param numTables  number of tables to probe
     * @param topK       intent knob (not directly used; EvenLSH reads its own limits)
     * @return list of per-table bucket id lists
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
     * Vectorized variant: custom topK per table (rare; useful for budget tuning in experiments).
     * @param lsh            EvenLSH router
     * @param query          plaintext query vector
     * @param numTables      number of tables
     * @param perTableTopK   array of topK values, one per table (must match numTables)
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
     * Convenience for evaluation sweeps: expands for each K in {@link #EVAL_TOPKS}.
     * Kept to make legacy ratios/ART plots reproducible during A/B runs.
     */
    public static List<List<List<Integer>>> expansionsForEvalSweep(EvenLSH lsh, double[] query, int numTables) {
        Objects.requireNonNull(lsh, "lsh");
        Objects.requireNonNull(query, "query");
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");

        List<List<List<Integer>>> all = new ArrayList<>(EVAL_TOPKS.length);
        for (int k : EVAL_TOPKS) {
            all.add(expansionsForQuery(lsh, query, numTables, k));
        }
        return all;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Parse sweep spec like "1,5,10-50" into a unique, ascending int[] */
    private static int[] parseEvalSweep(String spec) {
        LinkedHashSet<Integer> out = new LinkedHashSet<>();
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
        // toArray preserves insertion order in LinkedHashSet
        return out.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Human-friendly dump of per-table expansions for debugging. */
    public static String debugString(List<List<Integer>> perTable) {
        StringBuilder sb = new StringBuilder("[");
        for (int t = 0; t < perTable.size(); t++) {
            sb.append("T").append(t).append("=").append(perTable.get(t));
            if (t + 1 < perTable.size()) sb.append(", ");
        }
        return sb.append("]").toString();
    }
}
