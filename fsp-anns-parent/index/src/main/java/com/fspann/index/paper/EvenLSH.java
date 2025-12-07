package com.fspann.index.paper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Minimal EvenLSH implementation for compatibility.
 *
 * Notes:
 *  - This implementation is intentionally simple and deterministic.
 *  - It exists only to support QueryTokenFactory's need for:
 *      • getDimensions()
 *      • getBucketsForAllTables(...)
 *      • getBuckets(...)
 *  - In the new PAPER / partitioned indexing path, the server uses
 *    QueryToken.codes (BitSets) ONLY, and does NOT depend on these
 *    bucket indices. So this class does not need to be sophisticated.
 */
public class EvenLSH {

    private final int dimensions;
    private final int numTables;
    private final int maxBuckets;
    private final long baseSeed;

    /**
     * Basic constructor.
     *
     * @param dimensions dimensionality of vectors expected
     * @param numTables  number of LSH tables (used only for token-side hints)
     * @param maxBuckets upper bound on bucket id range (e.g., 1_000_000)
     * @param baseSeed   seed for deterministic hashing
     */
    public EvenLSH(int dimensions, int numTables, int maxBuckets, long baseSeed) {
        if (dimensions <= 0) throw new IllegalArgumentException("dimensions must be > 0");
        if (numTables <= 0)  throw new IllegalArgumentException("numTables must be > 0");
        if (maxBuckets <= 0) throw new IllegalArgumentException("maxBuckets must be > 0");

        this.dimensions = dimensions;
        this.numTables = numTables;
        this.maxBuckets = maxBuckets;
        this.baseSeed = baseSeed;
    }

    /** Convenience ctor: only supply dim + numTables + seed, with default maxBuckets. */
    public EvenLSH(int dimensions, int numTables, long baseSeed) {
        this(dimensions, numTables, 1_000_003, baseSeed);
    }

    /** Very simple ctor if you only know dim. */
    public EvenLSH(int dimensions) {
        this(dimensions, 1, 1_000_003, 13L);
    }

    /** Dimension of vectors this LSH expects. */
    public int getDimensions() {
        return dimensions;
    }

    /**
     * Return per-table bucket lists for a vector.
     * Each table gets a single primary bucket ID.
     *
     * @param vector      input vector
     * @param probeHint   ignored in this minimal implementation (kept for API compatibility)
     * @param numTables   number of tables requested (must be <= this.numTables for consistency)
     */
    public List<List<Integer>> getBucketsForAllTables(double[] vector, int probeHint, int numTables) {
        Objects.requireNonNull(vector, "vector");
        if (numTables <= 0) {
            return Collections.emptyList();
        }

        int tables = Math.min(numTables, this.numTables);
        List<List<Integer>> out = new ArrayList<>(tables);
        for (int t = 0; t < tables; t++) {
            int bucket = stableBucket(vector, t);
            List<Integer> one = new ArrayList<>(1);
            one.add(bucket);
            out.add(one);
        }
        return out;
    }

    /**
     * Return a small list of bucket IDs for a specific table.
     * For now we just return a single stable bucket.
     *
     * @param vector     input vector
     * @param topK       requested topK (not used to vary buckets in this minimal impl)
     * @param tableIndex table index
     */
    public List<Integer> getBuckets(double[] vector, int topK, int tableIndex) {
        Objects.requireNonNull(vector, "vector");
        if (tableIndex < 0 || tableIndex >= numTables) {
            throw new IllegalArgumentException("tableIndex out of range: " + tableIndex);
        }
        int bucket = stableBucket(vector, tableIndex);
        List<Integer> out = new ArrayList<>(1);
        out.add(bucket);
        return out;
    }

    // ---------------------------------------------------------------------
    // Internal: deterministic "hash" from vector + tableIndex → bucket ID
    // ---------------------------------------------------------------------

    private int stableBucket(double[] v, int tableIndex) {
        long seed = mix64(baseSeed ^ (31L * tableIndex) ^ (dimensions * 0x9E3779B97F4A7C15L));

        double acc = 0.0;
        long s = seed;

        // simple projection-like hash: combine vector components with pseudo-random coefficients
        for (double x : v) {
            s ^= (s << 21);
            s ^= (s >>> 35);
            s ^= (s << 4);
            double coef = ((s & 0x3fffffffL) / (double) 0x3fffffffL) * 2.0 - 1.0;
            acc += x * coef;
        }

        long bits = Double.doubleToLongBits(acc);
        long h = mix64(bits ^ seed);

        // fold to [0, maxBuckets)
        int bucket = (int) (Math.floorMod(h, maxBuckets));
        return bucket;
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdl;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53l;
        return z ^ (z >>> 33);
    }
}
