package com.fspann.index.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * EvenLSH: random Gaussian projections -> stable per-table bucketization.
 * - Per-table APIs only (no legacy single-table shortcuts).
 * - Contiguous neighbor expansion around main bucket for fanout (used by top-K probing).
 * - Projection vectors are L2-normalized; loop-unrolled dot products for speed.
 * - DETERMINISTIC: projections seeded so bucketization is stable across restarts.
 */
public final class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);

    private final int dimensions;
    private final int numBuckets;
    private final int numProjections;        // upper-bounded to 8*numBuckets
    private final double[][] projectionVectors;
    private final long seed;

    private static final int UNROLL_THRESHOLD = 256; // Loop unrolling threshold

    public EvenLSH(int dimensions, int numBuckets) {
        this(dimensions, numBuckets, calculateNumProjections(dimensions, numBuckets));
    }

    public EvenLSH(int dimensions, int numBuckets, int numProjections) {
        this(dimensions, numBuckets, numProjections,
                defaultSeed(dimensions, numBuckets, numProjections));
    }

    public EvenLSH(int dimensions, int numBuckets, int numProjections, long seed) {
        if (dimensions <= 0 || numBuckets <= 0 || numProjections <= 0) {
            throw new IllegalArgumentException("dimensions, numBuckets, numProjections must be > 0");
        }
        this.dimensions = dimensions;
        this.numBuckets = numBuckets;
        this.numProjections = Math.max(numBuckets, Math.min(numProjections, numBuckets * 8));
        this.seed = seed;
        this.projectionVectors = new double[this.numProjections][dimensions];

        // Build normalized random projections (deterministic via seed)
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = 0; i < this.numProjections; i++) {
            double norm = 0.0;
            for (int j = 0; j < dimensions; j++) {
                double v = nextGaussian(rnd);
                projectionVectors[i][j] = v;
                norm += v * v;
            }
            norm = Math.sqrt(norm);
            if (norm == 0.0) norm = 1.0;
            for (int j = 0; j < dimensions; j++) {
                projectionVectors[i][j] /= norm;
            }
        }
        logger.info("Initialized EvenLSH dims={}, buckets={}, projections={}, seed={}",
                dimensions, numBuckets, this.numProjections, seed);
    }

    /** Adaptive projection count ~ buckets * log(dim/16). */
    private static int calculateNumProjections(int dimensions, int numBuckets) {
        int proj = (int) Math.ceil(numBuckets * Math.log(Math.max(dimensions, 1) / 16.0) / Math.log(2));
        return Math.max(numBuckets, Math.min(proj, numBuckets * 8));
    }

    /** Default seed that stays stable across restarts for same (dims, buckets, projections). */
    private static long defaultSeed(int dims, int buckets, int projs) {
        long x = 0x9E3779B97F4A7C15L;
        x ^= (long) dims * 0xBF58476D1CE4E5B9L;
        x ^= (long) buckets * 0x94D049BB133111EBL;
        x ^= (long) projs + 0x2545F4914F6CDD1DL;
        x ^= (x >>> 33); x *= 0xff51afd7ed558ccdl;
        x ^= (x >>> 33); x *= 0xc4ceb9fe1a85ec53l;
        x ^= (x >>> 33);
        return x;
    }

    /** Box–Muller from uniform SplittableRandom (deterministic). */
    private static double nextGaussian(SplittableRandom r) {
        double u1 = Math.max(Double.MIN_VALUE, r.nextDouble());
        double u2 = r.nextDouble();
        double mag = Math.sqrt(-2.0 * Math.log(u1));
        return mag * Math.cos(2.0 * Math.PI * u2);
    }

    /** Projection using tableIndex-th projection (wrapped by numProjections). */
    public double project(double[] vector, int tableIndex) {
        validateVector(vector);
        int idx = Math.floorMod(tableIndex, numProjections);
        return dot(vector, projectionVectors[idx]);
    }

    /** Main bucket for a table (stable; not salted by epoch). */
    public int getBucketId(double[] vector, int tableIndex) {
        double p = project(vector, tableIndex);
        long bits = Double.doubleToLongBits(p);
        // light avalanche; independent per table
        bits ^= (bits << 21);
        bits ^= (bits >>> 35);
        bits ^= (bits << 4);
        long mix = 0x9E3779B97F4A7C15L * (tableIndex + 1L);
        int hashed = (int) ((bits ^ mix) & 0x7FFFFFFF);
        return hashed % numBuckets;
    }

    /**
     * Contiguous neighbor expansion for probing.
     * Example: range=2 -> [b-2, b-1, b, b+1, b+2] modulo numBuckets.
     */
    public List<Integer> getBuckets(double[] vector, int topK, int tableIndex) {
        validateVector(vector);
        int main = getBucketId(vector, tableIndex);
        int range = calculateRange(topK);
        return expandContiguous(main, range);
    }

    /** Main buckets for all tables [0..numTables-1]. */
    public int[] getBucketIdsForAllTables(double[] vector, int numTables) {
        validateVector(vector);
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        int[] out = new int[numTables];
        for (int t = 0; t < numTables; t++) out[t] = getBucketId(vector, t);
        return out;
    }

    /** Per-table contiguous expansions for all tables. */
    public List<List<Integer>> getBucketsForAllTables(double[] vector, int topK, int numTables) {
        validateVector(vector);
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        int range = calculateRange(topK);
        List<List<Integer>> all = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            int main = getBucketId(vector, t);
            all.add(expandContiguous(main, range));
        }
        return all;
    }

    public int getDimensions() { return dimensions; }
    public int getNumBuckets() { return numBuckets; }
    public long getSeed() { return seed; }

    // ---- internal helpers ----

    private int calculateRange(int topK) {
        // log-based growth; cap at 25% of buckets to keep fanout bounded
        int r = Math.max(1, (int) Math.ceil(Math.log(topK + 1) / Math.log(2)));
        return Math.min(r, Math.max(1, numBuckets / 4));
    }

    private List<Integer> expandContiguous(int main, int range) {
        List<Integer> out = new ArrayList<>(range * 2 + 1);
        for (int d = -range; d <= range; d++) {
            out.add(Math.floorMod(main + d, numBuckets));
        }
        return out;
    }

    private double dot(double[] a, double[] b) {
        int n = dimensions;
        if (n <= UNROLL_THRESHOLD) {
            double s = 0.0;
            int i = 0;
            for (; i <= n - 4; i += 4) {
                s += a[i] * b[i]
                        + a[i + 1] * b[i + 1]
                        + a[i + 2] * b[i + 2]
                        + a[i + 3] * b[i + 3];
            }
            for (; i < n; i++) s += a[i] * b[i];
            return s;
        } else {
            double s = 0.0;
            final int blk = 8;
            int i = 0;
            for (; i <= n - blk; i += blk) {
                for (int j = 0; j < blk; j++) s += a[i + j] * b[i + j];
            }
            for (; i < n; i++) s += a[i] * b[i];
            return s;
        }
    }

    private void validateVector(double[] v) {
        if (v == null || v.length != dimensions)
            throw new IllegalArgumentException("Vector dims mismatch: expected=" + dimensions + " got=" + (v == null ? 0 : v.length));
        for (double x : v) if (Double.isNaN(x) || Double.isInfinite(x))
            throw new IllegalArgumentException("Vector has NaN/Inf");
    }
}
