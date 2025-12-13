package com.fspann.index.paper;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Objects;
import java.util.SplittableRandom;

/**
 * Coding (Algorithm-1)
 * --------------------
 * MSANNP uses projection-based "LSH" ONLY to produce a sortable binary code.
 *
 *   h_j(v) = floor( (α_j · v + r_j) / ω_j )
 *
 * For each projection j (0..m-1) we extract λ least-significant bits.
 * Interleave them in (i, j) order to produce C(v) with (m * λ) bits:
 *
 *   bit position p = i * m + j
 *
 * where i ∈ [0..λ-1] is bit index in h_j, and j ∈ [0..m-1] is projection index.
 *
 * There is **NO** multi-table hashing, probing, or bucketization.
 * Each MSANNP "division" is its own GFunction instance.
 */
public final class Coding {

    private Coding() {}

    // Default parameters
    private static final int DEFAULT_LAMBDA = 8;
    private static final double DEFAULT_OMEGA = 1.0;

    // ============================================================
    // Code family interface
    // ============================================================
    public interface CodeFamily {
        int codeBits();
    }

    // ============================================================
    // Fully-materialized GFunction
    // ============================================================
    public static final class GFunction implements Serializable, CodeFamily {
        /** alpha[j][d] Gaussian projections */
        public final double[][] alpha;
        /** per-projection offset r_j ∈ [0, ω_j) */
        public final double[] r;
        /** per-projection ω_j > 0 */
        public final double[] omega;

        /** projections count */
        public final int m;
        /** bits per h_j */
        public final int lambda;
        /** vector dimensionality */
        public final int d;

        /** seed used to generate α, r, ω */
        public final long seed;

        public GFunction(double[][] alpha, double[] r, double[] omega,
                         int lambda, long seed) {

            this.alpha = Objects.requireNonNull(alpha, "alpha");
            this.r     = Objects.requireNonNull(r, "r");
            this.omega = Objects.requireNonNull(omega, "omega");

            this.m = alpha.length;
            if (m == 0) throw new IllegalArgumentException("alpha empty");

            this.d = alpha[0].length;
            if (r.length != m || omega.length != m)
                throw new IllegalArgumentException("r/omega size mismatch");

            for (double w : omega) {
                if (!(w > 0.0)) throw new IllegalArgumentException("omega_j ≤ 0");
            }
            if (lambda <= 0) throw new IllegalArgumentException("lambda ≤ 0");

            this.lambda = lambda;
            this.seed   = seed;
        }

        @Override
        public int codeBits() {
            return m * lambda;
        }
    }

    // ============================================================
    // Minimal, seed-only descriptor used for metadata
    // ============================================================
    public static final class GMeta implements Serializable, CodeFamily {
        private final int m;
        private final int lambda;
        private final long seed;

        public GMeta(int m, int lambda, long seed) {
            if (m <= 0 || lambda <= 0)
                throw new IllegalArgumentException("m, lambda must be > 0");
            this.m = m;
            this.lambda = lambda;
            this.seed = seed;
        }

        public int m()      { return m; }
        public int lambda() { return lambda; }
        public long seed()  { return seed; }

        @Override
        public int codeBits() { return m * lambda; }

        @Override
        public String toString() {
            return "GMeta{m=" + m + ", lambda=" + lambda + ", seed=" + seed + "}";
        }
    }

    // ============================================================
    // GFunction Builders (Algorithm-1 initialization)
    // ============================================================

    /**
     * Build a GFunction with random Gaussian α, a uniform ω for all projections,
     * and random r ∈ [0, ω). Used when dataset statistics unavailable.
     */
    public static GFunction buildRandomG(int d, int m, int lambda, double omega, long seed) {
        if (omega <= 0) throw new IllegalArgumentException("omega ≤ 0");

        SplittableRandom rnd = new SplittableRandom(seed);

        double[][] alpha = new double[m][d];
        for (int j = 0; j < m; j++) {
            double norm = 0.0;
            for (int i = 0; i < d; i++) {
                double v = nextGaussian(rnd);
                alpha[j][i] = v;
                norm += v * v;
            }
            norm = Math.sqrt(Math.max(1e-12, norm));
            for (int i = 0; i < d; i++) alpha[j][i] /= norm;
        }

        double[] r = new double[m];
        double[] w = new double[m];
        for (int j = 0; j < m; j++) {
            r[j] = rnd.nextDouble() * omega;
            w[j] = omega;
        }

        return new GFunction(alpha, r, w, lambda, seed);
    }

    public static GMeta fromSeedOnly(int m, int lambda, long seed) {
        return new GMeta(m, lambda, seed);
    }

    /**
     * Build GFunction from sample (Algorithm-1, data-aware ω).
     * ω_j ≈ (max(y_j) − min(y_j)) * 2^{-λ}
     */
    public static GFunction buildFromSample(double[][] sample, int m, int lambda, long seed) {
        if (sample == null || sample.length == 0)
            throw new IllegalArgumentException("sample empty");

        int d = sample[0].length;
        SplittableRandom rnd = new SplittableRandom(seed);

        double[][] alpha = new double[m][d];
        for (int j = 0; j < m; j++) {
            double norm = 0.0;
            for (int i = 0; i < d; i++) {
                double v = nextGaussian(rnd);
                alpha[j][i] = v;
                norm += v * v;
            }
            norm = Math.sqrt(Math.max(1e-12, norm));
            for (int i = 0; i < d; i++) alpha[j][i] /= norm;
        }

        double[] min = new double[m];
        double[] max = new double[m];
        java.util.Arrays.fill(min, Double.POSITIVE_INFINITY);
        java.util.Arrays.fill(max, Double.NEGATIVE_INFINITY);

        for (double[] v : sample) {
            for (int j = 0; j < m; j++) {
                double y = dot(v, alpha[j]);
                if (y < min[j]) min[j] = y;
                if (y > max[j]) max[j] = y;
            }
        }

        double[] r = new double[m];
        double[] w = new double[m];
        for (int j = 0; j < m; j++) {
            double range = Math.max(1e-6, max[j] - min[j]);
            double omega = range * Math.pow(2.0, -lambda);
            if (!(omega > 0)) omega = 1e-3;
            w[j] = omega;
            r[j] = rnd.nextDouble() * omega;
        }

        return new GFunction(alpha, r, w, lambda, seed);
    }

    // ============================================================
    // Algorithm-1: H(v) and C(v)
    // ============================================================

    /** Compute H(v) = { h_j(v) }. */
    public static int[] H(double[] v, GFunction G) {
        requireVector(v, G.d);
        int[] out = new int[G.m];
        for (int j = 0; j < G.m; j++) {
            double y = dot(v, G.alpha[j]) + G.r[j];
            out[j] = (int) Math.floor(y / G.omega[j]);
        }
        return out;
    }

    /** Compute bit-interleaved code C(v). */
    public static BitSet C(double[] v, GFunction G) {
        int[] H = H(v, G);
        BitSet out = new BitSet(G.codeBits());

        int pos = 0;
        for (int i = 0; i < G.lambda; i++) {
            for (int j = 0; j < G.m; j++) {
                if (((H[j] >>> i) & 1) != 0)
                    out.set(pos);
                pos++;
            }
        }
        return out;
    }

    // ============================================================
    // Wrapper method: code() - generates codes for all divisions
    // ============================================================

    /**
     * Generate bit-interleaved codes for all divisions.
     *
     * Creates a separate GFunction for each division using a unique seed,
     * then computes the code for each division independently.
     *
     * @param vec the vector to code
     * @param divisions number of divisions
     * @param m projections per division
     * @param seed base seed (division-specific seeds derived from this)
     * @return array of BitSets, one per division
     */
    public static BitSet[] code(double[] vec, int divisions, int m, long seed) {
        if (vec == null || vec.length == 0)
            throw new IllegalArgumentException("vec empty");
        if (divisions <= 0)
            throw new IllegalArgumentException("divisions ≤ 0");
        if (m <= 0)
            throw new IllegalArgumentException("m ≤ 0");

        BitSet[] result = new BitSet[divisions];

        // For each division, create a GFunction with unique seed
        for (int d = 0; d < divisions; d++) {
            long divisionSeed = seed + d;  // Unique seed per division
            GFunction G = buildRandomG(vec.length, m, DEFAULT_LAMBDA, DEFAULT_OMEGA, divisionSeed);
            result[d] = C(vec, G);
        }

        return result;
    }

    /**
     * Advanced variant: generate codes with custom lambda.
     *
     * @param vec the vector to code
     * @param divisions number of divisions
     * @param m projections per division
     * @param lambda bits per projection
     * @param seed base seed
     * @return array of BitSets, one per division
     */
    public static BitSet[] code(double[] vec, int divisions, int m, int lambda, long seed) {
        if (vec == null || vec.length == 0)
            throw new IllegalArgumentException("vec empty");
        if (divisions <= 0)
            throw new IllegalArgumentException("divisions ≤ 0");
        if (m <= 0)
            throw new IllegalArgumentException("m ≤ 0");
        if (lambda <= 0)
            throw new IllegalArgumentException("lambda ≤ 0");

        BitSet[] result = new BitSet[divisions];

        for (int d = 0; d < divisions; d++) {
            long divisionSeed = seed + d;
            GFunction G = buildRandomG(vec.length, m, lambda, DEFAULT_OMEGA, divisionSeed);
            result[d] = C(vec, G);
        }

        return result;
    }

    // ============================================================
    // Helpers
    // ============================================================

    private static double nextGaussian(SplittableRandom r) {
        double u1 = Math.max(Double.MIN_VALUE, r.nextDouble());
        double u2 = r.nextDouble();
        double mag = Math.sqrt(-2.0 * Math.log(u1));
        return mag * Math.cos(2.0 * Math.PI * u2);
    }

    private static double dot(double[] a, double[] b) {
        double acc = 0.0;
        for (int i = 0; i < a.length; i++) acc += a[i] * b[i];
        return acc;
    }

    private static void requireVector(double[] v, int d) {
        if (v == null || v.length != d)
            throw new IllegalArgumentException("Expected vector length " + d);
        for (double x : v)
            if (Double.isNaN(x) || Double.isInfinite(x))
                throw new IllegalArgumentException("Vector contains NaN/Inf");
    }
}