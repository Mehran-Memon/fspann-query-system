package com.fspann.index.paper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Objects;
import java.util.SplittableRandom;

/**
 * Coding (Algorithm-1) - FIXED VERSION
 * ------------------------------------
 * CRITICAL FIX: Bit ordering changed to MSB-first.
 *
 * Previous (WRONG):
 *   - i=0 (LSB) at positions 0 to m-1
 *   - i=1 (MSB for λ=2) at positions m to 2m-1
 *
 * This caused prefix relaxation to match on LESS significant bits,
 * which is semantically wrong. Result: candidates are essentially random.
 *
 * Fixed (CORRECT):
 *   - i=λ-1 (MSB) at positions 0 to m-1
 *   - i=0 (LSB) at positions (λ-1)*m to λ*m-1
 *
 * Now prefix relaxation (comparing fewer bits from position 0) matches
 * on the MORE significant bits first, which is correct for ANN search.
 *
 * This single fix should change precision from 0% to 50-90%.
 */
public final class Coding {

    private Coding() {}

    // Default parameters
    private static final double DEFAULT_OMEGA = 1.0;

    private static final Logger log =
            LoggerFactory.getLogger(Coding.class);

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
     *
     * ADAPTIVE OMEGA STRATEGY:
     * -----------------------
     * 1. Estimate typical pairwise distance from sample
     * 2. Set ω proportional to this distance
     * 3. Scale by 1/sqrt(m) for multi-projection LSH
     *
     * This approach automatically adapts to:
     * - Different datasets (SIFT, Glove, Deep1B)
     * - Different dimensions (96, 100, 128)
     * - Different configurations (m, lambda, tables)
     */
    /**
     * Build GFunction from sample with RANGE-BASED omega (simplified, robust).
     */
    public static GFunction buildFromSample(double[][] sample, int m, int lambda, long seed) {
        if (sample == null || sample.length == 0)
            throw new IllegalArgumentException("sample empty");

        int d = sample[0].length;
        SplittableRandom rnd = new SplittableRandom(seed);

        // Build random projections
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

        // Compute projection ranges
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

        // ============================================================
        // OMEGA CALCULATION: Range-based (simple and robust)
        // ============================================================
        // TUNING: Increase divisor for narrower buckets (lower recall, faster)
        //         Decrease divisor for wider buckets (higher recall, slower)
        // Recommended starting point: 2.0 for SIFT1M
        final double OMEGA_DIVISOR = 2.5;

        double[] r = new double[m];
        double[] w = new double[m];

        for (int j = 0; j < m; j++) {
            double range = Math.max(1e-6, max[j] - min[j]);
            double omega = range / OMEGA_DIVISOR;

            // Safety check
            if (!(omega > 0)) omega = 1e-3;

            w[j] = omega;
            r[j] = rnd.nextDouble() * omega;
        }

        return new GFunction(alpha, r, w, lambda, seed);
    }

    // ============================================================
    // Algorithm-1: H(v) and C(v)
    // ============================================================

    /**
     * Compute H(v) = { h_j(v) }.
     */
    public static int[] H(double[] v, GFunction G) {
        requireVector(v, G.d);
        int[] out = new int[G.m];
        for (int j = 0; j < G.m; j++) {
            double y = dot(v, G.alpha[j]) + G.r[j];
            out[j] = (int) Math.floor(y / G.omega[j]);
        }
        return out;
    }

    /**
     * Collapse multi-projection H(v) into a single integer hash
     * Deterministic, order-sensitive, fast.
     */
    public static int H1(double[] v, GFunction G) {
        int[] H = H(v, G);
        int h = 0;
        for (int x : H) {
            h = 31 * h + x;   // standard hash mixing
        }
        return h;
    }

    /**
     * Compute bit-interleaved code C(v).
     *
     * CRITICAL FIX: MSB-first ordering.
     *
     * For lambda=2, m=24:
     *   - positions 0-23: MSB (i=1) of each h_j
     *   - positions 24-47: LSB (i=0) of each h_j
     *
     * This ensures that prefix matching (comparing first N bits)
     * matches on MORE significant bits first.
     */
    public static BitSet C(double[] v, GFunction G) {
        int[] H = H(v, G);
        BitSet out = new BitSet(G.codeBits());

        int pos = 0;

        // ============================================================
        // CRITICAL FIX: Iterate i from (lambda-1) DOWN to 0
        // This puts MSBs at low positions for correct prefix matching
        // ============================================================
        for (int i = G.lambda - 1; i >= 0; i--) { // MSB first!
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
     * Generate codes for all divisions.
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