package com.fspann.index.paper;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.BitSet;
import java.util.Objects;
import java.util.SplittableRandom;

/**
 * Coding (Algorithm-1)
 * --------------------
 * Build an LSH family G and compute the interleaved bit code C(v).
 *
 * We implement the family G_m as a set of m independent projections with
 * per-projection offset r_j and scale ω_j. Then we compute:
 *
 *   h_j(v) = floor( (α_j · v + r_j) / ω_j )          (1)
 *
 * Interleave the i-th bit of each h_j across j for i∈[0..λ-1] to produce
 * a code C(v) of length (m * λ) bits.
 */
public final class Coding {

    private Coding() {}

    /** Parameters of a G-function family used to produce H(v) and C(v). */
    public static final class GFunction implements Serializable {
        public final double[][] alpha; // [m][d] Gaussian rows, L2-normalized
        public final double[] r;       // [m] offset r_j ∈ [0, ω_j)
        public final double[] omega;   // [m] bin width ω_j > 0
        public final int m;            // number of projections
        public final int lambda;       // bits taken from each h_j
        public final int d;            // dimensionality
        public final long seed;        // seed for reproducibility

        public GFunction(double[][] alpha, double[] r, double[] omega, int lambda, long seed) {
            this.alpha = Objects.requireNonNull(alpha, "alpha");
            this.r = Objects.requireNonNull(r, "r");
            this.omega = Objects.requireNonNull(omega, "omega");
            if (alpha.length == 0) throw new IllegalArgumentException("alpha empty");
            this.m = alpha.length;
            this.d = alpha[0].length;
            if (r.length != m || omega.length != m) {
                throw new IllegalArgumentException("r/omega length must equal m");
            }
            for (double w : omega) if (!(w > 0.0)) throw new IllegalArgumentException("omega_j must be > 0");
            if (lambda <= 0) throw new IllegalArgumentException("lambda must be > 0");
            this.lambda = lambda;
            this.seed = seed;
        }

        /** Total code length (bits). */
        public int codeBits() { return m * lambda; }
    }

    // ----------------------------------------------------------------------
    // Factories for G
    // ----------------------------------------------------------------------

    /**
     * Build a random G with Gaussian alpha, L2-normalized rows, fixed ω, randomized r∈[0,ω).
     * Use this when you don't have dataset stats yet.
     */
    public static GFunction buildRandomG(int d, int m, int lambda, double omega, long seed) {
        if (omega <= 0) throw new IllegalArgumentException("omega must be > 0");
        SplittableRandom rnd = new SplittableRandom(seed);
        double[][] alpha = new double[m][d];
        for (int j = 0; j < m; j++) {
            double norm = 0.0;
            for (int i = 0; i < d; i++) {
                double v = nextGaussian(rnd);
                alpha[j][i] = v; norm += v * v;
            }
            norm = Math.sqrt(Math.max(1e-12, norm));
            for (int i = 0; i < d; i++) alpha[j][i] /= norm;
        }
        double[] r = new double[m];
        double[] w = new double[m];
        for (int j = 0; j < m; j++) {
            w[j] = omega;
            r[j] = rnd.nextDouble() * omega;
        }
        return new GFunction(alpha, r, w, lambda, seed);
    }

    /**
     * Build G using a sample to estimate per-projection scale ω_j.
     * ω_j ≈ (max(y_j) - min(y_j)) * 2^-λ as per paper rationale.
     */
    public static GFunction buildFromSample(double[][] sample, int m, int lambda, long seed) {
        if (sample == null || sample.length == 0) throw new IllegalArgumentException("sample empty");
        int d = sample[0].length;
        SplittableRandom rnd = new SplittableRandom(seed);

        // alpha
        double[][] alpha = new double[m][d];
        for (int j = 0; j < m; j++) {
            double norm = 0.0;
            for (int i = 0; i < d; i++) {
                double v = nextGaussian(rnd);
                alpha[j][i] = v; norm += v * v;
            }
            norm = Math.sqrt(Math.max(1e-12, norm));
            for (int i = 0; i < d; i++) alpha[j][i] /= norm;
        }

        // project sample to estimate ranges
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

        double[] w = new double[m];
        double[] r = new double[m];
        for (int j = 0; j < m; j++) {
            double range = Math.max(1e-6, max[j] - min[j]);
            double omega = range * Math.pow(2.0, -lambda);
            if (!(omega > 0.0)) omega = 1e-3;
            w[j] = omega;
            r[j] = rnd.nextDouble() * omega;
        }

        return new GFunction(alpha, r, w, lambda, seed);
    }

    // ----------------------------------------------------------------------
    // Algorithm-1: H(v) and C(v)
    // ----------------------------------------------------------------------

    /** Compute H(v) = { h_j(v) } for j∈[0..m-1]. */
    public static int[] H(double[] v, GFunction G) {
        requireVector(v, G.d);
        int[] out = new int[G.m];
        for (int j = 0; j < G.m; j++) {
            double y = dot(v, G.alpha[j]) + G.r[j];
            out[j] = (int) Math.floor(y / G.omega[j]);
        }
        return out;
    }

    /** Compute interleaved bit code C(v) of length (m * λ). */
    public static BitSet C(double[] v, GFunction G) {
        int[] H = H(v, G);
        BitSet code = new BitSet(G.codeBits());
        int pos = 0;
        for (int i = 0; i < G.lambda; i++) {
            for (int j = 0; j < G.m; j++) {
                if (((H[j] >>> i) & 1) != 0) code.set(pos);
                pos++;
            }
        }
        return code;
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------
    private static double nextGaussian(SplittableRandom r) {
        double u1 = Math.max(Double.MIN_VALUE, r.nextDouble());
        double u2 = r.nextDouble();
        double mag = Math.sqrt(-2.0 * Math.log(u1));
        return mag * Math.cos(2.0 * Math.PI * u2);
    }

    private static double dot(double[] a, double[] b) {
        double s = 0.0; for (int i = 0; i < a.length; i++) s += a[i] * b[i]; return s;
    }

    private static void requireVector(double[] v, int d) {
        if (v == null || v.length != d)
            throw new IllegalArgumentException("Expected vector length " + d + " got " + (v == null ? 0 : v.length));
        for (double x : v) if (Double.isNaN(x) || Double.isInfinite(x))
            throw new IllegalArgumentException("Vector has NaN/Inf");
    }
}
