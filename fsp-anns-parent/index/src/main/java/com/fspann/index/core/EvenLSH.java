package com.fspann.index.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * LEGACY: EvenLSH (multi-probe Hamming fanout over sign projections)
 * -----------------------------------------------------------------
 * This class supports the older "multi-probe per-table bucketization" path.
 * It is kept ONLY for compatibility with existing tests/benchmarks when
 * -Dfspann.mode=multiprobe. The paper-aligned path (default) should use:
 *   Coding (Algorithm-1) -> Greedy partition (Algorithm-2) -> Tag query (Algorithm-3).
 *
 * What this does:
 *  - Builds a deterministic family of random Gaussian projections.
 *  - Uses sign(α · v) bits to form a per-table signature.
 *  - Expands around the primary signature by flipping the lowest-margin bits
 *    to generate additional buckets (multi-probe).
 *
 * Notes:
 *  - Metric & accuracy: this is *not* the SANNP path and typically yields
 *    lower recall at a given latency unless probed fairly widely.
 *  - If you use this for experiments, consider relaxing fanout clamps and
 *    widening per-table probe limits (see system properties below).
 *
 * System properties (read once at call sites):
 *  - probe.perTable (int, default 32): total probes per table.
 *  - probe.bits.max (int, default 2): maximum Hamming radius (bit flips).
 *
 * Thread-safety: instances are immutable after construction and are safe
 * for concurrent use.
 */
@Deprecated
public final class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);

    // ---- Core immutable state ------------------------------------------------
    private final int dimensions;
    private final int numBuckets;
    private final int numProjections;           // ∈ [numBuckets, 8*numBuckets]
    private final double[][] projectionVectors; // [numProjections][dimensions], L2-normalized
    private final long seed;
    private final int bitWidth;                 // number of signature bits == ceil(log2(numBuckets))

    // ---- Tuning constants ----------------------------------------------------
    private static final int UNROLL_THRESHOLD = 256; // loop unrolling pivot
    private volatile int overrideHashFuncsPerTable = -1;

    // ----------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------
    public EvenLSH(int dimensions, int numBuckets) {
        this(dimensions, numBuckets, calculateNumProjections(dimensions, numBuckets));
    }

    public EvenLSH(int dimensions, int numBuckets, int numProjections) {
        this(dimensions, numBuckets, numProjections, defaultSeed(dimensions, numBuckets, numProjections));
    }

    public EvenLSH(int dimensions, int numBuckets, int numProjections, long seed) {
        if (dimensions <= 0 || numBuckets <= 0 || numProjections <= 0) {
            throw new IllegalArgumentException("dimensions, numBuckets, numProjections must be > 0");
        }
        this.dimensions = dimensions;
        this.numBuckets = numBuckets;
        this.numProjections = Math.max(numBuckets, Math.min(numProjections, numBuckets * 8));
        this.seed = seed;

        // bitWidth = number of bits needed to address numBuckets
        // e.g., numBuckets=256 -> bitWidth=8; numBuckets=300 -> bitWidth=9 (folding used)
        this.bitWidth = 32 - Integer.numberOfLeadingZeros(this.numBuckets - 1);

        // Deterministic Gaussian projection vectors, L2-normalized
        this.projectionVectors = new double[this.numProjections][dimensions];
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

        logger.info("Initialized EvenLSH dims={}, buckets={}, projections={}, seed={}, bitWidth={}",
                dimensions, numBuckets, this.numProjections, seed, bitWidth);
    }

    /** Adaptive projection count: ~ buckets * log2(dim/16), clamped to [buckets, 8*buckets]. */
    private static int calculateNumProjections(int dimensions, int numBuckets) {
        int proj = (int) Math.ceil(numBuckets * Math.log(Math.max(dimensions, 1) / 16.0) / Math.log(2));
        return Math.max(numBuckets, Math.min(proj, numBuckets * 8));
    }

    /** Default seed stable across restarts for the same (dims, buckets, projections). */
    private static long defaultSeed(int dims, int buckets, int projs) {
        long x = 0x9E3779B97F4A7C15L;
        x ^= (long) dims    * 0xBF58476D1CE4E5B9L;
        x ^= (long) buckets * 0x94D049BB133111EBL;
        x ^= (long) projs   + 0x2545F4914F6CDD1DL;
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

    // ----------------------------------------------------------------------------
    // Public API (legacy multiprobe)
    // ----------------------------------------------------------------------------

    /** Compatibility: legacy code may reflect this as "m (rows/hash-funcs per table)". */
    public int getNumHashFuncsPerTable() { return (overrideHashFuncsPerTable > 0) ? overrideHashFuncsPerTable : bitWidth; }
   /** Compatibility alias used by some callers. */
   public int getRowsPerBand() { return getNumHashFuncsPerTable(); }
    /** Allow overriding "m" without rebuilding LSH; affects signature slice size and expansions. */
    public void setNumHashFuncsPerTable(int m) { if (m > 0) this.overrideHashFuncsPerTable = m; }
    /** Compatibility alias used by some callers. */
    public void setRowsPerBand(int m) { setNumHashFuncsPerTable(m); }

    /** Project onto the tableIndex-th (wrapped) projection vector. */
    public double project(double[] vector, int tableIndex) {
        validateVector(vector);
        int idx = Math.floorMod(tableIndex, numProjections);
        return dot(vector, projectionVectors[idx]);
    }

    /** Primary bucket id for a vector at a given table. */
    public int getBucketId(double[] vector, int tableIndex) {
        int sig = signature(vector, tableIndex, null);
        return foldToBucketRange(sig);
    }

    /** Primary bucket ids for all tables [0..numTables-1]. */
    public int[] getBucketIdsForAllTables(double[] vector, int numTables) {
        validateVector(vector);
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        int[] out = new int[numTables];
        for (int t = 0; t < numTables; t++) out[t] = getBucketId(vector, t);
        return out;
    }

    /**
     * Per-table expansions (multi-probe). Uses bit margins to rank flips:
     * smaller |p_j| means closer to the decision boundary -> more likely neighbor.
     * @param vector plaintext vector
     * @param topK drives caller’s intent; not used directly here but kept for signature
     * @param tableIndex which table (affects signature slice)
     */
    public List<Integer> getBuckets(double[] vector, int topK, int tableIndex) {
        validateVector(vector);

        final int limit   = Integer.getInteger("probe.perTable", 32); // total probes per table
        final int maxFlip = Integer.getInteger("probe.bits.max", 2);  // 1–2 good; 3 is expensive
        final int bw = getNumHashFuncsPerTable();
        double[] margins = new double[bw];
        int sig = signature(vector, tableIndex, margins);

        List<Integer> expanded = expandByHamming(sig, margins, maxFlip, limit);
        return expanded;
    }

    /** Per-table expansions for all tables. */
    public List<List<Integer>> getBucketsForAllTables(double[] vector, int topK, int numTables) {
        validateVector(vector);
        if (numTables <= 0) throw new IllegalArgumentException("numTables must be > 0");
        List<List<Integer>> all = new ArrayList<>(numTables);
        for (int t = 0; t < numTables; t++) {
            all.add(getBuckets(vector, topK, t));
        }
        return all;
    }

    public int getDimensions() { return dimensions; }
    public int getNumBuckets() { return numBuckets; }
    public long getSeed() { return seed; }
    public int getBitWidth() { return bitWidth; }

    // ----------------------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------------------

    /**
     * Build a bit-signature for a vector:
     *  - We take a table-dependent slice of size bitWidth from the projection pool.
     *  - Sign of each projection becomes one bit.
     *  - If marginsOut != null, we store |projection| as a proxy distance to the hyperplane.
     *
     * NOTE: This uses contiguous slices modulo numProjections for determinism.
     * For stronger independence across tables, consider a table-dependent
     * permutation/stride; retained as-is for backward compatibility.
     */
    private int signature(double[] v, int tableIndex, double[] marginsOut) {
        int s = 0;
        final int bw = getNumHashFuncsPerTable();
        int base = Math.floorMod(tableIndex * bw, numProjections);
        for (int j = 0; j < bw; j++) {
                double pj = dot(v, projectionVectors[(base + j) % numProjections]);
                if (marginsOut != null) marginsOut[j] = Math.abs(pj);
                if (pj >= 0) s |= (1 << j);
            }
            return s;
        }

    /** Expand by flipping the lowest-margin bits, up to maxBitsToFlip and global probe limit. */
    private List<Integer> expandByHamming(int sig, double[] margins, int maxBitsToFlip, int limit) {
        final int bw = margins.length;
        Integer[] order = new Integer[bw];
        for (int i = 0; i < bw; i++) order[i] = i;
        Arrays.sort(order, Comparator.comparingDouble(i -> margins[i]));
        List<Integer> out = new ArrayList<>(Math.min(limit, 1 + bw + bw * (bw - 1) / 2));
            out.add(sig); // primary first

        // 1-bit flips
        for (int i = 0; i < bw && out.size() < limit; i++) {
                    out.add(sig ^ (1 << order[i]));
                }
        if (out.size() >= limit || maxBitsToFlip < 2) return foldBuckets(out);

        // 2-bit flips
        outer:
        for (int i = 0; i < bw; i++) {
            for (int j = i + 1; j < bw; j++) {
                out.add(sig ^ (1 << order[i]) ^ (1 << order[j]));
                if (out.size() >= limit) break outer;
                            }
                        }
                        return foldBuckets(out);
                    }

    /** Fold arbitrary signature integers into [0, numBuckets-1]. */
    private List<Integer> foldBuckets(List<Integer> sigs) {
        List<Integer> buckets = new ArrayList<>(sigs.size());
        final boolean pow2 = isPowerOfTwo(numBuckets);
        final int mask = numBuckets - 1;
        for (int s : sigs) {
            buckets.add(pow2 ? (s & mask) : Math.floorMod(s, numBuckets));
        }
        return buckets;
    }

    /** Fold a single signature to bucket range. */
    private int foldToBucketRange(int sig) {
        return isPowerOfTwo(numBuckets) ? (sig & (numBuckets - 1)) : Math.floorMod(sig, numBuckets);
    }

    /** L2 dot product, lightly unrolled for medium/large dims. */
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
                s += a[i] * b[i]
                        + a[i + 1] * b[i + 1]
                        + a[i + 2] * b[i + 2]
                        + a[i + 3] * b[i + 3]
                        + a[i + 4] * b[i + 4]
                        + a[i + 5] * b[i + 5]
                        + a[i + 6] * b[i + 6]
                        + a[i + 7] * b[i + 7];
            }
            for (; i < n; i++) s += a[i] * b[i];
            return s;
        }
    }

    private void validateVector(double[] v) {
        if (v == null || v.length != dimensions) {
            throw new IllegalArgumentException("Vector dims mismatch: expected=" + dimensions + " got=" + (v == null ? 0 : v.length));
        }
        for (double x : v) {
            if (Double.isNaN(x) || Double.isInfinite(x)) {
                throw new IllegalArgumentException("Vector has NaN/Inf");
            }
        }
    }

    private static boolean isPowerOfTwo(int x) { return (x & (x - 1)) == 0; }
}
