package com.fspann.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.security.SecureRandom;
import java.util.*;

/**
 * EvenLSH – cosine-projection / dynamic-quantile version
 *
 *  • m (numBuckets) is chosen at runtime from dataset size N :  m ≈ √N
 *  • projection vector stays unit-length (random hyper-plane)
 *  • criticalAngles[] stores the quantile cut-points (cosθ values)
 */
public class EvenLSH {
    private static final Logger logger = LoggerFactory.getLogger(EvenLSH.class);

    private double[] projectionVector;      // random unit vector
    private double[] criticalAngles;        // quantile cut-points
    private final int dimensions;
    private int numBuckets;                 // may grow if data grows
    private static final double DEFAULT_BETA = 1.5;   // imbalance tolerance

    /* ---------- ctor ---------- */
    public EvenLSH(int dimensions, int numBuckets) {
        this.dimensions  = dimensions;
        this.numBuckets  = Math.max(2, numBuckets);
        this.projectionVector = randomUnitVector(dimensions);
        this.criticalAngles   = new double[this.numBuckets - 1];
    }

    /* ---------- public API ---------- */

    /** Scalar cosine projection of a point on the internal hyper-plane */
    public double project(double[] point) {                   // <-- ADDED
        double sum = 0;
        for (int i = 0; i < point.length; i++) sum += point[i] * projectionVector[i];
        return sum;
    }

    /** Returns bucket id in [1 … numBuckets] (1-based) */
    public int getBucketId(double[] point) {
        double proj = dot(point, projectionVector);
        for (int i = 0; i < criticalAngles.length; i++)
            if (proj <= criticalAngles[i]) return i + 1;
        return numBuckets;                       // last bucket
    }

    /** Recompute quantile cut-points from the whole dataset (dynamic N) */
    public void updateCriticalValues(List<double[]> data) {
        if (data == null || data.isEmpty()) {
            logger.warn("Cannot update critical values: dataset is null or empty");
            throw new IllegalArgumentException("Dataset must not be empty");
        }
        if (numBuckets <= 0) {
            logger.error("Invalid number of buckets: {}", numBuckets);
            throw new IllegalArgumentException("Number of buckets must be positive");
        }

        try {
            this.criticalAngles = new double[numBuckets];
            List<Double> proj = new ArrayList<>(data.size());
            for (double[] v : data) {
                if (v == null || v.length == 0) {
                    logger.warn("Skipping invalid vector in projection: {}", Arrays.toString(v));
                    continue;
                }
                proj.add(project(v));
            }

            if (proj.isEmpty()) {
                logger.error("No valid projections computed from dataset");
                throw new IllegalArgumentException("No valid projections computed");
            }

            Collections.sort(proj);
            for (int k = 1; k <= numBuckets; k++) {
                int idx = (int) Math.floor(k * proj.size() / (double)(numBuckets + 1));
                criticalAngles[k - 1] = proj.get(Math.min(idx, proj.size() - 1));
            }
            logger.debug("Updated critical values with {} projections", proj.size());
        } catch (Exception e) {
            logger.error("Error updating critical values: {}", e.getMessage(), e);
            throw new RuntimeException("Error updating critical values", e);
        }
    }

    /** Generate a fresh random projection vector (key-rotation use-case) */
    public void rehash(long seed) {
        this.projectionVector = randomUnitVector(dimensions, seed);
    }

    /** Expose cut-points for debugging / fake-point logic */
    public double[] getCriticalAngles() {
        return criticalAngles.clone();
    }

    public double[] getCriticalValues() {      // ✅
        return getCriticalAngles();
    }

    /* ---------- helpers ---------- */

    private static double dot(double[] a, double[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) s += a[i] * b[i];
        return s;
    }

    /* random unit vector – optional deterministic seed */
    private static double[] randomUnitVector(int d) { return randomUnitVector(d, new SecureRandom().nextLong()); }
    private static double[] randomUnitVector(int d, long seed) {
        SecureRandom rng = new SecureRandom();
        rng.setSeed(seed);

        double[] v = new double[d];
        double norm = 0;
        for (int i = 0; i < d; i++) {
            v[i] = rng.nextGaussian();
            norm += v[i] * v[i];
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < d; i++) v[i] /= norm;
        return v;
    }

    /**
     * Return a list of neighbouring bucket IDs around {@code mainBucket}.
     * This keeps the old “±expansionRange” behaviour so existing code compiles.
     * (We’ll later add a bit-flip variant when we move to binary codes.)
     */
    public List<Integer> expandBuckets(int mainBucket, int expansionRange) {
        List<Integer> neighbours = new ArrayList<>();
        for (int offset = -expansionRange; offset <= expansionRange; offset++) {
            int b = mainBucket + offset;
            if (b >= 1 && b <= numBuckets) neighbours.add(b);
        }
        return neighbours;
    }

}
